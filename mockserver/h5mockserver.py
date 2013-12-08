"""
This file implements an http server.
It serves up HDF5 datasets over http using the DVID REST API.

The server can also be started up in stand-alone mode:

    $ cd mockserver
    $ PYTHONPATH=.. python h5mockserver.py my_hdf5_file.h5

Internally, your hdf5 file must be a two-level heirarchy, such that each dataset is accessed via: `/uuid/dataset_name`.
Furthermore, each dataset:
- Must include a channel axis
- Must have an "axistags" attribute as produced by `vigra.AxisTags.toJSON()`
- Must be in C-order, e.g. zyxc

LIMITATIONS:
Obviously, the aim here is not to implement the full DVID API.
- The user's query MUST include all axes (i.e. the <dims> parameter must be something like 0_1_2, not 0_2).
- The <format> parameter is not supported.
  Data is always returned as binary volume buffer data.
  REST queries including the format parameter will result in error 400 (bad syntax)
"""
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler

import numpy
import h5py
import vigra

from dvidclient.volume_metainfo import get_dataset_metainfo, format_metainfo_to_json
from dvidclient.volume_codec import VolumeCodec

class H5CutoutRequestHandler(BaseHTTPRequestHandler):
    """
    The request handler for the H5MockServer.
    
    Supports the following DVID REST calls:
    
    Meta info:
        GET  /api/node/<UUID>/<data name>/info
    
    Cutout subvolume:
        GET  /api/node/<UUID>/<data name>/<dims>/<size>/<offset>
    
    Modify subvolume:
        POST  /api/node/<UUID>/<data name>/<dims>/<size>/<offset>
    """
    
    # Data is retrieved from the http response stream in chunks.
    STREAM_CHUNK_SIZE = 1000 # (bytes)
    VOLUME_MIMETYPE = "binary/imagedata"

    def do_GET(self):
        self._handle_request("GET")
    def do_POST(self):
        self._handle_request("POST")

    def _handle_request(self, method):
        """
        Entry point for all request handling.
        Support GET queries for dataset info or subvolume data.
        Also support POST for dataset subvolume data.
        """
        params = self.path.split('/')
        if params[0] == '':
            params = params[1:]

        if len(params) < 5:
            self.send_error(400, "Bad query syntax: {}".format( self.path ))
            return

        uuid, data_name = params[2:4]

        dataset_path = uuid + '/' + data_name
        if dataset_path not in self.server.h5_file:
            self.send_error(404, "Couldn't find dataset: {} in file {}".format( dataset_path, self.server.h5_file.filename ))
            return

        # For this mock server, we assume the data can be found inside our file at /uuid/data_name
        dataset = self.server.h5_file[dataset_path]

        if len(params) == 5:
            self._do_get_info(params, dataset)
        elif len(params) == 7:
            if method == "GET":
                self._do_get_data(params, dataset)
            elif method == "POST":
                self._do_modify_data(params, dataset)
            else:
                self.send_error(405, "Unsupported method: {}".format( method ))
                return
        else:
            self.send_error(400, "Bad query syntax: {}".format( self.path ))
            return

    def _do_get_info(self, params, dataset):
        """
        Respond to a query for dataset info.
        
        params: The full list of REST parameters with the current query.
                For example: ['api', 'node', 'abc123', 'grayscale_vol', 'info']
        dataset: An h5py.Dataset object the user wants info for.
        """
        assert len(params) == 5
        cmd = params[4]
        if cmd != 'schema':
            self.send_error(400, "Bad query syntax: {}".format( self.path ))
            return
        
        metainfo = get_dataset_metainfo(dataset)
        json_text = format_metainfo_to_json(metainfo)

        self.send_response(200)
        self.send_header("Content-type", "text/json")
        self.send_header("Content-length", str(len(json_text)))
        self.end_headers()
        self.wfile.write( json_text )

    def _do_get_data(self, params, dataset):
        """
        Respond to a query for volume data.

        params: The full list of REST parameters with the current query.
                For example: ['api', 'node', 'abc123', 'grayscale_vol', '10_20_30', '50_50_50']
        dataset: An h5py.Dataset object to extract the data from.
        """
        roi_start, roi_stop = self._determine_request_roi( params, dataset )
        slicing = tuple( slice(x,y) for x,y in zip(roi_start, roi_stop) )
        
        # Reverse here because API uses fortran order, but data is stored in C-order
        data = dataset[tuple(reversed(slicing))]
        axistags = vigra.AxisTags.fromJSON( dataset.attrs['axistags'] )
        v_array = vigra.taggedView( data, axistags )
        
        metainfo = get_dataset_metainfo(dataset)
        codec = VolumeCodec( metainfo )
        buffer_len = codec.calculate_buffer_len( data.shape )

        self.send_response(200)
        self.send_header("Content-type", self.VOLUME_MIMETYPE)
        self.send_header("Content-length", str(buffer_len) )
        self.end_headers()

        codec.encode_from_vigra_array( self.wfile, v_array.transpose() )
    
    def _do_modify_data(self, params, dataset):
        """
        Respond to a POST request to modify a subvolume of data.

        params: The full list of REST parameters with the current query.
                For example: ['api', 'node', 'abc123', 'grayscale_vol', '10_20_30', '50_50_50']
        dataset: An h5py.Dataset object to modify.
        """
        roi_start, roi_stop = self._determine_request_roi( params, dataset )
        # Prepend channel to make "full" roi
        full_roi_start = (0,) + roi_start
        full_roi_stop = (dataset.shape[-1],) + roi_stop
        full_roi_shape = numpy.subtract(full_roi_stop, full_roi_start)
        slicing = tuple( slice(x,y) for x,y in zip(full_roi_start, full_roi_stop) )
        
        metainfo = get_dataset_metainfo(dataset)
        codec = VolumeCodec( metainfo )
        v_array = codec.decode_to_vigra_array(self.rfile, full_roi_shape)

        # Reverse here because API uses fortran order, but data is stored in C-order
        dataset[tuple(reversed(slicing))] = v_array.transpose()
        self.send_response(204) # "No Content" (accepted)
        self.send_header("Content-length", 0 )
        self.end_headers()
    
    def _determine_request_roi(self, params, dataset):
        """
        Parse the given REST parameters to determine the request region of interest.
        
        Returns: Coordinates start, stop (in API order, without channel index)
        
        params: The full list of REST parameters with the current query.
                For example: ['api', 'node', 'abc123', 'grayscale_vol', '10_20_30', '50_50_50']
        dataset: An h5py.Dataset object, used for validation of the parsed roi.
        """
        assert len(params) == 7
        if params[0] != 'api' or \
           params[1] != 'node':
            self.send_error(400, "Bad query syntax: {}".format( self.path ))
            return
        
        dims_str, roi_shape_str, roi_start_str = params[4:]

        dataset_ndims = len(dataset.shape)
        expected_dims_str = "_".join( map(str, range( dataset_ndims-1 )) )
        if dims_str != expected_dims_str:
            self.send_error(400, "For now, queries must include all dataset axes.  Your query requested dims: {}".format( dims_str ))
            return
        
        roi_start = tuple( int(x) for x in roi_start_str.split('_') )
        roi_shape = tuple( int(x) for x in roi_shape_str.split('_') )

        if len(roi_start) != dataset_ndims-1:
            self.send_error(400, "Invalid start coordinate: {} Expected {} dims, got {} ".format( roi_start, dataset_ndims-1, len(roi_start) ) )
            return
        if len(roi_shape) != dataset_ndims-1:
            self.send_error(400, "Invalid cutout shape: {} Expected {} dims, got {} ".format( roi_shape, dataset_ndims-1, len(roi_shape) ) )
            return
        
        roi_stop = tuple( numpy.array(roi_start) + roi_shape )        
        return roi_start, roi_stop

class H5MockServer(HTTPServer):
    def __init__(self, h5filepath, *args, **kwargs):
        """
        h5filepath: The hdf5 file to serve data from.
        See docstring above for requirements on the file contents.
        """
        HTTPServer.__init__(self, *args, **kwargs)
        self.h5filepath = h5filepath
    
    def serve_forever(self):
        with h5py.File( self.h5filepath ) as h5_file:
            self.h5_file = h5_file
            HTTPServer.serve_forever(self)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        sys.stderr.write("Usage: python {} <filename.h5>\n".format( sys.argv[0] ))
        sys.exit(1)
    
    filename = sys.argv[1]
    
    server_address = ('', 8000)
    server = H5MockServer( filename, server_address, H5CutoutRequestHandler )
    server.serve_forever()

    print "SERVER EXITED."
