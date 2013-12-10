"""
This file implements an http server.
It serves up HDF5 datasets over http using the DVID REST API.

The server can also be started up in stand-alone mode:

    $ cd mockserver
    $ PYTHONPATH=.. python h5mockserver.py my_hdf5_file.h5

Internally, your hdf5 file must be a hierarchy, such that each 
dvid data item is accessed via: `/datasets/dataset_name/uuid/dataset_name`,
and also accessed via an internal SoftLink at the root level: /uuid/dataset_name
Node groups should contain attributes 'parents' and 'children', which specify the layout of the DAG.

Furthermore, each hdf5 dataset must:
- include a channel axis
- have an "axistags" attribute as produced by `vigra.AxisTags.toJSON()`
- be in C-order, e.g. zyxc

LIMITATIONS:
Obviously, the aim here is not to implement the full DVID API.
- The user's subvolume queries MUST include all axes 
    (i.e. the <dims> parameter must be something like 0_1_2, not 0_2).
- The <format> parameter is not supported.
  Data is always returned as binary volume buffer data.
  REST queries including the format parameter will result in error 400 (bad syntax)
"""
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler

import numpy
import h5py
import vigra

from dvidclient.volume_metainfo import MetaInfo
from dvidclient.volume_codec import VolumeCodec

class H5CutoutRequestHandler(BaseHTTPRequestHandler):
    """
    The request handler for the H5MockServer.
    
    Supports the following DVID REST calls:
    
    Description info (a.k.a. metainfo):
        GET  /api/node/<UUID>/<data name>/info
    
    Create volume:
        POST /api/dataset/<UUID>/new/<datatype name>/<data name>
    
    Retrieve subvolume:
        GET  /api/node/<UUID>/<data name>/<dims>/<size>/<offset>
    
    Modify subvolume:
        POST  /api/node/<UUID>/<data name>/<dims>/<size>/<offset>
    """

    class RequestError( Exception ):
        def __init__(self, status_code, message):
            self.status_code = status_code
            self.message = message
    
    def do_GET(self):
        self._handle_request("GET")
    def do_POST(self):
        self._handle_request("POST")

    def _handle_request(self, method):
        """
        Entry point for all request handling.
        Call `_execute_request` and handle any exceptions.
        """
        try:
            self._execute_request(method)
        except H5CutoutRequestHandler.RequestError as ex:
            self.send_error( ex.status_code, ex.message )
        except Exception as ex:
            self.send_error( 500, "Server Error: See response body for traceback.  Crashing now..." )
            
            # Write exception traceback to the response body as an html comment.
            import traceback
            self.wfile.write("<!--\n")
            traceback.print_exc(file=self.wfile)
            self.wfile.write("\n-->")
            self.wfile.flush()
            
            raise # Now crash...

    def _execute_request(self, method):
        """
        Execute the current request.  Exceptions must be handled by the caller.

        Support GET queries for dataset info or subvolume data.
        Also support POST for dataset subvolume data.
        """
        params = self.path.split('/')
        if params[0] == '':
            params = params[1:]

        if len(params) < 5:
            raise self.RequestError(400, "Bad query syntax: {}".format( self.path ))

        uuid = params[2]

        # First check to see if the user is attempting to create a new volume.
        if len(params) == 6 and method == "POST":
            self._do_create_volume(params, uuid)
            return

        data_name = params[3]
        dataset_path = uuid + '/' + data_name

        # Otherwise, the volume should already exist.
        if dataset_path not in self.server.h5_file:
            raise self.RequestError( 404, "Couldn't find dataset: {} in file {}"
                                          "".format( dataset_path, self.server.h5_file.filename ) )

        # For this mock server, we assume the data can be found inside our h5 file at /uuid/data_name
        dataset = self.server.h5_file[dataset_path]

        if len(params) == 5:
            self._do_get_info(params, dataset)
        elif len(params) == 7:
            if method == "GET":
                self._do_get_data(params, dataset)
            elif method == "POST":
                self._do_modify_data(params, dataset)
            else:
                raise self.RequestError( 405, "Unsupported method for query: {} {}"
                                              "".format( method, self.path ) )
        else:
            raise self.RequestError(400, "Bad query syntax: {}".format( self.path ))

    def _do_create_volume(self, params, uuid):
        """
        The http client wants to create a new volume.
        Create it.
        """
        assert len(params) == 6
        
        if params[3] != "new":
            raise self.RequestError(400, "Bad request syntax: {}".format( self.path ))
        
        if uuid not in self.server.h5_file:
            raise self.RequestError( 404, "No such node with uuid {}".format( uuid ) )
        
        data_name = params[5]
        dataset_path = uuid + '/' + data_name
        
        if dataset_path in self.server.h5_file:
            raise self.RequestError( 409, "Cannot create.  Volume {} already exists."
                                     .format( dataset_path ) )

        # Must read exact bytes.
        # Apparently rfile.read() just hangs.
        body_len = self.headers.get("Content-Length")
        metainfo_json = self.rfile.read( int(body_len) )
        try:
            metainfo = MetaInfo.create_from_json( metainfo_json )
        except ValueError as ex:
            raise self.RequestError( 400, 'Can\'t create volume.  '
                                          'Error parsing volume description: {}\n'
                                          'Invalid description text was:\n{}'
                                          ''.format( ex.args[0], metainfo_json ) )
        rest_typename = params[4]
        expected_typename = metainfo.determine_dvid_typename()
        if rest_typename != expected_typename:
            raise self.RequestError( 400, "Cannot create volume.  "
                                          "REST typename was {}, but metainfo JSON implies typename {}"
                                          "".format( rest_typename, expected_typename ) )

        metainfo.create_empty_h5_dataset( self.server.h5_file, dataset_path )
        self.server.h5_file.flush()

        self.send_response(204) # "No Content" (accepted)
        self.send_header("Content-length", "0" )
        self.end_headers()

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
            raise self.RequestError(400, "Bad query syntax: {}".format( self.path ))
        
        metainfo = MetaInfo.create_from_h5_dataset(dataset)
        json_text = metainfo.format_to_json()

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
        
        metainfo = MetaInfo.create_from_h5_dataset(dataset)
        codec = VolumeCodec( metainfo )
        buffer_len = codec.calculate_buffer_len( data.shape )

        self.send_response(200)
        self.send_header("Content-type", VolumeCodec.VOLUME_MIMETYPE)
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
        
        metainfo = MetaInfo.create_from_h5_dataset(dataset)
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
            raise self.RequestError(400, "Bad query syntax: {}".format( self.path ))
        
        dims_str, roi_shape_str, roi_start_str = params[4:]

        dataset_ndims = len(dataset.shape)
        expected_dims_str = "_".join( map(str, range( dataset_ndims-1 )) )
        if dims_str != expected_dims_str:
            raise self.RequestError( 400, "For now, queries must include all dataset axes.  "
                                          "Your query requested dims: {}".format( dims_str ) )
        
        roi_start = tuple( int(x) for x in roi_start_str.split('_') )
        roi_shape = tuple( int(x) for x in roi_shape_str.split('_') )

        if len(roi_start) != dataset_ndims-1:
            raise self.RequestError( 400, "Invalid start coordinate: {} Expected {} dims, got {} "
                                          "".format( roi_start, dataset_ndims-1, len(roi_start) ) )
        if len(roi_shape) != dataset_ndims-1:
            raise self.RequestError( 400, "Invalid cutout shape: {} Expected {} dims, got {} "
                                          "".format( roi_shape, dataset_ndims-1, len(roi_shape) ) )
        
        roi_stop = tuple( numpy.array(roi_start) + roi_shape )
        return roi_start, roi_stop

    def log_request(self, *args, **kwargs):
        """
        Override from BaseHTTPRequestHandler, so we can respect the H5MockServer's disable_logging setting.
        """
        if not self.server.disable_logging:
            BaseHTTPRequestHandler.log_request(self, *args, **kwargs )

class H5MockServer(HTTPServer):
    def __init__(self, h5filepath, disable_logging, *args, **kwargs):
        """
        h5filepath: The hdf5 file to serve data from.
        See docstring above for requirements on the file contents.
        """
        HTTPServer.__init__(self, *args, **kwargs)
        self.h5filepath = h5filepath
        self.disable_logging = disable_logging
    
    def serve_forever(self):
        with h5py.File( self.h5filepath ) as h5_file:
            self.h5_file = h5_file
            HTTPServer.serve_forever(self)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        sys.stderr.write("Usage: python {} <filename.h5>\n".format( sys.argv[0] ))
        sys.exit(1)
    
    try:
        filename = sys.argv[1]
        server_address = ('', 8000)
        server = H5MockServer( filename, False, server_address, H5CutoutRequestHandler )
        server.serve_forever()
    finally:
        print "SERVER EXITED."
