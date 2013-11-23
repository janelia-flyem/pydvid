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
"""
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler

import numpy
import h5py
import vigra

from dvidclient.volume_metainfo import get_dataset_metainfo, format_metainfo_to_json

class H5CutoutRequestHandler(BaseHTTPRequestHandler):
    """
    Supports the following DVID REST calls:
    
    Meta info:
        GET  /api/node/<UUID>/<data name>/info
    
    Cutout volume:
        GET  /api/node/<UUID>/<data name>/<dims>/<size>/<offset>[/<format>]
    """
    
    VOLUME_MIMETYPE = "binary/imagedata"
    STREAM_CHUNK_SIZE = 1000
    
    def do_GET(self):
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
        elif len(params) == 8:
            self._do_get_data(params, dataset)
        else:
            self.send_error(400, "Bad query syntax: {}".format( self.path ))
            return

    def _do_get_info(self, params, dataset):
        assert len(params) == 5
        cmd = params[4]
        if cmd != 'info':
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
        assert len(params) == 8
        if params[0] != 'api' or \
           params[1] != 'node':
            self.send_error(400, "Bad query syntax: {}".format( self.path ))
            return
        
        dims_str, roi_shape_str, roi_start_str, fmt = params[4:]

        dataset_ndims = len(dataset.shape)
        expected_dims_str = "_".join( map(str, range( dataset_ndims-1 )) )
        if dims_str != expected_dims_str:
            self.send_error(400, "For now, queries must include all dataset axes.  Your query requested dims: {}".format( dims_str ))
            return
        
        roi_start = tuple( int(x) for x in roi_start_str.split('_') )
        roi_shape = tuple( int(x) for x in roi_shape_str.split('_') )
        
        roi_stop = tuple( numpy.array(roi_start) + roi_shape )        
        slicing = tuple( slice(x,y) for x,y in zip(roi_start, roi_stop) )
        
        # Reverse here because API uses fortran order, but data is stored in C-order
        data = dataset[tuple(reversed(slicing))]
        axistags = vigra.AxisTags.fromJSON( dataset.attrs['axistags'] )
        v_array = vigra.taggedView( data, axistags )
        buf = numpy.getbuffer(v_array)

        self.send_response(200)
        self.send_header("Content-type", self.VOLUME_MIMETYPE)
        self.send_header("Content-length", str(len(buf)))
        self.end_headers()

        self._send_buffer( buf, self.wfile )

    def _send_buffer(self, buf, stream):
        remaining_bytes = len(buf)
        while remaining_bytes > 0:
            next_chunk_bytes = min( remaining_bytes, self.STREAM_CHUNK_SIZE )
            stream.write( buf[len(buf)-remaining_bytes:len(buf)-(remaining_bytes-next_chunk_bytes)] )
            remaining_bytes -= next_chunk_bytes
    

class H5MockServer(HTTPServer):
    def __init__(self, h5filepath, *args, **kwargs):
        HTTPServer.__init__(self, *args, **kwargs)
        self.h5filepath = h5filepath
    
    def serve_forever(self):
        with h5py.File( self.h5filepath, 'r' ) as h5_file: # FIXME: Read-only for now (we don't yet support PUT)
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

    print "SERVER EXITED"
