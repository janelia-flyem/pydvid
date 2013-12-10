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
import re

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
        GET  /api/node/<UUID>/<data name>/schema
    
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
    

    # Forward all requests to the common entry point
    def do_GET(self):  self._handle_request("GET")
    def do_POST(self): self._handle_request("POST")


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
        # Parameter patterns
        param_patterns = { 'uuid'     : r"[0-9a-fA-F]+",
                           'shape'    : r"(\d+_)*\d+",
                           'offset'   : r"(\d+_)*\d+", # (Same as shape)
                           'dims'     : r"(\d_)*\d",
                           'dataname' : r"\w+",
                           'typename' : r"\w+" }
        
        # Surround each pattern with 'named group' regex syntax
        named_param_patterns = {}
        for name, pattern in param_patterns.items():
            named_param_patterns[name] = "(?P<" + name + ">" + pattern + ")" 

        # This is the table of REST commands we support, 
        #  with the corresponding handler function for each supported http method.
        rest_cmds = { "^/api/node/{uuid}/{dataname}/schema$" :                  { "GET"  : self._do_get_volume_schema },
                      "^/api/dataset/{uuid}/new/{typename}/{dataname}$" :       { "POST" : self._do_create_volume },
                      "^/api/node/{uuid}/{dataname}/{dims}/{shape}/{offset}$" : { "GET"  : self._do_get_data,
                                                                                  "POST" : self._do_modify_data }
                    }

        # Find the matching rest command and execute the handler.
        for rest_cmd_format, cmd_methods in rest_cmds.items():
            rst_cmd_pattern = rest_cmd_format.format( **named_param_patterns )
            match = re.match( rst_cmd_pattern, self.path )
            if match:
                try:
                    handler = cmd_methods[method]
                except KeyError:
                    raise self.RequestError( 405, "Unsupported method for query: {} {}"
                                                  "".format( method, self.path ) )
                else:
                    # Execute the command, passing in the matched parameters
                    handler( **match.groupdict() )
                    return

        # We couldn't find a command for the user's query.
        raise self.RequestError( 400, "Bad query syntax: {}".format( self.path ) )


    def _do_create_volume(self, uuid, typename, dataname):
        """
        The http client wants to create a new volume.
        Create it.
        """
        if uuid not in self.server.h5_file:
            raise self.RequestError( 404, "No such node with uuid {}".format( uuid ) )
        
        dataset_path = uuid + '/' + dataname
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
        expected_typename = metainfo.determine_dvid_typename()
        if typename != expected_typename:
            raise self.RequestError( 400, "Cannot create volume.  "
                                          "REST typename was {}, but metainfo JSON implies typename {}"
                                          "".format( typename, expected_typename ) )

        metainfo.create_empty_h5_dataset( self.server.h5_file, dataset_path )
        self.server.h5_file.flush()

        self.send_response(204) # "No Content" (accepted)
        self.send_header("Content-length", "0" )
        self.end_headers()


    def _do_get_volume_schema(self, uuid, dataname):
        """
        Respond to a query for dataset info.
        """
        dataset = self._get_h5_dataset(uuid, dataname)
        metainfo = MetaInfo.create_from_h5_dataset(dataset)
        json_text = metainfo.format_to_json()

        self.send_response(200)
        self.send_header("Content-type", "text/json")
        self.send_header("Content-length", str(len(json_text)))
        self.end_headers()
        self.wfile.write( json_text )


    def _do_get_data(self, uuid, dataname, dims, shape, offset):
        """
        Respond to a query for volume data.
        
        All parameters are strings from the REST string.
        """
        dataset = self._get_h5_dataset(uuid, dataname)
        roi_start, roi_stop = self._determine_request_roi( dataset, dims, shape, offset )
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
    

    def _do_modify_data(self, uuid, dataname, dims, shape, offset):
        """
        Respond to a POST request to modify a subvolume of data.

        All parameters are strings from the REST string.
        """
        dataset = self._get_h5_dataset(uuid, dataname)
        roi_start, roi_stop = self._determine_request_roi( dataset, dims, shape, offset )
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
    

    def _get_h5_dataset(self, uuid, dataname):
        """
        Return the server's hdf5 dataset for the given uuid and data volume name.
        """
        dataset_path = uuid + '/' + dataname
        try:
            return self.server.h5_file[dataset_path]
        except KeyError:
            raise self.RequestError( 404, "Couldn't find dataset: {} in file {}"
                                          "".format( dataset_path, self.server.h5_file.filename ) )


    def _determine_request_roi(self, h5_dataset, dims_str, roi_shape_str, roi_start_str):
        """
        Parse the given REST parameters to determine the request region of interest.
        
        Returns: Coordinates start, stop (in API order, without channel index)

        h5_dataset: The dataset the roi applies to (used for error checking only)
        dims_str, roi_shape_str, roi_start_str: strings from a REST request.
        """
        dataset_ndims = len(h5_dataset.shape)
        expected_dims_str = "_".join( map(str, range( dataset_ndims-1 )) )
        if dims_str != expected_dims_str:
            raise self.RequestError( 400, "For now, queries must include all data axes.  "
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
