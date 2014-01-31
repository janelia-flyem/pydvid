"""
This file implements an http server.
It serves up HDF5 datasets over http using the DVID REST API.

The server can also be started up in stand-alone mode:

    $ cd mockserver
    $ PYTHONPATH=.. python h5mockserver.py my_hdf5_file.h5

Internally, your hdf5 file must be a hierarchy, with symlinks for easy access to uuids:

/datasets
    /dataset_name1
        /nodes
            /abc123
                /volumeA -> ../../volumes/volumeA
                /volumeB -> ../../volumes/volumeB
            /def456
                /volumeA -> ../../volumes/volumeA
                /volumeB -> ../../volumes/volumeB
        /volumes
            /volumeA
            /volumeB
/all_nodes
    /abc123 -> /datasets/dataset_name1/nodes/abc123
    /def456 -> /datasets/dataset_name1/nodes/def456

Furthermore, each hdf5 volume must:
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
import re
import json
import httplib
import threading
import multiprocessing
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler

import numpy
import h5py
import vigra

from dvidclient.volume_metainfo import MetaInfo
from dvidclient.volume_codec import VolumeCodec

class H5CutoutRequestHandler(BaseHTTPRequestHandler):
    """
    The request handler for the H5MockServer.
    Implements a subset of the DVID REST API for nd-data over http.
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
            self.send_error( httplib.INTERNAL_SERVER_ERROR, 
                             "Server Error: See response body for traceback.  Crashing now..." )
            
            # Write exception traceback to the response body as an html comment.
            import traceback
            self.wfile.write("<!-- Server Exception Traceback:\n")
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

        # Supported REST command formats -> methods and handlers
        rest_cmds = { "^/api/datasets/info$" :                                  { "GET"  : self._do_get_datasets_info },
                      "^/api/node/{uuid}/{dataname}/schema$" :                  { "GET"  : self._do_get_volume_schema },
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
                    raise self.RequestError( httplib.METHOD_NOT_ALLOWED,
                                             "Unsupported method for query: {} {}"
                                             "".format( method, self.path ) )
                else:
                    # Execute the command, passing in the matched parameters
                    handler( **match.groupdict() )
                    return

        # We couldn't find a command for the user's query.
        raise self.RequestError( httplib.BAD_REQUEST, "Bad query syntax: {}".format( self.path ) )

    
    def _do_get_datasets_info(self):
        """
        Respond to the query for dataset info.
        Note: For the purposes of this mock server, only a 
              subset of the json fields are provided here.
              Furthmore the "DAG" is just the alphabetized uuids.
        
        API Notes:  - Parents and children should be lists, and if there is no parent/child at a node, it should be represented with [], not null
        """
        # Dataset info is determined by the layout/attributes of the server's hdf5 file.
        # See the docstring above for details.
        info = {}
        datasets = info["Datasets"] = []
        
        h5file = self.server.h5_file
        for dataset_index, (dataset_name, dataset_group) in enumerate(sorted(h5file['datasets'].items())):
            uuids = sorted( dataset_group["nodes"].keys() ) 
            datasets.append( {} )
            dset_info = datasets[-1]
            dset_info["Root"] = uuids[0]
            dset_info["Nodes"] = {}
            dset_info["DatasetID"] = dataset_index
            dset_info["Alias"] = dataset_name
            for node_index, uuid in enumerate(uuids):
                # Don't bother with most node info fields
                dset_info["Nodes"][uuid] = { "GlobalID" : uuid }
                
                # Assign a single parent/child for each node,
                # except first/last
                if node_index == 0:
                    dset_info["Nodes"][uuid]["Parents"] = [] # TODO: Fix DVID API
                else:
                    dset_info["Nodes"][uuid]["Parents"] = [ uuids[node_index-1] ]

                if node_index == len(uuids)-1:
                    dset_info["Nodes"][uuid]["Children"] = [] # TODO: Fix DVID API
                else:
                    dset_info["Nodes"][uuid]["Children"] = [ uuids[node_index+1] ]
            
            datamap = dset_info["DataMap"] = {}
            volumes_group = 'datasets/{dataset_name}/volumes'.format( **locals() )
            for data_name, h5volume in sorted(h5file[volumes_group].items()):
                datamap[data_name] = {}
                datamap[data_name]["Name"] = data_name
                # TODO: Other fields...
        
        json_text = json.dumps( info )
        self.send_response(httplib.OK)
        self.send_header("Content-type", "text/json")
        self.send_header("Content-length", str(len(json_text)))
        self.end_headers()
        self.wfile.write( json_text )
            
         
    def _do_create_volume(self, uuid, typename, dataname):
        """
        The http client wants to create a new volume.
        Create it.
        """
        if uuid not in self.server.h5_file["all_nodes"]:
            raise self.RequestError( httplib.NOT_FOUND, "No such node with uuid {}".format( uuid ) )
        
        # Find the dataset that owns this node.
        volume_path = None
        for dataset_name, dataset_group in self.server.h5_file['datasets'].items():
            for node_uuid, node_group in dataset_group['nodes'].items():
                if node_uuid == uuid:
                    volume_path = '/datasets/{dataset_name}/volumes/{dataname}'.format( **locals() )
                    break

        if volume_path is None:
            raise self.RequestError( httplib.NOT_FOUND,
                                     "Cannot create.  Can't find node volumes dir in server hdf5 file." )
        
        if volume_path in self.server.h5_file:
            raise self.RequestError( httplib.CONFLICT,
                                     "Cannot create.  Volume {} already exists.".format( volume_path ) )

        # Must read exact bytes.
        # Apparently rfile.read() just hangs.
        body_len = self.headers.get("Content-Length")
        metainfo_json = self.rfile.read( int(body_len) )
        try:
            metainfo = MetaInfo.create_from_json( metainfo_json )
        except ValueError as ex:
            raise self.RequestError( httplib.BAD_REQUEST, 'Can\'t create volume.  '
                                     'Error parsing volume description: {}\n'
                                     'Invalid description text was:\n{}'
                                     ''.format( ex.args[0], metainfo_json ) )
        expected_typename = metainfo.determine_dvid_typename()
        if typename != expected_typename:
            raise self.RequestError( httplib.BAD_REQUEST,
                                     "Cannot create volume.  "
                                     "REST typename was {}, but metainfo JSON implies typename {}"
                                     "".format( typename, expected_typename ) )

        # Create the new volume in the appropriate 'volumes' group,
        #  and then link to it in the node group.
        metainfo.create_empty_h5_dataset( self.server.h5_file, volume_path )
        linkname = '/datasets/{dataset_name}/nodes/{uuid}/{dataname}'.format( **locals() )
        self.server.h5_file[linkname] = h5py.SoftLink( volume_path )
        self.server.h5_file.flush()

        self.send_response(httplib.NO_CONTENT)
        self.send_header("Content-length", "0" )
        self.end_headers()


    def _do_get_volume_schema(self, uuid, dataname):
        """
        Respond to a query for dataset info.
        """
        dataset = self._get_h5_dataset(uuid, dataname)
        metainfo = MetaInfo.create_from_h5_dataset(dataset)
        json_text = metainfo.format_to_json()

        self.send_response(httplib.OK)
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

        self.send_response(httplib.OK)
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
        self.server.h5_file.flush()

        self.send_response(httplib.NO_CONTENT) # "No Content" (accepted)
        self.send_header("Content-length", 0 )
        self.end_headers()
    

    def _get_h5_dataset(self, uuid, dataname):
        """
        Return the server's hdf5 dataset for the given uuid and data volume name.
        """
        dataset_path = '/all_nodes/' + uuid + '/' + dataname
        try:
            return self.server.h5_file[dataset_path]
        except KeyError:
            raise self.RequestError( httplib.NOT_FOUND,
                                     "Couldn't find dataset: {} in file {}"
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
            raise self.RequestError( httplib.BAD_REQUEST,
                                     "For now, queries must include all data axes.  "
                                     "Your query requested dims: {}".format( dims_str ) )
        
        roi_start = tuple( int(x) for x in roi_start_str.split('_') )
        roi_shape = tuple( int(x) for x in roi_shape_str.split('_') )

        if len(roi_start) != dataset_ndims-1:
            raise self.RequestError( httplib.BAD_REQUEST,
                                     "Invalid start coordinate: {} Expected {} dims, got {} "
                                     "".format( roi_start, dataset_ndims-1, len(roi_start) ) )
        if len(roi_shape) != dataset_ndims-1:
            raise self.RequestError( httplib.BAD_REQUEST,
                                     "Invalid cutout shape: {} Expected {} dims, got {} "
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
        self.shutdown_completed_event = threading.Event()
    
    def serve_forever(self):
        try:
            with h5py.File( self.h5filepath ) as h5_file:
                self.h5_file = h5_file
                HTTPServer.serve_forever(self)
        finally:
            self.server_close()
            self.shutdown_completed_event.set()

    @classmethod
    def create_and_start(cls, h5filepath, hostname, port, same_process=False, disable_server_logging=True):
        """
        Start the mock DVID server in a separate process or thread.
        
        h5filepath: The file to serve up.
        same_process: If True, start the server in this process as a 
                      separate thread (useful for debugging).
                      Otherwise, start the server in its own process (default).
        disable_server_logging: If true, disable the normal HttpServer logging of every request.
        """
        def server_main(shutdown_event):
            """
            This function can be used as the target function for either a thread or process.

            :param shutdown_event: Either a threading.Event or multiprocessing.Event, 
                                   depending how this function was started.
            
            - Start the server in a thread
            - Wait for the shutdown_event
            - Shutdown the server
            """
            # Fire up the server in a separate thread
            server_address = (hostname, port)
            server = H5MockServer( h5filepath, disable_server_logging, server_address, H5CutoutRequestHandler )
            server_thread = threading.Thread( target=server.serve_forever )
            server_thread.start()
            
            # Wait for the client to set the shutdown event
            shutdown_event.wait()
            server.shutdown()
            
            # Wait until shutdown is complete before exiting this thread/process
            server.shutdown_completed_event.wait()

        try:    
            if same_process:
                shutdown_event = threading.Event()
                server_start_thread = threading.Thread( target=server_main, args=(shutdown_event,) )
                server_start_thread.start()
                return server_start_thread, shutdown_event
            else:
                shutdown_event = multiprocessing.Event()
                server_proc = multiprocessing.Process( target=server_main, args=(shutdown_event,) )
                server_proc.start()
                return server_proc, shutdown_event
        finally:
            # Give the server some time to start up before clients attempt to query it.
            import time
            time.sleep(0.2)

class H5MockServerDataFile(object):
    """
    Convenience class for generating an hdf5 file that 
    can be served up by the H5MockServer.
    See file docstring above for format details.
    In the generated file, all nodes in a dataset contain the same volumes.
    """
    def __init__(self, filepath):
        self._f = h5py.File( filepath )
        if 'datasets' not in self._f:
            self._f.create_group('datasets')
        if 'all_nodes' not in self._f:
            self._f.create_group('all_nodes')

    def add_volume(self, dataset_name, volume_name, volume):
        assert isinstance( volume, vigra.VigraArray )
        assert volume.axistags[-1].key == 'c'

        volumes_group, nodes_group = self._get_dataset_groups(dataset_name)

        # Save the volume
        volume_dset = volumes_group.create_dataset( volume_name, data=volume )
        volume_dset.attrs['axistags'] = volume.axistags.toJSON()
        
        # Add a link to this volume in every node
        for node in nodes_group.values():
            node[volume_name] = h5py.SoftLink( volumes_group.name + '/' + volume_name )
    
    def add_node(self, dataset_name, node_uuid):
        volumes_group, nodes_group = self._get_dataset_groups(dataset_name)

        # Create the node
        node = nodes_group.create_group( node_uuid )
        
        # Add the node to the global list, too
        self._f['/all_nodes'][node_uuid] = h5py.SoftLink( nodes_group.name + '/' + node_uuid )
        
        # In this node, add a link to each volume of its dataset.
        for volume_name in volumes_group.keys():
            node[volume_name] = h5py.SoftLink( volumes_group.name + '/' + volume_name )

    def _get_dataset_groups(self, dataset_name):
        # Make dataset if necessary
        dataset_path = '/datasets/' + dataset_name
        if dataset_path not in self._f:
            self._f.create_group( dataset_path )
        
        # Make volumes group if necessary
        dataset_volumes_path = dataset_path + '/volumes'
        if dataset_volumes_path not in self._f:
            self._f.create_group( dataset_volumes_path )
        volumes_group = self._f[dataset_volumes_path]

        # Make nodes group if necessary        
        dataset_nodes_path = dataset_path + '/nodes'
        if dataset_nodes_path not in self._f:
            self._f.create_group( dataset_nodes_path )
        nodes_group = self._f[dataset_nodes_path]

        return volumes_group, nodes_group

    def close(self):
        self._f.close()
        
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()

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
