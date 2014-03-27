import httplib
from httplib import HTTPConnection
import threading
import contextlib
import StringIO
import json

import numpy

from volume_metainfo import VolumeInfo
from volume_codec import VolumeCodec

import logging
logger = logging.getLogger(__name__)

class VolumeClient(object):
    """
    Http client for retrieving a cutout volume from a DVID server.
    An instance of VolumeClient is capable of retrieving data from only one remote data volume.
    To retrieve data from multiple remote volumes, instantiate multiple VolumeClient objects.
    """
    class ErrorResponseException( Exception ):
        def __init__(self, attempted_action_name, status_code, reason, response_body, 
                     method, request_uri, request_body="<unspecified>", request_headers="<unspecified>"):
            self.attempted_action_name = attempted_action_name
            self.status_code = status_code
            self.reason = reason
            self.response_body = response_body
            self.method = method
            self.request_uri = request_uri
            self.request_body = request_body
            self.request_headers = request_headers
        
        def __str__(self):
            caption = 'While attempting "{}" DVID returned an error: {}, "{}"\n'\
                      ''.format( self.attempted_action_name, self.status_code, self.reason )
            caption += "Request METHOD: {}\n".format( self.method )
            caption += "Request URI: {}\n".format( self.request_uri )
            caption += "Request HEADERS: {}\n".format( self.request_headers )
            caption += "Request BODY: {}\n".format( self.request_body )
            caption += "Response body from server was:\n"
            caption += self.response_body
            caption += "\n"
            return caption

    @classmethod
    def create_volume(cls, hostname, uuid, data_name, volumeinfo):
        """
        Class method.
        Open a connection to the server and create a new remote volume.
        After creating the volume, you can instantiate a new VolumeClient to access it.
        """
        with contextlib.closing( HTTPConnection(hostname) ) as connection:
            dvid_typename = volumeinfo.determine_dvid_typename()
            rest_query = "/api/dataset/{uuid}/new/{dvid_typename}/{data_name}"\
                         "".format( **locals() )
            metadata_json = json.dumps(volumeinfo.metadata)
            headers = { "Content-Type" : "text/json" }
            connection.request( "POST", rest_query, body=metadata_json, headers=headers )
    
            with contextlib.closing( connection.getresponse() ) as response:
                if response.status != httplib.NO_CONTENT:
                    raise VolumeClient.ErrorResponseException( 
                        "create new data", response.status, response.reason, response.read(),
                         "POST", rest_query, metadata_json, headers)
                response_text = response.read()
                if response_text:
                    raise Exception( "Expected an empty response from the DVID server.  "
                                     "Got: {}".format( response_text ) )

    @classmethod
    def query_datasets_info(cls, hostname):
        """
        Query DVID for the list of datasets and the associated 
        nodes and data items within each node.
        """
        with contextlib.closing( HTTPConnection(hostname) ) as connection:
            rest_query = "/api/datasets/info"
            connection.request( "GET", rest_query )
            with contextlib.closing( connection.getresponse() ) as response:
                if response.status != httplib.OK:
                    raise VolumeClient.ErrorResponseException( 
                        "query datasets info", response.status, response.reason, response.read(),
                        "GET", rest_query, "")
                
                try:
                    datasets_info = json.loads( response.read() )
                except ValueError as ex:
                    raise Exception( "Couldn't parse the dataset info response as json:\n"
                                     "{}".format( ex.args ) )
                
                # TODO: Schema validation
                return datasets_info

    def __init__(self, hostname, uuid, data_name):
        """
        hostname: The DVID server hostname
        uuid: The node uuid
        data_name: The name of the volume
        """
        # Open a connection to the server
        self.hostname = hostname
        self.uuid = uuid
        self.data_name = data_name
        connection = HTTPConnection(hostname)
        self._connection = connection
        rest_query = "/api/node/{uuid}/{data_name}/metadata".format( uuid=uuid, data_name=data_name )
        connection.request( "GET", rest_query )
        
        response = connection.getresponse()
        if response.status != httplib.OK:
            raise self.ErrorResponseException( 
                "metadata query", response.status, response.reason, response.read(),
                "GET", rest_query, "" )

        self.volumeinfo = VolumeInfo( response.read() )
        self._codec = VolumeCodec( self.volumeinfo )
        
        self._lock = threading.Lock() # TODO: Instead of locking, auto-instantiate separate connections for each thread...
    
    def retrieve_subvolume(self, start, stop):
        """
        Retrieve a subvolume from the remote server.
        start, stop: The start and stop coordinates of the region to retrieve.
                     Must include all axes of the dataset.
        """
        rest_query = self._format_subvolume_rest_query(start, stop)
        # TODO: Instead of locking, auto-instantiate separate connections for each thread...
        with self._lock:
            self._connection.request( "GET", rest_query )
            with contextlib.closing( self._connection.getresponse() ) as response:
                if response.status != httplib.OK:
                    raise self.ErrorResponseException( 
                        "subvolume query", response.status, response.reason, response.read(),
                        "GET", rest_query, "" )
                
                # "Full" roi shape includes channel axis and ALL channels
                full_roi_shape = numpy.array(stop) - start
                full_roi_shape[0] = self.volumeinfo.shape[0]
                vdata = self._codec.decode_to_ndarray( response, full_roi_shape )
    
                # Was the response fully consumed?  Check.
                # NOTE: This last read() is not optional.
                # Something in the http implementation gets upset if we read out the exact amount we needed.
                # That is, we MUST read beyond the end of the stream.  So, here we go. 
                excess_data = response.read()
                if excess_data:
                    # Uh-oh, we expected it to be empty.
                    raise Exception( "Received data was longer than expected by {} bytes.  (Expected only {} bytes.)"
                                     "".format( len(excess_data), len(numpy.getbuffer(vdata)) ) ) 
        # Select the requested channels from the returned data.
        return vdata[start[0]:stop[0]]

    def modify_subvolume(self, start, stop, new_data):
        assert start[0] == 0, "Subvolume modifications must include all channels."
        assert stop[0] == self.volumeinfo.shape[0], "Subvolume modifications must include all channels."

        rest_query = self._format_subvolume_rest_query(start, stop)
        body_data_stream = StringIO.StringIO()
        self._codec.encode_from_ndarray(body_data_stream, new_data)
        with self._lock:
            headers = { "Content-Type" : VolumeCodec.VOLUME_MIMETYPE }
            self._connection.request( "POST", rest_query, body=body_data_stream.getvalue(), headers=headers )
            with contextlib.closing( self._connection.getresponse() ) as response:
                if response.status != httplib.NO_CONTENT:
                    raise self.ErrorResponseException( 
                        "subvolume post", response.status, response.reason, response.read(),
                         "POST", rest_query, "<binary data>", headers)
                
                # Something (either dvid or the httplib) gets upset if we don't read the full response.
                response.read()

    def _format_subvolume_rest_query(self, start, stop):
        start = numpy.asarray(start)
        stop = numpy.asarray(stop)
        shape = self.volumeinfo.shape

        assert len(start) == len(stop) == len(shape), \
            "start/stop/shape mismatch: {}/{}/{}".format( start, stop, shape )
        assert (start < stop).all(), "Invalid start/stop: {}/{}".format( start, stop )
        assert (start >= 0).all(), "Invalid start: {}".format( start )
        assert (start < shape).all(), "Invalid start/shape: {}/{}".format( start, shape )
        assert (stop <= shape).all(), "Invalid stop/shape: {}/{}".format( stop, shape )
        assert start[0] == 0, "Must request all channels with every query"
        assert stop[0] == shape[0], "Must request all channels with every query"

        # Drop channel before requesting from DVID
        start = start[1:]
        stop = stop[1:]

        # Dvid roi shape doesn't include channel
        dvid_roi_shape = stop - start
        roi_shape_str = "_".join( map(str, dvid_roi_shape) )
        start_str = "_".join( map(str, start) )
        
        num_dims = len(self.volumeinfo.shape)
        dims_string = "_".join( map(str, range(num_dims-1) ) )
        rest_query = "/api/node/{uuid}/{data_name}/raw/{dims_string}/{roi_shape_str}/{start_str}"\
                     "".format( uuid=self.uuid, 
                                data_name=self.data_name, 
                                dims_string=dims_string, 
                                roi_shape_str=roi_shape_str, 
                                start_str=start_str )
        return rest_query

