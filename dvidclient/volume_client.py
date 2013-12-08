from httplib import HTTPConnection
import threading
import contextlib
import StringIO

import numpy

from volume_metainfo import parse_meta_info_from_json
from volume_codec import VolumeCodec

import logging
logger = logging.getLogger(__name__)

class VolumeClient(object):
    """
    Http client for retrieving cutout volumes from a DVID server.
    An instance of VolumeClient is capable of retrieving data from only one remote dataset.
    To retrieve data from multiple datasets, instantiate multiple VolumeClient objects.
    """

    def __init__(self, hostname, uuid, dataset_name):
        """
        hostname: The DVID server hostname
        uuid: The node uuid
        dataset_name: The name of the dataset
        """
        # Open a connection to the server
        self.hostname = hostname
        self.uuid = uuid
        self.dataset_name = dataset_name
        connection = HTTPConnection(hostname)
        self._connection = connection
        rest_query = "/api/node/{uuid}/{dataset_name}/schema".format( uuid=uuid, dataset_name=dataset_name )
        connection.request( "GET", rest_query )
        
        response = connection.getresponse()
        if response.status != 200:
            raise Exception( "Error in response to metainfo query: {}, {}".format( response.status, response.reason ) )

        self.metainfo = parse_meta_info_from_json( response.read() )
        self._codec = VolumeCodec( self.metainfo )
        
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
                if response.status != 200:
                    raise Exception( "Error in response to subvolume query: {}, {}".format( response.status, response.reason ) )
                
                # "Full" roi shape includes channel axis
                full_roi_shape = numpy.array(stop) - start
                vdata = self._codec.decode_to_vigra_array( response, full_roi_shape )
    
                # Was the response fully consumed?  Check.
                # NOTE: This last read() is not optional.
                # Something in the http implementation gets upset if we read out the exact amount we needed.
                # That is, we MUST read beyond the end of the stream.  So, here we go. 
                excess_data = response.read()
                if excess_data:
                    # Uh-oh, we expected it to be empty.
                    raise Exception( "Received data was longer than expected by {} bytes.  (Expected only {} bytes.)"
                                     "".format( len(excess_data), len(numpy.getbuffer(vdata)) ) ) 
        return vdata

    def modify_subvolume(self, start, stop, new_data):
        rest_query = self._format_subvolume_rest_query(start, stop)
        body_data_stream = StringIO.StringIO()
        self._codec.encode_from_vigra_array(body_data_stream, new_data)
        with self._lock:
            self._connection.request( "POST", rest_query, body=body_data_stream.getvalue() )
            with contextlib.closing( self._connection.getresponse() ) as response:
                if response.status != 200:
                    raise Exception( "Error in response to subvolume query: {}, {}".format( response.status, response.reason ) )
                
                # Something (either dvid or the httplib) gets upset if we don't read the full response.
                response.read()

    def _format_subvolume_rest_query(self, start, stop):
        start = numpy.asarray(start)
        stop = numpy.asarray(stop)
        shape = self.metainfo.shape

        assert len(start) == len(stop) == len(shape), \
            "start/stop/shape mismatch: {}/{}/{}".format( start, stop, shape )
        assert (start < stop).all(), "Invalid start/stop: {}/{}".format( start, stop )
        assert (start >= 0).all(), "Invalid start: {}".format( start )
        assert (start < shape).all(), "Invalid start/shape: {}/{}".format( start, shape )
        assert (stop <= shape).all(), "Invalid stop/shape: {}/{}".format( stop, shape )

        # Drop channel before requesting from DVID
        channel_index = self.metainfo.axistags.channelIndex
        start = numpy.delete( start, channel_index )
        stop = numpy.delete( stop, channel_index )

        # Dvid roi shape doesn't include channel
        dvid_roi_shape = stop - start
        roi_shape_str = "_".join( map(str, dvid_roi_shape) )
        start_str = "_".join( map(str, start) )
        
        num_dims = len(self.metainfo.shape)
        dims_string = "_".join( map(str, range(num_dims-1) ) )
        rest_query = "/api/node/{uuid}/{dataset_name}/{dims_string}/{roi_shape_str}/{start_str}"\
                     "".format( uuid=self.uuid, 
                                dataset_name=self.dataset_name, 
                                dims_string=dims_string, 
                                roi_shape_str=roi_shape_str, 
                                start_str=start_str )
        return rest_query

