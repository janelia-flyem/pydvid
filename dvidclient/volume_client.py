from httplib import HTTPConnection
import threading
import contextlib

import numpy
import vigra

from volume_metainfo import parse_meta_info_from_json

import logging
logger = logging.getLogger(__name__)

class VolumeClient(object):
    """
    Http client for retrieving cutout volumes from a DVID server.
    An instance of VolumeClient is capable of retrieving data from only one remote dataset.
    To retrieve data from multiple datasets, instantiate multiple VolumeClient objects.
    """

    # Data is retrieved from the http response stream in chunks.
    STREAM_CHUNK_SIZE = 1000 # (bytes)

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
        
        self._lock = threading.Lock() # TODO: Instead of locking, auto-instantiate separate connections for each thread...
        
    def retrieve_subvolume(self, start, stop):
        """
        Retrieve a subvolume from the remote server.
        start, stop: The start and stop coordinates of the region to retrieve.
                     Must include all axes of the dataset.
        """
        start = numpy.asarray(start)
        stop = numpy.asarray(stop)
        shape = self.metainfo.shape

        assert len(start) == len(stop) == len(shape), \
            "start/stop/shape mismatch: {}/{}/{}".format( start, stop, shape )
        assert (start < stop).all(), "Invalid start/stop: {}/{}".format( start, stop )
        assert (start >= 0).all(), "Invalid start: {}".format( start )
        assert (start < shape).all(), "Invalid start/shape: {}/{}".format( start, shape )
        assert (stop <= shape).all(), "Invalid stop/shape: {}/{}".format( stop, shape )

        # "Full" roi shape includes channel axis
        full_roi_shape = stop - start

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
        # TODO: Instead of locking, auto-instantiate separate connections for each thread...
        with self._lock:
            self._connection.request( "GET", rest_query )
            with contextlib.closing( self._connection.getresponse() ) as response:
                if response.status != 200:
                    raise Exception( "Error in response to subvolume query: {}, {}".format( response.status, response.reason ) )
                vdata = self.decode_to_vigra_array( response, self.metainfo, full_roi_shape )
    
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

    def decode_to_vigra_array(self, stream, metainfo, full_roi_shape):
        """
        Decode the info in the given stream to a vigra.VigraArray.
        
        Note: metainfo.shape is IGNORED, because it refers to the entire DVID volume.
              Instead, the roi_shape parameter determines the size of the decoded dataset.
        """
        # Vigra is finicky about the integer types we give it in the shape field
        full_roi_shape = tuple( map(int, full_roi_shape) )
        
        # Note that dvid uses fortran order indexing
        a = vigra.VigraArray( full_roi_shape,
                              dtype=metainfo.dtype,
                              axistags=metainfo.axistags,
                              order='F' )
        buf = numpy.getbuffer(a)
        
        # We could read it in one step, but instead we'll read it in chunks to avoid big temporaries.
        # (See below.)
        # buf[:] = stream.read( len(buf) )

        # Read data from the stream in chunks
        remaining_bytes = len(buf)
        while remaining_bytes > 0:
            next_chunk_bytes = min( remaining_bytes, self.STREAM_CHUNK_SIZE )
            buf[len(buf)-remaining_bytes:len(buf)-(remaining_bytes-next_chunk_bytes)] = stream.read( next_chunk_bytes )
            remaining_bytes -= next_chunk_bytes
        return a
    