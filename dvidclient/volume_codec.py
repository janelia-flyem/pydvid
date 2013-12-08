import numpy
import vigra

from volume_metainfo import MetaInfo

class VolumeCodec(object):

    # Data is sent to/retrieved from the http response stream in chunks.
    STREAM_CHUNK_SIZE = 1000 # (bytes)
    
    def __init__(self, metainfo):
        """
        metainfo: a volume_metainfo.MetaInfo instance describing the remote volume.
        """
        assert isinstance(metainfo, MetaInfo)
        self._metainfo = metainfo
        
    def decode_to_vigra_array(self, stream, full_roi_shape):
        """
        Decode the info in the given stream to a vigra.VigraArray.
        
        Note: self._metainfo.shape is IGNORED, because it refers to the entire DVID volume.
              Instead, the full_roi_shape parameter determines the size of the decoded dataset,
              including the channel dimension.
        """
        # Vigra is finicky about the integer types we give it in the shape field
        full_roi_shape = tuple( map(int, full_roi_shape) )
        
        # Note that dvid uses fortran order indexing
        array = vigra.VigraArray( full_roi_shape,
                              dtype=self._metainfo.dtype,
                              axistags=self._metainfo.axistags,
                              order='F' )

        buf = numpy.getbuffer(array)
        self._read_to_buffer(buf, stream)

        return array

    def encode_from_vigra_array(self, stream, array):
        """
        Encode the array to the given bytestream.
        
        Prerequisites:
        - array must be a vigra.VigraArray
        - array must have the same dtype and axistags as this codec's metainfo
        """
        # Check for bad input.
        assert isinstance( array, vigra.VigraArray ), "Expected a vigra.VigraArray, not {}".format( type(array) )
        assert array.dtype == self._metainfo.dtype, "Wrong dtype.  Expected {}, got {}".format( self._metainfo.dtype, array.dtype )
        array_axes = [tag.key for tag in array.axistags]
        expected_axes = [tag.key for tag in self._metainfo.axistags]
        assert array_axes == expected_axes, "Axistags don't match: Expected {}, got {}"\
                                            "".format( "".join( expected_axes ), "".join( array_axes ) )
                
        # Unfortunately, if the array isn't F_CONTIGUOUS, we have to copy it.
        if not array.flags['F_CONTIGUOUS']:
            array_copy = numpy.empty_like(array, order='F')
            array_copy[:] = array[:]
            array = array_copy

        buf = numpy.getbuffer(array)
        self._send_from_buffer(buf, stream)

    def calculate_buffer_len(self, shape):
        return numpy.prod(shape) * self._metainfo.dtype().nbytes

    @classmethod
    def _read_to_buffer(cls, buf, stream):
        """
        Read the data from the stream into the given buffer.
        """
        # We could read it in one step, but instead we'll read it in chunks to avoid big temporaries.
        # (See below.)
        # buf[:] = stream.read( len(buf) )

        # Read data from the stream in chunks
        remaining_bytes = len(buf)
        while remaining_bytes > 0:
            next_chunk_bytes = min( remaining_bytes, VolumeCodec.STREAM_CHUNK_SIZE )
            buf[len(buf)-remaining_bytes:len(buf)-(remaining_bytes-next_chunk_bytes)] = stream.read( next_chunk_bytes )
            remaining_bytes -= next_chunk_bytes

    @classmethod
    def _send_from_buffer(cls, buf, stream):
        """
        Write the given buffer out to the provided stream in chunks.
        """
        remaining_bytes = len(buf)
        while remaining_bytes > 0:
            next_chunk_bytes = min( remaining_bytes, VolumeCodec.STREAM_CHUNK_SIZE )
            stream.write( buf[len(buf)-remaining_bytes:len(buf)-(remaining_bytes-next_chunk_bytes)] )
            remaining_bytes -= next_chunk_bytes
    