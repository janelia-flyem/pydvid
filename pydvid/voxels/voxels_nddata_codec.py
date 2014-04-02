import numpy

from voxels_metadata import VoxelsMetadata

class VoxelsNddataCodec(object):

    # Data is sent to/retrieved from the http response stream in chunks.
    STREAM_CHUNK_SIZE = 1000 # (bytes)

    # Defined here for clients to use.
    VOLUME_MIMETYPE = "application/octet-stream"
    
    def __init__(self, voxels_metadata):
        """
        voxels_metadata: a VoxelsMetadata instance describing the remote volume.
        """
        assert isinstance(voxels_metadata, VoxelsMetadata)
        self._voxels_metadata = voxels_metadata
        
    def decode_to_ndarray(self, stream, full_roi_shape):
        """
        Decode the info in the given stream to a numpy.ndarray.
        
        Note: self._voxels_metadata.shape is IGNORED, because it refers to the entire DVID volume.
              Instead, the full_roi_shape parameter determines the size of the decoded dataset,
              including the channel dimension.
        """
        # Note that dvid uses fortran order indexing
        array = numpy.ndarray( full_roi_shape,
                               dtype=self._voxels_metadata.dtype,
                               order='F' )

        buf = numpy.getbuffer(array)
        self._read_to_buffer(buf, stream)

        return array

    def encode_from_ndarray(self, stream, array):
        """
        Encode the array to the given bytestream.
        
        Prerequisites:
        - array must be a numpy.ndarray
        - array must have the same dtype as this codec's metainfo
        """
        # Check for bad input.
        assert isinstance( array, numpy.ndarray ), \
            "Expected a numpy.ndarray, not {}".format( type(array) )
        assert array.dtype == self._voxels_metadata.dtype, \
            "Wrong dtype.  Expected {}, got {}".format( self._metainfo.dtype, array.dtype )

        # Unfortunately, if the array isn't F_CONTIGUOUS, we have to copy it.
        if not array.flags['F_CONTIGUOUS']:
            array_copy = numpy.empty_like(array, order='F')
            array_copy[:] = array[:]
            array = array_copy

        buf = numpy.getbuffer(array)
        self._send_from_buffer(buf, stream)

    def calculate_buffer_len(self, shape):
        return numpy.prod(shape) * self._voxels_metadata.dtype.type().nbytes

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
            next_chunk_bytes = min( remaining_bytes, VoxelsNddataCodec.STREAM_CHUNK_SIZE )
            chunk_start = len(buf)-remaining_bytes
            chunk_stop = len(buf)-(remaining_bytes-next_chunk_bytes)
            buf[chunk_start:chunk_stop] = stream.read( next_chunk_bytes )
            remaining_bytes -= next_chunk_bytes

    @classmethod
    def _send_from_buffer(cls, buf, stream):
        """
        Write the given buffer out to the provided stream in chunks.
        """
        remaining_bytes = len(buf)
        while remaining_bytes > 0:
            next_chunk_bytes = min( remaining_bytes, VoxelsNddataCodec.STREAM_CHUNK_SIZE )
            chunk_start = len(buf)-remaining_bytes
            chunk_stop = len(buf)-(remaining_bytes-next_chunk_bytes)
            stream.write( buf[chunk_start:chunk_stop] )
            remaining_bytes -= next_chunk_bytes
    