import StringIO

import numpy
import vigra

from dvidclient.volume_metainfo import VolumeMetadata
from dvidclient.volume_codec import VolumeCodec

class TestVolumeCodec(object):
    
    def test_basic_roundtrip(self):
        data = numpy.random.randint(0,255, (3, 100, 200)).astype(numpy.uint8)
        
        metadata = VolumeMetadata.create_default_metadata(data.shape, data.dtype, 'cxy', 1.0, "nanometers")
        codec = VolumeCodec( metadata )
        
        stream = StringIO.StringIO()
        codec.encode_from_ndarray(stream, data)
        stream.seek(0)
        roundtrip_data = codec.decode_to_ndarray(stream, data.shape)
        assert roundtrip_data.flags['F_CONTIGUOUS']
        
        self._assert_matching(roundtrip_data, data)

    def test_all_dtypes(self):
        for dtype in [numpy.uint8, numpy.uint16, numpy.uint32, numpy.float32, numpy.float64]:
            data = numpy.random.randint(0,255, (3,100,200)).astype(dtype)
             
            metadata = VolumeMetadata.create_default_metadata(data.shape, data.dtype, 'cxy', 1.0, "nanometers")
            codec = VolumeCodec( metadata )
             
            stream = StringIO.StringIO()
            codec.encode_from_ndarray(stream, data)
            stream.seek(0)
            roundtrip_data = codec.decode_to_ndarray(stream, data.shape)
            assert roundtrip_data.flags['F_CONTIGUOUS']
             
            self._assert_matching(roundtrip_data, data)
 
    def _assert_matching(self, data, expected):
        assert expected is not data
        assert expected.dtype == data.dtype
        assert expected.shape == data.shape
        assert (expected == data).all(), "data didn't match"


if __name__ == "__main__":
    import sys
    import nose
    sys.argv.append("--nocapture")    # Don't steal stdout.  Show it on the console as usual.
    sys.argv.append("--nologcapture") # Don't set the logging level to DEBUG.  Leave it alone.
    nose.run(defaultTest=__file__)

        