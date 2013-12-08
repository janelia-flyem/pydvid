import StringIO

import numpy
import vigra

from dvidclient.volume_metainfo import MetaInfo
from dvidclient.volume_codec import VolumeCodec

class TestVolumeCodec(object):
    
    def test_basic(self):
        data = numpy.random.randint(0,255, (100,200,3)).astype(numpy.uint8)
        data = vigra.taggedView( data, vigra.defaultAxistags('yxc') )
        data = data.transpose()
        
        metainfo = MetaInfo( data.shape, data.dtype, data.axistags )
        codec = VolumeCodec( metainfo )
        
        stream = StringIO.StringIO()
        codec.encode_from_vigra_array(stream, data)
        stream.seek(0)
        roundtrip_data = codec.decode_to_vigra_array(stream, data.shape)
        assert roundtrip_data.flags['F_CONTIGUOUS']
        
        self._assert_matching(roundtrip_data, data)

    def test_all_dtypes(self):
        for dtype in [numpy.uint8, numpy.uint16, numpy.uint32, numpy.float32, numpy.float64]:
            data = numpy.random.randint(0,255, (100,200,3)).astype(dtype)
            data = vigra.taggedView( data, vigra.defaultAxistags('yxc') )
            data.transpose()
            
            metainfo = MetaInfo( data.shape, data.dtype, data.axistags )
            codec = VolumeCodec( metainfo )
            
            stream = StringIO.StringIO()
            codec.encode_from_vigra_array(stream, data)
            stream.seek(0)
            roundtrip_data = codec.decode_to_vigra_array(stream, data.shape)
            assert roundtrip_data.flags['F_CONTIGUOUS']
            
            self._assert_matching(roundtrip_data, data)

    def test_c_array_order(self):
        data = numpy.random.randint(0,255, (100,200,3)).astype(numpy.uint8)
        data = vigra.taggedView( data, vigra.defaultAxistags('yxc') )
        
        # Don't transpose for this test (will result in a copy internally)
        # data = data.transpose()
        
        metainfo = MetaInfo( data.shape, data.dtype, data.axistags )
        codec = VolumeCodec( metainfo )
        
        stream = StringIO.StringIO()
        codec.encode_from_vigra_array(stream, data)
        stream.seek(0)
        roundtrip_data = codec.decode_to_vigra_array(stream, data.shape)
        assert roundtrip_data.flags['F_CONTIGUOUS']
        
        self._assert_matching(roundtrip_data, data)

    def _assert_matching(self, data, expected):
        assert expected is not data
        assert expected.dtype == data.dtype
        assert expected.axistags == data.axistags
        assert expected.shape == data.shape
        assert (expected == data).all(), "data didn't match"


if __name__ == "__main__":
    import sys
    import nose
    sys.argv.append("--nocapture")    # Don't steal stdout.  Show it on the console as usual.
    sys.argv.append("--nologcapture") # Don't set the logging level to DEBUG.  Leave it alone.
    nose.run(defaultTest=__file__)

        