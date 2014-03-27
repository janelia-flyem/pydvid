import nose
import json
import numpy
import vigra
import h5py
from dvidclient.volume_metainfo import VolumeInfo

try:
    import vigra
    _has_vigra = True
except ImportError:
    _has_vigra = False

class TestVolumeInfo( object ):
    metadata_json = """
    {
        "Axes": [
            {
                "Label": "X",
                "Resolution": 3.1,
                "Units": "nanometers",
                "Size": 100
            },{
                "Label": "Y",
                "Resolution": 3.1,
                "Units": "nanometers",
                "Size": 200
            },{
                "Label": "Z",
                "Resolution": 40,
                "Units": "nanometers",
                "Size": 400
            }
        ],
        "Values": [
            {
                "DataType": "uint8",
                "Label": "intensity-R"
            },
            {
                "DataType": "uint8",
                "Label": "intensity-G"
            },
            {
                "DataType": "uint8",
                "Label": "intensity-B"
            }
        ]
    }
    """

    def test_parse(self):
        shape, dtype, axiskeys, metadata = VolumeInfo(self.metadata_json)
        assert shape == (3, 100,200,400), "Wrong shape: {}".format( shape )
        assert dtype == numpy.uint8
        assert axiskeys == 'cxyz'
        assert metadata['Axes'][0]["Resolution"] == 3.1
        assert metadata['Axes'][1]["Resolution"] == 3.1
        assert metadata['Axes'][2]["Resolution"] == 40
    
    def test_alternate_constructor(self):
        metadata = VolumeInfo.create_default_metadata( (2,10,11), numpy.int64, "cxy", 1.5, "nanometers" )
        metadata["Values"][0]["Label"] = "R"
        metadata["Values"][1]["Label"] = "G"
        volumeinfo = VolumeInfo( metadata )
        
        assert len( volumeinfo.metadata["Axes"] ) == 2
        assert volumeinfo.metadata["Axes"][0]["Label"] == "X"
        assert volumeinfo.metadata["Axes"][0]["Size"] == 10
        assert volumeinfo.metadata["Axes"][1]["Label"] == "Y"
        assert volumeinfo.metadata["Axes"][1]["Size"] == 11
        assert len(volumeinfo.metadata["Values"]) == 2 # 2 channels
        assert volumeinfo.metadata["Values"][0]["DataType"] == "int64"
        assert volumeinfo.metadata["Values"][1]["DataType"] == "int64"
        assert volumeinfo.metadata["Values"][0]["Label"] == "R"
        assert volumeinfo.metadata["Values"][1]["Label"] == "G"

    def test_create_axistags(self):
        if not _has_vigra:
            raise nose.SkipTest
        
        volumeinfo = VolumeInfo(self.metadata_json)
        tags = volumeinfo.create_axistags()
        assert tags['x'].resolution == 3.1
        assert tags['y'].resolution == 3.1
        assert tags['z'].resolution == 40
        assert tags.channelLabels == ["intensity-R", "intensity-G", "intensity-B"]
    
    def test_metainfo_from_h5(self):
        shape = (3, 9, 10, 11)
        metadata = VolumeInfo.create_default_metadata( shape, numpy.float32, "cxy", 1.0, "nanometers" )
        starting_volumeinfo = VolumeInfo( shape, numpy.float32, "cxyz", metadata )
        f = h5py.File("dummy.h5", mode='w', driver='core', backing_store=False) # In-memory
        dset = f.create_dataset( 'dset', shape=shape, dtype=numpy.float32, chunks=True )
         
        volumeinfo = VolumeInfo.create_volumeinfo_from_h5_dataset( dset )
        assert volumeinfo.dtype.type is numpy.float32
 
        # Order is auto-converted from C-order to Fortran-order
        assert volumeinfo.shape == ( 3, 9, 10, 11 ), "Wrong shape: {}".format( volumeinfo.shape )

if __name__ == "__main__":
    import sys
    import nose
    sys.argv.append("--nocapture")    # Don't steal stdout.  Show it on the console as usual.
    sys.argv.append("--nologcapture") # Don't set the logging level to DEBUG.  Leave it alone.
    nose.run(defaultTest=__file__)
