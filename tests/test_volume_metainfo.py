import nose
import numpy
import h5py
from dvidclient.voxels import VoxelsMetadata

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
        metadata = VoxelsMetadata(self.metadata_json)
        assert metadata.shape == (3, 100,200,400), "Wrong shape: {}".format( metadata.shape )
        assert metadata.dtype == numpy.uint8
        assert metadata.axiskeys == 'cxyz'
        assert metadata['Axes'][0]["Resolution"] == 3.1
        assert metadata['Axes'][1]["Resolution"] == 3.1
        assert metadata['Axes'][2]["Resolution"] == 40
    
    def test_create_default_metadata(self):
        metadata = VoxelsMetadata.create_default_metadata( (2,10,11), numpy.int64, "cxy", 1.5, "nanometers" )
        metadata["Values"][0]["Label"] = "R"
        metadata["Values"][1]["Label"] = "G"
        
        assert len( metadata["Axes"] ) == 2
        assert metadata["Axes"][0]["Label"] == "X"
        assert metadata["Axes"][0]["Size"] == 10
        assert metadata["Axes"][1]["Label"] == "Y"
        assert metadata["Axes"][1]["Size"] == 11
        assert len(metadata["Values"]) == 2 # 2 channels
        assert metadata["Values"][0]["DataType"] == "int64"
        assert metadata["Values"][1]["DataType"] == "int64"
        assert metadata["Values"][0]["Label"] == "R"
        assert metadata["Values"][1]["Label"] == "G"

    def test_create_axistags(self):
        try:
            import vigra
        except ImportError:
            raise nose.SkipTest

        metadata = VoxelsMetadata(self.metadata_json)
        tags = metadata.create_axistags()
        assert tags['x'].resolution == 3.1
        assert tags['y'].resolution == 3.1
        assert tags['z'].resolution == 40
        assert tags.channelLabels == ["intensity-R", "intensity-G", "intensity-B"]
    
    def test_metainfo_from_h5(self):
        shape = (3, 9, 10, 11)
        starting_metadata = VoxelsMetadata.create_default_metadata( shape, numpy.float32, "cxyz", 1.0, "nanometers" )
        f = h5py.File("dummy.h5", mode='w', driver='core', backing_store=False) # In-memory
        dset = f.create_dataset( 'dset', shape=shape, dtype=numpy.float32, chunks=True )
         
        metadata = VoxelsMetadata.create_from_h5_dataset( dset )
        assert metadata.dtype.type is numpy.float32
        assert metadata.shape == starting_metadata.shape, \
            "Wrong shape: {} vs. {}".format( metadata.shape, starting_metadata.shape )

if __name__ == "__main__":
    import sys
    import nose
    sys.argv.append("--nocapture")    # Don't steal stdout.  Show it on the console as usual.
    sys.argv.append("--nologcapture") # Don't set the logging level to DEBUG.  Leave it alone.
    nose.run(defaultTest=__file__)
