import json
import numpy
import vigra
import h5py
import dvidclient.volume_metainfo

class TestMetaInfo( object ):
    
    def test_parse(self):
            meta_string = """
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
            shape, dtype, tags = dvidclient.volume_metainfo.parse_meta_info_from_json(meta_string)
            assert shape == (3, 100,200,400), "Wrong shape: {}".format( shape )
            assert dtype == numpy.uint8
            assert [tag.key for tag in tags] == ['c', 'x', 'y', 'z']
            assert tags['x'].resolution == 3.1
            assert tags['y'].resolution == 3.1
            assert tags['z'].resolution == 40
        
    def test_format_to_json(self):
        metainfo = dvidclient.volume_metainfo.MetaInfo( (10,11,2), numpy.int64, vigra.defaultAxistags("xyc") )
        jsontext = dvidclient.volume_metainfo.format_metainfo_to_json( metainfo )
        metadict = json.loads( jsontext )
        assert len( metadict["Axes"] ) == 2
        assert metadict["Axes"][0]["Label"] == "X"
        assert metadict["Axes"][0]["Size"] == 10
        assert metadict["Axes"][1]["Label"] == "Y"
        assert metadict["Axes"][1]["Size"] == 11
        assert len(metadict["Values"]) == 2 # 2 channels
        assert metadict["Values"][0]["DataType"] == "int64"
        assert metadict["Values"][1]["DataType"] == "int64"
    
    def test_metainfo_from_h5(self):
        data = numpy.zeros( (11,10,9,3), dtype=numpy.float32 )
        data = vigra.taggedView(data, vigra.defaultAxistags("zyxc"))
        # In-memory hdf5 file...
        f = h5py.File("dummy.h5", mode='w', driver='core', backing_store=False)
        dset = f.create_dataset( 'dset', data=data )
        dset.attrs["axistags"] = data.axistags.toJSON()
        
        metainfo = dvidclient.volume_metainfo.get_dataset_metainfo( dset )
        assert metainfo.dtype == numpy.float32

        # Order is auto-converted from C-order to Fortran-order
        assert metainfo.shape == ( 3, 9, 10, 11 ), "Wrong shape: {}".format( metainfo.shape )
        assert [tag.key for tag in metainfo.axistags] == ['c', 'x', 'y', 'z']
        assert metainfo.axistags['c'].typeFlags == vigra.AxisType.Channels
        assert metainfo.axistags['x'].typeFlags == vigra.AxisType.Space
        assert metainfo.axistags['y'].typeFlags == vigra.AxisType.Space
        assert metainfo.axistags['z'].typeFlags == vigra.AxisType.Space

if __name__ == "__main__":
    import sys
    import nose
    sys.argv.append("--nocapture")    # Don't steal stdout.  Show it on the console as usual.
    sys.argv.append("--nologcapture") # Don't set the logging level to DEBUG.  Leave it alone.
    nose.run(defaultTest=__file__)
