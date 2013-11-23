import json
import numpy
import vigra
import h5py
import dvidclient.volume_metainfo

class TestMetaInfo( object ):
    
    def test_parse(self):
            meta_string = """
            {
                "axes": [
                    {
                        "label": "X",
                        "resolution": 3.1,
                        "units": "nanometers",
                        "size": 100
                    },{
                        "label": "Y",
                        "resolution": 3.1,
                        "units": "nanometers",
                        "size": 200
                    },{
                        "label": "Z",
                        "resolution": 40,
                        "units": "nanometers",
                        "size": 400
                    }
                ],
                "values": [
                    {
                        "type": "uint8",
                        "label": "intensity-R"
                    },
                    {
                        "type": "uint8",
                        "label": "intensity-G"
                    },
                    {
                        "type": "uint8",
                        "label": "intensity-B"
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
        assert len( metadict["axes"] ) == 2
        assert metadict["axes"][0]["label"] == "X"
        assert metadict["axes"][0]["size"] == 10
        assert metadict["axes"][1]["label"] == "Y"
        assert metadict["axes"][1]["size"] == 11
        assert len(metadict["values"]) == 2 # 2 channels
        assert metadict["values"][0]["type"] == "int64"
        assert metadict["values"][1]["type"] == "int64"
    
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

if __name__ == "__main__":
    import sys
    import nose
    sys.argv.append("--nocapture")    # Don't steal stdout.  Show it on the console as usual.
    sys.argv.append("--nologcapture") # Don't set the logging level to DEBUG.  Leave it alone.
    nose.run(defaultTest=__file__)
