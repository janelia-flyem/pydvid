import os
import shutil
import tempfile
import httplib

import numpy
import h5py

from dvidclient import voxels, general
from mockserver.h5mockserver import H5MockServer, H5MockServerDataFile

class TestGeneralApiCalls(object):
    
    @classmethod
    def setupClass(cls):
        """
        Override.  Called by nosetests.
        - Create an hdf5 file to store the test data
        - Start the mock server, which serves the test data from the file.
        """
        cls._tmp_dir = tempfile.mkdtemp()
        cls.test_filepath = os.path.join( cls._tmp_dir, "test_data.h5" )
        cls._generate_testdata_h5(cls.test_filepath)
        cls.server_proc, cls.shutdown_event = cls._start_mockserver( cls.test_filepath, same_process=True )
        cls.client_connection = httplib.HTTPConnection( "localhost:8000" )

    @classmethod
    def teardownClass(cls):
        """
        Override.  Called by nosetests.
        """
        shutil.rmtree(cls._tmp_dir)
        cls.shutdown_event.set()
        cls.server_proc.join()

    @classmethod
    def _generate_testdata_h5(cls, test_filepath):
        """
        Generate a temporary hdf5 file for the mock server to use (and us to compare against)
        """
        # Generate some test data
        data = numpy.indices( (10, 100, 200, 3) )
        assert data.shape == (4, 10, 100, 200, 3)
        data = data.astype( numpy.uint32 )
        cls.original_data = data

        # Choose names
        cls.dvid_dataset = "datasetA"
        cls.data_uuid = "abcde"
        cls.data_name = "indices_data"
        cls.volume_location = "/datasets/{dvid_dataset}/volumes/{data_name}".format( **cls.__dict__ )
        cls.node_location = "/datasets/{dvid_dataset}/nodes/{data_uuid}".format( **cls.__dict__ )
        cls.volume_metadata = voxels.VolumeMetadata.create_default_metadata(data.shape, data.dtype, "cxyzt", 1.0, "")

        # Write to h5 file
        with H5MockServerDataFile( test_filepath ) as test_h5file:
            test_h5file.add_node( cls.dvid_dataset, cls.data_uuid )
            test_h5file.add_volume( cls.dvid_dataset, cls.data_name, data, cls.volume_metadata )

            test_h5file.add_node( "datasetB", "12345" )
            test_h5file.add_volume( "datasetB", cls.data_name, data, cls.volume_metadata )


    @classmethod
    def _start_mockserver(cls, h5filepath, same_process=False, disable_server_logging=True):
        """
        Start the mock DVID server in a separate process.
        
        h5filepath: The file to serve up.
        same_process: If True, start the server in this process as a 
                      separate thread (useful for debugging).
                      Otherwise, start the server in its own process (default).
        disable_server_logging: If true, disable the normal HttpServer logging of every request.
        """
        return H5MockServer.create_and_start( h5filepath, "localhost", 8000, same_process, disable_server_logging )
    
    def test_query_datasets_list(self):
        datasets_list_info = general.get_datasets_list( self.client_connection )
        assert datasets_list_info["DatasetsUUID"][0] == "abcde"
        assert datasets_list_info["DatasetsUUID"][1] == "12345"
        assert datasets_list_info["NewDatasetID"] == 2

    def test_query_datasets_info(self):
        info = general.get_datasets_info( self.client_connection )
        assert info["Datasets"][0]["Root"] == "abcde"
        assert info["Datasets"][0]["Nodes"]["abcde"]["Parents"] == []
        assert info["Datasets"][0]["Nodes"]["abcde"]["Children"] == []
        assert info["Datasets"][0]["DataMap"][self.data_name]["Name"] == self.data_name

    def test_query_server_info(self):
        # Just run the query and see if we get a json schema error...
        server_info = general.get_server_info( self.client_connection )
        assert "Cores" in server_info
        assert "DVID datastore" in server_info

    def test_query_server_types(self):
        # Just run the query and see if we get a json schema error...
        server_types = general.get_server_types( self.client_connection )
        assert "grayscale8" in server_types
        assert "keyvalue" in server_types
        # ... etc...


if __name__ == "__main__":
    import sys
    import nose
    sys.argv.append("--nocapture")    # Don't steal stdout.  Show it on the console as usual.
    sys.argv.append("--nologcapture") # Don't set the logging level to DEBUG.  Leave it alone.
    nose.run(defaultTest=__file__)
