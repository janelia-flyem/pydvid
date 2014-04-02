import os
import shutil
import tempfile
import httplib

import h5py

from dvidclient import keyvalue
from mockserver.h5mockserver import H5MockServer, H5MockServerDataFile

class TestKeyValue(object):
    
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
        # Choose names
        cls.dvid_dataset = "datasetA"
        cls.data_uuid = "abcde"
        cls.data_name = "my_keyvalue_stuff"
        cls.keyvalue_store_location = "/datasets/{dvid_dataset}/volumes/{data_name}".format( **cls.__dict__ )
        cls.node_location = "/datasets/{dvid_dataset}/nodes/{data_uuid}".format( **cls.__dict__ )

        # Write to h5 file
        with H5MockServerDataFile( test_filepath ) as test_h5file:
            test_h5file.add_node( cls.dvid_dataset, cls.data_uuid )
        #    test_h5file.add_keyvalue_group( cls.dvid_dataset, cls.data_name )

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

    def test_basic(self):
        # (1) NEW
        keyvalue.create_new( self.client_connection, self.data_uuid, self.data_name )

        # Manually check that the keyvalue store was created by checking the underlying hdf5 file...
        with h5py.File(self.test_filepath, 'r') as f:
            assert self.keyvalue_store_location in f

        # (2) PUT
        keyvalue.put_value( self.client_connection, self.data_uuid, self.data_name, 'key_abc', 'abcdefghijklmnopqrstuvwxyz' )

        # Manual check...
        with h5py.File(self.test_filepath, 'r') as f:
            assert self.keyvalue_store_location + '/key_abc' in f

        # (3) GET
        value = keyvalue.get_value( self.client_connection, self.data_uuid, self.data_name, 'key_abc' )
        assert value == 'abcdefghijklmnopqrstuvwxyz'

if __name__ == "__main__":
    import sys
    import nose
    sys.argv.append("--nocapture")    # Don't steal stdout.  Show it on the console as usual.
    sys.argv.append("--nologcapture") # Don't set the logging level to DEBUG.  Leave it alone.
    nose.run(defaultTest=__file__)
