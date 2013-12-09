import os
import sys
import shutil
import tempfile
import functools
import threading
import multiprocessing

import numpy
import vigra
import h5py

from dvidclient.volume_client import VolumeClient
from dvidclient.volume_metainfo import MetaInfo
from mockserver.h5mockserver import H5MockServer, H5CutoutRequestHandler

def print_response_exception(func):
    """
    Decorator.
    If a test raises an ErrorResponseException, print the details to stderr.
    """
    @functools.wraps(func)
    def f( *args ):
        try:
            func(*args)
        except VolumeClient.ErrorResponseException as ex:
            sys.stderr.write( 'DVID server returned an error in response to {}: {}, "{}"\n'
                              ''.format( ex.attempted_action, ex.status_code, ex.reason ) )
            #if ex.status_code == 500: # Server internal error
            #    sys.stderr.write( 'Response body was:\n' )
            #    sys.stderr.write( ex.response_body )
            #    sys.stderr.write('\n')
            sys.stderr.flush()
            raise
    f.__wrapped__ = func # Emulate python 3 behavior of @wraps
    return f

class TestVolumeClient(object):
    
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
        cls.server_proc = cls._start_mockserver( cls.test_filepath, same_process=False )

    @classmethod
    def teardownClass(cls):
        """
        Override.  Called by nosetests.
        """
        shutil.rmtree(cls._tmp_dir)
        if isinstance( cls.server_proc, multiprocessing.Process ):
            cls.server_proc.terminate()

    @classmethod
    def _generate_testdata_h5(cls, test_filepath):
        """
        Generate a temporary hdf5 file for the mock server to use (and us to compare against)
        """
        # Generate some test data
        data = numpy.indices( (10, 100, 200, 3) )
        assert data.shape == (4, 10, 100, 200, 3)
        data = data.astype( numpy.uint32 )

        # Choose names
        cls.data_uuid = "abcde"
        cls.data_name = "indices_data"
        dataset_name = cls.data_uuid + '/' + cls.data_name

        # Write to h5 file
        with h5py.File( test_filepath, "w" ) as test_h5file:        
            dset = test_h5file.create_dataset(dataset_name, data=data)
            dset.attrs["axistags"] = vigra.defaultAxistags("tzyxc").toJSON()

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
        def server_main():
            server_address = ('', 8000)
            server = H5MockServer( h5filepath, disable_server_logging, server_address, H5CutoutRequestHandler )
            server.serve_forever()
    
        if same_process:
            server_proc = threading.Thread( target=server_main )
            server_proc.daemon = True
        else:
            server_proc = multiprocessing.Process( target=server_main )
        server_proc.start()
        return server_proc
    
    @print_response_exception
    def test_create_volume(self):
        """
        Create a new remote volume.  Verify that the server created it in the hdf5 file.
        """
        volume_name = 'new_volume'
        metainfo = MetaInfo( (4,100,100,100), numpy.uint8, vigra.defaultAxistags('cxyz') )
        VolumeClient.create_volume( "localhost:8000", self.data_uuid, volume_name, metainfo )
        
        with h5py.File(self.test_filepath, 'r') as f:
            assert volume_name in f[self.data_uuid], "Volume wasn't created"
            assert MetaInfo.create_from_h5_dataset( f[self.data_uuid][volume_name] ) == metainfo,\
                "New volume has the wrong metainfo"


    def test_cutout(self):
        """
        Get some data from the server and check it.
        """
        self._test_retrieve_volume( "localhost:8000", self.test_filepath, self.data_uuid, 
                                    self.data_name, (0,50,5,9,0), (3,150,20,10,4) )

    @print_response_exception    
    def _test_retrieve_volume(self, hostname, h5filename, h5group, h5dataset, start, stop):
        """
        hostname: The dvid server host
        h5filename: The h5 file to compare against
        h5group: The hdf5 group, also used as the uuid of the dvid dataset
        h5dataset: The dataset name, also used as the name of the dvid dataset
        start, stop: The bounds of the cutout volume to retrieve from the server. FORTRAN ORDER.
        """
        # Retrieve from server
        dvid_vol = VolumeClient( hostname, uuid=h5group, data_name=h5dataset )
        subvolume = dvid_vol.retrieve_subvolume( start, stop )
        
        # Compare to file
        self._check_subvolume(h5filename, h5group, h5dataset, start, stop, subvolume)

    def test_push(self):
        """
        Modify a remote subvolume and verify that the server wrote it.
        """
        # Cutout dims
        start, stop = (0,50,5,9,0), (3,150,20,10,4)
        shape = numpy.subtract( stop, start )

        # Generate test data
        subvolume = numpy.random.randint( 0,1000, shape ).astype( numpy.uint32 )
        subvolume = vigra.taggedView( subvolume, vigra.defaultAxistags('cxyzt') )

        # Run test.
        self._test_send_subvolume( "localhost:8000", self.test_filepath, self.data_uuid, 
                                   self.data_name, start, stop, subvolume )

    @print_response_exception    
    def _test_send_subvolume(self, hostname, h5filename, h5group, h5dataset, start, stop, subvolume):
        """
        hostname: The dvid server host
        h5filename: The h5 file to compare against
        h5group: The hdf5 group, also used as the uuid of the dvid dataset
        h5dataset: The dataset name, also used as the name of the dvid dataset
        start, stop: The bounds of the cutout volume to retrieve from the server. FORTRAN ORDER.
        subvolume: The data to send.  Must be of the correct shape for start,stop coordinates.
        """
        # Send to server
        dvid_vol = VolumeClient( hostname, uuid=h5group, data_name=h5dataset )
        dvid_vol.modify_subvolume(start, stop, subvolume)
        
        # Check file
        self._check_subvolume(h5filename, h5group, h5dataset, start, stop, subvolume)        

    def _check_subvolume(self, h5filename, h5group, h5dataset, start, stop, subvolume):
        """
        Compare a given subvolume to an hdf5 dataset.  Assert if they don't match.
        """
        # Retrieve from file
        slicing = [ slice(x,y) for x,y in zip(start, stop) ]
        slicing = tuple(reversed(slicing))
        with h5py.File(h5filename, 'r') as f:
            expected_data = f[h5group][h5dataset][slicing]

        # Compare.
        assert ( subvolume.view(numpy.ndarray) == expected_data.transpose() ).all(),\
            "Data from server didn't match data from file!"

    def test_zz_readme_usage(self):
        import numpy, vigra
        from dvidclient.volume_client import VolumeClient
        from dvidclient.volume_metainfo import MetaInfo
        
        # Create a new remote volume
        uuid = 'abcde'
        metainfo = MetaInfo( (4,200,200,200), numpy.uint8, vigra.defaultAxistags('cxyz') )
        VolumeClient.create_volume( "localhost:8000", uuid, "my_volume", metainfo )
    
        # Open connection for a particular volume    
        vol_client = VolumeClient( "localhost:8000", uuid, "my_volume" )
        
        # Read from it
        cutout_array = vol_client.retrieve_subvolume( (0,10,20,30), (1,110,120,130) ) # First axis is channel.
        assert isinstance(cutout_array, vigra.VigraArray)
        assert cutout_array.shape == (1,100,100,100)
    
        # Modify it
        new_data = numpy.ones( (4,100,100,100), dtype=numpy.uint8 ) # Must include all channels.
        tagged_data = vigra.taggedView( new_data, vigra.defaultAxistags('cxyz') )
        cutout_array = vol_client.modify_subvolume( (0,10,20,30), (4,110,120,130), tagged_data )

if __name__ == "__main__":
    import sys
    import nose
    sys.argv.append("--nocapture")    # Don't steal stdout.  Show it on the console as usual.
    sys.argv.append("--nologcapture") # Don't set the logging level to DEBUG.  Leave it alone.
    nose.run(defaultTest=__file__)
