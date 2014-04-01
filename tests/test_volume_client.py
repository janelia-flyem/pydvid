import os
import shutil
import tempfile
import httplib

import numpy
import h5py

from dvidclient import voxels, general
from mockserver.h5mockserver import H5MockServer, H5MockServerDataFile

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
    
    def test_query_datasets_info(self):
        info = general.get_datasets_info( self.client_connection )
        assert info["Datasets"][0]["Root"] == "abcde"
        assert info["Datasets"][0]["Nodes"]["abcde"]["Parents"] == []
        assert info["Datasets"][0]["Nodes"]["abcde"]["Children"] == []
        assert info["Datasets"][0]["DataMap"][self.data_name]["Name"] == self.data_name
        
    
    def test_create_volume(self):
        """
        Create a new remote volume.  Verify that the server created it in the hdf5 file.
        """
        volume_name = 'new_volume'
        metadata = voxels.VolumeMetadata.create_default_metadata((4,100,100,100), numpy.uint8, 'cxyz', 1.0, "")
        voxels.create_new( self.client_connection, self.data_uuid, volume_name, metadata )
         
        with h5py.File(self.test_filepath, 'r') as f:
            volumes_group = "/datasets/{dvid_dataset}/volumes".format( dvid_dataset=self.dvid_dataset )
            assert volume_name in f[volumes_group], "Volume wasn't created: {}".format( volumes_group + "/" + volume_name )
            assert voxels.VolumeMetadata.create_from_h5_dataset( f["all_nodes"][self.data_uuid][volume_name] ) == metadata,\
                "New volume has the wrong metadata"
 
 
    def test_get_ndarray(self):
        """
        Get some data from the server and check it.
        """
        # Retrieve from server
        start, stop = (0,9,5,50,0), (4,10,20,150,3)
        dvid_vol = voxels.VolumeClient( self.client_connection, self.data_uuid, self.data_name )
        subvolume = dvid_vol.get_ndarray( start, stop )
         
        # Compare to file
        self._check_subvolume(self.test_filepath, self.data_uuid, self.data_name, start, stop, subvolume)
     
    def _test_retrieve_volume(self, h5filename, uuid, data_name, start, stop):
        """
        h5filename: The h5 file to compare against
        h5group: The hdf5 group, also used as the uuid of the dvid dataset
        h5dataset: The dataset name, also used as the name of the dvid dataset
        start, stop: The bounds of the cutout volume to retrieve from the server. FORTRAN ORDER.
        """
 
    def test_post_ndarray(self):
        """
        Modify a remote subvolume and verify that the server wrote it.
        """
        # Cutout dims
        start, stop = (0,9,5,50,0), (4,10,20,150,3)
        shape = numpy.subtract( stop, start )
 
        # Generate test data
        subvolume = numpy.random.randint( 0,1000, shape ).astype( numpy.uint32 )
 
        # Send to server
        dvid_vol = voxels.VolumeClient( self.client_connection, self.data_uuid, self.data_name )
        dvid_vol.post_ndarray(start, stop, subvolume)
         
        # Check file
        self._check_subvolume(self.test_filepath, self.data_uuid, self.data_name, start, stop, subvolume)        
 
    def test_post_slicing(self):
        # Cutout dims
        start, stop = (0,9,5,50,0), (4,10,20,150,3)
        shape = numpy.subtract( stop, start )
 
        # Generate test data
        subvolume = numpy.random.randint( 0,1000, shape ).astype( numpy.uint32 )
 
        # Send to server
        dvid_vol = voxels.VolumeClient( self.client_connection, self.data_uuid, self.data_name )
        dvid_vol[0:4,9:10,5:20,50:150,0:3] = subvolume
         
        # Check file
        self._check_subvolume(self.test_filepath, self.data_uuid, self.data_name, start, stop, subvolume)        

    def test_post_reduced_dim_slicing(self):
        # Cutout dims
        start, stop = (0,9,5,50,0), (4,10,20,150,3)
        shape = numpy.subtract( stop, start )
 
        # Generate test data
        subvolume = numpy.random.randint( 0,1000, shape ).astype( numpy.uint32 )
 
        # Send to server
        dvid_vol = voxels.VolumeClient( self.client_connection, self.data_uuid, self.data_name )
        dvid_vol[0:4,9,5:20,50:150,0:3] = subvolume[:,0,...]
         
        # Check file
        self._check_subvolume(self.test_filepath, self.data_uuid, self.data_name, start, stop, subvolume)        

    def test_get_full_volume_via_slicing(self):
        # Retrieve from server
        dvid_vol = voxels.VolumeClient( self.client_connection, self.data_uuid, self.data_name )
        subvolume1 = dvid_vol[:]
        start = (0,) * len(self.original_data.shape)
        stop = self.original_data.shape
         
        # Compare to file
        self._check_subvolume(self.test_filepath, self.data_uuid, self.data_name, start, stop, subvolume1)        

        # Alternate sytax: Use Ellipsis
        subvolume2 = dvid_vol[...]
        # Results should match
        assert (subvolume1 == subvolume2).all()         
 
    def test_get_full_slicing(self):
        # Retrieve from server
        dvid_vol = voxels.VolumeClient( self.client_connection, self.data_uuid, self.data_name )
        subvolume = dvid_vol[0:4, 9:10, 5:20, 50:150, 0:3]
        start, stop = (0,9,5,50,0), (4,10,20,150,3)
         
        # Compare to file
        self._check_subvolume(self.test_filepath, self.data_uuid, self.data_name, start, stop, subvolume)
     
    def test_get_partial_slicing(self):
        # Retrieve from server
        dvid_vol = voxels.VolumeClient( self.client_connection, self.data_uuid, self.data_name )
        subvolume = dvid_vol[0:4, 9:10, 5:20, 50:150]
        start, stop = (0,9,5,50,0), (4,10,20,150,3)
        
        # Compare to file
        self._check_subvolume(self.test_filepath, self.data_uuid, self.data_name, start, stop, subvolume)

    def test_get_ellipsis_slicing(self):
        # Retrieve from server
        dvid_vol = voxels.VolumeClient( self.client_connection, self.data_uuid, self.data_name )
        subvolume = dvid_vol[0:4, 9:10, ..., 1:2]
        start, stop = (0,9,0,0,1), (4,10,100,200,2)
        
        # Compare to file
        self._check_subvolume(self.test_filepath, self.data_uuid, self.data_name, start, stop, subvolume)

    def test_get_reduced_dim_slicing(self):
        # Retrieve from server
        dvid_vol = voxels.VolumeClient( self.client_connection, self.data_uuid, self.data_name )
        subvolume = dvid_vol[0:4, 9:10, 3, :, 1:2] # Note that the third dimension will be dropped...
        start, stop = (0,9,3,0,1), (4,10,4,200,2) 
        
        # Check dimensionality/shape of returned volume
        full_shape = numpy.array(stop) - start
        reduced_shape = list(full_shape[:2]) + list(full_shape[3:])
        assert subvolume.shape == tuple(reduced_shape)

        # Before we compare to the file, re-insert the dropped axis
        subvolume = subvolume[:,:,numpy.newaxis,...]
        
        # Compare to file
        self._check_subvolume(self.test_filepath, self.data_uuid, self.data_name, start, stop, subvolume)

    def test_get_channel_slicing(self):
        """
        Test that slicing in the channel dimension works.
        This is a special case because the entire volume needs to be requested from DVID, 
        but only the requested subset of channels will be returned.
        """
        # Retrieve from server
        dvid_vol = voxels.VolumeClient( self.client_connection, self.data_uuid, self.data_name )
        subvolume = dvid_vol[1:3, 9:10, 5:20, 50:150]
        
        # Compare to file
        start, stop = (1,9,5,50,0), (3,10,20,150,3)
        self._check_subvolume(self.test_filepath, self.data_uuid, self.data_name, start, stop, subvolume)

    def test_get_stepped_slicing(self):
        """
        """
        # Retrieve from server
        dvid_vol = voxels.VolumeClient( self.client_connection, self.data_uuid, self.data_name )
        subvolume = dvid_vol[0:4:2, 1:10:3, 5:20:5, 50:150:10]
        
        # Compare to file
        full_start = (0,) * len( self.original_data.shape )
        full_stop = self.original_data.shape
        full_stored_volume = self._get_subvolume_from_file(self.test_filepath, self.data_uuid, self.data_name, full_start, full_stop)
        stored_stepped_volume = full_stored_volume[0:4:2, 1:10:3, 5:20:5, 50:150:10]

        assert subvolume.shape == stored_stepped_volume.shape
        assert subvolume.dtype == stored_stepped_volume.dtype
        assert (subvolume == stored_stepped_volume).all()

    def _check_subvolume(self, h5filename, uuid, data_name, start, stop, subvolume):
        """
        Compare a given subvolume to an hdf5 dataset.  Assert if they don't match.
        """
        # Retrieve from file
        stored_data = self._get_subvolume_from_file(h5filename, uuid, data_name, start, stop)
        # Compare.
        assert stored_data.shape == subvolume.shape
        assert stored_data.dtype == subvolume.dtype
        assert ( subvolume == stored_data ).all(),\
            "Data from server didn't match data from file!" 
 
    def _get_subvolume_from_file(self, h5filename, uuid, data_name, start, stop):
        slicing = tuple( slice(x,y) for x,y in zip(start, stop) )
        with h5py.File(h5filename, 'r') as f:
            return f["all_nodes"][uuid][data_name][slicing]
 
    def test_zz_readme_usage(self):
        import httplib
        import numpy
        from dvidclient import voxels
         
        # Open a connection
        connection = httplib.HTTPConnection( "localhost:8000" )
         
        # Create a new remote volume
        uuid = 'abcde'
        volume_metadata = voxels.VolumeMetadata.create_default_metadata( (4,200,200,200), numpy.uint8, 'cxyz', 1.0, "" )
        voxels.create_new( connection, uuid, "my_volume", volume_metadata )

        # Use the VolumeClient convenience class to manipulate a particular data volume     
        vol_client = voxels.VolumeClient( connection, uuid, "my_volume" )
         
        # Read from it
        cutout_array = vol_client.get_ndarray( (0,10,20,30), (4,110,120,130) ) # First axis is channel.
        assert isinstance(cutout_array, numpy.ndarray)
        assert cutout_array.shape == (4,100,100,100)
     
        # Modify it
        updated_data = numpy.ones( (4,100,100,100), dtype=numpy.uint8 ) # Must include all channels.
        cutout_array = vol_client.post_ndarray( (0,10,20,30), (4,110,120,130), updated_data )

if __name__ == "__main__":
    import sys
    import nose
    sys.argv.append("--nocapture")    # Don't steal stdout.  Show it on the console as usual.
    sys.argv.append("--nologcapture") # Don't set the logging level to DEBUG.  Leave it alone.
    nose.run(defaultTest=__file__)
