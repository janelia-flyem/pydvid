Quickstart
==========

::

    import json
    import httplib
    import numpy
    from pydvid import voxels, general
     
    # Open a connection to DVID
    connection = httplib.HTTPConnection( "localhost:8000" )
    
    # List the dataset roots: /api/datasets/list
    root_nodes_info = general.get_datasets_list( connection )
    # print json.dumps( root_nodes_info, indent=4 )

    # Get detailed dataset info: /api/datasets/info
    dataset_details = general.get_datasets_info( connection )
    # print json.dumps( dataset_details, indent=4 )
    
    # Create a new remote volume
    uuid = 'abcde'
    voxels_metadata = voxels.VoxelsMetadata.create_default_metadata( (4,200,200,200), numpy.uint8, 'cxyz', 1.0, "" )
    voxels.create_new( connection, uuid, "my_volume", voxels_metadata )

    # Use the VoxelsAccessor convenience class to manipulate a particular data volume     
    dvid_volume = voxels.VoxelsAccessor( connection, uuid, "my_volume" )
     
    # Read from it
    cutout_array = dvid_volume.get_ndarray( (0,10,20,30), (4,110,120,130) ) # First axis is channel.
    assert isinstance(cutout_array, numpy.ndarray)
    assert cutout_array.shape == (4,100,100,100)
 
    # Modify it
    updated_data = numpy.ones( (4,100,100,100), dtype=numpy.uint8 ) # Must include all channels.
    cutout_array = dvid_volume.post_ndarray( (0,10,20,30), (4,110,120,130), updated_data )
