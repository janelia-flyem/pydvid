==========
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
    print json.dumps( root_nodes_info, indent=4 )

    # Get detailed dataset info: /api/datasets/info
    dataset_details = general.get_datasets_info( connection )
    print json.dumps( dataset_details, indent=4 )
    
    # Create a new remote volume
    uuid = 'abcde'
    voxels_metadata = voxels.VoxelsMetadata.create_default_metadata( (4,0,0,0), numpy.uint8, 'cxyz', 1.0, "" )
    voxels.create_new( connection, uuid, "my_volume", voxels_metadata )

    # Use the VoxelsAccessor convenience class to manipulate a particular data volume     
    dvid_volume = voxels.VoxelsAccessor( connection, uuid, "my_volume" )
     
    # Add some data
    updated_data = numpy.ones( (4,100,100,100), dtype=numpy.uint8 ) # Must include all channels.
    dvid_volume[:, 10:110, 20:120, 30:130] = updated_data
    # OR:
    dvid_volume.post_ndarray( (0,10,20,30), (4,110,120,130), updated_data )
    
    # Read from it (First axis is channel.)
    cutout_array = dvid_volume[:, 10:110, 20:120, 30:130]
    # OR:
    cutout_array = dvid_volume.get_ndarray( (0,10,20,30), (4,110,120,130) )

    assert isinstance(cutout_array, numpy.ndarray)
    assert cutout_array.shape == (4,100,100,100)

A note about data axes
----------------------

pydvid gives you ND-data as a ``numpy.ndarray``. 
We use the same axis order convention that DVID uses (Fortran order).
In the DVID API, channel (i.e. 'Values' in DVID terminology) is not considered a separate array axis.
However, in pydvid, a separate axis is **always** used to represent the channel, **even for arrays with only a single channel**.
The channel axis is always in the first slicing position.

For example: DVID considers a 3D ``grayscale8`` volume of size ``(80,90,100)`` to have 3 axes (say, ``"X"``, ``"Y"``, ``"Z"``), 
but pydvid will give you a 4D array of shape ``(1,80,90,100)``, indexed by ``my_array[c,x,y,z]``.  
Again, note that the first axis is always ``'c'`` (channel) for all nd-arrays returned by pydvid. 

