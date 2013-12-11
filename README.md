DVID Client Implementation
==========================
Simple client for retrieving volume cutout data from a DVID Server.

Mock Server
-----------

For test purposes, a mock DVID server is implemented in the mockserver directory.
It serves up HDF5 datasets over http using the DVID REST API.

The server can also be started up in stand-alone mode:

    $ cd mockserver
    $ PYTHONPATH=.. python h5mockserver.py my_hdf5_file.h5

Internally, your hdf5 file must be a two-level heirarchy, such that each dataset is accessed via: `/uuid/dataset_name`.
Furthermore, each dataset:
- Must include a channel axis
- Must have an "axistags" attribute as produced by `vigra.AxisTags.toJSON()`
- Must be in C-order, e.g. zyxc

See h5mockserver.py for info about its limitations.

Client Usage
------------

    import numpy, vigra
    from dvidclient.volume_client import VolumeClient
    from dvidclient.volume_metainfo import MetaInfo
    
    # Create a new remote volume
    uuid = 'abcde'
    metainfo = MetaInfo( (4,200,200,200), numpy.uint8, vigra.defaultAxistags('cxyz') )
    VolumeClient.create_volume( "localhost:8000", uuid, "my_volume", metainfo )

    # Open connection for a particular volume    
    vol_client = VolumeClient( "localhost:8000", uuid, "my_volume" )
    
    # Read from it (first axis is channel)
    cutout_array = vol_client.retrieve_subvolume( (0,10,20,30), (1,110,120,130) )
    assert isinstance(cutout_array, vigra.VigraArray)
    assert cutout_array.shape == (1,100,100,100)

    # Modify it
    new_data = numpy.ones( (4,100,100,100), dtype=numpy.uint8 ) # Must include all channels.
    tagged_data = vigra.taggedView( new_data, metainfo.axistags )
    cutout_array = vol_client.modify_subvolume( (0,10,20,30), (4,110,120,130), tagged_data )

DVID Contents Browser UI
------------------------

A simple widget for viewing the list of datasets and nodes in a DVID instance is provided.
Requires PyQt4.  To see a demo of it in action, start up your dvid server run this:

    $ python dvidclient/gui/contents_browser.py localhost:8000

Run the tests
-------------
The unit tests require nosetests.

    $ cd tests
    $ PYTHONPATH=.. nosetests .

TODO Items
----------
- Allow users to provide a pre-allocated array when requesting data
- Support multithreaded parallel requests
- Both client and server try to avoid using lots of memory by "chunking" data onto the http stream. Is that necessary?  Does it help?  Does it hurt?
- Resolution units are not used.
- Volume metainfo json is not validated against a schema
