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

    >>> from dvidclient.volume_client import VolumeClient
    >>> vol_client = VolumeClient( "localhost:8000", "abc123", "grayscale_data" )
    >>> cutout_array = vol_client.retrieve_subvolume( (0,10,20,30), (1,110,120,130) ) # Must include channel
    >>> assert isinstance(cutout_array, vigra.VigraArray)
    >>> assert cutout_array.shape == (1,100,100,100)

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
