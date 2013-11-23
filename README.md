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

Run the tests
-------------
The unit tests require nosetests.

    $ cd tests
    $ PYTHONPATH=.. nosetests .

TODO Items
----------
- Resolution units are not used.
- Allow users to provide a pre-allocated array when requesting data
- Support multithreaded parallel requests
- Support PUT
- Fix format/mimetype strings
- Both client and server try to avoid using lots of memory by "chunking" data onto the http stream. Is that necessary?  Does it help?  Does it hurt?
- Current API specifies an image "size" for each dimension.  Original idea was for DVID coordinate system to be signed, with start/stop volume "extents" instead of a simple image shape.
