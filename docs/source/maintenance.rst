Testing
=======

The test suite requires `nosetests <https://nose.readthedocs.org/en/latest/>`_.
Here's how to run the test suite:

.. code-block:: bash

    cd tests
    PYTHONPATH=.. nosetests .

Note that pydvid uses its own "mock server" for testing purposes, which mimics the real responses provided by the DVID server.
This means that you cannot have DVID running while running the test suite, as it would conflict with the mock server in the test suite.

Mock Server
-----------
For test purposes, a mock DVID server is implemented in the ``mockserver`` directory.
It serves up HDF5 datasets over http using the `DVID REST API`_.

.. _DVID REST API: http://godoc.org/github.com/janelia-flyem/dvid/datatype/voxels#pkg-constants

The mock server pulls its data from an hdf5 file with a special structure.
The ``H5MockServerDataFile`` utility class can be used to generate the file:

.. code-block:: python

    import numpy
    from mockserver.h5mockserver import H5MockServerDataFile
    
    # Generate a volume to store.
    data = numpy.random.randint( 0, 256, (1,100,200,300) )
    voxels_metadata = VoxelsMetadata.create_default_metadata( (1,100,200,300), numpy.uint8, 'cxyz', 1.0, "" )
    
    # Create special server datafile with one dataset, with one node.
    # Then add our data volume to it.
    with H5MockServerDataFile( 'mock_storage.h5' ) as server_datafile:
        server_datafile.add_node( 'my_dataset', 'abc123' )
        server_datafile.add_volume( 'my_dataset', 'my_volume', data_view, voxels_metadata )
    
Once you have a datafile, the server can be started from the command line:

.. code-block:: bash
    
    $ cd mockserver
    $ PYTHONPATH=.. python h5mockserver.py mock_storage.h5
    
See ``h5mockserver.py`` the datafile format details.
    
Maintaining this Documentation
==============================

This documentation is built with `Sphinx <http://sphinx-doc.org/>`_, and 
hosted on github as a `github page <http://pages.github.com/>`_ for the pydvid repo.

To make a changes to the docs, edit the desired ``.rst`` file(s) in the docs directory, and then build the docs:

.. code-block:: bash

    cd docs
    PYTHONPATH=.. make html

First, view your changes locally:

.. code-block:: bash

    firefox build/html/index.html

Your changes will not be visible online until they are applied to the special ``gh-pages`` branch of pydvid and pushed.

There is a script in pydvid for automating this process.
It is highly recommended that you use a special local copy of the pydvid repo to do this.  Just follow these steps:

1) Make sure your changes to the .rst files are pushed to pydvid/master.
2) Make a new clone of the pydvid repo, and checkout the ``gh-pages`` branch.
3) Run the ``update_from_master.sh`` script.

Here's a walk-through (output not shown).

.. code-block:: bash

    $ pwd
    /home/bergs/workspace/pydvid/docs
    $ git add -u .
    $ git commit -m "docs: Added instructions for documentation maintenance."
    $ git push origin master
    $ cd /tmp
    $ git clone ssh://git@github.com/ilastik/pydvid pydvid-gh-pages
    $ cd pydvid-gh-pages/
    $ git checkout gh-pages
    $ ./update_from_master.sh 

The ``update_from_master.sh`` script handles the necessary pre-processing required by the github pages system.
You can view the updated documentation at `<http://janelia-flyem.github.com/pydvid>`_.

