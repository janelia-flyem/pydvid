API Reference
=============

.. currentmodule:: pydvid

.. _general:

general
-------

.. note:: All functions in this module can be used directly from the ``pydvid.general`` namespace, e.g. ``pydvid.general.get_server_info()``

.. automodule:: pydvid.general.general
   :members:    

keyvalue
--------

.. note:: All functions in this module can be used directly from the ``pydvid.keyvalue`` namespace, e.g. ``pydvid.keyvalue.get_value(...)``

.. automodule:: pydvid.keyvalue.keyvalue
   :members:

voxels
------

.. note:: All members of this module can be used directly from the ``pydvid.voxels`` namespace, e.g. ``pydvid.voxels.VoxelsAccessor``

.. note:: Please see the :ref:`quickstart <quickstart>` documentation for examples and important usage notes regarding the ``voxels`` API.

.. currentmodule:: pydvid.voxels

.. automodule:: pydvid.voxels.voxels
   :members:

.. currentmodule:: pydvid.voxels.voxels_metadata

.. autoclass:: pydvid.voxels.VoxelsMetadata
   :members:
   
   .. automethod:: __init__

.. currentmodule:: pydvid.voxels.voxels_accessor

.. _slicing:

.. autoclass:: pydvid.voxels.VoxelsAccessor
   :members:

   .. automethod:: __init__
   .. automethod:: __getitem__
   .. automethod:: __setitem__
   