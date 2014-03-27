import json

import numpy
try:
    import vigra
    _have_vigra = True
except ImportError:
    _have_vigra = False
    
try:
    import h5py
    _have_h5py = True
except:
    _have_h5py = False

class VolumeMetadata(dict):
    """
    A dict subclass for the dvid nd-data metadata response.
    Also provides the following convenience attributes:
    
        dtype: e.g. numpy.uint8
        shape: The shape of the array, including a prepended 'channel' axis
        axiskeys: A string representing the indexing order for the volume, e.g. "cxyz".
                  Note the prepended channel axis.
                  NOTE: By DVID convention, the axiskeys are generally expressed in fortran order.
    """
    
    @property
    def shape(self):
        return self._shape

    @property
    def dtype(self):
        return self._dtype

    @property
    def axiskeys(self):
        return self._axiskeys

    def __init__(self, metadata):
        """
        Constructor. Three signatures:
        - VolumeInfo(jsontext)
        - VolumeInfo(metadata_dict)
        
        The first form parses the given json text, and raises a ValueError if the json can't be parsed.
        """
        assert isinstance( metadata, (dict, str) ), "Expected metadata to be a dict or json str."
        if isinstance( metadata, str ):
            metadata = json.loads( metadata )

        # TODO: Validate metadata against a schema...

        # Init base class: just copy original metadata
        super( VolumeMetadata, self ).__init__( **metadata )

        dtypes = []
        for channel_fields in metadata["Values"]:
            dtypes.append( numpy.dtype( channel_fields["DataType"] ) )

        assert all( map( lambda dtype: dtype == dtypes[0], dtypes ) ), \
            "Can't support heterogeneous channel types: {}".format( dtypes )
        self._dtype = dtypes[0]
        
        # We always in include "channel" as the FIRST axis
        # (DVID uses fortran-order notation.)
        shape = []
        shape.append( len(metadata["Values"]) ) 
        for axisfields in metadata['Axes']:
            shape.append( axisfields["Size"] )
        self._shape = tuple(shape)
    
        axiskeys = 'c'
        for axisfields in metadata['Axes']:
            axiskeys += str(axisfields["Label"]).lower()
        self._axiskeys = axiskeys

    def to_json(self):
        """
        Convenience method: dump to json string.
        """
        # TODO: Validate schema
        return json.dumps(self)

    @classmethod
    def create_default_metadata(cls, shape, dtype, axiskeys, resolution, units):
        """
        Create a default VolumeMetadata object from scratch using the given parameters,
        which can then be customized as needed.
        
        Example usage:
        
            metadata = VolumeMetadata.create_default_metadata( (3,100,200,300), numpy.uint8, 'cxyz', 1.5, "micrometers" )
    
            # Customize: Adjust resolution for Z-axis
            assert metadata["Axes"][2]["Label"] == "Z"
            metadata["Axes"][2]["Resolution"] = 6.0

            # Customize: name channels
            metadata["Values"][0]["Label"] = "intensity-R"
            metadata["Values"][1]["Label"] = "intensity-G"
            metadata["Values"][2]["Label"] = "intensity-B"

            # Prepare for transmission: encode to json
            jsontext = metadata.to_json()
        
        """
        assert axiskeys[0] == 'c', "Channel axis must be first"
        assert len(axiskeys) == len(shape), "shape/axiskeys mismatch: {} doesn't match {}".format( axiskeys, shape )
        dtype = numpy.dtype(dtype)
        
        metadata = {}
        metadata["Axes"] = []
        for key, size in zip(axiskeys, shape)[1:]: # skip channel
            axisdict = {}
            axisdict["Label"] = key.upper()
            axisdict["Resolution"] = resolution
            axisdict["Units"] = units
            axisdict["Size"] = size
            metadata["Axes"].append( axisdict )
        
        metadata["Values"] = []
        num_channels = shape[ 0 ]
        for _ in range( num_channels ):
            metadata["Values"].append( { "DataType" : dtype.name,
                                         "Label" : "" } )
        return VolumeMetadata(metadata)
    
    def determine_dvid_typename(self):
        typenames = { ('uint8',  1) : 'grayscale8',
                      ('uint32', 1) : 'labels32',
                      ('uint64', 1) : 'labels64',
                      ('uint8',  4) : 'rgba8' }

        # First axis is always channel
        num_channels = self.shape[0]
        try:
            return typenames[(self.dtype.name, num_channels)]
        except KeyError:
            msg = "DVID does not have an associated typename for {} channels of pixel type {}"\
                  "".format( num_channels, self.dtype )
            raise Exception( msg )
    
    if _have_vigra:
        def create_axistags(self):
            """
            Generate a vigra.AxisTags object corresponding to this VolumeMetadata
            """
            tags = vigra.AxisTags()
            tags.insert( 0, vigra.AxisInfo('c', typeFlags=vigra.AxisType.Channels) )
            dtypes = []
            channel_labels = []
            for channel_fields in self["Values"]:
                dtypes.append( numpy.dtype( channel_fields["DataType"] ).type )
                channel_labels.append( channel_fields["Label"] )

            # We monkey-patch the channel labels onto the axistags object as a new member
            tags.channelLabels = channel_labels
            for axisfields in self['Axes']:
                key = str(axisfields["Label"]).lower()
                res = axisfields["Resolution"]
                tag = vigra.defaultAxistags(key)[0]
                tag.resolution = res
                tags.insert( len(tags), tag )
                # TODO: Check resolution units, because apparently 
                #        they can be different from one axis to the next...
    
            assert all( map( lambda dtype: dtype == dtypes[0], dtypes ) ), \
                "Can't support heterogeneous channel types: {}".format( dtypes )

            return tags
        
        @classmethod
        def create_volumeinfo_from_axistags(cls, shape, dtype, axistags):
            assert False, "TODO..."
            

    if _have_h5py:    
        @classmethod
        def create_from_h5_dataset(cls, dataset):
            """
            Create a VolumeInfo object to describe the given h5 dataset object.
            dataset: An hdf5 dataset object that meets the following criteria:
                     - Indexed in F-order
                     - Has an 'axistags' attribute, produced using vigra.AxisTags.toJSON()
                     - Has an explicit channel axis
            """
            dtype = dataset.dtype.type
            shape = dataset.shape
            if 'dvid_metadata' in dataset.attrs:
                metadata_json = dataset.attrs['dvid_metadata']
                metadata = json.loads( metadata_json )
                return VolumeMetadata( metadata )
            elif _have_vigra and 'axistags' in dataset.attrs:
                axistags = vigra.AxisTags.fromJSON( dataset.attrs['axistags'] )
                return cls.create_volumeinfo_from_axistags( shape, dtype, axistags )
            else:
                # Choose default axiskeys
                default_keys = 'cxyzt'
                axiskeys = default_keys[:len(shape)]
                return VolumeMetadata.create_default_metadata( shape, dtype, axiskeys, 1.0, "" )
    
    
