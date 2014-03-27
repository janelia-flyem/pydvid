import json
import collections

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

_VolumeInfo = collections.namedtuple('_VolumeInfo', 'shape dtype axiskeys metadata')
class VolumeInfo(_VolumeInfo):
    """
    Tuple describing a DVID volume. Fields are:
    
    shape: The shape of the array, including a prepended 'channel' axis
    dtype: e.g. numpy.uint8
    axiskeys: A string representing the indexing order for the volume, e.g. "cxyz".
              Note the prepended channel axis.
    metadata: The raw, parsed metadata provided by dvid (a dict)
    """
    def __new__(cls, *args, **kwargs):
        """
        Constructor. Three signatures:
        - VolumeInfo(jsontext)
        - VolumeInfo(metadata)
        - VolumeInfo(shape, dtype, axiskeys, metadata) # as a 'normal' tuple
        
        The second form parses the given json text and return a VolumeInfo namedtuple.
        Raise a ValueError if the json can't be parsed.
        
        NOTE: By DVID convention, the axiskeys are returned assuming FORTRAN ORDER.
        NOTE: The VolumeInfo will prepend a 'channel' axis to the information.
        """
        if len(args) + len(kwargs) > 1:
            # Ensure that the dtype arg is always literally a 
            #  numpy.dtype (e.g. numpy.dtype('uint8'), 
            #  not a scaler type (e.g. numpy.uint8)
            args = list(args)
            args[1] = numpy.dtype(args[1])
            return _VolumeInfo.__new__( cls, *args, **kwargs )

        assert len(kwargs) == 0 and len(args) == 1, \
            "Wrong numnber of arguments to VolumeInfo constructor."

        assert args[0] is not None
        if isinstance( args[0], str ):
            metadata = json.loads( args[0] )
        elif isinstance( args[0], dict ):
            metadata = args[0]

        # TODO: Validate metadata against a schema...
    
        # We always in include "channel" as the FIRST axis
        # (DVID uses fortran-order notation.)
        shape = []
        shape.append( len(metadata["Values"]) )
        axiskeys = 'c'
    
        dtypes = []
        for channel_fields in metadata["Values"]:
            dtypes.append( numpy.dtype( channel_fields["DataType"] ) )

        assert all( map( lambda dtype: dtype == dtypes[0], dtypes ) ), \
            "Can't support heterogeneous channel types: {}".format( dtypes )
    
        for axisfields in metadata['Axes']:
            key = str(axisfields["Label"]).lower()
            axiskeys += key
            shape.append( axisfields["Size"] )

        # Now init tuple baseclass
        return _VolumeInfo.__new__( cls, tuple(shape), dtypes[0], axiskeys, metadata )

    @classmethod
    def create_default_metadata(cls, shape, dtype, axiskeys, resolution, units):
        """
        Create a default metadata dict from scratch using the given parameters,
        which can then be customized as needed.
        
        Example usage:
        
            metadata = VolumeInfo.create_default_metadata( (3,100,200,300), numpy.uint8, 'cxyz', 1.5, "micrometers" )
    
            # Customize: Adjust resolution for Z-axis
            assert metadata["Axes"][2]["Label"] == "Z"
            metadata["Axes"][2]["Resolution"] = 6.0

            # Customize: name channels
            metadata["Values"][0]["Label"] = "intensity-R"
            metadata["Values"][1]["Label"] = "intensity-G"
            metadata["Values"][2]["Label"] = "intensity-B"

            # Prepare for transmission: encode to json
            jsontext = json.dumps( metadata )
        
        """
        assert axiskeys[0] == 'c', "Channel axis must be first"
        
        # Convert to numpy.dtype if necessary (consistent with ndarray.dtype)
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
        return metadata
    
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
            Generate a vigra.AxisTags object from this metadata
            """
            tags = vigra.AxisTags()
            tags.insert( 0, vigra.AxisInfo('c', typeFlags=vigra.AxisType.Channels) )
            dtypes = []
            channel_labels = []
            for channel_fields in self.metadata["Values"]:
                dtypes.append( numpy.dtype( channel_fields["DataType"] ).type )
                channel_labels.append( channel_fields["Label"] )

            # We monkey-patch the channel labels onto the axistags object as a new member
            tags.channelLabels = channel_labels
            for axisfields in self.metadata['Axes']:
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
        def create_volumeinfo_from_h5_dataset(cls, dataset):
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
                return VolumeInfo( metadata )
            elif _have_vigra and 'axistags' in dataset.attrs:
                axistags = vigra.AxisTags.fromJSON( dataset.attrs['axistags'] )
                return cls.create_volumeinfo_from_axistags( shape, dtype, axistags )
            else:
                # Choose default axiskeys
                default_keys = 'cxyzt'
                axiskeys = default_keys[:len(shape)]
                metadata = VolumeInfo.create_default_metadata( shape, dtype, axiskeys, 1.0, "" )
                return VolumeInfo( shape, dtype, axiskeys, metadata )
    
    
