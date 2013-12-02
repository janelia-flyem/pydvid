import sys
import json
import collections

import numpy
import vigra

MetaInfo = collections.namedtuple('MetaInfo', 'shape dtype axistags')

def parse_meta_info_from_json(jsontext):
    """
    Parse the given json text and return a MetaInfo namedtuple
    
    NOTE: By DVID convention, the axistags are returned assuming FORTRAN ORDER.
    """
    try:
        meta_dict = json.loads( jsontext )
    except ValueError:
        sys.stderr.write("Failed to parse response as json:\n{}\n".format( jsontext ))
        raise
    
    shape = []
    tags = vigra.AxisTags()

    # We always in include "channel" as the FIRST axis
    # (DVID uses fortran-order notation.)
    shape.append( len(meta_dict["Values"]) )
    tags.insert( 0, vigra.AxisInfo('c', typeFlags=vigra.AxisType.Channels) )

    dtypes = []
    for channel_fields in meta_dict["Values"]:
        dtypes.append( numpy.dtype( channel_fields["DataType"] ).type )

    for axisfields in meta_dict['Axes']:
        key = str(axisfields["Label"]).lower()
        res = axisfields["Resolution"]
        tag = vigra.defaultAxistags(key)[0]
        tag.resolution = res
        tags.insert( len(tags), tag )
        # TODO: Check resolution units, because apparently 
        #        they can be different from one axis to the next...
        shape.append( axisfields["Size"] )

    assert all( map( lambda dtype: dtype == dtypes[0], dtypes ) ), \
        "Can't support heterogeneous channel types: {}".format( dtypes )

    return MetaInfo( tuple(shape), dtypes[0], tags )

def format_metainfo_to_json(metainfo):
    """
    Encode the given MetaInfo object into json text for transmission over http.
    """
    assert metainfo.axistags.index('c') < len(metainfo.axistags),\
        "All DVID volume metainfo must include a channel axis!"
    
    metadict = {}
    metadict["Axes"] = []
    for tag, size in zip(metainfo.axistags, metainfo.shape):
        if tag.key == "c":
            continue
        axisdict = {}
        axisdict["Label"] = tag.key.upper()
        axisdict["Resolution"] = tag.resolution
        axisdict["Units"] = "nanometers" # FIXME: Hardcoded for now
        axisdict["Size"] = size
        metadict["Axes"].append( axisdict )
    metadict["Values"] = []
    
    num_channels = metainfo.shape[ metainfo.axistags.channelIndex ]
    for _ in range( num_channels ):
        metadict["Values"].append( { "DataType" : metainfo.dtype.__name__,
                                     "Label" : "" } ) # FIXME: No label for now.
    return json.dumps( metadict )

def get_dataset_metainfo(dataset):
    """
    Create a MetaInfo object to describe the given h5 dataset object.
    dataset: An hdf5 dataset object that meets the following criteria:
             - Indexed in C-order
             - Has an 'axistags' attribute, produced using vigra.AxisTags.toJSON()
             - Has an explicit channel axis
    """
    dtype = dataset.dtype.type
    # Tricky business here:
    # The dataset is stored as a C-order-array, but DVID wants fortran order.
    shape = tuple(reversed(dataset.shape))
    c_tags = vigra.AxisTags.fromJSON( dataset.attrs['axistags'] )
    f_tags = vigra.AxisTags( list(reversed(c_tags)) )
    return MetaInfo( shape, dtype, f_tags )

