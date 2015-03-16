import json
import httplib
import contextlib

import numpy

from pydvid.errors import DvidHttpError, UnexpectedResponseError
from pydvid.util import get_json_generic
from pydvid.voxels.voxels_metadata import VoxelsMetadata
from pydvid.voxels.voxels_nddata_codec import VoxelsNddataCodec

def get_metadata( connection, uuid, data_name ):
    """
    Query the voxels metedata for the given node/data_name.
    """
    rest_query = "/api/node/{uuid}/{data_name}/metadata".format( uuid=uuid, data_name=data_name )
    parsed_json = get_json_generic( connection, rest_query )
    return VoxelsMetadata( parsed_json )

def create_new( connection, uuid, data_name, voxels_metadata ):
    """
    Create a new volume in the dvid server.
    """
    dvid_typename = voxels_metadata.determine_dvid_typename()
    rest_query = "/api/repo/{uuid}/instance".format(uuid=uuid)
    
    # DVID API will change soon to allow us to send the 
    ## TODO: Validate schema
    ##message_json = voxels_metadata.to_json()

    # For now, we simply hard-code these settings.
    n_dims = len(voxels_metadata.shape)-1
    config_data = { "dataname" : data_name,
                    "typename" : dvid_typename,
                    "BlockSize" : ",".join( ("32",)*n_dims ),
                    "VoxelSize" : ",".join( ("1.0",)*n_dims ),
                    "VoxelUnits" : ",".join( ("nanometers",)*n_dims ) }
    
    message_json = json.dumps(config_data) 
    
    headers = { "Content-Type" : "text/json" }
    connection.request( "POST", rest_query, body=message_json, headers=headers )

    with contextlib.closing( connection.getresponse() ) as response:
        #if response.status != httplib.NO_CONTENT:
        if response.status != httplib.OK:
            raise DvidHttpError( 
                "voxels.new", response.status, response.reason, response.read(),
                 "POST", rest_query, message_json, headers)

        # Apparently dvid returns a status message in the response.  
        # We can just read it and ignore it.
        response_text = response.read()

def get_ndarray( connection, uuid, data_name, access_type, voxels_metadata, start, stop, query_args=None, throttle=False ):
    _validate_query_bounds( start, stop, voxels_metadata.shape )
    codec = VoxelsNddataCodec( voxels_metadata.dtype )
    response = get_subvolume_response( connection, uuid, data_name, access_type, start, stop, query_args=query_args, throttle=throttle )
    with contextlib.closing(response):
        # "Full" roi shape includes channel axis and ALL channels
        full_roi_shape = numpy.array(stop) - start
        full_roi_shape[0] = voxels_metadata.shape[0]
        decoded_data = codec.decode_to_ndarray( response, full_roi_shape )
    
        # Was the response fully consumed?  Check.
        # NOTE: This last read() is not optional.
        # Something in the http implementation gets upset if we read out the exact amount we needed.
        # That is, we MUST read beyond the end of the stream.  So, here we go. 
        excess_data = response.read()
        if excess_data:
            # Uh-oh, we expected it to be empty.
            raise UnexpectedResponseError( "Received data was longer than expected by {} bytes.  (Expected only {} bytes.)"
                                           "".format( len(excess_data), len(numpy.getbuffer(decoded_data)) ) ) 
        # Select the requested channels from the returned data.
        return decoded_data


def post_ndarray( connection, uuid, data_name, access_type, voxels_metadata, start, stop, new_data, throttle=False ):
    _validate_query_bounds( start, stop, voxels_metadata.shape, allow_overflow_extents=True )
    codec = VoxelsNddataCodec( voxels_metadata.dtype )
    rest_query = _format_subvolume_rest_uri( uuid, data_name, access_type, start, stop, format="", query_args=None, throttle=throttle )
    body_data_stream = codec.create_encoded_stream_from_ndarray(new_data)
    
    # Slightly tricky here:
    # The httplib docs say that we can only send a stream that has a fileno() method,
    #  but it turns out we can send any stream as long as we provide our own content-length header.
    headers = { "Content-Type" : VoxelsNddataCodec.VOLUME_MIMETYPE,
                "Content-Length" : str(codec.calculate_buffer_len(new_data.shape)) }
    
    connection.request( "POST", rest_query, body=body_data_stream, headers=headers )
    with contextlib.closing( connection.getresponse() ) as response:
        #if response.status != httplib.NO_CONTENT:
        if response.status != httplib.OK:
            raise DvidHttpError( 
                "subvolume post", response.status, response.reason, response.read(),
                 "POST", rest_query, "<binary data>", headers)
        
        # Something (either dvid or the httplib) gets upset if we don't read the full response.
        response.read()


def get_subvolume_response( connection, uuid, data_name, access_type, start, stop, format="", query_args=None, throttle=False ):
    """
    Request a subvolume from the server and return the raw HTTPResponse stream it returns.
    """
    rest_query = _format_subvolume_rest_uri( uuid, data_name, access_type, start, stop, format, query_args, throttle )
    connection.request( "GET", rest_query )
    response = connection.getresponse()
    if response.status != httplib.OK:
        raise DvidHttpError( 
            "subvolume query", response.status, response.reason, response.read(),
            "GET", rest_query, "" )
    return response


def _format_subvolume_rest_uri( uuid, data_name, access_type, start, stop, format="", query_args=None, throttle=False ):
    """
    Construct the REST URI for get/post of a voxels subvolume.
    """
    start = numpy.asarray(start)
    stop = numpy.asarray(stop)
    query_args = query_args or {}
    query_args = { str(k) : str(v) for k,v in query_args.items() }

    # Drop channel before requesting from DVID
    start = start[1:]
    stop = stop[1:]

    # Dvid roi shape doesn't include channel
    dvid_roi_shape = stop - start
    roi_shape_str = "_".join( map(str, dvid_roi_shape) )
    start_str = "_".join( map(str, start) )
    
    dims_string = "_".join( map(str, range(len(start)) ) )
    rest_query = "/api/node/{uuid}/{data_name}/{access_type}/{dims_string}/{roi_shape_str}/{start_str}"\
                 "".format( uuid=uuid, 
                            data_name=data_name, 
                            access_type=access_type,
                            dims_string=dims_string, 
                            roi_shape_str=roi_shape_str, 
                            start_str=start_str )
    if format != "":
        rest_query += "/" + format

    # throttle is a special arg: normally it is set explicity as an arg to this function.
    # but if the user also supplied it in the query_args, just verify consistency.
    if 'throttle' in query_args:
        if throttle:
            assert query_args['throttle'] == 'on'
        else:
            assert query_args['throttle'] == 'off'
    
    if throttle:
        query_args["throttle"] = "on"
    if query_args:
        rest_query += "?" + "&".join( map( "=".join, query_args.items() ) )
    return rest_query


def _validate_query_bounds( start, stop, volume_shape, allow_overflow_extents=True ):
    """
    Assert if the given start, stop, and volume_shape are not a valid combination. 
    If allow_overflow_extents is True, then this function won't complain if the 
    start/stop fields exceed the current bounds of the dataset.
    (For writing, it's okay to exceed the bounds.  
    For reading, that would probably be an error.)
    """
    shape = volume_shape
    start, stop, shape = map( numpy.array, (start, stop, shape) )
    assert start[0] == 0, "Subvolume get/post must include all channels.  Invalid roi: {}, {}".format( start, stop )
    assert stop[0] == shape[0], "Subvolume get/post must include all channels.  Invalid roi: {}, {}".format( start, stop )
    assert len(start) == len(stop) == len(shape), \
        "start/stop/shape mismatch: {}/{}/{}".format( start, stop, shape )
    assert (start < stop).all(), "Invalid start/stop: {}/{}".format( start, stop )
    #assert (start >= 0).all(), "Invalid start: {}".format( start )
    
    if not allow_overflow_extents and (None not in list(shape)):
        assert (start < shape).all(), "Invalid start/shape: {}/{}".format( start, shape )
        assert (stop <= shape).all(), "Invalid stop/shape: {}/{}".format( stop, shape )
    else:
        assert (start[0] < shape[0]).all(), "Invalid channel start/shape: {}/{}".format( start, shape )
        assert (stop[0] <= shape[0]).all(), "Invalid channel stop/shape: {}/{}".format( stop, shape )
