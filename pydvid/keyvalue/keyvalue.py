import httplib
import contextlib
from pydvid.errors import DvidHttpError, UnexpectedResponseError
from pydvid.util import get_json_generic
import json

def create_new( connection, uuid, data_name ):
    """
    Create a new keyvalue table in the dvid server.
    """
    rest_cmd = "/api/repo/{uuid}/instance".format(uuid=uuid)
    config_data = {}
    config_data["dataname"] = data_name
    config_data["typename"] = "keyvalue"
    connection.request( "POST", rest_cmd, body=json.dumps(config_data) )

    with contextlib.closing( connection.getresponse() ) as response:
        #if response.status != httplib.NO_CONTENT:
        if response.status != httplib.OK:
            raise DvidHttpError( "keyvalue.create_new", response.status, response.reason, 
                                 response.read(), "POST", rest_cmd )

def get_value( connection, uuid, data_name, key ):
    """
    Request the value for the given key and return the whole thing.
    """
    response = get_value_response( connection, uuid, data_name, key ) 
    return response.read()

def put_value( connection, uuid, data_name, key, value ):
    """
    Store the given value to the keyvalue data.
    value should be either str or a file-like object with fileno() and read() methods.
    """
    rest_cmd = "/api/node/{uuid}/{data_name}/{key}".format( **locals() )
    headers = { "Content-Type" : "application/octet-stream" }
    connection.request( "POST", rest_cmd, body=value, headers=headers )
    with contextlib.closing( connection.getresponse() ) as response:
        #if response.status != httplib.NO_CONTENT:
        if response.status != httplib.OK:
            raise DvidHttpError( 
                "keyvalue post", response.status, response.reason, response.read(),
                 "POST", rest_cmd, "<binary data>", headers)
        
        # Something (either dvid or the httplib) gets upset if we don't read the full response.
        response.read()

def del_value( connection, uuid, data_name, key, value ):
    assert False, "TODO"

def get_keys( connection, uuid, data_name ):
    rest_query = "/api/node/{uuid}/{data_name}/keys".format( **locals() )
    return get_json_generic( connection, rest_query, schema='dvid-keyvalue-keys-v0.01.schema.json' )

def get_value_response( connection, uuid, data_name, key ):
    """
    Request the value for the given key return the raw HTTPResponse object.
    The caller may opt to 'stream' the data from the response instead of reading it all at once.
    """
    rest_query = "/api/node/{uuid}/{data_name}/{key}".format( **locals() )
    connection.request( "GET", rest_query )
    response = connection.getresponse()
    if response.status != httplib.OK:
        raise DvidHttpError( 
            "keyvalue request", response.status, response.reason, response.read(),
            "GET", rest_query, "" )
    return response

if __name__ == "__main__":
    import httplib
    conn = httplib.HTTPConnection("localhost:8000")
    put_value( conn, '4a', 'greetings', 'english', 'hello' )
    print "Got greeting: ", get_value( conn, '4a', 'greetings', 'english')
    
