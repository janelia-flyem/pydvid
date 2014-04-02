import os
import json
import httplib
import contextlib

import jsonschema

import pydvid

def get_json_generic( connection, resource_path, schema=None ):
    """
    Request the json data found at the given resource path, e.g. '/api/datasets/info'
    If schema is a dict, validate the response against it.
    If schema is a str, it should be the name of a schema file found in pydvid/schemas.
    """
    connection.request( "GET", resource_path )
    with contextlib.closing( connection.getresponse() ) as response:
        if response.status != httplib.OK:
            raise pydvid.errors.DvidHttpError( 
                "requesting json for: {}".format( resource_path ),
                response.status, response.reason, response.read(),
                "GET", resource_path, "")
        
        try:
            parsed_response = json.loads( response.read() )
        except ValueError as ex:
            raise Exception( "Couldn't parse the dataset info response as json:\n"
                             "{}".format( ex.args ) )
        
        if schema:
            if isinstance( schema, str ):
                schema = parse_schema( schema )
            assert isinstance( schema, dict )
            jsonschema.validate( parsed_response, schema )

        return parsed_response

def parse_schema( schema_filename ):
    schema_dir = os.path.join( os.path.dirname(pydvid.__file__), 'dvidschemas' )
    schema_path = os.path.join( schema_dir, schema_filename )
    with open( schema_path ) as schema_file:
        return json.load( schema_file )
