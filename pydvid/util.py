import os
import json
import httplib
import contextlib

import jsonschema

import pydvid

import re

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

# Pattern for all json schema filenames, e.g. dvid-server-info-v0.01.schema.json
schema_name_pattern = re.compile('(?P<message_name>.*)-v\d+\.\d+\.schema.json')

def parse_schema( schema_filename ):
    """
    Parse the schema with the given schema filename.
    Note that the rest of the path should not be provided here:
    Instead, we'll parse the filename to determine the full path to the schema.
    
    For example, if schema_filename = 'dvid-server-info-v0.01.schema.json', 
    then we'll find it at 'pydvid/dvidschemas/json/dvid-server-info/dvid-server-info-v0.01.schema.json'
    """
    filename_match = schema_name_pattern.match( schema_filename )
    assert filename_match, "Schema filename does not match expected pattern: {}"\
                           "".format( schema_filename )
    
    # Extract the message name, which is also the directory name    
    message_name = filename_match.groupdict()['message_name']

    # Construct the full path
    pydvid_dir = os.path.dirname(pydvid.__file__)
    schema_json_dir = os.path.join( pydvid_dir, 'dvidschemas/json' )
    schema_dir = os.path.join( schema_json_dir, message_name )
    schema_path = os.path.join( schema_dir, schema_filename )

    # Parse the json
    with open( schema_path ) as schema_file:
        return json.load( schema_file )
