import json
import httplib
import contextlib
from dvidclient.errors import DvidHttpError

def get_json_generic( connection, resource_path, schema=None ):
    connection.request( "GET", resource_path )
    with contextlib.closing( connection.getresponse() ) as response:
        if response.status != httplib.OK:
            raise DvidHttpError( 
                "requesting json for: {}".format( resource_path ),
                response.status, response.reason, response.read(),
                "GET", resource_path, "")
        
        try:
            datasets_info = json.loads( response.read() )
        except ValueError as ex:
            raise Exception( "Couldn't parse the dataset info response as json:\n"
                             "{}".format( ex.args ) )
        
        # TODO: Schema validation
        return datasets_info
