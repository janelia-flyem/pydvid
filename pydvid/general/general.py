from pydvid.util import get_json_generic

def get_server_info( connection ):
    return get_json_generic( connection, "/api/server/info", schema='server_info.jsonschema' )

def get_server_types(connection):
    return get_json_generic( connection, "/api/server/types", schema='server_types.jsonschema' )

def get_datasets_info( connection ):
    return get_json_generic( connection, "/api/datasets/info" ) #, schema='server_info.jsonschema' )

def get_datasets_list(connection):
    return get_json_generic( connection, "/api/datasets/list", schema='datasets_list.jsonschema' )

