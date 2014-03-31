from dvidclient.util import get_json_generic

def get_server_info( connection ):
    return get_json_generic( connection, "/api/server/info" )

def get_server_types(connection):
    return get_json_generic( connection, "/api/server/types" )

def get_datasets_info( connection ):
    return get_json_generic( connection, "/api/datasets/info" )

def get_datasets_list(connection):
    return get_json_generic( connection, "/api/datasets/list" )

