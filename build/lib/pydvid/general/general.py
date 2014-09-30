from pydvid.util import get_json_generic

def get_server_info( connection ):
    """
    Return the json data provided by the ``/api/server/info`` DVID call.
    """
    return get_json_generic( connection, "/api/server/info", schema='dvid-server-info-v0.01.schema.json' )

def get_server_types(connection):
    """
    Return the json data provided by the ``/api/server/types`` DVID call.
    """
    return get_json_generic( connection, "/api/server/types", schema='dvid-server-types-v0.01.schema.json' )

def get_repos_info( connection ):
    """
    Return the json data provided by the ``/api/repos/info`` DVID call.
    """
    return get_json_generic( connection, "/api/repos/info", schema='dvid-repos-info-v0.01.schema.json' )
