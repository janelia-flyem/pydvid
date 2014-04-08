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

def get_datasets_info( connection ):
    """
    Return the json data provided by the ``/api/datasets/info`` DVID call.
    """
    return get_json_generic( connection, "/api/datasets/info", schema='dvid-datasets-info-v0.01.schema.json' )

def get_datasets_list(connection):
    """
    Return the json data provided by the ``/api/datasets/list`` DVID call.
    """
    return get_json_generic( connection, "/api/datasets/list", schema='dvid-datasets-list-v0.01.schema.json' )

