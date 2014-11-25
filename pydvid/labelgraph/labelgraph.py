import httplib
import contextlib
from pydvid.errors import DvidHttpError, UnexpectedResponseError
import json

def create_new( connection, uuid, data_name ):
    """
    Create a new labelgraph in the dvid server.
    """
    rest_cmd = "/api/repo/{uuid}/instance".format(uuid=uuid)
    config_data = {}
    config_data["dataname"] = data_name
    config_data["typename"] = "labelgraph"
    connection.request( "POST", rest_cmd, body=json.dumps(config_data) )

    with contextlib.closing( connection.getresponse() ) as response:
        #if response.status != httplib.NO_CONTENT:
        if response.status != httplib.OK:
            raise DvidHttpError( "labelgraph.create_new", response.status, response.reason, 
                                 response.read(), "POST", rest_cmd )

def update_vertex( connection, uuid, data_name, vert, weight ):
    """
    Create or update a vertex in the label graph
    """
   
    # construct graph                                                                            
    graph_data = {}
    vertices = []
    edges = []
    vertices.append({"Id": vert, "Weight": weight})
    graph_data["Vertices"] = vertices                                                            
    graph_data["Edges"] = edges                                                                  

    # post data
    rest_cmd = "/api/node/{uuid}/{data_name}/weight".format( **locals() )
    headers = { "Content-Type" : "application/json" }
    connection.request( "POST", rest_cmd, body=json.dumps(graph_data), headers=headers )
    with contextlib.closing( connection.getresponse() ) as response:
        #if response.status != httplib.NO_CONTENT:
        if response.status != httplib.OK:
            raise DvidHttpError( 
                "labelgraph vertex post", response.status, response.reason, response.read(),
                 "POST", rest_cmd, "json data", headers)
        
        # Something (either dvid or the httplib) gets upset if we don't read the full response.
        response.read()

def update_vertex( connection, uuid, data_name, vert1, vert2, weight ):
    """
    Create or update a vertex in the label graph
    """
   
    # construct graph                                                                            
    graph_data = {}
    vertices = []
    edges = []
    edges.append({"Id1": int(vert1), "Id2": int(vert2), "Weight": int(weight)})
    graph_data["Vertices"] = vertices                                                            
    graph_data["Edges"] = edges                                                                  

    # post data
    rest_cmd = "/api/node/{uuid}/{data_name}/weight".format( **locals() )
    headers = { "Content-Type" : "application/json" }
    connection.request( "POST", rest_cmd, body=json.dumps(graph_data), headers=headers )
    with contextlib.closing( connection.getresponse() ) as response:
        #if response.status != httplib.NO_CONTENT:
        if response.status != httplib.OK:
            raise DvidHttpError( 
                "labelgraph vertex post", response.status, response.reason, response.read(),
                 "POST", rest_cmd, "json data", headers)
        
        # Something (either dvid or the httplib) gets upset if we don't read the full response.
        response.read()


if __name__ == "__main__":
    import httplib
    conn = httplib.HTTPConnection("localhost:8000")
     
