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


def update_vertices( connection, uuid, data_name, vertex_list ):
    """
    Create or update vertices in the label graph
    """
    payload = 999
    for val in range(0, len(vertex_list), payload):
        _update_vertices(connection, uuid, data_name, vertex_list[val:val+payload])

def update_vertex( connection, uuid, data_name, vert, weight ):
    """
    Create or update a vertex in the label graph
    """
    vertex_list = []
    vertex_list.append((vert, weight))
    _update_vertices(conection, uuid, data_name, vertex_list) 


def _update_vertices( connection, uuid, data_name, vertex_list):
    """
    Create or update vertices in the label graph
    """
   
    # construct graph                                                                            
    graph_data = {}
    vertices = []
    edges = []

    for vertex in vertex_list:
        vert, weight = vertex
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




def update_edges( connection, uuid, data_name, edge_list ):
    """
    Create or update edges in the label graph
    """
    payload = 999/2 # be conservative and assume 2x edge unique vertices
    for val in range(0, len(edge_list), payload):
        _update_edges(connection, uuid, data_name, edge_list[val:val+payload])


def update_edge( connection, uuid, data_name, vert1, vert2, weight ):
    """
    Create or update an edge in the label graph
    """
    edge_list = []
    edge_list.append((vert1, vert2, weight))
    _update_edges(conection, uuid, data_name, edge_list) 


def _update_edges( connection, uuid, data_name, edge_list):
    # construct graph                                                                            
    graph_data = {}
    vertices = []
    edges = []


    for edge in edge_list:
        vert1, vert2, weight = edge
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
                "labelgraph edge post", response.status, response.reason, response.read(),
                 "POST", rest_cmd, "json data", headers)
        
        # Something (either dvid or the httplib) gets upset if we don't read the full response.
        response.read()



if __name__ == "__main__":
    import httplib
    conn = httplib.HTTPConnection("localhost:8000")
     
