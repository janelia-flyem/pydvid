class DvidHttpError( Exception ):
    """
    Raised when DVID returns an http error code to any request.
    """
    def __init__(self, attempted_action_name, status_code, reason, response_body, 
                 method, request_uri, request_body="<unspecified>", request_headers="<unspecified>"):
        self.attempted_action_name = attempted_action_name
        self.status_code = status_code
        self.reason = reason
        self.response_body = response_body
        self.method = method
        self.request_uri = request_uri
        self.request_body = request_body
        self.request_headers = request_headers
    
    def __str__(self):
        caption = 'While attempting "{}" DVID returned an error: {}, "{}"\n'\
                  ''.format( self.attempted_action_name, self.status_code, self.reason )
        caption += "Request METHOD: {}\n".format( self.method )
        caption += "Request URI: {}\n".format( self.request_uri )
        caption += "Request HEADERS: {}\n".format( self.request_headers )
        caption += "Request BODY: {}\n".format( self.request_body )
        caption += "Response body from server was:\n"
        caption += self.response_body
        caption += "\n"
        return caption

class UnexpectedResponseError( Exception ):
    """
    Raised when DVID did not return an Http error, 
    but the response nonetheless does not match our expectations.
    """
    pass
        