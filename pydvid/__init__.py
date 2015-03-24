__version__="0.1"

#
# Monkey-patch HTTPConnection to allow logging of all requests
#
import logging
import httplib
orig_request = httplib.HTTPConnection.request
http_connection_logger = logging.getLogger("httplib.HTTPConnection")
def logged_request(conn, method, url, *args, **kwargs):
    log_str = "{method} {host}:{port}{url}"\
              .format( method=method, host=conn.host, port=conn.port, url=url )
    http_connection_logger.debug( log_str )
    return orig_request(conn, method, url, *args, **kwargs)    
httplib.HTTPConnection.request = logged_request

import errors
import util
import general
import voxels
import keyvalue
import labelgraph
import dvid_connection

# Note that we DO NOT automatically import gui here, 
#  since PyQt4 is an optional dependency
# import gui
