import httplib
import threading

class DvidConnection(object):
    """
    Simple wrapper around a pool of HTTPConnection instances.
    One connection is maintained for each thread.
    All attribute access is forwarded to the underlying connection for the current thread.  
    So, to clients, this class looks just like a normal HTTPConnection, 
      but really each thread gets access to its own HTTPConnection.
    """

    def __init__(self, hostname, timeout=None):
        """
        hostname: The DVID server hostname, e.g. 'emdata1' or 'localhost:8000'
        """
        # Open a connection to the server
        self.hostname = hostname
        self._connections = {}
        self.timeout = timeout
        self._lock = threading.Lock()
    
    def __getattribute__(self, name):
        try:
            # If we have this attr, use it. 
            # (e.g. self.hostname, self.close, self._connections)
            return object.__getattribute__(self, name)
        except:
            # Get/create the HTTPConnection associated with the current thread
            thread_id = threading.current_thread().ident
            try:
                return getattr(self._connections[thread_id], name)
            except:
                with self._lock:
                    if thread_id not in self._connections:
                        connection = httplib.HTTPConnection(self.hostname, timeout=self.timeout)
                        self._connections[thread_id] = connection
                return getattr(self._connections[thread_id], name)

    def close(self):
        # Close all underlying connections for all threads.
        for conn in self._connections.values():
            conn.close()

    # TODO: Implement special request() override that ensures the previous request (if any) has already been fully read, and raises an exception otherwise.
    #       See httplib docs: https://docs.python.org/2/library/httplib.html#httplib.HTTPConnection.getresponse
    