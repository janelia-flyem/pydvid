import copy
import time
import httplib
import functools
import warnings

import numpy
import voxels

from pydvid.errors import DvidHttpError
from pydvid.voxels import VoxelsMetadata

class VoxelsAccessor(object):
    """
    Http client for retrieving a voxels volume data from a DVID server.
    An instance of VoxelsAccessor is capable of retrieving data from only one remote data volume.
    To retrieve data from multiple remote volumes, instantiate multiple DvidClient objects.
    
    **TODO**:
    
    * Allow users to provide a pre-allocated array when requesting data
    """
    
    class ThrottleTimeoutException(Exception):
        pass
    
    def __init__(self, connection, uuid, data_name, 
                 query_args=None, 
                 throttle=None, 
                 retry_timeout=60.0, 
                 retry_interval=1.0, 
                 warning_interval=30.0, 
                 _metadata=None,
                 _access_type="raw"):
        """
        :param uuid: The node uuid
        :param data_name: The name of the volume
        :param connection: An ``httplib.HTTPConnection`` instance or something like it.
        :param throttle: Enable the DVID 'throttle' flag for all get/post requests
        :param retry_timeout: Total time to spend repeating failed requests before giving up.
                              (Set to 0 to prevent retries.)
        :param retry_interval: Time to wait before repeating a failed get/post.
        :param warning_interval: If the retry period exceeds this interval (but hasn't 
                                 hit the retry_timeout yet), a warning is emitted.
        :param _metadata: If provided, used as the metadata for the accessor.  Otherwise, the server is queried to obtain this volume's metadata.
        
        .. note:: When DVID is overloaded, it may indicate its busy status by returning a ``503`` 
                  (service unavailable) error in response to a get/post request.  In that case, 
                  the get/post methods below will automatically repeat the failed request until 
                  the `retry_timeout` is reached.
        """
        self.uuid = uuid
        self.data_name = data_name
        self._connection = connection
        self._retry_timeout = retry_timeout
        self._retry_interval = retry_interval
        self._warning_interval = warning_interval
        self._query_args = query_args or {}
        self._access_type = _access_type
        
        # Special case: throttle can be set explicity via the keyword or implicitly via the query_args.
        # Make sure they are consistent.
        if 'throttle' in self._query_args:
            if self._query_args['throttle'] == 'on':
                assert throttle is None or throttle is True
                self._throttle = True
            if self._query_args['throttle'] == 'off':
                assert throttle is None or throttle is False
                self._throttle = False
        elif throttle is None:
            self._throttle = False
        else:
            self._throttle = throttle

        # Request this volume's metadata from DVID
        self.voxels_metadata = _metadata
        if self.voxels_metadata is None:
            self.voxels_metadata = voxels.get_metadata( self._connection, uuid, data_name )

    @property
    def shape(self):
        """
        Property.  The maximum coordinates in the DVID volume coordinate space.
        This is the stop coordinate of the volume's bounding box.
        All data above this coordinate in any dimension is guaranteed to be invalid.
        """
        return self.voxels_metadata.shape

    @property
    def minindex(self):
        """
        Property.  The starting coordinate of the volume's bounding box.
        All data below this coordinate in any dimension is guaranteed to be invalid.
        """
        return self.voxels_metadata.minindex

    @property
    def dtype(self):
        """
        Property.  The pixel datatype of the remote DVID volume, as a ``numpy.dtype`` object.
        """
        return self.voxels_metadata.dtype

    @property
    def axiskeys(self):
        """
        Property.  
        A string representing the axis indexing order of the volume, e.g. 'cxyz'
        Always starts with 'c' (channel).
        
        .. note:: By DVID convention, the axiskeys are expressed in fortran order.
        """
        return self.voxels_metadata.axiskeys

    def _auto_retry(func):
        """
        Decorator.  If the function raises a DvidHttpError with 
        the 503 (SERVICE_UNAVAILABLE) status code, try again 
        until successful or a timeout is reached.
        """
        @functools.wraps(func)
        def _retry_wrapper( self, *args, **kwargs ):
            try:
                # Fast path for the first attempt
                return func(self, *args, **kwargs)
            except DvidHttpError as ex:
                if ex.status_code != httplib.SERVICE_UNAVAILABLE:
                    raise # not 503: this is a real problem
                elif self._retry_timeout <= self._retry_interval:
                    raise VoxelsAccessor.ThrottleTimeoutException( 
                        "Timeout due to 503 response. "
                        "VoxelsAccessor auto-retry is disabled. "
                        "(timeout <= retry: {} <= {})"
                        .format( self._retry_timeout, self._retry_interval ) )
                
                start_time = time.time()
                time_so_far = 0.0
                last_warning_time = 0.0
                n_attempts = 1

                # Keep retrying until we timeout
                while time_so_far < self._retry_timeout:
                    if time_so_far - last_warning_time > self._warning_interval:
                        warnings.warn("DVID Server has been busy for {:.1f} seconds.  Still retrying..."
                                      .format( time_so_far ))
                        last_warning_time = time_so_far
                    time.sleep( self._retry_interval )
                    n_attempts += 1
                    try:
                        return func(self, *args, **kwargs)
                    except DvidHttpError as ex:
                        if ex.status_code == httplib.SERVICE_UNAVAILABLE:
                            # 503 error from DVID indicates 'busy'
                            # We'll keep looping...
                            time_so_far = time.time() - start_time
                        else:
                            raise # not 503: this is a real problem

                # Loop finished: we timed out.
                raise VoxelsAccessor.ThrottleTimeoutException( 
                    "Timeout due to repeated 503 responses: "
                    "DVID Server is still too busy after {} attempts over {:.1f} seconds"
                    .format(n_attempts, time_so_far) )

        _retry_wrapper.__wrapped__ = func # Emulate python 3 behavior of @wraps
        return _retry_wrapper

    @_auto_retry
    def get_ndarray( self, start, stop ):
        """
        Request the subvolume specified by the given start and stop pixel coordinates.
        """
        return voxels.get_ndarray( self._connection, 
                                   self.uuid, 
                                   self.data_name, 
                                   self._access_type,
                                   self.voxels_metadata, 
                                   start, 
                                   stop,
                                   self._query_args, 
                                   self._throttle )

    def post_ndarray( self, start, stop, new_data ):
        """
        Overwrite subvolume specified by the given start and stop pixel coordinates with new_data.
        """
        # Post the data (with auto-retry)
        self._post_ndarray(start, stop, new_data)

        if ( numpy.array(stop) > self.shape ).any() or \
           ( numpy.array(start) < self.minindex ).any():
            # It looks like this post UPDATED the volume's extents.
            # Therefore, RE-request this volume's metadata from DVID so we get the new volume shape
            self.voxels_metadata = voxels.get_metadata( self._connection, self.uuid, self.data_name )

    @_auto_retry
    def _post_ndarray( self, start, stop, new_data ):
        voxels.post_ndarray( self._connection, 
                             self.uuid, 
                             self.data_name, 
                             self._access_type,
                             self.voxels_metadata, 
                             start, 
                             stop, 
                             new_data,
                             self._throttle )

    def __getitem__(self, slicing):
        """
        Implement convenient numpy-like slicing syntax for volume access.

        Limitations: 
            - "Fancy" indexing via index arrays, etc. is not supported,
              but "normal" slicing, including stepping, is supported.
               
        Examples:
        
            .. code-block:: python            

                connection = httplib.HTTPConnection( "localhost:8000" ) 
                v = VoxelsAccessor( connection, uuid=abc123, data_name='my_3d_rgb_volume' )

                # The whole thing
                a = v[:]
                a = v[...]
                
                # Arbitrary slicing
                a = v[...,10,:]
                
                # Note: DVID always returns all channels.
                #       Here, you are permitted to slice into the channel axis,
                #       but be aware that this implementation requests all 
                #       channels and returns the ones you asked for.
                blue = v[2]
                
                # Therefore, avoid this, since it results in 2 requests for the same data
                red = v[0]
                green = v[1]
                
                # Instead, do this:
                rgb = v[:]
                red, green, blue = rgb[0], rgb[1], rgb[2]
                
                # Similarly, you are permitted to use slices with steps, but be aware that 
                # the entire bounding volume will be requested, and the sliced steps will be 
                # extracted from the dense volume.
                
                # Extract the upper-left 10x10 tile of every other z-slice:
                a = v[:,:10,:10,::2]
    
                # The above is equivalent to this:
                a = v[:,:10,:10,:][...,::2]            
        """
        shape = self.voxels_metadata.shape
        expanded_slicing = VoxelsAccessor._expand_slicing(slicing, shape)
        explicit_slicing = VoxelsAccessor._explicit_slicing(expanded_slicing, shape)
        request_slicing, result_slicing = self._determine_request_slicings(explicit_slicing, shape)

        start = map( lambda s: s.start, request_slicing )
        stop = map( lambda s: s.stop, request_slicing )

        retrieved_volume = self.get_ndarray(start, stop)
        return retrieved_volume[result_slicing]

    def __setitem__(self, slicing, array_data):
        """
        Implement convenient numpy-like slicing syntax for overwriting regions of a DVID volume.

        Limitations:
            Unlike the __getitem__ syntax implemented above, we do NOT permit arbitrary slicing.
            Instead, only "dense" subvolumes may be written.  
            That is, you must include all channels, and your slices may not include a step size.
        
        Examples:

            .. code-block:: python            

                connection = httplib.HTTPConnection( "localhost:8000" ) 
                v = VoxelsAccessor( connection, uuid=abc123, data_name='my_3d_rgb_volume' )
            
                # Overwrite the third z-slice
                v[...,2] = a
                
                # Forbidden: attempt to stepped slices
                v[...,0:10:2] = a # Error!
                
                # Forbidden: attempt to write only a subset of channels (the first axis)
                v[1,...] = green_data # Error!
        """
        shape = self.voxels_metadata.shape
        expanded_slicing = VoxelsAccessor._expand_slicing(slicing, shape)
        explicit_slicing = VoxelsAccessor._explicit_slicing(expanded_slicing, shape)
        request_slicing, result_slicing = self._determine_request_slicings(explicit_slicing, shape)

        # We only support pushing pure subvolumes, that is:
        # nothing that would require fetching a volume, changing it, and pushing it back.
        for s in result_slicing:
            if isinstance(s, slice):
                assert s.step is None, \
                    "You can't use step-slicing when pushing data back to DVID."
        assert result_slicing[0] == slice( 0, shape[0] ), \
            "When pushing a subvolume to DVID, you must include all channels, not a subset of them."

        start = map( lambda s: s.start, request_slicing )
        stop = map( lambda s: s.stop, request_slicing )

        ## This assertion is omitted because users are allowed to expand the size of 
        ##   the remote volume implicitly by simply giving it more data
        ##assert not (numpy.array(stop) > shape).any(), \
        ##    "Can't write data outside the bounds of the remote volume. "\
        ##    "Volume shape is {}, but you are attempting to write to start={},stop={}"\
        ##    "".format( shape, start, stop )

        slicing_shape = numpy.array(stop) - start
        assert isinstance(array_data, numpy.ndarray), \
            "Only array data can be posted.  Broadcasting of scalars, etc. not supported."
        assert numpy.prod(array_data.shape) == numpy.prod(slicing_shape), \
            "Provided data does not match the shape of the slicing:"\
            "data has shape {}, slicing {} has shape: {}"\
            "".format( array_data.shape, slicing, slicing_shape )

        self.post_ndarray(start, stop, array_data)

    @classmethod
    def _determine_request_slicings(cls, full_slicing, shape):
        """
        Determine the slicings to use for 
        (1) requesting the volume from DVID and 
        (2) extracting the specific pixels from the requested volume.
        """
        # Convert singletons axes (which would reduce the dimensionality of the result)
        #  into start:stop axes, but keep them as singletons in the result_slicing
        request_slicing = []
        result_slicing = []
        for s in full_slicing:
            if isinstance(s, slice):
                request_slicing.append( s )
                result_slicing.append( slice( 0, s.stop-s.start, s.step ) )
            else:
                request_slicing.append( slice(s, s+1) )
                result_slicing.append(0)

        # First dimension is channel, which we must request in full.
        request_slicing[0] = slice(0, shape[0])
        result_slicing[0] = full_slicing[0]

        return tuple(request_slicing), tuple(result_slicing)
        
    @classmethod
    def _explicit_slicing(cls, slicing, shape):
        """
        Replace all slice(None) items in the given 
        slicing with explicit start/stop coordinates using the given shape.
        """
        explicit_slicing = []
        for slc, maxstop in zip(slicing, shape):
            if not isinstance(slc, slice):
                explicit_slicing.append(slc)
            else:
                start, stop, step = slc.start, slc.stop, slc.step
                if start is None:
                    start = 0
                if stop is None:
                    stop = maxstop
                explicit_slicing.append( slice(start, stop, step) )
        return explicit_slicing        
    
    @classmethod
    def _expand_slicing(cls, s, shape):
        """
        Args:
            s: Anything that can be used as a numpy array index:
               - int
               - slice
               - Ellipsis (i.e. ...)
               - Some combo of the above as a tuple or list
            
            shape: The shape of the array that will be accessed
            
        Returns:
            A tuple of length N where N=len(shape)
            slice(None) is inserted in missing positions so as not to change the meaning of the slicing.
            e.g. if shape=(1,2,3,4,5):
                0 --> (0,:,:,:,:)
                (0:1) --> (0:1,:,:,:,:)
                : --> (:,:,:,:,:)
                ... --> (:,:,:,:,:)
                (0,0,...,4) --> (0,0,:,:,4)            
        """
        if type(s) == list:
            s = tuple(s)
        if type(s) != tuple:
            # Convert : to (:,), or 5 to (5,)
            s = (s,)
    
        # Compute number of axes missing from the slicing
        if len(shape) - len(s) < 0:
            assert s == (Ellipsis,) or s == (slice(None),), \
                "Slicing must not have more elements than the shape, except for [:] and [...] slices"
    
        # Replace Ellipsis with (:,:,:)
        if Ellipsis in s:
            ei = s.index(Ellipsis)
            s = s[0:ei] + (len(shape) - len(s) + 1)*(slice(None),) + s[ei+1:]
    
        # Shouldn't be more than one Ellipsis
        assert Ellipsis not in s, \
            "illegal slicing: found more than one Ellipsis"

        # Append (:,) until we get the right length
        s += (len(shape) - len(s))*(slice(None),)
        
        # Special case: we allow [:] and [...] for empty shapes ()
        if shape == ():
            s = ()
        
        return s

class RoiMaskAccessor(VoxelsAccessor):
    """
    Special subclass of VoxelsAccessor that can be used to access ROI data as a voxels type.
    
    - doesn't request voxels metadata (which isn't possible for roi data)
    - doesn't specify/need a fixed shape/minindex for the volume.
    - Axis order is HARD-CODED as CXYZ
    - dtype is HARD-CODED as uint8
    - uses the 'mask' access type in the REST query instead of 'raw'
    """
    
    def __init__(self, connection, uuid, data_name, *args, **kwargs):
        """
        Create a new VoxelsAccessor with all the same properties as the current instance, 
        except that it accesses a roi mask volume.
        """
        # Create default mask metadata.
        mask_metadata = {}
        mask_metadata["Properties"] = { "Values" : [ { "DataType" : "uint8", "Label": "roi-mask" } ] }

        # For now, we hardcode XYZ order
        # The size/offset are left as None, because that doesn't apply to ROI data.
        default_axis_info = { "Label": "", "Resolution": 1, "Units": "", "Size": 0, "Offset" : 0 }
        mask_metadata["Axes"] = [copy.copy(default_axis_info),
                                 copy.copy(default_axis_info),
                                 copy.copy(default_axis_info)]
        mask_metadata["Axes"][0]["Label"] = "X"
        mask_metadata["Axes"][1]["Label"] = "Y"
        mask_metadata["Axes"][2]["Label"] = "Z"
        
        assert '_metadata' not in kwargs or kwargs['_metadata'] is None
        kwargs['_metadata'] = VoxelsMetadata(mask_metadata)
        
        assert '_access_type' not in kwargs or kwargs['_access_type'] is None        
        kwargs['_access_type'] = 'mask'

        # Init base class with pre-formed metadata instead of querying for it.
        super(RoiMaskAccessor, self).__init__( connection, uuid, data_name, *args, **kwargs )
