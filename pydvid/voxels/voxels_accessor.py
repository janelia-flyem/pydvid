import voxels

class VoxelsAccessor(object):
    """
    Http client for retrieving a voxels volume data from a DVID server.
    An instance of VoxelsAccessor is capable of retrieving data from only one remote data volume.
    To retrieve data from multiple remote volumes, instantiate multiple DvidClient objects.
    
    **TODO**:
    
    * Allow users to provide a pre-allocated array when requesting data
    """
    def __init__(self, connection, uuid, data_name):
        """
        :param uuid: The node uuid
        :param data_name: The name of the volume
        """
        self.uuid = uuid
        self.data_name = data_name
        self._connection = connection

        # Request this volume's metadata from DVID
        self.voxels_metadata = voxels.get_metadata( self._connection, uuid, data_name )

    def get_ndarray( self, start, stop ):
        """
        Request the subvolume specified by the given start and stop pixel coordinates.
        """
        return voxels.get_ndarray( self._connection, self.uuid, self.data_name, self.voxels_metadata, start, stop )

    def post_ndarray( self, start, stop, new_data ):
        """
        Overwrite subvolume specified by the given start and stop pixel coordinates with new_data.
        """
        return voxels.post_ndarray( self._connection, self.uuid, self.data_name, self.voxels_metadata, start, stop, new_data )

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
        full_slicing = VoxelsAccessor._expand_slicing(slicing, shape)
        request_slicing, result_slicing = self._determine_request_slicings(full_slicing, shape)

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
