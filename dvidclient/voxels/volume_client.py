import voxels

class VolumeClient(object):
    """
    Http client for retrieving a cutout volume from a DVID server.
    An instance of VolumeClient is capable of retrieving data from only one remote data volume.
    To retrieve data from multiple remote volumes, instantiate multiple VolumeClient objects.
    """
    def __init__(self, connection, uuid, data_name):
        """
        uuid: The node uuid
        data_name: The name of the volume
        """
        self.uuid = uuid
        self.data_name = data_name
        self._connection = connection

        # Request this volume's metadata from DVID
        self.volume_metadata = voxels.get_metadata( self._connection, uuid, data_name )

    def get_ndarray( self, start, stop ):
        return voxels.get_ndarray( self._connection, self.uuid, self.data_name, self.volume_metadata, start, stop )

    def post_ndarray( self, start, stop, new_data ):
        return voxels.post_ndarray( self._connection, self.uuid, self.data_name, self.volume_metadata, start, stop, new_data )
