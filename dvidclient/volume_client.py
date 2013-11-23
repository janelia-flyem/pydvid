from httplib import HTTPConnection

import numpy
import vigra

from volume_metainfo import parse_meta_info_from_json

import logging
logger = logging.getLogger(__name__)

class DvidVolume(object):
    STREAM_CHUNK_SIZE = 1000

    def decode_to_vigra_array(self, stream, metainfo, roi_shape):
        """
        Decode the info in the given stream to a vigra.VigraArray.
        """
        # Vigra is finicky about the integer types we give it in the shape field
        roi_shape = tuple( map(int, roi_shape) )
        
        # Note that dvid uses fortran order indexing
        a = vigra.VigraArray( roi_shape,
                              dtype=metainfo.dtype,
                              axistags=metainfo.axistags,
                              order='F' )
        buf = numpy.getbuffer(a)
        
        # Read data from the stream in chunks
        remaining_bytes = len(buf)
        while remaining_bytes > 0:
            next_chunk_bytes = min( remaining_bytes, self.STREAM_CHUNK_SIZE )
            buf[len(buf)-remaining_bytes:len(buf)-(remaining_bytes-next_chunk_bytes)] = stream.read( next_chunk_bytes )
            remaining_bytes -= next_chunk_bytes
        return a

    def __init__(self, hostname, uuid, dataset_name):
        # Open a connection to the server
        self.hostname = hostname
        self.uuid = uuid
        self.dataset_name = dataset_name
        connection = HTTPConnection(hostname)
        self._connection = connection
        rest_query = "/api/node/{uuid}/{dataset_name}/info".format( uuid=uuid, dataset_name=dataset_name )
        connection.request( "GET", rest_query )
        
        response = connection.getresponse()
        if response.status != 200:
            raise Exception( "Error in response to metainfo query: {}, {}".format( response.status, response.reason ) )

        self.metainfo = parse_meta_info_from_json( response.read() )
            
    def retrieve_subvolume(self, start, stop):
        start = numpy.asarray(start)
        stop = numpy.asarray(stop)
        shape = self.metainfo.shape

        assert len(start) == len(stop) == len(shape), \
            "start/stop/shape mismatch: {}/{}/{}".format( start, stop, shape )
        assert (start < stop).all(), "Invalid start/stop: {}/{}".format( start, stop )
        assert (start >= 0).all(), "Invalid start: {}".format( start )
        assert (start < shape).all(), "Invalid start/shape: {}/{}".format( start, shape )
        assert (stop <= shape).all(), "Invalid stop/shape: {}/{}".format( stop, shape )
        
        roi_shape = stop - start
        roi_shape_str = "_".join( map(str, roi_shape) )
        start_str = "_".join( map(str, start) )
        
        num_dims = len(self.metainfo.shape)
        dims_string = "_".join( map(str, range(num_dims-1) ) )
        rest_query = "/api/node/{uuid}/{dataset_name}/{dims_string}/{roi_shape_str}/{start_str}/nd-data"\
                     "".format( uuid=self.uuid, 
                                dataset_name=self.dataset_name, 
                                dims_string=dims_string, 
                                roi_shape_str=roi_shape_str, 
                                start_str=start_str )
        self._connection.request( "GET", rest_query )
        response = self._connection.getresponse()
        response.getheaders()
        if response.status != 200:
            raise Exception( "Error in response to subvolume query: {}, {}".format( response.status, response.reason ) )
        return self.decode_to_vigra_array( response, self.metainfo, roi_shape )

if __name__ == "__main__":
    import h5py
    def test_volume(hostname, h5filename, h5group, h5dataset, start, stop):
        """
        hostname: The dvid server host
        h5filename: The h5 file to compare against
        h5group: The hdf5 group, also used as the uuid of the dvid dataset
        h5dataset: The dataset name, also used as the name of the dvid dataset
        start, stop: The bounds of the cutout volume to retrieve from the server. FORTRAN ORDER.
        """
        # Retrieve from server
        dvid_vol = DvidVolume( hostname, uuid=h5group, dataset_name=h5dataset )
        subvol = dvid_vol.retrieve_subvolume( start, stop )

        # Retrieve from file
        slicing = [ slice(x,y) for x,y in zip(start, stop) ]
        slicing = tuple(reversed(slicing))
        with h5py.File(h5filename, 'r') as f:
            expected_data = f[h5group][h5dataset][slicing]

        # Compare.
        assert ( subvol.view(numpy.ndarray) == expected_data.transpose() ).all(),\
            "Data from server didn't match data from file!"

    filename = "/magnetic/gigacube.h5"
    test_volume( "localhost:8000", filename, "volume", "data", (0,0,5,0,0), (1,100,20,10,1) )
    print "TEST COMPLETE"

    def test_metainfo_parsing():
            meta_string = """
        {
            "axes": [
                {
                    "label": "X",
                    "resolution": 3.1,
                    "units": "nanometers",
                    "size": 100
                },{
                    "label": "Y",
                    "resolution": 3.1,
                    "units": "nanometers",
                    "size": 200
                },{
                    "label": "Z",
                    "resolution": 40,
                    "units": "nanometers",
                    "size": 400
                }
            ],
            "values": [
                {
                    "type": "uint8",
                    "label": "intensity-R"
                },
                {
                    "type": "uint8",
                    "label": "intensity-G"
                },
                {
                    "type": "uint8",
                    "label": "intensity-B"
                }
            ]
        }
        """            
        
            shape, dtype, tags = parse_meta_info_from_json(meta_string)
            assert shape == (3, 100,200,400), "Wrong shape: {}".format( shape )
            assert dtype == numpy.uint8
            assert [tag.key for tag in tags] == ['c', 'x', 'y', 'z']
            assert tags['x'].resolution == 3.1
            assert tags['y'].resolution == 3.1
            assert tags['z'].resolution == 40

        
        
    