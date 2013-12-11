import collections

from PyQt4.QtGui import QDialog, QVBoxLayout, QGroupBox, QTreeWidget, \
                        QTreeWidgetItem, QSizePolicy, QListWidget, QListWidgetItem, \
                        QDialogButtonBox
from PyQt4.QtCore import Qt, QStringList

from dvidclient.volume_client import VolumeClient

class ContentsBrowser(QDialog):
    """
    Displays the contents of a DVID server, listing all datasets and the volumes/nodes within each dataset.
    The user's selected dataset, volume, and node can be accessed via the `get_selection()` method. 
    """
    def __init__(self, hostname, parent=None):
        """
        parent: The parent widget.
        hostname: The dvid server hostname, e.g. "localhost:8000"
        """
        super( ContentsBrowser, self ).__init__(parent)
        self._current_dset = None
        self._hostname = hostname
        
        # Create the UI
        self.setWindowTitle( self._hostname )
        self._init_layout()
        
        # Query the server
        self._datasets_info = VolumeClient.query_datasets_info(hostname)        
        self._populate_datasets_tree()

    VolumeSelection = collections.namedtuple( "VolumeSelection", "dataset_index data_name node_uuid" )
    def get_selection(self):
        """
        Get the user's current (or final) selection.
        Returns a VolumeSelection tuple.
        """
        selected_node_item = self._node_listwidget.selectedItems()[0]
        node_item_data = selected_node_item.data(Qt.UserRole)
        node_uuid = str( node_item_data.toString() )

        selected_data_item = self._data_treewidget.selectedItems()[0]
        data_item_data = selected_data_item.data(0, Qt.UserRole).toPyObject()
        if selected_data_item:
            dset_index, data_name = data_item_data
        else:
            dset_index = data_name = None

        return ContentsBrowser.VolumeSelection(dset_index, data_name, node_uuid)

    def _init_layout(self):
        """
        Create the GUI widgets (but leave them empty).
        """
        data_treewidget = QTreeWidget(parent=self)
        data_treewidget.setHeaderLabels( ["Data"] ) # TODO: Add type, shape, axes, etc.
        data_treewidget.setSizePolicy( QSizePolicy.Preferred, QSizePolicy.Preferred )
        data_treewidget.itemSelectionChanged.connect( self._handle_data_selection )

        data_layout = QVBoxLayout(self)
        data_layout.addWidget( data_treewidget )
        data_groupbox = QGroupBox("Data Volumes", parent=self)
        data_groupbox.setLayout( data_layout )
        
        node_listwidget = QListWidget(parent=self)
        node_listwidget.setSizePolicy( QSizePolicy.Preferred, QSizePolicy.Preferred )
        node_layout = QVBoxLayout(self)
        node_layout.addWidget( node_listwidget )
        node_groupbox = QGroupBox("Nodes", parent=self)
        node_groupbox.setLayout( node_layout )

        buttonbox = QDialogButtonBox( Qt.Horizontal, parent=self )
        buttonbox.setStandardButtons( QDialogButtonBox.Ok | QDialogButtonBox.Cancel )
        buttonbox.accepted.connect( self.accept )
        buttonbox.rejected.connect( self.reject )

        layout = QVBoxLayout(self)
        layout.addWidget( data_groupbox )
        layout.addWidget( node_groupbox )
        layout.addWidget( buttonbox )
        self.setLayout(layout)

        self._data_treewidget = data_treewidget
        self._node_listwidget = node_listwidget

    def _populate_datasets_tree(self):
        """
        Initialize the tree widget of datasets and volumes.
        """
        for dset_info in self._datasets_info["Datasets"]:
            dset_index = dset_info["DatasetID"]
            dset_name = str(dset_index) # FIXME when API is fixed
            dset_item = QTreeWidgetItem( self._data_treewidget, QStringList( dset_name ) )
            dset_item.setData( 0, Qt.UserRole, (dset_index, "") )
            for data_name in dset_info["DataMap"].keys():
                data_item = QTreeWidgetItem( dset_item, QStringList( data_name ) )
                data_item.setData( 0, Qt.UserRole, (dset_index, data_name) )
        
        # Expand everything
        self._data_treewidget.expandAll()
        
        # Select the first item by default.
        first_item = self._data_treewidget.topLevelItem(0).child(0)
        self._data_treewidget.setCurrentItem( first_item, 0 )

    def _handle_data_selection(self):
        """
        When the user clicks a new data item, respond by updating the node list.
        """
        item = self._data_treewidget.selectedItems()[0]
        item_data = item.data(0, Qt.UserRole).toPyObject()
        if not item_data:
            return
        dset_index, data_name = item_data
        if self._current_dset != dset_index:
            self._populate_node_list(dset_index)

    def _populate_node_list(self, dataset_index):
        """
        Replace the contentst of the node list widget 
        to show all the nodes for the currently selected dataset.
        """
        self._node_listwidget.clear()
        # For now, we simply show the nodes in sorted order, without respect to dag order
        all_uuids = sorted( self._datasets_info["Datasets"][dataset_index]["Nodes"].keys() )
        for node_uuid in all_uuids:
            node_item = QListWidgetItem( node_uuid, parent=self._node_listwidget )
            node_item.setData( Qt.UserRole, node_uuid )
        self._current_dset = dataset_index

        # Select the last one by default.
        last_row = self._node_listwidget.count() - 1
        last_item = self._node_listwidget.item( last_row )
        self._node_listwidget.setCurrentItem( last_item )
    
if __name__ == "__main__":
    """
    This main section permits simple command-line control.
    usage: contents_browser.py [-h] [--mock-server-hdf5=MOCK_SERVER_HDF5] hostname:port
    
    If --mock-server-hdf5 is provided, the mock server will be launched with the provided hdf5 file.
    Otherwise, the DVID server should already be running on the provided hostname.
    """
    import sys
    import argparse
    from PyQt4.QtGui import QApplication

    parser = argparse.ArgumentParser()
    parser.add_argument("--mock-server-hdf5", required=False)
    parser.add_argument("hostname", metavar="hostname:port")
    
    DEBUG = True
    if DEBUG and len(sys.argv) == 1:
        # default debug args
        parser.print_help()
        print ""
        print "*******************************************************"
        print "No args provided.  Starting with special debug args...."
        print "*******************************************************"
        sys.argv.append("--mock-server-hdf5=/magnetic/mockdvid_gigacube.h5")
        sys.argv.append("localhost:8000")

    parsed_args = parser.parse_args()
    
    server_proc = None
    if parsed_args.mock_server_hdf5:
        from mockserver.h5mockserver import H5MockServer
        hostname, port = parsed_args.hostname.split(":")
        server_proc = H5MockServer.start( parsed_args.mock_server_hdf5,
                                          hostname,
                                          int(port),
                                          same_process=False,
                                          disable_server_logging=False )
    
    app = QApplication([])
    browser = ContentsBrowser(parsed_args.hostname)

    try:
        if browser.exec_() == QDialog.Accepted:
            print "The dialog was accepted with result: ", browser.get_selection()
        else:
            print "The dialog was rejected."
    finally:
        if server_proc:
            server_proc.terminate()
