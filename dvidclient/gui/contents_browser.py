import collections

from PyQt4.QtGui import QDialog, QVBoxLayout, QGroupBox, QTreeWidget, \
                        QTreeWidgetItem, QSizePolicy, QListWidget, QListWidgetItem, \
                        QDialogButtonBox, QLineEdit, QLabel, QMessageBox
from PyQt4.QtCore import Qt, QStringList, QSize

from dvidclient.volume_client import VolumeClient

class ContentsBrowser(QDialog):
    """
    Displays the contents of a DVID server, listing all datasets and the volumes/nodes within each dataset.
    The user's selected dataset, volume, and node can be accessed via the `get_selection()` method.
    
    If the dialog is constructed with mode='specify_new', then the user is asked to provide a new data name, 
    and choose the dataset and node to which it will belong. 
    """
    def __init__(self, hostname, mode='select_existing', parent=None):
        """
        Constructor.  May raise socket.error if host can't be found.
        
        hostname: The dvid server hostname, e.g. "localhost:8000"
        mode: Either 'select_existing' or 'specify_new'
        parent: The parent widget.
        """
        super( ContentsBrowser, self ).__init__(parent)
        self._current_dset = None
        self._hostname = hostname
        self._mode = mode
        
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
        node_uuid = self._get_selected_node()
        dset_index, data_name = self._get_selected_data()
        
        if self._mode == "specify_new":
            data_name = str( self._new_data_edit.text() )
        
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
        node_listwidget.itemSelectionChanged.connect( self._update_display )

        node_layout = QVBoxLayout(self)
        node_layout.addWidget( node_listwidget )
        node_groupbox = QGroupBox("Nodes", parent=self)
        node_groupbox.setLayout( node_layout )

        new_data_edit = QLineEdit(parent=self)
        new_data_edit.textEdited.connect( self._update_display )
        full_url_label = QLabel(parent=self)
        full_url_label.setSizePolicy( QSizePolicy.Preferred, QSizePolicy.Maximum )

        new_data_layout = QVBoxLayout(self)
        new_data_layout.addWidget( new_data_edit )
        new_data_groupbox = QGroupBox("New Data Volume", parent=self)
        new_data_groupbox.setLayout( new_data_layout )
        new_data_groupbox.setSizePolicy( QSizePolicy.Preferred, QSizePolicy.Maximum )

        buttonbox = QDialogButtonBox( Qt.Horizontal, parent=self )
        buttonbox.setStandardButtons( QDialogButtonBox.Ok | QDialogButtonBox.Cancel )
        buttonbox.accepted.connect( self.accept )
        buttonbox.rejected.connect( self.reject )

        layout = QVBoxLayout(self)
        layout.addWidget( data_groupbox )
        layout.addWidget( node_groupbox )
        if self._mode == "specify_new":
            layout.addWidget( new_data_groupbox )
        else:
            new_data_groupbox.hide()
        layout.addWidget( full_url_label )
        layout.addWidget( buttonbox )
        self.setLayout(layout)

        # Save instance members
        self._data_treewidget = data_treewidget
        self._node_listwidget = node_listwidget
        self._new_data_edit = new_data_edit
        self._full_url_label = full_url_label
        self._buttonbox = buttonbox

    def sizeHint(self):
        return QSize(700, 500)

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
                if self._mode == 'specify_new':
                    # If we're in specify_new mode, only the dataset parent items are selectable.
                    flags = data_item.flags()
                    flags &= ~Qt.ItemIsSelectable
                    flags &= ~Qt.ItemIsEnabled
                    data_item.setFlags( flags )
        
        # Expand everything
        self._data_treewidget.expandAll()
        
        # Select the first item by default.
        if self._mode == "select_existing":
            first_item = self._data_treewidget.topLevelItem(0).child(0)
        else:
            first_item = self._data_treewidget.topLevelItem(0)            
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
        
        self._update_display()

    def _populate_node_list(self, dataset_index):
        """
        Replace the contents of the node list widget 
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
        self._update_display()

    def _get_selected_node(self):
        selected_items = self._node_listwidget.selectedItems()
        if not selected_items:
            return None
        selected_node_item = selected_items[0]
        node_item_data = selected_node_item.data(Qt.UserRole)
        return str( node_item_data.toString() )
        
    def _get_selected_data(self):
        selected_data_item = self._data_treewidget.selectedItems()[0]
        data_item_data = selected_data_item.data(0, Qt.UserRole).toPyObject()
        if selected_data_item:
            dset_index, data_name = data_item_data
        else:
            dset_index = data_name = None
        return dset_index, data_name
    
    def _update_display(self):
        """
        Update the path label to reflect the user's currently selected uuid and new volume name.
        """
        dset_index, dataname, node_uuid = self.get_selection()
        full_path = "http://{hostname}/api/node/{uuid}/{dataname}"\
                    "".format( hostname=self._hostname, uuid=node_uuid, dataname=dataname )
        self._full_url_label.setText( full_path )
        
        ok_button = self._buttonbox.button( QDialogButtonBox.Ok )
        ok_button.setEnabled( dataname != "" )

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
    parser.add_argument("--mode", choices=["select_existing", "specify_new"], default="select_existing")
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
        #sys.argv.append("--mode=specify_new")
        sys.argv.append("localhost:8000")

    parsed_args = parser.parse_args()
    
    server_proc = None
    if parsed_args.mock_server_hdf5:
        from mockserver.h5mockserver import H5MockServer
        hostname, port = parsed_args.hostname.split(":")
        server_proc = H5MockServer.create_and_start( parsed_args.mock_server_hdf5,
                                                     hostname,
                                                     int(port),
                                                     same_process=False,
                                                     disable_server_logging=False )
    
    app = QApplication([])
    browser = ContentsBrowser(parsed_args.hostname, parsed_args.mode)

    try:
        if browser.exec_() == QDialog.Accepted:
            print "The dialog was accepted with result: ", browser.get_selection()
        else:
            print "The dialog was rejected."
    finally:
        if server_proc:
            server_proc.terminate()
