
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""


# Maintain a tree structure from nodes that supports navigations


from abc import ABC, abstractmethod

import edge_lake.generic.process_status as process_status
import edge_lake.api.plc_utils as plc_utils

from edge_lake.generic import utils_json


try:
    from opcua import Client, ua
    from opcua.ua import BrowseDirection

    node_class_name_ = {
        ua.NodeClass.Object: "object",
        ua.NodeClass.Variable: "variable",
        ua.NodeClass.Method: "method",
        ua.NodeClass.ObjectType: "objecttype",
        ua.NodeClass.VariableType: "variabletype",
        ua.NodeClass.ReferenceType: "referencetype",
        ua.NodeClass.DataType: "datatype",
        ua.NodeClass.View: "view"
    }
except:
    opcua_installed_ = False
else:
    opcua_installed_ = True


import edge_lake.generic.utils_print as utils_print

# --------------------------------------------------------------------------
# A wrapper node for the OPCUA NODE
# --------------------------------------------------------------------------
class ParsedNode(ABC):
    def __init__(self, index, source_node, parent_node):
        self.index = index      # The ID of the child (starting at 0 for the first child)
        self.source_node = source_node  # The OPCUA Node
        self.children = []
        self.parent = parent_node

    def set_children(self, parent_node):
        return False

    def get_name(self):
        return None

    def get_id(self):
        return None

    def get_namespace(self):
        return 0


    def get_children(self):
        return self.children

    def print_info(self, status, file_handle, validate_value, read_status, attributes, depth):
        '''
        :param depth: Current depth in the tree (used for indentation)
        '''
        return process_status.SUCCESS
    def output_policy(self, status, file_handle, depth, dbms_name, new_table_id, bchain_insert, table_insert):
        '''
        :param depth: Current depth in the tree (used for indentation)
        '''
        return process_status.SUCCESS
    def output_path(self, status, file_handle, depth):
        '''
        :param depth: Current depth in the tree (used for indentation)
        '''
        return process_status.SUCCESS

    def get_node_key_type(self):
        '''
        The data type of the key to retrieve the node: Numeric  (e.g., i=2257) String  (e.g., s=TemperatureSensor1)
        '''

        return "Unknown"

    def get_node_data_type(self):
        '''
        The data type of the value
        '''

        return None

    def get_node_class(self,  node_class_name = None):
        '''
        :param node_class_name: the human-readable name for the given node_class (optional)
        '''
        return None

    def get_node_value(self):
        '''
        Get the current Node Value
        '''

        return "Unknown"
    def get_node_identifier(self):
        '''
        Get the identifier of the node
        '''

        return None
# --------------------------------------------------------------------------
# Extend Parse Node
# --------------------------------------------------------------------------
class OpcuaNode (ParsedNode):

    nodes_key_types_ = {
        "Numeric":      "i",
        "String":       "s",
        "Guid":         "g",
        "ByteString":   "b",
        "FourByte":     "i",    # subset of Numeric
    }

    # --------------------------------------------------------------------------
    # Get the OPCUA children and organize in the wrapper
    # --------------------------------------------------------------------------
    def set_children(self, parent_node):
        try:
            source_children = self.source_node.get_children()

            for index, child in enumerate(source_children):
                self.children.append( OpcuaNode(index, child, parent_node) )
        except:
            ret_val = False
        else:
            ret_val = True
        return ret_val


    def get_name(self):
        return self.source_node.get_display_name().Text

    def get_namespace(self):
        return self.source_node.nodeid.NamespaceIndex

    def get_id(self):
        identifier_type_name = self.source_node.nodeid.NodeIdType.name  # This returns the type (e.g., Numeric, String)
        identifier_type = OpcuaNode.nodes_key_types_.get(identifier_type_name, "?")

        identifier_value = self.source_node.nodeid.Identifier

        return f"{identifier_type}={identifier_value}"

    def get_node_value(self):
        '''
        Get the current Node Value
        '''
        try:
            value = self.source_node.get_value()
        except:
            value = None
        return value

    # -----------------------------------------------------------------------------------
    # Print a policy representing the node
    # -----------------------------------------------------------------------------------
    def output_policy(self, status, file_handle, depth, dbms_name, new_table_id, bchain_insert, table_insert):
        '''
        :param file_handle: if data written to file
        :param depth: Current depth in the tree (used for indentation)
        :param dbms_name: the human-readable name for the given database
        :param new_table_id: the id of the new table (a sequential number based on existing policies)
        :param bchain_insert: A user provided blockchain insert statement
        :param table_insert: A CREATE STMT to describe the table that contains the tag. This policy is pushed to the blockchain
        '''

        new_policy = {}

        new_policy["dbms"] = dbms_name
        table_name =  f"t{new_table_id}"
        new_policy["table"] = table_name
        new_policy["protocol"] = "opcua"


        path = []
        ret_val = process_status.SUCCESS
        node = self.source_node
        try:
            # Must exists
            new_policy["class"] = self.get_node_class()
            new_policy["ns"] = self.get_namespace()
            node_iid = self.get_id()
            if len(node_iid) > 2 and node_iid[:2] == 'i=':
                # always starts with "i="
                new_policy["node_iid"] = node_iid[2:]

        except Exception as e:
            # This part can't fail
            status.add_error(f"OPCUA: Failed to retrieve path: {e}")
            ret_val = process_status.Failed_opcua_process
        else:
            try:
                # Optional
                new_policy["node_sid"] = self.get_name()
            except:
                if not new_policy["node_iid"]:
                    # Either iid or sid or both needs to exist
                    status.add_error(f"OPCUA: Failed to retrieve node_iid and node name")
                    ret_val = process_status.Failed_opcua_process
            else:
                try:
                    data_type = self.get_node_data_type().name
                    new_policy["datatype"] = data_type      # The OPCUA data type
                except:
                    pass

                try:
                    counter = 0
                    while True:
                        counter += 1
                        browse_name = node.get_browse_name().Name
                        if counter == 2:
                            # Add the parent
                            new_policy["parent"] = browse_name

                        path.insert(0, browse_name)
                        parent = node.get_parent()
                        if not parent:
                            break
                        if parent.nodeid.Identifier == 84:  # RootFolder
                            path.insert(0, "Root")
                            break
                        node = parent

                except:
                    pass


        if not ret_val:
            if table_insert:
                # WRITE THE "CREATE TABLE" POLICY with the AnyLog data type

                # Per table the following are replaced:
                # [DBMS_NAME] with the DBMS name
                # [TABLE_NAME] with the table name
                # [DATA_TYPE] with the data type
                tag_table = plc_utils.create_tag_table(table_insert, dbms_name, table_name, data_type)
            else:
                tag_table = None


            new_policy["path"] = "/".join(path)
            tag_policy = {
                "tag": new_policy
            }

            # Output to stdout or file
            ret_val = plc_utils.output_policies(status, file_handle, tag_policy, bchain_insert, tag_table)

        return ret_val

    # -----------------------------------------------------------------------------------
    # Print the node full path
    # -----------------------------------------------------------------------------------
    def output_path(self, status, file_handle, depth=0):
        '''
        :param file_handle: if data written to file
        :param depth: Current depth in the tree (used for indentation)
        '''

        path = []
        ret_val = process_status.SUCCESS
        node = self.source_node
        try:
            while True:
                browse_name = node.get_browse_name().Name

                path.insert(0, browse_name)
                parent = node.get_parent()
                if not parent:
                    break
                if parent.nodeid.Identifier == 84:  # RootFolder
                    path.insert(0, "Root")
                    break
                node = parent

        except Exception as e:
            status.add_error(f"OPCUA: Failed to retrieve path: {e}")
            ret_val = process_status.Failed_opcua_process

        else:
            info_str = "\n"+"/".join(path)
            if file_handle:
                if not file_handle.append_data(info_str):
                    status.add_error(f"OPCUA: Failed to write into output file: {file_handle.get_file_name()}")
                    ret_val = process_status.File_write_failed
            else:
                utils_print.output(info_str, False)  # Print the node attribute name and value

        return ret_val

    # -----------------------------------------------------------------------------------
    # Print the node info and attributes
    # -----------------------------------------------------------------------------------
    def print_info(self, status, file_handle, validate_value, read_status, attributes = None, depth=0):
        '''
        :param file_handle: if data written to file
        :param validate_value: If True - the value is read to identify success or failure
        :param read_status: The read status
        :param attributes: specific attributes include in the output
        :param depth: Current depth in the tree (used for indentation)
        '''




        indentation = ' ' * depth * 2

        node_class = self.get_node_class()


        read_info = f', validate={"success" if read_status else "FAILURE"}' if validate_value else ""

        info_str = f"\r\n{indentation}[{node_class}], (ns={self.get_namespace()};{self.get_id()}, name={self.get_name()}, datatype={self.get_node_data_type()}{read_info})"

        if file_handle:
            if not file_handle.append_data(info_str):
                status.add_error(f"OPCUA: Failed to write into output file: {file_handle.get_file_name()}")
                return process_status.File_write_failed
        else:
            utils_print.output(info_str, False)     # Print the node Info

        # Print the attributes
        if attributes:
            indentation = ' ' * (depth +1) * 2 + "--> "
            if attributes[0] == '*':
                # print all
                name_val = vars(self.source_node.nodeid).items()        # The attribute names in the object
                for name, value in name_val:
                    info_str = f"\r\n{indentation}[{name} : {value}]"
                    if file_handle:
                        if not file_handle.append_data(info_str):
                            status.add_error(f"OPCUA: Failed to write into output file: {file_handle.get_file_name()}")
                            return process_status.File_write_failed
                    else:
                        utils_print.output(info_str, False)  # Print the node attribute name and value


        return process_status.SUCCESS
    # --------------------------------------------------------------------------
    # Get the data type of the value
    # --------------------------------------------------------------------------
    def get_node_data_type(self):
        '''
        The data type of the value
        '''
        try:
            data_type  = self.source_node.get_data_type_as_variant_type()  # Returns an enum variant type
        except:
            data_type = None
        return data_type

    # --------------------------------------------------------------------------
    # Get the OPCUA key Type
    '''
    #     
    # Value	Type	Description
    # 0	    Numeric	The identifier is a numeric value (e.g., i=2257).
    # 1	    String	The identifier is a string value (e.g., s=TemperatureSensor1).
    # 2	    GUID	The identifier is a globally unique identifier (e.g., g=12345678-1234-1234).
    # 3	    Opaque (ByteString)	The identifier is a binary value, often used for complex or vendor-specific IDs.
    '''
    # --------------------------------------------------------------------------
    def get_node_key_type(self):

        try:
            node_id_type = self.source_node.nodeid.NodeIdType.name
        except:
            node_id_type = None
        return node_id_type
    # --------------------------------------------------------------------------
    # Get the OPCUA children and organize in the wrapper
    # --------------------------------------------------------------------------
    def get_node_class(self):
        '''
        :param node_class_name: the human-readable name for the given node_class (optional)
        '''

        node_class = self.source_node.get_node_class()  # Retrieve the node_class
        class_name = node_class_name_.get(node_class, "Unknown")

        return class_name
    # --------------------------------------------------------------------------
    # Get the OPCUA node identifier - Name Space + ID
    # --------------------------------------------------------------------------
    def get_node_identifier(self):
        '''
        Get the identifier of the node - i.e. ns=2;i=1
        '''

        return f"ns={self.get_namespace()};{self.get_id()}"