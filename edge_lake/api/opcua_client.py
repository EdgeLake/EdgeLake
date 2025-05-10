"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

# Documentation: https://readthedocs.org/projects/python-opcua/downloads/pdf/latest/
# https://python-opcua.readthedocs.io/en/latest/_modules/opcua/client/ua_client.html
# https://github.com/FreeOpcUa/python-opcua/tree/master/examples

try:
    from opcua import Client, ua
except:
    opcua_installed_ = False
else:
    opcua_installed_ = True

import sys

import os
from contextlib import redirect_stdout, redirect_stderr

import edge_lake.generic.process_status as process_status
import edge_lake.generic.interpreter as interpreter
import edge_lake.api.struct_tree as struct_tree
import edge_lake.generic.utils_print as utils_print

import edge_lake.api.plc_utils as plc_utils

def test_lib_installed(status):
    if opcua_installed_:
        ret_val = process_status.SUCCESS
    else:
        status.add_error("Lib opcua not installed")
        ret_val = process_status.Failed_to_import_lib
    return ret_val

# ---------------------------------------------------------------------------------------
# Extract the opcua namespace
#
# Command:
#  get opcua namespace where url = opc.tcp://10.0.0.111:53530/OPCUA/SimulationServer
# ---------------------------------------------------------------------------------------
def opcua_namespace(status, io_buff_in, cmd_words, trace):
    if not opcua_installed_:
        status.add_error("Lib opcua not installed")
        return [process_status.Failed_to_import_lib, None]

    keywords = {"url":                      ("str",     True,   False,  True),      # OPCUA URL
                "user":                     ("str",     False,  False,  True),      # Username  (optional)
                "password":                 ("str",     False,  False,  True),      # Password (optional)
                }


    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return [ret_val, None]

    url = interpreter.get_one_value(conditions, "url")
    user = interpreter.get_one_value_or_default(conditions, "user", None)
    password = interpreter.get_one_value_or_default(conditions, "password", None)

    # Establish connection to the OPC-UA server
    connection = declare_connection(status=status, url=url, user=user, password=password)
    if not connection:
        return [process_status.Failed_OPC_CONNECT, None]

    try:
        # Get the namespace table
        namespace_array = connection.get_namespace_array()

        output_table = []
        for index, namespace_url in enumerate(namespace_array):
            output_table.append((index, namespace_url))
    except:
        errno, value = sys.exc_info()[:2]
        status.add_error(f'Failed to retrieve the Namespace Table (Error: {value})')
        ret_val = process_status.Failed_opcua_process
        output_txt = None
    else:
        output_txt = utils_print.output_nested_lists(output_table,  "OPCUA Namespace Table", ["Index", "Namespace URL"], True)
    finally:
        # Disconnect from the OPC-UA server after browsing
        disconnect_opcua(status=status, connection=connection)

    return [ret_val, output_txt]

# ---------------------------------------------------------------------------------------
# Extract the opcua tree structure
#
# Command:
#  get opcua struct where url = opc.tcp://10.0.0.111:53530/OPCUA/SimulationServer and output = stdout and user = use1 and password = my_password
#  get opcua struct where url = opc.tcp://10.0.0.111:53530/OPCUA/SimulationServer and output = stdout and limit = 10
#  get opcua struct where url = opc.tcp://10.0.0.111:53530/OPCUA/SimulationServer and output = stdout and depth = 1
#  get opcua struct where url = opc.tcp://10.0.0.111:53530/OPCUA/SimulationServer and output = stdout and attributes = * and limit = 10
#  get opcua struct where url = opc.tcp://10.0.0.111:53530/OPCUA/SimulationServer
#  get opcua struct where url = opc.tcp://10.0.0.111:53530/OPCUA/SimulationServer and output = stdout and node="ns=0;i=2257" and attributes = *
#  get opcua struct where url = opc.tcp://10.0.0.111:53530/OPCUA/SimulationServer and output = stdout and node="ns=6;s=MyObjectsFolder"
#  get opcua struct where url = opc.tcp://127.0.0.1:4840/freeopcua/server and format = path and limit = 100 and node = "ns=2;s=DeviceSet" and class = variable and output = !tmp_dir/my_file.out
#  get opcua struct where url = opc.tcp://127.0.0.1:4840/freeopcua/server and format = policy  and limit = 100 and node = "ns=2;s=DeviceSet" and class = variable and dbms = my_dbms and target = "local = true and master = !master_node" and output = !tmp_dir/my_file.out
'''
Filter sensor nodes:
node.data_type: Determines the data type of the node (like Int32, Float, etc.).
node.value_rank: -1 indicates a scalar value (suitable for monitoring sensor data).
node.access_level: Check if the AccessLevel allows reading (3 typically for read-only).
Identifying Updated Nodes:
Nodes with scalar ValueRank (-1) and appropriate DataType (like Int32, Float, etc.) are likely to be updated by sensors.
Nodes with proper AccessLevel that allows reading should also be monitored.
'''
# ---------------------------------------------------------------------------------------
def opcua_struct(status, io_buff_in, cmd_words, trace):

    if not opcua_installed_:
        status.add_error("Lib opcua not installed")
        return [process_status.Failed_to_import_lib, None]

    #                                                   Must    Add     Is
    #                                                   exists  Counter Unique
    keywords = {"url":                      ("str",     True,   False,  True),      # OPCUA URL
                "user":                     ("str",     False,  False,  True),      # Username  (optional)
                "password":                 ("str",     False,  False,  True),      # Password (optional)
                "output":                   ("str",     False,  False,  True),      # The type of output - stdout or file
                "append":                   ("bool",    False, False,   True),      # In file mode - append to the previous file (default)
                "type" :                    ("str",     False, False,   False),     # Type of nodes to consider: Object, Variable etc
                "limit":                    ("int",     False, False,   True),      # Max nodes to consider
                "attributes":               ("str",     False, False,   False),     # Attribute names to consider or * for all
                "node":                     ("str",     False, False,   True),      # get a different root by providing the node id: exampls: 'ns=0;i= i=84 or s=MyVariable
                "depth":                    ("int",     False, False,   True),      # Limit the depth of the traversal
                "class":                    ("str",     False, False,   False),     # classes to consider: i.e. object, variable
                "format":                   ("str",     False, False,   True),      # the type of output - "tree" (default), "get value", "run client"
                "frequency":                ("str",     False, False,    True),      # If output generates "run_client" - the frequency of the "run client" command
                "table":                    ("str",     False, False,    True),     # Table can be derived from the command - or from the policies that map tags to tables
                "dbms":                     ("str",     False, True,    True),      # If output generates "run_client" - the dbms name of the "run client" command
                "validate":                 ("bool",    False, False,   True),      # If output generates "run_client" - the dbms name of the "run client" command
                "target":                   ("str",     False, False,   True),     # The blockchain insert params - For policy insertions to the blockchain:    Example target = "local = true and master = !master_node"
                "schema":                   ("bool",    False, False,   True),     # generate the schema for the table representing the tags (create table insert)
                "name":                     ("str",     False, False,   True),      # A unique name for 'run opcua client' command
                # The blockchain insert params - For policy insertions to the blockchain:    Example target = "local = true and master = !master_node"
                }


    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return [ret_val, None]

    url = interpreter.get_one_value(conditions, "url")
    user = interpreter.get_one_value_or_default(conditions, "user", None)
    password = interpreter.get_one_value_or_default(conditions, "password", None)
    node_id = interpreter.get_one_value_or_default(conditions, "node", None)     # namespace + id - example: ns=2;i=1002
    output_format = interpreter.get_one_value_or_default(conditions, "format", "tree")
    validate_value = interpreter.get_one_value_or_default(conditions, "validate", False)

    valid_formats = ["tree", "path", "policy", "get_value", "run_client"]
    if not output_format in valid_formats:
        status.add_error("OPCUA Error: invalid output format, expected values are: " + ", ".join(valid_formats))
        return [process_status.ERR_command_struct, None]

    type_dict = {
        # A dictionary to count the number of nodes + number by type
        "total" : 0,        # Counter nodes visited
        "object" : 0,
        "variable" : 0,
        "method" : 0,
        "objecttype" : 0,
        "variabletype" : 0,
        "referencetype" : 0,
        "datatype" : 0,
        "view" : 0,
        "read_failure" : 0,
        "unknown" : 0,
    }

    # Establish connection to the OPC-UA server
    connection = declare_connection(status=status, url=url, user=user, password=password)
    if not connection:
        return [process_status.Failed_OPC_CONNECT, None]

    if not node_id:
        # get the root node
        root = get_root(status, connection)
    else:
        # take a different root
        if len(node_id) > 2 and node_id[0] == node_id[-1] and (node_id[0] == '"' or node_id[0] == "'"):
            node_id = node_id[1:-1]       # remove quotation
        root = get_node(status, connection, node_id)
    if not root:
        return [process_status.Failed_opcua_process, None]

    ret_val, navigation_info, file_handle = plc_utils.prep_plc_traversal(status, "opcua", conditions)

    if ret_val:
        return [ret_val, None]


    ret_val = navigate_tree(status, connection, root, 0, navigation_info, type_dict, validate_value)  # Start navigating from the root node


    # Disconnect from the OPC-UA server after browsing
    disconnect_opcua(status=status, connection=connection)


    output_txt = plc_utils.output_plc_struct(ret_val, "opcua", conditions, file_handle, url, output_format, navigation_info, type_dict)

    return [ret_val, output_txt]

# ---------------------------------------------------------------------------------------
# Get a single value for a node
# ---------------------------------------------------------------------------------------
def get_one_value(status, client, node_id):
    """
    Fetch the value of a specific node given its Node ID.
    :param status: thread status object
    :param client: OPC-UA client
    :param node_id: Node ID of the target node i.e. "ns=0;i=2257"
    """
    try:
        one_node = client.get_node(node_id)
        one_value = one_node.get_data_value()
    except:
        errno, value = sys.exc_info()[:2]
        status.add_error(f'Failed to retrieve node value from {node_id} (Error: {value})')
        one_node = None
        one_value = f"ERROR: {value}"

    return [one_node, one_value]

# ---------------------------------------------------------------------------------------
# Get multiple values in a single call
# ---------------------------------------------------------------------------------------
def get_multiple_opcua_values(status, client, id_nodes, attr_included, read_method, failures):
    """
    Fetch the value of a specific node given its Node ID.
    :param status: thread status object
    :param client: OPC-UA client
    :param id_nodes: A list of Node IDs  i.e. ["ns=0;i=2257", "ns=0;i=2258"]
    :param attr_included: return a list of attributes
    :param read_method: "collection" - a single call for the entire collection OR "individual" - a single call for each point
    :param failures: If set to True and executed with method = individual, only errors are returned


    return the list of the nodes names + the list of values
    """
    try:


        if read_method == "collection":
            # A single call for all the nodes
            opcua_nodes = [client.get_node(node_id) for node_id in id_nodes]
            nodes = [node.nodeid for node in opcua_nodes]

            multiple_results = client.uaclient.get_attributes(nodes, ua.AttributeIds.Value)          # Reads in a single call multiple values
            if not attr_included:
                # SEE HOW IT IS ORGANIZED IN:       multiple_values = client.get_values(opcua_nodes)
                result_list = [(result.Value.Value,) for result in multiple_results]

        else:
            # individual calls
            opcua_nodes = []
            multiple_results = []
            result_list = []
            for node_id in id_nodes:
                one_node, one_value = get_one_value(status, client, node_id)

                if failures and one_node:
                    # only output failures
                    continue

                if not attr_included:
                    if one_node:
                        result_list.append((one_value.Value.Value,))
                    else:
                        result_list.append((one_value,))
                else:
                    opcua_nodes.append(one_node)
                    multiple_results.append(one_value)

        if attr_included:
            result_list = []
            # Include additional attributes
            for index, entry in enumerate(multiple_results):

                if "all" in attr_included:
                    # Save the attribute values
                    if opcua_nodes[index]:
                        if failures:
                            # only output failures
                            continue
                        # Node is available and was read from the OPCUA
                        result_list.append((id_nodes[index], opcua_nodes[index].get_browse_name().Name.lower(), entry.SourceTimestamp, entry.ServerTimestamp, entry.StatusCode.name, entry.Value.Value))
                    else:
                        result_list.append((id_nodes[index], None, None, None, None, multiple_results[index]))
                else:
                    entry_info = []
                    if "id" in attr_included:  # The tag id
                        entry_info.append(id_nodes[index])
                    if opcua_nodes[index]:
                        if failures:
                            # only output failures
                            continue
                        # A value was retrieved from the OPCUA
                        if "name" in attr_included:     # The attribute name
                            entry_info.append(opcua_nodes[index].get_browse_name().Name.lower())
                        if "source_timestamp" in attr_included:     #  The timestamp of the value as determined by the data source (e.g., a sensor or device).
                            entry_info.append(entry.SourceTimestamp)
                        if "server_timestamp" in attr_included:     # The timestamp assigned by the OPC UA server when the data value was received or processed.
                            entry_info.append(entry.ServerTimestamp)
                        if "status_code" in attr_included:          # The status of the value (e.g., Good, Bad, Uncertain).
                            entry_info.append(entry.StatusCode.name)

                        entry_info.append(entry.Value.Value)
                    else:
                        # A value was retrieved from the OPCUA
                        if "name" in attr_included:  # The attribute name
                            entry_info.append(None)
                        if "source_timestamp" in attr_included:  # The timestamp of the value as determined by the data source (e.g., a sensor or device).
                            entry_info.append(None)
                        if "server_timestamp" in attr_included:  # The timestamp assigned by the OPC UA server when the data value was received or processed.
                            entry_info.append(None)
                        if "status_code" in attr_included:  # The status of the value (e.g., Good, Bad, Uncertain).
                            entry_info.append(None)

                        entry_info.append(multiple_results[index])

                    result_list.append(entry_info)

    except:
        errno, value = sys.exc_info()[:2]
        status.add_error(f'Failed to retrieve multiple values from {id_nodes} (Error: {value})')
        result_list = None

    return result_list
# ---------------------------------------------------------------------------------------
# Navigate in the TREE
# Common Node Attributes:
# NodeId:                               Unique identifier for the node.
# BrowseName:                           Human-readable name used for browsing.
# DisplayName:                          Localized name for display purposes.
# Description:                          Optional description of the node.
# WriteMask / UserWriteMask:            Indicates whether specific attributes of the node can be written.
# AccessLevel (for Variable nodes):     Indicates the access level (read/write).
# DataType (for Variable nodes):        Specifies the type of data (e.g., String, Integer).
# Value (for Variable nodes):           The actual data stored in the node.
# ---------------------------------------------------------------------------------------

def navigate_tree(status, client, node, depth, navigation_info, type_dict, validate_value):
    """
    Navigate the OPC-UA tree interactively, displaying options and fetching values.
    :param status: Status object
    :param client: OPC-UA client
    :param node: Current node in the tree
    :param depth: Current depth in the tree (used for indentation)
    :param navigation_info: a dictionary with navigation rules
    :param type_dict: Number of nodes in the tree - a dictionary for statistics
    :param validate_value: If True - read a value before adding the node to the output list
    """


    if navigation_info["max_depth"] and depth >= navigation_info["max_depth"]:
        # Limit by depth
        return process_status.SUCCESS


    ret_val = process_status.SUCCESS

    if navigation_info["@limit"] and navigation_info["@limit"] <= type_dict["total"]:
        return ret_val          # Stop here


    try:
        node_class = node.get_node_class()
    except:
        errno, value = sys.exc_info()[:2]

        # Check if `source_node` exists and contains `nodeid`
        if hasattr(node, 'source_node') and hasattr(node.source_node, 'nodeid'):
            node_id = str(node.source_node.nodeid)
            if node.source_node.nodeid.Identifier == 84:
                # i=84 is a standard, well-known node in the OPC UA specification — it's the Objects Folder, which is the root for all user-defined nodes.
                error_info = "Root Node - " # indicate this is a root node
                if not len(node.children):
                    error_info += "No Children - "
            else:
                error_info = "Child Node -"

            node_id = error_info + node_id
        else:
            node_id = "Unknown"

        err_msg = f'OPCUA: Failed traversal on node: {node_id} (Error: {value})'
        status.add_error(err_msg)
        utils_print.output_box(err_msg)

        return process_status.Unrecognized_source_node


    output_format = navigation_info["format"]  # Tree or get_value or run client
    file_handle = navigation_info["file_handle"]

    if not navigation_info["class"] or  node_class in navigation_info["class"]:
        # filter by class (if specified)

        if validate_value:
            # READ a value, if read fails, ignore node
            read_status, one_value = get_one_value(status, client, node.get_node_identifier())
            if read_status is None:
                type_dict["read_failure"] += 1
        else:
            read_status = True

        if output_format == "tree":
            ret_val = node.print_info(status, file_handle, validate_value, read_status, navigation_info["attributes"], depth)      # Output the node info on stdout
            if ret_val:
                return ret_val
        else:
            # Collect the nodes to a list

            if read_status:
                navigation_info["@nodes"].append(node.get_node_identifier())

        if not (output_format == "tree" or output_format == "path" or output_format == "policy") or file_handle:
            # If printing a command or if output is to a file - print bar showing navigation status
            print_bar(navigation_info, type_dict) # Print a bar showing an asterisk every 100 nodes visited

        type_dict["total"] += 1             # Count nodes considered
        type_dict[node_class] += 1  # Count by class

    if not node.set_children(node):         # Update a list with the children
        status.add_error("Failed call to get children nodes in OPCUA call")
        ret_val = process_status.Failed_opcua_process
    else:

        children = node.get_children()      # Get a list with all children

        if len(children):
            # Visit the children
            for child in children:
                ret_val = navigate_tree(status, client, child, depth + 1, navigation_info, type_dict, validate_value)
                if ret_val:
                    break
        elif output_format == "path":
            # This is an edge node
            if not navigation_info["class"] or node_class in navigation_info["class"]:
                # Only if no class, or the specific class is needed
                ret_val = node.output_path(status, file_handle, depth)  # Output the node path
                if ret_val:
                    return ret_val
        elif output_format == "policy":
            # This is an edge node
            if not navigation_info["class"] or node_class in navigation_info["class"]:
                # Only if no class, or the specific class is needed
                dbms_name = navigation_info["@dbms"]
                navigation_info["@table_id"] += 1
                new_table_id = navigation_info["@table_id"]
                bchain_insert = navigation_info["@target"]      # A string to make the inserts of a TAG policy to a blockchain.
                table_insert = navigation_info["@schema"]       # A string to make the inserts of a TABLE policy to a blockchain.

                ret_val = node.output_policy(status, file_handle, depth, dbms_name, new_table_id, bchain_insert, table_insert)  # Output the node path
                if ret_val:
                    return ret_val
    return ret_val
# ---------------------------------------------------------------------------------------
# Disconnect to OPCUA
# ---------------------------------------------------------------------------------------
def disconnect_opcua(status, connection):
    """
    Disconnect from OPC-UA
    :args:
        cur:Client - connection to OPC-UA
    """
    try:
        connection.disconnect()
    except:
        errno, value = sys.exc_info()[:2]
        url = connection.server_url.scheme + "://" + connection.server_url.netloc + connection.server_url.path
        status.add_error(f'OPC-UA: Failed to disconnect from {url} (Error: {value})')
        ret_val = False
    else:
        ret_val = True

    return ret_val

# ---------------------------------------------------------------------------------------
# Connect to OPCUA
# ---------------------------------------------------------------------------------------
def declare_connection(status:process_status, url: str, user: str, password: str):
    """
    Connect to OPC-UA
    :args:
        url:str - connection information for OPC-UA
    :params:
        cur:Client.connect - connection to OPC-UA
    :return:
        cur
    """
    cur = None
    try:
        client = Client(url=url)
    except:
        errno, value = sys.exc_info()[:2]
        status.add_error(f'Failed to identify OPC-UA against {url} (Error: {value})')
        client = None
    else:

        if user:
            client.set_user(user)
        if password:
            client.set_password(password)

        with open(os.devnull, 'w') as devnull:  # This call is to supress stack printouts from threads in some connect failures
            try:
                with redirect_stdout(devnull), redirect_stderr(devnull):  # This call is to supress stack printouts from threads in some connect failures
                    client.connect()
            except Exception as e:
                exc_type, exc_value = sys.exc_info()[:2]
                if not str(exc_value):
                    exc_value = type(e).__name__
                error_msg = f'Failed to connect to OPC-UA against {url} (Error: {exc_value})'
                status.add_error(error_msg)
                utils_print.output_box(error_msg)
                client = None

    return client

# ---------------------------------------------------------------------------------------
# Get OPCUA root node
# ---------------------------------------------------------------------------------------
def get_root(status, connection):
    try:
        root_opcua = connection.get_root_node()
    except Exception as error:
        errno, value = sys.exc_info()[:2]
        status.add_error(f'Failed to retrieve the root (Error: {value})')
        root_node = None
    else:
        # A wrapper on the OPCUA node
        root_node = struct_tree.OpcuaNode( 0, root_opcua, None)

    return root_node
# ---------------------------------------------------------------------------------------
# Get Node by namespace and id
# Node type and id is represented as follows:
# i: Integer-based NodeId (e.g., i=84).
# s: String-based NodeId (e.g., s=MyVariable).
# g: GUID-based NodeId (e.g., g=12345678-1234-1234-1234-123456789abc).
# b: ByteString-based NodeId (e.g., b=0123456789ABCDEF).
# ---------------------------------------------------------------------------------------
def get_node(status, connection, node_id):

    try:
        source_node = connection.get_node(node_id)
    except Exception as error:
        errno, value = sys.exc_info()[:2]
        status.add_error(f'Failed to retrieve a node using: {node_id} (Error: {value})')
        node_obj = None
    else:
        # A wrapper on the OPCUA node
        node_obj = struct_tree.OpcuaNode( 0, source_node, None)

    return node_obj


# ----------------------------------------------------------------------
# Print a bar showing an asterisk every 100 nodes visited
# ----------------------------------------------------------------------
def print_bar(navigation_info, type_dict):
    navigation_info["@counter"] += 1  # Used for the printout
    navigation_info["@tmp1"] += 1  # Used for the printout
    navigation_info["@tmp2"] += 1  # Used for the printout
    if (navigation_info["@tmp1"] == 1):
        # Print a message on the next 1000 nodes to be visited
        if navigation_info["@counter"] == 1:
            utils_print.output("\r\n\n", False)
        backspaces = '\b' * 51
        utils_print.output(f"\rProcessing nodes #{type_dict['total']:,} - #{type_dict['total'] + 1000 - 1:,}  [{' ' * 50}]" + f"{backspaces}",False)
    if navigation_info["@tmp1"] >= 1000:
        navigation_info["@tmp1"] = 0  # Used for the printout
        navigation_info["@tmp2"] = 0  # Used for the printout
    elif navigation_info["@tmp2"] >= 100:
        # Print the * - asterisk
        utils_print.output("  *  ", False)
        navigation_info["@tmp2"] = 0

# -----------------------------------------------------------------------------------------
# Return the tag key, attribute name, attribute value and timestamp from the entry pulled from the PLC
# -----------------------------------------------------------------------------------------
def get_plc_tag_info( entry ):

    tag_key = entry[0]

    timestamp = entry[2] if entry[2] else entry[3]

    attr_name, attr_val = plc_utils.normalize_attributes(entry[1], entry[5])

    return [tag_key, timestamp, attr_name, attr_val]
