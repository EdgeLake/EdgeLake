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
import time
from datetime import datetime

import edge_lake.generic.process_status as process_status
import edge_lake.generic.interpreter as interpreter
import edge_lake.api.struct_tree as struct_tree
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.utils_io as utils_io
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.params as params
import edge_lake.tcpip.mqtt_client as mqtt_client

from edge_lake.generic.streaming_data import add_data
from edge_lake.generic.params import get_param

# A dictionary on declared clients
clients_info_ = {

}

included_attr_ = ["id","name","source_timestamp","server_timestamp","status_code"]
# ----------------------------------------------------------------------
# Info returned to the get processes command
# ----------------------------------------------------------------------
def get_status_string(status):
    if not opcua_installed_:
        info_str = "OPC-UA library not installed"
    else:
        clients_count = 0
        for entry in clients_info_.values():
            if entry["status"] == "running":
                clients_count += 1
        info_str = f"{clients_count} OPC_UA Clients" if clients_count else ""
    return info_str

def is_running():
    clients_count = 0
    for entry in clients_info_.values():
        if entry["status"] == "running":
            clients_count += 1
            break

    return True if clients_count else False
# ---------------------------------------------------------------------------------------
# Extract opcua nodes
#
# Command:
#  OPTION A: get opcua values where url = opc.tcp://10.0.0.111:53530/OPCUA/SimulationServer and node = "ns=0;i=2257" and node = "ns=0;i=2258"
#  OPTION B: get opcua values where url=opc.tcp://10.0.0.78:4840/freeopcua/server/andoutput=cmdandnode=ns=2;i=1 and list = ["ns=2;i=1", "ns=2;i=2"]
# ---------------------------------------------------------------------------------------
def opcua_values(status, io_buff_in, cmd_words, trace):

    if not opcua_installed_:
        status.add_error("Lib opcua not installed")
        return [process_status.Failed_to_import_lib, None]

    keywords = {"url":                      ("str",     True,   False,  True),      # OPCUA URL
                "user":                     ("str",     False,  False,  True),      # Username  (optional)
                "password":                 ("str",     False,  False,  True),      # Password (optional)
                "include":                  ("str",     False,  False, False),      # Additional attributes: name, SourceTimestamp, ServerTimestamp, StatusCode
                "node":                     ("str",     False,  True,  False),      # One or more namespace + id: "ns=2;i=1002"
                "nodes":                    ("str",     False,  True,  True),       # A list of nodes: nodes = ["ns=2;i=1", "ns=2;i=2"]
                "method":                   ("str",     False,  False,  True),      # 2 options: "collection" - 1) a single call to all points 2) "single" - a call for each node
                "failures":                 ("bool",    False,  False, True),       # Default is false. If set to True and executed with method = individual, only errors are returned
                }


    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return [ret_val, None]

    if not counter:
        status.add_error("Missing specific nodes ot a list of nodes")
        return [process_status.ERR_process_failure, None]

    url = interpreter.get_one_value(conditions, "url")
    user = interpreter.get_one_value_or_default(conditions, "user", None)
    password = interpreter.get_one_value_or_default(conditions, "password", None)

    read_method = interpreter.get_one_value_or_default(conditions, "method", "collection")
    if read_method != "collection" and read_method != "individual":
        status.add_error("Get OPCUA values command method must be 'collection' or 'individual'")
        return [process_status.ERR_command_struct, None]

    attr_included = conditions.get("include", None)
    if attr_included:
        if attr_included[0] == "all":
            title = ["id","name","source_timestamp","server_timestamp","status_code", "value"]
        else:
            for entry in attr_included:
                if entry not in included_attr_:
                    status.add_error(f"OPCUA: Wrong 'include' attribute in get opcua values command: '{entry}'")
                    return [process_status.ERR_process_failure, None]
            title = attr_included + ["value"]
    else:
        title = ["value"]

    # If set to True and executed with method = individual, only errors are returned
    failures = interpreter.get_one_value_or_default(conditions, "failures", False)

    # Establish connection to the OPC-UA server
    client = declare_connection(status=status, url=url, user=user, password=password)
    if not client:
        return [process_status.Failed_OPC_CONNECT, None]

    nodes_list = interpreter.get_one_value_or_default(conditions, "nodes", None)
    if nodes_list:
        # A list specified in the command line  (Option B)
        id_nodes = utils_json.str_to_json(nodes_list)
    else:
        # User is not using a list (Option A)
        id_nodes = conditions["node"]



    multiple_values = get_multiple_opcua_values(status, client, id_nodes, attr_included, read_method, failures)

    if not multiple_values:
        ret_val = process_status.Failed_opcua_process
        output_txt = None
    else:

        output_txt = utils_print.output_nested_lists(multiple_values, "OPCUA Nodes values", title, True)

    disconnect_opcua(status=status, connection=client)

    return [ret_val, output_txt]

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
                "type" :                    ("str",     False, False,   False),     # Type of nodes to consider: Object, Variable etc
                "limit":                    ("int",     False, False,   True),      # Max nodes to consider
                "attributes":               ("str",     False, False,   False),     # Attribute names to consider or * for all
                "node":                     ("str",     False, False,   True),      # get a different root by providing the node id: exampls: 'ns=0;i= i=84 or s=MyVariable
                "depth":                    ("int",     False, False,   True),      # Limit the depth of the traversal
                "class":                    ("str",     False, False,   False),     # classes to consider: i.e. object, variable
                "format":                   ("str",     False, False,   True),      # the type of output - "tree" (default), "get value", "run client"
                "frequency":                ("str",     False, True,    True),      # If output generates "run_client" - the frequency of the "run client" command
                "table":                    ("str",     False, True,    True),      # If output generates "run_client" - the table name of the "run client" command
                "dbms":                     ("str",     False, True,    True),      # If output generates "run_client" - the dbms name of the "run client" command
                "validate":                 ("bool",    False, False,   True),      # If output generates "run_client" - the dbms name of the "run client" command
                }


    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return [ret_val, None]

    url = interpreter.get_one_value(conditions, "url")
    user = interpreter.get_one_value_or_default(conditions, "user", None)
    password = interpreter.get_one_value_or_default(conditions, "password", None)
    output_type = interpreter.get_one_value_or_default(conditions, "output", "stdout")
    limit = interpreter.get_one_value_or_default(conditions, "limit", 0)
    attributes = conditions["attributes"] if "attributes" in conditions else None
    node_id = interpreter.get_one_value_or_default(conditions, "node", None)     # namespace + id - example: ns=2;i=1002
    max_depth = interpreter.get_one_value_or_default(conditions, "depth", 0)        # Limit the depth of the traversal
    node_class = conditions["class"] if "class" in conditions else None              # Filter by namespace
    output_format = interpreter.get_one_value_or_default(conditions, "format", "tree")
    validate_value = interpreter.get_one_value_or_default(conditions, "validate", False)

    valid_formats = ["tree", "get_value", "run_client"]
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

    if output_type != "stdout":
        # Open an output file
        file_name = params.get_value_if_available(output_type)
        if not file_name:
            status.add_error(f"OPCUA: Failed to retrieve the output file name using '{output_type}'")
            return [process_status.Failed_opcua_process, None]

        file_handle = utils_io.IoHandle()
        if not file_handle.open_file("append", file_name):
            status.add_error(f"OPCUA: Failed to open the output file using '{file_name}'")
            return [process_status.File_open_failed, None]
    else:
        file_handle = None

    if output_format == "run_client":
        # Output the command to a "run client" command
        frequency, dbms_name, table_name = interpreter.get_multiple_values(conditions, ["frequency", "dbms", "table"],
                                                                           [None, None, None])
        if counter != 3:
            # Command line does not have all the variables of the run client command
            status.add_error(
                f"OPCUA: Missing variables to generate 'run client' command: frequency = {frequency} and dbms = {dbms_name} and table = {table_name}")
            return [process_status.Failed_opcua_process, None]

    navigation_info = {
        "max_depth"     :   max_depth,          # if not 0, will limit the traversal to max_depth
        "attributes"    :   attributes,         # Dictionary of attributes to print or * for all or None
        "output_type"   :   output_type,        # Type of output - stdout or a file name
        "class"         :   node_class,         # Filter to allow one or more classes like: object, variable
        "format"        :   output_format,      # tree (default), stats (statistics), get_value (get opcua value command), run client (run opcua client command)

        "file_handle"   : file_handle,           # Output file or None

        "@counter": 0, # Counter nodes visited
        "@tmp1": 0,  # Counter used in printing status, not returned to the user
        "@tmp2": 0,  # Counter used in printing status, not returned to the user
        "@limit": limit,  # Max nodes to visit
        "@nodes": [],  # Include nodes visited

    }
    output_txt = None
    ret_val = navigate_tree(status, connection, root, 0, navigation_info, type_dict, validate_value)  # Start navigating from the root node


    # Disconnect from the OPC-UA server after browsing
    disconnect_opcua(status=status, connection=connection)

    if not ret_val:
        if output_format == "get_value":
            # Output the command to retrieve the values
            # Set the multiple lines as a code block
            output_txt = f"\r\n\n<get opcua values where url = {url} and nodes = {make_identifiers_output_list(100, navigation_info['@nodes'])}>"

        elif output_format == "run_client":
            # Output the command to a "run client" command
            # Set the multiple lines as a code block
            output_txt = f"\r\n\n<run opcua client where url = {url} and frequency = {frequency} and dbms = {dbms_name} and table = {table_name} and nodes = {make_identifiers_output_list(100, navigation_info['@nodes'])}>"

        else:

            # get reply counters
            output_table = []
            for key, value in type_dict.items():
                output_table.append((key, f"{value:,}"))
            title = ["Type", "Counter"]

            output_txt = utils_print.output_nested_lists(output_table,  "OPCUA Nodes Considered", title, True)

    if file_handle:
        if output_txt:
            file_handle.write_from_buffer(output_txt)
            output_txt = None
        file_handle.close_file()

    utils_print.output("\r\n", True)    # Because of the printout of the bar

    return [ret_val, output_txt]

# ---------------------------------------------------------------------------------------
# Make the list of node identifiers such that it is organized with limited identifiers on each line
# ---------------------------------------------------------------------------------------
def make_identifiers_output_list(max_line_length, nodes_list):

    output_str = "["
    line_length = 0
    first = True
    for entry in nodes_list:
        if first:
            first = False
            output_str += f"\"{entry}\""
        else:
            output_str += f",\"{entry}\""

        if line_length >= max_line_length:
            output_str += "\r\n"
            line_length = 0
        else:
           line_length += (len(entry) + 3)
    output_str += "]"

    return output_str
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
                        result_list.append((id_nodes[index], opcua_nodes[index].get_browse_name().Name, entry.SourceTimestamp, entry.ServerTimestamp, entry.StatusCode.name, entry.Value.Value))
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
                            entry_info.append(opcua_nodes[index].get_browse_name().Name)
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
        else:
            node_id = "Unknown"

        status.add_error(f'OPCUA: Failed traversal on node: {node_id} (Error: {value})')
        return process_status.Unrecognized_source_node

    if not navigation_info["class"] or  node_class in navigation_info["class"]:
        # filter by class (if specified)

        output_format = navigation_info["format"]           # Tree or get_value or run client

        file_handle = navigation_info["file_handle"]

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

        if output_format != "tree" or file_handle:
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
        status.add_error(f'Failed to disconnection from {url} (Error: {value})')
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
        try:
            # Set the username and password
            if user:
                client.set_user(user)
            if password:
                client.set_password(password)
            client.connect()
        except:
            errno, value = sys.exc_info()[:2]
            status.add_error(f'Failed to connect to OPC-UA against {url} (Error: {value})')
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

# ------------------------------------------------------------------------------------------------------------------
# OPCUA Client that reads from the OPCUA connector every interval
# run opcua client where url = opc.tcp://10.0.0.111:53530/OPCUA/SimulationServer and frequency = 20
# run opcua client where url = opc.tcp://10.0.0.111:53530/OPCUA/SimulationServer and frequency = 10 and dbms = nov and table = sensor and node = "ns=0;i=2257" and node = "ns=0;i=2258"
# ------------------------------------------------------------------------------------------------------------------
def run_opcua_client(dummy: str, conditions: dict):

    global clients_info_

    status = process_status.ProcessStat()
    ret_val = process_status.SUCCESS
    err_msg = None


    prep_dir = get_param("prep_dir")
    watch_dir = get_param("watch_dir")
    err_dir = get_param("err_dir")

    url = interpreter.get_one_value(conditions, "url")
    user = interpreter.get_one_value_or_default(conditions, "user", None)
    password = interpreter.get_one_value_or_default(conditions, "password", None)
    frequency = interpreter.get_one_value(conditions, "frequency")      # Frequency in MS
    dbms_name = interpreter.get_one_value_or_default(conditions, "dbms", None)
    table_name = interpreter.get_one_value_or_default(conditions, "table", None)
    id_nodes = conditions.get("node", None)
    if not id_nodes:
        nodes_list = interpreter.get_one_value_or_default(conditions, "nodes", None)
        if nodes_list:
            # A list specified in the command line  (Option B)
            id_nodes = utils_json.str_to_json(nodes_list)

    topic_name = interpreter.get_one_value_or_default(conditions, "topic", None)
    policy_id = interpreter.get_one_value_or_default(conditions, "policy", None)

    # info for this client
    info_dict = {
        "status": "running",
        "frequency": frequency,
        "counter" : 0,
    }

    if dbms_name:
        # id of this client is dbms + table
        client_name = f"{dbms_name}.{table_name}"
    else:
        client_name = policy_id

    if not client_name in clients_info_ or clients_info_[client_name]["status"] == "terminated":
        clients_info_[client_name] = info_dict
        if topic_name:
            # Flag this is placed on the local broker
            ret_val, user_id = mqtt_client.register(status, {"broker": ["local"]})
        else:
            user_id = None
    else:
        status.add_error(f"Multiple OPC-UA Clients with client name: {client_name}")
        ret_val = process_status.ERR_command_struct


    if not ret_val:
        # Establish connection to the OPC-UA server
        client = declare_connection(status=status, url=url, user=user, password=password)
        if not client:
            err_msg = f"Failed to connect to OPC-UA using: {url}"
            status.add_error(err_msg)
            ret_val = process_status.Failed_OPC_CONNECT
        else:

            # Read according to the frequency
            while True:
                start_time = time.time()

                if id_nodes:
                    # Get value from OPCUA
                    ret_val = process_data(status, client, id_nodes, topic_name, user_id, prep_dir, watch_dir, err_dir, dbms_name, table_name)
                    if ret_val:
                        break

                if info_dict["status"] == "stop":
                    break
                info_dict["counter"] += 1

                diff_time =  time.time() - start_time
                if diff_time < frequency:
                    time.sleep(frequency - diff_time)


            disconnect_opcua(status=status, connection=client)

    if topic_name:
        # if local broker is used
        mqtt_client.end_subscription(user_id, True)

    utils_print.output("OPC-UA client process terminated: %s" % process_status.get_status_text(ret_val), True)
    if err_msg:
        utils_print.output(err_msg, True)

    info_dict["status"] = "terminated"

# ------------------------------------------------------------------------------------------------------------------
# Read data and send to broker ot buffers
# ------------------------------------------------------------------------------------------------------------------
def process_data(status, client, id_nodes, topic_name, user_id, prep_dir, watch_dir, err_dir, dbms_name, table_name):

    json_row = {
        "timestamp" : None,             # First timestamp
        "duration" : 0,
    }

    ret_val = process_status.SUCCESS

    multiple_values = get_multiple_opcua_values(status, client, id_nodes, ["all"], "collection", False)


    if multiple_values:
        # data provided from OPCUA
        timestamp_first = None
        timestamp_last = None
        for entry in multiple_values:
            if not timestamp_first:
                timestamp_first = entry[2] if entry[2] else entry[3]        # take source_timestamp if available or server timestamp (second option)
                timestamp_last = timestamp_first
            else:
                timestamp = entry[2] if entry[2] else entry[3]
                if timestamp < timestamp_first:
                    timestamp_first = timestamp
                elif timestamp > timestamp_last:
                    timestamp_last = timestamp
            attr_name, attr_val = normalize_attributes(entry[1], entry[5])
            if not attr_name:
                status.add_error(f"Failed to process an entry with name: {entry[1]} and value {entry[5]}")
                ret_val = process_status.ERR_process_failure
                break
            json_row[attr_name] = attr_val

        if timestamp_first:
            # Add timestamp on the row - representing the first timestamp and the duration
            try:
                json_row["timestamp"] = timestamp_first.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                json_row["duration"] = int((timestamp_last - timestamp_first).total_seconds() * 1000)
            except:
                pass

        if not ret_val:
            json_msg = utils_json.to_string(json_row)
            if not json_msg:
                status.add_error("Failed to process JSON with OPCUA data")
                ret_val = process_status.ERR_process_failure
            else:
                if topic_name:
                    ret_val = mqtt_client.process_message(topic_name, user_id, json_msg)  # Transfer data to MQTT Client
                else:
                    ret_val, hash_value = add_data(status, "streaming", 1, prep_dir, watch_dir, err_dir, dbms_name, table_name, '0', '0', "json", json_msg)
    else:
        ret_val = process_status.Failed_opcua_process

    return ret_val

# ------------------------------------------------------------------------------------------------------------------
# From OPCUA name values to the database name and value
# ------------------------------------------------------------------------------------------------------------------
def normalize_attributes(opcua_name, opcua_val):

    try:
        if isinstance(opcua_val, datetime):
            # Change to a string:
            new_val = opcua_val.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        elif not isinstance(opcua_val, str):
            new_val = str(opcua_val)
        else:
            new_val = opcua_val

        new_name = opcua_name

    except:
        new_name = None
        new_val = None

    return [new_name, new_val]
# -----------------------------------------------------------------------
# Exit one or more clients
# -----------------------------------------------------------------------
def exit(client_name):

    global clients_info_

    ret_val = process_status.SUCCESS
    if client_name == "all":
        # exit all
        for entry in clients_info_.values():
            entry["status"] = "stop"
    else:
        if client_name in clients_info_:
            clients_info_[client_name]["status"] = "stop"       # The object with the subscription info
        else:
            ret_val = process_status.Failed_opcua_process
    return ret_val

# ----------------------------------------------------------------------
# Get the info on the running clients
# Get opcua client
# ----------------------------------------------------------------------
def get_opcua_clients(status, io_buff_in, cmd_words, trace):

    global clients_info_

    if not cmd_words[2].startswith("client"):
        return [process_status.ERR_command_struct, None]

    # set reply counters
    output_table = []
    for key, value in  clients_info_.items():
        counter = value["counter"]
        output_table.append((key, value["status"], value["frequency"], counter))

    title = ["Client Name", "Status", "Frequency", "Rows"]
    output_txt = utils_print.output_nested_lists(output_table, "OPCUA Nodes values", title, True)

    return [process_status.SUCCESS, output_txt]

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
        utils_print.output(f"\rProcessing nodes #{type_dict['total']:,} - #{type_dict['total'] + 1000 - 1:,}  [{' ' * 50}]" + backspaces,False)
    if navigation_info["@tmp1"] >= 1000:
        navigation_info["@tmp1"] = 0  # Used for the printout
        navigation_info["@tmp2"] = 0  # Used for the printout
    elif navigation_info["@tmp2"] >= 100:
        # Print the * - asterisk
        utils_print.output("  *  ", False)
        navigation_info["@tmp2"] = 0