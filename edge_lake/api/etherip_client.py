"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

# Documentation  https://docs.pycomm3.dev/en/latest/usage/logixdriver.html

import sys
import os

try:
    # from pycomm3 import LogixDriver, CommError
    if 'SIMULATOR_MODE' in os.environ and str(os.environ['SIMULATOR_MODE']).lower() == 'true':
        from edge_lake.api.pycomm3_FakeLogixDriver import LogixDriver
    else:
        from pycomm3 import LogixDriver
except:
    etherip_installed_ = False
else:
    etherip_installed_ = True

import edge_lake.generic.process_status as process_status
import edge_lake.generic.interpreter as interpreter
import edge_lake.generic.params as params
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.utils_io as utils_io
import edge_lake.api.plc_utils as plc_utils


from edge_lake.generic.utils_columns import get_current_utc_time


# PLC to SQL type mapping
plc_to_sql_type_ = {
    "BOOL": "BOOLEAN",
    "BYTE": "TINYINT",
    "WORD": "SMALLINT",
    "DWORD": "BIGINT",
    "LWORD": "BIGINT",
    "SINT": "TINYINT",
    "INT": "SMALLINT",
    "DINT": "INTEGER",
    "LINT": "BIGINT",
    "REAL": "FLOAT",
    "LREAL": "DOUBLE PRECISION",
    "CHAR": "CHAR(1)",
    "STRING": "VARCHAR(255)",
    "TIME": "TIME",
    "DATE": "DATE",
    "TIME_OF_DAY": "TIME",
    "DATE_AND_TIME": "TIMESTAMP",
    "DT": "TIMESTAMP"
}

def test_lib_installed(status):
    if etherip_installed_:
        ret_val = process_status.SUCCESS
    else:
        status.add_error("Lib EtherNet/IP not installed")
        ret_val = process_status.Failed_to_import_lib
    return ret_val
# ---------------------------------------------------------------------------------------
# Connect to EthernetIP and retrieve the structure
# get etherip struct where url = 127.0.0.1
# get etherip struct where url = 127.0.0.1 and read = true
# ---------------------------------------------------------------------------------------
def etherip_struct(status, io_buff_in, cmd_words, trace):

    if not etherip_installed_:
        status.add_error("Lib pycomm3 for EthernetIP not installed")
        return [process_status.Failed_to_import_lib, None]

    keywords = {"url":                      ("str",     True,   False,  True),      # OPCUA URL
                "user":                     ("str",     False,  False,  True),      # Username  (optional)
                "password":                 ("str",     False,  False,  True),      # Password (optional)
                "output":                   ("str",     False,  False,  True),      # The type of output - stdout or file
                "append":                   ("bool",    False, False,   True),      # In file mode - append to the previous file (default)
                "prefix" :                  ("str",     False, False,   False),     # The prefix of the path to consider
                "limit":                    ("int",     False, False,   True),      # Max nodes to consider
                "read":                     ("bool",     False, False,   False),    # Read the values from the PLC
                "format":                   ("str",     False, False,   True),      # the type of output - "tags" (default), "policy", "run_client"
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
    limit = interpreter.get_one_value_or_default(conditions, "limit", 0)

    prefix = interpreter.get_one_value_or_default(conditions, "prefix", None)     # Prefix of the path to consider

    output_format = interpreter.get_one_value_or_default(conditions, "format", "tree")

    valid_formats = ["tree", "policy", "get_value", "run_client"]
    if not output_format in valid_formats:
        status.add_error("EthereNetIP Error: invalid output format, expected values are: " + ", ".join(valid_formats))
        return [process_status.ERR_command_struct, None]


    # Establish connection to the OPC-UA server
    connection = declare_connection(status=status, ip_address=url, username=user, password=password)
    if not connection:
        return [process_status.Failed_PLC_CONNECT, None]


    tags_dict = {}      # A dictionary containing all the tags

    ret_val = get_tags_dict(status, connection, prefix, tags_dict, limit)
    if ret_val:
        return [ret_val, None]

    ret_val, navigation_info, file_handle = plc_utils.prep_plc_traversal(status, "etherip", conditions)
    if ret_val:
        return [ret_val, None]

    if output_format == "tree":
        # organize a table with the tags
        ret_val = navigate_tree(status, connection, tags_dict, navigation_info)  # Start navigating from the root node
    elif output_format == "get_value" or output_format == "run_client":
        # organize a list with the tags
        navigation_info["@nodes"] = list(tags_dict)
    if output_format == "policy":
        # Set a list with name + data type
        for key, value in tags_dict.items():
            navigation_info["@nodes"].append((key, value["type"].lower()))
        ret_val = plc_utils.create_policies(status, "etherip", navigation_info)

    disconnect_etherip(status, connection, url)

    output_txt = plc_utils.output_plc_struct(ret_val, "etherip", conditions, file_handle, url, output_format, navigation_info, None)

    return [ret_val, output_txt]

# ---------------------------------------------------------------------------------------
# Update a dictionary representing the tags
# ---------------------------------------------------------------------------------------
def get_tags_dict(status, connection, prefix, tags_dict, limit):
    '''
    status - stat object
    connection - EtherIP connection object
    prefix - path prefix to consider, or None for all tags
    tags_dict - dict of tags to update as f(path)
    '''


    ret_val = process_status.SUCCESS

    try:
        # tags_list = connection.get_tag_list()
        tags_list = connection.tags
        countr = 0
        for tag_name, tag_info in tags_list.items():

            if limit:
                if countr >= limit:
                    break
                countr += 1

            data_type = tag_info.get("data_type")  # extract tag info

            if isinstance(data_type, str):  # map data type to PSQL

                if not prefix or tag_name.startswith(prefix):
                    sql_type = plc_to_sql_type_.get(data_type.upper(), "VARCHAR(255)").lower()
                    tags_dict[tag_name] = {
                                            "name" : tag_name,
                                            "type": sql_type
                                    }

            elif isinstance(data_type, dict) and "internal_tags" in data_type:
                """
                Get data type for param with multiple keys. Example: 
                "CombinedChlorinatorAI": lambda: {"PV": random_float(0.0, 10.0)}
                """
                for subtag_name, subtag_info in data_type["internal_tags"].items():
                    full_alias = f"{tag_name}.{subtag_name}"

                    if not prefix or full_alias.startswith(prefix):

                        subtag_data_type = subtag_info.get("data_type")

                        if isinstance(subtag_data_type, str):
                            sql_type = plc_to_sql_type_.get(subtag_data_type.upper(), "VARCHAR(255)").lower()
                        else:
                            sql_type = "varchar(255)"

                        tags_dict[full_alias] = {
                                            "name": subtag_name,
                                            "type": sql_type
                                        }


    except:
        status.add_error("EthereNetIP: unable to process tags")
        ret_val = process_status.Failed_PLC_INFO


    return  ret_val


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

def navigate_tree(status, client, tags_dict, navigation_info):
    """
    Navigate the OPC-UA tree interactively, displaying options and fetching values.
    :param status: Status object
    :param client: EternetIP client
    :param node: a dictionary with the list of the tags
    :param navigation_info: A dict with instruction on the navigation
    """

    file_handle = navigation_info["file_handle"]

    ret_val = process_status.SUCCESS

    with_val = navigation_info["read"]

    table_title = ["Index", "Tag Name", "Tag Path", "Data Type"]

    if with_val:
        # read from the PLC
        tags_list = list(tags_dict)
        ret_val, data_point =  read_data(status, client, tags_list)
        if ret_val:
            return ret_val
        table_title = table_title + ["Tag Value", "Error"]
    else:
        data_point = None


    output_table = set_tags_output_table(tags_dict, None, with_val, data_point)

    reply = utils_print.output_nested_lists(output_table, "", table_title, True)

    utils_print.output(reply, True)


    return ret_val

# ---------------------------------------------------------------------------------------
# Set the values read in a a table struct
# ---------------------------------------------------------------------------------------
def set_tags_output_table(tags_dict, tags_list, with_val, data_point):
    '''
    Provide one of the 2 - tags_dict or tags_list
    '''

    if tags_list:
        tags_dict = {}
        for entry in tags_list:
            tags_dict[entry] = {
                "name": entry,
                "type": ""
            }

    output_table = []
    for index, (tag_key, tag_info) in enumerate(tags_dict.items()):

        tag_name = tag_info["name"]
        tag_type = tag_info["type"]

        if with_val:
            plc_tag_value = str(data_point[index].value)
            tag_error = str(data_point[index].error)
            plc_tag_error = "" if tag_error == "None" else tag_error
            entry_info = [index + 1, tag_name, tag_key, tag_type, plc_tag_value, plc_tag_error]
        else:
            entry_info = [index + 1, tag_name, tag_key, tag_type]

        output_table.append(entry_info)

    return output_table

# ---------------------------------------------------------------------------------------
# Data related functions
# :missing Logic:
#   1. User creates a PLC mapping policy (view images)
#   2. Based on the mapping policy, we know the needed tags + updated naemes in table
# ---------------------------------------------------------------------------------------
def read_data(status:process_status, connection:LogixDriver, tags:list):
    """
    Given a list of tags, extract the results. If User doesn't specify tags then pull all tags
    :args:
        status:process_status - error message manager
        connection:LogixDriver - connection to PLC
        tags:lst - list of tags
    :paras:
        results:dict - key/value pairs for PLC tags
        data_points:list - results from plc
    :return:
        results, if fails return None
    """
    results = {}

    try:
        data_points = connection.read(*tags)
    except:
        errno, value = sys.exc_info()[:2]
        status.add_error(f"EtherIP: Failed to read tag values: {value}")
        data_points = None
        ret_val = process_status.Failed_PLC_INFO
    else:
        ret_val = process_status.SUCCESS


    return [ret_val, data_points]

# ---------------------------------------------------------------------------------------
# Connect to OPCUA
# ---------------------------------------------------------------------------------------
def declare_connection(status:process_status, ip_address: str, username: str, password: str, slot: int = 0):

    """
    Connect to a PLC over EtherNet/IP with authentication using pycomm3.

    Args:
        ip_address (str): PLC IP address (e.g., '192.168.1.10').
        username (str): Username for authentication.
        password (str): Password for authentication.
        slot (int): Slot number where the CPU resides.

    Returns:
        LogixDriver: Authenticated and connected driver instance.

    """
    path = f"{ip_address}/{slot}"
    path = f"{ip_address}"

    try:
        plc = LogixDriver(path)
        plc.open()
    except:
        errno, value = sys.exc_info()[:2]
        status.add_error(f"EthereNetIP: Failed to connect to PLC: {value}")
        plc = None
    return plc

# ---------------------------------------------------------------------------------------
# Disconnect
# ---------------------------------------------------------------------------------------
def disconnect_etherip(status, connection, url):
    """
    Disconnect from OPC-UA
    :args:
        cur:Client - connection to OPC-UA
    """
    try:
        connection.close()
    except:
        errno, value = sys.exc_info()[:2]
        status.add_error(f'EthereNetIP: Failed to disconnect from {url} (Error: {value})')
        ret_val = False
    else:
        ret_val = True

    return ret_val


# -----------------------------------------------------------------------------------------
# Return the tag key, attribute name, attribute value and timestamp from the entry pulled from the PLC
# -----------------------------------------------------------------------------------------
def get_plc_tag_info(entry):

    tag_key = "ns=0;s=" + entry.tag         # EMULATE TO OPCUa
    timestamp = get_current_utc_time()

    return [tag_key, timestamp, entry.tag, entry.value]
