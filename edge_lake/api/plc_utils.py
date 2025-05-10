
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

from datetime import datetime

import edge_lake.generic.process_status as process_status
import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.generic.interpreter as interpreter
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.params as params
import edge_lake.generic.utils_io as utils_io

from edge_lake.generic.utils_data import get_unified_data_type



tag_info_ = {}              # The existing tag info (as a function of the path prefix)
max_table_value_ = 0     # the value assigned to the last table created

# ----------------------------------------------------------------------
# Set a dictionary structure with the tag info - retrieved from the blockchain
# ----------------------------------------------------------------------
def set_tag_info(status):
    global tag_info_
    global max_table_value_

    ret_val, policies = member_cmd.blockchain_get(status, ["blockchain", "get", "tag"], "", True)
    if ret_val:
        status.add_error("PLC: Failed to get tag info using the command: 'blockchain get tag'")
        return ret_val
    ret_val = process_status.SUCCESS

    mapping_dict = {}  # Policy IDs to the policy info

    for tag_policy in policies:
        policy_inner = tag_policy["tag"]

        if "table" in policy_inner:
            # Table names are a number prefixed with 't' (i.e. t153) - find the largest number
            # It is used to generate the next table name as t + ( max_table_value_ + 1) --> t154
            table_value = policy_inner["table"]
            if isinstance(table_value,str) and table_value[1:].isdigit():
                table_value = int(table_value[1:])
                if table_value > max_table_value_:
                    max_table_value_ = table_value      # Keep the max value

        # Keep the policies as f (prefix)
        # The prefix is the unique entry key. For example: 'ns=2;s=D1001VFDStop'
        if "ns" in policy_inner:
            if "node_iid" in policy_inner:
                # This is the namespace + node id
                key = f"ns={policy_inner['ns']};i={policy_inner['node_iid']}"
                if key in mapping_dict:
                    status.add_error(f"PLC: Multiple policies with the same namespace and node_iid value = '{key}'")
                    ret_val = process_status.Conflicting_policies
                    break
                mapping_dict[key] = policy_inner

            if "node_sid" in policy_inner:
                # This is the namespace + path prefix
                key = f"ns={policy_inner['ns']};s={policy_inner['node_sid']}"
                if key in mapping_dict:
                    status.add_error(f"PLC: Multiple policies with the same namespace and node_sid value = '{key}'")
                    ret_val = process_status.Conflicting_policies
                    break
                mapping_dict[key] = policy_inner

    if not ret_val:
        tag_info_ = mapping_dict        # Reset previous and Make global

    return ret_val

# ------------------------------------------------------------------------------------------------------------------
# From OPCUA name values to the database name and value
# ------------------------------------------------------------------------------------------------------------------
def normalize_attributes(connector_name, datetime_val):

    try:
        if isinstance(datetime_val, datetime):
            # Change to a string:
            new_val = datetime_val.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        elif not isinstance(datetime_val, str):
            new_val = str(datetime_val)
        else:
            new_val = datetime_val

        new_name = connector_name

    except:
        new_name = None
        new_val = None

    return [new_name, new_val]


# ------------------------------------------------------------------------------------------------------------------
# Organize the user input to a traversal structure
# ------------------------------------------------------------------------------------------------------------------
def prep_plc_traversal(status, interface, conditions):

    output_type = interpreter.get_one_value_or_default(conditions, "output", "stdout")
    output_format = interpreter.get_one_value_or_default(conditions, "format", "tree")
    append_mode = interpreter.get_one_value_or_default(conditions, "append", False)
    attributes = conditions["attributes"] if "attributes" in conditions else None
    node_class = conditions["class"] if "class" in conditions else None              # Filter by namespace
    max_depth = interpreter.get_one_value_or_default(conditions, "depth", 0)        # Limit the depth of the traversal
    limit = interpreter.get_one_value_or_default(conditions, "limit", 0)
    is_read = interpreter.get_one_value_or_default(conditions, "read", False)   # Read from the PLC values


    if output_type != "stdout":
        # Open an output file
        file_name = params.get_value_if_available(output_type)
        if not file_name:
            status.add_error(f"PLC: Failed to retrieve the output file name using '{output_type}'")
            return [process_status.Failed_PLC_INFO, None, None]

        file_handle = utils_io.IoHandle()
        file_mode = "append" if append_mode else "new"  # Append to exiting file or delete previous file
        if not file_handle.open_file(file_mode, file_name):
            status.add_error(f"PLC: Failed to open the output file using '{file_name}'")
            return [process_status.File_open_failed, None, None]
    else:
        file_handle = None

    if output_format == "run_client":
        # Output the command to a "run client" command
        connector_name, frequency, dbms_name, table_name = interpreter.get_multiple_values(conditions,
                                                                                       ["name", "frequency", "dbms",
                                                                                        "table"],
                                                                                       [None, None, None, None])
        if not frequency or not connector_name or not dbms_name:
            # Command line does not have all the variables of the run client command
            # NOTE: Table and dbms can be derived from the command - or from the policies that map tags to tables
            if not dbms_name:
                missing_attr = "dbms"
            else:
                missing_attr = "name" if frequency else "frequency"
            status.add_error(f"PLC: Missing '{missing_attr}' to generate 'run client' command")
            return [process_status.Missing_required_attr, None, None]
    else:
        dbms_name = interpreter.get_one_value_or_default(conditions, "dbms", None)

    navigation_info = {
        "read" : is_read,         # True - will read from the plc current values
        "max_depth": max_depth,  # if not 0, will limit the traversal to max_depth
        "attributes": attributes,  # Dictionary of attributes to print or * for all or None
        "output_type": output_type,  # Type of output - stdout or a file name
        "class": node_class,  # Filter to allow one or more classes like: object, variable
        "format": output_format,
        # tree (default), path (the full path name as a string). stats (statistics), get_value (get opcua value command), run client (run opcua client command)

        "file_handle": file_handle,  # Output file or None

        "@counter": 0,  # Counter nodes visited
        "@tmp1": 0,  # Counter used in printing status, not returned to the user
        "@tmp2": 0,  # Counter used in printing status, not returned to the user
        "@limit": limit,  # Max nodes to visit
        "@nodes": [],  # Include nodes visited

        "@dbms": dbms_name,  # The dbms name
        "@table_id": 0,  # Table ID based on the blockchain policies
        "@target": None,  # Blockchain insert command to insert the policies
        "@schema": None,  # SQL CREATE TABLE statement for the tag
    }

    if output_format == "policy":
        # Create a policy for each entry --> load existing tag policies
        if not dbms_name:
            status.add_error(f"PLC: Missing dbms name in 'get {interface} struct' command with 'format = policy'")
            return [process_status.Failed_PLC_INFO, None, None]
        ret_val = set_tag_info(status)
        if ret_val:
            return [process_status.Failed_PLC_INFO, None, None]
        navigation_info[
            "@table_id"] = max_table_value_  # New policies will use this as a starting value to determine table ID

        bchain_insert = interpreter.get_one_value_or_default(conditions, "target", None)
        if bchain_insert:
            # Example for target: target = "local = true and master = !master_node"
            navigation_info[
                "@target"] = f"blockchain insert where {bchain_insert} and policy = "  # Blockchain insert command to insert the policies

            schema = interpreter.get_one_value_or_default(conditions, "schema", None)  # Generate the create table stmt
            if schema:
                # Per table the following are replaced:
                # [DBMS_NAME] with the DBMS name
                # [TABLE_NAME] with the table name
                # [DATA_TYPE] with the data type
                table_schema = {
                    "table": {
                        "name": "[TABLE_NAME]",
                        "dbms": "[DBMS_NAME]",
                        "create": "CREATE TABLE IF NOT EXISTS [TABLE_NAME](row_id SERIAL PRIMARY KEY,  insert_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),  tsd_name CHAR(3),  tsd_id INT,  timestamp timestamp not null default now(),  value [DATA_TYPE] ); CREATE INDEX [TABLE_NAME]_timestamp_index ON [TABLE_NAME](timestamp); CREATE INDEX [TABLE_NAME]_insert_timestamp_index ON [TABLE_NAME](insert_timestamp);",
                        "source": f"{interface.upper()} Interface"
                    }
                }

                navigation_info["@schema"] = utils_json.to_string(table_schema)

    return [process_status.SUCCESS, navigation_info, file_handle]


# ------------------------------------------------------------------------------------------------------------------
# Output - Struct, - Output the info of "get opcua/etherip struct"
# ------------------------------------------------------------------------------------------------------------------
def output_plc_struct(ret_val, plc_api, conditions, file_handle, url, output_format, navigation_info, type_dict):
    '''
    ret_val - if not 0, the output file will be closed
    plc_api - opcua / etherip
    conditions - the conditions dict updated by the user
    file_handle - the output file handle
    url - the url of the PLC
    output_format - generating one of the following commands: get_value,  run_client, policies
    navigation_info - the navigation_info dict - updated in the traversal of the plc
    type_dict - statistics
    '''
    output_txt = None
    if not ret_val:

        if output_format == "get_value":
            # Output the command to retrieve the values
            # Set the multiple lines as a code block
            output_txt = f"\r\n\n<get plc values where type = {plc_api} and url = {url} and nodes = {make_identifiers_output_list(100, navigation_info['@nodes'])}>"

        elif output_format == "run_client":
            # Output the command to a "run client" command
            connector_name, frequency, dbms_name, table_name = interpreter.get_multiple_values(conditions,
                                                                                           ["name", "frequency", "dbms",
                                                                                            "table"],
                                                                                           [None, None, None, None])
            table_string = f"and table = {table_name} " if table_name else ""
            output_txt = f"\r\n\n<run plc client where type = {plc_api} and name = {connector_name} and url = {url} and frequency = {frequency} and dbms = {dbms_name} {table_string}and nodes = {make_identifiers_output_list(100, navigation_info['@nodes'])}>"

        elif file_handle and output_format == "path":
            output_txt = ""  # only path strings are written to file, not the summaries

        elif type_dict:
            # Add statistics
            if not file_handle or output_format != "policy":
                # get reply counters - ignore if policies are written to file as they are read using "process". The statistics will create an error
                output_table = []
                for key, value in type_dict.items():
                    output_table.append((key, f"{value:,}"))
                title = ["Type", "Counter"]

                output_txt = utils_print.output_nested_lists(output_table, "PLC Nodes Considered", title, True)

    if file_handle:
        if output_txt:
            file_handle.write_from_buffer(output_txt)
        file_handle.close_file()
        output_txt = ""

    utils_print.output("\r\n", True)  # Because of the printout of the bar

    return output_txt

# ---------------------------------------------------------------------------------------
# Make the list of node identifiers such that it is organized with limited identifiers on each line
# ---------------------------------------------------------------------------------------
def make_identifiers_output_list(max_line_length, nodes_list):

    output_str = "\r\n["
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

# ----------------------------------------------------------------------------------------
# Create tag policies
# if "schema" = True --> create plicies representing the tables schema
# ----------------------------------------------------------------------------------------
def create_policies(status, protocol, navigation_info):

    nodes_list = navigation_info["@nodes"]      # Pairs of name + data type

    dbms_name = navigation_info["@dbms"]
    bchain_insert = navigation_info["@target"]  # A string to make the inserts of a TAG policy to a blockchain.
    table_insert = navigation_info["@schema"]  # A string to make the inserts of a TABLE policy to a blockchain.
    file_handle = navigation_info["file_handle"]

    tag_table = None

    tag_object = {
        "protocol" : protocol,
        "ns": 0,
        "dbms" : dbms_name,
    }
    tag_policy = {
        "tag" : tag_object
    }

    ret_val = process_status.SUCCESS

    for node_info in nodes_list:
        node_name, data_type = node_info


        navigation_info["@table_id"] += 1
        new_table_id = navigation_info["@table_id"]
        table_name = f"t{new_table_id}"
        tag_object["table"] = table_name

        tag_object["datatype"] = data_type
        tag_object["node_sid"] = node_name

        if table_insert:
            tag_table = create_tag_table(table_insert, dbms_name, table_name, data_type)


        ret_val = output_policies(status, file_handle, tag_policy, bchain_insert, tag_table)
        if ret_val:
            break

    return ret_val
# ----------------------------------------------------------------------------------------
# Make Create Statement for each tag policy
# ----------------------------------------------------------------------------------------
def create_tag_table(table_insert, dbms_name, table_name, data_type):
    # Per table the following are replaced:
    # [DBMS_NAME] with the DBMS name
    # [TABLE_NAME] with the table name
    # [DATA_TYPE] with the data type
    tag_table = table_insert.replace("[DBMS_NAME]", dbms_name, 1)
    tag_table = tag_table.replace("[TABLE_NAME]", table_name)

    if data_type.endswith("[]"):
        # Every basic type can be used as an array (e.g., Int32[], String[]).
        anylog_data_type = "varchar"
    else:
        anylog_data_type = get_unified_data_type(data_type.lower())  # get the AnyLog data type
        if not anylog_data_type:
            anylog_data_type = "varchar"
    tag_table = tag_table.replace("[DATA_TYPE]", anylog_data_type)

    return tag_table

# ----------------------------------------------------------------------------------------
# Output policies to stdout or file
# ----------------------------------------------------------------------------------------
def output_policies(status, file_handle, tag_policy, bchain_insert, tag_table_policy):
    '''
    :param file_handle: if data written to file
    :param tag_policy: The policy to write
    :param bchain_insert: A user provided blockchain insert statement
    :tag_table_policy: The policy with the create stmt
    '''

    ret_val = process_status.SUCCESS
    if file_handle:
        info_str = utils_json.to_string(tag_policy)
        if info_str:
            if bchain_insert:
                # add the insert statement
                info_str = bchain_insert + info_str + "\n"
                if tag_table_policy:
                    # push the schema (create table) policy
                    info_str += bchain_insert + tag_table_policy + "\r\n"

            if not file_handle.append_data(info_str):
                status.add_error(f"PLC: Failed to write into output file: {file_handle.get_file_name()}")
                ret_val = process_status.File_write_failed
    else:
        if bchain_insert:
            # print policies + inserts
            info_str = utils_json.to_string(tag_policy)
            if info_str:
                info_str = bchain_insert + info_str + "\r\n"
                utils_print.output(info_str, False)
        else:
            # Only show the policies
            utils_print.jput(tag_policy, False, indent=4)

        if tag_table_policy:
            # Show the schema
            info_str = bchain_insert + tag_table_policy + "\r\n"
            utils_print.output(info_str, False)

    return ret_val

