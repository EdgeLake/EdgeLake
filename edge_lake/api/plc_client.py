"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import sys
import time
from datetime import datetime

import edge_lake.generic.process_status as process_status
import edge_lake.generic.interpreter as interpreter
import edge_lake.generic.utils_json as utils_json
import edge_lake.tcpip.mqtt_client as mqtt_client
import edge_lake.generic.utils_print as utils_print

import edge_lake.api.plc_utils as plc_utils
import edge_lake.api.opcua_client as opcua_client
from edge_lake.api import etherip_client

from edge_lake.generic.streaming_data import add_data
from edge_lake.generic.params import get_param

plc_supported_ = ["opcua", "etherip"]

# A dictionary on declared clients
clients_info_ = {

}
included_attr_ = ["id","name","source_timestamp","server_timestamp","status_code", "value"]

# -----------------------------------------------------------------------
def is_plc_supported(plc_type):
    global plc_supported_
    return plc_type in plc_supported_
# -----------------------------------------------------------------------
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
# Get plc client
# ----------------------------------------------------------------------
def get_plc_clients(status, io_buff_in, cmd_words, trace):

    global clients_info_

    if not cmd_words[2].startswith("client"):
        return [process_status.ERR_command_struct, None]

    # set reply counters
    output_table = []
    for key, value in  clients_info_.items():
        counter = value["counter"]
        output_table.append((key, value["protocol"], value["status"], value["frequency"], counter))

    title = ["Client Name", "Protocol", "Status", "Frequency", "Reads"]
    output_txt = utils_print.output_nested_lists(output_table, "PLC Clients Status", title, True)

    return [process_status.SUCCESS, output_txt]
# ----------------------------------------------------------------------
# Info returned to the get processes command
# ----------------------------------------------------------------------
def get_status_string(status):
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
# ------------------------------------------------------------------------------------------------------------------
# OPCUA Client that reads from the OPCUA connector every interval
# run plc client where type = opcua and url = opc.tcp://10.0.0.111:53530/OPCUA/SimulationServer and frequency = 20
# run plc client where type = opcus and url = opc.tcp://10.0.0.111:53530/OPCUA/SimulationServer and frequency = 10 and dbms = nov and table = sensor and node = "ns=0;i=2257" and node = "ns=0;i=2258"
# ------------------------------------------------------------------------------------------------------------------
def run_plc_client(dummy: str, conditions: dict):

    global clients_info_

    status = process_status.ProcessStat()
    ret_val = process_status.SUCCESS
    err_msg = None


    prep_dir = get_param("prep_dir")
    watch_dir = get_param("watch_dir")
    err_dir = get_param("err_dir")

    plc_type = conditions["type"]

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
        "protocol" : plc_type,
        "status": "running",
        "frequency": frequency,
        "counter" : 0,
    }


    client_name = conditions["name"][0]

    if not client_name in clients_info_ or clients_info_[client_name]["status"] == "terminated":
        clients_info_[client_name] = info_dict
        if topic_name:
            # Flag this is placed on the local broker
            ret_val, user_id = mqtt_client.register(status, {"broker": ["local"]})
        else:
            user_id = None
    else:
        status.add_error(f"Multiple PLC Clients with client name: {client_name}")
        ret_val = process_status.ERR_command_struct


    if not ret_val:
        # Establish connection to the OPC-UA server
        if plc_type == "opcua":
            client = opcua_client.declare_connection(status=status, url=url, user=user, password=password)
        elif plc_type == "etherip":
            client = etherip_client.declare_connection(status, url, user, password)

        if not client:
            err_msg = f"Failed to connect to OPC-UA using: {url}"
            status.add_error(err_msg)
            info_dict["status"] = "Error"
            return process_status.Failed_OPC_CONNECT

        ret_val = plc_utils.set_tag_info(status)  # Copy tag metadata
        if ret_val:
            info_dict["status"] = "Error"
            return ret_val

        # Read according to the frequency
        while True:
            start_time = time.time()

            if id_nodes:
                # Get value from OPCUA
                ret_val = process_data(status, plc_type, client, id_nodes, topic_name, user_id, prep_dir, watch_dir, err_dir, dbms_name, table_name)
                if ret_val:
                    break

            if info_dict["status"] == "stop":
                break
            info_dict["counter"] += 1

            diff_time =  time.time() - start_time
            if diff_time < frequency:
                time.sleep(frequency - diff_time)

        if plc_type == "opcua":
            opcua_client.disconnect_opcua(status=status, connection=client)
        if plc_type == "etherip":
            etherip_client.disconnect_etherip(status=status, connection=client, url = url)

    if topic_name:
        # if local broker is used
        mqtt_client.end_subscription(user_id, True)

    utils_print.output("PLC client process terminated: %s" % process_status.get_status_text(ret_val), True)
    if err_msg:
        utils_print.output_box(err_msg)

    info_dict["status"] = "terminated"

# ------------------------------------------------------------------------------------------------------------------
# Read data and send to broker ot buffers
# ------------------------------------------------------------------------------------------------------------------
def process_data(status, plc_type, client, id_nodes, topic_name, user_id, prep_dir, watch_dir, err_dir, dbms_name, table_name):

    if table_name:
        # All readings to the same table
        single_table = True
        json_row = {
            "timestamp" : None,             # First timestamp
            "duration" : 0,
        }
    else:
        # each tag to a dedicated table
        single_table = False
        json_row = {}

    ret_val = process_status.SUCCESS

    if plc_type == "opcua":
        multiple_values = opcua_client.get_multiple_opcua_values(status, client, id_nodes, ["all"], "collection", False)
        get_plc_tag_info = opcua_client.get_plc_tag_info      # A method returning attr name, attr value, timestamp
    elif plc_type == "etherip":
        ret_val, multiple_values = etherip_client.read_data(status, client, id_nodes)
        if ret_val:
            return ret_val
        if not isinstance(multiple_values,list):
            multiple_values = [multiple_values]             # A single value is not returned in a list
        get_plc_tag_info = etherip_client.get_plc_tag_info  # A method returning attr name, attr value, timestamp

    if multiple_values:
        # data provided from OPCUA

        timestamp_first = None
        timestamp_last = None
        for entry in multiple_values:

            tag_key, timestamp, attr_name, attr_val = get_plc_tag_info( entry )    # Gets the info from each connector

            if not timestamp_first:
                timestamp_first = timestamp        # take source_timestamp if available or server timestamp (second option)
                timestamp_last = timestamp_first
            else:
                if timestamp < timestamp_first:
                    timestamp_first = timestamp
                elif timestamp > timestamp_last:
                    timestamp_last = timestamp
            if not attr_name:
                status.add_error(f"OPCUA Error: Failed to process an entry with name: {entry[1]} and value {entry[5]}")
                ret_val = process_status.ERR_process_failure
                break

            if single_table:
                # A single table to cover the tag data
                json_row[attr_name] = attr_val
            else:

                if not tag_key in plc_utils.tag_info_:
                    err_msg = f"OPCUA Error: No policies satisfies the needed namespace and nodeid: {tag_key}"
                    status.add_error(err_msg)
                    utils_print.output_box(err_msg)
                    ret_val = process_status.ERR_process_failure
                    break
                else:
                    policy_inner = plc_utils.tag_info_[tag_key]
                    if dbms_name:
                        # User provided dbms name
                        target_dbms = dbms_name
                    elif "dbms" in policy_inner:
                        # Tag policy provided dbms name
                        target_dbms = policy_inner["dbms"]
                    else:
                        policy_id = policy_inner["id"] if "id" in policy_inner else "(Policy is missing an ID)"
                        status.add_error(f"OPCUA Error: dbms name not specified in 'tag' policy {policy_id}")
                        ret_val = process_status.ERR_process_failure
                        break
                    if "table" in policy_inner:
                        # Tag policy provided dbms name
                        target_table = policy_inner["table"]
                    else:
                        policy_id = policy_inner["id"] if "id" in policy_inner else "(Policy is missing an ID)"
                        status.add_error(f"OPCUA Error: table name not specified in 'tag' policy {policy_id}")
                        ret_val = process_status.ERR_process_failure
                        break

                    # different dbms and table per inset
                    if timestamp_first:
                        # Add timestamp on the row - representing the first timestamp and the duration
                        try:
                            if isinstance(timestamp_first, str):
                                json_row["timestamp"] = timestamp_first
                            else:
                                # Format of OPCUA
                                json_row["timestamp"] = timestamp_first.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                        except:
                            pass
                    if not "timestamp" in json_row:
                        err_msg = f"OPCUA Error: Failed to set a timestamp attribute using: '{timestamp_first}'"
                        status.add_error(err_msg)
                        utils_print.output_box(err_msg)
                        break
                    json_row["value"] = attr_val
                    json_msg = utils_json.to_string(json_row)
                    if not json_msg:
                        status.add_error(f"OPCUA Error: Failed to process JSON with OPCUA data: '{str(json_row)}'")
                        ret_val = process_status.ERR_process_failure
                        break
                    ret_val, hash_value = add_data(status, "streaming", 1, prep_dir, watch_dir, err_dir, target_dbms,
                                                   target_table, '0', '0', "json", json_msg)
                    if ret_val:
                        break


        if single_table:
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
        ret_val = process_status.Failed_PLC_INFO

    return ret_val


# ---------------------------------------------------------------------------------------
# Extract opcua nodes
#
# Command:
#  OPTION A: get opcua values where url = opc.tcp://10.0.0.111:53530/OPCUA/SimulationServer and node = "ns=0;i=2257" and node = "ns=0;i=2258"
#  OPTION B: get opcua values where url=opc.tcp://10.0.0.78:4840/freeopcua/server/andoutput=cmdandnode=ns=2;i=1 and list = ["ns=2;i=1", "ns=2;i=2"]

# get plc values where type = etherip and url = 127.0.0.1 and nodes = ["CombinedChlorinatorAI.PV", "STRUCT.Status"]
# ---------------------------------------------------------------------------------------
def plc_values(status, io_buff_in, cmd_words, trace):

    keywords = {"url":                      ("str",     True,   False,  True),      # OPCUA URL
                "user":                     ("str",     False,  False,  True),      # Username  (optional)
                "password":                 ("str",     False,  False,  True),      # Password (optional)
                "include":                  ("str",     False,  False, False),      # Additional attributes: name, SourceTimestamp, ServerTimestamp, StatusCode
                "node":                     ("str",     False,  True,  False),      # One or more namespace + id: "ns=2;i=1002"
                "nodes":                    ("str",     False,  True,  True),       # A list of nodes: nodes = ["ns=2;i=1", "ns=2;i=2"]
                "method":                   ("str",     False,  False,  True),      # 2 options: "collection" - 1) a single call to all points 2) "single" - a call for each node
                "failures":                 ("bool",    False,  False, True),       # Default is false. If set to True and executed with method = individual, only errors are returned
                "type":                     ("str",     False,  False, True),       # PLC Type: opcua or etherip
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

    plc_type =  password = interpreter.get_one_value_or_default(conditions, "type", "opcua")
    if not is_plc_supported(plc_type):
        status.add_error(f"Get PLC Values cmd: Wrong connector 'type': {plc_type}")
        return [process_status.ERR_command_struct, None]

    attr_included = conditions.get("include", None)
    if attr_included:
        if attr_included[0] == "all":
            title = included_attr_
        else:
            for entry in attr_included:
                if entry not in included_attr_:
                    status.add_error(f"OPCUA: Wrong 'include' attribute in get opcua values command: '{entry}'")
                    return [process_status.ERR_process_failure, None]
            title = attr_included
            if not "value" in attr_included:
                title += ["value"]      # Always include value
    else:
        title = ["value"]

    # If set to True and executed with method = individual, only errors are returned
    failures = interpreter.get_one_value_or_default(conditions, "failures", False)

    # Establish connection to the OPC-UA server
    if plc_type == "opcua":
        ret_val = opcua_client.test_lib_installed(status)
        if ret_val:
            return [ret_val, None]
        client = opcua_client.declare_connection(status=status, url=url, user=user, password=password)
    elif plc_type == "etherip":
        ret_val = etherip_client.test_lib_installed(status)
        if ret_val:
            return [ret_val, None]
        client = etherip_client.declare_connection(status, url, user, password)
    if not client:
        return [process_status.Failed_OPC_CONNECT, None]

    nodes_list = interpreter.get_one_value_or_default(conditions, "nodes", None)
    if nodes_list:
        # A list specified in the command line  (Option B)
        id_nodes = utils_json.str_to_json(nodes_list)
    else:
        # User is not using a list (Option A)
        id_nodes = conditions["node"]

    if plc_type == "opcua":
        multiple_values = opcua_client.get_multiple_opcua_values(status, client, id_nodes, attr_included, read_method, failures)
    elif plc_type == "etherip":
        ret_val, multiple_values = etherip_client.read_data(status, client, id_nodes)
        if ret_val:
            return [ret_val, None]
        if not isinstance(multiple_values, list):
            multiple_values = [multiple_values]  # A single value is not returned in a list


    if not multiple_values:
        ret_val = process_status.Failed_opcua_process
        output_txt = None
    else:

        if plc_type == "opcua":
            output_txt = utils_print.output_nested_lists(multiple_values, "OPCUA Nodes values", title, True)
        if plc_type == "etherip":
            output_table = etherip_client.set_tags_output_table(None, id_nodes, True, multiple_values)
            etherip_title = ["Index", "Name", "Key", "Type", "Value", "Error"]
            output_txt = utils_print.output_nested_lists(output_table, "Ethernet/IP Nodes values", etherip_title, True)

    if plc_type == "opcua":
        opcua_client.disconnect_opcua(status=status, connection=client)
    if plc_type == "etherip":
        etherip_client.disconnect_etherip(status=status, connection=client, url=url)

    return [ret_val, output_txt]
