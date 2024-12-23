"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import threading
import re
import time
import random
import subprocess
import copy
from difflib import SequenceMatcher

import edge_lake.cmd.monitor as monitor
import edge_lake.cmd.data_monitor as data_monitor
import edge_lake.api.al_kafka as al_kafka
import edge_lake.tcpip.http_client as http_client
import edge_lake.tcpip.http_server as http_server
import edge_lake.tcpip.tcpip_server as tcpip_server
import edge_lake.tcpip.net_client as net_client
import edge_lake.tcpip.net_utils as net_utils
import edge_lake.tcpip.message_server as message_server
import edge_lake.tcpip.message_header as message_header
import edge_lake.tcpip.mqtt_client as mqtt_client
import edge_lake.tcpip.grpc_client as grpc_client
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.utils_timer as utils_timer
import edge_lake.generic.params as params
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_sql as utils_sql
import edge_lake.generic.utils_output as utils_output
import edge_lake.generic.utils_python as utils_python
import edge_lake.generic.utils_monitor as utils_monitor
import edge_lake.generic.utils_queue as utils_queue
import edge_lake.generic.events as events
import edge_lake.generic.interpreter as interpreter
import edge_lake.generic.input_kbrd as input_kbrd
import edge_lake.generic.utils_threads as utils_threads
import edge_lake.generic.utils_columns as utils_columns
import edge_lake.generic.streaming_data as streaming_data
import edge_lake.generic.output_data as output_data
import edge_lake.generic.streaming_conditions as streaming_conditions
import edge_lake.job.job_scheduler as job_scheduler
import edge_lake.job.job_instance as job_instance
import edge_lake.job.task_scheduler as task_scheduler
import edge_lake.dbms.ha as ha
import edge_lake.dbms.cursor_info as cursor_info
import edge_lake.dbms.db_info as db_info
import edge_lake.dbms.pi_dbms as pi_dbms
import edge_lake.dbms.unify_results as unify_results
import edge_lake.dbms.partitions as partitions
import edge_lake.blockchain.blockchain as blockchain
import edge_lake.blockchain.bsync as bsync
import edge_lake.blockchain.bplatform as bplatform
import edge_lake.blockchain.metadata as metadata
import edge_lake.json_to_sql.map_results_to_insert as map_results_to_insert
import edge_lake.json_to_sql.map_json_to_insert as map_json_to_insert
import edge_lake.members.aloperator as aloperator
import edge_lake.members.alarchiver as alarchiver
import edge_lake.members.policies as policies
import edge_lake.cmd.json_instruct as json_instruct
import edge_lake.generic.process_log as process_log
import edge_lake.generic.stats as stats
import edge_lake.generic.version as version
import edge_lake.generic.profiler as profiler
import edge_lake.generic.trace_func as trace_func

from edge_lake.json_to_sql.suggest_create_table import *
from edge_lake.dbms.dbms import connect_dbms, get_real_dbms_name
import edge_lake.generic.node_info as node_info
import edge_lake.tcpip.port_forwarding as port_forwarding

watch_directories = {}  # a dictionary of directories and threads watching each directory
dir_mutex = threading.Lock()  # mutex to check threads considering directories
connection_mutex = threading.Lock()  # mutex to sync threads yodating the connection list

# Organize and index with all commands
help_index_ = {}

config_policies_ = {}                # Update the config policies processed


message_compression_ = False        # Determines compression og messages

query_mode = {  # default query parameters like timeout
    "timeout": [0],
    "max_volume": [10000000],
    "send_mode": ["all"],
    "reply_mode": ["all"],
    "show_id": [False],
}
code_debug = {}  # a dictionary that represents code sections to debug
script_mutex = threading.Lock()  # mutex to avoid same id to multiple scripts
statistics_ = {}        # Statistics in get status command

#                                      Must     Add      Is
#                                      exists   Counter  Unique
cmd_instructions = {"include": ("str", False, False, False),
                    # dbms.table to include in the query - allows to treat remote tables with a different name as the table being queried
                    "extend": ("str", False, False, False),
                    # Variables to add to the query result set which are not in the table data. Example: extend = (@ip, @port.str, @DBMS, @table, !disk_space.int)
                    "table": ("str", False, False, True),  # Name of output table -
                    "output": ("bool", False, False, True),
                    # enable / disable print to stdout - to disable stdout use: output: none
                    "drop": ("bool", False, False, True),  # drop the output table when query starts (default is True)
                    "max_time": ("int.time", False, False, True),  # Cap the query execution time
                    "dest": ("str", False, False, True),
                    # Output dest like stdout (default), rest, none (if query only updates results dbms)
                    "file": ("str", False, False, True),
                    # With key and value: "dest = file", file path and name to accumulate the result set.
                    "format": ("str", False, False, True), # Output format, the default being JSON. Options: json, table, backup
                    "title": ("str", False, False, True), # in TESTING - title added to output file. * sign would add the query text
                    "source": ("str", False, False, True), # in TESTING - a file name that includes the correct output to validate
                    "test" : ("bool", False, False, True),    # Enable testing format (Header section, output section and stat section)
                    "option" : ("str", False, False, False),  # Allow different options. Examples: in testing option = time, option = dest.stdout, option = dest.dbms.qa.test_cases
                    "timezone": ("str", False, False, True),  # utc - data is presented in utc timezone
                    "stat": ("bool", False, False, True),    # Add statistics (time and row count to a query)
                    "pass_through" : ("bool", False, False, True),    # User can disable pass_through in the specific query
                    "topic" : ("str", False, False, True),    # A topic if data is send to a broker
                    "committed" : ("bool", False, False, True),    # A true value will limit the query to data on all nodes of the cluster
                    "info": ("str", False, False, False),   # Additional info to include with the query
                    "nodes": ("str", False, False, True),   # "main" or "all" - with HA: nodes = "main" - retrieve from main servers, "all" - retrieve from any server
                    }
format_values = {
                    "json" : 0,             # Output in JSON format
                    "json:output" : 0,      # Output as JSON rows
                    "json:list" : 0,        # Output as a JSON list
                    "table" : 0,            # Output as a table
}
dest_values = {
                    "stdout" : 0,             # Output to stdout
                    "rest" : 0,               # Output to rest
                    "file" : 0,               # Output to file
                    "buffer" : 0,            # Output to buffer
                    "kafka@" : 6,            # Output to kafka - the value after @ can be any value (to ignore the IP and port)
}

nodes_values = {
                    "main" : 0,               # With HA - query from operators designated as main - default
                    "all" : 0,                # With HA - query from any operator
}
metadata_query = {"dbms": ("str", False, False, True),
                  "table": ("str", False, False, True),
                  "company": ("str", False, False, True),
                  }

sql_commands = {
    "select" : 1,
    "insert" : 1,
    "update" : 1,
    "delete" : 1,
    "create" : 1,
}



nodes_options_ = ['operator','publisher','query','master']
nodes_pattern_ = '|'.join(map(re.escape, nodes_options_))   # This is to quickly find node type
commands_options_ = ["subset", "timeout"]
commands_pattern_ = "|".join(re.escape(word) for word in commands_options_)



# =======================================================================================================================
# Return True if the query pool is active
# =======================================================================================================================
def is_query_pool_active():
    global workers_pool

    return not workers_pool == None
# =======================================================================================================================
# Return info on the pool
# =======================================================================================================================
def get_query_pool_info(status):
    if workers_pool:
        info_str = "Threads Pool: %u" % workers_pool.get_number_of_threds()
    else:
        info_str = ""
    return info_str

test_active_ = {
    #                    Process name | Get is acrive | Comment
    "tcp": ("TCP", net_utils.is_tcp_connected, tcpip_server.get_info),
    "rest": ("REST", net_utils.is_rest_connected, http_server.get_info),
    "operator": ("Operator", aloperator.is_active, aloperator.get_info),
    "blockchain sync": ("Blockchain Sync", bsync.is_running, bsync.get_info),
    "scheduler": ("Scheduler", task_scheduler.is_running, task_scheduler.get_info),
    "blobs archiver": ("Blobs Archiver", alarchiver.is_arch_running, None),
    "msg client": ("MQTT", mqtt_client.is_running, None),
    "message broker": ("Message Broker", net_utils.is_msg_connected, message_server.get_info),
    "smtp": ("SMTP", utils_output.is_smtp_running, utils_output.get_smtp_info),
    "streamer": ("Streamer", streaming_data.is_active, streaming_data.get_info),
    "query pool": ("Query Pool", is_query_pool_active, get_query_pool_info),
    "kafka consumer": ("Kafka Consumer", al_kafka.is_active, al_kafka.get_info),
    "gRPC": ("gRPC", grpc_client.is_running, grpc_client.get_status_string),
}


class DebugScript:
    # Debug AnyLog Scripts
    # Every runing script is registered in running_scripts dictionary with this info
    def __init__(self):
        self.script_name = ""  # the name of the file containing the script
        self.deug = False
        self.debug_interactive = False  # debugs the code if - 'set debug interactive
        self.debug_next = False  # set to True with the command next
        self.stop = False  # this value is changed to TRue to stiop the process

    def set_script_name(self, script_name):
        self.script_name = script_name

    def get_script_name(self):
        return self.script_name

    def is_debug(self):
        return self.deug

    def set_debug(self, status: bool):
        self.deug = status

    def is_debug_interactive(self):
        return self.debug_interactive

    def set_debug_interactive(self, status: bool):
        self.debug_interactive = status

    def is_debug_next(self):
        return self.deug_next

    def set_debug_next(self, status: bool):
        self.deug_next = status

    def set_stop(self):
        self.stop = True  # Stop the script

    def is_stop(self):
        return self.stop


running_scripts = {}
event_scripts = {}  # scripts triggered by event kept in RAM
event_time = 0  # we measure the time elapsed from previous event. if less than 5 seconds, we do not read from disk

# events that are coded
coded_events = {
    "get_blockchain": events.blockchain_master_to_node,  # copy the blockchain from the master node to a local node
    "blockchain_use_new": events.blockchain_use_new,  # copy a new blockchain file to the destination
    "drop_old_partitions": events.drop_old_partitions, # Look at the partitions of the relevant tables and drop the oldest
    "get_recent_tsd_info" : ha.get_recent_tsd_info, # Query TSD info for the most updates row id and time --> return a message (event) with the info
    "recent_tsd_info" : ha.recent_tsd_info, # A reply to get_recent_tsd_info ==> update the local metadata with the info
    "metadata_ping" : ha.metadata_ping, # A message to determine the status of a server
    "metadata_pong" : ha.metadata_pong, # A reply to the ping message
    "missing_archived_file" : ha.process_missing_archived_file, # A message send when archived file is not found
    "job_stop" : events.job_stop,          # A message to stop processing a query
}

job_process_stat_ = {}          # A dictionary updated with job status - i.e. - from events.job_stop to flag query process that are terminated

workers_pool = None         # Pool of workers thread

echo_queue = None  # A message queue for buffered messages

# =======================================================================================================================
# Connect a logical database to a physical database
# 'connect dbms [db name] where type = [db type] and user = [db user] and ip = [db ip] and port = [db port] and password = [db passwd]  and memory = [true/false] and connection = [db string]
# ======================================================================================================================
def _connect_dbms(status, io_buff_in, cmd_words, trace):


    if len (cmd_words) < 3:
        return process_status.ERR_command_struct

    if cmd_words[2] == "message":  # a tcp sql message
        mem_view = memoryview(io_buff_in)
        command = message_header.get_command(mem_view)
        words_array = command.split()
    else:
        words_array = cmd_words

    words_count = len(words_array)
    if words_count < 7 or words_array[3] != "where":
        status.add_error("Wrong command format: 'connect dbms'")
        return process_status.ERR_command_struct

    #                             Must     Add      Is
    #                             exists   Counter  Unique

    keywords = {"type": ("str", True, False, True),     # PSQL / SQLite
                "user": ("str", False, False, True),
                "ip":   ("str", False, False, True),
                "port": ("str", False, False, True),
                "password": ("str", False, False, True),
                "memory": ("bool", False, False, True),     # True for in memory dbms
                "connection" : ("str", False, False, True), # special connection string for the database
                "autocommit": ("bool", False, False, True),  # change autocommit config with the underlying database
                "unlog" : ("bool", False, False, True),     # create unlogged tables
                # "fsync": ("bool", False, False, True),  # create unlogged tables
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, words_array,
                                                                   4, 0, keywords,
                                                                   False)
    if ret_val:
        return ret_val

    db_name = params.get_value_if_available(words_array[2])
    db_type = interpreter.get_one_value(conditions, "type")
    if db_type != "psql" and db_type != "sqlite" and db_type != "pi" and db_type != "mongo":
        return process_status.Wrong_dbms_type


    db_usr = interpreter.get_one_value_or_default(conditions, "user", "")
    password = interpreter.get_one_value_or_default(conditions, "password", None)
    host = interpreter.get_one_value_or_default(conditions, "ip", "")
    port = interpreter.get_one_value_or_default(conditions, "port", "")

    in_ram = interpreter.get_one_value_or_default(conditions, "memory", False)
    engine_string = interpreter.get_one_value_or_default(conditions, "connection", "") # special connection string for the database


    dbms_name = get_real_dbms_name( db_type, db_name )

    if db_info.is_dbms(dbms_name):
        if db_info.is_active_stat(dbms_name):
            # dbms is active
            if not db_info.test_db_type(dbms_name, db_type):
                status.add_error("Connect request to dbms %s conflicts with existing connection to dbms %s" % (db_type, db_info.get_db_type(dbms_name)))
                return process_status.DBMS_connection_error
            else:
                return process_status.SUCCESS  # the same database is connected


    dbms = connect_dbms(status, dbms_name, db_type, db_usr, password, host, port, in_ram, engine_string, conditions)

    if dbms == None:
        connected_type = db_info.get_db_type( dbms_name )
        if not connected_type:
            # dbms not connected - return an error
            ret_val = process_status.DBMS_NOT_COMNNECTED
            err_msg = "Database connect error: %s using %s failed to connect" % (dbms_name, db_type)
        elif connected_type != db_type:
            # Trying to connect a logical database to a physical database,
            # But logical database is connected to a different dbms
            ret_val = process_status.Connected_to_different_dbms
            err_msg = "Database connect error: Logical database %s is connected to %s " % (dbms_name, db_type)
        else:
            ret_val = process_status.SUCCESS

        if ret_val:
            status.add_error(err_msg)
            utils_print.output(err_msg, True)

        return ret_val

    if not dbms.exec_setup_stmt(status):
        # issue calls that configure the database - these are specific calls for each dbms
        err_msg = "Database connect error: %s using %s failed to execute config call" % (dbms_name, db_type)
        status.add_error(err_msg)
        utils_print.output(err_msg, True)
        return 0 # Return 0 even if the database created

    db_info.create_dbms(dbms.get_dbms_name(), dbms, db_type, dbms.with_connection_pool())

    process_log.add("SQL", "DBMS Connect: Type: " + db_type + " User: " + db_usr + " Port: " + port + " DBMS: " + dbms_name)

    utils_print.output("Database %s using %s connected!" % (dbms_name, db_type), True)

    return process_status.SUCCESS

# =======================================================================================================================
# Get the configuration policies used on the node
# command: get config policies
# =======================================================================================================================
def get_config_policies(status, io_buff_in, cmd_words, trace):
    global config_policies_
    list_title = ["Policy ID", "Policy_name", "Error", "Message"]

    out_list = []
    for policy_id, info in config_policies_.items():
        if info[1]:
            # Get the error
            err_text = process_status.get_status_text(info[1])
        else:
            err_text = ""

        out_list.append((policy_id, info[0], info[1], err_text))

    reply = utils_print.output_nested_lists(out_list, "", list_title, True)

    return [process_status.SUCCESS, reply]
# =======================================================================================================================
# Configure a node from a policy - read the policy and configure the node based on the policy values
# Example: config from policy where id = 64283dba96a4c818074d564c6be20d5c
# =======================================================================================================================
def config_from_policy(status, io_buff_in, cmd_words, trace):
    global config_policies_

    if cmd_words[3] != "where":
        status.add_error("Missing where condition in 'config from policy' command")
        return process_status.ERR_command_struct


    cmd = ["blockchain", "get", "*"] + cmd_words[3:]

    ret_val, policy_list = blockchain_get(status, cmd, "", True)

    if not ret_val:
        if not policy_list or not len(policy_list):
            ret_val = process_status.Needed_policy_not_available
        elif len(policy_list) != 1:
            ret_val = process_status.Non_unique_policy
        else:
            policy = policy_list[0]
            if isinstance(policy,dict) and len(policy) == 1:
                json_object = utils_json.get_inner(policy)
                if json_object and isinstance(json_object, dict):
                    # Configure TCP server
                    if not "id" in json_object:
                        return process_status.Missing_id_in_policy
                    policy_id = json_object["id"]
                    policy_name = json_object["name"] if "name" in json_object else ""

                    ret_val = config_servers(status, json_object, policy_id, trace)


                    if ret_val:
                        # Exit after error is saved
                        # Save the policy update and result
                        config_policies_[policy_id] = [policy_name, ret_val]
                        return ret_val

                    # process script in policy
                    if "script" in json_object:
                        commands_list = json_object["script"]
                        if isinstance(commands_list,str) and commands_list[0] == '[' and commands_list[-1] == ']':
                            commands_list = commands_list[1:-1].split(',')
                        if isinstance(commands_list,list):
                            ret_val = process_commands_list(status, io_buff_in, commands_list, policy_id, trace )
                        else:
                            status.add_error(f"Wrong script structure in config policy {policy_id}")
                            ret_val = process_status.Wrong_policy_structure

                        # Save the policy update and result
                    config_policies_[policy_id] = [policy_name, ret_val]

                else:
                    ret_val = process_status.Wrong_policy_structure
            else:
                ret_val = process_status.Wrong_policy_structure

    return ret_val

# =======================================================================================================================
# Process commands from a list of commands
# =======================================================================================================================
def process_commands_list(status, io_buff_in, cmd_list, policy_id, trace ):

    prefix = f"[Policy {policy_id[-6:]}]" if (isinstance(policy_id, str) and len(policy_id)) else "[Policy]"
    debug_script = register_script("process_commands_list")  # places the script name in a registry
    line_number = 0
    for index, command_entry in enumerate(cmd_list):
        if isinstance(command_entry, str):
            command = command_entry.strip()
            if command[0] == "'" and command[-1] == "'":
                command = command[1:-1]     # Remove single quotation
            if len (command):
                ret_val = process_cmd(status, command, False, None, None, io_buff_in)
                if debug_script.is_debug():
                    line_number += 1
                    print_command(prefix, line_number, command, ret_val)  # Debug - Print the command
                if ret_val:
                    status.add_error(f"Wrong format in script line {index + 1} of policy: {policy_id}")
                    break
        else:
            status.add_error(f"Wrong format of script in policy: {policy_id}")
            ret_val = process_status.Wrong_policy_structure

    unregister_script("process_commands_list")
    message = "processed" if not ret_val else "failed"

    utils_print.output_box(f"Script from policy {policy_id} {message}")

    return ret_val


# =======================================================================================================================
# Config TCP, REST, MQTT servers from a policy
#
# =======================================================================================================================
def config_servers(status, json_object, policy_id, trace):
    '''
    server_type - 0 TCP, 1 - REST, 2 - Broker
    json_object - the inner of the policy
    ip_name - the key in the policy for the IP value
    port_name - the key in the policy for the port value
    local_ip_name - the key in the policy for ip on a local network
    '''
    ret_val = process_status.SUCCESS

    ip = json_object["ip"] if "ip" in json_object else ""
    if isinstance(ip, str):
        ip = params.get_value_if_available(ip)

    port = json_object["port"] if "port" in json_object else ""
    if isinstance(port, str):
        port = params.get_value_if_available(port)

    local_ip = json_object["local_ip"] if "local_ip" in json_object else ""
    if isinstance(local_ip, str):
        local_ip = params.get_value_if_available(local_ip)

    local_port = json_object["local_port"] if "local_port" in json_object else ""
    if isinstance(local_port, str):
        local_port = params.get_value_if_available(local_port)

    rest_ip = json_object["rest_ip"] if "rest_ip" in json_object else ""
    if isinstance(rest_ip, str):
        rest_ip = params.get_value_if_available(rest_ip)

    rest_port = json_object["rest_port"] if "rest_port" in json_object else ""
    if isinstance(rest_port, str):
        rest_port = params.get_value_if_available(rest_port)

    broker_ip = json_object["broker_ip"] if "broker_ip" in json_object else ""
    if isinstance(broker_ip, str):
        broker_ip = params.get_value_if_available(broker_ip)

    broker_port = json_object["broker_port"] if "broker_port" in json_object else ""
    if isinstance(broker_port, str):
        broker_port = params.get_value_if_available(broker_port)

    if (ip and port) or (local_ip and local_port):
        # Configure the TCP server

        if "tcp_bind" in json_object:
            is_bind = params.get_value_if_available(json_object["tcp_bind"])
            if not isinstance(is_bind,bool):
                is_bind = True if (isinstance(is_bind, str) and is_bind == "true") else False

        elif ip == "" or local_ip == "":
            # Only one IP - bind is true
            is_bind = True
        else:
            is_bind = False  # Listen to all IP on the provided port

        if is_bind:
            url_bind = local_ip if local_ip else ip
        else:
            url_bind = ""
            if not local_port:
                local_port = port

        try:
            main_port = int(port)
        except:
            status.add_error("The Port value %s is not an int" % str(port))
            return process_status.Wrong_port_value

        if local_port:
            try:
                second_port = int(local_port)
            except:
                status.add_error("The Local Port value '%s' is not an int" % str(local_port))
                return process_status.Wrong_port_value
        else:
            second_port = ""

        if net_utils.is_new_connections(0, ip, main_port, local_ip, second_port, is_bind):
            # Exit existing threads
            threads_pool = tcpip_server.get_threads_obj()
            if threads_pool:
                process_type = "Reconfigure"
                process_status.set_exit("tcp")
                ret_val = net_utils.wait_for_exit_server(status, "tcp", threads_pool)
                if ret_val:
                    return ret_val
            else:
                process_type = "Configure"

            # get the number of threads
            ret_val, tcp_threads = net_utils.get_threads_from_ploicy(status, policy_id, json_object, "tcp", 6)
            if ret_val:
                return ret_val

            ret_val = start_tcp_threads(ip, main_port, local_ip, second_port, is_bind, url_bind, tcp_threads, trace)
            if ret_val:
                return ret_val
            utils_print.output_box(f"{process_type} TCP Server from policy {policy_id}")

    # Configure REST
    if rest_port:

        if rest_ip:
            external_ip = rest_ip
            internal_ip = rest_ip
            url_bind = rest_ip
            is_bind = True  # Bind to the provided REST_IP
        else:
            if not ip:
                status.add_error("Missing IP to configure REST Server using policy %s" % policy_id)
                return process_status.ERR_command_struct
            external_ip = ip
            internal_ip = ip
            url_bind = ""
            is_bind = False

        if "rest_bind" in json_object:
            # Change if specified
            is_bind = params.get_value_if_available(json_object["rest_bind"])
            if not isinstance(is_bind, bool):
                is_bind = True if (isinstance(is_bind, str) and is_bind == "true") else False
        try:
            external_port = int(rest_port)
        except:
            status.add_error("The REST Port value '%s' is not an int" % str(rest_port))
            return process_status.Wrong_port_value
        internal_port = external_port

        if net_utils.is_new_connections(1, external_ip, external_port, internal_ip, internal_port, is_bind):
            threads_pool = http_server.get_threads_obj()
            if threads_pool:
                process_type = "Reconfigure"
                process_status.set_exit("rest")
                http_server.signal_rest_server()  # wake the Rest server
                ret_val = net_utils.wait_for_exit_server(status, "rest", threads_pool)
                if ret_val:
                    return ret_val
            else:
                process_type = "Configure"

            # get the number of threads
            ret_val, rest_threads = net_utils.get_threads_from_ploicy(status, policy_id, json_object, "rest", 5)
            if ret_val:
                return ret_val

            conditions = {"threads": [rest_threads]}

            ret_val = start_rest_threads(external_ip, external_port, internal_ip, internal_port, is_bind, url_bind,
                                         conditions)
            if ret_val:
                return ret_val
            utils_print.output_box(f"{process_type} REST Server from policy {policy_id}")

    # Configure Message Broker
    if broker_port:

        if broker_ip:
            external_ip = broker_ip
            internal_ip = broker_ip
            url_bind = broker_ip
            is_bind = True  # Bind to the provided Broker IP and Port
        else:
            if not ip:
                status.add_error("Missing IP to configure Message Broker using policy %s" % policy_id)
                return process_status.ERR_command_struct
            external_ip = ip
            internal_ip = ip
            url_bind = ""
            is_bind = False

        if "broker_bind" in json_object:
            # Change if specified
            is_bind = params.get_value_if_available(json_object["broker_bind"])
            if not isinstance(is_bind, bool):
                is_bind = True if (isinstance(is_bind, str) and is_bind == "true") else False

        try:
            external_port = int(broker_port)
        except:
            status.add_error("The Broker Port value '%s' is not an int" % str(external_port))
            return process_status.Wrong_port_value
        internal_port = external_port

        if net_utils.is_new_connections(2, external_ip, external_port, internal_ip, internal_port, is_bind):
            threads_pool = message_server.get_threads_obj()
            if threads_pool:
                process_type = "Reconfigure"
                process_status.set_exit("broker")
                ret_val = net_utils.wait_for_exit_server(status, "msg", threads_pool)
                if ret_val:
                    return ret_val
            else:
                process_type = "Configure"

                # get the number of threads
            ret_val, msg_threads = net_utils.get_threads_from_ploicy(status, policy_id, json_object, "msg", 5)
            if ret_val:
                return ret_val

            ret_val = start_broker_threads(external_ip, external_port, internal_ip, internal_port, is_bind,
                                           url_bind, msg_threads, trace)
            if not ret_val:
                utils_print.output_box(f"{process_type} Message Broker from policy {policy_id}")

    return ret_val

# =======================================================================================================================
# Test if this thread is managing the directory, if no one is managing the directory register the thread
# =======================================================================================================================
def register_directory_watch(dir_path):
    thread_name = threading.current_thread().name

    dir_mutex.acquire()

    if dir_path in watch_directories:
        if watch_directories[dir_path] != thread_name:
            ret_value = process_status.Directory_already_in_watch  # a different thread is watching this directory
        else:
            ret_value = process_status.SUCCESS
    else:  # no one is watching this directory
        watch_directories[dir_path] = thread_name
        ret_value = process_status.SUCCESS

    dir_mutex.release()

    return ret_value

# =======================================================================================================================
# Remove registration from the directory
# =======================================================================================================================
def unregister_directory_watch():
    thread_name = threading.current_thread().name
    removed_keys = []

    dir_mutex.acquire()

    for key in watch_directories:
        if watch_directories[key] == thread_name:
            removed_keys.append(key)  # can't delete while iterating

    for key in removed_keys:
        del watch_directories[key]

    dir_mutex.release()

# =======================================================================================================================
# Retrieve from blockchain
# Key is the type of JSON object top seatch, like operator, table
# json_search is an object that is searched in the JSON that satisfy the key. Example: "table" { "table_name" : !table_name,  "dbms_name" : !dbms_name}
# =======================================================================================================================
def blockchain_retrieve(status, blockchain_file, operation, key, json_search, where_cond):
    '''
    status - process status object
    blockchain_file - path to the json file
    operation - get (from memory objects) or read (from json file - representing the source)
    key - policy type
    json_search - key value pairs that satisfy the search
    '''
    blockchain_out = None

    if json_search:  # search for a particular value instance in the JSON
        json_data = params.json_str_replace_key_with_value(
            json_search)  # replace keys in the JSON str with values from dictionary
        value_pairs = utils_json.str_to_json(json_data)  # map string to dictionary
        if value_pairs:
            if not utils_json.validate(value_pairs):  # test dictionary can be represented as JSON
                status.add_error("Error in JSON search key: '%s'" % str(value_pairs))
                ret_val = process_status.ERR_json_search_key
            else:
                ret_val, blockchain_out = blockchain.blockchain_search(status, blockchain_file, operation, key, value_pairs, None)
                if not ret_val and not blockchain_out:
                    status.set_warning_value(process_status.WARNING_empty_data_set)
        else:
            status.add_error("Error in JSON search key: '%s'" % str(json_data))
            ret_val = process_status.ERR_json_search_key
    else:  # No json_search object
        ret_val, blockchain_out = blockchain.blockchain_search(status, blockchain_file, operation, key, None, where_cond)

    return ret_val, blockchain_out


# =======================================================================================================================
# Process from command - it can be called independently or with get command:
# example one: from !oprerator bring ['operator']['ip'] ":" ['operator']['port'] separator = ","
# example two: blockchain get operator where dbms = lsl_demo bring ['operator']['ip'] ":" ['operator']['port'] separator = ","
# =======================================================================================================================
def process_bring(status, json_data, cmd_words, offset, bring_methods):
    words_count = len(cmd_words)
    if offset >= words_count:
        if not bring_methods:
            return [process_status.ERR_command_struct, ""]

    offset_condition = 0
    while cmd_words[words_count - offset_condition - 2] == '=':
        offset_condition += 3  # will be set on the offset representing the number of conditions from the end of the command
        if cmd_words[words_count - offset_condition - 1] == 'and':
            offset_condition += 1  # another condition

    if not offset_condition and len(cmd_words) > 2 and cmd_words[-3] == "separator":
        # No where conditions: Example: blockchain get table bring [dbms] separator = \n
        offset_condition = words_count - 3

    if offset_condition:
        # with conditions - get the conditions to execute the JOB
        # get the conditions to execute the JOB
        #                               Must     Add      Is
        #                               exists   Counter  Unique

        keywords = {"separator": ("str", False, False, True),
                    "python": ("str", False, False, False),
                    }

        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words,
                                                                       words_count - offset_condition, 0, keywords,
                                                                       False)
        if ret_val:
            # conditions not satisfied by keywords or command structure
            # example: blockchain get table bring [table][create] separator = "\n" and python = { ' '.join(.split()[3:]) }
            return [ret_val, ""]
    else:
        conditions = None

    counter_fields = words_count - (
                offset + offset_condition)  # number of fields to add to the dynamic string from each considered object

    ret_val, dynamic_string = utils_json.pull_info(status, json_data, cmd_words[offset:offset + counter_fields],
                                                   conditions, bring_methods)

    return [ret_val, dynamic_string]

# ----------------------------------------------------------------------
# Get destination IP and Ports from policies - a list of IP and Port string
# ----------------------------------------------------------------------
def get_valid_ip_port(policy_list):
    '''
    Find the IP, External IP and Port.
    Determine which values determine the destination and return as an IP Port string.
    With multiple policies, return IP-Port values seperated by a comma.
    '''

    ip_port_string = ""
    comma = ""

    for policy in policy_list:

        policy_type = utils_json.get_policy_type(policy)
        if not policy_type:
            break

        ip, port = net_utils.get_dest_ip_port(policy[policy_type])

        if ip and port:
            ip_port_string += f"{comma}{ip}:{port}"
            comma = ','

    return ip_port_string
# =======================================================================================================================
# Test the blockchain structure or, if value (of an ID) is provided - test that the id is in the file
# Example 1: blockchain test
# Example 2: blockchain test !id
# =======================================================================================================================
def blockchain_test(status, cmd_words, blockchain_file):
    offset = get_command_offset(cmd_words)
    words_count = len(cmd_words)
    if words_count == offset + 2:
        test_type = 1  # test the blockchain
    elif words_count == offset + 3:
        test_type = 2  # test if the ID exists
    else:
        return [process_status.ERR_command_struct, ""]

    reply = "False"
    message = ""
    ret_val = process_status.SUCCESS

    if blockchain_file == "":
        b_file = params.get_value_if_available("!blockchain_file")
        if b_file == "":
            message = "Missing dictionary definition for \'blockchain_file\'"
            status.add_keep_error(message)
            ret_val = process_status.No_local_blockchain_file
    else:
        b_file = blockchain_file

    if not ret_val:
        if test_type == 1:
            # test blockchain structure
            ret_val = blockchain.validate_struct(status, b_file)
            if ret_val == process_status.SUCCESS:
                reply = "true"
                message = "Blockchain file status: OK"
            elif ret_val == process_status.Empty_Local_blockchain_file:
                reply = "false"
                message = "Blockchain data not available with file: %s" % b_file
            else:
                reply = "false"
                message = "Blockchain file status: Failure"
        else:  # Test if an ID exists in the blockchain file
            object_id = params.get_value_if_available(cmd_words[offset + 2])
            if blockchain.validate_id(status, b_file, object_id):
                reply = "true"
                message = "ID Status: in Blockchain file"
            else:
                reply = "false"
                message = "ID status: not in Blockchain file"

        if offset:
            params.add_param(cmd_words[0], reply)
        elif cmd_words[1] == "message":
            message = node_info.get_node_name() + " %s" % message
        elif status.get_active_job_handle().is_rest_caller():
            status.get_active_job_handle().set_result_set(message)
            status.get_active_job_handle().signal_wait_event()  # signal the REST thread that output is ready
        else:
            utils_print.output(message, True)

    return [ret_val, message]


# =======================================================================================================================
# delete the local blockchain json file
# =======================================================================================================================
def blockchain_delete_json(status, cmd_words, blockchain_file):
    if blockchain_file == "":
        b_file = params.get_value_if_available("!blockchain_file")
        if b_file == "":
            message = "Missing dictionary definition for \'blockchain_file\'"
            utils_print.output(message, False)
            status.add_keep_error(message)
            return process_status.No_local_blockchain_file  # no blockchain file def
    else:
        b_file = blockchain_file

    utils_io.write_lock("blockchain")

    if utils_io.delete_file(b_file):
        metadata.reset_structure()              # Delete the data on the metadata layer
        ret_val = process_status.SUCCESS
    else:
        ret_val = process_status.ERR_process_failure
        status.add_error("Failed to delete local blockchain file: '%s'" % b_file)

    blockchain.delete_ledger()

    utils_io.write_unlock("blockchain")

    return ret_val


# =======================================================================================================================
# Load the Metadata - an abstract layer on top of the blockchain
# For example: blockchain load metadata
#              blockchain load metadata where dbms = purpleair and table = readings
# For the file to be loaded, it needs to see a different version of the blockchain file
#
# cmd is: ["blockchain", "get", "cluster"] or  ["blockchain", "get", "cluster", "where", "..."]
# =======================================================================================================================
def blockchain_load(status, cmd, force_load, trace):

    blockchain_file = params.get_value_if_available("!blockchain_file")
    if not blockchain_file:
        status.add_keep_error("Load of Metadata failed: blockchain file not declared")
        ret_val = process_status.No_local_blockchain_file  # no blockchain file def
    else:
        was_modified = bsync.blockchain_stat.set_blockchain_file(status, blockchain_file)  # If the file is new, it will reset the blockchain_stat object
        if was_modified or force_load:
            # Load the metadata - one thread at a time
            if trace:
                utils_print.output(f"\r\n[Blockchain Load] [Modified Flag : {str(was_modified)}] [Force Load : {str(force_load)}]", True)

            metadata.write_lock()

            ret_val, clusters = blockchain_get(status, cmd, blockchain_file, True)
            if not ret_val:
                if not clusters:
                    if trace:
                        utils_print.output(f"\r\nLocal Metadata not updated (no clusters policies)", True)
                    ret_val = process_status.SUCCESS    # No cluster policy - this is not an error
                else:
                    if trace:
                        utils_print.output(f"\r\n[Blockchain Load] [Retrieving {len(clusters)} cluster(s)]", True)

                    metadata.reset_structure()
                    # Load the blockchain data representing the distributions
                    ret_val, cluster_list = metadata.load(status, clusters)

                    if not ret_val:
                        # Load the operators
                        cmd = ["blockchain", "get", "operator", "where", "cluster", "with", str(cluster_list)]
                        ret_val, operators = blockchain_get(status, cmd, "", True)
                        if not ret_val:
                            if not operators:
                                if trace:
                                    utils_print.output(f"\r\nLocal Metadata not updated (no operators policies)", True)
                                status.add_error("Failed to retrieve 'operator' to support active clusters: %s" % str(cluster_list))
                                ret_val = process_status.Needed_policy_not_available
                            else:
                                if trace:
                                    utils_print.output(f"\r\n[Blockchain Load] [Retrieving {len(operators)} operator(s)]", True)
                                # Add Operators to the metadata
                                ret_val = metadata.update_operators(status, operators, trace)

            metadata.write_unlock()
        else:
            ret_val = process_status.SUCCESS  # No need to load as blockchain was not changed since last load
    return ret_val
# =======================================================================================================================
# Test the Cluster Policies
# For example: blockchain test cluster
#               blockchain test cluster where dbms = purpleair and table = readings
# =======================================================================================================================
def blockchain_test_cluster(status, cmd_words, is_message):
    words_count = len(cmd_words)

    ret_val = process_status.SUCCESS
    message = "Blockchain file status: Failure"
    result = "false"
    cmd = ["blockchain", "get", "cluster"]

    if words_count > 4 and cmd_words[3] == "where":
        cmd += cmd_words[3:]
    elif words_count != 3:
        ret_val = process_status.ERR_command_struct

    if not ret_val:
        ret_val, clusters = blockchain_get(status, cmd, "", True)
        if not ret_val:
            if not clusters:
                status.add_error("Failed to retrieve 'cluster' info from local blockchain file")
                ret_val = process_status.Needed_policy_not_available
            else:
                message = metadata.test_clusters_list(status, clusters)
                if message == "ok":
                    message = "Blockchain cluster policies status: OK"

    if is_message:
        message = node_info.get_node_name() + " %s" % message
    elif status.get_active_job_handle().is_rest_caller():
        status.get_active_job_handle().set_result_set(message)
        status.get_active_job_handle().signal_wait_event()  # signal the REST thread that output is ready
    else:
        utils_print.output(message, True)

    return [ret_val, message]


# =======================================================================================================================
# Query the Metadata - an abstract layer on top of the blockchain
# For example: blockchain query metadata
#              blockchain query metadata where dbms = purpleair and table = readings
# =======================================================================================================================
def blockchain_query(status, cmd_words):
    words_count = len(cmd_words)

    if words_count == 3:
        conditions = None
        ret_val = process_status.SUCCESS
    elif words_count > 4 and cmd_words[3] == "where":
        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, metadata_query, False)
    else:
        ret_val = process_status.ERR_command_struct

    if not ret_val:
        metadata.query(status, conditions, True)

    return ret_val

# =======================================================================================================================
# Process blockchain commands
# =======================================================================================================================
def _process_blockchain_cmd(status, io_buff_in, cmd_words, trace):

    offset = get_command_offset(cmd_words)

    if len(cmd_words) < offset + 2:
        return process_status.ERR_command_struct

    blockchain_file = params.get_value_if_available("!blockchain_file")
    if not blockchain_file:
        status.add_keep_error("PI connect: Missing dictionary definition for \'blockchain_file\'")
        return process_status.No_local_blockchain_file # no blockchain file def


    ret_val = process_status.SUCCESS
    reply = ""
    if cmd_words[1] == "message":  # a tcp sql message
        mem_view = memoryview(io_buff_in)
        command = message_header.get_command(mem_view)
        command = command.replace('\t', ' ')
        words_array, left_brackets, right_brakets = utils_data.cmd_line_to_list_with_json(status, command, 0, 0)
        if left_brackets != right_brakets:
            status.add_keep_error("Received inconsistent JSON structure from a different node")
            ret_val = process_status.BLOCKCHAIN_operation_failed

        message = True
        host = message_header.get_source_ip(mem_view)    # The source node that issued the command
    else:
        words_array = cmd_words
        message = False
        host = tcpip_server.get_ip()        # The source node that issued the command


    if not ret_val:
        func_params = [blockchain_file, message, host]
        ret_val, reply = _exec_child_dict(status, commands["blockchain"]["methods"], commands["blockchain"]["max_words"], io_buff_in, words_array, 1, trace, func_params)


    if message:
        if words_array[1] == "get":
            reply_method = "print"
        else:
            reply_method = "echo"

        if ret_val:
            err_msg = "%s Blockchain message '%s' failed" % (reply_method, words_array[1])
            if reply:
                err_msg += ": <%s>" %  reply
            error_message(status, io_buff_in, ret_val, message_header.BLOCK_INFO_TABLE_STRUCT, err_msg, status.get_saved_error())
        elif reply:
            if isinstance(reply,str):
                ret_mssg = reply
            else:
                ret_mssg = str(reply)

            ret_val = send_display_message(status, ret_val, reply_method, io_buff_in, ret_mssg, False)  # send text message to peers


    return ret_val

# =======================================================================================================================
# Return a number value representing the state of the contract
# blockchain state where platform = ethereum
# =======================================================================================================================
def blockchain_state(status, io_buff_in, cmd_words, trace, func_params):

    offset = get_command_offset(cmd_words)

    if not utils_data.test_words(cmd_words[offset:], 2, ["where", "platform", "="]):
        ret_val = process_status.ERR_command_struct
        counter = -1
    else:
        ret_val, counter = bplatform.get_txn_count(status, cmd_words[5 + offset], "contract")
        if not ret_val and not func_params[1]:      # func_params[1] needs to be false to indicate not a message from a different node
            if offset:
                # Assign the value
                params.add_param(cmd_words[0], str(counter))
            else:
                utils_print.output(f"\n{counter}", True)

    return [ret_val, counter]
# =======================================================================================================================
# Query the local blockchain file
# =======================================================================================================================
def blockchain_get_local(status, io_buff_in, cmd_words, trace, func_params):
    returned_values = blockchain_get(status, cmd_words, func_params[0], func_params[1])
    return returned_values

# =======================================================================================================================
# Add JSON to the local blockchain file
# =======================================================================================================================
def blockchain_add_local(status, io_buff_in, cmd_words, trace, func_params):
    ret_val = blockchain_add(status, cmd_words, func_params[0])
    return [ret_val, None]

# =======================================================================================================================
# Add JSON to the local database hosting the blockchain data
# func_params[2] -  the node that issued the command
# func_params[0] - the blockchain file
# Examples:
# blockchain push !policy
# blockchain push ignore !policy
# =======================================================================================================================
def blockchain_push_local(status, io_buff_in, cmd_words, trace, func_params):

    words_count = len(cmd_words)
    if words_count == 3:
        policy_offset = 2
        ignore_error = False
        ret_val = process_status.SUCCESS
    elif words_count == 4 and cmd_words[2] == "ignore":
        policy_offset = 3
        ignore_error = True
        ret_val = process_status.SUCCESS
    else:
        ret_val = process_status.ERR_command_struct
        json_dictionary = None

    if not ret_val:
        compare_to_file = not func_params[1]    # If message to blockchain - this is a master node, avoid compare to local blockchain file
        ret_val, json_dictionary = prep_policy(status, cmd_words[policy_offset], None, func_params[0], compare_to_file)

        if not ret_val:
            # if blockchain table was not created - create table ledger in blockchain dbms
            if db_info.blockchain_create_local_table(status):
                host = func_params[2]
                if not db_info.blockchain_insert_entry(status, host, json_dictionary, ignore_error):  # ignore duplicate keys error
                    ret_val = process_status.BLOCKCHAIN_operation_failed

            else:
                ret_val = process_status.BLOCKCHAIN_operation_failed

    if trace:
        if json_dictionary:
            policy_type = utils_json.get_policy_type(json_dictionary)
        else:
            policy_type = None

        if not policy_type:
            policy_type = "Wrong policy type"
        err_text = process_status.get_status_text(ret_val)
        utils_print.output("\r\n[Command: blockchain push] [Policy Type: %s] [Result: %s]" % (policy_type, err_text), False)
        if json_dictionary:
            utils_print.struct_print(json_dictionary, True, True)

    return [ret_val, None]
# =======================================================================================================================
# Pull the JSON policy from a local database hosting the blockchain data
# =======================================================================================================================
def blockchain_pull_local(status, io_buff_in, cmd_words, trace, func_params):
    ret_val = blockchain_pull(status, cmd_words, func_params[0], trace)
    return [ret_val, None]
# =======================================================================================================================
# Copy the named file over the current blockchain file. If file name not provided, copy blockchain.new
# =======================================================================================================================
def blockchain_change_file(status, io_buff_in, cmd_words, trace, func_params):
    ret_val = blockchain_update_file(status, cmd_words, func_params[0], trace)
    return [ret_val, None]
# =======================================================================================================================
# Update a dbms with the policies in the JSON file
# =======================================================================================================================
def blockchain_file_to_dbms(status, io_buff_in, cmd_words, trace, func_params):
    ret_val = blockchain_update_dbms(status, cmd_words, func_params[0], trace)
    return [ret_val, None]
# =======================================================================================================================
# Create the ledget table in the local database
# =======================================================================================================================
def blockchain_create_table(status, io_buff_in, cmd_words, trace, func_params):
    if not db_info.blockchain_create_local_table(status):  # create
        ret_val = process_status.BLOCKCHAIN_operation_failed
    else:
        ret_val = process_status.SUCCESS
    return [ret_val, None]
# =======================================================================================================================
# Drop the ledget table from the local database
# =======================================================================================================================
def blockchain_drop_table(status, io_buff_in, cmd_words, trace, func_params):

    if not db_info.drop_table(status, "blockchain", "ledger"):  # drop the ledger table
        ret_val = process_status.DBMS_not_open_or_table_err
    else:
        ret_val = process_status.SUCCESS

    return [ret_val, None]
# =======================================================================================================================
# Drop a policy from the blockchain - Executed on the Master Node
# blockchain drop policy where id = xyz
# blockchain drop policy from hyperledger where id = xyz
# =======================================================================================================================
def blockchain_drop_policies(status, io_buff_in, cmd_words, trace, func_params):
    words_count = len(cmd_words)

    if words_count > 5 and cmd_words[3] == "from":
        platform_name = cmd_words[4]        # local / ethereum / hyperledger
        platform_words = 2      # The count of: "from hyperledger"
    else:
        platform_name = "local"             # Local dbms (i.e. with a master node
        platform_words = 0

    if words_count >= (7 + platform_words) and utils_data.test_words(cmd_words, 3 + platform_words, ["where", "id", "="]):
        # blockchain drop policy where id = xyz
        # The policy with the id is dropped
        # ID can be a comma seperated list

        if words_count == (7 + platform_words):
            # either a single id or comma seperated ids without spaces
            ids_string =  params.get_value_if_available(cmd_words[6 + platform_words])
        elif words_count > (15 + platform_words) and cmd_words[6 + platform_words] == "blockchain" and cmd_words[7 + platform_words] == "get":
            # blockchain drop policy where id = blockchain get (cluster, operator) where [company] contains ibm bring [*][id] separator = ,
            # get a list of IDs
            ret_val, ids_string = blockchain_get(status, cmd_words[6:], None, True)
            if ret_val:
                return ret_val
            if not ids_string:
                status.add_error("No policies satisfying the criteria: %s" % ' '.join(cmd_words[6:]))
                return process_status.Policy_not_in_local_file
        else:
            # comma seperated ids
            ids_string =  ' '.join(cmd_words[6:])

        ids_list = ids_string.split(',')
        for policy_id in ids_list:
            policy_string = policy_id.strip()
            if platform_name == "local":
                # delete from the local database
                ret_val = db_info.blockchain_delete_by_id(status, policy_string)
            else:
                ret_val = bplatform.blockchain_delete(status, platform_name, policy_string, trace)
            if ret_val:
                break
    else:
        ret_val = blockchain_drop_policy(status, cmd_words, words_count, platform_words, platform_name)  # drop a single policy from the blockchain in the local database

    return [ret_val, None]
# =======================================================================================================================
# Delete one or more JSON policies from the local database
# Command: blockchain drop policy !json_data
# If !json_data includes more than a single entry, a where condition is required such that users will not delete the entire policies
# =======================================================================================================================
def blockchain_drop_policy(status, cmd_words, words_count, platform_words, platform_name):
    '''
    platform_words - 0 or 2 (if "from ethereum" is used)
    platform_name - local / ethereum / hyperledger
    '''

    if words_count >= (4 + platform_words):

        json_data = params.get_value_if_available(cmd_words[3 + platform_words])
        json_data = utils_data.remove_quotations(json_data)
        json_data = params.json_str_replace_key_with_value(json_data)  # replace keys with values from dictionary

        json_data = utils_data.replace_string_chars(True, json_data, {'|': '"'})

        ret_val = process_status.SUCCESS

    if len(cmd_words) == (4 + platform_words):
        # Only one entry is supported
        json_struct = utils_json.str_to_json(json_data)  # map string to dictionary
        if json_struct:
            if isinstance(json_struct, dict):
                json_dictionary = json_struct
            elif isinstance(json_struct, list) and len(json_struct) == 1 and isinstance(json_struct[0], dict):
                json_dictionary = json_struct[0]
            else:
                status.add_error(
                    "%s includes multiple policies - add a where condition to determine the policy to drop" %
                    cmd_words[3 + platform_words])
                ret_val = process_status.ERR_command_struct
                json_dictionary = None
        else:
            json_dictionary = None

        if not ret_val and json_dictionary:
            ret_val = db_info.blockchain_delete_entry(status, json_dictionary)
        else:
            if not json_dictionary:
                status.add_keep_error("Policy provided is not representative of JSON: " + cmd_words[3 + platform_words])
                ret_val = process_status.ERR_wrong_json_structure
    elif len(cmd_words) >= (8 + platform_words) and cmd_words[4 + platform_words] == "where":
        # consider where condition
        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 5 + platform_words, 0, {}, False)
        if not ret_val:
            if len(conditions):
                json_struct = utils_json.str_to_json(json_data)  # map string to dictionary
                if isinstance(json_struct, dict):
                    json_list = [json_struct]
                elif not isinstance(json_struct, list):
                    status.add_keep_error("Unrecogized JSON policies using: " + cmd_words[3 + platform_words])
                    ret_val = process_status.ERR_wrong_json_structure
                else:
                    json_list = json_struct
                if not ret_val:
                    # Go over the list and remove policies that satisfy the conditions
                    counter_policies = 0
                    counter_deleted = 0
                    for policy in json_list:
                        json_object = utils_json.get_inner(policy)
                        if json_object and isinstance(json_object, dict):
                            # Test that the where condition can be satisfied
                            if blockchain.__validate_value_pair(json_object, conditions):
                                counter_policies += 1  # Policies that satisfy the where condition
                                ret_val = db_info.blockchain_delete_entry(status, policy)
                                if not ret_val:
                                    counter_deleted += 1  # count the number of policies deleted
                    if counter_policies != counter_deleted:
                        if not counter_deleted:
                            status.add_error(
                                "%u policies droped (from %u policies that were provided and satisfy the conditions)" % (
                                counter_deleted, counter_policies))
                        ret_val = process_status.Subset_of_policies_deleted
            else:
                status.add_keep_error("Missing conditions to determine dropped policies")
                ret_val = process_status.ERR_process_failure
    else:
        ret_val = process_status.ERR_command_struct

    return ret_val
# =======================================================================================================================
# Replace ta policy from the blockchain
# func_params[2] -  the node that issued the command
# func_params[0] - the blockchain file
# =======================================================================================================================
def blockchain_change_policy(status, io_buff_in, cmd_words, trace, func_params):
    # blockchain replace policy X with y
    ret_val = blockchain_replace_policy(status, cmd_words, func_params[2], func_params[0])  # Update a specific policy in the local database
    return [ret_val, None]

# =======================================================================================================================
# Drop a policy from the blockchain
# =======================================================================================================================
def blockchain_delete_file(status, io_buff_in, cmd_words, trace, func_params):
    ret_val = blockchain_delete_json(status, cmd_words, func_params[0])
    return [ret_val, None]
# =======================================================================================================================
# Test the structure of the local blockchain file
# =======================================================================================================================
def blockchain_test_file(status, io_buff_in, cmd_words, trace, func_params):
    ret_val, reply = blockchain_test(status, cmd_words, func_params[0])
    return [ret_val, reply]
# =======================================================================================================================
# Load a copy of the metadata using the local copy of the blockchain
# Command: blockchain load metadata     - only if flags were modified
# Command: blockchain reload metadata   - Force reload
# =======================================================================================================================
def blockchain_load_metadata(status, io_buff_in, cmd_words, trace, func_params):

    blockchain_file = params.get_value_if_available("!blockchain_file")
    if not blockchain_file:
        status.add_keep_error("Load of Metadata failed: blockchain file not declared")
        ret_val = process_status.No_local_blockchain_file  # no blockchain file def
    else:
        if cmd_words and cmd_words[1] == "reload":
            force_reload = True
        else:
            force_reload = False
        blockchain.read_ledger_to_mem(status, True, blockchain_file, trace)       # Force load of file from disk to mem
        ret_val = blockchain_load(status, ["blockchain", "get", "cluster"], force_reload, trace)
    return [ret_val, None]
# =======================================================================================================================
# Get a copy of the blockchain file from a different node
# Example: blockchain seed from 73.202.142.172:7848
# =======================================================================================================================
def blockchain_seed_file(status, io_buff_in, cmd_words, trace, func_params):
    words_count = len(cmd_words)
    if words_count == 4:
        ip_port = cmd_words[3]
        copy_cmd = f"run client ({ip_port}) file get !!blockchain_file !blockchain_file"
        ret_val = process_cmd(status, copy_cmd, False, None, None, io_buff_in)
        if not ret_val:
            # Wait to allow the copy to be completed
            if not ret_val:
                time.sleep(5)       # wait for the file
                load_cmd = "blockchain reload metadata"
                ret_val = process_cmd(status, load_cmd, False, None, None, io_buff_in)
    else:
        ret_val = process_status.ERR_command_struct

    if ret_val:
        # Because multiple nested process_cmd calls, the reset wll cause the error to be leveraged on the root process_cmd
        status.reset_considerd_err_val()

    return [ret_val, None]
    # =======================================================================================================================
# Represent the metadata in a diagram
# =======================================================================================================================
def blockchain_query_metadata(status, io_buff_in, cmd_words, trace, func_params):

    ret_val = process_status.SUCCESS
    # load blockchain data to an abstract layer presenting the distribution of the table data and the Operators supporting each cluster
    load_cmd = ["blockchain", "get", "cluster"]

    if len(cmd_words) > 3:
        if cmd_words[3] == "where":
            load_cmd += cmd_words[3:]  # i.e.      "blockchain get cluster where company = my_company"
        else:
            ret_val = process_status.ERR_command_struct
    if not ret_val:
        ret_val = blockchain_load(status, load_cmd, False, 0)

    blockchain_query(status, cmd_words)

    return [ret_val, None]
# =======================================================================================================================
# Switch the master node for the blockchain sync process
# Example: blockchain switch network where master = 23.239.12.151:2048
# =======================================================================================================================
def switch_network(status, io_buff_in, cmd_words, trace, func_params):

    if utils_data.test_words(cmd_words, 3, ["where", "master", "="]):
        ip_port = params.get_value_if_available(cmd_words[6])
        bsync.switch_network(ip_port)
        ret_val = process_status.SUCCESS
    else:
        ret_val = process_status.ERR_command_struct

    return [ret_val, None]

# =======================================================================================================================
# Test the metadata structure
# =======================================================================================================================
def blockchain_test_metadata(status, io_buff_in, cmd_words, trace, func_params):
    ret_val, reply = blockchain_test_cluster(status, cmd_words, func_params[1])
    return [ret_val, reply]


# =======================================================================================================================
# Select info from a log file representing the blockchain
# For example:
# blockchain get operator where dbms = lsl_demo bring ['operator']['ip'] ":" ['operator']['port'] separator = ","
# blockchain get operator where dbms = lsl_demo bring.unique ['operator']['ip'] ":" ['operator']['port'] separator = ","
# blockchain get operator where dbms = lsl_demo bring.recent ['operator']['ip'] ":" ['operator']['port'] separator = ","
# blockchain get operator where dbms = lsl_demo bring.recent
# blockchain get operator where cluster with ["123","456"]  --> one of the values in the list
# =======================================================================================================================
def blockchain_get(status, cmd_words, blockchain_file, return_data):

    if not blockchain_file:
        b_file = params.get_value_if_available("!blockchain_file")
        if not b_file:
            status.add_keep_error("PI connect: Missing dictionary definition for \'blockchain_file\'")
            return [process_status.No_local_blockchain_file, None]  # no blockchain file def
    else:
        b_file = blockchain_file

    words_count = len(cmd_words)
    ret_val = process_status.SUCCESS

    if words_count > 3 and (cmd_words[3] == "get" or cmd_words[3] == "read"):
        assign = True  # assign data retrieved to a name
        offset = 4
        operation = cmd_words[3]        # get or read
    elif words_count > 2 and (cmd_words[1] == "get" or cmd_words[1] == "read"):
        assign = False  # only print the value
        offset = 2
        operation = cmd_words[1]  # get or read
    else:
        status.add_error("Error in blockchain get command: '%s'" % ' '.join(cmd_words))
        return [process_status.ERR_command_struct, None]

    if offset >= words_count:
        status.add_error(f"Missing policy type after: 'blockchain {cmd_words[offset-1]}'")
        return [process_status.ERR_command_struct, None]

    key = params.get_value_if_available(cmd_words[offset])

    offset += 1

    value_pairs = None
    where_cond = ""
    json_str = ""
    if words_count > offset:
        if cmd_words[offset] == "where":
            ret_val, offset, value_pairs = utils_json.make_jon_struct_from_where(cmd_words, offset + 1)
            if ret_val:
                # No key values pairs - take the where condition as a string
                with_bring = False
                if offset < words_count:
                    for counter, word in enumerate(cmd_words[offset:]):
                        if word == "bring" or word[:6] == "bring.":
                            with_bring = True
                            break
                    if not with_bring:
                        counter += 1        # Get all words
                    where_cond = ["if"] + cmd_words[offset:offset + counter]
                    offset += counter
                    ret_val = process_status.SUCCESS    # As we take whatever string which represents the where cond

        elif cmd_words[offset][0] == "{":
            value_pairs = cmd_words[offset]  # value pairs are provided in JSON struct. i.e. - { "dbms" : !dbms_name }
            offset += 1

    if words_count >= (offset + 1):
        with_bring = True
        bring_type, sort_fields = utils_json.get_bring_type(status, cmd_words[offset])
        if bring_type == -1:
            ret_val = process_status.ERR_command_struct
        elif words_count == offset + 1:
            if bring_type <= 1:
                # need more info after the "bring". Unless count is requested
                status.add_error("Missing Info after 'bring' directive in 'blockchain get' command ")
                ret_val = process_status.ERR_command_struct
    else:
        bring_type = 0
        sort_fields = None
        with_bring = False

    if not ret_val:

        ret_val, blockchain_out = blockchain_retrieve(status, b_file, operation, key, value_pairs, where_cond)
        if blockchain_out and offset < words_count and not ret_val:
            if utils_json.is_bring("first", bring_type):
                blockchain_out = utils_json.get_first_instance(blockchain_out)  # Get first
                if words_count == (offset + 1):
                    with_bring = False  # Only get recent JSON, ignore pulling value from the JSON
            elif utils_json.is_bring("recent", bring_type):
                blockchain_out = utils_json.get_recent_instance(blockchain_out)  # Get recent
                if words_count == (offset + 1):
                    with_bring = False  # Only get recent JSON, ignore pulling value from the JSON

            if with_bring:
                ret_val, json_str = process_bring(status, blockchain_out, cmd_words, offset + 1, bring_type)
    else:
        blockchain_out = None

    # REPLY TO THE GET REQUEST

    if not ret_val:
        output_str = json_str
        if utils_json.is_bring("table", bring_type):
            # Change the JSON format to table format
            if json_str:
                output_str = set_json_as_table(json_str, bring_type, sort_fields)

    if not return_data:
        rest_reply = False
        if not ret_val:
            if assign:  # assign to a variable
                if blockchain_out:
                    if not with_bring:
                        output_str = utils_json.to_string(blockchain_out)  # change to a string
                params.add_param(cmd_words[0], output_str)
            else:
                # print or return to REST caller
                if status.get_active_job_handle().is_rest_caller():
                    rest_reply = True # return to REST client
                else:
                    # print to stdout
                    if blockchain_out:
                        if not with_bring:
                            utils_print.struct_print(blockchain_out, True, True)      #  utils_print.jput(blockchain_out, True)
                        else:
                            utils_print.output(output_str, True)

        if rest_reply:
            if not ret_val:
                if blockchain_out:
                    if not with_bring:
                        output_str = utils_json.to_string(blockchain_out)  # change to a string
                if not output_str:
                    output_str = "[]"       # Return empty list
                status.get_active_job_handle().set_result_set(output_str)
            if status.is_rest_wait():
                # Single the rest thread (that data is available) if on wait
                status.get_active_job_handle().signal_wait_event()  # signal the REST thread that output is ready

        data_object = None
    else:
        if ret_val:
            data_object = None
        else:
            if with_bring:
                data_object = output_str
            else:
                if not blockchain_out and return_data:
                    data_object = ""
                else:
                    data_object = blockchain_out

    return [ret_val, data_object]

# =======================================================================================================================
# Organize a JSON struct with one layer as a table
# Input:
# '[{\'name\': \'nvidia-cluster1-operator1\', \'ip\': \'145.40.67.73\'},{\'name\': \'nvidia-cluster2-operator1\', \'ip\': \'145.40.67.73\'}]'
# Output
#name                      ip
#-------------------------|------------|
#nvidia-cluster1-operator1|145.40.67.73|
#nvidia-cluster2-operator1|145.40.67.73|
# =======================================================================================================================
def set_json_as_table(json_struct, bring_type, sort_fields):

    out_policies = utils_json.str_to_list(json_struct)
    if out_policies:
        list_title = []
        list_info = []
        title_set = False
        for policy_info in out_policies:
            values_list = []
            for key, value in policy_info.items():
                if not title_set:
                    list_title.append(key.capitalize())  # Title is set with first entry
                values_list.append(value)
            list_info.append(values_list)
            title_set = True

        if utils_json.is_bring("sort", bring_type):

            if not sort_fields:
                try:
                    list_info.sort()
                except:
                    pass    # DO nothing if sort fails
            else:
                # sort by the table columns - first column is 0
                if len(sort_fields) > 2:
                    sort_strings = sort_fields[1:-1].split(',')
                    try:
                        # Get the column ID
                        sort_columns = ([int(x) for x in sort_strings])
                        # Keep the Max col size for each co; participating in the sort
                        max_len = []
                        for col_id in sort_columns:
                            longest_column = max(len(str(item[col_id])) for item in list_info)
                            max_len.append(longest_column)
                    except:
                        pass
                    else:
                        try:
                            list_info.sort(key=lambda x: custom_sort_key(x, sort_columns, max_len))
                        except:
                            pass

        reply = utils_print.output_nested_lists(list_info, "", list_title, True)

    else:
        reply = None

    return reply

# =======================================================================================================================
# Sort by columns of a list
# Pull the compared columns to make the key - this would be used for the compare
# =======================================================================================================================
def custom_sort_key(item, sort_columns, max_len):
    '''
    sort_columns - the columns id used in the sort
    # max_len - the size of the largest col
    '''

    key = ""
    for index, col in enumerate(sort_columns):

        col_val = item[col]
        if isinstance(col_val, str):
            if any(char.isalpha() for char in col_val):
                key += col_val.ljust(max_len[index], ' ')  # Treat as a string - add spaces to the right
            else:
                key += col_val.rjust(max_len[index], ' ')  # Treat as a number - add spaces to the left
        else:
            # Not a string treat as a number
            key += str(col_val).rjust(max_len[index], ' ')

    return key

# =======================================================================================================================
# Issue a select like statement against the blockchain
# =======================================================================================================================
def blockchain_select_schema(status, dbms_name, table_name):
    blockchain_file = params.get_value_if_available("!blockchain_file")
    if blockchain_file == "":
        status.add_keep_error("Missing dictionary definition for \'blockchain_file\'")
        return None

    cmd = "blockchain get table where dbms = %s and name = %s bring ['table']['create']" % (dbms_name, table_name)

    ret_val, create_stmt = blockchain_get(status, cmd.split(), blockchain_file, True)
    if ret_val or not create_stmt:
        status.add_error("Failed to retrieve table definition from metadata policy for table: '%s.%s'" % (dbms_name, table_name))
        return None

    ret_val, t_name, schema_list = utils_sql.create_to_column_info(status, create_stmt)
    if ret_val:
        if t_name != table_name:
            status.add_error(
                "Wrong table name in blockchain 'table' info (%s.%s): 'create' section has the wrong table name (%s)" % (
                dbms_name, table_name, t_name))
            return None

    return schema_list


# =======================================================================================================================
# Given a dbms name and a table name, determine the name of the columns for a) date-time and b) value
# =======================================================================================================================
def get_time_value_columns(status, dbms_name, table_name, time_column_name, value_column_name, trace_level):
    time_column = time_column_name
    value_column = value_column_name
    if time_column:
        # column name was provided by the user
        time_grade = 5  # no need to guess
    else:
        time_grade = 0  # no name determined
    if value_column:
        value_grade = 5
    else:
        value_grade = 0  # no name determined

    ret_val = process_status.SUCCESS
    if db_info.is_dbms_connected(status, dbms_name):
        info_string = db_info.get_table_info(status, dbms_name, table_name, "columns")
    else:
        info_string = ""
    if not info_string:
        # Get the schema from the blockchain
        columns_struct = blockchain_select_schema(status, dbms_name, table_name)  # Info struct is a list
    else:
        info_struct = utils_json.str_to_json(info_string)  # Info struct is a dictionaru
        columns_struct = info_struct["Structure." + dbms_name + "." + table_name]

    if not columns_struct:
        msg = "Failed to retrieve the schema for table %s.%s" % (dbms_name, table_name)
        status.add_error(msg)
        if trace_level:
            utils_print.output("\r\n[Grafana] [Error: %s]" % msg, True)
        ret_val = process_status.No_metadata_info
    else:
        # find the time field and the value field
        # Take the best option: a) test name similarity, test data type similarity

        for entry in columns_struct:

            if isinstance(entry, dict):
                # by column name and data type - best option
                column_name = entry["column_name"]
                column_type = entry["data_type"]
            else:
                column_name = entry[0]
                column_type = entry[1]

            if time_grade < 5 and column_type.startswith("timestamp"):
                if column_name.startswith("timestamp"):
                    time_column = column_name
                    time_grade = 5
                elif time_grade < 4 and column_name.find("timestamp") > 0:
                    # timestamp inside the column name
                    time_column = column_name
                    time_grade = 4
                elif time_grade < 3 and column_name.find("time") > 0:
                    time_column = column_name
                    time_grade = 3
                elif time_grade < 2:
                    time_column = column_name
                    time_grade = 2

            if value_grade < 5 and (column_type == "int" or column_type == "float"):
                if column_name.startswith("value"):
                    value_column = column_name
                    value_grade = 5
                elif value_grade < 4 and column_name.find("value") > 0:
                    # timestamp inside the column name
                    value_column = column_name
                    value_grade = 4
                elif value_grade < 3:
                    value_column = column_name
                    value_grade = 3

            if time_grade + value_grade == 10:
                break  # columns identified

    return ret_val, time_column, value_column


# =======================================================================================================================
# Add a row to a network block, if the block is full, send the block over the network
# counter - number of entries added
# =======================================================================================================================
def add_row_to_netowk_block(status, soc, io_buff_in, rows_data, block_number, rows_counter):
    ret_val = process_status.SUCCESS

    mem_view = memoryview(io_buff_in)

    bytes_not_copied = 0

    while 1:
        # copy to block and send if block is full
        rows_counter += 1
        data_encoded = rows_data.encode()
        block_full, bytes_not_copied = message_header.copy_data(mem_view, data_encoded, bytes_not_copied)

        if block_full:

            message_header.set_block_number(mem_view, block_number, False)

            message_header.set_counter_jsons(mem_view, rows_counter)  # save the number of JSON segments in the block

            if not net_client.mem_view_send(soc, mem_view):  # send the data block
                ret_val = process_status.ERR_network  # failure in message send
                break

            block_number += 1
            rows_counter = 0
            message_header.set_data_segment_to_command(mem_view)  # set the starting point after the Select stmt

        if not bytes_not_copied:
            break  # all was copied to the block

    return [ret_val, block_number, rows_counter]


# =======================================================================================================================
# Flush the network block - send the last block
# =======================================================================================================================
def flush_network_block(status, soc, io_buff_in, block_number, rows_counter):
    ret_val = process_status.SUCCESS

    mem_view = memoryview(io_buff_in)

    message_header.set_block_number(mem_view, block_number, True)

    message_header.set_counter_jsons(mem_view, rows_counter)  # save the number of JSON segments in the block

    message_header.set_operator_time(mem_view, 0, 0)

    if not net_client.mem_view_send(soc, mem_view):  # send last block
        ret_val = process_status.ERR_network  # failure in message send

    net_client.socket_close(soc)

    if ret_val:
        # send an error message
        status.add_error("Query failed to send block #%u" % block_number)
        # error_message(status, io_buff_in, ret_val, message_header.BLOCK_INFO_RESULT_SET, "job reply", "")

    return ret_val


# =======================================================================================================================
# Return info on the table - info is taken from the blockchain data
# =======================================================================================================================
def get_blockchain_table_info(status, cursor, dbms_name, table_name, info_type):
    ret_val = process_status.SUCCESS
    output_prefix = "Structure." + dbms_name + "." + table_name

    if info_type == "columns":
        # load table metadata
        if db_info.get_table_structure(dbms_name, table_name) == None:
            ret_val = load_table_metadata_from_blockchain(status, cursor, output_prefix, dbms_name, table_name)

    return ret_val


# =======================================================================================================================
# Load table metadata info from blockchain - get table structure from blockchain
# =======================================================================================================================
def load_table_metadata_from_blockchain(status, db_cursor, output_prefix, dbms_name, table_name):
    if db_cursor:
        db_cursor.description[0][0] = "column_name"
        db_cursor.description[1][0] = "data_type"
    # get column name and data type
    output = blockchain_select_schema(status, dbms_name, table_name)

    if output and len(output):
        string_data = utils_sql.format_db_rows(status, db_cursor, output_prefix, output, [], None)
    else:
        status.add_error(f"Failed to get metadata info from Table Policy with DBMS: '{dbms_name}' and Table: '{table_name}'")
        return process_status.Failed_load_table_blockchain

    table_info = utils_json.str_to_json(string_data)
    if table_info == None:
        status.add_error(f"Error in the format of a Table Policy (DBMS: '{dbms_name}' and Table: '{table_name}')")
        return process_status.Failed_load_table_blockchain

    db_info.set_table(dbms_name, table_name, table_info)

    return process_status.SUCCESS


# =======================================================================================================================
# Load table metadata info from blockchain - get table structure from blockchain
# =======================================================================================================================
def load_table_list_from_blockchain(status, db_cursor, output_prefix, title_list, dbms_name):
    db_cursor.description[0][0] = "table_name"

    cmd_words = "blockchain get table where dbms = %s bring ['table']['name'] separator = ," % dbms_name
    err_val, tables = blockchain_get(status, cmd_words.split(), "", True)
    if err_val or not len(tables):
        return ""
    else:
        table_list = tables.split(',')
        output = [[(table_name)] for table_name in table_list]
        string_data = utils_sql.format_db_rows(status, db_cursor, output_prefix, output, title_list, None)

    return string_data

# =======================================================================================================================
# Resolve the IP and Ports from command line
# =======================================================================================================================
def get_command_ip_port(status, cmd_words):
    if len(cmd_words) <= 2:
        return [process_status.ERR_dest_IP_Ports, 0, "", None]

    ret_val = process_status.SUCCESS
    dest_keys = ""
    ip_port_values = []
    ip_port_val =  cmd_words[2]
    if not ip_port_val:
        status.add_error("Wrong IP:Port value")
        return [process_status.ERR_command_struct, 0, "", None]
    if ip_port_val[0] != '(':  # one Ip and one port without parenthesis
        # format can be - IP:PORT or IP PORT
        ip_port = params.get_value_if_available(cmd_words[2])
        offset = ip_port.find(":")  # test for ip:port
        if offset > 0:
            ip = ip_port[:offset]
            if ip == "localhost":
                ip = "127.0.0.1"
            if offset >= (len(ip_port) - 2):
                return [process_status.ERR_dest_IP_Ports, 0, "", None]
            port = ip_port[offset + 1:]
            word_offset = 3  # next word to consider
        else:  # test for ip port (space between ip and port)
            if len(cmd_words) < 5:
                return [process_status.ERR_command_struct, 0, "", None]

            word_offset = 4  # next word to consider
            ip = params.get_value_if_available(cmd_words[2])
            if ip == "localost":
                ip = "127.0.0.1"
            port = params.get_value_if_available(cmd_words[3])

        # add IP + Port + Table name + Invlude DBMS name, Include Table Name, active - False means no cluster node, error code in message send
        ip_port_values.append([ip, port, "", "", True, 0])

    else:
        # format in parentheses
        ips_ports = re.sub(r'\s+', ' ',ip_port_val[1:-1])  # remove extra spaces
        ips_ports = ips_ports.strip()  # remove leading and trailing spaces

        if len(ips_ports) and (ips_ports.startswith("blockchain get") or ips_ports[0][0] == '('):
            # Pull out the "subset" and "timeout" values
            matches = re.finditer(commands_pattern_, ips_ports)

            ip_port_list_all = []
            offset = 0
            for match in matches:
                key_offset = match.start()
                sub_command = ips_ports[offset:key_offset].strip()
                if sub_command[-1] == ',':
                    sub_command = sub_command[:-1]  # Ignore the separating comma
                ip_port_list_all.append(sub_command)
                offset = key_offset
            ip_port_list_all.append(ips_ports[offset:]) # add the last key

        else:
            ip_port_list_all = ips_ports.split(',')  # get an array with IPs and ports (ignore the brackets)

        ip_port_list = []

        # Manage: blockchain get, shortcut to blockchain get, subset and timeout
        for entry in ip_port_list_all:
            test_str = entry.strip()    # Remove spaces
            if not test_str:
                # this is the case of empty parenthesis for query
                continue

            if test_str[0] == '!' and len(test_str) > 1:
                index = test_str.find(' ')      # Replace test_str or first word with the value from the dictionary
                if index == -1:
                    test_str = params.get_value_if_available(test_str)
                else:
                    test_str = params.get_value_if_available(test_str[:index]) + test_str[index:]

                if not test_str:
                    # this is the case of null value returned from the dictionary
                    continue

                    # A call like: run client (  69.164.203.68:32148,50.116.61.153:32148,23.92.28.183:32148,timeout=30,subset=true) get status
            # The timeout and subset are passed to the status anf from there to the job handle
            if len(test_str) > 8 and test_str[:6] == "subset" and (test_str[6]=='=' or test_str[6:8]== ' =' ):
                # test for subset = true/false
                value = True if test_str.find("true",6) != -1 else False
                status.set_subset(value)  # The port is a bool like: subset=true

            elif len(test_str) >= 9 and  test_str[:7] == "timeout" and (test_str[7]=='=' or test_str[7:9]== ' =' ):
                value_str = test_str[8:] if test_str[7] == '=' else test_str[9:]
                value_str = value_str.strip()
                if value_str.isdecimal():
                    # test for timeout = number in seconds
                    status.set_timeout(int(value_str))  # The port serves as the seconds: timeout=30

            # A blokchain get
            elif test_str.startswith('dbms') or test_str.startswith("table"):
                if dest_keys:
                    dest_keys += ','
                dest_keys += entry.strip()  # keys on the blockchain representing destinations

            elif test_str.startswith("blockchain get "):
               dest_keys = entry.strip()  # keys on the blockchain representing destinations
               if not " bring" in dest_keys:
                   dest_keys += " bring.ip_port"

            elif test_str[0] == '(' or re.search(nodes_pattern_, test_str): # re.search() finds a match at the start of the string
                # This is a case of a shortcut - without specifying blockchain get and without bring
                # Example: operator where [name] == my_operator
                if " bring" in entry:
                    dest_keys = f"blockchain get {entry}"
                else:
                    dest_keys = f"blockchain get {entry} bring.ip_port"
            else:
                ip_port_list.append(entry)

        if len(ip_port_list):
            # manage a list of IPs and Ports

            entries = len(ip_port_list)

            counter = 0
            counter_instruct = 0        # instructions Like timeout=30,subset=true

            for x in range(entries):
                if ip_port_list[x] != "":
                    ips_ports = params.get_value_if_available(ip_port_list[x])
                    ips_ports = ips_ports.strip()

                    main_list = ips_ports.split(',')       # Split by comma

                    # store non duplicate IP and Port in a list - ip_port_values
                    for one_ip_port in main_list:
                        tmp_list = re.split(r'[:=\s]', one_ip_port)  # Split by space or colon or equal
                        new_list = [entry for entry in tmp_list if entry != ''] # Remove empty strings
                        if len(new_list) != 2:
                            status.add_error("Wrong IP and port value: '%s'" % one_ip_port)
                            ret_val = process_status.ERR_dest_IP_Ports
                            break

                        counter += 2
                        ip = params.get_value_if_available(new_list[0])
                        port= params.get_value_if_available(new_list[1])

                        try:
                            index = ip_port_values[::][0].index(ip)  # find if IP exists
                        except:
                            index = -1
                        if index > -1:  # IP is in the list
                            if ip_port_values[index][1] == port:
                                ip = ""
                                continue  # avoid multiple same IP and port
                        ip_port_values.append([ip, port, "", "", True, 0])
                    if ret_val:
                        break
                if ret_val:
                    break

                if not ret_val:
                    if (counter and (counter % 2)) or (len(ip_port_values) + counter_instruct) != counter / 2:
                        process_log.add("Error", "Destination IP and Port are not properly provided: %s" % str(new_list))
                        ret_val = process_status.ERR_dest_IP_Ports

        word_offset = 3  # next word to consider

    return [ret_val, word_offset, dest_keys, ip_port_values]


# =======================================================================================================================
# Return the offset to the command to execute
# =======================================================================================================================
def get_command_offset(cmd_words):
    if len(cmd_words) < 2 or cmd_words[1] != '=':
        return 0
    return 2  # assignment


# =======================================================================================================================
# print a command and return printable info
# =======================================================================================================================
def print_command(prefix, line_number, commnad, ret_val):

    details = get_command_trace(line_number, commnad, ret_val)
    details = f"[{prefix}] " + details
    utils_print.output(details, True)


# =======================================================================================================================
# debug a command and return printable info
# =======================================================================================================================
def get_command_trace(line_number, commnad, ret_val):
    if not line_number:
        # use thread ID
        line = "[%s] " % threading.current_thread().name
    else:
        line = "[%04u] " % line_number
    index = commnad.find('#')  # print the command without the comment
    if index > -1:
        cmd = commnad[:index].strip()
    elif commnad[-1] == '\n':
        cmd = commnad[:-1].strip()  # no new line
    else:
        cmd = commnad.strip()

    if isinstance(ret_val, int):
        if ret_val < process_status.NON_ERROR_RET_VALUE:
            outcome = " --> %s" % process_status.get_status_text(ret_val)
        else:
            outcome = " --> %s" % process_status.get_control_text(ret_val)
    else:
        outcome = " --> Undefined returned value"

    return line + cmd + outcome


# =======================================================================================================================
# Register a running script, place the thread id in debug_info
# =======================================================================================================================
def register_script(file_name):
    thread_name = threading.current_thread().name

    script_mutex.acquire()

    if thread_name not in running_scripts.keys():
        # first script of this thread
        scripts_array = []  # An array maintaining  the stack of the scripts for this thread
        running_scripts[thread_name] = scripts_array
    else:
        scripts_array = running_scripts[thread_name]

    if len(scripts_array) < 10:
        # limit the growth - if a thread terminated because of an error without unregister
        debug_info = DebugScript()
        debug_info.set_script_name(file_name)
        scripts_array.append(debug_info)
    else:
        debug_info = None

    script_mutex.release()

    return debug_info


# =======================================================================================================================
# unregister a script
# =======================================================================================================================
def unregister_script(file_name):
    thread_name = threading.current_thread().name

    script_mutex.acquire()

    if thread_name in running_scripts.keys():
        scripts_array = running_scripts[thread_name]
        scripts_counter = len(scripts_array)  # size of the stack
        if scripts_counter:
            # take the last from the stack
            debug_info = scripts_array[scripts_counter - 1]
            if debug_info.get_script_name() == file_name:
                del scripts_array[scripts_counter - 1]
                if scripts_counter == 1:
                    del running_scripts[thread_name]  # delete the thread from the registry
            else:
                pass  # this is an error
        else:
            pass  # this is an error
    else:
        pass  # this is an error

    script_mutex.release()


# =======================================================================================================================
# Get the goto line number, or return -1
# =======================================================================================================================
def get_goto_line(status, name_to_line, goto_name):
    if goto_name in name_to_line.keys():  # map name to the offset in the script
        return name_to_line[goto_name]

    status.add_error("Script includes goto command without declared destination: '%s'" % goto_name)
    return -1  # not found

# =======================================================================================================================
# List of threads running scrips and their info
# =======================================================================================================================
def get_user_threads(status, io_buff_in, cmd_words, trace):
    info = "Thread ID       Script\r\n" \
           "--------------- -----------------------\r\n"

    for key, scripts_array in running_scripts.items():
        for entry in scripts_array:
            info += (key.ljust(15)[:15] + " ")
            info += (entry.get_script_name() + "\r\n")

    return [process_status.SUCCESS, info]

# =======================================================================================================================
# Test if thread stopped by user
# =======================================================================================================================
def is_thread_stopped():
    thread_name = threading.current_thread().name
    if thread_name in running_scripts.keys():
        scripts_array = running_scripts[thread_name]
        if scripts_array[-1].is_stop():
            utils_print.output("Thread executing script '%s' stopped" % scripts_array[-1].get_script_name(), True)
            return True  # flagged to be terminated
    return False

# =======================================================================================================================
# Get an ID for the provided JSON file
# Blockchain get id [json]
# =======================================================================================================================
def blockchain_get_id(status, cmd_words):
    offset = get_command_offset(cmd_words)
    if len(cmd_words) != 4 + offset:
        return process_status.ERR_command_struct

    ret_val = process_status.SUCCESS

    json_data = params.get_value_if_available(cmd_words[3 + offset])
    json_data = params.json_str_replace_key_with_value(json_data)  # replace JSON keys with values from dictionary
    info_struct = utils_json.str_to_json(json_data)  # map string to dictionary
    if isinstance(info_struct, list) and len(info_struct) == 1:
        json_dictionary = info_struct[0]  # only one instance in the list - use the instance
    else:
        json_dictionary = info_struct
    if not isinstance(json_dictionary, dict):
        return process_status.ERR_wrong_json_structure

    if json_dictionary:
        id = utils_json.get_object_id(json_dictionary)
        if not id:
            # NO id in the object - calculate ID
            json_string = str(json_dictionary)
            id = utils_data.get_string_hash('md5', json_string, None)
        if not offset:
            utils_print.output(id, True)
        else:
            params.add_param(cmd_words[0], id)
    else:
        ret_val = process_status.ERR_wrong_json_structure

    return ret_val


# =======================================================================================================================
# Test if a process is in debug mode
# =======================================================================================================================
def is_process_debug(process_name):
    global code_debug
    if process_name in code_debug.keys():
        ret_val = True
    else:
        ret_val = False
    return ret_val


# -------------------------------------------------------------------------------
# Copy files in a directory from one machine to another
# Example: run client ip:port directory copy source_dir dest_dir where file_type = x and hash value = y
# -------------------------------------------------------------------------------
def copy_dir(status, ip_port_list, cmd_words, word_offset, trace):
    words_count = len(cmd_words)
    if words_count <= word_offset:
        # needs to have source dir
        return process_status.ERR_command_struct

    source_dir = params.get_value_if_available(cmd_words[word_offset])  # Source directory on current node
    if source_dir[-1] != '\\' and source_dir[-1] != '/':
        source_dir += params.get_path_separator()

    if words_count <= (word_offset + 1):
        # no dest directory - same name as source
        dest_dir = source_dir
        offset_where = word_offset + 1
    elif cmd_words[word_offset + 1] == "where":
        dest_dir = source_dir
        offset_where = word_offset + 1
    else:
        offset_where = word_offset + 2
        dest_dir = params.get_value_if_available(cmd_words[word_offset + 1])  # Dest directory on dest node
        if dest_dir[-1] != '\\' and dest_dir[-1] != '/':
            dest_dir += params.get_path_separator()

    if words_count > offset_where:
        if cmd_words[offset_where] != "where":
            return process_status.ERR_command_struct

    ret_val, files_list = get_files_in_dir(status, cmd_words, words_count, word_offset, offset_where)

    if not ret_val:
        # transfer file by file
        for file in files_list:
            file_source = source_dir + file
            file_dest = dest_dir + file
            ret_val = transfer_file(status, ip_port_list, file_source, file_dest, 0, trace, "CMD: Copy Dir", True)
            if ret_val:
                break

    return ret_val


# -------------------------------------------------------------------------------
# Copy a file from one machine to another
# Example: run client ip:port copy file source_file dest_file
# -------------------------------------------------------------------------------
def copy_file(status, ip_port_list, cmd_words, word_offset, trace):
    words_count, file_source = concatenate_words(cmd_words, word_offset, False)  # get the source file name to process
    if words_count == -1:  # not structured well
        return process_status.ERR_command_struct  # return "Command error - usage: ..."

    offset_second = word_offset + 1 + words_count  # offset second file name
    words_count, file_dest = concatenate_words(cmd_words, offset_second, False)  # get the dest file name to process
    if words_count == -1:  # not structured well
        return process_status.ERR_command_struct  # return "Command error - usage: ..."

    offset_flag = offset_second + 1 + words_count  # the offset to a flag name
    if len(cmd_words) > offset_flag:
        words_count, flag_name = concatenate_words(cmd_words, offset_flag, True)  # get the dest file name to process
        if words_count == -1:  # not structured well
            return process_status.ERR_command_struct  # return "Command error - usage: ..."
        if flag_name == "ledger":
            flag = message_header.LEDGER_FILE
        elif flag_name == "watch_dir":  # write the file at the watch dir of dest machine
            flag = message_header.GENERIC_USE_WATCH_DIR
        else:
            status.add_keep_error("Wrong flag provided to 'copy' command")
            return process_status.ERR_command_struct
    else:
        flag = 0

    if file_source[-1] == '*':
        # Copy multiple files
        if not params.is_directory(file_dest):
            status.add_error("Destination directory is missing a separator (%s) at the end of the directory name" % params.get_path_separator())
            ret_val = process_status.Err_dir_name
        else:
            src_dir, src_name, src_type = utils_io.extract_path_name_type(file_source)
            name_prefix, type_prefix = utils_io.get_name_type_prefix(src_name, src_type)
            ret_val = copy_multiple_files(status, ip_port_list, src_dir, name_prefix, type_prefix, file_dest, trace, "Copy files %s.%s" % (name_prefix, type_prefix))
    else:
        # Copy one file
        ret_val =  transfer_file(status, ip_port_list, file_source, file_dest, flag, trace, "CMD: Copy File", True)

    return ret_val


# -------------------------------------------------------------------------------
# Copy files from one machine to another
# If consider_err is False - ignore failures of send (continue with a next node)
# -------------------------------------------------------------------------------
def transfer_file(status, ip_port_list, file_source, file_dest, flag, trace, caller_code, consider_err):
    if not utils_io.is_path_exists(file_source):
        error_msg = "Input file does not exists: " + file_source
        status.add_keep_error(error_msg)
        return process_status.ERR_file_does_not_exists

    ret_val = process_status.SUCCESS

    for ip_port in ip_port_list:  # send message to all servers on the list

        ip = params.get_value_if_available(ip_port[0])
        if isinstance(ip_port[1], str):
            port = params.get_value_if_available(ip_port[1])
        else:
            port = ip_port[1]

        try:
            port_val = int(port)
        except:
            status.add_keep_error("Port value is not valid: '%s'" % str(port))
            ret_val = process_status.ERR_dest_IP_Ports
            break


        if trace:
            utils_print.output("\r\n[File Transfer: %s] [Destination: %s:%u] [File: %s]" % (caller_code, ip, port_val, file_source), True)

        # Node authentication - sign a message such that the receiver node can authenticate the sender and the authorization
        ret_val, auth_str = version.al_auth_get_transfer_str(status, net_utils.get_external_ip_port())
        if ret_val:
            break

        if not net_client.file_send(status, ip, port_val, file_source, None, file_dest, flag, trace, consider_err, auth_str):
            if consider_err:
                error_msg = f"[{caller_code} Error] [Failed to send file to server: {ip}:{port}] [Source File: {file_source}]"
                status.add_keep_error(error_msg)
                ret_val = process_status.ERR_network
                break

    return ret_val
# =======================================================================================================================
# consider the + sign to make a string from multiple words
# If last_words flag is True - test the size of the words array to see that the last word was considered.
# Return words_count + string. If words_count returned is - 1 --> error
# =======================================================================================================================
def concatenate_words(cmd_words, from_word, last_words):
    words_count = count_words_to_str(cmd_words, from_word)  # count the number of words seperated by + sign

    if words_count == -1:  # not structured well
        return [-1, ""]

    if len(cmd_words) <= from_word + 2 * words_count:  # not structured well
        return [-1, ""]

    if last_words:
        if len(cmd_words) != from_word + 1 + 2 * words_count:
            return [-1, ""]

    new_string = make_str_from_words(cmd_words, from_word, words_count)  # make file name from multiple words
    return [words_count, new_string]


# =======================================================================================================================
# consider the + sign to make a string
# =======================================================================================================================
def make_str_from_words(cmd_words, first_word, words_count):
    """
   Make a string from multiple worrds seperated by + sign
   :param cmd_words:
   :param first_word:
   :param words_count:
   :return:
   """
    new_string = params.get_value_if_available(cmd_words[first_word])
    counter = 0
    while (counter < words_count):
        index = first_word + (counter + 1) * 2
        new_string += params.get_value_if_available(cmd_words[index])
        counter = counter + 1

    return new_string


# =======================================================================================================================
# consider the + sign to provide the number of strings to concatenate
# =======================================================================================================================
def count_words_to_str(cmd_words, first_word):
    """
   consider the + sign to provide the number of strings to concatenate

   :param cmd_words: the list of words in command line
   :param first_word: the first word to consider in the array:
   :return: how many worrds to concatenate
   """
    last_word = len(cmd_words) - 1
    counter = 0

    while (first_word + counter * 2 < last_word):
        if cmd_words[first_word + 2 * counter + 1] != '+':
            break
        if first_word + counter * 2 + 2 > last_word:
            return -1  # nothing after +
        counter = counter + 1

    return counter


# =======================================================================================================================
# Wait for files to be added to a directory and return the list of files
# List of files is placed in the name_list array
# =======================================================================================================================
def get_files_from_dir(status, source, repeat_time, dir_path, object_type, file_types, name_list, private_process, ignore_dict, return_list):
    '''
    status - status object
    source - the name of the caller i.e.: "operator"
    repeat_time - seconds to process the dir
    dir_path - the directory to manage
    object_type - file or dir
    file_types - a list with the type of files monitored
    name_list - The output list
    private_process - a private process that is called. i.e. the operator can be configured to call streaming buffes flush
    ignore_dict - a list of file names to ignore
    return_list - a bool to determine that a list with all files that satisfy the condition is returned
    '''
    # test that a different thread is not watching on this directory and that the current thread is registered
    ret_val = register_directory_watch(dir_path)
    reply_data = None

    if not ret_val:

        while 1:
            if ignore_dict:
                # the name list needs to be emptied otherwise it can grow forever.
                # If ignore_dict is not used, name_list is reset with the caller.
                name_list.clear()
            if not utils_io.directory_walk(status, name_list, dir_path, object_type):
                status.add_error("Directory Get: Failed on path: " + dir_path)
                ret_val = process_status.ERR_process_failure
                break
            else:

                if len(name_list):  # test for 1 name or more
                    if return_list:
                        # Return a list of files
                        files_list = utils_io.update_file_list(name_list, file_types, ignore_dict, dir_path)
                        if len(files_list):
                            reply_data = files_list     # Return a list with file names
                            break
                    else:
                        # Return one file
                        file_name = utils_io.get_valid_file(name_list, file_types, ignore_dict)
                        if file_name:
                            reply_data = dir_path + file_name
                            break

                if not repeat_time:
                    status.set_warning_value(process_status.WARNING_no_files_in_dir)
                    break

                if process_status.is_exit(source):
                    ret_val = process_status.EXIT
                    break  # all threads stopped by user
                if is_thread_stopped():
                    ret_val = process_status.EXIT
                    break  # this thread stopped by user

                if private_process:
                    # A specific process to add to this process
                    is_sleep = private_process(status, dir_path)
                else:
                    is_sleep = True
                if is_sleep:
                    time.sleep(repeat_time)  # sleep and retry

        if status.is_unregister_watch_dir():  # Only repeat_commands would set the value to False
            unregister_directory_watch()  # remove registration of the directories being watched

    return [ret_val, reply_data]

# =======================================================================================================================
# Send a text message to the peer
# ======================================================================================================================
def send_display_message(status, caller_ret_val, display_cmd, io_buff_in, text_str, is_json):
    mem_view = memoryview(io_buff_in)

    ip, port = message_header.get_source_ip_port(mem_view)

    if isinstance(ip, str) and port > 0:
        message_header.set_message_format(io_buff_in, is_json)        # Flag the print format: string or json
        ret_val = send_message(caller_ret_val, status, ip, port, mem_view, display_cmd, text_str, message_header.BLOCK_INFO_COMMAND)
    else:
        ret_val = process_status.ERR_dest_IP_Ports

    return ret_val

# =======================================================================================================================
# Send a message to the server - add to the message info command + data
# Large message is a message that does not fit in a blocl
# ======================================================================================================================
def send_message(err_value, status, ip: str, port: int, mem_view: memoryview, command: str, data: str, info_type: int):
    """
    err_value - an error value that is delivered to the destination node
    status - thread status object
    ip - dest ip
    port - dest port
    mem_view - the IO buffer
    Command issued to the target node
    data - data transferred to the target node
    info_type - flags delivered to the target node. These flags are defined in message_header, for example:
            BLOCK_INFO_RESULT_SET = 1  # the info in the block is the result set of a query
    """

    # set the source IP and Port for outgoing messages
    ret_val = net_utils.set_ip_port_in_header(status, mem_view, ip, port)
    if ret_val:
        return ret_val

    # Node authentication - sign a message such that the receiver node can authenticate the sender and the authorization
    ret_val, auth_str = version.al_auth_get_transfer_str(status, net_utils.get_external_ip_port())
    if ret_val:
        return ret_val

    if command != "print" and command != "echo" and message_header.is_large_command(mem_view, command, data, auth_str):
        #  large_message -  large command is a message that does not fit in a block.
        #                         Allow to print or echo large messages - as large print/echo command have special treatment on the dest node.
        #                         Other messages return ERR_large_command and are processed like a file transfer.
        return process_status.ERR_large_command     # Send as a file command

    if ip == net_utils.get_external_ip():
        use_ip = net_utils.get_local_ip()   # Use the local IP to connect
    else:
        use_ip = ip

    soc = net_client.socket_open(use_ip, port, command, 6, 3)

    if soc:
        if net_client.message_prep_and_send(err_value, soc, mem_view, command, auth_str, data, info_type) == False:
            metadata.set_operator_status(status, ip, port, "not_responding", None)
            status.add_keep_error("Failed to send a message to destination node at: %s:%d" % (ip, port))
            ret_val = process_status.ERR_network
        else:
            metadata.set_operator_status(status, ip, port, "active", None)

        net_client.socket_close(soc)
    else:
        metadata.set_operator_status(status, ip, port, "not_responding", None)
        status.add_keep_error("Failed to create a socket using: %s:%d" % (ip, port))
        ret_val = process_status.NETWORK_CONNECTION_FAILED

    return ret_val
# =======================================================================================================================
# Get public key from the blockchain by IP and Port
# =======================================================================================================================
def get_public_str(status, ip, port):

    cmd_words = ["blockchain", "get", "*", "where", "ip", "=", ip, "and", "port", "=", str(port), "bring.recent", "[*][public_key]"]
    ret_val, public_str = blockchain_get(status, cmd_words, "", True)
    return [ret_val, public_str]

# =======================================================================================================================
# Resolve the IP and Ports from logical names
# =======================================================================================================================
def resolve_destination(status, dest_names, is_select, remote_dbms, remote_table, include_tables, query_nodes):
    """
    include_tables - additional tables to query as if these tables are remote_table
    query_nodes - with HA nodes: "main" - use the main node to query, "all" - use all nodes to query
    """

    ip_port_values = []
    ret_val = process_status.SUCCESS

    blockchain_file = params.get_value_if_available("!blockchain_file")
    if blockchain_file == "":
        status.add_error("Missing dictionary definition for \'blockchain_file\'")
        return [process_status.No_local_blockchain_file, ip_port_values]  # return empty array

    if dest_names != "":

        # take dest from the user string - user can specify (dbms=dbms_name, table=table_name ...)
        if dest_names.startswith("blockchain "):
            # Example: run client (blockchain get operator where dbms = lsl_demo bring ['operator']['ip'] ":" ['operator']['port'] separator = ",") "blockchain test"
            get_servers_by_cmd(status, dest_names, ip_port_values)
            return [ret_val, ip_port_values]

        use_metadata = False

        dbms_table = dest_names.replace(" and ", ",").replace(" ", "")
        if dbms_table.find(',') != -1:
            # multiple_names
            dest_keys = dbms_table.split(',')
            counter_entries = len(dest_keys)
            if  counter_entries >= 2:
                if len(dest_keys) == 2:
                    dest_keys.sort()
                    if dest_keys[0].startswith("dbms=") and dest_keys[1].startswith("table="):
                        use_metadata = True  # dbms and table are provided -> use metadata (if available)
        else:
            dest_keys = [dest_names]

    elif is_select or (remote_dbms and remote_table):
        # take dest from job_info
        dest_keys = ["dbms=" + remote_dbms, "table=" + remote_table]
        use_metadata = True # dbms and table are provided -> use metadata (if available)
    else:
        return [ret_val, ip_port_values]  # return empty array

    # If only dbms and table are provided - get the list from the metadata, else -> query the blockchain
    if use_metadata:
        # Load metadata (it will load only if file was changed than last call)
        ret_val = blockchain_load(status, ["blockchain", "get", "cluster"], False, 0)
        if ret_val:
            # Failed to load metadata
            if ret_val != process_status.Needed_policy_not_available:
                # If the error is Missing_script ->
                # there are no cluster definitions -> read from the blockchain operators supporting tables
                return [ret_val, ip_port_values]

    if use_metadata and metadata.is_active() and not  ret_val:
        # Use Metadata layer
        # The metadata is in memory or just loaded
        if is_select:
            # Bring one operator per Distribution
            use_main = True if query_nodes == "main" else False
            ret_val, operators = metadata.get_operators(status, dest_keys, include_tables, use_main, 0, False)
        else:
            operators = metadata.get_all_operators(status, dest_keys, include_tables)

    else:
        # Query the blockchain for operators supporting tables
        operators = blockchain.get_operators_objects(status, blockchain_file, dest_keys, include_tables)

    if operators:
        ip_port_used = {}       # Used to avoid multiple messages to the same machine
        for entry in operators:
            ip_key = f"{entry.ip}.{entry.port}.{entry.dbms}"    # one query to the same database if the logical dbms is the same
            if ip_key in ip_port_used:
                continue
            else:
                ip_port_used[ip_key] = True
            # add IP + Port + Table name + DBMS name (Table Name and DBMS Name are provided for Include tables", active - is node available, error code in message send
            ip_port_values.append([entry.ip, entry.port, entry.dbms, entry.table, entry.active, 0])
    else:
        status.add_error("Failed to retrieve operators using keys: %s" % str(dest_keys))

    return [ret_val, ip_port_values]
# =======================================================================================================================
# Get the Operators that need to receive a messgae:
# If a clusters are defined - one operator from each cluster
# Without a cluster - use the Operator ID as the cluster ID
# Return a dictionary of clusters and an array of operators for each cluster.
# =======================================================================================================================
def get_destination_operators(status, blockchain_file, company_name, dbms_name, table_name, operator_conditions):

    # Test cluster declaration for the table
    if company_name:
        cmd_get_clusters = ["blockchain", "get", "cluster", "where", "company", "=", company_name, "and", "table[dbms]", "=", dbms_name, "and", "table[name]", "=", table_name]
    else:
        cmd_get_clusters = ["blockchain", "get", "cluster", "where", "table[dbms]", "=", dbms_name, "and", "table[name]", "=", table_name]


    ret_val, clusters_list = blockchain_get(status, cmd_get_clusters, blockchain_file, True)

    by_cluster = {}
    if not ret_val:
        if len(clusters_list):
            # Get Operators using the clusters IDs
            cmd_get_operator = ["blockchain", "get", "operator", "where", "cluster", "=", None] + operator_conditions

            for cluster in clusters_list:
                if "parent" in cluster["cluster"]:
                    # This is an extension of the source cluster policy
                    cluster_id = cluster["cluster"]["parent"]
                else:
                    # This is the source cluster policy
                    cluster_id = cluster["cluster"]["id"]
                cmd_get_operator[6] = cluster_id
                ret_val, operators = blockchain_get(status, cmd_get_operator, blockchain_file, True)
                if ret_val:
                    break
                if operators:
                    by_cluster[cluster_id] = operators
        else:
            # Without clusters
            cmd_get_operator = ["blockchain", "get", "operator", "where", "dbms", "=", dbms_name, "and", "table", "=", table_name] + operator_conditions
            ret_val, operators = blockchain_get(status, cmd_get_operator, blockchain_file, True)
            if not ret_val:
                for operator in operators:
                    if "id" in operator["operator"]:
                        id = operator["operator"]["id"]
                        by_cluster[id] = [operator] # Without clusters - As a function of the operator id

    return [ret_val, by_cluster]

# =======================================================================================================================
# Destination servers are provided using a "blockchain get command"
# Call needs to be formated to return comma separated IP:Port
# Example: run client (blockchain get operator where dbms = lsl_demo bring ['operator']['ip'] ":" ['operator']['port'] separator = ",") "blockchain test"
# =======================================================================================================================
def get_servers_by_cmd(status, command_str, ip_port_values ):

    sub_str, left_brackets, right_brakets = utils_data.cmd_line_to_list_with_json(status, command_str, 0,
                                                                                  0)  # a list with words in command line
    if sub_str:
        err_val, ip_port_string = blockchain_get(status, sub_str, "", True)
        # err_val will not update ip_port_values --> caller will have empty set for destination
        if not err_val and len(ip_port_string):
            if isinstance(ip_port_string, str):  # Need t be comma separated IP:Port string
                result_list = ip_port_string.split(',')
                for entry in result_list:
                    index = entry.find(':')
                    if index and index < (len(entry) - 1):
                        # add IP + Port + Table name + Invlude DBMS name, Include Table Name, active - False means no cluster node, error code in message send
                        ip_port_values.append([entry[0:index], entry[index + 1:], "", "", True, 0])
# =======================================================================================================================
# Process REDO Query
# In the case that there is a need to bring info (from oter nodes) to the query, these queries would be executed first.
# After the leading queries are executed, the original query is issued again.
# When a leading query is issued, the status object is flagged with status.set_rerun_query() - such that the caller will
# reissue the query.
# =======================================================================================================================
def process_leading_message(status, select_parsed):
    if select_parsed.is_with_non_issued_leading_queries():
        # the process that considered the user query (in unify_results) determined that there is a need in a leading query
        message = select_parsed.init_leading_query()
        ret_val = True
    else:
        ret_val = False
        message = ""

    return [ret_val, message]


# =======================================================================================================================
# send sql query to a server
# Example: run tcp client 10.0.0.79 !port copy !test_json_in !test_json_out
# status - an object containing the proces status and error messages
# ip_port_list - A list of the IPs and Ports of the relevant Operators
# list_size - The number of entries in the list
# receivers - The number of Operators
# message - message to be send to the Operator nodes
# job_id - the entry in the job array
# unique_job_id - a sequential job counter
# job_mutexed - is the call done when j_instance.data_mutex_aquire(read or write)  is held
# =======================================================================================================================
def send_command_to_nodes(status, io_buff_in, ip_port_list: list, receivers: int, message, data, job_id, unique_job_id,
                          job_mutexed, trace):

    #      command  =  self.get_executable_command(message, None)
    mem_view = memoryview(io_buff_in)

    message_header.set_error(mem_view, 0)  # reset the error code
    message_header.set_reply_message(mem_view, False)  # false indicates this is a new message and not a reply
    message_header.reset_partitions(mem_view)  # Set 1 partition used and par ID to 0

    message_header.set_job_info(mem_view, job_id, unique_job_id)  # save the job_id where the job info is saved () and a unique id for the job

    message_header.set_counter_receivers(mem_view, receivers)  # number of nodes receiving the message

    receiver_id = 0

    if unique_job_id:
        j_instance = job_scheduler.get_job(job_id)
        is_subset = j_instance.get_job_handle().is_subset() # Ask job_handle (not status.is_subset() as status of TCP thread will not show subset flag
    else:
        j_instance = None
        is_subset = status.is_subset()

    for ip_port in ip_port_list:  # send message to all servers on the list
        ip = ip_port[0]
        if ip == "localhost":
            ip = "127.0.0.1"
        port = ip_port[1]

        if not net_utils.test_ipv4(ip, port):
            ret_val = process_status.ERR_dest_IP_Ports
        else:

            dbms_name = ip_port[2]
            if dbms_name:
                # Change the dbms name if INCLUDE referenced a different dbms for this call
                message = update_sql_msg(dbms_name, ip_port[3], message)

            # set the source IP and Port for outgoing messages - Note: can be different source depending if dest is external or local network
            ret_val = net_utils.set_ip_port_in_header(status, mem_view, ip, int(port))
            if not ret_val:

                message_header.set_receiver_id(mem_view, receiver_id)  # Set the receiver id (node x out of y receivers)
                if unique_job_id:
                    j_instance.set_receiver_info(receiver_id, ip, port)  # save the ip and port of the nodes receiving the message

                if trace:
                    from_ip, from_port = message_header.get_source_ip_port(mem_view)
                    utils_print.output_box("(%s:%s) --> (%s:%s) Command: '%s' ..." % (from_ip, from_port, ip, port, message[:20]))
                    utils_print.output("\rSend .....", False)

                if not ip_port[4] or ip_port[5]:
                    # ip_port[4] if FALSE - is the case in a query without a node supporting a cluster (and subset is set to True in the query destination)
                    # ip_port[5] is the error value in a send message. For example, with a leading query, no point to message a node that did not get the first message
                    ret_val = process_status.Operator_not_accessible
                else:
                    ret_val = send_message(0, status, ip, int(port), mem_view, message, data,
                                   message_header.BLOCK_INFO_AUTHENTICATE)  # If authentication is enabled - requires user authentication

                    if ret_val == process_status.ERR_large_command:
                        # Send as a file message
                        ret_val = net_client.large_message(status, ip, int(port), mem_view, message, data)

                if trace:
                    if ret_val:
                        utils_print.output("..... Failed", True)
                    else:
                        utils_print.output("..... OK", True)

        if ret_val != process_status.SUCCESS:
            # Ret_val will be changed to SUCCESS if subset (allowing subset of replies)
            ret_val = msg_send_failed(status, False, is_subset, ret_val, ip_port, j_instance, unique_job_id, receiver_id, job_mutexed)
            if ret_val:
                break   # An error an not a subset
            ret_val = process_status.SUCCESS

        receiver_id += 1

    return ret_val

# =======================================================================================================================
# Update the message to the target node by the dbms name
# =======================================================================================================================
def update_sql_msg(dbms_name, table_name, sql_stmt):
    # Change the dbms name if INCLUDE referenced a different dbms for this call
    if sql_stmt.startswith("sql "):
        message = utils_sql.update_dbms_table_names(dbms_name, table_name, sql_stmt)
    elif sql_stmt.startswith("info table "):
        if table_name:
            message = "info table " + dbms_name + " " + table_name + " " + sql_stmt.rsplit(' ', 2)[-1]
        else:
            message = sql_stmt
    else:
        message = sql_stmt

    return message
# =======================================================================================================================
# Process Error of a send message - flag the job instance with the info as f(target node)
# If subset allow - change ret_val to SUCCESS
# =======================================================================================================================
def msg_send_failed(status, is_async_io, is_subset, msg_ret_val, ip_port, j_instance, unique_job_id, receiver_id, job_mutexed):
    '''
    status - main thread status object
    is_async_io - using async_io to send messages
    is_subset - configured to continue when not all target nodes are available
    msg_ret_val - ret_val returned from the send process
    j_instance - object organizing the send info
    unique_job_id - identifying the jon process
    receiver_id - identifying the target node
    job_mutext - mutex status of  j_instance
    '''

    ip_port[5] = msg_ret_val  # Keep the return value with the error code

    if msg_ret_val == process_status.NETWORK_CONNECTION_FAILED:
        # Failed to create a socket
        err_message = "Connection Failed"
    elif msg_ret_val == process_status.Operator_not_accessible:
        err_message = "Operator not accessible"
    else:
        err_message = "Send Failed"

    ret_val = msg_ret_val       # Keep err_msg
    if unique_job_id:
        j_instance.set_job_status(err_message)
        # Continue even if some of the nodes are not accessible
        if not job_mutexed:
            # if called from next_query() - it is done when j_instance.data_mutex_aquire is TAKEN
            integrate_print_reply(status, is_async_io, is_subset, False, None, unique_job_id, j_instance, receiver_id, msg_ret_val, False, "Not responding")
        else:
            j_instance.set_par_ret_code(receiver_id, 0, process_status.NETWORK_CONNECTION_FAILED)  # Because mutex is taken
            j_instance.count_nodes_replied()  # Count the number of nodes replied and return TRUE is all replied
        if is_subset:
            ret_val = process_status.SUCCESS
    elif not status.is_rest_wait():
        # Message to stdout
        if is_subset:
            # Error to one node
            utils_print.output("\n\r[Message to Node %s:%s] [%s]" % (ip_port[0], ip_port[1], err_message), True)
            ret_val = process_status.SUCCESS


    return ret_val
# =======================================================================================================================
# "system_query" is the local database that keeps the query results that are returned from different operators.
# For a SQL query - prepare the info to interact with the local and remote database.
# The local database keeps the returned results from the remore databases (remore databases process the remore_query).
# When all the databases returned results, the local database is queried with the local query.
# =======================================================================================================================
def create_local_table(status, job_id, is_delete_table):
    # The call to set_job returns True for a SQL query

    ret_val = process_status.SUCCESS
    if db_info.is_dbms("system_query"):

        j_instance = job_scheduler.get_job(job_id)

        if is_delete_table:
            local_table = j_instance.get_local_table()
            command = "drop table if exists %s;" % local_table
            ret_val = db_info.non_select_sql_stmt(status, "system_query", command)
            if not ret_val:
                local_create = j_instance.get_local_create()
                ret_val = db_info.non_select_sql_stmt(status, "system_query", local_create)
                if not ret_val:
                    # this is a temp table which does not need recovery options
                    command = db_info.commands_for_temp_table("system_query", local_table)
                    if command:
                        # this call is dbms specific
                        ret_val = db_info.non_select_sql_stmt(status, "system_query", command)
    else:
        status.add_keep_error("Local query database not available: 'system_query'")
        ret_val = process_status.ERR_dbms_not_opened

    return ret_val

# =======================================================================================================================
# Send messages from this client to a server
# Example: run client 10.0.0.79 !port copy !test_json_in !test_json_out
# =======================================================================================================================
def run_client(status, io_buff_in, cmd_words, trace):

    if cmd_words[1] == '=':
        assignment = cmd_words[0]       # Assign reply to variable
        words_list = cmd_words[2:]
    else:
        assignment = None
        words_list = cmd_words

    words_count = len(words_list)

    if words_count == 3:
        # set remote nodes as the target of the CLI
        return node_info.set_assigned_cli(words_list[2])

    if words_count < 4:
        return process_status.ERR_command_struct

    ret_val, word_offset, dest_names, ip_port_values = get_command_ip_port(status, words_list)
    if ret_val:
        return ret_val

    if (words_list[word_offset] == "file" or words_list[word_offset] == "directory") and words_count > (
            word_offset + 1) and words_list[word_offset + 1] == "copy":  # read from file
        if dest_names != "":
            ret_val, ip_port_values = resolve_destination(status, dest_names, False, None, "main")
            if ret_val:
                return ret_val
            if not len(ip_port_values):
                process_log.add("Error", "Failed to determine destination IPs and Ports")
                return process_status.ERR_dest_IP_Ports

        if words_list[word_offset] == "file":
            ret_val = copy_file(status, ip_port_values, words_list, word_offset + 2, trace)
        else:
            # copy directory
            ret_val = copy_dir(status, ip_port_values, words_list, word_offset + 2, trace)
    else:

        ret_val = send_command_message(status, io_buff_in, words_list, word_offset, dest_names, ip_port_values, None, 0,
                                       "", assignment, trace)

    return ret_val

# =======================================================================================================================
# Send a message to a different node
# Destination jon is a job instance that is updated with this qiery data
# =======================================================================================================================
def send_command_message(status, io_buff_in, cmd_words, word_offset, dest_names, ip_port_values, dest_job,
                         input_query_id, data, assignment, trace):

    '''
    status - user object status
    io_buff_in - network message buffer
    cmd_words - user command
    word_offset - offset to  + sign between words to concatenate
    dest_names - blockchain command, or dbms and table
    ip_port_values - list of ip and port
    dest_job - (nested query) In the case of a query that issued a new query - the job instance of the original query
    input_query_id - (nested query) An ID representing the new query that was issued
    data - (nested query) Data associated with the nested query process
    assignment - assign result set to a key in the dictionary
    trace - trace level
    '''
    words_count, message = concatenate_words(cmd_words, word_offset, True)  # get the source file name to process
    if words_count == -1:  # not structured well
        message = params.get_translated_string(cmd_words, word_offset, 0, True)


    ret_val, destinations, dbms_name, conditions, cmd_str = preprocess_command(status, cmd_words, 2)
    if ret_val:
        return ret_val

    ret_val, is_select, sql_str, offset_select = utils_sql.get_select_stmt(status, cmd_str)
    if ret_val:
        return ret_val

    send_to_peers = True
    list_size = len(ip_port_values)

    if is_select:
        # Select statements create a job_instance to contain the replies

        # =========================================
        # Init Job instance to receive reply info
        # to the job (i.e. query) from the participating nodes
        # =========================================
        if conditions and "file" in conditions:
            # Output of queries goes to file - delete file if exists
            file_name = conditions["file"][0]
            if file_name[-1] != '*':     # If '*' - system will determine a unque file name
                utils_io.delete_file(file_name, False)  # Delete file and ignore error

        job_id, unique_job_id = job_scheduler.start_new_job()  # Sets the JOB ACTIVE Flag = needs to turn off with an error
        if job_id == -1:
            status.add_error("Failed to allocate resources for task - too many concurrent open tasks")
            return process_status.No_resources
        # When sending a message to a different node, the info is moved from the status object to the job instance object
        # set the job handle on the job instance --> by the thread that issues the process
        # 1) get the job handle from the user that issued the command
        j_handle_user = status.get_job_handle()  # Get the instructions of how to run the job and place it in job instance
        # 2) get the job instance from the scheduler
        j_instance = job_scheduler.get_job(job_id)
        j_instance.set_unique_job_id(unique_job_id)
        status.set_unique_job_id(unique_job_id)
        # 3) get the job handle of the job instance
        j_handle_instance = j_instance.get_job_handle()
        # 4) process request

        j_handle_user.set_conditions(conditions)

        j_instance.save_src_cmd(cmd_str)
        j_instance.set_job_type(is_select)
        select_parsed = j_instance.get_job_info().get_select_parsed()
        select_parsed.reset(True, True)
        if "table" in conditions:
            # Change the name of the local table created
            local_table_name = interpreter.get_one_value(conditions, "table")
            select_parsed.set_task_table_name(local_table_name)

        # Parse the SQL
        ret_val, formated_sql = utils_sql.format_select_sql(status, sql_str, offset_select, select_parsed)
        if ret_val:
            # Error in the SQL
            j_instance.set_not_active(True)  # flag the job instance is not active with local error
            return ret_val

        if not dbms_name:
            j_instance.set_not_active(True)  # flag the job instance is not active with local error
            status.add_keep_error("Missing database name in SQL statement")
            return process_status.Missing_dbms_name
        # Test if the node has tabloe definitions or view definitions
        is_valid = select_parsed.parse_sql(status, dbms_name, formated_sql)
        if is_valid:
            table_name = select_parsed.get_table_name()
            if not db_info.is_table_defs(dbms_name, table_name):
                if not db_info.is_view_defs(dbms_name, table_name):
                    # No table definitions and no view definitions
                    # Load table defs from the blockchan data
                    ret_val = get_blockchain_table_info(status, None, dbms_name, table_name, "columns")
                    if ret_val:
                        j_instance.set_not_active(True)  # flag the job instance is not active with local error
                        return ret_val
            select_parsed.set_target_names(dbms_name, table_name)

            if "extend" in conditions:
                # Extend the column values returned in the query with the extended values
                select_parsed.set_extended_columns(conditions["extend"])

            is_valid = utils_sql.process_projection(status, select_parsed)
            if is_valid:
                is_valid = unify_results.make_sql_stmt(status, select_parsed, False)
        if not is_valid:
            # error with this query
            j_instance.set_not_active(True)  # flag the job instance is not active with local error
            return process_status.Failed_to_parse_sql
    else:
        table_name = ""
        if status.is_rest_wait() or assignment:
            # A REST caller is on wait for a reply
            # Or assign the reply to a variable
            job_id, unique_job_id = job_scheduler.start_new_job()  # Sets the JOB ACTIVE Flag = needs to turn off with an error
            if job_id == -1:
                status.add_error("Failed to allocate resources for task - too many concurrent open tasks")
                return process_status.No_resources
            # 1) get the job handle from the user that issued the command
            j_handle_user = status.get_job_handle()  # Get the instructions of how to run the job and place it in job instance
            # 2) get the job instance from the scheduler
            j_instance = job_scheduler.get_job(job_id)
            j_instance.set_unique_job_id(unique_job_id)
            status.set_unique_job_id(unique_job_id)
            # 3) update the job instance with the instructions including the proper job handle
            j_handle_instance = j_instance.get_job_handle()
            status.set_active_job_handle(job_id, j_handle_instance)
            # 6) Set a wait event
            j_handle_instance.set_wait_event()

            if assignment:
                # Returned info is assigned to a key in the dictionary
                j_handle_user.set_assignment(assignment)
                params.key_to_job(assignment, job_id, unique_job_id)    # Keep a link from the dictionary to the data in the job

        else:
            job_id = 0         # No job_instance on the node
            unique_job_id = 0   # No job_instance on the node

        send_to_peers = True


    if not list_size:

        include_tables = conditions.get("include", None) # Additional tables to participate in the query
        query_nodes = conditions.get("nodes", "main") # set with "main" or "all"
        if isinstance(query_nodes,list):
            query_nodes = query_nodes[0]

        ret_val, ip_port_values = resolve_destination(status, dest_names, is_select, dbms_name, table_name, include_tables, query_nodes)
        if ret_val:
            return ret_val
        list_size = len(ip_port_values)
        if not list_size:
            if is_select:
                j_instance.set_not_active(True)  # flag the job instance is not active with local error
                ret_val = mising_ip_error(status, is_select, formated_sql, dest_names, dbms_name, table_name)
            else:
                ret_val = process_status.Failed_to_determine_dest  # Failed to determine destination node
            return ret_val

    # PREPARE SELECT STATEMENTS
    if is_select:  # test if a select statement
        j_instance.set_reply_buffers(list_size)  # set the buffers to maintain the reply

        # 5) update the job instance with the instructions including the proper job handle
        j_instance.set_job(list_size, j_handle_user)  # save the number of nodes participating and the sql message in the active job array

        if query_mode["show_id"]:
            # Display the Query ID
            if j_handle_instance.is_stdout() and not j_handle_instance.is_rest_caller():
                # print the query ID
                utils_print.output("[%u]" % j_handle_instance.get_job_id(), True)

        # 5) keep the active job handle on the status object of this thread
        status.set_active_job_handle(job_id, j_handle_instance)
        # 6) Set a wait event
        j_handle_instance.set_wait_event()

        j_instance.set_job_status("Receiving Data")

        j_instance.set_output_job(dest_job,
                                  input_query_id)  # This job_instance is a query that serves as an input to the destination job

        if status.get_task_id():        # Value 0 means not from scheduler
            # this is a scheduled SELECT job to execute
            sched_id = status.get_scheduler_id()
            task_id = status.get_task_id()
            task = task_scheduler.get_task(sched_id, task_id)
            is_table_created = task.is_table_created()  # only once returns False
            if is_table_created:
                is_delete_table = False  # first query - delete the table
            else:
                is_delete_table = True
                task.set_table_created()  # flag that the table was created - scheduled queries will not recreate

        else:
            # delete old table and create a new table every query
            is_delete_table = j_instance.is_drop_local_table()


        j_instance.save_ip_port_values(ip_port_values)

        # create local table for result set
        ret_val = create_local_table(status, job_id, is_delete_table)
        if ret_val:
            j_instance.set_not_active(True)  # flag the job instance is not active with local error
            return ret_val

        is_leading, new_message = process_leading_message(status, select_parsed)
        if is_leading:
            # send leading message
            message = new_message  # The Leading Message
        else:
            # Get the source string - but with time set by this node (for example now() function is determined on the sender)
            message = get_generic_query(status, dbms_name, conditions, select_parsed)
            if select_parsed.is_with_non_issued_input_queries():
                # Issue a different query that serves as an input to the current query
                ret_val, data = issue_input_queries(status, io_buff_in, j_instance, select_parsed, trace)
                if not data:
                    send_to_peers = False  # The query to bring the joined data was send in issue_input_queries
    elif unique_job_id:
        j_instance.set_reply_buffers(list_size)  # set the buffers to maintain the reply
        j_instance.set_job(list_size, j_handle_user)  # save the number of nodes participating and the sql message in the active job array
        j_instance.save_src_cmd(cmd_str)
        j_instance.set_job_type(is_select)

    if not ret_val:

        if send_to_peers:
            if is_select:
                j_instance.set_query_processig_info()  # set info how query should be processed
                j_instance.send_to_target(False)       # Flag that messages were not yet send to target

            ret_val = send_command_to_nodes(status, io_buff_in, ip_port_values, list_size, message, data, job_id,
                                            unique_job_id, False, trace)

            if is_select:
                j_instance.send_to_target(True)       # Flag that messages were send to target (or flagged as targe not available)
    if ret_val:
        if is_select:
            j_instance.set_job_status("Send Failed")
            # Error or no reply to this message - flag that the job object can be reused
            j_instance.set_not_active()

    return ret_val

# =======================================================================================================================
# Get the source query updated with the time set by this node (for example now() function is determined on the sender)
# =======================================================================================================================
def get_generic_query(status, dbms_name, conditions, select_parsed):

    instructions = ""

    if conditions:
        for key, values in conditions.items():
            for one_value in values:
                if instructions:
                    instructions += " and"
                instructions += " %s = %s" % (key, one_value)


    message = "sql %s%s %s" % (dbms_name, instructions, select_parsed.get_generic_query().replace(' ', '\t'))
    return message

# =======================================================================================================================
# Determine the error when the  IP and Port are not available
# Example dest_names: (dbms=dbms_name, table=table_name ...)
# =======================================================================================================================
def mising_ip_error(status, is_select, formated_sql, dest_names, dbms_name, table_name):

    search_keys = ""
    if dest_names:
        search_keys += " Keys: %s" % str(dest_names)
    if dbms_name:
        search_keys += " DBMS: %s" % str(dbms_name)
    if table_name:
        search_keys += " Table: %s" % str(table_name)

    if search_keys:
        search_keys = " - Using" + search_keys

    if is_select:
        command = "select"
    else:
        command = "AnyLog"
        if formated_sql:
            index = formated_sql.find(" ")
            if index > 0:
                command = formated_sql[:index]

    message = ("Failed to identify destination nodes for '%s' command" % command) + search_keys

    status.add_keep_error(message)

    return process_status.Failed_to_determine_dest  # Failed to determine destination node

# =======================================================================================================================
# 1) determine the command destinations
# 2) Identify the dbms name, the processing instructions and the SQL command
# =======================================================================================================================
def preprocess_command(status, cmd_words, offset):
    # Collect the destinations of the command
    words_count = len(cmd_words)
    # find destinations
    index = offset
    if cmd_words[index][0] == '(':
        # find destination in brackets
        while index < words_count:
            if cmd_words[index][-1] == ')':
                break
        if index == words_count:
            return [process_status.ERR_command_struct, "", None, "", ""]
        destinations = ' '.join(cmd_words[offset:index + 1])
    else:
        # only 1 IP and port in the format: IP:PORT
        destinations = cmd_words[index]

    index += 1  # set on instructions

    ret_val, dbms_name, conditions, cmd_str = get_sql_processing_info(status, cmd_words, words_count, index)

    return [ret_val, destinations, dbms_name, conditions, cmd_str]


# =======================================================================================================================
# Identify the dbms name, the processing instructions and the SQL command
# =======================================================================================================================
def get_sql_processing_info(status, cmd_words, words_count, index):
    global cmd_instructions
    global format_values
    global dest_values
    global nodes_values
    global sql_commands
    # Get the DBMS name in SQL statements
    if cmd_words[index] == "sql":  # Each word is an entry in the cmd_words arrau
        if index + 2 >= words_count:
            status.add_error("SQL statement details are not available with command: %s" % ' '.join(cmd_words))
            return [process_status.ERR_command_struct, None, "", ""]
        # the case of "sql", "dbms_name", "text"
        dbms_name = params.get_value_if_available(cmd_words[index + 1])
        index += 2

        # find the select statement to determine how many words to consider
        for select_offset in range(index, words_count, 1):
            word = cmd_words[select_offset].lower()
            sql_word = word.split(' ', 1)[0]
            if sql_word in sql_commands:
                break

        info_words = cmd_words
        info_offset = index
        info_count = select_offset

        index = select_offset  # set index on the SQL

    elif cmd_words[index][:4] == "sql ":  # The SQL statement is one entry in the array
        # the case of "sql dbms_name text"
        info_words = cmd_words[index].split()
        info_count = len(info_words)
        info_offset = 2
        if info_count < 2:
            status.add_error("Failed to identify the database name that follows the SQL key in the SQL statement: %s" % ' '.join(cmd_words))
            return [process_status.ERR_command_struct, None, "", ""]
        dbms_name = params.get_value_if_available(info_words[1])

        index += 1  # set index on the SQL
    else:
        # Not a SQL stmt
        dbms_name = ""

    # Get the command statement
    if words_count <= index:
        return [process_status.ERR_command_struct, None, "", ""]
    if cmd_words[index] == '+':
        index += 1
        if words_count <= index:
            return [process_status.ERR_command_struct, None, "", ""]

    if words_count == (index + 1):
        # SQL in a single word
        if utils_data.is_with_double_quotation(cmd_words[index]):
            cmd_str = cmd_words[index][1:-1]  # Remove the quotations
        else:
            cmd_str = cmd_words[index]
    else:
        cmd_str = ' '.join(cmd_words[index:])

    ret_val = process_status.SUCCESS
    conditions = {}

    if dbms_name:
        # Get the instructions
        if (info_offset + 1) < info_count:
            ret_val, counter, conditions = interpreter.get_dict_from_words(status, info_words, info_offset, info_count,
                                                                           cmd_instructions, False)

            if not ret_val:
                ret_val = interpreter.test_values(status, conditions, "format", format_values)
                if not ret_val:
                    ret_val = interpreter.test_values(status, conditions, "dest", dest_values)
                    if not ret_val:
                        ret_val = interpreter.test_values(status, conditions, "nodes", nodes_values)

    return [ret_val, dbms_name, conditions, cmd_str]


# =======================================================================================================================
# Set configuration params
# =======================================================================================================================
def _process_reset(status, io_buff_in, cmd_words, trace):
    global workers_pool
    global echo_queue

    if len(cmd_words) < 2:
        return process_status.ERR_command_struct

    if cmd_words[1] == "message":  # a tcp sql message
        mem_view = memoryview(io_buff_in)
        command = message_header.get_command(mem_view)
        msg_words = command.split()
        words_array = msg_words
    else:

        words_array = cmd_words

    words_count = len(words_array)
    if words_count < 2:
        return process_status.ERR_command_struct

    if words_count < 3:
        if not cmd_words[1] in _reset_methods:
            return process_status.ERR_command_struct
        if not "key_only" in _reset_methods[cmd_words[1]]:
            return process_status.ERR_command_struct

    ret_val = _exec_child_dict(status, commands["reset"]["methods"], commands["set"]["max_words"], io_buff_in, words_array, 1, trace)[0]  #

    return ret_val
# =======================================================================================================================
# Set configuration params
# =======================================================================================================================
def _process_set(status, io_buff_in, cmd_words, trace):
    global workers_pool
    global echo_queue

    words_count = len(cmd_words)
    if words_count < 2:
        ret_val = process_status.ERR_command_struct
    else:
        if cmd_words[1] == "message":  # a tcp sql message
            mem_view = memoryview(io_buff_in)
            command = message_header.get_command(mem_view)
            msg_words = command.split()
            words_array = msg_words
            words_count = len(words_array)
        else:

            words_array = cmd_words

        if words_count < 2:
            ret_val = process_status.ERR_command_struct
        elif words_count == 4 and words_array[2] == '=':
            # assignment - this is needed as asignments when keys are command terms will not work: default_dbms=test  ==> returns an error
            key = words_array[1]
            value = words_array[3]
            if len(key) > 1 and key[0] == '$':
                # set environment variable
                params.set_env_var(key, value)
            else:
                value = words_array[3]
                if len(value) > 1 and value[0] == '!':
                    value = params.get_value_if_available(value)
                elif len(value) > 1 and value[0] == '$':
                    value = params.get_env_var( value )
                params.add_param(words_array[1], value)
            ret_val = process_status.SUCCESS
        else:
            ret_val = _exec_child_dict(status, commands["set"]["methods"], commands["set"]["max_words"], io_buff_in, words_array, 1, trace)[0]  #

    return ret_val
# =======================================================================================================================
# Set the default query parameters
# Example: set query mode using timeout = 30 seconds and max_volume = 2MB and send_mode = all and reply_mode = any
# =======================================================================================================================
def set_query_mode(status, io_buff_in, cmd_words, trace):
    global query_mode

    #                             Must     Add      Is
    #                             exists   Counter  Unique
    keywords = {"timeout": ("int.time", False, False, True),
                "max_volume": ("int.storage", False, False, True),
                "send_mode": ("str", False, False, True),  # Query will be executed only if all servers are connected
                "reply_mode": ("str", False, False, True),  # Determine if wait for reply from all servers
                "show_id": ("bool", False, False, True),  # Show the sql id
                }
    ret_val, counter, new_mode = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)

    if not ret_val:
        interpreter.update_dict(query_mode, new_mode)  # add/replace existing dictionary with new values

    return ret_val


# =======================================================================================================================
# Get a list of all the active jobs
# job active all
# =======================================================================================================================
def list_active_jobs(status, cmd_words, trace):
    current_time = int(time.time())
    info_list = [[], [], [], []]
    info_list[0].append("ID")
    info_list[1].append("Time")
    info_list[2].append("Status")
    info_list[3].append("Command")
    rows_count = 1

    for x in range(job_scheduler.JOB_INSTANCES):
        start_time = job_scheduler.get_job(x).get_start_time()
        if job_scheduler.get_job(x).is_job_active():
            rows_count += 1
            info_list[0].append(str(x))
            hours_time, minutes_time, seconds_time = utils_data.seconds_to_hms(
                current_time - start_time)  # get the query time
            time_str = "%u:%u:%u" % (hours_time, minutes_time, seconds_time)
            info_list[1].append(time_str)
            job_status = job_scheduler.get_job(x).get_job_status()
            info_list[2].append(job_status)
            job_command = job_scheduler.get_job(x).get_job_command()
            info_list[3].append(job_command)

    utils_print.print_data_list(info_list, rows_count, True, False)

    return process_status.SUCCESS


# =======================================================================================================================
# View or modify the state of a job
# =======================================================================================================================
def job_state(status, cmd_words, trace):

    words_count = len(cmd_words)
    output_txt = ""
    max_jobs = job_scheduler.JOB_INSTANCES
    job_location = job_scheduler.get_recent_job()
    if words_count == 3:
        if cmd_words[2] != "all":
            if cmd_words[2].isdecimal():
                job_location = int(cmd_words[2])
                if job_location >= max_jobs:
                    status.add_error("Job instance is larger than max (" + str(max_jobs) + ")")
                    return process_status.ERR_job_id  # allow "all" or a number
            else:
                return process_status.ERR_command_struct  # allow "all" or a number

    y = job_location
    if words_count == 3 and cmd_words[2] == "all":  # the current is printed last
        y += 1
        counter = job_scheduler.JOB_INSTANCES  # go over all
    else:
        counter = 1  # go over one

    jobs_counter = 0  # counts job instances with data

    job_status_info = []

    if words_count == 3 and cmd_words[2] == "all":
        # go over the scheduled jobs
        for x in range(job_scheduler.JOB_INSTANCES, max_jobs):
            if job_scheduler.get_job(x).get_start_time():
                #  real job instance
                jobs_counter += 1
                if cmd_words[1] == "status":
                    job_scheduler.get_job(x).get_job_status_info(job_status_info)


    for x in range(counter):
        # go over the dynamic jobs
        if y >= job_scheduler.JOB_INSTANCES:
            y = 0

        if job_scheduler.get_job(y).get_start_time():
            jobs_counter += 1
            if cmd_words[1] == "status":
                job_scheduler.get_job(y).get_job_status_info(job_status_info)


        y += 1  # go over the next job

    if not jobs_counter:
        output_txt = "Job instances do not contain data"

    if cmd_words[1] == "status":
        if cmd_words[0] == 'job':
            text_field = "Node Status"
        else:
            # query status - shows partition status
            text_field = "Partition"

    title = ["Job", "ID", "Output", "Run Time", "Node", "Command/Status"]

    if status.get_job_handle().is_rest_caller():
        if cmd_words[1] == "status":
            output_txt = utils_print.output_nested_lists(job_status_info, "", title, True)

        status.get_active_job_handle().set_result_set(output_txt)
        status.get_active_job_handle().signal_wait_event()  # signal the REST thread that output is ready
    else:
        utils_print.output_nested_lists(job_status_info, "", title, False)

    return process_status.SUCCESS
# =======================================================================================================================
# Compare 2 policies
# get policies diff !poiicy_1 !policy_2
# =======================================================================================================================
def diff_policies(status, io_buff_in, cmd_words, trace):

    policy_1 = params.get_value_if_available(cmd_words[3])
    policy_2 = params.get_value_if_available(cmd_words[4])
    reply = ""
    object_1 = utils_json.str_to_json(policy_1)
    if not object_1:
        status.add_error(f"Compared object is not organized in JSON format: {policy_1}")
        ret_val = process_status.ERR_wrong_json_structure
    else:
        object_2 = utils_json.str_to_json(policy_2)
        if not object_2:
            status.add_error(f"Compared object is not in JSON format: {policy_2}")
            ret_val = process_status.ERR_wrong_json_structure
        else:
            if type(object_1) == dict:
                list1 = [object_1]
            else:
                list1 = object_1        # Object_1 is a list
            if type(object_2) == dict:
                list2 = [object_2]
            else:
                list2 = object_2

            if len(list1) != len(list2):
                status.add_error(f"Compared objects have different number of JSON entries: {len(list1)} and {len(list2)}")
                ret_val = process_status.ERR_wrong_json_structure
            else:
                ret_val = process_status.SUCCESS
                for index in range(len(list1)):
                    policy_1 = list1[index]
                    policy_2 = list2[index]
                    if not isinstance(policy_1, dict) or not isinstance(policy_2, dict):
                        status.add_error(f"Object #{index+1} is not in JSON format")
                        ret_val = process_status.ERR_wrong_json_structure
                        break

                    reply += f"\r\nCompare #{index+1}\r\n"

                    reply += utils_json.compare_policies(policy_1, policy_2)


    return [ret_val, reply]

# =======================================================================================================================
# Get the last job instance
# =======================================================================================================================
def get_active_queries(status, cmd_words, trace):

    words_count = len(cmd_words)

    if cmd_words[0] == "query":
        query_only = True
    else:
        query_only = False

    job_location = job_scheduler.get_recent_job()   # get the location of the last job
    y = job_location

    if words_count == 3:
        if cmd_words[2] == "all":  # the current is printed last
            y += 1                                      # start from the next
            counter = job_scheduler.JOB_INSTANCES  # go over all
        else:
            y = int(cmd_words[2])                  # Only the requested instance is printed
            counter = -1
    else:
        counter = 1  # go over one job

    job_status_info = []
    output_txt = ""

    jobs_counter = 0
    for x in range(job_scheduler.JOB_INSTANCES):
        # go over the dynamic jobs
        if y >= job_scheduler.JOB_INSTANCES:
            y = 0
        elif y < 0:
            y = job_scheduler.JOB_INSTANCES - 1     # backwards to find the last

        get_job = True
        if job_scheduler.get_job(y).get_start_time():
            if query_only:
                if not job_scheduler.get_job(y).is_select():
                    get_job = False        # Ignore - not a query

            if get_job:
                jobs_counter += 1
                if cmd_words[1] == "status":
                    job_scheduler.get_job(y).get_query_status_info(job_status_info)
                elif cmd_words[1] == "explain":
                    output_txt += job_scheduler.get_job(y).show_explain()
                if counter == 1:
                    # Only one result is needed
                    break

        if words_count == 3 and cmd_words[2] == "all":
            y += 1  # go over the next job
        elif counter == -1:
            # Only the requested ID is needed
            break
        else:
            y -= 1  # goto the previous

    if not jobs_counter:
        output_txt = "Job instances do not contain queries info"
    else:
        if cmd_words[1] == "status":
            title = ["Job", "ID", "Output", "Run Time", "Operator", "Par", "Status", "Blocks", "Rows", "Command"]
            output_txt = utils_print.output_nested_lists(job_status_info, "", title, True)

    return [process_status.SUCCESS, output_txt]

# =======================================================================================================================
# Place error of a different node on the log, or the job instance is not available
# =======================================================================================================================
def place_other_node_error(j_instance, job_location, unique_job_id, mem_view, message_type, error_code):
    ip_port = j_instance.get_operator_ip_port(mem_view)
    error_msg = process_status.get_status_text(error_code)  # error on the second node
    message = "From operator at: %s, relating job[%u] ID: %u, received error <%s>" % (
    ip_port, job_location, unique_job_id, error_msg)
    details = message_header.get_data_decoded(mem_view)
    if details != "":
        message += " Details: <%s>" % details

    process_log.add("Error", message)
    return message
# =======================================================================================================================
# Stop a particular job, if the job is scheduled, stop executing the scheduled job
# =======================================================================================================================
def job_stop(status, io_buff_in, cmd_words):
    words_count = len(cmd_words)
    ret_val = process_status.SUCCESS

    if words_count == 3:
        job_location_str = cmd_words[2]
        if not job_scheduler.is_valid_job_id(job_location_str):
            return process_status.ERR_job_id
        job_location = int(job_location_str)
    elif words_count == 2:
        job_location = str(job_scheduler.get_recent_job())
    else:
        return process_status.ERR_job_id

    j_instance = job_scheduler.get_job(job_location)

    if j_instance.is_job_active():

        # A running job is being stopped from processing

        j_instance.data_mutex_aquire(status, 'W')  # take exclusive lock

        if j_instance.is_job_active():
            j_instance.set_job_status("Stopped")
            text = "{\"job %u status\":\"stoped\"}" % job_location

            if status.get_active_job_handle().is_rest_caller():
                status.get_active_job_handle().set_result_set(text)
                status.get_active_job_handle().signal_wait_event()  # signal the REST thread that output is ready
            else:
                utils_print.output(text, True)
                j_instance.set_not_active()  # flag the the job_instance can be reused - With REST, the call is done by the REST thread

            # Send the participating servers a message to stop processing data
            operators = j_instance.get_participating_operators()
            job_id = j_instance.get_unique_job_id()
            events.message_stop_job(status, io_buff_in, operators, job_location, job_id)
        else:
            ret_val = process_status.ERR_job_id
            job_status = j_instance.get_job_status()

        j_instance.data_mutex_release(status, 'W')

    else:
        ret_val = process_status.ERR_job_id
        job_status = "Not Active"

    if ret_val == process_status.ERR_job_id:

        if job_status:
            text = "{\"job %u status\":\"%s\"}" % (job_location, job_status)
            utils_print.output(text, True)  # This job is not active - print the status

    return ret_val
# =======================================================================================================================
# Stop the running Job and Signal a thread waiting to completion of a job
# =======================================================================================================================
def stop_job_signal_rest(status, error_code, job_location, unique_job_id):
    j_instance = job_scheduler.get_job(job_location)
    j_handle = j_instance.get_job_handle()
    error_msg = process_status.get_status_text(error_code)  # error on the second node
    error_text = "REST thread with job[%u] ID: %u was signaled with error <%s>" % (
    job_location, unique_job_id, error_msg)
    j_instance.data_mutex_aquire(status, 'W')  # Write Mutex
    # test thread did not timeout - or a previous process terminated the task
    if j_instance.is_job_active() and j_instance.get_unique_job_id() == unique_job_id:
        # Flag that the job is completed
        if not j_handle.is_subset():        # Do not disable process with subset
            j_instance.set_job_status("Error")
            j_instance.set_not_active()  # flag the the job_instance can be reused
            j_handle.set_operator_error(error_code)  # PLace the returned error on the status object
            if j_handle.is_rest_caller():
                # Signal REST Thread
                process_log.add("Error", error_text)
                j_handle.signal_wait_event()  # signal the REST thread that output is ready
    j_instance.data_mutex_release(status, 'W')  # release the mutex that prevents conflict with the rest thread

# =======================================================================================================================
# Return True if allowing to return results from some nodes
# With subset flag is set to true
# =======================================================================================================================
def is_with_subset(status, job_location, unique_job_id):
    is_subset = False
    if unique_job_id:
        j_instance = job_scheduler.get_job(job_location)
        j_instance.data_mutex_aquire(status, 'R')  # Read Mutex
        if j_instance.is_job_active() and j_instance.get_unique_job_id() == unique_job_id:
            is_subset = job_scheduler.get_job(job_location).get_job_handle().is_subset()  # return True if error from node is ignored
        j_instance.data_mutex_release(status, 'R')
    return is_subset

# =======================================================================================================================
# Process the operator reply using the projection functions
# =======================================================================================================================
def process_projection_functions(status, io_buff_in, job_location, receiver_id, par_id, projection_functions):
    ret_val = process_status.SUCCESS

    j_instance = job_scheduler.get_job(job_location)
    functions_array = projection_functions.get_functions()

    segments_counter = j_instance.get_counter_jsons(receiver_id, par_id)
    offset = 0
    for x in range(segments_counter):

        offset, segment_stat, query_data = j_instance.get_complete_json(status, receiver_id, par_id, offset)
        if query_data:
            rows_returned = query_data["Query"]  # a list of rows returned

            for row in rows_returned:

                for index, entry in enumerate(functions_array):
                    function = entry[1]
                    function.process_value(row[str(index)])  # row[str(index)] gives the column value

    if j_instance.is_last_block(receiver_id, par_id):
        # the last block with the data was processed
        j_instance.count_replies(receiver_id, par_id)  # This call needs to be done after setting the reply data

    return ret_val
# =======================================================================================================================
# Deliver rows to the local database, or in the case of pass through - to the REST client
# =======================================================================================================================
def deliver_rows(status, io_buff_in, j_instance, receiver_id, par_id, is_pass_through, dbms_cursor, rest_call, io_stream):
    ret_val = process_status.SUCCESS

    select_parsed = j_instance.get_select_parsed()
    is_order_by = select_parsed.is_order_by()
    title_list = select_parsed.get_query_title()    # The projected column names
    time_columns = select_parsed.get_date_types()  # List of columns that are date-based
    data_types_list = select_parsed.get_query_data_types()   # the data types og the projected columns

    casting_list = select_parsed.get_casting_list()     # The list of casting functions
    casting_columns = select_parsed.get_casting_columns()

    segments_counter = j_instance.get_counter_jsons(receiver_id, par_id)
    offset = 0
    local_table = j_instance.get_local_table()

    counter_local_fields = select_parsed.get_counter_local_fields()  # the number of fields on the create stmt (or 0 for select *)
    local_fields = select_parsed.get_local_fields()  # the column name and data type of the fields in system_query database

    j_handle = j_instance.get_job_handle()
    conditions = j_handle.get_conditions()


    destination, format_type, timezone = interpreter.get_multiple_values(conditions,
                                                                    ["dest",    "format", "timezone"],
                                                                    ["stdout",  "json",    None])


    if is_pass_through:
        # Get the print format
        nodes_count = j_instance.get_nodes_participating()
        output_into = j_handle.get_output_into()    # If output generates HTML file
        output_manager = output_data.OutputManager(conditions, io_stream, output_into, False, nodes_count)
        if len(casting_columns):
            # Data types changed - create a new data type list representing the output
            types_list = copy.copy(data_types_list)
            for index, column_id in  enumerate(casting_columns):
                types_list[int(column_id)] = "casting"  # "casting" will force the output code to determine the type dynamically as it can change with casting
        else:
            types_list = data_types_list
        ret_val = output_manager.init(status, select_parsed.get_source_query(), title_list, types_list, select_parsed.get_dbms_name())
        if ret_val:
            return ret_val
        fetch_counter = 0


    limit = select_parsed.get_limit()       # Cap on the rows to deliver

    for x in range(segments_counter):

        row_number = j_instance.get_pass_rows() # counter for rows delivered

        if not select_parsed.get_per_column():  # If not null, AnyLog calculates the limit - used with extended tables and associating the limit to each table
            if limit and row_number >= limit and not is_order_by:
                # with limit - count rows
                break

        offset, segment_stat, query_data = j_instance.get_complete_json(status, receiver_id, par_id, offset)

        if not query_data or segment_stat == message_header.SEGMENT_PREFIX:
            break  # the last segment is not complete, (the prefix is stored) wait for the next block

        row_number = j_instance.count_pass_rows()  # counter for rows delivered
        if not select_parsed.get_per_column():  # If not null, AnyLog calculates the limit - used with extended tables and associating the limit to each table
            if limit and row_number > limit and not is_order_by:
                # with multiple threads - only one will deliver
                break


        if not is_pass_through:
            # update a local database
            # no need to modify time zone - only when data is transferred to the caller (with the row_by_row call)
            insert_data = utils_sql.make_insert_row(x + 1, local_table, counter_local_fields, local_fields, query_data)
            # insert_data = map_results_to_insert.map_results_to_insert_main(status, local_table, query_data)

            if insert_data == "":
                info_err = "Unable to map query data to insert"
                if status.get_active_job_handle().is_rest_caller():  # keep the data on the job handle instance object
                    status.get_active_job_handle().set_result_set(
                        "{\"" + "SQL Error Local Node" + "\":\"" + info_err + "\"}")

                status.add_error(info_err)
                ret_val = process_status.ERR_wrong_json_structure
                break

            # insert the result to a local database with the logical name - "system_query"
            if not db_info.process_sql_stmt(status, dbms_cursor, insert_data):
                status.add_error("Failed to insert data with a SQL stmt: " + insert_data)
                ret_val = process_status.ERR_SQL_failure
                break
        else:

            fetch_counter += 1

            # send to the REST caller or send to output
            if (len(time_columns) and (not timezone or timezone != "utc")) or len(casting_columns):
                # change time from utc to current
                ret_val = utils_columns.change_columns_values(status, timezone, time_columns, casting_columns,  casting_list, title_list, query_data['Query'])
                if ret_val:
                    break

            row_offset = 10
            # Mutex the first row to be printed in JSON format
            is_mutexed = j_instance.mutex_first()
            if is_mutexed:
                ret_val = output_manager.output_header(status)     # Output header/title only once
                row_offset = 0      # print of first row

            if not ret_val:
                # In case of the first row - Needs to be mutexed
                ret_val = output_manager.new_rows(status, None, query_data, row_offset, True)

            if is_mutexed:
                j_instance.release_mutex_first()

            if ret_val:
                break       # Has to be after the mutex release (if taken)


    if is_pass_through:
        if format_type == "table":
            if not ret_val:
                participating_nodes = j_instance.get_nodes_replied()
                ret_val = output_manager.finalize(status, io_buff_in, fetch_counter, 0, True, False, participating_nodes)
        output_manager.close_handles(status)  # Close the file (if used for output)

    return ret_val


# =======================================================================================================================
# If pass through is True - no need in unified database - results from operators are transferred to client as is
# =======================================================================================================================
def pass_data(status, io_buff_in, job_location: int, receiver_id: int, par_id: int):
    # multiple JSON structures are placed in each block.
    # Each JSON is prefixed by size + a byte indicating if the entire JSON in with the block

    j_instance = job_scheduler.get_job(job_location)

    j_handle = j_instance.get_job_handle()

    if j_handle.is_rest_caller():
        rest_call = True
        io_stream = j_handle.get_output_socket()
    else:
        rest_call = False
        io_stream = None

    ret_val = deliver_rows(status, io_buff_in, j_instance, receiver_id, par_id, True, None, rest_call, io_stream)

    if j_instance.is_last_block(receiver_id, par_id):
        # the last block with the data was processed
        j_instance.count_replies(receiver_id, par_id)  # This call needs to be done after setting the reply data

    if not ret_val and j_instance.all_replied():
        ret_val = query_summary(status, io_buff_in, j_instance, io_stream)

    return ret_val
# =======================================================================================================================
# Deliver the query summary
# =======================================================================================================================
def query_summary(status, io_buff_in, j_instance, io_stream):

    rows_transferred = j_instance.get_pass_rows()
    conditions = j_instance.get_job_handle().get_conditions()
    j_handle = j_instance.get_job_handle()

    j_handle.set_query_completed()      # Flag that the query is completed such that the REST thread will not re-print the summary

    j_handle.stop_timer()
    output_into = j_handle.get_output_into()  # If output generates HTML file

    show_stat = interpreter.get_one_value_or_default(conditions, "stat", True)
    nodes_count = j_instance.get_nodes_participating()

    output_manager = output_data.OutputManager(conditions, io_stream, output_into, show_stat, nodes_count)

    ret_val = output_manager.init(status, None, None, None, None)

    if not ret_val:
        if show_stat:
            # Statistics not disabled by user
            if not ret_val:
                limit = j_instance.get_select_parsed().get_limit()
                if limit and rows_transferred > limit:
                    rows_transferred = limit  # The counter calculates every row that arrives to the process, but the row above limit is ignored
                operator_time = j_handle.get_operator_time()  # get the time processed with the operator

            else:
                output_manager.close_handles(status)  # Close the file (if used for output)
        else:
            operator_time = 0
        participating_nodes = j_instance.get_nodes_replied()
        ret_val = output_manager.finalize(status, io_buff_in, rows_transferred, operator_time, False, True, participating_nodes)



    return ret_val
# =======================================================================================================================
# Query_row_by_row
# If query is local - print query result
# If query is from a remote server - organize the data in a sequence of blocks and send each block to the node that issued the query
#
# Multiple JSON structures can be placed in the block. Each JSON is prefixed by: a) size (1 byte) b) a flag if the entire JSON is inside
# the block (or the rest is in the next block).
# =======================================================================================================================
def query_row_by_row(status, dbms_cursor, io_buff_in, conditions, sql_time, sql_command, nodes_count, nodes_replied):
    global query_mode
    global message_compression_
    global commands

    ret_val = process_status.SUCCESS

    timers = utils_timer.ProcessTimer(2)  # create and start timers - timer 0 is for db process and timer 1 is for send message

    max_time = interpreter.get_one_value(query_mode, "timeout")  # Upper bound for query time
    max_volume = interpreter.get_one_value(query_mode, "max_volume")  # Upper bound for query data volume
    data_volume = 0
    operator_time = 0  # Time spend at the operators

    title_list = []  # title will be set on the caller node

    message, add_stat, timezone = interpreter.get_multiple_values(conditions,
                                                       ["message", "stat",  "timezone"],
                                                       [False,     True,     None])



    out_list = None
    per_counter_dict = None


    if message:
        if commands["sql"]["trace"]:
            # Output the data delivered
            utils_print.output("\r\n[Query DBMS rows for table: '%s'] [by thread: '%s']" % (dbms_cursor.get_table_name(), utils_threads.get_thread_name()), False)

        mem_view = memoryview(io_buff_in)

        ip, port = message_header.get_source_ip_port(mem_view)
        soc = net_client.socket_open(ip, port, "job reply", 6, 3)

        Job_location = message_header.get_job_location(mem_view)
        job_id = message_header.get_job_id(mem_view)

        if is_debug_method("query"):
            utils_print.output("\r\nStart: %s.%s.%s.%s" % (job_id, dbms_cursor.get_dbms_name(), dbms_cursor.get_table_name(), message_header.get_partition_id(io_buff_in)), False)

        if soc == None:
            status.add_error("Query process failed to open socket on: %s:%u" % (ip, port))
            return process_status.ERR_network

        block_number = 1

        # Node encryption, encrypt the data message
        f_object = None
        if version.al_auth_is_node_encryption():
            # get the public string from the node that issued the query
            public_str = message_header.get_public_str(mem_view)    # public str may be NULL if no encryption is needed
            if public_str:
                encrypted_key, f_object = version.al_auth_setup_encryption(status, public_str)
                if not encrypted_key or not f_object:
                    return process_status.Encryption_failed


        message_header.set_info_type(mem_view, message_header.BLOCK_INFO_RESULT_SET)
        message_header.set_block_struct(mem_view, message_header.BLOCK_STRUCT_JSON_MULTIPLE)
        message_header.prep_command(mem_view, "job reply")  # add command to the send buffer
        message_header.set_data_segment_to_command(mem_view)

        if f_object:
            message_header.set_authentication(mem_view, encrypted_key)  # Set the encrypted_key in the authentication string

        data_types_list = None
    else:

        j_handle = status.get_active_job_handle()

        # Output on this node
        if j_handle.is_with_job_instance():
            # this node unifies data from multiple operators
            select_parsed = j_handle.get_select_parsed()
            operator_time = j_handle.get_operator_time()  # get the time processed with the operator
        else:
            # Local sql on this Operor node
            select_parsed = status.get_select_parsed()

        title_list = select_parsed.get_query_title()
        time_columns = select_parsed.get_date_types()  # List of columns that are date-based
        data_types_list = select_parsed.get_query_data_types()

        casting_list = select_parsed.get_casting_list()  # The list of casting functions
        casting_columns = select_parsed.get_casting_columns()

        output_manager = output_data.OutputManager(conditions, j_handle.get_output_socket(), j_handle.get_output_into(), add_stat, nodes_count)

        ret_val = output_manager.init(status, select_parsed.get_source_query(), title_list, data_types_list, select_parsed.get_dbms_name())
        if ret_val:
            return ret_val
        ret_val = output_manager.output_header(status)  # Output header/title only once
        if ret_val:
            return ret_val

        per_column = select_parsed.get_per_column()     # If not null, ANyLog calculates the limit - used with extended tables and associating the limit to each table
        # Example: run client () sql lsl_demo extend=(@table_name as table) "SELECT table_name, timestamp, value FROM ping_sensor order by timestamp desc limit 1 per table;"
        if per_column:
            per_counter_dict = {}
            limit = select_parsed.get_limit()

    if "extend" in conditions:
        dest_type = "tcp"
        dest_ip = None
        if "info" in conditions:
            # Additional info to consider with the query
            info_dict = get_info_dict(conditions["info"])
            if "dest_ip" in info_dict:
                # The destination IP impacts if to return the local or global ip of the node
                dest_ip = info_dict["dest_ip"]
            if "dest_type" in info_dict:
                # if dest_type is rest, return the rest port
                dest_type = info_dict["dest_type"]

        source_ip_port = net_utils.get_source_addr(dest_type, dest_ip)  # Get the Local or Global IP for TCP or rest

        # Returned result set includes additional information defined in the extend directive
        # Example: extend = (@ip as Node IP, @port, @DBMS, @table, !!disk_space.int as space)
        extend_columns = conditions["extend"]
        if not len(title_list):
            # No column names are transferred - provide the first column id for the data
            counter_extended = len(extend_columns)
            title_list.append(counter_extended)
            with_title = False      # The title_list only sets the column id of the first data column
        else:
            with_title = True
    else:
        extend_columns = None

    get_next = True
    rows_counter = 0
    fetch_counter = 0
    dbms_rows = 0
    rows_to_transfer = 0  # rows_counter does not consider sending one row in 2 blocks
    next_offset = 0

    while get_next:  # read each row - when the rows is read, it is copied to one or more data blocks - see message message_header.copy_data() below.

        # test for a limit on processing time or data volume
        if max_time:
            if timers.get_timer(0) > max_time:
                ret_val = process_status.QUERY_TIME_ABOVE_LIMIT
                break
        if max_volume:
            if data_volume > max_volume:
                ret_val = process_status.QUERY_VOLUME_ABOVE_LIMIT
                break

        timers.start(0)  # start dbms timer
        get_next, rows_data = db_info.process_fetch_rows(status, dbms_cursor, "Query", 1, title_list, data_types_list)
        timers.pause(0)  # stop dbms timer

        if not rows_data:
            break

        dbms_rows += 1      # Count actual rows retrieved
        rows_to_transfer += 1  # rows_counter does not consider sending one row in 2 blocks

        if extend_columns:
            extended_data = get_extend_data(source_ip_port, dbms_cursor.get_dbms_name(), dbms_cursor.get_table_name(), extend_columns, title_list, with_title)
            rows_data = rows_data[:11] + extended_data + rows_data[11:]
        else:
            data_volume += len(rows_data)


        if message:  # prepare a reply

            fetch_counter += 1
            # if data retrieved is send to a query node

            if f_object:
                # Symetric encryption
                data_encoded = version.al_auth_symetric_encryption(status, f_object, rows_data)
                if not data_encoded:
                    ret_val = process_status.Encryption_failed
                    break
            else:
                data_encoded = rows_data.encode()

            bytes_not_copied = 0



            while True:  # place the data in a block, if the block is full, get a next block

                rows_counter += 1

                block_full, bytes_not_copied = message_header.copy_data(mem_view, data_encoded, bytes_not_copied)  # add data to the send buffer

                if block_full:

                    message_header.set_block_number(mem_view, block_number, False)

                    message_header.set_counter_jsons(mem_view,
                                                     rows_counter)  # save the number of JSON segments in the block

                    # text = "\n----: block_id = %u, par_id = %u, soc_id = %u" % (block_number, message_header.get_partition_id(mem_view), id(soc))
                    # utils_print.output(text, True)

                    timers.start(1)
                    if not net_client.mem_view_send(soc, mem_view):  # send the data block
                        timers.pause(1)
                        ret_val = process_status.ERR_network  # failure in message send
                        if is_debug_method("query"):
                            utils_print.output("\r\nErr  : %s.%s.%s.%s.%s" % (job_id, dbms_cursor.get_dbms_name(), dbms_cursor.get_table_name(), message_header.get_partition_id(io_buff_in), ret_val), False)

                        break
                    if is_debug_method("query"):
                        utils_print.output("\r\nMsg  : %s.%s.%s.%s.%s" % (job_id, dbms_cursor.get_dbms_name(), dbms_cursor.get_table_name(), message_header.get_partition_id(io_buff_in), ret_val), False)
                    timers.pause(1)

                    block_number += 1
                    message_header.set_data_segment_to_command(mem_view)  # set the starting point after the Select stmt

                    rows_counter = 0  # restart counting

                    if not bytes_not_copied:
                        # All rows transferred
                        # test ths status of the job and update rows count
                        ret_val = test_update_job_state(ip, Job_location, job_id, rows_to_transfer)
                        rows_to_transfer = 0
                        break

                if not bytes_not_copied:
                    break

            if ret_val:
                break

        else:
            # OUTPUT the data on this node to stdout or to a file
            query_data = None
            offset_row = 10         # If data string is changed to JSON and back to string - offsets are different
            if (len(time_columns) and (not timezone or timezone != "utc")) or len(casting_columns):
                # change time from utc to current
                query_data = utils_json.str_to_json(rows_data)
                if query_data:
                    ret_val = utils_columns.change_columns_values(status, timezone, time_columns, casting_columns,  casting_list, title_list, query_data['Query'])
                    if ret_val:
                        break
                    if not out_list:
                        # with out_list = we use the list
                        rows_data = utils_json.to_string(query_data)
                        offset_row = 11 # If data string is changed to JSON and back to string - offsets are different

            if per_column:
                # AnyLog calculates the limit
                if not query_data:
                    query_data = utils_json.str_to_json(rows_data)
                if query_data:
                    if not process_limit( query_data["Query"], per_column,  per_counter_dict, limit):
                        continue        # Skip the row as above limit
            fetch_counter += 1

            ret_val = output_manager.new_rows(status, rows_data, query_data, next_offset, False)
            next_offset = offset_row
            if ret_val:
                break


    if not ret_val and not fetch_counter:
        ret_val = process_status.Empty_data_set


    # operator_time is the time spend with the operators
    # sql_time is the time for the sql stmt
    # timers.get_timer(0) is the time to get the rows
    fetch_time = timers.get_timer(0)
    network_time = timers.get_timer(1)
    dbms_time = operator_time + sql_time + fetch_time

    if message:

        job_instance.get_query_monitor().update_monitor(dbms_time)
        query_log_time = job_instance.get_query_log_time()  # -1 means no logging, 0 - all or value representing query threshold in seconds
        if query_log_time >= 0:
            if query_log_time <= dbms_time:
                # log slow queries
                query_info = f"Sec: {dbms_time:>4,} Rows: {fetch_counter:>6,} DBMS: {dbms_cursor.get_table_name()} SQL: {sql_command}"
                process_log.add("query", query_info)

        # if data retrieved is send to a query node
        if ret_val != process_status.ERR_network:
            if ret_val == process_status.AT_LIMIT:          # Rows transferred at SQL LIMIT
                ret_val = process_status.SUCCESS
            else:
                par_id = message_header.get_partition_id(io_buff_in)
                ret_val = update_thread_query_done(status, ip, Job_location, job_id, ret_val, block_number, dbms_rows, rows_to_transfer,
                                                  sql_time, fetch_time, network_time, dbms_cursor, par_id, sql_command)  # Summary per thread

                if ret_val == process_status.AT_LIMIT:  # Rows transferred at SQL LIMIT
                    ret_val = process_status.SUCCESS

            # Transfer last rows (or the error message) with thee "last block" flag"
            if ret_val:
                # Some data may be in the block - make sure that the query process ignores this data
                message_header.set_data_segment_to_command(mem_view)

            message_header.set_error(mem_view, ret_val)

            message_header.set_block_number(mem_view, block_number, True)

            message_header.set_counter_jsons(mem_view, rows_counter)  # save the number of JSON segments in the block

            message_header.set_operator_time(mem_view, dbms_time, network_time)

            if net_client.mem_view_send(soc, mem_view):  # send last block
                ret_val = process_status.SUCCESS  # ret_value needs to be reset because it may be Empty_data_set
            else:
                ret_val = process_status.ERR_network  # failure in message send
                status.add_error("Query failed to send block #%u to: %s:%u" % (block_number, ip, port))

        net_client.socket_close(soc)

        # text ="\nLast: block_id = %u, par_id = %u, soc_id = %u" % (block_number, message_header.get_partition_id(mem_view), id(soc))
        # utils_print.output(text, True)
        if is_debug_method("query"):
            utils_print.output("\r\nEnd  : %s.%s.%s.%s.%s" % (job_id, dbms_cursor.get_dbms_name(), dbms_cursor.get_table_name(), message_header.get_partition_id(io_buff_in), ret_val), False)

        if commands["sql"]["trace"]:
            # Output the data delivered
            utils_print.output("\r\n[Total blocks Transferred from table: '%s'] [%u]" % (dbms_cursor.get_table_name(), fetch_counter), False)


    else:
        j_handle.set_query_completed()  # Flag that the query is completed such that the REST thread will not re-print the summary
        if not ret_val or ret_val > process_status.NON_ERROR_RET_VALUE:
            ret_val = output_manager.finalize(status, io_buff_in, fetch_counter, dbms_time, False, True, nodes_replied)

        if output_manager.is_with_result_set():
            j_handle.set_outpu_buff(output_manager.get_result_set())


    return ret_val

# =======================================================================================================================
# Transforms the info on the query to a dictionary:
#  info = (source_ip = 67.180.101.158, ip = rest, port = rest) -->
#  source_ip : 67.180.101.158,
#  ip : rest,
#  port : rest
# =======================================================================================================================
def get_info_dict( info_list ):

    info_dict = {}
    for entry in info_list:
        index = entry.find("=")
        if index > 0 and index < (len (entry) - 1):
            # get the key and value
            key = entry[:index].strip()
            value = entry[index+1:].strip()
            if len(key) and len (value):
                info_dict[key] = value

    return info_dict
# =======================================================================================================================
# Process the limit per column - with extended columns, the query is updated with:  "limit 1 per table" to allow one row per table
# Rather than one row per query
# =======================================================================================================================
def process_limit( data_rows, per_column,  per_counter_dict, limit ):
    '''
    data_rows - the list of the data rows to consider
    per_column - the name of the column to consider
    per_counter_dict - a struct that keeps the count of returned rows as f(per_column)
    '''
    ret_val = True
    for row in data_rows:
        if per_column in row:
            col_val = row[per_column]
            try:
                per_counter_dict[col_val] += 1       # Count
            except:
                per_counter_dict[col_val] = 1        # Set first

            if per_counter_dict[col_val] > limit:
                ret_val = False                         # Skip the row as above limit
    return ret_val
# =======================================================================================================================
# set a structure indicating that the job need to stop
# This call is triggered by an event message from the query node)
# =======================================================================================================================
def set_job_stop(source_ip, Job_location, job_id):

    job_struct = get_job_struct(source_ip, Job_location, job_id, True)      # get the dictionary representing the job process

    job_struct["stop"] = True
# =======================================================================================================================
# Get the dictionary that represents ip -> job location -> unique job-location
# =======================================================================================================================
def get_job_struct(source_ip, job_location, job_id, create_flag):
    '''
    If create flag is True - create the setup if doesn't exists
    '''
    global job_process_stat_

    if source_ip in job_process_stat_:
        node_dict = job_process_stat_[source_ip]
    elif create_flag:
        # set a dictionary for the node
        node_dict = {}
        job_process_stat_[source_ip] = node_dict
    else:
        return None

    if job_location in node_dict:
        job_dict = node_dict[job_location]
        if job_dict["id"] != job_id:        # test if belongs to an older query process
            # reset existing
            job_dict = get_new_job_dict(job_id)
            node_dict[job_location] = job_dict
    elif create_flag:
        # set a dictionary for the job
        job_dict = get_new_job_dict(job_id)
        node_dict[job_location] = job_dict
    else:
        return None

    return job_dict
# =======================================================================================================================
# Get a new job dictionary - orgaizes the status for each query processed on the node
# =======================================================================================================================
def get_new_job_dict(job_id):
    job_dict = {}
    job_dict["id"] = job_id
    job_dict["rows_count"] = 0  # Counter for the number of rows delivered
    job_dict["limit"] = 0
    job_dict["done"] = 0
    job_dict["info"] = []
    return job_dict
# =======================================================================================================================
# 1) test if the job was terminated by the node initiating the job
# 2) update rows transferred
# 3) terminate process if above limit
# =======================================================================================================================
def test_update_job_state(source_ip, job_location, job_id, rows_to_transfer):
    global job_process_stat_

    job_struct = job_process_stat_[source_ip][job_location] # get the dictionary representing the job process

    if job_struct["id"] != job_id or "stop" in job_struct:
        ret_val = process_status.JOB_terminated_by_caller
    else:
        job_struct["rows_count"] += rows_to_transfer
        if job_struct["limit"] and job_struct["rows_count"] >= job_struct["limit"]:
            # No need to continue data transfer as limit reached
            ret_val = process_status.AT_LIMIT
        else:
            ret_val = process_status.SUCCESS

    return ret_val
# =======================================================================================================================
# Flag that a thread completed query process and the error code
# =======================================================================================================================
def update_thread_query_done(status, source_ip, job_location, job_id, error_code, block_counter, dbms_rows, rows_to_transfer, sql_time, fetch_time, network_time, dbms_cursor, par_id, sql_command):
    '''
    status - object status
     source_ip - the ip of the node issueing the query (source node)
     job_location - location in the source node
     job_id - unique id of the query on the source node
     error_code - error code of the executing thread
     block_counter - number of blocks delivered
     dbms_rows - total queried rows
     rows_to_transfer - since last send (rows not yet send)
     sql_time - the time to execute the sql stmt
     fetch_time - the time to retrieve the rows from the dbms
     network_time - the time spend to send results over the network (not including last block)
     dbms_cursor - query info to include the dbms and table names
     par_id - partition ID
     sql_command - the command used by the executing thread
    '''
    global job_process_stat_

    ret_val = process_status.SUCCESS
    job_struct = job_process_stat_[source_ip][job_location] # get the dictionary representing the job process
    if job_struct["id"] == job_id:
        job_struct["done"] += 1

        dbms_name = copy.copy(dbms_cursor.get_dbms_name())
        table_name = copy.copy(dbms_cursor.get_table_name())
        ret_code, par_name = utils_sql.get_table_name_from_sql(status, sql_command, 0)


        job_struct["info"].append((dbms_name, table_name, par_id, par_name, error_code, block_counter, dbms_rows, sql_time, fetch_time, network_time))


        if job_struct["limit"] and job_struct["rows_count"] >= job_struct["limit"]:
            # No need to continue data transfer as limit reached
            ret_val = process_status.AT_LIMIT
        else:
            job_struct["rows_count"] += rows_to_transfer
    return ret_val
# =======================================================================================================================
# add data to the reply using the extend directive
# =======================================================================================================================
def get_extend_data(source_ip_port, dbms_name, table_name, extend_columns, title_list, with_title):

    extend_string = ""

    for index, column_name in enumerate(extend_columns):

        if not column_name:
            continue

        if column_name[0] == '@':
            if column_name[1:10] == "dbms_name":
                column_value = dbms_name
            elif column_name[1:11] == "table_name":
                column_value = table_name
            elif column_name[1:8] == "ip_port":
                column_value = table_name
            elif column_name[1:3] == "ip":
                offset = source_ip_port.find(":")
                if offset > 0:
                    column_value = source_ip_port[:offset]
                else:
                    column_value = ""
            elif column_name[1:5] == "port":
                offset = source_ip_port.find(":")
                if offset > 0:
                    column_value = source_ip_port[offset + 1: ]
                else:
                    column_value = ""
            else:
                column_value = ""
        elif column_name[0] == "+":
            # Get value from the dictionary
            offset_end = column_name.find(' ')
            if offset_end == -1:
                offset_end = len(column_name)

            offset = column_name.find('.', 1, offset_end)      # Up to the data type
            if offset != -1 and offset < offset_end:
                offset_end = offset     # Up to the dot

            key = '!' + column_name[1:offset_end]

            column_value = params.get_value_if_available(key)
        else:
            column_value = ""

        if with_title:
            extend_string += "\"%s\":\"%s\"," % (title_list[index], column_value)
        else:
            extend_string += "\"%u\":\"%s\"," % (index, column_value)

    return extend_string


# =======================================================================================================================
# Declare a table that is partitioned by time
# Example: partition lsl_demo ping_sensor using timestamp by 2 days
#          partition * * using timestamp by week
# =======================================================================================================================
def _partition_data(status, io_buff_in, cmd_words, trace):
    words_count = len(cmd_words)

    if words_count < 6 or cmd_words[3] != "using" or cmd_words[5] != "by":
        return process_status.ERR_command_struct

    par_column = params.get_value_if_available(cmd_words[4])

    if words_count == 8:
        units_count = params.get_value_if_available(cmd_words[6])
        if not units_count.isnumeric():
            status.add_error("Units count for time interval needs to be numeric")
            return process_status.ERR_command_struct
        time_unit = params.get_value_if_available(cmd_words[7])
    elif words_count == 7:
        time_value = params.get_value_if_available(cmd_words[6]).strip().split()
        if not time_value:
            status.add_error("Time unit as '%s' has no value" % cmd_words[6])
            return process_status.ERR_command_struct
        if len(time_value) == 1:
            time_unit = time_value[0]
            units_count = '1'
        elif len(time_value) == 2:
            units_count = time_value[0]
            time_unit = time_value[1]
        else:
            status.add_error("Time unit as '%s' is not recognized" % str(time_value))
            return process_status.ERR_command_struct
    else:
        return process_status.ERR_command_struct

    if time_unit not in partitions.time_units.keys():
        if time_unit == "week":
            status.add_error("Time unit 'week' is not supported as partition key, use '7 days'")
        else:
            status.add_error("Time unit '%s' is not supported as partition key" % time_unit)
        return process_status.ERR_command_struct

    dbms_name = params.get_value_if_available(cmd_words[1])
    if not dbms_name:
        status.add_error("DBMS name for partition command has no value: %s" % cmd_words[1])
        return process_status.ERR_command_struct

    table_name = params.get_value_if_available(cmd_words[2])
    if not table_name:
        status.add_error("Table name for partition command has no value: %s" % cmd_words[2])
        return process_status.ERR_command_struct

    key = dbms_name + '.' + table_name

    partitions.partition_struct[key] = (int(units_count), time_unit, par_column)

    return process_status.SUCCESS


# =======================================================================================================================
# Process query - run the SQL stmt and call the needed function to process the rows
# If output_key is not empty, the result is assigned to the output_key
# =======================================================================================================================
def process_query_sequence(status, io_buff_in, cmd_words, logical_dbms, table_name, conditions, sql_command, nodes_count, nodes_replied):

    j_handle = status.get_active_job_handle()

    if j_handle.is_rest_caller():
        # The REST Caller can execute a local query (without run client () - in that case conditions["message") is False. In this case we execute the query as is.
        # keep the QUERY on the job handle instance object
        # This query would be executed by the REST thread
        j_handle.process_query(logical_dbms, table_name, conditions, sql_command)  # Flag that the rest server needs to issue q query
        ret_val = process_status.SUCCESS

    elif j_handle.get_output_dest() == "none":
        # The data remains in the local database and being queried by the application using independent query
        ret_val = process_status.SUCCESS  # do nothing - for example the data is placed in a database for a different system to query
    else:
        # Query the data using this thread
        ret_val = query_local_dbms(status, io_buff_in, logical_dbms, table_name, conditions, sql_command, nodes_count, nodes_replied)

    return ret_val

# =======================================================================================================================
# Query Local Database
# =======================================================================================================================
def query_local_dbms(status, io_buff_in, logical_dbms, table_name, conditions, sql_command, nodes_count, nodes_replied):

    # Query the data using this thread

    j_handle = status.get_active_job_handle()

    anylog_cursor = cursor_info.CursorInfo()  # Get the AnyLog Generic Cusror Object
    if not db_info.set_cursor(status, anylog_cursor, logical_dbms):  # "system_query" - a local database that keeps the query results
        process_log.add("Error", "DBMS used for query is not recognized: " + logical_dbms)
        return process_status.DBMS_error

    anylog_cursor.set_table_name(table_name)

    # Update the info in the anylog_cursor object

    j_handle.stop_timer()  # This is a timer measuring the time from start of the query
    timer = utils_timer.ProcessTimer(1)  # create and start a timer for SQL execution
    timer.start(0)

    if not db_info.process_sql_stmt(status, anylog_cursor, sql_command):
        status.add_error("Failed to prepare a query with a SQL stmt: " + sql_command)
        ret_val = process_status.ERR_SQL_failure
        if interpreter.get_one_value(conditions, "message"):
            error_message(status, io_buff_in, ret_val, message_header.BLOCK_INFO_COMMAND, "job reply",
                          status.get_saved_error())
    else:

        timer.pause(0)
        sql_time = timer.get_timer(0)  # the time of the SQL stmt execution

        db_info.update_cusrosr(status, anylog_cursor)  # Update cursor with metadata info

        if db_info.is_row_by_row(logical_dbms):
            ret_val = query_row_by_row(status, anylog_cursor, io_buff_in, conditions, sql_time, sql_command, nodes_count, nodes_replied)
        else:
            # a single call sends all the rows to the caller
            get_next, json_data = db_info.process_fetch_rows(status, anylog_cursor, "Query", 1, [], None)
            if get_next:
                ret_val = process_status.SUCCESS
            else:
                ret_val = process_status.ERR_SQL_failure

    db_info.close_cursor(status, anylog_cursor)

    return ret_val

# =======================================================================================================================
# Return an error while executing a query
# =======================================================================================================================
def error_message(status, io_buff_in, err_value, info_type, command, data):
    mem_view = memoryview(io_buff_in)

    ip, port = message_header.get_source_ip_port(mem_view)

    job_location = message_header.get_job_location(mem_view)
    job_id = message_header.get_job_id(mem_view)

    local_message = ("Send error message to %s:%u on job[%u] ID: %u" % (ip, port, job_location, job_id))

    if command:
        local_message += (" Command: %s" % command)
    if data:
        local_message += (" Data: %s" % data)

    if isinstance(ip, str) and isinstance(port, int):
        for x in range(3):
            # 3 attempts to send this message
            ret_val = send_message(err_value, status, ip, port, mem_view, command, data, info_type)
            if not ret_val:
                break  # properly transferred
            time.sleep(1)  # sleep one second and retry
    else:
        ret_val = process_status.ERR_dest_IP_Ports

    if ret_val:
        local_message = " Failed: " + local_message
        local_message += (" Failure: %s" % process_status.get_status_text(ret_val))
        ret_val = process_status.ERR_failed_to_send_err_msg

    status.add_error(local_message)  # ading to the error log that a message was send to the dest node

    return ret_val

# =======================================================================================================================
# Issue a SQL command - prefix includes:
# sql [db_name] [process] [sql stmt]
# process is "text" or "file" or "backup"
# "db_name" is logical database
# "file" is the name of the file containing the sql
# =======================================================================================================================
def _issue_sql(status, io_buff_in, cmd_words, trace):

    output_key = ""
    if (len(cmd_words) == 2 and cmd_words[1] == "message"):  # a tcp sql message
        message = True
        return_no_data = True       # If the table was not created, return no data from this server
        command = message_header.get_command(io_buff_in)
        command = command.replace('\t', ' ')
        query_words = command.split()
        replace_avg = True  # Replace avg by count and sum
        query_info = message_header.get_data_decoded(io_buff_in)  # Additional info provided by the caller node (like how to partition the query)

    else:
        message = False
        return_no_data = False
        cmd_offset = get_command_offset(cmd_words)
        if cmd_offset:
            output_key = cmd_words[0]  # with assignments of the result
            query_words = cmd_words[cmd_offset:]
        else:
            query_words = cmd_words
        replace_avg = False
        query_info = ""

    ret_val, logical_dbms, conditions, src_command = get_sql_processing_info(status, query_words, len(query_words), 0)

    if not ret_val:
        if trace:
            utils_print.output("\r\n[Query] [DBMS: %s] [Query: %s]" % (logical_dbms, src_command), True)
        if db_info.is_dbms(logical_dbms):
            if src_command:
                conditions["message"] = message
                if output_key:
                    interpreter.add_value(conditions, "output_key", output_key)  # assign results to this dictionary ket
                if not interpreter.get_one_value(conditions, "dest"):
                    # where to send the retrieved data
                    if message:
                        interpreter.add_value(conditions, "dest", "network")
                    else:
                        if status.get_active_job_handle().is_rest_caller():
                            interpreter.add_value(conditions, "dest", "rest")
                        else:
                            interpreter.add_value(conditions, "dest", "stdout")



                 # Get the table name + Modify the SQL using the VIEW DECLARATION (if available)
                select_parsed = status.get_select_parsed()
                select_parsed.reset(replace_avg, False)

                if interpreter.get_one_value_or_default(conditions, "committed", False):
                    # Query only data that exists on all nodes assigned to the same cluster
                    nodes_safe_ids = metadata.get_safe_ids("all")  # Get the safe IDs for each TSD table on this node
                    ha.add_peers_safe_ids(nodes_safe_ids, "all")
                    if len(nodes_safe_ids) <= 1:
                        nodes_safe_ids = None       # Only one node supports the cluster
                else:
                    nodes_safe_ids = None
                ret_val, select_table_name, sql_command = db_info.select_parser(status, select_parsed, logical_dbms, src_command, return_no_data, nodes_safe_ids)


                # Test autherization to the table
                if (not ret_val or ret_val == process_status.Empty_data_set) and message and version.al_auth_is_node_authentication():
                    # with SQL, process is done when dbms name and table name are resolved

                    public_key = status.get_public_key()
                    ip_port = message_header.get_ip_port(io_buff_in)
                    if not version.permissions_permission_process(status, 0, ip_port, public_key, command, logical_dbms, select_table_name):
                        ret_val = process_status.Unauthorized_command

                if not ret_val:
                    if select_table_name:  # select_table_name is returned only with select query
                        if not message:
                            # Local query - no need to rewrite the query
                            index = src_command.lower().find(" from ")
                            if index == -1:
                                status.add_error("Missing 'from' keyword in SQL stmt: " + src_command)
                                ret_val = process_status.ERR_SQL_failure
                            else:
                                title_list = utils_sql.make_title_list(src_command[7: index]) # Extract the colum names/functions retrieved
                                if title_list:
                                    # Select * is using the existing title list from the db_info.select_parser() process
                                    select_parsed.set_query_title(title_list)
                                ret_val = process_query_sequence(status, io_buff_in, query_words, logical_dbms,
                                                                 select_table_name, conditions, src_command, 1, 1)
                        else:
                            ip = message_header.get_source_ip(io_buff_in)
                            job_location = message_header.get_job_location(io_buff_in)
                            job_id = message_header.get_job_id(io_buff_in)
                            job_struct = get_job_struct(ip, job_location, job_id, True)  # Create new structure with new job_id (job_id is unique per node)
                            job_struct["limit"] = select_parsed.get_limit()

                            # Remote query - Rewrite the query
                            ret_val = utils_sql.process_where_condition(status, select_parsed)

                            if not ret_val:
                                if workers_pool != None and workers_pool.is_active():
                                    ret_val = partition_query(status, io_buff_in, query_words, logical_dbms, select_table_name,
                                                              conditions, sql_command, select_parsed, query_info, job_struct)
                                else:
                                    ret_val = process_query_sequence(status, io_buff_in, query_words, logical_dbms,
                                                                     select_table_name, conditions, sql_command, 1, 1)

                        if not ret_val:
                            process_log.secondary_log("SQL", "(OK) %s" % sql_command)
                        else:
                            msg = process_status.get_status_text(ret_val)
                            process_log.secondary_log("SQL", "(Failed: %u : %s) %s" % (ret_val, msg, sql_command))
                    # -------------------------------------------------------------
                    # Process a contained SQL statement - no need to externalize cursor
                    # Commands like CREATE TABLE and DROP TABLE are executed here
                    # -------------------------------------------------------------
                    elif not db_info.process_contained_sql_stmt(status, logical_dbms, sql_command):
                        status.add_error("Failed to process DDL with a SQL stmt: " + sql_command)
                        ret_val = process_status.ERR_SQL_failure

                    if ret_val and ret_val < process_status.NON_ERROR_RET_VALUE:
                        # Avoid error for no data message
                        process_log.add("Error", "SQL Command Failed: " + sql_command)
            else:
                ret_val = process_status.ERR_command_struct
        else:
            if trace:
                utils_print.output("\r\n[Query] [DBMS: %s] [not connected]" % (logical_dbms), True)

            ret_val = process_status.ERR_dbms_name
            status.add_keep_error("Logical DBMS '%s' not opened on Operator Node" % logical_dbms)
    else:
        if trace:
            utils_print.output("\r\n[Query] [Failed to process query] [%s]" % (' '.join(query_words)), True)
            if trace > 1:
                list_dbms = db_info.get_dbms_array()
                utils_print.output("\r\n[Query] [DBMS Connected] %s" % (str(list_dbms)), True)

    if ret_val and message:
        error_message(status, io_buff_in, ret_val, message_header.BLOCK_INFO_COMMAND, "job reply", status.get_saved_error())

    return ret_val


# -------------------------------------------------------------
# Partition the query to support the data partitioning
# -------------------------------------------------------------
def partition_query(status, io_buff_in, cmd_words, logical_dbms, table_name, conditions, sql_command, select_parsed,
                    query_info, job_struct):

    tables_processed = {
        # A dictionary to keep the list of tables that would be processed - avoid duplicates because of the include
        f"{logical_dbms}.{table_name}" : True
    }
    sql_tables = []     # The table name for each query

    # Execute against the main table (which is on the cmd line)
    ret_val, sql_queries = get_queries_for_table(status, cmd_words, logical_dbms, table_name, sql_command, select_parsed, query_info)
    if ret_val:
        return ret_val

    for i in range (len(sql_queries)):
        sql_tables.append(table_name)        # List of tables to query

    if "include" in conditions:
        if not ret_val or ret_val == process_status.Empty_data_set:
            if not db_info.is_table_exists(status, logical_dbms, table_name):
                # Ignore the query from this table if the table is not available on this node
                # would be sufficient if one of the include tables has data
                sql_queries = []
                sql_tables = []

            # Get the data from multiple tables
            dbms_tables_list =  conditions["include"]
            index = sql_command.rfind(" from ") # rfind as include query may have multiple froms - Need to find the last "from"
            if index > 0:
                # the offsets of the table name in the SQL
                offset_start, offset_end = utils_data.find_word_after(sql_command, index + 6)

                for entry in dbms_tables_list:

                    index = entry.find('.')
                    if index == -1:
                        # Same dbms
                        include_dbms = logical_dbms
                        include_table = entry
                    else:
                        include_dbms = entry[:index]
                        include_table = entry[index + 1:]

                    table_key = f"{include_dbms}.{include_table}"
                    if table_key in tables_processed:
                        continue            # Because of the permutations of the include, the same table may be considered twice
                    tables_processed[table_key] = True

                    if not db_info.is_table_exists(status, include_dbms, include_table):
                        # Ignore the query from this table if the table is not available on this node
                        continue

                    # ------------------------------------------------------------------------------------
                    # Change the table name and remove column names that are missing on the included table
                    # ------------------------------------------------------------------------------------

                    columns_list = db_info.get_column_info(status, include_dbms, include_table)

                    prefix_sql = select_parsed.get_adjusted_projection(columns_list, sql_command[:offset_start]) # Get the projection list and set NULL for columns that are not included in the table

                    include_sql = prefix_sql + include_table + sql_command[offset_end:]  # Change table name based on location of "from"

                    ret_val, queries_list = get_queries_for_table(status, cmd_words, include_dbms, include_table, include_sql,
                                                                     select_parsed, query_info)
                    if ret_val:
                        if ret_val ==  process_status.Empty_data_set:
                            # no relevant partition
                            ret_val = process_status.SUCCESS
                            continue
                        else:
                            break

                    sql_queries += queries_list
                    for i in range (len(queries_list)):
                        sql_tables.append(include_table)  # List of tables to query

            if not sql_queries or not len(sql_queries):
                # Main query + include did not found relevant partition
                ret_val = process_status.Empty_data_set

        else:
            status.add_error("Missing 'from' on SQL command: '%s'" % sql_command)
            ret_val = process_status.Failed_to_parse_sql

    if not ret_val:

        queries_count = len(sql_queries)
        message_header.set_partitions_used(io_buff_in,
                                           queries_count)  # update the number of physical partitions that are used to satisfy the query

        counter_threads = len(sql_queries)
        job_struct["threads"] = counter_threads
        job_struct["done"] = 0
        job_struct["info"] = []
        if counter_threads:
            # Provide query to worker threads
            ret_val = process_status.SUCCESS

            if is_debug_method("query"):
                utils_print.output("\r\n\nQuery - %s sub-queries" % len(sql_queries), False)
            for index, entry in enumerate(sql_queries):
                task = workers_pool.get_free_task()
                message_header.set_partition_id(io_buff_in, index)  # Set an ID to the partition
                task.set_io_buff(io_buff_in)
                task.set_cmd(process_query_sequence, [cmd_words, logical_dbms, sql_tables[index], conditions, entry, 1, 1])
                workers_pool.add_new_task(task)
        else:
            ret_val = process_status.Empty_data_set  # no coverage of partitions

    return ret_val


# -------------------------------------------------------------
# Get the queries for each participating database and table
# -------------------------------------------------------------
def get_queries_for_table(status, cmd_words, logical_dbms, table_name, sql_command, select_parsed, query_info):

    if partitions.is_partitioned(logical_dbms, table_name):
        # Partition by the way the database table is partitioned
        par_str = db_info.get_table_partitions(status, logical_dbms, table_name, "all")
        if not par_str:
            sql_queries = None
            ret_val =  process_status.Empty_data_set  # No data on this database
        else:
            # process the query against the physical partitions -> tansform to multiple queries
            ret_val, sql_queries = partitions.partition_query(status, logical_dbms, table_name, par_str, sql_command,
                                                              select_parsed)  # Update the query according to the physical partitions

    else:
        if query_info:
            # Partition by the info provided by the node
            ret_val, sql_queries = partition_by_info(status, cmd_words, logical_dbms, table_name, sql_command,
                                                     query_info)
        else:
            # If partition fails, use a single thread.
            sql_queries = [sql_command]  # no partition - executes a single command
            ret_val = process_status.SUCCESS

    return [ret_val, sql_queries]

# -------------------------------------------------------------
# Partition by the info provided
# Split the info according to the number of query threads and update
# the SQL stmt accordingly
# -------------------------------------------------------------
def partition_by_info(status, cmd_words, logical_dbms, table_name, sql_command, query_info):
    global workers_pool

    sql_queries = []

    # Find the column name and operation that the data is associated with query_info
    ret_val, sql_info = utils_sql.get_input_offsets(status, sql_command)
    if ret_val:
        return [ret_val, None]
    if not len(sql_info):
        status.add_keep_error("Failed to process SQL stmt for input data: %s" % sql_command)
        return [process_status.ERR_SQL_failure, None]

    # get the number of threads to use
    threads_count = workers_pool.get_number_of_threds()

    # Get the number of "OR"
    joined_info = utils_json.str_to_json(query_info)
    if not joined_info:
        status.add_keep_error("Failed to process joined data with SQL command: %s" % sql_command)
        return [process_status.ERR_wrong_json_structure, None]

    conditions_remain = len(joined_info)  # number of conditions to satisfy

    conditions_count = int(conditions_remain / threads_count)  # number of conditions per each query
    remainder = conditions_remain % threads_count
    if remainder:
        conditions_count += 1

    row_id = 0  # ID of the row in the array

    # Prepare a query for each thread
    thread_num = 0

    columns_to_update = len(sql_info)

    while (conditions_remain):  # Conditions_remain represent how many conditions to add for each query

        thread_num += 1
        # WIth 3 threads, each thread would have a query with 1/3 of the conditions (and the last thread gets the remainder)
        if thread_num == threads_count:
            # last thread - provide all remained conditions
            counter = conditions_remain
        else:
            counter = conditions_count
        conditions_remain -= counter

        modified_sql = sql_command

        sql_extension = [""] * columns_to_update  # An array containint the extension for each portion oof the sql

        for rows_considered in range(0, counter):

            # Get the next row to join
            joined_row = joined_info[row_id]["Query"]
            row_id += 1
            # every condition can impact one or more values
            index_column = columns_to_update  # The number of values to pull from the row

            # Insert the row data into the query (one or more values are pulled from the row and placed in the query)
            for column_value in reversed(
                    joined_row.values()):  # visit the entries in reverse order such that offsets will not change
                index_column -= 1
                entry = sql_info[index_column]  # The info how to add the column to the SQL
                offset_col_name, column_name, operation, offset_var_start, offset_var_end = entry

                or_stmt = " OR %s %s %s" % (column_name, operation, sql_command[offset_var_start:offset_var_end])

                extension = sql_extension[index_column]
                if extension == "":
                    # no need first "OR"
                    extension += or_stmt[4:] % column_value
                else:
                    extension += or_stmt % column_value
                sql_extension[index_column] = extension

        index_column = columns_to_update
        for entry in reversed(sql_info):
            index_column -= 1
            # add the extension - based on all the rows and the columns in each row
            offset_col_name = entry[0]
            offset_var_end = entry[4]
            extension = sql_extension[index_column]
            new_segment = " (" + extension.replace("\'", "'") + ") "
            modified_sql = modified_sql[:offset_col_name] + new_segment + modified_sql[offset_var_end:]

        sql_queries.append(modified_sql)

    return [process_status.SUCCESS, sql_queries]

# =======================================================================================================================
# Creates a view based on a statement provided by the user
# create view dbms_name.view_name ( list of columns and type ...)
# =======================================================================================================================
def _create_view(status, io_buff_in, cmd_words, trace):
    if len(cmd_words) < 4:
        return process_status.ERR_command_struct

    dbms_table = params.get_value_if_available(cmd_words[2])  # dbms name dot table name
    index = dbms_table.find('.')  # get offset to table name
    if index > 1 and index < (len(dbms_table) - 1):
        dbms_name = dbms_table[0:index]
        table_name = dbms_table[index + 1:]
        sql_command = "create view " + table_name + " " + ' '.join(cmd_words[3:])
        ret_val = db_info.set_view(status, dbms_name, sql_command)
    else:
        status.add_error("Database name and table name in 'create view' are not properly declared")
        ret_val = process_status.ERR_command_struct

    return ret_val


# =======================================================================================================================
# Execute an output job - data is updated by  current job returned data
# Note: Input queries are processed in utils_sql.get_leading_queries()
# Note: Input queries are saved in al_parser.input_queries
# =======================================================================================================================
def update_target_query(status, io_buff_in, input_query_id, dest_job, name_key):
    dest_select = dest_job.get_job_info().get_select_parsed()  # The select info of the source query
    input_queries = dest_select.get_input_queries()

    value = params.get_value_if_available(
        "!" + name_key)  # Can use zlib to compress: compressed = zlib.compress(bytes array) zlib.decompress(compressed)
    output = utils_json.str_to_json(value)
    if not output:
        input_query = params.get_value_if_available(input_queries[input_query_id][0])
        status.add_error("Input query did not return relevant data: '%s'" % input_query)
        return process_status.No_data_with_input_query

    query_txt = "\"" + dest_select.get_generic_query() + "\""

    message = "run client () " + dest_job.get_sql_mesg_prefix() + query_txt

    sub_str, left_brackets, right_brakets = utils_data.cmd_line_to_list_with_json(status, message, 0,
                                                                                  0)  # a list with words in command line
    ret_val, word_offset, dest_names, ip_port_values = get_command_ip_port(status, sub_str)
    if ret_val:
        return ret_val
    # Send the new query
    ip_port_values = dest_job.get_ip_port_values()
    ret_val = send_command_message(status, io_buff_in, sub_str, word_offset, dest_names, ip_port_values, None, 0, value,
                                   None, 0)

    return ret_val


# =======================================================================================================================
# Issue queries which update a target query, or get data that is joined with the current query.
# If with joined data - issue the query + the joined data
# =======================================================================================================================
def issue_input_queries(status, io_buff_in, j_instance, select_parsed, trace):
    ret_val = process_status.SUCCESS
    joined_data = ""
    input_queries = select_parsed.get_input_queries()
    # a list with words in command line
    select_parsed.set_input_queries_issued()  # needs to be called before the issue of the queries

    for index, query_info in enumerate(input_queries):
        input_query = params.get_value_if_available(query_info[0])
        if not input_query:
            ret_val = process_status.Input_query_not_available
            break
        if input_query[0] == '[' and input_query[-1] == ']':
            # The result is in the node - issue the query with this data which will be joined on the operator
            joined_data = input_query
            break
        duplicate = False
        for index2 in range(index):
            # test if the same query was issued
            if query_info[0] == input_queries[index2][0]:
                duplicate = True
                break  # This query was issued - process next
        if duplicate:
            continue  # Get next input query
        sub_str, left_brackets, right_brakets = utils_data.cmd_line_to_list_with_json(status, input_query, 0, 0)
        ret_val, word_offset, dest_names, ip_port_values = get_command_ip_port(status, sub_str)
        if ret_val:
            break
        # Send the new query
        ret_val = send_command_message(status, io_buff_in, sub_str, word_offset, dest_names, ip_port_values, j_instance,
                                       index, "", None, trace)
        if ret_val:
            break

    return [ret_val, joined_data]


# =======================================================================================================================
# enable / disable debug code
# Update a dictionary with the processes to debug
# Example: debug on exception
#          debug on consumer files = 5
#          debug on rest
#          debug on query
# =======================================================================================================================
def _debug_method(status, io_buff_in, cmd_words, trace):

    global code_debug

    if len(cmd_words) < 3:
        ret_val = process_status.ERR_command_struct
    else:

        ret_val = process_status.SUCCESS
        if cmd_words[1] == "on":
            state = True
        elif cmd_words[1] == "off":
            state = False
        else:
            ret_val = process_status.ERR_command_struct

        if not ret_val:
            key = cmd_words[2]
            if state:
                # enable
                code_debug[key] = cmd_words
            else:
                if key in code_debug.keys():
                    del code_debug[key]

    return ret_val
# =======================================================================================================================
# Test if the debug_method was called using command:
# debug [on/off] [method name]
# Example: debug on exception
# =======================================================================================================================
def is_debug_method(method_name):
    global code_debug
    if method_name in code_debug.keys():
        ret_val = True
    else:
        ret_val = False
    return ret_val
# =======================================================================================================================
# Return the instructions for debug. THe instruction are organized as f(method_name)
# =======================================================================================================================
def get_debug_instructions(method_name):

    global code_debug
    if method_name in code_debug.keys():
        instructions = code_debug[method_name]
    else:
        instructions = None

    return instructions

# =======================================================================================================================
# Get one or 2 file names which can be assigned names and substring separated by comma.
# for example:  file copy !watch_dir + "my_file.json" "D:\AnyLog-Code/EdgeLake/data/error/" + "file.2"
# =======================================================================================================================
def get_file_names(words_array, counter_files, offset, end_array):
    '''
   :param words_array: an array with words
   :param counter_files: Number of file names on the array
   :param offset: offset of first file name
   :param end_array: is the last file ends at the end of the array
   :return: ret_val and 2 file names
   '''

    ret_val = process_status.SUCCESS
    file2 = ""
    if counter_files == 1 and end_array:
        is_end = end_array
    else:
        is_end = False  # 2 files
    words_count, file1 = concatenate_words(words_array, offset, is_end)  # get the source file name to process
    if words_count == -1:  # not structured well
        ret_val = process_status.ERR_command_struct  # return "Command error - usage: ..."
    else:
        if file1 == "":
            ret_val = process_status.ERR_command_struct
        else:
            # get second file
            if counter_files == 2:
                offset_second = offset + 1 + 2 * words_count  # offset second file name
                words_count, file2 = concatenate_words(words_array, offset_second,
                                                       end_array)  # get the dest file name to process
                if words_count == -1:  # not structured well
                    ret_val = process_status.ERR_command_struct  # return "Command error - usage: ..."

    return [ret_val, file1, file2]

# =======================================================================================================================
# Write Message - write the data from a data block to file
# Since data arrives in blocks, data is first written to a temp file with a name extended by .transfer
# =======================================================================================================================
def write_data_block(status, io_buff_in, mem_view, file_flag, file_name, trace):
    ret_val = process_status.SUCCESS

    if file_flag == message_header.GENERIC_USE_WATCH_DIR:
        file_dir = params.get_value_if_available("!watch_dir")
        if not file_dir:
            ret_val = process_status.Watch_dir_not_defined
        else:
            path_separator = params.get_path_separator()
            if file_dir[-1] != path_separator:
                file_dir += path_separator
            path_name = file_dir + file_name        # the name of the file including the watch dir path

    elif file_flag == message_header.LARGE_MSG:
        # Write a long message, when the message is written - execute the message
        file_dir = params.get_value_if_available("!prep_dir")
        if not file_dir:
            ret_val = process_status.Prep_dir_not_defined
        path_name = file_dir + params.get_path_separator() + file_name
    else:
        path_name = params.get_value_if_available(file_name)

    if not ret_val:
        # Add .transfer to the file name such that it will not be processed during write
        write_file_name = path_name + ".transfer"  # only when file write is done, we rename the file name

        is_first = message_header.is_first_block(mem_view)
        data_start_offset = message_header.get_data_offset_after_authentication(mem_view)
        data_end_offset = message_header.get_block_size_used(mem_view)

        if not utils_io.write_data_block(write_file_name, is_first, mem_view[data_start_offset: data_end_offset]):
            ret_val = process_status.ERR_process_failure
        else:
            if message_header.is_last_block(mem_view):

                utils_io.delete_file(path_name, False)  # Delete a file with the same name (if such file exists -> Ignore error)

                if path_name[-3:] ==  ".gz":
                    # unzip the file, rename to dest name, delete zipped file
                    err_dir = params.get_value_if_available("!err_dir")
                    dest_file_name = path_name[:-3]     # Remove the ".gz" type
                    ret_val = utils_io.uncompress_del_rename(status, write_file_name, dest_file_name, err_dir)
                else:
                    # rename the file to original name
                    dest_file_name = path_name
                    if not utils_io.rename_file(status, write_file_name, dest_file_name):
                        ret_val = process_status.Failed_to_rename_file

                if not ret_val:
                    # File was renamed
                    if file_flag == message_header.LEDGER_FILE:
                        # Process a new blockchain file
                        if commands["run blockchain sync"]['trace'] == 2:
                            utils_print.output_box("Blockchain file copied from Master to '%s', call events (blockchain_use_new)" %  dest_file_name)
                            trace = 2       # Set the same trace level in blockchain_use_new()
                        ret_val = events.blockchain_use_new(status, io_buff_in, [dest_file_name], trace)

                    elif file_flag == message_header.LARGE_MSG:
                        # Read and process the message
                        large_message = utils_io.read_to_string(process_status, dest_file_name)
                        if not large_message:
                            status.add_error(f"Failed to read Large Message from file: '{dest_file_name}'")
                            ret_val = process_status.File_read_failed
                        else:
                            source_ip, source_port = message_header.get_source_ip_port(mem_view)
                            ret_val = process_cmd(status, large_message, False, source_ip, source_port, io_buff_in, False)
                            if not ret_val:
                                # delete the command-file after the command was executed
                                utils_io.delete_file(dest_file_name, False)
                            else:
                                # Move to erro directory
                                err_dir = params.get_value_if_available("!err_dir")
                                if err_dir:
                                    if not utils_io.file_to_dir(status, err_dir, dest_file_name, "err_%u" % ret_val, True):
                                        ret_val = process_status.File_move_failed

    return ret_val
# =======================================================================================================================
# Show the list of files in a directory
# get files in dir_name where type = file_type and hash = hash_value
# =======================================================================================================================
def get_files_in_dir(status, words_array, words_count, offset_dir, offset_where):
    if words_count == offset_where:
        file_types = None
        hash_values = None

    elif words_count >= (4 + offset_where) and words_array[offset_where] == "where":

        #                              Must     Add      Is
        #                              exists   Counter  Unique

        keywords = {"type": ("str", False, False, False),
                    "hash": ("str", False, False, False),
                    }

        ret_val, counter, conditions = interpreter.get_dict_from_words(status, words_array, 1 + offset_where, 0,
                                                                       keywords, False)
        if ret_val:
            return [ret_val, None]
        if "type" in conditions.keys():
            file_types = conditions["type"]
        else:
            file_types = None
        if "hash" in conditions.keys():
            hash_values = conditions["hash"]
        else:
            hash_values = None
    else:
        return [process_status.ERR_command_struct, ""]

    source_dir = params.get_value_if_available(words_array[offset_dir])

    ret_val, files_list = utils_io.get_files_from_dir(status, source_dir, hash_values, file_types)

    return [ret_val, files_list]


# =======================================================================================================================
# Get a file name or a subdirectory which are rooted at the specified path.
# 'example': 'work_file = directory !work_dir get file repeat = 5'
# =======================================================================================================================
def get_from_directory(status, io_buff_in, cmd_words, trace):
    words_count = len(cmd_words)

    offset = get_command_offset(cmd_words)

    if words_count == (4 + offset):
        repeat_time = 0
    elif words_count == (7 + offset):
        if cmd_words[offset + 4] == "repeat" and cmd_words[offset + 5] == "=" and cmd_words[offset + 6].isdigit():
            # wait until file is found
            repeat_time = int(cmd_words[offset + 6])
        else:
            return process_status.ERR_command_struct

    if words_count < (4 + offset) or cmd_words[2 + offset] != "get":
        return process_status.ERR_command_struct

    object_type = cmd_words[3 + offset]  # file or dir

    name_list = []  # will maintain a list of files or subdirectories
    dir_path = params.get_value_if_available(cmd_words[1 + offset])


    if (len(dir_path) == 0 or dir_path[-1] != params.get_path_separator()):
        dir_path = dir_path + params.get_path_separator()

    file_types = ["*"]  # all files

    ret_val, name = get_files_from_dir(status, "scripts", repeat_time, dir_path, object_type, file_types, name_list, None, None, False)

    if not offset:
        utils_print.output(name, True)
    else:
        params.add_param(cmd_words[0], name)  # add key value pair

    return ret_val
# =======================================================================================================================
#  Set a dirctory as a watched directory - Get a file name or a subdirectory which are rooted at the specified path.
#  Copy files in a directory to a different node
# 'example': 'work_file = directory !work_dir get file repeat = 5'
# =======================================================================================================================
def _process_directory(status, io_buff_in, cmd_words, trace):
    words_count = len(cmd_words)
    if words_count < 2:
        return process_status.ERR_command_struct

    offset = get_command_offset(cmd_words)

    if words_count >= (offset + 4) and cmd_words[offset + 2] == "get":
        ret_val = get_from_directory(status, io_buff_in, cmd_words, trace)
    else:
        ret_val = process_status.ERR_command_struct

    return ret_val
# =======================================================================================================================
# Different methods to authenticate users and encrypt data
# Examples: id create keys using password = my_password, id create keys for node using password = my_password
# =======================================================================================================================
def _process_id(status, io_buff_in, cmd_words, trace):

    words_count = len(cmd_words)
    if words_count < 2:
        return process_status.ERR_command_struct

    ret_val, reply = _exec_child_dict(status, commands["id"]["methods"], commands["id"]["max_words"], io_buff_in, cmd_words, 1, trace)

    return ret_val

# =======================================================================================================================
# Send reply to the destination IP and Port (retrieved from the message header)
# =======================================================================================================================
def send_message_reply(status, io_buff_in, command, reply_str, info_type):
    mem_view = memoryview(io_buff_in)

    ip, port = message_header.get_source_ip_port(mem_view)

    if isinstance(ip, str) and port > 0:
        ret_val = send_message(0, status, ip, port, mem_view, command, reply_str, info_type)
    else:
        status.add_error("Wrong IP and Port values of node initiating a query")
        ret_val = process_status.ERR_source_IP_Ports

    return ret_val


# =======================================================================================================================
# Execute system command
# Example: system ls
# =======================================================================================================================
def _run_sys_process(status, io_buff_in, cmd_words, trace):
    assign = False
    timeout_sec = 5  # default 5 seconds wait for system call
    if (len(cmd_words) == 2 and cmd_words[1] == "message"):  # a tcp sql message
        os_string = message_header.get_command(io_buff_in)
        os_string = os_string.replace('\t', ' ')
        if len(os_string) <= 7:  # "system " is 7 bytes long
            return process_status.ERR_process_failure
        os_string = os_string[7:]
        offset = 0
    else:
        offset = get_command_offset(cmd_words)
        if offset:
            assign = True  # Assign results to a variable

        os_string = params.get_translated_string(cmd_words, 1 + offset, 0, False)  # get new array with mapped values
        if not os_string:
            return process_status.ERR_command_struct        # Was not traslated

    cmd_info = os_string.split()
    cmd_length = len(cmd_info)
    if not cmd_length:
        return process_status.ERR_command_struct

    if cmd_length > 3:
        # test if command specifies timeout,
        # For example: system timeout = 2 ls
        if cmd_info[0] == "timeout" and cmd_info[1] == '=' and cmd_info[2].isnumeric():
            timeout_sec = int(cmd_info[2 + offset])
            offset += 3
            cmd_info = cmd_info[3:]
            cmd_length -= 3
            os_string = ' '.join(cmd_info)

    ret_val = process_status.SUCCESS
    if cmd_info[0] == "mv":
        if cmd_length == 3:
            if utils_io.test_dir_exists(status, cmd_info[2], False):
                # dest dir does not exists
                err_msg = "Destination directory does not exists: " + cmd_info[2]
                ret_val = process_status.ERR_dir_does_not_exists
            elif not utils_io.is_path_exists(cmd_info[1]):
                err_msg = "Source file does not exists: " + cmd_info[1]
                ret_val = process_status.ERR_file_does_not_exists
        else:
            if len(cmd_words) != (4 + offset):
                err_msg = "Wrong number of params with 'system mv' command: " + os_string
                ret_val = process_status.ERR_command_struct
            else:
                # Missing params for move command
                if not params.validate_param(cmd_words[2 + offset]):
                    err_msg = "Source file is not defined: " + cmd_words[2 + offset]
                    ret_val = process_status.ERR_file_does_not_exists
                else:
                    err_msg = "Destination directory does not exists: " + cmd_words[3 + offset]
                    ret_val = process_status.ERR_dir_does_not_exists

    if ret_val:
        status.add_keep_error(err_msg)
        if offset:
            params.add_param(cmd_words[0], err_msg)
    else:
        ret_val, msg = system_call(status, os_string, timeout_sec)

        if not ret_val:
            if len(msg):

                if cmd_words[1] == "message":
                    utils_print.print_prompt()
                    # return the message reply
                    ret_val = send_message_reply(status, io_buff_in, "print \r\n" + msg, "",
                                                 message_header.BLOCK_INFO_COMMAND)
                else:
                    if assign:
                        params.add_param(cmd_words[0], msg)
                    else:
                        utils_print.output(msg, True)
            elif offset:
                params.add_param(cmd_words[0], "")  # no message -> reset value

    return ret_val

# =======================================================================================================================
# Run system call
# Exit if not executed within timeout_sec
# =======================================================================================================================
def system_call(status, os_string, timeout_sec):

    ret_val = process_status.SUCCESS
    try:
        process = subprocess.Popen(os_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except:
        msg = "Process invoked by '%s' failed" % os_string
        ret_val = process_status.System_call_error
    else:
        # if not failed
        try:
            stdout, stderr = process.communicate(timeout=timeout_sec)
        except subprocess.TimeoutExpired:
            process.kill()
            msg = "Process invoked by '%s' terminated after 5 seconds" % os_string
            status.add_keep_error(msg)
            ret_val = process_status.System_call_error
        except:
            msg = "Process invoked by '%s' failed without a clear message" % os_string
            status.add_keep_error(msg)
            ret_val = process_status.System_call_error
        else:
            if len(stderr):
                try:
                    msg = stderr.decode("utf-8")
                except:
                    msg = str(stderr)
                status.add_error("System call error: [%s] returned [%s]" % (os_string, msg))
            else:
                try:
                    msg = stdout.decode("utf-8")
                except:
                    msg = str(stdout)

            if len(msg):
                # change "\n" to "\r\n"
                if msg.find('\r') == -1:
                    msg = msg.replace("\n", "\r\n")

    return [ret_val, msg]

# =======================================================================================================================
# Returns the execution the instruction after the call.
# =======================================================================================================================
def _return_from_call(status, io_buff_in, cmd_words, trace):
    words_count = len(cmd_words)
    if words_count == 1:
        return process_status.RETURN

    return process_status.get_return_code(' '.join(cmd_words[1:]))

# =======================================================================================================================
# Stop a particular script
# stop thread_id
# =======================================================================================================================
def _stop_thread(status, io_buff_in, cmd_words, trace):
    ret_val = process_status.SUCCESS

    if len(cmd_words) == 2:
        # given the thread id
        thread_id = cmd_words[1]
        if thread_id == "all":
            # stop all threads
            for scripts_array in running_scripts.values():
                for debug_info in scripts_array:
                    debug_info.set_stop()  # Flag that the thread is to be terminated
        else:
            # given the thread id
            thread_id = cmd_words[1]
            if thread_id in running_scripts.keys():
                scripts_array = running_scripts[thread_id]
                for debug_info in scripts_array:
                    debug_info.set_stop()  # Flag that the thread is to be terminated
            else:
                ret_val = process_status.The_thread_has_no_script
    else:
        ret_val = process_status.ERR_command_struct

    return ret_val

# =======================================================================================================================
# Executes a special command on streaming data
# command starts with: streaming data
# streaming data ignore event
# streaming data ignore attribute
# streaming data ignore script
# streaming data change policy

# get the next event or attribute (called by a mapping policy script)
# =======================================================================================================================
def policy_script(status, io_buff_in, cmd_words, trace):

    ret_val = process_status.ERR_in_script_cmd
    if cmd_words[2] == "ignore":
        if  cmd_words[3] == "script":
            ret_val = process_status.IGNORE_SCRIPT
        elif cmd_words[3] == "attribute":
            ret_val = process_status.IGNORE_ATTRIBUTE
        elif cmd_words[3] == "event":
            ret_val = process_status.IGNORE_EVENT  # Skip the row
    elif cmd_words[2] == "change":
        if  cmd_words[3] == "policy":
            ret_val = process_status.CHANGE_POLICY

    return ret_val
# =======================================================================================================================
# Executes the next command in debug mode
# =======================================================================================================================
def _debug_next(status, io_buff_in, cmd_words, trace):
    ret_val = process_status.SUCCESS

    if len(cmd_words) == 1:
        # next to all threads
        for scripts_array in running_scripts.values():
            if len(scripts_array):
                scripts_array[-1].set_debug_next(True)
    elif len(cmd_words) == 2:
        # given the thread id
        thread_id = cmd_words[1]
        if thread_id in running_scripts.keys():
            scripts_array = running_scripts[thread_id]
            if len(scripts_array):
                scripts_array[-1].set_debug_next(True)
        else:
            ret_val = process_status.The_thread_has_no_script
    else:
        ret_val = process_status.ERR_command_struct

    return ret_val

# =======================================================================================================================
# Terminate debug interactive
# COmmand: Continue
# =======================================================================================================================
def _debug_continue(status, io_buff_in, cmd_words, trace):
    ret_val = process_status.SUCCESS
    if len(cmd_words) == 1:
        # continue to all threads
        for scripts_array in running_scripts.values():
            if len(scripts_array):
                scripts_array[-1].set_debug_interactive(False)
    elif len(cmd_words) == 2:
        # given the thread id
        thread_id = cmd_words[1]
        if thread_id in running_scripts.keys():
            scripts_array = running_scripts[thread_id]
            if len(scripts_array):
                scripts_array[-1].set_debug_interactive(False)
        else:
            ret_val = process_status.The_thread_has_no_script
    else:
        ret_val = process_status.ERR_command_struct
    return ret_val



# =======================================================================================================================
# Register PI server on the network
# =======================================================================================================================
def _process_pi(status, io_buff_in, cmd_words, trace):
    words_count = len(cmd_words)

    if words_count < 2:
        return process_status.ERR_command_struct

    offset = get_command_offset(cmd_words)

    if cmd_words[1 + offset] == "test" and (words_count == 2 or words_count == 4):
        if words_count == 2:
            # take default
            pi_process_ip = '127.0.0.1'
            pi_process_port = 7400
        else:
            pi_process_ip = cmd_words[2]
            pi_process_port = int(cmd_words[3])
        pi_dbms.test_pi(status, pi_process_ip, pi_process_port)
        return process_status.SUCCESS

    ret_val = _issue_sql(status, io_buff_in, cmd_words, trace)

    return ret_val


# =======================================================================================================================
# Function to allow user to disconnect from database
# =======================================================================================================================
def _disconnect_dbms(status, io_buff_in, cmd_words, trace):
    if len(cmd_words) != 3:
        return process_status.ERR_command_struct

    db_name = params.get_value_if_available(cmd_words[2])

    ret_val = db_info.close_connection(status, db_name)
    if ret_val:
        utils_print.output("Disconnected from database '%s'" % db_name, True)
        ret_val = process_status.SUCCESS
    else:
        utils_print.output("Unable to disconnect from database: '%s'" % db_name, True)
        ret_val = process_status.ERR_process_failure

    return ret_val


# =======================================================================================================================
# Get Info on partitions
# get partitions info where dbms = smart_city and table = test
'''
Replacing:
info table sensors readings partitions\n'
                            'info table sensors readings partitions last\n'
                            'info table sensors readings partitions first\n'
                            'info table sensors readings partitions count',
'''
# =======================================================================================================================
def get_partitions_info(status, io_buff_in, cmd_words, trace):

    words_count = len(cmd_words)

    offset = get_command_offset(cmd_words)

    #                        Must     Add      Is
    #                        exists   Counter  Unique

    keywords = {"dbms": ("str", True, False, True),
                "table": ("str", True, False, True),
            }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, offset + 4, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return [ret_val, None]


    dbms_name = conditions["dbms"][0]
    table_name = conditions["table"][0]

    output_list = []

    if partitions.is_partitioned(dbms_name, table_name):

        partition_list = db_info.get_partitions_info(status, dbms_name, table_name, False)
        '''
        Example Info Returned
        0 = {list: 3} ['par_test_2024_08_00_d14_insert_timestamp', '2024-08-01', '2024-08-14']
        0 = {str} 'par_test_2024_08_00_d14_insert_timestamp'
        1 = {str} '2024-08-01'
        2 = {str} '2024-08-14'
        __len__ = {int} 3
        '''
        if partition_list:
            previous_table = ""
            for entry in partition_list:
                # Pull table name
                match = re.search(r'\d', entry[0]) # Offset to date digit
                table_by_par = entry[0][4:match.start()-1]
                if table_by_par == previous_table:
                    par_table = ""      # Info in the list
                    counter += 1        # Count partitions per table
                else:
                    counter = 1
                    par_table = table_by_par
                    previous_table = table_by_par
                start_date = entry[1]
                end_date = entry[2]
                output_list.append([par_table, counter, entry[0], start_date, end_date])

            info_string = utils_print.output_nested_lists(output_list, f"Partitions in DBMS: '{dbms_name}'", ["Table", "Counter", "Partition", "Start Date", "End Data"], True, "    ")
        else:
            info_string = "Not Partitioned"
    else:
        info_string= "Not Partitioned"

    return [ret_val, info_string]
# =======================================================================================================================
# Return the info on the specified table or view.
# Examples:
# info table sensors readings column
# info view sensors readings column
# =======================================================================================================================
def _info_table_view(status, io_buff_in, cmd_words, trace):
    ret_value = process_status.SUCCESS
    words_count = len(cmd_words)

    if (words_count == 3 and cmd_words[2] == "message"):  # a tcp sql message
        is_msg = True
        offset_resource = message_header.get_word_offset(io_buff_in, 5)
        offset = message_header.get_word_offset(io_buff_in, 11)
        if offset_resource == -1 or offset == -1:
            status.add_error("Failed to determine database name from SQL message")
            ret_value = process_status.ERR_dbms_name
        else:
            resource_type = message_header.get_word_from_array(io_buff_in, offset_resource)  # Get TABLE or VIEW
            logical_dbms = message_header.get_word_from_array(io_buff_in, offset)
            offset += (len(logical_dbms) + 1)  # + 1 for the space
            offset = message_header.get_word_offset(io_buff_in, offset)  # get offset to next word
            if offset == -1:
                ret_value = process_status.ERR_table_name
            else:
                table_name = message_header.get_word_from_array(io_buff_in, offset)
                offset += (len(table_name) + 1)
                offset = message_header.get_word_offset(io_buff_in, offset)
                if offset == -1:
                    ret_value = process_status.ERR_command_struct
                else:
                    info_type = message_header.get_message_text(io_buff_in, offset)
                    if not logical_dbms or not table_name or not info_type:
                        ret_value = process_status.ERR_command_struct
    else:
        is_msg = False
        offset = get_command_offset(cmd_words)
        if words_count < (5 + offset):
            ret_value = process_status.ERR_command_struct
        else:
            resource_type = params.get_value_if_available(cmd_words[offset + 1])  # Get TABLE or VIEW
            logical_dbms = params.get_value_if_available(cmd_words[offset + 2])
            table_name = params.get_value_if_available(cmd_words[offset + 3])
            info_type = cmd_words[offset + 4]  # Columns/Partitions/Exists

    if not ret_value:

        if resource_type == "view" or db_info.is_dbms(logical_dbms):  # no need in connected dbms with a view

            if info_type == "columns":  # to be added - or info_type == "names" or "create":
                # info type determines the type of info to retrieve
                if resource_type == "table":
                    info_string = db_info.get_table_info(status, logical_dbms, table_name, info_type)
                else:  # info type is VIEW
                    info_string = db_info.get_view_info(status, logical_dbms, table_name, info_type)
                if info_string == "":
                    status.add_keep_error(
                        "Failed to retrieve info on dbms '%s' and table '%s'" % (logical_dbms, table_name))
                    ret_value = process_status.No_metadata_info
            elif resource_type == "table" and info_type == "partitions":
                if partitions.is_partitioned(logical_dbms, table_name):
                    if words_count == (6 + offset):
                        details = cmd_words[offset + 5]  # details can be "last" "first" or "all"
                    else:
                        details = "all"
                    info_string = db_info.get_table_partitions(status, logical_dbms, table_name, details)
                    if info_string == "":
                        status.add_keep_error(
                            "No partitions found on dbms '%s' and table '%s'" % (logical_dbms, table_name))
                        ret_value = process_status.No_metadata_info
                else:
                    info_string = ""
                    status.add_keep_error(
                        "Missing 'partition' declaration for table '%s.%s'" % (logical_dbms, table_name))
                    ret_value = process_status.Table_without_partition_def
            elif info_type == "exists":
                if resource_type == "table":
                    ret_code = db_info.is_table_exists(status, logical_dbms, table_name)
                else:
                    ret_code = db_info.is_view_exists(status, logical_dbms, table_name)
                if ret_code:
                    info_string = "true"
                else:
                    info_string = "false"
            else:
                status.add_keep_error("Type of info not recognized: " + info_type)
                ret_value = process_status.ERR_command_struct
        else:
            status.add_keep_error("Logical DBMS '%s' not connected" % logical_dbms)
            ret_value = process_status.DBMS_NOT_COMNNECTED

    if ret_value == process_status.No_metadata_info:
        # Return a different error if table on blockchain and not declared local
        get_cmd = "blockchain get table where dbms = %s and name = %s bring.recent" % (logical_dbms, table_name)
        ret_code, tables = blockchain_get(status, get_cmd.split(), "", True)
        if not ret_code and tables:
            ret_value = process_status.Blockchain_table_not_local

    if not ret_value:

        if is_msg:
            ret_value = send_message_reply(status, io_buff_in, "job reply", info_string,
                                           message_header.BLOCK_INFO_TABLE_STRUCT)
        else:
            if offset:
                params.add_param(cmd_words[0], info_string)
            else:
                utils_print.print_row(info_string, True)
    else:
        if is_msg:
            error_message(status, io_buff_in, ret_value, message_header.BLOCK_INFO_TABLE_STRUCT, "job reply",
                          status.get_saved_error())

    return ret_value

# =======================================================================================================================
# Creates a table based on the data in the blockchain
# Or creates an internal table with predetermined columns for metadata usage.
# Examples for internal tables: create table ledger where dbms = table blockchain  --  create table tsd_info where dbms = almgm
# Example for user table: create table new_sensor where dbms = lsl_demo
# =======================================================================================================================
def _create_table(status, io_buff_in, cmd_words, trace):

    if len(cmd_words) == 3 and cmd_words[2] == "message":  # a tcp sql message
        mem_view = memoryview(io_buff_in)
        command = message_header.get_command(mem_view)
        msg_words = command.split()
        words_array = msg_words
    else:
        words_array = cmd_words


    if len(words_array) != 7:
        return process_status.ERR_command_struct
    if not utils_data.test_words(words_array, 3, ["where", "dbms", '=']):
        return process_status.ERR_command_struct

    ret_value = process_status.SUCCESS

    table_name = params.get_value_if_available(words_array[2])
    dbms_name = params.get_value_if_available(words_array[6])

    if dbms_name == "blockchain" and table_name == "ledger":
        # AnyLog System Table
        if not db_info.blockchain_create_local_table(status):
            ret_value = process_status.ERR_process_failure
    elif dbms_name == "almgm" and table_name == "tsd_info":
        #  AnyLog System Table
        if not db_info.tsd_create_local_table(status):
            ret_value = process_status.ERR_process_failure
    else:
        # Create user table from the blockchain
        get_cmd = "blockchain get table where dbms = %s and name = %s" % (dbms_name, table_name)
        ret_value, tables = blockchain_get(status, get_cmd.split(), "", True)

        if not ret_value and len(tables) == 1:
            table_info = tables[-1]     # Take the most recent
            if isinstance(table_info, dict) and "create" in table_info["table"].keys():
                create_stmt = table_info["table"]["create"]
                # create_stmt = create_stmt.replace('|',"\"")  # this was added for sql when the create was saved to the blockchain
                create_stmt = utils_data.replace_string_chars(True, create_stmt, {'|': '"'})
                if not db_info.process_contained_sql_stmt(status, dbms_name, create_stmt):
                    ret_value = process_status.ERR_SQL_failure
            else:
                status.add_error("Failed to create a table from blockchain data for table '%s' and dbms '%s'" % (
                table_name, dbms_name))
                ret_value = process_status.ERR_process_failure
        else:
            status.add_error(
                "Failed to create a table from blockchain data for table '%s' and dbms '%s'" % (table_name, dbms_name))
            ret_value = process_status.ERR_process_failure

    return ret_value


# =======================================================================================================================
# Drops a dbms
# Examples: drop dbms lsl_demo from psql where user = [user] and passwd = [passwd] and ip = [host] and port = [port]
# =======================================================================================================================
def _drop_dbms(status, io_buff_in, cmd_words, trace):

    words_count = len(cmd_words)


    if words_count >= 9 and cmd_words[5] == "where" and cmd_words[3] == "from":
        #                           Must     Add      Is
        #                           exists   Counter  Unique
        keywords = {"user": ("str", False, False, True),
                    "password": ("str", False, False, True),
                    "ip": ("str", False, False, True),
                    "port": ("int", False, False, True),
                    }

        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 6, 0, keywords, False)
        if ret_val:
            # conditions not satisfied by keywords or command structure
            return ret_val

        user, password, host, port = interpreter.get_multiple_values(conditions,
                                                                                      ["user", "password", "ip", "port"],
                                                                                      ["", "", "", 0])

    elif words_count == 5 and cmd_words[3] == "from":
        user = ""
        password = ""
        host = ""
        port = 0
        ret_val = process_status.SUCCESS
    else:
        ret_val = process_status.ERR_command_struct

    if not ret_val:
        dbms_name = params.get_value_if_available(cmd_words[2])
        dbms_type = params.get_value_if_available(cmd_words[4])
        ret_val = db_info.drop_dbms(status, dbms_type, dbms_name, user, password, host, port)

    return ret_val


# =======================================================================================================================
# Drops a table
# Examples: drop table blockchain where dbms = almgm
# =======================================================================================================================
def _drop_table(status, io_buff_in, cmd_words, trace):

    if len(cmd_words) == 3 and cmd_words[2] == "message":  # a tcp sql message
        mem_view = memoryview(io_buff_in)
        command = message_header.get_command(mem_view)
        msg_words = command.split()
        words_array = msg_words
        is_msg = True
    else:
        words_array = cmd_words
        is_msg = False

    if len(words_array) == 7 and utils_data.test_words(words_array, 3, ["where", "dbms", "="]):
        table_name = params.get_value_if_available(words_array[2])
        dbms_name = params.get_value_if_available(words_array[6])

        if partitions.is_partitioned(dbms_name, table_name):
            # Drop the partitions if available
            par_str = db_info.get_table_partitions(status, dbms_name, table_name, "all")
            if par_str:
                # Get a list of partitions
                ret_val, par_list = partitions.par_str_to_list(status, dbms_name, table_name, par_str)
                if ret_val:
                    return ret_val

                for partition in par_list:
                    if "par_name" not in partition.keys():
                        status.add_error("Error in the structure providing the list of partitions: %s" % str(par_list))
                        return process_status.Drop_partition_failed
                    par_name = partition["par_name"]
                    if not db_info.drop_table(status, dbms_name, par_name):
                        status.add_error("Failed to drop partition: %s.%s" % (dbms_name, par_name))
                        return process_status.Drop_partition_failed

        # Drop the table
        if not db_info.drop_table(status, dbms_name, table_name):
            ret_value = process_status.ERR_process_failure
        else:
            ret_value = process_status.SUCCESS
    else:
        ret_value = process_status.ERR_command_struct

    if is_msg:
        reply = process_status.get_status_text(ret_value)
        display = "echo"  # error goes to the message queue
        ret_val = send_display_message(status, ret_value, display, io_buff_in, reply, False)  # send text message to peers

    return ret_value

# =======================================================================================================================
# Drops a partition
# Example - drop a named partition: drop partition par_readings_2019_08_02_d07_timestamp where dbms = purpleair and table = readings
# Example - drop the oldest partition: drop partition where dbms = purpleair and table = readings
# Example - drop the oldest partition: drop partition where dbms = aiops and table = *
# =======================================================================================================================
def _drop_partition(status, io_buff_in, cmd_words, trace):
    ret_val = process_status.Drop_partition_failed

    words_count = len(cmd_words)

    if words_count == 11 and utils_data.test_words(cmd_words, 3, ["where", "dbms", "="]) and utils_data.test_words(
            cmd_words, 7, ["and", "table", "="]):

        dbms_name = params.get_value_if_available(cmd_words[6])
        table_name = params.get_value_if_available(cmd_words[10])
        par_name = params.get_value_if_available(cmd_words[2])

        if partitions.is_partitioned(dbms_name, table_name):
            # Drop the partitions if available
            par_list = db_info.get_table_partitions_list(status, dbms_name, table_name)
            if par_list:
                if par_name in par_list:
                    ret_val = process_status.SUCCESS
                elif par_name == '*':
                    # drop all partitions
                    ret_val = process_status.SUCCESS
                else:
                    status.add_error("Wrong partition mame: %s for %s.%s is not partitioned" % (par_name, dbms_name, table_name))
            else:
                status.add_error("Table %s in dbms %s does not contain partitions" % (table_name, dbms_name))
        else:
            status.add_error("Table %s.%s is not partitioned" % (dbms_name, table_name))

        if not ret_val:
            # drop the partition
            if par_name == '*':
                # drop all partitions
                for partition_name in par_list:
                    if not db_info.drop_table(status, dbms_name, partition_name):
                        status.add_error("Failed to drop partition: %s.%s" % (dbms_name, partition_name))
                        ret_val = process_status.Drop_partition_failed
                        break
            else:
                # drop one partition
                if not db_info.drop_table(status, dbms_name, par_name):
                    status.add_error("Failed to drop partition: %s.%s" % (dbms_name, par_name))
                    ret_val = process_status.Drop_partition_failed

    elif (words_count == 10 or words_count == 14) and utils_data.test_words(cmd_words, 2, ["where", "dbms", "="]) and utils_data.test_words(
            cmd_words, 6, ["and", "table", "="]):
        min_par = 0     # The default for number of partitions to keep
        if words_count == 14:
            # This is the case which specifies how many partitions to keep. The default is one
            keep = params.get_value_if_available(cmd_words[13])
            if utils_data.test_words(cmd_words, 10, ["and", "keep", "="]) and keep.isdecimal():
                try:
                    min_par = int(keep)        # The min number of partitions
                except:
                    status.add_error("Error in 'drop partition' command - keep as '%s' is not an integer value" % keep)
                    return process_status.ERR_command_struct

            else:
                status.add_error("Error in 'drop partition' command - error in number of partitions to keep")
                return process_status.ERR_command_struct
        # Find the oldest partition
        # Example - drop the oldest partition: drop partition where dbms = purpleair and table = readings
        dbms_name = params.get_value_if_available(cmd_words[5])
        table_name = params.get_value_if_available(cmd_words[9])
        if table_name == '*':   # drop all partitions of the database
            cmd = "blockchain get table bring.unique ['table']['name'] separator = \\n"
            ret_val, tables = blockchain_get(status, cmd.split(), None, True)
            if not ret_val and tables:
                tables_list = tables.split()
                for entry in tables_list:
                    drop_table_partition(status, dbms_name, entry, min_par)
        else:
            ret_val = drop_table_partition(status, dbms_name, table_name, min_par)
    else:
        status.add_error("Error in 'drop partition' command: 'where dbms = ...' is not provided correctly")
        ret_val = process_status.ERR_command_struct


    return ret_val
# =======================================================================================================================
# For a given table - drop the last partition
# =======================================================================================================================
def drop_table_partition(status, dbms_name, table_name, min_par):

    ret_val = process_status.Drop_partition_failed

    if partitions.is_partitioned(dbms_name, table_name):
        par_list = db_info.get_table_partitions_list(status, dbms_name, table_name)
        if par_list:
            par_count = len(par_list)       # number of partitions
            if par_count == 0:
                status.add_error("Table %s.%s has no partitioned data" % (dbms_name, table_name))
            elif par_count == 1:
                status.add_error("Table %s.%s has only one partition" % (dbms_name, table_name))
            elif par_count <= min_par:
                status.add_error("Table %s.%s has %u partitions which fails to statisfy the minimum required (%u)" % (
                dbms_name, table_name, len(par_list), min_par))
            else:
                ret_val = process_status.SUCCESS
    else:
        status.add_error("Table %s.%s is not partitioned" % (dbms_name, table_name))

    if not ret_val:

        for counter, par_name in enumerate(par_list):
            if (par_count - counter) <=  min_par:
                break
            if not db_info.drop_table(status, dbms_name, par_name):
                status.add_error("Failed to drop partition: %s.%s" % (dbms_name, par_name))
                ret_val = process_status.Drop_partition_failed
                break
            if not min_par:
                break           # drop a single partition

    return ret_val

# =======================================================================================================================
# backup a table or a partition
# Backup table - backups all partitions in the table
# Backup partition - backup a single partition
# Example - backup table where dbms = purpleair and table = readings and dest = [backup path]
# Example - backup partition where dbms = purpleair and table = readings and partition = par_readings_2018_08_00_d07_timestamp and dest = [backup path]
# Backup outputs data in a JSON format to a destination drive
# =======================================================================================================================
def _backup(status, io_buff_in, cmd_words, trace):
    ret_val = process_status.SUCCESS
    if cmd_words[1] == "table":
        backup_type = 1
    elif cmd_words[1] == "partition":
        backup_type = 2
    else:
        ret_val = process_status.ERR_command_struct

    if not ret_val:
        # with conditions - get the conditions to execute the JOB
        # get the conditions to execute the JOB
        #                                   Must     Add      Is
        #                                   exists   Counter  Unique
        keywords = {"dbms": ("str", True, False, True),
                    "table": ("str", True, False, True),
                    "partition": ("str", False, False, True),
                    "dest": ("str", True, False, True),
                    "show": ("bool", False, False, True),  # Display backup status
                    }

        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3, 0, keywords, False)
        if ret_val:
            # conditions not satisfied by keywords or command structure
            return ret_val

        dbms = interpreter.get_one_value(conditions, "dbms").lower()
        table = interpreter.get_one_value(conditions, "table").lower()
        dest = interpreter.get_one_value(conditions, "dest").lower()
        show = interpreter.get_one_value(conditions, "show")
        if backup_type == 2:
            partition = interpreter.get_one_value(conditions, "partition")
            if not partition:
                status.add_error("Missing partition name in backup command")
                return process_status.ERR_command_struct

        ret_val = utils_io.test_dir_exists_and_writeable(status, dest, True)

        if not ret_val:
            # directory is writeable
            # get the list of partitions
            if backup_type == 1:
                # backup all partitions
                ret_val = backup_partition(status, io_buff_in, dbms, table, "", dest, show, trace)
            else:
                par_list = db_info.get_table_partitions_list(status, dbms, table)
                if par_list and len(par_list):
                    # backup one or all partitions
                    if partition == "all":
                        for par_name in par_list:
                            ret_val = backup_partition(status, io_buff_in, dbms, table, par_name, dest, show, trace)
                            if ret_val:
                                break
                    elif partition == "first":
                        # Backup first partition
                        par_name = par_list[0]
                        ret_val = backup_partition(status, io_buff_in, dbms, table, par_name, dest, show, trace)
                    elif partition == "last":
                        # Backup last  partition
                        par_name = par_list[-1]
                        ret_val = backup_partition(status, io_buff_in, dbms, table, par_name, dest, show, trace)
                    elif partition in par_list:
                        ret_val = backup_partition(status, io_buff_in, dbms, table, partition, dest, show, trace)
                    else:
                        status.add_error("The named partition: '%s' does not exists in dbms '%s' and table '%s'" % (
                        partition, dbms, table))
                        ret_val = process_status.Backup_failed
                else:
                    status.add_error("The table '%s.%s' has no partitioned data'" % (dbms, table))
                    ret_val = process_status.Backup_failed

    return ret_val


# =======================================================================================================================
# Select the data from one partition to a JSON file
# =======================================================================================================================
def backup_partition(status, io_buff_in, dbms, table, partition, dest, show, trace):
    backup_time = utils_columns.get_current_time("%Y_%m_%d_%H_%M_%S")

    if partition:
        sql_stmt = "select * from %s" % partition
        f_name = 'backup.partition.%s.%s.%s.%s' % (backup_time, dbms, table, partition)
    else:
        sql_stmt = "select * from %s" % table
        f_name = 'backup.table.%s.%s.%s' % (backup_time, dbms, table)

    ret_val, file_name = utils_io.make_path_file_name(status, dest, f_name, "json")

    if not ret_val:
        ret_val = utils_io.test_dir_exists_and_writeable(status, dest, True)
        if not ret_val:
            command = ["sql", dbms, "dest", "=", "file", "and", "file", "=", file_name, "and", "format", "=", "json",
                       sql_stmt]
            ret_val = _issue_sql(status, io_buff_in, command, trace)

    return ret_val


# =======================================================================================================================
# Return GOTO or ON_ERROR value
# Note: 2 commands call this method
# =======================================================================================================================
def _return_goto(status, io_buff_in, cmd_words, trace):
    words_count = len(cmd_words)
    if words_count == 2 and cmd_words[0] == "goto":
        status.set_goto_name(cmd_words[1])
        return process_status.GOTO
    if cmd_words[0] == "on":
        if words_count == 4:
            # on error goto X or on error call X
            if cmd_words[2] == "goto":
                status.set_goto_name(cmd_words[3])
                return process_status.ON_ERROR_GOTO
            elif cmd_words[2] == "call":
                status.set_goto_name(cmd_words[3])
                return process_status.ON_ERROR_CALL
            elif cmd_words[2] == "end" and cmd_words[3] == "script":
                return process_status.ON_ERROR_END_SCRIPT
        elif words_count == 3 and cmd_words[2] == "ignore":
            return process_status.ON_ERROR_IGNORE
    elif words_count == 2 and cmd_words[0] == "call":
        # call X
        status.set_goto_name(cmd_words[1])
        return process_status.CALL

    return process_status.ERR_command_struct


# =======================================================================================================================
# Raise exit flag for all active threads to terminate and then exit
# Or reset the exits flags: exists reset
# Or exists from one process: exit rest, exit tcp, exit threads
# =======================================================================================================================
def _exit(status, io_buff_in, cmd_words, trace):

    global workers_pool

    if cmd_words[1] == "message":  # a tcp sql message
        mem_view = memoryview(io_buff_in)
        command = message_header.get_command(mem_view)
        words_array = command.split()
        if len(words_array) < 2:
            return process_status.ERR_command_struct
    else:
        words_array = cmd_words

    words_count = len(words_array)

    process_name = words_array[1]

    ret_val = process_status.SUCCESS

    if words_count == 2 and process_name == "node":
        # EXIT ANYLOG
        # disconnect dbms
        dbms_list = db_info.get_dbms_array()  # array with all databases
        for dbms in dbms_list:
            db_info.close_connection(status, dbms)

        process_status.set_exit("all")
        # wake the Rest server
        http_server.signal_rest_server()
        if workers_pool:
            workers_pool.exit()  # exit query threads

        mqtt_client.exit(0)

        grpc_client.exit("all")        # Exit All processes

        ret_val = process_status.EXIT
        for sleep_event in process_status.process_sleep_event.values():
            # the thread can be signaled to exit sleep
            sleep_event.set()

    elif words_count == 2 and process_status.set_exit(process_name):  # <-- Exit Flag set
        if process_name == "rest":
            # wake the Rest server
            http_server.signal_rest_server()
        elif process_name == "scripts":
            # End script - end a single script
            # Exit scripts - end current and callers scripts
            ret_val = process_status.EXIT_SCRIPTS # Return the value to the app

    elif words_count == 2 and process_name == "workers":
        # End query threads
        if workers_pool:
            workers_pool.exit()  # exit query threads

    elif words_count == 2 and process_name == "smtp":
        utils_output.exit_smtp()     # exit SMTP Client

    elif words_count == 2 and process_name == "mqtt":
        mqtt_client.exit(0)     # exit all clients

    elif words_count == 3 and process_name == "mqtt" and words_array[2].isdecimal():
        client_id = int(words_array[2])
        if mqtt_client.is_subscription(client_id):
            if mqtt_client.is_local_subscription(client_id):
                ret_val = message_server.unsubscribe_mqtt_topics(status, client_id)
                if not ret_val:
                    ret_val = mqtt_client.end_subscription(client_id, True)
            else:
                ret_val = mqtt_client.exit(client_id)   # Exit one client
        else:
            status.add_error("No MQTT subscription for client: %u" % client_id)
            ret_val = process_status.MQTT_wrong_client_id

    elif words_count == 3 and process_name == "grpc":
        ret_val = grpc_client.exit(words_array[2])

    elif words_count == 3 and process_name == "scheduler" and words_array[2].isdecimal():
        # exit specific scheduler
        task_scheduler.set_not_active(int(words_array[2]))

    else:
        status.add_error("Process name '%s' in 'exit' command is not valid" % process_name)
        ret_val = process_status.ERR_command_struct


    return ret_val

# =======================================================================================================================
# End the current running script
# =======================================================================================================================
def _end_script(status, io_buff_in, cmd_words, trace):
    if len(cmd_words) != 2:
        ret_val = process_status.ERR_command_struct
    else:
        ret_val = process_status.END_SCRIPT
    return ret_val


# =======================================================================================================================
# get a random substring from a string
# 'example': one_machine = random substring " " !ips_ports
# =======================================================================================================================
def _random_substr(status, io_buff_in, cmd_words, trace):
    words_count = len(cmd_words)

    if words_count < 2:
        return process_status.ERR_command_struct

    if cmd_words[1] == '=':
        if words_count < 4:
            return process_status.ERR_command_struct
        offset = 2  # assignment
    else:
        offset = 0  # just print result

    if words_count != (offset + 4):
        return process_status.ERR_command_struct

    separator = cmd_words[offset + 2]

    process_string = params.get_value_if_available(cmd_words[offset + 3])

    if len(process_string):

        str_array = process_string.split(separator)
        length = len(str_array)
        id = random.randint(0, length - 1)

        new_string = str_array[id]
    else:
        new_string = ""

    if offset == 2:
        params.add_param(cmd_words[0], new_string)  # add key value pair
    else:
        utils_print.output(new_string, True)  # print

    return process_status.SUCCESS


# =======================================================================================================================
# Process execute python command - replace !keys by dictionary values
# =======================================================================================================================
def _python(status, io_buff_in, cmd_words, trace):
    words_count = len(cmd_words)

    if words_count < 2:
        return process_status.ERR_command_struct

    offset = get_command_offset(cmd_words)

    if words_count == 2 + offset:
        # replace !word with value from dictionary and execute python
        ret_code, reply, dummy1, dummy2 = json_instruct.process_one_rule(status, False, cmd_words[offset + 1].strip(), None,
                                                                         None)
        if not ret_code:
            new_string = ""
            ret_val = process_status.ERR_process_failure
        else:
            new_string = str(reply)
            ret_val = process_status.SUCCESS
    else:
        ret_val = process_status.SUCCESS
        python_words = []
        for word in cmd_words[offset + 1:]:
            value = params.get_value_if_available(word)
            if not value:
                status.add_error(f"Failed to retrieve dictionary value for the key: {word}")
                ret_val = process_status.ERR_process_failure
                break
            # Add casting if ends with .str .int, .float, .bool
            if isinstance(value, str):
                if len(word) > 4 and word[-4:] == ".str":
                    value = f"str({value})"
            elif isinstance(value, int):
                value = f"int({value})"
            elif isinstance(value, float):
                value = f"float({value})"
            elif isinstance(value, bool):
                value = f"bool({value})"
            else:
                value = str(value)

            python_words.append(value)

        if not ret_val:
            pre_string = utils_data.get_str_from_array(python_words, 0, 0)
            ret_val, new_string = utils_python.al_python(status, pre_string)

    if not ret_val:
        if offset:
            # error will put empty string in the var
            params.add_param(cmd_words[0], new_string)
        else:
            utils_print.output(new_string, True)  # print

    return ret_val


# ==================================================================
# Get policy of type 'instructions' by an ID
# ==================================================================
def get_instructions(status, instruct_id):
    cmd_get_instruct = ["blockchain", "get", "(mapping, transform)", "where", "id", "="]
    cmd_get_instruct.append(instruct_id)
    ret_val, mapping = blockchain_get(status, cmd_get_instruct, "", True)  # Get info from the blockchain
    if not ret_val:
        if isinstance(mapping, list) and len(mapping) == 1:
            instruct = mapping[0]
        else:
            instruct = None
    else:
        instruct = None

    if not instruct:
        status.add_error("Failed to retrieve Mapping or Transform instructions with id: '%s'" % instruct_id)

    return instruct


# =======================================================================================================================
# command - show servers for dbms = dbms_name
# or - show servers for dbms = dbms_name and table = table_name
# =======================================================================================================================
def show_servers_for_dbms(status, dbms_name, table_name):
    blockchain_file = params.get_value_if_available("!blockchain_file")
    if blockchain_file == "":
        info_string = "Missing dictionary definition for \'blockchain_file\'"
    else:
        operators = get_operators_ip_by_table(status, blockchain_file, None, dbms_name, table_name)
        if not operators:
            info_string = "No operators for database '%s' and table '%s'" % (dbms_name, table_name)
        else:
            info_string = utils_json.to_string(operators)

    return info_string

# =======================================================================================================================
# Get the list of operators that satisfy the keys provided
# The lookup considers the metadata - if not data in the metadata - search the blockchain file
# =======================================================================================================================
def get_operators_ip_by_table(status, blockchain_file, company_name, dbms_name, table_name):
    '''
    Return a comma seperated list (string) of ip:port of operators that manage - company-dbms-table info.
    '''

    operators_ips = metadata.get_data_destination_operator_ip_port(status, company_name, dbms_name, table_name, 1)
    if not operators_ips:
        operators_ips = blockchain.get_operators_ip_string(status, blockchain_file, ["dbms=" + dbms_name, "table=" + table_name], None)

    return operators_ips

# =======================================================================================================================
# Get the list of operators that satisfy the keys provided
# The lookup considers the metadata - if not data in the metadata - search the blockchain file
# =======================================================================================================================
def get_operators_json_by_table(status, blockchain_file, company_name, dbms_name, table_name):
    '''
    Return a list with the policies (JSONs) that satisfy the search
    '''

    operators_json = metadata.get_operator_json(status, company_name, dbms_name, table_name, False,1)
    if not operators_json:
        cmd_words = "blockchain get operator where company = %s and dbms = %s and table = %s" % (company_name, dbms_name, table_name)
        err_val, operators_json = blockchain_get(status, cmd_words.split(), blockchain_file, True)


    return operators_json
# =======================================================================================================================
# command - get the list of tables registered in the blockchain and assigned to the specified dbms
# =======================================================================================================================
def get_tables_for_dbms(status, dbms_name):
    cmd_words = "blockchain get table where dbms = %s bring ['table']['name'] separator = ," % dbms_name
    err_val, table_string = blockchain_get(status, cmd_words.split(), "", True)
    if err_val or not len(table_string):
        table_list = []
    else:
        table_list = table_string.split(',')
        table_list.sort()

    return table_list
# =======================================================================================================================
# Get configuration params
# get status, get connections,
# =======================================================================================================================
def _process_get(status, io_buff_in, cmd_words, trace):

    ret_val, is_msg, param_name, words_array =  pre_process_command(status, io_buff_in, cmd_words)

    is_policy = False       # Used as a print instruction with the CLI
    if not ret_val:
        reply_list = _exec_child_dict(status, commands["get"]["methods"], commands["get"]["max_words"], io_buff_in, words_array, 1, trace)
        if len(reply_list) == 3:
            # The 3rd entry is a print instruction
            if reply_list[2] == "json":
                is_policy = True
        ret_val = reply_list[0]
        reply = reply_list[1]
    else:
        reply = ""

    post_process_command(status, io_buff_in, ret_val, is_msg, is_policy, reply, param_name)

    return ret_val

# =======================================================================================================================
# Get configuration params
# get status, get connections,
# =======================================================================================================================
def _query_status(status, io_buff_in, cmd_words, trace):

    ret_val, is_msg, param_name, words_array =  pre_process_command(status, io_buff_in, cmd_words)

    if not ret_val:
        ret_val, reply = _exec_child_dict(status, commands["query"]["methods"], commands["query"]["max_words"], io_buff_in, words_array, 1, trace)
    else:
        reply = ""

    post_process_command(status, io_buff_in, ret_val, is_msg, False, reply, param_name)

    return ret_val


# =======================================================================================================================
# Determine if command is from a peer or current node - and provide command variables
# =======================================================================================================================
def pre_process_command(status, io_buff_in, cmd_words):

    if len(cmd_words) < 2:
        is_msg = False
        ret_val = process_status.ERR_command_struct
    else:
        ret_val = process_status.SUCCESS

        if cmd_words[1] == "message":  # a tcp sql message
            is_msg = True
            mem_view = memoryview(io_buff_in)
            command = message_header.get_command(mem_view)
            msg_words = command.split()
            words_array = msg_words
            if len(words_array) < 2:
                if len(words_array) == 1:
                    status.add_error("Peer message error: Command structure error: <%s>" % words_array[0])
                else:
                    status.add_error("Peer message error: Command structure error")
                ret_val = process_status.ERR_command_struct
        else:
            is_msg = False
            words_array = cmd_words

    if not ret_val:
        offset = get_command_offset(words_array)
        if offset:
            # assign returned data from command to a variable
            param_name = words_array[0]
        else:
            param_name = None
    else:
        offset = 0
        words_array = cmd_words
        param_name = None

    return [ret_val, is_msg, param_name, words_array]

# =======================================================================================================================
# Print output or send as a message
# is_msg - is command from a peer node
# is_policy - is reply a policy to print
# =======================================================================================================================
def post_process_command(status, io_buff_in, ret_val, is_msg, is_policy, reply, param_name):

    if is_msg:
        if ret_val:
            reply = process_status.get_status_text(ret_val)
            display = "echo"  # error goes to the message queue
        else:
            display = "print"  # Get reply is printed to stdout

        ret_val = send_display_message(status, ret_val, display, io_buff_in, reply, is_policy)  # send text message to peers

    elif ret_val == process_status.SUCCESS:
        if param_name:
            params.add_param(param_name, reply)
        else:
            if status.get_active_job_handle().is_rest_caller():
                status.get_active_job_handle().set_result_set(reply)
                if status.is_rest_wait():   # if a REST caller is on wait for a reply from the destinations nodes
                    status.get_active_job_handle().signal_wait_event()  # signal the REST thread that output is ready
            else:
                if reply:
                    if is_policy:
                        utils_print.struct_print(reply, True, True)
                    else:
                        utils_print.output(reply, True)

# =======================================================================================================================
# Process commands from a child dictionary
# max_words are the max words in the child dict that makes a key to the dictionary
# parent_key_length = the number of words in the parent key
# Values are params to pass to the methods
# =======================================================================================================================
def _exec_child_dict(status, cmd_dict, max_words, io_buff_in, cmd_words, parent_key_length, trace, func_params = None):

    offset = get_command_offset(cmd_words)
    words_count = len(cmd_words)

    new_count, method_name = _words_to_command(cmd_words[parent_key_length + offset:], cmd_dict, max_words)   # return the number of words that make a key
    if new_count:

        method = cmd_dict[method_name]["command"]       # Get the ptr to the command

        if "trace" in cmd_dict[method_name] and cmd_dict[method_name]["trace"]:
            trace = cmd_dict[method_name]["trace"]  # Change the trace level

        if "words_count" in cmd_dict[method_name] and (words_count - offset) !=  cmd_dict[method_name]["words_count"]:
            reply_array = [process_status.ERR_command_struct, "Error in command text or variables values"]     # Not eaqul to the required number of words
        elif "words_min" in cmd_dict[method_name] and (words_count - offset) <  cmd_dict[method_name]["words_min"]:
            reply_array = [process_status.ERR_command_struct, "Error in command text or variables values"]     # less than the min number of words
        elif "key_only" in cmd_dict[method_name] and (new_count + parent_key_length + offset) != len(cmd_words) and \
                not is_with_format(cmd_dict, method_name, cmd_words, new_count + parent_key_length + offset ):
            # Only the key is provided as a command
            reply_array = [process_status.ERR_command_struct, "Error in command text or variables values"]
        else:
            if func_params:
                # additional params to pass to the method
                reply = method(status, io_buff_in, cmd_words, trace, func_params) # Exec command
            else:
                reply = method(status, io_buff_in, cmd_words, trace)  # Exec command

            if isinstance(reply,list):
                reply_array = reply
            else:
                reply_array = [reply]       # always return a list
    else:
        if words_count == 2 and (cmd_words[1][0] == '!' or cmd_words[1][0] == '$'):
            # returns variable value: i.e.: get !abc
            dict_value = params.get_value_if_available(cmd_words[1]) # get value from dictionary
            reply_array = [process_status.SUCCESS, dict_value]
        else:
            reply_array = [process_status.ERR_command_struct, None]

    return reply_array

# =======================================================================================================================
# Test if command is defined with format: "where format = json"
# =======================================================================================================================
def is_with_format(cmd_dict, method_name, cmd_words, offset_format ):

    if not "with_format" in cmd_dict[method_name]:
        return False

    if len(cmd_words) != offset_format + 4:
        return False

    return utils_data.test_words(cmd_words, offset_format ,  ["where", "format", "="])

# =======================================================================================================================
# Echo text to output (does not translate from dictionary)
# =======================================================================================================================
def _echo(status, io_buff_in, cmd_words, trace):
    global echo_queue

    words_count = len(cmd_words)

    if words_count < 2:
        return process_status.ERR_command_struct

    if cmd_words[1] == "message":  # a tcp sql message
        mem_view = memoryview(io_buff_in)

        ip, port = message_header.get_source_ip_port(mem_view)
        command = message_header.get_command(mem_view)[5:]
        command = command.replace('\t', ' ')

        is_last = message_header.is_last_block(mem_view)
        info_txt = message_header.get_data_decoded(mem_view)

        unique_job_id = message_header.get_job_id(mem_view)  # a unique ID of this JOB
        if unique_job_id:  # Source node is True if the message is a reply
            # Transfer print data to REST caller
            integrate_print_reply(status, False, False, True, mem_view, unique_job_id, None, 0, 0, False, None)
        else:
            if message_header.is_first_block(mem_view):

                if not echo_queue:
                    message = "\r\n[From Node %s:%u] %s " % (ip, port, command)
                    utils_print.output(message, False)
                    utils_print.struct_print(info_txt, False, False)
                    if is_last:
                        utils_print.output("\r\n", True)
                else:
                    message = "[From Node %s:%u] %s %s" % (ip, port, command, info_txt)
                    echo_queue.add_msg(message)
            else:
                if not echo_queue:
                    utils_print.output(info_txt, is_last)    # Add new line
                else:
                    echo_queue.add_msg(info_txt)
    else:
        words_array = cmd_words
        message = params.get_translated_string(words_array, 1, 0, False)
        if not echo_queue:
            utils_print.struct_print(message, True, True)
        else:
            echo_queue.add_msg(message)

    return process_status.SUCCESS

# =======================================================================================================================
# Output to echo queue if enabled - or to stdout if not available
# =======================================================================================================================
def output_echo_or_stdout(ip, port, info_txt):

    if ip and port:
        message = "[From Node %s:%s] %s " % (ip, port, info_txt)
    else:
        message = "[From Local Node] %s " % (info_txt)

    if not echo_queue:
        utils_print.output("\r\n" + message, True)
    else:
        echo_queue.add_msg(message)


# =======================================================================================================================
# Print to stdout with translations from dictionary
# =======================================================================================================================
def _print(status, io_buff_in, cmd_words, trace):
    words_count = len(cmd_words)

    if trace:
        utils_print.output("\r\n\nNew print message: %s\r\n\n" % ' '.join(cmd_words), False)

    if words_count < 2:
        return process_status.ERR_command_struct

    if cmd_words[1] == "message":  # a tcp sql message
        mem_view = memoryview(io_buff_in)
        ip, port = message_header.get_source_ip_port(mem_view)
        command = message_header.get_command(mem_view)[5:]
        command = command.replace('\t', ' ')

        is_last = message_header.is_last_block(mem_view)
        info_txt = message_header.get_data_decoded(mem_view)

        unique_job_id = message_header.get_job_id(mem_view)  # a unique ID of this JOB
        if unique_job_id:  # Source node is True if the message is a reply
            # data is assigned to a job instance:
            # a) Transfer print data to REST caller
            # b) Data is assigned to a key in the dictionary
            integrate_print_reply(status, False, False, True, mem_view, unique_job_id, None, 0, 0, False, None)
        else:

            if message_header.is_first_block(mem_view):

                message = "\r\n\n[From Node %s:%u] %s\r\n" % (ip, port, command)
                utils_print.output(message, False)
                if is_last and len(info_txt) <= 120 and info_txt.find('\n') == -1:
                    # Short text + 1 line message - print in the same line
                    new_line = False
                else:
                    new_line = True

                utils_print.struct_print(info_txt, new_line, is_last)

            else:

                utils_print.struct_print(info_txt, False, is_last)

            if message_header.is_last_block(mem_view):
                utils_print.output("\n", True)

    else:
        string_text = ""
        for word_count, word in enumerate(cmd_words[1:]):
            if word_count:
                string_text += ' '    # Add spaces to words
            string_text += str(params.get_value_if_available(word))   # Use dictionary to extract
        if len(string_text) > 1 and string_text[0] == "'" and string_text[-1] == "'":
            string_text = string_text[1:-1]         # Remove single quotation

        utils_print.struct_print(string_text, False, True)

    return process_status.SUCCESS

# =======================================================================================================================
# Aggregate all the print replies and
# Deliver the PRINT reply from the external node to the rest caller
# or assign the reply to a parameter
# The method can be called by a message - or a process in the query node
# =======================================================================================================================
def integrate_print_reply(status, is_async_io, is_subset, is_reply_msg, io_buff_in, unique_job_id, job_instance, r_id, n_ret_val, json_format, message):
    '''
    provided if the error was in the query node
    is_async_io - was called from message send using async_io
    is_subset - allow partial results to be returned
    is_reply_msg - called from a new reply
    job_instance
    r_id - receiver_id
    n_ret_val - error value
    json_format - True/False
    message - message returned to the REST caller
    '''

    if io_buff_in:
        # Message received from a different node
        job_location = message_header.get_job_location(io_buff_in)
        # Return reply to the REST caller

        mem_view = memoryview(io_buff_in)
        cmd_text = message_header.get_data_decoded(mem_view)

        node_ret_val = message_header.get_error(mem_view)

        j_instance = job_scheduler.get_job(job_location)

        is_json = message_header.is_json_message_format(mem_view) # Returns True if message is in JSON

        receiver_id = message_header.get_receiver_id(mem_view)

        is_last = message_header.is_last_block(mem_view)
    else:
        j_instance = job_instance
        receiver_id = r_id
        node_ret_val = n_ret_val
        is_json = json_format
        cmd_text = message
        is_last = False if is_async_io else True        # If is_async_io then failure of message send doesnt't terminate the object

    j_instance.data_mutex_aquire(status, 'R')  # Read Mutex - to test that the job is active

    if j_instance.is_job_active() and j_instance.get_unique_job_id() == unique_job_id:

        j_instance.add_print_msg(receiver_id, node_ret_val, is_json, cmd_text, is_last)        # Add the print to the buffer

        j_instance.set_par_ret_code(receiver_id, 0, node_ret_val)

    j_instance.data_mutex_release(status, 'R')

    if is_last:
        # Last block from the node received
        j_instance.end_job_instance(status, unique_job_id, is_reply_msg, is_subset)


# =======================================================================================================================
# Get data from Job instance which is updated by peer nodes on the peer nodes
# The data is returned as a list or a dictionary
# =======================================================================================================================
def get_data_struct_from_job(status, job_id, unique_job_id):
    reply = ""
    j_instance = job_scheduler.get_job(job_id)
    j_instance.data_mutex_aquire(None, 'W')  # Write Mutex
    # test thread did not timeout - or a previous process terminated the task
    if j_instance.is_job_active() and j_instance.get_unique_job_id() == unique_job_id:
        j_handle = j_instance.get_job_handle()
        assignment = j_handle.get_assignment()  # If with value - results are assigned to the assignment key
        if assignment and isinstance(assignment,str):
            is_dict = assignment[-2] == '{'  # Flag if output is set in a dictionary or as a list
            out_obj = j_instance.get_nodes_reply_object(is_dict)
            if out_obj:
                reply = utils_json.to_string(out_obj)
    j_instance.data_mutex_release(None, 'W')  # release the mutex that prevents conflict with the rest thread
    return reply
# =======================================================================================================================
# Assign values to variables - when a script is processe, assign the values provided with the scripts
# Variables are declared with the keyword variables followed by variable names in parenthesis, example:
# variables (var_name_1, var_name-2 ...)
# =======================================================================================================================
def assign_values_to_vars(status, script_name, command, values):
    ret_val = process_status.SUCCESS
    assigned = False
    offset_var = command.find("variables")
    if offset_var != -1:
        if command[:offset_var].find('#') == -1:
            # variables not commented out
            if offset_var == 0 or command[offset_var - 1] == ' ':
                paren_offset = command[offset_var + 9:].find('(')
                if paren_offset != -1:
                    index_start = offset_var + (paren_offset + 10)
                    index_end = command[index_start:].find(')')
                    if index_end != -1:
                        # matching brackets
                        variables = command[index_start:index_start + index_end].split(',')

                        if len(variables) != len(values):
                            status.add_error("Arguments mismatch in a script: %s" % script_name)
                            ret_val = process_status.Arguments_mismatch
                        else:
                            for counter, entry in enumerate(variables):
                                value = params.get_value_if_available(values[counter])  # if event on local node
                                params.add_param(entry.strip(), value)
                            assigned = True

    return [ret_val, assigned]
# =======================================================================================================================
# provide the standard name to a file
# time file rename [source file path and name] to dbms = [dbms name] and table = [table name] and source = [source ID] and hash = [hash value] and instructions = [instructions id]
# =======================================================================================================================
def rename_time_file(status, io_buff_in, cmd_words, trace):

    offset = get_command_offset(cmd_words)

    new_file = ""
    words_count = len(cmd_words)

    if (words_count + offset) < 4:
        return [process_status.ERR_command_struct, ""]  # needs to be attribute-name = value

    source_file = params.get_value_if_available(cmd_words[offset + 3])
    if not source_file:
        status.add_error("File not identified for 'time file rename' command: %s" % cmd_words[offset + 3])
        return [process_status.ERR_command_struct, ""]  # needs to be attribute-name = value

    file_path, file_name, file_type = utils_io.extract_path_name_type(source_file)
    source_struct = file_name.split('.')  # The elements in the source file

    if (words_count + offset) >= 8 and cmd_words[4] == "to":
        # time file rename [source file path and name] to  ...

        # with conditions - get the conditions to execute the JOB
        # get the conditions to execute the JOB
        #                               Must     Add      Is
        #                               exists   Counter  Unique
        keywords = {"dbms": ("str", False, False, True),
                    "table": ("str", False, False, True),
                    "source": ("str", False, False, True),
                    "hash": ("str", False, False, True),
                    "instructions": ("str", False, False, True),
                    }

        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, offset + 5, 0, keywords, False)
        if ret_val:
            # conditions not satisfied by keywords or command structure
            return [ret_val, ""]

        dbms_name = interpreter.get_one_value(conditions, "dbms")
        table_name = interpreter.get_one_value(conditions, "table")
        source = interpreter.get_one_value_or_default(conditions, "source", '0')
        hash_value = interpreter.get_one_value(conditions, "hash")
        instructions = interpreter.get_one_value_or_default(conditions, "instructions", '0')

    elif (words_count + offset) == 4 and len(source_struct) >= 2:
        # The case of: time file rename [source file path and name] --> keeps table name and file name and adds hash value
        dbms_name = source_struct[0]
        table_name = source_struct[1]
        source = "0"
        hash_value = None
        instructions = "0"
        ret_val = process_status.SUCCESS
    else:
        ret_val = process_status.ERR_process_failure

    if not ret_val:
        if not hash_value:
            got_hash, hash_value = utils_io.get_hash_value(status, source_file, "", dbms_name + '.' + table_name)
            if not got_hash:
                ret_val = process_status.ERR_process_failure

    if not ret_val:

        if not dbms_name and len(source_struct) > 0:
            dbms_name = source_struct[0]
        if not dbms_name:
            ret_val = process_status.ERR_dbms_name
        else:
            if not table_name and len(source_struct) > 1:
                table_name = source_struct[1]
            if not table_name:
                ret_val = process_status.ERR_table_name
            else:
                was_renamed, new_file = utils_io.make_json_file(status, source_file, dbms_name, table_name, source, hash_value, instructions)

                if not was_renamed:
                    ret_val = process_status.ERR_process_failure

    return [ret_val, new_file]
# =======================================================================================================================
# Update new JSON file
# example: time file new [file name] [status]
# test_file = D:\AnyLog-Code\EdgeLake\data\prep\lsl_demo.ping_sensor.0.123.0.json
# time file new !test_file updated
# New - validates that the file exists
# time file new !prep_dir/lsl_demo.ping_sensor.0.c490e6000d2287962d890a7cba2e1e74.0.51.2.201011010121.json
# Add - ignores that the file exists:
# time file add !prep_dir/lsl_demo.ping_sensor.0.c490e6000d2287962d890a7cba2e1e74.0.51.2.201011010121.json
# =======================================================================================================================
def new_time_file_entry(status, io_buff_in, cmd_words, trace):

    if not db_info.is_dbms("almgm"):
        status.add_error("DBMS 'almgm' is not connected")
        return [process_status.ERR_dbms_not_opened, "0"]

    offset = get_command_offset(cmd_words)

    words_count = len(cmd_words)

    # Get (optional) 2 status fields that are updated on the database
    if words_count == 4 + offset:
        file_status1 = ""
        file_status2 = ""
    elif words_count == 5 + offset:
        file_status1 = params.get_value_if_available(cmd_words[offset + 4])
        file_status2 = ""
    elif words_count == 6 + offset:
        file_status1 = params.get_value_if_available(cmd_words[offset + 4])
        file_status2 = params.get_value_if_available(cmd_words[offset + 5])
    else:
        return [process_status.ERR_command_struct, "0"]

    file_path_name = params.get_value_if_available(cmd_words[offset + 3])
    # The list of IPs

    if cmd_words[2] == "add":
        # validate that the file exists
        if not utils_io.is_path_exists(file_path_name):
            status.add_error("Wrong file name: '%s'" % file_path_name)
            return [process_status.ERR_process_failure, "0"]

    source_metadata = utils_io.FileMetadata()
    ret_val = source_metadata.set_file_name_metadata(status, file_path_name)
    if ret_val:
        return [ret_val, "0"]      # File is not structured correctly
    if file_status1:
        source_metadata.set_status1(file_status1)
        if file_status2:
            source_metadata.set_status1(file_status2)

    # Update the TIME SERIES DATA Status (into tsd_info table)
    # Return a ROW ID which identifies the insert
    node_member_id = metadata.get_node_member_id()
    row_id, file_time = db_info.tsd_insert_entry(status, node_member_id, source_metadata)

    if row_id == "0":
        ret_val = process_status.Failed_INSERT
    else:
        # The row was inserted, get a unique ID that represents the file
        ret_val = process_status.SUCCESS

    return [ret_val, row_id]

# =======================================================================================================================
# Delete one row from a TSD table
# time file delete X from tsd_y
# =======================================================================================================================
def delete_from_time_file(status, io_buff_in, cmd_words, trace):
    offset = get_command_offset(cmd_words)
    words_count = len(cmd_words)

    if words_count + offset != 6:
        ret_val = process_status.ERR_command_struct
    else:
        row_id = cmd_words[offset + 3]
        table_name = cmd_words[offset + 5]
        ret_val = db_info.delete_tsd_row(status, table_name, row_id)
    return [ret_val, None]
# =======================================================================================================================
# Update the status of a JSON file
# example: 'time file update [hash value] status1 status2
# If status field is "" - it is not updated
# =======================================================================================================================
def update_time_file(status, io_buff_in, cmd_words, trace):

    if not db_info.is_dbms("almgm"):
        status.add_error("DBMS 'almgm' is not connected")
        return [process_status.ERR_dbms_not_opened, None]

    offset = get_command_offset(cmd_words)
    words_count = len(cmd_words)

    if words_count == 5 + offset:
        status1 = cmd_words[4 + offset]
        status2 = ""
    elif words_count == 6 + offset:
        status1 = cmd_words[4 + offset]
        status2 = cmd_words[5 + offset]
    else:
        return [process_status.ERR_command_struct, None]

    hash_value = params.get_value_if_available(cmd_words[3])

    update_status = db_info.tsd_update_entry(status, hash_value, status1, status2)
    if not update_status:
        status.add_error("Failed to update state of Time Series Data file using hash key: '%s'" % hash_value)
        ret_val = process_status.Failed_UPDATE
    else:
        ret_val = process_status.SUCCESS
    return [ret_val, None]


# =======================================================================================================================
# Query  info from tsd table. There are 3 query options:

# get tsd details where ...
# get tsd summary where ...
# get tsd errors where ...

# time file get where [options]
# time file summary where [options]
# time file errors where [options]
# =======================================================================================================================
def query_time_file(status, io_buff_in, cmd_words, trace):

    words_count = len(cmd_words)

    offset = get_command_offset(cmd_words)

    if words_count < (3 + offset) or words_count == (4 + offset):
        return [process_status.ERR_command_struct, None]

    errors = False      # Retrieve error files
    summary = False     # Retrieve summary
    details = False     # retrieve details

    if cmd_words[2 + offset] == "details":
        details = True
    elif cmd_words[2 + offset] == "errors":
        errors = True
    elif cmd_words[2 + offset] == "summary":
        summary = True
        total_list = []        # A table with total files and rows for every table
    else:
        return [process_status.ERR_command_struct, None]

    if words_count == 3:
        # time file get -> no where condition - set the default values
        conditions = {
        }
    else:
        #                                Must     Add      Is
        #                                exists   Counter  Unique
        keywords = {"limit": ("int", False, False, True),
                    "table": ("str", False, False, True),
                    "hash": ("str", False, False, True),
                    "start_date": ("date", False, False, True),
                    "end_date": ("date", False, False, True),
                    "format": ("str", False, False, True),
                    }

        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4 + offset, 0, keywords, False)
        if ret_val:
            return [ret_val, None]

    limit = interpreter.get_one_value_or_default(conditions, "limit", 100)
    table_name = interpreter.get_one_value_or_default(conditions, "table", "tsd_info")
    hash_value = interpreter.get_one_value(conditions, "hash")
    start_date = interpreter.get_one_value(conditions, "start_date")
    end_date = interpreter.get_one_value(conditions, "end_date")
    reply_format = interpreter.get_one_value_or_default(conditions, "format", "table")  # Table or JSON

    sql_where = ""
    if start_date:
        sql_segment = "file_time >= '%s'" % start_date
        if not sql_where:
            sql_where = " where " + sql_segment
        else:
            sql_where += " and " + sql_segment

    if end_date:
        sql_segment = "file_time < '%s'" % end_date
        if not sql_where:
            sql_where = " where " + sql_segment
        else:
            sql_where += " and " + sql_segment

    if details or errors:
        # Cap the number of rows returned
        if hash_value:
            sql_where = " where file_hash = '%s'" % hash_value

        if limit:
            limit_stmt = "limit %u" % limit
        else:
            limit_stmt = ""

    if table_name == '*':
        # All TSD tables supported by this cluster
        blockchain_load(status, ["blockchain", "get", "cluster"], False, 0)
        # Get the member ID from all operators of the cluster
        operator_info = metadata.get_operators_info(status, None, False, ["member"])

        tsd_list = ["tsd_info"]     # First table is the local table
        for entry in operator_info:
            tsd_list.append( "tsd_%u" % entry[0])     # ADd a table for each member of the cluster
    else:
        tsd_list = [table_name]

    reply = ""
    reply_json = {}
    for tsd_table in tsd_list:

        member_id = metadata.get_node_member_id()
        if reply_format != "json":
            # Table format
            reply += "\r\n\nInfo on TSD Table: %s" % tsd_table
            if tsd_table == "tsd_info":
                reply += " (tsd_%u)" % member_id

        if summary:
            sql_stmt = "select dbms_name, table_name, min(file_time), min(file_id), max(file_time), max(file_id), count(file_id), "\
                       "count(distinct source), count(distinct status1),  count(distinct status2), sum(rows)"\
                       "  from %s %s group by dbms_name, table_name order by dbms_name, table_name;" % (tsd_table, sql_where)
        elif details:
            sql_stmt = "select * from %s %s order by file_id desc %s;" % (tsd_table, sql_where, limit_stmt)
        else:
            # List of errors
            sql_stmt = "select file_id, dbms_name, table_name, source, file_hash, instructions, file_time from %s %s order by file_id %s;" % (tsd_table, sql_where, limit_stmt)

        ret_val, data = db_info.tsd_info_select(status, tsd_table, sql_stmt)

        if not ret_val:
            if summary:

                # Update the total list
                for summary_entry in data:          # Go over all tables in the summary
                    db_name = summary_entry[0]
                    tb_name = summary_entry[1]
                    files_count = summary_entry[6]
                    rows_count = summary_entry[-1]
                    # Update the total lists
                    if reply_format == "json":
                        table_key = f"{db_name}.{tb_name}"
                        if table_key in reply_json:
                            table_info =  reply_json[table_key]     # The info for this table in all tsd files
                        else:
                            table_info = {}
                            table_info["files"] = 0
                            table_info["rows"] = 0
                            reply_json[table_key] =  table_info

                        table_info["files"] += files_count
                        table_info["rows"] += rows_count

                    else:
                        total_set = False
                        for total_entry in total_list:
                            if total_entry[0] == db_name and total_entry[1] == tb_name:
                                total_entry[2] += files_count
                                total_entry[3] += rows_count
                                total_set = True
                                break
                        if not total_set:
                            total_list.append([db_name, tb_name, files_count, rows_count])

                if reply_format != "json":
                    reply += utils_print.output_nested_lists(data, "",
                                                        ["DBMS", "Table", "Start Date", "From ID", "End Date", "To ID", "Files Count",
                                                         "Source Count", "Status 1", "Status 2", "Total Rows"], True)

            elif details:
                tsd_columns = ["ID", "DBMS", "Table", "Source", "Hash", "Instructions", "Date",
                                             "Rows", "Status 1", "Status 2"]
                if reply_format == "json":

                    tsd_rows = []
                    for one_row in data:
                        row_instance = {}
                        for index, column_val in enumerate(one_row):
                            col_str = str(column_val)
                            row_instance[tsd_columns[index]] = col_str   # Build a JSON entry
                        tsd_rows.append(row_instance)

                    reply_json[tsd_table] = tsd_rows

                else:
                    reply += utils_print.output_nested_lists(data, "", tsd_columns, True)
            else:
                # file_id, dbms_name, table_name, source, file_hash, instructions, file_time, rows, status1, status2
                # [dbms name].[table name].[data source].[hash value].[instructions].[TSD member ID].[TSD row ID].[TSD date].json
                for entry in data:
                    tsd_date = utils_io.utc_timestamp_to_key(str(entry[6]))
                    file_name = "%s.%s.%s.%s.%s.%s.%s.%s.json\r\n" % (entry[1],entry[2],entry[3],entry[4],entry[5],tsd_table[4:],entry[0],tsd_date)
                    reply += file_name
        else:
            reply = ""

    if summary:
        # Add totals
        if reply_format == "json":
            reply = utils_json.to_string(reply_json)
        else:
            reply += "\r\n\nTotal all TSD Tables"
            reply += utils_print.output_nested_lists(total_list, "",
                                             ["DBMS", "Table", "Files Count", "Total Rows"], True)
    else:
        if reply_format == "json":
            reply = utils_json.to_string(reply_json)

    return [ret_val, reply]

# =======================================================================================================================
# Show the list of time file tables
# Command: time file tables
# =======================================================================================================================
def get_tsd_list(status, io_buff_in, cmd_words, trace):

    reply = ""

    ret_val, data_string = db_info.get_tsd_table_list(status)  # from dbms = almgm and tables starting with tsd_

    if ret_val:
        tsd_json = utils_json.str_to_json(data_string)
        if not tsd_json or "Tables.almgm.tsd" not in tsd_json:
            status.add_error("Failed to retrieve TSD tables list")
            ret_val = process_status.No_tsd_tables
        else:
            tables_list = tsd_json["Tables.almgm.tsd"]
            for entry in tables_list:
                reply += "\r\n%s" % entry["table_name"]
            reply += "\r\n"
            ret_val = process_status.SUCCESS
    else:
        status.add_error("Failed to retrieve TSD tables list")
        ret_val = process_status.No_tsd_tables

    return [ret_val, reply]

# =======================================================================================================================
# drop one or all tsd tables
# Command: time file drop all
# Command: time file drop tsd_id
# =======================================================================================================================
def drop_tsd_tables(status, io_buff_in, cmd_words, trace):

    ret_val, data_string = db_info.get_tsd_table_list(status)  # from dbms = almgm and tables starting with tsd_

    if ret_val:

        target = cmd_words[3]           # A TSD table name to drop - or "all"
        tsd_json = utils_json.str_to_json(data_string)
        if not tsd_json or "Tables.almgm.tsd" not in tsd_json:
            status.add_error("Failed to retrieve TSD tables")
            ret_val = process_status.TSD_not_available
        else:

            ret_val = process_status.ERR_table_name
            tables_list = tsd_json["Tables.almgm.tsd"]

            for entry in tables_list:
                table_name = entry["table_name"]
                if target == "all" or table_name == target:

                    # drop all tables
                    if not db_info.drop_table(status, "almgm", table_name):  # drop the ledger table
                        ret_val = process_status.DBMS_not_open_or_table_err
                        break
                    ret_val = process_status.SUCCESS
                    if target != "all":
                        break
    else:
        status.add_error("Failed to retrieve TSD tables list")
        ret_val = process_status.No_tsd_tables

    return [ret_val, None]
# =======================================================================================================================
# Manage / Get the status of time series data files
# Example: new_name = time file rename !old_name dbms = dweet_demo table = bldg_pmc device = 265X48X2X34:2787 publisher = 548X23X243X12:2048
# =======================================================================================================================
def _time_file(status, io_buff_in, cmd_words, trace):


    if len(cmd_words) == 3 and cmd_words[2] == "message":  # a tcp sql message
        mem_view = memoryview(io_buff_in)
        command = message_header.get_command(mem_view)
        msg_words = command.split()
        words_array = msg_words
        message = True
    else:
        words_array = cmd_words
        message = False

    offset = get_command_offset(words_array)

    if len(words_array)  < (3 + offset):
        return process_status.ERR_command_struct

    ret_val, reply = _exec_child_dict(status, commands["time file"]["methods"], commands["time file"]["max_words"], io_buff_in, words_array, 2, trace)

    if message:
        if ret_val:
            reply = "command: '%s' returned error #%u: '%s'" % (' '.join(words_array), ret_val, process_status.get_status_text(ret_val) )
        if reply:
            ret_val = send_display_message(status, ret_val, "print", io_buff_in, reply, False)  # send text message to peers

    elif status.get_active_job_handle().is_rest_caller():
        if ret_val:
            reply = process_status.get_status_text(ret_val)
        if reply:
            status.get_active_job_handle().set_result_set(reply)
        status.get_active_job_handle().signal_wait_event()  # signal the REST thread that output is ready

    elif ret_val == process_status.SUCCESS and reply:
        if offset == 2:
            params.add_param(words_array[0], reply)
        else:
            utils_print.output(reply, True)

    return ret_val

# ======================================================================================================================
# Based on a JSON file generate an INSERT statement and execute it
# generate insert from json where dbms_name = lsl_demo and table_name = ping_sensor and json_file = !source_file and sql_dir = !sql_dir and instructions = !instruction
# ======================================================================================================================
def _map_json_to_insert(status, io_buff_in, cmd_words, trace):
    if len(cmd_words) < 2:
        return process_status.ERR_command_struct

    if cmd_words[1] == '=':
        offset = 2  # assignment
    else:
        offset = 0

    if len(cmd_words) < 9 + offset or cmd_words[4] != "where":
        return process_status.ERR_command_struct

    #                                Must     Add      Is
    #                                exists   Counter  Unique

    keywords = {"dbms_name": ("str", True, False, True),
                "table_name": ("str", True, False, True),
                "json_file": ("str", True, False, True),
                "sql_dir": ("str", False, False, True),
                "instructions": ("str", False, False, True),
                "tsd_name": ("int", False, False, True),        # 3 chars for TSD table name: tsd_127
                "tsd_id": ("int", False, False, True),          # The row ID in the TSD table
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 5, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return ret_val

    dbms_name = interpreter.get_one_value(conditions, "dbms_name").lower()

    if not db_info.is_dbms(dbms_name):
        status.add_error("DBMS '%s' is not connected" % dbms_name)
        return process_status.ERR_dbms_name

    table_name = interpreter.get_one_value(conditions, "table_name").lower()
    json_file = interpreter.get_one_value(conditions, "json_file")

    if "sql_dir" in conditions.keys():
        sql_dir = interpreter.get_one_value(conditions, "sql_dir")
    else:
        # same dir as the JSON file
        sql_dir, file_name, file_type = utils_io.extract_path_name_type(json_file)

    if "instructions" in conditions.keys():
        instructions = interpreter.get_one_value(conditions, "instructions")
        if not instructions:
            status.add_error("Missing 'instructions id' to map JSON to Insert statements")
            return process_status.Failed_to_retrieve_instructions
    else:
        instructions = "0"

    tsd_name = interpreter.get_one_value_or_default(conditions, "tsd_name", 0)
    if tsd_name > 999:
        status.add_error("TSD name is required to be a number and up to 3 digits")
        return process_status.ERR_command_struct
    tsd_id = interpreter.get_one_value_or_default(conditions, "tsd_id", 0)

    sql_file_list, rows_count = map_json_to_insert.map_json_file_to_insert(status, str(tsd_name), tsd_id, dbms_name, table_name, 0, json_file, sql_dir, instructions)
    if not sql_file_list or not len(sql_file_list):
        ret_val = process_status.ERR_json_to_insert
    else:
        ret_val = process_status.SUCCESS
        if offset:
            # with assignment - place the name of the sql file
            if len(sql_file_list) == 1:
                # only one file - no partitions used
                returned_data = sql_file_list[0]  # return the file name
            else:
                returned_data = str(sql_file_list)
            params.add_param(cmd_words[0], returned_data)

    return ret_val


# =======================================================================================================================
# Get the profiler output
# get profiler output where target = operator
# =======================================================================================================================
def get_profiler_output(status, io_buff_in, cmd_words, trace):

    reply = None

    if cmd_words[3] != "where":
        return [process_status.ERR_command_struct, None]

    operation = cmd_words[2]

    if operation != "output":
        return [process_status.ERR_command_struct, None]

    #                                Must     Add      Is
    #                                exists   Counter  Unique

    keywords = {"target": ("str", True, False, True),  # i.e. target = "operator"

                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return [ret_val, None]

    if not profiler.is_active():
        return process_status.Profiler_lib_not_loaded

    target = interpreter.get_one_value(conditions, "target")

    if not profiler.is_target(target):
        status.add_error("Not recognized profile target: %s" % target)
        return process_status.ERR_command_struct

    if profiler.is_running(target):
        # profiler was not set to off
        status.add_error("profiler for target: %s is at 'on' mode (switch to 'off')" % target)
        ret_val = process_status.Profiler_call_not_in_sequence
    else:

        reply = profiler.get_profiler_results(target)

    return [ret_val, reply]
# =======================================================================================================================
# Enable and disable the profiler
# set profiler on where target = operator
# =======================================================================================================================
def set_profiler(status, io_buff_in, cmd_words, trace):

    if cmd_words[3] != "where":
        return process_status.ERR_command_struct

    operation = cmd_words[2]

    if operation != "on" and operation != "off":
        return process_status.ERR_command_struct

    #                                Must     Add      Is
    #                                exists   Counter  Unique

    keywords = {"target": ("str", True, False, True),   # i.e. target = "operator"
                "reset": ("bool", False, False, True),  # Reset / maintain results
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return ret_val

    if not profiler.is_active():
        return process_status.Profiler_lib_not_loaded

    target = interpreter.get_one_value(conditions, "target")

    if not profiler.is_target(target):
        status.add_error("Not recognized profile target: %s" % target)
        return process_status.ERR_command_struct

    if operation == "on":
        reset_results = interpreter.get_one_value_or_default(conditions, "reset", True)
        if not profiler.set_instructions(target, True, reset_results):
            # is already on
            status.add_error("Repeatable calls to set profiler to 'on' for target: %s" % target)
            return process_status.Profiler_call_not_in_sequence
        comment = "Sart New Run" if reset_results else "Continue Previous Run"

    elif operation == "off":
        # Turn off
        if not profiler.set_instructions(target, False, False):
            # profiler was not turned on
            status.add_error("profiler for target: %s is at 'off' mode" % target)
            return process_status.Profiler_call_not_in_sequence
        comment = "Run Paused"
    else:
        status.add_error("Profile command is missing on/off call")
        ret_val = process_status.ERR_command_struct

    if not ret_val:
        opr_name = "Start" if operation == "on" else "Stop"
        utils_print.output_box(f"{opr_name} {target} profiling ... {comment}")

    return ret_val
# =======================================================================================================================
# Identify arbitrary messages and associate streaming data with a logical database, table and a topic
# Example: set msg rule my_rule if ip = 139.162.126.241 and header = al.sl.header.new_company.syslog then dbms = test and table = syslog and syslog = true
# Example: set msg rule kiwi_rule if ip = 10.0.0.78 and port = 1468 then dbms = test and table = syslog and syslog = true
# =======================================================================================================================
def set_msg_rule(status, io_buff_in, cmd_words, trace):

    if cmd_words[4] != "if":
        status.add_error("Missing 'if' keyword in 'set msg rule' command")
        return process_status.ERR_command_struct
    words_count = len(cmd_words)
    offset_then = -1
    for i in range(8, words_count):
        if cmd_words[i] == "then":
            offset_then = i
            break

    if offset_then == -1:
        status.add_error("Missing 'then' keyword in 'set msg rule' command")
        return process_status.ERR_command_struct

    rule_name = params.get_value_if_available(cmd_words[3])
    if not rule_name:
        status.add_error("Missing rule name in 'set msg rule' command")
        return process_status.ERR_command_struct

    # Identify the message by the Source message IP, Port, message header
    #                                           Must       Add      Is
    #                                           exists     Counter  Unique

    keywords = {"ip": ("str",                   False, True, True),
                "port": ("int",                 False, True, True),
                "header": ("str",               False, True, True), # text in the header (i.e. SysLog)
                }

    ret_val, counter, identify_cond = interpreter.get_dict_from_words(status, cmd_words, 5, offset_then, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return ret_val

    if not counter:
        status.add_error("Missing msg identifiers for 'set msg rule' command")
        return process_status.ERR_command_struct

    ret_val = interpreter.add_value_if_missing(status, identify_cond, "ip", "*")  # if IP not provided - all IPs
    if ret_val:
        return ret_val
    ret_val = interpreter.add_value_if_missing(status, identify_cond, "port", "*")  # if port not provided - all ports
    if ret_val:
        return ret_val

    # Get the mapping conditions
    #                                           Must       Add      Is
    #                                           exists     Counter  Unique

    keywords = {"dbms": ("str",                 True, False, True),
                "table": ("str",                True, False, True),
                "syslog": ("bool",              False, False, True),
                "topic": ("str",                False, False, True),
                "extend": ("str",              False, False, False),   # Add info to the user's data i.e. "extend" : "ip"
                "structure": ("str", False, True, True),  # text in the header (i.e. SysLog)
                }

    ret_val, counter, mapping_cond = interpreter.get_dict_from_words(status, cmd_words, offset_then + 1, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return ret_val

    if interpreter.test_one_value(mapping_cond, "syslog", True):
        ret_val = interpreter.add_value_if_missing(status, mapping_cond, "format", "bsd")   # The default syslog format
        if ret_val:
            return ret_val
        syslog_format = interpreter.get_one_value(mapping_cond, "format")
        if syslog_format != "bsd":
            status.add_error(f"SysLog format not supported: '{syslog_format}'")
            return process_status.Format_not_supported

    structure = interpreter.get_one_value(mapping_cond, "structure")
    if structure:
        if structure != "included":
            status.add_error(f"The value '{structure}' is not supported format in 'set msg rule' command")
            return process_status.ERR_command_struct


    message_server.set_msg_rules(status, rule_name, identify_cond, mapping_cond)      # Keep the mapping info

    return ret_val
# =======================================================================================================================
# Access services running inside a pod from a local machine
# Example: set port forward where pod_name = my_pod_name and namespace = my_namespace and local_port = 8080 and pod_port=80
# =======================================================================================================================
def set_port_forward(status, io_buff_in, cmd_words, trace):


    if not port_forwarding.is_installed():
        # portforward lib is not installed
        status.add_error("portforward Lib failed to import")
        return process_status.Failed_to_import_lib


    if cmd_words[3] == "where":

        #                                           Must       Add      Is
        #                                           exists     Counter  Unique
        keywords = {    "pod_name": ("str",         True,       False, True),
                        "namespace": ("str",        False,      False, True),
                        "local_port": ("int",       False,      False, True),
                        "pod_port": ("int",         False,      False, True),
                    }

        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
        if ret_val:
            # conditions not satisfied by keywords or command structure
            return ret_val

        # Start a thread for port forwarding
        t = threading.Thread(target=port_forwarding.configure, args=("dummy", conditions), name=f"port_forwarding_{conditions['namespace'][0]}_{conditions['pod_name'][0]}")
        t.start()



# =======================================================================================================================
# test process operator  -- > returns running if enabled
# =======================================================================================================================
def test_process_active(status, io_buff_in, cmd_words, trace):

    offset = get_command_offset(cmd_words)
    process_name = ' '.join(cmd_words[offset + 2:])
    if process_name in test_active_:
        ret_val = process_status.SUCCESS
        get_state = test_active_[process_name][1]  # A method or a variable to determine status

        is_active = get_state()  # Function

        if is_active:
            reply = 'true'
        else:
            reply = 'false'
    else:
        status.add_error("Wrong process name: '%s'" % process_name)
        reply = None
        ret_val = process_status.ERR_command_struct
    return [ret_val, reply]

# =======================================================================================================================
# Return the status of background processes
# =======================================================================================================================
def get_processes_stat(status, io_buff_in, cmd_words, trace):
    processes = []

    words_count = len(cmd_words)
    info_str = ""
    reply_format = "table"
    if words_count == 2:
        ret_val = process_status.SUCCESS
    elif words_count == 6 and cmd_words[2] == "where":
        reply_format = get_reply_format(status, cmd_words, 3)
        if reply_format == None or (reply_format != "table" and reply_format != "json"):
            ret_val = process_status.ERR_command_struct
        else:
            if reply_format == "json":
                reply_dict = {}                 # Reply is retuened in JSON
            ret_val = process_status.SUCCESS
    else:
        ret_val = process_status.ERR_command_struct

    if not ret_val:

        for methods in test_active_.values():
            get_state = methods[1]     # A method or a variable to determine status

            is_active = get_state()     # Function

            if is_active:
                value = 'Running'
            else:
                value = 'Not declared'

            get_info = methods[2]
            if not get_info:
                info_str = ""       # No string explaining the state
            else:
                info_str = get_info(status)

            if reply_format == "json":
                process_info = {
                    "Status" : value
                }
                if info_str:
                    process_info["Details"] = info_str

                reply_dict[methods[0]] = process_info       # Add service name
            else:
                processes.append((methods[0], value, info_str)) # Add service name


        if reply_format == "json":
            info_str = utils_json.to_string(reply_dict)
        else:
            info_str = utils_print.output_nested_lists(processes, "",["Process","Status","Details"], True, "    ")

    return [ret_val, info_str, reply_format]
# =======================================================================================================================
# Issue a rest call
# =======================================================================================================================
def _rest_client(status, io_buff_in, cmd_words, trace):
    words_count = len(cmd_words)
    offset = get_command_offset(cmd_words)

    if words_count < (3 + offset):
        return process_status.ERR_command_struct

    if cmd_words[2 + offset] != "where":
        status.add_error("Missing 'where' keyword in 'rest' command")
        return process_status.ERR_command_struct

    if cmd_words[1 + offset] == "get":
        ret_val = http_client.do_GET(status, cmd_words, offset)

    elif cmd_words[1 + offset] == "put":
        ret_val = http_client.do_PUT(status, cmd_words)

    elif cmd_words[1 + offset] == "post":
        ret_val = http_client.do_POST(status, cmd_words)

    elif cmd_words[1 + offset] == "view":
        ret_val = http_client.do_VIEW(status, cmd_words)

    elif cmd_words[1 + offset] == "delete":
        ret_val = http_client.do_DELETE(status, cmd_words)

    else:
        ret_val = process_status.ERR_command_struct

    return ret_val


# =======================================================================================================================
# Run REST server thread
# Example: run rest server where internal_ip = !ip and internal_port = 7849 and timeout = 0 and threads = 6 and ssl = true and ca_org = AnyLog and server_org = "Node 128"
# =======================================================================================================================
def _run_rest_server(status, io_buff_in, cmd_words, trace):
    words_count = len(cmd_words)


    if words_count > 5 and cmd_words[3] == "where":

        #                                           Must       Add      Is
        #                                           exists     Counter  Unique
        keywords = {    "internal_ip": ("str",      False,      False, True),
                        "internal_port": ("int",    False,      False, True),
                        "external_ip": ("str",      False,      False, True),
                        "external_port": ("int",    False,      False, True),
                        "timeout": ("int",          False,      False, True),
                        "threads": ("int",          False,      False, True),
                        "ssl": ("bool",             False,      True, True),
                        "ca_org": ("str",           False,      True, True),
                        "server_org": ("str",       False,      True, True),
                        "bind": ("bool",            False,      False, True),
                    }

        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
        if ret_val:
            # conditions not satisfied by keywords or command structure
            return ret_val

        if counter == 1 and "ssl" in conditions and conditions["ssl"][0] == False:
            # ssl is set to False and ca_org and server_org are not specified
            pass    # Ignore the ssl setting
        elif counter and counter != 3:
            # With SSL - define ssl and ca_org and server_org
            if not interpreter.get_one_value(conditions, "ssl"):
                status.add_error("Missing 'ssl = True' in 'run rest server' command")
            elif not interpreter.get_one_value(conditions, "ca_org"):
                status.add_error("Missing value to CA organization name, set 'ca_org' = [organization name]")
            else:
                status.add_error("Missing value to Server organization name, set 'server_org' = [organization name]")

            return process_status.ERR_command_struct

        internal_ip = interpreter.get_one_value_or_default(conditions, "internal_ip", "")
        internal_port = interpreter.get_one_value_or_default(conditions, "internal_port",0)
        external_ip = interpreter.get_one_value_or_default(conditions, "external_ip", internal_ip)
        external_port = interpreter.get_one_value_or_default(conditions, "external_port", internal_port)

        if not internal_ip:
            # take the external
            internal_ip = external_ip
        if not internal_port:
            # take the external
            internal_port = external_port
    else:
        return process_status.ERR_command_struct

    is_bind = interpreter.get_one_value_or_default(conditions, "bind", False)

    url_bind = internal_ip if is_bind else "" # Listening to one IP all all IPs

    ret_val = start_rest_threads(external_ip, external_port, internal_ip, internal_port, is_bind, url_bind, conditions)

    return ret_val
# =======================================================================================================================
# Start the REST threads
# =======================================================================================================================
def start_rest_threads(external_ip, external_port, internal_ip, internal_port, is_bind, url_bind, conditions):

    ret_val = net_utils.add_connection(1, external_ip, external_port, internal_ip, internal_port, url_bind, internal_port)
    if ret_val:
        return ret_val

    t = threading.Thread(target=http_server.rest_server, args=(params, internal_ip, internal_port, is_bind, conditions))
    t.start()
    # t.join()
    if interpreter.get_one_value_or_default(conditions, "ssl", False):
        # Provide time to add the password
        time.sleep(10)

    return process_status.SUCCESS

# =======================================================================================================================
# A process that flushes the streaming data to files
# Example: run streamer
# Example: run streamer where watch_dir = ...
# =======================================================================================================================
def _run_streamer(status, io_buff_in, cmd_words, trace):

    words_count = len(cmd_words)
    if words_count < 2 or words_count == 3:
        return process_status.ERR_command_struct

    if streaming_data.is_running:
        status.add_error("Duplicate call to initiate Streamer process")
        return process_status.Process_already_running

    if words_count == 2:
        # only use defaults
        conditions = {}
    else:
        keywords = {
                    "prep_dir": ("str", False, False, True),
                    "watch_dir": ("str", False, False, True),
                    "err_dir": ("str", False, False, True),
                    }

        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4 , 0, keywords, False)
        if ret_val:
            return ret_val

    # test that directories are defined
    ret_val = interpreter.add_value_if_missing(status, conditions, "prep_dir", "!prep_dir")
    if ret_val:
        return ret_val

    test_dir = interpreter.get_one_value(conditions, "prep_dir")
    ret_val = utils_io.test_dir_exists_and_writeable(status, test_dir, True)
    if ret_val:
        return ret_val

    ret_val = interpreter.add_value_if_missing(status, conditions, "watch_dir", "!watch_dir")
    if ret_val:
        return ret_val

    test_dir = interpreter.get_one_value(conditions, "watch_dir")
    ret_val = utils_io.test_dir_exists_and_writeable(status, test_dir, True)
    if ret_val:
        return ret_val

    ret_val = interpreter.add_value_if_missing(status, conditions, "err_dir", "!err_dir")
    if ret_val:
        return ret_val

    test_dir = interpreter.get_one_value(conditions, "err_dir")
    ret_val = utils_io.test_dir_exists_and_writeable(status, test_dir, True)
    if ret_val:
        return ret_val


    t = threading.Thread(target=streaming_data.flush_buffers, args=("dummy", conditions))
    t.start()

    return process_status.SUCCESS

# =======================================================================================================================
# Subscribe to a gRPC broker
# Example: run grpc client where broker = IP and port = port
'''
run grpc client where ip = 127.0.0.1 and port = 50051 and grpc_dir = D:/AnyLog-Code/EdgeLake/dummy_source_code/kubearmor/proto and proto = kubearmor and function = WatchLogs and request = RequestMessage and response = Log and service = LogService and value = (Filter = policy) and debug = true and limit = 2 and ingest = false
run grpc client where ip = 127.0.0.1 and port = 50051 and grpc_dir = D:/AnyLog-Code/EdgeLake/dummy_source_code/kubearmor/proto and proto = kubearmor and function = HealthCheck and request = NonceMessage and response = ReplyMessage and service = LogService and value = (nonce = 10.int) and debug = true and limit = 1 and ingest = false
'''
# =======================================================================================================================
def _run_grpc_client(status, io_buff_in, cmd_words, trace):

    words_count = len(cmd_words)
    if words_count < 11 or cmd_words[3] != "where":
        return process_status.ERR_command_struct

    #                          Must     Add      Is
    #                          exists   Counter  Unique

    keywords = {"name":  ("str", True, False, True),
                "ip":   ("str", True, False, True),
                "port": ("str", True, False, True),
                "policy": ("str", False, False, True),      # The Policy to apply on the data
                "grpc_dir": ("str", True, False, True),     # The gRPC directory with .proto and compiled files
                "proto": ("str", True, False, True),        # The .proto file name
                "function": ("str", True, False, True),     # The function called on the gRPC server
                "request": ("str", True, False, True),      # The .proto request message
                "response": ("str", True, False, True),     # The .proto response message
                "service": ("str", True, False, True),      # The name of the service in the .proto file
                "value": ("nested", False, False, True),      # The values to set in the message to the server (key = value pairs passed to the server function)
                "debug": ("bool", False, False, True),      # True or False to enable debug
                "add_info": ("str", False, False, False),   # Add info to the JSON file: "proto", "request", "conn"
                "limit": ("int", False, False, True),       # Exit after the specified number of rows returned
                "dbms": ("str", False, True, True),        # DBMS name (if not provided in the policy)
                "table": ("str", False, True, True),       # Table name (if not provided in a policy)
                "ingest": ("bool", False, False, True),    # False value means that AnyLog is not updated - default is True
                "reconnect" : ("bool", False, False, True),    # True value - reconnect automatically if connection lost - default is False
                "prep_dir": ("str", False, False, True),
                "watch_dir":("str", False, False, True),
                "err_dir":  ("str", False, False, True),
                "blobs_dir": ("str", False, False, True),
                "bwatch_dir": ("str", False, False, True),
                "log_error": ("bool", False, False, True),
                }


    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4 , 0, keywords, False)
    if ret_val:
        return ret_val

    if not grpc_client.is_installed():
        # gRPC lib is not installed
        status.add_error("gRPC Lib failed to import")
        return process_status.Failed_to_import_lib

    if counter:
        # dbms provided
        if counter != 2 and not "policy" in conditions:
            # No policy provided
            # If database is provided, then table needed to be as well
            status.add_error("Missing dbms and table definnitions")
            return process_status.Missing_dest_dbms_table

        dbms_name = interpreter.get_one_value(conditions, "dbms")
        if not db_info.is_dbms(dbms_name):
            status.add_error(f"DBMS '{dbms_name}' for grpc client is not declared")
            return process_status.ERR_dbms_not_opened

        
    # test that directories are defined
    ret_val = interpreter.add_defualt_dir(status, conditions, ["prep_dir", "watch_dir", "err_dir", "bwatch_dir", "blobs_dir"])
    if ret_val:
        return ret_val

    grpc_dir = conditions["grpc_dir"][0]  # Directory of the .proto files and the compiled .proto files
    ret_val = utils_io.test_dir_exists_and_writeable(status, grpc_dir, True)
    if ret_val:
        return ret_val

    if "policy" in conditions:
        # Optional
        policy_id = conditions["policy"][0]
        ret_val, ledger_policy = policies.get_policy_by_id(status, "mapping", policy_id)
        if ret_val:
            return ret_val

        mapping_policy = copy.deepcopy(ledger_policy)  # Make a copy of the policy as it may be changed using compile_ in Mapping_policy.apply_policy_schema()

        if "import" in mapping_policy["mapping"]:
            # Process is leveraging additional policies
            import_dictionary = {}   # Set dictionary for the actual policies

            policies_dict = mapping_policy["mapping"]
            for import_key, import_id in policies_dict.items():
                ret_val, ledger_policy = policies.get_policy_by_id(status, "mapping", import_id)

                if ret_val:
                    return ret_val

                import_policy = copy.deepcopy(ledger_policy)  # Make a copy of the policy as it may be changed using compile_ in Mapping_policy.apply_policy_schema()
                import_dictionary[import_key] = import_policy["mapping"]

            mapping_policy["mapping"]["import_"] = import_dictionary    # Update with a dictionary to the imported policies

    else:
        policy_id = ""
        mapping_policy = ""

    if not "ingest" in conditions:
        # If false, AnyLog is not updated
        conditions["ingest"] = [True]   # Sets the default value


    conn = f"{conditions['ip'][0]}:{conditions['port'][0]}"    # IP:Port

    proto_name = conditions["proto"][0]             # The .proto file name


    connection_id = conditions["name"][0]

    if grpc_client.is_connected(connection_id):
        status.add_error(f"Failed to subscribe to '{conn}' with '{connection_id}' as connection is already active")
        ret_val =  process_status.NETWORK_CONNECTION_FAILED
    else:

        # Start a thread for each proto file
        t = threading.Thread(target=grpc_client.subscribe, args=("dummy", conditions, conn, grpc_dir, proto_name, connection_id, policy_id, mapping_policy), name=f"grpc_{conn}_{proto_name}")
        t.start()

    return ret_val

# =======================================================================================================================
# Subscribe to a broker according to the url provided to receive data on the provided topic
# Example: run msg client where broker = "mqtt.eclipse.org" and (topic = lsl_data.ping_sensor.$SYS/#.0 and topic = 3)
# run msg client where broker = "mqtt.eclipse.org" and topic = (name = $SYS/# and dbms = lsl_demo and table =ping_sensot and qos = 2)
# =======================================================================================================================
def _run_msg_client(status, io_buff_in, cmd_words, trace):

    words_count = len(cmd_words)
    if words_count < 5 or cmd_words[3] != "where":
        return process_status.ERR_command_struct

    #                          Must     Add      Is
    #                          exists   Counter  Unique

    keywords = {"broker":   ("str", True, False, True),
                "user": ("str", False, False, True),
                "password": ("str", False, False, True),
                "client_id": ("str", False, False, True),       # Used in google's MQTT
                "project_id": ("str", False, False, True),      # Used in google's MQTT
                "private_key" : ("str", False, False, True),    # Used in google's MQTT
                "location": ("str", False, False, True),        # Used in google's MQTT
                "log":      ("bool", False, False, True),            # Log MQTT events
                "topic":    ("nested", True, False, False),
                "port":     ("int", False,False, True),
                "prep_dir": ("str", False, False, True),
                "watch_dir":("str", False, False, True),
                "err_dir":  ("str", False, False, True),
                "blobs_dir": ("str", False, False, True),
                "bwatch_dir": ("str", False, False, True),
                "log_error": ("bool", False, False, True),
                "user-agent" : ("str", False, False, True)  # Map REST calls with this header value to the MQTT client process
                }


    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4 , 0, keywords, False)
    if ret_val:
        return ret_val

    broker = interpreter.get_one_value(conditions, "broker")

    if not mqtt_client.is_installed():
        if broker != "rest" and broker != "local":
            # Paho lib is not needed if not pulling data from 3rd party broker
            status.add_error("MQTT Lib (paho) failed to import")
            return process_status.Failed_to_import_lib

    interpreter.set_nested_default(status, conditions, "topic", "qos", 0)       # Set 0 on QOS if missing

    # Topic info is not provided in parenthesis - Test QOS range
    ret_val = interpreter.test_nested_range(status, conditions, "topic", "qos", (0,2) )  # test quality of service is between 0 and 2
    if ret_val:
        return ret_val

    # test that directories are defined
    ret_val = interpreter.add_defualt_dir(status, conditions, ["prep_dir", "watch_dir", "err_dir", "bwatch_dir", "blobs_dir"])
    if ret_val:
        return ret_val


    ret_val, user_id = mqtt_client.register(status, conditions)
    if ret_val:
        return ret_val

    if broker != "rest" and broker != "local":
        # Change to rest or local if trying to bind to the local node
        port = interpreter.get_one_value_or_default(conditions, "port", 0)
        ip_port = "%s:%s" % (broker,port)
        if net_utils.get_ip_port(1,1) == ip_port or net_utils.get_ip_port(1,2) == ip_port:
            broker = "rest"
            utils_print.output_box("Identified local address (%s) to bind - Changing broker to 'rest'" % (ip_port))
        elif net_utils.get_ip_port(2,1) == ip_port or net_utils.get_ip_port(2,2) == ip_port:
            broker = "local"
            utils_print.output_box("Identified local address (%s) to bind - Changing broker to 'local'" % (ip_port))

    if broker == "rest":
        user_agent = interpreter.get_one_value(conditions, "user-agent")
        if not user_agent:
            # The user-agent determines which header is transformed to an broker message
            status.add_error("Missing 'user-agent' value to determine REST calls to publish")
            return process_status.MQTT_info_err

        # Tranform REST POST calls to data publish
        topics_dict = mqtt_client.get_topics_by_id(user_id)
        # We assume one topic, otherwise, topic is expected in header of the REST call
        if topics_dict and isinstance(topics_dict, dict) and len(topics_dict):
            default_topic = next(iter(topics_dict))
            http_server.assign_user_agent_to_mqtt(user_agent, user_id, default_topic)  # Assign data with this user agent to the MQTT client
    elif broker == "local":
        # Use the message broker
        ret_val = message_server.subscribe_mqtt_topics(status, user_id)
    else:
        # Publish topics to external MQTT Broker
        t = threading.Thread(target=mqtt_client.subscribe, args=("dummy", conditions, user_id))
        t.start()
        ret_val = process_status.SUCCESS

    return ret_val
# =======================================================================================================================
# Publish a message to an mqtt broker
# Example: mqtt publish where broker = "mqtt.eclipseprojects.io" and log = true and topic = anylog and message = "test message',
# =======================================================================================================================
def _mqtt_request(status, io_buff_in, cmd_words, trace):

    words_count = len(cmd_words)
    if words_count <= 5 or cmd_words[1] != "publish" or cmd_words[2] != "where":
        return process_status.ERR_command_struct

    keywords = {"broker":   ("str", True, False, True),
                "user": ("str", False, False, True),
                "password": ("str", False, False, True),
                "topic": ("str", True, False, False),
                "message": ("str", True, False, True),
                "client_id": ("str", False, False, True),       # Used in google's MQTT
                "project_id": ("str", False, False, True),      # Used in google's MQTT
                "private_key" : ("str", False, False, True),    # Used in google's MQTT
                "log": ("bool", False, False, True),            # Log MQTT events
                "port":     ("int", False,False, True),
                "qos":  ("int", False, False, True),
                }

    if not mqtt_client.is_installed():
        status.add_error("MQTT Lib (paho) failed to import")
        return process_status.Failed_to_import_lib

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3 , 0, keywords, False)
    if ret_val:
        return ret_val

    ret_val = mqtt_client.publish(status, conditions)

    if status.get_active_job_handle().is_rest_caller():
        # WIth REST
        if not ret_val:
            status.get_active_job_handle().set_result_set("{\"MQTT Publish\" : \"OK\"}")
        status.get_active_job_handle().signal_wait_event()  # signal the REST thread that output is ready

    return ret_val

# =======================================================================================================================
# run kafka consumer where ip = 198.74.50.131 and port = 9092 and topic = (name = abc and dbms = lsl_demo and table = ping_sensor and column.timestamp.timestamp = "bring [timestamp]" and column.value.int = "bring [value]")
# =======================================================================================================================
def _run_kafka_consumer(status, io_buff_in, cmd_words, trace):

    words_count = len(cmd_words)
    if words_count < 7 or cmd_words[3] != "where":
        return process_status.ERR_command_struct

    keywords = {"ip":   ("str", True, False, True),
                "port": ("str", True, False, True),
                "topic":    ("nested", True, False, False),
                "threads": ("int", False, False, True),
                "reset": ("str", False, False, True),       # Can have the value earliest or latest
                }

    if not al_kafka.is_installed():
        status.add_error("Kafka Lib (confluent_kafka) failed to import")
        return process_status.Failed_to_import_lib


    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4 , 0, keywords, False)
    if ret_val:
        return ret_val

    interpreter.set_nested_default(status, conditions, "topic", "qos", 0)       # Set 0 on QOS if missing

    ret_val = interpreter.test_values(status, conditions, "reset", {"earliest" : 1, "latest" : 1})
    if ret_val:
        return ret_val


    # Topic info is not provided in parenthesis - Test QOS range
    ret_val = interpreter.test_nested_range(status, conditions, "topic", "qos", (0,2) )  # test quality of service is between 0 and 2
    if ret_val:
        return ret_val

    # test that directories are defined
    ret_val = interpreter.add_value_if_missing(status, conditions, "prep_dir", "!prep_dir")
    if ret_val:
        return ret_val
    ret_val = interpreter.add_value_if_missing(status, conditions, "watch_dir", "!watch_dir")
    if ret_val:
        return ret_val
    ret_val = interpreter.add_value_if_missing(status, conditions, "err_dir", "!err_dir")
    if ret_val:
        return ret_val


    interpreter.add_value(conditions, "broker" , "kafka")   # Flag that this is a Kafka broker

    # Associate this user with the topics and get back a USER ID
    ret_val, user_id = mqtt_client.register(status, conditions)
    if ret_val:
        return ret_val


    t = threading.Thread(target=al_kafka.pull_data, args=(user_id, conditions))
    t.start()

    return process_status.SUCCESS


# =======================================================================================================================
# Run message broker  !ip !port !local_ip !local_port threads
# =======================================================================================================================
def _run_message_broker(status, io_buff_in, cmd_words, trace):


    ret_val, url, port, url_internal, port_internal, workers_count, is_bind = net_utils.get_config_server_params(status, "Message Broker", cmd_words, 6)

    if ret_val:
        return ret_val        # Missing command params

    if is_bind:
        url_bind = url_internal
    else:
        url_bind = ""   # Listening to all IPs

    ret_val = start_broker_threads(url, port, url_internal, port_internal, is_bind, url_bind, workers_count, trace)
    return ret_val

# =======================================================================================================================
# Init the broler threads
# =======================================================================================================================
def start_broker_threads(url, port, url_internal, port_internal, is_bind, url_bind, workers_count, trace):

    ret_val = net_utils.add_connection(2, url, port, url_internal, port_internal, url_bind,  port_internal)
    if ret_val:
        return ret_val

    t = threading.Thread(target=message_server.message_broker, args=(url_internal, int(port_internal), is_bind, workers_count, trace))
    t.start()

    return process_status.SUCCESS

# =======================================================================================================================
# Run TCP server thread
# Example: run TCP server !ip !port !local_ip !local_port threads
# Example: run tcp server where internal_ip = !ip and internal_port = !port and external_ip = !external_ip and external_port = !extenal_port and bind = !is_bind and workers = !workers_count
# =======================================================================================================================
def _run_tcp_server(status, io_buff_in, cmd_words, trace):

    ret_val, url, port, url_internal, port_internal, workers_count, is_bind = net_utils.get_config_server_params(status, "TCP Server", cmd_words, 6)

    if ret_val:
        return ret_val        # Missing command params

    if is_bind:
        url_bind = url_internal
    else:
        url_bind = ""   # Listening to all IPs

    ret_val = start_tcp_threads(url, port, url_internal, port_internal, is_bind, url_bind, workers_count, trace)

    return ret_val

# =======================================================================================================================
# Run TCP server thread - called from different methods
# =======================================================================================================================
def start_tcp_threads(url, port, url_internal, port_internal, is_bind, url_bind,  workers_count, trace):

    if is_bind:
        url_to_use = url_bind if url_bind else url
    else:
        url_to_use = url

    if not url_to_use:
        url_to_use = url_internal

    port_to_use = port_internal if port_internal else port     # Always show port

    ret_val = net_utils.add_connection(0, url_to_use, port_to_use, url_internal, port_internal, url_bind,  port_to_use)
    if ret_val:
        return ret_val

    if (url and port) and not  url_internal:
        url_internal = url
        port_internal = port

    t = threading.Thread(target=tcpip_server.tcp_server, args=(url_to_use, port_to_use, is_bind, workers_count, trace))
    t.start()
    # t.join()

    return process_status.SUCCESS

# =======================================================================================================================
# Stream JSON to a database
# Example:
# stream !json_data where dbms = my_dbms and table = my_table
# =======================================================================================================================
def stream_data(status, io_buff_in, cmd_words, trace):
    if cmd_words[1] == "message":  # a tcp sql message
        mem_view = memoryview(io_buff_in)
        command = message_header.get_command(mem_view)
        command = command.replace('\t', ' ')
        words_array, left_brackets, right_brakets = utils_data.cmd_line_to_list_with_json(status, command, 0, 0)
        if left_brackets != right_brakets:
            status.add_keep_error("Stream Command: Received inconsistent JSON structure from a peer node")
            return process_status.ERR_wrong_json_structure
    else:
        words_array = cmd_words

    if len(words_array) < 10:
        return process_status.ERR_command_struct

    data_body = params.get_value_if_available(words_array[1])
    if not data_body:
        status.add_error("Stream command: failed to identify data")
        return process_status.ERR_wrong_json_structure

    ret_val, msg_data, row_counter = utils_json.make_json_rows(status, data_body)
    if ret_val:
        if not msg_data:
            err_msg = "Failed to process JSON data: Data Body is empty"
        else:
            err_msg = "Failed to process JSON input"
        status.add_error(f"Stream command: {err_msg}")
        return process_status.ERR_wrong_json_structure

    keywords = {
        "dbms": ("str", True, False, True),
        "table": ("str", True, False, True),
        "mode": ("str", False, False, False),           # "file or streaming"
        "source": ("str", False, False, False),  # "policy id"
        "instructions": ("str", False, False, False),   # "policy id"
    }

    if words_array[2] != "where":
        return process_status.ERR_command_struct

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, words_array, 3, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return ret_val

    dbms_name = interpreter.get_one_value(conditions, "dbms")
    table_name = interpreter.get_one_value(conditions, "table")


    mode = interpreter.get_one_value_or_default(conditions, "mode", "streaming")
    source = interpreter.get_one_value_or_default(conditions, "source", "0")
    instructions = interpreter.get_one_value_or_default(conditions, "instructions", "0")

    prep_dir = params.get_value_if_available("!prep_dir")  # dir where the data will be written
    watch_dir = params.get_value_if_available("!watch_dir")  # dir where the data will be written
    err_dir = params.get_value_if_available("!err_dir")
    if not prep_dir or not err_dir or not watch_dir:
        if not prep_dir:
            missing_name = "prep_dir"
        elif not watch_dir:
            missing_name = "watch_dir"
        else:
            missing_name = "err_dir"
        status.add_error(f"Stream command: Missing {missing_name} in local dictionary")
        return process_status.Missing_configuration

    ret_val, hash_value = streaming_data.add_data(status, mode, row_counter, prep_dir, watch_dir, err_dir, dbms_name, table_name, source, instructions, "json", msg_data)

    return ret_val
# =======================================================================================================================
# Executes user's Python code
# Example: ip_port = from !selected_operator bring ip ":" port " "
# =======================================================================================================================
def _execute_from(status, io_buff_in, cmd_words, trace):
    if len(cmd_words) < 4:
        return process_status.ERR_command_struct

    offset = get_command_offset(cmd_words)

    data_key = cmd_words[offset + 1]
    if data_key[-1] == ')':
        # get entry from array
        key, index = params.get_key_and_index(data_key)
        if key == "":
            status.add_error("'from' command failed to translate json object with index using: %s" % data_key)
            return process_status.ERR_process_failure
    else:
        key = data_key
        index = -1

    bring_type, sort_fields = utils_json.get_bring_type(status, cmd_words[offset + 2])
    if bring_type == -1:
        return process_status.ERR_command_struct


    str_data = params.get_value_if_available(key)

    str_data = params.json_str_replace_key_with_value(str_data)  # replace keys with values from dictionary

    str_data = utils_data.replace_string_chars(True, str_data, {'|': '"'})


    if not str_data:
        status.add_error("Error in command 'from': variable '%s' not assigned with value" % cmd_words[offset + 1])
        return process_status.ERR_wrong_data_structure

    json_struct = utils_json.str_to_json(str_data)

    if json_struct and index != -1:
        if index < len(json_struct):
            # use one entry
            json_data = [json_struct[index]]
        else:
            status.add_error(
                "Structure with %u json instances and index #%u is outside range" % (len(json_struct), index))
            return process_status.ERR_process_failure
    else:
        json_data = json_struct

    if json_data == None:
        status.add_error("Error in command 'from': unable to retrieve JSON from the string: " + cmd_words[offset + 1])
        return process_status.ERR_wrong_data_structure

    if isinstance(json_data, dict):
        json_list = [json_data]  # need to pass a list of objects into process_bring
    else:
        json_list = json_data

    ret_val, dynamic_string = process_bring(status, json_list, cmd_words, offset + 3, bring_type)

    if not ret_val:
        if utils_json.is_bring("table", bring_type):
            # Change the JSON format to table format
            if dynamic_string:
                dynamic_string = set_json_as_table(dynamic_string, bring_type, sort_fields)

        if offset == 2:
            params.add_param(cmd_words[0], dynamic_string)
        else:
            utils_print.output(dynamic_string, True)

    return ret_val

# =======================================================================================================================
# Connect to a blockchain platform
# blockchain connect to ethereum where
'''
<blockchain connect ethereum where provider = "https://rinkeby.infura.io/v3/45e96d7ac85c4caab102b84e13e795a1" and
		contract = "0x3899bED34d9e3032fb0d544CB76bA7F752Bf5EbE" and
		private_key = "a4caa21209188ef5c3be6ee4f73c12a8c306a917c969638fb69f164b0ed95380" and 
		public_key = "0x982AF5e1589f1486b4bA17aFB6eb940aAeBBdfdB" and 
		gas_read = 3000000  and
		gas_write = 3000000>
'''
# =======================================================================================================================
def blockchain_connect(status, io_buff_in, cmd_words, trace, func_params):

    #                               Must     Add      Is
    #                               exists   Counter  Unique

    keywords = {"provider": ("str", False, False, True),
                "private_key": ("str", False, False, True),
                "public_key": ("str", False, False, True),
                "contract": ("str", False, False, True),
                "network": ("str", False, False, True),
                "gas_read" :  ("int", False, False, True),
                "gas_write": ("int", False, False, True),
                "certificate_dir": ("str", False, False, True), # For hyperledger this would be the location of the hyperledger certificate files
                "config_file": ("str", False, False, True), # For hyperledger this would be the "network.json" file with the network connection info
    }

    if cmd_words[2] != "to" or cmd_words[4] != "where":
        return [process_status.ERR_command_struct, None]

    platform_name = cmd_words[3]

    if bplatform.is_connected(platform_name):
        status.add_error("Blockchain platform '%s' is already connected" % platform_name)
        return [process_status.Process_already_running, None]

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 5, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return [ret_val, None]

    ret_val = bplatform.blockchain_connect(status, platform_name, conditions)

    return [ret_val, None]
# =======================================================================================================================
# Create a new account on the blockchain platform
# Command: blockchain create account ethereum
# =======================================================================================================================
def blockchain_create_account(status, io_buff_in, cmd_words, trace, func_params):

    platform_name = cmd_words[3]
    ret_val, ethereum_keys = bplatform.create_account(status, platform_name)

    if not ret_val and ethereum_keys:
        utils_print.output(ethereum_keys, True)

    return [ret_val, None]

# =======================================================================================================================
# Associate account info with the connection info
# Command: blockchain set account info where platform = ethereum and private_key = !private_key and public_key = !public_key
# =======================================================================================================================
def blockchain_set_account_info(status, io_buff_in, cmd_words, trace, func_params):

    keywords = {
                "platform": ("str", True, False, True),
                "private_key": ("str", False, False, True),
                "public_key": ("str", False, False, True),
                "contract": ("str", False, False, True),
                "gas_read": ("int", False, False, False),
                "gas_write": ("int", False, False, False),
                "chain_id": ("int", False, False, True),
    }

    if cmd_words[4] != "where":
        return [process_status.ERR_command_struct, None]

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 5, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return [ret_val, None]

    platform_name = interpreter.get_one_value(conditions, "platform")

    ret_val = bplatform.set_account_info(status, platform_name, conditions)

    return [ret_val, None]

# =======================================================================================================================
# Deploy a contract to the blockchain platform
# blockchain deploy contract where platform = ethereum and private key = [] and public key = [] and gas = []
# =======================================================================================================================
def blockchain_deploy(status, io_buff_in, cmd_words, trace, func_params):

    keywords = {
                "platform": ("str", True, False, True),
                "public_key": ("str", True, False, True),
    }

    offset = get_command_offset(cmd_words)

    if cmd_words[3 + offset] != "where":
        return [process_status.ERR_command_struct, None]

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4 + offset, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return [ret_val, None]


    platform_name = interpreter.get_one_value(conditions, "platform")
    public_key = interpreter.get_one_value(conditions, "public_key")

    ret_val, contract_id = bplatform.deploy_contract(status, platform_name, public_key)

    if contract_id:
        if offset:
            params.add_param(cmd_words[0], contract_id)
        else:
            utils_print.output(contract_id, True)

    return [ret_val, contract_id]

# =======================================================================================================================
# Read the blockchain file from the blockchain platform
# blockchain checkout from ethereum [file_name]
# blockchain checkout from ethereum stdout --> to screen
# =======================================================================================================================
def blockchain_checkout(status, io_buff_in, cmd_words, trace, func_params):

    '''
    Get the ledger from the blockchain platform
    '''

    data = None

    if cmd_words[2] != "from":
        ret_val = process_status.Error_command_params
    else:
        ret_val = process_status.SUCCESS
        words_count = len(cmd_words)
        to_file = True      # output to file unless filename is stdout
        platform_name = cmd_words[3]
        if words_count == 4:
            # blockchain checkout from ethereum --> write to the blockchain file
            b_file = params.get_value_if_available("!blockchain_file")
            if not b_file:
                status.add_error("Missing dictionary definition for \'blockchain_file\'")
                ret_val = process_status.No_local_blockchain_file
            else:
                file_name = utils_io.change_name_type(b_file, "new")
                set_active = True  # Replace the local blockchain file
        elif words_count == 6 and cmd_words[4] == "to":
            # blockchain checkout from ethereum to file_name
            file_name = params.get_value_if_available(cmd_words[5])
            set_active = False  # Output to file (not to the blockchain file)
            if file_name == "stdout":
                to_file = False     # retrieve to stdout
        else:
            ret_val = process_status.ERR_command_struct

        if not ret_val:
            # Pull the data from  the platform
            ret_val, data = bplatform.blockchain_checkout(status, platform_name, trace)

            if not ret_val:
                if to_file:
                    if not utils_io.write_str_to_file(status, data, file_name):
                        status.add_error("Failed to write blockchain data to file: %s" % file_name)
                        ret_val = process_status.BLOCKCHAIN_operation_failed
                    else:
                        if set_active:
                            # Copy to the local blockchain file:  .new --> .json --> .old
                            words_array = ["blockchain", "update", "file"]
                            ret_val = blockchain_update_file(status, words_array, b_file, trace)
                else:
                    # Set printable in JSON format (i.e. replace double quotes with single"
                    data_entries = data.split("\n")
                    utils_print.output("\r\n", False)
                    for entry in data_entries:
                        utils_print.output("\r\n", False)
                        utils_print.struct_print(entry, False, False)
                    utils_print.output("\r\n\n", True)

    return [ret_val, data]

# =======================================================================================================================
# Write the JSON file to the blockchain
# Example command: blockchain commit to ethereum !json_script
# =======================================================================================================================
def blockchain_commit(status, io_buff_in, cmd_words, trace, func_params):

    reply = None
    if cmd_words[2] != "to":
        ret_val = process_status.ERR_command_struct
    else:
        b_file = params.get_value_if_available("!blockchain_file")
        if b_file == "":
            status.add_error("Missing dictionary definition for \'blockchain_file\'")
            ret_val = process_status.No_local_blockchain_file
        else:
            platform_name = params.get_value_if_available(cmd_words[3])
            json_key = params.get_value_if_available(cmd_words[4])

            ret_val, json_dictionary = prep_policy(status, json_key, None, b_file, True)

            if not ret_val:
                policy_id = utils_json.get_policy_value(json_dictionary, None, "id", None)

                json_data = utils_json.to_string(json_dictionary)
                if json_data:
                    ret_val, reply = bplatform.blockchain_commit(status, platform_name, policy_id, json_data, trace)
                else:
                    ret_val = process_status.Wrong_policy_structure

    return [ret_val, reply]

# =======================================================================================================================
# Update the JSON file to the blockchain
# Example command: blockchain update to ethereum !policy_id !json_script
# =======================================================================================================================
def blockchain_update(status, io_buff_in, cmd_words, trace, func_params):

    ret_val = None
    reply = None
    if cmd_words[2] != "to":
        ret_val = process_status.ERR_command_struct
    else:
        b_file = params.get_value_if_available("!blockchain_file")
        if b_file == "":
            status.add_error("Missing dictionary definition for \'blockchain_file\'")
            ret_val = process_status.No_local_blockchain_file
        else:
            platform_name = params.get_value_if_available(cmd_words[3])
            policy_id = params.get_value_if_available(cmd_words[4])  # get the provided policy id
            json_key = params.get_value_if_available(cmd_words[5])

            # JSONify policy
            policy_json = utils_json.str_to_json(json_key)

            # check if policy is a valid JSON
            if not isinstance(policy_json, dict):
                ret_val = process_status.Invalid_policy
                status.add_error("Policy is not a valid JSON")
            if not ret_val:
                # Check if new policy contains a policy id
                key = next(iter(policy_json))
                if not policy_json[key].get("id"):
                    policy_json[key]["id"] = policy_id  # add provided policy id if not included in policy
                # Check if policy id matches
                elif policy_id != policy_json[key].get("id"):
                    ret_val = process_status.Policy_id_not_match    # return error
                    status.add_error("Specified policy id does not match")
                if not ret_val:
                    ret_val, json_dictionary = prep_policy(status, None, policy_json, b_file, True)
                    if not ret_val:
                        json_data = utils_json.to_string(json_dictionary)
                        if json_data:
                            ret_val, reply = bplatform.blockchain_update(status, platform_name, policy_id, json_data,
                                                                                trace)
                        else:
                            ret_val = process_status.Wrong_policy_structure

    return [ret_val, reply]


# =======================================================================================================================
# Prep a policy with an ID and date
# blockchain prepare policy !my_policy
# =======================================================================================================================
def blockchain_prep(status, io_buff_in, cmd_words, trace, func_params):

    key = params.get_value_if_available(cmd_words[3])
    policy_str = params.get_value_if_available(key)
    policy_str = params.json_str_replace_key_with_value(policy_str)  # replace keys with values from dictionary
    policy_str = utils_data.replace_string_chars(True, policy_str, {'|': '"'})

    policy = utils_json.str_to_json(policy_str)
    if not policy or len(policy) != 1:
        ret_val = process_status.Wrong_policy_structure
    else:
        ret_val = policies.add_json_id_date(policy)  # if there is no ID to the JSON, add ID


    if not ret_val and cmd_words[3][0] == '!':
        # Set in the variable
        new_policy = utils_json.to_string(policy)
        if new_policy:
            params.add_param(cmd_words[3][1:], new_policy)
    return [ret_val, None]
# =======================================================================================================================
# Wait for the blockchain synchronizer to bring the latest copy of the metadata
# Example: blockchain wait where policy = !operator
# Example: blockchain wait where id = [id]
# Example: blockchain wait where command = "blockchain get cluster where name = cluster_1"
# =======================================================================================================================
def blockchain_wait(status, io_buff_in, cmd_words, trace, func_params):

    policy_type = ""
    policy_id = ""
    offset = get_command_offset(cmd_words)

    if cmd_words[offset + 2] != "where" or cmd_words[offset + 4] != "=":
        ret_val = process_status.ERR_command_struct
    else:

        wait_type =  cmd_words[offset + 3]  # value is one of the following: policy / id / command
        if wait_type == "policy":
            policy_str = params.get_value_if_available(cmd_words[offset + 5])
            policy = utils_json.str_to_json(policy_str)
            if not policy:
                ret_val = process_status.Wrong_policy_structure
            else:
                policy_type = utils_json.get_policy_type(policy)
                if not policy_type:
                    status.add_error("Blockchain wait command error: Empty wait policy")
                    ret_val = process_status.Wrong_policy_structure
                else:
                    if not "id" in policy[policy_type]:
                        status.add_error("Blockchain wait command error: Missing ID in wait policy")
                        ret_val = process_status.Wrong_policy_structure
                    else:
                        policy_id = policy[policy_type]["id"]
                        cmd_get_policy = ["blockchain", "get", policy_type, "where", "id", "=", policy_id]
                        ret_val = process_status.SUCCESS
        elif wait_type == "id":
            policy_id = params.get_value_if_available(cmd_words[offset + 5])
            cmd_get_policy = ["blockchain", "get", policy_type, "where", "id", "=", policy_id]
            ret_val = process_status.SUCCESS
        elif wait_type == "command":
            cmd_get_policy, left_brackets, right_brakets = utils_data.cmd_line_to_list_with_json(status, cmd_words[offset + 5], 0, 0)  # a list with words in command line
            if not cmd_get_policy or len (cmd_get_policy) < 3 or cmd_get_policy[0] != "blockchain" or cmd_get_policy[1] != "get":
                status.add_error("Blockchain wait command error: missing 'blockchain get' command")
                ret_val = process_status.ERR_command_struct
            elif left_brackets != right_brakets:
                status.add_error("Blockchain wait command error: Missing parenthesis in 'blockchain get' command")
                ret_val = process_status.ERR_command_struct
            else:
                ret_val = process_status.SUCCESS
        else:
            ret_val = process_status.Wrong_policy_structure

        if not ret_val:
            max_ime = bsync.get_sync_time()  + 15 # wait up to 15 seconds after sync time
            wait_time = 0

            while True:
                ret_val, on_file_plicy = blockchain_get(status, cmd_get_policy, None, True)
                if not ret_val and len(on_file_plicy):
                    ret_val = process_status.SUCCESS
                    break
                else:
                    if not wait_time:
                        if bsync.is_master():
                            time.sleep(1)  # give time for the update on the master
                            _blockchain_sync(status, None, ["run", "blockchain", "sync"], 0)  # Sync the local copy
                    wait_time += 5
                    if wait_time > max_ime:
                        ret_val = process_status.Policy_not_in_local_file
                        break
                    if not utils_threads.seconds_sleep("Blockchain wait", 5):
                        ret_val = process_status.Policy_not_in_local_file   # Keyboard Interrupt
                        break       # User CTR/Break while thread on sleep

    if offset:
        if ret_val:
            reply = "false"
        else:
            reply = "true"
        params.add_param(cmd_words[0], reply)

    if trace:
        text_msg = process_status.get_status_text(ret_val)
        utils_print.output("\r\n[Blockchain Wait] [Policy Type: %s] [Policy ID: %s] [Exit Value: %s]" % (policy_type, policy_id, text_msg), True)

    return [ret_val, None]

# =======================================================================================================================
# Add Missing components to the policy
# =======================================================================================================================
def prep_policy(status: process_status, json_key, json_obj, blockchain_file:str, compare_to_file:bool):
    '''
    Giving a key to the AnyLog dictionary, the value assigned to the key is retrieved.
    If the value represents a Policy, the policy is validated and missing attributes are added:
    * Depending on the Policy type - the policy missing attributes are added.
    * For all policies - the ID and Date attribute are added (if missing from the policy)

    compare_to_file - determine if to test policy against local file. In master this process is not done because:
    a) It was done at the edge nodes
    b) With master and operator in a single node, it can reject policies at the master
    '''

    if json_obj:
        json_dictionary = json_obj
    else:
        json_data = params.get_value_if_available(json_key)
        json_data = params.json_str_replace_key_with_value(json_data)  # replace keys with values from dictionary

        json_data = utils_data.replace_string_chars(True, json_data, {'|': '"'})

        json_struct = utils_json.str_to_json(json_data)  # map string to dictionary
        if isinstance(json_struct, dict):
            json_dictionary = json_struct
        elif isinstance(json_struct, list) and len(json_struct) == 1 and isinstance(json_struct[0], dict):
            json_dictionary = json_struct[0]
        else:
            json_dictionary = None

    if json_dictionary:
        ret_val = policies.add_json_id_date(json_dictionary)  # if there is no ID to the JSON, add ID
        if not ret_val:

            if compare_to_file:
                ret_val = policies.process_new_policy(status, json_dictionary, blockchain_file)  # Add values to special type of policies
                if not ret_val:
                    if not utils_json.validate(json_dictionary):  # test dictionary can be represented as JSON
                        ret_val = process_status.ERR_wrong_json_structure
                    else:
                        ret_val = process_status.SUCCESS
    else:
        status.add_keep_error("String is not representative of JSON: " + json_data)
        ret_val = process_status.ERR_wrong_json_structure

    return [ret_val, json_dictionary]
# =======================================================================================================================
# Replace a policy on the local database
# To replace an existing policy:
# blockchain replace policy x with y
# X is the policy ID to replace and y is the new policy
# =======================================================================================================================
def blockchain_replace_policy(status, cmd_words, host, blockchain_file):

    if cmd_words[4] != "with":
        ret_val = process_status.ERR_command_struct
    else:
        source_id = params.get_value_if_available(cmd_words[3])
        new_policy = params.get_value_if_available(cmd_words[5])

        json_target = utils_json.str_to_json(new_policy)  # map string to dictionary
        if not json_target or not isinstance(json_target, dict):
            status.add_error("Wrong format for target policy")
            ret_val = process_status.Wrong_policy_structure
        else:
            ret_val = policies.add_json_id_date(json_target)  # if there is no ID to the JSON, add ID
            if not ret_val:
                ret_val = policies.process_new_policy(status, json_target, blockchain_file)  # Add values to special type of policies
                if not ret_val:
                    if not db_info.blockchain_update_entry(status, host, source_id, json_target):
                        status.add_error("Failed to update policy '%s' with new data" % source_id)
                        ret_val = process_status.Failed_UPDATE
                    else:
                        ret_val = process_status.SUCCESS

    return ret_val

# =======================================================================================================================
# Delete all the policies which were added to the blockchain from the node (identified by IP:Port
# Command: blockchain drop by host [IP]
# =======================================================================================================================
def blockchain_drop_host(status, io_buff_in, cmd_words, trace, func_params):

    ip_port = params.get_value_if_available(cmd_words[4])

    sql_delete = "delete from ledger where host = '%s'" % ip_port

    reply_val, rows = db_info.process_contained_stmt(status, "blockchain", sql_delete)

    if reply_val:
        if rows >= 1:
            ret_val = process_status.SUCCESS
        else:
            ret_val = process_status.Wrong_dbms_key_in_delete
    else:
        ret_val = process_status.BLOCKCHAIN_operation_failed

    return [ret_val, None]

# =======================================================================================================================
# Add JSON file to a log file representing the JSON Data
# =======================================================================================================================
def blockchain_add(status, cmd_words, blockchain_file):

    ret_val = process_status.SUCCESS
    offset = get_command_offset(cmd_words)

    json_data = params.get_value_if_available(cmd_words[offset + 2])

    if len(json_data) >= 9 and json_data[:9] != "{\"policy\"":
        json_data = params.json_str_replace_key_with_value(json_data)  # replace keys with values from dictionary

    json_data = utils_data.replace_string_chars(True, json_data, {'|': '"'})

    json_struct = utils_json.str_to_json(json_data)
    if isinstance(json_struct, dict):
        json_dictionary = json_struct
        counter = 1  # only one json struct to update
    elif isinstance(json_struct, list):
        counter = len(json_struct)  # multiple json structures
        json_dictionary = json_struct[0]
    else:
        process_log.add("Error", "Error in 'blockchain add': wrong data structure provided: " + json_data)
        ret_val = process_status.ERR_wrong_json_structure

    if not ret_val:

        for x in range(counter):
            if x:
                json_dictionary = json_struct[x]  # get the next element on the list

            if json_dictionary:


                ret_val = policies.add_json_id_date(json_dictionary)  # if there is no ID to the JSON, add ID
                if ret_val:
                    status.add("Wrong JSON structure in 'blockchain add' command")
                    break

                ret_val = policies.process_new_policy(status, json_dictionary, blockchain_file)   # Add values to special type of policies
                if ret_val:
                    break

                # get the ID and test if already exists
                object_id = utils_json.get_object_id(json_dictionary)
                if blockchain.validate_id(status, blockchain_file, object_id):
                    policy_type = utils_json.get_policy_type(json_dictionary)
                    if not policy_type:
                        policy_type = "Not recognized"
                    status.add_error("Failed to add policy (%s) to a local blockchain file: duplicate id: '%s'" % (policy_type, object_id))
                    ret_val = process_status.Duplicate_object_id
                    break

                if not blockchain.blockchain_write(status, blockchain_file, json_dictionary, True):
                    ret_val = process_status.ERR_process_failure
                    break
                else:
                    bplatform.count_local_txn() # A counter for the number of updates done to the local ledger file
            else:
                process_log.add("Error", "String is not  a representative of JSON: " + json_data)
                ret_val = process_status.ERR_wrong_json_structure
                break

    if offset:
        if ret_val:
            reply = ""
        else:
            reply = "true"
        params.add_param(cmd_words[0], reply)

    return ret_val

# =======================================================================================================================
# retrieve the JSON file from a local database representing the blockchain
# Example: blockchain pull to stdout
# =======================================================================================================================
def blockchain_pull(status: process_status, words_array: list, blockchain_file: str, trace: int):

    ret_val = process_status.SUCCESS
    words_cout = len(words_array)
    if words_cout == 5:
        # Output file name is provided
        output_file = params.get_value_if_available(words_array[4])
        if not output_file:
            status.add_error("The command 'blockchain pull' is using uninitialized variable: '%s'" % words_array[4])
            ret_val = process_status.Uninitialized_variable
    elif words_cout == 4:
        output_file = None
        path, name, type = utils_io.extract_path_name_type(blockchain_file)
        if not path or not name:
            status.add_error("The command 'blockchain pull' is unable to identify the blockchain file name")
            ret_val = process_status.Uninitialized_variable
    else:
        ret_val = process_status.ERR_command_struct

    if not ret_val:
        lock_key = ""
        if words_array[2] != "to":
            ret_val = process_status.ERR_command_struct
        elif words_array[3] == "sql":
            lock_key = "sql"  # A key to prevent multiple writers
            if not output_file:
                ret_val, output_file = utils_io.make_path_file_name(status, path, name, "sql")  # default name for data pulled from local dbms

        elif words_array[3] == "json":
            if not output_file:
                lock_key = "new"  # A key to prevent multiple writers
                ret_val, output_file = utils_io.make_path_file_name(status, path, name, "new")  # default name for data pulled from local dbms
            else:
                if output_file == blockchain_file:
                    # Output to the blockchain file
                    lock_key = "blockchain"
                else:
                    lock_key = "json"

        elif words_array[3] == "stdout":
            ret_val = process_status.SUCCESS
            output_file = "stdout"
        else:
            ret_val = process_status.ERR_command_struct

        if not ret_val:
            # Pull the data from the local database to blockchain.new file
            if not db_info.blockchain_select(status, words_array[3], output_file, lock_key):
                ret_val = process_status.BLOCKCHAIN_operation_failed
    return ret_val
# =======================================================================================================================
# Move current blockchain file to .old and copy a file to the blockchain.json file
# .new --> .json --> .old
# =======================================================================================================================
def blockchain_update_file(status: process_status, words_array: list, blockchain_file: str, trace: int):
    path, name, type = utils_io.extract_path_name_type(blockchain_file)

    words_count = len(words_array)
    if words_count == 4:
        # get the new blockchain file to replace the existing json file
        new_file = params.get_value_if_available(words_array[3])
        ret_val = process_status.SUCCESS
    elif words_count == 3:
        ret_val, new_file = utils_io.make_path_file_name(status, path, name,
                                                         "new")  # default name for data pulled from local dbms
    else:
        ret_val = process_status.ERR_command_struct

    if not ret_val:
        ret_val, old_file = utils_io.make_path_file_name(status, path, name,
                                                         "old")  # default name for data pulled from local dbms
        if not ret_val:
            ret_val = events.use_new_blockchain_file(status, None, old_file, blockchain_file, new_file, False, trace)

    return ret_val


# =======================================================================================================================
# Update the local blockchain dbms with the policies from the provided file - or if a file not provided - from blockchain.js file
# file --> .dbms
# =======================================================================================================================
def blockchain_update_dbms(status: process_status, words_array: list, blockchain_file: str, trace: int):
    words_count = len(words_array)
    if words_array[-2] == "ignore" and words_array[-1] == "message":
        print_message = False  # Ignore the message on the number of updates
        words_count -= 2
    else:
        print_message = True  # When command is executed, the number of updates to the dbms are calculated and printed

    if words_count == 4:
        # get the file with the policies to use
        source_file = params.get_value_if_available(words_array[3])
    elif words_count == 3:
        source_file = blockchain_file
    else:
        return process_status.ERR_command_struct

    policies = blockchain.blockchain_get_all(status, source_file)
    host = tcpip_server.get_ip()
    if not policies:
        ret_val = process_status.Empty_Local_blockchain_file
    else:
        ret_val = db_info.update_local_blockchain_dbms(status, policies, host, print_message)

    return ret_val


# -----------------------------------------------------------------------------------------------------
# Delete an existing policy from the blockchain. This process is the generic way to add policies:
# 1. Update the local copy of the ledger - with policies using the keyword "ledger" and value "local"
# 2. Update the blockchain platform with policies using the keyword "ledger" and value "global"
# Command: blockchain delete policy where id = [policy id] and blockchain = [platform]
# blockchain delete policy where id = 64283dba96a4c818074d564c6be20d5c and master = !master_node
# -----------------------------------------------------------------------------------------------------
def blockchain_delete_policy(status, io_buff_in, cmd_words, trace, func_params):
    #                               Must     Add      Is
    #                               exists   Counter  Unique
    keywords = {"id": ("str", True, False, True),
                "blockchain": ("str", False, True, False),  # Flag to update master node
                "master": ("ip.port", False, True, False),  # The ip and port of the master node
                "local": ("bool", False, True, True),  # Flag to update local file
                }
    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return [ret_val, None]
    if not counter:
        status.add_error("Missing ledger location information in 'blockchain insert' command")
        return [process_status.ERR_command_struct, None]

    policy_id = interpreter.get_one_value(conditions, "id")


    mem_view = memoryview(io_buff_in)

    master = interpreter.get_all_values(conditions, "master")
    platform = interpreter.get_all_values(conditions, "blockchain")
    is_local = interpreter.get_one_value_or_default(conditions, "local", True)


    ret_val = blockchain_delete_all(status, mem_view, policy_id, None, is_local, master, platform)

    if not ret_val:
        blockchain_load(status, ["blockchain", "get", "cluster"], False, 0)

    return [ret_val, None]

# -----------------------------------------------------------------------------------------------------
# Validate policy structure + update policy in all platforms
# -----------------------------------------------------------------------------------------------------
def blockchain_delete_all(status, mem_view, policy_id, blockchain_file, is_local, master, platform):
    '''
    status - The process status object
    mem_view - buffer for outgoing messages
    policy - the policy to update
    is_local - Is update of the local ledger
    blockchain_file - Path to the local ledger
    master - an array of IPs and Ports representing master nodes to receive the JSON policy
    platform - an array of blockchain platforms to receive the JSON policy
    '''


    if blockchain_file:
        b_file = blockchain_file
    else:
        b_file = params.get_value_if_available("!blockchain_file")

    if not blockchain.validate_id(status, b_file, policy_id):
        status.add_error("Error in 'blockchain delete': policy id: '%s' not available" % policy_id)
        ret_val = process_status.Needed_policy_not_available
    else:
        if is_local:

            if not blockchain.delete_policy(status, b_file, policy_id):
                ret_val = process_status.ERR_process_failure

        # Update the Master Node using: blockchain drop policy where id = [policy id]
        ret_val = process_status.SUCCESS

        if master and len(master):
            for ip_port in master:
                push_words = ["run", "client", None, "blockchain", "drop", "policy", "where", "id", "=", policy_id]
                push_words[2] = "(" + ip_port + ")"
                reply_val = run_client(status, mem_view, push_words, 0)   # We ignore errors as the master may be down

        # Update the blockchain platform, example: blockchain drop policy from hyperledger where id = 9e6375bcfaf6ebaa975a2aaec8414ac8

        if platform and len(platform):
            for platform_name in platform:
                commit_words = ["blockchain", "drop", "policy", "from", platform_name, "where", "id", "=", policy_id]
                reply_val, reply = blockchain_commit(status, mem_view, commit_words, 0, None) # We ignore errors as the network may be down

        # Signal the synchronizer
        if bsync.get_sync_time() == -1 or not bsync.is_running():
            status.add_error( "Failed to trigger sync process after policy delete: synchronizer not active")
            ret_val = process_status.ERR_process_failure
        else:
            process_status.set_signal("synchronizer")


    return ret_val


# -----------------------------------------------------------------------------------------------------
# Insert a new policy to the blockchain. This process is the generic way to add policies:
# 1. Update the local copy of the ledger - with policies using the keyword "ledger" and value "local"
# 2. Update the blockchain platform with policies using the keyword "ledger" and value "global"
# Command: blockchain insert where policy = [policy] and blockchain = [platform]
# Example: blockchain insert where policy = !policy and blockchain = local and master = !master_node
# -----------------------------------------------------------------------------------------------------
def blockchain_insert(status, io_buff_in, cmd_words, trace, func_params):
    #                               Must     Add      Is
    #                               exists   Counter  Unique
    keywords = {"policy": ("str", True, False, True),
                "blockchain": ("str", False, True, False),      # Flag to update master node
                "local": ("bool", False, True, True),           # Flag to update local file
                "master": ("ip.port", False, True, False),      # The ip and port of the master node
                }
    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return [ret_val, None]
    if not counter:
        status.add_error("Missing ledger location information in 'blockchain insert' command - specify insert dest (local/master/blockchain)")
        return [process_status.ERR_command_struct, None]

    json_data = interpreter.get_one_value(conditions, "policy")

    json_policy = params.json_str_replace_key_with_value(json_data)  # replace keys with values from dictionary

    json_policy = utils_data.replace_string_chars(True, json_policy, {'|': '"'})

    policy = utils_json.str_to_json(json_policy)

    if not isinstance(policy, dict):
        status.add_error("Error in 'blockchain insert': provided policy is not a json object: " + json_data)
        return [process_status.ERR_command_struct, None]

    mem_view = memoryview(io_buff_in)
    is_local = interpreter.get_one_value_or_default(conditions, "local", True)
    master = interpreter.get_all_values(conditions, "master")
    platform = interpreter.get_all_values(conditions, "blockchain")

    ret_val = blockchain_insert_all(status, mem_view, policy, is_local, None, master, platform, True)

    if not ret_val:
        blockchain_load(status, ["blockchain", "get", "cluster"], False, 0)

    return [ret_val, None]


# -----------------------------------------------------------------------------------------------------
# Validate policy structure + update policy in all platforms
# -----------------------------------------------------------------------------------------------------
def blockchain_insert_all(status, mem_view, policy, is_local, blockchain_file, master, platform, return_err):
    '''
    status - The process status object
    mem_view - buffer for outgoing messages
    policy - the policy to update
    is_local - Is update of the local ledger
    blockchain_file - Path to the local ledger
    master - an array of IPs and Ports representing master nodes to receive the JSON policy
    platform - an array of blockchain platforms to receive the JSON policy
    return_err - if called from the CLI - an error message is returned
    '''

    policy_type = utils_json.get_policy_type(policy)
    if not policy_type:
        status.add_error("Policy in 'blockchain insert' command has no type")
        ret_val = process_status.Wrong_policy_structure
    else:

        # Add ID and date
        ret_val = policies.add_json_id_date(policy)  # if there is no ID to the JSON, add ID
        if ret_val:
            status.add_error("Error in 'blockchain insert': wrong policy '%s' structure " % policy_type)
            ret_val = process_status.ERR_command_struct
        else:
            if blockchain_file:
                b_file = blockchain_file
            else:
                b_file = params.get_value_if_available("!blockchain_file")
            if not b_file:
                status.add_keep_error("Missing dictionary definition for \'blockchain_file\'")
                ret_val = process_status.No_local_blockchain_file
            else:
                # Test that policy has all components and complete missing values
                ret_val = policies.process_new_policy(status, policy, b_file)  # Add values to special type of policies
                if not ret_val:
                    # Update the local Ledger
                    if is_local:
                        policy[policy_type]["ledger"] = "local"         # Flag - Updating the local ledger
                        # get the ID and test if already exists
                        policy_id = utils_json.get_object_id(policy)
                        if blockchain.validate_id(status, b_file, policy_id):
                            status.add_error("Error in 'blockchain insert': duplicate id: '%s' for policy type: '%s'" % (policy_id, policy_type))
                            ret_val = process_status.Duplicate_object_id
                        elif not blockchain.blockchain_write(status, b_file, policy, True):
                            ret_val = process_status.ERR_process_failure
                        else:
                            bplatform.count_local_txn() # A counter for the number of updates done to the local ledger file
                else:
                    # Print the error in the policy
                    err_msg = process_status.get_status_text(ret_val)
                    utils_print.output_box("New Policy '%s' Structure Error: %s" % (policy_type, err_msg))
                    utils_print.struct_print(policy, True, True)

            if not ret_val:     # If local  ledger is updates - the update must be completed

                policy[policy_type]["ledger"] = "global"  # Flag - Updating the global ledger
                policy_str = utils_json.to_string(policy)

                # Update the Master Node

                if master and len(master):
                    for ip_port in master:
                        push_words = ["run", "client", None, "blockchain", "push", policy_str]
                        push_words[2] = "(" + ip_port + ")"
                        reply_val = run_client(status, mem_view, push_words, 0)   # We ignore errors as the master may be down
                        if reply_val and return_err:
                            # if called from the CLI - an error message is returned
                            ret_val = reply_val

                # Update the blockchain platform

                if platform and len(platform):
                    for platform_name in platform:
                        if is_local:
                            # Skipping policy check to avoid duplicate keys in the check of the policies in the local file (the policy was just updated on the local file)
                            reply_val, reply = bplatform.blockchain_commit(status, platform_name, policy_id, policy_str, 0)  # We ignore errors as the network may be down
                        else:
                            commit_words = ["blockchain", "commit", "to", platform_name, policy_str]
                            reply_val, reply = blockchain_commit(status, mem_view, commit_words, 0, None) # We ignore errors as the network may be down
                        if reply_val and return_err:
                            # if called from the CLI - an error message is returned
                            ret_val = reply_val

    if policy_type == "operator" or policy_type == "cluster" or policy_type == "table":
        bsync.blockchain_stat.set_force_load()  # Force a reload of the metadata
        blockchain_load(status, ["blockchain", "get", "cluster"], False, 0)       # Do not wait for process sync or update from blockchain

    if _blockchain_methods["insert"]["trace"]:
        if not policy_type:
            policy_type = "Wrong policy type"
        err_text = process_status.get_status_text(ret_val)
        utils_print.output("\r\n[Command: blockchain insert] [Policy Type: %s] [Result: %s]" % (policy_type, err_text), False)
        utils_print.struct_print(policy, True, True)


    return ret_val

# -----------------------------------------------------------------------------------------------------
# Update the blockchain platform with a new policy
# Platform options
# - "local" - local file
# - "Master" - Master Node
# - "Blockchain"
# Process waits for blockchain sync by a wait for the update on the local file
# -----------------------------------------------------------------------------------------------------
def update_blockchain(status, platform, mem_view, policy, ip_port, blockchain_file, source, process, trace_level ):

    ret_val = policies.add_json_id_date(policy)  # if there is no ID to the JSON, add ID
    if not ret_val:

        policy_str = utils_json.to_string(policy)

        if trace_level > 1:
            utils_print.output("\r\n[%s] [%s] [%s]" % (source, process, policy_str), True)

        if platform == "master":
            push_words = ["run", "client", None, "blockchain", "push", policy_str]
            push_words[2] = "(" + ip_port + ")"
            ret_val = run_client(status, mem_view, push_words, 0)
            if not ret_val:
                time.sleep(1)           # give time for the update on the master
                _blockchain_sync(status, None, ["run", "blockchain", "sync"], trace_level)    # Sync the local copy
        elif platform == "blockchain":
            commit_words = ["blockchain", "commit", "to", "ethereum", policy_str]
            ret_val, reply = blockchain_commit(status, mem_view, commit_words, 0, None)
        elif platform == "local":
            ret_val = blockchain_add(status, ["blockchain", "add", policy_str], blockchain_file)
        else:
            status.add_error("Platform %s is not recognized" % platform)
            ret_val = process_status.ERR_process_failure

    if ret_val:
        utils_print.output_box("Failed to update the global metadata using platform: %s" % platform)
    else:
        cmd_words = ["blockchain", "wait", "where", "policy", "=", policy_str]
        ret_val, ret_val2 = blockchain_wait(status, None, cmd_words, trace_level, None)

        if ret_val:
            utils_print.output_box("Blockchain update is not represented in local file (update source: '%s', process: '%s')\nUsing command: %s" % (source, process, ' '.join(cmd_words)))

    return ret_val

# =======================================================================================================================
# Retrieve the policy and if available -
# send a message to the master node / or blockchain to delete the policy
# =======================================================================================================================
def drop_policy_from_metadata(status, io_buff_in, master_node, get_command):
    '''
    get_command - the command to retrieve the policy from the local blockchain
    '''

    ret_val, policy = blockchain_get(status, get_command.split(), None, True)

    if not ret_val:
        if len(policy) == 1:
            # Drop the operator policies
            if not master_node:
                cmd_get_master = f"blockchain get (master) bring.ip_port"
                ret_val, master_node = blockchain_get(status, cmd_get_master.split(), None, True)
                if ret_val or not master_node or not isinstance(master_node, str):
                    status.add_error("Failed to retrieve Master Node IP and port from the Ledger")
                    return process_status.ERR_process_failure

            policy_str = utils_json.to_string(policy[0])
            command_str = "run client %s blockchain drop policy %s" % (master_node, policy_str)
            ret_val = process_cmd(status, command_str, False, None, None, io_buff_in)
        else:
            status.add_error("Nedded policy retrieved by '%s' is not available" % get_command)
            ret_val = process_status.Policy_not_in_local_file

    return ret_val


# =======================================================================================================================
# reset statistics on one or more services
# Example: reset stats where service = operator and topic = summary
# =======================================================================================================================
def reset_statistics(status, io_buff_in, cmd_words, trace):

    #                                       Must    Add     Is
    #                                       exists  Counter Unique
    keywords = {"service":      ("str",     True,   False,  True),
                "topic":        ("str",     True,  False,   True)
                }

    offset = get_command_offset(cmd_words)
    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3 + offset, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return ret_val

    service = conditions["service"][0]
    topic = conditions["topic"][0]

    if service == "operator" and topic == "summary":
        # Rest summary values
        stats.reset_operator_summary()

    else:
        status.add_error(f"Wrong service and topic names: '{service}' and '{topic}'")
        ret_val = process_status.ERR_process_failure

    return ret_val

# =======================================================================================================================
# Get statistics on one or more services
# Example: get stats where service = operator
# Example: get stats where service = operator and topic = table
# =======================================================================================================================
def get_statistics(status, io_buff_in, cmd_words, trace):


    #                                       Must    Add     Is
    #                                       exists  Counter Unique
    keywords = {"service":      ("str",     True,   False,  False),
                "topic":        ("str",     False,  False,  False),
                "dbms":         ("str",     False,  False,  False),
                "table":        ("str",     False,  False,  False),
                "format":       ("str",     False,  False,  True),

                }

    offset = get_command_offset(cmd_words)
    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3 + offset, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return [ret_val, None]

    '''
    service_list = conditions["service"]
    for service in service_list:
        if not "topic" in conditions or "summary" in  conditions["topic"]:
            # "summary" is the default output
            if service == "operator":
                aloperator.prep_summary(status)     # Update the summary info

    '''

    reply_format = interpreter.get_one_value_or_default(conditions, "format", "table")
    ret_val, reply = stats.get_info(status, reply_format, conditions)

    return [ret_val, reply, reply_format]

# =======================================================================================================================
# Transforms a function to a date-time string
#'examples':
#   get datetime utc '2021-03-10 22:10:01.0' + 5 minutes
#   date_str = get datetime utc 'now() + 3 days\n'
#   get datetime local date(\'now\',\'start of month\',\'+1 month\',\'-1 day\', \'-2 hours\', \'+2 minuts\')',
#   get datetime local date('now','start of month','+1 month','-1 day', '-2 hours', '+2 minutes')
# =======================================================================================================================
def _to_datetime(status, io_buff_in, cmd_words, trace):

    ret_val = process_status.ERR_command_struct
    reply = None
    offset = get_command_offset(cmd_words)
    timezone = cmd_words[offset + 2]

    time_words, time_string = utils_sql.process_date_time(cmd_words, offset + 3, True)
    if time_words and  time_string:
        ret_val = process_status.SUCCESS
        if timezone != "utc":
            if utils_columns.is_valid_timezone(timezone):
                reply = utils_columns.utc_to_timezone(time_string, utils_columns.UTC_TIME_FORMAT_STR, timezone)
            else:
                ret_val = process_status.ERR_timezone
        else:
            reply = time_string

    return [ret_val, reply]

# =======================================================================================================================
# Transforms a string to JSON by replacing the keys with the values from the dictionary
# json !policy  - returns the policy
# json !policy test - tests the structure and returns TRUE or FALSE
# json !policy [key] - returns the value assigned to the key
# =======================================================================================================================
def _to_json(status, io_buff_in, cmd_words, trace):

    offset = get_command_offset(cmd_words)
    words_count = len(cmd_words)
    if words_count >= (offset + 2) and words_count <= (offset + 3):
        ret_val = process_status.SUCCESS

        json_data = params.get_value_if_available(cmd_words[offset + 1])

        json_data = params.json_str_replace_key_with_value(json_data)  # replace keys with values from dictionary

        json_data = utils_data.replace_string_chars(True, json_data, {'|': '"'})

        json_struct = utils_json.str_to_json(json_data)

        if (offset + 2) == words_count:
            if json_struct and isinstance(json_struct, dict):
                # Return the JSON data string
                ret_data = json_data
            else:
                ret_data = ""
        if (offset + 3) == words_count:
            if not json_struct or not isinstance(json_struct, dict):
                if cmd_words[offset + 2] == "test":
                    ret_data = "false"
                else:
                    ret_data = ""
            else:
                if cmd_words[offset + 2] == "test":
                    ret_data = "true"
                else:
                    # Pull the value from the message

                    ret_val, ret_data = utils_json.pull_info(status, [json_struct], [cmd_words[offset + 2]], None, 0)
                    if ret_val:
                        ret_data = ""

        if offset:
            # Assign value
            params.add_param(cmd_words[0], ret_data)
        else:
            utils_print.struct_print(ret_data, True, True)
    else:
        ret_val = process_status.ERR_command_struct


    return ret_val
# =======================================================================================================================
# Suggest a SQL create statement based on a JSON file
# Example: suggest create !prep_dir/london.readings.json
# suggest create !prep_dir/london.readings.json where policy = mapping_policy
# =======================================================================================================================
def _suggest_create(status, io_buff_in, cmd_words, trace):
    """
   use the JSON file to suggest a create statement
   :param cmd_words:
      file_name:str - JSON file with data to convert into CREATE
   :command:
      suggest create [file_name]
   :return: creat statement
   """
    words_count = len(cmd_words)
    if cmd_words[1] == '=':
        index = 2  # assignment
    else:
        index = 0

    if words_count != (3 + index) and words_count != (7 + index):
        return process_status.ERR_command_struct

    if words_count == 7 + index and utils_data.test_words(cmd_words, 3 + index ,  ["where", "policy", "="]):
        # suggest create !prep_dir/london.readings.json where policy = mapping_policy
        policy_id = cmd_words[6 + index]
        policy = get_instructions(status, policy_id)
        if not policy:
            return process_status.Needed_policy_not_available
    else:
        policy = None

    file_name = params.get_value_if_available(cmd_words[2 + index])

    create_table_stmt = suggest_create_table(status, file_name, "", False, policy, None)
    if create_table_stmt == "":
        ret_val = process_status.ERR_process_failure  # failed to suggest a create statement
    else:
        if index == 2:
            params.add_param(cmd_words[0], create_table_stmt)
        else:
            utils_print.output(create_table_stmt, True)

        ret_val = process_status.SUCCESS

    return ret_val
# =======================================================================================================================
# Get configuration params
# get status, get connections,
# =======================================================================================================================
def _process_file(status, io_buff_in, cmd_words, trace):

    ret_val = process_status.SUCCESS

    if (len(cmd_words) == 2 and cmd_words[1] == "message"):

        # command from a different server
        is_message = True
        mem_view = memoryview(io_buff_in)
        offset = 5  # len("file") + 1
        operation = message_header.get_word_from_array(mem_view, offset)
        text_opr = "MSG: File " + operation
        if not operation:
            ret_val = process_status.ERR_command_struct
        else:
            command = message_header.get_command(mem_view)
            words_array = command.split(' ')
            file_flag = message_header.get_generic_flag(mem_view)  # flag determined by the sender

            if operation == "deliver":
                file_name = None
                files_ids = message_header.get_data_decoded(mem_view)
            else:
                files_ids = None
                utils_data.replace_chars_in_list(words_array, 2, '\t', ' ')
                offset += (1 + len(operation))
                file_name = message_header.get_string_from_array(mem_view, offset)
                if not file_name:
                    ret_val = process_status.Missing_file_name
                else:
                    file_name = file_name.replace('\t', ' ')  # When a message is send, space is replaced by '\t'
                    offset = 0  # no assignment with message


    elif len(cmd_words) >= 3:  # Not a message - commands from this server
        files_ids = None
        file_flag = 0
        is_message = False
        if cmd_words[1] == "=":
            offset = 2
        else:
            offset = 0

        if len(cmd_words) <= (offset + 1):
            ret_val = process_status.ERR_command_struct
        else:
            operation = cmd_words[offset + 1]
            text_opr = "CMD: File " + operation
            file_name = params.get_value_if_available(cmd_words[offset + 2])
            words_array = cmd_words

    else:
        is_message = False
        ret_val = process_status.ERR_command_struct

    if not ret_val:
        command_info = (is_message, offset, file_flag, files_ids, text_opr,file_name)
        ret_val = _exec_child_dict(status, commands["file"]["methods"], commands["file"]["max_words"], io_buff_in, words_array, 1, trace, command_info)[0]


    if ret_val and is_message:
        # send an error message
        err_cmd = "echo 'File %s' operation failed" % operation
        err_details = status.get_saved_error()
        if err_details:
            err_cmd += (": <%s>" % err_details)
        elif file_name:
            # With no details - add the file name (if available)
            err_cmd += ": <%s>" % file_name

        error_message(status, io_buff_in, ret_val, message_header.BLOCK_INFO_COMMAND, err_cmd, "")

    return ret_val

# =======================================================================================================================
# Write File - write the message data to a file
# =======================================================================================================================
def file_write(status, io_buff_in, cmd_words, trace, func_params):

    is_message, offset, file_flag, files_ids, text_opr, file_name = func_params

    mem_view = memoryview(io_buff_in)
    ret_val = write_data_block(status, io_buff_in, mem_view, file_flag, file_name, trace)

    if trace:
        if is_message:
            source_node = message_header.get_source_ip_port_string(mem_view)
        else:
            source_node = "local"
        ret_txt = process_status.get_status_text(ret_val)
        utils_print.output("[File Write] [Source: %s] [Result: %s] [%s]" % (source_node, ret_txt, file_name), True)

    return ret_val

# =======================================================================================================================
# File Get - Copy a file from a remote machine
# Example: run client 10.0.0.78:2048 file get !!blockchain_file !blockchain_dir\other_node.json
# =======================================================================================================================
def file_get(status, io_buff_in, cmd_words, trace, func_params):

    is_message, offset, file_flag, files_ids, text_opr, file_name = func_params

    command_length = len(cmd_words)

    if is_message and command_length <= 4:

        mem_view = memoryview(io_buff_in)

        ip, port = message_header.get_source_ip_port(mem_view)

        ret_val, file_source, file_dest = get_file_names(cmd_words, command_length - 2, 2, True)    # Get the source and target file names

        if len(file_source) > 10 and file_source[0] == '(' and file_source[-1] == ')':
            # file source is a command to pull from storage: ie.: bms = blobs_edgex and id = sample-5s.mp
            thread_name = utils_threads.get_thread_name()   # Make unique file name per each thread
            retrieve_cmd = f"file retrieve where {file_source[1:-1]} and dest = !tmp_dir/blob_{thread_name}.tmp and dest_key = retreived_{thread_name}"
            ret_val = process_cmd(status, retrieve_cmd, False, None, None, io_buff_in)
            if not ret_val:
                file_source = params.get_value_if_available(f"!retreived_{thread_name}")

        if not ret_val:
            if command_length == 3:
                file_dest = file_source

            if file_source == "" or file_dest == "":
                if file_source == "":
                    status.add_keep_error("Source file not provided")
                else:
                    status.add_keep_error("Destination file not provided")
                ret_val = process_status.ERR_command_struct  # return "Command error - usage: ..."
            else:
                copy_multiple = False  # Copy a single file
                dest_path, dest_name, dest_type = utils_io.extract_path_name_type(file_dest)
                if not dest_name:
                    # use the source name for the destination
                    src_dir, src_name, src_type = utils_io.extract_path_name_type(file_source)

                    if (src_name and src_name[-1] == '*') or (src_type and src_type[-1] == '*'):
                        copy_multiple = True
                        name_prefix, type_prefix = utils_io.get_name_type_prefix(src_name, src_type)
                    else:
                        if src_name:
                            file_dest = dest_path + src_name
                            if src_type:
                                file_dest += ".%s" % src_type

                if copy_multiple:
                    # copy the directory files
                    if not params.is_directory(dest_path):
                        status.add_error("Destination directory is missing a separator (%s) at the end of the directory name" % params.get_path_separator())
                        ret_val = process_status.Err_dir_name
                    else:
                        ret_val = copy_multiple_files(status, [(ip, str(port))], src_dir, name_prefix, type_prefix, dest_path, trace, "Get files %s.%s" % (name_prefix, type_prefix))
                else:
                    # Copy 1 file
                    ret_val = transfer_file(status, [(ip, str(port))], file_source, file_dest, 0, trace, text_opr + "Get File", True)

    else:
        if not is_message:
            status.add_error("Missing 'run client' prefix as 'file get' copies the file to the node that issued the command")
        ret_val = process_status.ERR_command_struct

    return ret_val

# -------------------------------------------------------------------------------
# Copy multiple files from one machine to another
# -------------------------------------------------------------------------------
def copy_multiple_files(status, ip_port_list, source_dir, name_prefix, type_prefix, dest_dir, trace, trace_msg):

    files_list = []
    ret_val = utils_io.search_files_in_dir(status, files_list, source_dir, name_prefix, type_prefix, False, False)
    if not ret_val:
        if not len(files_list):
            ret_val = process_status.No_files_to_copy
        else:
            for file_name in files_list:
                file_source = source_dir + file_name
                file_dest = dest_dir + file_name

                ret_val = transfer_file(status, ip_port_list, file_source, file_dest, 0, trace, trace_msg, True)
                if ret_val:
                    break

    return ret_val

# =======================================================================================================================
# File Delete
# =======================================================================================================================
def file_delete(status, io_buff_in, cmd_words, trace, func_params):

    is_message, offset, file_flag, files_ids, text_opr, file_name = func_params

    command_length = len(cmd_words)

    if cmd_words[command_length - 2] == "ignore" and cmd_words[command_length - 1] == "error":
        ignore_error = True
    else:
        ignore_error = False

    if not file_name:
        ret_val = process_status.Missing_file_name
    elif (command_length == 3 and not ignore_error) or (command_length == 5 and ignore_error):
        ret_val = process_status.SUCCESS
        if not utils_io.delete_file(file_name):
            if not ignore_error:  # it prevents a delete (of a file that does not exists) in a script to stop the script process
                ret_val = process_status.ERR_process_failure
    else:
        ret_val = process_status.ERR_command_struct

    return ret_val

# =======================================================================================================================
# File Deliver
# Copy the listed files to the node sending the request.
# Files are identified using the TSD table
# Format is: file deliver ip, port tsd_x id1,id2,id3....
# =======================================================================================================================
def file_deliver(status, io_buff_in, cmd_words, trace, func_params):

    ret_val = process_status.SUCCESS
    is_message, offset, file_flag, files_ids, text_opr, file_name = func_params
    command_length = len(cmd_words)

    if is_message:
        if command_length == 3:
            mem_view = memoryview(io_buff_in)
            ip, port = message_header.get_source_ip_port(mem_view)
            tsd_table = cmd_words[2]  # the member id in the cluster that owns the requested files
        else:
            ret_val = process_status.ERR_command_struct

    elif command_length == 6:
        # example:  file deliver 10.0.0.78, 2048 tsd_41 1,2
        ip = cmd_words[2]
        port = cmd_words[3]
        tsd_table = cmd_words[4]
        files_ids = cmd_words[5]

    if not ret_val:
        ret_val = deliver_files(status, io_buff_in, ip, port, tsd_table, files_ids, trace)

    return ret_val

# ----------------------------------------------------------------------------
# Compression and decompression
# ----------------------------------------------------------------------------
def manipulate_file(status, io_buff_in, cmd_words, trace, func_params):

    is_message, offset, file_flag, files_ids, text_opr, file_name = func_params
    command_length = len(cmd_words)
    ret_val = process_status.SUCCESS

    if  command_length == (3 + offset) or (command_length == 4 + offset):
        if command_length == 4:
            out_file = params.get_value_if_available(cmd_words[offset + 3])
            if not out_file:
                error_msg = "'File %s' failed: Destination file is not recognized: %s" % (cmd_words[1 + offset], cmd_words[3 + offset])
                status.add_keep_error(error_msg)
                ret_val = process_status.Error_file_name_struct
        else:
            out_file = None  # outfile will use extension to file_name

        if not ret_val and not file_name:
            error_msg = "'File %s' failed: Source file is not recognized: %s" % (cmd_words[1 + offset], cmd_words[2 + offset])
            status.add_keep_error(error_msg)
            ret_val = process_status.Error_file_name_struct

        if not ret_val:

            file_name = utils_io.set_path_separator(file_name)  # Make file name consistent with the OS

            # test if the source file exists
            if not utils_io.is_path_exists(file_name):

                file_path, current_name, file_type = utils_io.extract_path_name_type(file_name)
                if current_name[-1] == '*' or (file_type and file_type[-1] == '*'):
                    if cmd_words[1 + offset] == "decompress":
                        # Manage:  file decompress !src_dir\*.gz
                        ret_val = utils_io.operate_multiple_files(status, "decompress", file_name, out_file)
                    elif cmd_words[1 + offset] == "compress":
                        # Manage:  file compress !src_dir\*.json
                        ret_val = utils_io.operate_multiple_files(status, "compress", file_name, out_file)
                    elif cmd_words[1 + offset] == "decode":
                        # Manage:  file compress !src_dir\*.json
                        ret_val = utils_io.operate_multiple_files(status, "decode", file_name, out_file)
                    elif cmd_words[1 + offset] == "encode":
                        # Manage:  file compress !src_dir\*.json
                        ret_val = utils_io.operate_multiple_files(status, "encode", file_name, out_file)
                    else:
                        ret_val = process_status.ERR_command_struct

                else:
                    error_msg = "'File %s' failed: File does not exists: " % cmd_words[1 + offset]
                    if file_name:
                        error_msg += file_name
                    else:
                        error_msg += cmd_words[2 + offset]

                    status.add_keep_error(error_msg)
                    ret_val = process_status.ERR_process_failure

            else:

                if cmd_words[offset + 1] == "compress":
                    ret_val = utils_io.compress(status, file_name, out_file)
                elif cmd_words[offset + 1] == "decompress":
                    ret_val = utils_io.decompress(status, file_name, out_file)
                elif cmd_words[offset + 1] == "encode":
                    ret_val = utils_io.encode(status, file_name, out_file)
                elif cmd_words[offset + 1] == "decode":
                    ret_val = utils_io.decode(status, file_name, out_file)

                if offset:
                    if ret_val:
                        result = "false"
                    else:
                        result = "true"
                    params.add_param(cmd_words[0], result)
    else:
        ret_val = process_status.ERR_command_struct

    return ret_val

# ----------------------------------------------------------------------------
# copy / move file
# run client 10.0.0.78:2048 file copy !prep_dir/machine_data.txt !!prep_dir/json_data.txt
# ----------------------------------------------------------------------------
def file_copy_move(status, io_buff_in, cmd_words, trace, func_params):

    is_message, offset, file_flag, files_ids, text_opr, file_name = func_params

    operation = cmd_words[offset + 1]

    ret_val, source, dest = get_file_names(cmd_words, 2, offset + 2, True)

    if source == "" or dest == "":
        if operation == "copy":
            ret_val = process_status.File_copy_failed
        else:
            ret_val = process_status.File_move_failed
    else:
        # process copy or move on this machine:
        file_path, current_name, file_type = utils_io.extract_path_name_type(file_name)

        if operation == "copy":
            if current_name[-1] == '*' or (file_type and file_type[-1] == '*'):
                # copy multiple files
                if utils_io.copy_multiple_files(status, source, dest):
                    ret_stat = False
                else:
                    ret_stat = True
            else:
                # copy a single file
                ret_stat = utils_io.copy_file(source, dest)
        else:
            if current_name[-1] == '*' or (file_type and file_type[-1] == '*'):
                # move multiple files
                if utils_io.move_multiple_files(status, source, dest):
                    ret_stat = False
                else:
                    ret_stat = True
            else:
                # move a single file
                ret_stat = utils_io.move_file(source, dest)
        if not ret_stat:
            result = "false"
            if operation == "copy":
                ret_val = process_status.File_copy_failed
            else:
                ret_val = process_status.File_move_failed
        else:
            result = "true"
            ret_val = process_status.SUCCESS
        if offset:
            params.add_param(cmd_words[0], result)

    return ret_val

# =======================================================================================================================
# Test that a file exists
# =======================================================================================================================
def file_test_exists(status, io_buff_in, cmd_words, trace, func_params):

    ret_val = process_status.SUCCESS
    is_message, offset, file_flag, files_ids, text_opr, file_name = func_params

     # Validate if a file exists on the OS
    test_result = utils_io.is_path_exists(file_name)

    if offset:
        params.add_param(cmd_words[0], str(test_result))
    else:
        if is_message:
            if test_result:
                ret_val = process_status.FILE_EXISTS
            else:
                ret_val = process_status.FILE_DOES_NOT_EXISTS
        else:
            utils_print.output(str(test_result), True)


    return ret_val

# ----------------------------------------------------------------------------
# Get a hash value representing the content of a file
# ----------------------------------------------------------------------------
def file_hash(status, io_buff_in, cmd_words, trace, func_params):

    ret_val = process_status.SUCCESS
    is_message, offset, file_flag, files_ids, text_opr, file_name = func_params

    got_hash, hash_value = utils_io.get_hash_value(status, file_name, "", None)

    if got_hash:
        if offset:
            params.add_param(cmd_words[0], str(hash_value))
        else:
            utils_print.output(str(hash_value), True)
    else:
        ret_val = process_status.ERR_process_failure

    return ret_val

# ----------------------------------------------------------------------------
# Get the list of files from the storage dbms
# get files where dbms = blobs_edgex and table = image
# ----------------------------------------------------------------------------
def get_list_from_storage(status, io_buff_in, cmd_words, trace):

    reply = None
    keywords = {"dbms": ("str", True, False, True),
                "table": ("str", False, True, True),
                "id": ("str", False, False, True),
                "hash": ("str", False, False, True),
                "date": ("str", False, False, True),    # Representing Archive Date YYMMDD
                "limit": ("int", False, False, True),
               }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3, 0, keywords, False)
    if not ret_val:

        dbms_name, table_name, id_str, hash_val, archive_date, limit = interpreter.get_multiple_values(conditions,
                                                                    ["dbms",    "table", "id",    "hash", "date", "limit"],
                                                                    [None,      None,    None,    None,    None,    0  ])

        if archive_date:
            ret_code = utils_columns.validate_date_string(archive_date, "%y%m%d")
            if not ret_code:
                status.add_error(f"The date key '{archive_date}' is not in YYMMDD format")
                ret_val = process_status.ERR_command_struct

        if hash_val or id_str:
            if not table_name:
                status.add_error("Missing 'table' name in 'get files command'")
                ret_val = process_status.ERR_command_struct

        if not ret_val:
            ret_val, files_list = db_info.get_files_list(status, dbms_name, table_name, id_str, hash_val, archive_date, limit)

            reply = ""
            if not ret_val:

                for entry in files_list:
                    reply += entry + "\r\n"

    return [ret_val, reply]


# ----------------------------------------------------------------------------
# Writes a user file in a destination folder

# file to !file_dest where source = !my_video
# file to !prep_dir/testdata2.txt where source = !prep_dir/testdata.txt
# Using REST:
# curl -X POST -H "command: file to !my_dest" -F "file=@testdata.txt" http://10.0.0.78:7849
# curl -X POST -H "command: file to !prep_dir/testdata2.txt where source = !prep_dir/testdata.txt"  http://10.0.0.78:7849
# ----------------------------------------------------------------------------
def file_to(status, io_buff_in, cmd_words, trace, func_params):
    keywords = {
    #                          Must     Add      Is
    #                          exists   Counter  Unique
                "source": ("str", True, False, True),     # Dest file name on disk
                }

    words_count = len(cmd_words)
    if words_count > 3:
        if cmd_words[3] != "where" or words_count < 7:
            return process_status.ERR_command_struct


        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
        if ret_val:
            # conditions not satisfied by keywords or command structure
            return ret_val

        source_file = interpreter.get_one_value_or_default(conditions, "source", None)
    else:
        source_file = None

    dest_file = params.get_value_if_available(cmd_words[2])

    ret_val = process_status.SUCCESS

    # Write the file in a destination directory
    if source_file:
        # Source and dest are files on this node
        # Copy source file to destination
        if not utils_io.copy_file(source_file, dest_file):
            ret_val = process_status.File_copy_failed

    elif status.get_active_job_handle().is_rest_caller():
        # Source was provides in a rest call:  curl -X POST -H "command: file store where dest = !prep_dir/file2.txt" -F "file=@testdata.txt" http://10.0.0.78:7849
        file_data = status.get_file_data()
        if file_data:
            if not utils_io.write_str_to_file(status, file_data, dest_file):
                ret_val = process_status.File_write_failed
        else:
            status.add_error("Missing source file in 'file to' Command")
            ret_val = process_status.Missing_source_file

    return ret_val

# ----------------------------------------------------------------------------
# Store a file in a local storage dbms

# 1. The database needs to be open:
#               connect dbms admin where type = mongo and ip = 127.0.0.1 and port= 27017
# 2. database and table needs to be included in the file name:
#               test.syslog.0.268367b541e31976bf6a18c6cefbc87c.0.json
#    or specified in the command line

# Examples:
#   DBMS and Table in the file name:
#               file store where source = !prep_dir/admin.files.test.txt
#   DBMS and table by the user
#               file store where dbms = admin and table = test and source = !prep_dir/testdata.txt


# Using REST:

# curl -X POST -H "command: file store where dbms = admin and table = files and dest = file_rest " -F "file=@testdata.txt" http://10.0.0.78:7849
# ----------------------------------------------------------------------------
def file_store(status, io_buff_in, cmd_words, trace, func_params):


    keywords = {
    #                          Must     Add      Is
    #                          exists   Counter  Unique
                "dbms": ("str", False, False, True),
                "table": ("str", False, False, True),
                "hash": ("str", False, False, True),
                "source": ("str", False, True, True),        # Source file name
                "dest": ("str", False, True, True),        # destination file name
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return ret_val

    dbms_name, table_name, hash_value, source_file, dest_file = interpreter.get_multiple_values(conditions,
                                                                                  ["dbms", "table", "hash", "source", "dest"],
                                                                                  [None, None, None, None, None])


    if counter != 1:
        # Either source file is provided - or with REST, dest name is provided
        status.add_error("Command failed: 'file store' command requires source file name, or, with HTTP call - a dest file name")
        return process_status.ERR_command_struct

    if source_file:
        file_path, s_file_name, file_type = utils_io.extract_path_name_type(source_file)
        file_name = f"{s_file_name}.{file_type}" if file_type else s_file_name
    else:
        s_file_name = None
        file_path = None
        file_name = dest_file


    if not dbms_name and not table_name:
        # get from the file name
        if not s_file_name:
            status.add_error("Missing database name or table name info in 'file store' command")
            return process_status.ERR_command_struct
        file_entries = s_file_name.split('.')
        if len(file_entries) < 3:
            message = f"Blobs file name in the wrong format, needs to start with [dbms name].[table name].[fime name] and is: '{s_file_name}'"
            status.add_error(message)
            if trace:
                utils_print.output(message, True)
            return process_status.Error_file_name_struct

        # take dbms name and table namee from the file name
        dbms_name = file_entries[0]
        table_name = file_entries[1]

    if not dbms_name or not table_name:
        return process_status.ERR_command_struct

    if not dbms_name.startswith("blobs_"):
        dbms_name = "blobs_" + dbms_name

    utc_time = utils_columns.get_current_utc_time("%Y-%m-%dT%H:%M:%S.%fZ")
    date_time_key = utils_io.utc_timestamp_to_key(utc_time)

    if not hash_value:
        if source_file:
            got_hash, hash_value = utils_io.get_hash_value(status, source_file, "", None)
            if not got_hash:
                message = f"Failed o calculate hash value to file: '{source_file}' or file not accessible"
                status.add_error(message)
                if trace:
                    utils_print.output(message, True)
                return process_status.ERR_process_failure
        else:

            file_data = status.get_file_data() # Source was provides in a rest call:  curl -X POST -H "command: file store where dest = file2.txt" -F "file=@testdata.txt" http://10.0.0.78:7849
            if not file_data:
                status.add_error("File data to 'file store' command is not available")
                return process_status.ERR_command_struct
            hash_value = utils_data.get_string_hash('md5', file_data, dbms_name + '.' + table_name)


    ret_val = db_info.store_file(status, dbms_name, table_name, file_path, file_name, hash_value, date_time_key, False, False, trace)

    return ret_val
# ----------------------------------------------------------------------------
# Delete a file from the local storage
# file remove where dbms = blobs_edgex and id = 9439d99e6b0157b11bc6775f8b0e2490
# ----------------------------------------------------------------------------
def file_remove(status, io_buff_in, cmd_words, trace, func_params):

    #                          Must     Add      Is
    #                          exists   Counter  Unique
    keywords = {"dbms": ("str", True, False, True),
                "table": ("str", True, True, True),
                "id": ("str", False, True, True),       # Unique file name
                "hash": ("hash", False, True, True),
                "date": ("str", False, True, True),
                }


    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return ret_val

    if not counter:
        status.add_error("Missing parameters in 'file remove' command")
        ret_val = process_status.ERR_command_struct
    else:

        dbms_name, table_name, id_str, hash_value, archive_date = interpreter.get_multiple_values(conditions,
                                                                       ["dbms", "table", "id", "hash", "date"],
                                                                       [None, None, None, None, None])

        if archive_date:
            # get a date string or value that is subtracted from current
            ret_code = utils_columns.validate_date_string(archive_date, "%y%m%d")
            if not ret_code:
                status.add_error(f"The date key '{archive_date}' is not in YYMMDD format")
                ret_val = process_status.ERR_command_struct

    if not ret_val:
        ret_val = db_info.remove_file(status, dbms_name, table_name, id_str, hash_value, archive_date)

    return ret_val
# ----------------------------------------------------------------------------
# Retrieve a file from the local database
# file retrieve where dbms = !my_dbms and table = !my_table and name = !file_name and dest = !my_file
# file retrieve where dbms = blobs_edgex and name = edgex.image.camera001.07da45a366e5778fc7d34bf231bddcfa.id_image_mapping.bin and dest = my_file
# ----------------------------------------------------------------------------
def file_retrieve(status, io_buff_in, cmd_words, trace, func_params):

    #                          Must     Add      Is
    #                          exists   Counter  Unique

    keywords = {"dbms": ("str", True, False, True),
                "table": ("str", True, False, True),
                "id": ("str", False, True, True),
                "hash": ("str", False, True, True),
                "limit": ("int", False, False, True),
                "date": ("str", False, True, True),
                "dest": ("str", False, False, True),        # A path and a file name or only a path to destination folder
                "dest_key": ("str", False, False, True),  # If the needed file is on the OS, dest_key holds the path to the file location
                "stream": ("bool", False, False, True),    # If True - write the file content to the rest client that issued the request
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3, 0, keywords, False)
    if not ret_val:
        # conditions are satisfied by keywords or command structure
        if not interpreter.get_one_value(conditions, "dest"):
            # Make a unique file name - because multiple threads may query the same file
            thread_name = utils_threads.get_thread_name()  # Make unique file name per each thread
            dest_name = f"!tmp_dir/blob_{thread_name}.tmp"
            interpreter.add_value(conditions, "dest", dest_name)

        db_filter = {}

        dbms_name, table_name, id_str, hash_value, archive_date, limit, file_dest, dest_key = interpreter.get_multiple_values(conditions,
                                                                        ["dbms", "table", "id", "hash" , "date", "limit", "dest", "dest_key"],
                                                                        [None,    None,    None, None,    None,   0,       None,  None])

        if id_str:
            db_filter["filename"] = table_name + '.' + id_str        # Unique file name per table
            limit = 1
        elif hash_value:
            db_filter["_id"] = table_name + '.' + hash_value  # Unique file name per table
            limit = 1
        else:

            db_filter["table"] = table_name
            if archive_date:
                # get a date string or value that is subtracted from current
                ret_code = utils_columns.validate_date_string(archive_date, "%y%m%d")
                if not ret_code:
                    status.add_error(f"The date key '{archive_date}' is not in YYMMDD format")
                    ret_val = process_status.ERR_command_struct
                else:
                    db_filter["archive_date"] = archive_date

        if not ret_val:
            # Option A - Retrieve from the object/blob dbms
            ret_val = db_info.retrieve_file(status, dbms_name, db_filter, limit, file_dest)

            if not ret_val:
                if interpreter.get_one_value(conditions, "stream") and status.get_job_handle().is_rest_caller():
                    # Stream the output data to the sender of the message
                    stream_file = interpreter.get_one_value(conditions, "dest")
                    status.get_job_handle().set_stream_file(stream_file)    # Save the name of the file that is streamed to the app or the browser

                if dest_key:
                    # Retrieved from a blob database - keep the path in the dictionary to maintain consistency with option B
                    params.add_param(dest_key, file_dest)  # Save the file name in the local dictionary as f(dest_key)
            elif ret_val == process_status.ERR_dbms_not_opened and id_str and archive_date and len(archive_date) >= 10:
                # No such blob Database - try from file storage - if File ID and File Date are provided
                date_key = f"{archive_date[2:4]}/{archive_date[5:7]}/{archive_date[8:10]}"
                db_name = dbms_name[6:] if dbms_name.startswith("blobs_") else dbms_name

                file_name = params.get_value_if_available(f"!blobs_dir/{date_key}/{db_name}.{table_name}.{id_str}")
                test_result = utils_io.is_path_exists(file_name)
                if test_result:
                    if dest_key:
                        # Option B - Data retrieved from the file system
                        params.add_param(dest_key, file_name)   # Save the file name in the local dictionary as f(dest_key)
                    ret_val = process_status.SUCCESS        # The file identified on the OS

    if trace:
        utils_print.output(
            f"\r\n[File Retrieve] [Result: {process_status.get_status_text(ret_val)}] [{' '.join(cmd_words)}]", True)

    return ret_val
# =======================================================================================================================
# Query the TSD table for the requested member and transfer the listed files to the provided IP and Port
# Member is the member ID in the cluster that owns the requested files
# =======================================================================================================================
def deliver_files(status, io_buff_in, ip, port, tsd_table, files_ids, trace_level):

    if metadata.is_current_node(tsd_table):
        tsd_name = "tsd_info"
    else:
        tsd_name = tsd_table
    tsd_member_id = tsd_table[4:]

    ret_val = process_status.SUCCESS

    files_list = files_ids.split(',')           # File IDs are comma separated. An ID can a nuber or a range seaerated by '-'

    archive_dir = params.get_value_if_available("!archive_dir")

    for range_id, ids_range in enumerate (files_list):
        if ids_range.isdecimal():
            start_id = int(ids_range)
            end_id = start_id + 1              # only a single file
        else:
            # find the range of IDs to copy
            index = ids_range.find('-')
            if index < 1 or index >= (len(ids_range) -1) or not ids_range[:index].isdecimal() or not ids_range[index + 1:].isdecimal():
                status.add_error("Error in file IDs requested from table '%s' from node: %s:%s" % (tsd_table, ip, port))
                ret_val = process_status.Failed_to_extract_msg_components
                break
            start_id = int(ids_range[:index])
            end_id = int(ids_range[index + 1:])     # Up to end_id (end_id not included)

        counter = 0
        for file_id in range(start_id, end_id):
            counter += 1
            # Get the Hash value of the file _ the date associated with the file from the TSD table
            file_id_str = str(file_id)
            ret_code, file_info = db_info.tsd_info_by_id(status, tsd_name, file_id_str)
            if not ret_code:
                if trace_level:
                    utils_print.output("\r\n[Deliver Files] [Range: %u/%u] [File: %u/%u] [Dest: %s:%u] [table: %s] [File ID: %s] [Missing in TSD table] --> [Exit]" % \
                        (range_id + 1, len(files_list), counter, end_id - start_id, ip, port, tsd_table, file_id_str), True)
                ret_val = process_status.TSD_not_available
                break
            if file_info:
                file_name = db_info.file_name_from_tsd_info(file_info, tsd_member_id, file_id_str)
                if not file_name:
                    status.add_error("Failed to retrieve file name from '%s' with file id: %s" % (tsd_table, file_id_str))
                    ret_val = process_status.Failed_to_extract_file_components
                else:
                    file_found = False
                    ret_val, file_path = utils_io.get_archival_file_dir(status, "json", archive_dir, file_name)
                    if not ret_val:
                        file_source = file_path + params.get_path_separator() + file_name
                        file_dest = file_name
                        if not utils_io.is_path_exists(file_source):
                            if trace_level > 1:
                                utils_print.output("\r\n[Deliver Files] [File not found] [%s]" % file_source, True)
                            file_source += ".gz"
                            file_dest += ".gz"
                            if utils_io.is_path_exists(file_source):
                                file_found = True
                            else:
                                if trace_level > 1:
                                    utils_print.output("\r\n[Deliver Files] [File not found] [%s]" % file_source, True)
                        else:
                            file_found = True

                        if trace_level:
                            utils_print.output("\r\n[Deliver Files] [Range: %u/%u] [File: %u/%u] [Dest: %s:%u] [table: %s] [File ID: %s] [File Available: %s]" %\
                                               (range_id + 1, len(files_list), counter, end_id - start_id, ip, port, tsd_table, file_id_str, str(file_found)), True)

                        if file_found:
                            ret_val = transfer_file(status, [(ip, str(port))], file_source, file_dest, message_header.GENERIC_USE_WATCH_DIR, trace_level, "MSG: Deliver File", True)
                        else:
                            # Send a special event message that the file is not available or can not be transferred
                            ret_val = ha.send_missing_arcived_file(status, io_buff_in, ip, port, tsd_table, file_name, trace_level)
                        if ret_val:
                            break   # Failed to send a file or a message
        if ret_val:
            break

    return ret_val
# =======================================================================================================================
#  Sync the blockchain from a source such as a blockchain or a master node
# Example: run blockchain sync where source = eos and time = 5 minutes and dest = file and dest = dbms
# Example: run blockchain sync where source = master and time = 5 minutes and dest = file and dest = dbms and connection = !ip
# Example: run blockchain sync where source = blockchain and platform = ethereum and time = 30 seconds and dest = file
# =======================================================================================================================
def _blockchain_sync(status, io_buff_in, cmd_words, trace):
    #                          Must     Add      Is
    #                          exists   Counter  Unique
    keywords = {"source": ("str", True, False, True),
                "time": ("int.time", True, False, True),
                "connection": ("ip.port", False, False, True),
                "dest": ("str", True, False, False),  # File, dbms
                "file": ("str", False, False, True),  # Blockchain file or taken from !blockchain_file
                "platform" : ("str", False, False, True),  # if connection is blockchain - ethereum, eoos ,etc
                "trace": ("bool", False, False, True),  # Blockchain file or taken from !blockchain_file
                }

    if bsync.get_sync_time() != -1:
        if len(cmd_words) == 3:
            # The command "run blockchain sync" only signals the process to sync
            if not bsync.is_running():
                status.add_error("Failed to trigger sync process: 'run blockchain sync' is called without an active synchronizer")
                return process_status.ERR_process_failure
            process_status.set_signal("synchronizer")
            return process_status.SUCCESS
        else:
            status.add_error("Duplicate call to initiate blockchain synchronizer process")
            return process_status.Process_already_running

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return ret_val

    # get the location of the blockchain file - either provided by the conditions or use !blockchain_file
    ret_val = interpreter.test_add_value(status, conditions, "file", "!blockchain_file", "str")
    if ret_val:
        process_log.add_and_print("Missing input or dictionary definition for \'blockchain_file\'")
        return process_status.ERR_process_failure

    s_time = conditions["time"][0]

    if not s_time or s_time < 1:
        status.add_error("Failed to run blockchain sync process as sync time is set to 0")
        return process_status.ERR_process_failure

    source = interpreter.get_one_value(conditions, "source")
    if  source == "master":
        if not "connection" in conditions.keys():
            # Get the connection from the master node policy
            cmd_get_master =  f"blockchain get (master) bring.ip_port"
            ret_val, reply_info = blockchain_get(status, cmd_get_master.split(), None, True)
            if ret_val or not reply_info or not isinstance(reply_info, str):
                status.add_error("Failed to run blockchain sync process: missing 'connection' info for master node")
                return process_status.ERR_process_failure
            conditions["connection"] = [reply_info]

    elif source == "blockchain":
        platform_name = interpreter.get_one_value(conditions, "platform")
        if not platform_name:
            status.add_error("Missing 'platform' is synchronizer params")
            return process_status.ERR_command_struct
        elif not bplatform.is_connected(platform_name):
            status.add_error("Blockchain platform '%s' is not connected" % platform_name)
            return process_status.Connection_error
        ret_val, txn_count = bplatform.get_txn_count(status, platform_name, 'contract') # check if valid smart contract address
        if ret_val:
            status.add_error("Provided smart contract address is not valid")
            return process_status.BLOCKCHAIN_contract_error
    elif source == "dbms":
        pass
    else:
        status.add_error("Synchronizing with '%s' is not supported - use: 'master' or 'blockchain' or 'dbms' " % source)
        return process_status.ERR_command_struct

    if interpreter.test_one_value(conditions, "dest", "dbms"):  # is sync to dbms
        if not db_info.is_table_exists(status, "blockchain", "ledger"):
            status.add_keep_error(
                "Failed to run blockchain sync process: Table \'ledger\' in 'blockchain' dbms not available")
            return process_status.ERR_process_failure

    if interpreter.get_one_value(conditions, "connection"):     # Connect to a blockchain or master node
        if interpreter.test_one_value(conditions, "source", "dbms"):  # Sync from dbms to a local file
            status.add_keep_error(
                "Failed to run blockchain sync process: Connection requires a source which is a blockchain or a master node")
            return process_status.ERR_process_failure

    if interpreter.test_one_value(conditions, "source", "dbms"):  # Sync from dbms to a local file
        t = threading.Thread(target=bsync.master_synchronizer, args=("dummy", conditions), name="dbms_synchronizer")  # Executed on the master node
    else:
        # Sync from a master node or a blockchain
        t = threading.Thread(target=bsync.synchronizer, args=("dummy", conditions), name="main_synchronizer")
    t.start()

    return process_status.SUCCESS

# =======================================================================================================================
#  Run Operator process. It loads files from the watch directory to a dbms
# Example: run operator where dbms_name = file_name[0] and table_name = file_name[4]
# =======================================================================================================================
def _run_operator(status, io_buff_in, cmd_words, trace):
    #                          Must     Add      Is
    #                          exists   Counter  Unique
    keywords = {"watch_dir": ("dir", False, False, True),
                "err_dir": ("dir", False, False, True),
                "distr_dir": ("dir", False, False, True),       # A watch directory for the distributer process
                "file_type": ("str", False, False, False),
                "master_node": ("ip.port", False, False, True),
                "blockchain": ("str", False, False, True),      # name of platform i.e. ethereum
                "company": ("str", False, False, True),
                "compress_json": ("bool", False, False, True),
                "compress_sql": ("bool", False, False, True),
                "archive_json": ("bool", False, False, True),
                "archive_sql": ("bool", False, False, True),
                "limit_tables": ("str", False, False, False),
                "create_table": ("str", False, False, True),  # Create a new table with new table name
                "update_tsd_info": ("bool", False, False, True),  # Update a summary table for the files being loaded
                "distributor": ("bool", False, False, True),  # Use distributor - move file to !distr_dir after processing
                "flush_streaming" : ("bool", False, False, True),       # If the streamer is not activated, user can use the operator to flush streaming data
                "policy": ("str", True, False, True),        # The operator policy ID
                "threads" : ("int", False, False, True),
                }

    if aloperator.is_running:
        status.add_error("Duplicate call to initiate Operator process")
        return process_status.Process_already_running

    if len(cmd_words) == 3 and cmd_words[2] == "message":  # a tcp sql message
        mem_view = memoryview(io_buff_in)
        command = message_header.get_command(mem_view)
        words_array = command.split()
    else:
        words_array = cmd_words

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, words_array, 3, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return ret_val

    # test that directories are defined
    ret_val = interpreter.add_defualt_dir(status, conditions, ["watch_dir", "err_dir", "bkup_dir", "archive_dir", "distr_dir"])
    if ret_val:
        return ret_val


    # test that blockchain_file is defined
    ret_val = interpreter.test_add_value(status, conditions, "blockchain_file", "!blockchain_file", "str")
    if ret_val:
        return ret_val


    if "limit_tables" in conditions.keys():
        ret_val = db_info.validate_tables(status, conditions["limit_tables"])
        if ret_val:
            return ret_val

    if "update_tsd_info" in conditions.keys():
        if interpreter.get_one_value(conditions, "update_tsd_info"):
            # Update the table 'tsd_info' when new file is ingested to the database
            if not db_info.is_table_exists(status, "almgm", "tsd_info"):
                status.add_error("Table 'tsd_info' in dbms 'almgm' does not exists or dbms is not connected")
                return process_status.ERR_process_failure

    if not "file_type" in conditions.keys():
        # add the default - SQL / JSON / GZ
        interpreter.add_value(conditions, "file_type", "json")
        interpreter.add_value(conditions, "file_type", "sql")
        interpreter.add_value(conditions, "file_type", "gz")

    threads_count = interpreter.get_one_value_or_default(conditions, "threads", 0)
    if threads_count > 10:
        status.add_error("The number of threads allowd for an operator can not be larger than 10")
        return process_status.ERR_process_failure


    ret_val = net_utils.wait_for_server(status, 5)
    if ret_val:
        return ret_val

    policy_id = interpreter.get_one_value(conditions, "policy")

    # Test that the policy exists
    cmd = f"blockchain get operator where id = {policy_id}"
    ret_val, table_info = blockchain_get(status, cmd.split(), None, True)

    if not table_info or not len(table_info):
        status.add_error(f"Missing Operator policy with ID '{policy_id}'")
        return process_status.Needed_policy_not_available
    if len(table_info) != 1:
        status.add_error(f"Multiple Operator policies with identical IDs: '{policy_id}'")
        return process_status.Non_unique_policy
    if ret_val:
        # Test the returned error after the detailed messages
        return ret_val

    metadata.set_operator_policy(policy_id)             # Set the Operator policy ID in the metadata layer
    bsync.blockchain_stat.set_force_load()              # Force a reload of the metadata
    blockchain_load(status, ["blockchain", "get", "cluster"], False, 0)   # Reload the metadata with the operator info

    t = threading.Thread(target=aloperator.run_operator, args=("dummy", conditions), name="main_operator")
    t.start()

    return process_status.SUCCESS


# ==================================================================
# Validate that there are operators supporting the provided tables
# ==================================================================
def validate_operators_support(status, dbms_tables: list, blockchain_file: str):
    ret_val = process_status.SUCCESS
    for dbms_dot_table in dbms_tables:
        # each entry is dbms_name.table_name
        entry = dbms_dot_table.strip()
        index = entry.find('.')
        if index <= 0 or index == (len(entry) - 1):
            status.add_error("Wrong dbms and table specification with the entry: %s" % entry)
            ret_val = process_status.ERR_process_failure
            break
        dbms_name = entry[:index]
        table_name = entry[index + 1:]
        operators = get_operators_ip_by_table(status, blockchain_file, None, dbms_name, table_name)
        if not operators:
            status.add_error("Missing operator info for dbms '%s' and table '%s'" % (dbms_name, table_name))
            ret_val = process_status.Missing_operators_for_table
    return ret_val

# =======================================================================================================================
#  Run archival process. It transfers files to the cluster member nodes
#  Example: run blobs archiver where dbms = true and file = true
# =======================================================================================================================
def _run_archiver(status, io_buff_in, cmd_words, trace):
    #                                Must     Add      Is
    #                                exists   Counter  Unique
    keywords = {
                "bwatch_dir": ("str", False, False, True),
                "blobs_dir": ("str", False, False, True),
                "dbms" : ("bool", False, False, True),      # Use DBMS storage  (Default is False)
                "folder": ("bool", False, False, True),       # Use Folder Storage as f(date)
                "compress": ("bool", False, False, True),  # Compress the file
                "reuse_blobs": ("bool", False, False, True),  # Multiple readings can point to the same files
               }

    if alarchiver.is_arch_running():
        status.add_error("Duplicate call to initiate archiver process")
        return process_status.Process_already_running

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return ret_val

    blob_dir = interpreter.get_one_value_or_default(conditions, "bwatch_dir", "!bwatch_dir")
    ret_val = utils_io.test_dir_exists_and_writeable(status, blob_dir, True)
    if ret_val:
        return ret_val

    blob_dir = interpreter.get_one_value_or_default(conditions, "blobs_dir", "!blobs_dir")
    ret_val = utils_io.test_dir_exists_and_writeable(status, blob_dir, True)
    if ret_val:
        return ret_val

    ret_val = interpreter.add_value_if_missing(status, conditions, "dbms", False)
    if ret_val:
        return ret_val

    ret_val = interpreter.add_value_if_missing(status, conditions, "folder", True)      # True value to archive in a folder
    if ret_val:
        return ret_val

    ret_val = interpreter.add_value_if_missing(status, conditions, "compress", False)
    if ret_val:
        return ret_val

    ret_val = interpreter.add_value_if_missing(status, conditions, "reuse_blobs", False) # True value if the same file is used multiple times
    if ret_val:
        return ret_val

    t = threading.Thread(target=alarchiver.data_archiver, args=("dummy", conditions), name="main_archiver")
    t.start()

    return process_status.SUCCESS

# =======================================================================================================================
#  Start the scheduler process with a dedicated thread
#  Command: run scheduler [id]
# =======================================================================================================================
def _scheduler(status, io_buff_in, cmd_words, trace):

    words_count = len(cmd_words)

    if words_count == 3 and cmd_words[2].isdecimal():
        sched_id = int(cmd_words[2])        # scheduler ID to start
    elif words_count == 2:
        sched_id = 1        # Default value
    else:
        return process_status.ERR_command_struct

    ret_val = task_scheduler.set_scheduler(status, sched_id)
    if ret_val != process_status.Scheduler_not_active:
        if ret_val == process_status.SUCCESS:
            utils_print.output("\r\nScheduler already running", True)
            return process_status.Process_already_running
        else:
            utils_print.output("\r\nWrong value for Scheduler ID", True)
            return process_status.ERR_command_struct

    wake_time = 10  # default time

    t = threading.Thread(target=task_scheduler.schedule_server, args=("dummy", sched_id, int(wake_time)))
    t.start()

    return process_status.SUCCESS
# =======================================================================================================================
#  Schedule a repeatable job -
# prerequisite - run scheduler
# Example schedule time = 1 minute command echo "scheduled message"
# Example schedule time = 1 minute command run client () "sql anylog_test text SELECT max(timestamp) ping_sensor"
# Example schedule time = 1 hour and name = "query 2" command "run client () "sql anylog_test text SELECT max(timestamp) ping_sensor"
# =======================================================================================================================
def _schedule(status, io_buff_in, cmd_words, trace):
    words_count = len(cmd_words)
    if words_count < 2:
        return process_status.ERR_command_struct

    if cmd_words[1] == "message":  # a tcp sql message
        mem_view = memoryview(io_buff_in)
        command = message_header.get_command(mem_view)
        words_array = command.split()
        message = True
        words_count = len(words_array)
    else:
        words_array = cmd_words
        message = False

    command_offset = 1
    word = ""
    for word in words_array[1:]:
        if word == "task":
            break
        command_offset += 1

    if word != "task" or words_count <= (command_offset + 1):
        status.add_keep_error("Missing 'task' in schedule statement")
        ret_val = process_status.ERR_command_struct
    else:

        # get the conditions to execute the JOB
        #                                      Must     Add      Is
        #                                      exists   Counter  Unique

        keywords = {"time": ("int.time", True, False, True),
                    "name" : ("str", False, False, True),           # A name to provide to the message
                    "scheduler": ("int", False, False, True),       # A scheduler ID, otherwise the default is 1
                    "start": ("date", False, False, True),
                    }

        ret_val, counter, conditions = interpreter.get_dict_from_words(status, words_array, 1, command_offset, keywords, False)
        if not ret_val:
            # conditions are satisfied by keywords or command structure

            repeat_time = conditions["time"][0]  # get time in seconds

            task_name = interpreter.get_one_value(conditions, "name")  # Get a name to the condition

            sched_id = interpreter.get_one_value_or_default(conditions, "scheduler", 1)

            start_time = interpreter.get_one_value(conditions, "start")

            # Scheduler may not be running, the call to set_scheduler will set the buffers for this scheduler
            ret_val = task_scheduler.set_scheduler(status, sched_id)
            if not ret_val or (ret_val != process_status.ERR_command_struct):
                # could be that the scheduler was running and the error is ignored
                if task_scheduler.get_task_by_name(sched_id, task_name):
                    status.add_keep_error("Duplicate task name: '%s'" % task_name)
                    ret_val = process_status.ERR_command_struct
                else:

                    # the command to execute
                    task_string = utils_data.get_str_from_array(words_array, command_offset + 1, 0)
                    if not task_string:
                        status.add_keep_error("Missing scheduled command to execute")
                        ret_val = process_status.ERR_command_struct
                    else:
                        task = task_scheduler.get_new_task(sched_id, task_name, start_time, repeat_time, task_string)
                        ret_val = process_status.SUCCESS


    if ret_val and message:
        error_message(status, io_buff_in, ret_val, message_header.BLOCK_INFO_COMMAND, "echo Failed to schedule task", status.get_saved_error())

    return ret_val
# =======================================================================================================================
# Operate on a Task in the scheduler
# Command options:
# Task stop where scheduler = 1 and name = "my task"
# Task resume where scheduler = 1 and name = "my task"
# Task remove where scheduler = 1 and name = "my task"
# Task init where scheduler = 1 and name = "my task" and start = +1d
# Task run where scheduler = 1 and name = "my task"
# =======================================================================================================================
def _process_task(status, io_buff_in, cmd_words, trace):

    words_count = len(cmd_words)
    if words_count < 2:
        return process_status.ERR_command_struct

    if cmd_words[1] == "message":  # a tcp sql message
        mem_view = memoryview(io_buff_in)
        command = message_header.get_command(mem_view)
        words_array = command.split()
        message = True
        words_count = len(words_array)
    else:
        words_array = cmd_words
        message = False

    if len(words_array) < 6:
        ret_val = process_status.ERR_command_struct
    else:

        cmd_options = [
            "stop", "resume", "remove", "init", "run"
        ]

        operation = words_array[1]
        if operation not in cmd_options or words_array[2] != "where":
            ret_val = process_status.ERR_command_struct
        else:

            keywords = {"scheduler": ("int", True, False, True),
                        "name": ("str", False, True, True),  # A name to provide to the message
                        "id": ("int", False, True, True),  # A scheduler ID, otherwise the default is 1
                        "start": ("date", False, False, True),
                        }

            ret_val, counter, conditions = interpreter.get_dict_from_words(status, words_array, 3, 0, keywords, False)
            if not ret_val:
                # conditions satisfied by keywords or command structure
                if not counter:
                    status.add_keep_error("Task Name or Task ID needs to be provided")
                    ret_val = process_status.ERR_command_struct
                elif counter == 2:
                    status.add_keep_error("Task Name and Task ID are provided - only one is needed")
                    ret_val = process_status.ERR_command_struct
                else:
                    sched_id = interpreter.get_one_value_or_default(conditions, "scheduler", 1)

                    task_id =  interpreter.get_one_value(conditions, "id")
                    if task_id:
                        task = task_scheduler.get_task(sched_id, task_id)
                    else:
                        task_name = interpreter.get_one_value(conditions, "name")
                        task = task_scheduler.get_task_by_name(sched_id, task_name)

                    if not task:
                        if task_id:
                            task_name = "#%u" % task_id
                        status.add_keep_error("Task '%s' in scheduler '%u' is not valid" % (task_name, sched_id))
                        ret_val = process_status.Task_not_in_scheduler
                    else:
                        if operation == "stop":
                            task.set_mode("Stopped")
                        elif operation == "resume":
                            task.set_mode("Paused")     # Paused will continue if start date and time is valid
                        elif  operation == "remove":
                            task.set_mode("Removed")    # Flag as removed
                        elif  operation == "init":
                            # Set new start date and time
                            start_time = interpreter.get_one_value(conditions, "start")
                            if not start_time:
                                status.add_error("Missing start date and time for scheduled task")
                                ret_val = process_status.ERR_command_struct
                            else:
                                task.set_start_time(start_time)
                                task.set_mode("Paused")    # Flag to consider start date and time
                        elif operation == "run":
                            command_str = task.get_task_command()
                            ret_val = process_cmd(status, command_str, False, None, None, io_buff_in)

    if ret_val and message:
        error_message(status, io_buff_in, ret_val, message_header.BLOCK_INFO_COMMAND, "echo Failed to process task", status.get_saved_error())

    return ret_val
# =======================================================================================================================
# Pause processing for the specified duration (in seconds)
# wait [max wait time in seconds] for [condition]
# wait 5
# wait 5 for !my_param
# wait 5 for !my_param.len > 1
# =======================================================================================================================
def _wait(status, io_buff_in, cmd_words, trace):

    offset = get_command_offset(cmd_words)
    words_count = len(cmd_words)

    seconds = params.get_value_if_available(cmd_words[offset + 1])
    try:
        sleep_time = int(seconds)
    except:
        status.add_error("Wait time in 'wait' command is not properly provided")
        return process_status.ERR_command_struct
    else:
        if words_count == 2:
            utils_threads.seconds_sleep("Sleep", sleep_time)
            return process_status.SUCCESS

    result = "False"

    if (len(cmd_words) + offset) >= 4 and cmd_words[ offset + 2] == "for":
        # wait for condition
        condition = ["wait_result_878", "=", "if"] + cmd_words[ offset + 3:]
        try:
            for i in range (sleep_time):
                ret_val = _process_if(status, io_buff_in, condition, trace)

                if ret_val:
                    # Error in the if condition
                    break

                if params.get_value_if_available("!wait_result_878") == "True":
                    result = "True"
                    break   # Condition satisfied

                time.sleep(1)
        except:
            utils_print.output("Keyboard Interrupt - exit from sleep(%u)" % sleep_time, True)

        ret_val = process_status.SUCCESS
    else:
        ret_val = process_status.ERR_command_struct

    if offset:
        params.add_param(cmd_words[0], result)

    return ret_val

# =======================================================================================================================
# Create a new policy and update the default values
# create policy master with defaults
# create policy master with defaults where company = my_company and country = my_country
# =======================================================================================================================
def create_policy(status, io_buff_in, cmd_words, trace):

    offset = get_command_offset(cmd_words)
    words_count = len(cmd_words)


    if cmd_words[offset + 3] == "with":
        if  cmd_words[offset + 4] != "defaults":
            status.add_error("Missing 'defaults' in 'create policy' command")
            return process_status.ERR_command_struct
        defaults = True     # add defaults values to the policy as f(policy type)
        offset_where = offset + 5       # Offset where condition
    else:
        defaults = False
        offset_where = offset + 3

    policy_type = cmd_words[offset + 2].lower()
    policy_inner = {}

    if words_count > offset_where:
        if cmd_words[offset_where] != "where":
            status.add_error("Missing 'where' in 'create policy' command")
            return process_status.ERR_command_struct

        if words_count < (offset_where + 4):
            status.add_error("Wrong structure to 'where condition' in 'create policy' command")
            return process_status.ERR_command_struct

        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, offset_where + 1, 0, None, False)
        if ret_val:
            return ret_val

        for key, value_list in conditions.items():
            # Add the user values
            policy_value = value_list[0]
            policy_inner[key] = policy_value


    if defaults:
        # Add the default values
        ret_val = policies.add_policy_defaults(status, policy_type, policy_inner)
        if ret_val:
            return ret_val

    new_policy = {
        policy_type: policy_inner
    }

    if defaults:
        blockchain_file = params.get_value_if_available("!blockchain_file")
        if blockchain_file == "":
            message = "Missing dictionary definition for \'blockchain_file\'"
            status.add_keep_error(message)
            ret_val = process_status.No_local_blockchain_file
        else:
            ret_val = policies.process_new_policy(status, new_policy, blockchain_file)

    if not ret_val:
        policy_str = utils_json.to_string(new_policy)

        if offset:
            params.add_param(cmd_words[0], policy_str)
        else:
            utils_print.struct_print(policy_str, False, True)

    return ret_val
# =======================================================================================================================
# Consider a variable as an integer and add the specified value to the variable
# If value is not specified it is considered to be 1
# value = incr value 3
# =======================================================================================================================
def _incr(status, io_buff_in, cmd_words, trace):
    offset = get_command_offset(cmd_words)
    words_count = len(cmd_words)

    if words_count == (offset + 2):
        value = 1  # defualt value
    elif words_count == (offset + 3):
        try:
            value = int(cmd_words[offset + 2])
        except:
            status.add_error("Wrong value provided to 'incr' command: %s" % value)
            return process_status.Error_command_params
    else:
        return process_status.ERR_command_struct

    variable = params.get_value_if_available(cmd_words[offset + 1])
    try:
        var = int(variable)
    except:
        status.add_error("Wrong value provided to 'incr' command: %s" % variable)
        return process_status.Error_command_params

    result = var + value

    if not offset:
        utils_print.output(str(result), True)
    else:
        params.add_param(cmd_words[0], str(result))

    return process_status.SUCCESS

# =======================================================================================================================
# Data Assignment:
# a = b or !a = b or a = a + b
# =======================================================================================================================
def data_assignment(status, first_word, cmd_words):

    words_count = len(cmd_words)
    type_value = None
    if (first_word == 2 and words_count >= 3):

        if words_count > 3 and cmd_words[3] != '+':
            # Example: a = 30 seconds
            new_string = ' '.join(cmd_words[2:])
        else:
            if words_count == 3 and  cmd_words[1] == '=':
                # Assignment
                # Example:
                # dict = {}
                # dict[key] = !k.float
                type_value = params.get_value_if_available(cmd_words[2])
                new_string = str(type_value)
            else:
                # Example: a = a + b
                new_string = params.get_added_string(cmd_words, 2, 0)

        dict_key = cmd_words[0]
        # Test if a policy
        if dict_key[-1] == ']':
            # Set the key of the root
            # Example:  schedule_policy[schedule] = { "id": !policy_id,  "name": !policy_name}
            index = dict_key.find('[')
            if index > 0 and index < (len(dict_key)-2):
                json_key = dict_key[index:]
                dict_key = dict_key[:index]
                value = type_value if (type_value != None) else new_string    # Assignment may be a non-string value
                ret_val = params.set_dictionary(status, dict_key, [json_key, '=', value] )
                return True if ret_val == process_status.SUCCESS else False

        if dict_key[0] == '!' and len(dict_key) > 1:
            params.add_param(dict_key[1:], new_string)  # !x = y
        else:
            if dict_key[0] == '$' and len(dict_key) > 1:
                # Add environment variable
                params.set_env_var(dict_key[1:], new_string)
            else:
                # Add to anylog dictionary
                params.add_param(dict_key, new_string)

        ret_val = True
    else:
        ret_val = False

    return ret_val


# =======================================================================================================================
# Save the structure of a table as f(dbms_name and a table name)
# =======================================================================================================================
def set_table_structure(status, job_location: int, receiver_id: int, par_id: int):
    j_instance = job_scheduler.get_job(job_location)
    db_name = j_instance.get_remote_dbms()
    table_name = j_instance.get_remote_table()
    # Place the metadata (the table structure) on the tables dictionary

    table_struct = j_instance.get_returned_data(receiver_id, par_id)
    with_info = False
    if table_struct and len(table_struct):
        key = next(iter(table_struct))
        if len(table_struct[key]):
            with_info = True
    if not with_info:
        status.add_keep_error("Received empty table structure for dbms '%s' and table '%s'" % (db_name, table_name))
        ret_val = process_status.Table_struct_not_available
    else:
        db_info.set_table(db_name, table_name, table_struct)
        ret_val = process_status.SUCCESS
    return ret_val


# =======================================================================================================================
# Run next query after a leading query (for example 'period' function)
# =======================================================================================================================
def next_query(status, io_buff_in, j_instance):
    # Apply the results of the leading query
    select_parsed = j_instance.get_job_info().get_select_parsed()

    ret_val = select_parsed.process_leading_results(status)  # apply the results on the next query
    if ret_val:
        error_msg = "Failed to process leading query for: " + select_parsed.get_generic_query()
        status.add_error(error_msg)
        utils_print.output(error_msg, True)
    else:

        select_parsed.set_on_next_query()  # flag that current leading query was processed
        j_instance.reset_prams_for_new_job()

        ip_port_values = j_instance.get_ip_port_values()
        list_size = len(ip_port_values)

        job_id = j_instance.get_job_id()
        unique_job_id = j_instance.get_unique_job_id()

        is_leading, new_message = process_leading_message(status, select_parsed)
        if is_leading:
            # send leading message
            message = new_message  # The Leading Message
            ret_val = process_status.SUCCESS
        else:
            # Get main query

            message = "sql " + j_instance.get_remote_dbms() + " "

            # Get the conditions (like extend and include
            conditions = j_instance.get_job_handle().get_conditions()
            first_condition = True
            for key, cond_list in conditions.items():
                if not first_condition:
                    message += " and"
                else:
                    first_condition = False
                message += " %s = " % key
                if isinstance(cond_list, list):
                    if len (cond_list) == 1:
                        entry = str(cond_list[0])   # Just one value in the list
                    else:
                        entry = None                # Multiple values in the list
                else:
                    # not in a list
                    entry = str(cond_list)

                if entry:
                    # A single value
                    if ' ' in entry:
                        message += "(" + entry + ")"    # multiple words make the value (like conditions with: extend=(@table_name as table)
                    else:
                        message += entry
                else:
                    # add conditions in parenthesis
                    message += "("
                    for index, entry in enumerate(cond_list):
                        if index:
                            message += ","
                        message += str(entry)
                    message += ") "

            # Add the SQL stmt
            sql_stmt = j_instance.get_remote_query()
            message += " " + sql_stmt

            select_parsed.get_projection_functions().reset()  # the functions in the array were used in the leading query
            j_instance.set_num_receivers(list_size)
            ret_val = create_local_table(status, job_id, True)

        if not ret_val:
            ret_val = send_command_to_nodes(status, io_buff_in, ip_port_values, list_size, message, "", job_id,
                                            unique_job_id, True, 0)

    return ret_val


# =======================================================================================================================
# Query the local database to provide a unified result
# "system_query" is the local database that keeps the query results that are returned from different operators.
# =======================================================================================================================
def query_local_table(status, io_buff_in, job_location: int, conditions, name_key: str):
    if db_info.is_dbms("system_query"):

        if name_key:
            interpreter.add_value(conditions, "output_key", name_key)  # assign results to this dictionary key
        if not "dest" in conditions.keys():
            interpreter.add_value(conditions, "dest", "stdout")
        conditions["message"] = False

        cmd_words = ["sql", "system_query", "text"]
        j_instance = job_scheduler.get_job(job_location)
        local_query = j_instance.get_local_query()
        local_table = j_instance.get_local_table()
        nodes_count = j_instance.get_nodes_participating()
        nodes_replied = j_instance.get_nodes_replied()

        ret_val = process_query_sequence(status, io_buff_in, cmd_words, "system_query", local_table, conditions,
                                         local_query, nodes_count, nodes_replied)

    else:
        ret_val = process_status.ERR_dbms_not_opened
        process_log.add("Error", "Query DBMS not recognized: system_query")
    return ret_val


# =======================================================================================================================
# Processing when all nodes replied
# Code status explain the type of processing completed:
#
#  Status   |  Processing
#    1      |  Error on the peer node - peer_error shows the peer error code
#    2      |  Projection functions like MIN, NAX, COUNT that are supported natively by AnyLog
#    3      |  pass through - no local database - results from operators are transferred when arrived
#    4      |  Local database is updated with the data returned from the Operators
#    5      |  Metadata was updated
#    6      |  Empty data set

# =======================================================================================================================
def process_all_nodes_replied(status, peer_error, node_error, code_status, j_instance, j_handle, io_buff_in,
                              job_location, unique_job_id):
    ret_val = process_status.SUCCESS
    name_key = ""
    select_parsed = j_instance.get_job_info().get_select_parsed()
    is_rest_caller = j_handle.is_rest_caller()

    if j_handle.is_subset():
        # wait for all messages to be send to destination nodes
        # otherwise flags may not be properly set (for example, number of nodes replied, which may be set by the run client () command
        # in the case of failure to open a socket against the target node.
        while not j_instance.is_in_target():
            # Wait that all messages are send, before testing j_instance.all_replied()
            time.sleep(1)

    j_instance.data_mutex_aquire(status, 'W')  # take exclusive lock

    if j_instance.all_replied() or (peer_error and peer_error != process_status.Empty_data_set and not j_handle.is_subset()) or node_error:

        if j_instance.is_input_job():
            # returned data is an input to the caller
            dest_job = j_instance.get_output_job()
            name_key = str(id(dest_job))  # A key to be assigned with the result set

        # PART A - EXIT if PROCESS WAS COMPLETED

        if not j_instance.is_job_active() or j_instance.get_unique_job_id() != unique_job_id or (is_rest_caller and j_handle.is_signaled()):
            # Process ended by a different thread
            j_instance.data_mutex_release(status, 'W')
            if peer_error:
                ret_val = peer_error
            else:
                ret_val = node_error
            return ret_val
        else:

            # PART B - RETURN DATA TO CALLER

            # The process that completes the JOB starts here. Needs to handle SUCCESS and ERROR
            completed = True
            j_instance.set_job_status("Processing Data")  # Processing Data Locally
            if code_status != 1 and not node_error:  # code_status is 1 with Peer Error
                if code_status == 4 or code_status == 6:
                    # Local database is updated with the data returned from the Operators
                    # Or the operator reported no data but a different partition or a different operator may be with data
                    # --------------------------------------------------------------
                    #  Reply on commands that return rows (without errors)
                    # --------------------------------------------------------------
                    conditions = j_handle.get_conditions()
                    if is_rest_caller:
                        conditions["dest"] = ["rest"]
                    if not interpreter.get_one_value(conditions, "table"):
                        # if table name - this is a repeatable query and the data remains in the source table
                        ret_val = query_local_table(status, io_buff_in, job_location, conditions, name_key)  # Query the unified results
                elif code_status == 5:  # Metadata was updated
                    if is_rest_caller:
                        info_data = j_instance.get_reply_data("Result.set")
                        status.get_active_job_handle().set_result_set(info_data)
                    else:
                        if j_instance.get_print_status():
                            # get print status can return False if stdout print of messages are suppressed
                            j_instance.print_reply_info()

                # PART C - END JOB PROCESS

                if select_parsed.is_processing_leading():
                    if j_instance.is_with_data():
                        # At least one node replied with data, otherwise, query ends
                        ret_val = next_query(status, io_buff_in, j_instance)
                        if ret_val:
                            j_instance.data_mutex_release(status, 'W')
                            return ret_val
                        completed = False

        if completed:
            j_handle.stop_timer()

            if code_status == 1:  # Error on Peer

                j_instance.set_job_status("Error")
                j_handle.set_operator_error(peer_error)  # PLace the returned error on the status object
                ret_val = peer_error
            else:
                j_instance.set_job_status("Completed")
            if j_handle.is_rest_caller():

                if code_status == 4:  # Local database is updated with the data returned from the Operators
                    # REST directed output to a table
                    conditions = j_handle.get_conditions()
                    if interpreter.get_one_value(conditions, "table"):  # A designated table to maintain the result sets
                        local_table = j_instance.get_local_table()
                        rest_message = "{\"Query.output.all\":\"Data in table '%s'\"}" % local_table
                        utils_io.write_to_stream(status, j_handle.get_output_socket(), rest_message, True, True)

                if not j_instance.is_with_data():
                    # all returned No Data
                    if not j_instance.is_pass_through():
                        # In pass through the message was written to the rest API - see method pass_data()
                        j_handle.set_result_set(
                            "{\"Query.output.all\":\"Empty result set\"}")  # PLace the returned error on the status object

                j_handle.signal_wait_event()  # signal the REST thread that output is ready
            else:
                j_instance.set_not_active()  # flag the the job_instance can be reused - With REST, the call is done by the REST thread
                if code_status == 6:
                    ret_val = process_status.Empty_data_set


    j_instance.data_mutex_release(status, 'W')

    if name_key:
        # Execute the target query - using current query as an input to the target
        input_query_id = j_instance.get_input_query_id()  # This is an ID of the input Query being executed. It is set when the queries are send
        ret_val = update_target_query(status, io_buff_in, input_query_id, dest_job, name_key)
        params.del_param(name_key)  # Free the space used

    return ret_val


# =======================================================================================================================
# Process AnyLog commands - main process
# =======================================================================================================================
def process_cmd(status, command, print_cmd, source_ip, source_port, io_buffer_in, user_input = False):
    '''
    status - user status object
    command - a string containing the command to consider
    print_cmd - DIsplay command for debug
    source_ip - the IP of the source node (issuing the command)
    source_port - the port of the source node (issuing the command)
    io_buffer_in - the command/data TCP buffer (Network IO buffer)
    user_input - user input cmd by keyboard
    '''

    global max_command_words

    if (command == ""):
        return process_status.SUCCESS  # empty string

    sub_str, left_brackets, right_brakets = utils_data.cmd_line_to_list_with_json(status, command, 0, 0)  # a list with words in command line


    if sub_str == None:
        if not status.get_active_job_handle().is_rest_caller():
            utils_print.output(process_status.get_status_text(process_status.ERR_command_struct), True)  # print error message
        return process_status.ERR_command_struct

    words_count = len(sub_str)

    if words_count == 0:
        return process_status.SUCCESS  # No commands on command line

    is_message = sub_str[-1] == "message"

    first_word = 0
    if (words_count > 2 and sub_str[1] == '='):
        first_word = 2  # assignment to dictionary

    if user_input:
        # Add Destination (if destination was set using: run client dest
        if sub_str[first_word] == '.':
            # ignore preset destination
            if words_count == (first_word + 1):
                utils_print.output(process_status.get_status_text(process_status.ERR_command_struct), True)  # print error message
                return process_status.ERR_command_struct
            if first_word:
                # assignment
                sub_str = sub_str[:first_word] + sub_str[first_word + 1:]
            else:
                sub_str = sub_str[1:]       # Execute the command locally
            words_count -= 1                # Remove the dot
        elif node_info.is_with_destination(sub_str):
            # Add the "run client" instruction
            sub_str = node_info.add_destination(sub_str, words_count, first_word)
            words_count = len(sub_str)

    data = sub_str[0]
    if words_count == 1 and len(data) > 1 and (data[0] == '!' or data[0] == '$'):
        # Print Value from dictionary
        value = params.get_value_if_available(data)
        utils_print.struct_print(value, True, True)
        return process_status.SUCCESS

    is_rest_caller = status.get_active_job_handle().is_rest_caller()

    if left_brackets != right_brakets:
        if left_brackets > right_brakets:
            error_code = process_status.ERR_missing_brackets
        else:
            error_code = process_status.ERR_wrong_json_structure
    else:
        error_code = 0

    if not error_code:

        if print_cmd:
            utils_print.output(f"[From IP: {source_ip}:{source_port}] {command}", True)

        y = words_count

        if (max_command_words + first_word) < y:
            y = max_command_words + first_word

        cmd_sub_string = ""
        error_code = process_status.Not_in_commands_dict  # initial value is command not found

        for x in range(first_word, y):  # go over the words in the command line
            if x != first_word:
                cmd_sub_string += ' '

            word = sub_str[x]
            if (len(sub_str[x]) > 1 and word[0:1] == "!"):
                # if first char is ! - replace word with dictionary
                word = params.get_param(sub_str[x][1:])

            cmd_sub_string += word  # set command string that is to be tested

            if cmd_sub_string in commands.keys():
                trace_level = commands[cmd_sub_string]['trace']

                if trace_level > 1:
                    utils_print.output("[Command] [%s]" % ' '.join(sub_str), True)
                if "words_min" in commands[cmd_sub_string] and commands[cmd_sub_string]["words_min"] > words_count:
                    continue        # Not enough words

                if cmd_sub_string != "job" and (source_ip or is_rest_caller) and version.al_auth_is_node_authentication():
                    # Use the public key to validate permissions
                    public_key = status.get_pub_key()
                    if not version.permissions_permission_process(status, 0, f"{source_ip}:{source_port}", public_key, cmd_sub_string, "", ""):
                        error_code = process_status.Unauthorized_command
                        break


                error_code = commands[cmd_sub_string]['command'](status, io_buffer_in, sub_str, trace_level)

                break

        if error_code:
            if error_code == process_status.Not_in_commands_dict:
                if data_assignment(status, first_word, sub_str):  # exit or dictionary assignments
                    error_code = process_status.SUCCESS

    cmd_text = utils_data.get_str_from_array(sub_str, 0, 0)

    if not error_code:
        if status.get_warning_value():
            warning_msg = status.get_warning_msg()
            process_log.add("Event", "[Warning: " + warning_msg + "] " + cmd_text)
        else:
            # if cmd_text != "job message":    # this is to reduce the amount of data in the log
            if not is_message or (x + 1) == words_count:
                process_log.add("Event", "(ok) " + cmd_text)  # update event log
    elif error_code < process_status.NON_ERROR_RET_VALUE:

        if source_port and source_ip and message_header.get_error(io_buffer_in):
            # The command JOB can return the error number from the peer node
            # Add source IP and Port
            error_msg = f"Process triggered by node: {source_ip}:{source_port} failed: {process_status.get_status_text(error_code)}"
            details = message_header.get_data_decoded(io_buffer_in)
            if details:
                error_msg += f" : '{details}'"

        else:
            error_msg = process_status.get_status_text(error_code)
        if is_message:
            mem_view = memoryview(io_buffer_in)
            cmd_text = message_header.get_command(mem_view).replace('\t', ' ')
            header_txt = "(Message Failed: "
        else:
            header_txt = "(Failed: "

        status.add_error(header_txt + error_msg + ") " + cmd_text)

        if status.get_considered_err_value() != error_code:
            # if equal - the error_code was set on the status objected and the error message was printed (this is a recursive call to process_cmd)
            if user_input:
                if not first_word:  # if not assignment
                    # if User input and not assignment
                    utils_print.output(error_msg, True)  # print error message
                elif echo_queue:
                    # assignment + error + echo queue is active
                    echo_queue.add_msg("Error executing: [%s] Message: [%s]" % (' '.join(sub_str), error_msg))
            elif echo_queue:
                echo_queue.add_msg("Error executing: [%s] Message: [%s]" % (' '.join(sub_str), error_msg))
            else:
                utils_print.output(error_msg, True)  # print error message
            status.set_considerd_err_value(error_code)

    return error_code
# =======================================================================================================================
# Get a structure with compiled commands
# It is when the same commands are executed repeatedly
# Each command is placed in an array with the following values:
# command_key - The key that trigers the command process
# cmd_words - the command as a list - including the if statement
# cmd_exec - the command as a list without the if statement
# offset_then - location of "then" in the cmd_words, or 0 if not and "if" stmt.
# with_paren - True if the command includes if within parenthesis
# conditions_list - each condition of the if stmt
# =======================================================================================================================
def compile_commands(status, non_compiled, policy_id):
    '''
    non_compiled - a list with the commands to compile
    policy_id - optional - used in error message
    '''

    ret_val = process_status.SUCCESS
    compiled_list = []  # Every entry in the list represents a command in the script

    for cmd_line in non_compiled:
        conditions_list = []
        cmd_entry = cmd_line.strip()
        if cmd_entry:
            cmd_words, left_brackets, right_brakets = utils_data.cmd_line_to_list_with_json(status, cmd_entry, 0,
                                                                                            0)  # a list with words in command line
            if left_brackets != right_brakets:
                status.add_error("Policy [%s] is missing parenthesis in the '%s' statement" % (policy_id, cmd_entry))
                ret_val = process_status.ERR_in_script_cmd
                break
            if cmd_words[0] == "if":
                # This is an if statement - analyze the if
                ret_val, offset_then, with_paren = params.analyze_if(status, cmd_words, 0, conditions_list)
                if ret_val:
                    break
                first_word = offset_then + 1
                if offset_then < 2 or len(cmd_words) <= (offset_then + 1):
                    status.add_error("Policy [%s] - wrong if structure: '%s'" % (policy_id, cmd_entry))
                    ret_val = process_status.ERR_in_script_cmd
                    break

                cmd_exec = cmd_words[offset_then + 1:]  # The command part after the if part
            else:
                first_word = 0
                offset_then = 0
                with_paren = False
                cmd_exec = cmd_words
                conditions_list = None

            command_key = get_cmd_sub_string(cmd_words, first_word)
            if not command_key:
                # test assignment:
                if (first_word + 3) > len(cmd_words) or cmd_words[first_word + 1] != '=':
                    # Not an assignment (a = b)
                    status.add_error("Policy [%s] with unknown command: '%s'" % (policy_id, cmd_entry))
                    ret_val = process_status.ERR_in_script_cmd
                    break
            compiled_list.append((command_key, cmd_words, cmd_exec, offset_then, with_paren, conditions_list))

    return [ret_val, compiled_list]
# =======================================================================================================================
# Get the cmd_sub_string from a command - find the key of a given command
# =======================================================================================================================
def get_cmd_sub_string(cmd_words, offset_word):
    '''
    cmd_words - the entire command as a parsed list
    offset_word - first word to consider
    '''

    cmd_sub_string = ""
    command_key = ""
    words_count = len(cmd_words)
    first_word = offset_word

    if offset_word + 4 <= words_count and cmd_words[offset_word + 1] == '=':
        # This is a command assignmemt like:  "if !alert_flag_1 then ingestion_alerts[Node_Name] = get node name",
        # Skip the assignment
        first_word += 2


    if (first_word + max_command_words) > words_count:
        y = words_count
    else:
        y = first_word + max_command_words

    for x in range(first_word, y):  # go over the words in the command line
        if x != first_word:
            cmd_sub_string += ' '

        word = cmd_words[x]
        if (len(cmd_words[x]) > 1 and word[0:1] == "!"):
            # if first char is ! - replace word with dictionary
            word = params.get_param(cmd_words[x][1:])

        cmd_sub_string += word  # set command string that is to be tested

        if cmd_sub_string in commands.keys():
            command_key = cmd_sub_string
        else:
            if command_key:
                break       # The key was found, but additional word fails

    return command_key

# =======================================================================================================================
# run a particular job, if the job is scheduled, continue executing the scheduled job using the scheduler
# =======================================================================================================================
def job_run(status, cmd_words, io_buff_in):
    words_count = len(cmd_words)

    if words_count == 3:
        job_location = cmd_words[2]
    elif words_count == 2:
        job_location = str(job_scheduler.get_recent_job())
    else:
        return process_status.ERR_job_id

    if not job_scheduler.is_valid_job_id(job_location):
        return process_status.ERR_job_id

    job_id = int(job_location)

    j_instance = job_scheduler.get_job(job_id)

    # A running job is being stopped from processing

    command_str = j_instance.get_job_command()

    if command_str != "":
        ret_val = process_cmd(status, command_str, False, None, None, io_buff_in)
    else:
        ret_val = process_status.ERR_job_id

    return ret_val


# =======================================================================================================================
# Update the local database with the rows returned
# "system_query" is the local database that keeps the query results that are returned from different operators.
# =======================================================================================================================
def insert_query_rows(status, io_buff_in, job_location: int, receiver_id: int, par_id: int, block_data_struct: int):
    ret_val = process_status.SUCCESS

    j_instance = job_scheduler.get_job(job_location)

    if db_info.is_dbms("system_query"):

        if block_data_struct == message_header.BLOCK_STRUCT_JSON_SINGLE:
            # a single JSON structure describes the data in the block
            query_data = j_instance.get_returned_data(receiver_id, par_id)
            local_table = j_instance.get_local_table()
            insert_data = map_results_to_insert.map_results_to_insert_main(status, local_table, query_data)

            ret_val = process_cmd(status, "sql system_query text \"" + insert_data + "\"", False, None, None,
                                  io_buff_in)
        elif block_data_struct == message_header.BLOCK_STRUCT_JSON_MULTIPLE:
            # multiple JSON structures are placed in each block.
            # Each JSON is prefixed by size + a byte indicating if the entire JSON in with the block

            # -------------------------------------------------------------
            # Process multiple SQL Inserts (to a local database with the logical name - "system_query")
            # -------------------------------------------------------------
            dbms_cursor = cursor_info.CursorInfo()  # Get the AnyLog Generic Cusror Object
            if not db_info.set_cursor(status, dbms_cursor,
                                      "system_query"):  # "system_query" - a local database that keeps the query results
                process_log.add("Error", "Query DBMS not recognized: system_query")
                return process_status.DBMS_error

            ret_val = deliver_rows(status, io_buff_in, j_instance, receiver_id, par_id, j_instance.is_pass_through(), dbms_cursor, False, None)

            if not ret_val:
                dbms_cursor.commit(status)

            db_info.close_cursor(status, dbms_cursor)
    else:
        ret_val = process_status.ERR_dbms_not_opened
        process_log.add("Error", "Query DBMS not recognized: system_query")

    return ret_val


# =======================================================================================================================
# Debug info that is added to the job_instance to track data returned from storage nodes.
# This process is called from tcpip_server as calls are received
# =======================================================================================================================
def debug_to_job_instance(mem_view):
    job_location = message_header.get_job_location(mem_view)
    j_instance = job_scheduler.get_job(job_location)
    message_type, receiver_id, par_counter, par_id, error_code = j_instance.process_message(mem_view)
    j_instance.add_debug_info("Reply: %s - %s.%s.%s.%s - %s" % (
        job_location, receiver_id, par_counter, par_id, error_code, message_header.is_last_block(mem_view)))

# =======================================================================================================================
# Process job message from a different node
# run client () "sql pge_test text select * from device_sensor where type = 'ab' limit 10"
# =======================================================================================================================
def process_reply(status, io_buff_in):
    ret_val = process_status.SUCCESS
    mem_view = memoryview(io_buff_in)

    # 1) get the job id from the message header
    job_location = message_header.get_job_location(mem_view)
    unique_job_id = message_header.get_job_id(mem_view)  # a unique ID of this JOB

    # 2) get the jon instance from the list of jobs
    j_instance = job_scheduler.get_job(job_location)
    # 3) get the job handle from the job instance -> the job handle was placed by the run_tcp_client process
    j_handle = j_instance.get_job_handle()
    # 4) set the job handle on the current thread status
    status.set_active_job_handle(job_location, j_handle)

    select_parsed = j_instance.get_job_info().get_select_parsed()

    # Use a read lock as a) multiple threads may update the job data and b) avoid conflict with the rest thread
    j_instance.data_mutex_aquire(status, 'R')

    # test that JOB was not terminated
    if not j_instance.is_job_active() or j_instance.get_unique_job_id() != unique_job_id:
        # the job is completed because of timeout or error of the REST thread
        error_code = message_header.get_error(mem_view)
        if error_code and j_instance.get_unique_job_id() == unique_job_id:
            # place the error on the j_instance
            receiver_id =  message_header.get_receiver_id(mem_view)
            par_id = message_header.get_partition_id(mem_view)
            par_counter = message_header.get_partitions_used(mem_view)  # The number of partitions used with the Operator
            j_instance.set_par_object(receiver_id, par_counter)
            j_instance.set_par_ret_code(receiver_id, par_id, error_code)

        opetrator_err = j_handle.get_operator_error_txt()

        j_instance.data_mutex_release(status, 'R')  # release the mutex that prevents conflict with the rest thread

        if error_code != process_status.JOB_terminated_by_caller:
            # Not an error to be logged if the query was called to stop
            opetrator_err = ("[Operator code: %u] [%s] [%s]" % (error_code, process_status.get_status_text(error_code), opetrator_err))
            ret_val = process_status.JOB_not_active
            message = "Received message has no active job object - job[%u] ID: %u Source Error: %s" % (job_location, unique_job_id, opetrator_err)
            status.add_error(message)

        if is_debug_method("query"):
            utils_print.output("\r\n\nREPLY NOT PROCESSED\r\n", True)

        return ret_val

    code_status = 0
    message_type, receiver_id, par_counter, par_id, error_code = j_instance.process_message(mem_view)
    # j_instance.set_pass_through(False)  # Data from the partitions needs to be unified

    if error_code == process_status.Empty_data_set:
        j_instance.count_replies(receiver_id, par_id)  # This call needs to be done after setting the reply data
        code_status = 6  # Empty data set

    elif error_code:
        # error on the sender node which is not empty data set
        details = place_other_node_error(j_instance, job_location, unique_job_id, mem_view, message_type, error_code)
        j_handle.set_operator_error_txt(details)
        j_instance.count_replies(receiver_id, par_id)  # This call needs to be done after setting the reply data
        j_instance.set_par_ret_code(receiver_id, par_id, error_code)
        code_status = 1  # Error on the Operator node
    else:

        auth_str = message_header.get_auth_str_decoded(mem_view)    # Returns asymnetric key if encryption was enabled
        if auth_str:
            # Store the password and salt to decrypt the data
            ret_val = j_instance.set_decryption_passwords(status, receiver_id, par_id, auth_str)

        '''
        Use PROJECTION FUNCTIONS
        '''
        projection_functions = select_parsed.get_projection_functions()
        if ret_val:
            pass    # Failed to get the keys for decryption using set_decryption_passwords()
        elif projection_functions.is_with_functions():
            # these are functions like MIN, NAX, etc. which are supported natively by AnyLog - without a local database
            ret_val = process_projection_functions(status, io_buff_in, job_location, receiver_id, par_id,
                                                   projection_functions)
            code_status = 2  # projection functions
        elif j_instance.is_pass_through():
            '''
            Use PASS THROUGH
            '''
            # If pass through is True - no need in unified database - results from operators are transferred to client as is
            ret_val = pass_data(status, io_buff_in, job_location, receiver_id, par_id)
            code_status = 3  # pass Through
            if ret_val:
                message = "Pass through failed on query: " + j_instance.get_job_command()
                process_log.add("Error", message)
        else:

            if message_type == message_header.BLOCK_INFO_RESULT_SET:
                '''
                Use DBMS
                '''
                # --------------------------------------------------------------
                #  Insert rows to the local table
                # --------------------------------------------------------------
                code_status = 4  # Update Local table
                block_data_struct = message_header.get_block_struct(mem_view)

                ret_val = insert_query_rows(status, io_buff_in, job_location, receiver_id, par_id,
                                            block_data_struct)  # Update the local database with the insert rows
                if ret_val:
                    err_msg = status.get_saved_error()
                    if err_msg != "":
                        err_msg = "SQL Error Local Node: " + err_msg
                        if j_handle.is_rest_caller():
                            info_err = db_info.format_db_err_messages(err_msg, "json")
                            status.get_active_job_handle().set_result_set(info_err)
                        else:
                            info_err = db_info.format_db_err_messages(err_msg, "text")
                            echo_queue.add_msg(info_err)

            elif message_type == message_header.BLOCK_INFO_TABLE_STRUCT:
                '''
            Update METADATA
            '''
                # --------------------------------------------------------------
                #  Update info on the structure of a table
                # --------------------------------------------------------------
                code_status = 5  # Update metadata
                ret_val = set_table_structure(status, job_location, receiver_id, par_id)

            if not ret_val:
                if j_instance.is_last_block(receiver_id, par_id):
                    # the last block with the data was processed
                    j_instance.count_replies(receiver_id,
                                             par_id)  # This call needs to be done after setting the reply data

    # release the read mutex
    j_instance.data_mutex_release(status, 'R')

    if error_code or ret_val or j_instance.is_last_block(receiver_id, par_id) or code_status == 6:
        # In the case of an error, no data (code 6), or in the case of last message -> test if the JOB is completed
        # Use a WRITE lock to be an exclusive thread that end the job process
        ret_val = process_all_nodes_replied(status, error_code, ret_val, code_status, j_instance, j_handle, io_buff_in,
                                            job_location, unique_job_id)

    return ret_val

# =======================================================================================================================
# Provide the list of destination nodes to a query
# Satisfy the command: get query destination
# =======================================================================================================================
def get_query_dest(status, io_buff_in, cmd_words, trace):

    words_count = len(cmd_words)
    output_txt = ""
    if words_count > 3:
        ret_val = process_status.ERR_command_struct
    elif words_count == 3 and cmd_words[2] != "all" and not cmd_words[2].isdecimal():
        ret_val = process_status.ERR_command_struct
    else:
        ret_val = process_status.SUCCESS
        job_location = job_scheduler.get_recent_job()   # get the location of the last job
        y = job_location

        if words_count == 3:
            if cmd_words[2] == "all":  # the current is printed last
                y += 1                                      # start from the next
                counter = job_scheduler.JOB_INSTANCES  # go over all
            else:
                y = int(cmd_words[2])                  # Only the requested instance is printed
                counter = -1
        else:
            counter = 1  # go over one job

        query_dest_info = []

        jobs_counter = 0
        for x in range(job_scheduler.JOB_INSTANCES):
            # go over the dynamic jobs
            if y >= job_scheduler.JOB_INSTANCES:
                y = 0
            elif y < 0:
                y = job_scheduler.JOB_INSTANCES - 1     # backwards to find the last

            get_job = True
            if job_scheduler.get_job(y).get_start_time():

                if not job_scheduler.get_job(y).is_select():
                    get_job = False        # Ignore - not a query

                if get_job:
                    jobs_counter += 1
                    query_dest_info += job_scheduler.get_job(y).get_query_dest_info()     # A copy of the list of IPs and Ports

                    if counter == 1:
                        # Only one result is needed
                        break

            if words_count == 3 and cmd_words[2] == "all":
                y += 1  # go over the next job
            elif counter == -1:
                # Only the requested ID is needed
                break
            else:
                y -= 1  # goto the previous

        if not jobs_counter:
            output_txt = "Job instances do not contain queries info"
        else:
            title = ["Job", "Destination", "DBMS", "Table", "Command"]
            output_txt = utils_print.output_nested_lists(query_dest_info, "", title, True)

    return [ret_val, output_txt]


# =======================================================================================================================
# Satisfy the command - "query status"
# =======================================================================================================================
def get_query_state(status, io_buff_in, cmd_words, trace):

    if len(cmd_words) > 3:
        ret_val = process_status.ERR_command_struct
        reply = None
    elif len(cmd_words) == 3 and cmd_words[2] != "all" and not cmd_words[2].isdecimal():
        ret_val = process_status.ERR_command_struct
        reply = None
    else:
        ret_val, reply = get_active_queries(status, cmd_words, trace)

    return [ret_val, reply]

# =======================================================================================================================
# Process a job - organize replies or present status.
# reply from a different node on the network. For example: a reply to a query includes the result set
# =======================================================================================================================
def _process_job(status, io_buff_in, cmd_words, trace):
    words_count = len(cmd_words)

    if words_count < 2 or words_count > 3:
        return process_status.ERR_command_struct

    if words_count == 2 and cmd_words[1] == "message":  # place a job in the job container
        # --------------------------------------------------------------
        # Process a message from a PEER in the network
        # --------------------------------------------------------------

        ret_val = process_reply(status, io_buff_in)

    else:

        # --------------------------------------------------------------
        #  Process a command line - JOB command to determine job status
        # --------------------------------------------------------------
        if cmd_words[1] == "status":

            ret_val = job_state(status, cmd_words, trace)

        elif cmd_words[1] == "run":  # run a specific job

            ret_val = job_run(status, cmd_words)

        elif cmd_words[1] == "stop":  # stop a specific job or all jobs

            ret_val = job_stop(status, io_buff_in, cmd_words)

        elif words_count == 3 and cmd_words[1] == "active" and cmd_words[2] == "all":

            ret_val = list_active_jobs(status, cmd_words, trace)  # job active all

        else:
            ret_val = process_status.ERR_command_struct

    return ret_val


# =======================================================================================================================
# Process a script
# =======================================================================================================================
def _process_script(status, io_buff_in, cmd_words, trace):
    if len(cmd_words) < 2:
        return process_status.ERR_command_struct

    if cmd_words[1] == "message":  # a tcp sql message
        mem_view = memoryview(io_buff_in)
        command = message_header.get_command(mem_view)
        words_array = command.split()
    else:
        words_array = cmd_words

    if len(words_array) < 2:
        return process_status.ERR_command_struct

    if len(words_array) >= 19:
        # Read columns from table and execute commands in these tows
        return process_from_dbms(status, io_buff_in, words_array)

    words_count, file_name = concatenate_words(words_array, 1, False)  # get the file name to process
    if words_count == -1:  # not structured well
        status.add_error("File name in script not structured correctly: [%s]" % ' '.join(words_array[1:]))
        return process_status.ERR_command_struct  # return "Command error - usage: ..."

    offset = 2 + words_count * 2

    if len(cmd_words) > offset:
        # data to transfer
        data = words_array[offset:]
    else:
        data = None

    script_file_name = utils_data.remove_quotations(file_name)
    script_file_name = script_file_name.replace(params.get_reverse_separator(), params.get_path_separator())

    if len(script_file_name) > 4 and script_file_name[-5:] == ".json":
        # Execute a script in JSON format
        return exec_json(status, io_buff_in, script_file_name)

    return exec_script(status, io_buff_in, script_file_name, False, data)
# =======================================================================================================================
# Execute a script in JSON format
# Example autoexec script:
'''
{
	"config" : [
		{
			"name" : "Generic Variables",
			"description" : "Init params on start",
			"setting" : {
				"anylog_root_dir" : "C:",
				"node_name" : "<node name>",
                "company_name " : "<Company name>"
			},
          "commands" : [
            "hostname = get hostname"
          ]

		},

		{
            "name" : "IP / Port Variables",
			"description" : "Init params on start",
			"setting" : {
				"external_ip" : "<external_ip>",
                "master_node" : "<ip:port>",
                "sync_time" : "<30 seconds>"
            }
		}
    ]
}

'''
# =======================================================================================================================
def exec_json(status, io_buff_in, script_file_name):

    # Get script file from disk

    script_info = utils_io.read_to_string(status, script_file_name)
    if not script_info:
        return process_status.Missing_script

    ret_val = process_status.SUCCESS
    jscript = utils_json.str_to_json(script_info)

    if isinstance(jscript, dict):
        if "config" in jscript:
            config = jscript["config"]      # Get a list with vars and values and commands
            for section in config:
                # Assign variables and commands
                if "name" in section:
                    section_name = section["name"]
                else:
                    section_name = "Section name not defined"
                if "details" in section:
                    section_details = section["details"]
                else:
                    section_details = "Section details not defined"

                utils_print.output("\r\n\nConfigure section: %s (%s)" % (section_name, section_details), False)
                if "setting" in section:
                    setting = section["setting"]
                    for key, value in setting.items():
                        params.add_param(key, str(value))
                        utils_print.output("\r\n%s = %s" % (key, value), False)
                if "commands" in section:
                    commands = section["commands"]  # A list of commands
                    for one_command in commands:
                        reply_val = process_cmd(status, str(one_command), False, None, None, io_buff_in)
                        reply_txt = process_status.get_status_text(reply_val)
                        utils_print.output("\r\n[%s] [%s]" % (reply_txt, one_command), False)

            utils_print.output("\r\n\nEnd JSON script", True)
        else:
            status.add_error("Error in JSON script structure (Missing 'config' key): %s" % script_file_name)
            ret_val = process_status.ERR_wrong_json_structure

    else:
        status.add_error("Error in JSON script structure: %s" % script_file_name)
        ret_val = process_status.ERR_wrong_json_structure


    return ret_val

# =======================================================================================================================
# Execute commands contained in a database table
# EXAMPLE: process from table where name = my_config and dbms = config_dbms and value = al_value and command = al_command and condition = "order by command_id"
# =======================================================================================================================
def process_from_dbms(status, io_buff_in, cmd_words):

    if not utils_data.test_words(cmd_words, 1, ["from", "table", "where"]):
        return process_status.ERR_command_struct

    #                        Must     Add      Is
    #                       exists   Counter  Unique
    keywords = {
        "name": ("str", True, False, True),
        "dbms": ("str", True, False, True),
        "value": ("str", False, False, False),       # Value is an optional field + can have mutiple values
        "command": ("str", False, False, True),
        "condition": ("str", False, False, False),
    }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        return ret_val

    dbms_name = interpreter.get_one_value(conditions, "dbms")
    if not db_info.is_dbms_connected(status, dbms_name):
        return process_status.ERR_dbms_not_opened

    table_name = interpreter.get_one_value(conditions, "name")

    value_field = interpreter.get_one_value_or_default(conditions, "value", "")
    cmd_field = interpreter.get_one_value(conditions, "command")
    cond_field = interpreter.get_one_value_or_default(conditions, "condition", None)

    # the command to query the config table
    sql_cmd = "select %s" % cmd_field
    if value_field:
        sql_cmd += ", %s" % value_field

    sql_cmd += " from %s" % table_name
    if cond_field:
        sql_cmd += " %s" % cond_field

    ret_val, data_list = db_info.select_rows_list(status, dbms_name, sql_cmd, 0)
    if not ret_val:
        status.add_error("SQL query to config file with database %s failed: [%s]" % (dbms_name, sql_cmd))
        ret_val = process_status.ERR_SQL_failure
    elif not data_list:
        status.add_error("SQL query to config file with database %s returned without data: [%s]" % (dbms_name, sql_cmd))
        ret_val = process_status.Empty_result_set
    else:
        # Go over the returned data - every entry is a command
        ret_val = process_status.SUCCESS
        for entry in data_list:
            if entry[0]:
                # if command is not null
                al_cmd = str(entry[0])  # First value in the entry is the AnyLog Command
                for value in entry[1:]:
                    # ADd al values to command (from left to right)
                    index = al_cmd.find("<>")
                    if index != -1:
                        # Set value in command
                        str_value = str(value).strip()

                        complete_cmd = al_cmd.replace("<>", str_value, 1)
                    else:
                        complete_cmd = al_cmd

                reply_val = process_cmd(status, complete_cmd, False, None, None, io_buff_in)

                reply_txt = process_status.get_status_text(reply_val)
                utils_print.output("\r\n[%s] [%s]" % (reply_txt, complete_cmd), False)

        utils_print.output("\r\n\nEnd DBMS Configuration", True)

    return ret_val

# =======================================================================================================================
# Execute a script
# Variables are declared with the keyword variables followed by variable names in parenthesis, example:
# variables (var_name_1, var_name-2 ...)
# =======================================================================================================================
def exec_script(status, io_buff_in, file_name, is_event, values):
    global event_time
    stack = []

    thread_name = threading.current_thread().name
    debug_script = register_script(file_name)  # places the script name in a registry

    read_file = True  # file can be in RAM and then does not need to be read
    place_in_ram = False  # Events are maintained in RAM
    if is_event:
        # test if the script is in RAM
        if file_name in event_scripts.keys():
            # Measure the time elapsed from previous event. if less than 5 seconds, we do not read from disk
            # Otherwise, a user may change the script while system is running but the change will not be picked without restart
            current_time = int(time.time())
            if (current_time - event_time) < 5:
                commands_list = event_scripts[file_name]
                read_file = False  # File retrieved from RAM
            event_time = current_time
        else:
            place_in_ram = True  # The first read of the event will place in RAM

    if read_file:
        # Get script file from disk
        io_handle = utils_io.IoHandle()
        if not io_handle.open_file("get", file_name):
            utils_print.output_box("Failed to read script: '%s'" % file_name)
            status.add_error("Script not available: " + file_name)
            unregister_script(file_name)
            return process_status.Missing_script

        commands_list = io_handle.read_lines()

        io_handle.close_file()
        if io_handle.is_with_error():
            unregister_script(file_name)
            return process_status.ERR_process_failure

        if place_in_ram:
            event_scripts[file_name] = commands_list  # next time will be taken from RAM



    # Store the if status as it may change inside the called process
    if_status = status.get_if_result()

    # ----------------------------------------------------------------
    # Step 1: preprocess - find all identified locations used in goto command
    # ----------------------------------------------------------------

    script_f_name = {}  # dictionary maintaining location as f(goto name)
    script_f_line = {}  # dictionary maintaining location as f(line number)
    line_number = 0
    start_line = 0
    assigned = False

    for command in commands_list:
        if values and not assigned:
            # assign the variables values to each variable
            ret_val, assigned, = assign_values_to_vars(status, file_name, command, values)
            if ret_val:
                unregister_script(file_name)
                status.set_if_result(if_status)     # Revert to the original if state
                return ret_val
            if assigned:
                start_line = line_number + 1  # start processing data from the first line after the assignment
        elif not values:
            test_cmd = command.lstrip()
            if test_cmd.startswith("variables") and (test_cmd[9] == '(' or test_cmd[9] == ' '):
                # variables not provided:
                status.add_error(
                    "Script requires variables in line %u which are not provided: %s" % (line_number + 1, file_name))
                unregister_script(file_name)
                status.set_if_result(if_status)  # Revert to the original if state
                return process_status.ERR_process_failure

        if command[0] == ':':
            # name place
            index = command[1:].find(':')
            if index > 0:
                goto_name = command[1:index + 1]
                if goto_name in script_f_name:
                    status.add_error(
                        "Script %s is using the same location name (%s) multiple times" % (file_name, goto_name))
                    unregister_script(file_name)
                    status.set_if_result(if_status)  # Revert to the original if state
                    return process_status.ERR_process_failure

                script_f_name[goto_name] = line_number
                script_f_line[line_number] = goto_name
        line_number += 1

    ret_val = process_status.SUCCESS
    left_bracket = 0
    right_bracket = 0
    json_command = []
    json_data = ""

    line_number = start_line
    total_lines = len(commands_list)

    location_name = ""  # the name of the location executed
    on_error = ["", ""]  # The name of the command (goto or call) + the name of the location
    # ----------------------------------------------------------------
    # Step 2: process commands
    # ----------------------------------------------------------------
    join_lines = False
    while line_number < total_lines:

        if line_number in script_f_line.keys():
            # this is a named line - :name: - used as a reference in the goto command
            line_number += 1
            continue  # named line is skipped

        command = commands_list[line_number].strip()

        if not command or command[0] == '#' or command[0] == '\n' or command[0] == ':':
            line_number += 1
            continue  # comment, new line, goto name - are skipped before further processing

        if join_lines:
            command_joined += command.rstrip(" \r\n\t")
            if command_joined[-1] != '>':
                command_joined += ' '       # Replace new line with a single space
                line_number += 1
                continue
            join_lines = False
            command = command_joined[:-1]   # ignore the < sign
        elif command[0] == '<':       # join multiple lines
            join_lines = True
            line_number += 1
            command_joined = command.rstrip(" \r\n\t")[1:]  # ignore the < sign
            command_joined += ' '  # Replace new line with a single space
            continue

        # remove comments and group & commands
        sub_str, left_bracket, right_bracket = utils_data.cmd_line_to_list_with_json(status, command, left_bracket,
                                                                                     right_bracket, True)
        if not sub_str:
            ret_val = process_status.ERR_command_struct
            break

        if left_bracket != right_bracket:  # JSON info
            json_data += sub_str[-1]  # add more of the JSON data
            if len(json_command) == 0:
                # save the command (without the JSON part) to operate with the JSON (like json_data = {...})
                json_command = sub_str[:-1]
            line_number += 1
            continue  # get more JSON data until brackets match
        left_bracket = 0
        right_bracket = 0

        if len(json_data):  # Push the JSON
            json_data += sub_str[0]  # add last part of JSON
            command = utils_data.get_str_from_array(json_command, 0, 0)
            command += json_data
            json_data = ""
            json_command = []

        words_count = len(sub_str)
        if not words_count:
            line_number += 1
            continue  # empty line

        if command[0] == ' ':
            index = 1  # remove prefix space of each command when '&' is used
        else:
            index = 0


        ret_val = process_cmd(status, command[index:], False, None, None, io_buff_in)

        if debug_script:
            if debug_script.is_stop():
                break  # flagged by the user to stop
            if debug_script.is_debug():
                print_command(thread_name, line_number + 1, command[index:], ret_val)  # Debug - Print the command
                if debug_script.is_debug_interactive():
                    # wait for next command
                    while not debug_script.is_debug_next():
                        time.sleep(3)  # sleep and retry
                        if process_status.is_exit("scripts"):
                            break
                        if not debug_script.is_debug_interactive():
                            break  # may have been changed by user setting: set debug on or off
                    debug_script.set_debug_next(False)

        if process_status.is_exit("scripts"):
            break

        if not ret_val:
            line_number += 1
            continue  # if executed successfully --> no need to continue, can take the next command

        code_jump = ""  # will be set in a name value if shifting to a new location in the code

        if ret_val > process_status.NON_ERROR_RET_VALUE:
            # controll commands

            if ret_val == process_status.GOTO:
                # A GOTO command
                location_name = status.get_goto_name()
                code_jump = location_name  # Goto this location
            elif ret_val == process_status.ON_ERROR_GOTO:
                on_error[0] = "goto"
                on_error[1] = status.get_goto_name()  # goto this part of the code with an error
            elif ret_val == process_status.ON_ERROR_CALL:
                on_error[0] = "call"
                on_error[1] = status.get_goto_name()  # goto this part of the code with an error
            elif ret_val == process_status.ON_ERROR_IGNORE:
                on_error[0] = "ignore"
            elif ret_val == process_status.ON_ERROR_END_SCRIPT:
                on_error[0] = "end"
            elif ret_val == process_status.END_SCRIPT:
                ret_val = process_status.SUCCESS
                break  # end the current script
            elif ret_val == process_status.EXIT_SCRIPTS:
                break   # End the current and caller scripts
            elif ret_val == process_status.CALL:
                location_name = status.get_goto_name()
                stack.append(line_number)
                code_jump = location_name  # Goto this location
            elif ret_val == process_status.RETURN:
                if len(stack):
                    # Get the "call" line number
                    line_number = stack.pop()
                else:
                    ret_val = process_status.SUCCESS
                    break  # end the script
        elif ret_val != process_status.SUCCESS:
            if ret_val == process_status.ERR_unrecognized_command or \
                    ret_val == process_status.ERR_command_struct or \
                    ret_val == process_status.Missing_script:
                if on_error[0] == "ignore":
                    ret_val = 0
                else:
                    break  # exit in case of an error in commands
            # In case of an ERROR -> Goto the part of the code to manage errors
            # Testing on_error_name != location_name --> avoid recursive call
            if on_error[0] == "goto" and on_error[1] != location_name:
                code_jump = on_error[1]
            elif on_error[0] == "call" and on_error[1] != location_name:
                stack.append(line_number)
                code_jump = on_error[1]
            elif on_error[0] == "ignore":
                ret_val = process_status.SUCCESS        # Ignore the error
                pass
            elif on_error[0] == "end":
                break
            else:
                # there is no logic in the code for the error encountered
                error_text = "Script terminated on line %u with error: '%s'" % (
                line_number, process_status.get_status_text(ret_val))
                status.add_error(error_text)
                ret_val = process_status.Uncovered_err
                break

        if code_jump:  # move to a different location in the code
            line_number = get_goto_line(status, script_f_name, code_jump)
            if line_number == -1:
                # goto name is not in script
                ret_val = process_status.ERR_command_struct
                break
            line_number += 1  # First line after the goto name
            continue  # use new line

        line_number += 1

    if ret_val and ret_val < process_status.NON_ERROR_RET_VALUE:
        if command and len(command):
            if command[0] == ' ':
                index = 1  # remove prefix space of each command when '&' is used
            else:
                index = 0
            command_info = get_command_trace(line_number + 1, command[index:], ret_val)
            err_msg = "Script terminated: Error: '%s' in file %s" % (command_info, file_name)
        else:
            err_msg = "Script in file %s failed to be processed" % (file_name)
        status.add_error(err_msg)
        utils_print.output(err_msg, True)

    unregister_script(file_name)

    # Revert to the if status before the called process
    status.set_if_result(if_status)

    return ret_val

# =======================================================================================================================
# Events trigger the processing of scripts
# Example: event file_processed 78eba9a0938d5f36b0a41135bf55b0e2.uploaded
# =======================================================================================================================
def _event_trigger(status, io_buff_in, cmd_words, trace):
    global coded_events

    if len(cmd_words) < 2:
        return process_status.ERR_command_struct

    if cmd_words[1] == "message":  # a tcp sql message
        is_message = True
        mem_view = memoryview(io_buff_in)
        command = message_header.get_command(mem_view)
        words_array = utils_data.split_quotations(command)  # Split considers quotations as one word
    else:
        is_message = False
        words_array = cmd_words

    event_name = words_array[1]
    script_file = params.get_param(event_name)  # get the script name assigned to the event

    if len(words_array) > 2:
        # data to transfer
        data = words_array[2:]
    else:
        data = None

    if script_file:
        # A script with this name has preference over coded scripts
        return exec_script(status, io_buff_in, script_file, True, data)

    if event_name in coded_events.keys():
        return coded_events[event_name](status, io_buff_in, data, trace)

    status.add_error("Missing script for event: %s" % event_name)
    return process_status.Missing_event_script


# =======================================================================================================================
# Process a script with a new thread
# =======================================================================================================================
def _new_thread(status, io_buff_in, cmd_words, trace):
    # new thread for repeat commands
    thread_status = process_status.ProcessStat()
    t = threading.Thread(target=_process_script, args=(thread_status, io_buff_in, cmd_words, trace))
    t.start()

    return process_status.SUCCESS
# =======================================================================================================================
# Get executable command from words. The words can be organized as a string or within the message header
# =======================================================================================================================
def get_executable_command(status, command_in, mem_view: memoryview):
    if command_in:
        words_split = command_in.split()  # break to individual words if multiple words are grouped
        offset = 1
        command = words_split[0]
    else:
        command = message_header.get_word_from_array(mem_view, 0)  # get first word

    while 1:

        ret_val = words_for_existing_command(status, command)
        if ret_val == 0:
            break  # command is recognized
        elif ret_val == -1:
            command = ""  # wrong command
            break
        if command_in:
            next_word = words_split[offset]
            offset += 1
        else:
            next_word = message_header.get_word_from_array(mem_view, len(command) + 1)  # get next word
        if not next_word:
            command = ""  # wrong command
            break
        command += (" " + next_word)

    return command

# =======================================================================================================================
# Get data distribution - similar to get rows count but on all operator nodes in the network that support the dbms and table
# Example: get data distribution where dbms = litsanleandro and table = ping_sensor
# =======================================================================================================================
def get_data_distribution(status, io_buff_in, cmd_words, trace):

    keywords = {"dbms": ("str", True, False, False),
                "table": ("str", True, False, False),
                }


    if cmd_words[3] != "where":
        status.add_error("missing 'where' keyword in 'get rows count' command")
        ret_val = process_status.ERR_command_struct
        return [ret_val, None]


    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        return [ret_val, None]

    dbms_name = interpreter.get_one_value(conditions, "dbms")
    table_name = interpreter.get_one_value(conditions, "table")

    # The query for all nodes
    al_cmd = f"get rows count where dbms = {dbms_name} and table = {table_name} and group = table and format = json".split()

    aggregate_table = utils_print.PrintTable("")
    aggregate_table.add_title("addr", "Address")
    aggregate_table.add_title("type", "Node Type")
    aggregate_table.add_title("name", "Node Name")
    aggregate_table.add_title("rows", "Rows")   # Depending on the command issued
    aggregate_table.add_title("percent", "Percent")  # Depending on the command issued

    # nodes_list = metadata.get_all_operators_by_keys(status, "Rows", table_name, True)
    dest_keys = [
        f"dbms={dbms_name}",
        f"table={table_name}",
    ]
    ret_val, nodes_list = metadata.get_operators(status, dest_keys, None, False, 0, True)
    total = 0
    if not ret_val and len(nodes_list):
        message = ["node_x", "=", "run", "client", "(destination)"] + al_cmd
        for entry in nodes_list:
            ip = entry["ip"]
            port = entry["port"]

            ip_port = f"{ip}:{port}"
            aggregate_table.add_entry("addr", ip_port, ip_port)  # The first column with the ip and port

            aggregate_table.add_entry("type", ip_port, "operator")  # The policy type defined by the user

            if "name" in entry:
                aggregate_table.add_entry("name", ip_port, entry["name"])  # The policy name


            message[4] = ip_port        # Destination
            node_key = f"node_{ip_port}"
            message[0] = node_key
            params.del_param(node_key)  # Clear old value
            if ip and port:
                run_client(status, io_buff_in, message, trace)  # Ask for the node status

        aggregate_table.add_entry("addr", "total", "Total")  # The Last column with the total rows and percentage

        time.sleep(3)  # Wait for the reply


        for index, entry in enumerate(nodes_list):
            ip = entry["ip"]
            port = entry["port"]
            if ip and port:
                ip_port = f"{ip}:{port}"
                node_key = f"!node_{ip_port}"
                node_val = params.get_value_if_available(node_key)
                if node_val:
                    # Remove the [From Node 10.0.0.78:7848] prefix
                    index = node_val.find(']', 1)
                    if index > 0 and index < (len(node_val) - 2):
                        info_returned = node_val[index + 2:]
                    else:
                        info_returned = node_val


                    # Pull the number of rows from the string
                    offset = info_returned.rfind(":")
                    if offset != -1:
                        try:
                            rows_count = int(info_returned[offset+1:-2])
                        except:
                            rows_count = 0      # not available
                        else:
                            total += rows_count
                            aggregate_table.add_entry("rows", ip_port, rows_count)  # Node replied with: get metadata version

    aggregate_table.add_entry("rows", "total", format(total, ","))  # Node replied with: get metadata version
    aggregate_table.add_entry("percent", "total", 100)  # Node replied with: get metadata version

    if not ret_val:
        total_percent = 0
        for index, entry in enumerate(nodes_list):
            ip = entry["ip"]
            port = entry["port"]
            if ip and port:
                ip_port = f"{ip}:{port}"
                # Update the percentage
                try:
                    value = aggregate_table.get_entry("percent", ip_port)
                    percent = int((value / total) * 100)
                    aggregate_table.add_entry("rows", ip_port, format(value, ","))  # Replace with comma separated value
                except:
                    pass
                else:
                    if index == (len(nodes_list) - 1):
                        # Last node in the list
                        aggregate_table.add_entry("percent", ip_port, 100-total_percent)
                    else:
                        total_percent += percent
                        aggregate_table.add_entry("percent", ip_port, percent)


        reply = aggregate_table.output("", True, "")
    else:
        reply = ""

    return [ret_val, reply]

# =======================================================================================================================
# A generic structure to query multiple network nodes and provide an aggregated reply
# For example:
#           test network - test network status on all nodes
#           test network metadata version - test metadata version on all nodes
#           test network table - test table definition on all nodes
# =======================================================================================================================
def network_get_info(status, reply_format, io_buff_in, al_cmd, cmd_name, nodes_types, target_node, dbms_name, table_name, trace):
    '''
    reply_format - "table" or "json"
    al_cmd - the command to issue like: ["get", "status"] or ["get", "metadata", "version"] or ["test", "table" ...]
    cmd_name - the name of the command like: "Status" or "Metadata Version" or "Table Test"
    nodes_types - A string with the list of policies to consider, like "master,operator,query"
    dbms_name - identifies nodes that service the database
    table_name - identifies nodes that service the database
    '''

    if reply_format == "json":
        aggregate_dict = {}
    else:
        aggregate_table = utils_print.PrintTable("")
        aggregate_table.add_title("addr", "Address")
        aggregate_table.add_title("type", "Node Type")
        aggregate_table.add_title("name", "Node Name")
        aggregate_table.add_title("status", cmd_name)   # Depending on the command issued


    if dbms_name:
        # get operators that service the database and table
        nodes_list = metadata.get_all_operators_by_keys(status, dbms_name, table_name, True)
        ret_val = process_status.SUCCESS
    else:
        if target_node:
            # test with the specific node
            index = target_node.find(":")
            if index > 0 and index < (len(target_node)-1):
                ip = target_node[:index]
                port = target_node[index + 1:]
                nodes_list = [
                    {
                        "policy" : "node",
                        "ip" : ip,
                        "port" : port,
                        "local_ip": ip,
                        "name" : "node"
                    }
                ]
                ret_val = process_status.SUCCESS
            else:
                ret_val = process_status.ERR_dest_IP_Ports
        else:
            # Get all nodes
            cmd =  f"blockchain get ({nodes_types}) bring.json [] [*][ip] : [*][port] [*][local_ip] : [*][local_port] [*][name]"
            ret_val, reply_info = blockchain_get(status, cmd.split(), None, True)
            if not ret_val:
                nodes_list = utils_json.str_to_json(reply_info)

    if not ret_val:
        if not nodes_list:
            ret_val = process_status.No_network_peers
        else:
            message = ["node_x", "=", "run", "client", "(destination)"] + al_cmd
            print_length = 0
            for entry in nodes_list:
                ip, port = net_utils.get_dest_ip_port(entry)  # Determine which IP and Port to use
                if ip and port:
                    ip_port = f"{ip}:{port}"
                    if reply_format == "json":
                        aggregate_dict[ip_port] = {}
                        aggregate_dict[ip_port]["address"] = ip_port

                        if "policy" in entry:
                            aggregate_dict[ip_port]["type"] = entry["policy"]  # The policy type
                        else:
                            aggregate_dict[ip_port]["type"] = nodes_types  # The policy type defined by the user

                        if "name" in entry:
                            aggregate_dict[ip_port]["name"] =  entry["name"]  # The policy name
                        elif "table" in entry:
                            aggregate_dict[ip_port]["name"] =  entry["table"]  # The table name if specified
                    else:
                        aggregate_table.add_entry("addr", ip_port, ip_port)  # The first column with the ip and port

                        if "policy" in entry:
                            aggregate_table.add_entry("type", ip_port, entry["policy"])  # The policy type
                        else:
                            aggregate_table.add_entry("type", ip_port, nodes_types)  # The policy type defined by the user

                        if "name" in entry:
                            aggregate_table.add_entry("name", ip_port, entry["name"])  # The policy name
                        elif "table" in entry:
                            aggregate_table.add_entry("name", ip_port, entry["table"])  # The table name if specified

                    message[4] = ip_port        # Destination
                    node_key = f"node_{ip_port}"
                    message[0] = node_key
                    params.del_param(node_key)  # Clear old value

                    if not status.get_active_job_handle().is_rest_caller():
                        policy_type = (" " + entry["policy"] ) if "policy" in entry else ""
                        node_name = (" (" + entry["name"] + ")") if "name" in entry else ""
                        print_msg = f"\rTest connection with{policy_type}{node_name}: {ip_port} ..."
                        print_msg_len = len(print_msg)
                        if print_msg_len >= print_length:
                            print_length = print_msg_len
                        else:
                            print_msg = print_msg.ljust(print_length)
                        utils_print.output(print_msg, False)
                    run_client(status, io_buff_in, message, trace)  # Ask for the node status

            # SLEEP - Wait for replies
            if status.get_active_job_handle().is_rest_caller():
                time.sleep(4) # Wait for the reply
            else:
                utils_print.output("\r" + ' ' * print_length, False)    # Delete previous printout
                utils_print.time_star_print("Test Network", 4, 8, 64)

            # Organize output
            for index, entry in enumerate(nodes_list):
                ip, port = net_utils.get_dest_ip_port(entry)  # Determine which IP and Port to use
                if ip and port:
                    ip_port = f"{ip}:{port}"
                    node_key = f"!node_{ip_port}"
                    node_val = params.get_value_if_available(node_key)
                    if node_val and node_val[-14:] != "Not responding":
                        if cmd_name == "Status":
                            if reply_format == "json":
                                aggregate_dict[ip_port]["status"] = '+'
                            else:
                                aggregate_table.add_entry("status", ip_port, "  +")  # Node replied
                        else:
                            # Remove the [From Node 10.0.0.78:7848] prefix
                            index = node_val.find(']', 1)
                            if index > 0 and index < (len(node_val) - 2):
                                info_returned = node_val[index + 2:]
                            else:
                                info_returned = node_val

                            if reply_format == "json":
                                aggregate_dict[ip_port]["status"] = info_returned
                            else:
                                aggregate_table.add_entry("status", ip_port, info_returned)  # Node replied with: get metadata version

    if not ret_val:
        if reply_format == "json":
            jsons_list = list(aggregate_dict.values())    # The IP and port is an item with a title
            reply = utils_json.to_string(jsons_list)
        else:
            reply = aggregate_table.output("", True, "")
    else:
        reply = ""

    return [ret_val, reply]

# =======================================================================================================================
# Test the network by issuing get status to all nodes in the network
# test network
# test network metadata version
# test network table ping_sensor where dbms = lsl_demo
# test network with master
# test network with !ip_port
# =======================================================================================================================
def test_network(status, io_buff_in, cmd_words, trace):

    words_count = len(cmd_words)
    dbms_name = ""
    table_name = ""
    target_node = ""

    if words_count >= 6 and utils_data.test_words(cmd_words, words_count - 3, ["format", "=", "json"]):
        # Support test node if reply is in JSON
        if cmd_words[words_count-4] != "where" and cmd_words[words_count-4] != "and":
            return [process_status.ERR_command_struct, ""]
        reply_format = "json"
        words_count -= 4
    else:
        reply_format = "table"


    if words_count == 2:
        al_cmd = ["get", "status"]      # Issue get status to all nodes
        cmd_name = "Status"
        nodes_types = "master,operator,query,publisher"
    elif words_count == 4 and cmd_words[2] == "metadata" and cmd_words[3] == "version":
        # The command is: test network metadata
        al_cmd = ["get", "metadata", "version"]  # Issue get to all nodes
        cmd_name = "Metadata Version"
        nodes_types = "master,operator,query,publisher"
    elif words_count == 8 and cmd_words[2] == "table" and utils_data.test_words(cmd_words, 4, ["where", "dbms", "="]):
        # test network table ping_sensor where dbms = lsl_demo
        al_cmd = ["test"] + cmd_words[2:]
        cmd_name = "Table Test"
        nodes_types = "operator"

        table_name = cmd_words[3]
        dbms_name = cmd_words[7]
    elif words_count == 4 and cmd_words[2] == "with":
        # Test network with master
        # test network with operator
        # test network with !node_ip_port
        al_cmd = ["get", "status"]  # Issue get status to all nodes
        cmd_name = "Status"
        target_node = cmd_words[3]
        if target_node == "master" or target_node == "operator" or target_node == "query" or target_node == "publisher":
            nodes_types = target_node
            target_node = ""
        else:
            nodes_types = "node"
            target_node = params.get_value_if_available(target_node)    # Get a specific node
            if not target_node:
                return [process_status.ERR_command_struct, ""]

    else:
        return [process_status.ERR_command_struct, ""]


    ret_val, reply = network_get_info(status, reply_format, io_buff_in, al_cmd, cmd_name, nodes_types, target_node, dbms_name, table_name, trace)
    return [ret_val, reply]

# =======================================================================================================================
# Compare the databases defined on each member of the cluster
# Example: test cluster databases
# =======================================================================================================================
def test_cluster_databases(status, io_buff_in, cmd_words, trace):


    cluster_id = metadata.get_cluster_id()  # The cluster to test
    if cluster_id == '0':
        return [process_status.Missing_cluster_info, ""]

    operators_list = metadata.get_operators_info(status, cluster_id, True, ["ip", "port", "member", "operator_status"])

    message = ["node_x", "=", "run", "client", "(destination)", "get", "databases", "where", "format", "=", "json"]

    aggregate_table = utils_print.PrintTable("")
    aggregate_table.add_title("dbms", "DBMS")

    for operator in operators_list:
        ip_port = f"{operator[0]}:{operator[1]}"
        aggregate_table.add_title(operator[2], f"Node_{operator[2]}\n{ip_port}")  # Add the node member ID

        node_key = f"node_{operator[2]}"
        params.del_param(node_key)  # Clear old value
        message[0] = node_key  # Set a key in the dictionary - node_member_id
        message[4] = ip_port  # IP - Port
        run_client(status, io_buff_in, message, trace)  # Get the values from the peer node

    time.sleep(3)  # Wait for the reply

    for index, operator in enumerate(operators_list):
        node_key = f"!node_{operator[2]}"
        node_reply = params.get_value_if_available(node_key)
        start_offset = node_reply.find("{")
        end_offset = node_reply.rfind("}")
        if start_offset != -1 and end_offset != -1:
            # Set the peer reply as a dictionary
            peer_reply = utils_json.str_to_json(
                node_reply[start_offset:end_offset + 1])  # The reply of the node as a dictionary
            if peer_reply and len(peer_reply) == 1:
                # Info is organized in a dictionary and the root key is the IP and port
                node_databases = utils_json.get_inner(peer_reply)
                if node_databases and isinstance(node_databases, dict):
                    for dbms_name in node_databases:
                        aggregate_table.add_entry("dbms", dbms_name, dbms_name)     # The first column with the dbms name
                        aggregate_table.add_entry(operator[2], dbms_name, "    +")    # The value as f( node id and dbms name )

    reply = aggregate_table.output("", True, "")

    return [process_status.SUCCESS, reply]

# =======================================================================================================================
# Compare the partitions defined on each member of the cluster
# Example: test cluster partitions
# =======================================================================================================================
def test_cluster_partitions(status, io_buff_in, cmd_words, trace):


    cluster_id = metadata.get_cluster_id()  # The cluster to test
    if cluster_id == '0':
        return [process_status.Missing_cluster_info, ""]

    operators_list = metadata.get_operators_info(status, cluster_id, True, ["ip", "port", "member", "operator_status"])

    message = ["node_x", "=", "run", "client", "(destination)", "get", "partitions", "where", "format", "=", "json"]

    aggregate_table = utils_print.PrintTable("")
    aggregate_table.add_title("partition", "Partition")

    for operator in operators_list:
        ip_port = f"{operator[0]}:{operator[1]}"
        aggregate_table.add_title(operator[2], f"Node_{operator[2]}\n{ip_port}")  # Add the node member ID

        node_key = f"node_{operator[2]}"
        params.del_param(node_key)  # Clear old value
        message[0] = node_key  # Set a key in the dictionary - node_member_id
        message[4] = ip_port  # IP - Port
        run_client(status, io_buff_in, message, trace)  # Get the values from the peer node

    time.sleep(3)  # Wait for the reply

    for index, operator in enumerate(operators_list):
        node_key = f"!node_{operator[2]}"
        node_reply = params.get_value_if_available(node_key)
        start_offset = node_reply.find("{")
        end_offset = node_reply.rfind("}")
        if start_offset != -1 and end_offset != -1:
            # Set the peer reply as a dictionary
            peer_reply = utils_json.str_to_json(
                node_reply[start_offset:end_offset + 1])  # The reply of the node as a dictionary
            if peer_reply and len(peer_reply) == 1:
                # Info is organized in a dictionary and the root key is the IP and port
                node_partitions = utils_json.get_inner(peer_reply)
                if node_partitions and isinstance(node_partitions, dict):
                    for dbms_table_name, partition_info in node_partitions.items():
                        partition_details = dbms_table_name + ' ' + str(partition_info)
                        aggregate_table.add_entry("partition", partition_details, partition_details)     # The first column with the partition name and info
                        aggregate_table.add_entry(operator[2], partition_details, "    +")    # The value as f( node id and dbms name )

    reply = aggregate_table.output("", True, "")

    return [process_status.SUCCESS, reply]

# =======================================================================================================================
# Compare the tsd tables from all the nodes that share the cluster
# Example: test cluster data
# Example: test cluster data where start_date = -3d
# =======================================================================================================================
def test_cluster_data(status, io_buff_in, cmd_words, trace):

    if len(cmd_words) > 6:
        if  cmd_words[3] == "where":

            keywords = {
                        "start_date": ("date", False, False, True),
                        "end_date": ("date", False, False, True),
                        }

            ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords,
                                                                           False)
            if ret_val:
                return [ret_val, None]

            where_cond = cmd_words[4:]   # i.e. where start_date = -3d

        else:
            return [process_status.ERR_command_struct]
    else:
        where_cond = ""

    cluster_id = metadata.get_cluster_id()      # The cluster to test
    if cluster_id == '0':
        return [process_status.Missing_cluster_info, ""]

    operators_list = metadata.get_operators_info(status, cluster_id, True, ["ip", "port", "member", "operator_status"])

    message = ["node_x", "=", "run", "client", "(destination)", "get", "tsd", "summary", "where", "table", "=", "*", "and", "format", "=", "json"]
    if where_cond:
        message += ["and"] + where_cond

    aggregate_table = utils_print.PrintTable(0)

    aggregate_table.add_title("table", "Table")

    for operator in operators_list:
        ip_port = f"{operator[0]}:{operator[1]}"
        aggregate_table.add_title(operator[2], f"Node_{operator[2]}\n{ip_port}")    # Add the node member ID

        node_key = f"node_{operator[2]}"
        params.del_param(node_key)  # Clear old value
        message[0] = node_key  # Set a key in the dictionary - node_member_id
        message[4] = ip_port  # IP - Port
        run_client(status, io_buff_in, message, trace)      # Get the values from the peer node

    time.sleep(3)  # Wait for the reply

    for index, operator in enumerate(operators_list):
        node_key = f"!node_{operator[2]}"
        node_reply = params.get_value_if_available(node_key)
        start_offset = node_reply.find("{")
        end_offset = node_reply.rfind("}")
        if start_offset != -1 and end_offset != -1:
            # Set the peer reply as a dictionary
            peer_reply = utils_json.str_to_json(node_reply[start_offset:end_offset+1])        # The reply of the node as a dictionary
            if peer_reply:
                for table_name, info in peer_reply.items():
                    aggregate_table.add_entry("table", table_name, table_name)      # The first column with the table name
                    files = info["files"]
                    rows = info["rows"]
                    value = f"{files}/{rows}"
                    aggregate_table.add_entry(operator[2], table_name, value)       # The value as f(table name and node id)

    reply = "\n\rTSD Summary on all nodes in the cluster (files/rows)\n\r" + aggregate_table.output("", True, "")

    return [process_status.SUCCESS, reply]
# =======================================================================================================================
# Update a table that organizes the reply from every cluster member by table
# =======================================================================================================================
def update_aggregate_table(aggregate_table, counter_operators, peer_counter, table_name, files, rows ):
    '''
    aggregate_table - a nested table that shows the summary of files and rows for each TSD table (for each node in the cluster)
    counter_operators -  Number of operators supporting the cluster
    peer_counter - a counter for each peer member
    table_name - the user table that is represented in the tsd table
    files - the total number of files per the table (represented in the TSD tables on a cluster node)
    rows - the total number of rows per the table (represented in the TSD tables on a cluster node)
    '''

    # Find if the table is represnted in the aggregate_table
    table_offset = -1
    for index, entry in enumerate(aggregate_table):
        if entry[0] == table_name:
            table_offset = index
            break
    if table_offset == -1:
        # First time info on the table is provided
        table_offset = len(aggregate_table)
        aggregate_table.append(["" for _ in range(counter_operators + 1)])
        aggregate_table[table_offset][0] = table_name

    aggregate_table[table_offset][peer_counter + 1] = f"{files}/{rows}"


# =======================================================================================================================
# Test if the Operator node is configured to support HA
# Command: test cluster setup
# =======================================================================================================================
def test_cluster_setup(status, io_buff_in, cmd_words, trace):

    info_table = []
    if aloperator.is_running:
        operator_stat = ("Running: distributor flag enabled" if aloperator.is_with_distributor() else "Running: distributor flag disabled")      # Operator call need to include: distributor = true
    else:
        operator_stat = "Disabled"

    info_table.append(("Operator", operator_stat))
    info_table.append(("Distributor", "Running" if version.aldistributor_is_distr_running() else "Disabled"))
    info_table.append(("Consumer", "Running" if version.alconsumer_is_consumer_running() else "Disabled"))


    info_table.append(("Operator Name", metadata.get_node_operator_name()))

    member_id = metadata.get_node_member_id()
    if not member_id:
        "Not a member"
    info_table.append(("Member ID", member_id))

    info_table.append(("Cluster ID", metadata.get_cluster_id()))

    # Test TSD Info setup - Test if the tsd_info table is defined
    is_tsd_info = db_info.is_table_exists(status, "almgm", "tsd_info")

    tsd_status = "Defined" if is_tsd_info else "Not Defined"
    info_table.append(("almgm.tsd_info", tsd_status))

    reply = utils_print.output_nested_lists(info_table, "", ["Functionality", "Details"], True)

    return [process_status.SUCCESS, reply]
# =======================================================================================================================
# Test current node
# Test for the following:
# 1) Blockchain version
# 2) TCP - local nad remote
# 3) REST
# =======================================================================================================================
def test_node(status, io_buff_in, cmd_words, trace):

    status_list = []

    ret_val, dummy = blockchain_load_metadata(status, io_buff_in, cmd_words, trace, None)

    if ret_val:
        metadata_test = f"Failed to load metadata - {process_status.get_status_text(ret_val)}"
    else:

        metadata_version = bsync.blockchain_stat.get_hash_value()        # Metadata version - get the blockchain version

        status_list.append(("Metadata Version", metadata_version))

        command = "btest = blockchain test"     # Test metadata struvtured correctly
        ret_val = process_cmd(status, command, False, None, None, io_buff_in)
        btest = params.get_value_if_available("!btest")
        if ret_val or btest != "true":
            metadata_test = "Failed"
        else:
            metadata_test = "Pass"

    status_list.append(("Metadata Test", metadata_test))

    # TEST TCP server

    # Find the TCP IP and Port to use
    dest_obj = {}
    ip = net_utils.get_external_ip()
    port = net_utils.get_external_port()
    if ip and port:
        dest_obj["ip"] = ip
        dest_obj["port"] = port

    local_ip = net_utils.get_local_ip()
    local_port = net_utils.get_local_port()
    if local_ip and local_port:
        dest_obj["local_ip"] = local_ip
        dest_obj["local_port"] = local_port

    dest_ip, dest_port = net_utils.get_dest_ip_port(dest_obj)  # Determine which IP and Port to use


    if not dest_ip or not dest_port:
        tcp_test = "No TCP IP and Port in use"
    else:
        command = f"tcp_node_test = run client {dest_ip}:{dest_port} get status"
        ret_val = process_cmd(status, command, False, None, None, io_buff_in)
        if ret_val:
            tcp_test = f"Failed to send message using: {dest_ip}:{dest_port} with error: {process_status.get_status_text(ret_val)}"
        else:

            if status.get_active_job_handle().is_rest_caller():
                time.sleep(1)
            else:
                utils_print.time_star_print("Test TCP", 1, 4, 60)

            tcp_test = params.get_value_if_available("!tcp_node_test")
            if tcp_test:
                tcp_test = tcp_test.replace("\r\n", "")
        params.set_dictionary(status, "tcp_node_test", "")

    status_list.append((f"TCP test using {dest_ip}:{dest_port}", tcp_test))

    # TEST REST server

    rest_ip_port = net_utils.get_ip_port(1, 2)      # Use the local network first
    if not rest_ip_port:
        rest_ip_port = net_utils.get_ip_port(1,1)       # Get the REST / External connection
    if http_server.is_ssl():
        rest_ip_port = f"https://{rest_ip_port}"
    else:
        rest_ip_port = f"http://{rest_ip_port}"

    if not rest_ip_port:
        rest_test = "No REST IP and Port in use"
    else:
        command = "rest_node_test = rest get where url = %s and type = info and command = \"get status\" and User-Agent = AnyLog/1.23" % rest_ip_port
        ret_val = process_cmd(status, command, False, None, None, io_buff_in)
        if ret_val:
            rest_test = f"Failed to send REST GET using: {rest_ip_port} with error: {process_status.get_status_text(ret_val)}"
        else:
            if status.get_active_job_handle().is_rest_caller():
                time.sleep(1)
            else:
                utils_print.time_star_print("Test REST", 1, 4, 60)

            rest_test = params.get_value_if_available("!rest_node_test")

        params.set_dictionary(status, "rest_node_test", "")

    status_list.append((f"REST test using {rest_ip_port} ", rest_test))

    reply = utils_print.output_nested_lists(status_list, "", ["Test", "Status"], True)

    return [process_status.SUCCESS, reply]

# =======================================================================================================================
# Test particular IP and port connections
# =======================================================================================================================
def test_connection(status, io_buff_in, cmd_words, trace):

    ip_port = params.get_value_if_available(cmd_words[2])
    if not ip_port:
        ip_port = cmd_words[2]
        index = ip_port.find(":")
        if index > 0:
            ip = params.get_value_if_available(ip_port[:index])
            port = params.get_value_if_available(ip_port[index + 1:])
            ip_port = ip + ":" + port

    ret_val, reply = net_utils.test_host_port(ip_port)
    return [ret_val, reply]

# =======================================================================================================================
# Issue "drop table" command on all nodes that maintain the table's data
# AND removes the blockchain ledger
# Example: drop network table where name = ping_sensor and dbms = lsl_demo and master = 10.0.0.25:2548
# =======================================================================================================================
def drop_network_table(status, io_buff_in, cmd_words, trace):

    reply = ""
    keywords = {
        #                 Must     Add      Is
        #                 exists   Counter  Unique

        "dbms": ("str", True, False, True),
        "name": ("str", True, False, True),
        "master": ("str", True, False, False),
    }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        return ret_val

    dbms_name = conditions["dbms"][0]
    if dbms_name == '*':
        status.add_error(f"Unrecognized database name - asterisk (*) for dbms name is not allowed")
        return process_status.Error_command_params

    table_name = conditions["name"][0]
    if table_name == '*':
        status.add_error(f"Unrecognized table name - asterisk (*) for table name is not allowed")
        return process_status.Error_command_params

    master_node = interpreter.get_one_value_or_default(conditions, "master", None)

    # Get the list of nodes that have local tables to drop
    ret_val, operators_list = resolve_destination(status, "", False, dbms_name, table_name, None, "main")

    # Get the list of operators that host the data
    if not ret_val:
        if not len(operators_list):
            status.add_error(f"No operators for dbms: '{dbms_name}' and table '{table_name}'")
            ret_val = process_status.Error_command_params
        else:
            ip_port_list = []
            for operator in operators_list:
                ip_port_list.append(f"{operator[0]}:{operator[1]}")
            drop_cmd = f"drop_table_cmd{{}} = run client ({','.join(ip_port_list)}) drop table {table_name} where dbms = {dbms_name}"
            ret_val = process_cmd(status, drop_cmd, False, None, None, io_buff_in)
            if not ret_val:

                time.sleep(3)       # Wait for the round trips

                nodes_reply_str = params.get_value_if_available("!drop_table_cmd")
                nodes_reply = utils_json.str_to_json(nodes_reply_str)
                if not nodes_reply:
                    status.add_error(f"Failed to interpret replies for 'test network tables' command with DBMS: '{dbms_name}' and table '{table_name}'")
                    ret_val = process_status.ERR_process_failure
                else:
                    reply_summary = []
                    all_dropped = True
                    for operator in operators_list:
                        ip_port = f"{operator[0]}:{operator[1]}"
                        if ip_port in nodes_reply:
                            # this node replied to the 'test network table command"
                            single_reply = nodes_reply[ip_port]
                            if single_reply != "Success":
                                all_dropped = False
                        else:
                            single_reply = "No reply from node"
                            all_dropped = False
                        reply_summary.append([ip_port, single_reply])

                    if all_dropped:
                        get_command = f"blockchain get table where name = {table_name} and dbms = {dbms_name}"
                        ret_val =  drop_policy_from_metadata(status, io_buff_in, master_node, get_command)
                        if not ret_val:
                            reply_summary.append(["Ledger Policy", "Dropped"])
                        else:
                            reply_summary.append(["Ledger Policy", "Not Dropped"])

                    reply = utils_print.output_nested_lists(reply_summary, f"Drop Network Table: {dbms_name}.{table_name}", ["IP:Port","Status"], True)
                    utils_print.output(reply, True)

    return ret_val

# =======================================================================================================================
# Issue "test table" command on all nodes that maintain the table's data
# Example: test network table where name = ping_sensor and dbms = lsl_demo
# =======================================================================================================================
def test_network_table(status, io_buff_in, cmd_words, trace):

    reply = ""
    keywords = {
        #                 Must     Add      Is
        #                 exists   Counter  Unique

        "dbms": ("str", True, False, True),
        "name": ("str", True, False, True),
    }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        return [ret_val, None]

    dbms_name = conditions["dbms"][0]
    if dbms_name == '*':
        status.add_error(f"Unrecognized database name - asterisk (*) for dbms name is not allowed")
        return [process_status.Error_command_params, reply]

    table_name = conditions["name"][0]

    # Get the list of operators that host the data
    operators = metadata.get_operators_by_company('*', dbms_name, table_name)
    if not len(operators):
        status.add_error(f"No operators for dbms: '{dbms_name}' and table '{table_name}'")
        ret_val = process_status.Error_command_params
    else:
        ip_port_list = []
        for operator in operators:
            ip_port_list.append(operator[7])
        test_cmd = f"test_table_cmd{{}} = run client ({','.join(ip_port_list)}) test table {table_name} where dbms = {dbms_name}"
        ret_val = process_cmd(status, test_cmd, False, None, None, io_buff_in)

        time.sleep(3)       # Wait for the round trips

        nodes_reply_str = params.get_value_if_available("!test_table_cmd")
        nodes_reply = utils_json.str_to_json(nodes_reply_str)
        if not nodes_reply:
            status.add_error(f"Failed to interpret replies for 'test network tables' command with DBMS: '{dbms_name}' and table '{table_name}'")
            ret_val = process_status.ERR_process_failure
        else:
            reply_summary = []
            for entry in operators:
                operator_name = entry[5]
                ip_port = entry[7]
                if ip_port in nodes_reply:
                    # this node replied to the 'test network table command"
                    single_reply = nodes_reply[ip_port]
                else:
                    single_reply = "No reply from node"
                reply_summary.append([operator_name, ip_port, single_reply])

            reply = utils_print.output_nested_lists(reply_summary, f"Network status for DBMS: '{dbms_name}' Table '{table_name}'", ["Operator","IP:Port","Status"], True)

    return [ret_val, reply]  # Return a message if an error
# =======================================================================================================================
# Test that a table definition in the dbms is the same as the blockchain definition of the table
# Example: test table ping_sensor where dbms = lsl_demo
# =======================================================================================================================
def test_table(status, io_buff_in, cmd_words, trace):

    if cmd_words[3] != "where" and cmd_words[5] != "=":
        return [process_status.ERR_command_struct, ""]

    reply = "Passed"

    table_name = cmd_words[2]
    dbms_name = cmd_words[6]

    if table_name == '*':
        ret_val = process_status.SUCCESS
        # get all the tables for the database from the blockchain
        tables_list = get_tables_for_dbms(status, dbms_name) # get the list of tables registered in the blockchain and assigned to the specified dbms

        tested_tables = []
        for entry in tables_list:
            tested_tables.append({"table_name": entry})     # The table name from the blockchain
            ret_val, part_list = db_info.get_partitions_list(status, dbms_name, entry)
            if ret_val:
                reply = f"Test table {dbms_name}.{entry} schema failed: failed to retrieve partitions from dbms or dbms not connected"
                ret_val = process_status.Missing_par_info
                break
            tested_tables += part_list
    else:
        # Make a list with the table name and the partitions names
        ret_val, tested_tables = db_info.get_partitions_list(status, dbms_name, table_name)
        if not ret_val:
            tested_tables.insert(0, {"table_name": table_name})  # consider the table as a partition in the list
        else:
            reply = f"Test table {dbms_name}.{table_name} schema failed: failed to retrieve partitions from dbms or dbms not connected"
            ret_val = process_status.Missing_par_info

    if not ret_val:
        if not tested_tables:
            reply = f"Test table failed: No tables for the specified database: '{dbms_name}'"
            ret_val = process_status.Missing_par_info
        else:
            for entry in tested_tables:
                if "table_name" in entry:
                    # Get the blockchain def of the table
                    tested_name = entry["table_name"]
                    blockchain_schema = blockchain_select_schema(status, dbms_name, tested_name)
                    if not blockchain_schema:
                        reply = f"Test table {dbms_name}.{tested_name} schema failed: Blockchain schema not available"
                        break
                elif "par_name" in entry:
                    tested_name = entry["par_name"]
                else:
                    reply = f"Test table {dbms_name}.{tested_name} schema failed: Failed to retrieve partition list for the tested table "
                    ret_val = process_status.Missing_par_info
                    break

                dbms_schema_str = db_info.get_table_info(status, dbms_name, tested_name, "columns")
                if not dbms_schema_str:
                    reply = f"Test table {dbms_name}.{tested_name} schema failed: dbms schema not available or dbms not connected"
                    ret_val = process_status.Missing_par_info
                    break
                ret_val, reply = compare_schema_ledger_to_table(status, dbms_name, tested_name, blockchain_schema, dbms_schema_str)
                if ret_val:
                    break


    return [process_status.SUCCESS, reply]       # Return a message if an error

# =======================================================================================================================
# Compare the schema of a table in the ledger to the dbms definition
# =======================================================================================================================
def compare_schema_ledger_to_table(status, dbms_name, table_name, blockchain_schema, dbms_schema_str):

    ret_val = process_status.SUCCESS
    reply = "Passed"

    dbms_schema_json = utils_json.str_to_json(dbms_schema_str)
    key = "Structure.%s.%s" % (dbms_name, table_name)
    if dbms_schema_json and key in dbms_schema_json.keys():
        dbms_schema_list = dbms_schema_json[key]
        # make the same structure as blockchain_schema
        dbms_schema = []
        for entry in dbms_schema_list:
            if "column_name" in entry.keys() and "data_type" in entry.keys():
                dbms_schema.append((entry["column_name"], entry["data_type"]))

        columns_count =  len(blockchain_schema)
        if len(blockchain_schema) != len(dbms_schema):
            reply = f"Test table {dbms_name}.{table_name} schema failed: blockchain schema has {columns_count} columns and dbms schema has {len(dbms_schema)} columns"
            ret_val = process_status.Inconsistent_schema
        else:
            # Compare the column name and type
            for index in range (columns_count):
                ledger_entry = blockchain_schema[index]
                l_column_name = ledger_entry[0]     # Ledger schema column name
                l_column_type = ledger_entry[1]     # Ledge schema data type

                dbms_entry =  dbms_schema[index]
                d_column_name = dbms_entry[0]  # dbms_schema column name
                d_column_type = dbms_entry[1]  # dbms_schema data type

                if l_column_name != d_column_name:
                    reply = f"Test table {dbms_name}.{table_name} schema failed: column {index + 1} is named '{l_column_name}' in blockchain and '{d_column_name}' in dbms"
                    ret_val = process_status.Inconsistent_schema
                    break

                # Data type in Ledger
                ret_val, l_data_type = utils_data.unify_data_type(status, l_column_type) # Map the data type to the supported name
                if ret_val:
                    reply = f"Test table {dbms_name}.{table_name} schema failed: column {index + 1} with unsupported data type in blockchain def: '{l_column_type}'"
                    ret_val = process_status.Inconsistent_schema
                    break

                # Data type in DBMS
                ret_val, d_data_type = utils_data.unify_data_type(status, d_column_type)  # Map the data type to the supported name
                if ret_val:
                    reply = f"Test table {dbms_name}.{table_name} schema failed: column {index + 1} with unsupported data type in dbms schema: '{d_column_type}'"
                    ret_val = process_status.Inconsistent_schema
                    break

                if l_data_type != d_data_type:
                    l_unified_dt = utils_data.get_unified_data_type(l_data_type)
                    d_unified_dt = utils_data.get_unified_data_type(d_data_type)
                    if not l_unified_dt or not d_unified_dt or l_unified_dt != d_unified_dt:
                        # Can be "timestamp without time zone" in the dbms and timestamp in the ledger
                        reply = f"Test table {dbms_name}.{table_name} schema failed: column {index + 1} ({l_column_name}) with data type '{l_data_type}' in blockchain and '{d_data_type}' in dbms schema"
                        ret_val = process_status.Inconsistent_schema
                        break

    else:
        reply = "\r\n\r\nSchema for %s.%s in the local database is not recognized" % (dbms_name, table_name)
        ret_val = process_status.Inconsistent_schema

    return [ret_val, reply]


# =======================================================================================================================
# Executes Local tests - example: test node
# =======================================================================================================================
def _process_test(status, io_buff_in, cmd_words, trace):

    ret_val, is_msg, param_name, words_array =  pre_process_command(status, io_buff_in, cmd_words)

    if not ret_val:
        ret_val, reply = _exec_child_dict(status, commands["test"]["methods"], commands["test"]["max_words"], io_buff_in, words_array, 1, trace)
    else:
        reply = ""

    post_process_command(status, io_buff_in, ret_val, is_msg, False, reply, param_name)

    return ret_val

# =======================================================================================================================
# Test condition
# if not !json_data then process !script_create_table
# =======================================================================================================================
def _process_if(status, io_buff_in, cmd_words, trace, json_struct = None):
    offset = get_command_offset(cmd_words)

    words_count = len(cmd_words)
    if words_count < (2 + offset):
        return process_status.ERR_command_struct


    conditions_list = []
    ret_val, offset_then, with_paren = params.analyze_if(status, cmd_words, offset, conditions_list)
    if ret_val:
        return ret_val

    # test_condition returns: a) offset to then, b) 0 (False), 1 (True), -1 (Error)
    next_word, ret_code = params.process_analyzed_if(status, cmd_words, offset, offset_then, with_paren, conditions_list, json_struct)

    if ret_code == -1:
        return process_status.ERR_command_struct

    ret_val = process_status.SUCCESS

    result = bool(ret_code)

    if words_count <= next_word or not "then" in cmd_words[next_word:]:
        # print result
        if offset:
            params.add_param(cmd_words[0], str(result))
        else:
            utils_print.output(str(result), True)
    else:
        if result:
            if cmd_words[next_word] != "then":
                status.add_error("Missing \'then\' in if condition")
                ret_val = process_status.ERR_command_struct
            elif words_count > (next_word + 1):
                then_command = utils_data.get_str_from_array(cmd_words, next_word + 1, 0)
                ret_val = process_cmd(status, then_command, False, None, None, io_buff_in)

    status.set_if_result(result)  # save result for the 'else' stmt

    return ret_val  # return offset to command to execute


# =======================================================================================================================
# Process the else and the do commands based on the if condition
# 'do' are executed if the 'if' stmt returned true
# 'else' is executed if the last 'if' stmt returned False
# =======================================================================================================================
def _process_do_else(status, io_buff_in, cmd_words, trace):
    if len(cmd_words) == 1:
        return process_status.ERR_command_struct

    if cmd_words[0] == 'else':
        if not status.get_if_result():
            # if failed, do the else stmt
            else_command = utils_data.get_str_from_array(cmd_words, 1, 0)

            if_status = status.get_if_result() # Store the if status as it may change inside the called process

            ret_val = process_cmd(status, else_command, False, None, None, io_buff_in)

            if else_command.startswith("process "):
                # Returned from the process, revert the status change
                status.set_if_result(if_status)  # Revert to the original if state
        else:
            # if statement was executed
            ret_val = process_status.ELSE_IGNORED
    else:  # do
        if status.get_if_result():
            # if succeeded, execute the do stmt
            do_command = utils_data.get_str_from_array(cmd_words, 1, 0)

            if_status = status.get_if_result()  # Store the if status as it may change inside the called process
            ret_val = process_cmd(status, do_command, False, None, None, io_buff_in)
            if do_command.startswith("process "):
                # Returned from the process, revert the status change
                status.set_if_result(if_status)  # Revert to the original if state
        else:
            # if statement was executed
            ret_val = process_status.DO_IGNORED



    return ret_val


# =======================================================================================================================
# Show command options - show usage example or help for a particular command or all commands
# =======================================================================================================================
def _print_help(status, io_buff_in, cmd_words, trace):
    global max_command_words

    words_count = len(cmd_words)
    if words_count >= 2 and cmd_words[1] == "message":
        return process_status.Non_supported_remote_call

    if status.get_active_job_handle().is_rest_caller():
        help_msg = True
    else:
        help_msg = False

    ret_val, help_info = _help_commands(status, cmd_words[1:], commands, max_command_words, help_msg)

    documentation = "\r\nDocumentation: https://github.com/AnyLog-co/documentation/blob/master/README.md\r\n"
    if help_msg:
        if words_count == 1:
            # Help
            help_info += documentation
        status.get_active_job_handle().set_result_set(help_info[2:])
        status.get_active_job_handle().signal_wait_event()  # signal the REST thread that output is ready
    else:
        if words_count == 1:
            # Help
            utils_print.output(documentation, True)
        utils_print.output("\r\n", True)

    return process_status.SUCCESS       # Always returns success as error is printed

# =======================================================================================================================
# return the number of words that make a command
# =======================================================================================================================
def _words_to_command(cmd_words, cmd_dict, max_words):

    if len(cmd_words) < max_words:
        words_count = len(cmd_words)  # The max number of words that makes a key in this dictionary
    else:
        words_count = max_words  # The max number of words that makes a key in this dictionary

    cmd = ""
    while words_count:
        cmd = ' '.join(cmd_words[:words_count])
        if cmd in cmd_dict.keys():
            break
        words_count -= 1
    return [words_count, cmd]

# =======================================================================================================================
# print all commands usage
# help *.url  - shows help URLs
# =======================================================================================================================
def _help_commands(status, cmd_txt, cmd_dict, max_key_words, help_msg):

    help_info = ""

    if not len(cmd_txt):
        ret_val = process_status.SUCCESS
        # show all commands
        # print help for all commands
        help_info += _help_all_commands(cmd_dict, help_msg)
    elif cmd_txt[0].startswith("*.url"):
        ret_val = process_status.SUCCESS
        # Show help urls
        if cmd_txt[0].endswith(".validate"):
            validate_link = True        # Validate link is not broken
        else:
            validate_link = False
        help_info = _help_urls(status, cmd_dict, help_msg, validate_link)
    elif cmd_txt[0] == "index":
        ret_val = process_status.SUCCESS
        help_info = _help_index(status, cmd_txt, help_msg)
    else:
        ret_val = process_status.ERR_command_struct
        # show single command
        words_count, new_key = _words_to_command(cmd_txt, cmd_dict, max_key_words)   # return the number of words that make a key

        if words_count:

            if 'methods' in cmd_dict[new_key]:
                ret_val, info = _help_commands(status, cmd_txt[words_count:], cmd_dict[new_key]["methods"], cmd_dict[new_key]["max_words"], help_msg)
                help_info += info
            else:
                ret_val = process_status.SUCCESS
                usage_info = utils_print.output_lines("\r\nUsage:", cmd_dict[new_key]['help']['usage'], "\r\n\t", False, help_msg)
                expln_info = utils_print.output_lines("\r\n\nExplanation:", cmd_dict[new_key]['help']['text'], "\r\n\t", False, help_msg)
                exmpl_info = utils_print.output_lines("\r\n\nExamples:", cmd_dict[new_key]['help']['example'], "\r\n\t", False, help_msg)
                if "keywords" in cmd_dict[new_key]['help']:
                    exmpl_keywords = utils_print.output_lines("\r\n\nIndex:", str(cmd_dict[new_key]['help']['keywords']),"\r\n\t", False, help_msg)
                else:
                    exmpl_keywords = ""

                if help_msg:
                    # send the data via rest call
                    help_info += (usage_info + expln_info + exmpl_info + exmpl_keywords)

            if not ret_val:
                if 'link' in cmd_dict[new_key]['help'].keys():
                    link_info = "\r\n\r\nLink: https://github.com/AnyLog-co/documentation/%s" % cmd_dict[new_key]['help']['link']
                    if help_msg:
                        help_info += link_info
                    else:
                        utils_print.output(link_info, False)
        else:
            # find similar strings
            help_info += _suggest_help_commands(cmd_dict, new_key, help_msg)

    return [ret_val, help_info]
# =======================================================================================================================
# print commands by index
# =======================================================================================================================
def _help_index(status, cmd_txt, help_msg):

    global help_index_

    help_info = ""
    if len(cmd_txt) > 1:
        index_word = ' '.join(cmd_txt[1:])      # The keyword to search
    else:
        index_word = ""

    for keyword in sorted(help_index_.keys()):
        # Print the index term

        if not index_word or index_word == '*' or  keyword.startswith(index_word):

            if help_msg:
                help_info += f"\r\n{keyword}"
            else:
                utils_print.output(f"\r\n{keyword}", False)

            if not index_word:
                continue            # Show only index keywords

            if not index_word:
                continue            # Only keywords

            values = help_index_[keyword]
            # Print the commands
            for cmd in values:
                if help_msg:
                    help_info += f"\r\n     {cmd}"
                else:
                    utils_print.output(f"\r\n     {cmd}", False)

    return help_info

# =======================================================================================================================
# print all commands usage
# =======================================================================================================================
def _help_all_commands(cmd_dict, help_msg):
    help_info = ""
    for cmd in sorted(cmd_dict.keys()):
        if not 'internal' in cmd_dict[cmd]['help'] or cmd_dict[cmd]['help']['internal'] == False:
            usage_info = "\r\n" + cmd_dict[cmd]['help']['usage']
            if help_msg:
                help_info += usage_info
            else:
                utils_print.output(usage_info, False)

    return help_info

# =======================================================================================================================
# Show all the help URLS
# =======================================================================================================================
def _help_urls(status, cmd_dict, help_msg, validate_link):

    help_info = ""
    for cmd in sorted(cmd_dict.keys()):
        link_info = _get_cmd_url(status, cmd_dict, "", cmd, validate_link)
        if help_msg:
            help_info += link_info
        else:
            utils_print.output(link_info, False)
    return help_info

# =======================================================================================================================
# Show all the help URLS
# =======================================================================================================================
def _get_cmd_url(status, cmd_dict, parent, cmd, validate_link):
    '''
    cmd_dict - the main dictionary or a method dictionary
    parent - for a method dictionary, the parent (i.e. get, set, blockchain) and a space as the sufix
    cmd - the command in the dictionary to consider
    validate_link - test that link is not broken
    '''

    help_info = "\r\n"
    help_info += parent + cmd + ' '        # Add command name
    cmd_info = cmd_dict[cmd]
    if 'help' in cmd_info:
        help = cmd_info['help']
        is_valid_link = True
        if 'link' in help:
            url_link = f"https://github.com/AnyLog-co/documentation/{help['link']}"
            if validate_link:
                if http_client.do_GET(status, f"123valid_link = rest get where validate = true and url = {url_link}".split() , 2):
                    is_valid_link = False
                elif params.get_value_if_available("!123valid_link") != "true":
                    is_valid_link = False

            help_info += f" {url_link}"

            if not is_valid_link:
                help_info += " *** Broken Link ***"  # Flag broken link

    if 'methods' in cmd_info:
        methods = cmd_info['methods']
        for cmd_method in sorted(methods.keys()):
            help_info += _get_cmd_url(status, methods, cmd + ' ', cmd_method, validate_link)

    return help_info
# =======================================================================================================================
# print suggested command options that the user may want to use
# =======================================================================================================================
def _suggest_help_commands(cmd_dict, new_key, help_msg):
    help_info = ""
    # find similar strings
    message = 'Unrecognized command\r\n'
    if help_msg:
        help_info += message
    else:
        utils_print.output(message, False)

    help_info += get_similar_cmd(cmd_dict, new_key, help_msg, 0)

    return help_info
# =======================================================================================================================
# Get the list of similar commands
# =======================================================================================================================
def get_similar_cmd(cmd_dict, new_key, help_msg, maybe_counter):

    help_info = ""
    if len(new_key) > 1:
        for cmd in sorted(cmd_dict.keys()):
            if 'methods' in cmd_dict[cmd]:
                methods = cmd_dict[cmd]['methods']
                help_info += get_similar_cmd(methods, new_key, help_msg, maybe_counter)
            else:
                if new_key in cmd:
                    maybe_counter += 1
                    message = '\r\nMaybe: %s' % cmd_dict[cmd]["help"]['usage']
                    if help_msg:
                        help_info += message
                    else:
                        utils_print.output(message, False)
        if not maybe_counter:
            # remove last word or last char and try
            key_terms = new_key.split()
            if len(key_terms) > 1:
                new_key = ' '.join(key_terms[:-1])
            else:
                new_key = new_key[:-1]
            if new_key:
                for cmd in sorted(cmd_dict.keys()):
                    if new_key in cmd or SequenceMatcher(a=cmd, b=new_key).ratio() > 0.5:
                        message = '\r\nMaybe: %s' % cmd
                        if help_msg:
                            help_info += message
                        else:
                            utils_print.output(message, False)
    return help_info
# ------------------------------------------
# Get the Logged data
# ------------------------------------------
def reset_logged_data(status, io_buff_in, cmd_words, trace):

    if process_log.reset_events(cmd_words[1]) == False:
        ret_val = process_status.ERR_command_struct  # no such dic
    else:
        ret_val = process_status.SUCCESS

    return ret_val
# ------------------------------------------
# Reset the Reply IP - reply messages will use the source ip
# reset reply ip
# reset self ip
# ------------------------------------------
def reset_replacement_ip(status, io_buff_in, cmd_words, trace):

    if cmd_words[1] == "reply":
        net_utils.set_reply_ip(None, None)
    elif cmd_words[1] == "self":
        net_utils.set_self_ip(None, None)

    return process_status.SUCCESS
# =======================================================================================================================
# Set the trace info to on/off for all or a single command
# =======================================================================================================================
def _set_trace(status, io_buff_in, cmd_words, trace):

    ret_val = process_status.SUCCESS
    if  len(cmd_words) > 2 and cmd_words[2] == "message":  # a tcp sql message
        mem_view = memoryview(io_buff_in)
        command = message_header.get_command(mem_view)
        command = command.replace('\t', ' ')
        words_array = command.split()
    else:
        words_array = cmd_words

    words_count = len(words_array)

    if words_count == 2:
        # show all commands with level > 0
        for command in commands.keys():
            trace_level = commands[command]['trace']
            if trace_level:
                trace_msg = "%3u : %s\r\n" % (trace_level, command)
                utils_print.output(trace_msg, False)
        if utils_sql.is_trace_sql():
            # Print trace level for "sql command" (different than "sql")
            trace_msg = "%3u : %s\r\n" % (utils_sql.get_trace_level(), "sql command " + utils_sql.get_trace_command())
            utils_print.output(trace_msg, False)

        utils_print.print_prompt()
    elif words_array[2] != '=':
        # print command level for a single command
        command = utils_data.get_str_from_array(words_array, 2, 0)
        if command in commands:
            trace_level = commands[command]['trace']
            utils_print.output(str(trace_level), True)
            ret_val = process_status.SUCCESS
        elif command == "sql command":
            trace_msg = "%3u : %s\r\n" % (utils_sql.get_trace_level(), "sql " + utils_sql.get_trace_command())
            utils_print.output(trace_msg, False)
        else:
            ret_val = process_status.ERR_command_struct
        return ret_val
    elif words_count >= 4:
        # set trace value

        if not words_array[3].isdigit():
            ret_val = process_status.ERR_command_struct  # trace level has to be a number
        else:

            trace_level = int(words_array[3])

            if len(words_array) == 4:  # modify for all commands
                for command in commands.keys():
                    commands[command]['trace'] = trace_level
            else:
                if words_array[4] == "insert":
                    # trace the number of inserts
                    # Example: trace level = 1 inserts 2000 --> Report every 2000
                    # By default report every 1000
                    if not trace_level:
                        inserts_threshold = 0
                    else:
                        if len(words_array) >= 6 and words_array[5].isdigit():
                            inserts_threshold = int(words_array[5])
                        else:
                            inserts_threshold = 1000
                    db_info.set_insert_threshold(inserts_threshold)
                else:
                    command = utils_data.get_str_from_array(words_array, 4, 0)
                    if command in commands:
                        commands[command]['trace'] = trace_level
                    elif command == "tcp":
                            # This will enable trace on IP and ports used
                            # command: trace level = 1 tcp
                            net_utils.set_tcp_debug(trace_level)
                    elif command[:11] == "sql command":
                        # This will enable trace with SQL stmt
                        # command: trace level = 1 sql command
                        if len(command) > 11:
                            trace_command = command[11:].lower().strip()
                        else:
                            trace_command = ""      # All commands
                        utils_sql.set_trace_level(trace_level, trace_command)
                    elif trace_func.is_traced(command):
                        # Enables trace on functions
                        # For example: when source data is mapped in mapping_policy.apply_policy_schema()
                        # command: trace level = 1 mapping
                        trace_func.set_trace_level(command, trace_level)
                    else:
                        # test sub command
                        command_list = command.split(' ',1)
                        if len(command_list) == 2 and command_list[0] in commands and 'methods' in commands[command_list[0]]:
                            # get the subcommand
                            subcommands = commands[command_list[0]]['methods']
                            if command_list[1] in subcommands:
                                subcommands[command_list[1]]['trace'] = trace_level
                            else:
                                ret_val = process_status.ERR_command_struct
                        else:
                            ret_val = process_status.ERR_command_struct

    else:
        ret_val = process_status.ERR_command_struct

    return ret_val


# =======================================================================================================================
# Consider a command prefix to determine the number of words to generate a unique command.
# For example, with the word 'run', 3 words are needed to be "run tcp server" or "run tcp client"
# Return -1 for an error, 0 if command is found or 1 if more words are needed
# =======================================================================================================================
def words_for_existing_command(status, command):
    global max_command_words

    sub_str = utils_data.cmd_line_to_list_with_json(status, command, -1, -1)  # a list with words in command line
    words_count = len(sub_str)
    if (words_count == 0 or words_count > max_command_words):
        return -1  # no command or too many command words

    test_string = utils_data.get_str_from_array(sub_str, 0)  # this process makes sure only one space between words

    ret_val = -1
    for key in commands.keys():
        if key == test_string:
            ret_val = 0  # was found
            break
        if key.startswith(test_string):
            ret_val = 1  # get another word
            break

    return ret_val

# ------------------------------------------
# Returns the size of a logical database
# Command: get database size lsl_demo
# ------------------------------------------
def get_dbms_size(status, io_buff_in, cmd_words, trace):

    words_count = len(cmd_words)
    offset = get_command_offset(cmd_words)

    if words_count != (4 + offset) or cmd_words[2 + offset] != "size":
        reply = ""
        ret_val = process_status.ERR_command_struct
    else:
        dbms_name = params.get_value_if_available(cmd_words[3 + offset])
        reply = db_info.get_dbms_size(status, dbms_name)    # Returns -1 if failed to determine the size
        ret_val = process_status.SUCCESS

    return [ret_val, reply]

# ------------------------------------------
# Get info on the metadata version and setup
# ------------------------------------------
def get_metadata_version(status, io_buff_in, cmd_words, trace):
    version = bsync.blockchain_stat.get_hash_value()
    return [process_status.SUCCESS, version]

# ------------------------------------------
# Get info on the metadata version and setup
# ------------------------------------------
def get_synchronizer(status, io_buff_in, cmd_words, trace):

    version = bsync.blockchain_stat.get_hash_value()

    synchronizer_stat = "Running" if bsync.is_running() else "Not Running"
    sync_time = bsync.get_sync_time()
    sync_source = bsync.get_source()
    connection = bsync.get_connection()

    update_time = metadata.get_update_time()
    if not update_time:
        time_since_update = "Not updated"
    else:
        seconds_diff = utils_columns.get_current_time_in_sec() - update_time
        hours_time, minutes_time, seconds_time = utils_data.seconds_to_hms(seconds_diff)
        time_since_update = "%02u:%02u:%02u" % (hours_time, minutes_time, seconds_time)

    call_count = bsync.get_counter_process()    # The number of times the metadata was considering the blockchain file
    load_count = metadata.get_metadata_version()       # The number of times data was loaded to the metadata
    policies_count = blockchain.get_policies_count()

    metadata_info = [(synchronizer_stat, sync_source, connection, sync_time, version,
                      time_since_update, call_count, load_count, policies_count)]

    reply = utils_print.output_nested_lists(metadata_info, "",
                                            ["Status", "Sync\nSource", "Connection", "Sync\nTime", "Metadata Version",
                                             "Time\nSince Update", "Calls", "Loads", "Policies"], True)


    return [process_status.SUCCESS, reply]
# ------------------------------------------
# Get the script data (from the script directory)
# get script autoexec
# ------------------------------------------
def get_script(status, io_buff_in, cmd_words, trace):
    offset = get_command_offset(cmd_words)
    script_file  = params.get_value_if_available(cmd_words[2 + offset])
    if '.' in script_file:
        f_name = script_file.lower()     # File type is included
    else:
        f_name = script_file.lower() + ".al"        # Add al extension

    file_name = params.get_value_if_available("!local_scripts") + params.get_path_separator() + f_name

    reply = utils_io.read_all_lines_in_file(status, file_name)

    if reply:
        ret_val = process_status.SUCCESS
    else:
        ret_val = process_status.ERR_file_does_not_exists

    return [ret_val, reply]

# ------------------------------------------
# Create or update a policy in the dictionary
# Example: set policy config_policy [config] = {} and  [config][company] = anylog
# ------------------------------------------
def set_policy(status, io_buff_in, cmd_words, trace):

    policy_name = cmd_words[2]

    offset = policy_name.find('[')
    if offset > 0 and offset < len(policy_name) - 2:
        # seperate the dictionary name from the index: config_policy[config][ip] --> config_policy [config][ip]
        policy_name = policy_name[:offset]
        key_value = [cmd_words[2][offset:]] + cmd_words[3:]
    else:
        key_value = cmd_words[3:]

    ret_val = params.set_dictionary(status, policy_name, key_value)


    return [ret_val, None]
# ----------------------------------------------------------------------
# Set the node name
# ----------------------------------------------------------------------
def set_current_node_name(status, io_buff_in, cmd_words, trace):
    node_name = params.get_value_if_available(cmd_words[3])
    node_info.set_node_name(node_name)
    ret_val = process_status.SUCCESS
    return ret_val
# ------------------------------------------
# Write the script data (to the script directory)
# set script toexec data
# ------------------------------------------
def set_script(status, io_buff_in, cmd_words, trace):
    offset = get_command_offset(cmd_words)
    script_file  = params.get_value_if_available(cmd_words[2 + offset])

    if '.' in script_file:
        f_name = script_file.lower()     # File type is included
    else:
        f_name = script_file.lower() + ".al"        # Add al extension

    file_name = params.get_value_if_available("!local_scripts") + params.get_path_separator() + f_name

    file_data = params.get_value_if_available(cmd_words[3 + offset])

    json_obj = utils_json.str_to_json(file_data)
    if not json_obj:
        ret_val = process_status.ERR_wrong_json_structure
    else:
        formatted_str = utils_json.to_formatted_string(json_obj, 4)

        if not formatted_str:
            ret_val = process_status.ERR_wrong_json_structure
        else:
            reply = utils_io.write_str_to_file(status, formatted_str, file_name)

            if reply:
                ret_val = process_status.SUCCESS
            else:
                ret_val = process_status.File_write_failed

    return ret_val

# --------------------------------------------------------------
# Set Buffer threshold - include a flag to signal if Publisher is running
# Publisher node is not allowed with write_immediate set to True
# --------------------------------------------------------------
def set_buff_thresholds(status, io_buff_in, cmd_words, trace):

    return streaming_data.set_thresholds(status, io_buff_in, cmd_words, trace, False)   # The False value is as Publisher is not supported in EdgeLake

# ------------------------------------------
# Create a list of ip and ports representing monitored nodes
# Associate the script with a topic
#
# set monitored nodes where topic = operators and nodes = 10.0.0.78:7848,10.0.0.78:3048
# set monitored nodes where topic = operators and nodes = blockchain get operator bring.ip_port
# ------------------------------------------
def set_monitored_nodes(status, io_buff_in, cmd_words, trace):

    if cmd_words[3] != "where":
        return process_status.ERR_command_struct

    keywords = {
        #                 Must     Add      Is
        #                 exists   Counter  Unique

        "topic": ("str", True, False, True),
        "nodes": ("str", True, False, False),
    }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        return ret_val

    monitored_topic = interpreter.get_one_value(conditions, "topic")
    ret_val = process_status.SUCCESS

    target_nodes = conditions["nodes"]
    for entry in target_nodes:
        if entry.startswith("blockchain get"):
            ret_val, nodes_string = blockchain_get(status, entry.split(), None, True)
            if ret_val:
                break
        else:
            nodes_string = entry

        # Can be a comma seperated list
        nodes_list = nodes_string.split(',')
        monitor_cmd = ["monitor", monitored_topic, "where", "ip", "=", None, "and", "setup", "=", "true"]
        for monitored_node in nodes_list:
            monitor_cmd[5] = monitored_node.strip()
            monitor.monitor_info(status, io_buff_in, monitor_cmd, trace)

    return ret_val

# ------------------------------------------
# Get The Status of the current node
# Command: get status
# get status where format = json
# get status where format = json include !disk_space !temperature
# get status where format = json include statistics
# Returns ret_code + node status
# ------------------------------------------
def get_node_status(status, io_buff_in, cmd_words, trace):
    global statistics_
    # get status - returns a message that the node is running
    offset = get_command_offset(cmd_words)
    words_count = len(cmd_words)

    ret_val = process_status.SUCCESS
    reply = node_info.get_node_name() + " running"
    if profiler.is_active():
        reply += " -- in profiling mode"

    if words_count == (offset + 2):
        # Only get status
        reply_format = None
    else:
        reply_struct = {}
        reply_struct["Status"] = reply

        if words_count >= (offset + 6) and utils_data.test_words(cmd_words, offset + 2, ["where", "format", "="]):
            reply_format = cmd_words[5]
            include_offset = offset + 6
        else:
            include_offset = offset + 2
            reply_format = "json"
        if words_count > include_offset:
            # A list of variable names to include in the get status reply
            if cmd_words[include_offset] != "include":
                if cmd_words[include_offset] == "format":
                    status.add_error("Missing 'format' details in 'get status' command")
                ret_val = process_status.ERR_command_struct
            else:
                if include_offset + 1 >= words_count:
                    status.add_error("Missing 'include' details in 'get status' command")
                    ret_val = process_status.ERR_command_struct
                else:
                    for variable in cmd_words[include_offset + 1:]:
                        var_value = params.get_value_if_available(variable)
                        if var_value and var_value != variable:
                            reply_struct[variable[1:]] = var_value      # Provide the value to the caller
                        elif variable == "statistics":
                            # Update the statistics
                            utils_monitor.update_process_statistics(statistics_, False, True)
                            # Add the statistics
                            for stat_key, stat_value in statistics_.items():
                                reply_struct[stat_key] = stat_value

        reply = utils_json.to_string(reply_struct)

    return [ret_val, reply, reply_format]
# ------------------------------------------
# Get The partition declarations
# ------------------------------------------
def get_partitions(status, io_buff_in, cmd_words, trace):

    ret_val = process_status.SUCCESS
    offset = get_command_offset(cmd_words)

    words_count = len(cmd_words)

    reply = ""
    out_format = "table"

    if words_count == (2 + offset):
        # ----------------------------------------------------------
        # Show how partitions are defined - show partitions
        # ----------------------------------------------------------
        if len(partitions.partition_struct):
            reply = utils_print.format_dictionary(partitions.partition_struct, True, False, False, None)
        else:
            reply = "No partitions declared"
    elif words_count == (3 + offset) and cmd_words[2 + offset] == "dropped":
        # ----------------------------------------------------------
        # Show partitions that were dropped using command: event drop_old_partitions
        # get partitioned dropped
        # ----------------------------------------------------------
        if len(events.partitions_dropped):
            reply = utils_print.format_dictionary(events.partitions_dropped, True, False, False, None)
        else:
            reply = "No partitions dropped"

    elif words_count >= (6 + offset) and cmd_words[2 + offset] == "where":
        # ----------------------------------------------------------
        # Show partitions for a database and a table
        # Example "get partitions where dbms = lsl_demo and table = ping_sensor and format = json"
        # ----------------------------------------------------------
        keywords = {
            #                        Must     Add      Is
            #                       exists   Counter  Unique

            "dbms":     ("str", False, True, True),
            "table":    ("str", False, True, True),
            "format":     ("str", False, False, True),
        }

        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3 + offset, 0, keywords, False)
        if not ret_val:
            if counter == 1:
                # Counter needs to be 0 or 2
                status.add_error("Database name and Table name needs to be provided")
                ret_val = process_status.ERR_command_struct
            else:

                dbms_name = interpreter.get_one_value_or_default(conditions, "dbms", None)
                table_name = interpreter.get_one_value_or_default(conditions, "table",None)
                out_format = interpreter.get_one_value_or_default(conditions, "format","table")

                if dbms_name:       # In this case - both table and dbms are provided as counter is 2

                    if table_name == '*':
                        par_struct = {}
                        dbms_name_len = len(dbms_name)
                        for key, val in partitions.partition_struct.items():
                            if len(key) >  (dbms_name_len + 1) and key[dbms_name_len] == '.' and key[:dbms_name_len] == dbms_name:
                                par_struct[key] = val

                        if out_format == "json":
                            reply = utils_json.to_string(par_struct)
                        else:
                            reply = utils_print.format_dictionary(par_struct, True, False, False, None)
                    else:

                        reply = db_info.get_partitions_info(status, dbms_name, table_name, True)
                        if not reply:
                            ret_val = process_status.Missing_par_info
                            reply = "No partitions info"

                else:
                    # Reply with JSON
                    reply = utils_json.to_string(partitions.partition_struct)
    else:
        reply = ""
        ret_val = process_status.ERR_command_struct
    return [ret_val, reply, out_format]

# ------------------------------------------
# Get the list of watch directories
# ------------------------------------------
def get_watch_directories(status, io_buff_in, cmd_words, trace):

    if len(watch_directories):
        dir_mutex.acquire()
        reply = str(watch_directories.keys())[10:-1].replace('\'', '')
        dir_mutex.release()
    else:
        reply = "No watch directories"

    return [process_status.SUCCESS, reply]
# ------------------------------------------
# Statistics on queries execution time. The statistics is configurable by the command ***set query log profile [n] seconds***
# ------------------------------------------
def get_queries_time(status, io_buff_in, cmd_words, trace):

    cmd_offset = get_command_offset(cmd_words)
    words_count = len(cmd_words)

    out_format = "table"
    if words_count == (3 + cmd_offset):
        is_json = False
        ret_val = process_status.SUCCESS
    elif words_count == (7 + cmd_offset) and utils_data.test_words(cmd_words, cmd_offset + 3, ["where", "format", "="]):
        # get queries time where format = JSON
        if cmd_words[cmd_offset + 6] == "json":
            is_json = True
            out_format = "json"
        else:
            is_json = False
        ret_val = process_status.SUCCESS
    else:
        ret_val = process_status.ERR_command_struct
        reply = None

    if not ret_val:
        reply = job_instance.get_query_stats(is_json)

    return [ret_val, reply, out_format]
# ------------------------------------------
# Get info on the scheduler
# Commands options:
# get scheduler
# get scheduler 1
# ------------------------------------------
def get_scheduler(status, io_buff_in, cmd_words, trace):

    words_count = len(cmd_words)

    ret_val = process_status.SUCCESS

    if words_count == 2:
        # command: show scheduler X
        reply = task_scheduler.show_all()
    elif words_count == 3 and cmd_words[2].isdecimal():
        # command: show scheduler X
        reply = task_scheduler.show_info(int(cmd_words[2]))
    else:
        reply = ""
        ret_val = process_status.ERR_command_struct
    return [ret_val, reply]
# ------------------------------------------
# Provide the info on the operator

# get operator where format = json
# get operator summary
# get operator config

# get publisher where format = json
# get publisher files
# get publisher summary
# get publisher config

# ------------------------------------------
def get_service_info(status, io_buff_in, cmd_words, trace):

    service_name = cmd_words[1]         # operator or publisher

    conditions = {
        "service" : [service_name],
    }

    offset = get_command_offset(cmd_words)

    words_count = len(cmd_words)
    ret_val = process_status.SUCCESS
    reply = ""

    reply_format = "table"
    if (words_count - offset) >= 6:
        # Test ending where format = json
        if utils_data.test_words(cmd_words, words_count - 4, ["where", "format", "="]):
            reply_format = cmd_words[-1]
            words_count -= 4        # Ignore the where ...

    is_active = aloperator.is_active() if service_name == "operator" else version.alpublisher_is_active()
    if not is_active:
        # If service is not active - show the configuration
        conditions["topic"] = ["config"]
    elif (words_count - offset) == 2:
        # Get all topics
        if service_name == "operator":
            # get operator
            conditions["topic"] = ["json", "sql", "inserts", "error" ]
        else:
            # get publisher
            conditions["topic"] = ["files"]
    elif words_count == (3 + offset):
        # Get a single topic
        conditions["topic"] = [cmd_words[ 2 + offset]]
    else:
        ret_val = process_status.ERR_command_struct

    if not ret_val:

        ret_val, reply = stats.get_info(status, reply_format, conditions)

    return [ret_val, reply, reply_format]

# ------------------------------------------
# Test if format command
# ------------------------------------------
def get_reply_format(status, cmd_words, offset):
    '''
    return the type of format for a reply:
    json/table/None
    '''
    if (offset + 3) > len(cmd_words):
        reply_format = None
    else:
        if cmd_words[offset] == "format" and  cmd_words[offset + 1] == "=":
            reply_format =  cmd_words[offset + 2]
        else:
            reply_format = None

    return reply_format

# ------------------------------------------
# Provide the info on the Message Broker
# ------------------------------------------
def get_local_broker(status, io_buff_in, cmd_words, trace):

    ret_val = process_status.SUCCESS
    reply = message_server.show_info()  # statistics

    return [ret_val, reply]

# ------------------------------------------------
# show Info on Streaming Data (from REST calls and MQTT calls)
# Command: get streaming
# Command: get streaming where format = json
# Command: get streaming config     # Configuration
# ------------------------------------------------
def get_streaming_info(status, io_buff_in, cmd_words, trace):
    cmd_offset = get_command_offset(cmd_words)

    ret_val = process_status.SUCCESS
    words_count = len(cmd_words)
    if words_count == (2 + cmd_offset):
        out_format = "table"
        config_only = False
    else:
        if cmd_words[cmd_offset + 2] == "config":
            config_only = True
            cmd_offset += 1
        else:
            config_only = False

        if words_count == (2 + cmd_offset):
            out_format = "table"        # get streaming config
        else:
            if words_count == (6 + cmd_offset) and  utils_data.test_words(cmd_words, 2 + cmd_offset, ["where", "format", "="]):
                out_format = cmd_words[5 + cmd_offset]
            else:
                out_format = "table"
                ret_val = process_status.ERR_command_struct

    if not ret_val:
        reply = streaming_data.show_info(config_only, out_format)
    else:
        reply = None

    return [ret_val, reply, out_format]
# ------------------------------------------------
# show Info on REST calls
# Command: get rest
# ------------------------------------------------
def get_rest_calls(status, io_buff_in, cmd_words, trace):
    reply = http_server.show_info()
    return [process_status.SUCCESS, reply]


# ------------------------------------------------
# get msg client (used to be get mqtt clients)
# get msg client 1
# get msg broker
# ------------------------------------------------
def get_msg_client_broker(status, io_buff_in, cmd_words, trace):


    ret_val = process_status.ERR_command_struct
    reply = ""
    words_count = len(cmd_words)

    if words_count == 3 and (cmd_words[2] == "broker" or cmd_words[2] == "brokers"):
        # Show the list of message brokers
        reply = mqtt_client.show_brokers()
        ret_val = process_status.SUCCESS
    elif words_count >= 3 and (cmd_words[2] == "client" or cmd_words[2] == "clients"):
        # Show the list of message clients

        # get msg client  - show all
        # get msg client 3 - show one
        # get msg client detailed- show all
        # get msg client 3 detailed - show one



        if words_count == 3:
            # Show all
            reply = mqtt_client.show_info( 0, False, None, None )
            ret_val = process_status.SUCCESS
        elif words_count >= 7 and cmd_words[3] == "where":
            keywords = {
                #                        Must     Add      Is
                #                       exists   Counter  Unique

                "detailed": ("str", False, True, True),
                "broker": ("str", False, True, True),
                "topic": ("str", False, True, True),
                "id": ("int", False, True, True),
            }

            ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
            if not ret_val:
                client_id, is_detailed, broker, topic = interpreter.get_multiple_values(conditions,
                                                                                     ["id", "detailed", "broker", "topic"],
                                                                                     [0,               False,      None,     None])
                reply = mqtt_client.show_info(client_id, is_detailed, broker, topic)

                if not reply:
                    reply = "No such client subscription"


    return [ret_val, reply]

# ------------------------------------------
# Command: get data nodes where company = company_name and dbms = dbms_name and table =  table_name
# Command: get data nodes where sort = (1,2)
# ------------------------------------------
def enumarate(operator):
    pass


def get_data_nodes(status, io_buff_in, cmd_words, trace):

    if len(cmd_words) == 3:
        company_name =  '*'
        dbms_name =  '*'
        table_name = '*'
        sort_fields = None
        out_format = "table"
        ret_val = process_status.SUCCESS
    elif len(cmd_words) >= 7 and cmd_words[3] == "where":


        #                        Must     Add      Is
        #                       exists   Counter  Unique
        keywords = {
                    "company": ("str", False, False, True),
                    "dbms": ("str", False, False, True),
                    "table": ("str", False, False, True),
                    "sort" :  ("sort", False, False, False),
                    "format": ("str", False, False, False),
                }

        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
        if not ret_val:
            company_name = interpreter.get_one_value_or_default(conditions,"company", '*')
            dbms_name = interpreter.get_one_value_or_default(conditions,"dbms", '*')
            table_name = interpreter.get_one_value_or_default(conditions,"table", '*')
            out_format = interpreter.get_one_value_or_default(conditions, "format", 'table')

            sort_fields = interpreter.get_one_value_or_default(conditions, "sort", None)   # List of columns ID for the sort in parenthesis (0,3)


    else:
        ret_val = process_status.ERR_command_struct

    if not ret_val:
        title = ["Company", "DBMS", "Table", "Cluster ID", "Cluster Status", "Node Name", "Member ID",
                                                      "External IP/Port", "Local IP/Port", "Main", "Node Status"]
        # version changed
        blockchain_load(status, ["blockchain", "get", "cluster"], False, 0)
        operators = metadata.get_operators_by_company(company_name, dbms_name, table_name)

        if out_format == "json":
            json_list = []
            for operator in operators:
                operator_json = {}
                index = 0
                for attribute in operator:
                    operator_json[title[index]] = attribute
                    index += 1
                json_list.append(operator_json)
            reply = utils_json.to_string(json_list)

        else:
            if sort_fields:
                sort_strings = sort_fields[1:-1].split(',')
                try:
                    # Get the column ID
                    sort_columns = ([int(x) for x in sort_strings])
                except:
                    status.add_error("Wrong sort values in 'get data nodes' command")
                    ret_val = process_status.ERR_command_struct
                else:
                    # need to complete the space fields before the sort (for company, dbms and table)
                    entry_company = ""
                    entry_dbms = ""
                    entry_table = ""
                    cluster_id = ""

                    for index, entry in enumerate(operators):
                        if entry[0] == "" or entry[1] == "" or entry[2] == "" or entry[3] == "":
                            new_entry = []
                            for x, column in enumerate(entry):
                                if x == 0:
                                    if column:
                                        entry_company = column
                                    new_entry.append(entry_company)
                                elif x == 1:
                                    if column:
                                        entry_dbms = column
                                    new_entry.append(entry_dbms)
                                elif x == 2:
                                    if column:
                                        entry_table = column
                                    new_entry.append(entry_table)
                                elif x == 3:
                                    if column:
                                        cluster_id = column
                                    new_entry.append(cluster_id)
                                else:
                                    new_entry.append(column)
                            operators[index] = new_entry        # Replace with complete row
                        else:
                            entry_company = entry[0]
                            entry_dbms = entry[1]
                            entry_table = entry[2]
                            cluster_id = entry[3]

                    # get the column size for every sorted field
                    max_len = []
                    try:
                        for col_id in sort_columns:
                            longest_column = max(len(str(item[col_id])) for item in operators)
                            max_len.append(longest_column)
                        # do the sort
                        operators.sort(key=lambda x: custom_sort_key(x, sort_columns, max_len))
                    except:
                        status.add_error("Wrong sort values in 'get data nodes' command")
                        ret_val = process_status.ERR_command_struct

            reply = utils_print.output_nested_lists(operators, "",
                                                     title, True)
    else:
        reply = None


    return [ret_val, reply]

# ------------------------------------------
# Command: get virtual tables where company = company_name and dbms = dbms_name and table =  table_name
# Or with additional info
# Command: get virtual tables info where company = company_name and dbms = dbms_name and table =  table_name
# ------------------------------------------
def get_virtual_tables(status, io_buff_in, cmd_words, trace):

    reply = None
    if len(cmd_words) >= 4 and cmd_words[3] == "info":
        # adding cluster ID, Copies of the data, and Permissions
        with_info = True
        offset = 1
    else:
        with_info = False   # Show only Company + DBMS + Table
        offset = 0

    if len(cmd_words) == (3 + offset):
        company_name =  '*'
        dbms_name =  '*'
        table_name = '*'
        ret_val = process_status.SUCCESS
    elif len(cmd_words) >= (7+ offset) and cmd_words[3+offset] == "where":


        #                        Must     Add      Is
        #                       exists   Counter  Unique
        keywords = {
                    "company": ("str", False, False, True),
                    "dbms": ("str", False, False, True),
                    "table": ("str", False, False, True),
                }

        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4 + offset, 0, keywords, False)
        if not ret_val:
            company_name = interpreter.get_one_value_or_default(conditions,"company", '*')
            dbms_name = interpreter.get_one_value_or_default(conditions,"dbms", '*')
            table_name = interpreter.get_one_value_or_default(conditions,"table", '*')
    else:
        ret_val = process_status.ERR_command_struct

    if not ret_val:
        # version changed
        blockchain_load(status, ["blockchain", "get", "cluster"], False, 0)

        info_list = []
        operators = metadata.get_operators_by_company(company_name, dbms_name, table_name)

        # Get the basic info
        for entry in operators:
            company_name = entry[0]
            dbms_name = entry[1]
            table_name = entry[2]
            if with_info:
                cluster_id = entry[3]
                if cluster_id:
                    info_list.append([company_name, dbms_name, table_name, cluster_id, 1, True])
                else:
                    # Only update the number of operators supporting the cluster
                    len_info = len(info_list)
                    info_list[len_info-1][4] += 1       # Update the number of copies of the data
            else:
                if table_name:
                    # Ignore different clusters to the same dbms and table
                    info_list.append([company_name, dbms_name, table_name])

        if with_info:
            # get the permissions for each table
            if version.al_auth_is_node_authentication():
                ret_val, public_key_str = version.al_auth_get_signatory_public_key(status)
                if not ret_val and public_key_str:
                    for entry in info_list:
                        dbms_name = entry[1]
                        table_name = entry[2]
                        ret_val, permissions_list = version.permissions_permissions_by_public_key(status, public_key_str)
                        if not version.permissions_permission_process(status, 0, None, public_key_str, None, dbms_name, table_name):
                            entry[5] = False

    if not ret_val:
        if with_info:
            header = ["Company", "DBMS", "Table", "Cluster ID", "Copies", "Permissions"]
        else:
            header = ["Company", "DBMS", "Table"]
        reply = utils_print.output_nested_lists(info_list, "", header, True)

    return [ret_val, reply]
# ------------------------------------------
# get servers where company = company_name and  dbms = dbms_name and table = table_name
# ------------------------------------------
def get_servers(status, io_buff_in, cmd_words, trace):

    company_name =  '*'
    dbms_name =  '*'
    table_name = '*'
    reply = ""
    ret_val = process_status.SUCCESS

    words_count = len(cmd_words)
    if "bring" in cmd_words:
        # specify the output
        bring_offset = cmd_words.index("bring")  # get offset to the bring statement
        operators_list = []
        if len(cmd_words) == bring_offset +1:
            ret_val = process_status.ERR_command_struct
    else:
        # Default output - ip:port
        bring_offset = 0

    if not ret_val:
        if words_count >= 6 and cmd_words[2] == "where":


            #                        Must     Add      Is
            #                       exists   Counter  Unique
            keywords = {
                        "company": ("str", False, False, True),
                        "dbms": ("str", False, False, True),
                        "table": ("str", False, False, True),
                    }


            ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3, bring_offset, keywords, False)
            if not ret_val:
                company_name = interpreter.get_one_value_or_default(conditions,"company", '*')
                dbms_name = interpreter.get_one_value_or_default(conditions,"dbms", '*')
                table_name = interpreter.get_one_value_or_default(conditions,"table", '*')
        elif (not bring_offset and words_count == 2) or bring_offset == 2:
            company_name = '*'
            dbms_name = '*'
            table_name = '*'
            ret_val = process_status.SUCCESS
        else:
            ret_val = process_status.ERR_command_struct

        if not ret_val:
            b_file = params.get_value_if_available("!blockchain_file")
            if b_file == "":
                message = "Missing dictionary definition for \'blockchain_file\'"
                status.add_keep_error(message)
                ret_val = process_status.No_local_blockchain_file
            else:
                if company_name == '*':
                    companies_list = metadata.get_companies_list()
                else:
                    companies_list = [company_name]

                for company in companies_list:
                    if dbms_name == '*':
                        dbms_list = metadata.get_dbms_list(company)
                    else:
                        dbms_list = [dbms_name]

                    if dbms_list:
                        for dbms in dbms_list:
                            if table_name == '*':
                                table_list = metadata.get_tables_list(company, dbms)
                            else:
                                table_list = [table_name]

                            for table in table_list:
                                if bring_offset:
                                    # with bring (determine the output info)
                                    operators_list += get_operators_json_by_table(status, b_file, company, dbms, table)
                                else:
                                    # Return IP + Port
                                    reply += (get_operators_ip_by_table(status, b_file, company, dbms, table) + "\r\n")

                if bring_offset:
                    if len(operators_list):
                        # pull the data out
                        ret_val, reply = utils_json.pull_info(status, operators_list, cmd_words[bring_offset + 1:], None, 0)

                elif not reply:
                    # not in the metadata - search in the blockchain file
                    reply = show_servers_for_dbms(status, dbms_name, table_name)
        else:
            ret_val = process_status.ERR_command_struct

    return [ret_val, reply]
# ------------------------------------------
# Get Views for dbms dbms_name
# ------------------------------------------
def get_views(status, io_buff_in, cmd_words, trace):

    offset = get_command_offset(cmd_words)

    if utils_data.test_words(cmd_words, 2 + offset, ["for", "dbms"]):

        logical_dbms = cmd_words[4 + offset]

        reply = db_info.get_database_views(status, logical_dbms)
        if reply == "":
            ret_val = process_status.No_metadata_info
        else:
            ret_val = process_status.SUCCESS
    else:
        ret_val = process_status.ERR_command_struct
        reply = ""

    return [ret_val, reply]

# ------------------------------------------
# Get the list of databases for the blockchain
# get network databases where company = *
# ------------------------------------------
def get_network_dbms(status, io_buff_in, cmd_words, trace):

    words_count = len(cmd_words)
    if words_count == 3 or (words_count == 7 and utils_data.test_words(cmd_words, 3, ["where", "company", "="])):
        if words_count == 7:
            company_name = cmd_words[6]
            if company_name == '*':
                # all databases from the blockchain
                where_cond = ""     # Bring all companies
            else:
                where_cond = "where company = %s" % company_name
        else:
            where_cond = ""

        cmd = "blockchain get table %s bring.unique ['table']['dbms'] separator = \\n" % where_cond
        ret_val, databases = blockchain_get(status, cmd.split(), None, True)

        if databases:
            dbms_list = []
            company_database_list = databases.split('\n')
            for company_dbms in company_database_list:
                index = company_dbms.find('.')
                if index > 0 and index < (len(company_dbms) - 1):
                    company_name = company_dbms[:index]
                    dbms_name = company_dbms[index + 1:]
                else:
                    company_name = "N/A"
                    dbms_name = company_dbms

                dbms_list.append((company_name, dbms_name))

            reply = utils_print.output_nested_lists(dbms_list,"",["Company", "DBMS"], True, "")
        else:
            reply = "No Table policies"

    else:
        ret_val = process_status.ERR_command_struct
        reply = None

    return [ret_val, reply]


# ------------------------------------------
# Get the list of columns for the table and dbms
# get columns where dbms = x and table = y
# ------------------------------------------
def get_columns(status, io_buff_in, cmd_words, trace):

    reply = None
    out_format = "table"

    if cmd_words[2] == "where":
        # get the conditions to execute the JOB
        #                                Must     Add      Is
        #                                exists   Counter  Unique

        keywords = {"dbms":     ("str", True, False, True),
                    "table":    ("str", True, False, True),
                    "format":   ("str", False, False, True),
                }

        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3, 0, keywords, False)
        if not ret_val:
            dbms_name = interpreter.get_one_value(conditions, "dbms")
            table_name = interpreter.get_one_value(conditions, "table")

            out_format = interpreter.get_one_value_or_default(conditions, "format", "table")
            schema_list = blockchain_select_schema(status, dbms_name, table_name)
            if not schema_list:
                ret_val = process_status.Local_table_not_in_blockchain
            else:
                if out_format == "json":
                    columns_dict = {}
                    for entry in schema_list:
                        columns_dict[entry[0]] = entry[1]
                    reply = utils_json.to_string(columns_dict)
                else:
                    # table format
                    reply = utils_print.output_nested_lists(schema_list, "Schema for DBMS: '%s' and Table: '%s'" % (dbms_name, table_name), ["Column Name", "Column Type"], True, "")
    else:
        ret_val = process_status.ERR_command_struct
        out_format = None

    return [ret_val, reply, out_format]


# ------------------------------------------
# Count the number of files stored in a blobs dbms
# Command: get files count where dbms = edgex and table = videos
# ------------------------------------------
def get_files_count(status, io_buff_in, cmd_words, trace):


    words_count = len(cmd_words)
    ret_val = process_status.SUCCESS

    if words_count == 3:
        # all databases and tables
        dbms_name = None
        table_name = None
    elif words_count <= 11 or cmd_words[3] == "where":
        # get the database name and table name (if available)
        keywords = {"dbms": ("str", True, False, False),
                    "table": ("str", False, False, False),
                    }

        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
        if ret_val:
            return [ret_val, None]

        dbms_name = interpreter.get_one_value_or_default(conditions, "dbms", None)
        table_name = interpreter.get_one_value_or_default(conditions, "table", None)

    else:
        ret_val = process_status.ERR_command_struct
        return [ret_val, None]


    blobs_dbms_list = []
    if not dbms_name:
        # get files count for all databases
        db_info.get_blobs_dbms_list(blobs_dbms_list)
    else:
        blobs_dbms_list.append(dbms_name)

    files_count_list = []
    for dbms_name in blobs_dbms_list:
        ret_val, counter, per_table_count = db_info.get_files_count(status, dbms_name, table_name)
        if ret_val:
            break

        if counter == 0:
            files_count_list.append([dbms_name, table_name, counter])  # Total per dbms and table
        else:
            files_count_list.append([dbms_name,"",counter])  # Total per dbms
            for table_name, files_counter in per_table_count.items():
                files_count_list.append(["", table_name, files_counter])  # Total per table

    reply = utils_print.output_nested_lists(files_count_list, "", ["DBMS Name", "Table Name", "Files Count"], True)

    return [ret_val, reply, "table"]

# ------------------------------------------
# Count rows maintained in tables managed by the current node
# get rows count
# get rows count where dbms = [dbms name] and format = json
# get rows count where dbms = [dbms name] and table = [table name] and group = table
# ------------------------------------------
def get_rows_count(status, io_buff_in, cmd_words, trace):

    keywords = {"dbms": ("str", False, False, False),
                "table": ("str", False, False, False),
                "group": ("str", False, False, False),      # group by table or partition
                "format": ("format", False, False, False),
                }

    words_count = len(cmd_words)
    ret_val = process_status.SUCCESS

    if words_count > 3:
        if words_count < 7 or cmd_words[3] != "where":
            status.add_error("missing 'where' keyword in 'get rows count' command")
            ret_val = process_status.ERR_command_struct
            return [ret_val, None]


        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
        if ret_val:
            return [ret_val, None]

        dbms_name = interpreter.get_one_value_or_default(conditions, "dbms", None)
        table_name = interpreter.get_one_value_or_default(conditions, "table", None)
        out_format = interpreter.get_one_value_or_default(conditions, "format", None)
        group_by = interpreter.get_one_value_or_default(conditions, "group", "partition")   # value is "table" or "partition"
    else:
        dbms_name = None
        table_name = None
        out_format = None
        group_by = "partition"

    if out_format and out_format == "json":
        json_struct = {}        # Organize in a JSON structure
        table_struct = None
    else:
        table_struct = []       # Organize in a table structure
        json_struct = None

    # Get the list of tables to consider

    if not dbms_name:
        # Get tables for all databases
        dbms_list = db_info.get_connected_databases()
        for d_name in dbms_list:
            if d_name == "system_query":
                continue    # Skip the system_query tables
            get_rows_in_dbms_tables(status, group_by, d_name, out_format, json_struct, table_struct)
    elif not table_name:
        # Get all tables for the specific database
        get_rows_in_dbms_tables(status, group_by, dbms_name, out_format, json_struct, table_struct)

    else:
        # Get a specific table in a database
        if table_name[:4] != "par_" and partitions.is_partitioned(dbms_name, table_name):
            # If partitioned table - get info on partitions
            par_str = db_info.get_table_partitions(status, dbms_name, table_name, "all")
            if par_str:
                # Get a list of partitions
                ret_val, par_list = partitions.par_str_to_list(status, dbms_name, table_name, par_str)
                d_name = dbms_name
                for partition_name in par_list:
                    par_name = partition_name["par_name"]
                    rows_count = db_info.get_rows_count(status, dbms_name, par_name)
                    if out_format == "json":
                        count_rows_json_format(json_struct, group_by, dbms_name, par_name, rows_count)
                    else:
                        count_rows_table_format(table_struct, group_by, dbms_name, d_name, par_name, rows_count)
                        d_name = ""
        else:
            if db_info.is_table_exists(status, dbms_name, table_name):
                rows_count = db_info.get_rows_count(status, dbms_name, table_name)
                if out_format == "json":
                    json_struct[dbms_name + '.' + table_name] = rows_count
                else:
                    table_struct.append([dbms_name, table_name, rows_count])


    if out_format == "json":
        reply = utils_json.to_string(json_struct)
    else:
        reply = utils_print.output_nested_lists(table_struct, "", ["DBMS Name", "Table Name", "Rows Count"], True)


    return [ret_val, reply, out_format]

# ------------------------------------------
# Get the number of rows for all tables in a database
# ------------------------------------------
def get_rows_in_dbms_tables(status, group_by, dbms_name, out_format, json_struct, table_struct):
    '''
    If the call specified a specific database, database name is not included in the reply
    '''

    tables_list = db_info.get_database_tables_list(status, dbms_name)

    if dbms_name.startswith("blobs_"):
        blobs_dbms = True
    else:
        blobs_dbms = False

    if tables_list and len(tables_list):
        d_name = dbms_name
        for entry in tables_list:
            t_name = entry["table_name"]
            rows_count = db_info.get_rows_count(status, dbms_name, t_name)
            if not rows_count and blobs_dbms:
                continue        # Not returning 0 rows from blobs DBMS
            if out_format == "json":
                count_rows_json_format(json_struct, group_by, dbms_name, t_name, rows_count)
            else:
                count_rows_table_format(table_struct, group_by, dbms_name, d_name, t_name, rows_count)
                d_name = ""
# ------------------------------------------
# Update a Table structure with the output
# ------------------------------------------
def count_rows_table_format(table_struct, group_by, dbms_name, d_name, par_name, rows_count):
    '''
    dbms_name - the dbms name
    d_name - the printed dbms name (can be empty string)
    '''
    if group_by == "partition":
        # rows are presented for each partition
        table_struct.append([d_name, par_name, rows_count])
    else:
        # rows are presented for each table - group by table
        is_table, table_name = partitions.get_table_name_by_par_name(par_name)
        if is_table:
            key = 'par_' + table_name
            if len(table_struct) and d_name == "" and table_struct[-1][1] == key:
                table_struct[-1][2] += rows_count
            else:
                table_struct.append([d_name, key, rows_count])
        else:
            table_struct.append([d_name, par_name, rows_count])
# ------------------------------------------
# Update a JSON structure with the output
# ------------------------------------------
def count_rows_json_format(json_struct, group_by, dbms_name, par_name, rows_count):
    if group_by == "partition":
        # rows are presented for each partition
        json_struct[dbms_name + '.' + par_name] = rows_count
    else:
        # rows are presented for each table - group by table
        is_table, table_name = partitions.get_table_name_by_par_name(par_name)
        if is_table:
            key = dbms_name + '.par_' + table_name
            if key in json_struct:
                json_struct[key] += rows_count
            else:
                json_struct[key] = rows_count
        else:
            json_struct[dbms_name + '.' + par_name] = rows_count
# ------------------------------------------
# Get info on a particular table
# get table [exist status/local status/blockchain status/rows count] where name = table_name and dbms = dbms_name
#  get table exist status where name = ping_sensor and dbms = anylog
#  get table rows count where name = ping_sensor and dbms = anylog
#  get table complete status where name = ping_sensor and dbms = anylog
# ------------------------------------------
def get_table(status, io_buff_in, cmd_words, trace):

    ret_val = process_status.SUCCESS
    reply = None
    info_struct = {}

    offset = get_command_offset(cmd_words)
    if utils_data.test_words(cmd_words, 4 + offset, ["where", "dbms", "="]) and utils_data.test_words(cmd_words, 8 + offset, ["and", "name", "="]):
        table_name = cmd_words[offset + 11]
        dbms_name = cmd_words[offset + 7]
    elif utils_data.test_words(cmd_words, 4 + offset, ["where", "name", "="]) and utils_data.test_words(cmd_words, 8 + offset, ["and", "dbms", "="]):
        table_name = cmd_words[offset + 7]
        dbms_name = cmd_words[offset + 11]
    else:
        ret_val = process_status.ERR_command_struct

    if not ret_val:
        instruct1 = cmd_words[offset + 2]
        instruct2 = cmd_words[offset + 3]
        if (instruct1 == 'exist' or instruct1 == 'local' or instruct1 == "complete") and instruct2 == "status" :
            ret_code = db_info.is_table_exists(status, dbms_name, table_name)
            info_struct["local"] =ret_code


        if (instruct1 == 'exist' or instruct1 == 'blockchain' or instruct1 == "complete") and instruct2 == "status":
            cmd = "blockchain get table where dbms = %s and name = %s" % (dbms_name, table_name)

            ret_val, table_info = blockchain_get(status, cmd.split(), None, True)
            if not ret_val and len(table_info):
                info_struct["blockchain"] = "True"
            else:
                info_struct["blockchain"] = "False"

        if (instruct1 == 'rows' and cmd_words[offset + 3] == 'count') or (instruct1 == "complete" and instruct2 == "status"):
            if table_name[:4] != "par_" and partitions.is_partitioned(dbms_name, table_name):
                # If partitioned table - get info on partitions
                par_str = db_info.get_table_partitions(status, dbms_name, table_name, "all")
                if par_str:
                    # Get a list of partitions
                    ret_val, par_list = partitions.par_str_to_list(status, dbms_name, table_name, par_str)
                    rows_count = 0
                    for partition_name in par_list:
                        par_name = partition_name["par_name"]
                        rows_count += db_info.get_rows_count(status, dbms_name, par_name)
                    info_struct["rows"] = rows_count
                    info_struct["partitions"] = len(par_list)
            else:
                if db_info.is_table_exists(status, dbms_name, table_name):
                    rows_count = db_info.get_rows_count(status, dbms_name, table_name)
                    info_struct["rows"] = rows_count
                    info_struct["partitions"] = 0

        reply = utils_json.to_string(info_struct)

    return [ret_val, reply, "json"]
# ------------------------------------------
# Get tables where dbms dbms_name
# Get tables where dbms = x and format = json
# ------------------------------------------
def get_tables(status, io_buff_in, cmd_words, trace):

    words_count = len(cmd_words)

    offset = get_command_offset(cmd_words)

    if words_count - offset == 10:
        # get tables where dbms = x and format = json
        print_format = get_reply_format(status, cmd_words, words_count - 3)
    elif words_count - offset == 6:
        print_format = "table"
    else:
        return [process_status.ERR_command_struct, None]

    if utils_data.test_words(cmd_words, 2 + offset, ["where", "dbms", "="]):
        # Get all the tables defined on  the local database and all the tables of the database on the blockchain
        if print_format == "json":
            dbms_table_dict= {}
        else:
            dbms_table_list = []

        # Get from the local database
        logical_dbms = cmd_words[5 + offset]
        if logical_dbms == '*':
            # all databases from the blockchain
            cmd = "blockchain get table bring.unique ['table']['dbms'] separator = \\n"
            ret_val, databases = blockchain_get(status, cmd.split(), None, True)
            if not ret_val and databases:
                database_list = databases.split('\n')
                for index, database_name in enumerate(database_list):
                    if print_format == "json":
                        table_dict = tables_per_dbms_dict(status, database_name)
                        dbms_table_dict[database_name] = table_dict
                    else:
                        tables_per_dbms_list(status, database_name, dbms_table_list)
        else:
            if print_format == "json":
                table_dict = tables_per_dbms_dict(status, logical_dbms)
                dbms_table_dict[logical_dbms] = table_dict
            else:
                tables_per_dbms_list(status, logical_dbms, dbms_table_list)
            ret_val = process_status.SUCCESS

        if print_format == "json":
            reply = utils_json.to_string(dbms_table_dict)
        else:
            reply = utils_print.output_nested_lists(dbms_table_list, "", ["Database", "Table name", "Local DBMS", "Blockchain"], True)
    else:
        ret_val = process_status.ERR_command_struct
        reply = None

    return [ret_val, reply, print_format]
# ------------------------------------------
# Get tables in dbms - organize in a dictionary
# ------------------------------------------
def tables_per_dbms_dict(status, logical_dbms):

    tables_defined = {}  # The key is the table name and the value determine if defined on the local dbms or blockchain or both

    tables_list = db_info.get_database_tables_list(status, logical_dbms)

    if tables_list and len(tables_list):
        for entry in tables_list:
            if "table_name" in entry.keys():
                tables_defined[entry["table_name"]] = {"local": True}  # Flag that the table exists in the local database

    # Get from the blockchain
    tables_list = get_tables_for_dbms(status, logical_dbms)
    for entry in tables_list:
        if entry in tables_defined.keys():
            tables_defined[entry]["blockchain"] = True
        else:
            tables_defined[entry] = {"blockchain": True}

    for entry in tables_defined.values():
        if "local" not in entry:
            entry["local"] = False
        elif "blockchain" not in entry:
            entry["blockchain"] = False

    return tables_defined
# ------------------------------------------
# Query the list of tables locally and on the blockchain for each database
# Organize result in a list
# ------------------------------------------
def tables_per_dbms_list(status, logical_dbms, dbms_table_list):

    tables_defined = {}  # The key is the table name and the value determine if defined on the local dbms or blockchain or both

    tables_list = db_info.get_database_tables_list(status, logical_dbms)

    if tables_list and len(tables_list):
        for entry in tables_list:
            if "table_name" in entry.keys():
                tables_defined[entry[
                    "table_name"]] = 1  # Flag that the table exists in the local database

    # Get from the blockchain
    tables_list = get_tables_for_dbms(status, logical_dbms)
    for entry in tables_list:
        if entry in tables_defined.keys():
            tables_defined[entry] = 2  # On both - local dbms and blockchain
        else:
            tables_defined[entry] = 3

    if not len(tables_defined):
        ret_val = process_status.No_metadata_info
        reply = ""
    else:
        for index, entry in enumerate(tables_defined):
            if not index:
                dbms_name = logical_dbms        # Place the dbms name only once
            else:
                dbms_name = ""
            if tables_defined[entry] == 1:
                local_dbms = ' V'
                blockchain_dbms = ' -'
            elif tables_defined[entry] == 2:
                local_dbms = ' V'
                blockchain_dbms = ' V'
            else:
                local_dbms = ' -'
                blockchain_dbms = ' V'
            dbms_table_list.append((dbms_name, entry, local_dbms, blockchain_dbms))

# ------------------------------------------
# Get Error text - translate the error code to text
# Command: get error 32
# ------------------------------------------
def err_to_txt(status, io_buff_in, cmd_words, trace):
    words_count = len(cmd_words)
    offset = get_command_offset(cmd_words)
    if words_count != (offset + 3):
        reply = ""
        ret_val = process_status.ERR_command_struct
    else:
        ret_val = process_status.SUCCESS
        err_value = cmd_words[2 + offset]
        if err_value.isdecimal():
            reply = "Error text: \"%s\"" % process_status.get_status_text(int(err_value))
        else:
            reply = "Command 'get error' failed: error value '%s' is not decimal" % err_value
    return [ret_val, reply]
# ------------------------------------------
# Get Info from the file:
# Command: get file timestamp file_name
# ------------------------------------------
def get_file_info(status, io_buff_in, cmd_words, trace):
    words_count = len(cmd_words)
    offset = get_command_offset(cmd_words)
    if words_count != (offset + 4) or cmd_words[2 + offset] != "timestamp":
        reply = ""
        ret_val = process_status.ERR_command_struct
    else:
        ret_val = process_status.SUCCESS
        file_name = params.get_value_if_available(cmd_words[3 + offset])
        timestamp = utils_io.get_file_modified_time(file_name)
        reply = utils_columns.seconds_to_date(timestamp)
    return [ret_val, reply]
# ------------------------------------------
# Get the Logged data
# Example: get event log where format = json and keys = error query
# ------------------------------------------
def get_logged_data(status, io_buff_in, cmd_words, trace):
    '''
    Returns the log info in a table format or JSON format
    If command includes keys, only events containing the keys are returned
    '''

    offset = get_command_offset(cmd_words)
    log_type = cmd_words[offset + 1]
    offset += 3
    words_count = len(cmd_words)
    ret_val = process_status.SUCCESS
    out_format = "table"  # The default output format
    reply = None

    if words_count > offset:
        # Get the output format
        if cmd_words[offset] != "where" or words_count < offset + 4:
            ret_val = process_status.ERR_command_struct
        else:
            out_format = get_reply_format(status, cmd_words, offset + 1)
            if not out_format:
                offset += 1
            else:
                if  out_format != "json":
                    out_format = "table"
                offset += 4
                if words_count > offset:
                    if cmd_words[offset] != "and" or words_count < offset + 4:
                        ret_val = process_status.ERR_command_struct
                    else:
                        offset += 1     # skip the "and"


            # get the keywords offset
            if not ret_val and words_count > offset:
                # has keywords
                if words_count < offset + 3:
                    # Needs to be: keys = keyword ...
                    ret_val = process_status.ERR_command_struct
                else:
                    if (cmd_words[offset] == "keys" or cmd_words[offset] == "key") and cmd_words[offset+1] == "=":
                        offset += 2     # offset to the keywords
                    else:
                        ret_val = process_status.ERR_command_struct

    if not ret_val:

        log_exists, reply = process_log.show_events(log_type, cmd_words, out_format, offset)
        if not log_exists:
            ret_val = process_status.ERR_command_struct  # no such dictionary
        else:
            ret_val = process_status.SUCCESS

    return [ret_val, reply]
# ------------------------------------------
# Get the dictionary variables and their assigned values
# get dictionary where format = json
# ------------------------------------------
def get_dictionary_values(status, io_buff_in, cmd_words, trace):
    ret_val = process_status.SUCCESS

    words_count = len(cmd_words)
    if words_count == 2 or (words_count == 6 and utils_data.test_words(cmd_words, 2, ["where", "format", "=", "table"])):
        reply = params.get_dict_as_table(None)
        format_type = "table"
    elif words_count == 6 and utils_data.test_words(cmd_words, 2, ["where", "format", "=", "json"]):
        reply = params.get_dict_as_json(None)
        format_type = "json"        # print as JSON
    elif words_count == 3:
        # Find dictionary values where key includes the substring
        substr = params.get_value_if_available(cmd_words[2])
        reply = params.get_dict_as_table(substr)
        format_type = "table"
    elif words_count == 7 and utils_data.test_words(cmd_words, 3, ["where", "format", "="]):
        # Find dictionary values where key includes the substring
        substr = params.get_value_if_available(cmd_words[2])
        format_type = cmd_words[6]
        if format_type == "json":
            reply = params.get_dict_as_json(substr)
        else:
            reply = params.get_dict_as_table(substr)
            format_type = "table"
    else:
        reply = None
        ret_val = process_status.ERR_command_struct
        format_type = "table"
    return [ret_val, reply, format_type]
# ------------------------------------------
# Get the environment variables and their assigned values
# get environment variables where format = json
# ------------------------------------------
def get_environment_variables(status, io_buff_in, cmd_words, trace):
    ret_val = process_status.SUCCESS

    words_count = len(cmd_words)
    if words_count == 3 or (words_count == 7 and utils_data.test_words(cmd_words, 3, ["where", "format", "=", "table"])):
        format_type = "table"
    elif words_count == 7 and utils_data.test_words(cmd_words, 3, ["where", "format", "=", "json"]):
        format_type = "json"        # print as JSON
    else:
        reply = None
        ret_val = process_status.ERR_command_struct
        format_type = "table"

    if not ret_val:
        reply = params.environment_var(format_type)

    return [ret_val, reply, format_type]

# ------------------------------------------
# Get the list of databases
# get databases
# get databases where format = json
# ------------------------------------------
def get_databases(status, io_buff_in, cmd_words, trace):
    out_format = "table"
    if db_info.count_dbms() == 0:
        reply = "No DBMS connections found\r\n"
    else:
        offset = get_command_offset(cmd_words)
        if len(cmd_words) > (2 + offset):
            # get databases where format = json
            out_format = "json"
            db_dict = db_info.get_dbms_dict()
            reply = utils_json.to_string(db_dict)
        else:
            reply = db_info.get_dbms_list(status)


    return [process_status.SUCCESS, reply, out_format]

# ------------------------------------------
# Get the Info from the message queue
# Command: get echo queue
# ------------------------------------------
def get_echo_queue(status, io_buff_in, cmd_words, trace):

    offset = get_command_offset(cmd_words)

    if len(cmd_words) == (3 + offset) and cmd_words[2 + offset] == "queue":
        ret_val = process_status.SUCCESS
        if not echo_queue:
            reply = "Message Queue not declared"
        else:
            reply = echo_queue.get_mssages()
    else:
        ret_val = process_status.ERR_command_struct
        reply = ""

    return [ret_val, reply]

# ------------------------------------------
# get compression
# Returns on/off to determine if active
# ------------------------------------------
def get_compression(status, io_buff_in, cmd_words, trace):
    global message_compression_

    if message_compression_:
        reply = "Node Compression: On"
    else:
        reply = "Node Compression: Off"

    return [process_status.SUCCESS, reply]
# ------------------------------------------------
# get cluster info - info on participating operators and tables supported
# ------------------------------------------------
def get_cluster_info(status, io_buff_in, cmd_words, trace):

    blockchain_load(status, ["blockchain", "get", "cluster"], False, 0)
    reply = metadata.show_info(status)
    return [process_status.SUCCESS, reply]

# ----------------------------------------------------------
# Delete files older than the specified number of days
# Example: delete archive where days = 60
# ----------------------------------------------------------
def delete_archive(status, io_buff_in, cmd_words, trace):

    num_of_days = params.get_value_if_available(cmd_words[5])
    if isinstance(num_of_days, str):
        if not num_of_days.isdigit():
            return process_status.ERR_command_struct
        days = int(num_of_days)
    elif isinstance(num_of_days, int): # i.e: delete archive where days = !archive_delete.int
        days = num_of_days
    else:
        return process_status.ERR_command_struct

    if not utils_data.test_words(cmd_words, 2, ["where", "days", "="]):
        return process_status.ERR_command_struct

    ret_val = process_status.SUCCESS


    unix_time_seconds = utils_columns.get_current_time_in_sec()
    unix_time_seconds -= (days * 86400)      # 60 * 60 * 24 = 86400 seconds in a day
    path_searator = params.get_path_separator()
    date_format =  f"{path_searator}%y{path_searator}%m{path_searator}%d"
    keep_date = utils_columns.get_utc_time_from_seconds(unix_time_seconds, date_format)      # The day that terminates the process
    if not keep_date:
        ret_val = process_status.ERR_process_failure
    else:
        dir_root = params.get_path("!archive_dir")
        if dir_root:
            ret_val, root_length, directory_list = utils_io.get_directories_list(status, dir_root)
            if not ret_val:
                # Delete old directories (every dir represents a day)
                for entry in directory_list:
                    directory_name = entry[root_length:]
                    if keep_date <= directory_name:
                        break       # Stop delete
                    dir_name_length = len(directory_name)
                    if dir_name_length != 9:
                        if directory_name >= keep_date[:dir_name_length]:
                            continue        # Only delete directories of previous month or year to the keep_date month and year
                    if not utils_columns.is_valid_date(directory_name, date_format[:dir_name_length]):
                        # Should be /yy/mm/dd - otherwise some error
                        status.add_error("Delete archive command encountered wrong directory name (Not a proper date format): '%s'" % entry)
                        ret_val = process_status.Wrong_directory_name
                        break

                    ret_val = utils_io.delete_dir_with_content(status, entry)
                    if ret_val:
                        break

    return ret_val
# ----------------------------------------------------------
# Return a list of file in a given directory
# ----------------------------------------------------------
def get_files_list(status, io_buff_in, cmd_words, trace):

    words_count = len(cmd_words)
    if  words_count == 3:
        ret_val = process_status.SUCCESS
        directory_name = cmd_words[2]
        reply = utils_io.get_from_dir(status, "file", directory_name)
    elif words_count >= 6 and cmd_words[2] == "where":
        # get files where dbms = blobs_edgex and table = image
        ret_val, reply = get_list_from_storage(status, io_buff_in, cmd_words, trace)
    else:
        ret_val = process_status.ERR_command_struct
        reply = ""
    return [ret_val, reply]
# ----------------------------------------------------------
# Get the status of the query on the operator node
# Example: get operator execution where node = 10.0.0.78 and job = 12
# ----------------------------------------------------------
def get_operator_exec(status, io_buff_in, cmd_words, trace):

    global job_process_stat_

    words_count = len(cmd_words)
    if words_count == 3:
        # Bring all
        source_ip = '*'
        job_location = -1
    elif words_count >= 7 and cmd_words[3] == "where":
        #                               Must     Add      Is
        #                               exists   Counter  Unique
        keywords = {"node": ("str", False, False, True),
                    "job": ("int", False, False, False),
                    }

        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words,
                                                                       4, 0, keywords,
                                                                       False)

        if ret_val:
            return [ret_val, None, None]
        source_ip = interpreter.get_one_value_or_default(conditions, "node", "*")
        job_location = interpreter.get_one_value_or_default(conditions, "job", -1)
    else:
        return [process_status.ERR_command_struct, None, None]

    # Get data based on the info collected at update_thread_query_done()
    reply = ""
    info_list = []

    counter_listed = 0

    for node, query_status in job_process_stat_.items():
        if source_ip != '*' and source_ip != node:
            continue        # This node is not requested
        for query_id, query_details in query_status.items():
            if job_location != -1 and job_location != query_id:
                continue  # This query is not requested

            counter_listed += 1

            query_counter = query_details["id"]
            rows_count = query_details["rows_count"]
            limit = query_details["limit"]
            if not "threads" in query_details:
                break       # Query was not yet set by the query thread
            threads_participating = query_details["threads"]
            threads_done = query_details["done"]
            thread_info = query_details["info"]
            for entry in thread_info:
                dbms_name = entry[0]
                table_name = entry[1]
                par_id = entry[2]
                par_name = entry[3]
                error_code = entry[4]
                block_counter = entry[5]
                rows_to_transfer = entry[6]
                sql_time = entry[7]
                fetch_time = entry[8]
                network_time = entry[9]

                hours_time, minutes_time, seconds_time = utils_data.seconds_to_hms(sql_time)
                sql_time_formatted = "%02u:%02u:%02u" % (hours_time, minutes_time, seconds_time)
                hours_time, minutes_time, seconds_time = utils_data.seconds_to_hms(fetch_time)
                fetch_time_formatted = "%02u:%02u:%02u" % (hours_time, minutes_time, seconds_time)
                hours_time, minutes_time, seconds_time = utils_data.seconds_to_hms(network_time)
                network_time_formatted = "%02u:%02u:%02u" % (hours_time, minutes_time, seconds_time)

                info_list.append((node, query_id, query_counter, rows_count, limit,  threads_participating, threads_done,\
                                  dbms_name, table_name, par_id, par_name, error_code, block_counter, rows_to_transfer,\
                                  sql_time_formatted, fetch_time_formatted, network_time_formatted))

                title = ["Node", "Job", "ID", "Rows", "limit", "threads", "Completed", "DBMS", "Table", "Par ID", "Par Name",\
                         "Error", "blocks", "Rows", "SQL Time", "Fetch Time", "Network Time"]

                reply = utils_print.output_nested_lists(info_list, "", title, True)
                node = ""
                query_id = ""
                query_counter = ""
                rows_count = ""
                limit = ""
                threads_participating = ""
                threads_done = ""


    if not counter_listed:
        reply = "No info satisfies the 'get operator execution' command"

    return [process_status.SUCCESS, reply, "json"]

# ----------------------------------------------------------
# Return a list of directories in a given directory
# ----------------------------------------------------------
def get_dir_list(status, io_buff_in, cmd_words, trace):

    if len(cmd_words) == 3:
        ret_val = process_status.SUCCESS
        directory_name = cmd_words[2]
        reply = utils_io.get_from_dir(status, "dir", directory_name)
    else:
        ret_val = process_status.ERR_command_struct
        reply = ""
    return [ret_val, reply]

# ----------------------------------------------------------
# Return the size of the directory
# ----------------------------------------------------------
def get_directory_size(status, io_buff_in, cmd_words, trace):

    dir_list = []       # appended with directories and size
    dir_name = params.get_value_if_available(cmd_words[2])

    if not dir_name:
        if len(cmd_words[2]) and cmd_words[2][0] == '!':
            message = "No value is assigned to %s" % cmd_words[2]
        else:
            message = "Root directory is not recognized"

        status.add_error(message)
        ret_val = process_status.ERR_process_failure
    else:
        ret_val, counter, total = utils_io.get_directory_size(status, dir_name, dir_list, True)

    if not ret_val:
        title = ["Directory", "Files", "Size (Bytes)"]
        reply = utils_print.output_nested_lists(dir_list, "", title, True)
    else:
        reply = None

    return [ret_val, reply]

# ------------------------------------------------
# TSD sync status of all the nodes in the network
# Info of the tsd tables or on a particular tsd table
# and how it is represented on each node of the cluster,
# The sync process supports the HA processes.
# Example: get ha cluster status
# Example: get ha cluster status where table = tsd_128
# The info is based on messages between the peers supporting the cluster
# ------------------------------------------------
def get_ha_cluster_stat(status, io_buff_in, cmd_words, trace):

    ret_val = process_status.SUCCESS
    if not version.alconsumer_is_consumer_running():
        # The consumer process interacts with the servers that process data on the same cluster.
        # If consumer is not enabled - there is no info on the status
        reply = "Info on TSD not available - enable Consumer process"
    else:
        words_count = len(cmd_words)

        if words_count == 4:
            tsd_table = "all"
        elif words_count == 8 and utils_data.test_words(cmd_words, 4, ["where", "table", "="]):
            # get tsd sync status where table = tsd_128
            tsd_table = cmd_words[7]  # tsd table name or "all" for all tables
        else:
            ret_val = process_status.ERR_command_struct
            reply = ""

        if not ret_val:
            nodes_safe_ids = metadata.get_safe_ids(tsd_table) # Get the safe IDs for each TSD table on this node
            ha.add_peers_safe_ids(nodes_safe_ids, tsd_table)

            status_table = []
            title = ["TSD table", "Consensus"]
            for tsd_table, members in nodes_safe_ids.items():

                status_table.append([tsd_table, members["consensus"]]) # Add TSD table name
                # Add the Safe ID on each Peer

                for member_id, safe_id in members.items():
                    if member_id == "consensus":
                        continue
                    try:
                        index = title.index(member_id)
                    except:
                        title.append(member_id)  # add a ne member (node)
                        index = len(title) - 1

                    nodes_count = len(title)            # Number of nodes participoating in all TSD tables
                    for tsd_entry in status_table:
                        while len(tsd_entry) <  nodes_count:
                            tsd_entry.append(0)        # Add column for member

                    status_table[-1][index] = safe_id          # Add the safe ID to the lst column


            reply = utils_print.output_nested_lists(status_table, f"Status by member {metadata.get_node_member_id()}", title, True)



    return [ret_val, reply]

# ------------------------------------------------
# Get the status of one of the threads pool
# get operator pool where details = true and reset = true/false
# works on: Query, TCP, REST, MSG, Operator
# ------------------------------------------------
def get_pool_status(status, io_buff_in, cmd_words, trace):
    reply = ""
    ret_val = process_status.SUCCESS

    pool_name = cmd_words[1]
    if pool_name == "operator":
        threads_obj = aloperator.get_threads_obj()
    elif pool_name == "query":
        threads_obj = workers_pool
    elif pool_name == "rest":
        threads_obj = http_server.get_threads_obj()
    elif pool_name == "tcp":
        threads_obj = tcpip_server.get_threads_obj()
    elif pool_name == "msg":
        threads_obj = message_server.get_threads_obj()
    else:
        ret_val = process_status.ERR_command_struct

    if not ret_val:
        words_count = len(cmd_words)
        if words_count > 3:
            if cmd_words[3] != "where" and words_count < 7:
                ret_val = process_status.ERR_command_struct
            else:
                keywords = {"details": ("str", False, False, True),
                            "reset": ("str", False, False, True),
                            }

                ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words,
                                                                               4, 0,
                                                                               keywords,
                                                                               False)
                if not ret_val:
                    with_details = interpreter.get_one_value_or_default(conditions, "details", False)
                    with_reset = interpreter.get_one_value_or_default(conditions, "reset", False)   # Reset stat
        else:
            with_details = False
            with_reset = False

    if not ret_val:
        if threads_obj:
            reply = threads_obj.get_info(with_details, with_reset)
        else:
            reply = pool_name[0].upper() + f"{pool_name[1:]} Pool is not declared"

    return [process_status.SUCCESS, reply]
# ------------------------------------------------
# Get the software version
# ------------------------------------------------
def get_version(status, io_buff_in, cmd_words, trace):

    code_version = node_info.get_version(status)

    reply = f"EdgeLake Version: {code_version}"   # Includes git version and date

    return [process_status.SUCCESS, reply]

# ------------------------------------------------
# Get the Github version
# get github version !network_dir
# ------------------------------------------------
def get_git_version(status, io_buff_in, cmd_words, trace):

    ret_val = process_status.SUCCESS
    reply_str = ""
    words_count = len(cmd_words)

    if cmd_words[2] == "version":
        is_version = True
    elif cmd_words[2] == "info":
        is_version = False
    else:
        ret_val = process_status.ERR_command_struct

    if not ret_val:
        if words_count == 3:
            directory = params.get_value_if_available("!edgelake_path")
        elif words_count == 4:
            directory = params.get_value_if_available(cmd_words[3])
        else:
            ret_val = process_status.ERR_command_struct

        if not ret_val:
            # Pull info from github
            github_cmd = 'git -C "%s/EdgeLake" log -1' % directory
            ret_val, github_str = system_call(status, github_cmd, 5)
            if not ret_val:
                # Format the info
                if is_version:
                    if len(github_str) > 12 and github_str.startswith("commit "):
                        reply_str = github_str[7:12]
                    else:
                        reply_str = github_str
                else:
                    if github_str:
                        entries = github_str.split("\r\n")
                        if len(entries):
                            if entries[0].startswith("commit "):
                                reply_str = entries[0]
                            if len(entries) > 2:
                                if entries[2].startswith("Date: "):
                                    reply_str += "\r\n" + entries[2] + "\r\n"

    return [ret_val, reply_str]

# --------------------------------------------------------------
# Get the query config params
# --------------------------------------------------------------
def get_query_params(status, io_buff_in, cmd_words, trace):
    reply = utils_print.format_dictionary(query_mode, True, False, False, None)
    return [process_status.SUCCESS, reply]

# ----------------------------------------------------------
# show json file structure
# ----------------------------------------------------------
def get_json_file_struct(status, io_buff_in, cmd_words, trace):
    reply = "[dbms name].[table name].[data source].[hash value].[mapping policy].[TSD member ID].[TSD row ID].[TSD date].json"
    return [process_status.SUCCESS, reply]

# ------------------------------------------------------------------------
# For each commands dictionary - set the max number of words to consider as a key
# ------------------------------------------------------------------------
def set_max_cmd_words(cmd_dict: dict):  # Returns the max words for each disctionary

    # find max words in self.commands
    max_words = 0
    for cmd, child_dict in cmd_dict.items():
        if 'methods' in child_dict:
            # A child dictionary
            child_dict["max_words"] =  set_max_cmd_words(child_dict["methods"])     # Add a key at the parent with max key words of the child
        counted = cmd.count(' ') + 1
        if counted > max_words:
            max_words = counted

    return max_words

# ------------------------------------------------------------------------
# set query log on
# set query log off
# [optional] profile 2 seconds
# Example: set query log profile 2 seconds
# ------------------------------------------------------------------------
def set_query_log(status, io_buff_in, cmd_words, trace):

    ret_val = process_status.SUCCESS
    words_count = len(cmd_words)
    if words_count == 4:
        # log all queries
        if cmd_words[3] == "on":
            job_instance.query_log_time = 0  # log all queries
        elif cmd_words[3] == "off":
            job_instance.query_log_time = -1  # disable the log
        else:
            ret_val = process_status.ERR_command_struct
    elif words_count == 6 and cmd_words[3] == "profile" and cmd_words[4].isdecimal() and (
        cmd_words[5] == "seconds" or cmd_words[5] == "second"):
        job_instance.query_log_time = int(cmd_words[4])
    else:
        ret_val = process_status.ERR_command_struct

    return ret_val
# ------------------------------------------------------------------------
# Assigns a string to a function key
# Example: set function key f5 "system ls"
# ------------------------------------------------------------------------
def set_function_key(status, io_buff_in, cmd_words, trace):

    ret_val = process_status.ERR_command_struct
    words_count = len(cmd_words)
    if words_count == 5:
        if cmd_words[3][0] == 'f' and cmd_words[3][1:].isdecimal():
            value = int(cmd_words[3][1:])
            if value >= 3 and value <= 8:
                input_kbrd.add_function_key(value, " ".join(cmd_words[4:]))
                ret_val = process_status.SUCCESS

    return ret_val

# ------------------------------------------------------------------------
# Set debug on/off/interactive
# ------------------------------------------------------------------------
def set_debug_options(status, io_buff_in, cmd_words, trace):

    words_count = len(cmd_words)

    if (words_count == 3 or words_count == 4) and cmd_words[1] == "debug":
        ret_val = process_status.SUCCESS
        # [set debug on] or [set debug off] or [set debug interactive]
        # set the print of a script while running to on or off
        if words_count == 3:
            # debug current thread
            thread_name = threading.current_thread().name
        else:
            thread_name = cmd_words[3]  # provide the thread name

        script_mutex.acquire()  # lock the array

        if thread_name in running_scripts.keys():

            scripts_array = running_scripts[thread_name]

            if cmd_words[2] == "off":
                debug = False
                debug_interactive = False
                debug_next = False
            elif cmd_words[2] == "on":
                debug = True
                debug_interactive = False
                debug_next = False
            elif cmd_words[2] == "interactive":
                if params.is_input_thread(thread_name):
                    # debug interactive is not doable on main thread is
                    status.add_error("Main thread does not support 'debug interactive', call the scripty using 'thread' command")
                    ret_val = process_status.Not_suppoerted_on_main_thread
                else:
                    debug_interactive = True
                debug = True
                debug_next = False
            else:
                ret_val = process_status.ERR_command_struct

            if not ret_val:
                # set all entries in the array of the thread
                for entry in scripts_array:
                    entry.set_debug(debug)
                    entry.set_debug_interactive(debug_interactive)
                    entry.set_debug_next(debug_next)
        else:
            ret_val = process_status.The_thread_has_no_script

        script_mutex.release()
    else:
        ret_val = process_status.ERR_command_struct

    return ret_val

# --------------------------------------------------------------
# Set MQTT debug ON / OFF
# --------------------------------------------------------------
def set_mqtt_debug(status, io_buff_in, cmd_words, trace):

    ret_val = process_status.SUCCESS
    words_count = len(cmd_words)

    if words_count == 4:
        # set mqtt debug on / off
        debug = cmd_words[3]
        if debug == "on":
            mqtt_client.set_debug(True)
        elif debug == "off":
            mqtt_client.set_debug(False)
        else:
            ret_val = process_status.ERR_command_struct
    else:
        ret_val = process_status.ERR_command_struct
    return ret_val

# --------------------------------------------------------------
# Set a local message Queue to buffer echo messages
# Set echo queue [on][off]
# --------------------------------------------------------------
def set_echo_queue(status, io_buff_in, cmd_words, trace):
    global echo_queue

    ret_val = process_status.SUCCESS
    words_count = len(cmd_words)

    if words_count == 4:

        on_off = cmd_words[3]
        if on_off == "on":

            if echo_queue:
                queue_size = echo_queue.get_queue_size()
                if not queue_size:
                    queue_size = 20
            else:
                queue_size = 20

            echo_queue = utils_queue.MsgQueue(queue_size)
            echo_queue.update_prompt(False)  # Indicate the buffer is empty (prompt removes the + sign)
        elif on_off == "off":
            if echo_queue:
                echo_queue.update_prompt(False)  # Indicate the buffer is empty (prompt removes the + sign)
                echo_queue = None
        else:
            ret_val = process_status.ERR_command_struct
    else:
        ret_val = process_status.ERR_command_struct
    return ret_val

# --------------------------------------------------------------
# set traceback on / off
# set traceback on "which is not a member of the cluster"
# --------------------------------------------------------------
def set_traceback(status, io_buff_in, cmd_words, trace):

    words_count = len(cmd_words)

    ret_val = process_status.SUCCESS
    if cmd_words[2] == "on":
        if words_count == 3:
            process_status.with_traceback_ = True
            process_status.traceback_text_ = ""    # All error calls will show traceback
        elif words_count == 4:
            process_status.with_traceback_ = True
            process_status.traceback_text_ = cmd_words[3]  # only error calls that include the text provided
        else:
            ret_val = process_status.ERR_command_struct
    elif words_count == 3 and cmd_words[2] == "off":
        process_status.traceback_text_ = ""
        process_status.with_traceback_ = False
    else:
        ret_val = process_status.ERR_command_struct
    return ret_val

# --------------------------------------------------------------
# Reply to a different address
# command: set reply ip = [ip] / dynamic
# or:      set reply ip and port = 24.23.250.144:4078

# Reply to a different address
# set reply ip  = [ip:port] / dynamic   - When a message is send, place "0.0.0.0" as the reply address
# set self ip  = [ip:port] / dynamic   -  when a message is send to the local ip and port, replace with the ip value at !ip
# --------------------------------------------------------------
def set_replacement_ip(status, io_buff_in, cmd_words, trace):
    ret_val = process_status.SUCCESS

    words_count = len(cmd_words)
    if words_count == 5:
        # Port not provided
        if cmd_words[3] != '=':
            ret_val = process_status.ERR_command_struct
        else:
            ip_str = params.get_value_if_available(cmd_words[4]).lower()
            port_val = 0
            with_port = False
    elif words_count == 7:
        # port provided
        if cmd_words[5] != '=':
            ret_val = process_status.ERR_command_struct
        else:
            ip_port = cmd_words[6]
            index = ip_port.find(':')
            if index > 1 and index < (len(ip_port) - 1):
                ip_str = params.get_value_if_available(ip_port[:index])
                port_str = params.get_value_if_available(ip_port[index + 1:])
                if not ip_str or ip_str[0] == '!':
                    status.add_error(f"Wrong IP string value: {ip_str}")
                    ret_val = process_status.ERR_source_IP_Ports
                elif not port_str or port_str[0] == '!':
                    status.add_error(f"Wrong Port value: {port_str}")
                    ret_val = process_status.ERR_source_IP_Ports
                else:
                    try:
                        port_val = int(port_str)
                    except:
                        status.add_error(f"Wrong Port value: {port_str}")
                        ret_val = process_status.ERR_source_IP_Ports
            else:
                status.add_error(f"Wrong IP and Port with value: {cmd_words[6]}")
                ret_val = process_status.ERR_command_struct
            with_port = True
    else:
        ret_val = process_status.ERR_command_struct


    if not ret_val:
        if cmd_words[1] == "reply":
            # Set the reply ip and port that is added to each AnyLog message header
            if ip_str == "dynamic":
                net_utils.set_reply_ip("0.0.0.0", port_val)
            elif not net_utils.test_ipaddr(ip_str, port_val):
                ret_val = process_status.ERR_source_IP_Ports
            else:
                net_utils.set_reply_ip(ip_str, port_val)

        elif cmd_words[1] == "self":
            # Set the IP and Port that will be used when a node is messaging itself
            # It is used on kubernetes deployments which do not allow for a pod to
            # send a message to itself on the virtual local address
            if ip_str == "dynamic":
                ip_str = tcpip_server.get_ip()
                if not ip_str:
                    status.add_error("Failed to determine the local IP address (using 'dynamic')")
                    ret_val = process_status.ERR_source_IP_Ports
                if not ret_val:
                    if not port_val:
                        # Get the assigned local port value
                        port_val = net_utils.get_local_port()
                        if not port_val:
                            status.add_error("Failed to determine the local port (specify port or declare local (TCP) IP and Port before calling 'set self ip')")
                            ret_val = process_status.ERR_source_IP_Ports

            if not ret_val:
                net_utils.set_self_ip(ip_str, port_val)

    return ret_val

# --------------------------------------------------------------
# Reset the echo queue
# reset echo queue
# reset echo queue where size = 30
# --------------------------------------------------------------
def reset_echo_queue(status, io_buff_in, cmd_words, trace):
    global echo_queue

    ret_val = process_status.SUCCESS
    words_count = len(cmd_words)

    if words_count == 3:
        if not echo_queue:
            new_size = 20       # Use default
        else:
            new_size = echo_queue.get_queue_size()
        if not new_size:
            new_size = 20
    elif words_count == 8 and utils_data.test_words(cmd_words, 3, ["where", "size", "="]) and cmd_words[7].isdecimal():
        new_size = int(cmd_words[7])
        if new_size < 1 or new_size > 100:
            status.add_error("Wrong size for echo queue (range is 1 to 100)")
            ret_val = process_status.ERR_command_struct
    else:
        ret_val = process_status.ERR_command_struct

    if not ret_val:
        echo_queue = utils_queue.MsgQueue(new_size)
        echo_queue.update_prompt(False)  # Indicate the buffer is empty (prompt removes the + sign)
    return ret_val

# --------------------------------------------------------------
# Set the size of the threads supporting a query
# --------------------------------------------------------------
def set_query_pool(status, io_buff_in, cmd_words, trace):

    global workers_pool

    words_count = len(cmd_words)

    if words_count == 4:
        # set thread pool 3
        workers_count = params.get_value_if_available(cmd_words[3])
        if not workers_count.isnumeric() or not int(workers_count):
            status.add_error("Missing number of threads for 'threads pool'")
            ret_val = process_status.ERR_command_struct
        else:
            if workers_pool and not workers_pool.all_threads_terminated():
                status.add_error("Workers pool is defined, use 'exit workers' and define the pool size after the exit")
                ret_val = process_status.ERR_process_failure
            else:
                workers_pool = utils_threads.WorkersPool("Query", int(workers_count))
                ret_val = process_status.SUCCESS
    else:
        ret_val = process_status.ERR_command_struct
    return ret_val

# --------------------------------------------------------------
# Enable / disable compression of messages
# set compression on off
# --------------------------------------------------------------
def set_compression(status, io_buff_in, cmd_words, trace):

    global message_compression_

    ret_val = process_status.SUCCESS

    if cmd_words[2] == "on":
        message_compression_ = True
    elif cmd_words[2] == "off":
        message_compression_ = False
    else:
        ret_val = process_status.ERR_command_struct

    return ret_val
# --------------------------------------------------------------
# Declare the AnyLog root directory
# set anylog home [path to root]
# --------------------------------------------------------------
def set_anylog_home(status, io_buff_in, cmd_words, trace):

    ret_val = process_status.ERR_command_struct
    words_count = len(cmd_words)
    if words_count == 4:
        anylog_root = params.get_value_if_available(cmd_words[3])
        ret_val = utils_io.test_dir_writeable(status, anylog_root, True)
        if not ret_val:
            params.set_directory_locations(anylog_root)
    return ret_val


# --------------------------------------------------------------
# "set servers ping" - send a ping message to unresponsive servers
# --------------------------------------------------------------
def set_servers_ping(status, io_buff_in, cmd_words, trace):

    if len(cmd_words) == 3:

        servers_list = metadata.get_failed_servers()
        if len(servers_list):
            message = ["run", "client", "(destination)", "event", "metadata_ping"]
            for ip_port in servers_list:
                message[2] = ip_port
                run_client(status, io_buff_in, message, trace)

    return process_status.SUCCESS


# =======================================================================================================================
# =======================================================================================================================
# Commands dictionaries
# =======================================================================================================================
# =======================================================================================================================
_blockchain_methods = {

    "get": {'command': blockchain_get_local,
               'help': {
                   'usage': "blockchain get [policy type] [where] [attribute name value pairs] [bring] [bring command variables]",
                   'example': "blockchain get *\n"
                              "blockchain get operator where dbms = lsl_demo\n"
                              "blockchain get cluster where table[dbms] = purpleair and table[name] = air_data bring [cluster][id] separator = ,\n"
                              "blockchain get operator bring.table [*] [*][name] [*][ip] [*][port]\n"
                              "blockchain get * bring.table.unique [*]",
                   'text': "Get the policies or information from the policies that satisfy the search criteria.",
                   'link' : "blob/master/blockchain%20commands.md#query-policies",
                   'keywords' : ["blockchain"]
                    }
               },
    "state": {'command': blockchain_state,
            'words_count' : 6,
             'help': {
                 'usage': "blockchain state where platform = [platform name]",
                 'example': "blockchain state where platform = ethereum",
                 'text': "Returns an ID representing the state of the contract. If the ID changes, data was added or deleted.",
                 'link': "blob/master/blockchain%20commands.md#query-policies",
                 'keywords': ["blockchain"]
             }
             },

    "read": {'command': blockchain_get_local,
            'help': {
                'usage': "blockchain read [policy type] [where] [attribute name value pairs] [bring] [bring command variables]",
                'example': "blockchain read *\n"
                           "blockchain read operator where dbms = lsl_demo\n"
                           "blockchain read cluster where table[dbms] = purpleair and table[name] = cos_data bring [cluster][id] separator = ,",
                'text': "Get the policies or information from the policies that satisfy the search criteria. The policies are retrieved from the local blockchain file.",
                'link': "blob/master/blockchain%20commands.md#the-blockchain-read-command",
                'keywords' : ["blockchain"]
                }
            },
    "insert": {'command': blockchain_insert,
            'words_min': 3,
            'help': {
                'usage': "blockchain insert where policy = [policy] and blockchain = [platform] and local = [true/false] and master = [IP:Port]",
                'example': "blockchain insert where policy = !policy and local = true and master = !master_node\n"
                           "blockchain insert where policy = !policy and local = true and blockchain = ethereum",
                'text': "Add a JSON policy to the specified blockchain platforms",
                "link": "blob/master/blockchain%20commands.md#the-blockchain-insert-command",
                'keywords' : ["blockchain"],
                },
               "trace" : 0,
            },

    "add": {'command': blockchain_add_local,
            'words_count' : 3,
            'help': {
                'usage': "blockchain add [policy]",
                'example': "blockchain add !operator",
                'text': "Add a JSON policy to a local file representing the blockchain",
                'keywords' : ["blockchain"]
                }
            },
    "push": {'command': blockchain_push_local,
            'words_min' : 3,
            'help': {
                'usage': "blockchain push [policy]",
                'example': "blockchain push !operator",
                'text': "Add a JSON policy to a local database hosting the blockchain data",
                'keywords' : ["blockchain"]
                }
            },
    "pull": {'command': blockchain_pull_local,
             'words_min' : 4,
             'help': {
                 'usage': "blockchain pull to [json | sql | stdout] [file name]",
                 'example': "blockchain pull to stdout\n"
                            "blockchain pull to json !out_file",
                 'text': "Get a copy of the blockchain data from a local database hosting the metadata.",
                'keywords' : ["blockchain"]
                }
             },
    "update file": {'command': blockchain_change_file,
             'help': {
                 'usage': "blockchain update file [path and file name]",
                 'example': "blockchain update file",
                 'text': "Copy the named file over the current blockchain file. If file name not provided, copy blockchain.new.",
                 'keywords' : ["blockchain"],
                }
             },
    "delete local file": {'command': blockchain_delete_file,
                    'key_only' : True,
                    'help': {
                        'usage': "blockchain delete local file",
                        'example': "blockchain delete local file",
                        'text': "Delete the local JSON file with the blockchain data.",
                        'keywords' : ["blockchain"]
                    }
                    },
    "delete policy": {'command': blockchain_delete_policy,
                          'words_min': 7,
                          'help': {
                              'usage': "blockchain delete policy where id = [policy id] and master = [IP:Port] and local =[true/false] and blockchain = [platform]",
                              'example': "blockchain delete policy where id = 64283dba96a4c818074d564c6be20d5c and master = !master_node\n"
                                         "blockchain delete policy where id = 64283dba96a4c818074d564c6be20d5c and local = true and blockchain = ethereum",
                              'text': "Delete a policy from the ledger.",
                              'link' : "blob/master/blockchain%20commands.md#the-blockchain-delete-policy-command",
                              'keywords': ["blockchain"]
                          }
                          },

    "update dbms": {'command': blockchain_file_to_dbms,
                    'help': {
                        'usage': "blockchain update dbms [path and file name] [ignore message]",
                        'example': "blockchain update dbms",
                        'text': "Update the local DBMS with the policies in the named file. If file name is not provided, use blockchain.json file",
                        'keywords' : ["blockchain"],
                        }
                    },

    "create table": {'command': blockchain_create_table,
                    'key_only' : True,
                    'help': {
                        'usage': "blockchain create table",
                        'example': "blockchain create table",
                        'text': "On the master node, create the 'ledger' table on the local 'blockchain' DBMS.",
                        'keywords' : ["blockchain"],
                        }
                    },
    "drop table": {'command': blockchain_drop_table,
                     'key_only': True,
                     'help': {
                         'usage': "blockchain drop table",
                         'example': "blockchain drop table",
                         'text': "On the master node, drop the 'ledger' table from the local blockchain DBMS.",
                         'keywords' : ["blockchain"],
                     }
                     },
    "drop policy": {'command': blockchain_drop_policies,
                   'words_min' : 4,
                   'help': {
                       'usage': "blockchain drop policy where id = [policy id]\n"
                                "blockchain drop policy [policy]",
                       'example': "blockchain drop policy where id = 4a0c16ff565c6dfc05eb5a1aca4bf825\n"
                                  "blockchain drop policy !operator",
                       'text': "On the master node, delete the provided one policy (or policies) from the local blockchain DBMS.",
                       'link' : "blob/master/blockchain%20commands.md#removing-policies-from-a-master-node",
                       'keywords' : ["blockchain"]
                    }
                   },
    "drop by host": {'command': blockchain_drop_host,
                    'words_count' : 5,
                    'help': {
                        'usage': "blockchain drop by host [ip]",
                        'example': "blockchain drop by host 10.0.0.78",
                        'text': "On the master node, delete all the policies that were added by the node using the provided IP.",
                        'keywords' : ["blockchain"],
                    }
                    },

    "replace policy": {'command': blockchain_change_policy,
                    'words_count' : 6,
                    'help': {
                        'usage': "blockchain replace policy [policy id] with [new policy]",
                        'example': "blockchain replace policy !policy_id with !operator",
                        'text': "Replace an existing policy in the local blockchain database.",
                        'keywords' : ["blockchain"],
                        }
                    },
    "test":         {'command': blockchain_test_file,
                       'key_only': True,
                       'help': {
                           'usage': "blockchain test",
                           'example': "blockchain test",
                           'text': "Validate the structure of the local copy of the ledger.",
                           'link': "blob/master/blockchain%20commands.md#interacting-with-the-blockchain-data",
                           'keywords' : ["blockchain"],
                        }
                       },
    "load metadata": {'command': blockchain_load_metadata,
                     'key_only': True,
                     'help': {
                         'usage': "blockchain load metadata",
                         'example': "blockchain load metadata",
                         'text': "Recreate the internal representation of the metadata from a local JSON file.\n"
                                 "The command is called if a file is copied (not through the block sync process) to serve as the local metadata layer.",
                         'link' : "blob/master/blockchain%20commands.md#copying-policies-representing-the-metadata-to-the-local-ledger",
                         'keywords' : ["blockchain"],
                        }
                     },
    "reload metadata": {'command': blockchain_load_metadata,
                      'key_only': True,
                      'help': {
                          'usage': "blockchain reload metadata",
                          'example': "blockchain reload metadata",
                          'text': "Recreate the internal representation of the metadata from a local JSON file.\n"
                                  "Force reload of the metadata ignoring state.",
                          'link': "blob/master/blockchain%20commands.md#copying-policies-representing-the-metadata-to-the-local-ledger",
                          'keywords' : ["blockchain"],
                        }
                      },

    "query metadata": {'command': blockchain_query_metadata,
                      'key_only': True,
                      'help': {
                          'usage': "blockchain query metadata",
                          'example': "blockchain query metadata",
                          'text': "Provide a diagram of the local metadata.",
                          'link' : "blob/master/high%20availability.md#view-data-distribution-policies",
                          'keywords' : ["blockchain", "high availability"],
                        }
                      },
    "seed from": {'command': blockchain_seed_file,
                       'words_min' : 4,
                       'help': {
                           'usage': "blockchain seed from [ip:port]",
                           'example': "blockchain seed from 73.202.142.172:7848",
                           'text': "Pull the metadata from a source node.",
                           'link': "blob/master/blockchain%20commands.md#retrieving-the-metadata-from-a-source-node",
                           'keywords': ["blockchain", "configuration"],
                       }
                       },

    "switch network": {'command': switch_network,
                       'words_count': 7,
                       'help': {
                           'usage': "blockchain switch network where master = [IP:Port]",
                           'example': "blockchain switch network where master = 23.239.12.151:2048",
                           'text': "Switch the master node for the blockchain synchronizer process.",
                           'keywords' : ["blockchain"],
                            }
                       },

    "test cluster": {'command': blockchain_test_metadata,
                      'key_only': True,
                      'help': {
                          'usage': "blockchain test cluster",
                          'example': "blockchain test cluster",
                          'text': "Provides analysis of the policies.",
                          'link' : 'blob/master/high%20availability.md#test-cluster-policies',
                          'keywords' : ["blockchain", "high availability"],
                        }
                      },
    "checkout": {'command': blockchain_checkout,
                    'words_min' : 4,
                    'help': {
                         'usage': "blockchain checkout from [blockchain name]",
                         'example': "blockchain checkout from ethereum",
                         'text': "Get a copy of the metadata from the blockchain platform.",
                         'keywords' : ["blockchain"],
                        }
                    },
    "connect": {'command': blockchain_connect,
                 'words_min': 8,
                 'help': {
                     'usage': "blockchain connect to [platform] where provider = [provider] and [connection params]",
                     'example': "blockchain connect to ethereum where provider = https://rinkeby.infura.io/v3/... and "
                                "contract = 0x3899bED... and private_key = a4caa21209188 ... and public_key = 0x982AF5e15... and "
                                "gas_read = 3000000 and gas_write = 4000000",
                     'text': "Connect to the blockchain platform using the connection params.",
                     'keywords' : ["blockchain"],
                    }
                 },
    "create account": {'command': blockchain_create_account,
                'words_count': 4,
                'help': {
                    'usage': "blockchain create account [platform]",
                    'example': "blockchain create account ethereum",
                    'text': "Create a new account on the blockchain platform.",
                    'keywords' : ["blockchain"],
                    }
                },

    "set account info": {'command': blockchain_set_account_info,
                       'words_min': 12,
                       'help': {
                           'usage': "blockchain set account info where platform = [platform name] and [platform parameters]",
                           'example': "blockchain set account info where platform = ethereum and private_key = !private_key and public_key = !public_key and chain_id = 11155111",
                           'text': "Associate different parameters with the blockchain platform.",
                           'keywords' : ["blockchain"],
                       }
                       },

    "deploy contract": {'command': blockchain_deploy,
                'words_count': 11,
                'help': {
                    'usage': "blockchain deploy contract where platform = [platform name] and public_key = [public key]",
                    'example': "blockchain deploy ethereum where platform = ethereum ...",
                    'text': "Deploy the AnyLog contract on the blockchain platform.",
                    'keywords' : ["blockchain"],
                }
                },
    "commit": {'command': blockchain_commit,
                    'words_count' : 5,
                     'help': {
                         'usage': "blockchain commit to [blockchain name] [policy]",
                         'example': "blockchain commit ethereum !operator",
                         'text': "Update the blockchain platform with a new JSON policy.",
                         'keywords' : ["blockchain"],
                        }
                     },
    "update": {'command': blockchain_update,
                    'words_count' : 6,
                     'help': {
                         'usage': "blockchain update to [blockchain name] [policy_id] [policy]",
                         'example': "blockchain update to ethereum !policy_id !policy",
                         'text': "Update an existing JSON policy in the blockchain platform.",
                         'keywords' : ["blockchain"],
                        }
                     },

    "wait": {'command': blockchain_wait,
                   'words_count': 6,
                   'help': {
                       'usage': "blockchain wait where [condition]",
                       'example': "blockchain wait where policy = !operator\n"
                                  "blockchain wait where id = [id]\n"
                                  "blockchain wait where command = \"blockchain get cluster where name = cluster_1\"",
                       'text': "Pause process until the local copy of the blockchain is updated with the policy.\n"
                               "[condition] is specified as [key] = [value]",
                       'keywords' : ["blockchain"],

                   }
                },
    "prepare policy": {'command': blockchain_prep,
                 'words_count': 4,
                 'help': {
                     'usage': "blockchain prepare policy [policy]",
                     'example': "blockchain prepare policy !operator",
                     'text': "Add an ID and a date attributes to a policy.",
                     'keywords' : ["blockchain"],
                    }
                 },
}
_time_file_methods = {
        "rename": {   'command': rename_time_file,
                        'help': {
                            'usage': "time file rename [source file path and name] to dbms = [dbms name] and table = [table name] and source = [source ID] and hash = [hash value] and instructions = [instructions id]",
                            'example': "time file rename !prep_dir to dbms = al_dbms and table = ping_sensor",
                            'text': "change an arbitrary file name to a name that satisfies the naming convention.",
                            'keywords' : ["high availability"],
                        }
                     },
        "new": {'command': new_time_file_entry,
               'help': {
                   'usage': "time file new [file name] [optional status 1] [optional status 2]",
                   'example': "time file new !json_file",
                   'text': "Add a new entry in the TSD table. The entry info derived from the file name.",
                   'keywords' : ["high availability"],
                }
               },
        "add": {'command': new_time_file_entry,
                'help': {
                    'usage': "time file add [file name] [optional status 1] [optional status 2]",
                    'example': "time file add !json_file",
                    'text': "Add a new entry in the TSD table. The entry info derived from the file name. Validate that the file exists on the local disk",
                    'keywords' : ["high availability"],
                    }
                },

        "update": {'command': update_time_file,
            'help': {
                'usage': "time file update [hash value] [optional status 1] [optional status 2]",
                'example': "time file update 6c78d0b005a86933ba44573c09365ad5 \"From Publisher 778299-2\" \"File delivered to backup\"",
                'text': "Update a TSD row entry.",
                'keywords' : ["high availability"],
                }
            },
        "drop": {'command': drop_tsd_tables,
            'words_count' : 4,
            'help': {
                'usage': "time file drop [all/table_name]",
                'example': "time file drop all\n" 
                           "time file drop tsd_51",
                'text': "Drop a TSD table or all the TSD tables.",
                'keywords' : ["high availability"],
                }
            },
        "delete": {'command': delete_from_time_file,
            'help': {
                'usage': "time file delete [row id] from [TSD table name]",
                'example': "time file delete 12 from tsd_32",
                'text': "Delete a single row from a TSD table.",
                'keywords' : ["high availability"],
                }
            },
}
_reset_methods = {
        "port forward": {'command': port_forwarding.reset_port_forward,
                 'words_min': 4,
                 'help': {
                     'usage': "reset port forward where namespace = [namespace] and pod_name = [pod name]",
                     'example': "reset port forward where namespace = test and pod = web-svc",
                     'text': "reset Kubernetes port-forwarding for the given namespace and pod",
                     'keywords': ["streaming"],
                 }
                 },
        "stats": {'command': reset_statistics,
                     'words_count': 10,
                     'help': {
                         'usage': "reset stats where service = [service name] and topic = [topic]",
                         'example': "reset stats where service = operator and topic = summary",
                         'text': "reset the statistics on the service and topic provided",
                         'keywords': ["streaming"],
                     }
                     },

        "msg rule": {'command': message_server.reset_msg_rules,
                  'words_count': 4,
                  'help': {
                      'usage': "reset msg rule [rule name]",
                      'example': "reset msg rule syslog",
                      'text': "remove a message rule",
                      'link' : 'blob/master/using%20syslog.md#remove-a-rule',
                      'keywords': ["streaming"],
                  }
                  },

        "monitored": {'command': monitor.reset,
                  'words_count': 3,
                  'help': {
                      'usage': "reset monitored [topic]",
                      'example': "reset monitored operators",
                      'text': "Reset the list of nodes and the info associated to a monitored topic",
                      'link': "blob/master/monitoring%20nodes.md#reset-a-monitored-topic",
                      'keywords': ["configuration", "monitor"],
                    }
                  },

        "event log": {'command': reset_logged_data,
                      'key_only' : True,
                      'help': {
                          'usage': "reset event log",
                          'example': "reset event log",
                          'text': "Reset the event log.",
                          'link' : "blob/master/logging%20events.md#reset-the-log-data",
                          'keywords' : ["log", "profiling"],
                        }
                      },
        "error log": {'command': reset_logged_data,
                      'key_only' : True,
                      'help': {
                          'usage': "reset error log",
                          'example': "reset error log",
                          'text': "Reset the error log.",
                          'link' : "blob/master/logging%20events.md#reset-the-log-data",
                          'keywords' : ["log", "profiling"],
                        }
                      },
        "query log": {'command': reset_logged_data,
                      'key_only' : True,
                      'help': {
                          'usage': "reset query log",
                          'example': "reset query log",
                          'text': "Reset the query log.",
                          'link' : "blob/master/logging%20events.md#reset-the-log-data",
                          'keywords' : ["log", "profiling"],
                      }
                      },
        "file log": {'command': reset_logged_data,
                     'key_only' : True,
                     'help': {
                         'usage': "reset file log",
                         'example': "reset file log",
                         'text': "Reset the file log.",
                         'link' : "blob/master/logging%20events.md#reset-the-log-data",
                         'keywords' : ["log", "profiling"],
                        }
                     },
        "streaming log": {'command': reset_logged_data,
                    'key_only': True,
                    'help': {
                        'usage': "reset streaming log",
                        'example': "reset streaming log",
                        'text': "Reset the streaming log.",
                        'link' : "blob/master/logging%20events.md#reset-the-log-data",
                        'keywords' : ["log", "streaming", "profiling"],
                    }
                 },
        "query timer": {'command': job_instance.reset_query_timer,
                    'key_only': True,
                    'help': {
                        'usage': "reset query timer",
                        'example': "reset query timer",
                        'text': "Reset the query timer. The query timer monitors the execution time of queries on the node.",
                        'link' : 'blob/master/profiling%20and%20monitoring%20queries.md#statistical-information',
                        'keywords' : ["query", "profiling"],
                        }
                    },
        "reply ip":     {'command': reset_replacement_ip,
                            'key_only': True,
                            'help': {
                                'usage': "reset reply ip",
                                'example': "reset reply ip",
                                'text': "Reply messages will use the TCP server configured IP and port.",
                                'link' : "blob/master/network%20configuration.md#reset-the-reply-ip-to-the-source-ip",
                                'keywords' : ["configuration", "internal"],
                        }
                    },


        "self ip":     {'command': reset_replacement_ip,
                     'key_only': True,
                     'help': {
                         'usage': "reset self ip",
                         'example': "reset self ip",
                         'text': "Self-messaging is reset to use the local IP and port.",
                         'link': "blob/master/network%20configuration.md#reset-self-messaging",
                         'keywords' : ["configuration", "internal"],
                        }
                     },

        "echo queue": {'command': reset_echo_queue,
                       'help': {
                           'usage': "reset echo queue where size = [n]",
                           'example': "reset echo queue\n"
                                      "reset echo queue where size = 50",
                           'text': "Resets the echo queue and sets the number of messages that can be stored in the queue (the default is 20).",
                           'keywords' : ["cli"],
                           }
                       },

        "streaming condition": {'command': streaming_conditions.reset_streaming_condition,
                    'words_min': 3,
                    'help': {
                        'usage': "reset streaming condition where dbms = [dbms name] and table = [table name] and id = [condition id]",
                        'example': "reset streaming condition where dbms = edgex and table = rabd_data and id = 1 and id = 5\n"
                                   "reset streaming condition where dbms = edgex",
                        'text': "Resets one or more streaming conditions.",
                        'link' : "blob/master/streaming%20conditions.md#reset-streaming-condition",
                        'keywords' : ["streaming", "data", "configuration"],

                        }
                    },

}
_set_methods = {

        "profiler": {'command': set_profiler,
                     'words_min': 7,
                     'help': {
                         'usage': "set profiler [on/off] where target = [process name]",
                         'example': "set profiler on where target = operator",
                         'text': "Enable and disable profiling. Note: Profile libraries needs to be loaded per moudle using sys variables."\
                                 "i.e.: PROFILE_OPERATOR=true",
                         'link' : 'blob/master/profiling%20and%20monitoring%20queries.md#profiling',
                         'keywords': ["debug", "profile"],
                     }
                     },

        "msg rule": {'command': set_msg_rule,
                        'words_min': 12,
                        'help': {
                            'usage': "set msg rule [rule name] if ip = [IP address] and port = [port] and header = [header text] then dbms = [dbms name] and table = [table name] and syslog = [true/false] and extend = ip and format = [data format] and topic = [topic name]",
                            'example': "set msg rule my_rule if ip = 10.0.0.78 and port = 1468 then dbms = test and table = syslog and syslog = true",
                            'text': "Identify arbitrary messages and associate streaming data with a logical database, table and a topic",
                            'link' : 'blob/master/using%20syslog.md#setting-a-rule-to-accept-syslog-data',
                            'keywords': ["streaming"],
                        }
                        },

        "buffer threshold": {'command': set_buff_thresholds,
                         'help': {
                             'usage': "set buffer threshold where [configuration key value pairs]",
                             'example': "set buffer threshold where dbms = al_demo and table = ping_sensor and time = 2 minutes and volume = 1MB",
                             'text': "Configure time and volume thresholds for buffered streaming data.",
                             'link': "blob/master/adding%20data.md#setting-and-retrieving-thresholds-for-a-streaming-mode",
                             'keywords': ["data", "configuration", "streaming", "enterprise"],
                         }
                         },

        "port forward": {'command': set_port_forward,
                        'words_count': 19,
                        'help': {
                            'usage': "set port forward where pod_name = [pod name] and namespace = [anylog namespace] and local_port = [local port] and pod_port= [podort]",
                            'example': "set port forward where pod_name = my_pod_name and namespace = my_namespace and local_port = 8080 and pod_port=80",
                            'text': "Declare a port forwarding to access services running inside a pod from your local machine.",
                            'keywords': ["streaming"],
                        }
                        },

        "monitored nodes": {'command': set_monitored_nodes,
                'words_min': 11,
                'help': {
                    'usage': "set monitored nodes where topic = [topic name] and nodes = [the nodes IPs and Ports]",
                    'example': "set monitored nodes where topic = operators and nodes = 10.0.0.78:7848,10.0.0.78:3048\n"
                               "set monitored nodes where topic = operators and nodes = blockchain get operator bring.ip_port",
                    'text': "Create a list of a monitored nodes and associate the list to a topic.",
                    'keywords': ["monitor"],
                }
                },

        "cli": {'command': node_info.set_cli,
                  'words_count': 3,
                  'help': {
                      'usage': "set cli [on/off]",
                      'example': "set cli off",
                      'text': "Disable the AnyLog CLI, when AnyLog is configured as a background process.",
                      'keywords': ["cli", "configuration"],
                  }
                  },

        "node name": {'command': set_current_node_name,
                            'words_count': 4,
                            'help': {
                                'usage': "set node name [node name]",
                                'example': "set node name Operator_3",
                                'text': "Sets a name that identifies the node",
                                'link': "blob/master/anylog%20commands.md#set-node-name",
                                'keywords': ["cli", "configuration"],
                                }
                            },

        "streaming condition": {'command': streaming_conditions.set_streaming_condition,
                          'words_min': 13,
                          'help': {
                              'usage': "set streaming conditions where dbms = [dbms_name] and table = [table_name] and limit = [limit] if [condition] then [command]",
                              'example': "set streaming condition where dbms = test and table = rand_data and limit = 3 if [value] > 10 then send sms to 6508147334 where gateway = tmomail.net\n"
                                         "set streaming condition where dbms = test and table = rand_data  if [value] < 3 then ignore",
                              'text': "Set a condition on the streaming data",
                              'link': "blob/master/streaming%20conditions.md#condition-declaration",
                              'keywords' : ["streaming", "data", "configuration"],
                          }
                      },


        "script": {'command': set_script,
                   'words_count' : 4,
                   'help': {
                       'usage': "set script [file name] [script data]",
                       'example': "set script autoexec !script_data",
                       'text': "Update the names script file in the scripts directory.",
                       'keywords' : ["cli", "configuration"],
                   }
                   },

        "policy": {'command': set_policy,
               'words_min': 5,
               'help': {
                   'usage': "set policy [policy name] [policy key value pairs]",
                   'example': "set policy config_policy [config] = {} and [config][company] = anylog",
                   'text': "Update a dictionary policy or policy values.",
                   'link' : "blob/master/policies.md#declaring-a-policy-using-set-commands",
                   'keywords': ["cli", "configuration"],
               }
               },

        "rest log": {'command': http_server.set_streaming_log,
                  'words_count' : 4,
                  'help': {
                      'usage': "set streaming log on/off",
                      'example': "set streaming log on",
                      'text': "Enable/Disable a log to include last executed REST calls. The log is queried using 'get rest calls'.",
                      'keywords' : ["log", "configuration", "profiling", "streaming"],
                  }
                  },

        "query log": {   'command': set_query_log,
                        'help': {
                            'usage': "set query log  profile [n] seconds",
                            'example': "set query log on\n"
                                       "set query log off\n"
                                       "set query log profile 10 seconds",
                            'text': "Sets a query log to include all queries or only queries slower than a threshold.",
                            'link': 'blob/master/logging%20events.md#the-query-log',
                            'keywords' : ["log", "configuration", "profiling", "query"],
                        }
                     },

        "function key": {'command': set_function_key,
                        'help': {
                            'usage': "set function key f5 [function string]",
                            'example': "set function key f5 \"system ls\"",
                            'text': "Assigns a string to a function key.",
                            'keywords' : ["cli"],
                            }
                        },
        "debug": {'command': set_debug_options,
                     'help': {
                         'usage': "set debug [on/off/interactive]",
                         'example': "set debug on",
                         'text': "Enables and disables debug. Interactive can be activated with a thread command.",
                         'link' : "blob/master/cli.md#the-set-debug-command",
                         'keywords' : ["cli", "debug"],
                        }
                     },
        "mqtt debug": {'command': set_mqtt_debug,
                    'help': {
                        'usage': "set mqtt debug [on/off]",
                        'example': "set mqtt debug on",
                        'text': "Enables and disables debug of MQTT messages.",
                         'keywords' : ["cli", "debug", "streaming"],
                        }
                    },
        "rest timeout": {'command': http_server.set_rest_timeout,
                   'help': {
                       'usage': "set rest timeout [time]",
                       'example': "set rest timeout 30 seconds\n"
                                  "set rest timeout 2 minutes",
                       'text': "Sets a time limit for a REST reply. If limit is 0, the process will wait for a reply without timeout.",
                       'keywords' : ["data", "streaming", "api", "configuration"],
                   }
                   },

        "echo queue": {'command': set_echo_queue,
                   'help': {
                       'usage': "set echo queue [on/off]",
                       'example': "set echo queue on",
                       'text': "Enable / disable the echo queue. With echo queue, messages from peer nodes are stored in a queue rather than send to sdtout.",
                       'keywords' : ["cli"],
                        }
                   },

        "query pool": {'command': set_query_pool,
                       'help': {
                           'usage': "set query pool [n]",
                           'example': "set query pool 5",
                           'text': "Sets the number of threads supporting queries (the default value is 3).",
                           'link' : 'blob/master/node%20configuration.md#configuring-the-size-of-the-query-pool',
                           'keywords' : ["query", "profiling"],
                        }
                    },
        "query mode": {'command': set_query_mode,
                     'help': {
                         'usage': "set query mode using [query params]",
                         'example': "set query mode using timeout = 30 seconds and max_volume = 2MB and send_mode = all and reply_mode = any",
                         'text': "Sets the query params to determine thresholds on execution time and failures of participating nodes.",
                         'link' : "blob/master/anylog%20commands.md#set-query-mode",
                         'keywords' : ["query", "profiling"],
                        }
                     },


        "compression": {'command': set_compression,
                   'words_count': 3,
                   'help': {
                       'usage': "set compression [on/off]",
                       'example': "set compression on",
                       'text': "Enable/disable compression of data in message transfer.",
                       'keywords' : ["data", "configuration"],

                        }
                   },

        "anylog home": {'command': set_anylog_home,
                         'help': {
                             'usage': "set anylog home [path to AnyLog root]",
                             'example': "set anylog home $EDGELAKE_HOME",
                             'text': "Declare the location of the root directory to the AnyLog Files.",
                             'link' : "blob/master/getting%20started.md#switching-between-different-setups",
                             'keywords' : ["configuration"],
                            }
                         },
        "servers ping": {'command': set_servers_ping,
                    'help': {
                        'usage': "set servers ping",
                        'example': "set servers ping",
                        'text': "Send a ping message to unresponsive servers.",
                        'keywords' : ["high availability"],
                        }
                    },
        "traceback": {'command': set_traceback,
                    'words_min' : 3,
                    'help': {
                         'usage': "set traceback [on/off] [optional text]",
                         'example': "set traceback on\n"
                                    "set traceback on \"which is not a member of the cluster\"",
                         'text': "Prints stack trace when messages are added to the error log. If the text is specified, stacktrace is added only if the text is a sustring in the error message",
                         'keywords' : ["debug"],
                        }
                    },
        "reply ip": {'command': set_replacement_ip,
                  'words_min': 5,
                  'help': {
                      'usage': "set reply ip = [IP/'dynamic']\n"
                               "set reply ip and port = [IP:Port]",
                      'example': "set reply ip = !external_ip\n"
                                 "set reply ip = dynamic\n"
                                 "set reply ip and port = 184.32.123.120:4078",
                      'text': "Set an IP address (and optionally port) to be considered by the receiving node as the reply address.",
                      'link': "blob/master/network%20configuration.md#setting-a-different-ip-address-for-replies",
                      'keywords' : ["configuration", "internal"],
                    }
                  },

        "self ip": {'command': set_replacement_ip,
                     'words_min': 5,
                     'help': {
                         'usage': "set self ip = [IP/'dynamic']\n"
                                  "set self ip and port = [IP:Port]",
                         'example': "set self ip = !external_ip\n"
                                    "set self ip = dynamic\n"
                                    "set self ip and port = 184.32.123.120:4078",
                         'text': "Set an IP and port address that will be used for self-messaging",
                         'link': "blob/master/network%20configuration.md#self-messaging",
                         'keywords' : ["configuration", "internal"],
                     }
                 },


        "threshold": {'command': data_monitor.set_threshold,
                      'words_min': 10,
                      'help': {
                          'usage': "set threshold where dbms = [dbms name] and table = [table name] and alert = [threshold info]",
                          'example': "set threshold where dbms = dmci and table = sensor_table and min = value\n"
                                     "set threshold where dbms = dmci and table = sensor_table and min = value",
                          'text': "Set a threshold on monitored data",
                          'keywords' : ["monitor"],
                        }
                      },

}

_test_methods = {
        "process": {'command': test_process_active,
             'words_min': 3,
             'help': {
                 'usage': "test process [process name]",
                 'example': "test process operator",
                 'text': "The call returns 'true' if the service is enables, otherwise 'false'",
                 'link' : 'blob/master/test%20commands.md#test-process',
                 'keywords': ["config"],
             }
             },

        "node": {   'command': test_node,
                    'key_only' : True,
                    'help': {
                        'usage': "test node",
                        'example': "test node",
                        'text': "Execute a test to validate the TCP connection, the REST connection and the local blockchain structure.",
                        'link' : 'blob/master/test%20commands.md#test-node',
                        'keywords' : ["debug"],
                        }
                     },
        "cluster setup": {'command': test_cluster_setup,
             'key_only': True,
             'help': {
                 'usage': "test cluster setup",
                 'example': "test cluster setup",
                 'text': "Test the configuration and setup of the node to support HA.",
                 'link' : "blob/master/high%20availability.md#testing-the-node-configuration-for-ha",
                 'keywords': ["high availability"],
                }
             },

        "cluster data": {'command': test_cluster_data,
                 'help': {
                     'usage': "test cluster data",
                     'example': "test cluster data\n"
                                "test cluster data where start_date = -7d",
                     'text': "Compare the TSD tables of the nodes supporting the cluster.",
                     'link' : "blob/master/high%20availability.md#cluster-synchronization-status",
                     'keywords': ["high availability"],
                    }
                 },

        "cluster databases": {'command': test_cluster_databases,
                'help': {
                    'usage': "test cluster databases",
                    'example': "test cluster databases",
                    'text': "Compare the databases defined on each member of the cluster.",
                    'link': "blob/master/high%20availability.md#cluster-databases",
                    'keywords': ["high availability"],
                    }
                },
        "cluster partitions": {'command': test_cluster_partitions,
                          'help': {
                              'usage': "test cluster partitions",
                              'example': "test cluster partitions",
                              'text': "Compare the partitions defined on each member of the cluster.",
                              'link': "blob/master/high%20availability.md#cluster-databases",
                              'keywords': ["high availability"],
                          }
                          },

        "network": {'command': test_network,
                 'help': {
                     'usage': "test network [with] [object]",
                     'example': "test network\n"
                                "test network with master\n"
                                "test network metadata version\n"
                                "test network table ping_sensor where dbms = lsl_demo\n"
                                "test network * ping_sensor where dbms = lsl_demo",
                     'text': "Send a command to a group of nodes in the network and organize the replies from all participating nodes.",
                     'link' : 'blob/master/test%20commands.md#the-test-network-commands',
                     'keywords' : ["debug", "high availability", "configuration"],
                 }
             },

        "connection": {'command': test_connection,
             'words_count': 3,
             'help': {
                 'usage': "test connection [IP:port]",
                 'example': "test connection  10.0.0.223:2041",
                 'text': "Execute a test on a target IP and port to determine if accessible and open.",
                 'link' : 'blob/master/test%20commands.md#test-connection',
                 'keywords' : ["debug"],
                }
             },

        "table": {'command': test_table,
                   'words_count': 7,
                   'help': {
                       'usage': "test table [table name] where dbms = [dbms name]",
                       'example': "test table ping_sensor where dbms = lsl_demo",
                       'text': "Compare the table definition in the blockchain to the table definition in local database schema.",
                       'link' : 'blob/master/test%20commands.md#test-table',
                       'keywords' : ["debug", "dbms"],
                    }
                   },
        "network table": {'command': test_network_table,
              'words_count': 11,
              'help': {
                  'usage': "test network table where name = [table name] and dbms = [dbms name]",
                  'example': "test network table where name = ping_sensor and dbms = lsl_demo\n"
                             "test network table where name = * and dbms = lsl_demo",
                  'text': "Issue 'test table' command for all the operator nodes that host the table.",
                  'link': 'blob/master/test%20commands.md#test-table',
                  'keywords': ["debug", "dbms"],
              }
        },

}

_file_methods = {
        "write": {'command': file_write,
                    'help': {
                        'internal' : True,          # Only for internal usage
                        'usage': "file write",
                        'example': "file write",
                        'text': "Write the message data to a file.",
                        'keywords' : ["file", "internal"],
                    }
                },

        "get": {'command': file_get,
                    'words_min' : 3,
                    'help': {
                        'usage': "file get [remote node file path and name] [local node file path and name]",
                        'example': "run client 10.0.0.23:2048 file get !!source_file !dest_file",
                        'text': "Copy a file from a remote node to the local node.",
                        'link' : "blob/master/file%20commands.md#file-copy-from-a-remote-node-to-a-local-node",
                        'keywords' : ["file"],
                    }
                },

        "store": {'command': file_store,
                'words_min': 6,
                'help': {
                    'usage': "file store where dbms = [dbms_name] and table = [table name] and hash = [hash value] and file = [path and file name]",
                    'example': "file store where dbms = blobs_edgex and table = video and hash = ce2ee27c4d192a60393c5aed1628c96b and file = !prep_dir/device12atpeak.bin",
                    'text': "Insert a file into the blobs dbms.",
                    'link' : "blob/master/image%20mapping.md#insert-a-file-to-a-local-database",
                    'keywords' : ["unstructured data", "file"],
                    }
                },
        "to": {'command': file_to,
              'words_min': 3,
              'help': {
                  'usage': "file to [dest file name] where source = [source file name]",
                  'example': "file to !config_dir/operator_config.al where source = !tmp_dir/new_config.al\n"
                             "Using REST: curl -X POST -H \"command: file to !my_dest\" -F \"file=@testdata.txt\" http://10.0.0.78:7849",
                  'text': "Copy  file to a destination folder",
                  'link': "blob/master/file%20commands.md#copy-a-file-to-a-folder",
                  'keywords': ["unstructured data", "file"],
              }
              },

        "delete": {'command': file_delete,
            'words_min': 3,
            'help': {
                'usage': "file delete [file path and name]",
                'example': "file delete !prep_dir/sensor.json",
                'text': "Delete the specified file.",
                 'keywords' : ["file"],
                }
            },

        "deliver": {'command': file_deliver,
               'words_min': 3,
               'help': {
                   'usage': "file deliver [ip] [port] tsd_x id1,id2,id3....",
                   'example': "file deliver 10.0.0.25 2048 1,3,4,8",
                   'text': "Copy the listed files identified by a TSD table name and row IDs to the node sending the request",
                   'keywords' : ["file", "high availability"],
                   }
               },

        "compress": {'command': manipulate_file,
                    'words_min': 3,
                    'help': {
                        'usage': "file compress [source file path and name] [compressed file path and name]",
                        'example': "file compress !prep_dir/in_file\n"
                                   "file compress !prep_dir/in_file !prep_dir/out_file\n"
                                   "file compress !src_dir/*.json\n"
                                   "file compress !src_dir/*.json !dest_dir",
                        'text': "Compress the specified file or multiple files in a specified directory.",
                        'link' : 'blob/master/file%20commands.md#compress-and-decompress-a-file',
                        'keywords' : ["file"],
                        }
                    },

        "decompress": {'command': manipulate_file,
                    'words_min': 3,
                    'help': {
                        'usage': "file decompress [compressed file path and name] [destination file path and name]",
                        'example': "file decompress !prep_dir/in_file\n"
                                   "file decompress !prep_dir/in_file !prep_dir/out_file\n"
                                   "file decompress !src_dir/*.gz\n"
                                   "file decompress !src_dir/*.gz !dest+dir",
                        'text': "Decompress the specified file or multiple files in a specified directory.",
                        'link' : 'blob/master/file%20commands.md#compress-and-decompress-a-file',
                        'keywords' : ["file"],
                        }
                    },
        "encode": {'command': manipulate_file,
                       'words_min': 3,
                       'help': {
                           'usage': "file encode [compressed file path and name] [destination file path and name]",
                           'example': "file encode !prep_dir/in_file\n"
                                      "file encode !prep_dir/in_file !prep_dir/out_file\n"
                                      "file encode !src_dir/*.png\n"
                                      "file encode !src_dir/*.mp4 !dest+dir",
                           'text': "base64 encode the specified file or multiple files in a specified directory.",
                           'link': 'blob/master/file%20commands.md#encode-and-decode-a-file',
                           'keywords' : ["file", "unstructured data"],
                            }
                       },
        "decode": {'command': manipulate_file,
                       'words_min': 3,
                       'help': {
                           'usage': "file decode [compressed file path and name] [destination file path and name]",
                           'example': "file decode !prep_dir/in_file\n"
                                      "file decode !prep_dir/in_file !prep_dir/out_file\n"
                                      "file decode !src_dir/*.msg\n"
                                      "file decode !src_dir/*.msg !dest+dir",
                           'text': "base64 decode the specified file or multiple files in a specified directory.",
                           'link': 'blob/master/file%20commands.md#encode-and-decode-a-file',
                           'keywords' : ["file", "unstructured data"],
                            }
                       },

        "copy": {'command': file_copy_move,
                       'words_count': 4,
                       'help': {
                            'usage': "file copy [source file path and name] [destination file path and name]",
                            'example': "file copy !prep_dir/in_file !prep_dir/out_file",
                            'text': "Copies the specified file to the destination.",
                            'link' : 'blob/master/file%20commands.md#copy-a-file-on-the-local-node',
                            'keywords' : ["file"],
                            }
                       },

        "move": {'command': file_copy_move,
                 'words_count': 4,
                 'help': {
                         'usage': "file move [source file path and name] [destination file path and name]",
                         'example': "file move !prep_dir/in_file !prep_dir/out_file",
                         'text': "Moves the specified file to the destination.",
                         'keywords' : ["file"],
                        }
                 },

        "test": {'command': file_test_exists,
                 'words_count': 3,
                 'help': {
                     'usage': "file test [file path and name]",
                     'example': "file test !prep_dir/my_file",
                     'text': "Test that the specified file exists.",
                     'keywords' : ["file"],
                 }
             },

        "hash": {'command': file_hash,
             'words_count': 3,
             'help': {
                 'usage': "file hash [file path and name]",
                 'example': "file hash !prep_dir/my_file",
                 'text': "Get the hash value of the data contained in the file",
                 'keywords' : ["file"],
                }
             },
        "retrieve": {'command': file_retrieve,
             'words_min': 10,
             'help': {
                 'usage':   "file retrieve where dbms = [dbms name] and table = [table name] and id = [file id] and dest = [dest]\n"
                            "file retrieve where dbms = [dbms name] and table = [table name] and name = [file name] and dest = [file location]",
                 'example': "file retrieve where dbms = blobs_edgex and table = videos and id = 9439d99e6b0157b11bc6775f8b0e2490.png and dest = !blobs_dir/test_video.mp4\n"
                            "file retrieve where dbms = !my_dbms and table = !my_table and dest = !my_file",
                 'text': "Retrieve the file or files from the designated database.",
                 'link' : 'blob/master/image%20mapping.md#retrieve-a-file-or-files',
                           'keywords' : ["unstructured data"],
                }
             },
        "remove": {'command': file_remove,
                     'words_min': 10,
                     'help': {
                         'usage': "file remove where dbms = [dbms name] and table = [table name] and id = [file name]\n"
                                  "file remove where dbms = [dbms name] and table = [table name] and hash = [file hash]\n"
                                  "file remove where dbms = [dbms name] and date = [date key] and table = [table_name]",
                         'example': "file remove where dbms = blobs_edgex and table = image and hash = 9439d99e6b0157b11bc6775f8b0e2490\n"
                                    "file remove where dbms = my_dbms and table = image",
                         'text': "Delete the file or files from the designated storage.",
                         'link' : 'blob/master/image%20mapping.md#delete-a-file-or-a-group-of-files',
                         'keywords' : ["unstructured data"],
                     }
             },

}

_query_status_methods = {
        "status": {
                    'command': get_query_state,
                    'words_min' :2,
                    'help': {
                        'usage': "query status [all/ID]",
                        'example':  "query status\n"
                                    "query status 8\n"
                                    "query status all",
                        'text': "Get the status of the last executed queries.",
                        'link' : 'blob/master/profiling%20and%20monitoring%20queries.md#command-options-for-monitoring-queries',
                         'keywords' : ["debug", "profiling", "query"],
                    }
                 },
        "destination": {
                    'command': get_query_dest,
                    'words_min': 2,
                    'help': {
                            'usage':    "query destination [all/ID]",
                    'example': "query destination\n"
                               "query destination 12\n"
                               "query destination all",
                    'text': "Get the list of nodes that process the query.",
                    'link' : 'blob/master/profiling%20and%20monitoring%20queries.md#command-options-for-monitoring-queries',
                    'keywords' : ["debug", "profiling", "query"],
                    }
                },
        "explain": {
                    'command': get_query_state,
                    'words_min': 2,
                    'help': {
                            'usage':    "query explain [all/ID]",
                    'example': "query explain\n"
                               "query explain 12\n"
                               "query explain all",
                    'text': "The explain plan on the last executed query or queries.",
                    'link' : 'blob/master/profiling%20and%20monitoring%20queries.md#command-options-for-monitoring-queries',
                   'keywords' : ["debug", "profiling", "query"],
                    }
                },
}

_get_methods = {

        'profiler': {
            'command': get_profiler_output,
            'words_min': 7,
            'help': {'usage': 'get profiler output where target = [process name]',
                     'example': 'get profiler output where target = operator',
                     'text': 'Output the profiler info',
                     'link' : 'blob/master/profiling%20and%20monitoring%20queries.md#profiling',
                     'keywords': ["debug", "profile"],
                     },
            'trace': 0,
        },

        'stack trace': {
            'command': process_status.get_stack_traces,
            'words_min': 3,
            'help': {'usage': 'get stack trace',
                     'example': 'get stack trace\n'
                                'get stack trace main\n'
                                'get stack trace main tcp',
                     'text': 'Output the stack trace of all threads',
                     'keywords': ["debug"],
                     },
                'trace': 0,
            },

        'partitions info': {
                'command': get_partitions_info,
                'words_min': 10,
                'help': {'usage': 'get partitions info where dbms = smart_city and table = test and details = [true/false]',
                         'example': 'get partitions info where dbms = smart_city and table = test and details = true',
                         'text': 'Provide info on the partitions used or a list of all partitions in the database',
                         'keywords': ["node info", "dbms"],
                         },
                'trace': 0,
            },

        'policies diff': {
                    'command': diff_policies,
                    'words_count': 5,
                    'help': {'usage': 'get policies diff [policy 1] [policy 2]',
                             'example': 'get policies diff !policy1 !policy2',
                             'text': 'Output a report indicating the differences between the two policies, or between lists of policies',
                             'link' : "blob/master/policies.md#compare-policies",
                             'keywords': ["cli"],
                             },
                    'trace': 0,
                },

        "port forward": {'command': port_forwarding.get_port_forward,
                  'words_count': 3,
                  'help': {
                      'usage': "get port forward",
                      'example': "get port forward",
                      'text': "Return the list of Kubernetes port-forwarding.",
                      'keywords': ["streaming"],
                    }
                  },

        "msg rules": {'command': message_server.get_msg_rules,
              'words_count': 3,
              'help': {
                  'usage': "get msg rules",
                  'example': "get msg rules",
                  'text': "Return the message rules used by the message server.",
                  'link' : 'blob/master/using%20syslog.md#get-the-list-of-rules',
                  'keywords': ["streaming"],
              }
              },

        "stats": {'command': get_statistics,
               'words_min': 6,
               'help': {
                   'usage': "get stats where [service type and info type]",
                   'example': "get stats where service = operator and topic = inserts\n"
                              "get stats where service = publisher and topic = files\n"
                              "get stats where service = operator and topic = summary and format = json\n"
                              "get stats where service = publisher and topic = summary and format = json",
                   'text': "Provide statistics on a service enabled omn the node.",
                   'keywords': ["monitor"],
               }
               },

        "unused": {'command': net_utils.get_available_port,
                  'words_count': 6,
                  'help': {
                      'usage': "get unused [tcp/rest/msg] port for [node type]",
                      'example': "get unused tcp port for operator",
                      'text': "Return a port number that can be assigned to the service.",
                      'keywords': ["configuration"],
                  }
                  },

        "node name": {'command': node_info.get_current_node_name,
                      'key_only': True,
                      'help': {
                          'usage': "get node name",
                          'example': "get node name",
                          'text': "Returns the node name.",
                          'link': 'blob/master/anylog%20commands.md#get-node-name',
                          'keywords': ["cli", "configuration"],
                      }
                      },

        "streaming conditions": {'command': streaming_conditions.get_streaming_condition,
                            'words_min': 3,
                            'help': {
                                'usage': "get streaming conditions where dbms = [dbms_name] and table = [table_name]",
                                'example': "get streaming conditions\n"
                                           "get streaming conditions where dbms = my_dbms and table = my_table\n"
                                           "get streaming conditions where dbms = my_dbms and table = *",
                                'text': "Set a condition on the streaming data",
                                'link': "blob/master/streaming%20conditions.md#view-declared-conditions",
                                'keywords' : ["streaming", "data", "profiling"],
                                }
                            },


        "script": {'command': get_script,
                      'words_count': 3,
                      'help': {
                          'usage': "get script [script name]",
                          'example': "gets script autoexec",
                          'text': "Get the script data",
                          'keywords' : ["cli", "configuration"],
                        }
                      },

        "timezone info": {'command': utils_columns.get_timezone,
                 'words_count' : 3,
                 'help': {
                     'usage': "get timezone info",
                     'example': "get timezone info",
                     'text': "Get the timezone of the local machine.",
                     'keywords' : ["cli", "script"],
                    }
                 },

        'datetime': {
            'command': _to_datetime,
            'words_min' : 4,
            'help': {'usage': 'get datetime timezone [date-time string or function]',
                     'example': 'get datetime local now() + 3 days\n'
                                'get datetime utc date(\'now\',\'start of month\',\'+1 month\',\'-1 day\', \'-2 hours\', \'+2 minutes\')',
                     'text': 'Transform the function to a date-time string.\n',
                     'link': 'blob/master/queries.md#get-datetime-command',
                     'keywords' : ["cli", "script"],
                     },
            'trace': 0,
        },

        "hostname": {   'command': net_utils.get_local_host_name,
                        'help': {
                            'usage': "get hostname",
                            'example': "get hostname",
                            'text': "Get the hostname of te local machine.",
                            'keywords' : ["node info"],
                        }
                     },
        "connections": {'command' : net_utils.get_connections,
                        'key_only': True,
                        'with_format' : True,
                        'help': {
                            'usage': "get connections",
                            'example': "get connections",
                            'text': "Get the external connections to the node.",
                            'keywords' : ["node info"],
                        }
                    },
        "platforms": {'command': bplatform.get_platforms,
                        'key_only': True,
                        'help': {
                            'usage': "get platforms",
                            'example': "get platforms",
                            'text': "Get the list of connected blockchain platforms.",
                            'keywords' : ["node info"],
                        }
                    },
        "status": {'command': get_node_status,
                    'help': {
                        'usage': "get status where format = [reply format] include [list of dictionary names]",
                        'example': "get status\n"
                                   "get status where format = json\n"
                                   "get status include !disk_space !utilization !sensor_state\n"
                                   "get status include statistics",
                        'text': "Returns the keyword 'running' if the node is active.\n"
                                "If the 'include' keyword is specified, the values assigned to the specified variables are returned. The key 'statistics' is a special key adding default statistics.",
                        'link': "blob/master/monitoring%20nodes.md#the-get-status-command",
                        'keywords' : ["node info"],
                        }
                    },

        "disk": {'command': utils_io.get_disk_info,
                    "words_count" : 4,
                    'help': {
                        'usage': "get disk [usage/free/total/used/percentage] [path]",
                        'example': "get disk free d:",
                        'text': "Get information on the status of the disk addressed by the path.",
                        'link' : 'blob/master/monitoring%20nodes.md#monitoring-nodes',
                        'keywords' : ["node info", "monitor"],
                        }
                    },
        "access": {'command': utils_io.get_access,
                    'words_count' : 3,
                     'help': {
                            'usage': "get access [path and file or directory name]",
                            'example': "get access !archive_dir",
                            'text': "Get access rights to the provided file or directory.",
                            'keywords' : ["node info"],
                    }
             },

        "node info": {'command': utils_monitor.get_node_info,
                'words_min' : 4,
                'help': {
                    'usage': "get node info [function monitored] [attribute name]",
                    'example': "get node info cpu_percent\n"
                               "get node info net_io_counters bytes_recv",
                    'text': "Get monitored information on the current node (cpu_percent, cpu_times, cpu_times_percent, getloadavg, swap_memory, disk_io_counters, net_io_counters).",
                    'link' : 'blob/master/monitoring%20nodes.md#statistics-commands',
                    'keywords' : ["node info", "monitor"],
                    }
               },

        "memory": {'command': utils_monitor.get_memory_info,
                'words_min' : 3,
                'help': {
                    'usage': "get memory info",
                    'example': "get memory info",
                    'text': "Get information on the memory of the current node.",
                    'link' : 'blob/master/monitoring%20nodes.md#monitoring-nodes',
                    'keywords' : ["node info", "monitor"],
                    }
                 },

        "cpu info": {'command': utils_monitor.get_cpu_info,
                'key_only' : True,
                'help': {
                    'usage': "get cpu info",
                    'example': "get cpu info",
                    'text': "get information on the CPU of the current node.",
                    'link' : 'blob/master/monitoring%20nodes.md#monitoring-nodes',
                    'keywords' : ["node info", "monitor"],
                    }
                 },
        "cpu usage": {'command': utils_monitor.get_cpu_usage,
            'key_only': True,
            'help': {
                'usage': "get cpu usage",
                'example': "get cpu usage",
                'text': "get information on the current node usage of the CPU.",
                'link': 'blob/master/monitoring%20nodes.md#monitoring-nodes',
                'keywords' : ["node info", "monitor"],
                }
            },

    "os process": {'command': utils_monitor.get_os_process,
                        'words_min' : 3,
                        'help': {
                            'usage': "get os process\n"
                                     "get os process anylog\n"
                                     "get os process [pid]\n"
                                     "get os process all\n"
                                     "get os process list",
                            'example': "get os process anylog",
                            'text': "Get memory and CPU info for each OS process.\n"
                                    "get os process anylog - AnyLog Info.\n"
                                    "get os process [pid]  - Info on a specific pid.\n"
                                    "get os process all    - Info on all processes.\n"
                                    "get os process list   - The list of processes with their pids.\n",
                            'link': 'blob/master/monitoring%20nodes.md#the-get-os-process-command',
                            'keywords' : ["node info"],
                            }
                        },

        "machine connections": {'command': utils_monitor.get_machine_connections,
                'words_min' : 3,
                'help': {
                    'usage': "get machine connections\n"
                             "get machine connections where port = [port]",
                    'example': "get machine connections\n""get machine connections where port = 7850",
                    'text': "Return system-wide socket connections.",
                    'link' : 'blob/master/monitoring%20nodes.md#monitoring-nodes',
                    'keywords' : ["node info"],
                    }
                },
        "ip": {'command': utils_monitor.get_all_ip_addresses,
           'words_min': 3,
           'help': {
               'usage': "get ip list",
               'example': "get ip list",
               'text': "Get the list of IPs on this node.",
               'link': 'blob/master/monitoring%20nodes.md#monitoring-nodes',
               'keywords' : ["node info"],
           }
           },

        "cpu temperature": {'command': utils_monitor.get_cpu_temperature,
                'help': {
                    'usage': "get cpu temperature",
                    'example': "get cpu temperature",
                    'text': "Get the CPU temperature.",
                    'link' : 'blob/master/monitoring%20nodes.md#monitoring-nodes',
                    'keywords' : ["node info", "monitor"],
                }
           },

        "database": {'command': get_dbms_size,
                'help': {
                    'usage': "get database size [dbms name]",
                    'example': "get database size lsl_demo",
                    'text': "Get the size in bytes of a logical database.",
                    'link' : 'blob/master/sql%20setup.md#the-get-database-size-command',
                    'keywords' : ["data", "dbms"],
                    }
                },


        "servers": {'command': get_servers,
                 'help': {
                     'usage': "get servers where company = [company name] and dbms = [dbms name] and table = [table name] bring [key string]",
                     'example': "get servers\n"
                                "get servers where dbms = lsl_demo and table = ping_sensor\n"
                                "get servers where company = anylog and dbms = lsl_demo and table = ping_sensor\n"
                                "get servers where company = anylog bring [operator][ip] : [operator][port] --- [operator][id]",
                    'text': "Get info representing Operators supporting the detailed database and table associated with the company.",
                    'link' : "blob/master/anylog%20commands.md#get-servers",
                    'keywords' : ["metadata"],
                    }
                 },

        "tables": {'command': get_tables,
                'words_min' : 6,
                'help': {
                    'usage': "get tables where dbms = [dbms name] and format = [format type]",
                    'example': "get tables where dbms = dmci\n"
                               "get tables where dbms = *\n"
                               "get tables where dbms = aiops and format = json",
                    'text': "Get the list of tables for the named dbms or all databases (if named dbms is asterisk). Each table is flagged if declared on the blockchain and if declared locally.\n"
                            "[format type] is optional to determine the output format and is 'table' (default) or 'json'.",
                    'link' : "blob/master/sql%20setup.md#the-get-tables-command",
                    'keywords' : ["metadata", "data", "dbms"],
                    }
                },

        "table": {'command': get_table,
               'words_count': 12,
               'help': {
                   'usage': "get table [exist status/local status/blockchain status/rows count/complete status] where name = table_name and dbms = dbms_name",
                   'example': "get table local status where dbms = aiops and name = lic1_s\n"
                              "get table partitions names where dbms = aiops and name = lic1_sp\n"
                              "get table complete status where name = ping_sensor and dbms = anylog",
                   'text': "Provide info on a particular table.\n"
                           "exist status - details 'local status' and 'blockchain status'.\n"
                           "local status - True for a table hosted locally on the node or False if not defined.\n"
                           "Blockchain status - True for a defined on the blockchain or False if not defined.\n"
                           "rows count - provides the number of rows in the table.\n"
                           "complete status - returns all the optional info.",
                   'link' : 'blob/master/sql%20setup.md#the-get-table-command-get-table-status',
                   'keywords' : ["metadata", "data"],
               }
               },

        "rows count": {'command': get_rows_count,
               'words_min': 3,
               'help': {
                   'usage': "get rows count where dbms = [dbms name] and table = [table name] and format = [json] and group =[table/partition]",
                   'example': "get rows count\n"
                              "get rows count where dbms = my_dbms and group = table\n"
                              "get rows count where dbms = my_dbms\n"
                              "get rows count where dbms = my_dbms and table = my_table",
                   'text': "Get the count of rows in the specified table or all tables asigned to the specified database. The group variable determines if partitioned table shows an aggregated value for all prtitions.",
                   'link' : "blob/master/sql%20setup.md#the-get-rows-count-command",
                   'keywords' : ["metadata", "data", "dbms"],
                    }
               },
        "data distribution": {'command': get_data_distribution,
               'words_count': 11,
               'help': {
                   'usage': "get data distribution where dbms = [dbms name] and table = [table name]",
                   'example': "get data distribution where dbms = lightsanleandro and table = ping_sensor",
                   'text': "Get the count of rows in the requested table from all operator nodes that service the table.",
                   'link': "blob/master/sql%20setup.md#the-get-data-distribution-command",
                   'keywords': ["metadata", "data", "dbms"],
                    }
               },

        "files count": {'command': get_files_count,
                   'words_min': 3,
                   'help': {
                       'usage': "get files count where dbms = [dbms name] and table = [table name]",
                       'example': "get files count\n"
                                  "get files count where dbms = my_dbms and table = my_table",
                       'text': "Get the count of files in the specified table or all tables asigned to the specified database.",
                       'link': "blob/master/image%20mapping.md#get-files-count",
                       'keywords' : ["metadata", "unstructured data"],
                    }
                   },

        "columns": {'command': get_columns,
               'words_min': 10,
               'help': {
                   'usage': "get columns where dbms = [dbms name] and table = [table name] and format = [table/json]",
                   'example': "get columns where table = ping_sensor and dbms = dmci\n"
                              "get columns where table = ping_sensor and dbms = dmci and format = json",
                   'text': "Get the list of columns for the detailed table. The format key is optional.",
                   'link' : 'blob/master/sql%20setup.md#the-get-columns-command',
                   'keywords' : ["metadata", "data"],
                }
               },

        "network databases": {'command': get_network_dbms,
                'words_min': 3,
                'help': {
                    'usage': "get network databases where comapny = [company name]",
                    'example': "get network databases\n"
                               "get network databases where comapny = anylog",
                    'text': "Get the list of databases registered on the blockchain for a given company or all the companies.",
                    'keywords' : ["metadata", "data"],
                }
                },

        "views": {'command': get_views,
               'words_count': 5,
               'help': {
                   'usage': "get views for dbms [dbms name]",
                   'example': "get views for dbms lsl_demo",
                   'text': "Get the list of views for the detailed dbms.",
                   'keywords' : ["metadata", "data"],
                }
               },

        "error": {'command': err_to_txt,
               'help': {
                   'usage': "get error [error number]",
                   'example': "get error 32",
                   'text': "Get the message text associated with an error value.",
                   'keywords' : ["cli", "debug"],
                   }
               },
        "file timestamp": {'command': get_file_info,
              'words_count' : 4,
              'help': {
                  'usage': "get file timestamp [file name]",
                  'example': "get file timestamp d:/tmp/senor/readings.json",
                  'text': "Get the timestamp of the provided file.",
                  'keywords' : ["file"],
                }
              },

        "echo": {'command': get_echo_queue,
                    'help': {
                        'usage': "get echo queue",
                        'example': "get echo queue",
                        'text': "Get the echo commands from the current nodes and peer nodes.",
                        'keywords' : ["cli"],
                        }
                    },
        "config policies": {'command': get_config_policies,
             'Key_only' : True,
             'help': {
                 'usage': "get config policies",
                 'example': "get config policies",
                 'text': "List the configuration policies processed on the node.",
                 'link' : "blob/master/policies.md#monitor-policies-used-to-configure-a-node",
                 'keywords': ["configuration", "cli"],
             }
             },

        "compression": {'command': get_compression,
            'words_count' : 2,
            'help': {
                'usage': "get compression",
                'example': "get compression",
                'text': "Get compression ON/OFF status.",
                'keywords' : ["data", "configuration"],
                }
            },

        "members status": {'command': metadata.get_peers_status,
                'help': {
                    'usage': "get members status",
                    'example': "get members status",
                    'text': "Get status on members of the network.",
                    'keywords' : ["profiling"],
                }
            },

        "event log": {'command': get_logged_data,
                'help': {
                         'usage': "get event log",
                         'example': "get event log",
                         'text': "Get recently executed commands.",
                         'keywords' : ["log", "profiling"],
                    }
                },
        "error log": {'command': get_logged_data,
                  'help': {
                      'usage': "get error log",
                      'example': "get error log",
                      'text': "Get recently failed commands.",
                      'keywords' : ["log", "profiling"],
                    }
                  },
        "query log": {'command': get_logged_data,
                  'help': {
                      'usage': "get query log",
                      'example': "get query log",
                      'text': "Get recently executed queries (if query log was enabled).",
                      'keywords' : ["log", "profiling", "query"],
                    }
                  },
        "streaming log": {'command': get_logged_data,
                  'help': {
                      'usage': "get streaming log",
                      'example': "get streaming log",
                      'text': "If streaming log is enabled, returns southbound streaming calls (to enable use: \"set streaming log on\").",
                      'keywords' : ["log", "profiling", "streaming"],
                  }
                  },

        "file log": {'command': get_logged_data,
                  'help': {
                      'usage': "get file log",
                      'example': "get file log",
                      'text': "Get recently processed files.",
                      'keywords' : ["log", "profiling"],
                    }
                  },
        "dictionary": {'command': get_dictionary_values,
                 'words_min': 2,
                 'help': {
                     'usage': "get dictionary where format = [json/table]",
                     'example': "get dictionary\n"
                                "get dictionary where format = json",
                     'text': "Get the declared variables with their assigned values.",
                     'link' : "blob/master/monitoring%20nodes.md#the-get-dictionary-command",
                     'keywords' : ["cli", "debug", "profiling"],
                    }
                 },
        "env var": {'command': get_environment_variables,
                   'words_min': 3,
                   'help': {
                       'usage': "get env var where format = [json/table]",
                       'example': "get env var\n"
                                  "get env var where format = json",
                       'text': "Get the declared environment variables with their assigned values.",
                       'link': "blob/master/monitoring%20nodes.md#the-get-env-var-command",
                        'keywords' : ["cli", "configuration"],
                        }
                   },

    "databases": {'command': get_databases,
                   'key_only': True,
                   'with_format': True,
                   'help': {
                       'usage': "get databases",
                       'example': "get databases",
                       'text': "Get the list of connected databases on this node.",
                       'link' : 'blob/master/sql%20setup.md#the-get-databases-command',
                       'keywords' : ["node info", "dbms"],
                       }
                   },

        "user threads": {'command': get_user_threads,
                  'key_only': True,
                  'help': {
                      'usage': "get user threads",
                      'example': "get user threads",
                      'text': "Get the list of processes activated by the 'thread' command.",
                      'keywords' : ["node info", "configuration"],
                    }
                  },
        "system threads": {'command': utils_threads.get_system_threads,
                     'min_words': 3,
                     'help': {
                         'usage': "get system threads where pool = [pool type] and with_details = [true/false] and reset = [true/false]",
                         'example': "get system threads\n"
                                    "get system threads where pool = query\n"
                                    "get system threads where pool = rest and details = true",
                         'text': "Get the list of system threads and their status.",
                         'link' : "blob/master/node%20configuration.md#threads-configuration-and-monitoring",
                         'keywords': ["node info", "configuration", "monitor"],
                     }
                     },

    "query mode": {'command': get_query_params,
                'key_only': True,
                'help': {
                    'usage': "get query mode",
                    'example': "get query mode",
                    'text': "Get the query config params",
                    'keywords' : ["node info", "configuration"],
                    }
                },

        "partitions": {'command': get_partitions,
                   'help': {
                       'usage': "get partitions [info string]",
                       'example': "get partitions\n"
                                  "get partitioned dropped\n"
                                  "get partitions where dbms = lsl_demo and table = ping_sensor",
                       'text': "Get partitions declarations for all tables or a designated table or the recently dropped partitions.",
                       'keywords' : ["node info", "dbms"],
                        }
                   },

        "watch directories": {'command': get_watch_directories,
                   'key_only': True,
                   'help': {
                       'usage': "get watch directories",
                       'example': "get watch directories",
                       'text': "Get the list of watch directories.",
                       'keywords' : ["node info", "configuration"],
                    }
                   },

        "queries time": {'command': get_queries_time,
                          'help': {
                              'usage': "get queries time",
                              'example': "get queries time",
                              'text': "Retrieve statistics on queries execution time.",
                              'link' : 'blob/master/profiling%20and%20monitoring%20queries.md#statistical-information',
                              'keywords' : ["query", "profiling"],
                            }
                          },
        "local broker":   {'command': get_local_broker,
                        'key_only': True,
                            'help': {
                            'usage': "get local broker",
                            'example': "get local broker",
                            'text': "Statistics on the local broker (if the data is published to the IP and Port of the node's message broker server).",
                            'keywords' : ["data", "profiling", "streaming"],
                        }
                 },

        "scheduler": {'command': get_scheduler,
                     'help': {
                         'usage': "get scheduler [n]",
                         'example': "get scheduler\n"
                                    "get scheduler 1",
                         'text': "Information on the scheduled tasks. [n] - an optional ID for the scheduler. Scheduler 1 manage user scheduled tasks, 0 is the system scheduler.",
                         'link' : 'blob/master/alerts%20and%20monitoring.md#view-scheduled-commands',
                         'keywords' : ["monitor"],
                        }
                     },


        "operator": {'command': get_service_info,
                     'words_min': 2,
                     'help': {
                         'usage': "get operator [options] [where format = json]",
                         'example': "get operator\n"
                                    "get operator inserts\n"
                                    "get operator summary\n"
                                    "get operator config\n"
                                    "get operator summary where format = json",
                         'text': "Information on the Operator processes and configuration.",
                         'link' : "blob/master/monitoring%20calls.md#get-operator",
                         'keywords' : ["node info", "configuration", "background processes"],
                        }
                     },

        "blobs archiver": {'command': alarchiver.get_archiver,
                 'key_only': True,
                 'help': {
                     'usage': "get blobs archiver",
                     'example': "get blobs archiver",
                     'text': "Information on the Blobs Archiver processes.",
                     'link': "blob/master/background%20processes.md#the-blobs-archiver",
                     'keywords' : ["node info", "configuration", "background processes"],
                    }
                 },

        "data nodes": {'command': get_data_nodes,
                    'help': {
                        'usage': "get data nodes where company = [company name] and dbms = [dbms name] and table = [table name] and sort = (columns IDs)",
                        'example':  "get data nodes\n"
                                    "get data nodes where table = ping_sensor\n"
                                    "get data nodes where sort = (1,2)",
                        'text': "Information on the distribution of data from tables to servers hosting the data.",
                        'link' : 'blob/master/high%20availability.md#view-the-distribution-of-data-to-clusters',
                        'keywords' : ["metadata", "high availability"],
                    }
                 },
        "virtual tables": {'command': get_virtual_tables,
                   'help': {
                       'usage': "get virtual tables [info] where company = [company name] and dbms = [dbms name] and table = [table name]",
                       'example': "get virtual tables\n"
                                   "get virtual tables info\n"
                                  "get virtual tables where table = ping_sensor",
                       'text': "The list of tables managed by the network. The keyword 'info' is optional to include the Cluster ID, the number of copies, and if permissions to the table are granted.",
                       'keywords': ["metadata", "high availability"],
                   }
                   },


        "publisher": {'command': get_service_info,      # get_publisher,
                 'words_min': 2,
                 'help': {
                     'usage':   "get publisher [options] [where format = json]",
                     'example': "get publisher\n""get publisher files\n"
                                "get publisher summary\n"
                                "get publisher summary where format = json",
                     'text': "Information on the Publisher processes and configuration.",
                     'keywords' : ["node info", "configuration", "background processes"],
                    }
                 },

        "synchronizer": {'command': get_synchronizer,
                  'key_only': True,
                  'help': {
                      'usage': "get synchronizer",
                      'example': "get synchronizer",
                      'text': "Information on the Synchronizer processes.",
                      "link" : "blob/master/background%20processes.md#synchronizer-status",
                      'keywords' : ["metadata", "background processes", "configuration"],
                    }
                  },
        "metadata version": {'command': get_metadata_version,
                     'key_only': True,
                     'help': {
                         'usage': "get metadata version",
                         'example': "get metadata version",
                         'text': "Provide the ID of the metadata version used by the node.",
                         "link": "blob/master/background%20processes.md#synchronizer-status",
                         'keywords': ["metadata", "debug", "high availability"],
                     }
                     },

        "streaming": {'command': get_streaming_info,
                     'words_min': 2,
                     'help': {
                         'usage': "get streaming [info type] where format = [table/json]",
                         'example': "get streaming\n"
                                    "get streaming config\n"    # only configuration
                                    "get streaming where format = json",
                         'text': "Statistics on the streaming processes.",
                         'link' : "blob/master/monitoring%20calls.md#get-streaming",
                         'keywords' : ["streaming"],
                        }
                     },
        "rest calls": {'command': get_rest_calls,
                    'key_only': True,
                    'help': {
                        'usage': "get rest calls",
                        'example': "get rest calls",
                        'text': "Statistics on the REST calls.",
                        'link' : "blob/master/monitoring%20calls.md#get-rest-calls",
                        'keywords' : ["node info", "profiling"],
                    }
                  },


        "msg": {'command': get_msg_client_broker,
                     'help': {
                         'usage': "get msg clients where [options]\n"
                                  "get msg broker",
                         'example': "get msg clients\n"
                                    "get msg client where id = 3\n"
                                    "get msg client where topic = anylogedgex\n"
                                    "get msg client where broker = driver.cloudmqtt.com:18785 and topic = anylogedgex\n"
                                    "get msg broker",
                         'text': "Information on messages received by clients subscribed to message brokers.\n"
                                 "get msg client - return the info by client.\n"
                                 "get msg broker - return the info by broker.",
                         'link' : "blob/master/monitoring%20calls.md#get-msg-clients",
                         'keywords' : ["streaming"],
                        }
                     },

        "grpc": {'command': grpc_client.get_info,
                   'help': {
                       'usage': "get grpc clients",
                       'example': "get grpc clients",
                       'text': "List the active gRPC clients",
                       'link': 'blob/master/using%20grpc.md#retrieving-the-list-of-grpc-clients',
                       'keywords': ["streaming"],
                    }
                   },
        "grpc services": {'command': grpc_client.get_services_list,
                     'words_count': 7,
                     'help': {
                         'usage': "get grpc services where conn = [ip:port]",
                         'example': "get grpc services where conn = 127.0.0.1:50051",
                         'text': "List the gRPC sevices offered by the gRPC server",
                         'link': 'blob/master/using%20grpc.md#retrieving-the-list-of-grpc-services',
                         'keywords': ["streaming"],
                     }
                     },

        "query pool": {'command': get_pool_status,
                'words_min': 3,
                'help': {
                    'usage': "get query pool",
                    'example': "get query pool",
                    'text': "Status of query threads assigned by the command 'set threads pool [n]'.",
                    'link' : 'blob/master/node%20configuration.md#threads-configuration-and-monitoring',
                    'keywords' : ["query", "node info", "configuration"],
                    }
                },
        "tcp pool": {'command': get_pool_status,
                 'words_min': 3,
                 'help': {
                     'usage': "get tcp pool",
                     'example': "get tcp pool",
                     'text': "Status of TCP threads.",
                     'link' : 'blob/master/node%20configuration.md#threads-configuration-and-monitoring',
                     'keywords' : ["data", "node info", "configuration"],
                 }
                 },

        "rest pool":     {'command': get_pool_status,
                        'words_min': 3,
                        'help': {
                            'usage': "get rest pool",
                            'example': "get rest pool",
                            'text': "Status of REST threads.",
                            'link' : 'blob/master/node%20configuration.md#threads-configuration-and-monitoring',
                            'keywords' : ["streaming", "node info", "configuration"],
                            }
                        },
        "msg pool": {'command': get_pool_status,
                  'words_min': 3,
                  'help': {
                      'usage': "get msg pool",
                      'example': "get msg pool",
                      'text': "Status of Message Server threads.",
                      'link' : 'blob/master/node%20configuration.md#threads-configuration-and-monitoring',
                      'keywords' : ["streaming", "node info", "configuration"],
                  }
                  },
        "operator pool": {'command': get_pool_status,
                 'words_min': 3,
                 'help': {
                     'usage': "get operator pool",
                     'example': "get operator pool",
                     'text': "Status of Operator threads.",
                     'link' : 'blob/master/anylog%20commands.md#get-pool-info',
                     'keywords' : ["profiling"],
                 }
                 },

        "version": {'command': get_version,
                 'key_only': True,
                 'help': {
                     'usage': "get version",
                     'example': "get version",
                     'text': "Return the code version.",
                     'keywords' : ["node info"],
                    }
                 },
        "git":  {'command': get_git_version,
                'words_min': 3,
                'help': {
                    'usage': "get git [version/info] [path to github root]",
                    'example': "get git version\n"
                               "get git info\n"
                               "get git version D:/AnyLog-Code",
                    'text': "Return the git version or info.",
                    'keywords' : ["node info"],
                    }
                },

        "processes": {'command': get_processes_stat,
                'words_min': 2,
                'help': {
                    'usage': "get processes where format = [table/json]",
                    'example': "get processes\n""get processes where format = json",
                    'text': "List the background processes and their status.",
                    'link' : "blob/master/monitoring%20nodes.md#the-get-processes-command",
                    'keywords' : ["node info", "configuration"],
                    }
                },
        "json file struct": {'command': get_json_file_struct,
                  'key_only': True,
                  'help': {
                      'usage': "get json file struct",
                      'example': "get json file struct",
                      'text': "The JSON file name structure.",
                      'keywords' : ["debug"],
                    }
                  },
        "cluster info": {'command': get_cluster_info,
                         'key_only': True,
                         'help': {
                             'usage': "get cluster info",
                             'example': "get cluster info",
                             'text': "Info on the cluster Operators and tables supported.",
                             'link' : 'blob/master/high%20availability.md#view-the-distribution-of-data-to-an-operator',
                             'keywords' : ["metadata", "high availability"],
                            }
                         },

        "ha cluster status": {'command': get_ha_cluster_stat,
                       'help': {
                           'usage': "get ha cluster status where table = [TSD table name]",
                           'example': "get ha cluster status\n"
                                      "get ha cluster status where table = tsd_128",
                           'text': "Provide the HA synchronization status of peers supporting the same cluster.",
                           'link': 'blob/master/high%20availability.md#node-synchronization-status',
                           'keywords': ["high availability"],
                            }
                       },

        "tsd": {'command': query_time_file,
            'words_min': 3,
            'help': {
                'usage': "get tsd [details/summary/error] where [options]",
                'example': "get tsd details\n"
                           "get tsd error details table = tsd_info and hash = a00e6d4636b9fd8e1742d673275a75f7 and format = json\n"
                           "get tsd summary where table = *\n"
                           "get tsd summary where table = tsd_61",
                'text': "Retrieve entries or summaries from one or more TSD tables.",
                "link": "blob/master/high%20availability.md#retrieve-information-from-tsd-tables",
                'keywords': ["high availability"],
            }
            },

        "tsd list": {'command': get_tsd_list,
               'key_only': True,
               'help': {
                   'usage': "get tsd list",
                   'example': "get tsd list",
                   'text': "Provide the list of TSD tables on this node.",
                   'link' : 'blob/master/high%20availability.md#the-tsd-tables-on-each-node',
                   'keywords': ["high availability"],
               }
               },

        "files": {'command': get_files_list,
                 'help': {
                     'usage': "get files [directory path]\n"
                              "get files where dbms = [dbms name] and table = [table name] and id = [file name] and hash = [hash value] and date = [YYMMDD in UTC] and limit = [limit]",
                     'example': "get files !prep_dir\n"
                                "get files where dbms = blobs_video and date = 220723 and limit = 100",
                     'text': "List of files in a given directory, or, list of files stored in a blob database.",
                     'link': "blob/master/image%20mapping.md#get-the-list-of-files-stores-in-the-blobs-database",
                     'keywords' : ["file"],
                    }
                 },
        "directories": {'command': get_dir_list,
              'help': {
                  'usage': "get directories [directory path]",
                  'example': "get directories !prep_dir",
                  'text': "List of sub-directories in a given directory.",
                  'keywords' : ["file"],
                }
              },
        "size": {'command': get_directory_size,
               'words_count' : 3,
              'help': {
                  'usage': "get size [directory path]",
                  'example': "get size !archive_dir",
                  'text': "List directories and their size for a given path.",
                  'keywords' : ["file"],
                }
              },

        "reply ip": {'command': net_utils.get_reply_ip,
              'key_only': True,
              'help': {
                  'usage': "get reply ip",
                  'example': "get reply ip",
                  'text': "The IP address to use for reply messages.",
                  'link': "blob/master/network%20configuration.md#setting-a-different-ip-address-for-replies",
                  'keywords' : ["configuration"],
                  }
                },

        "platform info": {'command': utils_monitor.get_platform_info,
                  'key_only': True,
                  'help': {
                      'usage': "get platform info",
                      'example': "get platform info",
                      'text': "Provide platform information.",
                      'keywords' : ["node info"],
                  }
               },
        "rest server info": {'command': http_server.get_server_info,
                      'key_only': True,
                      'help': {
                          'usage': "get rest server info",
                          'example': "get rest server info",
                          'text': "Provide configuration information of the REST server.",
                          'keywords' : ["configuration", "streaming", "background processes"],
                      }
                      },

        "archived files": {'command': utils_io.get_archived,
                      'words_count': 4,
                      'help': {
                          'usage': "get archived files [YYYY-MM-DD]",
                          'example': "get archived files 2021-02-21",
                          'text': "List files in archive for the specified date",
                          'link' : 'blob/master/background%20processes.md#view-archived-file',
                          'keywords' : ["configuration", "high availability"],
                        }
                      },
        "monitored": {'command': monitor.get_info,
                      'words_min': 2,
                      'help': {
                          'usage': "get monitored [topic]",
                          'example': "get monitored operator\n"
                                     "get monitored",
                          'text': "Provide the info on the monitored topics",
                          'link' : "blob/master/monitoring%20nodes.md#retrieving-monitored-info",
                          'keywords' : ["configuration", "monitor"],
                        }
                      },
        "data monitored": {'command': data_monitor.get_info,
                      'words_min': 3,
                      'help': {
                          'usage': "get data monitored",
                          'example': "get data monitored\n"
                                     "get data monitored where dbms = dmci and table = sensor_reading",
                          'text': "Provide the info on the monitored data",
                          'keywords' : ["monitor", "data"],
                        }
                      },
        "operator execution": {'command': get_operator_exec,
                       'words_min': 3,
                       'help': {
                           'usage': "get operator execution where node = [node id] and job = [job id]",
                           'example': "get operator execution where node = 10.0.0.78 and job = 12",
                           'text': "Provide the info on the status of the running queries identified by the ip and job id of the query node",
                           'link' : 'blob/master/profiling%20and%20monitoring%20queries.md#retrieving-the-status-of-queries-being-processed-on-an-operator-node',
                           'keywords' : ["profiling", "query"],
                           }
                       },

}

_id_methods = {

}
# ------------------------------------------------------------------------
# Command Dictionaries
# ------------------------------------------------------------------------
commands = {
    'connect dbms': {
        'command': _connect_dbms,
        'help': {'usage': 'connect dbms [db name] where type = [db type] and user = [db user] and password = [db passwd] and ip = [db ip] and port = [db port] and memory = [true/false] and connection = [db string] and autocommit = [true/false] and unlog = [true/false]',
                 'example': 'connect dbms test where type = sqlite\n'
                            'connect dbms sensor_data where type = psql and user = anylog and password = demo and ip = 127.0.0.1 and port = 5432',
                 'text': 'Connect to the specified dbms.\n'
                         '[db name] - The logical name of the database\n'
                         '[db_type] - The physical database - One of the supported databases such as psql, sqlite, pi\n'
                         '[db user] - A username recognized by the database\n'
                         '[db passwd] - The user dbms password\n'
                         '[db port] - The database port\n'
                         '[memory] - a bool value to determine memory resident data (if supported by the database)\n'
                         '[connection] - Database connection string\n'
                         '[autocommit] - A false value groups multiple statements into a single transaction\n'
                         '[unlog] - A true value unlogs tables to not write their changes to the WAL.',
                 'link' : 'blob/master/sql%20setup.md#connecting-to-a-local-database',
                 'keywords' : ["dbms", "configuration"],
        },
        'trace': 0,
    },
    'id': {
        'command': _process_id,
        'methods': _id_methods,
        'help': {'usage': 'id [command options]',
                 'link': 'blob/master/authentication.md#users-authentication'},
        'trace': 0,
    },

    'pi': {
        'command': _process_pi,
        'help': {'usage': 'pi sql text [sql stmt]\n'
                          'pi debug [on/off]',
                 'example': 'pi sql text \"select element_name, sensor_name from sub_t01\"',
                 'text': 'Issue a command to PI',
                 'keywords' : ["dbms"],
                 },
        'trace': 0,
    },

    'config from policy' : {
        'command': config_from_policy,
        'words_min' : 7,
        'help': {'usage': 'config from policy where id = [policy id]',
                 'example': 'config from policy where id = 7cfd6828c19aac35cf9462e20f2600f8',
                 'text': 'Configure a node from the policy info.',
                 'link' : 'blob/master/policies.md#assigning-a-configuration-policy-to-a-node',
                 'keywords': ["configuration", "script"], },
        'trace': 0,

    },

    'debug': {
        'command': _debug_method,
        'help': {'usage': 'debug [on/off] [process name] [values]',
                 'example': 'debug on exception\n'
                            'debug on consumer files = 5\n'
                            'debug off consumer',
                 'text': 'Enable or disable debug code on the detailed process.',
                 'keywords' : ["debug", "script"],},
        'trace': 0,
    },

    'disconnect dbms': {
        'command': _disconnect_dbms,
        'help': {'usage': 'disconnect dbms [dbms name]',
                 'example': 'disconnect dbms test',
                 'text': 'Disconnect from the specified dbms\n'
                         '[db name] - logical database',
                 'link' : 'blob/master/sql%20setup.md#disconnecting-from-a-database',
                 'keywords' : ["dbms", "configuration"],
                 },
        'trace': 0,
    },

    'info view': {
        'command': _info_table_view,
        'help': {'usage': 'info view [db name] [table name] [info type]',
                 'example': 'info view sensors readings columns\n'
                            'info view sensors readings exists',
                 'text': 'Return information on the specified view.\n'
                         '[dbms name] - the name of the logical database containing the table\n'
                         '[table name] - the name of the table\n'
                         '[info type] - the type of the requested info:\n'
                         'columns - provides the columns names and data types\n'
                         'exists - returns \'true\' or \'false\' indicating if the view exists',
                         'keywords' : ["dbms"],
                 },
        'trace': 0,
    },

    'info table': {
        'command': _info_table_view,
        'help': {'usage': 'info table [db name] [table name] [info type]',
                 'example': 'info table sensors readings columns\n'
                            'info table sensors readings exists\n'
                            'info table sensors readings partitions\n'
                            'info table sensors readings partitions last\n'
                            'info table sensors readings partitions first\n'
                            'info table sensors readings partitions count',
                 'text': 'Return information on the specified table.\n'
                         '[dbms name] - the name of the logical database containing the table\n'
                         '[table name] - the name of the table\n'
                         '[info type] - the type of the requested info:\n'
                         'columns - provides the columns names and data types\n'
                         'partitions - provides the partitions assigned to the table\n'
                         'partitions first - provides the oldest partition assigned to the table\n'
                         'partitions last - provides the latest partition assigned to the table\n'
                         'partitions count - returns the number of partition assigned to the table\n'
                         'partitions dates - provides the partitions assigned to the table with the date range for each partition\n'
                         'exists - returns \'true\' or \'false\' indicating if the table exists',
                 'link' : 'blob/master/anylog%20commands.md#partition-command',
                 'keywords' : ["dbms"],
                 },
        'trace': 0,
    },


    'sql': {
        'command': _issue_sql,
        'help': {'usage': 'sql [dbms name] [query options] [select statement]',
                 'example': 'run client () sql my_dbms format = table "select insert_timestamp, device_name, timestamp, value from ping_sensor limit 100"',
                 'text': 'Issue a SQL statement to nodes in the network. The "run client" directs the call to the designated nodes or, if empty, the network protocol determines the destination.',
                 'link': 'blob/master/queries.md#query-nodes-in-the-network',
                 'keywords' : ["query"],

                 },
        'trace': 0,
    },

    'create work directories': {
        'command': utils_io._create_anylog_dirs,
        "key_only" : True,
        'help': {'usage': 'create work directories',
                 'example': 'create work directories',
                 'text': 'Create the work directories at their default locations or locations configured using "set anylog home" command.',
                 'link' : 'blob/master/getting%20started.md#local-directory-structure',
                 'keywords' : ["configuration"],
                 },
        'trace': 0,
    },

    'create table': {
        'command': _create_table,
        'help': {'usage': 'create table [table name] where dbms = [dbms_name]',
                 'example': 'create table ping_sensor where dbms = lsl_demo\n'
                            'create table ledger where dbms = blockchain\n'
                            'create table tsd_info where dbms = almgm',
                 'text': 'Creates a data table assigned to the named database. The structure of the table is derived from the metadata in the blockchain\n'
                         'The tables \'ledger\' and \'tsd_info\' are system tables and their structure is predefined',
                'link' : 'blob/master/sql%20setup.md#creating-tables',
                 'keywords' : ["data"],
                 },
        'trace': 0,
    },

    'create view': {
        'command': _create_view,
        'help': {
            'usage': 'create view [dbms name].[table name] (comma seperated column names, values and mapping information)',
            'example': 'create view pi_central.Master.Element.ElementHierarchy (  row_id SERIAL PRIMARY KEY,  insert_timestamp TIMESTAMP  NOT NULL DEFAULT NOW(),  Element VARCHAR,  Attribute VARCHAR,  ar.timestamp TIMESTAMP, ar.value INT )\n'
                       'create view pge_pi_central.Master.Element.ElementHierarchy (  path using webid VARCHAR,  Element using parentelement VARCHAR,  Attribute not used,  timestamp TIMESTAMP, value INT )',
            'text': 'Creates a view assigned to the named database. The view allows to unify data from tables with different schemas\n'
                    'The column information can include mapping associating table columns to the columns declared in the view\n'
                    'The mapping is done by adding the keyword \'using\' followed by the column name in the table\n'
                    'The keywords \'not used\' declare that a column defined in the view is not represented in the table',
            'keywords' : ["data"],
        },
        'trace': 0,
    },

    'drop dbms': {
        'command': _drop_dbms,
        'help': {'usage': 'drop dbms [dbms name] from [dbms type] where user = [user] and password = [password] and ip = [ip] and port = [port]',
                 'example': 'drop dbms lsl_demo from psql where user = anylog and ip = 127.0.0.1 and password = demo and port = 5432',
                 'text': 'Drop a logical database from a physical database.\r\nNote: if the database is active, call "disconnect dbms" before the "drop".',
                 'keywords' : ["data"],
        },
        'trace': 0,
    },

    "drop network table": {'command': drop_network_table,
                      'words_mon': 11,
                      'help': {
                          'usage': "drop network table where name = [table name] and dbms = [dbms name] and master = [master_node]",
                          'example': "drop network table where name = ping_sensor and dbms = lsl_demo and master = 10.0.0.25:2548",
                          'text': "Issue a 'drop table' command for all the operator nodes that host the table.\n"
                                  "Issue a 'blockchain drop policy' command to remove the policy from the ledger.",
                          'link' : 'blob/master/sql%20setup.md#dropping-a-table-on-all-nodes',
                          'keywords': ["data", "dbms"],
                      },
                    'trace': 0,
    },

    'drop table': {
        'command': _drop_table,
        'help': {'usage': 'drop table [table name] where dbms = [dbms name]',
                 'example': 'drop table ping_sensor where dbms = lsl_demo\n'
                            'drop table tsd_info where dbms = almgm',
                 'text': 'Drop a table in the named database. If the table is partitioned, all partitions are dropped.',
                 'link' : 'blob/master/sql%20setup.md#dropping-tables',
                 'keywords' : ["data", "dbms"],
                 },
        'trace': 0,
    },

    'drop partition': {
        'command': _drop_partition,
        'help': {'usage': 'drop partition [partition name] where dbms = [dbms name] and table = [table name] and keep = [value]',
                 'example': 'drop partition par_readings_2019_08_02_d07_timestamp where dbms = purpleair and table = readings\n'
                            'drop partition where dbms = purpleair and table = readings\n'
                            'drop partition where dbms = aiops and table = cx_482f2efic11_fb_factualvalue and keep = 5\n'
                            'drop partition where dbms = aiops and table = * and keep = 30',
                 'text': 'Drops a partition in the named database and table.\n'
                         '[partition name] is optional. If partition name is omitted, the oldest partition of the table is dropped.\n'
                         'keep = [value] is optional. If a value is provided, the oldest partitions will be dropped to keep the number of partitions as the value provided.'
                         'If the table has only one partition, an error value is returned.\n'
                         'If table name is asterisk (*), a partition from every table from the specified database is dropped.\n'
                         'If partition name is asterisk (*), all the partitions are dropped.',
                 'link': 'blob/master/anylog%20commands.md#drop-partition-command',
                 'keywords' : ["data"],
                 },

        'trace': 0,
    },

    'backup': {
        'command': _backup,
        'help': {
            'usage': 'backup table where dbms = [dbms name] and table = [table name] and partition = [partition name] and dest = [output directory]',
            'example': 'backup table where dbms = purpleair and table = readings and dest = !bkup_dir\n'
                       'backup partition where dbms = purpleair and table = readings and partition = par_readings_2018_08_00_d07_timestamp and dest = !bkup_dir',
            'text': 'Backup the data of a particular partition or all the partitions of a table.\n'
                    'Partition name is optional, if omitted all the partitions of the table participate in the backup process.\n'
                    'The data of each partition is writted in a JSON format to a file at the location specified with the keyword \'dest\'.',
            'link': 'blob/master/anylog%20commands.md#backup-command',
            'keywords' : ["data", "configuration"],
        },
        'trace': 0,
    },

    'event': {
        'command': _event_trigger,
        'help': {'usage': 'event [event_name] [info]',
                 'example': 'event drop_old_partitions where dbms_name = lsl_demo and max_size = 10MB\n'
                            'event file_processed 78eba9a0938d5f36b0a41135bf55b0e2.uploaded',
                 'text': 'Events trigger the processing of a script\n'
                         '[event_name] - A name that is assigned, in the dictionary to the script file. Some events are pre-defined (such as drop old partitions)\n'
                         '[info] - additional values that are passed to the script as variables',
                'keywords' : ["script", "configuration"],
                 },
        'trace': 0,
    },

    'time file': {
        'command': _time_file,
        'methods': _time_file_methods,
        'help': {
            'usage': 'time file [function name] [function params]',
            'link': 'blob/master/managing%20data%20files%20status.md#managing-data-files'},
        'trace': 0,
    },

    'from': {
        'command': _execute_from,
        'help': {'usage': 'from [json string] bring [attribute names or strings] separator = [value]',
                 'example': 'ip_port = from !selected_operator bring [\'operator\'][\'ip\'] \":\" [\'operator\'][\'port\']',
                 'text': 'Retriev values from a json object and creates a formatted string with the retrieved values.\n'
                         'Separator is optional and added to seperate multiple retrieved values.\n'
                         'If separator is the string "\\n" or "\\t", it is set as a new line or a tab respectively.',
                 'link': 'blob/master/json%20data%20transformation.md#the-from-json-object-bring-command',
                 'keywords' : ["data", "json"],
                 },
        'trace': 0,
    },

    'blockchain': {
        'command': _process_blockchain_cmd,
        'methods' : _blockchain_methods,
        'help': {'usage': 'blockchain [fumction name] [function params]',
                 'link': 'blob/master/blockchain%20commands.md'},
        'trace': 0,
    },

    'suggest create': {
        'command': _suggest_create,
        'help': {'usage': 'suggest create [input JSON file] where policy = [policy id]',
                 'example': 'suggest create !prep_dir/london.readings.json\n'
                            'suggest create !prep_dir/london.readings.json where policy = london_mapping',
                 'text': 'Generates a create statement based on the data in the JSON file. Adding a mapping policy ID is optional.',
                 'keywords' : ["data", "json"],
                 },
        'trace': 0,
    },

    'generate insert from json': {
        'command': _map_json_to_insert,
        'help': {
            'usage': 'generate insert from json where dbms_name = [dbms_name] and table_name = [table_name] and json_file = [json_file] and sql_dir = [sql_directory] and instructions = [instructions_id]',
            'example': 'generate insert from json where dbms_name = lsl_demo and table_name = ping_sensor and json_file = !source_file and sql_dir = !sql_dir and instructions = !instruction',
            'text': 'Based on a JSON file, generate INSERT statements to the specified file\n'
                    '[dbms name] - The logical database name\n'
                    '[table_name] - The logical table name\n'
                    '[json file] - input json file\n'
                    '[sql diectory] - The output directory, if not specified, the same directory as the JSON file\n'
                    '[instructions id] - The ID of the instructions dictating mapping JSON to SQL\n'
                    'The call returns the sql file name and path',
            'keywords' : ["data", "json"],
            },
        'trace': 0,
    },

    'system': {
        'command': _run_sys_process,
        'help': {'usage': 'system [timeout = seconds] [OS command]',
                 'example': 'system ls\n'
                            'system timeout = 20 python3 /home/ubuntu/EdgeLake/tests/gui/min_max_avg_per_day.py',
                 'text': 'Executes the specified OS command\n'
                         'timeout is an optional value allowing to exits after the threshold, default value is 5 and 0 value means no threshold.',
                 'keywords' : ["cli", "script"],
                 },
        'trace': 0,
    },

    'delete archive': {
        'command': delete_archive,
        'words_count' : 6,
        'help': {'usage': 'delete archive where days = [number of days]',
                 'example': 'delete archive where days = 60',
                 'text': 'Delete the files in the archive directory which are older than the specified number of days.\n'
                         'Always have a proper backup prior to deleting archived data.',
                 'link' : 'blob/master/background%20processes.md#delete-archived-files',
                 'keywords': ["configuration", "high availability"],
                 },
        'trace': 0,
    },

    'if': {
        'command': _process_if,
        'help': {'usage': 'if [int/str] [condition] then [command]',
                 'example': 'if not !json_data then process !script_create_table\n'
                            'if !old_value == \"128" then print values are equal\n'
                            'if not !old_value then old_value = 5\n'
                            'if !number.int < !value then echo true',
                 'text': 'A condition that is validated to determine if or which AnyLog commands are executed.\n'
                         'Conditions variables are evaluated as strings unless \'int\' or \'float\' is specified for the evaluation process.',
                 'link' : 'blob/master/anylog%20commands.md#conditional-execution',
                 'keywords' : ["script"],
                 },
        'trace': 0,
    },

    'else': {
        'command': _process_do_else,
        'help': {'usage': 'else [command]',
                 'example': 'else process !script_create_table',
                 'text': 'Executes the specified command if the last if stmt failed',
                 'link' : 'blob/master/anylog%20commands.md#conditional-execution',
                 'keywords' : ["script"],
                 },
        'trace': 0,
    },
    'do': {
        'command': _process_do_else,
        'help': {'usage': 'do [command]',
                 'example': 'do process !script_create_table',
                 'text': 'Executes the specified command if the last \'if\' stmt returned True',
                 'link' : 'blob/master/anylog%20commands.md#multiple-do---then-instruction',
                 'keywords' : ["script"],

                 },
        'trace': 0,
    },

    'rest': {
        'command': _rest_client,
        'words_min' : 6,
        'help': {'usage': 'rest [operation] where url=[url] and [option] = [value] and [option] = [value] ...',
                 'example': 'rest get where url = http://10.0.0.159:2049 and type = info and details = "get status"\n'
                            'rest get where url = http://73.202.142.172:7849 and User-Agent = AnyLog/1.23 and command = "get !my_key\n'
                            'rest get where url =  http://10.0.0.78:7849 and command = "sql aiops format = table select timestamp, value from lic_mv order by timestamp asc" and User-Agent = AnyLog/1.23\n'
                            '[file=!prep_dir/purpleair.json, key=results, show=true] = rest get where url = https://www.purpleair.com/json\n'
                            'rest put where url = http://10.0.0.25:2049 and dbms = alioi and table = temperature and mode = file and body = {"value": 50, "timestamp": "2019-10-14T17:22:13.0510101Z"},{"value": 50, "timestamp": "2019-10-14T17:22:13.0510101Z"}\n'
                            'rest delete where url = https://ipinfo.io/json',
                 'text': 'Issue a rest call. URL must be provided, the other key value pairs are optional headers and data values.',
                 'link': '/blob/master/anylog%20commands.md#rest-command',
                 'keywords' : ["api"],
                 },
        'trace': 0,
    },

    'run rest server': {
        'command': _run_rest_server,
        'words_min': 5,
        'help': {'usage': 'run rest server where external_ip = [external_ip ip] and external_port = [external port] and internal_ip = [internal ip] and internal_port = [internal port]'
                          ' and timeout = [timeout] and ssl = [true/false] and bind = [true/false]',
                 'example': 'run rest server where internal_ip = !ip and internal_port = 7849 and timeout = 0 and threads = 6 and ssl = true and ca_org = AnyLog and server_org = "Node 128"',
                 'text': 'Enable a REST srvice in a listening mode on the specified ip and port\n'
                         '[timeout] - Max wait time in seconds. A 0 value means no wait limit and the default value is 20 seconds.\n'
                         'If ssl is set to True, connection is using HTTPS.',
                 'link': 'blob/master/background%20processes.md#rest-requests',
                 'keywords' : ["configuration", "background processes", "api"],

                 },
        'trace': 0,
    },

    'run streamer': {
        'command': _run_streamer,
        'help': {'usage': 'run streamer',
                 'example': 'run streamer',
                 'text': 'Writes streaming data to files',
                 'link': 'blob/master/background%20processes.md#streamer-process',
                 'keywords' : ["configuration", "background processes"],

                 },
        'trace': 0,
    },

    'run grpc client': {
        'command': _run_grpc_client,
        'help': {'usage': 'run grpc client where name = [unique name] and ip = [IP] and port = [port] and policy = [policy id]',
                 'example': 'run grpc client where name = kubearmor and ip = 127.0.0.1 and port = 32767 and policy = deff520f1096bcd054b22b50458a5d1c',
                 'text': 'Subscribe to a gRPC broker on the provided IP and Port and map data using the designated mapping policy',
                 'link' : 'blob/master/using%20grpc.md#initiating-a-grpc-client',
                 'keywords': ["configuration", "background processes"],
                 },

        'trace': 0,
    },

    'run msg client': {
        'command': _run_msg_client,
        'help': {'usage': 'run msg client where broker = [url] and port = [port] and user = [user] and password = [password] and topic = (name = [topic name] and dbms = [dbms name] and table = [table name] and [participating columns info])',
                 'example': 'run msg client where broker = "driver.cloudmqtt.com" and port = 18975 and user = mqwdtklv and password = uRimssLO4dIo and topic = (name = test and dbms = "bring [metadata][company]" and table = "bring [metadata][machine_name] _ [metadata][serial_number]" and column.timestamp.timestamp = "bring [ts]" and column.value.int = "bring [value]")',
                 'text': 'Subscribe to a broker according to the url provided to receive data on the provided topic.',
                 'link' : 'blob/master/message%20broker.md#subscribing-to-a-third-party-broker',
                 'keywords' : ["configuration", "background processes"],
                 },

        'trace': 0,
    },

    'mqtt': {
        'command': _mqtt_request,
        'help': {'usage': 'mqtt publish where broker = [url] and topic = [topic]',
                 'example': 'mqtt publish where broker = "driver.cloudmqtt.com" and port = 18975 and user = mqwdtklv and password = uRimssLO4dIo and topic = test and message = "hello world"',
                 'text': 'Publish a message to the MQTT broker',
                 'link' : 'blob/master/message%20broker.md#publishing',
                'keywords' : ["api"],
                 },
        'trace': 0,
    },

    'run tcp server': {
        'command': _run_tcp_server,
        'words_min': 5,
        'help': {'usage': "run tcp server where external_ip = [ip] and external_port = [port] and internal_ip = [local_ip] and internal_port = [local_port] and bind = [true/false] and threads = [threads count]",
                'example': 'run tcp server where external_ip = !ip and external_port = !port  and threads = 3\n'
                           'run tcp server where external_ip = !external_ip and external_port = 7850 and internal_ip = !ip and internal_port = 7850 and threads = 6',
                'text': 'Set a TCP server in a listening mode on the specified IP and port.\n'
                        'The first pair of IP and Port that are used by a listener process to receive messages from members of the network.\n'
                        'The second pair of IP and Port are optional, to indicate the IP and Port that are accessible from a local network.\n'
                        'threads - an optional parameter for the number of workers threads that process requests which are send to the provided IP and Port. The default value is 6.',
                'link' :  'blob/master/background%20processes.md#the-tcp-server-process',
                'keywords' : ["configuration", "background processes"],
                 },
        'trace': 0,
    },

    'run message broker': {
        'command': _run_message_broker,
        'help': {'usage': 'run message broker where external_ip = [ip] and external_port = [port] and internal_ip = [local_ip] and internal_port = [local_port] and bind = [true/false] and threads = [threads count]',
                 'example': 'run message broker where external_ip = !ip and external_port = !port  and threads = 3\n'
                            'run message broker where external_ip = !external_ip and external_port = 7850 and internal_ip = !ip and internal_port = 7850 and threads = 6',
                 'text': 'Set a message broker in a listening mode on the specified IP and port.',
                 'link':'blob/master/background%20processes.md#message-broker',
                 'keywords' : ["streaming", "api", "configuration", "background processes"],
                 },
        'trace': 0,
    },

    'run kafka consumer': {
        'command': _run_kafka_consumer,
        'help': {'usage': 'run kafka consumer where ip = [ip] and port = [port]] and reset = [latest/earliest] and topic = [topic and mapping instructions]',
                 'example': 'run kafka consumer where ip = 198.74.50.131 and port = 9092 and reset = earliest and topic = (name = sensor and dbms = lsl_demo and table = ping_sensor and column.timestamp.timestamp = "bring [timestamp]" and column.value.int = "bring [value]")',
                 'text': 'Initialize a Kafka consumer that subscribes to one or more topics of a kafka instance and continuously polls data\n'
                         'assigned to the subscribed topics using the provided IP and Port. The reset value determines the offset whereas the default is latest.\n',
                         'link': 'blob/master/using%20kafka.md#anylog-serves-as-a-data-consumer',
                        'keywords' : ["streaming", "api", "configuration", "background processes"],
                 },
        'trace': 0,
    },

    'stream': {
        'command': stream_data,
        "words_min" : 2,        # addressing: "stream message" which are 2 words
        'help': {
            'usage': 'stream [json data] where dbms = [dbms name] and table = [table name]',
            'example': 'stream !json_data where dbms = my_dbms and table = my_table',
            'text': 'Stream json data into a database ',
            'keywords': ["streaming"],
        },
        'trace': 0,
    },

    'email': {
        'command': utils_output.email_send,
        'help': {
            'usage': 'email to [receiver email] where subject = [message subject] and message = [message text]',
            'example': 'email to my_name@my_company.com where subject = "anylog alert" and message = "message text"',
            'text': 'Sends an email message with the specified text to the destination email.',
            'link' : 'blob/master/alerts%20and%20monitoring.md#sending-an-email',
            'keywords' : ["monitor"],
        },
        'trace': 0,
    },

    'sms': {
        'command': utils_output.sms_send,
        'help': {
            'usage': 'sms to [receiver phone] where gateway = [sms gateway] and subject = [message subject] and message = [message text]',
            'example': 'sms to 6508147334 where gateway = tmomail.net',
            'text': 'Sends an email message with the specified text to the destination email.',
            'link' : 'blob/master/alerts%20and%20monitoring.md#sending-sms-messages',
            'keywords' : ["monitor"],
        },
        'trace': 0,
    },

    'run smtp client': {
        'command': utils_output.start_smtp_client,
        'help': {
            'usage': 'run smtp client where host = [host name] and port = [port] and email = [email address] and password = [email password] and ssl = [true/false]',
            'example': 'run smtp client where email = anylog.iot@gmail.com and password = google4anylog',
            'text': 'Initiates an SMTP instance encapsulates an SMTP connection to a server.',
            'link': 'blob/master/background%20processes.md#smtp-client',
            'keywords' : ["configuration", "background processes", "monitor"],
        },
        'trace': 0,
    },

    'run client': {
        'command': run_client,
        'min_words' : 3,
        'help': {'usage': 'run client (IPs and Ports) [AnyLog command]',
                 'example': 'run client 10.0.0.79 !port copy !test_json_in !test_json_out\n'
                            'run client (10.0.0.79:3376, 125.32.16.243:6734) copy !test_json_in !test_json_out\n'
                            'run client 10.0.0.79:3376 "get status"\n'
                            'run client (dbms=my_dbms) "event test_rows_added"\n'
                            'run client 10.0.0.79:3376 "show dbms"\n'
                            'run client () "sql my_dbms text select count(*) from my_table"',
                 'text': 'Make the current node a client of a peer or a group of peers in the network and send the peers a message.',
                 'link' : 'blob/master/network%20processing.md#the-tcp-messages',
                 'keywords' : ["configuration", "cli"],
                 },
        'trace': 0,
    },

    'file': {
        'command': _process_file,
        'methods': _file_methods,
        'help': {'usage': 'file  [option] [file name] [second file] [ignore error]',
                 'link': 'blob/master/file%20commands.md#file-commands'},
        'trace': 0,
    },

    'directory': {
        'command': _process_directory,
        'help': {'usage': 'directory [path] get [file|dir] [repeat = n]',
                 'example': 'work_file = directory !work_dir get file repeat = 5',
                 'text': 'get - Given a directory, retrieves a name of a files or a sub-directory from the specified path.\n'
                         '\tIf repeat is specified, n represents the number of seconds for a retry.',
                 'keywords' : ["configuration"],
                 },
        'trace': 0,
    },

    'process': {
        'command': _process_script,
        'help': {'usage': 'process [path and file name]',
                 'example': 'process $HOME/EdgeLake/tests/scripts/test_sensor.txt',
                 'text': 'Process the commands in the specified file',
                'link' : 'blob/master/node%20configuration.md#the-configuration-process',
                'keywords' : ["cli", "configuration"],},
        'trace': 0,
    },

    'thread': {
        'command': _new_thread,
        'help': {'usage': 'thread [path and file name]',
                 'example': 'thread $HOME/EdgeLake/tests/scripts/test_sensor.txt',
                 'text': 'Initiate a new thread to process the commands in the specified file',
                 'keywords' : ["script"],},
        'trace': 0,
    },

    'wait': {
        'command': _wait,
        'words_min' : 2,
        'help': {'usage': 'wait [max wait time in seconds] for [condition]',
                 'example': 'wait 8\n'
                 'wait 5 for !my_param\n'
                 'wait 5 for !my_dict.len == 2',
                 'text': 'Pause processing until time elapsed or a condition is satisfied.',
                 'link' : 'blob/master/anylog%20commands.md#the-wait-command',
                 'keywords' : ["cli", "script"],
                 },
        'trace': 0,
    },

    'create policy': {
        'command': create_policy,
        'words_min': 5,
        'help': {'usage': 'create policy [policy type] [with defaults] where [key value pairs]',
                 'example': 'create policy master with defaults where company = my_company and country = my_country',
                 'text': 'Create a policy with the specified attribute names and values.',
                 'keywords': ["cli", "script"],
                 },
        'trace': 0,
    },

    'incr': {
        'command': _incr,
        'help': {'usage': 'incr [variable] [value]',
                 'example': 'value = incr !value 2',
                 'text': 'Consider a variable as an integer and return the result of adding the value to the variable\n'
                         'If value is not specified it is considered to be 1',
                 'link' : 'blob/master/cli.md#the-incr-command',
                 'keywords' : ["cli", "script"],
                 },
        'trace': 0,
    },

    'partition': {
        'command': _partition_data,
        'help': {'usage': 'partition [dbms name] [table name] using [column name] by [time interval]',
                 'example': 'partition lsl_demo ping_sensor using timestamp by 2 days\n'
                            'partition lsl_demo ping_sensor using timestamp by month\n'
                            'partition lsl_demo * using timestamp by month',
                 'text': 'Partition a table or a group of tables by time interval\n'
                         'Time intervals options are: year, month, week, days in a month',
                 'link': 'blob/master/anylog%20commands.md#partition-command',
                 'keywords' : ["configuration", "query"],

                 },

        'trace': 0,
    },

    'task': {
        'command': _process_task,
        'help': {'usage': 'task [stop/run/remove/init] where scheduler = [scheduler id] and name = [task name] and start = [start date and time]',
                 'example': 'task stop where name = "monitor disk space"\n'
                            'task resume where name = "monitor disk space"\n'
                            'task remove where name = "monitor disk space"\n'
                            'task run where name = "monitor disk space"\n'
                            'task init where name = "monitor disk space" and start = + 1d',
                 'text': 'Change the mode of a task or remove a task from the scheduler',
                 'link': 'blob/master/alerts%20and%20monitoring.md#managing-tasks',
                 'keywords' : ["monitor"],
                 },

        'trace': 0,
    },

    'query': {
        'command': _query_status,
        'methods': _query_status_methods,
        'help': {
                    'usage': 'query [operation] [job id|\'all\']',
                    'link': 'blob/master/profiling%20and%20monitoring%20queries.md#command-options-for-profiling-and-monitoring-queries'
                },
        'trace': 0,
    },

    'job': {
        'command': _process_job,
        'help': {'usage': 'job [operation] [job id|\'all\']',
                 'example': 'job status\n'
                            'job status 5\n'
                            'job status all\n'
                            'job active all\n'
                            'job run 3\n'
                            'job stop 23',
                 'text': 'Provides information on the jobs being executed and jobs recently executed and scheduled jobs\n'
                         'Info is provided to the specified \'Job ID\', to \'all\' buffered jobs or if the jobs are not identified, to the recent job\n'
                         'job status - Returns the status or the command of the most recent jobs or a particular job or all jobs\n'
                         'job active all - The list of jobs which are not completed\n'
                         'job run - Executes the specified job command\n'
                         'job stop - stops the execution of a particular running job or stops a scheduled job',
                 'keywords' : ["monitor", "profiling"],
                 },
        'trace': 0,
    },

    'data monitor': {
            'command': data_monitor.data_monitoring,
            'help': {'usage': 'data monitor where dbms = [dbms name] and table = [table name] intervals = [counter] and time = [interval time length] and value_column = [column name]',
                     'example': ' data monitor where dbms = dmci and table = sensor_table and intervals = 8 and time = 1 minute and value_column = value',
                     'text': 'Defines a monitoring process over the streaming data',
                     'link' : 'blob/master/monitoring%20data.md#monitoring-streaming-data',
                     'keywords' : ["monitor", "streaming"],
                     },
            'trace': 0,
    },

    'schedule': {
        'command': _schedule,
        'help': {'usage': 'schedule [options] task [command to execute]',
                 'example': 'schedule time = 10 seconds task system date\n'
                            'schedule time = 1 minute and name = "repeatable query" task run client () "sql anylog_test text SELECT max(timestamp) ping_sensor"',
                 'text': 'Schedule a repeatable command\n'
                         'options:\n'
                         'time - repeatable time specified in seconds, minutes hours and days\n'
                         'start - date and time to initiate repeatable calls to the task\n'
                         'name - a unique name identifying the task',
                 'link' : 'blob/master/alerts%20and%20monitoring.md#adding-tasks-to-the-scheduler',
                 'keywords' : ["monitor"],
                 },
        'trace': 0,
    },

    'run scheduler': {
        'command': _scheduler,
        'help': {'usage': 'run scheduler',
                 'example': 'run scheduler',
                 'text': 'Repeatedly execute scheduled jobs.',
                 'link' : 'blob/master/alerts%20and%20monitoring.md#invoking-a-scheduler',
                 'keywords' : ["configuration", "background processes", "monitor"],
                 },
        'trace': 0,
    },

    'run blockchain sync': {
        'command': _blockchain_sync,
        'help': {'usage': 'run blockchain sync [options]',
                 'example': 'run blockchain sync where source = master and time = 3 seconds and dest = file and dest = dbms and connection = !ip_port\n'
                            'run blockchain sync where source = blockchain and time = !sync_time and dest = file and platform = ethereum',
                 'text': 'Repeatedly update the local copy of the blockchain\n'
                         'Options:\n'
                         'source - The source of the metadata (blockchain or a Master Node).\n'
                         'dest - The destination of the blockchain data such as a file (a local file) or a DBMS (a local DBMS).\n'
                         'connection - The connection information that is needed to retrieve the data. For a Master, the IP and Port of the master node.\n'
                         'time - The frequency of updates.',
                 'link': 'blob/master/background%20processes.md#blockchain-synchronizer',
                 'keywords' : ["configuration", "background processes"],
                 },
        'trace': 0,
    },

    'run operator': {
        'command': _run_operator,
        'help': {'usage': 'run operator where [option] = [value] and [option] = [value] ...',
                 'example': 'run operator where create_table = true and update_tsd_info = true and archive_json = true and distributor = true and master_node = !master_node and policy = !operator_policy  and threads = 3\n'
                            'run operator where create_table = true and update_tsd_info = true and archive_json = true and distributor = true and blockchain = ethereum and policy = !operator_policy  and threads = 3',
                 'text': 'Monitors new data added to the watch directory and load the new data to a local database\n'
                         'Options:\n'
                         'policy - The ID of the operator policy.\n'
                         'compress_json - True/False to enable/disable compression of the JSON file.\n'
                         'compress_sql - True/False to enable/disable compression of the SQL file.\n'
                         'archive_json - True moves the JSON file to the \'archive\' dir if processing is successful. The file deleted if archive_sql is false.\n'
                         'archive_sql -  True moves the SQL file to the \'archive\' dir if processing is successful. The file deleted if archive_sql is false.\n'
                         'limit_tables - A list of comma separated names within brackets listing the table names to process.\n'
                         'craete_table - A True value creates a table if the table doesn\'t exist.\n'
                         'master_node - The IP and Port of a Master Node (if a master node is used).\n'
                         'update_tsd_info - True/False to update a summary table (tsd_info table in almgm dbms) with status of files ingested.',
                 'link': 'blob/master/background%20processes.md#operator-process',
                'keywords' : ["configuration", "background processes"],
                 },
        'trace': 0,
    },


    'run blobs archiver': {
        'command': _run_archiver,
        'help': {
            'usage': 'run blobs archiver where blobs_dir = [data directory location] and archive_dir = [archive directory location] and dbms = [true/false] and file = [true/false] and compress = [true/false]',
            'example': 'run blobs archiver where dbms = true and file = true and compress = false',
            'text': 'Archive large objects.',
            'link': 'blob/master/background%20processes.md#the-blobs-archiver',
            'keywords' : ["configuration", "background processes"],
            },
        'trace': 0,
    },

    'print': {
        'command': _print,
        'help': {'usage': 'print [text to print]',
                 'example': 'print !json_struct',
                 'text': 'Print output to the console, words starting with exclamation point are replaced with dictionary values\n'
                         'and words starting with dollar sign are replaced with system params.\n'
                         'A words or multiple words inside a single quotation are not modified.',
                 'keywords' : ["cli"],
                 },
        'trace': 0,
    },
    'echo': {
        'command': _echo,
        'help': {'usage': 'echo [text]',
                 'example': 'echo Hello World',
                 'text': 'If echo queue is enabled (set echo queue on) - text is send to the echo queue, otherwise it is send to stdout.',
                 'keywords' : ["cli"],
         },
        'trace': 0,
    },
    'get': {
        'command': _process_get,
        'methods' : _get_methods,
        'help': {'usage': 'get [info type] [info string]',
                 'link': 'blob/master/anylog%20commands.md#get-command'
                 },
        'trace': 0,
    },

    'reset': {
        'command': _process_reset,
        'methods': _reset_methods,
        'help': {'usage': 'reset [function name]',
                 'link': 'blob/master/anylog%20commands.md#reset-command'
                 },
        'trace': 0,
    },

    'set': {
        'command': _process_set,
        'methods' : _set_methods,
        'help': {'usage': 'set [function and config setting]',
                 'link': 'blob/master/anylog%20commands.md#set-command'
                 },
        'trace': 0,
    },

    'python': {
        'command': _python,
        'help': {'usage': 'python [python string]',
                 'example': "ip_port = python !ip + ':4028'\n"
                            "python 'D:/Node/EdgeLake/data/watch/'.rsplit('/',1)[0] + '.out'\n" 
                            "new_dir = python !watch_dir.rsplit('/',1)[0] + '.out'",
                 'text': 'Execute a Python command. Keys are replaced with values from dictionary',
                 'link' : 'blob/master/cli.md#the-python-command',
                 'keywords' : ["internal", "script"],
                 },
        'trace': 0,
    },

    'monitor': {
        'command': monitor.monitor_info,
        'help': {'usage': 'monitor [topic] where ip = [node-ip] and name = [node-name] and info = [json-struct] ',
                 'example': 'monitor operator where ip = 127.0.0.1 and name = operator and info = { "total events" : 1000, "events per second" : 10" }',
                 'text': 'Provide status information to an aggregator node that monitors status associated with a topic',
                 'link' : 'blob/master/monitoring%20nodes.md#organizing-nodes-status-in-an-aggregator-node',
                 'keywords' : ["monitor"],
              },
        'trace': 0,
    },

    'json': {
        'command': _to_json,
        'help': {'usage': 'json [json object] [test]',
                 'example': 'cluster = json !my_policy\n'
                            'json !my_policy test',
                 'text': 'Validates that a data structure is in JSON.\n'
                         'If attribute-values in the JSON Object are variables, these variables are transformed to values and '
                         'the json command returns the JSON object with the transformed variables.\n'
                         'The keyword "test" is optional. If used, the comand returns "true" if the policy structure is valid and "false" if the policy structure is not valid',
                 'link' : 'blob/master/json%20data%20transformation.md#transforming-json-representatives-to-json-objects',
                 'keywords' : ["json", "debug"],
                 },
        'trace': 0,
    },


    'random substr': {
        'command': _random_substr,
        'help': {'usage': 'random substr [separator] [string]',
                 'example': 'one_machine = random substring " " !ips_ports',
                 'text': 'Split a string using a separator and returns a random substring',
                 'keywords' : ["debug"]
                 },
        'trace': 0,
    },

    'trace level': {
        'command': _set_trace,
        'help': {'usage': 'trace level = [value] [command]',
                 'example': 'trace level = 3 run client\n'
                            'trace level run tcp server\n'
                            'trace level = 1 tcp\n'
                            'trace level = 1 sql command\n'
                            'trace level = 1 sql command select',
                 'text': 'Enable debug on requested commands. Note: Trace level if off if the level is 0.',
                 'keywords' : ["debug"],
             },

        'trace': 0,
    },
    'help': {
        'command': _print_help,
        'help': {'usage': 'help [command]',
                 'example':
                     'help\n'
                     'help get\n'
                     'help connect dbms\n'
                     'help index\n'
                     'help index blockchain',
                 'text': 'Provide information on AnyLog commands.',
                 'link' : 'blob/master/getting%20started.md#the-help-command',
                 'keywords' : ["help"],
                 },
        'trace': 0,
    },

    'end script': {
        'command': _end_script,
        'help': {'usage': 'end script',
                 'example': 'end script',
                 'text': 'Terminates the current running script',
                 'link' : 'blob/master/cli.md#the-end-script-command',
                 'keywords' : ["script"],
                 },
        'trace': 0,
    },
    'exit': {
        'command': _exit,
        'words_min' : 2,
        'help': {'usage': 'exit [process type|reset]',
                 'example': 'exit node\r\n'
                            'exit tcp\r\n'
                            'exit rest\r\n'
                            'exit broker\r\n'
                            'exit scripts\r\n'
                            'exit scheduler\r\n'
                            'exit synchronizer\r\n'
                            'exit mqtt\r\n'
                            'exit kafka\r\n'
                            'exit smtp\r\n'
                            'exit workers\r\n'
                            'exit grpc all',
                 'text': 'exit node - terminate all process and shutdown\r\n'
                         'exit tcp - terminate the TCP listener thread\r\n'
                         'exit rest - terminate the REST listener thread\r\n'
                         'exit scripts - terminate the running scripts\r\n'
                         'exit scheduler - terminate the scheduler process\r\n'
                         'exit workers - terminate the query threads',
                'keywords' : ["script"],
                 },
        'trace': 0,
    },
    'goto': {
        'command': _return_goto,
        'help': {'usage': 'goto [name]',
                 'example': 'goto process_readings',
                 'text': 'Change the code execution location',
                 'link' : 'blob/master/cli.md#the-goto-command',
                 'keywords' : ["script"],
                 },
        'trace': 0,
    },
    'call': {
        'command': _return_goto,
        'help': {'usage': 'call [name]',
                 'example': 'call process_new_table',
                 'text': 'Changes the code execution location until \'return\' is called',
                 'keywords' : ["script"],
                 },
        'trace': 0,
    },

    'on error': {
        'command': _return_goto,
        'help': {'usage': 'on error goto [name]\n'
                          'on error call [name]\n'
                          'on error end script\n'
                          'on error ignore',
                 'example': 'on error goto process_error',
                 'text': 'Changes the execution location when a command returns an error\n'
                         'Using \'end script\' terminates the currently executed script\n'
                         'Using \'ignore\' continues code execution ignoring error values',
                'keywords' : ["script"],
                 },
        'trace': 0,
    },

    'return': {
        'command': _return_from_call,
        'help': {'usage': 'return',
                 'example': 'return',
                 'text': 'Terminate a process execution and sets the execution location to the instruction that follows the call to the terminated process.\n'
                         '\'return\' from the root script terminates the running script.',
                'keywords' : ["script"],
                 },
        'trace': 0,
    },

    'stop': {
        'command': _stop_thread,
        'help': {'usage': 'stop [all/thread id]',
                 'example': 'stop all',
                 'text': 'In debug mode, stops a particular thread or all threads',
                'keywords' : ["script"],
                },
        'trace': 0,
    },

    'next': {
        'command': _debug_next,
        'help': {'usage': 'next',
                     'example': 'next',
                     'text': 'In debug mode, executes the next command',
                     'link' : 'blob/master/cli.md#the-set-debug-command',
                     'keywords' : ["debug"],
                 },
        'trace': 0,
    },
    'continue': {
        'command': _debug_continue,
        'help': {'usage': 'continue',
                 'example': 'continue',
                 'text': 'Terminate debug interactive mode',
                 'link' : 'blob/master/cli.md#the-set-debug-command',
                 'keywords': ["debug"],
                 },
        'trace': 0,
    },

    'streaming data': {
        'command': policy_script,
        'words_count' : 4,
        'help': {'usage': 'streaming data [operation]',
                 'example': "streaming data ignore event\n"
                            "streaming data ignore attribute\n"
                            "streaming data ignore script\n"
                            "streaming data change policy",
                 'text': 'Manipulate processing of a script in a policy',
                 'keywords': ["script", "streaming"],
                 },
        'trace': 0,
    },

    'continuous': {
        'command': utils_monitor.continuous_monitoring,
        'words_min' : 2,
        'help': {'usage': 'continuous [list of monitored functions]',
                 'example': 'continuous cpu, cpu anylog, cpu postgres, get operator summary, get cpu usage',
                 'text': 'Provides continuous status. The process terminates if the enter key is hit.',
                 'link' : 'blob/master/monitoring%20nodes.md#monitoring-nodes-operations',
                 'keywords' : ["profiling"],
                 },
        'trace': 0,
    },

    'test': {
        'command': _process_test,
        'methods' : _test_methods,
        'help': {'usage': 'test [type of test to execute] [test params]',
                'link': 'blob/master/test%20commands.md#test-commands',
                },
        'trace': 0,
    }

}


# ---------------------------------------------------------------
# Add each command to a list as f(keyword)
# ---------------------------------------------------------------
def add_to_index(commands_dict, cmd_prefix, cmd):
    global help_index_
    if 'keywords' in commands_dict[cmd]["help"]:
        keywords = commands_dict[cmd]["help"]['keywords']
    else:
        keywords = ["not indexed"]

    if cmd_prefix:
        cmd_txt = cmd_prefix + ' ' + cmd
    else:
        cmd_txt = cmd

    for key in keywords:
        if not key in help_index_:
            help_index_[key] = []        # New index term

        help_index_[key].append(cmd_txt)
# ---------------------------------------------------------------
# Using the commands and help to create an index by keywords
# Get the keywords and represent the command as f(keyword)
# ---------------------------------------------------------------
def update_help_index(commands_dict, cmd_prefix) :
    for cmd, info in commands_dict.items():
        if "methods" in info:
            update_help_index(info["methods"], cmd)
        else:
            add_to_index(commands_dict, cmd_prefix, cmd)
# ---------------------------------------------------------------
# Sort the list of commands
# ---------------------------------------------------------------
def sort_help_index():
    global help_index_
    for commands_list in help_index_.values():
        commands_list.sort()

# ---------------------------------------------------------------
# Add a service to service list
# Example: "tcp" :  ("TCP", net_utils.is_tcp_connected, tcpip_server.get_info),
# ---------------------------------------------------------------
def add_service(service_key, service_info):
    '''
    service_key - a unique key representing the service
    service_info - a list with service name + 2 methods - to determine if active + a method returning service info string
    '''
    global test_active_
    test_active_[service_key] = service_info