"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

# https://grpc.github.io/grpc/python/grpc.html#grpc-status-code
# Library: https://pypi.org/project/grpcio-tools/
# Example code: https://grpc.io/docs/languages/python/basics/

import os
import sys
import threading
import importlib
import time


try:
    import grpc
except:
    grpc_installed_ = False
else:
    grpc_installed_ = True
    try:
        # Install grpcio-reflection
        from grpc_reflection.v1alpha.reflection_pb2 import ListServiceResponse
        from grpc_reflection.v1alpha.reflection_pb2 import ServerReflectionRequest
        from grpc_reflection.v1alpha.reflection_pb2 import ServiceResponse
        from grpc_reflection.v1alpha.reflection_pb2 import ServerReflectionResponse
        from grpc_reflection.v1alpha.reflection_pb2_grpc import ServerReflectionStub

    except:
        grpc_reflection_installed_ = False
    else:
        grpc_reflection_installed_ = True
        try:
            from google.protobuf import json_format
        except:
            google_protobuff_installed_ = False
        else:
            google_protobuff_installed_ = True


import edge_lake.generic.process_status as process_status
import edge_lake.generic.process_log as process_log
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.params as params
from edge_lake.generic.interpreter import get_value_dict
from edge_lake.generic.streaming_data import add_data
from edge_lake.tcpip.mqtt_client import process_policy

connect_list_ = {}                  # The list of connections, key is ip and port, values include: exit_event, policy_id, mapping_policy, policy_type, policy_name
conn_list_attr_ = [
    ("Status","External"),           # "Active" "Terminated (SUCCESS)", "Terminated (Error)"
    ("Exit Event","Internal"),           # Thread exit object
    ("Connection","External"),           # IP and Port of target
    ("Proto Name","External"),
    ("Request Msg","External"),          # The request Message name
    ("Mapping Policy","Internal"),
    ("Policy Type","External"),
    ("Policy Name","External"),
    ("Policy ID","External"),
    ("Data Msgs","External"),
    ("Timeouts","External"),
    ("Error","External"),
]
title_list_ = ["Name (ID)"]
for entry in conn_list_attr_:
    if entry[1] == "External":
        title_list_.append(entry[0])    # The list of attributes returned to the user in a "get grpc clients" call


connect_offset_status_ = 0     #"Active" "Terminated (SUCCESS)", "Terminated (Error)"
connect_offset_exit_ = 1        # Location of exit event
connect_offset_data_ = 9      # Location of Counter events
connect_offset_timeouts_ = 10  # Location of Counter timeouts
connect_offset_err_msg_ = 11  # Location of Error message

# ----------------------------------------------------------------------
# Test if needed lib in installed
# ----------------------------------------------------------------------
def is_installed():
    global grpc_installed_
    global grpc_reflection_installed_
    global google_protobuff_installed_
    return (grpc_installed_ and grpc_reflection_installed_ and google_protobuff_installed_)
# ----------------------------------------------------------------------
# Exit a process, process id is ip and port, or the string "all"
# ----------------------------------------------------------------------
def exit( process_id:str ):
    ret_val = process_status.SUCCESS
    if process_id == "all":
        for values in connect_list_.values():
            exit_event = values[connect_offset_exit_]
            try:
                exit_event.set()
            except:
                ret_val = process_status.gRPC_process_failed
    else:
        # Exit specific connection
        # Process ID is available using the command: get grpc clients
        if not process_id in connect_list_:
            ret_val = process_status.gRPC_wrong_connect_info
        else:
            exit_event = connect_list_[process_id][connect_offset_exit_]
            try:
                exit_event.set()
            except:
                ret_val = process_status.gRPC_process_failed

    return ret_val
# ----------------------------------------------------------------------
# Test if gRPC Client process is running
# ----------------------------------------------------------------------
def is_running():
    # At least one subscription
    return len(connect_list_) > 0
# ----------------------------------------------------------------------
# Test if a connection is established to the IP and Port
# Key value is: [ip:port].proto_name
# ----------------------------------------------------------------------
def is_connected( key: str ):

    if key in connect_list_ and connect_list_[key][connect_offset_status_] == "Active":
        return True
    return False

# ----------------------------------------------------------------------
# Info returned to the get processes command
# ----------------------------------------------------------------------
def get_status_string(status):
    if not grpc_installed_:
        info_str = "gRPC library not installed"
    else:
        if not is_running():
            info_str = ""
        else:
            info_str = "connected to %s gRPC servers" % len(connect_list_)
    return info_str

# ----------------------------------------------------------------------
# Return the info on the gRPC clients
# satisfies command: get grpc clients
# ----------------------------------------------------------------------
def get_info(status, io_buff_in, cmd_words, trace):

    reply_list = []

    if len(cmd_words) != 3:
        return [process_status.ERR_command_struct, None]
    if cmd_words[2] != "clients" and cmd_words[2] != "client":
        # we allow clients or client
        return [process_status.ERR_command_struct, None]

    if not len(connect_list_):
        return [process_status.SUCCESS, "No active gRPC clients"]

    for key, info_list in connect_list_.items():
        output_info = [key]   # A list of values to return to user
        for index, attr_val in enumerate(info_list):
            if conn_list_attr_[index][1] == "External":
                # return value to user
                output_info.append(attr_val)

        reply_list.append(output_info)


    reply = utils_print.output_nested_lists(reply_list, "", title_list_, True)

    return [process_status.SUCCESS, reply]

# ----------------------------------------------------------------------
# Subscribe ti data on host and port
# Example: run grpc client where ip = 127.0.0.1 and port = 32767
# Example: run grpc client where ip = 127.0.0.1 and port = 50051


'''
Example .proto:

syntax = "proto3";
package clihandler;
option go_package="github.com/kubearmor-client/vm/clihandler";


message CliRequest {
    string KvmName = 1;
}
message ResponseStatus {
    string ScriptData = 1;
    string StatusMsg = 2;
    int32 Status = 3;
}
service HandleCli {
    rpc HandleCliRequest (CliRequest) returns (ResponseStatus);
}
        service - the name of the service between the client and server     - HandleCli
        server_func - the name of the function on the server                - HandleCliRequest
        request_msg - the request Message                                   - CliRequest
        response_msg - the response Message                                 - ResponseStatus
'''

# ----------------------------------------------------------------------
def subscribe(dummy: str, conditions: dict, conn:str, proto_dir:str, proto_name:str, connection_id:str, policy_id:str, mapping_policy:dict):
    '''
    conditions - the user provided data
    conn - IP and Port of target
    proto_dir - directory with .proto and the compiled files
    proto_name - procto file name
    connection_id - conn.proto_name
    policy_id - the mapping policy id
    mapping_policy - the json policy
    '''

    is_reconnect = conditions["reconnect"] if "reconnect" in conditions else False
    while 1:
        ret_val = process_grpc(conditions, conn, proto_dir, proto_name, connection_id, policy_id, mapping_policy)
        if ret_val != process_status.ERR_process_failure or not is_reconnect:
            break   # Exit or an error which is not connection error
        # Wait and Try again
        time.sleep(3)       #Wait for the gRPC server to restart

# ----------------------------------------------------------------------
def process_grpc(conditions: dict, conn:str, proto_dir:str, proto_name:str, connection_id:str, policy_id:str, mapping_policy:dict):
    '''
    conditions - the user provided data
    conn - IP and Port of target
    proto_dir - directory with .proto and the compiled files
    proto_name - procto file name
    connection_id - conn.proto_name
    policy_id - the mapping policy id
    mapping_policy - the json policy
    '''

    status = process_status.ProcessStat()
    exit_msg = f"gRPC Client process terminated {proto_name}.proto (name: {connection_id}) "
    ret_val = process_status.SUCCESS
    # Load the proto libraries from where they are placed by the user
    lib_name = f'{proto_name}_pb2'
    module_pb2 = get_load_moudle(status, proto_dir, lib_name)
    if not module_pb2:
        exit_msg += f"Failed to import lib to '{conn}' with '{proto_name}': {lib_name}"
        ret_val = process_status.Failed_to_import_lib
    else:
        lib_name = f"{proto_name}_pb2_grpc"
        module_pb2_grpc = get_load_moudle(status, proto_dir, lib_name)
        if not module_pb2_grpc:
            exit_msg += f"Failed to import lib to '{conn}' with '{proto_name}': {lib_name}"
            ret_val = process_status.Failed_to_import_lib
        else:
            policy_name = ""
            policy_type = ""
            if mapping_policy:
                policy_type = utils_json.get_policy_type(mapping_policy)
                if policy_type and "name" in mapping_policy[policy_type]:
                    policy_name = mapping_policy[policy_type]["name"]

            exit_event = threading.Event()   # Call exit_event.set() to terminate the thread

            service_name = conditions["service"][0]      # the name of the service between the client and server
            server_func = conditions["function"][0]     # the name of the function on the server
            request_msg = conditions["request"][0]      # the request Message
            response_msg = conditions["response"][0]     # the response Message
            debug_flag = conditions["debug"][0] if "debug" in conditions else False # A debug flag

            # Info that will be added to the JSON like: "request", "proto", "conn"
            if "add_info" in conditions:
                added_info = "{"
                for index, info_type in enumerate(conditions["add_info"]):
                    if index:
                        added_info += ','
                    if info_type == "conn":
                        added_info += f"\"<conn>\": \"{conn}\""
                    elif info_type == "proto":
                        added_info += f"\"<proto>\": \"{proto_name}\""
                    elif info_type == "request":
                        added_info += f"\"<request>\": \"{request_msg}\""
                added_info += ', '
            else:
                added_info = None  # DBMS name to use if policy is not provided


            connect_list_[connection_id] = ["Active", exit_event, conn, proto_name, request_msg, mapping_policy, policy_type, policy_name,  policy_id, 0, 0, ""]       # Counter timeouts + counter MSG

            # Create a channel to connect to the server
            channel = create_channel(status, conn)
            if channel:
                # Associate the stub with the server and allow the client to make RPC calls
                stub = set_stub(status=status, module_pb2_grpc=module_pb2_grpc, service_name=service_name, channel=channel)

                if stub:

                    request_obj = get_request_obj(status, debug_flag, conditions, module_pb2, request_msg)     # Make a request object based on the send message and send values
                    if request_obj:

                        while True:
                            # Execute the service against the server
                            ret_val, err_msg = execute_service(status, conditions, policy_id, mapping_policy, added_info, debug_flag, module_pb2, connection_id, exit_event, stub, server_func, request_msg, request_obj)
                            if ret_val:
                                if ret_val == process_status.AT_LIMIT:
                                    limit = conditions["limit"][0] if "limit" in conditions else 0  # Max messages to processes
                                    exit_msg += f"Exit from Service execution - events count at limit ({limit})"
                                elif ret_val == process_status.EXIT:
                                    exit_msg += "Exit from Service execution"
                                else:
                                    exit_msg += "Service execution failed"
                                    if err_msg:
                                        exit_msg += (": " + err_msg)
                                break

                            pass
                    else:
                        exit_msg += "Failed to create a request object for the gRPC call"
                        ret_val = process_status.ERR_process_failure


                close_channel(status=status, channel=channel)


            if ret_val != process_status.EXIT:
                # Test if schema has an error - error on a schema is updated in mapping_policy.process_event()
                if mapping_policy and "mapping" in mapping_policy and "schema" in mapping_policy["mapping"] and "__error__" in mapping_policy["mapping"]["schema"]:
                    error_msg = mapping_policy["mapping"]["schema"]["__error__"]
                else:
                    error_msg = exit_msg
                connect_list_[connection_id][connect_offset_status_] = "Terminated (Error)"
                connect_list_[connection_id][connect_offset_err_msg_] = error_msg
            else:
                connect_list_[connection_id][connect_offset_status_] = "Terminated (Success)"


    process_log.add_and_print("event", exit_msg + f" [{process_status.get_status_text(ret_val)}]")
    return ret_val

# ----------------------------------------------------------------------
# Create a gRPC channel against another machine.
# ----------------------------------------------------------------------
def create_channel(status:process_status, conn:str, credentials:dict=None):

    if credentials is not None:
        try:
            channel = grpc.secure_channel(target=conn, credentials=credentials)
        except:
            errno, value = sys.exc_info()[:2]
            status.add_error(f"Failed to create a secure channel against {conn} (Error: ({errno}) {value})")
            channel = None
    else:
        try:
            channel = grpc.insecure_channel(target=conn)
        except:
            errno, value = sys.exc_info()[:2]
            status.add_error(f"Failed to create a insecure channel against {conn} (Error: ({errno}) {value})")
            channel = None

    return channel

# ----------------------------------------------------------------------
# Close the gRPC channel
# ----------------------------------------------------------------------
def close_channel(status, channel):
    try:
        channel.close()
    except:
        errno, value = sys.exc_info()[:2]
        status.add_error(f"Failed to close gRPC channel (Error: ({errno}) {value})")

# ----------------------------------------------------------------------
#    Create service connection
# ----------------------------------------------------------------------
def set_stub(status, module_pb2_grpc, service_name, channel):
    '''
    module_pb2_grpc - the gRPC compiled library (an output from compiled .proto file)
    service_name - the service name in the .proto file + "Stub" Prefix
    channel - The tcp channel based on the connection info (conn to the gRPC server)
    '''

    # Example call:  stub = module_pb2_grpc.HandleCliStub(channel)

    attr_name = service_name + "Stub"

    try:

        stub = getattr(module_pb2_grpc, attr_name)(channel)
    except:
        errno, value = sys.exc_info()[:2]
        status.add_error(f"Failed to create a stub against the channel (Error: ({errno}) {value})")
        stub = None

    return stub


# ----------------------------------------------------------------------
#     Make a request with the request values that will be pushed to the server
# ----------------------------------------------------------------------
def get_request_obj(status, debug_flag, conditions, module_pb2, request_msg):


    try:
        request_obj = getattr(module_pb2, request_msg)()
    except:
        errno, value = sys.exc_info()[:2]
        status.add_error(f"Failed to initiate a request message ({request_msg}) (Error: ({errno}) {value})")
        request_obj = None

    if "value" in conditions:
        # Set the message with values (the values passed to the server function)
        Key_values = conditions["value"][0]
        if isinstance(Key_values, dict):
            # Needs to be a dictionary with the key + value to assign
            for attr_name, value_str in Key_values.items():
                if not isinstance(value_str, list) or len(value_str) != 1:
                    status.add_error(f"Failed to assign attr value ({attr_val}) to name ({value_str}) for: {request_msg}")
                    request_obj = None
                    break
                if isinstance(value_str[0],str):
                    attr_val = params.get_value_if_available(value_str[0])
                else:
                    attr_val = value_str[0]
                if debug_flag:
                    utils_print.output(f"[gRPC] [set] [attr name: '{attr_name}'] [attr value: '{attr_val}']", True)
                try:
                    setattr(request_obj, attr_name, attr_val)
                except:
                    status.add_error(f"Failed to assign attr value ({attr_val}) to name ({attr_name}) for: {request_msg}")
                    request_obj = None
                    break
        else:
            status.add_error(f"Failed to find attr name and attr value from: {Key_values}")
            request_obj = None

    return request_obj
# ----------------------------------------------------------------------
#     Execute the service.
# ----------------------------------------------------------------------
def execute_service(status, conditions, policy_id, mapping_policy, added_info, debug_flag, module_pb2, connection_id, exit_event, stub, server_func, request_msg, request_obj):
    '''
    added_info - Additional info to add to the json output
    connection_id - the ID with process Info
    '''
    global connect_list_        # Info on connection

    dbms_name = conditions["dbms"][0] if "dbms" in conditions else None  # DBMS name to use if policy is not provided
    table_name = conditions["table"][0] if "table" in conditions else None  # Table name to use if policy is not provided
    limit = conditions["limit"][0] if "limit" in conditions else 0  # Max messages to processes
    ingest_data = conditions["ingest"][0]  # If True - process AnyLog
    timeout_sec = conditions["timeout"][0] if "timeout" in conditions else 5  # timeout in seconds

    dir_dict = get_value_dict(conditions, ["prep_dir", "watch_dir", "err_dir", "bwatch_dir", "blobs_dir", ])

    policy_inner = mapping_policy["mapping"] if mapping_policy else None

    ret_val = process_status.SUCCESS
    info_list = connect_list_[connection_id]                    # Info on this connection
    err_msg = ""


    while True :                         # Triggered to exit the thread from wait
        # Get the data
        if exit_event.is_set():
            ret_val = process_status.EXIT
            break

        # Call the streaming RPC
        # Iterate through the stream of responses
        try:
            response = getattr(stub, server_func)(request_obj, metadata=None, timeout=timeout_sec)  # timeout=None should be timeout=5
        except grpc.RpcError as e:
            # Check if the exception is of the type _InactiveRpcError
            if isinstance(e, grpc._channel._InactiveRpcError):
                # Extract status code
                status_code = e.code()
                if status_code == grpc.StatusCode.DEADLINE_EXCEEDED:
                    # Timeout
                    info_list[connect_offset_timeouts_] += 1  # Count timeouts
                    continue

                # Extract details
                details = e.details()
                # Extract debug error string
                debug_error_string = e.debug_error_string()

                err_msg = f"gRPC service call failed: (status_code: {status_code}), (details: {details}), (error_str: {debug_error_string}))"
                status.add_error(err_msg)
                ret_val = process_status.ERR_process_failure
                break  # Exit because received data or exit_event_.set() was called
        except:
            errno, value = sys.exc_info()[:2]
            status.add_error(f"Failed to call a gRPC server (Error: ({errno}) {value})")
            ret_val = process_status.ERR_process_failure
            break

        if not hasattr(module_pb2, 'ReplyMessage'):
            status.add_error("ReplyMessage class or message definition is not defined in the module, which is generated from a .proto file")
            ret_val = process_status.ERR_process_failure
            break

        if isinstance(response, module_pb2.ReplyMessage):
            # Get the values from the reply message - for example:  example_message.name
            info_list[connect_offset_data_] += 1 # Counter messages
            ret_val = process_grpc_data(status, policy_id, policy_inner, added_info, response, request_msg, debug_flag, info_list[9], dir_dict, dbms_name, table_name, ingest_data)

            if ret_val:
                break

            if limit:
                # Max replies:
                if info_list[connect_offset_data_] >= limit:
                    ret_val = process_status.AT_LIMIT
                    break       # Exit because of limit


            continue        # Iterate again

        # Test: response.code() == grpc.StatusCode.DEADLINE_EXCEEDED  / response.details()
        try:
            iter(response)
        except:
            pass
        else:
            iter_flag = False
            try:
                for response_entry in response:
                    # Each response is a proto message object
                    iter_flag = True
                    info_list[connect_offset_data_] += 1
                    ret_val = process_grpc_data(status, policy_id, policy_inner, added_info, response_entry, request_msg, debug_flag, info_list[9],dir_dict, dbms_name, table_name, ingest_data)
                    if ret_val:
                        break

                    info_list[connect_offset_timeouts_] = 0  # Was connected - restart counting

                    if limit and info_list[connect_offset_data_] >= limit:
                        # Max replies:
                        break  # Exit because of limit

                    if exit_event.is_set():
                        # User terminated using the command: exit grpc id
                        ret_val = process_status.EXIT
                        break

                is_timeout = False
                if limit and info_list[connect_offset_data_] >= limit:
                    ret_val = process_status.AT_LIMIT
                    break
                if exit_event.is_set():
                    # User terminated using the command: exit grpc id
                    ret_val = process_status.EXIT
                    break

                #response_code = str(response)
                #response_details = f"Unrecognized gRPC error with message: {request_msg}"
                #ret_val = process_status.ERR_process_failure

            except grpc.RpcError as e:
                response_code = e.code()
                if response_code == grpc.StatusCode.DEADLINE_EXCEEDED:
                    is_timeout = True
                else:
                    is_timeout = False
                    response_details = e.details()
                    ret_val = process_status.ERR_process_failure
            except:
                errno, errval = sys.exc_info()[:2]
                is_timeout = False
                response_code = str(errno)
                response_details = errval
                ret_val = process_status.ERR_process_failure
            finally:
                if is_timeout:
                    info_list[connect_offset_timeouts_] += 1  # Count timeouts
                    continue
                if ret_val == process_status.ERR_process_failure:
                    err_msg = f"gRPC service failed to iterate with response: (status_code: {response_code}), (details: {response_details})"
                    status.add_error(err_msg)
                if ret_val:
                    # If ret_val is success - it will loop again
                    break   # end process
                if not iter_flag:
                    time.sleep(3)       # No data was pulled - sleep and redo

    if debug_flag:
        if ret_val == process_status.EXIT:
            utils_print.output(f"[gRPC] [Exit]", True)
        else:
            utils_print.output(f"[gRPC] [get] [error: {err_msg}]", True)

    return [ret_val, err_msg]

# ----------------------------------------------------------------------
# Transform to JSON and update AnyLog
# ----------------------------------------------------------------------
def process_grpc_data(status, policy_id, policy_inner, added_info, grpc_data, request_msg, debug_flag, msg_counter, dir_dict, dbms_name, table_name, ingest_data):

    try:
        json_data = json_format.MessageToJson(grpc_data)
    except:
        errno, value = sys.exc_info()[:2]
        status.add_error(f"Failed to map gRPC response to JSON (Error: ({errno}) {value})")
        ret_val = process_status.ERR_process_failure
    else:
        ret_val = process_status.SUCCESS
        json_msg = str(json_data).replace("\n", "")

        if added_info:
            # Additional info to add to each JSON obj. It is used to identify the source or any other info that is important to include
            json_msg = added_info + json_msg[1:]


        if debug_flag:
            utils_print.struct_print(json_msg, True, True)

        if ingest_data:
            if policy_id:
                json_obj = utils_json.str_to_json(json_msg)
                if not json_obj:
                    status.add_error(f"Failed to map gRPC to JSON: {json_msg}")
                    ret_val = process_status.ERR_wrong_json_structure
                else:
                    if len(json_obj) == 1 and isinstance(json_obj, dict):
                        # Some gRPC frameworks wrap payloads in a default key (e.g., "message") when encoding responses.
                        # This happens when using protocol buffers with certain serialization settings.
                        first_value = next(iter(json_obj.values())) # This is the first value without the key
                        if isinstance(first_value, str):
                            json_obj = utils_json.str_to_json(first_value)
                            if not json_obj:
                                status.add_error(f"Failed to map the inner part of the gRPC message to JSON: {json_msg}")
                                ret_val = process_status.ERR_wrong_json_structure
                        elif not isinstance(first_value, dict):
                            status.add_error(f"Wrong format of the inner part of the gRPC message: {json_msg}")
                            ret_val = process_status.ERR_wrong_json_structure
                        else:
                            json_obj = first_value
                    if not ret_val:
                        ret_val = process_policy(status, policy_inner, policy_id, json_obj,  dir_dict["prep_dir"], dir_dict["watch_dir"], dir_dict["bwatch_dir"], dir_dict["err_dir"], dir_dict["blobs_dir"])
            elif dbms_name and table_name:
                # No mapping policy
                ret_val, hash_value = add_data(status, "streaming", 1, dir_dict["prep_dir"], dir_dict["watch_dir"], dir_dict["err_dir"], dbms_name, table_name, '0', '0', "json", json_msg)

    return ret_val
# ----------------------------------------------------------------------
# Get the list of services on a given IP and Port of a gRPC server
# Reply to command: get grpc services where conn = ip:port
#   https://grpc.github.io/grpc/python/grpc_reflection.html
#   https://grpc.github.io/grpc/python/_modules/grpc_reflection/v1alpha/proto_reflection_descriptor_database.html#ProtoReflectionDescriptorDatabase.get_services
# ----------------------------------------------------------------------
def get_services_list(status, io_buff_in, cmd_words, trace):

    if cmd_words[3] != "where":
        ret_val = process_status.ERR_command_struct
        reply = None
    else:
        conn = cmd_words[6]     # IP + Port string
        reply = None

        if not grpc_reflection_installed_:
            status.add_error("Failed to install grpcio_reflection")
            ret_val = process_status.Failed_to_import_lib
        else:

            channel = create_channel(status, conn)
            stub = ServerReflectionStub(channel)
            ret_val, services_list = get_services(status, stub)
            if not ret_val:
                if services_list:
                    reply_list = list( [entry] for entry in services_list)
                    reply = utils_print.output_nested_lists(reply_list, "", ["gRPC Services"], True)

    return [ret_val, reply]

# ----------------------------------------------------------------------
# Get the list of services on a given IP and Port of a gRPC server
# ----------------------------------------------------------------------
def get_services(status, stub):
    """
    Get list of full names of the registered services.

    Returns:
        A list of strings corresponding to the names of the services.
    """
    ret_val = process_status.SUCCESS
    try:
        request = ServerReflectionRequest(list_services="")
        response = do_one_request(stub, request, key="")
    except:
        errno, value = sys.exc_info()[:2]
        status.add_error(f"gRPC request from server failed (Error: ({errno}) {value})")
        ret_val = process_status.gRPC_process_failed
        reply = None
    else:
        try:
            list_services: ListServiceResponse = response.list_services_response
            services: list[ServiceResponse] = list_services.service
            reply = [service.name for service in services]
        except:
            errno, value = sys.exc_info()[:2]
            status.add_error(f"gRPC reply not recognized (Error: ({errno}) {value})")
            ret_val = process_status.gRPC_process_failed
            reply = None

    return [ret_val, reply]

def do_one_request(stub, request, key):
        # Return -> ServerReflectionResponse:
    response = stub.ServerReflectionInfo(iter([request]))
    res = next(response)
    if res.WhichOneof("message_response") == "error_response":
        # Only NOT_FOUND errors are expected at this layer
        error_code = res.error_response.error_code
        assert (
            error_code == grpc.StatusCode.NOT_FOUND.value[0]
        ), "unexpected error response: " + repr(res.error_response)
        raise KeyError(key)
    return res

# ----------------------------------------------------------------------
# If the module is not loaded, get the moudle, always return the module
# ----------------------------------------------------------------------
def get_load_moudle(status, proto_dir, moudle_name):

    separation_char = os.path.sep
    removed_chat = '\\' if separation_char == '/' else '/'  # set for windows or linux
    module_path = proto_dir.replace(removed_chat,separation_char)

    try:
        if not module_path in sys.path:
            sys.path.append(module_path)
    except:
        errno, value = sys.exc_info()[:2]
        status.add_error(f"Failed to set module path (Error: ({errno}) {value})")
        module_obj = None
    else:
        if moudle_name not in sys.modules:
            try:
                module_obj = importlib.import_module(moudle_name)
            except:
                errno, value = sys.exc_info()[:2]
                status.add_error(f"Failed to import library (Error: ({errno}) {value})")
                module_obj = None
        else:
            module_obj = sys.modules[moudle_name]

    return module_obj

