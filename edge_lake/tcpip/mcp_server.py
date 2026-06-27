"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

# '''
# The process initiated here does the following:
# 1) Identifies the MCP request
# 2) Map the request to native commands
# 3) Orchestrates the reply
#
# Claude configs for connection
# 1. pip install mcp-proxy
# 2. locate mcp-proxy.exe
#
# "edgelake-mcp-proxy": {
#   "command": "D:\\EdgeLake-code\\tom-EdgeLake\\venv\\Scripts\\mcp-proxy.exe",
#   "args": ["http://10.0.0.78:32149/mcp/sse"]
# },
#
# Config in: C:\Users\mshad\AppData\Roaming\Claude\claude_desktop_config.json
#
# {
#   "mcpServers": {
#     "anylog-api-mcp-proxy": {
#       "command": "D:\\AnyLog-Code\\EdgeLake\\venv\\Scripts\\mcp-proxy.exe",
#       "args": ["http://10.0.0.78:7849/mcp/sse"],              # "args": ["http://129.212.178.167:32349 /mcp/sse"],
#       "env": {},
#       "timeout": 30000
#     }
#   }
# }
#
# '''

# Note: Claude logs are at: C:\Users\mshad\AppData\Roaming\Claude\logs

MCP_OK = 200
MCP_BAD_REQUEST = 400

JSONRPC_VER = "2.0"
SERVER_NAME = "AnyLog Server"
SERVER_VERSION = "1.21.1"

import sys
import json
import uuid
import threading
import time


import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.version as version
import edge_lake.generic.trace_func as trace_func
import edge_lake.generic.utils_threads as utils_threads
import edge_lake.generic.utils_json as utils_json
import edge_lake.cmd.native_api as native_api
import edge_lake.generic.params as al_params
import edge_lake.job.job_scheduler as job_scheduler
import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.generic.interpreter as interpreter

ignore_list_ = [
    "sse_socket",
    "lock",
    "raw_socket"
]

info_to_client_ = [         # These are keys that are included in the info provided to the client
        "returns"
]

local_err_to_json_rpc_ = {
    process_status.ERR_wrong_json_structure : -32700,   # - Parse error
    process_status.ERR_process_failure : -32603,        # Internal error
    process_status.ERR_command_struct : -32602,         # Invalid params
    process_status.ERR_unrecognized_command :  -32601,  # - Method not found
    process_status.Client_call_not_supported : -32600   # Invalid Request
}

open_sessions_ = {}     # Dynamic session info as a f(session id)

integrate_cmd_struct_ = False       # A flag indicating if mcp defs from member_cmd.commands was integrated.

_session_counter = 0                # A counter for self generated session ID
_session_lock = threading.Lock()    # A lock for generating session ID

def exit_mcp():
    '''
    is_exit_node - If True, do not allow new sessions, If False, terminate existing sessions and allow new sessions

    Set to True if the node is in exit and properly terminate all MCP threads
    '''
    global open_sessions_

    if not len(open_sessions_):
        utils_print.output_box("No active MCP sessions", "green")

    # Mark all sessions as inactive to trigger thread exit
    for session_id, session in list(open_sessions_.items()):
        session["active"] = False

    # Wait for threads to exit (max 30 seconds)
    max_wait = 30
    waited = 0
    while len(open_sessions_) and waited < max_wait:
        time.sleep(1)
        waited += 1

    # Force cleanup any remaining sessions
    if len(open_sessions_):
        utils_print.output_box(f"Warning: {len(open_sessions_)} MCP sessions did not close cleanly", "yellow")
        open_sessions_.clear()
    else:
        utils_print.output_box("MCP sessions terminated", "green")


def is_mcp_connected():
    global open_sessions_
    # Called on "get processes"
    return True if len(open_sessions_) else False

def get_mcp_info( status = None ):
    global open_sessions_
    open_connections = len(open_sessions_)
    if not open_connections:
        reply = ""
    else:
        if open_connections == 1:
            reply = "1 connection open"
        else:
            reply = f"{open_connections} connections open"

    return reply

def get_mcp_connections():
    '''
    Return active connections
    '''
    global open_sessions_
    return len(open_sessions_)

# -------------------------------------------------------------------------------
# Return Statistics
# -------------------------------------------------------------------------------
def get_mcp_status(status, io_buff_in, cmd_words, trace):

    global ignore_list_     # List to ignore

    list_info = []
    for session_id, info in open_sessions_.items():
        ip_port = info["client"]
        is_active = info["active"]
        activity_time = info["activity_time"]
        idle_time = int(time.time()) - activity_time      # The time elapsed since last operation in seconds
        hours_time, minutes_time, seconds_time = utils_data.seconds_to_hms(idle_time)
        idle_time_str = f"{hours_time}:{minutes_time}:{seconds_time}"
        max_time = info["max_time"]
        hours_time, minutes_time, seconds_time = utils_data.seconds_to_hms(max_time)
        max_idle_time_str = f"{hours_time}:{minutes_time}:{seconds_time}"
        post_counter = info["post_counter"]     # Counter POST requests on the same session

        list_info.append((session_id, ip_port, is_active, idle_time_str, max_idle_time_str, post_counter))

    list_title = ["Session ID", "IP:Port", "Active", "Idle Time", "Max Idle Time", "Post Counter"]
    reply = utils_print.output_nested_lists(list_info, "", list_title, True)

    return [process_status.SUCCESS, reply]

# -------------------------------------------------------------------------------
# Set the number of seconds which an MCP connection can be open without activity

# set mcp client config where ip = * and time = 10 minutes      # Exit after 10 minutes
# -------------------------------------------------------------------------------
def set_mcp_client_config(status, io_buff_in, cmd_words, trace):

    global open_sessions_

    # get the conditions to execute the JOB
    #                               Must     Add      Is
    #                               exists   Counter  Unique

    keywords = {"ip": ("str",True, False, True),
                "port": ("bool", False, False, True),
                "time": ("int.time", True, False, True),
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words,
                                                                   5, 0, keywords,
                                                                   False)

    if ret_val:
        return ret_val

    ip = interpreter.get_one_value_or_default(conditions, "ip", None)
    port = interpreter.get_one_value_or_default(conditions, "port", None)
    sec_time = interpreter.get_one_value_or_default(conditions, "time", None)

    for session_info in open_sessions_.values():
        if ip == '*' and (port == '*' or not port):
            # Flag all with the time
            session_info["max_time"] = sec_time
        else:
            counter = 0
            ip_port = session_info["client"].split(":")
            if len(ip_port) == 2:
                if ip_port[0] == ip or ip == '*':
                    counter += 1
                if ip_port[1] == port or port == '*' or port == None:
                    counter += 1
                if counter == 2:
                    # Condition satisfied
                    session_info["max_time"] = sec_time
    return process_status.SUCCESS

# -------------------------------------------------------------------------------
# handle_sse_endpoint - Keeps a write socket open
# The SSE socket is kept open - because the caller flagged self._is_sse = True
# -------------------------------------------------------------------------------
def handle_sse_endpoint(handler, status, number_of_threads, listen_info):
    '''
    handler - the parent "self"
    status - status object
    number_of_threads - number of threads in the REST connections
    '''
    global open_sessions_
    global integrate_cmd_struct_
    """
    Handle SSE endpoint (called from http_server.py).

    Args:
        handler: ChunkedHTTPRequestHandler

    Returns:
        True if handled
    """

    trace_level = trace_func.get_func_trace_level("mcp")

    if not integrate_cmd_struct_:
        # Update mcp functionalities from member_cmd.commands
        update_functionalities(commands=member_cmd.commands)
        integrate_cmd_struct_ = True

    session_id = _get_session_id(handler.path)
    if not session_id:
        # Generate unique session ID
        is_new = True
        session_id = str(uuid.uuid4())
    else:
        is_new = False      # Reconnect existing socket

    sse_socket = handler.wfile
    raw_socket = handler.connection  # the underlying socket (we no longer touch it directly)

    if trace_level:
        utils_print.output("\n\n----------------------------\n\n", False)
        connect_type = "New" if is_new else "Existing"
        utils_print.output(f"\n\nCONNECT-{connect_type}: {session_id}\n\n", False)

    # End the HTTP GET request (set up SSE)
    try:
        handler.send_response(MCP_OK)
        handler.send_header("Content-Type", "text/event-stream")
        handler.send_header("Cache-Control", "no-cache")
        handler.send_header("Connection", "keep-alive")
        handler.send_header('Access-Control-Allow-Origin', '*')
        handler.send_header('X-Accel-Buffering', 'no')  # Disable nginx buffering
        handler.end_headers()
    except:
        errno, value, stack_trace = sys.exc_info()[:3]
        error_msg = f"MCP Error: Failed to send msg headers on GET request with error: '{value}'"
        status.add_error(error_msg)
        utils_print.output_box(error_msg)
        return False

    # If session existed earlier, mark old connection as inactive (RECONNECT)
    old = open_sessions_.get(session_id)
    if old:
        # 🔴 IMPORTANT CHANGE: do NOT shutdown/close the socket here.
        # We just tell the old loop to stop.
        old["active"] = False  # old SSE thread will see this and exit its loop

    try:
        client_ip, client_port = handler.connection.getpeername()
        client_ip_port = f"{client_ip}:{client_port}"
    except:
        client_ip_port = "Failed to retrieve"


    # Keep the session info as f(session_id)
    open_sessions_[session_id] = {
        "sse_socket": sse_socket,
        "lock": threading.Lock(),          # Multiple POSTs can be executed on the same session
        "raw_socket": raw_socket,          # kept for info, but we don't touch it
        "client" : client_ip_port,
        "active": True,                     # 🔵 NEW FLAG to control loop lifetime
        "activity_time": int(time.time()),      #  The time of last operation
        "max_time" : 3600,                # 1 hour max time without activity (default): Note 0 for no max, -1 for exit now
    }

    # If new session, send session_id to client
    err_msg = ""
    error_flag = False
    if is_new:
        threads_used = len(open_sessions_)
        if (threads_used) > (number_of_threads / 2):
            # No sufficient threads
            err_msg = f"No sufficient REST threads defined in AnyLog ({listen_info}) - REST threads: {number_of_threads}, MCP threads used: {threads_used} - Increase REST threads"
            status.add_error(err_msg)
            error_flag = True

        open_sessions_[session_id]["get_counter"] = 1        # Counter GET requests on the same session
        open_sessions_[session_id]["post_counter"] = 0       # Counter POST requests on the same session

        endpoint_path = f'/mcp/messages/{session_id}'
        try:
            # Send endpoint event (MCP SSE protocol spec)
            # Client needs to know where to POST messages
            # Format: data: /mcp/messages/{session_id}\nevent: endpoint\nid: 0\n\n
            sse_socket.write(f"data: {endpoint_path}\n".encode('utf-8'))
            sse_socket.write(b"event: endpoint\n")
            sse_socket.write(b"id: 0\n\n")
            sse_socket.flush()
        except:
            errno, value, stack_trace = sys.exc_info()[:3]
            error_msg = f"MCP Error: Failed to message a new session ID to client: '{value}'"
            status.add_error(error_msg)
            utils_print.output_box(error_msg)
            return False
    else:
        open_sessions_[session_id]["get_counter"] += 1  # Counter GET requests on the same session

    # Make client see a valid JSON message over SSE quickly
    if error_flag:
        event_msg = {
            "jsonrpc": JSONRPC_VER,
            "method": "notifications/message",
            "params": {
                "level": "info",
                "code": "CONNECTION_REJECTED",
                "data": f"SSE connection failed: {err_msg}"
            }
        }
    else:
        event_msg = {
            "jsonrpc": JSONRPC_VER,
            "method": "notifications/message",
            "params": {
                "level": "info",
                "data": "SSE connection established"
            }
        }
    sse_socket.write(f"data: {json.dumps(event_msg)}\n\n".encode("utf-8"))
    sse_socket.flush()

    if trace_level:
        utils_print.output(f"\n\nWait to close connection: {session_id}\n\n", False)

    try:
        if error_flag:
            time.sleep(5)   # sleep 5 seconds and exit
        else:
            while True:
                # 🔵 Check if this session is still active.
                session = open_sessions_.get(session_id)
                if not session or not session.get("active", True):
                    break

                max_time = session["max_time"]
                if max_time:
                    # Remove if no activity for max time sessions
                    if (int(time.time()) - session["activity_time"]) > max_time:
                        session["active"] = False
                        break   # Exit


                try:
                    with session["lock"]:
                        msg = {
                            "jsonrpc": "2.0",
                            "method": "notifications/message",
                            "params": {
                                "level": "info",
                                "data": "heartbeat " + str(time.time())
                            }
                        }
                        sse_socket.write(f"data: {json.dumps(msg)}\n\n".encode("utf-8"))
                        sse_socket.flush()
                except Exception:
                    # client disconnected or write failed
                    break

                time.sleep(15)   # sleep keeps thread alive

    finally:
        # cleanup this session's SSE
        session = open_sessions_.get(session_id, None)
        ip_port = session["client"] if session else "Not Available"
        if session and session["sse_socket"] is sse_socket:
            # Mark inactive and remove from map so any other thread sees it's gone
            session["active"] = False
            open_sessions_.pop(session_id, None)

        # 🔴 IMPORTANT: do NOT call shutdown() or close() on raw_socket here.
        # Let BaseHTTPRequestHandler.finish() handle socket closure safely.


        if error_flag:
            utils_print.output_box(
                f"MCP Server disconnected from client - Session ID: {session_id} : {err_msg}",
                "red"
            )
        else:
            if trace_level:
                utils_print.output_box(
                    f"MCP Server disconnected from client using: '{ip_port}' - Session ID: {session_id}",
                    "green"
                )

    return True

# -------------------------------------------------------------------------------
# Ping each of the MCP clients and close connection if client does not respond
# -------------------------------------------------------------------------------
def ping_mcp_clients(status, io_buff_in, cmd_words, trace):
    global open_sessions_

    terminated = []
    for session_id, info in open_sessions_.items():
        sse_socket = info['sse_socket']
        try:
            with open_sessions_[session_id]["lock"]:
                msg = {
                    "jsonrpc": "2.0",
                    "method": "notifications/message",
                    "params": {
                        "level": "info",
                        "data": "heartbeat " + str(time.time())
                    }
                }
                sse_socket.write(f"data: {json.dumps(msg)}\n\n".encode("utf-8"))
                sse_socket.flush()
        except Exception:
            # client disconnected
            terminated.append(session_id)


    for session_id in terminated:
        # cleanup this session's SSE
        sse_socket = open_sessions_[session_id]['sse_socket']
        try:
            sse_socket.close()
        except:
            pass
        open_sessions_.pop(session_id, None)
        utils_print.output_box(f"\n\nMCP Client Disconnected - Session ID:  {session_id}\n\n", "green")


    return process_status.SUCCESS

# ------------------------------------------------------------------------------------
# Process post
#-------------------------------------------------------------------------------------
def handle_messages_endpoint(handler, status):
    '''
    The starting point for the MCP process
    Handle POST /mcp/messages/{session_id} - receive MCP messages.

    Parses JSON-RPC requests from the client, routes them to the MCP

    Args:

        status - the status object of the thread
        handler - the REST (POST) object (handler: ChunkedHTTPRequestHandler instance)

    Returns:
        True if handled successfully

    '''
    global open_sessions_

    # Enable trace on the cli: trace level = 1 mcp
    trace_level = trace_func.get_func_trace_level("mcp")

    session_id = _get_session_id(handler.path)

    if not session_id:

        # This behaviour from some clients, no Session ID - we will create an ephemeral session
        # And process the call - hope that the client will be using it

        ephemeral_session = True        # Made up session ID

        sse_socket = handler.wfile
        raw_socket = handler.connection  # the underlying socket (we no longer touch it directly)
        try:
            client_ip, client_port = handler.connection.getpeername()
            client_ip_port = f"{client_ip}:{client_port}"
        except:
            client_ip_port = "Failed to retrieve"
            client_ip = "00000000000"

        session_id = f"eph_{client_ip}"  # No session ID

        if not session_id in open_sessions_:
            # Create a session by the IP
            open_sessions_[session_id] = {
                "sse_socket": sse_socket,
                "lock": threading.Lock(),  # Multiple POSTs can be executed on the same session
                "raw_socket": raw_socket,  # kept for info, but we don't touch it
                "client": client_ip_port,
                "active": True,  # 🔵 NEW FLAG to control loop lifetime
                "activity_time": int(time.time()),  # The time of last operation
                "max_time": 120,  # 2 minutes without activity (default): Note 0 for no max, -1 for exit now
                "get_counter" : 0,
                "post_counter": 1
            }
    else:
        open_sessions_[session_id]["post_counter"] += 1
        ephemeral_session = False

    try:
        # Read message body
        content_length = int(handler.headers.get('Content-Length', 0))
        body = handler.rfile.read(content_length)
        message = json.loads(body.decode('utf-8'))
    except:
        errno, value, stack_trace = sys.exc_info()[:3]
        error_msg = f"MCP Error: Failed to pull message: session: '{session_id}' Error: '{value}'"
        status.add_error(error_msg)
        error_reply = {
            "jsonrpc": JSONRPC_VER,
            "id": None,
            "error": {"code": -32700, "message": f"Invalid JSON: {value}"}
        }
        _send_json(status, session_id, handler, error_reply)
        utils_print.output_box(error_msg)
        return False


    req_id = message.get("id")
    method = message.get("method")
    params = message.get("params", {})

    if method is None:
        error_reply = {
            "jsonrpc": JSONRPC_VER,
            "id": req_id,
            "error": {"code": -32600, "message": "Missing id or method"}
        }
        _send_json(status, session_id, handler, error_reply)
        return False


    if trace_level:
        utils_print.output(f"\n\n{session_id} - MESSAGE:\n\n", False)
        utils_print.jput(message, True, 4)

    if req_id is None:
        # THis is a notification - nothing to do - do not return a message
        if method == "notifications/initialized":
            pass

        if handler.al_trace:
            # with trace
            handler.al_trace["include_method"] = method
        if trace_level:
            utils_print.output_box(f"AnyLog Ignores: {method}", "green")

        _send_header(status, handler, 202)
        return True # ← just return internally, no send



    if trace_level:
        utils_print.output(f"\n\n{session_id} - MESSAGE:\n\n", False)
        utils_print.jput(message, True, 4)

    session = open_sessions_.get(session_id)
    if not session:
        error_msg = f"MCP Error: Failed to identify open session for ID: '{session_id}'"
        status.add_error(error_msg)
        utils_print.output_box(error_msg)
        error_reply = {
            "jsonrpc": JSONRPC_VER,
            "id": req_id,
            "error": {
                "code": -32001,
                "message": f"No active SSE session for {session_id}"
            }
        }
        _send_json(status, session_id, handler, error_reply)
        return False


    if not ephemeral_session:
        # ⭐⭐⭐ SEND OK Message to the MCP CLIENT using the HTTP connection ⭐⭐⭐

        # 2) Also send a trivial HTTP 200 OK so httpx is happy
        try:
            handler.send_response(MCP_OK)
            handler.send_header("Content-Type", "application/json")
            handler.send_header("Content-Length", "0")  # Empty body

            # CORS headers
            handler.send_header("Access-Control-Allow-Origin", "*")
            handler.send_header("Vary", "Origin")
            handler.send_header("Access-Control-Expose-Headers", "*")

            handler.end_headers()
            handler.wfile.flush()
        except:
            errno, value = sys.exc_info()[:2]
            error_msg = f"MCP Error: Failed to send HTTP acknowledgment: '{value}'"
            status.add_error(error_msg)
            utils_print.output_box(error_msg)
            return False

    session["activity_time"] = int(time.time())          # This determines if threads exit without activity

    # ⭐⭐⭐ PROCESS The MESSAGE and reply using SSE socket ⭐⭐⭐

    if method == "tools/call":
        # RULE 2 —  For ALL other POST requests:  ✅ Reply is sent via HTTP (not SSE).
        params = message.get("params")
        arguments = params["arguments"] if params and "arguments" in params else None

        mode = arguments.get('mode', 'execute')
        if mode == "post":
            ret_val, reply_obj =  _execute_post(status, handler, params, arguments, req_id, trace_level)     # process a post request
        elif mode == "execute":
            # Execute the command
            ret_val, reply_obj = _execute_cmd(status, handler, params, arguments, req_id, trace_level)
        else:
            # Return AnyLog command to be used in HTTP or cURL (for HTTPS) or CLI
            ret_val, reply_obj = _reply_with_al_cmd(status, handler, params, arguments, req_id, trace_level)

    elif method == "tools/list":
        # RULE 2 —  For ALL other POST requests:  ✅ Reply is sent via HTTP (not SSE).
        ret_val, reply_obj = _list_tools(status, session_id, session, req_id, message)

    elif method == "initialize":
        # Wait for SSE socket to be ready
        if not _wait_for_sse_socket(session_id):
            error_msg = f"MCP Error: SSE socket not ready for session '{session_id}'"
            status.add_error(error_msg)
            return False

        # ⭐ Send initialize reply over SSE (required by MCP spec)
        ret_val, reply_obj = _init_client(status, session_id, session, req_id, message)

    else:
        error_msg = f"MCP Error: Failed to identify the method in message: '{session_id}'"
        status.add_error(error_msg)
        utils_print.output_box(error_msg)
        return False

    if ephemeral_session:
        ret_val = _send_json(status, session_id, handler, reply_obj)
    else:
        ret_val = _send_sse_json(status, session_id, session, reply_obj)


    if trace_level:
        ret_val_txt = process_status.get_status_text(ret_val)
        utils_print.output(f"\n\n{session_id} - REPLY: {ret_val_txt}\n\n", False)
        utils_print.jput(reply_obj, True, 4)

    return  ret_val == process_status.SUCCESS


# ---------------------------------------------------------------------------------
# Execute an AnyLog command
# ---------------------------------------------------------------------------------
def _execute_cmd(status, handler, params, arguments, req_id, trace_level):
    # status is the AnyLog object
    # handler is the REST object

    # STEP 1 - Get the AnyLog command
    command = None
    al_command = None
    if params:
        ret_val = process_status.Client_call_not_supported
        local_name = params.get("name")

        thread_name = utils_threads.get_thread_name()
        reply_key = f"mcp_process_{thread_name}"
        al_params.reset_key(reply_key)      # Reset the key buffer

        if local_name:
            al_info = functionalities_.get(local_name)
            if al_info:
                ret_val, with_wait, sql_query, al_command, command = get_execution_params(arguments, al_info,reply_key)
                if not ret_val:
                    # Create the command dynamically from functionalities_
                    ret_val, al_command = make_al_command(status, al_command, arguments, al_info)
    else:
        # Message must include params
        local_name = None
        reply_key = None
        ret_val = process_status.ERR_wrong_json_structure

    # Step 2 - Execute the AnyLog command
    # Commands can't be returned in chunks, - define a key name to contain the reply
    if not ret_val:

        if sql_query or not with_wait:
            # SQL Query or an AnyLog Native command without wait
            command = f"{reply_key} = {al_command}"
        else:
            # AnyLog command with wait
            command = f"{reply_key}[] = {al_command}"       # Place node replies in a list


        if trace_level:
            utils_print.output(f"\r\n***** {command} *****\r\n", False)

        if not ret_val:
            if sql_query and with_wait:
                # Multiple servers participate - wait for reply
                ret_val = native_api.exec_al_cmd(status, command, None, None, 20)
                if not ret_val:
                    # NOTE: wait for reply is called in exec_al_cmd()
                    ret_val = pull_query_data(status)
            else:
                io_buff = status.get_io_buff()
                ret_val = native_api.exec_no_wait(status, command, io_buff, None, None)
                if not ret_val and with_wait:
                    # native command with wait
                    wait_cmd = f"wait 15 for !{reply_key}.diff == 0"
                    ret_val = member_cmd.process_cmd(status, command=wait_cmd, print_cmd=False, source_ip=None, source_port=None, io_buffer_in=io_buff)

    reply_obj = get_reply_object(status, "exec", arguments, command, ret_val, reply_key, req_id, local_name)

    if handler.al_trace:
        handler.al_trace["cmd"] = "MCP EXECUTE Command"
        handler.al_trace["result"] = ret_val
        handler.al_trace["include_server_func"] = local_name if local_name else "Not recognized"
        handler.al_trace["include_generated"] = al_command if al_command else "Not generated"


    return ret_val, reply_obj

# ---------------------------------------------------------------------------
# Get the reply object for success or failure
# ---------------------------------------------------------------------------
def get_reply_object(status, process_type, arguments, command, ret_val, reply_key, req_id, local_name):

    # SUCCESS
    reply_txt = al_params.get_value_if_available(f"!{reply_key}")

    if ret_val == process_status.SUCCESS:
        reply_obj = {
            "jsonrpc": JSONRPC_VER,
            "id": req_id,
            "result": {
                "content": [
                    {
                        "type": "text",
                        "text": reply_txt
                    }
                ]
            }
        }

    else:
        # ERROR
        json_string = utils_json.to_string(arguments)
        status.add_error(f"Wrong message body in MCP POST request: {json_string}")

        reply_obj = get_err_msg(process_type, command if command else "Not Determined", ret_val, req_id, local_name)
        msg_text = json.dumps(reply_obj)
        status.add_error("MPC Error: Failed to execute command: " + msg_text)


    al_params.reset_key(reply_key)  # Release memory

    return reply_obj

# ---------------------------------------------------------------------------
# Transform the MCP client request + functionality instance to AnyLog command
# ---------------------------------------------------------------------------
def make_al_command(status, al_command, arguments, al_info):

    for key, value in arguments.items():

        if key == "mode":
            continue

        if isinstance(value, list):
            value_str = ",".join(value)
        else:
            value_str = value
        al_command = al_command.replace(f"<{key}>", str(value_str))  # Place the arguments on command

    if "default" in al_info:
        # use the defaults if not provided by the MCP client
        default_list = al_info["default"]
        for default_key, default_value in default_list.items():
            if not default_key in arguments:
                # The value not provided by the MCP client
                al_command = al_command.replace(f"{default_key}", default_value, 1)

    if "remove" in al_info:
        # Remove from the AL command - if optional values are not provided
        remove_list = al_info["remove"]
        for remove_key in remove_list:
            al_command = al_command.replace(remove_key, "")

    return process_status.SUCCESS if al_command else process_status.Client_call_not_supported, al_command

# ---------------------------------------------------------------------------
# Pull the params for the request and return the needed params to execute the query
# ---------------------------------------------------------------------------
def get_execution_params(arguments, al_info, reply_key):

    ret_val = process_status.Client_call_not_supported
    al_command = None       # The AnyLog command text
    command = None          # The command to issue
    with_wait = False
    sql_query = False

    if al_info:
        if "native" in al_info:
            al_command = al_info["native"]
            ret_val = process_status.SUCCESS
        elif "query" in al_info:
            with_wait = True  # Wait for servers to replay
            sql_query = True
            al_command = al_info["query"]
            ret_val = process_status.SUCCESS
        elif "al_basic_commands" in al_info:
            # Change the command by the keys requested
            with_wait = True  # No wait for execution
            cmd_options = al_info["al_basic_commands"]
            if "status_type" in arguments:
                cmd_key = arguments["status_type"]
                if cmd_key in cmd_options:
                    al_command = cmd_options[cmd_key]
                    ret_val = process_status.SUCCESS
        elif arguments and "network" in al_info and "data_nodes" in arguments:
            # Use: run client (<data_nodes>) as prefix
            with_wait = True  # wait for execution
            al_command = al_info["network"]
            ret_val = process_status.SUCCESS
        elif "local" in al_info:
            al_command = al_info["local"]
            ret_val = process_status.SUCCESS
    elif arguments:
        if "dbms" in arguments and "sql" in arguments:
            # This is the case of a SQL Query without Session ID
            with_wait = True  # Wait for servers to replay
            sql_query = True
            dbms = arguments["dbms"]
            sql = arguments["sql"]
            al_command = f"run client () sql {dbms} format = json:list and stat = false {sql}"
            command = f"{reply_key} = {al_command}"
            ret_val = process_status.SUCCESS

    return ret_val, with_wait, sql_query, al_command, command

# ---------------------------------------------------------------------------
# Return AL command
# the mode values (cli, http, curl - https, execute) are parameters thet the MCP server interprets
# Claude just passes them through faithfully based on the tool defined by the user.
# Mode CLI - native AnyLog CLI command string
# Mode HTTP - Returns an embedded URL where the AnyLog command headers are baked into the URL itself (no need in Proxy)
# Mode cURL - a full REST call specification — the complete cURL headers, and body needed to make the call via a proxy-based HTTPS setup.
# Use case: Secure/proxy environments where you need the full call spec to construct the request yourself, rather than hitting the node directly.
# ---------------------------------------------------------------------------
def _reply_with_al_cmd(status, handler, params, arguments, req_id, trace_level):
    # status is the AnyLog object
    # handler is the REST object

    global functionalities_

    ret_val = process_status.Client_call_not_supported
    al_command = None
    local_name = None

    mode = arguments.get("mode")    # HTTP or HTTPS or CLI
    if params and "name" in params and mode:
        local_name = params["name"]
        if local_name in functionalities_:
            al_info = functionalities_.get(local_name)
            if al_info:
                ret_val, with_wait, sql_query, al_command, command = get_execution_params(arguments, al_info,None)
                if not ret_val:
                    # Create the command dynamically from functionalities_
                    ret_val, al_command = make_al_command(status, al_command, arguments, al_info)
                    if handler.al_trace:
                        handler.al_trace["include_server_func"] = local_name
                        handler.al_trace["include_generated"] = al_command


    if not ret_val:
        reply_obj = None
        if mode == "http":
            if al_command.startswith("run client ("):
                # Change - format "run client" will be added as destination : network
                al_command, destination = get_destination_nodes(al_command)
                reply_txt = f"http://<IP>:<PORT>/?AnyLog-Agent=AnyLog/1.23?destination={destination}?command={al_command}"
            else:
                reply_txt = f"http://<IP>:<PORT>/?AnyLog-Agent=AnyLog/1.23?command={al_command}"
            if "target_node" in arguments:
                reply_txt = reply_txt.replace("<IP>:<PORT>", arguments["target_node"])

        elif mode == "curl":
            if al_command.startswith("run client ("):
                # Change - format "run client" will be added as destination : network
                al_command, destination = get_destination_nodes(al_command)
                reply_txt = "curl --request GET '<IP>:<PORT>'" \
                            " --header 'AnyLog-Agent: AnyLog/1.23'" \
                            f" --header 'destination: {destination}'" \
                            f" --header 'command: {al_command}'"
            else:
                reply_txt = "curl --request GET '<IP>:<PORT>'" \
                            " --header 'AnyLog-Agent: AnyLog/1.23'" \
                            f" --header 'command: {al_command}'"
            if "target_node" in arguments:
                reply_txt = reply_txt.replace("<IP>:<PORT>", arguments["target_node"])
        elif mode == "post":
            reply_obj = {
                "AnyLog-Agent" : "AnyLog/1.23"
            }
            if al_command.startswith("run client ("):
                # Change - format "run client" will be added as destination : network
                al_command, destination = get_destination_nodes(al_command)
                reply_obj["command"] = al_command
                reply_obj["destination"] = destination
            else:
                reply_obj["command"] = al_command

        elif mode == "cli":
            reply_txt = al_command
        else:
            status.add_error(f"MPC Error: Invalid mode: received mode '{mode}'")
            ret_val = process_status.Client_call_not_supported



        reply_obj = {
            "jsonrpc": JSONRPC_VER,
            "id": req_id,
            "result": {
                "content": [
                    {
                        "type": "text",
                        "text": reply_obj
                    }
                ]
            }
        }
    else:

        # ERROR
        if ret_val in local_err_to_json_rpc_:
            # Translate to JSON-RPC code
            rpc_err_code = local_err_to_json_rpc_[ret_val]
        else:
            rpc_err_code = -32603  # ("Internal error") is a generic JSON-RPC error code indicating something went wrong on the server side during execution.

        reply_obj = {
            "jsonrpc": JSONRPC_VER,
            "id": req_id,
            "error": {
                "code": rpc_err_code,
                "data": {
                    "anylog_error": "Failed Reply",
                    "anylog_error_code": ret_val,
                    "details": "Failed to generate the call to an AnyLog Node"
                }
            }
        }
        msg_text = json.dumps(reply_obj)
        status.add_error("MPC Error: Failed to generate an AnyLog command: " + msg_text)


    if handler.al_trace:
        handler.al_trace["cmd"] = "MCP Generate Command"
        handler.al_trace["result"] = ret_val
        handler.al_trace["include_server_func"] = local_name if local_name else "Not recognized"
        handler.al_trace["include_generated"] = al_command if al_command else "Not generated"

    return ret_val, reply_obj

# ---------------------------------------------------------------------------
# Get the destination nodes inside the parenthesis, otherwise return "network"
# Search for the nodes inside the "run client ( ... )"
# ---------------------------------------------------------------------------
def get_destination_nodes(al_command):

    index = al_command.find(")", 12)
    if index > 12:
        # paren with info
        destination = al_command[12 : index ]       # List of nodes
    else:
        destination = "network"

    updated_command = al_command[index + 2:]

    return updated_command, destination
# ---------------------------------------------------------------------------
# Direct the client to use aggregations for this query
# Example reply:
'''
{
  "status": "needs_refinement",
  "reason": "Columns are aggregated in separate per-metric tables (min/max/count per interval).",
  "interval": "1m",
  "columns": [
    { "column": "temperature", "table": "agg_1m_temperature", "fields": ["ts_bucket", "min", "max", "count"] },
    { "column": "pressure",    "table": "agg_1m_pressure",    "fields": ["ts_bucket", "min", "max", "count"] },
    { "column": "vibration",   "table": "agg_1m_vibration",   "fields": ["ts_bucket", "min", "max", "count"] }
  ],
  "instruction": "Please query each column separately from its designated aggregation table."
}
'''
# ---------------------------------------------------------------------------
def refine_to_aggregations(status, req_id, command, trace_level):
    # Test if needs to use aggregation dbms and table
    reply_obj = None

    index = command.find(' sql ',12)    # 12 is the location to search " sql " in "run client() sql dbms_name select ... from ...."
    if index != -1:
        offset_start = index + 5
        offset_end = command.find(' ', offset_start)
        if offset_end != -1:
            dbms_name = command[offset_start:offset_end]
            index = command.find(' FROM ', offset_end + 8)      # Machine will give all capital letters or smaller letters
            if index == -1:
                index = command.lower().find(' from ',offset_end + 8)  # Machine will give all capital letters or smaller letters
            if index != -1:
                offset_start = index + 6    # char after "from
                offset_end = command.find(' ', offset_start)
                if offset_end != -1:
                    table_name = command[offset_start:offset_end]

                    metadata = version.get_agg_metadata(status, dbms_name, table_name)
                    if metadata:
                        reply_obj = {
                            "jsonrpc": JSONRPC_VER,
                            "id": req_id,
                            "result": {
                                "content": [
                                    {
                                        "type": "json",
                                        "text": metadata
                                    }
                                ]
                            }
                        }

    return reply_obj

# _________________________________________________________________________
'''
JSON-RPC 2.0 defines several standard error codes:
-32700 - Parse error
-32600 - Invalid Request
-32601 - Method not found
-32602 - Invalid params
-32603 - Internal error
'''
# _________________________________________________________________________
def get_err_msg( process_type, command, local_err_code, req_id, local_name ):
    '''
    local_err_code - the AnyLog err value
    '''

    if local_err_code in local_err_to_json_rpc_:
        # Translate to JSON-RPC code
        rpc_err_code = local_err_to_json_rpc_[local_err_code]
    else:
        rpc_err_code = -32603 # ("Internal error") is a generic JSON-RPC error code indicating something went wrong on the server side during execution.

    if local_name:
        rpc_msg = f"Internal error: Unable to process '{local_name}'"
    else:
        rpc_msg = f"Internal error: Failed to identify the function to process"

    local_details = process_status.get_status_text(local_err_code)

    err_msg = {
        "jsonrpc": JSONRPC_VER,
        "id": req_id,
         "error": {
            "code": rpc_err_code,
            "message": rpc_msg,
            "data": {
              "anylog_command": command,
              "anylog_error": "Failed Reply",
              "anylog_error_code": local_err_code,
              "details": local_details
            }
          }
        }

    return err_msg
# ---------------------------------------------------------------------------------
# Get the session ID from the path
# ---------------------------------------------------------------------------------
def _init_client(status, session_id, session, req_id, message):

    client_protocol = message.get("params", {}).get("protocolVersion", "2025-06-18")

    # ✅ DON'T build tools list here - just declare capability

    reply = {
        "id": req_id,
        "jsonrpc": JSONRPC_VER,
        "result": {
            "protocolVersion": client_protocol,
            "capabilities": {
                "tools": {
                    "listChanged": False  # ✅ Set to False unless tools can change dynamically
                },
                "experimental": {},
                "completions": {}
            },
            "serverInfo": {
                "name": SERVER_NAME,
                "version": SERVER_VERSION
            }
        }
    }

    return process_status.SUCCESS, reply

# ---------------------------------------------------------------------------------
# Get the session ID from the path
# ---------------------------------------------------------------------------------
def _get_session_id( url ):
    path_parts = url.split('/') if url else []
    if len(path_parts) < 4:
        return None

    return path_parts[3]
# ---------------------------------------------------------------------------------
# Address a race condition: the initialize POST arrives before the SSE GET has fully established the socket.
# ---------------------------------------------------------------------------------
def _wait_for_sse_socket(session_id, timeout=5.0):
    """Wait for SSE socket to be ready."""
    start = time.time()
    while time.time() - start < timeout:
        session = open_sessions_.get(session_id)
        if session and session.get("sse_socket"):
            return True
        time.sleep(0.05)
    return False

# ---------------------------------------------------------------------------------
# Return a reply with all supported tools
# ---------------------------------------------------------------------------------
def _list_tools(status, session_id, session, req_id, message):
    tools = _get_tools_list()
    reply = {
        "jsonrpc": "2.0",
        "id": req_id,
        "result": {"tools": tools}
    }

    return process_status.SUCCESS, reply

# ---------------------------------------------------------------------------------
# Go over functionalities and list the tools
# ---------------------------------------------------------------------------------
def _get_tools_list():

    global functionalities_
    global info_to_client_

    tools = []
    for al_func, info in functionalities_.items():
        entry = {
            "name": al_func,
            "description": info["description"],
            "inputSchema": info["inputSchema"]
        }
        for key in info_to_client_:
            if key in info:
                entry[key] = info[key]
        tools.append(entry)
    return tools

def _send_json(status, session_id, handler, obj):
    try:
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")

        handler.send_response(200)
        handler.send_header("Content-Type", "application/json; charset=utf-8")
        handler.send_header("Content-Length", str(len(data)))
        handler.send_header("Cache-Control", "no-store")
        handler.send_header("Connection", "close")

        # CORS headers
        handler.send_header("Access-Control-Allow-Origin", "*")
        handler.send_header("Vary", "Origin")
        handler.send_header("Access-Control-Expose-Headers", "*")

        # IMPORTANT: disable chunked encoding if handler supports it
        if hasattr(handler, "use_chunked"):
            handler.use_chunked = False
        if hasattr(handler, "chunked"):
            handler.chunked = False

        handler.end_headers()

        try:
            handler.wfile.write(data)
            handler.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass

        handler.close_connection = True
        return process_status.SUCCESS

    except Exception as e:
        error_msg = f"MCP Error: Failed to send JSON reply: '{e}'"
        status.add_error(error_msg)
        utils_print.output_box(error_msg)
    return process_status.ERR_process_failure

def _send_header(status, handler, response_code):
    try:
        handler.send_response(response_code)   # or 200

        # Standard headers
        handler.send_header("Cache-Control", "no-store")
        handler.send_header("Content-Length", "0")

        # CORS headers
        handler.send_header("Access-Control-Allow-Origin", "*")
        handler.send_header("Vary", "Origin")
        handler.send_header("Access-Control-Expose-Headers", "*")


        handler.end_headers()
        return process_status.SUCCESS

    except Exception as e:
        error_msg = f"MCP Error: Failed to send HTTP reply: '{e}'"
        status.add_error(error_msg)
        utils_print.output_box(error_msg)
    return process_status.ERR_process_failure

def _send_sse_json(status, session_id, session, obj):
    """Send a JSON-RPC message via SSE to the open session."""

    """Send a JSON-RPC message via SSE to the open session."""

    sse_socket = session.get("sse_socket")
    if not sse_socket:
        error_msg = f"MCP Error: No SSE socket for session '{session_id}'"
        status.add_error(error_msg)
        ret_val = process_status.ERR_process_failure
    else:

        try:
            # Check if socket is still open
            if hasattr(sse_socket, 'closed') and sse_socket.closed:
                error_msg = f"MCP Error: SSE socket for session is closed'{session_id}'"
                ret_val = process_status.ERR_process_failure
            else:
                with session["lock"]:  # ✅ Use the lock!
                    data = json.dumps(obj)
                    sse_socket.write(b"event: message\n")
                    sse_socket.write(f"data: {data}\n\n".encode("utf-8"))
                    sse_socket.flush()
                ret_val = process_status.SUCCESS
        except:
            errno, value = sys.exc_info()[:2]
            error_msg = f"MCP Error: Failed to send reply using SSE: '{value}'"
            ret_val = process_status.ERR_process_failure

    if ret_val:
        # With error
        status.add_error(error_msg)
        utils_print.output_box(error_msg)

    return ret_val

# --------------------------------------------------------------------------------------------------
# This process will pull the query results from the local databases
# --------------------------------------------------------------------------------------------------
def pull_query_data(status):

    ret_val = process_status.Failed_query_process
    job_id = status.get_job_id()
    j_handle = status.get_active_job_handle()  # Need to be done after the execution of the commands
    if (job_id != process_status.JOB_INSTANCE_NOT_USED):
        # job instance was used
        j_instance = job_scheduler.get_job(job_id)
        # Mutex such that the job instance is not reused by a different thread in the query process
        j_instance.data_mutex_aquire(status, 'W')
        if j_instance.is_job_active() and j_instance.get_unique_job_id() == status.get_unique_job_id():
            if not j_instance.is_pass_through():
                # Otherwise the data was written to the caller
                nodes_count = j_instance.get_nodes_participating()
                nodes_replied = j_instance.get_nodes_replied()

                logical_dbms, table_name, conditions, sql_command = j_handle.get_local_query_params()

                if not logical_dbms:
                    # This is the case of subset and not all nodes replied
                    logical_dbms = "system_query"
                    sql_command = j_handle.select_parsed.local_query
                    table_name = j_handle.select_parsed.get_local_table()

                try:
                    ret_val = member_cmd.query_local_dbms(status, None, logical_dbms, table_name, conditions,
                                                          sql_command, nodes_count, nodes_replied)
                except:
                    pass
            else:
                ret_val = process_status.SUCCESS        # Pass through - the data was assigned to the dictionary key dynamically

            if j_handle.is_subset():
                # Reply with partial nodes

                # provide summary of reply
                error_list = j_instance.get_nodes_error_list()
                if error_list:
                    # not all nodes replied
                    # Include: IP + Port + Partition + RetCode + Text
                    reply = utils_print.output_nested_lists(error_list, "\r\nReply errors:",
                                                            ["Node IP", "Port", "Par", "Ret-Code", "Error Message"],
                                                            True)
                elif not j_handle.is_query_completed():
                    reply = "Query not completed"

        j_instance.data_mutex_release(status, 'W')

        j_instance.set_not_active()

    return ret_val


# --------------------------------------------------------------------------------------------------
# Execute a post command using the MCP call - note: transfer_encoding not supported by the MCP client
# --------------------------------------------------------------------------------------------------

def _execute_post(status, handler, params, arguments, req_id, trace_level):

    local_name = params.get("name")
    command = None
    al_command = None
    thread_name = utils_threads.get_thread_name()
    reply_key = f"mcp_post_{thread_name}"

    if arguments:
        # this is a POST request --> process as get
        ret_val, al_command = generate_data_query(status, arguments)
        if not ret_val:
            command = f"{reply_key} = run client () {al_command}"

            # Multiple servers participate - wait for reply
            ret_val = native_api.exec_al_cmd(status, command, None, None, 20)
            if not ret_val:
                # NOTE: wait for reply is called in exec_al_cmd()
                ret_val = pull_query_data(status)
        else:
            # An AnyLog command which is not a query
            if local_name:
                al_info = functionalities_.get(local_name)
                if al_info:
                    ret_val, with_wait, sql_query, al_command, command = get_execution_params(arguments, al_info,
                                                                                              reply_key)
                    if not ret_val:
                        # Create the command dynamically from functionalities_
                        ret_val, al_command = make_al_command(status, al_command, arguments, al_info)
                        if not ret_val:
                            if sql_query or not with_wait:
                                # SQL Query or an AnyLog Native command without wait
                                command = f"{reply_key} = {al_command}"
                            else:
                                # AnyLog command with wait
                                command = f"{reply_key}[] = {al_command}"  # Place node replies in a list

                            io_buff = status.get_io_buff()
                            ret_val = native_api.exec_no_wait(status, command, io_buff, None, None)
                            if not ret_val and with_wait:
                                # native command with wait
                                wait_cmd = f"wait 15 for !{reply_key}.diff == 0"
                                ret_val = member_cmd.process_cmd(status, command=wait_cmd, print_cmd=False,
                                                                 source_ip=None, source_port=None, io_buffer_in=io_buff)

    else:
        ret_val = process_status.ERR_wrong_json_structure


    reply_obj = get_reply_object(status, "post", arguments, command if command else al_command, ret_val, reply_key, req_id, local_name)

    if handler.al_trace:
        handler.al_trace["cmd"] = "MCP POST Command"
        handler.al_trace["result"] = ret_val
        handler.al_trace["include_generated"] = al_command if al_command else "Not generated"

    return ret_val, reply_obj
# =======================================================================================================================
# A) Generate Increments query from JSON - The input json message needs to includes:
# DBMS -- TABLE -- timeColumn -- startTime -- endTime -- intervalLength -- timeUnit -- projections
# B) or generate SQL query
# =======================================================================================================================
def generate_data_query(status, json_msg):

    ret_val = process_status.ERR_wrong_json_structure
    command = None

    dbms = json_msg.get("dbms")

    if dbms:
        if "timeColumn" in json_msg and "table" in json_msg:
            # Make an increment query


            table = json_msg["table"]
            time_column = json_msg["timeColumn"]

            interval_length = json_msg.get("intervalLength")
            time_unit = json_msg.get("timeUnit")
            projections_list = json_msg.get("projections")

            if not interval_length:
                json_string = utils_json.to_string(json_msg)
                status.add_error(f"Missing intervalLegth in the JSON with increment query info: {json_string}")
            elif not time_unit:
                json_string = utils_json.to_string(json_msg)
                status.add_error(f"Missing timeUnit in the JSON with increment query info: {json_string}")
            elif not projections_list:
                json_string = utils_json.to_string(json_msg)
                status.add_error(f"Missing projections in the JSON with increment query info: {json_string}")
            else:

                where_cond = json_msg.get("whereCond")
                if where_cond:
                    if isinstance(where_cond, list):
                        where_cond = "where " + " and ".join(where_cond)
                    elif isinstance(projections_list, str):
                        where_cond = "where " + where_cond
                    else:
                        where_cond = ""  # Error will be presented when query is executed

                start_time = json_msg.get("startTime")
                end_time = json_msg.get("endTime")

                if start_time:
                    if where_cond:
                        where_cond += f' and {time_column} >= {start_time}'
                    else:
                        where_cond = f' where {time_column} >= {start_time}'

                if end_time:
                    if where_cond:
                        where_cond += f' and {time_column} <= {end_time}'
                    else:
                        where_cond += f' where {time_column} <= {end_time}'

                if isinstance(projections_list, list):
                    projections = ",".join(projections_list)
                elif isinstance(projections_list, str):
                    projections = projections_list
                else:
                    projections = ""  # Error will be presented when query is executed

                command = f"sql {dbms} format = json and stat = false select increments({time_unit}, {interval_length}, {time_column}), {projections} from {table} {where_cond}"
                ret_val = process_status.SUCCESS
        else:

            sql = json_msg.get("sql")
            if sql:
                command = f"sql {dbms} {sql}"
                ret_val = process_status.SUCCESS

    return ret_val, command
# --------------------------------------------------------------------------------------------------
# This process will integrate mcp functionalities defined in member_cmd.commands
# This is done once
# --------------------------------------------------------------------------------------------------
def update_functionalities(commands):

    for key in commands.keys():
        if "methods" in commands[key]:
            update_functionalities(commands[key]["methods"])
        else:
            if "mcp" in commands[key]:
                mcp_info = commands[key]["mcp"]

                name = mcp_info["name"]     # The name to use in the MCP functionalities
                is_network = mcp_info["network"] if "network" in mcp_info else False    # If can be sent to nodes
                cmd = mcp_info["cmd"]
                description = commands[key]["help"]["text"]


                functionality =  {
                        "description" : description,
                        "inputSchema": {        # something the server sends to the client to describe what input a tool expects
                            "type": "object",   # The input MUST be an object
                            "properties": {},
                            "required": []
                        }
                    }

                if is_network:
                    nodes = {
                        "type": "string",
                        "description": (
                            "Optional: The AnyLog network node(s) to execute this command on. "
                            "If omitted, the command executes only on the node acting as the MCP/HTTP server — "
                            "the node that received this request from the client. "
                            "If specified, the command is sent to all listed nodes and results are returned to the client. "
                            "Note: The IP and Port are the AnyLog internal network addresses, "
                            "not the REST/HTTP interface addresses. "
                            "Use getNodesList or getClusterNodeMapping to discover the correct internal IP:Port for each node. "
                            "Single node: '172.105.60.50:32148'  "
                            "Multiple nodes: '172.105.60.50:32148,172.105.6.90:32148'"
                        )
                    }

                    functionality["inputSchema"]["properties"]["nodes"] = nodes

                    functionality["network"] = f"run client (<nodes>) {cmd}"

                functionality["local"] = cmd

                functionalities_[name] = functionality      # Update the functionalities_ dictionary



# Mode parameter definition — reused across all tools
MODE_PROPERTY = {
    "mode": {
        "type": "string",
        "enum": ["execute", "http", "post", "curl", "cli"],
        "description": (
            "Determines how the command is processed. "
            "IMPORTANT: Use 'curl' when the user asks for a REST call, curl command, HTTPS request, or proxy-based call — "
            "returns a cURL command for use against a proxy, and can be used to generate dashboard fetch() calls. "
            "Use 'http' when the user asks for a direct HTTP call without a proxy (headers embedded in URL, callable from a browser). "
            "Use 'post' when the user asks to issue AnyLog commands using POST directly from the browser — "
            "returns a JSON dict that must be passed as the message body of a POST request to the AnyLog node; "
            "the dashboard should call fetch(url, { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(<returned dict>) }). "
            "Use 'cli' when the user asks for the AnyLog command, CLI string, or native command. "
            "Use 'execute' (default) when the agent needs to run the command and process the returned results."
        ),
        "default": "execute"
    },
    "target_node": {
        "type": "string",
        "description": (
                    "Optional: The IP:Port of the AnyLog node that receives the REST request. "
                    "Used when mode is 'http', 'post', or 'curl' to embed the destination address "
                    "into the returned command string or URL. "
                    "This is the REST/HTTP interface address of the node — not the internal AnyLog network address. "
                    "If omitted, the returned command will contain '<IP>:<PORT>' as a placeholder "
                    "that the caller must replace manually. "
                    "Example: '24.5.219.50:7849' — the node that will receive and process the REST call. "
                    "Ignored for 'execute' and 'cli' modes."
                )
    }
}

functionalities_ = {

    "checkStatus": {
        "description": "Checks the status of the node servicing the MCP client, representing the MCP server status",
        "inputSchema": {
            "type": "object",
            "properties": {
                **MODE_PROPERTY
            },
            "required": ["mode"]
        },
        "native": "get status where format = mcp"
    },

    "getNodesList": {
        "description": "Returns the list of nodes in the network, for each node, the type, the name the IP and port",
        "inputSchema": {
            "type": "object",
            "properties": {
                **MODE_PROPERTY
            },
            "required": ["mode"]
        },
        "native": "blockchain get (master, query, operator) bring.json [*] [*][name] [*][ip] [*][port] "
    },

    "dataLocation": {
        "description": "returns, for each database and table, which are the nodes that are hosting the data",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dbms":  {"type": "string"},
                "table": {"type": "string"},
                **MODE_PROPERTY
            },
            "required": ["mode"]
        },
        "native": "get data nodes where dbms = <dbms> and table = <table> and format = json",
        "default": {
            "<dbms>":  "*",
            "<table>": "*",
        },
    },

    "listNetworkDatabases": {
        "description": "Returns a list of all the databases in the network",
        "inputSchema": {
            "type": "object",
            "properties": {
                **MODE_PROPERTY
            },
            "required": ["mode"]
        },
        "native": "blockchain get (table) bring.json.unique [*][dbms]"
    },

    "listTables": {
        "description": "Returns a list of all the tables for a given database",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dbms": {"type": "string"},
                **MODE_PROPERTY
            },
            "required": ["dbms", "mode"]
        },
        "native": "blockchain get (table) where dbms = <dbms> bring.json.unique [*][name]"
    },

    "listColumns": {
        "description": "Returns column definitions for a given table",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dbms":  {"type": "string"},
                "table": {"type": "string"},
                **MODE_PROPERTY
            },
            "required": ["dbms", "table", "mode"]
        },
        "native": "get columns where dbms = <dbms> and table = <table> and format = mcp and sys_col = false"
    },

    "monitorNodes": {
        "description": (
            "Returns information for one or more nodes, status is requested to one of the following: "
            "node status, node resources, node version. node cpu usage. "
            "By default queries all nodes in the network, but you can optionally target specific node(s) by providing IP:Port addresses."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "nodes": {
                    "type": "string",
                    "description": "Optional: Target specific node(s) for status query. Can be a single node (e.g., '172.105.60.50:32148') or comma-separated list of nodes (e.g., '172.105.60.50:32148,172.105.6.90:32148'). If not provided, returns status for all nodes in the network."
                },
                "status_type": {
                    "type": "string",
                    "description": "Optional: Type of information to retrieve. One of the following options are allowed: 'status', 'resources', 'version', 'cpu'.",
                    "enum": ["status", "resources", "version", "cpu"]
                },
                **MODE_PROPERTY
            },
            "required": ["mode"],
        },
        "al_basic_commands": {
            "status":    "run client (<nodes>) get status where format = mcp",
            "resources": "run client (<nodes>) get node resources where format = json",
            "version":   "run client (<nodes>) get version where format = mcp",
            "cpu":       "run client (<nodes>) get cpu usage where format = json",
        },
        "default": {
            "nodes": "blockchain get (master, query, operator) bring.ip_port",
        }
    },

    "listPolicyTypes": {
        "description": (
            "Returns all unique policy types stored in the blockchain, "
            "allowing discovery of what policy structures are available. "
            "Common policy types include: master, query, operator, publisher, tag, and namespace. "
            "Use the returned values as the policyType input to listPolicies."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                **MODE_PROPERTY
            },
            "required": ["mode"]
        },
        "native": "blockchain get * bring.unique.json [*]"
    },

    "listPolicies": {
        "description": (
            "Returns policies stored in the blockchain for a specific policy type. "
            "This includes infrastructure policies (master, query, operator, publisher), "
            "tag and namespace policies, and other metadata relationships. "
            "Use listPolicyTypes to discover available policy types."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "policyType": {
                    "type": "string",
                    "description": "The policy type to retrieve (e.g., 'table', 'operator', 'master', 'query', 'publisher', 'tag', 'mapping', 'uns', etc.). "
                                   "Use listPolicyTypes to discover available types."
                },
                "whereCond": {
                    "type": "string",
                    "description": (
                        "Optional WHERE clause to filter policies. Multiple filter criteria are separated by the keywords 'and' or 'or'. "
                        "Format: 'attribute = value' or 'attribute1 = value1 AND attribute2 = value2'.  "
                        "For values containing spaces, wrap in double quotes: attribute = \"value with spaces\". "
                        "Example: 'name = smart-city-operator1' and 'dbms = lsl_demo'"
                    )
                },
                **MODE_PROPERTY
            },
            "required": ["policyType", "mode"]
        },
        "native": "blockchain get <policyType> where <whereCond>",
        "remove": [" where <whereCond>"]
    },

    "getRootPolicies": {
        "description": (
                "STEP 1 of the metadata-first workflow: call this before executeQuery whenever "
                "the user asks about assets, devices, sensors, or to determine data tables. "
                "Returns top-level UNS/policy roots — the entry points into the asset hierarchy. "
                "Each result is a JSON object whose root key is the policy type "
                "(e.g., {'uns': {...}}). UNS roots have policy type 'uns'. "
                "Use the returned 'id' with getPolicyChildren to traverse downward. "
                "Leaf nodes (representing assets and objects and their relations) contain 'dbms', 'table' "
                "that map directly to executeQuery parameters — no schema guessing needed. "
                "Workflow: getRootPolicies → getPolicyChildren (repeat until needed asset is identified) "
                "→ extract dbms/table/where → executeQuery."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                **MODE_PROPERTY
            },
            "required": ["mode"]
        },
        "returns": {
            "type": "array",
            "description": (
                "List of root policies. Each policy is a JSON object where the root key is the policy type. "
                "Each policy includes an 'id' attribute that uniquely identifies it. "
                "Pass the policy type and id to getPolicyChildren to retrieve its children."
            )
        },
        "native": "blockchain get root policies",
    },

    "getPolicyChildren": {
        "description": (
            "STEP 2 of the metadata-first workflow. "
            "Returns all immediate child policies under a given parent from the blockchain. "
            "Use this to traverse the UNS/policy hierarchy: start from a root returned by getRootPolicies, "
            "retrieve its children, identify the child of interest by name or description, "
            "then use that child's 'id' as whereCond to retrieve its children. "
            "Each child policy is a JSON object whose root key is the policy type. "
            "Nodes in the hierarchy represent assets, sites, equipment groups, and devices. "
            "Each node may contain 'dbms' and 'table' attributes — extract these to use in executeQuery. "
            "Some device-level nodes also carry a 'where' attribute (a SQL filter condition for that specific device) "
            "— include it in the SQL WHERE clause when present. "
            "The path from root to a node represents the asset's place in the UNS hierarchy (its context and relationships). "
            "Stop traversing when the retrieved node represents the target asset, "
            "or when no children are returned."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "policyType": {
                    "type": "string",
                    "description": (
                        "The policy type of the parent (e.g., 'table', 'operator', 'master', 'query', "
                        "'publisher', 'tag', 'mapping', etc.). Use listPolicyTypes to discover available types. "
                        "If omitted, children of all policy types are returned."
                    )
                },
                "whereCond": {
                    "type": "string",
                    "description": (
                        "WHERE clause that identifies which parent policy to query (or multiple parents). "
                        "Returns all immediate children of the matching parent. "
                        "Format: 'key = value' or 'key1 = value1 and key2 = value2' or 'key1 = value1 or key2 = value2'. "
                        "For values containing spaces, wrap in double quotes: key = \"value with spaces\". "
                        "Common key names: id, name, namespace."
                    ),
                    "examples": [
                        "id = main_enterprise",
                        "name = \"enterprise c\"",
                        "name = \"enterprise c\" and country = USA"
                    ]
                },
                **MODE_PROPERTY
            },
            "required": ["whereCond", "mode"]
        },
        "returns": {
            "type": "array",
            "description": (
                "List of child policies. Each child is a JSON object where the root key is the policy type "
                "(e.g., {'namespace': {...}}, {'device': {...}}, {'sensor': {...}}). "
                "Each child includes a 'parent' attribute containing the id of its parent policy. "
                "The 'id' attribute in each parent and child is the unique identifier for that policy. "
                "Use the child's policyType (root key) and id in the next query to find its children."
            )
        },
        "native": "blockchain get <policyType> where <whereCond> bring.json.children",
        "default": {
            "<policyType>": "*",
        },
    },

    "listCommandsIndex": {
        "description": "Returns a list of keys, each key serves as an index to a group of anylog commands",
        "inputSchema": {
            "type": "object",
            "properties": {
                **MODE_PROPERTY
            },
            "required": ["mode"]
        },
        "native": "help index where format = mcp"
    },

    "listCommandsByIndex": {
        "description": "Returns a list of all commands that are associated with a given key",
        "inputSchema": {
            "type": "object",
            "properties": {
                "key": {
                    "type": "string",
                    "description": (
                        "The key associated with the commands (e.g., 'background processes', 'blockchain', 'aggregations', "
                        "'bucket', 'high availability', 'metadata', 'unstructured data', 'video', etc.). "
                        "Use listCommandsByIndex to discover commands by keys (topics)"
                    )
                },
                **MODE_PROPERTY
            },
            "required": ["key", "mode"]
        },
        "native": "help index <key> where format = mcp",
    },

    "helpCommand": {
        "description": "Returns information about a specific command. The information includes: usage, explanation, examples, optionally - a link to the documentation.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "The command that is processed through the help mechanism"
                },
                **MODE_PROPERTY
            },
            "required": ["command", "mode"]
        },
        "native": "help <command> where format = mcp",
    },

    "executeQuery": {
        "description": (
                "Executes a SQL query on the AnyLog network. "
                "If a UNS is available for the target table, use getRootPolicies + getPolicyChildren first "
                "to retrieve 'dbms' and 'table' from the relevant UNS node before calling this tool. "
                "Traverse the hierarchy, identify the node of interest, and extract 'dbms' and 'table'. "
                "Some device-level nodes also carry a 'where' attribute (a SQL filter for that specific device) "
                "— include it as an additional WHERE condition in the SQL when present. "
                "Do not guess table names or column names — retrieve them from the UNS when available. "
                "The MCP server routes the request through its configured AnyLog node (the gateway). "
                "By default the query is distributed across ALL operator nodes in the network. "
                "Use 'data_nodes' to restrict the query to specific operator nodes that host the data. "
                "NOTE: For time-based WHERE clauses, use 'NOW() - N hours/days/minutes' format "
                "(e.g., 'WHERE timestamp >= NOW() - 24 hours'). "
                "PostgreSQL INTERVAL syntax is not supported. Nested queries and JOINs are not supported."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "dbms": {
                    "type": "string",
                    "description": "The database name"
                },
                "sql": {
                    "type": "string",
                    "description": (
                        "The SQL query to execute. For time filters, use format: "
                        "'WHERE timestamp >= NOW() - N hours/days/minutes' "
                        "(e.g., 'NOW() - 24 hours', 'NOW() - 7 days'). "
                        "Do not use INTERVAL syntax. "
                        "Nested queries and JOINs are not supported — query single tables only."
                    ),
                    "examples": [
                        "SELECT * FROM table WHERE timestamp >= NOW() - 1 day",
                        "SELECT AVG(value) FROM table WHERE timestamp >= NOW() - 24 hours",
                        "SELECT COUNT(*) FROM table WHERE timestamp >= NOW() - 30 minutes and timestamp < NOW() - 10 minutes"
                    ]
                },
                "data_nodes": {
                    "type": "string",
                    "description": (
                        "Optional: The operator node(s) (IP:Port) that HOST the data to query. "
                        "Omit this parameter by default — queries are automatically distributed to ALL nodes in the network hosting the table. "
                        "Only provide data_nodes when explicitly restricting the query to specific nodes (e.g., comparing nodes, debugging a single node, or targeting a known subset). "
                        "Single node: '172.105.60.50:32148'  "
                        "Multiple nodes: '172.105.60.50:32148,172.105.6.90:32148'"
                    )
                },
                **MODE_PROPERTY
            },
            "required": ["dbms", "sql", "mode"]
        },
        "query": "run client (<data_nodes>) sql <dbms> format = json:list and stat = false <sql>",
        "default": {
            "<data_nodes>": ""
        }
    },

    "queryWithIncrement": {
            "description": (
            "PREFERRED over executeQuery whenever the query involves aggregation over time intervals "
            "(e.g., hourly averages, daily min/max, per-minute counts). "
            "Use executeQuery only when raw/non-aggregated rows are needed or no time interval is applicable. "
            "Executes a SQL query with time-based increments on the AnyLog network, "
            "splitting data into time intervals for aggregated analysis. "
            "If a UNS is available for the target table, use getRootPolicies + getPolicyChildren first "
            "to retrieve 'dbms' and 'table' from the relevant UNS node before calling this tool. "
            "Traverse the hierarchy, identify the node of interest, and extract 'dbms' and 'table'. "
            "Some device-level nodes also carry a 'where' attribute (a SQL filter for that specific device) "
            "— if present, it must be passed as an additional filter condition alongside the time range. "
            "Do not guess table names or column names — retrieve them from the UNS when available. "
            "The MCP server routes the request through its configured AnyLog node (the gateway). "
            "By default the query is distributed across ALL operator nodes in the network. "
            "Use 'data_nodes' to restrict the query to specific operator nodes that host the data."

        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "dbms": {
                    "type": "string",
                    "description": "The database name"
                },
                "table": {
                    "type": "string",
                    "description": "The table name to query"
                },
                "timeUnit": {
                    "type": "string",
                    "enum": ["minute", "hour", "day", "week", "month", "year"],
                    "description": "The time interval unit for data aggregation"
                },
                "intervalLength": {
                    "type": "integer",
                    "description": "The number of time units (e.g., 5 for '5 minutes', 3 for '3 days')",
                    "minimum": 1
                },
                "timeColumn": {
                    "type": "string",
                    "description": "The name of the timestamp column"
                },
                "projections": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Array of SQL aggregate functions with actual column names from the target table. "
                        "Format: 'aggregation_function(column_name)'. "
                        "Examples: 'min(realpower)', 'max(a_current)', 'avg(reactivepower)', 'count(reactivepower)'."
                    )
                },
                "startTime": {
                    "type": "string",
                    "description": (
                        "Query start time. Supports multiple formats: ISO 8601 as a UTC string YYYY-MM-DD HH:MM:SS "
                        "(2026-01-07T12:24:15Z), or YYYY-MM-DD (2026-01-06), or relative time expressions "
                        "(NOW() - 7 days, NOW() - 24 hours). "
                        "Note: do not include single or double quote in the time string. "
                        "Important: Pass the value as a raw string without adding quote characters around it. "
                        "For example, pass NOW() - 1 hour not 'NOW() - 1 hour'."
                    ),
                    "examples": [
                        "2026-01-06T12:24:15Z",
                        "NOW()",
                        "NOW() - 7 days",
                        "NOW() - 24 hours"
                    ]
                },
                "endTime": {
                    "type": "string",
                    "description": (
                        "Query end time. Supports multiple formats: ISO 8601 as a UTC string YYYY-MM-DD HH:MM:SS "
                        "(2026-01-07T12:24:15Z), or YYYY-MM-DD (2026-01-06), or relative time expressions "
                        "(NOW() - 7 days, NOW() - 24 hours). "
                        "Note: do not include single or double quote in the time string. "
                        "Important: Pass the value as a raw string without adding quote characters around it. "
                        "For example, pass NOW() - 1 hour not 'NOW() - 1 hour'."
                    ),
                    "examples": [
                        "2026-01-06T12:24:15Z",
                        "NOW()",
                        "NOW() - 7 days",
                        "NOW() - 24 hours"
                    ]
                },
                "data_nodes": {
                    "type": "string",
                    "description": (
                        "Optional: The operator node(s) (IP:Port) that HOST the data to query. "
                        "Omit this parameter by default — queries are automatically distributed to ALL nodes in the network hosting the table. "
                        "Only provide data_nodes when explicitly restricting the query to specific nodes (e.g., comparing nodes, debugging a single node, or targeting a known subset). "
                        "Single node: '172.105.60.50:32148'  "
                        "Multiple nodes: '172.105.60.50:32148,172.105.6.90:32148'"
                    )
                },
                **MODE_PROPERTY
            },
            "required": ["dbms", "table", "timeUnit", "intervalLength", "timeColumn", "projections", "startTime", "endTime", "mode"]
        },
        "query": "run client (<data_nodes>) sql <dbms> format = json:list and stat = false SELECT increments(<timeUnit>, <intervalLength>, <timeColumn>), <projections> FROM <table> WHERE <timeColumn> >= <startTime> AND <timeColumn> < <endTime>",
        "default": {
            "<data_nodes>": ""
        }
    },

    "getClusterNodeMapping": {
        "description": (
            "Returns a hierarchical view of data assignments across nodes in the network: "
            "for each company → databases → tables → clusters → physical nodes, "
            "including the IP:Port of each node and whether each cluster and node is currently active. "
            "Use this when you need to know which operator nodes host specific data, "
            "or when you want to restrict a query to a subset of nodes using the 'data_nodes' parameter "
            "in executeQuery or queryWithIncrement."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "company": {
                    "type": "string",
                    "description": "Optional: Filter by specific company name. If not provided, returns data for all companies."
                },
                "dbms": {
                    "type": "string",
                    "description": "Optional: Filter by specific database name. If not provided, returns all databases."
                },
                "table": {
                    "type": "string",
                    "description": "Optional: Filter by specific table name. If not provided, returns all tables."
                },
                **MODE_PROPERTY
            },
            "required": ["mode"]
        },
        "native": "get data nodes where company = <company> and dbms = <dbms> and table = <table> and format = json",
        "remove": [
            "company = <company> and ",
            "dbms = <dbms> and ",
            "table = <table> and ",
        ]
    }
}

