"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

'''
The process initiated here does the following:
1) Identifies the MCP request
2) Map the request to native commands
3) Orchestrates the reply
'''


"""
Claude configs for connection
1. pip install mcp-proxy 
2. locate mcp-proxy.exe

"edgelake-mcp-proxy": {
  "command": "D:\\EdgeLake-code\\tom-EdgeLake\\venv\\Scripts\\mcp-proxy.exe", 
  "args": ["http://10.0.0.78:32149/mcp/sse"] 
},
"""

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
import socket


import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.trace_func as trace_func
import edge_lake.generic.utils_threads as utils_threads
import edge_lake.cmd.native_api as native_api
import edge_lake.generic.params as al_params
import edge_lake.job.job_scheduler as job_scheduler
import edge_lake.cmd.member_cmd as member_cmd




local_err_to_json_rpc_ = {
    process_status.ERR_wrong_json_structure : -32700,   # - Parse error
    process_status.ERR_process_failure : -32603,        # Internal error
    process_status.ERR_command_struct : -32602,         # Invalid params
    process_status.ERR_unrecognized_command :  -32601,  # - Method not found
    process_status.Client_call_not_supported : -32600   # Invalid Request
}

open_sessions_ = {}     # Dynamic session info as a f(session id)

# -------------------------------------------------------------------------------
# handle_sse_endpoint - Keeps a write socket open
# The SSE socket is kept open - because the caller flagged self._is_sse = True
# -------------------------------------------------------------------------------
def handle_sse_endpoint(handler, status):
    global open_sessions_
    """
    Handle SSE endpoint (called from http_server.py).

    Args:
        handler: ChunkedHTTPRequestHandler

    Returns:
        True if handled
    """

    trace_level = trace_func.get_func_trace_level("mcp")

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

    # Keep the session info as f(session_id)
    open_sessions_[session_id] = {
        "sse_socket": sse_socket,
        "lock": threading.Lock(),          # Multiple POSTs can be executed on the same session
        "raw_socket": raw_socket,          # kept for info, but we don't touch it
        "active": True                     # 🔵 NEW FLAG to control loop lifetime
    }

    # If new session, send session_id to client
    if is_new:
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
    ready_event = {
        "jsonrpc": JSONRPC_VER,
        "method": "notifications/message",
        "params": {
            "level": "info",
            "data": "SSE connection established"
        }
    }
    sse_socket.write(f"data: {json.dumps(ready_event)}\n\n".encode("utf-8"))
    sse_socket.flush()

    if trace_level:
        utils_print.output(f"\n\nWait to close connection: {session_id}\n\n", False)

    try:
        while True:
            # 🔵 Check if this session is still active.
            session = open_sessions_.get(session_id)
            if not session or not session.get("active", True):
                break

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
        session = open_sessions_.get(session_id)
        if session and session["sse_socket"] is sse_socket:
            # Mark inactive and remove from map so any other thread sees it's gone
            session["active"] = False
            open_sessions_.pop(session_id, None)

        # 🔴 IMPORTANT: do NOT call shutdown() or close() on raw_socket here.
        # Let BaseHTTPRequestHandler.finish() handle socket closure safely.

        if trace_level:
            utils_print.output_box(
                f"MCP Server disconnected from client - Session ID: {session_id}",
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
    if trace_level:
        time_ms_start = time.time()
        utils_print.output(f"\n\n--------- Time Start: {time_ms_start} -------------------\n\n", False)


    session_id = _get_session_id(handler.path)
    open_sessions_[session_id]["post_counter"] += 1

    if not session_id:
        error_msg = f"MCP Error: Invalid MCP messages path: {handler.path}"
        status.add_error(error_msg)
        handler.send_error(400, "Invalid path - missing session_id")
        utils_print.output_box(error_msg)
        return False

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

    if method is None or req_id is None:
        error_reply = {
            "jsonrpc": JSONRPC_VER,
            "id": req_id,
            "error": {"code": -32600, "message": "Missing id or method"}
        }
        return _send_json(status, session_id, handler, error_reply)


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

    # ⭐⭐⭐ SEND OK Message to the MCP CLIENT using the HTTP connection ⭐⭐⭐

    # 2) Also send a trivial HTTP 200 OK so httpx is happy
    try:
        handler.send_response(MCP_OK)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", "0")  # Empty body
        handler.end_headers()
        handler.wfile.flush()
    except:
        errno, value = sys.exc_info()[:2]
        error_msg = f"MCP Error: Failed to send HTTP acknowledgment: '{value}'"
        status.add_error(error_msg)
        utils_print.output_box(error_msg)
        return False

    # ⭐⭐⭐ PROCESS The MESSAGE and reply using SSE socket ⭐⭐⭐

    if method == "tools/call":
        # RULE 2 —  For ALL other POST requests:  ✅ Reply is sent via HTTP (not SSE).
        ret_val, reply_obj = _execute_cmd(status, session_id, session, req_id, message, trace_level)
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

    ret_val = _send_sse_json(status, session_id, session, reply_obj)
    if ret_val:
        return False


    if trace_level:
        ret_val_txt = process_status.get_status_text(ret_val)
        time_ms_end = time.time()
        time_ms_diff = time_ms_end - time_ms_start
        utils_print.output(f"\n\n--------- Time End: {time_ms_end} -----Time Diff: {time_ms_diff} --------------\n\n", False)
        utils_print.output(f"\n\n{session_id} - REPLY: {ret_val_txt}\n\n", False)
        utils_print.jput(reply_obj, True, 4)

    return  ret_val == process_status.SUCCESS


# ---------------------------------------------------------------------------------
# Execute an AnyLog command
# ---------------------------------------------------------------------------------
def _execute_cmd(status, session_id, session, req_id, message, trace_level):

    # STEP 1 - Get the AnyLog command
    reply_obj = None
    params = message.get("params")
    if params:
        local_name = params.get("name")
        arguments = params.get("arguments")
        if local_name:
            al_info = functionalities_.get(local_name)
            if "native" in al_info:
                with_wait = False           # No wait for execution
                al_command = al_info["native"]
                ret_val = process_status.SUCCESS
            elif "query" in al_info:
                with_wait = True            # Wait for servers to replay
                al_command = al_info["query"]
                ret_val = process_status.SUCCESS
            else:
                ret_val = process_status.Client_call_not_supported
        else:
            ret_val = process_status.Client_call_not_supported
    else:
        # Message must include params
        local_name = None
        ret_val = process_status.ERR_wrong_json_structure

    # Step 2 - Execute the AnyLog command
    # Commands can't be returned in chunks, - define a key name to contain the reply
    if not ret_val:
        thread_name = utils_threads.get_thread_name()
        reply_key = f"mcp_process_{thread_name}"
        al_params.reset_key(reply_key)      # Reset the key buffer

        for key, value in arguments.items():
            al_command = al_command.replace(f"<{key}>", value)        # Place the arguments on command

        command = f"{reply_key} = {al_command}"

        if trace_level:
            utils_print.output(f"\r\n***** {command} *****\r\n", False)

        if with_wait:
            # Multiple servers participate - wait for reply
            ret_val = native_api.exec_al_cmd(status, command, None, None, 20)
            if not ret_val:
                ret_val = pull_query_data(status)
        else:
            io_buff = status.get_io_buff()
            ret_val = native_api.exec_no_wait(status, command, io_buff, None, None)

        if not ret_val:
            # SUCCESS
            reply_txt = al_params.get_value_if_available(f"!{reply_key}")
            al_params.reset_key(reply_key)      # Release memory
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


    if ret_val:
        # ERROR
        reply_obj = get_err_msg(ret_val, req_id, local_name)
        msg_text = json.dumps(reply_obj)
        status.add_error("MPC Error: Failed to execute command: " + msg_text)

    return ret_val, reply_obj

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
def get_err_msg( local_err_code, req_id, local_name ):
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
    tools = []
    for al_func, info in functionalities_.items():
        entry = {
            "name": al_func,
            "description": info["description"],
            "inputSchema": info["inputSchema"]
        }
        tools.append(entry)
    return tools

def _send_json(status, session_id, handler, obj):
    try:
        print("HTTP-REPLY SOCKET =", handler.wfile, file=sys.stderr)

        data = json.dumps(obj).encode("utf-8")
        handler.send_response(MCP_OK)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(data)))
        handler.end_headers()
        handler.wfile.write(data)
        handler.wfile.flush()   # <-- REQUIRED FIX


    except:
        errno, value = sys.exc_info()[:2]
        error_msg = f"MCP Error: Failed to send JSON reply: '{value}'"
        status.add_error(error_msg)
        utils_print.output_box(error_msg)
        ret_val = process_status.ERR_process_failure
    else:
        ret_val = process_status.SUCCESS

    return ret_val

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

functionalities_ = {

        "checkStatus": {
            "description": "Checks whether a node is running and connected to the network (maps to 'get status')",
            "inputSchema": {            # something the server sends to the client to describe what input a tool expects
                "type": "object",       # The input MUST be an object
                "properties": {
                    "name": { "type": "string" }
                },
                "required": ["name"]
            },
            "native"   : "get status where format = mcp"    # Run command as is
        },

        "getNodesList": {
            "description": "Returns the list of nodes in the network, for each node, the type, the name the IP and port",
            "inputSchema": {  # No parameters required.
                "type": "object",  # The input MUST be an object
                "properties": {},
                "required": []
            },
            "native"   : "blockchain get (master, query, operator) bring.json [*] [*][name] [*][ip] [*][port] "    # Run command as is
        },

        "listDatabases": {
            "description": "Returns a list of all the databases and tables in the network",
            "inputSchema": {            # something the server sends to the client to describe what input a tool expects
                "type": "object",       # The input MUST be an object
                "properties": {},
                "required": []
            },
            "native"   : "blockchain get (table) bring.json.unique [*][dbms]"    # Run command as is
        },

        "listTables": {
            "description": "Returns a list of all the tables for a given database",
            "inputSchema": {  # something the server sends to the client to describe what input a tool expects
                "type": "object",  # The input MUST be an object
                "properties": {
                    "dbms": {"type": "string"}
                },
                "required": ["dbms"]
            },
            "native": " blockchain get (table) where dbms = <dbms> bring.unique.json [*][name]"  # Run command as is
        },

        "listColumns": {
            "description": "Returns column definitions for a given table",
            "inputSchema": {  # something the server sends to the client to describe what input a tool expects
                "type": "object",  # The input MUST be an object
                "properties": {
                    "dbms": { "type": "string" },
                    "table": { "type": "string" }
                },
                "required": ["dbms", "table"]
            },
            "native" : "get columns where dbms = <dbms> and table = <table> and format = mcp and sys_col = false"
        },

        "executeQuery" : {
          "description": "Executes a SQL query on the AnyLog network",
          "inputSchema": {
            "type": "object",
            "inputSchema": {
              "dbms": {
                "type": "string",
                "description": "The database name"
              },
              "sql": {
                "type": "string",
                "description": "The SQL query to execute"
              }
            },
            "required": ["dbms", "sql"]
          },
          "query" : "run client () sql <dbms> format = json:list and stat = false and pass_through = false <sql>",
        }

    }
