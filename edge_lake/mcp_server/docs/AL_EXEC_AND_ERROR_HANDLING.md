# al_exec and sync_execute: Pseudo Code and Error Handling Analysis

## Overview

This document provides pseudo code for the happy path of `al_exec` and `_sync_execute`, along with an analysis of error handling in the context of MCP vs SSE.

## Table of Contents

1. [al_exec Happy Path Pseudo Code](#al_exec-happy-path-pseudo-code)
2. [_sync_execute Happy Path Pseudo Code](#sync_execute-happy-path-pseudo-code)
3. [Error Handling Analysis](#error-handling-analysis)
4. [Recommendations](#recommendations)

---

## al_exec Happy Path Pseudo Code

**Location:** `edge_lake/tcpip/http_server.py:1126-1257`

**Purpose:** Main HTTP REST execution function that handles EdgeLake commands via HTTP.

### Pseudo Code

```python
def al_exec(status, http_method, command):
    """
    Execute EdgeLake command via HTTP REST interface.

    Args:
        status: ProcessStat object for tracking execution state
        http_method: HTTP method (GET, POST, PUT)
        command: EdgeLake command string (e.g., "get status")

    Returns:
        ret_val: Success/error code
    """

    # 1. VALIDATION: Check command exists
    if not command:
        write_headers_and_msg(REST_BAD_REQUEST, "Missing 'command' attribute")
        return error_code

    # 2. PARSE: Split command into words
    cmd_words = str_to_list(command, 3)  # Split into max 3 parts

    # 3. METHOD VALIDATION: Check HTTP method matches command type
    if not is_correct_method(http_method, cmd_words):
        return error_wrong_method(http_method, command)

    # 4. PREPARATION: Prepare command list and determine execution mode
    commands_list = []
    into_output = get_value_from_headers("into")  # HTML output flag

    ret_val, with_wait, content_type, is_select, is_stream, file_data = \
        prepare_commands(status, command, cmd_words, commands_list, into_output)

    if ret_val != SUCCESS:
        return ret_val  # Preparation failed

    # 5. SEND HEADERS EARLY (for streaming/query responses)
    if with_wait and not into_output:
        # Send HTTP 200 headers first for streaming results
        send_reply_headers(REST_OK, "", chunked=True, content_type)

    # 6. EXECUTION: Execute the command(s)
    buff_size = get_param("io_buff_size")
    io_buff = bytearray(buff_size)

    ret_val = execute_al_commands(status, io_buff, commands_list, into_output, file_data)

    # 7. ERROR HANDLING: Check execution result
    j_handle = status.get_active_job_handle()

    if ret_val != SUCCESS and ret_val < NON_ERROR_RET_VALUE:
        # Command failed - send error response
        err_msg = j_handle.get_operator_error_txt()
        error_failed_process(status, with_wait, ret_val, http_method, into_output, command, err_msg)
        return ret_val

    # 8. RESULT HANDLING: Handle different result types

    # Case A: Local SELECT query (query on this node)
    if not with_wait and is_select:
        job_id = status.get_job_id()
        j_instance = job_scheduler.get_job(job_id)
        nodes_count = j_instance.get_nodes_participating()
        nodes_replied = j_instance.get_nodes_replied()

        # Write query results to HTTP stream
        local_table_query(status, j_handle, with_wait, nodes_count, nodes_replied, send_reply_headers)
        j_instance.set_not_active()

    # Case B: Distributed SELECT query (with_wait=True, aggregated results)
    elif with_wait and is_select:
        job_id = status.get_job_id()
        j_instance = job_scheduler.get_job(job_id)

        j_instance.data_mutex_acquire(status, 'W')

        if j_instance.is_job_active():
            if not j_instance.is_pass_through():
                # Query system_query database for aggregated results
                nodes_count = j_instance.get_nodes_participating()
                nodes_replied = j_instance.get_nodes_replied()
                local_table_query(status, j_handle, with_wait, nodes_count, nodes_replied, send_reply_headers)

                if into_output:
                    # Write HTML including header and data
                    write_headers_and_msg(REST_OK, content_type, j_handle.get_output_buff())

            if j_handle.is_subset():
                # Reply with partial nodes - include summary
                if not j_handle.is_query_completed():
                    query_summary(status, io_buff, j_instance, wfile)

                # Include error list if not all nodes replied
                error_list = j_instance.get_nodes_error_list()
                if error_list:
                    reply = output_nested_lists(error_list, "Reply errors:",
                                               ["Node IP", "Port", "Par", "Ret-Code", "Error Message"])
                    write_to_stream(status, wfile, reply)

        j_instance.data_mutex_release(status, 'W')
        j_instance.set_not_active()

    # Case C: Distributed non-SELECT command (print message from multiple nodes)
    elif with_wait and not is_select:
        job_id = status.get_job_id()
        j_instance = job_scheduler.get_job(job_id)

        j_instance.data_mutex_acquire(status, 'W')

        if j_instance.is_job_active():
            result_set = j_instance.get_nodes_print_message()
            transfer_encoding = False if "url" in headers else True
            write_to_stream(status, wfile, result_set, transfer_encoding)

        j_instance.data_mutex_release(status, 'W')

    # Case D: Stream file (e.g., image, PDF)
    elif is_stream:
        stream_file = j_handle.get_stream_file()
        deliver_stream_file(status, stream_file, content_type)

    # Case E: Regular result (most common for non-query commands)
    else:
        result_set = j_handle.get_result_set()

        if isinstance(result_set, list):
            result_set = "\r\n".join([str(item) for item in result_set])

        content_type = get_result_set_type(result_set)  # 'text/json' or 'text'

        if not with_wait:
            if result_set:
                write_headers_and_msg(REST_OK, content_type, result_set)
            else:
                is_chunked = True if http_method == "get" else False
                send_reply_headers(REST_OK, "", chunked=is_chunked, content_type)
        elif result_set:
            write_to_stream(status, wfile, result_set)

    return ret_val
```

### Key Points

1. **Method Validation:** Ensures HTTP method matches command type (e.g., POST for data ingestion)
2. **Streaming:** Headers sent early for queries to enable streaming results
3. **Job Management:** Uses job_scheduler for distributed queries with mutex protection
4. **Multiple Result Types:** Handles queries, commands, streams, and HTML output
5. **Error Path:** Uses `error_failed_process()` for execution errors

---

## _sync_execute Happy Path Pseudo Code

**Location:** `edge_lake/mcp_server/core/direct_client.py:94-234`

**Purpose:** Direct client execution that mimics `al_exec` without HTTP overhead.

### Pseudo Code

```python
def _sync_execute(command, headers=None, socket=None):
    """
    Execute command using SHARED command_execution layer (same as al_exec).

    Args:
        command: EdgeLake command string
        headers: Optional headers with 'destination', 'subset', 'timeout'
        socket: Socket for streaming results (BytesIO for MCP)

    Returns:
        Command result (parsed JSON or string)

    Note:
        Uses shared command_execution module for consistency with REST API.
    """

    # 1. INITIALIZATION: Create status object
    status = ProcessStat()

    # 2. SETUP: Configure job handle for REST-style output
    j_handle = status.get_job_handle()
    j_handle.set_rest_caller()  # Enable JSON formatting

    if socket:
        j_handle.set_output_socket(socket)  # Enable streaming to socket
    else:
        status.add_error("No socket provided, results may be lost")

    # 3. PARSE: Split command into words (same as al_exec)
    cmd_words = str_to_list(command, 3)

    # 4. EXTRACT HEADERS: Get execution options
    destination = headers.get('destination') if headers else None  # Target node(s)
    subset = headers.get('subset') if headers else False          # Allow partial results
    sec_timeout = headers.get('timeout') if headers else 20       # Network timeout

    # 5. BUILD WRAPPER: Create "run client ()" wrapper for distributed commands
    run_client = command_execution.get_run_client(destination, subset, sec_timeout)

    # 6. PREPARATION: Prepare commands list (shared with al_exec)
    commands_list = []
    ret_val, with_wait, content_type, is_select, is_stream, file_data = \
        command_execution.prepare_commands(
            status, command, cmd_words, commands_list, None,
            run_client, None, None, False, None
        )

    if ret_val != SUCCESS:
        error_msg = status.get_saved_error() or f"Command preparation failed: {ret_val}"
        status.add_error(error_msg)
        raise Exception(error_msg)

    # 7. EXECUTION: Execute commands (shared with al_exec)
    buff_size = int(params.get_param("io_buff_size"))
    io_buff = bytearray(buff_size)

    ret_val = command_execution.execute_al_commands(
        status, io_buff, commands_list, None, file_data, socket
    )

    # 8. GET RESULTS: Retrieve job handle (contains results)
    j_handle = status.get_active_job_handle()

    # 9. HANDLE AGGREGATED QUERIES: Special case for distributed SELECT
    if ret_val == SUCCESS and with_wait and is_select:
        job_id = status.get_job_id()

        if job_id != JOB_INSTANCE_NOT_USED:
            j_instance = job_scheduler.get_job(job_id)
            j_instance.data_mutex_acquire(status, 'W')

            if j_instance.is_job_active():
                if not j_instance.is_pass_through():
                    # Query system_query database for aggregated results
                    nodes_count = j_instance.get_nodes_participating()
                    nodes_replied = j_instance.get_nodes_replied()

                    # Call shared local_table_query (no HTTP headers callback for MCP)
                    ret_val = command_execution.local_table_query(
                        status, j_handle, with_wait, nodes_count, nodes_replied, None
                    )

                    # Aggregation results written to socket - read them back
                    if socket:
                        socket.seek(0)
                        buffer_data = socket.read()
                        if buffer_data:
                            j_instance.data_mutex_release(status, 'W')
                            j_instance.set_not_active()
                            return _parse_result_set(buffer_data.decode('utf-8'))

            j_instance.data_mutex_release(status, 'W')
            j_instance.set_not_active()

        # For aggregated queries, results streamed to socket
        return ""

    # 10. HANDLE NON-AGGREGATED RESULTS: Get from result_set
    if ret_val == SUCCESS:
        result_set = j_handle.get_result_set()

        if result_set:
            return _parse_result_set(result_set)  # Parse JSON if possible
        else:
            # Results streamed to socket (for local queries)
            return ""

    elif ret_val == 141:
        # Error 141: EdgeLake not ready yet
        status.add_error(f"EdgeLake not ready for command '{command}' (code 141)")
        return ""

    else:
        # Command failed - get error from status
        error_msg = status.get_saved_error() or f"Command failed with code {ret_val}"
        status.add_error(error_msg)
        raise Exception(error_msg)


def _parse_result_set(result_set):
    """
    Parse result_set from job_handle.

    Args:
        result_set: Result string or object

    Returns:
        Parsed JSON object or original string
    """
    if not result_set:
        return ""

    if isinstance(result_set, str):
        result_trimmed = result_set.strip()
        if result_trimmed:
            try:
                # Try to parse as JSON
                return json.loads(result_trimmed)
            except (json.JSONDecodeError, TypeError):
                # Not JSON - return as string
                return result_trimmed
    else:
        # Already an object - return directly
        return result_set

    return ""
```

### Key Points

1. **Shared Logic:** Uses same `command_execution` module as `al_exec`
2. **Socket Streaming:** Uses BytesIO socket for MCP (instead of HTTP wfile)
3. **No HTTP Overhead:** No HTTP headers, status codes, or chunked encoding
4. **Direct Results:** Returns parsed JSON or string directly
5. **Error Handling:** Raises exceptions (caught by MCP layer)

---

## Error Handling Analysis

### error_failed_process in http_server.py

**Location:** `edge_lake/tcpip/http_server.py:626-674`

**Purpose:** Send HTTP error response when EdgeLake command execution fails.

#### Error Response Structure

```python
def error_failed_process(status, with_wait, error_code, http_method, into_output, command, err_msg):
    """
    Send HTTP error response for failed EdgeLake command.

    Creates error reply with:
    - method: HTTP method (GET, POST, PUT)
    - node: Node address
    - err_code: EdgeLake error code
    - err_text: Generic error message from process_status
    - operator_msg: Error from operator (if distributed command)
    - local_msg: Detailed error from status.get_saved_error()
    """

    # Build error response
    err_reply = {
        "method": http_method,
        "node": self.address_string(),
        "err_code": error_code,
        "err_text": process_status.get_status_text(error_code)
    }

    # Add operator error (from distributed command)
    if err_msg:
        err_reply["operator_msg"] = err_msg

    # Add detailed local error
    local_err = status.get_saved_error()
    if local_err:
        err_reply["local_msg"] = local_err

    # Convert to JSON string
    reply = utils_json.to_string(err_reply)

    # Send response
    if into_output:
        # HTML output
        send_html_with_error(reply)
    else:
        if not with_wait:
            # Send HTTP 400 headers + error body
            send_reply_headers(REST_BAD_REQUEST, reply, content_type='text/json')

        # Write error to stream
        write_to_stream(status, wfile, reply)
```

#### Example Error Response

```json
{
  "method": "put",
  "node": "10.0.0.78:7849",
  "err_code": 155,
  "err_text": "Failed to execute command",
  "operator_msg": "Table 'xyz' does not exist",
  "local_msg": "SQL execution failed: no such table: xyz"
}
```

### SSE Error Handling

**Location:** `edge_lake/mcp_server/transport/sse_handler.py:414-430`

**Purpose:** Send JSON-RPC 2.0 error response via SSE event.

#### Error Response Structure

```python
def _process_message_sync(session_id, message, socket):
    """
    Process MCP message and handle errors.
    """
    try:
        # Process message via MCP server
        response = mcp_server.process_message(message, socket)

        # Queue response for SSE delivery
        connection.queue_message('message', response)

    except Exception as e:
        # Build JSON-RPC 2.0 error response
        error_response = {
            'jsonrpc': '2.0',
            'id': message.get('id'),
            'error': {
                'code': -32603,           # JSON-RPC internal error
                'message': f'Internal error: {str(e)}'
            }
        }

        # Queue error for SSE delivery
        connection.queue_message('error', error_response)
```

#### Example SSE Error Event

```
data: {"jsonrpc":"2.0","id":123,"error":{"code":-32603,"message":"Internal error: Command failed"}}
event: error
id: 5

```

### MCP Protocol Error Codes

JSON-RPC 2.0 standard error codes:

| Code | Meaning | Use Case |
|------|---------|----------|
| -32700 | Parse error | Invalid JSON received |
| -32600 | Invalid request | Missing required fields |
| -32601 | Method not found | Unknown MCP method |
| -32602 | Invalid params | Wrong parameter types |
| -32603 | Internal error | Command execution failed |

---

## Comparison: error_failed_process vs SSE Error Handling

### error_failed_process (HTTP REST)

**Protocol:** HTTP with custom EdgeLake error format

**Response Format:**
```json
{
  "method": "put",
  "node": "10.0.0.78:7849",
  "err_code": 155,
  "err_text": "Failed to execute command",
  "operator_msg": "...",
  "local_msg": "..."
}
```

**HTTP Status:** 400 Bad Request

**Use Case:** REST API clients (curl, HTTP libraries)

**Pros:**
- Rich error context (node, method, multiple error messages)
- EdgeLake-specific error codes

**Cons:**
- Not standard JSON-RPC format
- Requires custom parsing by clients

### SSE Error Handling (MCP Protocol)

**Protocol:** JSON-RPC 2.0 via SSE

**Response Format:**
```json
{
  "jsonrpc": "2.0",
  "id": 123,
  "error": {
    "code": -32603,
    "message": "Internal error: Command failed"
  }
}
```

**HTTP Status:** N/A (SSE event, not HTTP response)

**Use Case:** MCP clients (Claude Desktop, AI agents)

**Pros:**
- Standard JSON-RPC 2.0 format
- Compatible with MCP protocol spec
- Works with SSE streaming

**Cons:**
- Less detailed error context
- Generic error codes

---

## Recommendations

### 1. Do NOT Use error_failed_process for MCP

**Reason:** MCP protocol requires JSON-RPC 2.0 error format, not EdgeLake custom format.

**Current Implementation:** SSE handler already implements JSON-RPC 2.0 errors correctly (line 422-430).

### 2. When to Use Each Error Handler

| Context | Handler | Format |
|---------|---------|--------|
| HTTP REST API | `error_failed_process()` | EdgeLake custom JSON |
| MCP via SSE | SSE `queue_message('error', ...)` | JSON-RPC 2.0 |
| Direct Client | Raise exception | Python exception |

### 3. Enhance SSE Error Messages

Consider adding more context to JSON-RPC errors while maintaining spec compliance:

```python
# Current (minimal)
error_response = {
    'jsonrpc': '2.0',
    'id': message.get('id'),
    'error': {
        'code': -32603,
        'message': f'Internal error: {str(e)}'
    }
}

# Enhanced (with data field - allowed by JSON-RPC spec)
error_response = {
    'jsonrpc': '2.0',
    'id': message.get('id'),
    'error': {
        'code': -32603,
        'message': f'Internal error: {str(e)}',
        'data': {
            'edgelake_code': error_code,
            'command': command,
            'node': node_address,
            'details': status.get_saved_error()
        }
    }
}
```

### 4. al_exec Refactoring for Direct Client

**Current Issue:** Direct client duplicates al_exec logic with slight variations.

**Recommendation:** Extract common logic into shared functions in `command_execution.py`:

```python
# Proposed refactoring
def execute_command_shared(status, command, headers, socket=None, http_callback=None):
    """
    Shared command execution logic for both HTTP and direct clients.

    Args:
        status: ProcessStat object
        command: EdgeLake command
        headers: Execution headers (destination, subset, timeout)
        socket: Output socket (wfile for HTTP, BytesIO for MCP)
        http_callback: Optional HTTP header callback (for al_exec only)

    Returns:
        (ret_val, result_data, error_info)
    """
    # All shared logic here
    pass
```

**Benefits:**
- Single source of truth for command execution
- Easier to maintain and test
- Reduces code duplication

---

## Summary

### al_exec (HTTP REST)

- Full-featured HTTP command execution
- Handles streaming, queries, and file delivery
- Uses `error_failed_process()` for errors
- Sends HTTP status codes and headers

### _sync_execute (Direct Client)

- Mimics al_exec without HTTP overhead
- Shares command_execution logic
- Uses BytesIO socket for streaming
- Raises exceptions for errors

### Error Handling

- **HTTP REST:** Use `error_failed_process()` - EdgeLake custom format
- **MCP/SSE:** Use JSON-RPC 2.0 format - already implemented correctly
- **Do NOT** use `error_failed_process()` for MCP - it's HTTP-specific

### Next Steps

1. Keep current SSE error handling (JSON-RPC 2.0)
2. Consider enhancing error messages with `data` field
3. Consider refactoring al_exec to share logic with direct client
