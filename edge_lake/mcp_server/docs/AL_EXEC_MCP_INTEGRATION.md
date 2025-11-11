# Integrating al_exec with MCP: The Error Handling Challenge

## The Core Problem

**al_exec is deeply coupled to HTTP transport:**
- Calls `self.write_headers_and_msg()` - writes HTTP response
- Calls `self.send_reply_headers()` - sends HTTP headers
- Calls `self.error_failed_process()` - sends HTTP 400/500
- Assumes `self.wfile` is HTTP socket

**MCP requires different transport:**
- Errors must be JSON-RPC error objects
- Success must be JSON-RPC result objects
- Both sent via SSE events (not HTTP responses)
- Different socket (SSE long-lived, not HTTP request/response)

---

## Error Path Mapping

### HTTP REST Error Flow

```
al_exec()
  ├─ Command missing → write_headers_and_msg(400, "Missing command")
  │                    └─ HTTP/1.1 400 Bad Request
  │                       {"error": "Missing command"}
  │
  ├─ Wrong method → error_wrong_method()
  │                 └─ HTTP/1.1 400 Bad Request
  │                    {"method": "post", "err_code": 123, ...}
  │
  ├─ Execute fails → error_failed_process()
  │                  └─ HTTP/1.1 400 Bad Request
  │                     {"err_code": 155, "err_text": "...", ...}
  │
  └─ Success → send_reply_headers(200, ...)
               └─ HTTP/1.1 200 OK
                  {query results}
```

**Channel:** Same HTTP socket (request → response)

### MCP/SSE Error Flow (Required)

```
SSE Handler
  ├─ Command missing → Queue JSON-RPC error
  │                    └─ data: {"jsonrpc":"2.0","id":123,"error":{"code":-32602,"message":"Missing command"}}
  │                       event: message
  │
  ├─ Wrong method → Queue JSON-RPC error
  │                 └─ data: {"jsonrpc":"2.0","id":123,"error":{"code":-32602,"message":"Wrong method"}}
  │                    event: message
  │
  ├─ Execute fails → Queue JSON-RPC error
  │                  └─ data: {"jsonrpc":"2.0","id":123,"error":{"code":-32603,"message":"Execution failed"}}
  │                     event: message
  │
  └─ Success → Queue JSON-RPC result
               └─ data: {"jsonrpc":"2.0","id":123,"result":{"content":[...]}}
                  event: message
```

**Channel:** Different socket (SSE stream from GET /mcp/sse)

---

## Integration Approaches

### Approach 1: Mock HTTP Handler (Intercept Method)

**Idea:** Create a fake handler object that looks like HTTP handler but redirects to MCP

```python
class MCPHandlerAdapter:
    """
    Fake HTTP handler that intercepts al_exec's HTTP calls
    and converts them to MCP JSON-RPC messages.
    """

    def __init__(self, sse_connection, json_rpc_id, session_id):
        self.sse_connection = sse_connection
        self.json_rpc_id = json_rpc_id
        self.session_id = session_id
        self.wfile = BytesIO()  # Fake socket
        self.error_occurred = False
        self.error_code = None
        self.error_message = None

    def write_headers_and_msg(self, status, http_status_code, content_type, message):
        """Intercept HTTP error - convert to JSON-RPC"""
        if http_status_code >= 400:
            # This is an error
            self.error_occurred = True
            self.error_message = message
            self.error_code = -32603  # Internal error

    def send_reply_headers(self, status, http_status_code, body, chunked, content_type, length, transfer_encoding, file_name):
        """Intercept HTTP headers - ignore for MCP"""
        if http_status_code >= 400:
            self.error_occurred = True
            self.error_message = body

    def error_failed_process(self, status, with_wait, error_code, http_method, into_output, command, err_msg):
        """Intercept error handler - convert to JSON-RPC"""
        self.error_occurred = True
        self.error_code = -32603
        self.error_message = err_msg or f"Command failed with code {error_code}"

    def get_result(self):
        """Get JSON-RPC response after al_exec completes"""
        if self.error_occurred:
            return {
                "jsonrpc": "2.0",
                "id": self.json_rpc_id,
                "error": {
                    "code": self.error_code or -32603,
                    "message": self.error_message or "Unknown error",
                    "data": {
                        "session_id": self.session_id
                    }
                }
            }
        else:
            # Success - read from wfile
            self.wfile.seek(0)
            result_data = self.wfile.read()
            return {
                "jsonrpc": "2.0",
                "id": self.json_rpc_id,
                "result": {
                    "content": [{
                        "type": "text",
                        "text": result_data.decode('utf-8') if result_data else ""
                    }]
                }
            }
```

**Usage:**

```python
def _process_message_with_al_exec(self, session_id, message):
    """Process MCP message using al_exec"""

    # Create adapter
    adapter = MCPHandlerAdapter(
        self.connections[session_id],
        message.get('id'),
        session_id
    )

    # Extract EdgeLake command from MCP message
    tool_name = message.get('params', {}).get('name')
    arguments = message.get('params', {}).get('arguments', {})
    command = self._build_edgelake_command(tool_name, arguments)

    # Create status
    status = ProcessStat()

    # Call al_exec (but it writes to adapter, not real HTTP socket)
    ret_val = al_exec_modified(adapter, status, "get", command)

    # Get JSON-RPC response
    response = adapter.get_result()

    # Queue for SSE delivery
    connection = self.connections.get(session_id)
    connection.queue_message('message', response)
```

**Problem:** al_exec is an instance method (`self.al_exec`), expects `self` to be ChunkedHTTPRequestHandler

**Verdict:** ⚠️ Would require refactoring al_exec to be standalone function

---

### Approach 2: Extract Core Logic (Refactor Method)

**Idea:** Pull al_exec's logic into transport-agnostic functions

```python
# New module: edge_lake/cmd/command_execution.py (expand existing)

def execute_command_generic(status, command, transport_callbacks):
    """
    Execute EdgeLake command with pluggable transport.

    Args:
        status: ProcessStat object
        command: EdgeLake command string
        transport_callbacks: Dict with send_error, send_success callbacks

    Returns:
        (ret_val, result_data, error_info)
    """

    if not command:
        # Instead of: self.write_headers_and_msg(400, "Missing command")
        return (process_status.FAILURE, None, {
            "code": -32602,
            "message": "Missing command parameter"
        })

    cmd_words = utils_data.str_to_list(command, 3)

    # Validate method (for HTTP compatibility)
    # For MCP, we can skip this or adapt it

    # Prepare commands
    commands_list = []
    ret_val, with_wait, content_type, is_select, is_stream, file_data = \
        prepare_commands(status, command, cmd_words, commands_list, None)

    if ret_val != SUCCESS:
        return (ret_val, None, {
            "code": -32603,
            "message": "Command preparation failed"
        })

    # Execute commands
    buff_size = int(params.get_param("io_buff_size"))
    io_buff = bytearray(buff_size)

    # Need socket for result streaming
    output_socket = transport_callbacks.get('output_socket')
    ret_val = execute_al_commands(status, io_buff, commands_list, None, file_data, output_socket)

    # Check for errors
    j_handle = status.get_active_job_handle()

    if ret_val != SUCCESS and ret_val < NON_ERROR_RET_VALUE:
        err_msg = j_handle.get_operator_error_txt()
        return (ret_val, None, {
            "code": -32603,
            "message": err_msg or f"Command failed with code {ret_val}",
            "data": {
                "edgelake_code": ret_val,
                "command": command
            }
        })

    # Handle different result types
    if is_select:
        # Query result
        if with_wait:
            # Aggregated query - results in socket
            output_socket.seek(0)
            result_data = output_socket.read()
        else:
            # Local query
            result_data = j_handle.get_result_set()
    else:
        # Command result
        result_data = j_handle.get_result_set()

    return (SUCCESS, result_data, None)
```

**Usage in HTTP (al_exec):**

```python
def al_exec(self, status, http_method, command):
    """HTTP REST endpoint - uses generic execution"""

    # Create output socket (self.wfile for HTTP)
    transport_callbacks = {
        'output_socket': self.wfile
    }

    ret_val, result_data, error_info = execute_command_generic(
        status, command, transport_callbacks
    )

    if error_info:
        # Send HTTP error
        self.write_headers_and_msg(status, REST_BAD_REQUEST, "json",
                                   json.dumps(error_info))
    else:
        # Send HTTP success
        content_type = get_result_set_type(result_data)
        self.write_headers_and_msg(status, REST_OK, content_type, result_data)

    return ret_val
```

**Usage in MCP:**

```python
def _process_message_with_generic_exec(self, session_id, message):
    """MCP endpoint - uses generic execution"""

    # Create output socket (BytesIO for MCP)
    output_socket = BytesIO()
    transport_callbacks = {
        'output_socket': output_socket
    }

    # Extract command
    command = self._build_edgelake_command(message)

    # Create status
    status = ProcessStat()

    # Execute
    ret_val, result_data, error_info = execute_command_generic(
        status, command, transport_callbacks
    )

    # Build JSON-RPC response
    if error_info:
        response = {
            "jsonrpc": "2.0",
            "id": message.get('id'),
            "error": error_info
        }
    else:
        response = {
            "jsonrpc": "2.0",
            "id": message.get('id'),
            "result": {
                "content": [{
                    "type": "text",
                    "text": result_data
                }]
            }
        }

    # Queue for SSE
    connection = self.connections.get(session_id)
    connection.queue_message('message', response)
```

**Verdict:** ✅ Clean, but requires refactoring al_exec

---

### Approach 3: Keep Separate (Current - direct_client.py)

**Idea:** Don't use al_exec directly. Use shared command_execution module functions.

**Current implementation:**

```python
# edge_lake/mcp_server/core/direct_client.py

def _sync_execute(self, command, headers=None, socket=None):
    """Execute using SHARED command_execution layer"""

    status = ProcessStat()

    # Use shared functions (NOT al_exec)
    cmd_words = utils_data.str_to_list(command, 3)
    run_client = command_execution.get_run_client(...)

    commands_list = []
    ret_val, with_wait, content_type, is_select, is_stream, file_data = \
        command_execution.prepare_commands(status, command, cmd_words, ...)

    if ret_val != SUCCESS:
        raise Exception(f"Command preparation failed: {ret_val}")

    # Execute
    ret_val = command_execution.execute_al_commands(status, io_buff, commands_list, ...)

    if ret_val != SUCCESS:
        raise Exception(f"Command failed: {ret_val}")

    # Return result
    return result_set
```

**Error handling in SSE layer:**

```python
# edge_lake/mcp_server/transport/sse_handler.py

def _process_message_sync(self, session_id, message, socket):
    try:
        # Process via MCP server
        response = self.mcp_server.process_message(message, socket)

        # Queue success response
        connection.queue_message('message', response)

    except Exception as e:
        # Build JSON-RPC error
        error_response = {
            "jsonrpc": "2.0",
            "id": message.get('id'),
            "error": {
                "code": -32603,
                "message": f"Internal error: {str(e)}"
            }
        }

        # Queue error response
        connection.queue_message('error', error_response)
```

**Verdict:** ✅ This is what you already have! Works, no refactoring needed.

---

## Comparison Matrix

| Approach | Code Reuse | Refactor Required | Error Handling | Complexity | Status |
|----------|------------|-------------------|----------------|------------|--------|
| **Mock Handler** | High (use al_exec) | Medium (make al_exec standalone) | Complex (intercept all HTTP calls) | High | Not implemented |
| **Extract Core** | Medium (share logic) | High (refactor al_exec and MCP) | Clean (return tuples) | Medium | Not implemented |
| **Separate Paths** | Medium (share command_execution) | None | Clean (exceptions + JSON-RPC) | Low | ✅ **Current** |

---

## The Real Question

**Why integrate al_exec with MCP?**

### Common Reasoning
"We already have al_exec working for REST. Just use it for MCP too. Why duplicate code?"

### The Reality
**al_exec is not just execution logic - it's HTTP transport logic!**

```python
# What al_exec ACTUALLY does:
al_exec() {
    // 10% - Validation
    // 20% - Command preparation
    // 30% - Execution
    // 40% - HTTP-specific result handling ← Problem!
}
```

**The 40% that's HTTP-specific:**
- `self.send_reply_headers()` - HTTP headers
- `self.write_headers_and_msg()` - HTTP responses
- `self.error_failed_process()` - HTTP 400/500
- `local_table_query(..., self.send_reply_headers)` - HTTP callback
- Stream to `self.wfile` - HTTP socket

**What MCP actually shares with al_exec:**
- Command preparation (20%)
- Execution (30%)
- **Total: 50%**

**Current approach (direct_client.py) ALREADY shares this 50%:**
- Uses `command_execution.prepare_commands()` ← Shared
- Uses `command_execution.execute_al_commands()` ← Shared
- Uses `command_execution.local_table_query()` ← Shared

---

## What You're Actually Sharing Now

```
┌─────────────────────────────────────────────────────┐
│           HTTP REST (al_exec)                       │
│                                                     │
│  al_exec() {                                        │
│    prepare_commands() ───────┐                     │
│    execute_al_commands() ────┤ Shared              │
│    local_table_query() ──────┤ Functions           │
│    send_reply_headers() ← HTTP-specific            │
│    error_failed_process() ← HTTP-specific          │
│  }                            │                     │
└───────────────────────────────┼─────────────────────┘
                                │
                    ┌───────────▼──────────────┐
                    │  command_execution.py    │
                    │  (Shared Logic)          │
                    │  - prepare_commands()    │
                    │  - execute_al_commands() │
                    │  - local_table_query()   │
                    └───────────┬──────────────┘
                                │
┌───────────────────────────────┼─────────────────────┐
│           MCP (direct_client)                       │
│                                                     │
│  _sync_execute() {            │                     │
│    prepare_commands() ───────┘                     │
│    execute_al_commands() ← Shared                  │
│    local_table_query() ← Shared                    │
│    raise Exception() ← MCP-specific                │
│  }                                                  │
│                                                     │
│  sse_handler._process_message_sync() {             │
│    catch Exception → JSON-RPC error ← MCP-specific │
│  }                                                  │
└─────────────────────────────────────────────────────┘
```

**You're ALREADY sharing the core logic!**

---

## How to Begin Using al_exec with BytesIO

### Answer: You Already Are (Sort Of)

**Current code** uses the same underlying functions that al_exec uses:

| Function | al_exec Uses | direct_client Uses | Shared? |
|----------|--------------|-------------------|---------|
| `prepare_commands()` | ✅ http_server.py:1145 | ✅ direct_client.py:147 | ✅ |
| `execute_al_commands()` | ✅ http_server.py:1157 | ✅ direct_client.py:163 | ✅ |
| `local_table_query()` | ✅ http_server.py:1177 | ✅ direct_client.py:191 | ✅ |
| `send_reply_headers()` | ✅ http_server.py:1151 | ❌ (N/A for MCP) | ❌ |
| `error_failed_process()` | ✅ http_server.py:1164 | ❌ (raises exception) | ❌ |

**The non-shared functions are HTTP-specific and CAN'T be shared!**

### If Integration is Required

**Option 1: Rename for clarity**

```python
# Just rename _sync_execute to make it obvious
def mcp_exec(self, command, headers=None, socket=None):
    """
    MCP version of al_exec.
    Uses same core functions but with MCP error handling.
    """
    # ... current _sync_execute implementation ...
```

**Result:** "We have mcp_exec, parallel to al_exec for clarity"

**Reality:** Same code, different name for better documentation

---

**Option 2: Extract common base**

```python
# New in command_execution.py
def exec_core(status, command, cmd_words, socket, transport_type='http'):
    """
    Core execution logic shared by al_exec and mcp_exec.

    Returns:
        (ret_val, result_data, error_info, execution_metadata)
    """
    commands_list = []
    ret_val, with_wait, content_type, is_select, is_stream, file_data = \
        prepare_commands(status, command, cmd_words, commands_list, ...)

    if ret_val != SUCCESS:
        return (ret_val, None, {"message": "Preparation failed"}, None)

    ret_val = execute_al_commands(status, io_buff, commands_list, None, file_data, socket)

    # Return raw data, let caller format for their transport
    return (ret_val, result_data, error_info, metadata)
```

**HTTP uses it:**

```python
def al_exec(self, status, http_method, command):
    ret_val, result, error, meta = exec_core(status, command, ...)

    if error:
        self.error_failed_process(...)  # ← HTTP-specific
    else:
        self.send_reply_headers(...)     # ← HTTP-specific
```

**MCP uses it:**

```python
def mcp_exec(self, status, command, socket):
    ret_val, result, error, meta = exec_core(status, command, ...)

    if error:
        return {"jsonrpc": "2.0", "error": error}  # ← MCP-specific
    else:
        return {"jsonrpc": "2.0", "result": result}  # ← MCP-specific
```

**Result:** "One function handles both transports"

**Reality:** Core logic shared, but separate error handling still required per transport

---

## Recommended Path Forward

### My Recommendation: Keep Current Approach

**Why:**
1. ✅ Already shares core logic via command_execution
2. ✅ Clean separation of HTTP vs MCP concerns
3. ✅ No refactoring needed
4. ✅ Easy to test and debug
5. ✅ Follows best practice (separation of concerns)

### If Integration is Prioritized

**Path 1: Show This Document**
- Explain that we ALREADY share the important parts
- HTTP-specific code (error_failed_process, send_reply_headers) can't be shared
- Current approach is the "al_exec way" - just adapted for MCP transport

**Path 2: Cosmetic Refactor**
- Rename `_sync_execute` to `mcp_exec`
- Add comments showing parallel with al_exec
- Extract `exec_core()` function to make sharing explicit
- **Result:** Same functionality, looks more "unified"

**Path 3: Full Refactor (Not Recommended)**
- Extract all of al_exec's logic into transport-agnostic functions
- Create transport callback system
- Rewrite both al_exec and mcp_exec to use new system
- **Time:** 2-3 weeks
- **Risk:** High (might break existing HTTP REST)
- **Benefit:** Minimal (already sharing what matters)

---

## The Real Error Handling Differences

### What Errors Look Like

**HTTP REST (al_exec):**
```
← HTTP/1.1 400 Bad Request
← Content-Type: application/json
←
← {
←   "method": "put",
←   "node": "10.0.0.78:7849",
←   "err_code": 155,
←   "err_text": "Failed to execute command",
←   "operator_msg": "Table not found",
←   "local_msg": "SQL error: no such table"
← }
```

**MCP/SSE:**
```
← data: {"jsonrpc":"2.0","id":123,"error":{"code":-32603,"message":"Failed to execute command","data":{"edgelake_code":155,"operator_msg":"Table not found","local_msg":"SQL error: no such table"}}}
← event: message
← id: 456
←
```

**Key Differences:**
1. **Channel:** HTTP response vs SSE event
2. **Format:** EdgeLake custom vs JSON-RPC 2.0
3. **Status:** HTTP 400 vs SSE message with error field
4. **Socket:** Request socket (closes after) vs SSE stream (stays open)

### Can't Be Unified

These are fundamentally different transports. Trying to unify them is like trying to make HTTP and WebSocket use the same error handling - they're different protocols!

---

## Summary

### Can we use al_exec with BytesIO?

**Technical Answer:** Yes, with Approach 2 (extract core logic)

**Practical Answer:** You already are (via command_execution shared functions)

**Honest Answer:** al_exec IS BytesIO-compatible, but the HTTP error handling isn't MCP-compatible

### Where are errors sent?

**HTTP REST:**
- Errors: Same request socket (immediate response)
- Success: Same request socket (immediate response)
- Socket closes after response

**MCP/SSE:**
- Errors: SSE stream socket (queued message)
- Success: SSE stream socket (queued message)
- Socket stays open for next message

**Can't be the same code because they're different channels!**

### What should we actually do?

**Option A (Recommended):** Keep current approach, maybe rename for clarity

**Option B:** Extract `exec_core()` to make sharing explicit, but keep separate error handling

**Option C:** Full refactor (overkill, high risk, minimal benefit)

---

## Next Steps

1. **Decision Point:** Requirements:
   - Same code paths (impossible due to different transports)
   - Same execution logic (already achieved)
   - Just naming clarity (easy, rename _sync_execute to mcp_exec)

2. **If refactor needed:**
   - Create `exec_core()` in command_execution.py
   - Make al_exec use it (with HTTP error handling)
   - Make mcp_exec use it (with JSON-RPC error handling)
   - Test both paths thoroughly

3. **Documentation:**
   - Present this analysis to stakeholders
   - Explain what's already shared
   - Explain why error handling must differ

A specific implementation plan can be created based on project requirements.
