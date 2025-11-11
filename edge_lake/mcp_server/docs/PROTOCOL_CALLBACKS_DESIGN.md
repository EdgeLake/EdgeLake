## Protocol Callbacks Design

# Protocol Callbacks Architecture

## Overview

This design enables sharing command execution logic between HTTP REST and MCP/SSE
(and future transports) by using a callback-based architecture.

**Key Files:**
- `edge_lake/generic/protocol_callbacks.py` - Callback interface and implementations
- `edge_lake/cmd/protocol_exec.py` - Transport-agnostic execution function
- `edge_lake/tcpip/http_server.py` - HTTP usage (modified al_exec)
- `edge_lake/mcp_server/transport/sse_handler.py` - MCP usage

---

## The Problem We're Solving

**Before:** al_exec is tightly coupled to HTTP:

```python
def al_exec(self, status, http_method, command):
    # ... validation ...

    if error:
        self.error_failed_process(...)  # ← HTTP-specific
        self.send_reply_headers(...)    # ← HTTP-specific
        write_to_stream(self.wfile, ...) # ← HTTP socket
```

**After:** protocol_exec uses callbacks:

```python
def protocol_exec(status, command, protocol_callbacks):
    # ... validation ...

    if error:
        protocol_callbacks.send_error(...)  # ← Protocol-agnostic

    protocol_callbacks.send_success(...)     # ← Protocol-agnostic
    socket = protocol_callbacks.get_output_socket()  # ← Any socket type
```

---

## Architecture Diagram

```
┌───────────────────────────────────────────────────────┐
│                  protocol_exec()                      │
│         (Transport-Agnostic Execution)                │
│                                                       │
│  1. Validate command                                  │
│  2. Prepare commands                                  │
│  3. Execute commands                                  │
│  4. Handle results                                    │
│  5. Call protocol_callbacks ─────────────────────┐    │
└───────────────────────────────────────────────────┼────┘
                                                    │
                              ┌─────────────────────┼──────────────────────┐
                              │                     │                      │
                              v                     v                      v
                    ┌─────────────────┐   ┌─────────────────┐   ┌────────────────┐
                    │ HTTPProtocol    │   │ MCPProtocol     │   │ Future:        │
                    │ Callbacks       │   │ Callbacks       │   │ WebSocket, etc │
                    │                 │   │                 │   │                │
                    │ send_error()    │   │ send_error()    │   │ send_error()   │
                    │ → HTTP 400      │   │ → JSON-RPC err  │   │ → WS frame     │
                    │                 │   │                 │   │                │
                    │ send_success()  │   │ send_success()  │   │ send_success() │
                    │ → HTTP 200      │   │ → JSON-RPC res  │   │ → WS frame     │
                    │                 │   │                 │   │                │
                    │ get_output_     │   │ get_output_     │   │ get_output_    │
                    │ socket()        │   │ socket()        │   │ socket()       │
                    │ → self.wfile    │   │ → BytesIO()     │   │ → ws_socket    │
                    └─────────────────┘   └─────────────────┘   └────────────────┘
```

---

## Protocol Callbacks Interface

### Base Class

```python
class ProtocolCallbacks(ABC):
    """Abstract interface for protocol-specific operations"""

    @abstractmethod
    def send_error(self, status, error_code: int, error_message: str,
                   error_details: Optional[Dict] = None):
        """Send error via protocol-specific mechanism"""

    @abstractmethod
    def send_success(self, status, result_data: Any,
                     content_type: Optional[str] = None,
                     metadata: Optional[Dict] = None):
        """Send success via protocol-specific mechanism"""

    @abstractmethod
    def get_output_socket(self, status):
        """Get socket for streaming output"""

    @abstractmethod
    def send_headers(self, status, content_type: str, is_chunked: bool = False):
        """Send protocol headers (if applicable)"""

    @abstractmethod
    def get_protocol_name(self) -> str:
        """Get protocol name for logging"""

    @abstractmethod
    def supports_streaming(self) -> bool:
        """Does protocol support header-then-body streaming?"""
```

---

## HTTP Implementation

### HTTPProtocolCallbacks

```python
class HTTPProtocolCallbacks(ProtocolCallbacks):
    """Wraps HTTP handler for protocol_exec"""

    def __init__(self, http_handler, http_method: str):
        self.http_handler = http_handler
        self.http_method = http_method

    def send_error(self, status, error_code, error_message, error_details):
        """Calls http_handler.error_failed_process()"""
        self.http_handler.error_failed_process(
            status,
            error_details.get('with_wait', False),
            error_code,
            self.http_method,
            error_details.get('into_output'),
            error_details.get('command', ''),
            error_details.get('operator_msg')
        )

    def send_success(self, status, result_data, content_type, metadata):
        """Calls http_handler.write_headers_and_msg()"""
        if self.headers_sent:
            # Streaming case - headers already sent
            utils_io.write_to_stream(status, self.http_handler.wfile, result_data)
        else:
            # Send headers + body together
            self.http_handler.write_headers_and_msg(status, REST_OK, content_type, result_data)

    def get_output_socket(self, status):
        """Return HTTP socket"""
        return self.http_handler.wfile

    def supports_streaming(self) -> bool:
        return True
```

### Usage in HTTP (Modified al_exec)

```python
def al_exec(self, status, http_method, command):
    """HTTP REST endpoint - now uses protocol_exec"""

    # Create HTTP callbacks
    from edge_lake.generic.protocol_callbacks import create_http_callbacks
    protocol_callbacks = create_http_callbacks(self, http_method)

    # Execute using protocol_exec
    from edge_lake.cmd.protocol_exec import protocol_exec
    ret_val = protocol_exec(status, command, protocol_callbacks, http_method, into_output)

    return ret_val
```

**Result:** Minimal changes to al_exec, all HTTP-specific code moved to callbacks

---

## MCP Implementation

### MCPProtocolCallbacks

```python
class MCPProtocolCallbacks(ProtocolCallbacks):
    """Wraps SSE connection for protocol_exec"""

    def __init__(self, sse_connection, json_rpc_id: int):
        self.sse_connection = sse_connection
        self.json_rpc_id = json_rpc_id
        self.output_buffer = BytesIO()

    def send_error(self, status, error_code, error_message, error_details):
        """Queue JSON-RPC error for SSE delivery"""
        error_response = {
            "jsonrpc": "2.0",
            "id": self.json_rpc_id,
            "error": {
                "code": -32603,  # Internal error
                "message": error_message,
                "data": {
                    "edgelake_code": error_code,
                    **(error_details or {})
                }
            }
        }
        self.sse_connection.queue_message('error', error_response)

    def send_success(self, status, result_data, content_type, metadata):
        """Queue JSON-RPC result for SSE delivery"""
        # Format as MCP content
        import json
        if isinstance(result_data, (dict, list)):
            text_content = json.dumps(result_data)
        else:
            text_content = str(result_data) if result_data else ""

        success_response = {
            "jsonrpc": "2.0",
            "id": self.json_rpc_id,
            "result": {
                "content": [{
                    "type": "text",
                    "text": text_content
                }]
            }
        }
        self.sse_connection.queue_message('message', success_response)

    def get_output_socket(self, status):
        """Return BytesIO buffer"""
        return self.output_buffer

    def supports_streaming(self) -> bool:
        """MCP requires complete messages"""
        return False
```

### Usage in MCP (Modified sse_handler)

```python
def _process_message_with_protocol_exec(self, session_id, message):
    """Process MCP message using protocol_exec"""

    # Extract command from MCP message
    tool_name = message['params']['name']
    arguments = message['params']['arguments']
    command = self._build_edgelake_command(tool_name, arguments)

    # Get SSE connection
    with self.connection_lock:
        connection = self.connections.get(session_id)

    if not connection:
        logger.error(f"Session not found: {session_id}")
        return

    # Create MCP callbacks
    from edge_lake.generic.protocol_callbacks import create_mcp_callbacks
    protocol_callbacks = create_mcp_callbacks(connection, message.get('id'))

    # Create status
    from edge_lake.generic import process_status
    status = process_status.ProcessStat()

    # Execute using protocol_exec
    from edge_lake.cmd.protocol_exec import protocol_exec
    ret_val = protocol_exec(status, command, protocol_callbacks)

    # Done! Callbacks already queued response for SSE delivery
    logger.debug(f"Command executed: ret_val={ret_val}")
```

**Result:** MCP now uses same execution logic as HTTP, with clean error handling

---

## Error Flow Comparison

### HTTP Error Flow

```python
protocol_exec()
  └─ Error detected
     └─ protocol_callbacks.send_error(error_code=155, error_message="Table not found")
        └─ HTTPProtocolCallbacks.send_error()
           └─ http_handler.error_failed_process()
              └─ HTTP/1.1 400 Bad Request
                 {
                   "method": "put",
                   "err_code": 155,
                   "err_text": "Table not found",
                   "operator_msg": "...",
                   "local_msg": "..."
                 }
```

**Channel:** HTTP response on request socket (immediate)

### MCP Error Flow

```python
protocol_exec()
  └─ Error detected
     └─ protocol_callbacks.send_error(error_code=155, error_message="Table not found")
        └─ MCPProtocolCallbacks.send_error()
           └─ sse_connection.queue_message('error', {json_rpc_error})
              └─ SSE Event:
                 data: {
                   "jsonrpc": "2.0",
                   "id": 123,
                   "error": {
                     "code": -32603,
                     "message": "Table not found",
                     "data": {"edgelake_code": 155, ...}
                   }
                 }
                 event: error
```

**Channel:** SSE stream on long-lived connection (queued)

---

## Success Flow Comparison

### HTTP Success Flow

```python
protocol_exec()
  └─ Query result ready
     └─ protocol_callbacks.send_success(result_data="{...}")
        └─ HTTPProtocolCallbacks.send_success()
           └─ http_handler.write_headers_and_msg(REST_OK, "text/json", data)
              └─ HTTP/1.1 200 OK
                 Content-Type: text/json

                 {"columns": [...], "rows": [...]}
```

**Channel:** HTTP response on request socket (immediate)

### MCP Success Flow

```python
protocol_exec()
  └─ Query result ready
     └─ protocol_callbacks.send_success(result_data="{...}")
        └─ MCPProtocolCallbacks.send_success()
           └─ sse_connection.queue_message('message', {json_rpc_result})
              └─ SSE Event:
                 data: {
                   "jsonrpc": "2.0",
                   "id": 123,
                   "result": {
                     "content": [{
                       "type": "text",
                       "text": "{\"columns\":[...],\"rows\":[...]}"
                     }]
                   }
                 }
                 event: message
```

**Channel:** SSE stream on long-lived connection (queued)

---

## Socket Handling

### HTTP (Direct Socket)

```python
# HTTP uses real socket
socket = protocol_callbacks.get_output_socket(status)
# → Returns http_handler.wfile (TCP socket)

# Query writes directly to HTTP socket
query_engine.execute(socket)

# Data flows:
Query → wfile → TCP → Client
```

**Behavior:** Streaming to client as query executes

### MCP (Buffered Socket)

```python
# MCP uses BytesIO buffer
socket = protocol_callbacks.get_output_socket(status)
# → Returns BytesIO()

# Query writes to buffer
query_engine.execute(socket)

# Read buffer, wrap in JSON-RPC, queue for SSE
socket.seek(0)
data = socket.read()
protocol_callbacks.send_success(data)

# Data flows:
Query → BytesIO → JSON-RPC wrap → SSE queue → TCP → Client
```

**Behavior:** Buffered, then sent as complete JSON-RPC message

---

## What Gets Shared vs. Different

### Shared (protocol_exec)

```
✅ Command validation
✅ Command parsing (utils_data.str_to_list)
✅ Command preparation (command_execution.prepare_commands)
✅ Command execution (command_execution.execute_al_commands)
✅ Result type handling (query, command, stream, etc.)
✅ Job management (job_scheduler, mutex, etc.)
✅ Local table queries (command_execution.local_table_query)
```

**~70% of execution logic**

### Different (protocol callbacks)

```
❌ Error formatting (HTTP 400 vs JSON-RPC error)
❌ Success formatting (HTTP 200 vs JSON-RPC result)
❌ Socket type (wfile vs BytesIO)
❌ Streaming behavior (direct vs buffered)
❌ Response channel (request socket vs SSE stream)
```

**~30% of transport logic**

---

## Integration Steps

### Step 1: Add protocol_callbacks.py

```bash
# Create callback interface and implementations
edge_lake/generic/protocol_callbacks.py
```

### Step 2: Add protocol_exec.py

```bash
# Create transport-agnostic execution function
edge_lake/cmd/protocol_exec.py
```

### Step 3: Modify HTTP (Optional - for consistency)

```python
# edge_lake/tcpip/http_server.py

def al_exec(self, status, http_method, command):
    """HTTP REST endpoint - now uses protocol_exec"""

    # Import at function level to avoid circular dependency
    from edge_lake.generic.protocol_callbacks import create_http_callbacks
    from edge_lake.cmd.protocol_exec import protocol_exec

    # Create callbacks
    protocol_callbacks = create_http_callbacks(self, http_method)

    # Execute
    into_output = get_value_from_headers(self.al_headers, "into")
    ret_val = protocol_exec(status, command, protocol_callbacks, http_method, into_output)

    return ret_val
```

**Note:** This is optional. You can keep al_exec as-is and only use protocol_exec for MCP.

### Step 4: Modify MCP Handler

```python
# edge_lake/mcp_server/transport/sse_handler.py

def _process_message_sync(self, session_id: str, message: Dict[str, Any], socket=None):
    """Process MCP message using protocol_exec"""

    try:
        # Extract EdgeLake command from MCP message
        tool_name = message.get('params', {}).get('name')
        arguments = message.get('params', {}).get('arguments', {})

        # Build command (move from tool_executor)
        command = self._build_edgelake_command(tool_name, arguments)

        # Get SSE connection
        with self.connection_lock:
            connection = self.connections.get(session_id)

        if not connection:
            logger.error(f"Session not found: {session_id}")
            return

        # Create MCP callbacks
        from edge_lake.generic.protocol_callbacks import create_mcp_callbacks
        protocol_callbacks = create_mcp_callbacks(connection, message.get('id'))

        # Create status
        from edge_lake.generic import process_status
        status = process_status.ProcessStat()

        # Execute
        from edge_lake.cmd.protocol_exec import protocol_exec
        ret_val = protocol_exec(status, command, protocol_callbacks)

        # Response already queued by callbacks
        logger.debug(f"Command completed: {ret_val}")

    except Exception as e:
        logger.error(f"Error in protocol_exec: {e}", exc_info=True)

        # Fallback error handling
        with self.connection_lock:
            connection = self.connections.get(session_id)

        if connection:
            error_response = {
                "jsonrpc": "2.0",
                "id": message.get('id'),
                "error": {
                    "code": -32603,
                    "message": f"Internal error: {str(e)}"
                }
            }
            connection.queue_message('error', error_response)
```

### Step 5: Move Command Building Logic

```python
def _build_edgelake_command(self, tool_name: str, arguments: Dict) -> str:
    """
    Build EdgeLake command from MCP tool call.

    Moved from tool_executor to sse_handler for direct protocol_exec usage.
    """

    if tool_name == 'query':
        database = arguments.get('database')
        query = arguments.get('query')
        destination = arguments.get('destination', 'network')

        # Build: sql dbname format=mcp and destination=network SELECT ...
        command = f'sql {database} format=json and destination={destination} {query}'

    elif tool_name == 'list_database_schema':
        command = 'blockchain get table where format=mcp'

    elif tool_name == 'server_info':
        command = 'get version where format=mcp'

    # ... etc for other tools

    return command
```

---

## Testing

### Test HTTP Path

```python
def test_http_with_protocol_exec():
    """Test HTTP using protocol_exec"""

    # Mock HTTP handler
    class MockHTTPHandler:
        def __init__(self):
            self.wfile = BytesIO()
            self.error_called = False

        def error_failed_process(self, *args):
            self.error_called = True

        def write_headers_and_msg(self, status, code, content_type, data):
            self.wfile.write(data.encode('utf-8'))

    # Create callbacks
    handler = MockHTTPHandler()
    callbacks = HTTPProtocolCallbacks(handler, "get")

    # Execute
    status = ProcessStat()
    ret_val = protocol_exec(status, "get status", callbacks)

    # Verify
    assert ret_val == process_status.SUCCESS
    assert not handler.error_called
    assert len(handler.wfile.getvalue()) > 0
```

### Test MCP Path

```python
def test_mcp_with_protocol_exec():
    """Test MCP using protocol_exec"""

    # Mock SSE connection
    class MockSSEConnection:
        def __init__(self):
            self.messages = []

        def queue_message(self, event_type, data):
            self.messages.append((event_type, data))

    # Create callbacks
    connection = MockSSEConnection()
    callbacks = MCPProtocolCallbacks(connection, json_rpc_id=123)

    # Execute
    status = ProcessStat()
    ret_val = protocol_exec(status, "get status", callbacks)

    # Verify
    assert ret_val == process_status.SUCCESS
    assert len(connection.messages) == 1
    assert connection.messages[0][0] == 'message'
    assert connection.messages[0][1]['jsonrpc'] == '2.0'
    assert connection.messages[0][1]['id'] == 123
    assert 'result' in connection.messages[0][1]
```

---

## Benefits

### Code Reuse
- **70% of execution logic shared** between HTTP and MCP
- No duplication of validation, parsing, execution, job management

### Clean Separation
- **Transport logic isolated** in callback implementations
- Easy to understand: "This is HTTP stuff, this is MCP stuff"

### Maintainability
- **Single source of truth** for execution logic
- Bug fixes in protocol_exec benefit both HTTP and MCP

### Extensibility
- **Easy to add new transports** (WebSocket, gRPC, stdio)
- Just implement ProtocolCallbacks interface

### Testability
- **Mock callbacks** for unit testing
- Test execution logic without HTTP or MCP dependencies

---

## Migration Path

### Phase 1: MCP Only (Recommended)
- Keep al_exec as-is (no changes to HTTP)
- Use protocol_exec for MCP only
- Low risk, easy to test

### Phase 2: HTTP + MCP (Optional)
- Refactor al_exec to use protocol_exec
- Both transports use same code path
- Higher confidence in shared logic

### Phase 3: Future Transports
- Add WebSocketProtocolCallbacks
- Add StdioProtocolCallbacks
- Minimal work per new transport

---

## Summary

### What We Built

1. **ProtocolCallbacks** - Abstract interface for transport operations
2. **HTTPProtocolCallbacks** - HTTP-specific implementation
3. **MCPProtocolCallbacks** - MCP-specific implementation
4. **protocol_exec()** - Transport-agnostic execution function

### What Gets Shared

- Command validation, parsing, preparation
- Command execution via command_execution module
- Job management, query handling, result formatting
- **70% of execution logic**

### What Stays Different

- Error response formatting (HTTP 400 vs JSON-RPC error)
- Success response formatting (HTTP 200 vs JSON-RPC result)
- Socket type (wfile vs BytesIO)
- Response channel (request socket vs SSE stream)
- **30% of transport logic**

### Result

**Clean architecture that shares execution logic while respecting transport differences!**

The team gets code reuse and clean separation of concerns. A win for maintainability!
