# SSE and MCP Protocol Guide

## Overview

This document explains how Server-Sent Events (SSE) and Model Context Protocol (MCP) use and extend HTTP, including their exchange formats and protocol details.

## Table of Contents

1. [Protocol Stack](#protocol-stack)
2. [HTTP Fundamentals](#http-fundamentals)
3. [SSE Protocol](#sse-protocol)
4. [MCP Protocol](#mcp-protocol)
5. [EdgeLake Implementation](#edgelake-implementation)
6. [Protocol Comparison](#protocol-comparison)

---

## Protocol Stack

```
┌─────────────────────────────────────────┐
│   MCP (Model Context Protocol)         │  ← Application Layer (JSON-RPC 2.0)
├─────────────────────────────────────────┤
│   SSE (Server-Sent Events)              │  ← Transport Layer (text/event-stream)
├─────────────────────────────────────────┤
│   HTTP/1.1                              │  ← Network Protocol
├─────────────────────────────────────────┤
│   TCP/IP                                │  ← Transport Protocol
└─────────────────────────────────────────┘
```

### Key Relationships

- **MCP** = Application protocol (defines messages, methods, tools)
- **SSE** = Transport mechanism (defines how messages are delivered)
- **HTTP** = Foundation protocol (provides connection, headers, status codes)

---

## HTTP Fundamentals

### Standard HTTP Request-Response

**How Normal HTTP Works:**

```
Client                          Server
  │                               │
  ├──── HTTP Request ─────────>   │
  │     GET /api/data             │
  │     Headers...                │
  │                               │
  │   <───── HTTP Response ──────┤
  │     200 OK                    │
  │     Headers...                │
  │     Body: {...}               │
  │                               │
  └─── Connection Closes ─────────┘
```

**Characteristics:**
- One request → One response
- Connection typically closes after response
- Client initiates all communication
- Server cannot push data to client

### HTTP Long-Polling (Alternative Approach)

```
Client                          Server
  │                               │
  ├──── HTTP Request ─────────>   │
  │     GET /poll                 │
  │                               │
  │                            (waits)
  │                               │
  │   <───── Response ───────────┤
  │     200 OK                    │
  │     Body: {...}               │
  │                               │
  ├──── New Request ──────────>   │  ← Client immediately reconnects
  │     GET /poll                 │
```

**Characteristics:**
- Simulates push with repeated polling
- High overhead (new connection each time)
- Latency on each reconnection
- Not used by MCP

---

## SSE Protocol

### What SSE Uses from HTTP

SSE is **built on top of HTTP** but extends it for server-to-client streaming.

#### Standard HTTP Components Used

1. **HTTP Request (Client → Server)**
   ```http
   GET /mcp/sse HTTP/1.1
   Host: localhost:32049
   Accept: text/event-stream
   ```

2. **HTTP Response Headers (Server → Client)**
   ```http
   HTTP/1.1 200 OK
   Content-Type: text/event-stream
   Cache-Control: no-cache
   Connection: keep-alive
   Access-Control-Allow-Origin: *
   X-Accel-Buffering: no
   ```

### What SSE Extends/Changes

#### 1. **Connection Persistence**

**Standard HTTP:**
- Response sent → Connection closes

**SSE:**
- Response headers sent → **Connection stays open indefinitely**
- Server can send data at any time
- Connection maintained with keepalive pings

```
Client                          Server
  │                               │
  ├──── GET /sse ─────────────>   │
  │                               │
  │   <───── 200 OK ─────────────┤
  │   Content-Type: text/event-   │
  │              stream            │
  │   (headers end)               │
  │                               │
  │   <───── Event 1 ────────────┤  ← Server pushes anytime
  │                               │
  │   <───── Event 2 ────────────┤  ← Server pushes anytime
  │                               │
  │   <───── Event 3 ────────────┤  ← Server pushes anytime
  │                               │
  └───────────────────────────────┘  (connection stays open)
```

#### 2. **Event Stream Format**

**Standard HTTP Body:**
```
Regular text or JSON all at once
```

**SSE Body (text/event-stream):**
```
data: First event data
event: message
id: 1

data: Second event data
event: message
id: 2

:keepalive

data: Third event data
event: error
id: 3

```

**SSE Event Structure:**
```
Field Format: field_name: field_value\n
Terminator:   \n\n (double newline)
```

### SSE Event Fields

| Field | Purpose | Required | Example |
|-------|---------|----------|---------|
| `data:` | Event payload (can span multiple lines) | Yes | `data: {"id": 123}` |
| `event:` | Event type (default: "message") | No | `event: error` |
| `id:` | Event ID (for reconnection) | No | `id: 42` |
| `retry:` | Reconnection time (ms) | No | `retry: 3000` |
| `:comment` | Comment/keepalive | No | `:keepalive` |

### SSE Keepalive Mechanism

**Problem:** HTTP proxies/firewalls may close idle connections.

**Solution:** Send periodic comments (ignored by client, keeps connection alive)

```
:keepalive

:ping

:still here
```

**EdgeLake Implementation:**
```python
def send_keepalive(self) -> bool:
    # Send SSE comment (keepalive)
    self.handler.wfile.write(b":keepalive\n\n")
    self.handler.wfile.flush()
```

**Location:** `edge_lake/mcp_server/transport/sse_handler.py:131-149`

**Interval:** 30 seconds (configurable)

### SSE Complete Example

**Request:**
```http
GET /mcp/sse HTTP/1.1
Host: localhost:32049
Accept: text/event-stream
```

**Response:**
```http
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive

data: /mcp/messages/abc-123-def-456
event: endpoint
id: 0

data: {"jsonrpc":"2.0","id":1,"result":{"status":"ok"}}
event: message
id: 1

:keepalive

data: {"jsonrpc":"2.0","id":2,"error":{"code":-32603,"message":"Error"}}
event: error
id: 2

```

**Key Points:**
- First event tells client where to POST messages
- Subsequent events deliver MCP responses
- Comments maintain connection
- Connection stays open until client disconnects or timeout

---

## MCP Protocol

### MCP Architecture

MCP uses **bidirectional communication** over SSE, which is normally unidirectional.

```
┌──────────────────────────────────────────────────┐
│                    MCP Client                    │
│              (Claude Desktop, etc.)              │
└────────────┬─────────────────────────┬───────────┘
             │                         │
       POST  │                         │  GET (SSE Stream)
    Requests │                         │  Events
             │                         │
             v                         │
┌────────────────────────┐             │
│   POST /mcp/messages   │             │
│   /{session_id}        │             │
│                        │             │
│   JSON-RPC Request ────┼─────────────┤
└────────────────────────┘             │
                                       │
                              ┌────────┴──────────┐
                              │  GET /mcp/sse     │
                              │                   │
                              │  SSE Stream       │
                              │  (JSON-RPC        │
                              │   Responses)      │
                              └───────────────────┘
```

### How MCP Achieves Bidirectional Communication

**Challenge:** SSE is server → client only. How does client send to server?

**Solution:** MCP uses **two channels**:

1. **Downstream (Server → Client): SSE Stream**
   - GET /mcp/sse
   - Long-lived connection
   - Server pushes responses as events

2. **Upstream (Client → Server): HTTP POST**
   - POST /mcp/messages/{session_id}
   - Short-lived request/response
   - Client sends JSON-RPC requests
   - Server immediately returns "202 Accepted"
   - Actual response sent via SSE channel

### MCP Connection Lifecycle

#### Phase 1: Establish SSE Connection

```http
Client → Server:
-----------------
GET /mcp/sse HTTP/1.1
Host: localhost:32049
Accept: text/event-stream

Server → Client:
-----------------
HTTP/1.1 200 OK
Content-Type: text/event-stream
Connection: keep-alive

data: /mcp/messages/abc-123-def-456
event: endpoint
id: 0

```

**What Happens:**
1. Client requests SSE connection
2. Server generates unique session_id
3. Server sends "endpoint" event with POST URL
4. Connection stays open for responses

**EdgeLake Code:**
```python
# Generate unique session ID
session_id = str(uuid.uuid4())

# Send SSE headers
handler.send_response(200, "OK")
handler.send_header('Content-Type', 'text/event-stream')
handler.send_header('Cache-Control', 'no-cache')
handler.send_header('Connection', 'keep-alive')
handler.end_headers()

# Send endpoint event (tells client where to POST)
endpoint_path = f'/mcp/messages/{session_id}'
handler.wfile.write(f"data: {endpoint_path}\n".encode('utf-8'))
handler.wfile.write(b"event: endpoint\n")
handler.wfile.write(b"id: 0\n\n")
handler.wfile.flush()
```

**Location:** `edge_lake/mcp_server/transport/sse_handler.py:198-246`

#### Phase 2: MCP Handshake (Initialize)

```http
Client → Server (POST):
------------------------
POST /mcp/messages/abc-123-def-456 HTTP/1.1
Host: localhost:32049
Content-Type: application/json
Content-Length: 67

{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "initialize",
  "params": {
    "protocolVersion": "2024-11-05",
    "capabilities": {},
    "clientInfo": {
      "name": "claude-desktop",
      "version": "1.0.0"
    }
  }
}

Server → Client (HTTP Response - immediate):
----------------------------------------------
HTTP/1.1 202 Accepted
Content-Type: application/json

{
  "status": "accepted",
  "message": "Request will be processed and response sent via SSE"
}

Server → Client (SSE Event - actual response):
------------------------------------------------
data: {"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"serverInfo":{"name":"edgelake-mcp-server","version":"0.3.0"}}}
event: message
id: 1

```

**What Happens:**
1. Client POSTs JSON-RPC "initialize" request
2. Server immediately returns "202 Accepted" (closes HTTP POST)
3. Server processes request
4. Server sends actual JSON-RPC response via SSE

#### Phase 3: List Tools

```http
Client → Server (POST):
------------------------
POST /mcp/messages/abc-123-def-456 HTTP/1.1
Content-Type: application/json

{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "tools/list"
}

Server → Client (HTTP - immediate):
-------------------------------------
HTTP/1.1 202 Accepted

Server → Client (SSE - actual response):
------------------------------------------
data: {"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"query","description":"Execute SQL query","inputSchema":{"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}}]}}
event: message
id: 2

```

#### Phase 4: Call Tool

```http
Client → Server (POST):
------------------------
POST /mcp/messages/abc-123-def-456 HTTP/1.1
Content-Type: application/json

{
  "jsonrpc": "2.0",
  "id": 3,
  "method": "tools/call",
  "params": {
    "name": "query",
    "arguments": {
      "query": "SELECT * FROM my_table LIMIT 5",
      "database": "lsl_demo"
    }
  }
}

Server → Client (HTTP - immediate):
-------------------------------------
HTTP/1.1 202 Accepted

Server → Client (SSE - actual response):
------------------------------------------
data: {"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"{\"columns\":[\"id\",\"name\"],\"rows\":[[1,\"Alice\"],[2,\"Bob\"]]}"}]}}
event: message
id: 3

```

### MCP JSON-RPC Format

MCP uses **JSON-RPC 2.0** for all messages.

#### Request Format

```json
{
  "jsonrpc": "2.0",          // Protocol version (required)
  "id": 123,                 // Request ID (required for requests)
  "method": "tools/call",    // Method name (required)
  "params": {                // Parameters (optional)
    "name": "query",
    "arguments": {...}
  }
}
```

#### Success Response Format

```json
{
  "jsonrpc": "2.0",          // Protocol version (required)
  "id": 123,                 // Matches request ID (required)
  "result": {                // Result object (required for success)
    "content": [...]
  }
}
```

#### Error Response Format

```json
{
  "jsonrpc": "2.0",          // Protocol version (required)
  "id": 123,                 // Matches request ID (required)
  "error": {                 // Error object (required for errors)
    "code": -32603,          // Error code (integer)
    "message": "Internal error",  // Error message (string)
    "data": {...}            // Additional data (optional)
  }
}
```

### MCP Methods

| Method | Purpose | Returns |
|--------|---------|---------|
| `initialize` | Handshake, exchange capabilities | Server info, protocol version |
| `tools/list` | Get list of available tools | Array of tool definitions |
| `tools/call` | Execute a tool | Tool result (content array) |
| `notifications/list` | Get notification types | Notification definitions |
| `resources/list` | Get available resources | Resource definitions |

### MCP Error Codes (JSON-RPC 2.0)

| Code | Meaning | When Used |
|------|---------|-----------|
| -32700 | Parse error | Invalid JSON received |
| -32600 | Invalid Request | Missing required fields |
| -32601 | Method not found | Unknown method name |
| -32602 | Invalid params | Wrong parameter types/values |
| -32603 | Internal error | Server error during execution |

**EdgeLake Error Handling:**
```python
def process_message(self, message: Dict[str, Any], socket=None) -> Dict[str, Any]:
    try:
        method = message.get('method')
        # ... process method ...

        return {
            "jsonrpc": "2.0",
            "id": message.get('id'),
            "result": {...}
        }

    except Exception as e:
        return {
            "jsonrpc": "2.0",
            "id": message.get('id'),
            "error": {
                "code": -32603,
                "message": f"Internal error: {str(e)}"
            }
        }
```

**Location:** `edge_lake/mcp_server/mcp_server.py:110-196`

---

## EdgeLake Implementation

### Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│              http_server.py (HTTP Layer)            │
│                                                     │
│  ┌───────────────────────────────────────────────┐ │
│  │  ChunkedHTTPRequestHandler                    │ │
│  │                                               │ │
│  │  do_GET():                                    │ │
│  │    if path == "/mcp/sse":                     │ │
│  │      → sse_handler.handle_sse_endpoint()      │ │
│  │                                               │ │
│  │  do_POST():                                   │ │
│  │    if path.startswith("/mcp/messages/"):      │ │
│  │      → sse_handler.handle_messages_endpoint() │ │
│  └───────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────┘
                        │
                        v
┌─────────────────────────────────────────────────────┐
│        sse_handler.py (SSE Transport Layer)         │
│                                                     │
│  ┌─────────────────────────────────────────────┐   │
│  │  SSETransport                               │   │
│  │                                             │   │
│  │  handle_sse_endpoint():                     │   │
│  │    - Generate session_id                    │   │
│  │    - Send SSE headers                       │   │
│  │    - Send "endpoint" event                  │   │
│  │    - Keep connection open                   │   │
│  │                                             │   │
│  │  handle_messages_endpoint():                │   │
│  │    - Parse JSON-RPC request                 │   │
│  │    - Return 202 Accepted                    │   │
│  │    - → mcp_server.process_message()         │   │
│  │    - Queue response for SSE                 │   │
│  └─────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
                        │
                        v
┌─────────────────────────────────────────────────────┐
│        mcp_server.py (MCP Protocol Layer)           │
│                                                     │
│  ┌─────────────────────────────────────────────┐   │
│  │  MCPServer                                  │   │
│  │                                             │   │
│  │  process_message():                         │   │
│  │    - Route by method                        │   │
│  │    - "initialize" → handshake               │   │
│  │    - "tools/list" → list tools              │   │
│  │    - "tools/call" → execute tool            │   │
│  │    - Return JSON-RPC response               │   │
│  └─────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
                        │
                        v
┌─────────────────────────────────────────────────────┐
│         tool_executor.py (EdgeLake Layer)           │
│                                                     │
│  ┌─────────────────────────────────────────────┐   │
│  │  ToolExecutor                               │   │
│  │                                             │   │
│  │  execute_tool():                            │   │
│  │    - Build EdgeLake command                 │   │
│  │    - → direct_client.execute_command()      │   │
│  │    - Format result as MCP content           │   │
│  └─────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
                        │
                        v
┌─────────────────────────────────────────────────────┐
│       direct_client.py (EdgeLake Executor)          │
│                                                     │
│  ┌─────────────────────────────────────────────┐   │
│  │  EdgeLakeDirectClient                       │   │
│  │                                             │   │
│  │  execute_command():                         │   │
│  │    - → _sync_execute()                      │   │
│  │    - Uses command_execution module          │   │
│  │    - Returns result                         │   │
│  └─────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

### Key Implementation Details

#### 1. SSE Connection Management

**Location:** `edge_lake/mcp_server/transport/sse_handler.py:39-154`

```python
class SSEConnection:
    """Represents an active SSE connection to a client."""

    def __init__(self, session_id: str, handler):
        self.session_id = session_id
        self.handler = handler              # HTTP handler (for wfile)
        self.message_queue = Queue()        # Queue for outgoing messages
        self.last_activity = time.time()    # For timeout detection
        self.connected = True               # Connection state
        self.message_counter = 0            # Event ID counter

    def send_event(self, event_type: str, data: Dict, event_id: int = None):
        """Send SSE event to client."""
        event_data = json.dumps(data)

        # Write SSE event format
        self.handler.wfile.write(f"data: {event_data}\n".encode('utf-8'))
        self.handler.wfile.write(f"event: {event_type}\n".encode('utf-8'))
        self.handler.wfile.write(f"id: {event_id}\n\n".encode('utf-8'))
        self.handler.wfile.flush()
```

**Key Points:**
- Each connection has unique session_id
- Uses HTTP handler's wfile (socket) for writing events
- Message queue decouples POST processing from SSE sending
- Keepalive thread maintains all connections

#### 2. Bidirectional Message Flow

**POST Handler (Client → Server):**

**Location:** `edge_lake/mcp_server/transport/sse_handler.py:288-366`

```python
def handle_messages_endpoint(self, handler) -> bool:
    """Handle POST /mcp/messages/{session_id}"""

    # Extract session_id from path
    session_id = handler.path.split('/')[3]

    # Read JSON-RPC message from POST body
    content_length = int(handler.headers.get('Content-Length', 0))
    body = handler.rfile.read(content_length)
    message = json.loads(body.decode('utf-8'))

    # Send immediate HTTP response (202 Accepted)
    handler.send_response(202, "Accepted")
    handler.send_header('Content-Type', 'application/json')
    handler.end_headers()
    handler.wfile.write(json.dumps({
        'status': 'accepted',
        'message': 'Request will be processed and response sent via SSE'
    }).encode('utf-8'))

    # Process message (synchronous)
    socket_buffer = BytesIO()  # For capturing EdgeLake output
    response = self.mcp_server.process_message(message, socket_buffer)

    # Queue response for SSE delivery
    connection.queue_message('message', response)
```

**SSE Handler (Server → Client):**

**Location:** `edge_lake/mcp_server/transport/sse_handler.py:254-274`

```python
# Block until connection closes
while connection.connected:
    try:
        # Check if there are queued messages
        message = connection.message_queue.get(timeout=1.0)

        # Send queued message via SSE
        connection.send_event(
            message['event'],
            message['data'],
            message['id']
        )

    except Empty:
        # No messages, continue loop
        pass
```

**Flow:**
1. POST request arrives with JSON-RPC message
2. Server immediately returns "202 Accepted"
3. POST connection closes
4. Server processes message
5. Server queues JSON-RPC response
6. SSE loop picks up queued message
7. SSE event sent to client via open stream

#### 3. MCP Message Processing

**Location:** `edge_lake/mcp_server/mcp_server.py:110-196`

```python
def process_message(self, message: Dict[str, Any], socket=None) -> Dict[str, Any]:
    """Process incoming MCP message (JSON-RPC)."""

    method = message.get('method')
    params = message.get('params', {})
    msg_id = message.get('id')

    # Route by method
    if method == 'initialize':
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {
                    "name": "edgelake-mcp-server",
                    "version": __version__
                }
            }
        }

    elif method == 'tools/list':
        tools = asyncio.run(self._list_tools())
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {"tools": tools}
        }

    elif method == 'tools/call':
        tool_name = params.get('name')
        arguments = params.get('arguments', {})

        content = asyncio.run(self._call_tool(tool_name, arguments, socket))
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {"content": content}
        }

    else:
        # Unknown method
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "error": {
                "code": -32601,
                "message": f"Method not found: {method}"
            }
        }
```

---

## Protocol Comparison

### Standard HTTP vs SSE vs MCP

| Feature | Standard HTTP | SSE | MCP over SSE |
|---------|---------------|-----|--------------|
| **Connection** | Request → Response → Close | Request → Response headers → Open indefinitely | GET /sse (open) + POST /messages (short) |
| **Direction** | Bidirectional (sequential) | Unidirectional (server → client) | Bidirectional (SSE + POST) |
| **Data Flow** | Request/Response pairs | Server pushes events | Client POST + Server SSE events |
| **Content-Type** | varies (application/json, text/html, etc.) | `text/event-stream` | POST: `application/json`, SSE: `text/event-stream` |
| **Message Format** | Any (JSON, XML, HTML, etc.) | SSE event format (`data:`, `event:`, `id:`) | JSON-RPC 2.0 over SSE events |
| **Keepalive** | TCP keepalive (low-level) | SSE comments (`:keepalive`) | SSE comments + timeout detection |
| **Error Handling** | HTTP status codes (400, 500) | SSE error events | JSON-RPC error responses |
| **Use Case** | REST APIs, web pages | Real-time updates, notifications | AI agent tool execution |

### What Each Protocol Adds

#### HTTP → SSE

**HTTP Provides:**
- TCP connection
- Request/response structure
- Status codes (200, 404, 500)
- Headers (Content-Type, Cache-Control)

**SSE Adds:**
- Long-lived connections
- Server push capability
- Event stream format
- Event types and IDs
- Automatic reconnection (client-side)

#### SSE → MCP

**SSE Provides:**
- Server → Client streaming
- Event delivery mechanism
- Connection persistence

**MCP Adds:**
- Bidirectional communication (POST for client → server)
- JSON-RPC 2.0 message format
- Tool/resource/notification framework
- Capability negotiation (initialize)
- Standardized error codes

### HTTP Methods Usage

| Endpoint | HTTP Method | Purpose | Response Type |
|----------|-------------|---------|---------------|
| `/mcp/sse` | GET | Establish SSE connection | `text/event-stream` (streaming) |
| `/mcp/messages/{session_id}` | POST | Send JSON-RPC request | `application/json` (202 Accepted) |

### Header Comparison

#### Standard HTTP GET Response

```http
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 145

{"result": "data"}
```

#### SSE GET Response

```http
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
Transfer-Encoding: chunked

data: event content
event: message
id: 1

```

**Key Differences:**
- `Content-Type: text/event-stream` (not application/json)
- `Cache-Control: no-cache` (prevent proxy caching)
- `Connection: keep-alive` (don't close)
- `Transfer-Encoding: chunked` (stream in chunks)
- No `Content-Length` (unknown length, streaming)

#### MCP POST Request

```http
POST /mcp/messages/abc-123 HTTP/1.1
Host: localhost:32049
Content-Type: application/json
Content-Length: 67

{"jsonrpc":"2.0","id":1,"method":"tools/list"}
```

**Standard HTTP POST** (no MCP-specific headers)

---

## Summary

### What SSE Uses from HTTP

✅ **Uses:**
- TCP connection
- GET request for initial handshake
- HTTP status code 200
- Standard headers (Host, Connection, etc.)
- HTTP/1.1 protocol

❌ **Does NOT Use:**
- Request/response pairs (connection stays open)
- Content-Length (streaming, unknown length)
- Standard body format (uses event stream format)

### What SSE Extends/Changes

✨ **Extends:**
- Connection persistence (stays open)
- Server push (events at any time)
- Special Content-Type (`text/event-stream`)
- Event stream format (`data:`, `event:`, `id:`, comments)
- Keepalive mechanism (comments)

### What MCP Adds to SSE

✨ **Adds:**
- Bidirectional communication (POST + SSE)
- JSON-RPC 2.0 message protocol
- Tool/resource framework
- Capability negotiation
- Session management (session_id)
- Immediate acknowledgment (202 Accepted)

### Protocol Layers

```
┌─────────────────────────────────────────────┐
│  MCP: Application protocol                  │
│  - Methods: initialize, tools/list, etc.    │
│  - JSON-RPC 2.0 format                      │
│  - Bidirectional via POST + SSE             │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  SSE: Transport mechanism                   │
│  - text/event-stream Content-Type           │
│  - Event format: data/event/id              │
│  - Server push capability                   │
│  - Keepalive comments                       │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  HTTP: Foundation protocol                  │
│  - GET /mcp/sse (establish SSE)             │
│  - POST /mcp/messages/{id} (send messages)  │
│  - Status codes, headers, TCP connection    │
└─────────────────────────────────────────────┘
```

### Key Takeaways

1. **SSE is HTTP with extensions** - Uses HTTP for initial connection, extends for streaming
2. **MCP is application protocol** - Defines message format and semantics
3. **Bidirectional via dual channels** - POST for client→server, SSE for server→client
4. **Standard compliance** - JSON-RPC 2.0, SSE spec, HTTP/1.1
5. **EdgeLake integration** - Bridges MCP protocol to EdgeLake commands via direct_client

---

## References

- **SSE Specification:** https://html.spec.whatwg.org/multipage/server-sent-events.html
- **MCP Specification:** https://spec.modelcontextprotocol.io/
- **JSON-RPC 2.0:** https://www.jsonrpc.org/specification
- **EdgeLake MCP Implementation:** `edge_lake/mcp_server/`
