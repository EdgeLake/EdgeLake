# MCP Server Threading Refactoring Plan

**Status**: In Progress
**Date Started**: 2025-10-31
**Goal**: Optimize threading architecture, eliminate unnecessary threads, integrate message_server for large response streaming
**Context**: Weekend refactoring to ensure production-ready architecture

---

## Executive Summary

The current MCP SSE implementation has threading inefficiencies that waste HTTP worker threads and create unnecessary overhead. This document outlines a phased refactoring approach to:

1. **Remove unnecessary threading** (DirectClient pool, per-message threads)
2. **Fix SSE connection blocking** (single dedicated SSE manager thread)
3. **Integrate message_server** for large response streaming (>10MB)

**Key Insight**: MCP SSE protocol requires streaming + keepalives, but current implementation blocks HTTP workers and spawns unnecessary threads.

---

## Current Architecture Problems

### Threading Analysis

| Component | Location | Threads Created | Problem | Impact |
|-----------|----------|-----------------|---------|--------|
| **HTTP Workers Pool** | `http_server.py:217` | 5 (configurable) | ✅ Needed for all HTTP | Shared resource |
| **SSE Connection Loop** | `sse_handler.py:245` | Blocks HTTP worker | **HTTP worker blocked indefinitely** | With 3 SSE clients, only 2 workers left for REST! |
| **Message Processing** | `sse_handler.py:400` | 1 per MCP message | Unnecessary - only for 202 response | Overhead + resource waste |
| **DirectClient ThreadPool** | `direct_client.py:36` | N (configurable) | Unnecessary - `member_cmd.process_cmd()` is sync | Memory + context switch overhead |
| **Keepalive Thread** | `sse_handler.py:189` | 1 (daemon) | ✅ **NEEDED** for SSE protocol | Required for pings during long queries |

### Current Flow (INEFFICIENT):

```
[Client] → GET /mcp/sse
              ↓
[HTTP Worker Thread #1] ← BLOCKS HERE in while loop (line 245)
              ↓
    SSE connection established
              ↓
[Client] → POST /mcp/messages/{session_id}
              ↓
[HTTP Worker Thread #2] → Spawns new thread (line 400) ← UNNECESSARY
              ↓                    ↓
    Returns 202          [Message Thread] → DirectClient.executor.submit() ← UNNECESSARY
                                   ↓                    ↓
                              ThreadPool    [Worker Thread] → member_cmd.process_cmd()
                                                              (synchronous)
```

**Problem**:
- HTTP Worker #1 is **permanently blocked** on SSE connection
- HTTP Worker #2 spawns **unnecessary message thread** just to return 202
- DirectClient **unnecessarily uses ThreadPool** when `process_cmd()` is already synchronous
- **Result**: Massive thread waste for simple synchronous operations!

---

## MCP SSE Protocol Requirements

### Official Spec (2024-11-05)

**Endpoints**:
1. **GET `/mcp/sse`** - Establish SSE connection, return session_id to client
2. **POST `/mcp/messages/{session_id}`** - Receive JSON-RPC messages from client

**Streaming Requirements**:
- ✅ **Unidirectional streaming** (server → client via SSE)
- ✅ **Keepalives required** during long operations (prevent connection timeout)
- ✅ **Event format**: `data: {json}\nevent: {type}\nid: {id}\n\n`
- ✅ **Bidirectional communication** (client sends via POST, server responds via SSE)

**Critical Design Constraints**:
- **Streaming is fundamental** - results must flow as they arrive (not batch)
- **Keepalives are mandatory** - queries can run for minutes
- **Large responses need chunking** - 10MB+ results cannot fit in memory

**Status**: SSE was **deprecated in MCP spec 2025-03-26** in favor of Streamable HTTP, but we're using 2024-11-05 spec which requires SSE.

---

## Optimal Architecture (TARGET)

### Thread Model (Minimal & Efficient)

```
┌─────────────────────────────────────────────────────────┐
│ HTTP Server (http_server.py)                            │
│  - Workers Pool (5 threads) - handles ALL HTTP requests │
│  - NO threads blocked on SSE connections                │
│  - Workers handle requests and return to pool           │
└──────────────┬──────────────────────────────────────────┘
               │
      ┌────────┴────────┐
      │                 │
┌─────▼─────┐   ┌──────▼────────┐
│ GET /mcp  │   │ POST /mcp/msg │
│ /sse      │   │               │
│  - Register│   │  - Parse JSON │
│    with   │   │  - Call MCP   │
│    manager│   │  - Queue resp │
│  - Return │   │  - Return 202 │
└─────┬─────┘   └──────┬────────┘
      │                │
      └────────┬───────┘
               │
    ┌──────────▼──────────────────────────────────────────┐
    │ SSEConnectionManager (SINGLE DEDICATED THREAD)      │
    │  - Uses select()/epoll for non-blocking I/O        │
    │  - Manages ALL active SSE connections              │
    │  - Sends keepalives (every 30s)                    │
    │  - Streams queued responses                        │
    │  - Detects client disconnects                      │
    │  - Thread name: "MCP-SSE-Manager"                  │
    └──────────┬──────────────────────────────────────────┘
               │
    ┌──────────▼──────────────┐
    │ MCP Server              │
    │  - list_tools()         │ ← Called directly in HTTP worker thread
    │  - call_tool()          │   (synchronous, no threading)
    │  - process_message()    │
    │  - NO threading here    │
    └──────────┬──────────────┘
               │
    ┌──────────▼──────────────┐
    │ Direct Client           │
    │  - NO ThreadPool        │ ← Calls member_cmd directly
    │  - execute_command()    │   (synchronous)
    └──────────┬──────────────┘
               │
    ┌──────────▼──────────────┐
    │ member_cmd              │
    │  - process_cmd()        │ ← Already synchronous
    └─────────────────────────┘
```

### For Large Responses (>10MB threshold):

```
member_cmd.process_cmd()
    ↓
Detect large response (>10MB)
    ↓
message_server.send_msg() ← Block transport (chunked delivery)
    ↓
SSEConnectionManager ← Receives chunks, streams to client
    ↓
Client ← Receives data as SSE events
```

**Total Threads Created by MCP**:
- ✅ **1 SSEConnectionManager thread** (persistent, handles ALL connections)
- ❌ **0 message processing threads** (removed)
- ❌ **0 DirectClient pool threads** (removed)

---

## Phased Refactoring Plan

### Phase 1: Remove Unnecessary Threading (PRIORITY - Quick Win)

**Goal**: Eliminate DirectClient ThreadPool and per-message threads

**Changes**:

1. **Remove DirectClient ThreadPool** (`edge_lake/mcp_server/core/direct_client.py`)
   ```python
   # REMOVE:
   self.executor = ThreadPoolExecutor(max_workers=max_workers)

   # CHANGE execute_command() to synchronous:
   def execute_command(self, command: str) -> Dict[str, Any]:
       # Call member_cmd.process_cmd() directly (no submit())
       return self._execute_sync(command)
   ```

2. **Remove Message Processing Thread** (`edge_lake/mcp_server/transport/sse_handler.py:400`)
   ```python
   # CHANGE _process_message_async() to _process_message_sync():
   def _process_message_sync(self, session_id: str, message: Dict[str, Any]):
       # No thread.start() - just call process() directly
       response = self.mcp_server.process_message(message)

       # Queue response for SSEConnectionManager to send
       with self.connection_lock:
           connection = self.connections.get(session_id)
       if connection:
           connection.queue_message('message', response)
   ```

3. **Update handle_messages_endpoint()** (`sse_handler.py:341`)
   ```python
   # BEFORE: self._process_message_async(session_id, message)
   # AFTER:  self._process_message_sync(session_id, message)
   ```

**Benefits**:
- ✅ Eliminates DirectClient pool (N threads → 0)
- ✅ Eliminates per-message threads (1 per message → 0)
- ✅ Reduces memory footprint
- ✅ Reduces context switching overhead
- ✅ Maintains existing functionality (all calls are synchronous anyway)

**Risk**: Low - just removing unnecessary async wrappers around sync code

**Testing**:
- Verify tool execution still works
- Verify responses still sent via SSE
- Check performance (should improve due to less overhead)

**Estimated Time**: 2-3 hours

---

### Phase 2: Fix SSE Connection Blocking (CORE FIX)

**Goal**: Stop blocking HTTP worker threads on SSE connections

**Problem**: Currently `handle_sse_endpoint()` blocks HTTP worker thread in `while connection.connected` loop (line 245). This prevents the thread from returning to the pool.

**Solution**: Create dedicated `SSEConnectionManager` thread that uses non-blocking I/O to handle all connections.

**Design**:

```python
# edge_lake/mcp_server/transport/sse_manager.py (NEW FILE)

import select
import threading
from typing import Dict
from queue import Queue

class SSEConnectionManager:
    """
    Single dedicated thread managing ALL SSE connections.
    Uses select() for non-blocking multiplexed I/O.
    """

    def __init__(self, keepalive_interval=30):
        self.connections: Dict[str, SSEConnection] = {}
        self.connection_lock = threading.Lock()
        self.message_queue = Queue()  # Responses to send

        self.keepalive_interval = keepalive_interval
        self.running = True

        # Start manager thread
        self.manager_thread = threading.Thread(
            target=self._manager_loop,
            daemon=True,
            name="MCP-SSE-Manager"
        )
        self.manager_thread.start()

    def register_connection(self, session_id: str, handler):
        """
        Register new SSE connection.
        Called from HTTP worker thread (doesn't block).
        """
        with self.connection_lock:
            connection = SSEConnection(session_id, handler)
            self.connections[session_id] = connection

        # Send initial connected event
        self.queue_response(session_id, 'connected', {
            'session_id': session_id,
            'message': 'MCP SSE connection established'
        })

    def queue_response(self, session_id: str, event_type: str, data: dict):
        """Queue response to be sent to client."""
        self.message_queue.put((session_id, event_type, data))

    def _manager_loop(self):
        """
        Main loop managing all SSE connections.
        Runs in dedicated thread, uses select() for I/O multiplexing.
        """
        last_keepalive = time.time()

        while self.running:
            current_time = time.time()

            # Send keepalives if needed (every 30s)
            if current_time - last_keepalive > self.keepalive_interval:
                self._send_keepalives()
                last_keepalive = current_time

            # Process queued responses
            self._process_queued_messages()

            # Check for disconnected clients
            self._cleanup_stale_connections()

            # Sleep briefly to avoid busy loop
            time.sleep(0.1)

    def _send_keepalives(self):
        """Send keepalive to all active connections."""
        with self.connection_lock:
            for session_id, conn in list(self.connections.items()):
                if not conn.send_keepalive():
                    # Connection failed, remove it
                    del self.connections[session_id]

    def _process_queued_messages(self):
        """Send queued messages to clients."""
        while not self.message_queue.empty():
            try:
                session_id, event_type, data = self.message_queue.get_nowait()

                with self.connection_lock:
                    conn = self.connections.get(session_id)

                if conn:
                    conn.send_event(event_type, data)

            except Empty:
                break

    def shutdown(self):
        """Shutdown manager and close all connections."""
        self.running = False

        with self.connection_lock:
            for conn in self.connections.values():
                conn.close()
            self.connections.clear()

        self.manager_thread.join(timeout=5.0)
```

**Changes to sse_handler.py**:

```python
# CHANGE handle_sse_endpoint() to not block:

def handle_sse_endpoint(self, handler) -> bool:
    """
    Handle GET /mcp/sse - register SSE connection.
    Returns IMMEDIATELY after registration (doesn't block).
    """
    # Generate session ID
    session_id = str(uuid.uuid4())

    # Send SSE headers
    handler.send_response(200, "OK")
    handler.send_header('Content-Type', 'text/event-stream')
    handler.send_header('Cache-Control', 'no-cache')
    handler.send_header('Connection', 'keep-alive')
    handler.end_headers()

    # Register with manager (doesn't block)
    self.sse_manager.register_connection(session_id, handler)

    # HTTP worker thread returns to pool immediately!
    return True
```

**Benefits**:
- ✅ HTTP workers no longer blocked on SSE connections
- ✅ Single thread handles ALL SSE connections (efficient multiplexing)
- ✅ Better scalability (100s of SSE connections with 1 thread)
- ✅ Keepalives still work during long queries

**Risk**: Medium - requires careful handling of socket I/O

**Testing**:
- Verify multiple SSE connections work
- Verify keepalives sent during long queries
- Verify responses stream correctly
- Load test with 10+ concurrent SSE clients

**Estimated Time**: 6-8 hours

---

### Phase 3: Integrate message_server for Large Responses

**Goal**: Route large query results (>10MB) through message_server for chunked delivery

**Trigger**: Detect response size in `query_executor.py` or `direct_client.py`

**Design**:

```python
# In direct_client.py or query_executor.py:

def execute_query(self, dbms: str, query: str, ...) -> str:
    # Execute query
    result = member_cmd.process_cmd(...)

    # Check result size
    result_size = len(result)

    if result_size > 10_485_760:  # 10MB threshold
        # Use message_server block transport
        return self._stream_via_message_server(result, session_id)
    else:
        # Normal SSE streaming
        return result

def _stream_via_message_server(self, data: str, session_id: str) -> str:
    """
    Stream large response via message_server block transport.
    Integrates with SSEConnectionManager to deliver chunks.
    """
    # Split into blocks (e.g., 1MB chunks)
    block_size = 1_048_576
    blocks = [data[i:i+block_size] for i in range(0, len(data), block_size)]

    # Send blocks via message_server
    for block_id, block in enumerate(blocks):
        # Queue block for SSEConnectionManager
        self.sse_manager.queue_response(
            session_id,
            'block',
            {
                'block_id': block_id,
                'total_blocks': len(blocks),
                'data': block
            }
        )

    return f"Streaming {len(blocks)} blocks via message_server"
```

**Integration with message_server.py**:
- Leverage existing `send_msg()` function for chunked delivery
- SSEConnectionManager receives blocks and streams as SSE events
- Client reassembles blocks into complete response

**Benefits**:
- ✅ Support for massive query results (GB-scale)
- ✅ Prevents memory exhaustion
- ✅ Reuses proven message_server infrastructure
- ✅ Streaming delivery (client sees data as it arrives)

**Risk**: Low - message_server is proven, just need integration layer

**Testing**:
- Create query returning >10MB result
- Verify chunked delivery works
- Verify client receives all blocks
- Test with 100MB+ results

**Estimated Time**: 4-6 hours

---

## Implementation Order

### This Weekend (Phase 1 ONLY):

**Saturday**:
1. ✅ Document plan (this file)
2. ✅ Remove DirectClient ThreadPool
3. ✅ Test basic tool execution still works

**Sunday**:
4. ✅ Remove message processing threads
5. ✅ Test full MCP workflow (list_tools, call_tool)
6. ✅ Performance benchmarking

**Next Weekend (Phase 2)**:
7. Implement SSEConnectionManager
8. Refactor handle_sse_endpoint()
9. Integration testing with multiple clients

**Future (Phase 3)**:
10. Integrate message_server for large responses
11. Load testing with massive query results

---

## Success Criteria

### Phase 1 (This Weekend):
- ✅ DirectClient has NO ThreadPool
- ✅ No per-message threads spawned
- ✅ All existing functionality works
- ✅ Performance improved (less overhead)

### Phase 2 (Next Weekend):
- ✅ HTTP workers never blocked on SSE connections
- ✅ Single SSEConnectionManager thread handles all connections
- ✅ Keepalives work during long queries
- ✅ Support 10+ concurrent SSE clients

### Phase 3 (Future):
- ✅ Query results >10MB stream via message_server
- ✅ Support 100MB+ results without memory issues
- ✅ Client receives chunked data correctly

---

## Rollback Plan

If refactoring breaks functionality:

1. **Phase 1 rollback**: Restore DirectClient ThreadPool, restore per-message threads
2. **Phase 2 rollback**: Revert to blocking handle_sse_endpoint()
3. **Phase 3 rollback**: Disable message_server integration

All phases are independent and can be rolled back individually.

---

## Performance Expectations

### Current (Before Refactoring):
- **Thread count** (3 SSE clients): 5 HTTP + 3 blocked + 3 message threads + N DirectClient pool = **15-20 threads**
- **HTTP worker availability**: 2/5 (3 blocked on SSE)
- **Memory overhead**: ThreadPool + per-message threads

### After Phase 1:
- **Thread count**: 5 HTTP + 3 blocked + 1 keepalive = **9 threads**
- **Savings**: ~6-11 threads eliminated

### After Phase 2:
- **Thread count**: 5 HTTP + 1 SSEConnectionManager = **6 threads**
- **HTTP worker availability**: 5/5 (none blocked!)
- **Savings**: ~9-14 threads eliminated
- **Scalability**: 100+ SSE clients with same 6 threads

### After Phase 3:
- **Large response support**: GB-scale results
- **Memory usage**: Constant (streaming, no buffering)

---

## Testing Checklist

### Phase 1 Testing:
- [ ] Tool execution works (call_tool)
- [ ] Responses sent via SSE correctly
- [ ] Multiple concurrent tool calls work
- [ ] No threading errors or deadlocks
- [ ] Performance benchmark (should improve)

### Phase 2 Testing:
- [ ] HTTP workers return to pool after SSE registration
- [ ] Keepalives sent every 30s
- [ ] Responses streamed correctly
- [ ] 10+ concurrent SSE clients work
- [ ] Long-running queries (1+ minutes) work
- [ ] Client disconnect detected and cleaned up

### Phase 3 Testing:
- [ ] Query >10MB triggers message_server
- [ ] Blocks streamed correctly
- [ ] Client reassembles blocks
- [ ] 100MB+ results work without memory issues
- [ ] Multiple large queries concurrent

---

## Notes & Learnings

### Key Insights:
1. **MCP SSE requires streaming** - batching breaks the protocol
2. **Keepalives are mandatory** - queries can run for minutes
3. **HTTP worker blocking is the root cause** - fixing this enables efficiency
4. **message_server is perfect for large responses** - proven infrastructure

### Design Principles:
1. **Minimize threads** - only create when absolutely necessary
2. **Use synchronous code** - async wrappers around sync calls are wasteful
3. **Multiplex I/O** - one thread can handle many connections
4. **Reuse proven components** - message_server already handles chunking

### Questions to Address:
- [ ] What's the actual large response threshold? (10MB? 50MB?)
- [ ] Should message_server integration be configurable?
- [ ] Do we need backpressure handling for fast clients?

---

## References

- **MCP Spec (2024-11-05)**: Official protocol for SSE transport
- **http_server.py**: EdgeLake HTTP infrastructure (lines 217, 775, 963)
- **sse_handler.py**: Current SSE implementation (lines 189, 245, 400)
- **message_server.py**: Block transport infrastructure (line 913)
- **direct_client.py**: Current ThreadPool usage (line 36)

---

## TODO: MCP Server Autostart

**Issue**: MCP server requires manual `run mcp server` command after EdgeLake starts.

**Investigation Needed**:
1. Check `../EdgeLake-working/` for MCP server registration and status command support
2. Check `../deployment-scripts/` for autostart scripts (main.al, *start*.al)
3. Determine where to add `run mcp server` to autostart sequence

**Options**:
- Add to query node autoexec script
- Add to main.al startup sequence
- Make MCP server auto-start when REST server starts

**Priority**: Medium (manual start works for testing, but autostart needed for production)

---

**Document Version**: 1.1
**Last Updated**: 2025-11-01
**Status**: Phase 1 complete, autostart investigation pending
