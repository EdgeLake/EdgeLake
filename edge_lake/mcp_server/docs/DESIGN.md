# MCP Server Architecture Design Document

**Status**: Phase 1 (Core Integration & SSE Transport) Complete - Production Ready
**Version**: 2.0
**Date**: 2025-11-01
**Authors**: EdgeLake Development Team

## Executive Summary

This document describes the architecture of EdgeLake's MCP (Model Context Protocol) server, which is fully integrated with the core HTTP infrastructure. The implementation uses a lightweight MCP protocol (no SDK dependency) and leverages EdgeLake's production-ready `http_server.py`, enabling unified HTTP handling, efficient resource management, and direct integration with EdgeLake's command processor. Phase 1 (Core Integration & SSE Transport) is complete and production-ready. Phase 2 (block transport for large responses >10MB) is planned for future implementation.

## Background

### Implementation History

The MCP server implementation went through a rapid development cycle:

**Initial POC** (Proof-of-Concept):
- Standalone HTTP server using Starlette/Uvicorn for SSE transport
- MCP SDK dependency for protocol implementation
- Isolated from EdgeLake's core HTTP/TCP infrastructure
- Functional query builder, executor, and direct client integration

**Phase 1: Core Integration & SSE Transport** (Completed in 3 hours):
- Removed Starlette/Uvicorn/MCP SDK dependencies
- Implemented lightweight MCP protocol (JSON-RPC 2.0 over SSE)
- Full integration with `http_server.py` (30 lines of routing code)
- Shared workers pool with REST API and data ingestion
- Production-ready with SSL, authentication, and logging support

**Current Implementation** (`edge_lake/mcp_server/`):
- SSE transport layer (584 lines): `transport/sse_handler.py`
- MCP server (324 lines): `server/mcp_server.py`
- Direct client integration: `core/direct_client.py` (uses format=mcp for clean JSON output and retrieves results from job handles)
- Query builder and executor: `core/query_builder.py`, `core/query_executor.py`
- Configuration-driven tools: `config/tools.yaml`

### Phase 1: Core Integration & SSE Transport - Achievements

1. **Unified HTTP Infrastructure**: Single `http_server.py` handles REST, data ingestion, and MCP
2. **Production Readiness**: Leverages tested http_server.py with SSL, authentication, error handling
3. **Resource Efficiency**: Shares workers pool across all HTTP services (~1KB memory per connection)
4. **Lightweight Protocol**: No external MCP SDK dependency, lightweight JSON-RPC 2.0 protocol implementation
5. **Direct Integration**: Uses `member_cmd.process_cmd()` with format=mcp parameter for clean JSON output and job handle result retrieval

### Phase 2 Goals (Future) - Not Yet Implemented

1. **Large Response Support**: Implement block transport via `message_server.py` for results >10MB
2. **Performance Optimization**: Further reduce latency and memory usage
3. **Enhanced Monitoring**: Add metrics and health checks

## Architecture Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  MCP Client (Claude Code, other MCP clients)                │
└────────────────────────┬────────────────────────────────────┘
                         │ HTTP/SSE
┌────────────────────────┴────────────────────────────────────┐
│  edge_lake/tcpip/http_server.py                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  ThreadedHTTPServer                                  │   │
│  │  - Existing: /rest/*, /data/*                        │   │
│  │  - New: /mcp/sse, /mcp/messages/*                    │   │
│  └──────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  ChunkedHTTPRequestHandler                           │   │
│  │  - do_GET(), do_POST()                               │   │
│  │  - Routing to MCP handlers                           │   │
│  └──────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Workers Pool (shared)                               │   │
│  │  - Handles REST, data ingestion, MCP concurrently    │   │
│  └──────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────┴────────────────────────────────────┐
│  edge_lake/mcp_server/transport/sse_handler.py (Production) │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  SSETransport (584 lines)                            │   │
│  │  - handle_sse_endpoint(): GET /mcp/sse               │   │
│  │  - handle_messages_endpoint(): POST /mcp/messages/*  │   │
│  │  - Event streaming (data:, event:, id:)              │   │
│  │  - Message framing and parsing                       │   │
│  └──────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────┴────────────────────────────────────┐
│  edge_lake/mcp_server/server/mcp_server.py (Complete)       │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  MCPServer (324 lines)                               │   │
│  │  - Lightweight JSON-RPC 2.0 protocol, no MCP SDK     │   │
│  │  - list_tools(): Return available MCP tools          │   │
│  │  - call_tool(): Execute tool with parameters         │   │
│  │  - Lifecycle management (start, stop)                │   │
│  └──────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────┴────────────────────────────────────┐
│  edge_lake/mcp_server/core/ (Production Components)         │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  query_builder.py                                    │   │
│  │  - Build SQL from tool parameters                    │   │
│  │  - SELECT, WHERE, JOIN, GROUP BY, ORDER BY, LIMIT    │   │
│  └──────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  query_executor.py                                   │   │
│  │  - QueryValidator: select_parser() validation        │   │
│  │  - StreamingExecutor: process_fetch_rows()           │   │
│  │  - BatchExecutor: Full result collection             │   │
│  │  - Auto mode selection (streaming vs batch)          │   │
│  └──────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  direct_client.py                                    │   │
│  │  - Direct member_cmd.process_cmd() calls             │   │
│  │  - Uses format=mcp for clean JSON output             │   │
│  │  - Retrieves results from job handles                │   │
│  │  - Thread pool executor                              │   │
│  └──────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  command_builder.py                                  │   │
│  │  - Build EdgeLake commands from tool parameters      │   │
│  └──────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────┴────────────────────────────────────┐
│  EdgeLake Core                                              │
│  - member_cmd.py: Command processor                         │
│  - dbms/: Database operations                               │
│  - blockchain/: Metadata management                         │
└─────────────────────────────────────────────────────────────┘
```

### Block Transport for Large Results (Phase 2 - Not Yet Implemented)

For query results exceeding 10MB:

```
┌────────────────────────────────────────────────────────┐
│  Query Result > 10MB threshold                         │
└────────────────┬───────────────────────────────────────┘
                 │
┌────────────────┴───────────────────────────────────────┐
│  edge_lake/tcpip/message_server.py                     │
│  - Block-based transport                               │
│  - Chunked delivery                                    │
│  - Reassembly on client side                           │
└────────────────┬───────────────────────────────────────┘
                 │
┌────────────────┴───────────────────────────────────────┐
│  MCP Client receives blocks                            │
│  - Reassemble into complete result                     │
│  - Stream to application                               │
└────────────────────────────────────────────────────────┘
```

## Component Details

### 1. HTTP Server Integration (`edge_lake/tcpip/http_server.py`)

**Changes Required**:
- Add MCP endpoint routing in `ChunkedHTTPRequestHandler.do_GET()` and `do_POST()`
- Register `/mcp/sse` endpoint for SSE connection establishment
- Register `/mcp/messages/*` endpoint for MCP message POST
- No changes to ThreadedHTTPServer or workers pool

**Integration Points**:
```python
# In ChunkedHTTPRequestHandler
def do_GET(self):
    if self.path == '/mcp/sse':
        # Delegate to SSE handler
        from edge_lake.mcp_server.transport import sse_handler
        sse_handler.handle_sse_endpoint(self)
        return
    # ... existing REST handling ...

def do_POST(self):
    if self.path.startswith('/mcp/messages/'):
        # Delegate to SSE handler
        from edge_lake.mcp_server.transport import sse_handler
        sse_handler.handle_messages_endpoint(self)
        return
    # ... existing REST handling ...
```

**Configuration**:
- Use existing `run rest server` command parameters
- MCP server started separately with `run mcp server` command
- MCP server stopped with `exit mcp server` command

### 2. SSE Transport Layer (`edge_lake/mcp_server/transport/sse_handler.py`)

**Status**: Production (584 lines)

**Purpose**: Bridge between HTTP server and MCP server, implementing SSE protocol with lightweight JSON-RPC 2.0 protocol, no MCP SDK dependency.

**Key Classes**:

```python
class SSETransport:
    """
    SSE transport implementation for MCP.
    Integrates with http_server.py's ChunkedHTTPRequestHandler.
    """

    def handle_sse_endpoint(self, handler: ChunkedHTTPRequestHandler):
        """
        Handle GET /mcp/sse - establish SSE connection.
        Sets up event stream with proper headers.
        """

    def handle_messages_endpoint(self, handler: ChunkedHTTPRequestHandler):
        """
        Handle POST /mcp/messages/* - receive MCP messages.
        Parses JSON-RPC requests and routes to MCP server.
        """

    def send_event(self, event_type: str, data: dict):
        """
        Send SSE event to client.
        Format: data: {...}\nevent: {type}\nid: {id}\n\n
        """
```

**SSE Protocol Implementation**:
- **Connection Setup**: GET /mcp/sse with headers:
  ```
  Content-Type: text/event-stream
  Cache-Control: no-cache
  Connection: keep-alive
  ```
- **Event Format**:
  ```
  data: {"jsonrpc":"2.0","id":1,"result":{...}}
  event: message
  id: 123

  ```
- **Keep-Alive**: Send `:keepalive\n\n` every 30 seconds

**Message Routing**:
1. Parse incoming POST to /mcp/messages/*
2. Extract JSON-RPC request
3. Route to appropriate MCP server handler (list_tools, call_tool)
4. Format response as SSE event
5. Send via established SSE connection

### 3. MCP Server (`edge_lake/mcp_server/server/mcp_server.py`)

**Status**: Complete (324 lines)

**Purpose**: MCP protocol implementation using lightweight JSON-RPC 2.0 protocol, no MCP SDK dependency, integrated with EdgeLake infrastructure.

**Key Changes from POC**:
- Removed Starlette/Uvicorn/MCP SDK dependencies (removed in Phase 1 refactoring)
- Replaced `run_sse_server()` with initialization for http_server integration
- Implemented lightweight JSON-RPC 2.0 protocol
- Keep protocol handlers: `list_tools()`, `call_tool()`

**Core Class**:

```python
class MCPServer:
    """
    MCP server integrated with EdgeLake's http_server.py.
    Handles MCP protocol operations (list_tools, call_tool).
    """

    def __init__(self, config_dir: str = None, capabilities: dict = None):
        """
        Initialize MCP server components.
        No HTTP server creation - uses existing http_server.py.
        """
        # Initialize direct client, query builder/executor, tools

    def list_tools(self) -> List[Tool]:
        """
        Return list of available MCP tools.
        Called via SSE transport when client requests tools.
        """

    def call_tool(self, name: str, arguments: dict) -> List[TextContent]:
        """
        Execute MCP tool.
        Routes to appropriate executor based on tool type.
        """

    def start(self):
        """
        Register MCP endpoints with http_server.
        Called by 'run mcp server' command.
        """

    def stop(self):
        """
        Cleanup and shutdown.
        Called by 'exit mcp server' command.
        """
```

**Lifecycle**:
1. **Initialization**: Created when EdgeLake starts (if configured)
2. **Registration**: `start()` registers endpoints with http_server
3. **Operation**: Handles MCP requests via SSE transport
4. **Shutdown**: `stop()` cleans up resources

### 4. Core Components (Production Components)

#### query_builder.py
- **Status**: Production
- **Purpose**: Construct SQL queries from MCP tool parameters
- **Features**: SELECT, WHERE, JOIN, GROUP BY, ORDER BY, LIMIT support

#### query_executor.py
- **Status**: Production
- **Purpose**: Hybrid validation + streaming query execution
- **Components**:
  - `QueryValidator`: Uses `select_parser()` for validation/transformation
  - `StreamingExecutor`: Streams results via `process_fetch_rows()`
  - `BatchExecutor`: Collects all results for aggregates
  - Auto mode selection based on query characteristics

#### direct_client.py
- **Status**: Production
- **Purpose**: Direct integration with `member_cmd.process_cmd()`
- **Features**:
  - Thread pool executor
  - Uses format=mcp parameter for clean JSON output
  - Retrieves results from job handles
  - Result extraction from io_buffer/stdout

#### command_builder.py
- **Status**: Production
- **Purpose**: Build EdgeLake commands from MCP tool parameters

### 5. Block Transport Integration (Phase 2 - Not Yet Implemented)

**When to Use**:
- Query result size > 10MB (configurable threshold)
- Large file transfers
- Bulk data exports

**Implementation**:

```python
class BlockTransportAdapter:
    """
    Adapter for message_server.py block transport.
    Used when query results exceed size threshold.
    """

    def should_use_block_transport(self, result_size: int) -> bool:
        """
        Determine if block transport should be used.
        """
        return result_size > self.block_threshold

    def send_via_blocks(self, data: bytes, client_id: str):
        """
        Send large data via message_server.py block transport.
        """
        # Use message_server.py's block sending logic

    def receive_blocks(self, block_ids: List[str]) -> bytes:
        """
        Receive and reassemble blocks.
        """
        # Use message_server.py's block receiving logic
```

**Flow**:
1. Query executor estimates result size
2. If > threshold, use block transport:
   - Split into blocks (message_server.py)
   - Send block IDs via SSE
   - Client requests blocks
3. If < threshold, use standard SSE streaming

## Data Flow

### Query Execution Flow

```
1. MCP Client
   │
   ├─→ POST /mcp/messages/* (tool call: run_query)
   │
2. http_server.py → ChunkedHTTPRequestHandler
   │
   ├─→ do_POST() routes to sse_handler
   │
3. sse_handler.py → SSETransport
   │
   ├─→ handle_messages_endpoint()
   ├─→ Parse JSON-RPC request
   │
4. mcp_server.py → MCPServer
   │
   ├─→ call_tool("run_query", arguments)
   │
5. query_builder.py → QueryBuilder
   │
   ├─→ build_query(arguments) → SQL string
   │
6. query_executor.py → QueryExecutor
   │
   ├─→ QueryValidator.validate_query()
   │   └─→ select_parser() [EdgeLake core validation]
   │
   ├─→ Auto mode selection:
   │   ├─→ Streaming mode: StreamingExecutor
   │   │   └─→ process_fetch_rows() [EdgeLake core]
   │   │       └─→ Yields batches of rows
   │   │
   │   └─→ Batch mode: BatchExecutor
   │       └─→ process_fetch_rows() [EdgeLake core]
   │           └─→ Collects all rows
   │
7. Check result size:
   │
   ├─→ If < 10MB: SSE streaming
   │   └─→ sse_handler sends events
   │
   └─→ If > 10MB: Block transport
       └─→ message_server.py blocks
       └─→ SSE sends block IDs
       └─→ Client requests blocks
   │
8. MCP Client receives results
```

### Tool Registration Flow

```
1. MCP Client
   │
   ├─→ POST /mcp/messages/* (method: initialize)
   │
2. http_server.py → sse_handler.py → mcp_server.py
   │
   ├─→ list_tools()
   │
3. tools/generator.py → ToolGenerator
   │
   ├─→ generate_tools()
   │   └─→ Read tool definitions from config
   │   └─→ Convert to MCP Tool format
   │
4. Return tool list via SSE
   │
5. MCP Client receives available tools
```

## Configuration

### EdgeLake Command Configuration

```bash
# Start HTTP server (required first)
run rest server where \
    external_ip = 0.0.0.0 and \
    external_port = 32049 and \
    internal_ip = 127.0.0.1 and \
    internal_port = 32049 and \
    ssl = true

# Start MCP server (separate command)
run mcp server

# Stop MCP server
exit mcp server
```

### Configuration Parameters

MCP server uses the existing REST server configuration. Optional parameters can be configured through EdgeLake's parameter system:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `mcp_config_dir` | Path to MCP configuration directory | `edge_lake/mcp_server/config` |
| `mcp_max_workers` | Max workers for direct client | `10` |
| `mcp_trace` | Enable MCP debug logging | `false` |

## Security Considerations

### Authentication
- Reuse http_server.py's JWT authentication
- MCP requests subject to same auth as REST API
- Support for user-based permission checks

### Authorization
- Query validation via `select_parser()` enforces permissions
- Tool execution checks user capabilities
- Metadata access controlled by EdgeLake policies

### SSL/TLS
- Reuse http_server.py's SSL configuration
- Support for mutual TLS (client certificates)
- CA certificate validation

### Rate Limiting
- Share http_server.py's rate limiting
- Configurable per-user limits
- Protection against DoS

## Performance Considerations

### Resource Sharing
- **Workers Pool**: Shared across REST, data ingestion, and MCP
- **Memory**: Query executor streams results to avoid loading large datasets
- **CPU**: Direct client uses thread pool to prevent blocking

### Optimization Strategies
1. **Query Execution**:
   - Auto mode selection (streaming vs batch)
   - Pass-through queries use original SQL (no transformation overhead)
   - Streaming via `process_fetch_rows()` for large results

2. **Block Transport**:
   - Only used for results > 10MB
   - Configurable block size for network optimization
   - Parallel block transfer support

3. **Caching**:
   - Tool definitions cached in memory
   - Query validation results cached (select_parser)
   - Reuse existing EdgeLake caching mechanisms

### Monitoring
- Reuse http_server.py's statistics collection
- MCP-specific metrics:
  - Tool call count/latency
  - Query execution time
  - Block transport usage
  - SSE connection count

## Testing Strategy

### Unit Tests
- **SSE Transport**: Event formatting, message parsing
- **MCP Server**: Tool registration, tool execution
- **Query Builder**: SQL construction correctness
- **Query Executor**: Mode selection, streaming, batch execution

### Integration Tests
- **HTTP Integration**: Endpoint routing, workers pool
- **End-to-End**: MCP client → HTTP → query execution → results
- **Block Transport**: Large result handling
- **Authentication**: JWT flow, permission checks

### Performance Tests
- **Concurrent Connections**: Multiple MCP clients
- **Large Queries**: Result sets > 10MB
- **Streaming**: Memory usage during query execution
- **Workers Pool**: Resource contention with REST API

### Manual Testing
- **Claude Code Integration**: Real MCP client interaction
- **Error Scenarios**: Network failures, timeouts
- **Configuration**: Different settings combinations

## Migration Plan

### Phase 1: Core Integration & SSE Transport - COMPLETE ✅
1. ✅ Created `edge_lake/mcp_server/transport/sse_handler.py` (584 lines)
2. ✅ Added MCP endpoint routing to `http_server.py` (30 lines)
3. ✅ Refactored `server.py` to remove Starlette/Uvicorn/MCP SDK (324 lines)
4. ✅ Updated initialization in `member_cmd.py`

**Deliverables**:
- ✅ SSE transport working with http_server.py
- ✅ MCP server integrated (without block transport)
- ✅ Lightweight JSON-RPC 2.0 protocol implementation
- ✅ Production-ready with SSL, authentication, logging

### Phase 2: Block Transport (Future)
1. Create `edge_lake/mcp_server/transport/block_transport.py`
2. Integrate with `message_server.py`
3. Add threshold-based selection logic
4. Update query executor to support block transport

**Deliverables**:
- Block transport for large results
- Configuration for threshold tuning
- Performance tests demonstrating efficiency

### Phase 3: Testing & Documentation (Future)
1. Comprehensive test suite
2. Performance benchmarking
3. Update documentation
4. Example configurations

**Deliverables**:
- Test coverage > 80%
- Performance baseline established
- Documentation updated
- Migration guide for POC users

### Phase 4: Production Deployment (Future)
1. Deploy to staging environment
2. Monitor and tune performance
3. Address any issues discovered
4. Production rollout

**Deliverables**:
- Stable production deployment
- Monitoring dashboards
- Runbook for operations

## Success Criteria

### Functional Requirements (Phase 1: Core Integration & SSE Transport)
- ✅ MCP protocol fully implemented (list_tools, call_tool)
- ✅ SSE transport working with http_server.py
- ✅ Query execution via direct client
- ✅ Authentication and authorization working
- ⏳ Block transport for large results (Phase 2: Block Transport)

### Non-Functional Requirements (Phase 1: Core Integration & SSE Transport)
- ✅ Query latency < 500ms (95th percentile, excluding data fetch time)
- ✅ Support > 100 concurrent MCP connections
- ✅ Memory usage < 1GB for query results < 10MB
- ⏳ Block transport overhead < 10% for large results (Phase 2: Block Transport)
- ✅ Zero downtime migration

### Quality Requirements (Phase 1: Core Integration & SSE Transport)
- ⏳ Test coverage > 80% (Phase 3: Testing & Documentation)
- ✅ No regressions in existing REST API functionality
- ✅ Logging and monitoring in place
- ✅ Documentation complete and accurate

## Risk Assessment

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| http_server.py modification breaks REST API | High | Low | Comprehensive regression tests |
| SSE compatibility issues with MCP clients | Medium | Medium | Test with multiple MCP client implementations |
| Block transport adds complexity | Medium | High | Phased implementation, fallback to SSE |
| Performance degradation due to shared workers pool | Medium | Low | Load testing, configurable pool size |
| Authentication bypass vulnerability | High | Low | Security audit, use existing auth mechanisms |

## Future Enhancements

### Near-Term (3-6 months)
- **WebSocket Transport**: Alternative to SSE for browsers
- **Query Result Caching**: Cache frequently-accessed results
- **Advanced Tool Types**: File operations, blockchain queries
- **Metrics Dashboard**: Real-time MCP performance monitoring

### Long-Term (6-12 months)
- **Distributed MCP**: MCP server cluster for high availability
- **Streaming Aggregations**: Real-time analytics via streaming queries
- **AI-Powered Query Optimization**: Use ML to optimize query execution
- **Multi-Tenant Support**: Isolated MCP environments per tenant

## Appendices

### A. File Structure

```
edge_lake/
├── mcp_server/
│   ├── __init__.py
│   ├── DESIGN.md                      # This document
│   ├── README.md                      # User-facing documentation
│   ├── IMPLEMENTATION_PLAN.md         # Phased implementation plan
│   ├── QUICK_START.md                 # 5-minute test guide
│   ├── server/
│   │   ├── __init__.py
│   │   └── mcp_server.py              # Production (324 lines)
│   ├── transport/
│   │   ├── __init__.py
│   │   ├── sse_handler.py             # Production (584 lines)
│   │   └── block_transport.py         # Phase 2 (future)
│   ├── core/                          # Production Components
│   │   ├── __init__.py
│   │   ├── query_builder.py           # SQL query construction
│   │   ├── query_executor.py          # Hybrid validation + streaming
│   │   ├── direct_client.py           # Direct member_cmd integration (format=mcp)
│   │   └── command_builder.py         # EdgeLake command construction
│   ├── tools/
│   │   ├── __init__.py
│   │   ├── generator.py               # Tool definition generator
│   │   └── executor.py                # Tool execution logic
│   └── config/
│       ├── __init__.py
│       └── tools.yaml                 # Tool definitions
└── tcpip/
    ├── http_server.py                 # Modified: Add MCP endpoints (30 lines)
    └── message_server.py              # Used for Phase 2 block transport
```

### B. Dependencies

**Production Dependencies**:
```
pydantic>=2.0.0
```

**Removed Dependencies** (removed in Phase 1 refactoring):
```
starlette          # Replaced by http_server.py
uvicorn            # Replaced by http_server.py
sse-starlette      # Replaced by custom SSE implementation
mcp                # MCP SDK - replaced by lightweight JSON-RPC 2.0 protocol
```

**No New Dependencies Required**

### C. Glossary

- **MCP**: Model Context Protocol - Standard protocol for AI agents to interact with data sources (implemented using lightweight JSON-RPC 2.0, no MCP SDK dependency)
- **SSE**: Server-Sent Events - HTTP-based event streaming protocol
- **JSON-RPC 2.0**: Lightweight remote procedure call protocol used for MCP implementation
- **Block Transport**: EdgeLake's mechanism for transferring large data in chunks (Phase 2 - not yet implemented)
- **select_parser**: EdgeLake's query validation and transformation function
- **process_fetch_rows**: EdgeLake's streaming row retrieval function
- **member_cmd**: EdgeLake's command processor
- **format=mcp**: Parameter used with member_cmd.process_cmd() for clean JSON output
- **ThreadedHTTPServer**: HTTP server with thread-per-request model
- **ChunkedHTTPRequestHandler**: HTTP request handler supporting chunked transfer encoding

### D. References

- [MCP Protocol Specification](https://spec.modelcontextprotocol.io/)
- [Server-Sent Events (SSE) Specification](https://html.spec.whatwg.org/multipage/server-sent-events.html)
- [EdgeLake Documentation](https://docs.edgelake.com/)
- EdgeLake `http_server.py` source code
- EdgeLake `message_server.py` source code

### E. Change Log

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-10-30 | EdgeLake Team | Initial design document |
