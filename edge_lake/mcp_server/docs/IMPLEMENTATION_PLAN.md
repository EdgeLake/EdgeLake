# MCP Server Refactoring - Implementation Plan

**Status**: Phase 1 (Core Integration & SSE Transport) Complete ✅ | Phase 2-4 Planned
**Start Date**: 2025-10-30
**Phase 1 Completion**: 2025-11-01 (~3 hours)
**Planned Duration**: 4 phases

## Overview

This document provides a detailed, step-by-step implementation plan for refactoring the MCP server to integrate with EdgeLake's core HTTP infrastructure. Phase 1 (Core Integration & SSE Transport) has been completed in ~3 hours instead of the planned 1 week. The plan is organized into phases with specific tasks, dependencies, and acceptance criteria.

## Prerequisites

- [x] Design document completed (`edge_lake/mcp_server/DESIGN.md`)
- [x] Core components identified (query_builder, query_executor, direct_client)
- [x] Integration points identified (http_server.py, message_server.py)
- [x] Lightweight JSON-RPC 2.0 protocol approach confirmed (no MCP SDK dependency)

## Phase 1: Core Integration & SSE Transport ✅ **COMPLETE**

**Planned**: 1 week | **Actual**: ~3 hours
**Status**: Production Ready

### Goal ✅ ACHIEVED
Integrate MCP server with http_server.py, implementing SSE transport with lightweight JSON-RPC 2.0 protocol (no MCP SDK dependency).

### Tasks

#### 1.1 Create SSE Transport Layer ✅
**File**: `edge_lake/mcp_server/transport/__init__.py`
**File**: `edge_lake/mcp_server/transport/sse_handler.py` (584 lines)

**Implementation**:
```python
# edge_lake/mcp_server/transport/__init__.py
"""MCP transport layer implementations."""

from .sse_handler import SSETransport

__all__ = ['SSETransport']
```

```python
# edge_lake/mcp_server/transport/sse_handler.py
"""
SSE Transport for MCP Server

Implements Server-Sent Events transport integrated with http_server.py.
"""

import json
import logging
import time
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class SSETransport:
    """
    SSE transport for MCP protocol.
    Integrates with http_server.py's ChunkedHTTPRequestHandler.
    """

    def __init__(self, mcp_server):
        """
        Initialize SSE transport.

        Args:
            mcp_server: Reference to MCPServer instance
        """
        self.mcp_server = mcp_server
        self.connections = {}  # session_id -> handler
        self.message_counter = 0

    def handle_sse_endpoint(self, handler):
        """
        Handle GET /mcp/sse - establish SSE connection.

        Args:
            handler: ChunkedHTTPRequestHandler instance
        """
        # TODO: Implement SSE connection setup
        pass

    def handle_messages_endpoint(self, handler):
        """
        Handle POST /mcp/messages/* - receive MCP messages.

        Args:
            handler: ChunkedHTTPRequestHandler instance
        """
        # TODO: Implement message reception and routing
        pass

    def send_event(self, session_id: str, event_type: str, data: Dict[str, Any]):
        """
        Send SSE event to client.

        Args:
            session_id: Session identifier
            event_type: Event type (message, error, etc.)
            data: Event data (will be JSON-encoded)
        """
        # TODO: Implement SSE event sending
        pass
```

**Acceptance Criteria**:
- [ ] SSETransport class created with skeleton methods
- [ ] Basic SSE connection establishment working
- [ ] Message routing from POST to MCP server
- [ ] Event sending to client via SSE

**Estimated Time**: 2 days

---

#### 1.2 Modify http_server.py for MCP Endpoints
**File**: `edge_lake/tcpip/http_server.py`

**Changes**:
1. Import SSE handler in relevant methods
2. Add routing logic for `/mcp/sse` and `/mcp/messages/*`
3. Preserve all existing REST functionality

**Implementation** (add to ChunkedHTTPRequestHandler):
```python
def do_GET(self):
    # Add MCP routing before existing REST handling
    if self.path == '/mcp/sse':
        # Handle SSE connection establishment
        try:
            from edge_lake.mcp.transport import sse_handler
            if sse_handler.handle_sse_endpoint(self):
                return
        except ImportError:
            # MCP not available, continue with normal handling
            pass
        except Exception as e:
            logger.error(f"MCP SSE endpoint error: {e}")

    # ... existing REST handling continues ...

def do_POST(self):
    # Add MCP routing before existing REST handling
    if self.path.startswith('/mcp/messages/'):
        # Handle MCP message
        try:
            from edge_lake.mcp.transport import sse_handler
            if sse_handler.handle_messages_endpoint(self):
                return
        except ImportError:
            # MCP not available, continue with normal handling
            pass
        except Exception as e:
            logger.error(f"MCP messages endpoint error: {e}")

    # ... existing REST handling continues ...
```

**Acceptance Criteria**:
- [ ] `/mcp/sse` endpoint accessible via GET
- [ ] `/mcp/messages/*` endpoint accessible via POST
- [ ] Existing REST endpoints unaffected
- [ ] Error handling for MCP unavailable

**Estimated Time**: 1 day

---

#### 1.3 Refactor MCP Server Class
**File**: `edge_lake/mcp/server/__init__.py`
**File**: `edge_lake/mcp/server/mcp_server.py`

**Changes**:
1. Remove Starlette/Uvicorn dependencies
2. Remove `run_sse_server()` method
3. Add `start()` and `stop()` methods for lifecycle
4. Keep protocol handlers (list_tools, call_tool)

**Implementation**:
```python
# edge_lake/mcp_server/server/mcp_server.py
"""
MCP Server integrated with EdgeLake http_server.py
"""

import logging
from typing import List, Dict, Any, Optional

# MCP imports
try:
    from mcp.server import Server
    from mcp.types import TextContent, Tool
    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False
    logger.warning("MCP library not available")

from ..core import QueryBuilder, QueryExecutor
from ..core.direct_client import EdgeLakeDirectClient
from ..tools import ToolGenerator, ToolExecutor
from ..config import Config

logger = logging.getLogger(__name__)


class MCPServer:
    """
    MCP Server integrated with EdgeLake's http_server.py.
    """

    def __init__(self, config_dir: str = None, capabilities: dict = None):
        """
        Initialize MCP server.

        Args:
            config_dir: Configuration directory path
            capabilities: Node capabilities dictionary
        """
        self.capabilities = capabilities or {}

        # Load configuration
        self.config = Config(config_dir) if config_dir else Config()

        # Initialize direct client
        self.client = EdgeLakeDirectClient(
            max_workers=self.config.get_max_workers()
        )

        # Initialize builders
        self.query_builder = QueryBuilder()
        self.query_executor = QueryExecutor()

        # Initialize tools
        self.tool_generator = ToolGenerator(
            self.config.get_all_tools()
        )
        self.tool_executor = ToolExecutor(
            self.client,
            self.query_builder,
            self.query_executor,
            self.config
        )

        # Initialize MCP protocol server
        if MCP_AVAILABLE:
            self.server = Server("edgelake-mcp-server")
            self._register_handlers()
        else:
            self.server = None
            logger.error("MCP library not available")

        # SSE transport (will be set by http_server)
        self.transport = None

        logger.info("MCP Server initialized")

    def _register_handlers(self):
        """Register MCP protocol handlers."""

        @self.server.list_tools()
        async def list_tools():
            """List available MCP tools."""
            logger.debug("Listing tools")
            tools = self.tool_generator.generate_tools()

            # Convert to MCP Tool objects
            mcp_tools = []
            for tool_dict in tools:
                mcp_tools.append(Tool(
                    name=tool_dict['name'],
                    description=tool_dict['description'],
                    inputSchema=tool_dict['inputSchema']
                ))

            logger.debug(f"Returning {len(mcp_tools)} tools")
            return mcp_tools

        @self.server.call_tool()
        async def call_tool(name: str, arguments: dict):
            """Execute a tool."""
            logger.debug(f"Calling tool '{name}' with arguments: {arguments}")

            try:
                result = await self.tool_executor.execute_tool(name, arguments)

                # Convert to MCP TextContent
                mcp_result = []
                for item in result:
                    mcp_result.append(TextContent(
                        type="text",
                        text=item['text']
                    ))

                return mcp_result

            except Exception as e:
                logger.error(f"Error calling tool '{name}': {e}", exc_info=True)
                return [TextContent(
                    type="text",
                    text=f"Error: {str(e)}"
                )]

    def start(self):
        """
        Start MCP server (register with http_server).
        Called by 'run mcp server' command.
        """
        if not MCP_AVAILABLE:
            raise RuntimeError("MCP library not available")

        # Initialize SSE transport
        from ..transport.sse_handler import SSETransport
        self.transport = SSETransport(self)

        logger.info("MCP Server started")

    def stop(self):
        """
        Stop MCP server and cleanup.
        Called by 'exit mcp server' command.
        """
        logger.info("Shutting down MCP Server")

        # Close direct client
        if self.client:
            self.client.close()

        # Cleanup transport
        if self.transport:
            self.transport = None

    def process_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process incoming MCP message.

        Args:
            message: JSON-RPC message

        Returns:
            JSON-RPC response
        """
        # Route to appropriate handler based on method
        method = message.get('method')

        if method == 'tools/list':
            # Call list_tools handler
            pass
        elif method == 'tools/call':
            # Call call_tool handler
            pass
        else:
            return {
                "jsonrpc": "2.0",
                "id": message.get('id'),
                "error": {
                    "code": -32601,
                    "message": f"Method not found: {method}"
                }
            }
```

**Acceptance Criteria**:
- [ ] MCPServer class refactored without Starlette/Uvicorn
- [ ] `start()` and `stop()` methods implemented
- [ ] Protocol handlers (list_tools, call_tool) preserved
- [ ] Integration with SSETransport working

**Estimated Time**: 2 days

---

#### 1.4 Update member_cmd.py Integration
**File**: `edge_lake/cmd/member_cmd.py`

**Changes**:
1. Add `run mcp server` command handler
2. Add `exit mcp server` command handler
3. Initialize MCP server on demand

**Implementation**:
```python
# In member_cmd.py, add command handlers

def run_mcp_server(status, io_buff_in, cmd_words, trace):
    """
    Start MCP server integrated with HTTP server.

    Command: run mcp server where port = 50051 and enabled = true
    """
    # Parse parameters
    params = interpreter.get_multiple_values(cmd_words, ["port", "enabled"], [50051, True])

    if not params.get("enabled"):
        status.add_error("MCP server not enabled")
        return process_status.MCP_not_enabled

    try:
        from edge_lake.mcp.server import MCPServer

        # Create MCP server instance
        global mcp_server_instance
        mcp_server_instance = MCPServer()

        # Start server (registers with http_server)
        mcp_server_instance.start()

        logger.info(f"MCP server started")
        return process_status.SUCCESS

    except ImportError:
        status.add_error("MCP dependencies not available")
        return process_status.ERR_process_failure
    except Exception as e:
        status.add_error(f"Failed to start MCP server: {e}")
        return process_status.ERR_process_failure


def exit_mcp_server(status, io_buff_in, cmd_words, trace):
    """
    Stop MCP server.

    Command: exit mcp server
    """
    global mcp_server_instance

    if mcp_server_instance is None:
        status.add_error("MCP server not running")
        return process_status.MCP_not_running

    try:
        mcp_server_instance.stop()
        mcp_server_instance = None

        logger.info("MCP server stopped")
        return process_status.SUCCESS

    except Exception as e:
        status.add_error(f"Failed to stop MCP server: {e}")
        return process_status.ERR_process_failure


# Register commands
commands = {
    # ... existing commands ...
    "run mcp server": {
        "function": run_mcp_server,
        "trace": 0,
    },
    "exit mcp server": {
        "function": exit_mcp_server,
        "trace": 0,
    },
}
```

**Acceptance Criteria**:
- [ ] `run mcp server` command starts MCP server
- [ ] `exit mcp server` command stops MCP server
- [ ] Error handling for missing dependencies
- [ ] Server instance managed globally

**Estimated Time**: 1 day

---

#### 1.5 End-to-End Testing
**Create**: `edge_lake/mcp_server/tests/test_integration.py` (Phase 3)

**Tests**:
1. MCP server startup via `run mcp server` command
2. SSE connection establishment (GET /mcp/sse)
3. Tool listing (list_tools)
4. Tool execution (call_tool with simple query)
5. MCP server shutdown via `exit mcp server` command

**Acceptance Criteria**:
- [ ] All integration tests passing
- [ ] No regressions in existing REST API
- [ ] Logs show correct MCP lifecycle

**Estimated Time**: 2 days

---

### Phase 1 Deliverables
- [x] Design document complete
- [ ] SSE transport implementation (`sse_handler.py`)
- [ ] http_server.py modifications for MCP endpoints
- [ ] Refactored MCPServer class
- [ ] member_cmd.py integration
- [ ] Basic end-to-end test passing

**Total Time**: 1 week (8 days with buffer)

---

## Phase 2: Block Transport (Future - Not Yet Implemented)

### Goal
Add block transport support for large query results (>10MB).

### Tasks

#### 2.1 Create Block Transport Adapter
**File**: `edge_lake/mcp_server/transport/block_transport.py`

**Implementation**:
```python
"""
Block Transport for Large MCP Responses

Integrates with message_server.py for chunked data transfer.
"""

import logging
from typing import List, Optional

logger = logging.getLogger(__name__)


class BlockTransportAdapter:
    """
    Adapter for message_server.py block transport.
    Used when query results exceed size threshold.
    """

    def __init__(self, block_threshold: int = 10485760):
        """
        Initialize block transport adapter.

        Args:
            block_threshold: Size threshold in bytes (default 10MB)
        """
        self.block_threshold = block_threshold

        # Import message_server
        try:
            from edge_lake.tcpip import message_server
            self.message_server = message_server
        except ImportError:
            logger.error("message_server not available")
            self.message_server = None

    def should_use_block_transport(self, data_size: int) -> bool:
        """
        Determine if block transport should be used.

        Args:
            data_size: Size of data in bytes

        Returns:
            True if block transport should be used
        """
        return data_size > self.block_threshold

    def send_via_blocks(self, data: bytes, session_id: str) -> List[str]:
        """
        Send data via block transport.

        Args:
            data: Data to send
            session_id: Session identifier

        Returns:
            List of block IDs
        """
        # TODO: Implement using message_server.py
        pass

    def receive_blocks(self, block_ids: List[str]) -> bytes:
        """
        Receive and reassemble blocks.

        Args:
            block_ids: List of block IDs to retrieve

        Returns:
            Reassembled data
        """
        # TODO: Implement using message_server.py
        pass
```

**Acceptance Criteria**:
- [ ] BlockTransportAdapter class created
- [ ] Integration with message_server.py working
- [ ] Block sending and receiving functional

**Estimated Time**: 2 days

---

#### 2.2 Integrate Block Transport with Query Executor
**File**: `edge_lake/mcp/core/query_executor.py`

**Changes**:
1. Add result size estimation
2. Add block transport selection logic
3. Modify streaming executor to support blocks

**Implementation**:
```python
# In QueryExecutor class

async def execute_query(
    self,
    dbms_name: str,
    sql_query: str,
    mode: str = "auto",
    fetch_size: int = 100,
    use_block_transport: bool = True
) -> Any:
    """
    Execute query with block transport support.
    """
    # ... existing validation ...

    # Estimate result size
    estimated_size = self._estimate_result_size(dbms_name, validated_sql)

    # Determine if block transport needed
    if use_block_transport and estimated_size > 10485760:
        # Use block transport
        return await self._execute_with_blocks(
            dbms_name,
            validated_sql,
            select_parsed
        )
    else:
        # Use standard streaming/batch
        # ... existing logic ...
```

**Acceptance Criteria**:
- [ ] Result size estimation working
- [ ] Block transport selection logic correct
- [ ] Query execution with blocks functional

**Estimated Time**: 2 days

---

#### 2.3 Update SSE Handler for Block Transport
**File**: `edge_lake/mcp/transport/sse_handler.py`

**Changes**:
1. Add block ID notification via SSE
2. Add block request handling
3. Add block reassembly on client side

**Implementation**:
```python
def send_large_result_via_blocks(self, session_id: str, data: bytes):
    """
    Send large result using block transport.

    Args:
        session_id: Session identifier
        data: Data to send
    """
    # Send data via blocks
    block_ids = self.block_transport.send_via_blocks(data, session_id)

    # Notify client of block IDs via SSE
    self.send_event(session_id, "blocks_available", {
        "block_ids": block_ids,
        "total_size": len(data)
    })
```

**Acceptance Criteria**:
- [ ] Block ID notification via SSE working
- [ ] Client can request blocks
- [ ] Block reassembly functional

**Estimated Time**: 2 days

---

#### 2.4 Configuration and Tuning
**File**: `edge_lake/mcp/config/__init__.py`

**Changes**:
1. Add `block_transport_enabled` setting
2. Add `block_threshold` setting
3. Add `block_size` setting

**Implementation**:
```python
class Config:
    def __init__(self, config_dir: str = None):
        # ... existing config ...

        # Block transport settings
        self.block_transport_enabled = True
        self.block_threshold = 10485760  # 10MB
        self.block_size = 1048576  # 1MB per block
```

**Acceptance Criteria**:
- [ ] Configuration options available
- [ ] Tunable thresholds working
- [ ] Documentation updated

**Estimated Time**: 1 day

---

#### 2.5 Performance Testing
**Create**: `edge_lake/mcp/tests/test_performance.py`

**Tests**:
1. Query with result < 10MB (should use SSE)
2. Query with result > 10MB (should use blocks)
3. Concurrent queries with mixed sizes
4. Block transport overhead measurement

**Acceptance Criteria**:
- [ ] Block transport used for large results
- [ ] Overhead < 10% vs standard streaming
- [ ] Performance baseline established

**Estimated Time**: 2 days

---

### Phase 2 Deliverables
- [ ] Block transport adapter (`block_transport.py`)
- [ ] Query executor integration
- [ ] SSE handler block support
- [ ] Configuration options
- [ ] Performance tests passing

**Total Time**: 1 week (9 days with buffer)

---

## Phase 3: Testing & Documentation (Future - Planned)

### Goal
Comprehensive testing, performance benchmarking, and documentation.

### Tasks

#### 3.1 Unit Tests
**Files**:
- `edge_lake/mcp/tests/test_sse_handler.py`
- `edge_lake/mcp/tests/test_mcp_server.py`
- `edge_lake/mcp/tests/test_block_transport.py`

**Coverage Targets**:
- SSE transport: >80%
- MCP server: >80%
- Block transport: >80%
- Query executor: >80% (already exists)

**Estimated Time**: 3 days

---

#### 3.2 Integration Tests
**Files**:
- `edge_lake/mcp/tests/test_http_integration.py`
- `edge_lake/mcp/tests/test_end_to_end.py`

**Test Scenarios**:
1. Multiple concurrent MCP clients
2. Mixed REST API and MCP traffic
3. Authentication and authorization
4. Error handling and recovery
5. Graceful degradation (block transport unavailable)

**Estimated Time**: 2 days

---

#### 3.3 Performance Benchmarking
**File**: `edge_lake/mcp/tests/benchmark.py`

**Benchmarks**:
1. Query latency (p50, p95, p99)
2. Throughput (queries/second)
3. Memory usage during streaming
4. Block transport overhead
5. Concurrent connection handling

**Estimated Time**: 2 days

---

#### 3.4 Documentation Updates
**Files**:
- `edge_lake/mcp/README.md` (user-facing)
- `edge_lake/mcp/DESIGN.md` (update with actuals)
- `CLAUDE.md` (update with final implementation)

**Content**:
1. Installation and setup
2. Configuration options
3. Usage examples
4. Troubleshooting guide
5. API reference

**Estimated Time**: 2 days

---

#### 3.5 Migration Guide
**File**: `edge_lake/mcp/MIGRATION.md`

**Content**:
1. Differences from POC
2. Configuration changes
3. Breaking changes (if any)
4. Step-by-step migration

**Estimated Time**: 1 day

---

### Phase 3 Deliverables
- [ ] Test coverage > 80%
- [ ] Performance baseline documented
- [ ] All documentation updated
- [ ] Migration guide complete

**Total Time**: 1 week (10 days with buffer)

---

## Phase 4: Production Deployment (Future - Planned)

### Goal
Deploy to production with monitoring and operational readiness.

### Tasks

#### 4.1 Staging Deployment
**Environment**: Staging EdgeLake cluster

**Steps**:
1. Deploy refactored MCP server
2. Configure monitoring (metrics, logs)
3. Run smoke tests
4. Performance validation

**Acceptance Criteria**:
- [ ] Staging deployment successful
- [ ] All smoke tests passing
- [ ] Performance meets targets

**Estimated Time**: 2 days

---

#### 4.2 Monitoring Setup
**Tools**: Existing EdgeLake monitoring infrastructure

**Metrics to Track**:
- MCP request count/rate
- Query latency (p50, p95, p99)
- Block transport usage
- Error rate
- SSE connection count
- Workers pool utilization

**Acceptance Criteria**:
- [ ] All metrics visible in dashboards
- [ ] Alerts configured for anomalies

**Estimated Time**: 1 day

---

#### 4.3 Operational Runbook
**File**: `edge_lake/mcp/RUNBOOK.md`

**Content**:
1. Starting/stopping MCP server
2. Configuration tuning
3. Common issues and solutions
4. Escalation procedures
5. Rollback process

**Acceptance Criteria**:
- [ ] Runbook complete and reviewed
- [ ] Operations team trained

**Estimated Time**: 1 day

---

#### 4.4 Production Rollout
**Strategy**: Gradual rollout with canary deployment

**Steps**:
1. Deploy to 10% of production nodes
2. Monitor for 24 hours
3. If stable, deploy to 50% of nodes
4. Monitor for 24 hours
5. Deploy to 100% of nodes

**Acceptance Criteria**:
- [ ] Zero downtime migration
- [ ] No increase in error rate
- [ ] Performance within targets

**Estimated Time**: 4 days (including monitoring windows)

---

### Phase 4 Deliverables
- [ ] Staging deployment validated
- [ ] Monitoring dashboards live
- [ ] Operational runbook complete
- [ ] Production rollout successful

**Total Time**: 1 week (8 days)

---

## Risk Mitigation

### Technical Risks

| Risk | Mitigation | Owner |
|------|------------|-------|
| http_server.py modification breaks REST | Comprehensive regression tests, feature flag | Dev Team |
| Block transport adds latency | Performance testing, configurable thresholds | Dev Team |
| SSE compatibility issues | Test with multiple MCP clients (Claude, others) | QA Team |
| Workers pool contention | Load testing, configurable pool size | Dev Team |

### Operational Risks

| Risk | Mitigation | Owner |
|------|------------|-------|
| Production deployment failure | Canary deployment, rollback plan | Ops Team |
| Monitoring gaps | Pre-deployment monitoring validation | Ops Team |
| Knowledge transfer | Documentation, runbook, training | Dev/Ops Teams |

## Success Metrics

### Functional Metrics
- [ ] All MCP protocol features working (list_tools, call_tool)
- [ ] Block transport operational for results > 10MB
- [ ] Authentication and authorization functional

### Performance Metrics
- [ ] Query latency < 500ms (p95, excluding data fetch)
- [ ] Support > 100 concurrent MCP connections
- [ ] Block transport overhead < 10%

### Quality Metrics
- [ ] Test coverage > 80%
- [ ] Zero production incidents in first week
- [ ] All documentation complete

## Timeline Summary

| Phase | Duration | Start | End |
|-------|----------|-------|-----|
| Phase 1: Core Integration | 8 days | Week 1 Day 1 | Week 1 Day 8 |
| Phase 2: Block Transport | 9 days | Week 2 Day 1 | Week 2 Day 9 |
| Phase 3: Testing & Docs | 10 days | Week 3 Day 1 | Week 3 Day 10 |
| Phase 4: Production | 8 days | Week 4 Day 1 | Week 4 Day 8 |
| **Total** | **35 days** | **Week 1** | **Week 4** |

## Next Steps

1. Review and approve this implementation plan
2. Assign owners for each phase
3. Set up project tracking (GitHub issues/project board)
4. Begin Phase 1, Task 1.1 (Create SSE Transport Layer)

## Appendix: Task Dependencies

```
Phase 1:
  1.1 (SSE Transport) ─┐
  1.2 (http_server)   ─┼─→ 1.5 (Testing)
  1.3 (MCP Server)    ─┤
  1.4 (member_cmd)    ─┘

Phase 2:
  2.1 (Block Adapter)  ─┬─→ 2.5 (Performance Testing)
  2.2 (Query Executor) ─┤
  2.3 (SSE Handler)    ─┤
  2.4 (Configuration)  ─┘

Phase 3:
  3.1 (Unit Tests)     ─┐
  3.2 (Integration)    ─┼─→ 3.4 (Documentation)
  3.3 (Benchmarking)   ─┤      └─→ 3.5 (Migration)
                        └──────────┘

Phase 4:
  4.1 (Staging) ─→ 4.2 (Monitoring) ─→ 4.3 (Runbook) ─→ 4.4 (Production)
```

## Change Log

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-10-30 | EdgeLake Team | Initial implementation plan |
