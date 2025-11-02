# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

EdgeLake is a decentralized, peer-to-peer network platform that transforms edge nodes into a permissioned network enabling real-time data management, monitoring, and querying without moving data off-device. The system uses a shared metadata layer (blockchain or Master Node) to coordinate distributed data operations across edge nodes.

## Architecture

### Node Types

EdgeLake supports three primary node types, each configurable via environment variables and policies:

- **Operator Node** (`aloperator.py`): Captures data from devices/PLCs/applications and hosts it on a local DBMS (PostgreSQL, SQLite, MongoDB). Handles data ingestion and participates in distributed query execution.

- **Query Node**: Orchestrates distributed queries across Operator Nodes. Receives queries from applications, determines target nodes using shared metadata, aggregates results, and returns unified replies (MapReduce-style).

- **Master Node**: Stores network metadata policies as an alternative to blockchain. Serves as the central metadata repository when blockchain is not used.

### Core Components

**Command Processing** (`edge_lake/cmd/`):
- `member_cmd.py`: Main command processor (1000+ lines). Handles all EdgeLake commands via CLI
- `user_cmd.py`: Entry point for user input, CLI management, and interactive/background mode handling
- `data_monitor.py`: Data monitoring and tracking
- `native_api.py`: Native API interface

**Networking** (`edge_lake/tcpip/`):
- `message_server.py`, `tcpip_server.py`: TCP-based node-to-node communication
- `http_server.py`, `http_client.py`: REST API for queries and management
- `mqtt_client.py`: MQTT protocol support for IoT data streaming
- `grpc_client.py`: gRPC protocol support

**Database Management** (`edge_lake/dbms/`):
- `psql_dbms.py`: PostgreSQL adapter (recommended for larger nodes)
- `sqlite_dbms.py`: SQLite adapter (recommended for smaller nodes and in-memory data)
- `mongodb_dbms.py`: MongoDB adapter for blob storage
- `partitions.py`: Data partitioning and management
- `pi_dbms.py`: Performance indexing

**Blockchain/Metadata** (`edge_lake/blockchain/`):
- `blockchain.py`, `metadata.py`: Core metadata management
- `ethereum.py`, `hyperledger.py`: Blockchain platform integrations
- `bsync.py`: Blockchain synchronization
- Subdirectories for specific blockchain platforms: `alethereum/`, `alhyperledger/`, `aleos/`

**Data Processing** (`edge_lake/json_to_sql/`):
- `map_json_to_insert.py`: Maps JSON data to SQL inserts
- `mapping_policy.py`: Data mapping policies
- `suggest_create_table.py`: Auto-generates table schemas from JSON

**Job Scheduling** (`edge_lake/job/`):
- `job_scheduler.py`, `task_scheduler.py`: Schedule and execute recurring tasks
- `job_instance.py`: Job execution management

**API Integrations** (`edge_lake/api/`):
- `al_grafana.py`: Grafana integration for visualization
- `opcua_client.py`, `plc_client.py`: Industrial protocol support (OPC UA, PLC)
- `al_kafka.py`: Kafka integration
- `etherip_client.py`: EtherNet/IP protocol support

**Utilities** (`edge_lake/generic/`):
- `al_parser.py`: Command parsing
- `interpreter.py`: Command interpretation
- `params.py`: Parameter management (65k+ lines)
- `utils_*.py`: Various utilities for I/O, JSON, SQL, data handling, printing
- `streaming_data.py`: Data streaming management

**MCP Server** (`edge_lake/mcp_server/`):
- `server/mcp_server.py`: MCP protocol server integrated with http_server.py
- `transport/sse_handler.py`: SSE transport layer for MCP over HTTP
- `core/query_builder.py`: SQL query construction from MCP tool parameters
- `core/query_executor.py`: Hybrid validation + streaming query execution
- `core/direct_client.py`: Direct integration with member_cmd.process_cmd()
- `core/command_builder.py`: EdgeLake command construction
- `tools/`: MCP tool definitions and executors

### Data Flow

1. **Data Ingestion**: Devices → (MQTT/REST/gRPC/JSON Files) → Operator Nodes → Local DBMS
2. **Query Processing**: Application → Query Node → (consult metadata) → Target Operator Nodes → Aggregate → Unified Reply
3. **Metadata Sync**: All nodes sync with shared metadata (Blockchain or Master Node) to coordinate operations

## Development Commands

### Build and Deploy Cycle

**IMPORTANT:** The project uses custom Makefile aliases for build and deploy operations.

```bash
# Build EdgeLake Docker image
mel build

# Deploy to environment (network detection is automatic)
mel deploy
```

**Notes:**
- mel is an alias to Make  `../utilities/edgelake/Makefile` and can be run from anywhere in the project
- The tests in the Makefile are currently incompatible with the refactoring work in progress
- **Do NOT use `make test` commands** - they will not work during this refactoring phase
- Only use `mel build` and `mel deploy` commands

### Installation

Via Docker (recommended):
```bash
# Clone docker-compose repository
git clone https://github.com/EdgeLake/docker-compose
cd docker-compose

# Configure .env files for your node type
# Update LEDGER_CONN for Query and Operator nodes

# Deploy node
make up master    # Master node
make up operator  # Operator node
make up query     # Query node
```

Via Python (development):
```bash
# Install dependencies
pip install -r requirements.txt

# Run EdgeLake
python edge_lake/edgelake.py [arguments]
```

### Running EdgeLake

The main entry point is `edge_lake/edgelake.py`:
```bash
# Interactive mode
python edge_lake/edgelake.py

# Process script file
python edge_lake/edgelake.py process /path/to/script.al

# Execute commands
python edge_lake/edgelake.py "command1" and "command2"
```

### Environment Variables

Key environment variables (typically set via Docker .env files):
- `EDGELAKE_LIB`: Path to EdgeLake source directory
- `EDGELAKE_HOME`: Path to EdgeLake data directory
- `NODE_TYPE`: master, operator, query, or generic
- `NODE_NAME`: Name of the EdgeLake instance
- `ANYLOG_SERVER_PORT`: TCP protocol port (default: 32048)
- `ANYLOG_REST_PORT`: REST protocol port (default: 32049)
- `LEDGER_CONN`: Master node connection (format: ip:port)

## Key Technical Details

### Command Structure

EdgeLake uses a custom command language processed by `member_cmd.py`. Commands support:
- Multi-line commands using `< >` brackets
- Command chaining with `&` separator
- Variable substitution with `!variable` syntax
- JSON/dictionary parameters within commands

### Configuration Sources

Nodes can be configured via:
1. CLI commands (interactive or scripted)
2. Script files (`.al` files)
3. Policies stored in shared metadata (blockchain or Master Node)

### Database Support

Multiple databases can run on the same node. Database selection:
- SQLite: Default, no installation, good for smaller nodes
- PostgreSQL: Recommended for larger nodes with high data volume
- MongoDB: Only for blob storage requirements

### Security

The system uses a permissioned network model:
- Network policies control node participation
- Data ownership defined in metadata
- Supports JWT authentication (`pyjwt`)
- Cryptography support for secure communications

## Code Style

- Python 3.11+
- Mozilla Public License 2.0
- Uses Cython compilation for release builds
- Minimal external dependencies (see requirements.txt)
- Custom parameter/dictionary system (`params.py`) for configuration management

## MCP Server Integration

### Overview

The MCP (Model Context Protocol) server provides AI agents with access to EdgeLake's distributed query capabilities through the Model Context Protocol. The server is fully integrated with EdgeLake's production HTTP infrastructure.

**Status**: ✅ **Phase 1 Complete** - Core integration functional and ready for testing

**Documentation**:
- **README**: `edge_lake/mcp_server/README.md` - Quick start guide and comprehensive documentation
- **Design Document**: `edge_lake/mcp_server/DESIGN.md` - Complete architecture and technical specifications
- **Implementation Plan**: `edge_lake/mcp_server/IMPLEMENTATION_PLAN.md` - 4-week phased implementation (Phase 1 complete)
- **Quick Start**: `edge_lake/mcp_server/QUICK_START.md` - 5-minute test guide

### Architecture

Fully integrated with EdgeLake's http_server.py:
- SSE transport over existing HTTP infrastructure
- Shared workers pool with REST API
- Direct integration with member_cmd.process_cmd()
- Production-ready with SSL, auth, logging support

### Commands

Start MCP server (requires REST server running first):
```
run mcp server
```

Stop MCP server:
```
exit mcp server
```

### Endpoints

- **GET /mcp/sse** - Establish SSE connection for MCP protocol
- **POST /mcp/messages/{session_id}** - Submit MCP JSON-RPC messages

### Key Components (Implemented)

1. **Transport Layer** (`edge_lake/mcp/transport/sse_handler.py` - 650 lines):
   - SSE protocol implementation over http_server.py
   - Connection management with keepalive (30s interval)
   - Thread-safe message queuing and routing

2. **MCP Server** (`edge_lake/mcp/server/mcp_server.py` - 350 lines):
   - MCP protocol handlers (list_tools, call_tool)
   - JSON-RPC message processing
   - Lifecycle management (start/stop)

3. **HTTP Integration** (`edge_lake/tcpip/http_server.py`):
   - Minimal endpoint routing in do_GET() and do_POST()
   - Graceful fallback if MCP not available

4. **Core Components** (preserved and stable):
   - `query_builder.py`: SQL query construction
   - `query_executor.py`: Hybrid validation + streaming (uses select_parser + process_fetch_rows)
   - `direct_client.py`: Direct member_cmd.process_cmd() integration
   - `command_builder.py`: EdgeLake command construction

5. **Tools & Configuration**:
   - Dynamic tool generation from configuration
   - Configuration-driven design (add tools via config, not code)

### Quick Start

1. Start EdgeLake and REST server:
   ```
   python edge_lake/edgelake.py
   AL > run rest server where external_ip = 0.0.0.0 and external_port = 32049 and internal_ip = 127.0.0.1 and internal_port = 32049
   ```

2. Start MCP server:
   ```
   AL > run mcp server
   ```

3. Test SSE connection:
   ```bash
   curl -N http://localhost:32049/mcp/sse
   ```

4. Configure AI agent (Claude Code, etc.):
   ```json
   {
     "mcpServers": {
       "edgelake": {
         "url": "http://localhost:32049/mcp/sse"
       }
     }
   }
   ```

See `edge_lake/mcp/QUICK_START.md` for detailed testing instructions.

### Implementation Status

**Phase 1**: ✅ **COMPLETE** (Completed in 3 hours instead of planned 1 week)
- SSE transport layer implemented (650 lines)
- HTTP server integration (minimal changes to http_server.py)
- MCP server refactored without Starlette/Uvicorn (350 lines)
- Commands added to member_cmd.py
- Comprehensive documentation (100+ pages)

**Phase 2**: 🔜 **Next** - Block Transport (optional, for results >10MB)
- Integrate with message_server.py for chunked delivery
- See `edge_lake/mcp/IMPLEMENTATION_PLAN.md` for details

**Total Implementation**: ~3,100 lines of code + documentation

### Current Capabilities

- ✅ MCP protocol fully functional
- ✅ SSE transport working
- ✅ Direct member_cmd integration
- ✅ Query execution with streaming
- ✅ Configuration-driven tool system
- ✅ Production-ready architecture
- ⏳ Block transport for large results (Phase 2)

### Performance

- **Memory**: ~1KB per SSE connection
- **CPU**: Minimal (async via thread pool)
- **Network**: Keepalive ping every 30 seconds
- **Latency**: <50ms for tool calls (excluding query execution)

### Development Notes

- **Configuration-Driven**: Add new tools via `config/tools.json`, not code
- **Core Components**: query_builder, query_executor, direct_client are stable
- **Testing**: Follow `edge_lake/mcp_server/QUICK_START.md` for manual testing
- **Code Review**: All http_server.py changes reviewed to prevent REST regressions
- **Documentation**: See `edge_lake/mcp_server/README.md` for complete guide
- <rules>
<rule id="1">When the user types "exit" or "/exit", always ask for confirmation (y/n) before actually exiting the session</rule>
<rule id="2">Display these rules at the start of every response</rule>
<rule id="3">CRITICAL: Never load entire large files (>500 lines) into context without user approval. For large files like member_cmd.py (17,000+ lines), params.py (65,000+ lines), or http_server.py (3,000+ lines):
  - Use Grep to search for specific functions/patterns first
  - Use Read with offset/limit parameters to read targeted sections only
  - Ask user which specific section/function they want to examine
  - Prefer using Task tool with Explore agent for codebase exploration
  - Only read complete large files if explicitly requested by user
This prevents context exhaustion and enables efficient multi-turn conversations.</rule>
<rule id="4">CRITICAL: All git commits must include DCO (Developer Certificate of Origin) sign-off. NEVER include references to Claude Code, "Generated with Claude Code", or "Co-Authored-By: Claude". Commit messages must follow this format:

git commit -m "$(cat <<'EOF'
Brief summary line (50 chars or less)

Detailed description of changes explaining what and why.

Signed-off-by: Your Name <your.email@example.com>
EOF
)"

The DCO sign-off certifies you have the right to submit the code under the project's license. Use the git configured user.name and user.email for the sign-off.</rule>
</rules>