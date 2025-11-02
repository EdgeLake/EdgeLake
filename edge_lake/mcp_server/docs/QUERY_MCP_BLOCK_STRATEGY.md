# Query, MCP Format, and Block Transport Strategy

**Version**: 1.0
**Date**: 2025-11-02
**Status**: Design Document
**Purpose**: Comprehensive strategy for handling query formats, MCP integration, and large result transport across EdgeLake's distributed architecture

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Query Execution Modes](#query-execution-modes)
3. [Format Parameter Strategy](#format-parameter-strategy)
4. [Network Query Flow](#network-query-flow)
5. [Local Query Flow](#local-query-flow)
6. [MCP Format Handling](#mcp-format-handling)
7. [Block Transport Strategy](#block-transport-strategy)
8. [Implementation Roadmap](#implementation-roadmap)

---

## Architecture Overview

### Node Types and Roles

EdgeLake supports three primary node types with distinct query processing responsibilities:

1. **Operator Node**
   - Hosts data in local DBMS (PostgreSQL, SQLite, MongoDB)
   - Executes queries against local data
   - Returns results to Query Node or directly to client
   - **Key Constraint**: Must handle both local queries (CLI/REST/MCP) AND network queries (from Query Node)

2. **Query Node**
   - Receives queries from clients (CLI/REST/MCP)
   - Determines target Operator Nodes using metadata
   - Forwards queries to Operator Nodes
   - Consolidates/aggregates results from multiple Operators
   - Returns unified response to client

3. **Master Node**
   - Stores network metadata policies
   - Does not participate in query execution

### Query Processing Flow

```
Client Request
    ↓
Query Node (if network query)
    ↓
[Distribute to Operator Nodes]
    ↓
Operator Nodes (execute locally)
    ↓
[Return to Query Node]
    ↓
Query Node (consolidate)
    ↓
Return to Client
```

---

## Query Execution Modes

EdgeLake queries execute in two distinct modes based on the `destination` parameter:

### 1. Local Execution (`destination=local` or no destination)

**Scenario**: Query executes on the current node's DBMS

**Use Cases**:
- Direct query to Operator Node
- Query Node querying its own local database
- CLI commands on any node

**Format Options**:
- `format=json` - JSON array of rows
- `format=table` - ASCII table (default for CLI)
- `format=mcp` - **NOT SUPPORTED** for SQL queries (metadata commands only)

**Example**:
```sql
sql mydb "SELECT * FROM readings WHERE device_id = 'sensor1'"
```

### 2. Network Execution (`destination=network`)

**Scenario**: Query Node forwards query to Operator Nodes for distributed execution

**Use Cases**:
- Distributed queries across multiple Operator Nodes
- Queries requiring consolidation/aggregation
- MapReduce-style operations

**Format Requirements**:
- **Operator → Query**: Must return `format=json` for consolidation
- **Query → Client**: Can return `json`, `table`, or **potentially** `mcp`

**Example**:
```sql
sql mydb and destination=network "SELECT AVG(temperature) FROM readings GROUP BY device_id"
```

---

## Format Parameter Strategy

### Current Implementation

The `format` parameter currently supports:

1. **`format=json`**: Returns JSON array of row objects
2. **`format=table`**: Returns ASCII table (CLI default)
3. **`format=mcp`**: Special format for metadata commands ONLY (not SQL)

### Critical Constraint: SQL Queries Do NOT Support format=mcp

**Why?**

From `MCP_FORMAT_DESIGN.md`:

> **CRITICAL DISTINCTION:**
> - **Metadata Commands** use `format = mcp`:
>   - `blockchain get table where format = mcp`
>   - `get version where format = mcp`
> - **SQL Queries** use `format = json`:
>   - `sql dbname format = json "SELECT ..."`
>   - SQL engine does NOT support `format = mcp`
>   - Using `format = mcp` in SQL will cause errors on operator nodes

The SQL engine on Operator Nodes does not understand `format=mcp`. It only supports:
- `format=json`
- `format=table`

### Actual JSON Format Structures

EdgeLake supports two JSON structures:

1. **`format=json`** (Standard - Object-Wrapped Array):
   ```json
   {
     "Query": [
       {"col1": "val1", "col2": "val2"},
       {"col1": "val3", "col2": "val4"}
     ]
   }
   ```
   - Array wrapped in object with `"Query"` key
   - Used by consolidation logic
   - Standard EdgeLake output format
   - Defined in `utils_sql.py:2095`: `data = "{\"" + output_prefix + "\":[{"`

2. **`format=mcp`** (Currently - Unwrapped Array):
   ```json
   [
     {"col1": "val1", "col2": "val2"},
     {"col1": "val3", "col2": "val4"}
   ]
   ```
   - **IMPORTANT**: Currently only works for **metadata commands**, NOT SQL queries
   - MCP server wraps this in `TextContent` at the transport layer
   - Cleaner for AI agents (no extra nesting)

### Format Naming Consideration

**TODO**: Consider renaming formats for clarity:
- `format=json-object` or `format=json` (current default - object-wrapped)
- `format=json-array` (new name for current `format=mcp` behavior)
- Keep `format=mcp` as alias for backward compatibility

**Rationale**:
- "mcp" is misleading - it's not MCP-specific, it's just an unwrapped array
- Makes it clearer what format you're getting
- Allows SQL queries to support unwrapped arrays if needed

### Format Handling by Context

| Context | Local Query (Operator) | Network Query (Op→Query) | Final Output (Query→Client) |
|---------|------------------------|--------------------------|----------------------------|
| **CLI** | `json` (object-wrapped), `table` | `json` (object-wrapped) | `json` (object-wrapped), `table` |
| **REST** | `json` (object-wrapped), `table` | `json` (object-wrapped) | `json` (object-wrapped), `table` |
| **MCP** | `json` (object-wrapped) | `json` (object-wrapped) | `json` (object-wrapped), then unwrapped by MCP server |

---

## Network Query Flow

### Phase 1: Query Node → Operator Nodes

When Query Node distributes a query to Operator Nodes:

1. **Query Node receives**:
   ```sql
   sql mydb format=table destination=network "SELECT * FROM readings LIMIT 2000"
   ```

2. **Query Node forwards to Operators**:
   ```sql
   sql mydb format=json "SELECT * FROM readings LIMIT 2000"
   ```
   - **Key**: Force `format=json` regardless of client's requested format
   - **Reason**: Consolidation logic requires JSON structure

3. **Operator executes locally**:
   - Runs SQL against local DBMS
   - Returns JSON object with "Query" key wrapping the array:
     ```json
     {
       "Query": [
         {"timestamp": "...", "device_id": "sensor1", "temperature": 72.5},
         {"timestamp": "...", "device_id": "sensor2", "temperature": 73.1},
         ...
       ]
     }
     ```
   - **Note**: `format=json` always wraps the array in `{"Query": [...]}` structure

### Phase 2: Query Node Consolidation

Query Node receives JSON results from multiple Operators in `{"Query": [...]}` format.

**✅ INVESTIGATION COMPLETE**: Consolidation logic **REQUIRES** object-wrapped JSON `{"Query": [...]}`

**Evidence**:

1. **Operator nodes send** `{"Query": [...]}` to Query Node
   - Source: `utils_sql.py:2095` - `data = "{\"" + output_prefix + "\":[{"`
   - `output_prefix` is always set to `"Query"` for query results

2. **Query Node stores results in "system_query" database**:
   - Function: `insert_query_rows()` (`member_cmd.py:12700`)
   - Gets data from Operator: `query_data = j_instance.get_returned_data(receiver_id, par_id)` (line 12709)
   - Converts to INSERT: `map_results_to_insert_main(status, local_table, query_data)` (line 12711)

3. **Consolidation parser requires object-wrapped format**:
   - Function: `map_results_to_insert_main()` (`map_results_to_insert.py:49`)
   - **Critical Code** (lines 62-64):
     ```python
     for key in json_object:  # only one key in the dictionary
         columns = list(json_object[key][0].keys())
         return create_insert(status, table_name, json_object[key], columns)
     ```
   - This code:
     - Expects `json_object` to be a **dict** with one key (e.g., `"Query"`)
     - Accesses `json_object[key]` to get the array
     - Extracts column names from first row: `json_object[key][0].keys()`
   - **Would FAIL** if given unwrapped array `[...]` because you can't iterate over keys of a list

4. **Final query from "system_query"**:
   - Function: `query_local_table()` (`member_cmd.py:12232`)
   - Queries consolidated data with `output_prefix="Query"`
   - Returns `{"Query": [...]}` to client

**Pass-Through Mode** (code_status = 3):

When queries don't require aggregation/consolidation, EdgeLake uses "pass-through" mode:

- **Function**: `deliver_rows()` (`member_cmd.py:4134`)
- **Behavior**: Results from Operators are sent directly to client **without** storing in "system_query"
- **Key Code** (line 4164-4246):
  ```python
  if is_pass_through:
      # Get the print format
      output_manager = output_data.OutputManager(...)
      # ...
      ret_val = output_manager.new_rows(status, None, query_data, row_offset, True)
  ```

**Format Handling in Pass-Through**:

Even in pass-through mode:
1. Operators still send `{"Query": [...]}` (line 4233: `query_data['Query']`)
2. `OutputManager` receives full `{"Query": [...]}`
3. **Unwrapping happens at output layer** (`output_data.py:337-370`):
   - `format=json` → strips `{"Query":[` prefix (offset 10)
   - `format=mcp` → strips `{"Query":[` and builds array format (offset 9)
   - Result: Clean JSON array `[...]` sent to client

**Critical Finding**: `format=mcp` IS supported in pass-through mode!
- Code at `output_data.py:343-344` specifically handles `format_type == "mcp"`
- Converts `{"Query": [...]}` → `[...]` at output layer
- Used for queries without aggregation

**Conclusion**:
- ✅ Object-wrapped format `{"Query": [...]}` is **MANDATORY** for Operator → Query Node communication (both modes)
- ✅ In **consolidation mode** (code_status=4): Results stored in "system_query", then queried with `{"Query": [...]}`
- ✅ In **pass-through mode** (code_status=3): Results streamed directly, unwrapped at output layer if `format=mcp`
- ❌ Unwrapped array format `[...]` **CANNOT** be used for Operator → Query Node wire protocol
- ⚠️ MCP server must continue to receive `{"Query": [...]}` and unwrap at the transport layer (similar to pass-through)

### Phase 3: Query Node → Client

Query Node returns final results in client's requested format:

**If client requested `format=json`**:
```json
{
  "Query": [
    {"timestamp": "...", "device_id": "sensor1", "temperature": 72.5},
    ...
  ]
}
```

**If client requested `format=table`**:
```
+------------+-----------+-------------+
| timestamp  | device_id | temperature |
+------------+-----------+-------------+
| 2024-...   | sensor1   | 72.5        |
| 2024-...   | sensor2   | 73.1        |
+------------+-----------+-------------+
```

**If client is MCP**:
- Returns JSON (MCP protocol wraps it in TextContent)

---

## Local Query Flow

### Operator Node (Direct Query)

**Scenario**: Client queries Operator Node directly (no Query Node)

**CLI/REST**:
```sql
sql mydb format=json "SELECT * FROM readings LIMIT 10"
```

**Returns**: JSON array directly to client

**MCP**:
- MCP server on Operator issues query with `format=json`
- Receives JSON array
- Wraps in MCP TextContent for SSE transport

### Query Node (Local DBMS Query)

**Scenario**: Query Node has local DBMS and queries it directly

**Same as Operator** - no consolidation needed

---

## MCP Format Handling

### Current MCP Server Behavior

The MCP server (`edge_lake/mcp_server/`) currently:

1. **For SQL Queries** (`tools/executor.py:206`):
   ```python
   # Execute query using hybrid approach (validation + execution)
   result = await self.query_executor.execute_query(
       dbms_name=database,
       sql_query=sql_query,
       mode="batch",  # Use batch for MCP compatibility
       fetch_size=100
   )

   # Return rows as JSON
   return json.dumps(rows, indent=2)
   ```

2. **Wraps in TextContent**:
   ```python
   [{
       "type": "text",
       "text": "[\n  {\"col1\": \"val1\"},\n  ...\n]"
   }]
   ```

3. **Sends via SSE**:
   - Entire JSON string sent as single SSE event
   - **Problem**: Large results (>1000 rows) get truncated

### MCP Format Strategy

**Decision**: MCP server should NOT use `format=mcp` for SQL queries

**Reason**:
1. SQL engine doesn't support `format=mcp`
2. `format=mcp` is for metadata extraction, not query results
3. Query results are already structured JSON

**Implementation**:
- MCP server always uses `format=json` for SQL queries
- MCP protocol wrapping happens AFTER JSON retrieval
- Focus on fixing truncation issue (see Block Transport Strategy)

---

## Block Transport Strategy

### Problem Statement

**Issue**: Query results >1000 rows are truncated in MCP server

**Root Cause**:
1. Entire result set converted to JSON string (`json.dumps(rows)`)
2. Wrapped in TextContent and JSON-RPC message
3. Sent as single SSE event (`data: {...}\n`)
4. Large SSE events are getting truncated (suspected socket buffer or SSE client limit)

**Impact**:
- MCP server unusable for large queries
- Phase 2 (Block Transport) is blocked

### Solution: Phased Approach

#### Phase 2A: Chunked SSE Events (Quick Fix)

**Objective**: Split large results into multiple SSE events

**Implementation**:
1. **In `tools/executor.py`**:
   - Don't convert entire result to single JSON string
   - Return results as iterator/generator

2. **In `sse_handler.py`**:
   - Send results as multiple SSE events:
     ```
     data: {"type": "chunk", "index": 0, "data": [row1, row2, ...]}
     event: query_chunk
     id: 1

     data: {"type": "chunk", "index": 1, "data": [row101, row102, ...]}
     event: query_chunk
     id: 2

     data: {"type": "complete", "total_rows": 2000}
     event: query_complete
     id: 3
     ```

3. **Client reassembly**:
   - MCP client receives chunks
   - Reassembles into single result array
   - Returns to AI agent

**Advantages**:
- Fixes truncation without major refactor
- Works within SSE transport
- No dependency on message_server.py

**Limitations**:
- Still limited by overall SSE connection size
- Not ideal for multi-GB results

#### Phase 2B: Block Transport via message_server.py (Future)

**Objective**: Handle very large results (>10MB) via separate TCP transport

**Threshold Logic**:
```python
if estimated_result_size > 10_MB:
    # Use block transport
    block_ids = send_via_message_server(results)
    # Notify via SSE
    send_sse_event({
        "type": "blocks_available",
        "block_ids": block_ids,
        "total_size": size
    })
else:
    # Use chunked SSE
    send_chunked_sse(results)
```

**Implementation**:
1. Integrate with `edge_lake/tcpip/message_server.py`
2. Send large results as binary blocks
3. Client retrieves blocks via separate connection
4. Reassembles into final result

**See**: `IMPLEMENTATION_PLAN.md` Phase 2 for details

---

## Implementation Roadmap

### Immediate Priorities (Phase 2A)

1. **Fix Truncation Issue**:
   - Implement chunked SSE events
   - Test with 1000+ row queries
   - **Target**: Complete in 1 week

2. **Document Format Handling**:
   - ✅ This document
   - Update `CLAUDE.md` with format strategy
   - Update `README.md` with format examples

3. **Test Network Queries**:
   - Test Operator → Query → Client flow
   - Verify `format=json` consolidation
   - Test `format=table` final output

### Future Work (Phase 2B)

1. **Block Transport**:
   - Integrate with message_server.py
   - Implement threshold-based routing
   - **Target**: 2-3 weeks after Phase 2A

2. **MCP Format for Queries** (Optional):
   - Evaluate if `format=mcp` makes sense for SQL queries
   - Would require changes to SQL engine on Operator Nodes
   - **Decision**: Deferred - not worth complexity

---

## Key Takeaways

### Format Parameter Rules

| Command Type | Supported Formats | Notes |
|--------------|-------------------|-------|
| **SQL Queries** | `json`, `table` | **NO `format=mcp`** |
| **Metadata Commands** | `json`, `table`, `mcp` | `format=mcp` for AI-friendly extraction |
| **Network Queries (Op→Query)** | `json` ONLY | Required for consolidation |
| **MCP Server** | Always uses `json` | MCP protocol wrapping is separate |

### Query Flow Rules

1. **Local Query** (any node):
   - Client specifies `format=json` or `format=table`
   - Node executes and returns in requested format

2. **Network Query** (Operator → Query):
   - Query Node forces `format=json` when forwarding to Operators
   - Operators return JSON regardless of client's original request
   - Query Node consolidates JSON
   - Query Node returns to client in client's requested format

3. **MCP Queries**:
   - MCP server always uses `format=json` internally
   - MCP protocol wrapping happens after JSON retrieval
   - Focus on chunked delivery for large results

### Truncation Fix Strategy

1. **Phase 2A** (Immediate):
   - Chunked SSE events
   - Split large results into manageable chunks
   - Client-side reassembly

2. **Phase 2B** (Future):
   - Block transport via message_server.py
   - Threshold-based routing (>10MB → blocks)
   - Binary protocol for efficiency

---

## References

- **MCP Format Design**: `edge_lake/mcp_server/docs/MCP_FORMAT_DESIGN.md`
- **Implementation Plan**: `edge_lake/mcp_server/IMPLEMENTATION_PLAN.md`
- **Known Issues**: `edge_lake/mcp_server/KNOWN_ISSUES.md`
- **Query Processing**: `edge_lake/cmd/member_cmd.py` (functions: `_issue_sql`, `query_local_dbms`, `process_query_sequence`)
- **Consolidation Logic**: `edge_lake/cmd/member_cmd.py` (search for "consolidat")

---

## Change Log

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-11-02 | EdgeLake Team | Initial strategy document |

---

## Questions for Resolution

1. **Consolidation Format - Object vs Array**: Does Query Node consolidation logic require object-wrapped JSON `{"Query": [...]}`, or can it work with unwrapped arrays `[...]`?
   - **Status**: ⚠️ **NEEDS INVESTIGATION**
   - **Location**: Search `member_cmd.py` for consolidation logic
   - **Impact**: Determines if we can support `format=json-array` for network queries
   - **Current Behavior**: Operators return `{"Query": [...]}` via `format=json`
   - **Question**: If Operators returned `[...]`, would consolidation break?

2. **Format Naming**: Should we rename `format=mcp` to `format=json-array` for clarity?
   - **Status**: ⚠️ **PROPOSED**
   - **Rationale**:
     - "mcp" is misleading - it's just an unwrapped array
     - Makes format expectations clearer
     - Allows SQL queries to support unwrapped arrays
   - **Proposal**:
     - `format=json` or `format=json-object` → `{"Query": [...]}`
     - `format=json-array` → `[...]` (new name for `format=mcp` behavior)
     - Keep `format=mcp` as alias for backward compatibility
   - **Decision**: TBD

3. **MCP Server Array Unwrapping**: Where should MCP server unwrap the `{"Query": [...]}` to `[...]`?
   - **Status**: ⚠️ **CURRENT BEHAVIOR**
   - **Current**: MCP server receives `{"Query": [...]}`, extracts array, returns to client
   - **Code**: `tools/executor.py:197` - `wrapped_result = {"Query": rows}`
   - **Question**: Should we keep wrapping/unwrapping, or use `format=json-array` if supported?

4. **Threshold for Block Transport**: What size threshold triggers block transport (10MB? 100MB?)?
   - **Answer**: 10MB recommended (see IMPLEMENTATION_PLAN.md Phase 2)

5. **Chunked SSE Size**: What's optimal chunk size for SSE events (100 rows? 1000 rows?)?
   - **Answer**: TBD - requires testing (start with 100 rows/chunk)
