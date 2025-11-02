# Known Issues - MCP Server

This document tracks known issues discovered during testing and development of the EdgeLake MCP server.

---

## Issue 1: HTTP Response Truncation at 1000 Rows

**Status**: 🔴 Critical - Phase 2 Blocker
**Severity**: High
**Discovered**: 2025-11-02
**Branch**: mcp-query-blocks

### Description

The HTTP protocol appears to cut off query responses mid-way when results exceed approximately 1000 rows. The JSON object for the result is not closed properly, resulting in malformed JSON that cannot be parsed by clients.

### Symptoms

- Queries returning >1000 rows are truncated
- JSON response is incomplete (missing closing braces/brackets)
- Client receives invalid JSON and fails to parse response
- No error is reported - the response simply ends abruptly

### Example

**Query**: `SELECT * FROM readings LIMIT 2000`

**Expected**: Complete JSON array with 2000 records, properly closed
```json
{
  "rows": [
    {"id": 1, ...},
    {"id": 2, ...},
    ...
    {"id": 2000, ...}
  ]
}
```

**Actual**: JSON array truncated around row 1000, missing closing brackets
```json
{
  "rows": [
    {"id": 1, ...},
    {"id": 2, ...},
    ...
    {"id": 1000, ...}
```

### Root Cause Analysis

**Suspected Cause**: HTTP response buffer limitation in EdgeLake's http_server.py

The issue likely stems from one of these areas:

1. **Response Buffer Size**: `http_server.py` may have a fixed buffer size that gets exhausted
2. **Chunked Transfer Encoding**: HTTP chunked transfer may not be properly implemented for large responses
3. **Timeout**: Connection timeout during large response transmission
4. **Memory Limit**: Response size limit to prevent memory exhaustion

### Impact

- **Phase 1 (SSE Transport)**: Works fine for small-to-medium queries (<1000 rows)
- **Phase 2 (Block Transport)**: This issue blocks implementation of large query support
- **Production Use**: Severely limits usefulness of MCP server for real-world data queries

### Related Components

- `edge_lake/tcpip/http_server.py` - HTTP server implementation
- `edge_lake/mcp_server/transport/sse_handler.py` - SSE transport (sends responses via HTTP)
- `edge_lake/mcp_server/core/query_executor.py` - Query execution and result streaming

### Investigation Needed

1. Check `http_server.py` for response size limits or buffer constraints
2. Review how SSE events are sent for large payloads
3. Test if the issue occurs with REST API (non-MCP queries)
4. Determine if this is an SSE-specific issue or general HTTP issue

### Workaround

**Short-term**: Limit queries to <1000 rows using LIMIT clause
```sql
SELECT * FROM readings LIMIT 500
```

**Long-term**: Implement Phase 2 Block Transport
- Use message_server.py for large result delivery
- Chunk results into manageable blocks
- Client reassembles blocks into complete result set

### Resolution Plan

This issue will be addressed as part of **Phase 2: Block Transport Implementation**:

1. Implement threshold-based routing (>10MB → block transport, <10MB → SSE)
2. Integrate with message_server.py for chunked delivery
3. Add block reassembly logic to client
4. See `IMPLEMENTATION_PLAN.md` Phase 2 for details

### Testing Notes

To reproduce:
```bash
# Start MCP server
python edge_lake/edgelake.py
AL > run rest server where external_ip = 0.0.0.0 and external_port = 32049 and internal_ip = 127.0.0.1 and internal_port = 32049
AL > run mcp server

# Run query via MCP client
# Query: SELECT * FROM table WHERE ... LIMIT 2000
# Observe truncated response around row 1000
```

---

## Issue 2: Aggregate Function Support - stddev()

**Status**: 🟡 Medium - Enhancement
**Severity**: Medium
**Discovered**: 2025-11-02
**Branch**: mcp-query-blocks

### Description

The `stddev(column)` aggregate function is not supported in EdgeLake SQL queries. When used, it results in a query error.

### Symptoms

- Queries using `stddev()` fail with error
- Error message indicates unsupported function
- Other aggregate functions (avg, sum, count, min, max) work correctly

### Example

**Query**:
```sql
SELECT
  device_id,
  avg(value) as avg_value,
  stddev(value) as stddev_value
FROM readings
GROUP BY device_id
```

**Error**: `Unsupported aggregate function: stddev` (or similar)

### Root Cause

EdgeLake's SQL parser (`edge_lake/generic/al_parser.py`) or query executor does not include `stddev()` in its list of supported aggregate functions.

### Impact

- Users cannot calculate standard deviation via SQL queries
- Workaround requires client-side calculation from raw data
- Limits statistical analysis capabilities

### Supported Functions

Currently supported aggregate functions:
- `count(*)`, `count(column)`
- `sum(column)`
- `avg(column)`
- `min(column)`
- `max(column)`

### Investigation Needed

1. Check `al_parser.py` for aggregate function definitions
2. Determine if underlying DBMS (PostgreSQL/SQLite) supports stddev
3. Assess difficulty of adding stddev support

### Workaround

**Option 1**: Calculate client-side
```python
# Fetch raw data
result = client.query("SELECT value FROM readings")
values = [row['value'] for row in result['rows']]

# Calculate stddev
import statistics
stddev = statistics.stdev(values)
```

**Option 2**: Use variance (if supported)
```sql
SELECT sqrt(variance(value)) as stddev_value FROM readings
```

### Resolution Plan

**Phase 3: Advanced Features** (Future)

1. Add `stddev()` to aggregate function registry
2. Map to underlying DBMS stddev function:
   - PostgreSQL: `stddev_samp()` or `stddev_pop()`
   - SQLite: May need custom implementation
3. Test with various data types (int, float)
4. Update documentation

### Priority

Medium - This is an enhancement, not a blocker. Phase 2 work should proceed first.

### Related Issues

None

---

## Issue Tracking

| Issue | Status | Priority | Phase | Assignee |
|-------|--------|----------|-------|----------|
| HTTP Response Truncation | 🔴 Open | Critical | Phase 2 | - |
| stddev() Support | 🟡 Open | Medium | Phase 3 | - |

---

## Reporting New Issues

When reporting new issues, include:

1. **Clear Description**: What happens vs. what should happen
2. **Reproduction Steps**: Exact steps to reproduce the issue
3. **Environment**: EdgeLake version, OS, node type (Query/Operator)
4. **Logs**: Relevant error messages or log output
5. **Impact**: How does this affect functionality?
6. **Workaround**: Any temporary solutions discovered

Submit issues to: https://github.com/EdgeLake/edgelake/issues

---

**Last Updated**: 2025-11-02
**Document Version**: 1.0
