# MCP Format Design Documentation

## Overview

The MCP (Model Context Protocol) format provides clean, structured JSON output specifically designed for AI agents to consume EdgeLake data.

## Design Philosophy

### Key Principle: Processing in member_cmd.py

All format processing **MUST** occur in `edge_lake/cmd/member_cmd.py`. This ensures:
- Consistent behavior across REST and CLI contexts
- Centralized logic for format handling
- No parsing dependencies in MCP server layer

## Important: format=mcp vs format=json

**CRITICAL DISTINCTION:**

- **Metadata Commands** use `format = mcp`:
  - `blockchain get table where format = mcp`
  - `get version where format = mcp`
  - `get status where format = mcp`

- **SQL Queries** use `format = json`:
  - `sql dbname format = json "SELECT ..."`
  - SQL engine does NOT support `format = mcp`
  - Using `format = mcp` in SQL will cause errors on operator nodes

**Why?**
- MCP format is for extracting/transforming metadata into AI-friendly structures
- SQL queries already return structured data - just use JSON format
- The SQL engine on operator nodes doesn't understand `format = mcp`

## Commands

### 1. Database/Table Schema Discovery

**Command:**
```bash
blockchain get table where format = mcp
```

**Returns:**
```json
[
  {"database": "db1", "table": "table1"},
  {"database": "db1", "table": "table2"},
  {"database": "db2", "table": "table3"}
]
```

**Use Case:** Discover all available databases and tables in one call

**MCP Tool:** `list_database_schema`

**Implementation:**
- Extracts from table policies in blockchain
- Sorts by database name, then table name
- Returns array of objects with `database` and `table` fields

### 2. Version Information

**Command:**
```bash
get version where format = mcp
```

**Returns:**
```json
{
  "version": "3.0.2345",
  "node_name": "query-node-1",
  "node_type": "query"
}
```

**Use Case:** Get structured node information

**MCP Tool:** `server_info`

### 3. Query Execution

**Command:**
```sql
sql <dbname> format=mcp and destination=network SELECT * FROM table
```

**Returns:**
```json
{
  "columns": ["col1", "col2", "col3"],
  "rows": [
    ["val1", "val2", "val3"],
    ["val4", "val5", "val6"]
  ]
}
```

**Use Case:** Execute distributed queries with clean JSON output

**MCP Tool:** `query`

## Implementation Details

### Location of MCP Logic

**File:** `edge_lake/cmd/member_cmd.py`

**Function:** `blockchain_get()` (starting at line ~1568)

**Key Section:**
```python
# Handle MCP format extraction (before REST/CLI split)
if mcp_format and blockchain_out:
    if key == "table" or key == "schema":
        # Extract all database/table combinations
        tables_list = []
        for policy in blockchain_out:
            if isinstance(policy, dict) and "table" in policy:
                table_info = policy["table"]
                if "dbms" in table_info and "name" in table_info:
                    tables_list.append({
                        "database": table_info["dbms"],
                        "table": table_info["name"]
                    })
        # Sort and return
        tables_list.sort(key=lambda x: (x["database"], x["table"]))
        output_str = utils_json.to_string(tables_list)
        blockchain_out = utils_json.str_to_json(output_str)
```

### Where Clause Parsing

The `format = mcp` directive is extracted **before** standard where clause parsing:

1. Check if `where format = mcp` is present
2. Extract and set `mcp_format = True`
3. Parse any remaining where conditions
4. Execute query normally
5. Apply MCP extraction to results

This ensures `format = mcp` doesn't interfere with actual data filtering.

## Testing

### Manual Testing

```bash
# Test via CLI
docker exec -it <container> python3 /app/EdgeLake/edge_lake/edgelake.py "blockchain get table where format = mcp"

# Test via REST
curl -H "User-Agent: anylog" \
     -H "command: blockchain get table where format = mcp" \
     http://localhost:32049
```

### Automated Testing

```bash
# Run test suite
python tests/test_format_changes.py \
  --mode test \
  --host localhost \
  --port 32049
```

## Comparison: Standard vs MCP Format

### Standard JSON (blockchain get table)
```json
[
  {
    "table": {
      "dbms": "db1",
      "name": "table1",
      "create": "CREATE TABLE...",
      "timestamp": "2024-01-01...",
      ...many more fields...
    }
  },
  ...
]
```
**Size:** Large (all metadata)
**Use:** Full policy inspection

### MCP Format (blockchain get table where format = mcp)
```json
[
  {"database": "db1", "table": "table1"},
  {"database": "db1", "table": "table2"}
]
```
**Size:** Minimal (only what's needed)
**Use:** AI agent discovery

## Future Extensions

### Possible Additional MCP Commands

1. **Node Discovery:**
   ```bash
   blockchain get operator where format = mcp
   ```
   Returns: `[{"name": "...", "ip": "...", "port": ...}]`

2. **Cluster Info:**
   ```bash
   blockchain get cluster where format = mcp
   ```
   Returns: `[{"cluster_id": "...", "tables": [...]}]`

3. **Statistics:**
   ```bash
   get status where format = mcp include statistics
   ```
   Returns: Structured stats object

## Common Issues

### Issue: CLI returns full JSON instead of MCP format
**Cause:** MCP extraction was in REST-only code path
**Fix:** Moved extraction before REST/CLI split (line 1691)

### Issue: `format = json` returns nothing
**Cause:** Format was being treated as a where filter
**Fix:** Extract `format =` before where clause parsing

### Issue: Need separate commands for databases vs tables
**Cause:** Overloading one command with multiple behaviors
**Fix:** Single `list_database_schema` tool returns both

## References

- MCP Specification: https://modelcontextprotocol.io/
- EdgeLake Blockchain Commands: See `member_cmd.py` docstrings
- Tool Definitions: `edge_lake/mcp/config/tools.yaml`
