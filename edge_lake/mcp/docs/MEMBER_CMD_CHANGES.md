# member_cmd.py Changes for format=mcp Support

This document details all changes made to `edge_lake/cmd/member_cmd.py` to support `format=mcp` for MCP (Model Context Protocol) integration.

## Overview

The changes add support for `format=mcp` directive to metadata commands (blockchain get, get version, get status, get columns) to return clean, structured JSON optimized for AI agent consumption. The MCP format extracts only essential information without CLI noise or metadata overhead.

## Change Summary

| Function | Lines | Purpose |
|----------|-------|---------|
| `format_values` | 130-137 | Document that format=mcp is NOT for SQL queries |
| `blockchain_get()` | 1602-1714 | Add format=mcp parsing and schema extraction |
| `get_node_status()` | 15498-15512 | Add MCP format output for status |
| `get_columns()` | 16276-16283 | Add MCP format output for columns |
| `get_version()` | 17249-17277 | Add format=mcp support and None handling |
| `_get_methods` | 19918-19928 | Add with_format flag to version command |

---

## Detailed Changes

### 1. format_values Dictionary (Lines 130-137)

**Purpose:** Document that format=mcp is currently enabled for metadata commands only.

**Change:**
```python
format_values = {
    "json" : 0,             # Output in JSON format
    "json:output" : 0,      # Output as JSON rows
    "json:list" : 0,        # Output as a JSON list
    "table" : 0,            # Output as a table
    # Note: format=mcp is NOT supported for SQL queries
    # Use format=mcp only for metadata commands (blockchain get, get version, etc.)
}
```

**Rationale:**
- SQL queries already return clean JSON with `format=json`
- Adding `format=mcp` to SQL causes operator node compatibility issues
- Clear separation: MCP for metadata, JSON for data queries, we will probably visit this next.

---

### 2. blockchain_get() Function (Lines 1602-1714)

**Purpose:** Parse `format=mcp` directive and extract database/table schema information.

#### 2a. Variable Initialization (Lines 1604-1605)

**Added:**
```python
mcp_format = False
mcp_filter_dbms = None  # For filtering tables by database name
```

#### 2b. Format Directive Parsing (Lines 1609-1634)

**Purpose:** Extract and skip format directive before parsing where conditions.

**Logic:**
1. Check if `format =` appears after `where`
2. Extract format value (json, mcp, etc.)
3. Set `mcp_format = True` if format is "mcp"
4. **Skip the format directive** (prevents it from being treated as a data filter)
5. Check for additional filters after format (e.g., `and dbms = xxx`)
6. Parse remaining where conditions

**Code:**
```python
# Check for format= directive BEFORE parsing where clause
# Format directives are not data filters, so extract and skip them
where_start = offset + 1
if where_start + 2 < words_count and utils_data.test_words(cmd_words, where_start, ["format", "="]):
    format_value = cmd_words[where_start + 2]
    if format_value == "mcp":
        mcp_format = True
    # Skip the format directive regardless of value (json, mcp, etc.)
    where_start += 3

    # Check for additional filter parameters after format
    if where_start < words_count and cmd_words[where_start] == "and":
        where_start += 1
        if where_start + 2 < words_count and utils_data.test_words(cmd_words, where_start, ["dbms", "="]):
            mcp_filter_dbms = params.get_value_if_available(cmd_words[where_start + 2])
            where_start += 3

# Now parse the remaining where conditions (if any)
if where_start < words_count and cmd_words[where_start] not in ["bring", "bring."]:
    ret_val, offset, value_pairs = utils_json.make_jon_struct_from_where(cmd_words, where_start)
else:
    # No additional where conditions, just format
    ret_val = process_status.SUCCESS
    offset = where_start
    value_pairs = None
```

**Key Points:**
- Format directive is extracted **before** standard where clause parsing
- Prevents `format = json/mcp` from being treated as a policy filter
- Allows commands like `blockchain get table where format = json` to work correctly
- Previously these would fail because no policies have a "format" field

#### 2c. MCP Schema Extraction (Lines 1696-1713)

**Purpose:** Transform table policies into clean database/table list for MCP.

**Placement:** After standard output formatting, before REST/CLI split

**Logic:**
```python
# Handle MCP format extraction (before REST/CLI split)
if mcp_format and blockchain_out:
    if key == "table" or key == "schema":
        # Extract all database/table combinations for MCP schema discovery
        # Note: "schema" is an alias for this operation - both work the same
        tables_list = []
        for policy in blockchain_out:
            if isinstance(policy, dict) and "table" in policy:
                table_info = policy["table"]
                if "dbms" in table_info and "name" in table_info:
                    tables_list.append({
                        "database": table_info["dbms"],
                        "table": table_info["name"]
                    })
        # Sort by database, then table name
        tables_list.sort(key=lambda x: (x["database"], x["table"]))
        output_str = utils_json.to_string(tables_list)
        # Override blockchain_out with the extracted list for consistent handling
        blockchain_out = utils_json.str_to_json(output_str)
```

**Input (Standard blockchain get table):**
```json
[
  {
    "table": {
      "dbms": "db1",
      "name": "table1",
      "create": "CREATE TABLE...",
      "timestamp": "...",
      ... many more fields ...
    }
  },
  {
    "table": {
      "dbms": "db1",
      "name": "table2",
      ...
    }
  }
]
```

**Output (format=mcp):**
```json
[
  {"database": "db1", "table": "table1"},
  {"database": "db1", "table": "table2"}
]
```

**Why Before REST/CLI Split:**
- Ensures consistent output in both REST API and CLI contexts
- Previous implementation only worked for REST, CLI showed raw policies
- Extraction at line 1696 happens before the `if not return_data:` check at line 1716

---

### 3. get_node_status() Function (Lines 15498-15512)

**Purpose:** Add MCP format output for node status.

**Added:**
```python
if reply_format == "mcp":
    # MCP format: clean structure with node information
    mcp_struct = {
        "node_name": node_info.get_node_name(),
        "status": "running",
        "profiling": profiler.is_active()
    }
    # Add any additional included variables
    for key, value in reply_struct.items():
        if key != "Status":
            mcp_struct[key] = value
    reply = utils_json.to_string(mcp_struct)
else:
    reply = utils_json.to_string(reply_struct)
```

**Example Output:**
```json
{
  "node_name": "query-node-1",
  "status": "running",
  "profiling": false
}
```

---

### 4. get_columns() Function (Lines 16276-16283)

**Purpose:** Add MCP format for column schema output.

**Added:**
```python
elif out_format == "mcp":
    # MCP format: list of objects with name and type
    output_list = []
    for entry in new_list:
        output_list.append({"name": entry[0], "type": entry[1]})
    reply = utils_json.to_string(output_list)
```

**Example Output:**
```json
[
  {"name": "timestamp", "type": "TIMESTAMP"},
  {"name": "value", "type": "FLOAT"},
  {"name": "device_id", "type": "TEXT"}
]
```

---

### 5. get_version() Function (Lines 17249-17277)

**Purpose:** Add format=mcp support and fix initialization bug.

#### 5a. Handle None cmd_words (Lines 17252-17257)

**Problem:** Function was called during initialization with `cmd_words=None`, causing crash.

**Fix:**
```python
# Handle case where cmd_words is None (called during initialization)
if cmd_words is None:
    offset = 0
    words_count = 0
else:
    offset = get_command_offset(cmd_words)
    words_count = len(cmd_words)
```

#### 5b. Parse format Directive (Lines 17259-17266)

**Added:**
```python
# Check for format specification
# Command structure: get version where format = mcp
# Positions:         0   1       2     3      4 5
if words_count >= (offset + 6) and utils_data.test_words(cmd_words, offset + 2, ["where", "format", "="]):
    reply_format = cmd_words[offset + 5]
else:
    reply_format = None
```

**Note:** Requires 6 words minimum to have complete command

#### 5c. MCP Format Output (Lines 17268-17277)

**Added:**
```python
if reply_format == "mcp":
    # MCP format: structured JSON with version details
    mcp_struct = {
        "version": code_version,
        "node_name": node_info.get_node_name()
    }
    reply = utils_json.to_string(mcp_struct)
else:
    reply = f"EdgeLake Version: {code_version}"
```

**Example Output:**
```json
{
  "version": "3.0.2345",
  "node_name": "query-node-1"
}
```

---

### 6. Command Definition Update (Lines 19918-19928)

**Purpose:** Allow `get version where format = mcp` to pass validation.

**Added:**
```python
"version": {
    'command': get_version,
    'key_only': True,
    'with_format': True,  # NEW: Allow format directive
    'help': {
        'usage': "get version [where format = mcp]",
        'example': "get version\n"
                   "get version where format = mcp",
        'text': "Return the code version. Use format=mcp for structured JSON output.",
        'keywords' : ["node info"],
    }
}
```

**Why Needed:**
- `key_only: True` means command expects exactly "get version" with no extra words
- `with_format: True` creates exception to allow `where format = xxx`
- Without this flag, validation rejects the command with "Error Command Structure"
- See `is_with_format()` function at line 7287 for validation logic

---

## Command Examples

### blockchain get table

**Standard:**
```bash
blockchain get table
# Returns: Full table policies with all metadata
```

**With format=mcp:**
```bash
blockchain get table where format = mcp
# Returns: [{"database": "db1", "table": "table1"}, ...]
```

**With format=json (documented, not broken):**
```bash
blockchain get table where format = json
# Returns: Full table policies (same as standard)
```

### get version

**Standard:**
```bash
get version
# Returns: "EdgeLake Version: 3.0.2345"
```

**With format=mcp:**
```bash
get version where format = mcp
# Returns: {"version": "3.0.2345", "node_name": "query-node-1"}
```

### get columns

**With format=mcp:**
```bash
get columns where dbms="mydb" and table="mytable" and format=mcp
# Returns: [{"name": "col1", "type": "TEXT"}, ...]
```

### get status

**With format=mcp:**
```bash
get status where format = mcp include statistics
# Returns: {"node_name": "...", "status": "running", ...}
```

---

## Testing

### Manual Testing

```bash
# Test blockchain schema extraction
blockchain get table where format = mcp

# Test version format
get version where format = mcp

# Test that format=json still works (regression)
blockchain get table where format = json
blockchain get table  # No format - should work
```

### Automated Testing - IN PROGRESS

See `tests/test_format_changes.py` for comprehensive test suite covering:
- Regression testing (existing commands still work)
- MCP format validation
- Baseline comparison with known-good output

---

## Important Notes

### Format Directives Are Not Data Filters

**Problem:** Before these changes, `blockchain get table where format = json` would:
1. Parse `format = json` as a where condition
2. Create filter: `{"format": "json"}`
3. Try to find table policies with `format` field
4. Find nothing (policies don't have a format field)
5. Return empty result

**Solution:** Format directives are now extracted and skipped before where clause parsing.

### MCP Format Location Matters

The MCP extraction at line 1696 is placed:
- **After** standard output formatting
- **Before** the REST/CLI split (line 1716)

This ensures:
- ✅ Works in both REST API and CLI contexts
- ✅ Consistent output regardless of how command is invoked
- ✅ No duplicate code for REST vs CLI

### Why No format=mcp for SQL

SQL queries do NOT support `format=mcp`:
- SQL already returns clean JSON with `format=json`
- SQL engine on operator nodes would need updates
- Causes compatibility issues with existing deployments
- Adds complexity with no real benefit

**Use Cases:**
- ✅ Metadata commands: `format=mcp`
- ✅ SQL queries: `format=json`

---

## Migration Notes

### For Existing Deployments

These changes are **backward compatible**:
- Existing commands work unchanged
- `format=json` behavior preserved
- No format directive = standard output (unchanged)

### For MCP Integration

To use these features in MCP tools:
1. Use `blockchain get table where format = mcp` for schema discovery
2. Use `get version where format = mcp` for node info
3. Use `format=json` for SQL queries (not `format=mcp`)

---

## Related Files

- `edge_lake/mcp/config/tools.yaml` - MCP tool definitions using these commands
- `edge_lake/mcp/docs/MCP_FORMAT_DESIGN.md` - Overall MCP format design
- `tests/test_format_changes.py` - Test suite for validation (left in mcp-to-core branch)
- `tests/README_FORMAT_TESTS.md` - Testing documentation (left in mcp-to-core branch)

## References

- MCP Specification: https://modelcontextprotocol.io/
- EdgeLake Blockchain Commands: `member_cmd.py` docstrings
- Format Validation: `is_with_format()` at line 7287
- Where Clause Parsing: `make_jon_struct_from_where()` in `utils_json.py`
