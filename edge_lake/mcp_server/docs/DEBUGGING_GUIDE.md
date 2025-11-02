# MCP Server Debugging Guide

**Purpose**: Step-by-step guide to get MCP server working from scratch
**Status**: Fresh start after refactoring (no known working state)
**Date**: 2025-10-31

---

## Pre-Flight Checklist

### ✅ Completed Refactoring
- [x] Renamed `edge_lake/mcp/` → `edge_lake/mcp_server/` (avoid SDK conflict)
- [x] Created lightweight protocol (no MCP SDK dependency)
- [x] Removed DirectClient ThreadPool (unnecessary)
- [x] Removed message processing threads (unnecessary)
- [x] Kept keepalive thread (required for SSE)

### 🔍 Known State
- **MCP SDK removed** from requirements.txt (lightweight protocol used instead)
- **Threading optimized** (Phase 1 complete)
- **Never tested** since initial refactoring (naming conflict prevented testing)

---

## Startup Sequence

### 1. Start EdgeLake

```bash
cd /Users/tviviano/Documents/GitHub/EdgeLake
python3 edge_lake/edgelake.py
```

**Expected**: EdgeLake CLI starts with `AL >` prompt

**Common Issues**:
- Missing dependencies: `pip install -r requirements.txt`
- Import errors: Check Python path

### 2. Start REST Server

```
AL > run rest server where external_ip = 0.0.0.0 and external_port = 32049 and internal_ip = 127.0.0.1 and internal_port = 32049 and threads = 5
```

**Expected Output**:
```
REST server started on 0.0.0.0:32049
Workers pool: 5 threads
```

**Verify**:
```bash
curl http://localhost:32049/
```

Should return EdgeLake version/info

**Common Issues**:
- Port already in use: Change port or kill existing process
- Workers pool error: Check utils_threads module

### 3. Test MCP SSE Endpoint (WITHOUT starting MCP server)

```bash
curl -N http://localhost:32049/mcp/sse
```

**Expected Result**:
```
503 Service Unavailable
MCP server not available
```

**Why**: MCP endpoints are registered in http_server.py but SSE transport not initialized yet

**If you get this**: ✅ HTTP server routing works!

**Common Issues**:
- 404 Not Found: MCP endpoints not registered in http_server.py
- Connection refused: REST server not running

### 4. Start MCP Server

```
AL > run mcp server
```

**Expected Output**:
```
MCP server started
SSE transport initialized
Endpoints: GET /mcp/sse, POST /mcp/messages/{session_id}
```

**Common Issues**:
- Import errors: Check edge_lake.mcp_server imports
- SSE transport error: Check sse_handler.py initialization
- Config error: Check config/tools.yaml exists

### 5. Test MCP SSE Endpoint (AFTER starting MCP server)

```bash
curl -N http://localhost:32049/mcp/sse
```

**Expected Result** (SSE stream):
```
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive

data: {"session_id":"...","message":"MCP SSE connection established"}
event: connected
id: 0

:keepalive

:keepalive
...
```

**If you see this**: ✅ SSE endpoint working!

**Common Issues**:
- 503 still: SSE transport not initialized properly
- Connection closes immediately: Check SSE handler while loop
- No keepalives: Check keepalive thread started

---

## Testing MCP Protocol

### Test 1: List Tools

**Setup**: Keep SSE connection open in one terminal:
```bash
curl -N http://localhost:32049/mcp/sse
```

**Extract session_id** from the `connected` event data (e.g., `abc-123-def`)

**Send Message** (in another terminal):
```bash
SESSION_ID="<paste-session-id-here>"

curl -X POST http://localhost:32049/mcp/messages/$SESSION_ID \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/list",
    "params": {}
  }'
```

**Expected in SSE terminal**:
```
data: {"jsonrpc":"2.0","id":1,"result":[{"name":"query_timeseries","description":"..."},...]}
event: message
id: 1
```

**If you see tool list**: ✅ MCP protocol working!

**Common Issues**:
- 404: Wrong session_id or path
- Empty result: Tools not loading from config
- Error response: Check MCP server process_message()

### Test 2: Call Tool

```bash
curl -X POST http://localhost:32049/mcp/messages/$SESSION_ID \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 2,
    "method": "tools/call",
    "params": {
      "name": "query_timeseries",
      "arguments": {
        "database": "test",
        "table": "data",
        "columns": ["timestamp", "value"],
        "limit": 10
      }
    }
  }'
```

**Expected in SSE terminal**:
```
data: {"jsonrpc":"2.0","id":2,"result":[{"type":"text","text":"...query results..."}]}
event: message
id: 2
```

**If you see results**: ✅ Full MCP stack working!

---

## Debugging Checklist

### Issue: "run mcp server" command not recognized

**Cause**: Command not registered in member_cmd.py

**Fix**:
```bash
grep -n "run mcp server" edge_lake/cmd/member_cmd.py
```

Should find the command handler. If not, check git status.

### Issue: ImportError for mcp_server modules

**Cause**: Python can't find edge_lake.mcp_server

**Fix**:
```bash
# Check __init__.py exists
ls -la edge_lake/mcp_server/__init__.py

# Check if it's importable
python3 -c "from edge_lake.mcp_server import protocol; print('OK')"
```

### Issue: SSE connection closes immediately

**Cause**: Error in handle_sse_endpoint() while loop

**Debug**:
1. Check EdgeLake logs for errors
2. Add debug prints in sse_handler.py line 245 (while loop)
3. Check if connection.connected = True

### Issue: No keepalives sent

**Cause**: Keepalive thread not starting

**Debug**:
```bash
# In EdgeLake, check threads
AL > get threads

# Should see "MCP-SSE-Keepalive" thread
```

### Issue: Tool execution fails

**Cause**: DirectClient or member_cmd error

**Debug**:
1. Check if member_cmd.process_cmd() is called (add logging)
2. Verify command string format (e.g., "sql test ...")
3. Check status.get_saved_error() for error messages

### Issue: Response not sent via SSE

**Cause**: Response queued but not delivered

**Debug**:
1. Check if connection.queue_message() called
2. Verify SSE while loop processes queue (line 249)
3. Check if send_event() succeeds

---

## Log Locations

**EdgeLake Logs**:
```bash
tail -f $EDGELAKE_HOME/EdgeLake/data/*.log
```

**Python Logging** (if enabled):
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

**Add Debug Logging**:
```python
# In sse_handler.py, add at top:
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Then add throughout:
logger.debug(f"SSE connection established: {session_id}")
```

---

## Quick Health Check

Run this to verify everything is connected:

```bash
# 1. Check REST server
curl http://localhost:32049/

# 2. Check MCP endpoint (should be 503 or 200)
curl -I http://localhost:32049/mcp/sse

# 3. If 200, test SSE connection
timeout 5 curl -N http://localhost:32049/mcp/sse

# Should see connected event and keepalives
```

---

## Architecture Verification

**Current Thread Model** (after Phase 1):
```
HTTP Server (5 workers)
  ↓
SSE Handler (1 keepalive thread)
  ↓
MCP Server (no threads)
  ↓
DirectClient (no threads)
  ↓
member_cmd (synchronous)
```

**Total Expected Threads**:
- 5 HTTP workers
- 1 SSE keepalive
- 3 HTTP workers blocked on SSE (if 3 clients connected)
- = ~9 threads with 3 SSE clients

**Verify with**:
```
AL > get threads
```

---

## Success Criteria

- [ ] EdgeLake starts without errors
- [ ] REST server starts on port 32049
- [ ] MCP server starts without errors
- [ ] SSE endpoint returns 200 (not 503)
- [ ] SSE connection receives "connected" event
- [ ] Keepalives arrive every 30 seconds
- [ ] tools/list returns tool definitions
- [ ] tools/call executes and returns results
- [ ] Multiple SSE clients work simultaneously

---

## Next Steps After Working

Once basic functionality works:

1. **Performance Testing**
   - Measure thread count vs old version
   - Test with 10+ SSE clients
   - Long-running queries (>1 minute)

2. **Phase 2 Prep**
   - Plan SSEConnectionManager implementation
   - Design select()/epoll integration
   - Test socket non-blocking I/O

3. **Phase 3 Prep**
   - Identify query size threshold (10MB?)
   - Design message_server integration
   - Plan chunked response assembly

---

## Emergency Rollback

If refactoring breaks everything:

```bash
# Revert changes
git diff edge_lake/mcp_server/

# Restore original files
git checkout edge_lake/mcp_server/core/direct_client.py
git checkout edge_lake/mcp_server/transport/sse_handler.py

# Rebuild/redeploy
mel build
mel deploy
```

---

**Good luck! 🚀 Let's get this working!**
