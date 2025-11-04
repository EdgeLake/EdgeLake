#!/usr/bin/env python3
"""
EdgeLake MCP SSE Comprehensive Test Suite

Tests all MCP tools using proper SSE protocol implementation.
Covers all 5 currently implemented tools:
  1. server_info - MCP server information (internal)
  2. node_status - EdgeLake node status
  3. list_database_schema - List all database/table combinations in network
  4. get_schema - Get table schema
  5. query - Execute distributed SQL query

Usage:
    python3 test-mcp-sse.py --host localhost --port 50051
    python3 test-mcp-sse.py --host 192.168.1.106 --port 50051 --database new_company --table rand_data
"""

import argparse
import json
import sys
import time
import threading
import urllib.request
import urllib.error
from typing import Optional, Dict, Any, List
from queue import Queue


class MCPSSETester:
    def __init__(self, host: str, port: int, test_database: str = None, test_table: str = None):
        self.host = host
        self.port = port
        self.base_url = f"http://{host}:{port}"
        self.sse_url = f"{self.base_url}/mcp/sse"
        self.test_database = test_database
        self.test_table = test_table

        # Session state
        self.message_endpoint = None
        self.session_active = False
        self.response_queue = Queue()
        self.sse_thread = None

        # Store discovered databases/tables for dependent tests
        self.discovered_databases = []
        self.discovered_tables = []

    def _parse_sse_event(self, line: str) -> tuple:
        """Parse SSE event line"""
        if line.startswith('event: '):
            return ('event', line[7:].strip())
        elif line.startswith('data: '):
            return ('data', line[6:].strip())
        return (None, None)

    def _sse_listener(self):
        """Background thread to listen to SSE stream"""
        try:
            req = urllib.request.Request(self.sse_url)
            req.add_header('Accept', 'text/event-stream')

            with urllib.request.urlopen(req) as response:
                print(f"✓ SSE connection established")

                event_type = None
                event_data = []

                for line in response:
                    line = line.decode('utf-8').rstrip('\n\r')

                    if not line:
                        # Empty line = end of event
                        if event_type and event_data:
                            data = '\n'.join(event_data)

                            if event_type == 'endpoint':
                                # Server sent us the message endpoint
                                # MCP SSE protocol spec: endpoint is sent as plain string
                                # Format: data: /mcp/messages/{session_id}
                                self.message_endpoint = data
                                print(f"✓ Received message endpoint: {self.message_endpoint}")
                                self.session_active = True

                            elif event_type == 'message':
                                # Server sent a response
                                message = json.loads(data)
                                self.response_queue.put(message)

                        event_type = None
                        event_data = []
                        continue

                    field, value = self._parse_sse_event(line)
                    if field == 'event':
                        event_type = value
                    elif field == 'data':
                        event_data.append(value)

        except Exception as e:
            print(f"✗ SSE connection error: {e}")
            self.session_active = False

    def establish_session(self) -> bool:
        """Establish SSE session and wait for endpoint"""
        print(f"\n{'='*60}")
        print(f"Establishing SSE session with {self.sse_url}")
        print(f"{'='*60}")

        # Start SSE listener in background
        self.sse_thread = threading.Thread(target=self._sse_listener, daemon=True)
        self.sse_thread.start()

        # Wait for session to be established (max 5 seconds)
        for i in range(50):
            if self.session_active and self.message_endpoint:
                print(f"✓ Session established successfully")
                return True
            time.sleep(0.1)

        print(f"✗ Session establishment timeout")
        return False

    def send_mcp_request(self, method: str, params: Optional[Dict[str, Any]] = None, timeout: float = 10.0) -> Optional[Dict]:
        """Send an MCP JSON-RPC request and wait for response"""
        if not self.message_endpoint:
            print(f"✗ No message endpoint - session not established")
            return None

        message_url = f"{self.base_url}{self.message_endpoint}"

        payload = {
            "jsonrpc": "2.0",
            "id": int(time.time() * 1000),
            "method": method
        }

        if params:
            payload["params"] = params

        try:
            print(f"\n→ Sending {method} request to {self.message_endpoint}")

            # Send the request
            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                message_url,
                data=data,
                headers={"Content-Type": "application/json"}
            )

            with urllib.request.urlopen(req, timeout=timeout) as response:
                status_code = response.status

                if status_code == 202:  # Accepted
                    print(f"✓ Request accepted, waiting for response via SSE...")

                    # Wait for response from SSE stream
                    start_time = time.time()
                    while time.time() - start_time < timeout:
                        if not self.response_queue.empty():
                            response_msg = self.response_queue.get()
                            print(f"✓ Received response via SSE")
                            return response_msg
                        time.sleep(0.1)

                    print(f"✗ Response timeout after {timeout}s")
                    return None

                else:
                    print(f"✗ Unexpected status code: {status_code}")
                    return None

        except urllib.error.HTTPError as e:
            print(f"✗ Request failed (HTTP {e.code})")
            try:
                error_body = e.read().decode('utf-8')
                print(f"  Response: {error_body[:200]}")
            except:
                pass
            return None
        except Exception as e:
            print(f"✗ Request error: {e}")
            return None

    def test_initialize(self) -> bool:
        """Test MCP initialize"""
        print(f"\n{'='*60}")
        print("Test 1: MCP Initialize")
        print(f"{'='*60}")

        params = {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {
                "name": "edgelake-mcp-tester",
                "version": "1.0.0"
            }
        }

        response = self.send_mcp_request("initialize", params)

        if response and "result" in response:
            result = response["result"]
            print(f"✓ Initialize successful")
            print(f"  Protocol Version: {result.get('protocolVersion', 'unknown')}")

            server_info = result.get('serverInfo', {})
            print(f"  Server Name: {server_info.get('name', 'unknown')}")
            print(f"  Server Version: {server_info.get('version', 'unknown')}")

            capabilities = result.get('capabilities', {})
            if capabilities:
                print(f"  Capabilities:")
                for cap, value in capabilities.items():
                    print(f"    - {cap}: {value}")

            return True
        else:
            print(f"✗ Initialize failed")
            return False

    def test_list_tools(self) -> bool:
        """Test MCP tools/list"""
        print(f"\n{'='*60}")
        print("Test 2: MCP tools/list")
        print(f"{'='*60}")

        response = self.send_mcp_request("tools/list")

        if response and "result" in response:
            tools = response["result"].get("tools", [])
            print(f"✓ Found {len(tools)} tools")

            for tool in tools:
                print(f"\n  Tool: {tool.get('name')}")
                print(f"    Description: {tool.get('description', 'N/A')}")

                input_schema = tool.get('inputSchema', {})
                if input_schema:
                    properties = input_schema.get('properties', {})
                    required = input_schema.get('required', [])
                    if properties:
                        print(f"    Parameters:")
                        for prop_name, prop_info in properties.items():
                            req_marker = " (required)" if prop_name in required else ""
                            print(f"      - {prop_name}{req_marker}: {prop_info.get('description', 'N/A')[:60]}...")

            return len(tools) > 0
        else:
            print(f"✗ tools/list failed")
            return False

    def test_server_info(self) -> bool:
        """Test server_info tool (internal, no EdgeLake call)"""
        print(f"\n{'='*60}")
        print("Test 3: server_info tool")
        print(f"{'='*60}")
        print("  Tool: server_info (internal)")
        print("  Type: Internal MCP server query")
        print("  Arguments: None")

        params = {
            "name": "server_info",
            "arguments": {}
        }

        response = self.send_mcp_request("tools/call", params, timeout=10.0)

        if response and "result" in response:
            print(f"✓ server_info successful")
            result = response["result"]
            content = result.get("content", [])
            if content:
                try:
                    text = content[0].get("text", "")
                    server_data = json.loads(text)
                    print(f"  Server: {server_data.get('server_name', 'unknown')}")
                    print(f"  Version: {server_data.get('version', 'unknown')}")
                    print(f"  Mode: {server_data.get('mode', 'unknown')}")
                    config = server_data.get('configuration', {})
                    if config:
                        print(f"  Tools: {config.get('total_tools', 0)}")
                        print(f"  Timeout: {config.get('request_timeout', 0)}s")
                except:
                    print(f"  Raw response: {text[:200]}...")
            return True
        else:
            print(f"✗ server_info failed")
            return False

    def test_node_status(self) -> bool:
        """Test node_status tool"""
        print(f"\n{'='*60}")
        print("Test 4: node_status tool")
        print(f"{'='*60}")
        print("  Tool: node_status")
        print("  EdgeLake Command: get status")
        print("  Arguments: None")

        params = {
            "name": "node_status",
            "arguments": {}
        }

        response = self.send_mcp_request("tools/call", params, timeout=35.0)

        if response and "result" in response:
            print(f"✓ node_status successful")
            result = response["result"]
            content = result.get("content", [])
            if content:
                text = content[0].get("text", "")
                # Try to parse as JSON
                try:
                    status_data = json.loads(text)
                    if isinstance(status_data, dict):
                        print(f"  Status fields: {', '.join(list(status_data.keys())[:5])}")
                    print(f"  Response size: {len(text)} bytes")
                except:
                    print(f"  Response preview: {text[:150]}...")
            return True
        else:
            print(f"✗ node_status failed")
            return False

    def test_list_database_schema(self) -> bool:
        """Test list_database_schema tool"""
        print(f"\n{'='*60}")
        print("Test 5: list_database_schema tool")
        print(f"{'='*60}")
        print("  Tool: list_database_schema")
        print("  EdgeLake Command: blockchain get table bring.json")
        print("  Arguments: None")

        params = {
            "name": "list_database_schema",
            "arguments": {}
        }

        response = self.send_mcp_request("tools/call", params, timeout=35.0)

        if response and "result" in response:
            print(f"✓ list_database_schema successful")
            result = response["result"]
            content = result.get("content", [])
            if content:
                text = content[0].get("text", "")
                try:
                    schema_data = json.loads(text)
                    if isinstance(schema_data, list):
                        # Extract unique databases and tables for later tests
                        databases_set = set()
                        tables_set = set()

                        for entry in schema_data:
                            if isinstance(entry, dict):
                                db = entry.get('database')
                                table = entry.get('table')
                                if db:
                                    databases_set.add(db)
                                if table:
                                    tables_set.add(table)

                        self.discovered_databases = list(databases_set)
                        self.discovered_tables = list(tables_set)

                        print(f"  Found {len(schema_data)} database/table combinations:")
                        print(f"  Unique databases: {len(self.discovered_databases)}")
                        print(f"  Unique tables: {len(self.discovered_tables)}")

                        # Show first few entries
                        for entry in schema_data[:5]:
                            if isinstance(entry, dict):
                                db = entry.get('database', 'unknown')
                                table = entry.get('table', 'unknown')
                                print(f"    - {db}.{table}")
                        if len(schema_data) > 5:
                            print(f"    ... and {len(schema_data) - 5} more")
                    else:
                        print(f"  Response: {text[:200]}...")
                except Exception as e:
                    print(f"  Response parsing error: {e}")
                    print(f"  Raw response: {text[:200]}...")
            return True
        else:
            print(f"✗ list_database_schema failed")
            return False

    def test_get_schema(self) -> bool:
        """Test get_schema tool"""
        print(f"\n{'='*60}")
        print("Test 7: get_schema tool")
        print(f"{'='*60}")
        print("  Tool: get_schema")
        print("  EdgeLake Command: get columns where dbms=... and table=...")

        # Determine which database and table to use
        database = self.test_database or (self.discovered_databases[0] if self.discovered_databases else None)
        table = self.test_table or (self.discovered_tables[0] if self.discovered_tables else None)

        if not database or not table:
            print(f"⚠ Skipping get_schema - no database/table available")
            print(f"  (No --database/--table specified and none discovered)")
            return True

        print(f"  Arguments: database={database}, table={table}")

        params = {
            "name": "get_schema",
            "arguments": {
                "database": database,
                "table": table
            }
        }

        response = self.send_mcp_request("tools/call", params, timeout=35.0)

        if response and "result" in response:
            print(f"✓ get_schema successful")
            result = response["result"]
            content = result.get("content", [])
            if content:
                text = content[0].get("text", "")
                try:
                    schema = json.loads(text)
                    if isinstance(schema, list):
                        print(f"  Found {len(schema)} columns:")
                        for col in schema[:5]:
                            if isinstance(col, dict):
                                col_name = col.get('column_name', 'unknown')
                                col_type = col.get('data_type', 'unknown')
                                print(f"    - {col_name}: {col_type}")
                        if len(schema) > 5:
                            print(f"    ... and {len(schema) - 5} more")
                    else:
                        print(f"  Response: {text[:200]}...")
                except:
                    print(f"  Response: {text[:200]}...")
            return True
        else:
            print(f"✗ get_schema failed")
            return False

    def test_query(self) -> bool:
        """Test query tool"""
        print(f"\n{'='*60}")
        print("Test 8: query tool")
        print(f"{'='*60}")
        print("  Tool: query")
        print("  EdgeLake Command: run client () sql ... (distributed query)")

        # Determine which database and table to use
        database = self.test_database or (self.discovered_databases[0] if self.discovered_databases else None)
        table = self.test_table or (self.discovered_tables[0] if self.discovered_tables else None)

        if not database or not table:
            print(f"⚠ Skipping query - no database/table available")
            print(f"  (No --database/--table specified and none discovered)")
            return True

        print(f"  Arguments: database={database}, table={table}, limit=5")

        params = {
            "name": "query",
            "arguments": {
                "database": database,
                "table": table,
                "select": ["*"],
                "limit": 5
            }
        }

        response = self.send_mcp_request("tools/call", params, timeout=35.0)

        if response and "error" in response:
            # Query failed with an error
            error = response["error"]
            error_msg = error.get("message", "Unknown error")
            print(f"✗ query failed: {error_msg}")
            return False
        elif response and "result" in response:
            result = response["result"]
            content = result.get("content", [])
            if content:
                text = content[0].get("text", "")

                # Check if the content contains an error message
                if text.startswith("Error:") or "failed with code" in text:
                    print(f"✗ query failed: {text}")
                    return False

                print(f"✓ query successful")
                try:
                    rows = json.loads(text)
                    if isinstance(rows, list):
                        print(f"  Returned {len(rows)} rows")
                        if rows:
                            # Show first row columns
                            first_row = rows[0]
                            if isinstance(first_row, dict):
                                print(f"  Columns: {', '.join(list(first_row.keys())[:5])}")
                                if len(first_row.keys()) > 5:
                                    print(f"    ... and {len(first_row.keys()) - 5} more")
                    else:
                        print(f"  Response: {text[:200]}...")
                except:
                    print(f"  Response: {text[:200]}...")
            return True
        else:
            print(f"✗ query failed - no response")
            return False

    def test_query_aggregation(self) -> bool:
        """Test query tool with aggregation functions (AVG, MIN, MAX, COUNT)"""
        print(f"\n{'='*60}")
        print("Test 9: query tool with aggregation")
        print(f"{'='*60}")
        print("  Tool: query")
        print("  EdgeLake Command: run client () sql ... (distributed aggregation query)")
        print("  Functions: AVG, MIN, MAX, COUNT")

        # Determine which database and table to use
        database = self.test_database or (self.discovered_databases[0] if self.discovered_databases else None)
        table = self.test_table or (self.discovered_tables[0] if self.discovered_tables else None)

        if not database or not table:
            print(f"⚠ Skipping aggregation query - no database/table available")
            print(f"  (No --database/--table specified and none discovered)")
            return True

        print(f"  Arguments: database={database}, table={table}")
        print(f"  Select: avg(value), min(value), max(value), count(*)")

        params = {
            "name": "query",
            "arguments": {
                "database": database,
                "table": table,
                "select": [
                    "avg(value) as average",
                    "min(value) as minimum",
                    "max(value) as maximum",
                    "count(*) as total_rows"
                ]
            }
        }

        response = self.send_mcp_request("tools/call", params, timeout=35.0)

        if response and "error" in response:
            # Query failed with an error
            error = response["error"]
            error_msg = error.get("message", "Unknown error")
            print(f"✗ aggregation query failed: {error_msg}")
            return False
        elif response and "result" in response:
            result = response["result"]
            content = result.get("content", [])
            if content:
                text = content[0].get("text", "")

                # Check for empty response (the bug we're fixing)
                if not text or text.strip() == "":
                    print(f"✗ Got empty response (BUG: aggregation polling timeout)")
                    return False

                # Check if the content contains an error message
                if text.startswith("Error:") or "failed with code" in text:
                    print(f"✗ aggregation query failed: {text}")
                    return False

                print(f"✓ aggregation query successful")
                try:
                    data = json.loads(text)
                    if isinstance(data, list) and len(data) > 0:
                        agg_result = data[0]
                        print(f"  Aggregation results:")
                        if isinstance(agg_result, dict):
                            for key, value in agg_result.items():
                                print(f"    {key}: {value}")
                        else:
                            print(f"    {agg_result}")
                    elif isinstance(data, dict):
                        print(f"  Aggregation results:")
                        for key, value in data.items():
                            print(f"    {key}: {value}")
                    else:
                        print(f"  Response: {text[:200]}...")
                except Exception as e:
                    print(f"  Response parsing error: {e}")
                    print(f"  Raw response: {text[:200]}...")
            return True
        else:
            print(f"✗ aggregation query failed - no response")
            return False

    def run_all_tests(self) -> bool:
        """Run all tests and return overall success"""
        print(f"\n{'#'*60}")
        print(f"# EdgeLake MCP SSE Comprehensive Test Suite")
        print(f"# Host: {self.host}:{self.port}")
        print(f"# Using proper MCP SSE protocol")
        if self.test_database:
            print(f"# Test Database: {self.test_database}")
        if self.test_table:
            print(f"# Test Table: {self.test_table}")
        print(f"{'#'*60}")

        # First establish the session
        if not self.establish_session():
            print(f"\n✗ Failed to establish SSE session")
            return False

        tests = [
            ("1. Initialize", self.test_initialize),
            ("2. List Tools", self.test_list_tools),
            ("3. Tool: server_info", self.test_server_info),
            ("4. Tool: node_status", self.test_node_status),
            ("5. Tool: list_database_schema", self.test_list_database_schema),
            ("6. Tool: get_schema", self.test_get_schema),
            ("7. Tool: query", self.test_query),
            ("8. Tool: query with aggregation", self.test_query_aggregation),
        ]

        results = []
        for name, test_func in tests:
            try:
                result = test_func()
                results.append((name, result))
            except Exception as e:
                print(f"\n✗ {name} test failed with exception: {e}")
                import traceback
                traceback.print_exc()
                results.append((name, False))

        # Summary
        print(f"\n{'='*60}")
        print("Test Summary")
        print(f"{'='*60}")

        passed = sum(1 for _, result in results if result)
        total = len(results)

        for name, result in results:
            status = "✓ PASS" if result else "✗ FAIL"
            print(f"{status}: {name}")

        print(f"\nTotal: {passed}/{total} tests passed")
        print(f"{'='*60}\n")

        return passed == total


def main():
    parser = argparse.ArgumentParser(
        description="EdgeLake MCP SSE Comprehensive Test Suite - Tests all 5 implemented tools"
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="MCP server host (default: localhost)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=50051,
        help="MCP server port (default: 50051)"
    )
    parser.add_argument(
        "--database",
        type=str,
        default=None,
        help="Test database name (optional - will use first discovered if not specified)"
    )
    parser.add_argument(
        "--table",
        type=str,
        default=None,
        help="Test table name (optional - will use first discovered if not specified)"
    )

    args = parser.parse_args()

    tester = MCPSSETester(args.host, args.port, args.database, args.table)
    success = tester.run_all_tests()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
