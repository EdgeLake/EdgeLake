#!/usr/bin/env python3
"""
Test Protocol Exec Integration

Tests the protocol_exec integration with MCP callbacks.

Usage:
    python3 test_protocol_exec_integration.py
"""

import sys
import os

# Add EdgeLake to path
sys.path.insert(0, os.path.abspath('.'))


def test_protocol_callbacks():
    """Test MCPProtocolCallbacks directly"""

    print("=" * 60)
    print("Test 1: Protocol Callbacks")
    print("=" * 60)

    from edge_lake.generic.protocol_callbacks import MCPProtocolCallbacks
    from edge_lake.generic import process_status

    # Mock SSE connection
    class MockConnection:
        def __init__(self):
            self.messages = []

        def queue_message(self, event_type, data):
            self.messages.append((event_type, data))
            print(f"  → Queued {event_type}: id={data.get('id')}, has_result={'result' in data}, has_error={'error' in data}")

    # Test 1a: Success response
    print("\nTest 1a: Send success response")
    connection = MockConnection()
    callbacks = MCPProtocolCallbacks(connection, json_rpc_id=123)

    status = process_status.ProcessStat()
    callbacks.send_success(status, '{"result": "test"}', "text/json")

    assert len(connection.messages) == 1, "Should have 1 message"
    assert connection.messages[0][0] == 'message', "Should be message event"
    assert connection.messages[0][1]['id'] == 123, "Should have correct ID"
    assert 'result' in connection.messages[0][1], "Should have result field"
    print("  ✓ Success response formatted correctly")

    # Test 1b: Error response
    print("\nTest 1b: Send error response")
    connection = MockConnection()
    callbacks = MCPProtocolCallbacks(connection, json_rpc_id=456)

    status = process_status.ProcessStat()
    callbacks.send_error(status, 155, "Test error message", {"detail": "test"})

    assert len(connection.messages) == 1, "Should have 1 message"
    assert connection.messages[0][0] == 'error', "Should be error event"
    assert connection.messages[0][1]['id'] == 456, "Should have correct ID"
    assert 'error' in connection.messages[0][1], "Should have error field"
    assert connection.messages[0][1]['error']['message'] == "Test error message"
    print("  ✓ Error response formatted correctly")

    # Test 1c: Output socket
    print("\nTest 1c: Get output socket")
    connection = MockConnection()
    callbacks = MCPProtocolCallbacks(connection, json_rpc_id=789)

    socket = callbacks.get_output_socket(status)
    assert hasattr(socket, 'write'), "Should have write method"
    assert hasattr(socket, 'getvalue'), "Should have getvalue method (BytesIO)"

    # Write some data
    socket.write(b'test data')
    socket.seek(0)
    data = socket.read()
    assert data == b'test data', "Should read back written data"
    print("  ✓ Output socket works correctly")

    print("\n✓ All protocol callbacks tests passed!\n")


def test_protocol_exec_simple():
    """Test protocol_exec with simple command"""

    print("=" * 60)
    print("Test 2: Protocol Exec (Simple Command)")
    print("=" * 60)

    from edge_lake.cmd.protocol_exec import protocol_exec
    from edge_lake.generic.protocol_callbacks import MCPProtocolCallbacks
    from edge_lake.generic import process_status

    # Mock SSE connection
    class MockConnection:
        def __init__(self):
            self.messages = []

        def queue_message(self, event_type, data):
            self.messages.append((event_type, data))
            print(f"  → Response queued: {event_type}")
            if 'result' in data:
                content = data['result'].get('content', [])
                if content:
                    text = content[0].get('text', '')
                    print(f"     Result preview: {text[:100]}...")
            elif 'error' in data:
                print(f"     Error: {data['error']['message']}")

    # Test 2a: get status command
    print("\nTest 2a: Execute 'get status' command")
    connection = MockConnection()
    callbacks = MCPProtocolCallbacks(connection, json_rpc_id=100)

    status = process_status.ProcessStat()
    ret_val = protocol_exec(status, "get status", callbacks)

    print(f"  → protocol_exec returned: {ret_val}")
    print(f"  → Messages queued: {len(connection.messages)}")

    if connection.messages:
        msg_type, msg_data = connection.messages[0]
        if 'result' in msg_data:
            print("  ✓ Success response received")
        elif 'error' in msg_data:
            print(f"  ⚠ Error response: {msg_data['error']['message']}")
    else:
        print("  ⚠ No messages queued")

    # Test 2b: Invalid command
    print("\nTest 2b: Execute invalid command")
    connection = MockConnection()
    callbacks = MCPProtocolCallbacks(connection, json_rpc_id=200)

    status = process_status.ProcessStat()
    ret_val = protocol_exec(status, "", callbacks)  # Empty command

    print(f"  → protocol_exec returned: {ret_val}")

    if connection.messages:
        msg_type, msg_data = connection.messages[0]
        if 'error' in msg_data:
            print(f"  ✓ Error response: {msg_data['error']['message']}")
        else:
            print("  ⚠ Expected error but got success")

    print("\n✓ Protocol exec tests completed!\n")


def test_command_builder():
    """Test CommandBuilder integration"""

    print("=" * 60)
    print("Test 3: Command Builder")
    print("=" * 60)

    from edge_lake.mcp_server.core.command_builder import CommandBuilder

    builder = CommandBuilder()

    # Test 3a: Simple template
    print("\nTest 3a: Build command from template")
    edgelake_command = {
        'type': 'command',
        'template': 'get {resource} where format = {format}'
    }
    arguments = {'resource': 'status', 'format': 'json'}

    command, headers = builder.build_command(edgelake_command, arguments)
    print(f"  → Command: {command}")
    assert command == 'get status where format = json'
    print("  ✓ Template filled correctly")

    # Test 3b: SQL query
    print("\nTest 3b: Build SQL query")
    arguments = {
        'select': ['id', 'name'],
        'table': 'my_table',
        'where': 'id > 100',
        'limit': 10
    }

    query = builder.build_sql_query(arguments)
    print(f"  → Query: {query}")
    assert 'SELECT id, name' in query
    assert 'FROM my_table' in query
    assert 'WHERE id > 100' in query
    assert 'LIMIT 10' in query
    print("  ✓ SQL query built correctly")

    print("\n✓ Command builder tests passed!\n")


def test_integration_flow():
    """Test complete integration flow (mock)"""

    print("=" * 60)
    print("Test 4: Complete Integration Flow (Mock)")
    print("=" * 60)

    from edge_lake.mcp_server.transport.protocol_integration import MCPProtocolIntegration
    from edge_lake.mcp_server.core.command_builder import CommandBuilder

    # Mock config
    class MockConfig:
        def __init__(self):
            self.tools = []
            self.testing_mode = True

        def get_tool_by_name(self, name):
            if name == 'server_info':
                return type('Tool', (), {
                    'edgelake_command': {
                        'type': 'internal',
                        'method': 'server_info'
                    }
                })()
            return None

        def get_request_timeout(self):
            return 30

        def get_max_workers(self):
            return 5

    # Mock SSE connection
    class MockConnection:
        def __init__(self):
            self.messages = []

        def queue_message(self, event_type, data):
            self.messages.append((event_type, data))
            print(f"  → {event_type} queued: id={data.get('id')}")

    print("\nTest 4a: Execute server_info via protocol integration")

    builder = CommandBuilder()
    config = MockConfig()
    integration = MCPProtocolIntegration(builder, config)

    connection = MockConnection()

    import asyncio
    asyncio.run(
        integration.execute_tool_via_protocol_exec(
            connection,
            json_rpc_id=999,
            tool_name='server_info',
            arguments={}
        )
    )

    assert len(connection.messages) == 1, "Should have 1 message"
    msg_type, msg_data = connection.messages[0]
    assert 'result' in msg_data, "Should have result"

    result_text = msg_data['result']['content'][0]['text']
    print(f"  → Result: {result_text[:100]}...")

    import json
    result_json = json.loads(result_text)
    assert 'version' in result_json
    assert result_json['execution'] == 'protocol_exec with callbacks'

    print("  ✓ Integration flow works correctly")

    print("\n✓ All integration tests passed!\n")


def main():
    """Run all tests"""

    print("\n" + "=" * 60)
    print("Protocol Exec Integration Test Suite")
    print("=" * 60 + "\n")

    try:
        test_protocol_callbacks()
        test_protocol_exec_simple()
        test_command_builder()
        test_integration_flow()

        print("=" * 60)
        print("✓ ALL TESTS PASSED!")
        print("=" * 60 + "\n")

        print("Next steps:")
        print("  1. Review integration in sse_handler_protocol_exec.py")
        print("  2. Test with MCP client (Claude Desktop)")
        print("  3. Compare results with old approach")
        print("  4. Commit when ready")

    except Exception as e:
        print(f"\n✗ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
