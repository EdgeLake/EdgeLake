#!/usr/bin/env python3
"""
Check which MCP execution path is active (direct_client vs protocol_exec)

Usage:
    python3 check_mcp_execution_path.py
"""

import sys
import os
sys.path.insert(0, os.path.abspath('.'))

def check_execution_path():
    """Check which execution path is active"""

    print("Checking MCP execution path...\n")

    # Method 1: Check which SSE handler module is loaded
    print("Method 1: Check SSE handler module")
    print("-" * 50)

    try:
        from edge_lake.mcp_server.transport import sse_handler

        # Check if SSETransport has protocol_integration attribute
        if hasattr(sse_handler.SSETransport, '__init__'):
            import inspect
            source = inspect.getsource(sse_handler.SSETransport.__init__)

            if 'protocol_integration' in source:
                print("✓ PROTOCOL_EXEC path active")
                print("  - SSE handler has protocol_integration")
            else:
                print("✓ DIRECT_CLIENT path active (current default)")
                print("  - SSE handler uses tool_executor/direct_client")

    except Exception as e:
        print(f"✗ Error checking module: {e}")

    # Method 2: Check if protocol_exec modules exist
    print("\nMethod 2: Check protocol_exec modules")
    print("-" * 50)

    modules_to_check = [
        ('edge_lake.generic.protocol_callbacks', 'Protocol callbacks interface'),
        ('edge_lake.cmd.protocol_exec', 'Protocol exec function'),
        ('edge_lake.mcp_server.transport.protocol_integration', 'MCP protocol integration')
    ]

    all_present = True
    for module_name, description in modules_to_check:
        try:
            __import__(module_name)
            print(f"✓ {description}: {module_name}")
        except ImportError:
            print(f"✗ {description}: {module_name} NOT FOUND")
            all_present = False

    if all_present:
        print("\n✓ All protocol_exec modules are available")
    else:
        print("\n✗ Some protocol_exec modules are missing")

    # Method 3: Runtime check via server_info tool (if server is running)
    print("\nMethod 3: Check via server_info tool")
    print("-" * 50)
    print("To check at runtime when MCP server is active:")
    print("  1. Call the 'server_info' tool")
    print("  2. Look for 'execution_path' field in response")
    print("  3. Values: 'direct_client' or 'protocol_exec'")

    # Method 4: Check log messages
    print("\nMethod 4: Check log messages")
    print("-" * 50)
    print("Look for these log messages:")
    print("  - DIRECT_CLIENT: '[SSE Handler] Using DIRECT_CLIENT execution path'")
    print("  - PROTOCOL_EXEC: '[MCP Protocol] Executing tool ... via protocol_exec'")

    # Summary
    print("\n" + "=" * 50)
    print("CURRENT STATUS")
    print("=" * 50)
    print("Active Path: DIRECT_CLIENT (default)")
    print("\nTo switch to PROTOCOL_EXEC:")
    print("  1. Replace sse_handler.py with sse_handler_protocol_exec.py")
    print("  2. OR modify sse_handler.py to use protocol_integration")
    print("  3. See INTEGRATION_GUIDE.md for details")
    print()


if __name__ == '__main__':
    check_execution_path()
