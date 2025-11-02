#!/usr/bin/env python3
"""
Test DirectClient behavior with different format parameters.

This script tests how DirectClient handles:
1. blockchain get table where format=json
2. blockchain get table where format=mcp
3. get version where format=mcp

Purpose: Understand current behavior before making changes.
"""

import asyncio
import json
import sys
import os

# Add EdgeLake to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'edge_lake'))

from edge_lake.mcp_server.core.direct_client import EdgeLakeDirectClient


async def test_format(client, command, description):
    """Test a command and show the result format"""
    print(f"\n{'='*70}")
    print(f"Test: {description}")
    print(f"Command: {command}")
    print(f"{'='*70}")

    try:
        result = await client.execute_command(command)

        print(f"\nResult type: {type(result)}")
        print(f"Result length: {len(str(result))} chars")

        # Show first 500 chars
        result_preview = str(result)[:500]
        print(f"\nResult preview:")
        print(result_preview)

        # Try to parse as JSON
        print(f"\nJSON parsing test:")
        if isinstance(result, str):
            try:
                parsed = json.loads(result)
                print(f"✓ Valid JSON - parsed successfully")
                print(f"  Parsed type: {type(parsed)}")
                if isinstance(parsed, list) and len(parsed) > 0:
                    print(f"  First item: {parsed[0]}")
            except json.JSONDecodeError as e:
                print(f"✗ Invalid JSON - parse failed: {e}")
                # Try to show what's wrong
                print(f"  Looking for Python dict syntax (single quotes)...")
                if "'" in result_preview:
                    print(f"  ✗ Found single quotes - this is Python dict format, not JSON")
        elif isinstance(result, (dict, list)):
            print(f"✓ Already a Python object (dict/list)")
            print(f"  Type: {type(result)}")

    except Exception as e:
        print(f"✗ Command failed: {e}")
        import traceback
        traceback.print_exc()


async def main():
    print("="*70)
    print("DirectClient Format Behavior Test")
    print("="*70)

    # Initialize DirectClient
    print("\nInitializing DirectClient...")
    client = EdgeLakeDirectClient(max_workers=1)

    # Test 1: blockchain get table with format=json
    await test_format(
        client,
        "blockchain get table where format=json",
        "blockchain get table where format=json"
    )

    # Test 2: blockchain get table with format=mcp
    await test_format(
        client,
        "blockchain get table where format=mcp",
        "blockchain get table where format=mcp"
    )

    # Test 3: get version with format=mcp
    await test_format(
        client,
        "get version where format=mcp",
        "get version where format=mcp"
    )

    # Test 4: blockchain get table with NO format (default)
    await test_format(
        client,
        "blockchain get table",
        "blockchain get table (no format, default behavior)"
    )

    # Cleanup
    client.close()

    print("\n" + "="*70)
    print("Test complete")
    print("="*70)


if __name__ == "__main__":
    asyncio.run(main())
