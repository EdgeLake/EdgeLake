#!/bin/bash

# Test blockchain get table with different format parameters via MCP server
# This helps us understand the current behavior before making changes

HOST="${1:-192.168.1.65}"
PORT="${2:-32349}"

echo "=========================================="
echo "Testing blockchain format behavior"
echo "Host: $HOST:$PORT"
echo "=========================================="

# First establish SSE session and get session ID
echo ""
echo "Step 1: Establishing SSE session..."
SESSION_RESPONSE=$(timeout 2 curl -N -s "http://$HOST:$PORT/mcp/sse" 2>/dev/null | head -20)

# Extract session ID from endpoint event
SESSION_ID=$(echo "$SESSION_RESPONSE" | grep "data:" | grep "endpoint" | sed 's/.*messages\/\([^"]*\).*/\1/' | head -1)

if [ -z "$SESSION_ID" ]; then
    echo "✗ Failed to get session ID"
    echo "Response:"
    echo "$SESSION_RESPONSE"
    exit 1
fi

echo "✓ Session ID: $SESSION_ID"

# Helper function to call a tool and show the result
call_tool() {
    local tool_name="$1"
    local args="$2"
    local description="$3"

    echo ""
    echo "=========================================="
    echo "Test: $description"
    echo "Tool: $tool_name"
    echo "=========================================="

    # Send tool call request
    RESPONSE=$(curl -s -X POST "http://$HOST:$PORT/mcp/messages/$SESSION_ID" \
        -H "Content-Type: application/json" \
        -d "{
            \"jsonrpc\": \"2.0\",
            \"id\": $(date +%s),
            \"method\": \"tools/call\",
            \"params\": {
                \"name\": \"$tool_name\",
                \"arguments\": $args
            }
        }")

    echo "HTTP Response: $RESPONSE"

    # Wait a bit for SSE response
    sleep 2
}

# Test list_database_schema (uses format=mcp)
call_tool "list_database_schema" "{}" "list_database_schema tool (format=mcp)"

echo ""
echo "=========================================="
echo "Now check the SSE stream for responses"
echo "Run this in another terminal:"
echo "  curl -N http://$HOST:$PORT/mcp/sse"
echo "=========================================="
