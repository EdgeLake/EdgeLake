"""
SSE Handler with Protocol Exec Integration

Modified version of sse_handler.py that uses protocol_exec with callbacks
instead of direct_client.

This file demonstrates how to integrate protocol_exec into the SSE handler.

License: Mozilla Public License 2.0
"""

# Import original SSE handler components
from edge_lake.mcp_server.transport.sse_handler import (
    SSEConnection,
    SSETransport,
    logger
)

# Override the _process_message_sync method to use protocol_exec


def create_protocol_exec_sse_transport(mcp_server):
    """
    Create SSE transport with protocol_exec integration.

    This factory function creates a custom SSETransport that uses
    protocol_exec instead of direct_client.

    Args:
        mcp_server: MCPServer instance

    Returns:
        CustomSSETransport instance with protocol_exec
    """

    class ProtocolExecSSETransport(SSETransport):
        """
        SSE Transport that uses protocol_exec with callbacks.
        """

        def __init__(self, mcp_server):
            super().__init__(mcp_server)

            # Create protocol integration helper
            from edge_lake.mcp_server.transport.protocol_integration import MCPProtocolIntegration
            self.protocol_integration = MCPProtocolIntegration(
                mcp_server.command_builder,
                mcp_server.config
            )

            logger.info("SSE transport initialized with protocol_exec integration")

        def _process_message_sync(self, session_id: str, message: dict, socket=None):
            """
            Process MCP message using protocol_exec with callbacks.

            This overrides the base SSETransport._process_message_sync to use
            protocol_exec instead of direct_client.

            Args:
                session_id: Session identifier
                message: JSON-RPC message
                socket: Optional socket (not used with protocol_exec - callbacks handle it)
            """

            try:
                # Extract tool call information
                method = message.get('method')
                params = message.get('params', {})
                msg_id = message.get('id')

                logger.debug(f"Processing message via protocol_exec: method={method}, id={msg_id}")

                # Handle non-tool methods (initialize, tools/list)
                if method == 'initialize':
                    # Handle initialize via MCP server (not protocol_exec)
                    response = self.mcp_server.process_message(message, None)
                    with self.connection_lock:
                        connection = self.connections.get(session_id)
                    if connection:
                        connection.queue_message('message', response)
                    return

                elif method == 'tools/list':
                    # Handle tools/list via MCP server (not protocol_exec)
                    response = self.mcp_server.process_message(message, None)
                    with self.connection_lock:
                        connection = self.connections.get(session_id)
                    if connection:
                        connection.queue_message('message', response)
                    return

                elif method == 'tools/call':
                    # Execute tool via protocol_exec
                    tool_name = params.get('name')
                    arguments = params.get('arguments', {})

                    logger.debug(f"Executing tool '{tool_name}' via protocol_exec")

                    # Get SSE connection
                    with self.connection_lock:
                        connection = self.connections.get(session_id)

                    if not connection:
                        logger.error(f"Session not found: {session_id}")
                        return

                    # Execute via protocol integration (uses protocol_exec + callbacks)
                    import asyncio
                    asyncio.run(
                        self.protocol_integration.execute_tool_via_protocol_exec(
                            connection,
                            msg_id,
                            tool_name,
                            arguments
                        )
                    )

                    logger.debug(f"Tool '{tool_name}' execution completed via protocol_exec")

                else:
                    # Unknown method
                    logger.warning(f"Unknown method: {method}")

                    with self.connection_lock:
                        connection = self.connections.get(session_id)

                    if connection:
                        error_response = {
                            "jsonrpc": "2.0",
                            "id": msg_id,
                            "error": {
                                "code": -32601,
                                "message": f"Method not found: {method}"
                            }
                        }
                        connection.queue_message('error', error_response)

            except Exception as e:
                logger.error(f"Error processing message via protocol_exec: {e}", exc_info=True)

                # Send error response
                with self.connection_lock:
                    connection = self.connections.get(session_id)

                if connection:
                    error_response = {
                        "jsonrpc": "2.0",
                        "id": message.get('id'),
                        "error": {
                            "code": -32603,
                            "message": f"Internal error: {str(e)}"
                        }
                    }
                    connection.queue_message('error', error_response)

    # Return instance of custom transport
    return ProtocolExecSSETransport(mcp_server)


# Module-level instance (will be initialized by MCP server)
_sse_transport_instance = None


def initialize(mcp_server):
    """
    Initialize SSE transport with protocol_exec integration.

    Args:
        mcp_server: MCPServer instance

    Returns:
        ProtocolExecSSETransport instance
    """
    global _sse_transport_instance

    if _sse_transport_instance is not None:
        logger.warning("SSE transport already initialized")
        return _sse_transport_instance

    _sse_transport_instance = create_protocol_exec_sse_transport(mcp_server)
    return _sse_transport_instance


def get_instance():
    """Get global SSE transport instance"""
    return _sse_transport_instance


def handle_sse_endpoint(handler):
    """Handle SSE endpoint (called from http_server.py)"""
    if _sse_transport_instance is None:
        logger.error("SSE transport not initialized")
        handler.send_error(503, "MCP server not available")
        return False

    return _sse_transport_instance.handle_sse_endpoint(handler)


def handle_messages_endpoint(handler):
    """Handle messages endpoint (called from http_server.py)"""
    if _sse_transport_instance is None:
        logger.error("SSE transport not initialized")
        handler.send_error(503, "MCP server not available")
        return False

    return _sse_transport_instance.handle_messages_endpoint(handler)


def shutdown():
    """Shutdown SSE transport"""
    global _sse_transport_instance

    if _sse_transport_instance is not None:
        _sse_transport_instance.shutdown()
        _sse_transport_instance = None
