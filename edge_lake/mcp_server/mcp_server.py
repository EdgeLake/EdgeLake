"""
EdgeLake MCP Server

Lightweight MCP protocol server integrated with EdgeLake's http_server.py.
Uses SSE transport for communication with MCP clients.

License: Mozilla Public License 2.0
"""

import asyncio
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List

from .config import Config
from .core import CommandBuilder
from .core.direct_client import EdgeLakeDirectClient
from .tools import ToolGenerator, ToolExecutor
from . import __version__

logger = logging.getLogger(__name__)


class MCPServer:
    """
    MCP Server integrated with EdgeLake's http_server.py.

    Handles JSON-RPC protocol for MCP tool listing and execution.
    Uses SSE transport for streaming responses to clients.
    """

    def __init__(self, config_dir: str = None, enabled_tools: list = None,
                 capabilities: dict = None):
        """
        Initialize MCP server.

        Args:
            config_dir: Path to configuration directory (optional)
            enabled_tools: List of tool names to enable, or None for all
            capabilities: Node capability dictionary
        """
        self.enabled_tools = enabled_tools
        self.capabilities = capabilities or {}
        self.transport = None  # Will be set by start()

        # Load configuration
        if config_dir:
            self.config = Config(Path(config_dir))
        else:
            # Use default config directory
            config_path = Path(__file__).parent / "config"
            self.config = Config(config_path)

        # Initialize direct client
        self.client = EdgeLakeDirectClient(
            max_workers=self.config.get_max_workers()
        )
        logger.debug("Direct EdgeLake client initialized")

        # Initialize builders
        self.command_builder = CommandBuilder()

        # Initialize tools
        self.tool_generator = ToolGenerator(
            self.config.get_all_tools(),
            enabled_tools=enabled_tools
        )
        self.tool_executor = ToolExecutor(
            self.client,
            self.command_builder,
            self.config
        )

        logger.info(f"MCP Server initialized (version={__version__})")

    def start(self):
        """
        Start MCP server (register with http_server).

        Called by 'run mcp server' command in member_cmd.py.
        This initializes the SSE transport which integrates with http_server.py.
        """
        # Initialize SSE transport
        from .transport.sse_handler import initialize as init_sse
        self.transport = init_sse(self)

        logger.info("MCP Server started and integrated with http_server.py")
        logger.info(f"Endpoints: GET /mcp/sse, POST /mcp/messages/{{session_id}}")

    def stop(self):
        """
        Stop MCP server and cleanup.

        Called by 'exit mcp server' command in member_cmd.py.
        """
        logger.info("Shutting down MCP Server")

        # Shutdown SSE transport
        if self.transport:
            from .transport.sse_handler import shutdown as shutdown_sse
            shutdown_sse()
            self.transport = None

        # Close direct client
        if self.client:
            self.client.close()

        logger.info("MCP Server shutdown complete")

    def process_message(self, message: Dict[str, Any], socket=None) -> Dict[str, Any]:
        """
        Process incoming MCP message (JSON-RPC).

        This method is called by SSETransport when a message is received
        via POST /mcp/messages/{session_id}.

        Args:
            message: JSON-RPC message from client
            socket: Optional handler socket for streaming large results

        Returns:
            JSON-RPC response
        """
        # Extract method
        method = message.get('method')
        params = message.get('params', {})
        msg_id = message.get('id')

        logger.debug(f"Processing MCP method: {method}")

        try:
            # Route to appropriate handler
            if method == 'initialize':
                # MCP initialize handshake
                return {
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {
                            "tools": {}
                        },
                        "serverInfo": {
                            "name": "edgelake-mcp-server",
                            "version": __version__
                        }
                    }
                }

            elif method == 'tools/list':
                # List available tools
                tools = asyncio.run(self._list_tools())
                return {
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "result": {
                        "tools": tools
                    }
                }

            elif method == 'tools/call':
                # Execute a tool
                tool_name = params.get('name')
                arguments = params.get('arguments', {})

                # Pass socket for streaming aggregated query results
                content = asyncio.run(self._call_tool(tool_name, arguments, socket))
                return {
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "result": {
                        "content": content
                    }
                }

            else:
                # Unknown method
                return {
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "error": {
                        "code": -32601,
                        "message": f"Method not found: {method}"
                    }
                }

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "error": {
                    "code": -32603,
                    "message": f"Internal error: {str(e)}"
                }
            }

    async def _list_tools(self) -> List[Dict[str, Any]]:
        """
        List available MCP tools.

        Returns:
            List of tool definitions as dicts
        """
        logger.debug("Listing tools")
        tools = self.tool_generator.generate_tools()
        logger.debug(f"Returning {len(tools)} tools")
        return tools

    async def _call_tool(self, name: str, arguments: dict, socket=None) -> List[Dict[str, Any]]:
        """
        Execute a tool.

        Args:
            name: Tool name
            arguments: Tool arguments
            socket: Optional handler socket for streaming large results

        Returns:
            List of content items (text responses)
        """
        logger.debug(f"Calling tool '{name}' with arguments: {arguments}")

        try:
            # Execute tool with socket for streaming aggregated query results
            result = await self.tool_executor.execute_tool(name, arguments, socket)

            # result is already in correct format: [{"type": "text", "text": "..."}]
            return result

        except Exception as e:
            logger.error(f"Error calling tool '{name}': {e}", exc_info=True)
            # Return error as text content
            return [{
                "type": "text",
                "text": f"Error: {str(e)}"
            }]

    def get_info(self) -> Dict[str, Any]:
        """
        Get server information (for debugging/monitoring).

        Returns:
            Server info dict
        """
        active_connections = []
        if self.transport:
            active_connections = self.transport.get_active_connections()

        # Detect which execution path is active
        execution_path = "direct_client"  # Default
        if self.transport:
            # Check if transport has protocol_integration attribute
            if hasattr(self.transport, 'protocol_integration'):
                execution_path = "protocol_exec"

        return {
            "version": __version__,
            "protocol": "MCP via SSE",
            "execution_path": execution_path,
            "active_connections": len(active_connections),
            "connection_ids": active_connections,
            "enabled_tools": self.enabled_tools or "all",
            "transport": "SSE over http_server.py",
            "tools_count": len(self.tool_generator.generate_tools())
        }
