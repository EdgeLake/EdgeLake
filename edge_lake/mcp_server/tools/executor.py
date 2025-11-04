"""
Tool Executor

Executes MCP tools and formats responses.

License: Mozilla Public License 2.0
"""

import json
import logging
from typing import Any, Dict, List, Optional
# JSONPath removed - using direct dict access for {"Query": [...]} unwrapping

logger = logging.getLogger(__name__)


class ToolExecutor:
    """
    Executes MCP tools using EdgeLake client and formats responses.
    """

    def __init__(self, client, command_builder, config):
        """
        Initialize tool executor.

        Args:
            client: EdgeLakeClient instance
            command_builder: CommandBuilder instance
            config: Config instance
        """
        self.client = client
        self.command_builder = command_builder
        self.config = config
    
    async def execute_tool(self, name: str, arguments: Dict[str, Any], socket=None) -> List[Dict[str, Any]]:
        """
        Execute a tool by name with given arguments.

        Args:
            name: Tool name
            arguments: Tool arguments
            socket: Optional socket for streaming large results (aggregated queries)

        Returns:
            List of TextContent dicts for MCP response
        """
        # Store current tool name for debugging/logging
        self._current_tool = name

        # Testing mode: log tool entry and inputs
        if self.config.testing_mode:
            logger.info(f"[TESTING] Tool '{name}' called")
            logger.info(f"[TESTING] Input arguments: {json.dumps(arguments, indent=2)}")
        else:
            logger.debug(f"Executing tool '{name}' with arguments: {arguments}")

        try:
            # Get tool configuration
            tool_config = self.config.get_tool_by_name(name)
            if not tool_config:
                raise ValueError(f"Unknown tool: {name}")

            # Execute based on command type
            edgelake_cmd = tool_config.edgelake_command
            cmd_type = edgelake_cmd.get('type')

            if cmd_type == 'internal':
                result = await self._execute_internal(edgelake_cmd, arguments)
            else:
                # All tools (including SQL): use member_cmd.process_cmd() path
                # This provides consistent behavior for local and network queries
                result = await self._execute_edgelake_command(tool_config, arguments, self.client, socket)

            # Testing mode: log final response
            if self.config.testing_mode:
                logger.info(f"[TESTING] Final response for '{name}': {result[:500] if isinstance(result, str) else str(result)[:500]}...")

            # Format response
            return self._format_response(result)

        except Exception as e:
            logger.error(f"Error executing tool '{name}': {e}", exc_info=True)
            # Re-raise exception so MCP SDK can convert it to proper JSON-RPC error
            # This ensures test clients see an error response, not a successful result
            raise
    
    
    async def _execute_internal(self, edgelake_cmd: Dict[str, Any], 
                                arguments: Dict[str, Any]) -> str:
        """
        Execute internal command (handled by server, not EdgeLake).
        
        Args:
            edgelake_cmd: Command configuration
            arguments: Arguments
        
        Returns:
            Result string
        """
        method = edgelake_cmd.get('method', '')
        
        if method == 'server_info':
            return self._get_server_info()
        else:
            raise ValueError(f"Unknown internal method: {method}")
    
    def _get_server_info(self) -> str:
        """Get MCP server information (embedded mode)"""
        from .. import __version__

        info = {
            "version": __version__,
            "server_name": "edgelake-mcp-server",
            "mode": "embedded",
            "configuration": {
                "total_tools": len(self.config.tools),
                "request_timeout": self.config.get_request_timeout(),
                "max_workers": self.config.get_max_workers()
            }
        }

        return json.dumps(info, indent=2)

    async def _execute_edgelake_command(self, tool_config, arguments: Dict[str, Any],
                                       client, socket=None) -> str:
        """
        Execute EdgeLake command.

        Args:
            tool_config: Tool configuration
            arguments: Tool arguments
            client: EdgeLake client to use
            socket: Optional socket for streaming large results

        Returns:
            Result string
        """
        edgelake_cmd = tool_config.edgelake_command

        # Check if we need to build SQL query
        if edgelake_cmd.get('build_sql'):
            # Build SQL query from arguments
            sql_query = self.command_builder.build_sql_query(arguments)

            # Build full SQL command
            database = arguments.get('database')
            # Get format from edgelake_cmd config, default to 'json' for backward compatibility
            output_format = edgelake_cmd.get('format', 'json')
            command = f'sql {database} format = {output_format} "{sql_query}"'

            # Get headers
            headers = edgelake_cmd.get('headers')

            logger.debug(f"Built SQL command: {command}")
        else:
            # Build command from template
            command, headers = self.command_builder.build_command(
                edgelake_cmd,
                arguments
            )

        # For network queries, wrap with 'run client ()' to distribute across network
        if headers and headers.get('destination') == 'network':
            command = f'run client () {command}'
            if self.config.testing_mode:
                logger.info(f"[TESTING] Network query - wrapped with run client")
            else:
                logger.debug(f"Network query - wrapped with run client: {command}")

        # Testing mode: log command sent to member_cmd
        if self.config.testing_mode:
            logger.info(f"[TESTING] Command sent to member_cmd.process_cmd(): {command}")

        # Execute command with socket for streaming large results
        result = await client.execute_command(command, headers=headers, socket=socket)

        # Testing mode: log raw output from member_cmd
        if self.config.testing_mode:
            raw_preview = result[:500] if isinstance(result, str) else str(result)[:500]
            logger.info(f"[TESTING] Raw output from member_cmd: {raw_preview}...")

        # Format result
        if isinstance(result, dict):
            return json.dumps(result, indent=2)
        elif isinstance(result, list):
            return json.dumps(result, indent=2)
        else:
            return str(result)

    def _format_response(self, result: str) -> List[Dict[str, Any]]:
        """
        Format result as MCP TextContent.
        
        Args:
            result: Result string
        
        Returns:
            List of TextContent dicts
        """
        return [{
            "type": "text",
            "text": result
        }]
    
    def _format_error(self, error_message: str) -> List[Dict[str, Any]]:
        """
        Format error as MCP TextContent.
        
        Args:
            error_message: Error message
        
        Returns:
            List of TextContent dicts
        """
        return [{
            "type": "text",
            "text": f"Error: {error_message}"
        }]
