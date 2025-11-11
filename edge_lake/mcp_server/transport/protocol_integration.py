"""
Protocol Integration for MCP

Integrates protocol_exec with MCP/SSE transport using protocol callbacks.

This module bridges between MCP tool calls and EdgeLake command execution
using the protocol_exec callback architecture.

License: Mozilla Public License 2.0
"""

import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class MCPProtocolIntegration:
    """
    Integrates protocol_exec with MCP/SSE transport.

    Handles:
    - Building EdgeLake commands from MCP tool calls
    - Creating protocol callbacks for MCP
    - Executing commands via protocol_exec
    - Error handling and response formatting
    """

    def __init__(self, command_builder, config):
        """
        Initialize MCP protocol integration.

        Args:
            command_builder: CommandBuilder instance for building EdgeLake commands
            config: Config instance for tool definitions
        """
        self.command_builder = command_builder
        self.config = config

    async def execute_tool_via_protocol_exec(
        self,
        sse_connection,
        json_rpc_id: int,
        tool_name: str,
        arguments: Dict[str, Any]
    ) -> None:
        """
        Execute MCP tool using protocol_exec with callbacks.

        Args:
            sse_connection: SSEConnection instance
            json_rpc_id: JSON-RPC message ID
            tool_name: Tool name
            arguments: Tool arguments

        Note:
            Response is automatically queued for SSE delivery via callbacks.
            This function does not return a value - callbacks handle the response.
        """

        logger.debug(f"[MCP Protocol] Executing tool '{tool_name}' via protocol_exec")

        # Testing mode logging
        if self.config.testing_mode:
            logger.info(f"[TESTING] Tool '{tool_name}' called via protocol_exec")
            import json
            logger.info(f"[TESTING] Input arguments: {json.dumps(arguments, indent=2)}")

        try:
            # 1. Get tool configuration
            tool_config = self.config.get_tool_by_name(tool_name)
            if not tool_config:
                raise ValueError(f"Unknown tool: {tool_name}")

            edgelake_cmd = tool_config.edgelake_command

            # 2. Handle internal commands (server_info, etc.)
            if edgelake_cmd.get('type') == 'internal':
                result = await self._execute_internal(edgelake_cmd, arguments)

                # Format and queue response
                import json
                success_response = {
                    "jsonrpc": "2.0",
                    "id": json_rpc_id,
                    "result": {
                        "content": [{
                            "type": "text",
                            "text": result
                        }]
                    }
                }
                sse_connection.queue_message('message', success_response)
                return

            # 3. Build EdgeLake command
            command = self._build_edgelake_command(tool_config, arguments)
            logger.debug(f"[MCP Protocol] Built command: {command}")

            # 4. Create protocol callbacks
            from edge_lake.generic.protocol_callbacks import create_mcp_callbacks
            protocol_callbacks = create_mcp_callbacks(sse_connection, json_rpc_id)

            # 5. Create status object
            from edge_lake.generic import process_status
            status = process_status.ProcessStat()

            # Set REST caller for JSON formatting
            j_handle = status.get_job_handle()
            j_handle.set_rest_caller()

            # 6. Execute via protocol_exec
            from edge_lake.cmd.protocol_exec import protocol_exec

            logger.debug(f"[MCP Protocol] Calling protocol_exec")
            ret_val = protocol_exec(status, command, protocol_callbacks)

            logger.debug(f"[MCP Protocol] protocol_exec completed: ret_val={ret_val}")

            # 7. Handle buffered output (if any)
            # For queries, results may have been written to buffer
            if protocol_callbacks.has_buffered_output():
                buffered_data = protocol_callbacks.get_buffered_output()
                if buffered_data and ret_val == process_status.SUCCESS:
                    # Buffer has data but wasn't sent yet - send it now
                    result_text = buffered_data.decode('utf-8') if isinstance(buffered_data, bytes) else str(buffered_data)
                    if result_text.strip():
                        logger.debug(f"[MCP Protocol] Sending buffered output: {len(result_text)} bytes")
                        protocol_callbacks.send_success(status, result_text)

            # Testing mode logging
            if self.config.testing_mode:
                logger.info(f"[TESTING] Tool '{tool_name}' completed via protocol_exec: ret_val={ret_val}")

        except Exception as e:
            logger.error(f"[MCP Protocol] Error executing tool '{tool_name}': {e}", exc_info=True)

            # Send error via protocol callbacks (if we have them)
            try:
                from edge_lake.generic.protocol_callbacks import create_mcp_callbacks
                from edge_lake.generic import process_status

                error_callbacks = create_mcp_callbacks(sse_connection, json_rpc_id)
                error_status = process_status.ProcessStat()

                error_callbacks.send_error(
                    error_status,
                    process_status.ERR_command_struct,
                    str(e),
                    {"tool": tool_name, "arguments": arguments}
                )
            except Exception as inner_e:
                logger.error(f"[MCP Protocol] Failed to send error via callbacks: {inner_e}")

                # Fallback: queue error directly
                error_response = {
                    "jsonrpc": "2.0",
                    "id": json_rpc_id,
                    "error": {
                        "code": -32603,
                        "message": f"Internal error: {str(e)}"
                    }
                }
                sse_connection.queue_message('error', error_response)

    def _build_edgelake_command(self, tool_config, arguments: Dict[str, Any]) -> str:
        """
        Build EdgeLake command from tool configuration and arguments.

        Args:
            tool_config: Tool configuration from config
            arguments: Tool arguments

        Returns:
            EdgeLake command string
        """

        edgelake_cmd = tool_config.edgelake_command

        # Check if we need to build SQL query
        if edgelake_cmd.get('build_sql'):
            # Build SQL query from arguments
            sql_query = self.command_builder.build_sql_query(arguments)

            # Build full SQL command
            database = arguments.get('database')
            output_format = edgelake_cmd.get('format', 'json')

            # Add destination if specified
            destination = arguments.get('destination')
            if destination:
                command = f'sql {database} format = {output_format} and destination = {destination} "{sql_query}"'
            else:
                command = f'sql {database} format = {output_format} "{sql_query}"'

            logger.debug(f"Built SQL command: {command}")
        else:
            # Use command builder to build from template
            command, headers = self.command_builder.build_command(edgelake_cmd, arguments)

            # TODO: Handle headers if needed (for run client, etc.)
            if headers:
                logger.debug(f"Command has headers: {headers}")

        return command

    async def _execute_internal(self, edgelake_cmd: Dict[str, Any], arguments: Dict[str, Any]) -> str:
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
        import json

        info = {
            "version": __version__,
            "server_name": "edgelake-mcp-server",
            "mode": "embedded",
            "execution": "protocol_exec with callbacks",
            "configuration": {
                "total_tools": len(self.config.tools),
                "request_timeout": self.config.get_request_timeout(),
                "max_workers": self.config.get_max_workers()
            }
        }

        return json.dumps(info, indent=2)
