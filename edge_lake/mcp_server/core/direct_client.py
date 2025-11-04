"""
EdgeLake Direct Client

Direct integration client for embedded MCP server - calls member_cmd.process_cmd()
directly without HTTP overhead. Uses socket streaming like HTTP REST.

License: Mozilla Public License 2.0
"""

import asyncio
import json
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class EdgeLakeDirectClient:
    """
    Direct integration client for EdgeLake MCP server.

    Calls member_cmd.process_cmd() directly (synchronously) with socket streaming.
    Architecture matches HTTP REST server - no stdout capture, results stream to socket.
    """

    def __init__(self, max_workers: int = None):
        """
        Initialize direct client.

        Args:
            max_workers: Ignored (kept for API compatibility)
        """
        self._shutdown = False

        # Import EdgeLake modules
        try:
            from edge_lake.cmd import member_cmd, native_api
            from edge_lake.generic import process_status, params

            self.member_cmd = member_cmd
            self.native_api = native_api
            self.process_status = process_status
            self.params = params

            logger.debug("EdgeLake direct client initialized")

        except ImportError as e:
            logger.error(f"Failed to import EdgeLake modules: {e}")
            raise

    async def execute_command(self, command: str, headers: Optional[Dict[str, str]] = None, timeout: float = 30.0, socket=None) -> Any:
        """
        Execute an EdgeLake command directly via socket streaming (like HTTP REST).

        Args:
            command: EdgeLake command string
            headers: Optional headers (for compatibility, mostly ignored in direct mode)
            timeout: Command timeout in seconds (default: 30, currently not enforced)
            socket: Socket for streaming results (required for MCP protocol)

        Returns:
            Command result via socket streaming or result_set for non-query commands

        Note:
            Socket-based approach matches HTTP REST server architecture.
            Query results stream directly to socket via OutputManager.
            Non-query commands return results via result_set.
        """
        logger.debug(f"Executing command directly: {command} (socket={'provided' if socket else 'none'})")

        # Check if client is shutting down - return empty result instead of raising
        if self._shutdown:
            logger.debug(f"Client is shutting down, returning empty result for: {command}")
            return ""

        # Socket is required for proper MCP operation (like HTTP REST)
        if not socket:
            logger.warning(f"No socket provided for command: {command}, results may be lost")

        try:
            # Call synchronously - no ThreadPool needed
            return self._sync_execute(command, headers, socket)
        except Exception as e:
            logger.error(f"Command execution failed: {e}")
            raise

    def _sync_execute(self, command: str, headers: Optional[Dict[str, str]] = None, socket=None) -> Any:
        """
        Execute command synchronously with socket streaming (like HTTP REST).

        Args:
            command: EdgeLake command
            headers: Optional headers
            socket: Socket for streaming results

        Returns:
            Command result (from result_set for non-query commands, empty for socket-streamed queries)
        """
        logger.debug(f"Starting sync execution of: {command}")

        try:
            # Create status and buffer objects
            status = self.process_status.ProcessStat()

            # Get buffer size (use default if not initialized yet)
            buff_size_str = self.params.get_param("io_buff_size")
            if not buff_size_str or buff_size_str == '':
                buff_size = 32768  # Default 32KB buffer
                logger.warning("io_buff_size not initialized, using default: 32768")
            else:
                buff_size = int(buff_size_str)
            io_buff = bytearray(buff_size)

            # Set REST caller flag for JSON formatting (not struct_print Python dict format)
            j_handle = status.get_job_handle()
            j_handle.set_rest_caller()

            # Set output socket - enables direct streaming like HTTP REST
            # Query results write to socket via OutputManager, bypassing result_set
            if socket:
                j_handle.set_output_socket(socket)
                logger.debug("Output socket set for streaming results")

            is_async_command = 'run client' in command.lower()
            logger.debug(f"Calling member_cmd.process_cmd for: {command} (async={is_async_command})")

            # Execute command via member_cmd (no stdout capture - socket gets output)
            ret_val = self.member_cmd.process_cmd(
                status,
                command=command,
                print_cmd=False,
                source_ip=None,
                source_port=None,
                io_buffer_in=io_buff
            )

            # For async commands (run client), wait for reply like HTTP REST does
            if is_async_command and ret_val == self.process_status.SUCCESS:
                logger.debug("Async command detected, waiting for reply...")
                subset = status.is_subset()
                timeout_sec = 20  # Default timeout
                ret_val = self.native_api.wait_for_reply(status, command, subset, timeout_sec)

            logger.debug(f"Command completed with return value: {ret_val}")

            if ret_val == self.process_status.SUCCESS:
                # For queries with socket: results already streamed to socket, return empty
                # For non-query commands: results in result_set
                result_set = status.get_job_handle().get_result_set()

                if result_set:
                    logger.debug(f"Result set available: {len(result_set)} bytes")
                    # Parse and return result_set (non-query commands)
                    return self._parse_result_set(result_set)
                else:
                    logger.debug("No result_set (results streamed to socket)")
                    # Query results streamed to socket, return empty string
                    return ""

            elif ret_val == 141:
                # Error code 141: EdgeLake not ready yet
                logger.warning(f"EdgeLake not ready for command '{command}' (code 141)")
                return ""
            else:
                # Command failed
                error_msg = status.get_saved_error() or f"Command failed with code {ret_val}"
                logger.error(f"Command execution failed: {error_msg}")
                raise Exception(error_msg)

        except Exception as e:
            logger.error(f"Error executing command '{command}': {e}", exc_info=True)
            raise

    def _parse_result_set(self, result_set: str) -> Any:
        """
        Parse result_set from job_handle (for non-query commands).

        Args:
            result_set: Result string from job_handle.get_result_set()

        Returns:
            Parsed result (dict/list) or original string
        """
        if not result_set:
            return ""

        # result_set should be a JSON string from member_cmd
        if isinstance(result_set, str):
            result_trimmed = result_set.strip()
            if result_trimmed:
                try:
                    result = json.loads(result_trimmed)
                    logger.debug(f"Parsed JSON result: {type(result)}")
                    return result
                except (json.JSONDecodeError, TypeError) as e:
                    logger.debug(f"Result set is not valid JSON: {e}, returning as string")
                    return result_trimmed
        else:
            # If result_set is already a dict/list, return it directly
            logger.debug(f"Result set is already an object: {type(result_set)}")
            return result_set

        return ""

    def close(self):
        """Shutdown the direct client"""
        logger.debug("Shutting down EdgeLake direct client")
        self._shutdown = True
