"""
EdgeLake Direct Client

Direct integration client for embedded MCP server - uses shared command_execution
layer for consistency with REST API. Handles status management and error logging.

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

    Uses shared command_execution layer for consistency with REST API al_exec().
    Manages status objects for proper error tracking and socket management.
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
            from edge_lake.cmd import member_cmd, native_api, command_execution
            from edge_lake.generic import process_status, params

            self.member_cmd = member_cmd
            self.native_api = native_api
            self.command_execution = command_execution
            self.process_status = process_status
            self.params = params

            logger.debug("EdgeLake direct client initialized")

        except ImportError as e:
            # Can't use status.add_error here - no status object yet
            err_msg = f"Failed to import EdgeLake modules: {e}"
            logger.error(err_msg)
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
        Execute command using SHARED command_execution layer (same as al_exec).

        Args:
            command: EdgeLake command
            headers: Optional headers with 'destination', 'subset', 'timeout'
            socket: Socket for streaming results

        Returns:
            Command result (from result_set for non-query commands, empty for socket-streamed queries)

        Note:
            Now uses shared command_execution.execute_command_simple() which provides:
            - Identical wait logic as http_server.al_exec()
            - Proper file command handling (no wait)
            - Correct native_api wrapper usage (exec_al_cmd vs exec_no_wait)
            - Proper job management (end_job, status reset)
        """
        logger.debug(f"Starting sync execution of: {command}")

        # Create status object
        status = self.process_status.ProcessStat()

        try:
            # Set REST caller flag for JSON formatting
            j_handle = status.get_job_handle()
            j_handle.set_rest_caller()

            # Set output socket for streaming results
            if socket:
                j_handle.set_output_socket(socket)
                logger.debug("Output socket set for streaming results")
            else:
                err_msg = f"No socket provided for command: {command}, results may be lost"
                status.add_error(err_msg)

            # Use shared execution layer (same logic as al_exec)
            logger.debug(f"Calling shared command_execution layer for: {command}")
            ret_val = self.command_execution.execute_command_simple(
                status, command, socket, headers
            )

            logger.debug(f"Command completed with return value: {ret_val}")

            # Handle results
            if ret_val == self.process_status.SUCCESS:
                result_set = status.get_job_handle().get_result_set()

                if result_set:
                    logger.debug(f"Result set available: {len(result_set)} bytes")
                    return self._parse_result_set(result_set)
                else:
                    logger.debug("No result_set (results streamed to socket)")
                    return ""

            elif ret_val == 141:
                # Error code 141: EdgeLake not ready yet
                err_msg = f"EdgeLake not ready for command '{command}' (code 141)"
                status.add_error(err_msg)
                return ""
            else:
                # Command failed - get error from status
                error_msg = status.get_saved_error() or f"Command failed with code {ret_val}"
                status.add_error(error_msg)
                raise Exception(error_msg)

        except Exception as e:
            err_msg = f"Error executing command '{command}': {e}"
            status.add_error(err_msg)
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
