"""
Protocol Callbacks for Transport-Agnostic Command Execution

Defines callback interfaces that allow al_exec and related execution functions
to work with different transport protocols (HTTP REST, MCP/SSE, stdio, etc.)
without being tightly coupled to any specific transport.

This enables:
- Sharing execution logic across HTTP REST and MCP
- Easy addition of new transports (WebSocket, gRPC, etc.)
- Clean separation of execution vs. transport concerns
- Testability (mock callbacks)

License: Mozilla Public License 2.0
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Optional, Dict
from io import BytesIO

logger = logging.getLogger(__name__)


class ProtocolCallbacks(ABC):
    """
    Abstract base class for protocol-specific callbacks.

    Each transport (HTTP REST, MCP/SSE, stdio, etc.) implements this interface
    to provide transport-specific behavior for:
    - Error handling (send_error)
    - Success responses (send_success)
    - Output streaming (get_output_socket)
    - Header management (send_headers - optional)
    """

    @abstractmethod
    def send_error(self, status, error_code: int, error_message: str,
                   error_details: Optional[Dict[str, Any]] = None) -> None:
        """
        Send error response via protocol-specific mechanism.

        Args:
            status: ProcessStat object
            error_code: Error code (EdgeLake error code)
            error_message: Human-readable error message
            error_details: Optional additional error context
                          (operator_msg, local_msg, command, etc.)
        """
        pass

    @abstractmethod
    def send_success(self, status, result_data: Any,
                     content_type: Optional[str] = None,
                     metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Send success response via protocol-specific mechanism.

        Args:
            status: ProcessStat object
            result_data: Result data (string, dict, list, etc.)
            content_type: Content type hint ('text/json', 'text', etc.)
            metadata: Optional additional context
                     (with_wait, is_select, nodes_count, etc.)
        """
        pass

    @abstractmethod
    def get_output_socket(self, status):
        """
        Get output socket for streaming results.

        Returns:
            File-like object (socket, BytesIO, etc.)

        Note:
            For queries, results may be written directly to this socket.
            The protocol callback should handle reading and formatting.
        """
        pass

    @abstractmethod
    def send_headers(self, status, content_type: str, is_chunked: bool = False) -> None:
        """
        Send protocol headers (if applicable).

        Args:
            status: ProcessStat object
            content_type: Content type
            is_chunked: Whether response is chunked/streaming

        Note:
            Some protocols (HTTP) need headers sent before body.
            Others (MCP/SSE) don't have separate headers.
            Default implementation does nothing.
        """
        pass

    @abstractmethod
    def get_protocol_name(self) -> str:
        """
        Get protocol name for logging/debugging.

        Returns:
            Protocol name (e.g., "HTTP", "MCP", "stdio")
        """
        pass

    @abstractmethod
    def supports_streaming(self) -> bool:
        """
        Does this protocol support streaming responses?

        Returns:
            True if protocol can send headers first, then stream body.
            False if protocol requires complete response before sending.
        """
        pass


class HTTPProtocolCallbacks(ProtocolCallbacks):
    """
    HTTP REST protocol callbacks.

    Wraps HTTP handler methods to provide ProtocolCallbacks interface.
    """

    def __init__(self, http_handler, http_method: str):
        """
        Initialize HTTP callbacks.

        Args:
            http_handler: ChunkedHTTPRequestHandler instance
            http_method: HTTP method (GET, POST, PUT)
        """
        self.http_handler = http_handler
        self.http_method = http_method
        self.headers_sent = False

    def send_error(self, status, error_code: int, error_message: str,
                   error_details: Optional[Dict[str, Any]] = None) -> None:
        """Send HTTP error response using error_failed_process"""

        # Import here to avoid circular dependency
        from edge_lake.generic import process_status as ps

        # Determine with_wait and into_output from error_details
        with_wait = error_details.get('with_wait', False) if error_details else False
        into_output = error_details.get('into_output') if error_details else None
        command = error_details.get('command', '') if error_details else ''
        operator_msg = error_details.get('operator_msg') if error_details else None

        # Call HTTP-specific error handler
        self.http_handler.error_failed_process(
            status, with_wait, error_code, self.http_method,
            into_output, command, operator_msg
        )

    def send_success(self, status, result_data: Any,
                     content_type: Optional[str] = None,
                     metadata: Optional[Dict[str, Any]] = None) -> None:
        """Send HTTP success response"""

        # Import here to avoid circular dependency
        from edge_lake.tcpip.http_server import REST_OK, get_result_set_type

        # Determine content type
        if not content_type:
            content_type = get_result_set_type(result_data)

        # Check if headers already sent (streaming case)
        if self.headers_sent:
            # Headers already sent, just write body
            from edge_lake.generic import utils_io
            utils_io.write_to_stream(status, self.http_handler.wfile, result_data, True, True)
        else:
            # Send headers + body together
            self.http_handler.write_headers_and_msg(
                status, REST_OK, content_type, result_data
            )

    def get_output_socket(self, status):
        """Return HTTP handler's wfile socket"""
        return self.http_handler.wfile

    def send_headers(self, status, content_type: str, is_chunked: bool = False) -> None:
        """Send HTTP headers before streaming"""

        # Import here to avoid circular dependency
        from edge_lake.tcpip.http_server import REST_OK

        if not self.headers_sent:
            self.http_handler.send_reply_headers(
                status, REST_OK, "", True, content_type, 0, is_chunked, None
            )
            self.headers_sent = True

    def get_protocol_name(self) -> str:
        return "HTTP"

    def supports_streaming(self) -> bool:
        return True


class MCPProtocolCallbacks(ProtocolCallbacks):
    """
    MCP/SSE protocol callbacks.

    Queues JSON-RPC messages for SSE delivery.
    """

    def __init__(self, sse_connection, json_rpc_id: int):
        """
        Initialize MCP callbacks.

        Args:
            sse_connection: SSEConnection instance
            json_rpc_id: JSON-RPC message ID for response
        """
        self.sse_connection = sse_connection
        self.json_rpc_id = json_rpc_id
        self.output_buffer = BytesIO()  # Buffer for capturing output

    def send_error(self, status, error_code: int, error_message: str,
                   error_details: Optional[Dict[str, Any]] = None) -> None:
        """Send JSON-RPC error via SSE"""

        # Map EdgeLake error code to JSON-RPC error code
        # -32603 = Internal error (generic)
        json_rpc_code = -32603

        # Build error response
        error_response = {
            "jsonrpc": "2.0",
            "id": self.json_rpc_id,
            "error": {
                "code": json_rpc_code,
                "message": error_message,
                "data": {
                    "edgelake_code": error_code,
                    **(error_details or {})
                }
            }
        }

        # Queue for SSE delivery
        self.sse_connection.queue_message('error', error_response)

        logger.debug(f"MCP error queued: {error_message} (code={error_code})")

    def send_success(self, status, result_data: Any,
                     content_type: Optional[str] = None,
                     metadata: Optional[Dict[str, Any]] = None) -> None:
        """Send JSON-RPC result via SSE"""

        # Format result data as MCP content
        if isinstance(result_data, (dict, list)):
            # Already structured - convert to JSON string
            import json
            text_content = json.dumps(result_data)
        else:
            # Plain text or string
            text_content = str(result_data) if result_data else ""

        # Build success response
        success_response = {
            "jsonrpc": "2.0",
            "id": self.json_rpc_id,
            "result": {
                "content": [
                    {
                        "type": "text",
                        "text": text_content
                    }
                ]
            }
        }

        # Queue for SSE delivery
        self.sse_connection.queue_message('message', success_response)

        logger.debug(f"MCP success queued: {len(text_content)} bytes")

    def get_output_socket(self, status):
        """Return BytesIO buffer for capturing output"""
        return self.output_buffer

    def send_headers(self, status, content_type: str, is_chunked: bool = False) -> None:
        """MCP doesn't use separate headers - no-op"""
        pass

    def get_protocol_name(self) -> str:
        return "MCP"

    def supports_streaming(self) -> bool:
        """MCP requires complete JSON-RPC messages"""
        return False

    def get_buffered_output(self) -> bytes:
        """
        Get buffered output from BytesIO.

        Returns:
            Buffered data
        """
        self.output_buffer.seek(0)
        return self.output_buffer.read()

    def has_buffered_output(self) -> bool:
        """Check if buffer has data"""
        return self.output_buffer.tell() > 0


# Factory function for convenience
def create_http_callbacks(http_handler, http_method: str) -> HTTPProtocolCallbacks:
    """
    Create HTTP protocol callbacks.

    Args:
        http_handler: ChunkedHTTPRequestHandler instance
        http_method: HTTP method (GET, POST, PUT)

    Returns:
        HTTPProtocolCallbacks instance
    """
    return HTTPProtocolCallbacks(http_handler, http_method)


def create_mcp_callbacks(sse_connection, json_rpc_id: int) -> MCPProtocolCallbacks:
    """
    Create MCP protocol callbacks.

    Args:
        sse_connection: SSEConnection instance
        json_rpc_id: JSON-RPC message ID

    Returns:
        MCPProtocolCallbacks instance
    """
    return MCPProtocolCallbacks(sse_connection, json_rpc_id)
