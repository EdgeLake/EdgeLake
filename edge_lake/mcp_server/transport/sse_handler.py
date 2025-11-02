"""
SSE Transport for MCP Server

Implements Server-Sent Events (SSE) transport integrated with EdgeLake's
http_server.py infrastructure.

The SSE protocol provides a simple way to push server-sent updates to clients
over HTTP. This implementation bridges between EdgeLake's ChunkedHTTPRequestHandler
and the MCP protocol server.

Protocol Flow:
1. Client establishes SSE connection: GET /mcp/sse
2. Server responds with text/event-stream content type
3. Client posts MCP messages: POST /mcp/messages/{session_id}
4. Server processes messages and sends responses via SSE events
5. Connection maintained with periodic keepalive pings

SSE Event Format:
    data: {json_rpc_message}
    event: {event_type}
    id: {message_id}

    (blank line)

License: Mozilla Public License 2.0
"""

import json
import logging
import time
import threading
import uuid
from typing import Optional, Dict, Any, List
from queue import Queue, Empty

logger = logging.getLogger(__name__)


class SSEConnection:
    """
    Represents an active SSE connection to a client.

    Manages the bidirectional communication between client and server:
    - Incoming: POST requests to /mcp/messages/{session_id}
    - Outgoing: SSE events via established GET connection
    """

    def __init__(self, session_id: str, handler):
        """
        Initialize SSE connection.

        Args:
            session_id: Unique session identifier
            handler: ChunkedHTTPRequestHandler instance
        """
        self.session_id = session_id
        self.handler = handler
        self.message_queue = Queue()
        self.last_activity = time.time()
        self.connected = True
        self.message_counter = 0

        logger.debug(f"SSE connection established: {session_id}")

    def queue_message(self, event_type: str, data: Dict[str, Any]):
        """
        Queue a message to be sent to the client.

        Args:
            event_type: Type of event (message, error, ping)
            data: Event data (will be JSON-encoded)
        """
        if not self.connected:
            logger.warning(f"Attempted to queue message to disconnected session: {self.session_id}")
            return

        self.message_counter += 1
        message = {
            'id': self.message_counter,
            'event': event_type,
            'data': data,
            'timestamp': time.time()
        }

        self.message_queue.put(message)
        self.last_activity = time.time()

        logger.debug(f"Message queued for {self.session_id}: {event_type}")

    def send_event(self, event_type: str, data: Dict[str, Any], event_id: Optional[int] = None) -> bool:
        """
        Send SSE event immediately to client.

        Args:
            event_type: Event type (message, error, ping)
            data: Event data
            event_id: Optional event ID (auto-generated if not provided)

        Returns:
            True if sent successfully, False otherwise
        """
        if not self.connected:
            return False

        try:
            # Increment counter if no ID provided
            if event_id is None:
                self.message_counter += 1
                event_id = self.message_counter

            # Format SSE event
            # Format: data: {json}\nevent: {type}\nid: {id}\n\n
            event_data = json.dumps(data)

            # Write SSE event to stream
            self.handler.wfile.write(f"data: {event_data}\n".encode('utf-8'))
            self.handler.wfile.write(f"event: {event_type}\n".encode('utf-8'))
            self.handler.wfile.write(f"id: {event_id}\n\n".encode('utf-8'))
            self.handler.wfile.flush()

            self.last_activity = time.time()

            logger.debug(f"SSE event sent to {self.session_id}: {event_type} (id={event_id})")
            return True

        except Exception as e:
            logger.error(f"Failed to send SSE event to {self.session_id}: {e}")
            self.connected = False
            return False

    def send_keepalive(self) -> bool:
        """
        Send keepalive ping to maintain connection.

        Returns:
            True if sent successfully, False otherwise
        """
        try:
            # Send SSE comment (keepalive)
            self.handler.wfile.write(b":keepalive\n\n")
            self.handler.wfile.flush()

            self.last_activity = time.time()
            return True

        except Exception as e:
            logger.error(f"Failed to send keepalive to {self.session_id}: {e}")
            self.connected = False
            return False

    def close(self):
        """Close the connection."""
        self.connected = False
        logger.debug(f"SSE connection closed: {self.session_id}")


class SSETransport:
    """
    SSE transport implementation for MCP protocol.

    Integrates with EdgeLake's http_server.py by handling MCP-specific
    endpoints (/mcp/sse and /mcp/messages/*) and routing messages to the
    MCP server.

    Architecture:
    - GET /mcp/sse: Establishes SSE connection (event stream)
    - POST /mcp/messages/{session_id}: Receives MCP JSON-RPC requests
    - Maintains active connections with keepalive pings
    - Routes messages to MCP server for processing
    """

    def __init__(self, mcp_server):
        """
        Initialize SSE transport.

        Args:
            mcp_server: Reference to MCPServer instance
        """
        self.mcp_server = mcp_server
        self.connections: Dict[str, SSEConnection] = {}
        self.connection_lock = threading.Lock()

        # Configuration
        self.keepalive_interval = 30  # seconds
        self.connection_timeout = 300  # 5 minutes

        # Start keepalive thread
        self.running = True
        self.keepalive_thread = threading.Thread(
            target=self._keepalive_loop,
            daemon=True,
            name="MCP-SSE-Keepalive"
        )
        self.keepalive_thread.start()

        logger.info("SSE transport initialized")

    def handle_sse_endpoint(self, handler) -> bool:
        """
        Handle GET /mcp/sse - establish SSE connection.

        This creates a new SSE connection and keeps it open for streaming
        events to the client. The connection is identified by a unique
        session_id returned to the client.

        Args:
            handler: ChunkedHTTPRequestHandler instance

        Returns:
            True if handled successfully
        """
        try:
            # Generate unique session ID
            session_id = str(uuid.uuid4())

            # Send SSE headers
            handler.send_response(200, "OK")
            handler.send_header('Content-Type', 'text/event-stream')
            handler.send_header('Cache-Control', 'no-cache')
            handler.send_header('Connection', 'keep-alive')
            handler.send_header('Access-Control-Allow-Origin', '*')
            handler.send_header('X-Accel-Buffering', 'no')  # Disable nginx buffering
            handler.end_headers()

            # Create connection object
            connection = SSEConnection(session_id, handler)

            # Store connection
            with self.connection_lock:
                self.connections[session_id] = connection

            # Send endpoint event (MCP SSE protocol spec)
            # Client needs to know where to POST messages
            # The MCP library sends the endpoint as a PLAIN STRING (not JSON-encoded)
            # Format: data: /mcp/messages/{session_id}\nevent: endpoint\nid: 0\n\n
            endpoint_path = f'/mcp/messages/{session_id}'
            try:
                handler.wfile.write(f"data: {endpoint_path}\n".encode('utf-8'))
                handler.wfile.write(b"event: endpoint\n")
                handler.wfile.write(b"id: 0\n\n")
                handler.wfile.flush()
                connection.last_activity = time.time()
                logger.debug(f"SSE endpoint event sent to {session_id}: {endpoint_path}")
            except Exception as e:
                logger.error(f"Failed to send endpoint event to {session_id}: {e}")
                connection.close()

            logger.info(f"SSE connection established: {session_id} from {handler.client_address}")

            # Keep connection alive
            # The connection will be maintained by the keepalive thread
            # and closed when the client disconnects or timeout occurs

            # Block until connection closes
            while connection.connected:
                try:
                    # Check if there are queued messages
                    try:
                        message = connection.message_queue.get(timeout=1.0)

                        # Send queued message
                        connection.send_event(
                            message['event'],
                            message['data'],
                            message['id']
                        )

                    except Empty:
                        # No messages, continue loop
                        pass

                except Exception as e:
                    logger.error(f"Error in SSE loop for {session_id}: {e}")
                    break

            # Cleanup
            with self.connection_lock:
                if session_id in self.connections:
                    del self.connections[session_id]

            logger.info(f"SSE connection closed: {session_id}")
            return True

        except Exception as e:
            logger.error(f"Error in handle_sse_endpoint: {e}", exc_info=True)
            return False

    def handle_messages_endpoint(self, handler) -> bool:
        """
        Handle POST /mcp/messages/{session_id} - receive MCP messages.

        Parses JSON-RPC requests from the client, routes them to the MCP
        server for processing, and sends responses via the SSE connection.

        Args:
            handler: ChunkedHTTPRequestHandler instance

        Returns:
            True if handled successfully
        """
        try:
            # Extract session ID from path
            # Path format: /mcp/messages/{session_id}
            path_parts = handler.path.split('/')
            if len(path_parts) < 4:
                logger.warning(f"Invalid MCP messages path: {handler.path}")
                handler.send_error(400, "Invalid path - missing session_id")
                return False

            session_id = path_parts[3]

            # Check if connection exists
            with self.connection_lock:
                connection = self.connections.get(session_id)

            if not connection:
                logger.warning(f"Message received for unknown session: {session_id}")
                handler.send_error(404, "Session not found")
                return False

            # Read message body
            content_length = int(handler.headers.get('Content-Length', 0))
            if content_length == 0:
                logger.warning(f"Empty message body from {session_id}")
                handler.send_error(400, "Empty message body")
                return False

            body = handler.rfile.read(content_length)

            # Parse JSON-RPC message
            try:
                message = json.loads(body.decode('utf-8'))
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in message from {session_id}: {e}")
                handler.send_error(400, f"Invalid JSON: {e}")
                return False

            logger.debug(f"MCP message received from {session_id}: {message.get('method')}")

            # Send immediate HTTP response (202 Accepted)
            # Actual response will come via SSE
            handler.send_response(202, "Accepted")
            handler.send_header('Content-Type', 'application/json')
            handler.end_headers()
            handler.wfile.write(json.dumps({
                'status': 'accepted',
                'message': 'Request will be processed and response sent via SSE'
            }).encode('utf-8'))

            # Process message synchronously (no new thread)
            self._process_message_sync(session_id, message)

            return True

        except Exception as e:
            logger.error(f"Error in handle_messages_endpoint: {e}", exc_info=True)
            try:
                handler.send_error(500, f"Internal server error: {e}")
            except:
                pass
            return False

    def _process_message_sync(self, session_id: str, message: Dict[str, Any]):
        """
        Process MCP message synchronously (no new thread).

        Routes the message to the MCP server for processing and queues
        the response for the SSE connection manager to send.

        Note: Renamed from _process_message_async to _process_message_sync.
        No thread spawning needed - this runs in the HTTP worker thread
        and returns quickly (MCP processing is synchronous, response is queued).

        Args:
            session_id: Session identifier
            message: JSON-RPC message
        """
        try:
            # Process message via MCP server (synchronous)
            response = self.mcp_server.process_message(message)

            # Queue response for SSE delivery
            with self.connection_lock:
                connection = self.connections.get(session_id)

            if connection:
                connection.queue_message('message', response)
            else:
                logger.warning(f"Cannot send response - session closed: {session_id}")

        except Exception as e:
            logger.error(f"Error processing message for {session_id}: {e}", exc_info=True)

            # Queue error response for SSE delivery
            with self.connection_lock:
                connection = self.connections.get(session_id)

            if connection:
                error_response = {
                    'jsonrpc': '2.0',
                    'id': message.get('id'),
                    'error': {
                        'code': -32603,
                        'message': f'Internal error: {str(e)}'
                    }
                }
                connection.queue_message('error', error_response)

    def _keepalive_loop(self):
        """
        Background thread that sends keepalive pings and cleans up stale connections.
        """
        logger.debug("Keepalive loop started")

        while self.running:
            try:
                time.sleep(self.keepalive_interval)

                current_time = time.time()

                with self.connection_lock:
                    # Copy list to avoid modification during iteration
                    sessions = list(self.connections.items())

                for session_id, connection in sessions:
                    # Check if connection is stale
                    if current_time - connection.last_activity > self.connection_timeout:
                        logger.info(f"Connection timeout: {session_id}")
                        connection.close()

                        with self.connection_lock:
                            if session_id in self.connections:
                                del self.connections[session_id]
                        continue

                    # Send keepalive
                    if connection.connected:
                        if not connection.send_keepalive():
                            logger.debug(f"Keepalive failed for {session_id}")

                            with self.connection_lock:
                                if session_id in self.connections:
                                    del self.connections[session_id]

            except Exception as e:
                logger.error(f"Error in keepalive loop: {e}", exc_info=True)

        logger.debug("Keepalive loop stopped")

    def send_event(self, session_id: str, event_type: str, data: Dict[str, Any]) -> bool:
        """
        Send SSE event to specific session.

        Args:
            session_id: Target session ID
            event_type: Event type
            data: Event data

        Returns:
            True if sent successfully
        """
        with self.connection_lock:
            connection = self.connections.get(session_id)

        if connection:
            return connection.send_event(event_type, data)
        else:
            logger.warning(f"Cannot send event - session not found: {session_id}")
            return False

    def get_active_connections(self) -> List[str]:
        """
        Get list of active session IDs.

        Returns:
            List of session IDs
        """
        with self.connection_lock:
            return list(self.connections.keys())

    def close_connection(self, session_id: str):
        """
        Close specific connection.

        Args:
            session_id: Session to close
        """
        with self.connection_lock:
            connection = self.connections.get(session_id)
            if connection:
                connection.close()
                del self.connections[session_id]
                logger.info(f"Connection closed: {session_id}")

    def shutdown(self):
        """
        Shutdown transport and close all connections.
        """
        logger.info("Shutting down SSE transport")

        # Stop keepalive loop
        self.running = False

        # Close all connections
        with self.connection_lock:
            for session_id, connection in list(self.connections.items()):
                connection.close()
            self.connections.clear()

        # Wait for keepalive thread
        if self.keepalive_thread.is_alive():
            self.keepalive_thread.join(timeout=5.0)

        logger.info("SSE transport shutdown complete")


# Global instance (will be initialized by MCP server)
_sse_transport_instance: Optional[SSETransport] = None


def initialize(mcp_server) -> SSETransport:
    """
    Initialize global SSE transport instance.

    Args:
        mcp_server: MCP server instance

    Returns:
        SSETransport instance
    """
    global _sse_transport_instance

    if _sse_transport_instance is not None:
        logger.warning("SSE transport already initialized")
        return _sse_transport_instance

    _sse_transport_instance = SSETransport(mcp_server)
    return _sse_transport_instance


def get_instance() -> Optional[SSETransport]:
    """
    Get global SSE transport instance.

    Returns:
        SSETransport instance or None if not initialized
    """
    return _sse_transport_instance


def handle_sse_endpoint(handler) -> bool:
    """
    Handle SSE endpoint (called from http_server.py).

    Args:
        handler: ChunkedHTTPRequestHandler

    Returns:
        True if handled
    """
    if _sse_transport_instance is None:
        logger.error("SSE transport not initialized")
        handler.send_error(503, "MCP server not available")
        return False

    return _sse_transport_instance.handle_sse_endpoint(handler)


def handle_messages_endpoint(handler) -> bool:
    """
    Handle messages endpoint (called from http_server.py).

    Args:
        handler: ChunkedHTTPRequestHandler

    Returns:
        True if handled
    """
    if _sse_transport_instance is None:
        logger.error("SSE transport not initialized")
        handler.send_error(503, "MCP server not available")
        return False

    return _sse_transport_instance.handle_messages_endpoint(handler)


def shutdown():
    """Shutdown global SSE transport instance."""
    global _sse_transport_instance

    if _sse_transport_instance is not None:
        _sse_transport_instance.shutdown()
        _sse_transport_instance = None
