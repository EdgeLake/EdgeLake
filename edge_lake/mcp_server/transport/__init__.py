"""
MCP Transport Layer

This module provides transport implementations for the MCP (Model Context Protocol)
server, integrating with EdgeLake's core HTTP infrastructure.

License: Mozilla Public License 2.0
"""

from .sse_handler import SSETransport

__all__ = ['SSETransport']
