"""
Dynamic Tool Generator

Generates MCP Tool objects from configuration with optional filtering.

License: Mozilla Public License 2.0
"""

import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class ToolGenerator:
    """
    Generates MCP Tool objects from tool configurations with optional filtering.
    """

    def __init__(self, tool_configs: List[Any], enabled_tools: Optional[List[str]] = None):
        """
        Initialize tool generator.

        Args:
            tool_configs: List of ToolConfig objects from configuration
            enabled_tools: Optional list of tool names to enable (None = all tools)
        """
        self.tool_configs = tool_configs
        self.enabled_tools = enabled_tools

        if enabled_tools:
            logger.debug(f"Tool filtering enabled: {len(enabled_tools)} tools allowed")
        else:
            logger.debug("Tool filtering disabled: all tools enabled")

    def generate_tools(self) -> List[Dict[str, Any]]:
        """
        Generate MCP Tool objects from configurations.

        Returns:
            List of Tool dictionaries for MCP protocol (filtered if enabled_tools set)
        """
        tools = []

        for tool_config in self.tool_configs:
            # Apply filtering if enabled_tools is set
            if self.enabled_tools is not None and tool_config.name not in self.enabled_tools:
                logger.debug(f"Skipping tool '{tool_config.name}' (not in enabled list)")
                continue

            try:
                tool = self._generate_tool(tool_config)
                tools.append(tool)
                logger.debug(f"Generated tool: {tool_config.name}")
            except Exception as e:
                logger.error(f"Failed to generate tool '{tool_config.name}': {e}")

        logger.debug(f"Generated {len(tools)} tools")
        return tools
    
    def _generate_tool(self, tool_config: Any) -> Dict[str, Any]:
        """
        Generate a single MCP Tool object.
        
        Args:
            tool_config: ToolConfig object
        
        Returns:
            Tool dictionary
        """
        return {
            "name": tool_config.name,
            "description": tool_config.description,
            "inputSchema": tool_config.input_schema
        }
