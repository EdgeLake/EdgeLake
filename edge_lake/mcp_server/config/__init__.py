"""
Configuration management for EdgeLake MCP Server

Loads and manages configuration from YAML files and environment variables.

License: Mozilla Public License 2.0
"""

import os
import yaml
import logging
from typing import Dict, List, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class ToolConfig:
    """Configuration for an MCP tool"""
    
    def __init__(self, data: Dict[str, Any]):
        self.name = data['name']
        self.description = data['description']
        self.edgelake_command = data['edgelake_command']
        self.input_schema = data['input_schema']
    
    def __repr__(self):
        return f"ToolConfig(name={self.name})"


class Config:
    """Main configuration manager"""
    
    def __init__(self, config_dir: Optional[Path] = None):
        """
        Initialize configuration.
        
        Args:
            config_dir: Path to configuration directory. If None, uses default location.
        """
        if config_dir is None:
            # Default to config directory relative to this file
            config_dir = Path(__file__).parent
        
        self.config_dir = Path(config_dir)
        self.tools: List[ToolConfig] = []
        self.testing_mode: bool = False  # Testing mode flag

        # Load configurations
        self._load_tools_config()

    def _load_tools_config(self):
        """Load tools configuration from YAML"""
        tools_file = self.config_dir / "tools.yaml"
        
        if not tools_file.exists():
            logger.warning(f"Tools configuration not found: {tools_file}")
            return
        
        try:
            with open(tools_file, 'r') as f:
                config = yaml.safe_load(f)

            # Load testing mode flag (defaults to False if not present)
            self.testing_mode = config.get('testing', False)

            # Load tools
            for tool_data in config.get('tools', []):
                self.tools.append(ToolConfig(tool_data))

            logger.debug(f"Loaded {len(self.tools)} tool configurations")
            if self.testing_mode:
                logger.info("Testing mode ENABLED - detailed tool execution logs will be shown")
            
        except Exception as e:
            logger.error(f"Failed to load tools configuration: {e}")
            raise

    def get_tool_by_name(self, name: str) -> Optional[ToolConfig]:
        """Get a tool configuration by name"""
        for tool in self.tools:
            if tool.name == name:
                return tool
        return None
    
    def get_all_tools(self) -> List[ToolConfig]:
        """Get all tool configurations"""
        return self.tools

    def get_request_timeout(self) -> int:
        """Get request timeout from environment (default: 30)"""
        timeout_env = os.getenv("EDGELAKE_TIMEOUT")
        if timeout_env:
            return int(timeout_env)
        return 30

    def get_max_workers(self) -> int:
        """Get max workers from environment (default: 10)"""
        workers_env = os.getenv("EDGELAKE_MAX_WORKERS")
        if workers_env:
            return int(workers_env)
        return 10

    def __repr__(self):
        return f"Config(tools={len(self.tools)})"
