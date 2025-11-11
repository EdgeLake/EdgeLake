"""
EdgeLake Command Builder

Builds EdgeLake commands from tool configurations and user arguments.

License: Mozilla Public License 2.0
"""

import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class CommandBuilder:
    """
    Builds EdgeLake command strings from tool configurations.
    """
    
    def __init__(self):
        """Initialize command builder"""
        pass
    
    def build_command(self, edgelake_command: Dict[str, Any], arguments: Dict[str, Any]) -> tuple[str, Optional[Dict[str, str]]]:
        """
        Build EdgeLake command from edgelake_command configuration and arguments.

        Args:
            edgelake_command: EdgeLake command config dict from tools.yaml
            arguments: User-provided arguments for the tool

        Returns:
            Tuple of (command_string, headers_dict)
        """
        cmd_type = edgelake_command.get('type')

        # Handle special internal commands
        if cmd_type == 'internal':
            return self._build_internal_command(edgelake_command, arguments)

        # Build command based on template
        template = edgelake_command.get('template', '')
        conditional_template = edgelake_command.get('conditional_template', {})
        
        # Choose appropriate template
        command_template = self._select_template(template, conditional_template, arguments)
        
        # Fill in template placeholders
        command = self._fill_template(command_template, arguments)

        # Get headers if specified
        headers = edgelake_command.get('headers')
        
        logger.debug(f"Built command: {command}")
        if headers:
            logger.debug(f"With headers: {headers}")
        
        return command, headers
    
    def _build_internal_command(self, edgelake_cmd: Dict[str, Any], arguments: Dict[str, Any]) -> tuple[str, None]:
        """
        Build internal command (handled by server, not EdgeLake).
        
        Args:
            edgelake_cmd: EdgeLake command config
            arguments: Arguments
        
        Returns:
            Tuple of (internal_method_name, None)
        """
        method = edgelake_cmd.get('method', '')
        return f"internal:{method}", None
    
    def _select_template(self, default_template: str, conditional_templates: Dict[str, str], 
                        arguments: Dict[str, Any]) -> str:
        """
        Select appropriate template based on provided arguments.
        
        Args:
            default_template: Default command template
            conditional_templates: Dict of conditional templates
            arguments: User arguments
        
        Returns:
            Selected template string
        """
        if not conditional_templates:
            return default_template
        
        # Check for full template (all conditions met)
        if 'full' in conditional_templates:
            required_keys = self._extract_template_keys(conditional_templates['full'])
            if all(key in arguments and arguments[key] for key in required_keys):
                return conditional_templates['full']
        
        # Check for partial templates
        for template_key, template in conditional_templates.items():
            if template_key == 'full':
                continue
            
            # Extract condition from key (e.g., 'with_where' -> 'where')
            condition_key = template_key.replace('with_', '')
            
            # Check if this condition is met
            if condition_key in arguments and arguments[condition_key]:
                # Check if all required keys for this template are present
                required_keys = self._extract_template_keys(template)
                if all(key in arguments or not self._is_required_key(key, template) 
                       for key in required_keys):
                    return template
        
        return default_template
    
    def _extract_template_keys(self, template: str) -> list:
        """
        Extract placeholder keys from template.
        
        Args:
            template: Template string with {placeholders}
        
        Returns:
            List of placeholder keys
        """
        import re
        return re.findall(r'\{(\w+)\}', template)
    
    def _is_required_key(self, key: str, template: str) -> bool:
        """Check if a key is required (not in optional part of template)"""
        # Simple heuristic: keys in brackets are required
        return f'{{{key}}}' in template
    
    def _fill_template(self, template: str, arguments: Dict[str, Any]) -> str:
        """
        Fill template placeholders with argument values.
        
        Args:
            template: Template string with {placeholders}
            arguments: Argument values
        
        Returns:
            Filled command string
        """
        command = template
        
        # Replace each placeholder
        for key, value in arguments.items():
            placeholder = f'{{{key}}}'
            if placeholder in command:
                # Convert value to string
                if isinstance(value, (list, dict)):
                    value_str = str(value)
                else:
                    value_str = str(value)
                
                command = command.replace(placeholder, value_str)
        
        return command
    
    def build_sql_query(self, arguments: Dict[str, Any]) -> str:
        """
        Build SQL query from structured arguments.
        
        This is a helper for the 'query' tool that constructs SQL from parts.
        
        Args:
            arguments: Query arguments (select, from, where, group_by, order_by, limit)
        
        Returns:
            SQL query string
        """
        # Get query components
        select_cols = arguments.get('select', ['*'])
        table = arguments.get('table')
        where_clause = arguments.get('where')
        group_by = arguments.get('group_by', [])
        order_by = arguments.get('order_by', [])
        limit = arguments.get('limit')
        
        # Build SELECT clause
        if isinstance(select_cols, list):
            select_part = ', '.join(select_cols)
        else:
            select_part = select_cols
        
        # Start query
        query = f"SELECT {select_part} FROM {table}"
        
        # Add WHERE clause
        if where_clause:
            query += f" WHERE {where_clause}"
        
        # Add GROUP BY clause
        if group_by:
            if isinstance(group_by, list):
                group_by_str = ', '.join(group_by)
            else:
                group_by_str = group_by
            query += f" GROUP BY {group_by_str}"
        
        # Add ORDER BY clause
        if order_by:
            order_parts = []
            if isinstance(order_by, list):
                for order_item in order_by:
                    if isinstance(order_item, dict):
                        col = order_item.get('column')
                        direction = order_item.get('direction', 'ASC')
                        order_parts.append(f"{col} {direction}")
                    else:
                        order_parts.append(str(order_item))
                order_by_str = ', '.join(order_parts)
            else:
                order_by_str = order_by
            query += f" ORDER BY {order_by_str}"
        
        # Add LIMIT clause
        if limit:
            query += f" LIMIT {limit}"
        
        logger.debug(f"Built SQL query: {query}")
        return query
