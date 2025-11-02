"""
SQL Query Builder

Constructs SQL queries from structured parameters for EdgeLake.

License: Mozilla Public License 2.0
"""

import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class QueryBuilder:
    """
    SQL query builder for EdgeLake queries.
    """
    
    def build_query(self, arguments: Dict[str, Any]) -> str:
        """
        Build SQL query from structured arguments.
        
        Args:
            arguments: Query parameters including:
                - database: Database name (required)
                - table: Table name (required)
                - select: List of columns to select (default: ['*'])
                - where: WHERE clause conditions
                - group_by: List of columns to group by
                - order_by: List of order specifications
                - limit: Row limit
                - include_tables: Additional tables for JOIN
                - extend_fields: Additional fields to extend
        
        Returns:
            SQL query string
        """
        table = arguments.get('table')
        if not table:
            raise ValueError("Table name is required")
        
        # Build SELECT clause
        select_clause = self._build_select_clause(arguments)
        
        # Build FROM clause
        from_clause = self._build_from_clause(arguments)
        
        # Build WHERE clause
        where_clause = self._build_where_clause(arguments)
        
        # Build GROUP BY clause
        group_by_clause = self._build_group_by_clause(arguments)
        
        # Build ORDER BY clause
        order_by_clause = self._build_order_by_clause(arguments)
        
        # Build LIMIT clause
        limit_clause = self._build_limit_clause(arguments)
        
        # Assemble query
        query_parts = [select_clause, from_clause]
        
        if where_clause:
            query_parts.append(where_clause)
        
        if group_by_clause:
            query_parts.append(group_by_clause)
        
        if order_by_clause:
            query_parts.append(order_by_clause)
        
        if limit_clause:
            query_parts.append(limit_clause)
        
        query = ' '.join(query_parts)
        
        logger.debug(f"Built SQL query: {query}")
        return query
    
    def _build_select_clause(self, arguments: Dict[str, Any]) -> str:
        """Build SELECT clause"""
        select_cols = arguments.get('select', ['*'])
        extend_fields = arguments.get('extend_fields', [])
        
        # Combine regular columns and extended fields
        all_cols = []
        
        if isinstance(select_cols, list):
            all_cols.extend(select_cols)
        else:
            all_cols.append(select_cols)
        
        if extend_fields:
            if isinstance(extend_fields, list):
                all_cols.extend(extend_fields)
            else:
                all_cols.append(extend_fields)
        
        return f"SELECT {', '.join(all_cols)}"
    
    def _build_from_clause(self, arguments: Dict[str, Any]) -> str:
        """Build FROM clause with optional JOINs"""
        table = arguments.get('table')
        include_tables = arguments.get('include_tables', [])
        
        from_part = f"FROM {table}"
        
        # Add JOIN clauses for included tables
        if include_tables:
            if isinstance(include_tables, list):
                for include_table in include_tables:
                    # Support format: "db_name.table_name" or just "table_name"
                    from_part += f", {include_table}"
            else:
                from_part += f", {include_tables}"
        
        return from_part
    
    def _build_where_clause(self, arguments: Dict[str, Any]) -> Optional[str]:
        """Build WHERE clause"""
        where_conditions = arguments.get('where')
        
        if not where_conditions:
            return None
        
        return f"WHERE {where_conditions}"
    
    def _build_group_by_clause(self, arguments: Dict[str, Any]) -> Optional[str]:
        """Build GROUP BY clause"""
        group_by = arguments.get('group_by', [])
        
        if not group_by:
            return None
        
        if isinstance(group_by, list):
            group_by_str = ', '.join(group_by)
        else:
            group_by_str = group_by
        
        return f"GROUP BY {group_by_str}"
    
    def _build_order_by_clause(self, arguments: Dict[str, Any]) -> Optional[str]:
        """Build ORDER BY clause"""
        order_by = arguments.get('order_by', [])
        
        if not order_by:
            return None
        
        order_parts = []
        
        if isinstance(order_by, list):
            for order_item in order_by:
                if isinstance(order_item, dict):
                    # Format: {"column": "col_name", "direction": "ASC"}
                    col = order_item.get('column')
                    direction = order_item.get('direction', 'ASC').upper()
                    if col:
                        order_parts.append(f"{col} {direction}")
                elif isinstance(order_item, str):
                    # Simple string format
                    order_parts.append(order_item)
        elif isinstance(order_by, str):
            order_parts.append(order_by)
        
        if not order_parts:
            return None
        
        return f"ORDER BY {', '.join(order_parts)}"
    
    def _build_limit_clause(self, arguments: Dict[str, Any]) -> Optional[str]:
        """Build LIMIT clause"""
        limit = arguments.get('limit')
        
        if limit is None:
            return None
        
        try:
            limit_val = int(limit)
            if limit_val > 0:
                return f"LIMIT {limit_val}"
        except (ValueError, TypeError):
            logger.warning(f"Invalid limit value: {limit}")
        
        return None
    
    def build_query_command(self, database: str, query: str, output_format: str = "json") -> str:
        """
        Build complete EdgeLake SQL command.
        
        Args:
            database: Database name
            query: SQL query
            output_format: Output format (json or table)
        
        Returns:
            EdgeLake command string
        """
        return f'sql {database} format = {output_format} "{query}"'
