"""
Shared command execution logic for REST API and MCP server.

This module extracts common execution patterns from http_server.al_exec()
to provide a unified command execution layer that both REST API and MCP
server can use. This ensures consistent behavior, proper wait handling,
and shared maintenance of core execution logic.

License: Mozilla Public License 2.0
"""

from edge_lake.cmd import member_cmd, native_api
from edge_lake.generic import process_status, params


def build_run_client_wrapper(destination=None, subset=None, timeout=None):
    """
    Build 'run client ()' wrapper for network commands.

    This function implements the same logic as http_server.get_run_client()
    to construct the run client wrapper with proper parameters.

    Args:
        destination: Query destination
            - None or 'local': Local query only (no run client)
            - 'network': Broadcast to all network nodes
            - 'ip:port': Specific node(s)
        subset: Allow partial results (bool)
        timeout: Timeout in seconds (int)

    Returns:
        tuple: (run_client_str, has_run_client) where:
            - run_client_str: 'run client (...)' or empty string
            - has_run_client: True if network query, False if local

    Examples:
        >>> build_run_client_wrapper('network', True, 20)
        ('run client (subset=true ,timeout=20)', True)

        >>> build_run_client_wrapper('10.0.0.1:7848', False, 30)
        ('run client (10.0.0.1:7848 ,timeout=30)', True)

        >>> build_run_client_wrapper('local')
        ('', False)

        >>> build_run_client_wrapper(None)
        ('', False)
    """
    # Local query - no run client wrapper
    if not destination or destination == 'local':
        return ('', False)

    # Build run client wrapper
    parts = []

    # Add specific destination (if not 'network')
    if destination != 'network':
        parts.append(destination)

    # Add subset parameter
    if subset:
        parts.append(f"subset={str(subset).lower()}")

    # Add timeout parameter
    if timeout:
        parts.append(f"timeout={timeout}")

    # Construct wrapper
    if parts:
        wrapper = f"run client ({' ,'.join(parts)})"
    else:
        wrapper = "run client ()"

    return (wrapper, True)


def should_wait_for_reply(command, has_run_client):
    """
    Determine if command needs to wait for reply.

    This implements the same logic as http_server.prepare_commands()
    to determine when to wait for command execution to complete.

    Args:
        command: Full command string (may include 'run client' wrapper)
        has_run_client: True if command includes 'run client' wrapper

    Returns:
        bool: True if should wait for reply

    Logic (from http_server.al_exec):
    - Wait if: Network query (has_run_client) AND not a file command
    - Don't wait: Local query OR file command

    Rationale:
    - File commands are async (copy happens in background)
    - SQL and other network queries need to wait for results
    - Local commands execute synchronously (no wait needed)
    """
    # Local queries don't need wait
    if not has_run_client:
        return False

    # Extract actual command (after 'run client' wrapper)
    actual_command = command
    if 'run client' in command.lower():
        idx = command.lower().find('run client')
        remaining = command[idx:].strip()
        close_paren = remaining.find(')')
        if close_paren > 0:
            actual_command = remaining[close_paren+1:].strip()

    # File commands should NOT wait (async file transfer)
    if actual_command.startswith('file '):
        return False

    # All other network commands should wait
    return True


def execute_command_with_options(status, command, wfile=None,
                                  destination=None, subset=False,
                                  timeout=None, into_output=None,
                                  io_buff=None, file_data=None):
    """
    Execute EdgeLake command with options (shared by REST API and MCP).

    This is the unified command execution layer that both http_server.al_exec
    and MCP direct_client use. It handles:
    - Building run client wrapper
    - Determining wait behavior
    - Calling appropriate native_api function
    - Proper error handling via status object

    Args:
        status: ProcessStat object for error tracking and job management
        command: EdgeLake command string (without run client wrapper)
        wfile: Output socket for streaming results
        destination: Query destination ('network', 'local', or 'ip:port')
        subset: Allow partial results (bool, default False)
        timeout: Timeout in seconds (int, default 20)
        into_output: Output format (e.g., 'html', None for default)
        io_buff: IO buffer (bytearray, created if None)
        file_data: File data for 'file store' commands

    Returns:
        int: Return code (process_status.SUCCESS or error code)

    Error Handling:
        All errors are logged to status object using status.add_error()
        format as specified in CLAUDE.md rule #3

    Examples:
        # REST API usage (network query)
        ret = execute_command_with_options(
            status, 'sql mydb "select * from table"',
            wfile=self.wfile, destination='network', timeout=30
        )

        # MCP usage (local query)
        ret = execute_command_with_options(
            status, 'get status',
            wfile=socket, destination='local'
        )

        # Network query with subset
        ret = execute_command_with_options(
            status, 'sql mydb "select avg(value) from sensors"',
            wfile=socket, destination='network', subset=True, timeout=60
        )
    """
    ret_val = process_status.SUCCESS

    try:
        # Create io_buff if not provided
        if io_buff is None:
            buff_size = int(params.get_param("io_buff_size"))
            io_buff = bytearray(buff_size)

        # Build run_client wrapper
        run_client_prefix, has_run_client = build_run_client_wrapper(
            destination, subset, timeout
        )

        # Wrap command with 'run client ()' if network query
        if run_client_prefix:
            full_command = f"{run_client_prefix} {command}"
        else:
            full_command = command

        # Determine if we need to wait (using al_exec logic)
        with_wait = should_wait_for_reply(full_command, has_run_client)

        # Execute using appropriate native_api path
        # This ensures proper job management, status reset, and wait handling
        if with_wait:
            # Network query - use exec_al_cmd (includes wait_for_reply)
            timeout_sec = timeout if timeout else 20
            ret_val = native_api.exec_al_cmd(
                status, full_command, wfile, into_output, timeout_sec
            )
        else:
            # Local query or file command - use exec_no_wait
            ret_val = native_api.exec_no_wait(
                status, full_command, io_buff, file_data, wfile
            )

    except Exception as e:
        err_msg = f"Command execution failed: {e}"
        status.add_error(err_msg)
        ret_val = process_status.ERR_process_failure

    return ret_val


def execute_command_simple(status, command, wfile=None, headers=None):
    """
    Simplified command execution for MCP server (no HTTP parsing).

    This is a convenience wrapper around execute_command_with_options()
    that extracts execution parameters from a headers dictionary.

    Args:
        status: ProcessStat object
        command: EdgeLake command (may include 'run client' wrapper already)
        wfile: Output socket
        headers: Optional dict with execution options:
            - 'destination': 'network', 'local', or 'ip:port'
            - 'subset': Allow partial results (bool)
            - 'timeout': Timeout in seconds (int)

    Returns:
        int: Return code

    Examples:
        # MCP server usage
        ret = execute_command_simple(
            status,
            'sql mydb "select * from table"',
            wfile=socket,
            headers={'destination': 'network', 'timeout': 30}
        )

        # Simple local command
        ret = execute_command_simple(status, 'get status', wfile=socket)
    """
    # Extract options from headers if provided
    if headers:
        destination = headers.get('destination')
        subset = headers.get('subset', False)
        timeout = headers.get('timeout')
    else:
        destination = None
        subset = False
        timeout = None

    return execute_command_with_options(
        status, command, wfile,
        destination=destination,
        subset=subset,
        timeout=timeout
    )