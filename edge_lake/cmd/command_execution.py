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


def prepare_al_command(status, command, headers_dict=None):
    """
    Prepare EdgeLake command for execution (simplified from http_server.prepare_commands).

    This implements the core logic of http_server.prepare_commands() without HTTP-specific
    features like message body parsing and file uploads. For MCP usage.

    Args:
        status: ProcessStat object (for future error handling)
        command: EdgeLake command string (without run client wrapper)
        headers_dict: Optional dict with 'destination', 'subset', 'timeout'

    Returns:
        list: Commands list in format [(full_command, with_wait), ...]
              Same format as http_server.prepare_commands uses

    Example:
        >>> prepare_al_command(status, 'sql mydb "select * from t"', {'destination': 'network'})
        [('run client () sql mydb "select * from t"', True)]

        >>> prepare_al_command(status, 'get status', {'destination': 'local'})
        [('get status', False)]
    """
    # Extract options from headers
    destination = headers_dict.get('destination') if headers_dict else None
    subset = headers_dict.get('subset') if headers_dict else False
    timeout = headers_dict.get('timeout') if headers_dict else None

    # Build run_client wrapper (same as http_server.get_run_client)
    run_client_prefix, has_run_client = build_run_client_wrapper(
        destination, subset, timeout
    )

    # Wrap command if network query
    if run_client_prefix:
        full_command = f"{run_client_prefix} {command}"
    else:
        full_command = command

    # Determine wait behavior (same logic as http_server.prepare_commands line 1330-1336)
    with_wait = should_wait_for_reply(full_command, has_run_client)

    # Return in commands_list format (same as http_server)
    commands_list = [(full_command, with_wait)]

    return commands_list


def execute_al_commands_list(status, wfile, commands_list, io_buff=None, into_output=None, file_data=None):
    """
    Execute command list using http_server.execute_al_commands logic.

    This is functionally identical to http_server.execute_al_commands() (line 1416-1436)
    but implemented as a module function so both REST API and MCP can use it.

    Args:
        status: ProcessStat object for error tracking
        wfile: Output socket for streaming results
        commands_list: List of (command, with_wait) tuples from prepare_al_command()
        io_buff: IO buffer (bytearray, created if None)
        into_output: Output format (e.g., 'html', None for default)
        file_data: File data for 'file store' commands

    Returns:
        int: Return code (process_status.SUCCESS or error code)

    Note:
        This is the EXACT same logic as http_server.execute_al_commands():
        - Iterates through commands_list
        - Calls native_api.exec_al_cmd() if with_wait=True
        - Calls native_api.exec_no_wait() if with_wait=False
        - Stops on first error
    """
    # Create io_buff if not provided
    if io_buff is None:
        buff_size = int(params.get_param("io_buff_size"))
        io_buff = bytearray(buff_size)

    ret_val = process_status.SUCCESS

    # Execute each command (same as http_server.execute_al_commands)
    for index, entry in enumerate(commands_list):
        command = entry[0]
        with_reply = entry[1]  # Indicate if thread needs to wait for reply

        if with_reply:
            # Network query - wait for reply (same as line 1427)
            timeout_sec = 20  # Default timeout for command lists
            ret_val = native_api.exec_al_cmd(status, command, wfile, into_output, timeout_sec)
        else:
            # Local query or file command - no wait (same as line 1429)
            ret_val = native_api.exec_no_wait(status, command, io_buff, file_data, wfile)

        if ret_val:
            # Error occurred
            if index != (len(commands_list) - 1):
                # Not the last command - add error context (same as line 1433)
                err_msg = f"Error with command #{index+1} in command list: '{command}'"
                status.add_error(err_msg)
            break

    return ret_val


def execute_command_with_options(status, command, wfile=None,
                                  destination=None, subset=False,
                                  timeout=None, into_output=None,
                                  io_buff=None, file_data=None):
    """
    Execute EdgeLake command using http_server.prepare_commands + execute_al_commands logic.

    This combines prepare_al_command() and execute_al_commands_list() to provide
    the same functionality as http_server.al_exec() but as a reusable module function.

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
    try:
        # Prepare command (same as http_server.prepare_commands)
        headers_dict = {'destination': destination, 'subset': subset, 'timeout': timeout}
        commands_list = prepare_al_command(status, command, headers_dict)

        # Execute commands (same as http_server.execute_al_commands)
        ret_val = execute_al_commands_list(
            status, wfile, commands_list, io_buff, into_output, file_data
        )

        return ret_val

    except Exception as e:
        err_msg = f"Command execution failed: {e}"
        status.add_error(err_msg)
        return process_status.ERR_process_failure


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