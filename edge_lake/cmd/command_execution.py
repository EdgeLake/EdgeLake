"""
Shared command execution logic - EXTRACTED from http_server.al_exec()

This module contains the ACTUAL methods from http_server.ChunkedHTTPRequestHandler,
converted from instance methods (self.) to module functions. This eliminates code
duplication and ensures 100% identical logic between REST API and MCP.

Methods extracted:
- get_run_client() - from ChunkedHTTPRequestHandler.get_run_client()
- execute_al_commands() - from ChunkedHTTPRequestHandler.execute_al_commands()
- prepare_commands() - from ChunkedHTTPRequestHandler.prepare_commands()

License: Mozilla Public License 2.0
"""

from edge_lake.cmd import native_api
from edge_lake.generic import process_status, params, utils_data


def get_run_client(destination, subset, sec_timeout):
    """
    Build 'run client ()' wrapper - EXTRACTED from http_server.get_run_client()

    Original location: http_server.py ChunkedHTTPRequestHandler.get_run_client() line 1367

    Changes from original:
    - Removed self parameter
    - Takes destination, subset, sec_timeout as parameters (not from self.al_headers)
    - Returns only run_client string (ret_val always SUCCESS with params)

    Args:
        destination: 'network', 'local', or 'ip:port' string
        subset: Bool - allow partial results
        sec_timeout: Timeout in seconds (int)

    Returns:
        str: run_client wrapper like 'run client (subset=true ,timeout=20)' or ''
    """
    if not destination or destination == "local":
        run_client = ""  # Not a network call - apply on the query node
    else:
        run_client = "run client ("
        if destination != "network":
            run_client += destination

        if subset:
            if run_client[-1] == '(':
                run_client += f"subset={str(subset).lower()}"
            else:
                run_client += f" ,subset={str(subset).lower()}"

        if sec_timeout:
            # max timeout in seconds for execution completion
            if run_client[-1] == '(':
                run_client += f"timeout={sec_timeout}"
            else:
                run_client += f" ,timeout={sec_timeout}"
        run_client += ') '  # Add space for concatenation with command

    return run_client


def execute_al_commands(status, io_buff, commands_list, into_output, file_data, wfile):
    """
    Execute command list - EXTRACTED from http_server.execute_al_commands()

    Original location: http_server.py ChunkedHTTPRequestHandler.execute_al_commands() line 1435

    Changes from original:
    - Removed self parameter
    - Takes wfile as parameter (not from self.wfile)
    - Changed status.add_keep_error() to status.add_error() for consistency

    Args:
        status: ProcessStat object
        io_buff: IO buffer (bytearray)
        commands_list: List of (command, with_reply) tuples
        into_output: Output format (e.g., 'html' or None)
        file_data: File data for 'file store' commands
        wfile: Output socket for streaming

    Returns:
        int: Return code (process_status.SUCCESS or error code)
    """
    ret_val = process_status.SUCCESS

    for index, entry in enumerate(commands_list):
        command = entry[0]
        with_reply = entry[1]  # Indicate if the thread needs to wait for a reply (i.e. SQL query)
        if with_reply:
            ret_val = native_api.exec_al_cmd(status, command, wfile, into_output, 20)  # Timeout for a list of commands is the default
        else:
            ret_val = native_api.exec_no_wait(status, command, io_buff, file_data, wfile)
        if ret_val:
            if index != (len(commands_list) - 1):
                # the command that failed is in the message body (not the header)
                err_msg = f"Error with command #{index+1} in the message body: '{command}'"
                status.add_error(err_msg)
            break

    return ret_val


def prepare_commands(status, command, rest_cmd_words, commands_list, into_output,
                     run_client, msg_body, msg_body_commands_callback, is_binary_data,
                     file_data_callback):
    """
    Prepare commands for execution - EXTRACTED from http_server.prepare_commands()

    Original location: http_server.py ChunkedHTTPRequestHandler.prepare_commands() line 1275

    Changes from original:
    - Removed self parameter
    - Takes run_client as parameter (not from self.get_run_client())
    - Takes msg_body as parameter (not from self.get_msg_body())
    - Takes callbacks for HTTP-specific operations:
        - msg_body_commands_callback(status, commands_list, msg_body) for parsing body commands
        - file_data_callback(status, msg_body) for parsing file data from body

    Args:
        status: ProcessStat object
        command: Command string from header
        rest_cmd_words: Command split into words (list)
        commands_list: List to populate with (command, with_wait) tuples
        into_output: Output format (e.g., 'html' or None)
        run_client: run client wrapper string (from get_run_client())
        msg_body: Message body content (str or bytes, None if no body)
        msg_body_commands_callback: Function(status, commands_list, msg_body) to parse body commands
        is_binary_data: Bool - is message body binary
        file_data_callback: Function(status, msg_body) -> (ret_val, content_type, file_data)

    Returns:
        list: [ret_val, with_wait, content_type, is_select, is_stream, file_data]
    """
    with_wait = False  # No wait for a reply from a different node
    is_select = False
    is_stream = False
    content_type = 'text/json'
    file_data = None
    ret_val = process_status.SUCCESS

    if rest_cmd_words[0] == "body":
        # The command is passed in the message body
        command = msg_body
        msg_body = None
        cmd_words = utils_data.str_to_list(command, 3)
    else:
        cmd_words = rest_cmd_words

    # Detect SELECT queries
    if cmd_words[0] == "sql":
        cmd_lower = command[4:].lower()
        index = cmd_lower.find("select ", 4)
        if index > 0:
            char_before = cmd_lower[index - 1]
            if char_before == ' ' or char_before == '"':
                is_select = True
                # A select stmt - find output format to determine content-type
                out_format = utils_data.find_next_word(cmd_lower, 4, index, ["format", "="])
                if out_format == "table":
                    content_type = "text"

    elif len(cmd_words) >= 3:
        if utils_data.test_words(cmd_words, 0, ["file", "retrieve", "where"]):
            # If streaming - Need to deliver headers first:
            index = command.find(" stream", 19)
            if index > -1:
                # Test if command includes stream = true
                stream_text = command[index + 1:].replace(" ", "").lower()  # remove spaces
                if stream_text[:11] == "stream=true":
                    content_type = "video/mp4"
                    is_stream = True

        elif cmd_words[0] == "file" and (cmd_words[1] == "store" or cmd_words[1] == "to"):
            # The file can be provided in 2 ways:
            # 1) By identifying a source file
            # 2) by a buffer in the message body
            if msg_body:
                if is_binary_data:
                    file_data = msg_body  # Transfer binary data as is
                else:
                    # The path is provided from the msg body
                    if file_data_callback:
                        ret_val, content_type, file_data = file_data_callback(status, msg_body)
                    msg_body = None  # Message was pushed to the status object

    # Determine if wait is needed
    if run_client:
        # For SQL command
        if command[:5] != "file ":
            # No wait for file copy
            with_wait = True  # Place thread on wait for reply

    # Execute the command (or commands in message body)
    if msg_body and not is_binary_data and msg_body_commands_callback:
        # These are assignments of values or pre-processed commands
        msg_body_commands_callback(status, commands_list, msg_body)

    commands_list.append((run_client + command, with_wait))  # Add a flag if needed to wait for a reply

    return [ret_val, with_wait, content_type, is_select, is_stream, file_data]
