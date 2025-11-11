"""
Protocol-Agnostic Command Execution

Transport-independent version of al_exec that works with any protocol
via callback interface.

This module extracts the core execution logic from http_server.al_exec()
and makes it work with HTTP, MCP, stdio, or any other transport.

License: Mozilla Public License 2.0
"""

import logging
from typing import Optional

from edge_lake.generic import process_status, params, utils_data
from edge_lake.generic.protocol_callbacks import ProtocolCallbacks
from edge_lake.cmd import command_execution
from edge_lake.job import job_scheduler

logger = logging.getLogger(__name__)


def protocol_exec(status, command: str, protocol_callbacks: ProtocolCallbacks,
                  http_method: str = "get", into_output=None, headers: dict = None) -> int:
    """
    Execute EdgeLake command with protocol-agnostic callbacks.

    This is the core execution logic from al_exec, but transport-independent.
    Works with HTTP REST, MCP/SSE, stdio, or any protocol that implements
    ProtocolCallbacks interface.

    Args:
        status: ProcessStat object
        command: EdgeLake command string (e.g., "get status", "sql dbname ...")
        protocol_callbacks: Protocol-specific callbacks for sending responses
        http_method: HTTP method hint (for validation) - "get", "post", "put"
        into_output: Optional output format (e.g., "html")
        headers: Optional headers dict (may contain 'destination', 'subset', 'timeout')

    Returns:
        Return value (process_status.SUCCESS or error code)

    Architecture:
        1. Validate command
        2. Prepare commands list
        3. Execute commands
        4. Handle results based on type (query, command, stream)
        5. Call protocol callbacks for success or error

    Example (HTTP):
        callbacks = HTTPProtocolCallbacks(http_handler, "get")
        ret_val = protocol_exec(status, "get status", callbacks)

    Example (MCP):
        callbacks = MCPProtocolCallbacks(sse_connection, json_rpc_id)
        ret_val = protocol_exec(status, "sql dbname SELECT ...", callbacks)
    """

    ret_val = process_status.SUCCESS

    logger.debug(f"[{protocol_callbacks.get_protocol_name()}] Executing: {command}")

    # 1. VALIDATION: Check command exists
    if not command:
        error_msg = "Missing 'command' attribute in header"
        logger.warning(f"[{protocol_callbacks.get_protocol_name()}] {error_msg}")
        protocol_callbacks.send_error(status, process_status.Missing_command, error_msg)
        return process_status.Missing_command

    # 2. PARSE: Split command into words
    cmd_words = utils_data.str_to_list(command, 3)

    # 3. METHOD VALIDATION: Check HTTP method matches command (for HTTP compatibility)
    # Note: For non-HTTP protocols, this may not apply
    # TODO: Make this optional or move to HTTP-specific code
    # For now, skip for non-HTTP protocols
    if protocol_callbacks.get_protocol_name() == "HTTP":
        from edge_lake.tcpip.http_server import is_correct_method
        if cmd_words[0] != "body" and not is_correct_method(status, http_method, cmd_words):
            error_msg = f"Wrong HTTP method for command: {http_method}"
            logger.warning(f"[HTTP] {error_msg}")
            protocol_callbacks.send_error(
                status, process_status.Wrong_http_metod, error_msg,
                {"command": command, "http_method": http_method}
            )
            return process_status.Wrong_http_metod

    # 4. PREPARATION: Prepare command list and determine execution mode
    commands_list = []

    # Extract execution parameters from headers (if provided)
    # These control how commands are routed: local vs network
    if headers:
        destination = headers.get('destination', 'local')
        subset = headers.get('subset', False)
        sec_timeout = headers.get('timeout', None)
    else:
        # Default: run locally (no network call)
        destination = "local"
        subset = False
        sec_timeout = None

    # Build run_client wrapper for network/local execution
    run_client = command_execution.get_run_client(destination, subset, sec_timeout)

    logger.debug(f"[{protocol_callbacks.get_protocol_name()}] Execution mode: destination={destination}, subset={subset}, timeout={sec_timeout}")

    # For HTTP compatibility, these would come from headers
    # For MCP, these are not used
    msg_body = None
    format_type = None
    pass_through = False

    ret_val, with_wait, content_type, is_select, is_stream, file_data = \
        command_execution.prepare_commands(
            status, command, cmd_words, commands_list, into_output,
            run_client, msg_body, format_type, pass_through, None
        )

    if ret_val != process_status.SUCCESS:
        error_msg = f"Command preparation failed: {ret_val}"
        logger.error(f"[{protocol_callbacks.get_protocol_name()}] {error_msg}")
        protocol_callbacks.send_error(status, ret_val, error_msg, {"command": command})
        return ret_val

    # 5. SEND HEADERS EARLY (for streaming protocols)
    if with_wait and protocol_callbacks.supports_streaming():
        if not into_output:  # If not HTML - HTML is organized as a single write
            protocol_callbacks.send_headers(status, content_type, is_chunked=True)
            logger.debug(f"[{protocol_callbacks.get_protocol_name()}] Headers sent for streaming")

    # 6. EXECUTION: Execute the command(s)
    buff_size = int(params.get_param("io_buff_size"))
    io_buff = bytearray(buff_size)

    # Get output socket from protocol callbacks
    output_socket = protocol_callbacks.get_output_socket(status)

    # Set output socket on job handle
    j_handle = status.get_job_handle()
    if hasattr(j_handle, 'set_output_socket'):
        j_handle.set_output_socket(output_socket)

    ret_val = command_execution.execute_al_commands(
        status, io_buff, commands_list, into_output, file_data, output_socket
    )

    logger.debug(f"[{protocol_callbacks.get_protocol_name()}] Execution completed: ret_val={ret_val}")

    # 7. ERROR HANDLING: Check execution result
    j_handle = status.get_active_job_handle()

    if ret_val != process_status.SUCCESS and ret_val < process_status.NON_ERROR_RET_VALUE:
        # Command failed - send error response
        err_msg = j_handle.get_operator_error_txt() if j_handle else None
        local_err = status.get_saved_error()

        error_message = err_msg or local_err or f"Command failed with code {ret_val}"

        logger.error(f"[{protocol_callbacks.get_protocol_name()}] {error_message}")

        protocol_callbacks.send_error(
            status, ret_val, error_message,
            {
                "command": command,
                "operator_msg": err_msg,
                "local_msg": local_err,
                "with_wait": with_wait,
                "into_output": into_output
            }
        )
        return ret_val

    # 8. RESULT HANDLING: Handle different result types

    # Case A: Local SELECT query (query on this node, not distributed)
    if not with_wait and is_select:
        logger.debug(f"[{protocol_callbacks.get_protocol_name()}] Handling local SELECT query")

        job_id = status.get_job_id()
        if job_id != process_status.JOB_INSTANCE_NOT_USED:
            j_instance = job_scheduler.get_job(job_id)
            nodes_count = j_instance.get_nodes_participating()
            nodes_replied = j_instance.get_nodes_replied()

            # Execute local query
            # For HTTP, this uses send_reply_headers callback
            # For MCP, callback is None (results go to socket buffer)
            send_headers_callback = None
            if protocol_callbacks.get_protocol_name() == "HTTP":
                # Need to pass HTTP handler's send_reply_headers method
                # This is a bit awkward - might need to refactor local_table_query
                # For now, we'll skip this and let results go to buffer
                pass

            write_ret_value = command_execution.local_table_query(
                status, j_handle, with_wait, nodes_count, nodes_replied,
                send_headers_callback
            )

            j_instance.set_not_active()

            # For non-streaming protocols, read result from buffer
            if not protocol_callbacks.supports_streaming():
                # Read from output socket buffer
                if hasattr(output_socket, 'getvalue'):
                    output_socket.seek(0)
                    result_data = output_socket.read()
                    if result_data:
                        result_text = result_data.decode('utf-8') if isinstance(result_data, bytes) else str(result_data)
                        protocol_callbacks.send_success(status, result_text, content_type,
                                                       {"with_wait": with_wait, "is_select": is_select})

    # Case B: Distributed SELECT query (with_wait=True, aggregated results)
    elif with_wait and is_select:
        logger.debug(f"[{protocol_callbacks.get_protocol_name()}] Handling distributed SELECT query")

        job_id = status.get_job_id()
        if job_id != process_status.JOB_INSTANCE_NOT_USED:
            j_instance = job_scheduler.get_job(job_id)

            # Mutex to protect job instance
            j_instance.data_mutex_aquire(status, 'W')

            try:
                if j_instance.is_job_active() and j_instance.get_unique_job_id() == status.get_unique_job_id():
                    if not j_instance.is_pass_through():
                        # Query system_query database for aggregated results
                        nodes_count = j_instance.get_nodes_participating()
                        nodes_replied = j_instance.get_nodes_replied()

                        write_ret_value = command_execution.local_table_query(
                            status, j_handle, with_wait, nodes_count, nodes_replied, None
                        )

                        # For non-streaming, read from buffer
                        if not protocol_callbacks.supports_streaming():
                            if hasattr(output_socket, 'getvalue'):
                                output_socket.seek(0)
                                result_data = output_socket.read()
                                if result_data:
                                    result_text = result_data.decode('utf-8') if isinstance(result_data, bytes) else str(result_data)
                                    protocol_callbacks.send_success(status, result_text, content_type,
                                                                   {"with_wait": with_wait, "is_select": is_select})

                        # Handle into_output (HTML) case
                        if into_output:
                            output_buff = j_handle.get_output_buff()
                            if output_buff:
                                protocol_callbacks.send_success(status, output_buff, content_type)

                    # Handle subset case (partial results)
                    if j_handle.is_subset():
                        if not j_handle.is_query_completed():
                            # Query summary
                            from edge_lake.cmd import member_cmd
                            summary_result = member_cmd.query_summary(status, io_buff, j_instance, None)

                        # Error list from failed nodes
                        error_list = j_instance.get_nodes_error_list()
                        if error_list:
                            # Include error summary in metadata
                            logger.warning(f"Partial results: {len(error_list)} nodes failed")

            finally:
                j_instance.data_mutex_release(status, 'W')

            j_instance.set_not_active()

    # Case C: Distributed non-SELECT command (print message from multiple nodes)
    elif with_wait and not is_select:
        logger.debug(f"[{protocol_callbacks.get_protocol_name()}] Handling distributed command")

        job_id = status.get_job_id()
        if job_id != process_status.JOB_INSTANCE_NOT_USED:
            j_instance = job_scheduler.get_job(job_id)

            j_instance.data_mutex_aquire(status, 'W')

            try:
                if j_instance.is_job_active() and j_instance.get_unique_job_id() == status.get_unique_job_id():
                    result_set = j_instance.get_nodes_print_message()
                    if result_set:
                        protocol_callbacks.send_success(status, result_set, content_type,
                                                       {"with_wait": with_wait})
            finally:
                j_instance.data_mutex_release(status, 'W')

    # Case D: Stream file (e.g., image, PDF)
    elif is_stream:
        logger.debug(f"[{protocol_callbacks.get_protocol_name()}] Handling stream file")

        stream_file = j_handle.get_stream_file()
        # TODO: Implement protocol_callbacks.send_file() for file streaming
        # For now, just log
        logger.warning(f"File streaming not yet implemented for {protocol_callbacks.get_protocol_name()}")

    # Case E: Regular result (most common for non-query commands)
    else:
        logger.debug(f"[{protocol_callbacks.get_protocol_name()}] Handling regular result")

        result_set = j_handle.get_result_set()

        # Convert list to string if needed
        if isinstance(result_set, list):
            result_set = "\r\n".join([str(item) for item in result_set])

        # Determine content type
        from edge_lake.tcpip.http_server import get_result_set_type
        if not content_type:
            content_type = get_result_set_type(result_set)

        # Send result
        if result_set:
            protocol_callbacks.send_success(status, result_set, content_type)
        elif not with_wait:
            # Empty result but no error
            protocol_callbacks.send_success(status, "", content_type)

    logger.debug(f"[{protocol_callbacks.get_protocol_name()}] Command completed successfully")

    return ret_val
