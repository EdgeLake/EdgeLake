"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.generic.params as params
import edge_lake.generic.process_status as process_status
import edge_lake.job.job_scheduler as  job_scheduler
import edge_lake.dbms.cursor_info as cursor_info
import edge_lake.dbms.db_info as db_info
import edge_lake.generic.utils_io as utils_io
import edge_lake.generic.utils_print as utils_print
from edge_lake.generic.interpreter import get_multiple_values
from edge_lake.generic.utils_json import str_to_json, to_string, str_to_list
from edge_lake.generic.utils_columns import change_columns_values

# ------------------------------------------------------------------------
# Call sql statement
# timeout value - determines max wait time, 0 - wait forever
# ------------------------------------------------------------------------
def exec_sql_stmt(status, servers, dbms_name, conditions, sql_stmt, timeout):
    if not dbms_name:
        status.add_error("Missing database name in Grafana query")
        ret_val = process_status.Missing_dbms_name
    else:
        # The thread will be on wait for processing to complete and return data string
        # "DEST = REST" means that output will be written to  wfile
        al_command = "run client (%s) sql %s dest = rest " % (servers, dbms_name)
        if conditions:
            al_command += ("and " + conditions + " ")
        al_command += "\"" + sql_stmt + "\""

        ret_val = exec_al_cmd(status, al_command, None, None, timeout)

    return ret_val
# ------------------------------------------------------------------------
# AnyLog command that does not return data
# ------------------------------------------------------------------------
def exec_no_wait(status, command, io_buff, wfile):

    end_job(status)  # If multiple queries are executed in the same session/report
    public_key = status.get_public_key()
    status.reset(public_key)  # Keep the same public key with multiple queries (on the same sessions)

    status.get_job_handle().set_rest_caller()
    status.get_job_handle().set_output_socket(wfile)  # Queries executed locally will use the socket to transfer data

    ret_val = member_cmd.process_cmd(status, command=command, print_cmd=False, source_ip=None,
                                             source_port=None, io_buffer_in=io_buff)

    # Flag the JOB Instance as not in use

    return ret_val
# ------------------------------------------------------------------------
# AnyLog command which is not SQL
# ------------------------------------------------------------------------
def exec_native_cmd(status, servers, command, timeout):
    if servers:
        command = f"run client ({servers}) {command}"
    ret_val = exec_al_cmd(status, command, None, None, timeout)
    return ret_val

# ------------------------------------------------------------------------
# Call anylog command statement
# Thread will be on wait for execution and return data
# ------------------------------------------------------------------------
def exec_al_cmd(status, al_command, wfile, into_output, timeout_sec):
    '''
    status - status object
    al_command - command or sql to execute
    wfile - output file
    into_output - could be into HTML report
    '''
    end_job(status)  # If multiple queries are executed in the same session/report
    public_key = status.get_public_key()
    status.reset(public_key)        # Keep the same public key with multiple queries (on the same sessions)
    status.set_rest_wait()  # Flag that thread is on wait for reply

    buff_size = int(params.get_param("io_buff_size"))
    io_buff = bytearray(buff_size)
    j_handle = status.get_job_handle()
    j_handle.set_rest_caller()
    j_handle.set_output_socket(wfile)  # socket set to null will keep data in a dbms
    if into_output:
        j_handle.set_output_into(into_output)   #   into_output can be html

    command_ret_val = member_cmd.process_cmd(status, command=al_command, print_cmd=False, source_ip=None,
                                             source_port=None, io_buffer_in=io_buff)

    if command_ret_val == process_status.SUCCESS:
        # Wait for the reply from all the Operators
        subset = status.is_subset()
        ret_val = wait_for_reply(status, al_command, subset, timeout_sec)

    # Flag the JOB Instance as not in use

    if command_ret_val:
        ret_val = command_ret_val

    # Test for error on current thread or coming from an operator
    if not ret_val and not j_handle.is_subset():
        # Exit with a node returning an error:
        ret_val = status.get_active_job_handle().get_operator_error()  # error returned by operator

    return ret_val


# =======================================================================================================================
# End reply - use the JOB ID to identify the job instance and flag the job instance as done
# =======================================================================================================================
def end_job(status):
    job_id = status.get_job_id()

    if (job_id != process_status.JOB_INSTANCE_NOT_USED):
        j_instance = job_scheduler.get_job(job_id)
        # job instance was used
        # mutex to avoid a case that the REST thread timesout while thread in process_reply continue to push data
        # with an error - test JOB is not used by a different thread
        j_instance.data_mutex_aquire(status, 'W')
        if j_instance.is_job_active() and j_instance.get_unique_job_id() == status.get_unique_job_id():
            if not j_instance.get_job_handle().is_subset(): # Do not disable process with subset
                j_instance.set_not_active()
        j_instance.data_mutex_release(status, 'W')


# =======================================================================================================================
# Wait for reply to commands issued
# =======================================================================================================================
def wait_for_reply(status, al_command, subset, timeout):
    '''
    status - the thread status flag
    subset - return subset replies and not as an error
    timeout - max wait in seconds for nodes to reply
    '''
    sleep_time = 4  # sleep time in seconds
    loop_time = 0
    message_flag = False

    j_handle = status.get_active_job_handle()

    job_id = status.get_job_id()
    j_instance = job_scheduler.get_job(job_id)
    status_unique_id = status.get_unique_job_id()

    messages_processed = j_instance.get_process_counter()   # Counter for the number of messages processed

    ret_val = process_status.SUCCESS

    while 1:
        if process_status.is_exit("rest"):
            ret_val = process_status.EXIT
            break  # terminate code

        if not j_handle.is_subset():
            # Exit with a node returning an error
            ret_val = j_handle.get_operator_error()  # error returned by operator
            if ret_val:
                break

        if j_handle.wait_event(sleep_time):
            break  # was signaled because JOB completed or a different thread encountered an error

        loop_time += sleep_time

        counter = j_instance.get_process_counter() # Get the current number of messages processed
        if counter != messages_processed:
            loop_time = 0                           # Work is being done by threads - do not terminate
            messages_processed = counter

        if loop_time > 20 and not message_flag:
            # print message once for a long wait
            status.add_error("REST Server is waiting for reply on job[%u] ID: %u command: '%s'" % (
            job_id, status_unique_id, al_command))
            message_flag = True

        if timeout:
            # 0 timeout waits forever
            if loop_time >= timeout:
                if subset:
                    break           # with subset flag, a partial replies works
                status.add_error("REST process exits wait after %u with timeout: '%s'" % (loop_time, al_command))
                ret_val = process_status.TIMEOUT

                status.keep_error(
                    "{\"Result.set\":\"REST Server Timeout\"}")  # this message would be returned to the caller
                if member_cmd.is_debug_method("rest"):
                    # Enabled by command:   -    debug on rest
                    job_status_info = []
                    job_id = j_handle.get_job_id()
                    job_scheduler.get_job(job_id).get_job_status_info(job_status_info)
                    title = ["ID", "Start-Time", "time", "Status", "Output", "IP", "Port", "Partition", "blocks",
                             "Rows",
                             "Command"]
                    output_txt = utils_print.output_nested_lists(job_status_info, "", title, True)
                    output_txt += j_instance.get_debug_info()
                    utils_print.output(output_txt, True)

                break

        # mutex to avoid a case that the REST thread timesout while thread in process_reply continue to push data
        j_instance.data_mutex_aquire(status, 'R')
        if not j_instance.is_job_active() or j_instance.get_unique_job_id() != status_unique_id:
            ret_val = j_handle.get_process_error()  # an error while processing the replies
            if not ret_val:
                ret_val = process_status.JOB_not_active
        j_instance.data_mutex_release(status, 'R')

        if ret_val:
            status.add_error("REST Server exits (process failure) with command: '%s'" % al_command)
            break

    return ret_val


# =======================================================================================================================
# Get the errpr message -
# From the current thread or from an operator satisfying a request from the current thread
# =======================================================================================================================
def get_error_text(status, ret_val):
    # Test for an error on current thread or comming from an operator
    if ret_val == status.get_active_job_handle().get_operator_error():
        err_msg = status.get_active_job_handle().get_operator_error_txt()  # error text from an operator
        if not err_msg:
            err_msg = process_status.get_status_text(ret_val)
    else:
        err_msg = process_status.get_status_text(ret_val)

    return err_msg


# =======================================================================================================================
# Query the local database with the result sets from all participating nodes
# io_stream -> an I/O stream connected to the destination socket. If not available, query data is returned.
# =======================================================================================================================
def get_sql_reply_data(status: process_status, logical_dbms: str, io_stream):
    out_data = ""

    j_handle = status.get_active_job_handle()
    select_parsed = j_handle.get_select_parsed()
    title_list = select_parsed.get_query_title()
    time_columns = select_parsed.get_date_types()  # List of columns that are date-based
    casting_list = select_parsed.get_casting_list()     # The list of casting functions
    casting_columns = select_parsed.get_casting_columns()


    conditions = j_handle.get_conditions()
    format, timezone = get_multiple_values(conditions, ["format", "timezone"], ["json", None])

    sql_local_node = j_handle.get_command()  # The SQL to execute on the local node to retrieve the result set for the user query

    dbms_cursor = cursor_info.CursorInfo()  # Get the AnyLog Generic Cusror Object
    if not db_info.set_cursor(status, dbms_cursor,
                              "system_query"):  # "system_query" - a local database that keeps the query results
        status.add_error("Error", "Query DBMS not recognized: system_query")
        return [process_status.DBMS_error, "", 0]

    if not db_info.process_sql_stmt(status, dbms_cursor, sql_local_node):
        if io_stream:
            result_set = "{\"Query.output.error\":\"SQL on local node not supported\"}"
            utils_io.write_to_stream(status, io_stream, result_set, True, True)
            status.add_error("Failed to prepare a query on local node: " + sql_local_node)
        db_info.close_cursor(status, dbms_cursor)
        return [process_status.ERR_SQL_failure, "", 0]

    get_next = True
    first = True
    ret_val = process_status.SUCCESS
    rows_counter = 0

    while get_next:  # read each row - when the rows is read, it is copied to one or more data blocks - see message message_header.copy_data() below.

        get_next, rows_data = db_info.process_fetch_rows(status, dbms_cursor, "Query", 1, title_list, None)
        if not get_next or not rows_data:
            break

        rows_counter += 1

        if (len(time_columns) and (not timezone or timezone != "utc")) or len(casting_columns):
            # change time from utc to current
            query_data = str_to_json(rows_data)
            if query_data:
                change_columns_values(status, timezone, time_columns, casting_columns, casting_list, title_list, query_data['Query'])
                # with out_list = we use the list
                rows_data = to_string(query_data)   # json.dumps(json_obj) --> the conversion to string can be inconsistent with the location of the brachets


        if not first:
            # the conversion to string can be inconsistent with the location of the brachets
            if rows_data[9] == '[':
                offset_from = 10         # The offset after the brackets
            elif rows_data[10] == '[':
                offset_from = 11        # The offset after the brackets
            else:
                offset_from = rows_data.find('[', 9)

            if offset_from > 0:
                send_data = "," + rows_data[offset_from:-2]
        else:
            send_data = rows_data[:-2]
            first = False

        if io_stream:
            ret_val = utils_io.write_to_stream(status, io_stream, send_data, True, False)
        else:
            out_data += send_data

        if ret_val:
            break

    if not ret_val:
        if first:
            # No data delivered
            if io_stream:
                send_data = "{\"Query.output.all\":\"Empty.set\"}"
                ret_val = utils_io.write_to_stream(status, io_stream, send_data, True, True)
        else:
            send_data = "]}"
            if io_stream:
                ret_val = utils_io.write_to_stream(status, io_stream, send_data, True, True)
            else:
                out_data += send_data

    db_info.close_cursor(status, dbms_cursor)

    return [ret_val, out_data, rows_counter]


# =======================================================================================================================
# Execute a blockchain command
# Example: "blockchain get html where id = "example_html"
# =======================================================================================================================
def get_from_blockchain(status, bchain_cmd):

    ret_val, policy = member_cmd.blockchain_get(status, bchain_cmd.split(), None, True)
    json_policy = None
    if not ret_val:
        if len(policy) != 1:
            status.add_error(f"Failed to identify the HTML policy as {len(policy)} policies were retrieved: '{bchain_cmd}'")
            ret_val = process_status.Wrong_policy_structure
        else:
            json_policy = policy[0]
    else:
        json_policy = None
        status.add_error(f"Failed to retrieve HTML policy using: '{bchain_cmd}'")
        ret_val = process_status.Policy_not_in_local_file
    return [ret_val, json_policy]

# =======================================================================================================================
# Return a unified reply from all the participating nodes
# =======================================================================================================================
def get_network_reply(status):

    ret_val = process_status.Failed_to_retrieve_network_reply
    job_id = status.get_job_id()
    if (job_id != process_status.JOB_INSTANCE_NOT_USED):
        # job instance was used
        j_instance = job_scheduler.get_job(job_id)
        # Mutex such that the job instance is not reused by a different thread in the query process
        j_instance.data_mutex_aquire(status, 'W')
        if j_instance.is_job_active() and j_instance.get_unique_job_id() == status.get_unique_job_id():
            reply_data = j_instance.get_nodes_print_message()
            ret_val = process_status.SUCCESS
        j_instance.data_mutex_release(status, 'W')

    if ret_val:
        reply_data = "Failed to retrieve reply from network servers"

    return [ret_val, reply_data]




