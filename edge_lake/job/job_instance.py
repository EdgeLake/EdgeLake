"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import copy

import edge_lake.tcpip.message_header as message_header
import edge_lake.generic.utils_print as utils_print
import time
import threading

import edge_lake.generic.process_status as process_status
import edge_lake.generic.process_log as process_log
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.utils_threads as utils_threads
import edge_lake.job.job_handle as job_handle
import edge_lake.job.job_scheduler as job_scheduler
import edge_lake.job.job_member as job_member
import edge_lake.dbms.db_info as db_info
import edge_lake.generic.al_parser as al_parser
import edge_lake.generic.params as params

# from edge_lake.generic.utils_io import write_to_stream

query_log_time = -1  # if the value is 0 or greater, log the queries that show execution time greater or equal to query_log_time


class QueryMonitor:
    def __init__(self, entries, time_unit):
        self.entries = entries  # entries in the monitor
        self.time_unit = time_unit  # time unit in seconds
        self.query_time_counter = [0 for _ in range(self.entries + 1)]  # an array counting time of queries
        self.reset()

    # -----------------------------------------
    # Reset counters to ) and restart time
    # -----------------------------------------
    def reset(self):
        for x in range(self.entries + 1):
            self.query_time_counter[x] = 0
        self.query_time_start = time.time()

    # -----------------------------------------
    # Update the monitor
    # -----------------------------------------
    def update_monitor(self, job_time):

        index = int(job_time / self.time_unit)

        if index >= self.entries:
            self.query_time_counter[self.entries] += 1
        else:
            self.query_time_counter[index] += 1

    # -----------------------------------------
    # Get the stats data
    # -----------------------------------------
    def get_stats(self, is_json):

        if is_json:
            info_string = "{\"Queries Statistics\":{"
        else:
            info_string = ""

        total = 0
        for x in range(self.entries):
            queries_counter = self.query_time_counter[x]
            if x == self.entries - 1:
                if is_json:
                    info_string += ("\"Over  %2d sec.\":\"" % (x * self.time_unit) + "{:}\",".format(queries_counter))
                else:
                    info_string += ("Over  %2d sec.: " % (x * self.time_unit) + "{:}\r\n".format(queries_counter))
            else:
                if is_json:
                    info_string += (
                                "\"Up to %2d sec.\":\"" % (x * self.time_unit + 1) + "{:}\",".format(queries_counter))
                else:
                    info_string += ("Up to %2d sec.: " % (x * self.time_unit + 1) + "{:}\r\n".format(queries_counter))
            total += queries_counter

        if is_json:
            info_string += "\"Total queries\":\"%d\"," % total
        else:
            info_string += "Total queries: %d\r\n" % total

        total_time = int((time.time() - self.query_time_start))
        hours_time, minutes_time, seconds_time = utils_data.seconds_to_hms(total_time)

        if is_json:
            info_string += "\"Time interval\":\"{:} (sec.) : {:}:{:}:{:} (H:M:S)\"".format(total_time, hours_time,
                                                                                           minutes_time, seconds_time)
            info_string += "}}"
        else:
            info_string += "Time interval: {:} (sec.) : {:}:{:}:{:} (H:M:S)".format(total_time, hours_time,
                                                                                    minutes_time,
                                                                                    seconds_time)

        return info_string


query_monitor = QueryMonitor(10, 1)


class JobInfo:
    formated_sql = ""  # the original command
    sql_words = ""  # the SQL provided by the user as an array of words
    generic_query = ""  # the query to send to the remote server (proprietary format)
    select_offset = 4  # a counter for words in the select statement up to the "SELECT" keyword
    include_tables = ""  # tables that are to be treated as remote tables
    is_query = False
    functions_counter = 0  # number of functions executed with the sql call
    job_str = ""  # the source command
    counter_local_fields = 0  # number of fields on the local create stmt

    drop_local_table = True  # Default - table is dropped and recreated when query starts. If value is false, the query adds data to the local table.


    def __init__(self):
        self.dbms_type_remote = "psql"  # SQL for remote query
        self.dbms_type_local = "psql"  # SQL for local query
        self.do_create = True  # generate local create stmt
        self.do_remote = True  # generate remote query
        self.use_view = False  # A flag indicating if column info is taken from the view data
        self.select_parsed = al_parser.SelectParsed()  # An object to parse SQL queries

    # =======================================================================================================================
    # Select parsed maintains the SQL Query info
    # =======================================================================================================================
    def get_select_parsed(self):
        return self.select_parsed

    # =======================================================================================================================
    # Set the value to TRUE if the metadata is maintained in the view definition
    # =======================================================================================================================
    def set_view(self, status):
        self.use_view = status

    # =======================================================================================================================
    # Returns True if the metadata is in the view definition
    # =======================================================================================================================
    def is_use_view(self):
        return self.use_view

    # =======================================================================================================================
    # Get the remote query
    # =======================================================================================================================
    def get_remote_query(self):
        return self.select_parsed.remote_query

    # =======================================================================================================================
    # The name of the DBMS that manage the queries locally
    # =======================================================================================================================
    def set_local_dbms(self, local_dbms):
        self.dbms_type_local = local_dbms

    # =======================================================================================================================
    # Return the name of the DBMS that manage the queries locally
    # =======================================================================================================================
    def get_remote_dbms(self):
        return self.dbms_type_remote

    # =======================================================================================================================
    # Get the remote_table name
    # =======================================================================================================================
    def get_remote_table(self):
        return self.select_parsed.remote_table

    # =======================================================================================================================
    # Return the name of the DBMS that manage the queries locally
    # =======================================================================================================================
    def get_local_dbms(self):
        return self.dbms_type_local

    # =======================================================================================================================
    # Reurn True if remote query is participating
    # =======================================================================================================================
    def is_with_remote_query(self):
        return self.do_remote


class JobInstance:

    def __init__(self, id: int):

        self.job_info = JobInfo()
        self.job_info.select_parsed.set_local_table("query_" + str(id))  # the local table name

        self.job_id = id
        self.start_time = 0
        self.job_counter = 0  # unique job id
        self.description = ""  # the command to execute as provided by the user or app
        self.job_status = ""  # Done - Processing Data (locally) - Receiving Data = ""
        self.unique_job_id = 0  # a unique job id used to com[are messages with JobInstance
        self.process_counter = 0  # number of messages processed

        self.delete_table = True  # a flag to delete the name of the local table every query
        self.table_created = False
        self.is_active = False
        self.receivers = 0  # the number of nodes that will be processing the message
        self.reply_counter = 0  # the number of nodes that returned a reply
        self.rows_counter = 0  # a counter used in the pass_through process to indicate rows processed

        self.scheduled_table = ""  # output table name
        self.end_time = 0

        self.previous_time = ""  # time used in repeatable queries with time range

        self.j_handle = job_handle.JobHandle(self.job_id)
        self.j_handle.set_select_parsed(self.job_info.select_parsed)

        self.j_handle.reset()

        self.header_flag = False  # a flag used in the pass_through process to indicate first printout

        self.data_mutex = utils_threads.ReadWriteLock()  # a mutex to prevent conflicting processes between threads that are leveraging the same data

        self.ip_port_values = []

        self.reply_counter_mutex = threading.Lock()  # mutex to the counter of threads replies

        self.members = [[job_member.JobMember()]]  # initialize for 1 data partition

        self.output_job = None  # This value is updated if current job is an input to a different job
        self.input_query_id = 0  # If this is an input query - an id that identifies the query when multiple ubput queries
        self.with_data = False  # Set to True if at least one node replies with data

        self.debug_counter = 0
        self.debug_info = []         # struct for debug info strings
        self.target_send = False     # When messages are send to multiple nodes, the flag is set with True when send is completed


    # =======================================================================================================================
    # Add debug info strings
    # =======================================================================================================================
    def add_debug_info(self, info_str):

        if self.debug_counter < 1000:
            # Keep some limit
            self.debug_info.append(info_str)
            self.debug_counter += 1

    # =======================================================================================================================
    # Add debug info strings
    # =======================================================================================================================
    def get_debug_info(self):

        out_text = ""
        for entry in self.debug_info:
            out_text += ("\r\n" + entry)

        return out_text
    # =======================================================================================================================
    # This job is an input to the destination job
    # =======================================================================================================================
    def set_output_job(self, dest_job, input_query_id):
        self.output_job = dest_job
        self.input_query_id = input_query_id

    def set_job_type(self, is_select):
        self.job_info.is_query = is_select

    # =======================================================================================================================
    # Returns True if the data of this query updates a second query
    # =======================================================================================================================
    def is_input_job(self):
        if self.output_job:
            ret_val = True
        else:
            ret_val = False
        return ret_val

    # =======================================================================================================================
    # If this is an input query, return an id that identifies the query when multiple ubput queries
    # =======================================================================================================================
    def get_input_query_id(self):
        return self.input_query_id

    # =======================================================================================================================
    # This job is an input to the destination job - return the dest job
    # =======================================================================================================================
    def get_output_job(self):
        return self.output_job

    # =======================================================================================================================
    # Set the buffers to contain the replies.
    # The buffer is 2 dimensional:
    # - an Entry for each participating operator
    # - an Entry for each partition of the operator
    # The buffer is initiated before the message is send
    # =======================================================================================================================
    def set_reply_buffers(self, participating_nodes):
        buffer_size = len(self.members)
        if buffer_size < participating_nodes:
            for x in range(buffer_size, participating_nodes):
                # Place an array to contain the partition
                # set the buffer for a single partition
                self.members.append([job_member.JobMember()])

    # =======================================================================================================================
    # Provide the Job ID
    # =======================================================================================================================
    def get_job_id(self):
        return self.job_id

    # =======================================================================================================================
    # Provide the Job Info
    # =======================================================================================================================
    def get_job_info(self):
        return self.job_info

    # =======================================================================================================================
    # Get the IP and Port values of destination members
    # =======================================================================================================================
    def get_ip_port_values(self):
        return self.ip_port_values  # an array with the IP and ports

    # =======================================================================================================================
    # Provide the IP and Port values of destination members
    # =======================================================================================================================
    def save_ip_port_values(self, ip_port_values):
        self.ip_port_values = ip_port_values  # an array with the IP and ports

    # =======================================================================================================================
    # Get the destination nodes for each query
    # =======================================================================================================================
    def get_query_dest_info(self):

        job_id =  self.job_id
        job_str = self.job_info.job_str
        job_dest_info = []
        for entry in self.ip_port_values:
                                #   Job ID         Node IP + Port     DBMS       Table     SQL
            job_dest_info.append((job_id, entry[0] + ':' + entry[1], entry[2], entry[3], job_str))
            job_id = ""
            job_str = ""
        return job_dest_info

    # =======================================================================================================================
    # Add a print message to the objects representing the network nodes
    # =======================================================================================================================
    def add_print_msg(self, receiver_id, node_ret_val, is_json, print_msg, is_last_block):
        '''
        receiver_id - represents the peer node
        node_ret_val - ret_val from the peer node
        is_json - True if peer reply is in JSON format
        print_msg - the msg data (in JSON or str)
        is_last_block - True when the last block returned from the node is processed
        '''

        self.members[receiver_id][0].save_print_reply(node_ret_val, is_json, print_msg)

        if is_last_block:
            self.members[receiver_id][0].count_replies()
            self.members[receiver_id][0].set_is_last_block(True)

    # =======================================================================================================================
    # Add a print message to the member representing the peer node
    # =======================================================================================================================
    def get_nodes_print_message(self):

        json_reply = "{"
        reply = ""
        for x in range(self.receivers):

            node_info = self.members[x][0]

            message = node_info.get_reply_msg()
            ip_port = node_info.get_ip_port()

            if node_info.is_msg_json():
                # JSON reply
                if len(json_reply) > 1:
                    json_reply += ','
                json_reply += "\"%s\" : %s" % (ip_port, message)
            else:
                # string reply
                if len(message) <= 120 and message.find('\n') == -1:
                    reply += "\r\n[From Node %s] %s" % (ip_port, message)
                else:
                    reply += "\r\n[From Node %s]\r\n%s" % (ip_port, message)


        if len(json_reply) > 1:
            if reply:
                # reply may include json and error messages which are not in JSON
                reply = json_reply + ",\"messages\" : \"%s\"}" % reply
            else:
                reply = json_reply + "}"

        return reply

    # ----------------------------------------------------------------------
    # Organize the nodes replies in a list or a dictionary
    # ----------------------------------------------------------------------
    def get_nodes_reply_object(self, is_dict):
        # If is_dict is true - return a dictionary
        # If is_dict is false - return a list

        out_obj = {} if is_dict else []     # The struct with the results from all nodes

        for x in range(self.receivers):

            node_info = self.members[x][0]

            message = node_info.get_reply_msg()
            ip_port = node_info.get_ip_port()

            if is_dict:
                out_obj[ip_port] = message
            else:
                out_obj.append([ip_port, message])
        return out_obj

    # =======================================================================================================================
    # Set the number of nodes receiving the sql and the sql stmt
    # Note that the job handle is copied to the current
    # =======================================================================================================================
    def set_job(self, receivers: int, j_handle: job_handle):

        self.start_time = time.time()
        self.end_time = 0
        self.receivers = receivers

        self.j_handle.reset()
        job_scheduler.copy_job_handle_info(self.j_handle, j_handle)  # the instructions on how to execute the job
        self.reset_prams_for_new_job()

    # =======================================================================================================================
    # A new job initiated by the user - or a new job initiated by a leading query
    # =======================================================================================================================
    def reset_prams_for_new_job(self):

        self.job_status = "Initialized"

        self.table_created = False  # this flag will force a create statement
        self.reply_counter = 0  # counter for each reply received
        self.rows_counter = 0  # a counter used in the pass_through process to indicate rows processed
        self.header_flag = False  # a flag used in the pass_through process to indicate first printout
        self.process_counter = 0  # number of messages processed
        self.with_data = False  # Set to True when first server returns data
        self.debug_counter = 0

        if len(self.debug_info):
            self.debug_info = []         # struct for debug info strings

        for x in range(self.receivers):
            counter_par = self.members[x][0].get_data_partitions()  # The number of data partitions with this operator
            for y in range(counter_par):
                self.members[x][y].reset()

    # =======================================================================================================================
    # Return True if at least one server returned data
    # =======================================================================================================================
    def is_with_data(self):
        return self.with_data

    # =======================================================================================================================
    # Save the source command
    # =======================================================================================================================
    def save_src_cmd(self, job_string):
        self.job_info.job_str = copy.copy(job_string)   # Shallow copy as the ptr to the job_string may change

    # =======================================================================================================================
    # Mutex to prevent pushing data to local database or to the REST client while the rest thread is in a timeout
    # mutex_type can be 'R' for read and 'W' for write
    # =======================================================================================================================
    def data_mutex_aquire(self, mutex_type):
        if mutex_type == 'R':
            self.data_mutex.acquire_read()
        else:
            self.data_mutex.acquire_write()

    # =======================================================================================================================
    # Release data mutex
    # mutex_type can be 'R' for read and 'W' for write
    # =======================================================================================================================
    def data_mutex_release(self, mutex_type):
        if mutex_type == 'R':
            self.data_mutex.release_read()
        else:
            self.data_mutex.release_write()


    # =======================================================================================================================
    # If pass through is True - count rows provided to caller
    # =======================================================================================================================
    def count_pass_rows(self):
        self.rows_counter += 1
        return self.rows_counter

    # =======================================================================================================================
    # This call is used when output JSON format from multiple threads.
    # The call only mutex with the first printed row
    # =======================================================================================================================
    def mutex_first(self):
        '''
        If first row - keep the mutex and return True
        Else - release the mutex, change the header flag,  and return False
        '''
        ret_val = False
        if not self.header_flag:
            # a flag used in the pass_through process to indicate first printout:
            # if a row was printed - no need to mutex
            self.reply_counter_mutex.acquire()

            if self.header_flag:
                self.reply_counter_mutex.release()
            else:
                ret_val = True      # mutex is maintained

        return ret_val
    # =======================================================================================================================
    # This call is used when output JSON format from multiple threads.
    # Release the mutex on the first output call
    # =======================================================================================================================
    def release_mutex_first(self):
        self.header_flag = True  # change the header flag
        self.reply_counter_mutex.release()

    # =======================================================================================================================
    # If pass through is True - return rows provided to caller
    # =======================================================================================================================
    def get_pass_rows(self):
        return self.rows_counter

    # =======================================================================================================================
    # If pass through is TRue - no need in unified database - results from operators are transferred to client as is
    # =======================================================================================================================
    def is_pass_through(self):
        '''
        There are 2 validations - user did not disable the option + select_parsed is set to True (from inspecting the
        query in unify_results)
        '''
        ret_val = self.job_info.select_parsed.get_pass_through()    # value from query process in unify_result
        if ret_val:
            # Test if disabled by user
            if self.j_handle.test_condition_value("pass_through", False):
                ret_val = False     # disabled by user
        return ret_val
    # =======================================================================================================================
    # get the instructions on how to execute the job
    # =======================================================================================================================
    def get_job_handle(self):
        return self.j_handle

    # =======================================================================================================================
    # Test if deleting the table before each run.
    # For a scheduled task, if a table name is provided in the schedule command, the table is not deleted.
    # =======================================================================================================================
    def is_delete_table(self):
        return self.delete_table

    # =======================================================================================================================
    # Flag that a local table was created
    # This flag prevent multiple creates with scheduled tables
    # =======================================================================================================================
    def set_table_created(self):
        self.table_created = True

    # =======================================================================================================================
    # Test if a local table was created
    # This call prevent multiple creates with scheduled tables
    # =======================================================================================================================
    def is_table_created(self):
        return self.table_created

    # =======================================================================================================================
    # Return True if SQL Select Query
    # =======================================================================================================================
    def is_select(self):
        return self.job_info.is_query

    # =======================================================================================================================
    # Return the number of fields on the local/unifying table
    # =======================================================================================================================
    def get_counter_local_fields(self):
        return self.job_info.get_select_parsed().get_counter_local_fields()

    # =======================================================================================================================
    # Get command to provide metadata
    # =======================================================================================================================
    def get_metadata_command(self):
        if self.job_info.select_parsed.remote_dbms == "" or self.job_info.select_parsed.remote_table == "":
            return ""
        return "info table %s %s columns" % (
        self.job_info.select_parsed.remote_dbms, self.job_info.select_parsed.remote_table)

    # =======================================================================================================================
    # Before messages are send to target nodes, this flag is set to False,
    # When all messages are send, this flag is set to True.
    # The flag is tested when it is needed that sending is not yet in process
    # =======================================================================================================================
    def send_to_target(self, flag:bool):
        self.target_send = flag
    # =======================================================================================================================
    # Before messages are send to target nodes, this flag is set to False,
    # When all messages are send, this flag is set to True.
    # The flag is tested when it is needed that sending is not yet in process
    # =======================================================================================================================
    def is_in_target(self):
        return self.target_send

    # =======================================================================================================================
    # Set info on how reply should be processed
    # =======================================================================================================================
    def set_query_processig_info(self):

        select_parsed = self.job_info.get_select_parsed()

        if self.j_handle.is_output_to_table() or self.is_input_job():
            # j_handle.is_output_to_table - user selected a database table that is to be updated
            # is_input_job - the data of this query updates a second query
            # Force update of the system or user dbms with the result set
            select_parsed.set_pass_through(False)

        local_dbms = db_info.get_db_type("system_query")  # the dbms used to manage queries
        self.job_info.set_local_dbms(local_dbms)

        # Multiple dest servers - use a leading query, or one server with partitions
        select_parsed.use_leading_queries = True

    # =======================================================================================================================
    # Test if meta info of remote database and table are available on local node
    # =======================================================================================================================
    def is_with_remote_table_info(self, status):

        # Test if the table exists in the metadata dictionary
        if not db_info.is_table_defs(self.job_info.select_parsed.remote_dbms, self.job_info.select_parsed.remote_table):
            # Test if a view exists in the metadata dictionary
            if not db_info.is_view_defs(self.job_info.select_parsed.remote_dbms,
                                        self.job_info.select_parsed.remote_table):
                error_message = "Missing metadata on node for table: " + self.job_info.select_parsed.remote_dbms + "." + self.job_info.select_parsed.remote_table
                status.add_error(error_message)
                status.keep_error(error_message)

                return process_status.Missing_metadata_info  # No such table and no such view

        return process_status.SUCCESS

    # =======================================================================================================================
    # Get the Query Message send to the Operators.
    # The query may be different than the user query to reflect the data inserted to the local database
    # =======================================================================================================================
    def get_message_with_remote_query(self):
        message = "sql " + self.job_info.select_parsed.remote_dbms + " " + "text " + self.job_info.select_parsed.remote_query + ';'
        return message

    # =======================================================================================================================
    # Get the prefix to the SQL statement
    # =======================================================================================================================
    def get_sql_mesg_prefix(self):
        return "sql " + self.job_info.select_parsed.remote_dbms + " "

    # =======================================================================================================================
    # Get the Generic Query - this is a proprietary format to use by non-sql servers
    # =======================================================================================================================
    def get_generic_query(self):
        return self.job_info.generic_query

    # =======================================================================================================================
    # Get the remote query
    # =======================================================================================================================
    def get_remote_query(self):
        return self.job_info.select_parsed.remote_query

    # =======================================================================================================================
    # Return True if the Job Instance is active
    # =======================================================================================================================
    def is_job_active(self):
        return self.is_active

    # =======================================================================================================================
    # Set a job counter and make active
    # =======================================================================================================================
    def set_active(self, counter: int):
        self.is_active = True
        self.job_counter = counter

    # =======================================================================================================================
    # Stop a scheduled job - when a user issues: job stop ID (for an ID pointing to a scheduled job)
    # =======================================================================================================================
    def stop_scheduled(self):
        self.is_active = False
        self.job_status = "Stopped"

    # =======================================================================================================================
    # Continue a scheduled job - when a user issues: job run ID (for an ID pointing to a scheduled job)
    # =======================================================================================================================
    def continue_scheduled(self):
        self.is_active = True
        self.job_status = "Pending"

    # =======================================================================================================================
    # Keep a uique job id. This value is also placed on a message to identify that threads did not timed out.
    # =======================================================================================================================
    def set_unique_job_id(self, unique_value):
        self.unique_job_id = unique_value

    # =======================================================================================================================
    # Get the uique job id.
    # =======================================================================================================================
    def get_unique_job_id(self):
        return self.unique_job_id

    # =======================================================================================================================
    # Set not active (job completed) - record statistics before setting the flag
    # =======================================================================================================================
    def set_not_active(self, is_local_err=False):

        if is_local_err:
            # local error ob the query node
            self.job_status = "Local Err"

        if self.job_info.is_query:
            self.end_time = time.time()
            job_time = int(self.end_time - self.start_time)
            query_monitor.update_monitor(job_time)

            if query_log_time >= 0:
                if query_log_time <= job_time:
                    # log slow queries
                    time_str = str(job_time) + " sec.  :       "
                    process_log.add("query", time_str[:15] + self.job_info.job_str)
        self.is_active = False

    # =======================================================================================================================
    # On the query node - Get the operator IP and Port
    # =======================================================================================================================
    def get_operator_ip_port(self, mem_view: memoryview):
        receiver_id = message_header.get_receiver_id(mem_view)  # The id of the node issueing the reply
        member_instance = self.members[receiver_id][0]  # Use the first partition
        return member_instance.get_ip_port()

    # =======================================================================================================================
    # Get the list of IP and ports of the nodes participating in the query process
    # =======================================================================================================================
    def get_participating_operators(self):

        operators = []
        for x in range(self.receivers):
            # Get the number of data partitions with this operator
            try:
                member_instance =  self.members[x][0]
                ip_port = member_instance.get_ip_port()
            except:
                pass
            else:
                operators.append(ip_port)


        return operators

    # =======================================================================================================================
    # Process Incomming Message
    # =======================================================================================================================
    def process_message(self, mem_view: memoryview):

        self.process_counter += 1  # number of messages processed

        operator_id = message_header.get_receiver_id(mem_view)  # The id of the node issuing the reply
        par_id = message_header.get_partition_id(
            mem_view)  # If the operator data is partitioned - get the id of the partition
        par_counter = message_header.get_partitions_used(mem_view)  # The number of partitions used with the Operator
        if not par_counter:
            par_counter = 1  # In the case of empty set, par_counter is returned as 0


        if par_counter > 1:
            # Set object for the partition that replies
            self.set_par_object(operator_id, par_counter)

        self.members[operator_id][0].set_data_partitions(
            par_counter)  # set the number of partitions on he first instance

        member_instance = self.members[operator_id][par_id]

        error_code = message_header.get_error(mem_view)

        member_instance.set_returned_code(error_code)

        member_instance.set_process_time(time.time())  # the time for last reply

        member_instance.incr_message_counter()  # count returning messages

        message_type = message_header.get_info_type(mem_view)

        reply_header = bytes(mem_view[:message_header.SIZE_OF_HEADER])

        member_instance.set_reply_header(reply_header)

        member_instance.set_is_first_block(message_header.is_first_block(reply_header))
        member_instance.set_is_last_block(message_header.is_last_block(reply_header))

        member_instance.set_dbms_time(message_header.get_dbms_time(reply_header))

        member_instance.set_send_time(message_header.get_send_time(reply_header))

        if message_header.get_block_struct(mem_view) == message_header.BLOCK_STRUCT_JSON_MULTIPLE:
            reply_data = bytes(mem_view[message_header.SIZE_OF_HEADER:])
            member_instance.set_reply_data(reply_data)
        else:
            # map to string
            reply_data = message_header.get_data_decoded(mem_view)
            member_instance.set_reply_data(reply_data)

        if not error_code:

            if message_type == message_header.BLOCK_INFO_TABLE_STRUCT:
                # get the info on the table structure

                offset_end_dbms_name = 12 + reply_data[12:-1].find(
                    '.')  # get the offset by the length of dbms name from string
                self.job_info.select_parsed.remote_dbms = reply_data[12:offset_end_dbms_name]
                offset_end_table_name = 1 + offset_end_dbms_name + reply_data[offset_end_dbms_name + 1:-1].find('\"')
                self.job_info.select_parsed.remote_table = reply_data[offset_end_dbms_name + 1: offset_end_table_name]
            self.with_data = True  # at least one node replied with data

        segments_counter = message_header.get_counter_jsons(mem_view)  # Number of message instances
        if message_header.is_with_row_suffix(mem_view):
            segments_counter -= 1  # First row of this block is the suffix of a row in a previous block - no need to be counted
        self.set_reply_entries(operator_id, par_id, segments_counter)  # Update number of rows returned

        return [message_type, operator_id, par_counter, par_id, error_code]

    # =======================================================================================================================
    # Declare, if needed the partition objects for each operator
    # =======================================================================================================================
    def set_par_object(self, operator_id, par_counter):

        par_buffers = len(self.members[operator_id])
        if par_buffers < par_counter:
            for x in range(par_buffers, par_counter):
                # Allocate a JobMember instance for each data partition of the operator
                self.members[operator_id].append(job_member.JobMember())

    # =======================================================================================================================
    # Set decryption passwords on each member instance (the first data reply includes the decryption key)
    # =======================================================================================================================
    def set_decryption_passwords(self, status, operator_id, par_id, auth_str):

        member_instance = self.members[operator_id][par_id]
        # Store the password and salt to decrypt the data
        ret_val = member_instance.set_decryption_passwords(status, auth_str)

        return ret_val
    # =======================================================================================================================
    # Get number of messages processed
    # =======================================================================================================================
    def get_process_counter(self):
        return self.process_counter

    # =======================================================================================================================
    # This method is used with a query
    # Count replies - count the number of replies to the query. THis needs to be called after the processing of the reply data.
    # =======================================================================================================================
    def count_replies(self, receiver_id, par_id):

        self.reply_counter_mutex.acquire()  # The mutex to prevent multiple threads updating the counter at the same time

        par_replies = self.members[receiver_id][0].count_replies()  # partition 0 counts for the entire partition

        if par_replies == self.members[receiver_id][0].get_data_partitions():
            # all partitions replied
            self.reply_counter += 1  # count the number of nodes returned a reply

        self.reply_counter_mutex.release()

    # =======================================================================================================================
    # This method is used with commands which are not queries
    # =======================================================================================================================
    def count_nodes_replied(self):
        # Count new node replied
        # Return True if all replied
        self.reply_counter += 1  # count the number of nodes returned a reply
        return self.reply_counter == self.receivers

    # =======================================================================================================================
    # Test if all nodes replied
    # =======================================================================================================================
    def all_replied(self):
        return self.reply_counter == self.receivers

    # =======================================================================================================================
    # Number of nodes completed a reply
    # Test that last block was considered in each participating partition
    # =======================================================================================================================
    def get_nodes_replied(self):

        nodes_replied = 0
        for x in range(self.receivers):
            # Get the number of data partitions with this operator
            par_buffers = self.members[x][0].get_data_partitions()  # Number of data partitions per participating node
            par_replied = 0
            for y in range(par_buffers):
                member_info = self.members[x][y]
                if member_info.returned_code != process_status.Empty_data_set:  # Empty data set is considered as node replied
                    if not member_info.is_last_block() or member_info.returned_code:
                        break
                par_replied += 1
            if par_replied == par_buffers:
                # Reply from all partitions
                nodes_replied += 1

        return nodes_replied

    # =======================================================================================================================
    # Test if all nodes replied or flagged with an error
    # =======================================================================================================================
    def is_process_completed(self):
        # Returns True if process completed

        nodes_replied = 0
        for x in range(self.receivers):
            # Get the number of data partitions with this operator
            par_buffers = self.members[x][0].get_data_partitions()  # Number of data partitions per participating node
            par_replied = 0
            for y in range(par_buffers):
                member_info = self.members[x][y]
                if member_info.returned_code != process_status.No_reply:
                    # An error - consider node replied
                    nodes_replied += 1
                    break
                if not member_info.is_last_block():
                    # Partition is still processing
                    break
                par_replied += 1    # Partition completed properly
            if par_replied == par_buffers:
                # Reply from all partitions
                nodes_replied += 1

        return nodes_replied == self.receivers

    # =======================================================================================================================
    # Change number of receivers - if a query is changed to retrieve metadata
    # =======================================================================================================================
    def set_num_receivers(self, counter):
        self.receivers = counter

    # =======================================================================================================================
    # Return the number of nodes participating
    # =======================================================================================================================
    def get_nodes_participating(self):
        return self.receivers
    # =======================================================================================================================
    # save the IP and port on each receiver of the message (Only update on the first partition)
    # =======================================================================================================================
    def set_receiver_info(self, receiver_id, ip, port):
        self.members[receiver_id][0].set_ip_port(ip, port)

    # =======================================================================================================================
    # Return a unique id of the job
    # =======================================================================================================================
    def get_job_counter(self):
        return self.job_counter

    # =======================================================================================================================
    # Test if message was not send to one of the nodes - by testing if Error message on partition 0 is process_status.NETWORK_CONNECTION_FAILED
    # =======================================================================================================================
    def is_failed_connection(self):
        for x in range(self.receivers):
            # Get the number of data partitions with this operator
            ret_code = self.members[x][0].get_returned_code()
            if ret_code == process_status.NETWORK_CONNECTION_FAILED or ret_code == process_status.Operator_not_accessible:
                return True
        return False  # All messages were send
    # =======================================================================================================================
    # Test if all operators returned empty data set
    # =======================================================================================================================
    def is_empty_result_set(self):
        for x in range(self.receivers):
            # Get the number of data partitions with this operator
            par_buffers = self.members[x][0].get_data_partitions()
            for y in range(par_buffers):
                if self.members[x][y].get_returned_code() != process_status.Empty_data_set:
                    return False
        return True  # all returned empty result set

    # =======================================================================================================================
    # Return a message with all errors by nodes
    # =======================================================================================================================
    def get_nodes_error_list(self):
        error_list = None
        for x in range(self.receivers):
            # Get the number of data partitions with this operator
            par_buffers = self.members[x][0].get_data_partitions()
            for y in range(par_buffers):
                member_info = self.members[x][y]
                ret_code = member_info.get_returned_code()
                if ret_code and ret_code != process_status.Empty_data_set:
                    if not error_list:
                        error_list = []
                    # Include: IP + Port + Partition + RetCode + Text
                    error_list.append([member_info.get_ip(), member_info.get_port(), y, ret_code, process_status.get_status_text(ret_code)])
        return error_list  # Return error per partition

    # =======================================================================================================================
    # Get reply data from all receivers
    #  Reply on commands that do not return rows:
    #  Examples: Metadata, Error reply
    # =======================================================================================================================
    def get_reply_data(self, output_prefix: str):

        reply_data = ""
        for x in range(self.receivers):
            # Get the number of data partitions with this operator
            par_buffers = self.members[x][0].get_data_partitions()
            for y in range(par_buffers):

                member_instance = self.members[x][y]

                if reply_data != "":
                    reply_data += ","

                returned_code = member_instance.get_returned_code()
                if not returned_code:
                    # ---------------------------------------------------------
                    #  Metadata
                    # ---------------------------------------------------------
                    reply_data += member_instance.get_reply_data()
                else:
                    # ---------------------------------------------------------
                    #  Format error messages
                    # ---------------------------------------------------------
                    message = process_status.get_status_text(returned_code)
                    operator_ip = member_instance.get_ip()
                    operator_port = member_instance.get_port()
                    reply_data += ("{\"From %s:%s\":\"Error %u: %s\"}" % (
                    operator_ip, operator_port, returned_code, message))

                    if message == "SQL Failure":
                        reply_data += self.format_error_msg(x, returned_code, "json")

        reply_data = "{\"" + output_prefix + "\":[" + reply_data + "]}"
        return reply_data

    # =======================================================================================================================
    # Format error message
    # This forma code assumes a postgress string ... need to extend to other sources
    # =======================================================================================================================
    def format_error_msg(self, receiver_id: int, returned_code: int, format_type: str):

        member_instance = self.members[receiver_id][0]
        message = process_status.get_status_text(returned_code)
        operator_ip = member_instance.get_ip()
        operator_port = member_instance.get_port()

        if format_type == "json":
            reply_data = ("{\"From %s:%s\":\"Error %u: %s\"}" % (operator_ip, operator_port, returned_code, message))
        else:
            reply_data = "%02u Received error %u from node: %s : " % (self.job_id, returned_code, message)

        if message == "SQL Failure":
            # This forma code assumes a postgress string ... need to extend to other sources
            message_returned = member_instance.get_reply_data()
            if (message_returned):
                reply_data += db_info.format_db_err_messages( message_returned, format_type)

        return reply_data

    # =======================================================================================================================
    # Print reply info
    # =======================================================================================================================
    def print_reply_info(self):

        if not self.receivers:
            utils_print.output("%02u Job contains no info" % self.job_id, True)
        else:
            for x in range(self.receivers):
                # Get the number of data partitions with this operator
                par_buffers = self.members[x][0].get_data_partitions()
                for y in range(par_buffers):

                    operator_ip = self.members[x][0].get_ip()
                    operator_port = self.members[x][0].get_port()

                    utils_print.output("[From Node %s:%s] " % (operator_ip, operator_port), False)
                    returned_code = self.members[x][y].get_returned_code()
                    if not returned_code:
                        if self.members[x][y].get_reply_data() == "":
                            utils_print.output("No data returned", True)
                        else:
                            self.print_data_segments(x, y)
                    else:
                        message = self.format_error_msg(x, returned_code, "text")
                        utils_print.output(
                            "%02u Received error %u from node: %s" % (self.job_id, returned_code, message), True)


    # =======================================================================================================================
    # Get the Job Status Info
    # status_info is an array, every entry contains partition Info
    # title = ["Job", "ID", "Output", "Run Time", "Node", "Command/Status"]
    # =======================================================================================================================
    def get_job_status_info(self, status_info: list):

        # Generic info for the entire query

        job_id = "%04u" % self.job_id
        unique_id = self.unique_job_id
        output_dest = self.get_job_handle().get_output_dest()

        if not self.end_time:
            total_time = int(time.time() - self.start_time)  # query not completed
        else:
            total_time = int(self.end_time - self.start_time)

        hours_time, minutes_time, seconds_time = utils_data.seconds_to_hms(total_time)
        run_time = "%02u:%02u:%02u" % (hours_time, minutes_time, seconds_time)

        status_info.append([job_id, unique_id, output_dest, run_time, "All", self.job_info.job_str])

        for x in range(self.receivers):
            operator_ip = self.members[x][0].get_ip()
            operator_port = self.members[x][0].get_port()
            ip_port = f"{operator_ip}:{operator_port}"

            returned_code = self.members[x][0].get_returned_code()
            if returned_code:
                if returned_code == process_status.No_reply:
                    status = "Sending"  # message was not yet sent
                else:
                    if returned_code == process_status.Empty_data_set:
                        status = "Empty set"  # Returned empty set
                    else:
                        status = "Error: %s" % (process_status.get_status_text(returned_code))
            else:
                status = "Completed"


            status_info.append(["", "", "", "", ip_port, status])

    # =======================================================================================================================
    # Get the query Status Info
    # status_info is an array, every entry contains partition Info
    # title = ["Job", "ID", "Output", "Run Time", "Node", "Partition", "Status", "Blocks", "Rows", "Command"]
    # =======================================================================================================================
    def get_query_status_info(self, status_info: list):

        # Generic info for the entire query

        job_id = "%04u" % self.job_id
        unique_id = self.unique_job_id
        output_dest = self.get_job_handle().get_output_dest()

        if not self.end_time:
            total_time = int(time.time() - self.start_time)  # query not completed
        else:
            total_time = int(self.end_time - self.start_time)

        hours_time, minutes_time, seconds_time = utils_data.seconds_to_hms(total_time)
        run_time = "%02u:%02u:%02u" % (hours_time, minutes_time, seconds_time)

        status_info.append([job_id, unique_id, output_dest, run_time, "All", "---", self.job_status, 0, 0, self.job_info.job_str])

        # Per partition info

        for x in range(self.receivers):

            operator_ip = self.members[x][0].get_ip()
            operator_port = self.members[x][0].get_port()
            ip_port = f"{operator_ip}:{operator_port}"

            par_buffers = self.members[x][0].get_data_partitions()
            if not par_buffers:
                # This is the case where a reply did not arrive and the value was not updated
                par_buffers = 1

            for y in range(par_buffers):
                par_info = []
                # title = ["Job", "ID", "Output", "Run Time", "Node", "Partition", "Status", "Blocks", "Rows", "Command"]
                par_info.append("")  # Job
                par_info.append("") # ID
                par_info.append("")  # Output
                if self.members[x][y].is_last_block():
                    # Partition finished
                    par_end_time = self.members[x][y].get_process_time()  # The time that the last block was received
                    if not par_end_time:
                        # Node not completed
                        total_time = 0
                    else:
                        total_time = int(par_end_time - self.start_time) # The partition execution time
                else:
                    total_time = int(time.time() - self.start_time)

                hours_time, minutes_time, seconds_time = utils_data.seconds_to_hms(total_time)
                run_time = "%02u:%02u:%02u" % (hours_time, minutes_time, seconds_time)

                par_info.append(run_time)
                if not y:
                    # Only first line of the partition
                    par_info.append(ip_port)
                else:
                    par_info.append("")     # IP - Port

                par_info.append(y)

                returned_code = self.members[x][y].get_returned_code()

                if returned_code:
                    if returned_code == process_status.No_reply:
                        status = "Sending"  # message was not yet sent
                    else:
                        if returned_code == process_status.Empty_data_set:
                            status = "Empty set"  # Returned empty set
                        else:
                            status = "Error: %s" % (process_status.get_status_text(returned_code))
                else:
                    if self.job_status == "Delivered":
                        status = "Received"  # message that does not need a reply
                    else:
                        if self.members[x][y].is_last_block() == False:
                            status = "Processing"
                        else:
                            status = "Completed"

                par_info.append(status)

                msg_counter = self.members[x][y].get_message_counter()  # number of blocks
                status_info[0][7] += msg_counter        # Update Total number of blocks

                par_info.append(msg_counter)

                rows_counter = self.members[x][y].get_reply_entries()
                status_info[0][8] += rows_counter  # Update Total number of Rows

                par_info.append(rows_counter)

                par_info.append("")     # Command

                status_info.append(par_info)

    # =======================================================================================================================
    # When messages are not triggering replies - Set 0 on returned code after the message is send
    # =======================================================================================================================
    def set_only_send_message(self):
        for x in range(self.receivers):
            self.members[x][0].set_returned_code(process_status.SUCCESS)
    # =======================================================================================================================
    # Set the partition returned code
    # =======================================================================================================================
    def set_par_ret_code(self, operator_id, par_id, ret_code):
        self.members[operator_id][par_id].set_returned_code(ret_code)

    # =======================================================================================================================
    # Print data segments
    # =======================================================================================================================
    def print_data_segments(self, receiver_id, par_id):
        if not message_header.get_block_struct(
                self.members[receiver_id][par_id].get_reply_header()) == message_header.BLOCK_STRUCT_JSON_MULTIPLE:
            reply_data = self.members[receiver_id][par_id].get_reply_data()
            json_data = utils_json.str_to_json(reply_data)
            if json_data:
                utils_print.output("\r\n", False)
                utils_print.print_row(str(json_data), True)
            else:
                # Not a JSON struct
                utils_print.output(reply_data, True)
        else:
            utils_print.output("Data Segments in Block", True)

    # =======================================================================================================================
    # Print the job command
    # =======================================================================================================================
    def show_explain(self):

        output_txt = ""

        data = []
        data.append(["Job ID", self.job_id])

        if self.job_info.select_parsed.remote_dbms != "":  # the DBMS on the operator node
            data.append(["Remote DBMS", self.job_info.select_parsed.remote_dbms])
        if self.job_info.select_parsed.remote_table != "":  # the Table on the operator node
            data.append(["Remote Table", self.job_info.select_parsed.remote_table])

        if self.job_info.job_str != "":  # the Table on the operator node
            data.append(["Source Command", self.job_info.job_str])
        if self.job_info.select_parsed.remote_query != "":  # the SQL to send to the remote server (SQL to be executed by the operator)
            data.append(["Remote Query", self.job_info.select_parsed.remote_query])
        if self.job_info.select_parsed.local_create != "":  # the SQL table to keep the returned results from the operators
            data.append(["Local Create", self.job_info.select_parsed.local_create])
        if self.job_info.select_parsed.local_query != "":  # the SQL to provide the unified result
            data.append(["Local Query", self.job_info.select_parsed.local_query])


        output_txt += utils_print.output_nested_lists(data, "", None, True)

        return output_txt

    # =======================================================================================================================
    # Get job command
    # =======================================================================================================================
    def get_job_command(self):
        return self.job_info.job_str

    # =======================================================================================================================
    # Get the remote_dbms name
    # =======================================================================================================================
    def get_remote_dbms(self):
        return self.job_info.select_parsed.remote_dbms

    # =======================================================================================================================
    # Get the remote_table name
    # =======================================================================================================================
    def get_remote_table(self):
        return self.job_info.select_parsed.remote_table

    # =======================================================================================================================
    # Get the local_table name
    # =======================================================================================================================
    def get_local_table(self):
        return self.job_info.select_parsed.get_local_table()

    # =======================================================================================================================
    # Set a flag that will not drop the local table when query starts - data will be added to an existing table
    # =======================================================================================================================
    def set_keep_local_table(self):
        self.job_info.drop_local_table = False

    # =======================================================================================================================
    # Return False if the local table needs to be dropped and recreated when query starts
    # =======================================================================================================================
    def is_drop_local_table(self):
        return self.job_info.drop_local_table

    # =======================================================================================================================
    # Get Select Parsed object
    # =======================================================================================================================
    def get_select_parsed(self):
        return self.job_info.select_parsed

    # =======================================================================================================================
    # Get the local_create
    # =======================================================================================================================
    def get_local_create(self):
        return self.job_info.select_parsed.local_create

    # =======================================================================================================================
    # Get the output destination
    # =======================================================================================================================
    def get_output_dest(self):
        return self.j_handle.get_output_dest()

    # =======================================================================================================================
    # Is the output to stdout
    # =======================================================================================================================
    def is_stdout(self):
        return self.j_handle.is_stdout()

    # =======================================================================================================================
    # The print status is set to suppress prints to stdout
    # =======================================================================================================================
    def set_print_status(self, status):
        self.j_handle.set_print_status(status)

    # =======================================================================================================================
    # Return false to suppress stdout print of replied data from peer messages
    # =======================================================================================================================
    def get_print_status(self):
        return self.j_handle.get_print_status()

    # =======================================================================================================================
    # Return the local query
    # =======================================================================================================================
    def get_local_query(self):
        return self.job_info.select_parsed.local_query

    # =======================================================================================================================
    # Return TRUE if the last block with the result set is being procesed
    # =======================================================================================================================
    def is_last_block(self, receiver_id, par_id):
        # Get the number of data partitions with this operator
        return self.members[receiver_id][par_id].is_last_block()

    # =======================================================================================================================
    # Update the number of entries returned by the partition
    # =======================================================================================================================
    def set_reply_entries(self, receiver_id, par_id, counter):
        return self.members[receiver_id][par_id].set_reply_entries(counter)

    # =======================================================================================================================
    # Return the query data in JSON format
    # =======================================================================================================================
    def get_returned_data(self, receiver_id, par_id):
        return utils_json.str_to_json(self.members[receiver_id][par_id].get_reply_data())

    # =======================================================================================================================
    # Return the number of segments in a block
    # =======================================================================================================================
    def get_counter_jsons(self, receiver_id, par_id):
        return message_header.get_counter_jsons(self.members[receiver_id][par_id].get_reply_header())

    # =======================================================================================================================
    # Return the JSON - if the entire structure is available
    # =======================================================================================================================
    def get_complete_json(self, status, receiver_id, par_id, offset):

        member_instance = self.members[receiver_id][par_id]

        if not offset:
            # offset of the first segment in the block
            location = message_header.get_command_length(member_instance.get_reply_header())
            if member_instance.is_first_block():
                # First block in the sequence returned the encryption key in the header (at the auth_str position)
                location += member_instance.get_length_encrypt_key()    # This is the key (generated on the peer node) used to encrypt the data
        else:
            location = offset

        # two first bytes of the header of the JSON is the size of the JSON
        size = int.from_bytes(member_instance.get_reply_data()[
                              location + message_header.OFFSET_SEGMENT_LENGTH: location + message_header.OFFSET_SEGMENT_LENGTH + 2],
                              byteorder='big', signed=False)

        # 1 byte is indicating if the entire JSON in contained in the block
        segment_stat = member_instance.get_reply_data()[
                       location + message_header.OFFSET_SEGMENT_COMPLETED: location + message_header.OFFSET_SEGMENT_COMPLETED + 1]

        # get as a string
        data = bytes(member_instance.get_reply_data()[
                     location + message_header.INFO_PREFIX_LENGTH: location + message_header.INFO_PREFIX_LENGTH + size]).decode()

        if segment_stat == message_header.SEGMENT_PREFIX:
            # the data is not complete - keep to include the data with the next block
            member_instance.add_to_incomplete_segment(data)
            return [0, segment_stat, None]
        elif segment_stat == message_header.SEGMENT_SUFIX:
            # extend the prefix by the previous block
            data = member_instance.get_incomplete_segment() + data
            member_instance.reset_incomplete_segment()  # empty the segment

        if member_instance.is_data_encrypted():
            # If the data was encrypted - decrypt the data
            data = member_instance.decrypt_data(status, data)

        # calculate offset of the next segment
        next_offset = location + message_header.INFO_PREFIX_LENGTH + size

        return [next_offset, segment_stat, utils_json.str_to_json(data)]

    # =======================================================================================================================
    # Set Previous time for repeatable SQL queries
    # =======================================================================================================================
    def set_previous_time(self, time_str: str):
        self.previous_time = time_str

    # =======================================================================================================================
    # Get Previous time for repeatable SQL queries
    # =======================================================================================================================
    def get_previous_time(self):
        return self.previous_time

    # =======================================================================================================================
    # Set the current status
    # status is: Done - Processing Data (locally) - Receiving Data
    # =======================================================================================================================
    def set_job_status(self, new_status):
        self.job_status = new_status

    # =======================================================================================================================
    # Get the current status
    # status is: Done - Processing Data (locally) - Receiving Data
    # =======================================================================================================================
    def get_job_status(self):
        return self.job_status

    # =======================================================================================================================
    # Get the time recorded on the job
    # =======================================================================================================================
    def get_start_time(self):
        return self.start_time

    # =======================================================================================================================
    # Get the end time recorded on the job
    # =======================================================================================================================
    def get_end_time(self):
        return self.end_time

    # =======================================================================================================================
    # End Job Instance if active - and signal REST caller (if needed)
    # =======================================================================================================================
    def end_job_instance(self, unique_job_id, is_reply_msg, is_subset):
        '''
        unique_job_id - the id of the job_instance - to make sure it was not modified
        is_reply_msg - True value will update the counter showing number of nodes replied
        '''
        is_done = True      # The default - representing Job Instance is not used with this unique_job_id
        self.data_mutex_aquire('W')  # Write Mutex
        # test thread did not timeout - or a previous process terminated the task
        if self.is_job_active() and self.get_unique_job_id() == unique_job_id:
            if is_reply_msg:
                is_done = self.count_nodes_replied()  # Count the number of nodes replied and return TRUE is all replied
            else:
                # If not subset, end process, else - if all replied or with an error
                is_done = self.is_process_completed()
            # Flag that the job is completed
            if is_done:
                assignment = self.j_handle.get_assignment()  # If with value - results are assigned to the assignment key
                if assignment:

                    if len(assignment) >= 3 and (assignment.endswith("[]") or assignment.endswith("{}")):
                        # Update result in a list or a dictionary
                        # Return a dictionary or a list with the result set
                        is_dict = assignment[-2] == '{'     # Flag if output is set in a dictionary or as a list
                        out_obj = self.get_nodes_reply_object(is_dict)
                        params.create_result_struct(assignment, out_obj)
                    else:
                        result_set = self.get_nodes_print_message()
                        # Assign result to a key
                        offset = 0
                        if self.get_nodes_participating() == 1:
                            # With one node participating, remove the peer node ID: i.e.: [From Node 10.0.0.78:3048]
                            index = result_set.find("]\r\n", 11)
                            if index != -1:
                                offset = index + 3
                        params.add_param(assignment, result_set[offset:])        # Add key value pair to the dictionary
                elif self.j_handle.is_rest_caller():
                    # Signal REST Thread
                    self.set_job_status("Completed")
                    self.j_handle.signal_wait_event()  # signal the REST thread that output is ready

        self.data_mutex_release('W')  # release the mutex that prevents conflict with the rest thread
        return is_done
# =======================================================================================================================
# Reset the query timer - set 0 in counters and set current time
# =======================================================================================================================
def reset_query_timer(status, io_buff_in, cmd_words, trace):
    global query_monitor
    query_monitor.reset()
    return process_status.SUCCESS

# =======================================================================================================================
# Get the query timer stats
# =======================================================================================================================
def get_query_stats(is_json):
    global query_monitor
    return query_monitor.get_stats(is_json)
