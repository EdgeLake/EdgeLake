'''
By using this source code, you acknowledge that this software in source code form remains a confidential information of AnyLog, Inc.,
and you shall not transfer it to any other party without AnyLog, Inc.'s prior written consent. You further acknowledge that all right,
title and interest in and to this source code, and any copies and/or derivatives thereof and all documentation, which describes
and/or composes such source code or any such derivatives, shall remain the sole and exclusive property of AnyLog, Inc.,
and you shall not edit, reverse engineer, copy, emulate, create derivatives of, compile or decompile or otherwise tamper or modify
this source code in any way, or allow others to do so. In the event of any such editing, reverse engineering, copying, emulation,
creation of derivative, compilation, decompilation, tampering or modification of this source code by you, or any of your affiliates (term
to be broadly interpreted) you or your such affiliates shall unconditionally assign and transfer any intellectual property created by any
such non-permitted act to AnyLog, Inc.
'''
import threading

import anylog_node.generic.utils_io as utils_io
import anylog_node.generic.utils_print as utils_print
import anylog_node.generic.utils_timer as utils_timer


# =======================================================================================================================
# Maintains the instructions and info on ho to manage a job. This info is exchanged between threads
# =======================================================================================================================
class JobHandle:

    def __init__(self, j_id):

        self.job_id = j_id  # -1 value is when job handle is not on the job instance (i.e. a job handle on the status object)
        self.event_object = threading.Event()
        self.timer = utils_timer.ProcessTimer(1)  # Monitor job time
        self.select_parsed = None
        self.reset()

    # =======================================================================================================================
    # Maintain a link to the select parsed which is on the job_instance->Job_handle->select_parsed
    # =======================================================================================================================
    def set_select_parsed(self, select_parsed):
        self.select_parsed = select_parsed

    # =======================================================================================================================
    # Same as: job_instance->Job_handle->select_parsed
    # =======================================================================================================================
    def get_select_parsed(self):
        return self.select_parsed

    # =======================================================================================================================
    # Set error returned by operator
    # =======================================================================================================================
    def set_operator_error(self, error_no):
        self.operator_error = error_no

    # =======================================================================================================================
    # Get error returned by operator
    # =======================================================================================================================
    def get_operator_error(self):
        return self.operator_error

    # =======================================================================================================================
    # Set error returned by operator
    # =======================================================================================================================
    def set_operator_error_txt(self, error_text):
        self.operator_error_txt = error_text

    # =======================================================================================================================
    # Get error returned by operator
    # =======================================================================================================================
    def get_operator_error_txt(self):
        return self.operator_error_txt

    # =======================================================================================================================
    # Set processing error
    # =======================================================================================================================
    def set_process_error(self, error_no):
        self.process_error = error_no

    # =======================================================================================================================
    # Get error returned by operator
    # =======================================================================================================================
    def get_process_error(self):
        return self.process_error

    # =======================================================================================================================
    # Set the job id executing this task
    # =======================================================================================================================
    def set_job_id(self, job_id):
        self.job_id = job_id

    # =======================================================================================================================
    # Get the job id executing this task
    # =======================================================================================================================
    def get_job_id(self):
        return self.job_id

    # =======================================================================================================================
    # Save a result set
    # =======================================================================================================================
    def set_result_set(self, result):
        self.result_set = result

    # =======================================================================================================================
    # Get a result set
    # =======================================================================================================================
    def get_result_set(self):
        return self.result_set

    # =======================================================================================================================
    # get the destination of the output - stdout, graph or keep (place on self.result_set)
    # =======================================================================================================================
    def get_output_dest(self):
        if self.conditions:
            if "dest" in self.conditions.keys():
                dest = self.conditions["dest"]
            elif "table" in self.conditions.keys():
                dest = "Table: %s" % self.conditions["table"][0]
            else:
                dest = "stdout"  # The default
        else:
            dest = "stdout"  # The default
        return dest

    # =======================================================================================================================
    # Is the output to stdout
    # =======================================================================================================================
    def is_stdout(self):
        if self.get_output_dest() == "stdout":
            ret_val = True
        else:
            ret_val = False
        return ret_val
    # =======================================================================================================================
    # Is the output to table
    # =======================================================================================================================
    def is_output_to_table(self):
        if self.conditions and "table" in self.conditions.keys():
            ret_val = True
        else:
            ret_val = False
        return ret_val


    # =======================================================================================================================
    # If output to stdout, some calls can indicate not to print to stdout
    # =======================================================================================================================
    def set_print_status(self, status):
        self.print_status = status

    # =======================================================================================================================
    # Print status set to False will suppress stdout output of replied data from peer messages
    # =======================================================================================================================
    def get_print_status(self):
        return self.print_status

    # =======================================================================================================================
    # Set a flag indicating a rest caller -  as the output_dest may change (for example when output is directed to a table)
    # =======================================================================================================================
    def set_rest_caller(self):
        self.rest_caller = True

    # =======================================================================================================================
    # Get a flag indicating a rest caller -  as the output_dest may change (for example when output is directed to a table)
    # =======================================================================================================================
    def is_rest_caller(self):
        return self.rest_caller

    # =======================================================================================================================
    # Copy the SQL Command conditions - Shallow copy
    # =======================================================================================================================
    def set_conditions(self, conditions):
        self.conditions = conditions

    # =======================================================================================================================
    # Set the subset flag - the flag determines if a reply is returned with timeout
    # =======================================================================================================================
    def set_subset(self, subset_flag):
        self.subset = subset_flag

    # =======================================================================================================================
    # return the the subset flag - the flag determines if a reply is returned with timeout
    # =======================================================================================================================
    def is_subset(self):
        return self.subset

    # =======================================================================================================================
    # Set a timeout in seconds for the command execution
    # =======================================================================================================================
    def set_timeout(self, timeout_sec):
        self.timeout_sec = timeout_sec

    # =======================================================================================================================
    # Get a timeout in seconds for the command execution
    # =======================================================================================================================
    def get_timeout(self):
        return self.timeout_sec

    # =======================================================================================================================
    # Set a key value to maintain returned results in the dictionary
    # =======================================================================================================================
    def set_assignment(self, assignment):
        self.assignment = assignment

    # =======================================================================================================================
    # Get the key value that maintains returned results in the dictionary
    # =======================================================================================================================
    def get_assignment(self):
        return self.assignment

    # =======================================================================================================================
    # Return the SQL Command conditions
    # =======================================================================================================================
    def get_conditions(self):
        return self.conditions
    # =======================================================================================================================
    # Copy the SQL Command conditions - DEEP copy
    # =======================================================================================================================
    def copy_cmd_conditions(self, conditions):
        if conditions == None:
            self.conditions = None
        else:
            self.conditions = conditions.copy()

    # =======================================================================================================================
    # Set the ouput socket for a REST call
    # =======================================================================================================================
    def set_output_socket(self, output_sock):
        self.output_socket = output_sock

    # =======================================================================================================================
    # Get the ouput socket for a REST call
    # =======================================================================================================================
    def get_output_socket(self):
        return self.output_socket

    # =======================================================================================================================
    # Maintain the name of the file (like MP4) that is streamed to the user/app browser
    # =======================================================================================================================
    def set_stream_file(self, stream_file):
        self.stream_file = stream_file

    # =======================================================================================================================
    # Get the name of the file (like MP4) that is streamed to the user/app browser
    # =======================================================================================================================
    def get_stream_file(self):
        return self.stream_file

    # =======================================================================================================================
    # Test the REST output socket
    # =======================================================================================================================
    def test_output_socket(self, source):
        ret_val = True
        if self.is_rest_caller():
            soc = self.get_output_socket()
            if not utils_io.test_stream_socket(soc):
                utils_print.output(("\nFailed output socket from source: %u" % source), True)
                ret_val = False
        return ret_val

    # =======================================================================================================================
    # Is process with Job Instance (when the query is send to operator nodes)
    # =======================================================================================================================
    def is_with_job_instance(self):
        if self.job_id == -1:
            return False
        return True

    # =======================================================================================================================
    # Process query with the provided SQL command (or file to exexcute"
    # =======================================================================================================================
    def process_query(self, logical_dbms: str, table_name:str, conditions: dict, command: str):
        self.logical_dbms = logical_dbms
        self.table_name = table_name
        self.conditions = conditions
        self.command = command
    # =======================================================================================================================
    # Is local query on this node
    # =======================================================================================================================
    def is_local_query(self):
        if self.conditions != None and "message" in self.conditions and self.conditions["message"] == False:
            return True
        return False

    # =======================================================================================================================
    # Set the destination of the query result set
    # =======================================================================================================================
    def set_result_set_condition(self, key, value):
        self.conditions[key] = [value]

    # =======================================================================================================================
    # Set the destination of the query result set
    # =======================================================================================================================
    def test_condition_value(self, key, value):
        if self.conditions != None and key in self.conditions:
            cond_value = self.conditions[key]
            if isinstance(cond_value, list):
                ret_val = cond_value[0] == value
            else:
                ret_val = cond_value == value
        else:
            ret_val = False     # No such value

        return ret_val
    # =======================================================================================================================
    # Get query variables - used to execute local query by a rest caller
    # =======================================================================================================================
    def get_local_query_params(self):
        return [self.logical_dbms, self.table_name, self.conditions, self.command]
    # =======================================================================================================================
    # Test if result set is by executing a command
    # =======================================================================================================================
    def is_execute_command(self):
        return self.conditions != None

    # =======================================================================================================================
    # Get command to execute
    # =======================================================================================================================
    def get_command(self):
        return self.command

    # =======================================================================================================================
    # Get the name of the dbms to use
    # =======================================================================================================================
    def get_logical_dbms(self):
        return self.logical_dbms

    # =======================================================================================================================
    # Reset a wait event - the thread will wait on this event until signaled
    # =======================================================================================================================
    def set_wait_event(self):
        self.event_object.clear()  # Reset the internal flag to False

    # =======================================================================================================================
    # Return true if the event was signaled and False if was not signaled ( self.event_object.set() was not called).
    # =======================================================================================================================
    def is_signaled(self):
        return self.event_object.is_set()

    # =======================================================================================================================
    # Signal a wait event
    # =======================================================================================================================
    def signal_wait_event(self):
        self.event_object.set()  # Set the internal flag to True

    # =======================================================================================================================
    # Wait for signal or timeout
    # =======================================================================================================================
    def wait_event(self, timeout: float):
        return self.event_object.wait(timeout)  # block until true or timeout

    # =======================================================================================================================
    # Stop the process timer
    # =======================================================================================================================
    def stop_timer(self):
        self.timer.stop(0)

    # =======================================================================================================================
    # Get Operator time
    # =======================================================================================================================
    def get_operator_time(self):
        return self.timer.get_timer(0)

    # =======================================================================================================================
    # Test if the query summary was printed - used by the REST thread to determine if summary printing is needed
    # =======================================================================================================================
    def is_query_completed(self):
        return self.query_completed
    # =======================================================================================================================
    # Flag that the query summary was printed - used by the REST thread to determine if summary printing is needed
    # =======================================================================================================================
    def set_query_completed(self):
        self.query_completed = True

    # =======================================================================================================================
    # Reset output info
    # =======================================================================================================================
    def reset(self):
        self.result_set = ""  # Used to transfer a result set between the thread that issued the command and the thread that got the resulr
        self.print_status = True  # A False value is set to suppress prints to stdout of replied data from peers
        self.logical_dbms = ""
        self.table_name = ""
        self.conditions = None  # the command conditions (like type of output)
        self.command = ""  # a command for the thread to execute. For example, SQL stmt
        self.operator_error = 0  # error returned from operator node
        self.operator_error_txt = ""  # text returned by the operator
        self.process_error = 0  # error on the processing node
        self.output_socket = None  # output socket for a REST call
        self.stream_file = None     # the name of the file that is streamed to a browser or an app in a REST call
        self.rest_caller = False
        self.results_set = []    # An array with query result

        self.timer.start(0)  # monitor the process time
        self.subset = False  # True determines if partial results are allowed
        self.timeout_sec = 0 # A param to set the max time for the command execution
        self.assignment = None  # A key name to contain the message results

        self.query_completed = False    # set to True when query summary is pronted
