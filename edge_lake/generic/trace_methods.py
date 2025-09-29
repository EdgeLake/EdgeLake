"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.utils_threads as utils_threads
import edge_lake.generic.interpreter as interpreter


process_logging_ = {}       # method --> thread name --> info

supported_trace_ = {
    "blockchain insert" : 0,
    "blockchain push" : 0,
    "blockchain merge" : 0,     # Blockchain sync process
    "blockchain sync" : 0,
    "policy write" : 0,     # Write new policy
    "exec script" : 0,
    "tcp out" : 0,
    "tcp in" : 0,
}
# =======================================================================================================================
# Determine if trace is enabled
# =======================================================================================================================
def is_traced(process_name):
    global supported_trace_
    return supported_trace_[process_name] == 1  # Is set to 1 (ON)


# =======================================================================================================================
# Set debug on a specific methon or command
# Example: trace method on blockchain insert
# trace method on/off tcp out
# =======================================================================================================================
def _set_trace_method(status, io_buff_in, cmd_words, trace):

    global process_logging_
    global supported_trace_

    on_off = cmd_words[2]
    if on_off != "on" and on_off != "off":
        status.add_error("Missing 'on' or 'off' in 'trace method' command")
        ret_val = process_status.ERR_command_struct
    else:

        process_name = ' '.join(cmd_words[3:])
        if process_name in supported_trace_:
            if on_off == "on":
                supported_trace_[process_name] = 1      # Set ON
            else:
                supported_trace_[process_name] = 0      # Set Off
            ret_val = process_status.SUCCESS
        else:
            status.add_error("Wrong method name in 'trace method' command")
            ret_val = process_status.ERR_command_struct


    return ret_val

# =======================================================================================================================
# Count method usage
# =======================================================================================================================
def count_method(process_name, errid):
    '''
    process_name - name of process to debug
    errid - the ret_val
    is_print - print status after the update
    '''
    global supported_trace_

    if supported_trace_[process_name] == 1:
        thread_info = get_thread_info(process_name)

        thread_info["counter"] += 1
        if errid:
            thread_info["errid"] = errid       # Keep last error ID
        else:
            thread_info["success"] += 1

# =======================================================================================================================
# Get the traced info including the statistics and the details
# Command: get trace info where process = "tcp in" and details = true
# =======================================================================================================================
def _get_trace_info(status, io_buff_in, cmd_words, trace):
    reply = ""
    keywords = {
        #                 Must     Add      Is
        #                 exists   Counter  Unique

        "process": ("str", True, False, True),
        "thread": ("str", False, False, True),
        "details" :("bool", False, False, True),
    }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        return [ret_val, None]
    process_name = conditions["process"][0]
    thread_key = interpreter.get_one_value_or_default(conditions, "thread", None) # Can be a thread name or a prefix of the name

    is_details = interpreter.get_one_value_or_default(conditions, "details", False) # Can be a thread name or a prefix of the name


    if not process_name in process_logging_:
        if supported_trace_[process_name] == 1:
            ret_val = process_status.No_trace_data
        else:
            ret_val = process_status.Wrong_process_name
        reply = None
    else:
        threads_info = process_logging_[process_name]

        reply_table = []

        for thread_name, trace_info in threads_info.items():

            if not is_details:
                counter = trace_info["counter"]
                success = trace_info["success"]
                errid = trace_info["errid"]
                err_msg = process_status.get_status_text(errid) if errid else ""

                reply_table.append((thread_name, counter, success, counter- success, errid, err_msg))

            else:
                thread_details = trace_info["details"]
                utils_print.format_dictionary(thread_details, True, True, False, ["Step", f"Trace of : '{process_name}' - '{thread_name}'"], "green")


    if not ret_val:
        if not is_details:
            reply = utils_print.output_nested_lists(reply_table, None,
                                                ["Thread", "Counter", "Success", "Failure", "Last Err", "Err message"],
                                                True)

    return [ret_val, reply]

# =======================================================================================================================
# Collect Detailed Info
# =======================================================================================================================
def add_details(process_name, **kwargs):
    global supported_trace_

    if supported_trace_[process_name] == 1:
        thread_info = get_thread_info(process_name)
        logging_details = thread_info["details"]
        for key, value in kwargs.items():
            if key in logging_details:
                # Reset - new process
                logging_details = {}
                thread_info["details"] = logging_details    # Start a new details dict

            logging_details[key] = value
# =======================================================================================================================
# Update Existing Detailed Info
# =======================================================================================================================
def update_details(process_name, **kwargs):
    global supported_trace_

    if supported_trace_[process_name] == 1:
        thread_info = get_thread_info(process_name)
        logging_details = thread_info["details"]
        for key, value in kwargs.items():
            logging_details[key] = value
# =======================================================================================================================
# Get the trace info of a particular thread
# =======================================================================================================================
def get_thread_info(process_name):
    thread_name = utils_threads.get_thread_name()
    if not process_name in process_logging_:
        process_logging_[process_name] = {}


    if thread_name in process_logging_[process_name]:
        thread_info = process_logging_[process_name][thread_name]
    else:
        # Start new info
        thread_info = {
            "counter": 0,
            "success": 0,
            "errid": 0,  # last error id
            "details": {
                "Thread": thread_name,
            },  # Additional info
        }
        process_logging_[process_name][thread_name] = thread_info

    return thread_info

# =======================================================================================================================
# Print the collected Info
# =======================================================================================================================
def print_details(process_name, color):
    '''
    process_name - name of process to debug
    is_reset - if True reset process details
    '''

    global supported_trace_
    if supported_trace_[process_name] == 1:
        thread_info = get_thread_info(process_name)
        thread_details = thread_info["details"]
        utils_print.format_dictionary(thread_details, True, True, False, ["Step", f"Trace method of : '{process_name}'"], color)

def print_string(process_name, string_info):
    global supported_trace_
    if supported_trace_[process_name] == 1:
        thread_name = utils_threads.get_thread_name()
        utils_print.output(f"\r\n[{process_name}] : [{thread_name}] : [{string_info}]", False)

