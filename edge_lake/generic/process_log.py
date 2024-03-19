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
import time

import anylog_node.generic.utils_print as utils_print
import anylog_node.generic.utils_json as utils_json

EVENTS = 0
ERRORS = 1
FILES = 2
QUERY = 3
STREAMING = 4

log_types = {"event": EVENTS, "error": ERRORS, "file": FILES, "query": QUERY, "streaming" : STREAMING}

log_mutex = threading.Lock()  # variable on the class level are static
list_size = [100, 20, 20, 20, 20]  # list_size[0] - event log, list_size[1] - eror log, list_size[2] - file log
index_entries = [-1, -1, -1, -1, -1]
counter_logs = len(list_size)
events = [[[None for _ in range(list_size[x])] for _ in range(6)] for x in range(counter_logs)]  # log list
id = 0
secondary_id = [0, 0, 0, 0, 0]


# =======================================================================================================================
# Add event to log
# =======================================================================================================================
def add(info_type, info):
    global index_entries
    global id

    if (not isinstance(info_type, str) or not isinstance(info,
                                                         str)):  # if info_type or info ae not strings, error is changed to identify the poblem
        info_type = "Error"
        info = "Non string object is added to log"

    the_time = time.ctime()
    thread_name = threading.current_thread().name

    log_mutex.acquire()

    if info == events[EVENTS][5][index_entries[EVENTS]] and info_type == events[EVENTS][4][
        index_entries[EVENTS]] and thread_name == events[EVENTS][2][index_entries[EVENTS]]:
        # same error or warning by the same thread
        events[EVENTS][1][index_entries[EVENTS]] += 1  # increase counter of occurences
        events[EVENTS][3][index_entries[EVENTS]] = the_time  # set last time
    else:
        # set new log entry

        id = id + 1
        index_entries[EVENTS] = index_entries[EVENTS] + 1
        if index_entries[EVENTS] >= list_size[EVENTS]:
            index_entries[EVENTS] = 0

        add_info(events[EVENTS], index_entries[EVENTS], id, thread_name, the_time, info_type, info)

        log_id = get_log_id(info_type.lower())

        if log_id >= 0:
            # add to secondary log
            add_to_one_log(log_id, id, thread_name, the_time, info_type, info)

    log_mutex.release()


# =======================================================================================================================
# Add event to a secondary log
# =======================================================================================================================
def secondary_log(info_type, info):
    global secondary_id

    the_time = time.ctime()
    thread_name = threading.current_thread().name
    log_id = get_log_id(info_type.lower())

    log_mutex.acquire()

    if log_id >= 0:
        secondary_id[log_id] += 1
        # add to secondary log
        add_to_one_log(log_id, secondary_id[log_id], thread_name, the_time, info_type, info)

    log_mutex.release()


# =======================================================================================================================
# Add event/error to secondary log
# =======================================================================================================================
def add_to_one_log(log_id, id, thread_name, the_time, info_type, info):
    if info == events[log_id][5][index_entries[log_id]] and info_type == events[log_id][4][
        index_entries[log_id]] and thread_name == events[log_id][2][index_entries[log_id]]:
        # same error or warning by the same thread
        events[log_id][1][index_entries[log_id]] += 1  # increase counter of occurences
        events[log_id][3][index_entries[log_id]] = the_time  # set last time
    else:
        index_entries[log_id] = index_entries[log_id] + 1
        if index_entries[log_id] >= list_size[log_id]:
            index_entries[log_id] = 0

        add_info(events[log_id], index_entries[log_id], id, thread_name, the_time, info_type, info)


# =======================================================================================================================
# Add event/error to info table. The table could be "event log or "error log"
# =======================================================================================================================
def add_info(info_struct, index, id, thread_name, the_time, info_type, info):
    info_struct[0][index] = id  # counter

    info_struct[1][index] = 1  # occurences of the same event by the same thread

    info_struct[2][index] = thread_name  # thread name (used as unique id)

    info_struct[3][index] = the_time

    info_struct[4][index] = info_type  # command / Error / Warning

    info_struct[5][index] = info


# =======================================================================================================================
# Add event to log
# =======================================================================================================================
def add_and_print(info_type, info):
    add(info_type, info)
    utils_print.output(info_type + ":   " + info, True)


# =======================================================================================================================
# Loop in the log and print events - oldest one first
# If search_string is not NULL, find the substring in the message
# =======================================================================================================================
def show_events(log_name, cmd_words, out_format, offset_keys):
    '''
    log_name - options are: event, error, query, file streaming
    format - table or json
    offset_keys - keywods that are searched on the logged events )to filter the output)
    '''
    log_id = get_log_id(log_name)
    if log_id == -1:
        return False, ""

    len_cmd_words = len(cmd_words)
    log_mutex.acquire()

    info_struct = events[log_id]
    offset = index_entries[log_id]
    entries = list_size[log_id]

    out_list = []

    if offset > -1:
        for num in range(0, entries):
            offset = offset + 1
            if offset >= entries:
                offset = 0
            if info_struct[0][offset] is not None:
                if len_cmd_words == offset_keys:
                    print_flag = True  # print the entire event log
                else:  # test if one of the keywords in the event
                    print_flag = False
                    for key in cmd_words[offset_keys:]:
                        search_key = key.lower()
                        if info_struct[4][offset].lower().find(search_key) != -1:      # Search in event type
                            print_flag = True  # one keyword was found
                            break
                        if info_struct[5][offset].lower().find(search_key) != -1:   # Search in message text
                            print_flag = True  # one keyword was found
                            break

                if print_flag:
                    if out_format == "json":
                        out_list.append({
                            "ID" : info_struct[0][offset],
                            "Count": info_struct[1][offset],
                            "Thread": info_struct[2][offset],
                            "Time": info_struct[3][offset],
                            "Type": info_struct[4][offset],
                            "Text": info_struct[5][offset],
                        })
                    else:
                        out_list.append((info_struct[0][offset], info_struct[1][offset], info_struct[2][offset], info_struct[3][offset], info_struct[4][offset], info_struct[5][offset]))

    log_mutex.release()

    if out_format == "json":
        out_message = utils_json.to_string(out_list)
    else:
        out_message =  utils_print.output_nested_lists(out_list, "", ["ID", "Count", "Thread", "Time", "Type", "Text"], True, "")

    return True, out_message


# =======================================================================================================================
# Loop in the log and reset the events
# =======================================================================================================================
def reset_events(log_name):
    log_id = get_log_id(log_name)
    if log_id == -1:
        return False

    log_mutex.acquire()

    info_struct = events[log_id]
    entries = list_size[log_id]

    for num in range(0, entries):
        if info_struct[0][num] is not None:
            info_struct[0][num] = None
            info_struct[1][num] = None
            info_struct[2][num] = None
            info_struct[3][num] = None
            info_struct[4][num] = None
            info_struct[5][num] = None

    index_entries[log_id] = -1  # start at first location

    log_mutex.release()

    return True


# =======================================================================================================================
# Get the log ID by name - return -1 if not in dictionaru
# =======================================================================================================================
def get_log_id(log_name):
    log_id = -1
    if log_name in log_types:
        log_id = log_types[log_name]
    return log_id
