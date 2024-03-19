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

# ---------------------------------------------------------------------------------------------------
# Collect and Monitor on data ingested in the node.
# Data remains in a memory structure that collects and stores N intervals of data
# ---------------------------------------------------------------------------------------------------
# examples:
import copy
import time

''' 
data monitor where dbms = dmci and table = sensor_table and intervals = 10 and time = 1 minute and time_column = timestamp and value_column = value
data monitor where dbms = dmci and table = sensor_table and remove = true
set alert where dbms = dmci and table = sensor_table and min < 10 
set alert where dbms = dmci and table = sensor_table and avg > 100
'''

import anylog_node.generic.process_status as process_status
import anylog_node.generic.interpreter as interpreter
import anylog_node.generic.utils_json as utils_json
import anylog_node.generic.utils_data as utils_data
import anylog_node.generic.utils_print as utils_print

# dbms --> table --> Intervals List (Interval ID, Min, Max, Sum, Count)
#                --> Time       # Start time
#                --> Start_Time
#                --> Counter Intervals
#                --> Current Interval
#                --> time_column
#                --> value_column
#                --> Alerts  (Min, Max, Avg, Count)

monitored_data_ = {}

# ---------------------------------------------------------------------------------------------------
# Setup of the data monitoring
# data monitor where dbms = dmci and intervals = 8 and time = 1 minute and time_column = timestamp and value_column = value
# ---------------------------------------------------------------------------------------------------
def data_monitoring(status, io_buff_in, cmd_words, trace):

    global monitored_data_
    #                          Must     Add      Is
    #                          exists   Counter  Unique

    if len (cmd_words) < 10 or cmd_words[2] != "where":
        return process_status.ERR_command_struct

    keywords = {"dbms": ("str", True, False, True),
                "table": ("str", False, False, True),
                "intervals": ("int", False, False, True),    # The number of time intervals to keep - default is 10
                "time": ("int.time", False, False, True),     # the length of the intervals - default is 10
                "time_column": ("str", False, False, True),   # default is timestamp
                "value_column": ("str", False, False, True),  # Default is value
                "remove": ("bool", False, False, True),      # remove from structure
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3, 0, keywords, False)
    if ret_val:
        return ret_val

    dbms_name = interpreter.get_one_value(conditions, "dbms")
    table_name =  interpreter.get_one_value(conditions, "table")
    is_remove = interpreter.get_one_value_or_default(conditions, "remove", False)
    if is_remove:
        # delete the info and configurations from the structure
        return remove_info(dbms_name, table_name)

    if not table_name:
        table_name = '*'        # new tables will be created dynamically as data streams in and maintain these configurations

    monitored_info = {}    # A single table info

    intervals = interpreter.get_one_value_or_default(conditions, "intervals", 10)
    if intervals > 100:
        status.add_error("Intervals value can not exceed 100")
        ret_val = process_status.ERR_command_struct
    else:
        if not intervals:
            intervals = 1       # At least 1
        interval_time = interpreter.get_one_value_or_default(conditions, "time", 60)
        time_column = interpreter.get_one_value_or_default(conditions, "time_column", "timestamp")
        value_column = interpreter.get_one_value_or_default(conditions, "value_column", "value")

        intervals_list = []
        for _ in range(intervals):
            interval_info = {}
            reset_interval(interval_info)
            intervals_list.append(interval_info)

        monitored_info["intervals"] = intervals_list
        monitored_info["start_time"] = int(time.time())
        monitored_info["current"] = 0       # Current interval
        monitored_info["counter"] = intervals
        monitored_info["interval_time"] = interval_time
        monitored_info["time_column"] = time_column
        monitored_info["value_column"] = value_column
        monitored_info["thresholds"] = {}

        if not dbms_name in monitored_data_:
            monitored_data_[dbms_name] = {}

        tables_info = monitored_data_[dbms_name]

        tables_info[table_name] = monitored_info        # Update new setup

    return ret_val

# ---------------------------------------------------------------------------------------------------
# Remove the info assigned to the database + table.
# If table name is not provided, all the tables are removed
# ---------------------------------------------------------------------------------------------------
def  remove_info(dbms_name, table_name):
    global monitored_data_
    ret_val = process_status.SUCCESS

    if not dbms_name in monitored_data_:
        ret_val = process_status.ERR_dbms_name
    elif not table_name:
        # Delete all tables
        try:
            del monitored_data_[dbms_name]
        except:
            pass
    elif not table_name in process_status[dbms_name]:
        ret_val  = process_status.ERR_table_name
    else:
        try:
            del monitored_data_[dbms_name][table_name]
        except:
            pass

    return ret_val

# ---------------------------------------------------------------------------------------------------
# Get the info on the monitored data
# Examples:
#  get data monitored
#  get data monitored where format = json
#  get data monitored where dbms = dmci
# get data monitored where dbms = dmci and table = sensor_table
# ---------------------------------------------------------------------------------------------------
def get_info(status, io_buff_in, cmd_words, trace):

    global monitored_data_

    ret_val = process_status.SUCCESS
    format = "table"
    dbms_name = None
    table_name = None
    info_structure = copy.deepcopy(monitored_data_) # Because data is updated as structure is evaluated

    words_count = len(cmd_words)
    if words_count >= 7:

        if cmd_words[3] != "where":
            status.add_error("Missing 'where' keyword in 'get data monitored' command")
            ret_val = process_status.ERR_command_struct
        else:
            keywords = {"dbms": ("str", False, False, True),
                        "table": ("str", False, False, True),
                        "format": ("str", False, False, True),
                        }

            ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
            if not ret_val:

                dbms_name = interpreter.get_one_value(conditions, "dbms")
                table_name = interpreter.get_one_value(conditions, "table")

                format = interpreter.get_one_value_or_default(conditions, "format", "table")

                if dbms_name:
                    try:
                        info_structure = monitored_data_[dbms_name]
                    except:
                        ret_val = process_status.No_monitored_info
                    else:
                        if table_name:
                            try:
                                info_structure = info_structure[table_name]
                            except:
                                ret_val = process_status.No_monitored_info
                elif table_name:
                    # With table but without dbms
                    ret_val = process_status.No_monitored_info
    elif words_count != 3:
        ret_val = process_status.ERR_command_struct

    info_str = ""
    if not ret_val:
        if not len(info_structure):
            ret_val = process_status.No_monitored_info
        else:

            if format == "json":

                try:
                    info_str = utils_json.to_string(info_structure)
                except:
                    info_str = ""

            else:
                # organize table with info
                table_info = []
                if not dbms_name and not table_name:
                    # get all
                    for dbms in info_structure:
                        for table in info_structure[dbms]:
                            if table == '*':
                                continue
                            add_table_info(table_info, dbms, table, info_structure[dbms][table]) # sort table info and add to table
                elif dbms_name:
                    if not table_name:
                        # All tables of the dbms
                        for table in info_structure:
                            if table == '*':
                                continue
                            add_table_info(table_info, dbms_name, table, info_structure[table])  # sort table info and add to table
                    else:
                        add_table_info(table_info, dbms_name, table_name, info_structure)  # sort table info and add to table
                else:
                    ret_val = process_status.No_monitored_info

                if len(table_info):
                    info_str = utils_print.output_nested_lists(table_info, None, ["DBMS", "Table", "H:M:S", "Events/sec", "Count", "Min", "Max", "Avg" ], True, "")
                else:
                    ret_val = process_status.No_monitored_info

    return [ret_val, info_str, "json"]

# ---------------------------------------------------------------------------------------------------
# Organize the table info to a structure ordered by time
# ---------------------------------------------------------------------------------------------------
def add_table_info(table_info, dbms_name, table_name, info_struct):


    intervals = info_struct["intervals"]
    # sort the list by id
    intervals.sort(key=get_interval_id)


    # Get the max _id
    entries_count = info_struct["counter"]
    max_id = intervals[entries_count - 1]["id"]
    interval_time = info_struct["interval_time"]


    # Calculate the time elapsed since last reading
    current_interval_id = get_current_interval_id(info_struct)


    for entry in intervals:
        interval_id = get_interval_id(entry)  # The time elapsed from start
        if interval_id == -1:
            continue        # Not set with data

        hours_time, minutes_time, seconds_time = utils_data.seconds_to_hms((current_interval_id - interval_id) * interval_time)
        elapsed_time = "%u:%u:%u" % (hours_time, minutes_time, seconds_time)
        min = '{:0,.2f}'.format(entry["min"])
        max = '{:0,.2f}'.format(entry["max"])

        count = entry["count"]
        sum = entry["sum"]

        if count:
            avg = '{:0,.2f}'.format(sum/count)
        else:
            avg = 0

        if interval_time:
            events_sec = '{:0,.2f}'.format(count/interval_time)
        else:
            events_sec = 0


        count = format(count, ",")


        table_info.append((dbms_name, table_name, elapsed_time, events_sec, count, min, max, avg))

# ---------------------------------------------------------------------------------------------------
# retrieve the id from the list
# ---------------------------------------------------------------------------------------------------
def get_interval_id(interval_entry):
    return interval_entry["id"]

# ---------------------------------------------------------------------------------------------------
# Determine if the table needs to be  monitored - if an entry does not exist - set an entry to the table
# ---------------------------------------------------------------------------------------------------
def set_monitoring(dbms_name, table_name):

    global monitored_data_

    try:
        if not dbms_name in monitored_data_:
            ret_val = False
        elif table_name in monitored_data_[dbms_name]:
            ret_val = True      # Structure for table Exists
        else:
            if '*' in monitored_data_[dbms_name]:
                # all tables of dbms are monitored
                table_def = copy.deepcopy(monitored_data_[dbms_name]['*'])
                monitored_data_[dbms_name][table_name] = table_def
                ret_val = True
            else:
                ret_val = False
    except:
        ret_val = False

    return ret_val

# ---------------------------------------------------------------------------------------------------
# Place values from list in structure
# Example: data monitor where dbms = dmci and intervals = 8 and time = 1 minute and time_column = timestamp and value_column = value
# ---------------------------------------------------------------------------------------------------
def process_monitored_data(dbms_name, table_name, columns_info, values_list):

    try:
        monitored_info = monitored_data_[dbms_name][table_name] # get the info on the table
    except:
        pass
    else:

        interval_id = get_current_interval_id(monitored_info)

        current_slot = monitored_info["current"]
        if monitored_info["intervals"][current_slot]["id"] == interval_id:
            interval_info = monitored_info["intervals"][current_slot]
            new_interval = False
        else:
            # Move to the next interval and reset the values
            current_slot += 1
            if current_slot >= monitored_info["counter"]:
                current_slot = 0
            monitored_info["current"] = current_slot
            interval_info = monitored_info["intervals"][current_slot]
            reset_interval(interval_info)
            new_interval = True

        value_column = monitored_info["value_column"]   # The name of the value column
        column_id = -1
        for index, col_info in enumerate(columns_info[4:]):
            # Find the info on the value column
            if col_info[1] == value_column:
                column_id = index + 4       # The first 4 fields are system fields
                break

        if column_id >= 0:
            for entry in values_list:
                value = entry[column_id]
                number = utils_data.get_number_value(value)
                if number != None:
                    if new_interval:
                        interval_info["id"] = interval_id
                        interval_info["min"] = number
                        interval_info["max"] = number
                        interval_info["sum"] = number
                        interval_info["count"] = 1
                    else:
                        if number < interval_info["min"]:
                            interval_info["min"] = number
                        if number > interval_info["max"]:
                            interval_info["max"] = number
                        interval_info["sum"] += number
                        interval_info["count"] += 1
                    new_interval = False            # All data in this list updates the same interval

# ---------------------------------------------------------------------------------------------------
# Get the current Interval ID - the number of intervals between current time and when monitoring started
# The difference between current time and the time when monitoring started, divided by the interval length.
# ---------------------------------------------------------------------------------------------------
def get_current_interval_id(monitored_info):
    current_time = int(time.time())
    start_time = monitored_info["start_time"]  # The time when monitored started
    interval_time = monitored_info["interval_time"]  # Length of interval in seconds

    interval_id = int((current_time - start_time) / interval_time)

    return interval_id
# ---------------------------------------------------------------------------------------------------
# Reset the dictionary that maintains the status
# ---------------------------------------------------------------------------------------------------
def reset_interval(interval_info):
    interval_info["id"] = -1     # some number so it will be reset
    interval_info["min"] = 0
    interval_info["max"] = 0
    interval_info["sum"] = 0
    interval_info["count"] = 0

# ---------------------------------------------------------------------------------------------------
# Setting threshold on the monitored data
# set threshold where dbms = dmci and table = sensor_table and min = 5
# ---------------------------------------------------------------------------------------------------
def set_threshold(status, io_buff_in, cmd_words, trace):

    #                          Must     Add      Is
    #                          exists   Counter  Unique

    keywords = {"dbms": ("str", True, False, True),
                "table": ("str", True, False, True),
                "min": ("float", False, True, True),
                "max": ("float", False, True, True),
                "count": ("int", False, True, True),
                "avg": ("float", False, True, True),

                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3, 0, keywords, False)
    if not ret_val:
        if cmd_words[2] != 'where':
            status.add_error("Missing 'where' keyword in 'set threshold' command")
            ret_val = process_status.ERR_command_struct
        elif not counter:
            status.add_error("Missing threshold function in 'set threshold' command")
            ret_val = process_status.ERR_command_struct
        else:
            dbms_name = interpreter.get_one_value(conditions, "dbms")
            table_name = interpreter.get_one_value(conditions, "table")

            # Take the min, max, count, avg and add to the monitored info as f(table)
            for function in conditions:
                if function == "min" or function == "max" or function == "count" or function == "avg":
                    value = interpreter.get_one_value(conditions, function)
                    if value != None:
                        ret_val = add_threshold(status, dbms_name, table_name, function, value)
                        if ret_val:
                            break

    return ret_val
# ---------------------------------------------------------------------------------------------------
# Add Threshold on monitored table
# ---------------------------------------------------------------------------------------------------
def add_threshold(status, dbms_name, table_name, function, value):

    global monitored_data_

    try:
        monitored_data_[dbms_name][table_name]["thresholds"][function] = value
    except:
        status.add_error("Table %s.%s is not monitored" % (dbms_name, table_name))
        ret_val = process_status.No_monitored_info
    else:
        ret_val = process_status.SUCCESS
    return ret_val


