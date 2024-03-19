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

# Maintain statistics on different modules

import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_columns as utils_columns
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.utils_data as utils_data

from edge_lake.generic.node_info import get_node_name
'''
Commands Suppoerted

Used on the remote_cli monitor form:
get stats where service = operator and topic = summary  and format = json (replacing  get operator stat format = json)

get stats where service = streaming and service = operator and join = table  and format = json  
'''

# ---------------------------------------------------------------------------------------------
# Before stats on operator errors are provided
# Update values in the statistics relating to the operator errror code
# ---------------------------------------------------------------------------------------------
def prep_operator_error(instructions):

    error_list = statistics_["operator"]["error"]
    for error_entry in error_list:
        if error_entry["last error"]:
            error_entry["last error text"] = process_status.get_status_text(error_entry["last error"])
# ---------------------------------------------------------------------------------------------
# Set elapsed time in nested dictionary
# ---------------------------------------------------------------------------------------------
def set_elapsed_time(instructions):

    service_name, topic_name, operation, operand1_dict, operand1, offset_timestamp, offset_elapsed = instructions

    topic_dict = statistics_[service_name][topic_name]
    current_time = utils_columns.get_current_time_in_sec()

    # Go over the dbms
    for table_dict in topic_dict.values():
        # Go over the Tables
        for table_info in table_dict.values():
            # Update elapsed time between now and when the table was updated
            if isinstance(table_info, list):
                previous_time = table_info[offset_timestamp]
                hours_time, minutes_time, seconds_time = utils_data.seconds_to_hms(current_time - previous_time)
                time_diff = "%02u:%02u:%02u" % (hours_time, minutes_time, seconds_time)

                table_info[offset_elapsed] = time_diff




statistics_ = {
    "operator" : {

        "prep_info" : {

            "total rows": 0,  # Since started
            "total errors": 0,  # Since started
            "operator timestamp": 0,  # The timestamp when the operator process started
            "last_file_time"    : 0,   # the timestamp of the last file that was processed

        },

        "prep_config": (

            ("config", "operational time", 'T', "prep_info", "operator timestamp", None, None), # Update operational time
        ),

        "config": {
            "status": "Not Active",
            "operational time": 0,
        },


        "prep_summary" :    (
                                # Dictionary Key to update       Operation    Dictionary   operand 1        Dictionary  2perand 2
                                ("summary", "node name",           'M',        None,       get_node_name,   None,       None),             # Update the node name using a method
                                ("summary", "elapsed time",        'T',        "summary",  "query timestamp",   None,       None ),      # Time from last call for stat
                                ("summary", "query timestamp",     'M',        None,       utils_columns.get_current_time_in_sec,   None,       None),  # Update current timestamp
                                ("summary", "operational time",    'T',        "summary",  "start timestamp",   None,       None),  # Time from last call for stat
                                ("summary", "new rows",          'D',        "prep_info","total rows",   "summary",  "total rows"),  # new rows
                                ("summary", "total rows",        'E',        "prep_info","total rows",   None, None),  # Move value from prep to summary
                                ("summary", "new errors",          'D',      "prep_info","total errors",   "summary",  "total errors"),
                                ("summary", "total errors",        'E',      "prep_info", "total errors", None, None),
                            ),



        "summary" : {
            "node name": "",
            "status": "Not Active",
            "start timestamp": 0,  # The date and time operator started
            "operational time": "",  # HH:MM:SS of operations
            "query timestamp" : 0,     # Query timestamp
            "elapsed time": "",  # Time from last call for stat
            "new rows": 0,  # Since last call
            "total rows": 0,  # Since started
            "new errors": 0,  # Since last call
            "total errors": 0,  # Since started

        },

        "prep_error" :    (
            # Dictionary Key to update       Operation    Dictionary   operand 1        Dictionary  2perand 2
            (None,      None, 'P', None, prep_operator_error, None, None),   # Apply a process
            ("error", "elapsed time", 'T', "error", "timestamp", None, None),      # Time since the error happened
        ),
        "error" : [
            {
                "type" : "JSON Errors",
                "counter" : 0,
                "timestamp" : 0,
                "elapsed time": "",     # Time since the error
                "dbms name" : "",
                "table name" : "",
                "last error" : 0,
                "last error text" : "",
            },
            {
                "type" : "SQL Errors",
                "counter" : 0,
                "timestamp" : 0,
                "elapsed time": "",     # Time since the error
                "dbms name" : "",
                "table name" : "",
                "last error" : 0,
                "last error text" : "",
            },
            ],

        "attr_name_sql" : ["files", "immediate", "timestamp", "elapsed_time"],
        "attr_name_json" : ["files", "immediate", "timestamp", "elapsed_time"],
        "attr_name_inserts" : ["first timestamp", "last timestamp", "first insert", "last insert", "Batch inserts", "Immediate inserts"],

        "prep_sql": (
            # Dictionary Key to update       Operation    Dictionary   operand 1        Dictionary  2perand 2
            ("operator", "sql", 'P', None, set_elapsed_time, 2, 3),   # Time since the last successful process
        ),
        "prep_json": (
            # Dictionary Key to update       Operation    Dictionary   operand 1        Dictionary  2perand 2
            ("operator", "json", 'P', None, set_elapsed_time, 2, 3),  # Time since the last successful process
        ),

        "prep_inserts": (
            # Dictionary Key to update       Operation    Dictionary   operand 1        Dictionary  2perand 2
            ("operator", "inserts", 'P', None, set_elapsed_time, 1, 3),  # Time since the last successful insert
            ("operator", "inserts", 'P', None, set_elapsed_time, 0, 2),  # Time since the first successful insert
        ),

        "sql" : {},
        "json" : {},
        "inserts" : {},
    },
    # -----------------------------------------------------------------------------------------------------
    "publisher": {


        "prep_info": {

            "total files": 0,  # Since started
            "total errors": 0,  # Since started
            "publisher timestamp": 0,  # The timestamp when the operator process started
            "last_file_time": 0,  # the timestamp of the last file that was processed

        },

        "prep_config": (

            ("config", "operational time", 'T', "prep_info", "publisher timestamp", None, None),
        # Update operational time
        ),

        "prep_summary": (
            # Dictionary Key to update       Operation    Dictionary   operand 1        Dictionary  2perand 2
            ("summary", "node name", 'M', None, get_node_name, None, None),  # Update the node name using a method
            ("summary", "elapsed time", 'T', "summary", "query timestamp", None, None),  # Time from last call for stat
            ("summary", "query timestamp", 'M', None, utils_columns.get_current_time_in_sec, None, None),
            # Update current timestamp
            ("summary", "operational time", 'T', "summary", "start timestamp", None, None),
            # Time from last call for stat
            ("summary", "new files", 'D', "prep_info", "total files", "summary", "total files"),  # new rows
            ("summary", "total files", 'E', "prep_info", "total files", None, None),  # Move value from prep to summary
            ("summary", "new errors", 'D', "prep_info", "total errors", "summary", "total errors"),
            ("summary", "total errors", 'E', "prep_info", "total errors", None, None),
        ),

        "summary": {
            "node name": "",
            "status": "Not Active",
            "start timestamp": 0,  # The date and time operator started
            "operational time": "",  # HH:MM:SS of operations
            "query timestamp": 0,  # Query timestamp
            "elapsed time": "",  # Time from last call for stat
            "new files": 0,  # Since last call
            "total files": 0,  # Since started
            "new errors": 0,  # Since last call
            "total errors": 0,  # Since started

        },

        "config": {
            "status": "Not Active",
            "operational time": 0,
        },


        "files" : {},

    }


}

# ---------------------------------------------------------------------------------------------
# Copy values to the dictionary
# List_values is organized as a list of key-value pairs
# ---------------------------------------------------------------------------------------------
def set_values(service_name, topic_name, list_values):
    target_dict = statistics_[service_name][topic_name] # the dictionary to update
    for entry in list_values:
        target_dict[entry[0]] = entry[1]

# ---------------------------------------------------------------------------------------------
# Add one value to an entry in the dictionary
# ---------------------------------------------------------------------------------------------
def add_one_value(service_name, topic_name, key, value):
    statistics_[service_name][topic_name][key] += value # the dictionary updated by the value
# ---------------------------------------------------------------------------------------------
# Update one value
# ---------------------------------------------------------------------------------------------
def update_one_value(service_name, topic_name, key, value):
    statistics_[service_name][topic_name][key] = value # the dictionary updated by the value

# ---------------------------------------------------------------------------------------------
# Get table Entry  - get the dictionary location for the update
# ---------------------------------------------------------------------------------------------
def get_table_entry(service_name, topic_name, dbms_name, table_name):


    try:
        table_info = statistics_[service_name][topic_name][dbms_name][table_name]
    except:
       table_info = None

    return table_info

# ---------------------------------------------------------------------------------------------
# New table Entry  - Create new table entry
# ---------------------------------------------------------------------------------------------
def new_table_entry(service_name, topic_name, dbms_name, table_name, table_struct):
    # table_struct is the info to maintain as f(table) - a dictionary or a list with values
    if not dbms_name in statistics_[service_name][topic_name]:
        statistics_[service_name][topic_name][dbms_name] = {}
    if not table_name in statistics_[service_name][topic_name][dbms_name]:
        statistics_[service_name][topic_name][dbms_name][table_name] = table_struct
        table_info = table_struct
    else:
        table_info = statistics_[service_name][topic_name][dbms_name][table_name]

    return table_info
# ---------------------------------------------------------------------------------------------
# Test if a table entry exosts for the given service + topic
# ---------------------------------------------------------------------------------------------
def is_with_table(service_name, topic_name, dbms_name, table_name):
    try:
        value = statistics_[service_name][topic_name][dbms_name][table_name]
    except:
        ret_val = False
    else:
        ret_val = True
    return ret_val

# ---------------------------------------------------------------------------------------------
# Update Statistics entry for a table
# ---------------------------------------------------------------------------------------------
def set_table_entry(service_name, topic_name, dbms_name, table_name, attr_name, attr_value):

    table_entry = get_table_entry(service_name, topic_name, dbms_name, table_name)
    table_entry[attr_name] = attr_value

# ---------------------------------------------------------------------------------------------
# Reset Statistics
# ---------------------------------------------------------------------------------------------
def reset(status, service_name, topic_name, name_val_list):
    '''
    status - thread object
    service_name - the service to reset
    topic_name - the topic to reset
    name_val - a list of attributes names and values for the reset
    '''

    if not service_name in statistics_:
        status.add_error(f"No service '{service_name}' in statistics")
        ret_val = process_status.Error_command_params
    elif not topic_name in statistics_[service_name]:
        status.add_error(f"No statistics on topic {topic_name} in service '{service_name}'")
        ret_val = process_status.Error_command_params
    else:
        ret_val = process_status.SUCCESS
        stats_info = statistics_[service_name][topic_name]

        for attr_name in stats_info:

            if isinstance(attr_name, int):
                if "timestamp" in attr_name:
                    # Set current timestamp
                    stats_info[attr_name] = utils_columns.get_current_time_in_sec()
                else:
                    stats_info[attr_name] = 0
            else:
                stats_info[attr_name] = ""

        if name_val_list:
            # reset from the list of attr names and values
            for name_val in name_val_list:
                attr_name = name_val[0]
                attr_val = name_val[1]
                if attr_name in stats_info:
                    stats_info[attr_name] = attr_val        # Update value from the given list

    return ret_val
# ---------------------------------------------------------------------------------------------
# Get statistical info
# get stats where service = operator
# ---------------------------------------------------------------------------------------------
def get_info(status, reply_format, conditions):


    reply = ""

    service_name_list = conditions["service"]

    if "topic" in conditions:
        topic_list = conditions["topic"]
    else:
        topic_list = ["summary"]      # Default topic

    suffix = ""

    for service in service_name_list:
        if not service in statistics_:
            status.add_error(f"No service named '{service_name_list}'")
            return [process_status.ERR_command_struct, None]

        for topic_counter, topic in enumerate(topic_list):

            if not topic in statistics_[service]:
                status.add_error(f"No statistics on topic '{topic}' in service '{service}'")
                return [process_status.ERR_command_struct, None]

            prep_key = f"prep_{topic}"      # Execute these instructions prior to providing the stats info
            if prep_key in statistics_[service]:
                prepare_stats(service, topic, prep_key)


            if reply_format == "json":
                if len(topic_list) > 1:
                    # Multiple JSONs
                    if topic_counter == 0:
                        reply = '['
                    else:
                        reply += ','        # Separate between JSONs
                        if topic_counter == (len(topic_list) -1):
                            suffix = ']'

                reply += utils_json.to_string(statistics_[service][topic])

                if suffix:
                    # end multiple JSONs
                    reply += suffix

            else:

                reply += f"\r\n\nStats: {service.upper()} {topic.upper()}"
                if service == "operator":
                    reply += get_operator_info(service, topic)
                elif service == "publisher":
                    reply += get_publisher_info(service, topic)


    return [process_status.SUCCESS, reply]
# ---------------------------------------------------------------------------------------------
# Pull the publisher stats
# ---------------------------------------------------------------------------------------------
def get_publisher_info(service, topic):
    reply = ""
    if topic == "files":
        data_info = []
        attr_names = ["DBMS", "table", "Destination", "Files", "Errors", "Err Message"]

        reply_dict = statistics_[service][topic]

        previous_dbms = ""
        previous_table = ""
        for dbms_name, table_info in reply_dict.items():

            for table_name, table_values in table_info.items():

                if isinstance(table_values, dict):
                    for destination, dest_val in table_values.items():
                        data_entry = []  # organize the info on a particular table
                        if previous_dbms == dbms_name:
                            data_entry.append("")  # Ignore repeating the database name in the output
                        else:
                            data_entry.append(dbms_name)
                            previous_dbms = dbms_name

                        if previous_table == table_name:
                            data_entry.append("")  # Ignore repeating the table name in the output
                        else:
                            data_entry.append(table_name)
                            previous_table = table_name

                        data_entry.append(destination)      # The node to receive the data

                        data_entry += dest_val # Add: Counter, Counter Error, Time, Err Msg

                        data_info.append(data_entry)  # Add the info on a table

                    reply += utils_print.output_nested_lists(data_info, "", attr_names, True, "", True)
    else:
        # Return the Summary
        reply = get_stat_dict_output(service, topic)


    return reply
# ---------------------------------------------------------------------------------------------
# Pull the operator stats
# ---------------------------------------------------------------------------------------------
def get_operator_info(service, topic):

    reply = ""

    if topic == "sql" or topic == "json" or topic == "inserts":
        # Print info on each table
        data_info = []
        attr_names = ["DBMS", "table"]
        title_flag = False  # The title is created with the first iteration

        reply_dict = statistics_[service][topic]

        previous_dbms = ""
        for dbms_name, table_info in reply_dict.items():

            for table_name, table_values in table_info.items():
                data_entry = []  # organize the info on a particular table
                if previous_dbms == dbms_name:
                    data_entry.append("")  # Ignore repeating the database name in the output
                else:
                    data_entry.append(dbms_name)
                    previous_dbms = dbms_name
                data_entry.append(table_name)
                if isinstance(table_values, dict):
                    for attr_name, attr_val in table_values.items():
                        if not title_flag:
                            # Only with first iteration - organize the title
                            attr_names.append(attr_name)
                        data_entry.append(attr_val)
                else:
                    # list
                    if not title_flag:
                        if f"attr_name_{topic}" in statistics_[service]:
                            attr_names += statistics_[service][f"attr_name_{topic}"]
                    data_entry += table_values

                data_info.append(data_entry)  # Add the info on a table
                title_flag = True  # Title is set

        if not len(data_info):
            attr_names = None  # no data to print
        reply += utils_print.output_nested_lists(data_info, "", attr_names, True, "", True)

    else:
        # Return the Summary
        reply = get_stat_dict_output(service, topic)

    return reply
# ---------------------------------------------------------------------------------------------
# Get dictionary for output
# ---------------------------------------------------------------------------------------------
def get_stat_dict_output(service, topic):
    source_object = statistics_[service][topic]
    if isinstance(source_object, dict):
        # A dictionary like a policy
        entries_list = [source_object]  # A single dictionary (like [operator][summary])
    else:
        # as a list (like [operator][error]
        entries_list = source_object

    attr_names = list(entries_list[0].keys())  # Take the names from the first dictionary in the list

    reply = utils_print.print_dict_as_table(None, entries_list, None, attr_names, True, "", None, True)

    return reply
# ---------------------------------------------------------------------------------------------
# Update values in the statistics dictionary
# ---------------------------------------------------------------------------------------------
def prepare_stats(service, topic, prep_key):

    stats_dict = statistics_[service]                   # The info to update
    prep_list = statistics_[service][prep_key]          # The update instructions


    for entry in prep_list:

        target_dict, stats_key, operation, operand1_dict, operand1, operand2_dict, operand2 = entry

        if operation == 'M':
            # apply a menthod that returns a value to a dictionary entry
            stats_dict[target_dict][stats_key] = operand1()        # Source is a function
        elif operation == 'P':
            # apply a process that does not return a value
            operand1(entry)        # Source is a function
        elif operation == 'T':
            # Set a time difference from now()
            current_time = utils_columns.get_current_time_in_sec()
            stats_info = stats_dict[operand1_dict]
            if isinstance(stats_info,list):
                # Go over all list objects
                for entry in stats_info:
                    set_time_diff(entry, stats_key, current_time, entry, operand1)
            else:
                set_time_diff(stats_dict[target_dict], stats_key, current_time, stats_dict[operand1_dict], operand1)

        elif operation == 'D':
            # Difference netween valies
            stats_dict[target_dict][stats_key] = stats_dict[operand1_dict][operand1] - stats_dict[operand2_dict][operand2]
        elif operation == 'E':
            # Equals - move a value from one place to another
            stats_dict[target_dict][stats_key] = stats_dict[operand1_dict][operand1]


# ---------------------------------------------------------------------------------------------
# Calculate the time difference between now and an event
# ---------------------------------------------------------------------------------------------
def set_time_diff(target_dict, target_key, current_time, source_dict, source_key):
    '''
    target_dict - the dictionary to update (like operator errors sql)
    target_key - the key in the dictionary
    current_time - timestamp now()
    source_dict - dictionary with previous time
    source_key - the key with the source timestamp
    '''

    previous_time = source_dict[source_key]
    if not previous_time or previous_time >= current_time:
        target_dict[target_key] = "00:00:00"
    else:
        hours_time, minutes_time, seconds_time = utils_data.seconds_to_hms(current_time - previous_time)
        time_diff = "%02u:%02u:%02u" % (hours_time, minutes_time, seconds_time)
        target_dict[target_key] = time_diff


# ---------------------------------------------------------------------------------------------
# Update Operator Errors
# ---------------------------------------------------------------------------------------------
def operator_error(file_type, dbms_name, table_name, error_code):

    error_list = statistics_["operator"]["error"]

    # Get JSON or SQL error struvture from the list
    error_dict = error_list[0] if file_type == "json" else error_list[1]

    error_dict["counter"] += 1
    error_dict["timestamp"] = utils_columns.get_current_time_in_sec()
    error_dict["dbms name"] = dbms_name
    error_dict["table name"] = table_name
    error_dict["last error"] = error_code

# -----------------------------------------------------------------
# operator - update statistics on files processed
# -----------------------------------------------------------------
def operator_update_stats(topic_name, dbms_name, table_name, write_immediate, tsd_info_updated):
    '''
    topic_name - json or sql
    dbms_name
    table_name
    write_immediate - True means every entry was updated when arrived (no need to update the current dbms)
    tsd_info_updated - True when data is from sensors (vd from servers supporting the cluster)
    '''
    table_entry = get_table_entry("operator", topic_name, dbms_name, table_name)
    if table_entry == None:
        table_entry = new_table_entry("operator", topic_name, dbms_name, table_name, [0,0,0,0])
        # The entries as a f(table):
        # 0) Counter to the number of json files processed
        # 1) Counter to the number of json files from streaming with write_immediate flag
        # 2) Date and time for the last file processed
        # 3) Time since last update hh:mm:ss  (updated in the prep)
    if write_immediate:
        table_entry[1] += 1
    else:
        table_entry[0] += 1     # file processed

    current_time = utils_columns.get_current_time_in_sec()
    table_entry[2] = current_time

    statistics_["operator"]["prep_info"]["last_file_time"] = current_time # replacing last_file_time_


# -----------------------------------------------------------------
# operator - update statistics on rows inserted
# -----------------------------------------------------------------
def operator_update_inserts(dbms_name, table_name, rows_updated, is_immediate):
    '''
    dbms_name
    table_name
    rows_updated - the number of rows
    is_immediate - True means every entry was updated when arrived (no need to update the current dbms)
    '''
    table_entry = get_table_entry("operator", "inserts", dbms_name, table_name)
    if table_entry == None:
        table_entry = new_table_entry("operator", "inserts", dbms_name, table_name, [0,0,0,0,0,0])
        # The entries as a f(table):
        # 0) timestamp - timestamp of first insert
        # 1) timestamp - timestamp of last insert
        # 2) operational time - the time elapsed since first insert
        # 3) last inserts - the time elapsed since last insert
        # 4) new inserts added in batch
        # 5) new inserts from write immediate
    if is_immediate:
        table_entry[5] += rows_updated
    else:
        table_entry[4] += rows_updated


    current_time = utils_columns.get_current_time_in_sec()
    if not table_entry[0]:
        table_entry[0] = current_time       # Time of first insert
    table_entry[1] = current_time

    statistics_["operator"]["prep_info"]["last_file_time"] = current_time # replacing last_file_time_


# -----------------------------------------------------------------
# For the publisher
# update statistics on files processed
# -----------------------------------------------------------------
def publisher_stats(service_name, topic_name, dbms_name, table_name, destination, is_sucess, is_failure, err_message):
    '''
    service_name - publisher
    topic_name - "files"
    dbms_name
    table_name
    destination - the node that gets the file

    '''
    info_entry = get_table_entry(service_name, topic_name, dbms_name, table_name)
    if info_entry == None:
        info_entry = new_table_entry(service_name, topic_name, dbms_name, table_name, {})
        # The entries as a f(destination):

        # 0) Counter to the number of success
        # 1) Counter to the number of failures
        # 2) Error Message
    if destination in info_entry:
        table_entry = info_entry[destination]       # The info as f (destination node)
    else:
        table_entry = [0,0,""]
        info_entry[destination] = table_entry

    if is_sucess:
        table_entry[0] += 1     # file processed
    if is_failure:
        table_entry[1] += 1     # file processed with error

    current_time = utils_columns.get_current_time_in_sec()
    statistics_[service_name]["prep_info"]["last_file_time"] = current_time # replacing last_file_time_

    if err_message:
        table_entry[2] = err_message

