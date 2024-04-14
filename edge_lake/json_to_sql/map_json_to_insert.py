"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import os
import copy

import edge_lake.dbms.db_info as db_info
import edge_lake.dbms.partitions as partitions
import edge_lake.generic.utils_io as utils_io
import edge_lake.generic.utils_columns as utils_columns
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.process_status as process_status
import edge_lake.generic.process_log as process_log
import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.cmd.data_monitor as data_monitor
import edge_lake.json_to_sql.mapping_policy as mapping_policy

from edge_lake.json_to_sql.suggest_create_table import policy_to_columns_list

MAX_COL_LENGTH_ = 1000

# ==================================================================
# Get a list describing the mapping. Every entry in the list includes the following:
# <JSON Attribute Name><Table Column Name><Data Type><Default Value>
# ==================================================================
def get_columns_list(status: process_status, dbms_name: str, table_name: str, instruct):
    columns = []

    if instruct and "schema" in instruct["mapping"].keys():
        # Get the names of the participating columns from the schema in the instructions
        policy_to_columns_list(status, dbms_name, table_name, instruct, columns)
    else:
        # Get columns from metadata
        column_info = db_info.get_column_info(status, dbms_name, table_name)
        if column_info:
            for entry in column_info:
                # Add - <JSON Attribute Name> <Table Column Name> <Data Type> <Default Value>
                columns.append((None, entry[1], entry[2].lower(), entry[4]))

    if len(columns) <= 2:
        # First 2 columns are row id and insert time
        status.add_error("No data at information_schema on table: %s.%s" % (dbms_name, table_name))
        columns = None

    return columns

# ==================================================================
# Map keys in JSON dict object to INSERT statement based on columns_list
# ==================================================================
def map_columns(status: process_status, dbms_name, table_name, tsd_name, tsd_id, json_data, columns_list):
    """
    Map keys in JSON dict object to INSERT statement based on columns_list
    :args:
       json_data:list - list of JSON objects
       columns_list:list - list of columns in table
    :return:
       list of keys that have corresponding columns
    """

    currentt_utc = "\'" + utils_columns.get_current_utc_time() + "\'"

    insert_list = []
    attr_names_map = {}  # Map the keys in the JSON entry to lower

    for entry in json_data:
        column_array = []
        time_presence = False
        value_presence = False
        for index, column_info in enumerate(columns_list):

            attribute_name = column_info[1]  # use the column name in the table
            column_type = column_info[2]
            default_declared = column_info[3]
            if index < 4:
                # First 2 columns are row_id and insert_timestamp - they are set with - Default
                if not index:
                    column_value = "DEFAULT"  # Row ID
                elif index == 1:
                    column_value = currentt_utc  # Current timestamp in utc
                elif index == 2:
                    column_value = tsd_name
                elif index == 3:
                    column_value = tsd_id
            else:
                if isinstance(attribute_name, list):
                    column_value = get_value_by_function(attribute_name, entry,
                                                         attr_names_map)  # Apply the function in the attribute name to get the column value
                else:
                    column_value = get_value_ignore_case(entry, attribute_name, attr_names_map)
                    if column_type == "varchar" and len(column_value) > MAX_COL_LENGTH_:
                        # Put the blob in file ot a database and replace the column with the ID to the blob
                        if dbms_name and table_name:
                            ret_val, blob_file_name = mapping_policy.archive_blob_file(status, dbms_name, table_name, column_value)
                            if blob_file_name:
                                column_value = blob_file_name       # Replace the value  with the id of the file
                            else:
                                column_value = ""                   # Otherwise the blob data will go to the row

                if isinstance(column_value, str):
                    if column_value == "":
                        if default_declared:
                            if column_type.startswith("timestamp"):
                                if default_declared == "current_timestamp":
                                    column_value = currentt_utc
                                else:
                                    column_value = "DEFAULT"
                            else:
                                column_value = "DEFAULT"
                        else:
                            column_value = "NULL"
                    else:
                        value_presence = True  # A value which is not set as default
                        if column_type.startswith("timestamp"):
                            if len(column_value) > 10:
                                if column_value[-6] == '+' or column_value[-6] == '-':
                                    # Using utcoffset: for example: '2021-11-04T00:05:23+04:00'
                                    # Details: https://en.wikipedia.org/wiki/UTC_offset
                                    utcoffset = utils_columns.time_iso_format(column_value)
                                    if utcoffset:
                                        column_value = utcoffset
                                elif column_value[10] != 'T':
                                    column_value = utils_columns.local_to_utc(column_value)  # CHANGE to UTC
                        column_value = "\'" + utils_data.replace_string_chars(True, column_value,
                                                                              {'\'': '`', '"': '`'}) + "\'"
                elif isinstance(column_value, int):
                    time_presence = True  # This row has time value
                    if column_type.startswith("timestamp"):
                        # Assumes seconds and convert to string time
                        column_value += utils_columns.utc_diff  # Change to UTC
                        column_value = "\'" + utils_columns.seconds_to_date(column_value) + "\'"
                elif isinstance(column_value, list) or isinstance(column_value, dict):
                    column_value = "\'" + utils_data.replace_string_chars(True, str(column_value),
                                                                          {'\'': '`', '"': '`'}) + "\'"
                else:
                    if column_value == None:
                        column_value = "DEFAULT"
                    else:
                        value_presence = True  # for example, integers, floats


            column_array.append(str(column_value))

        if time_presence or value_presence:
            # A minimum of time and value
            insert_list.append(
                column_array)  # The insert list is a list of arrays, every entry represent a row as a list of columns

    return insert_list


# ==================================================================
# Given a Key - Get value from JSON - ignore Upper Lower Case
# ==================================================================
def get_value_ignore_case(json_entry, key, attr_names_map):
    # First try without mapping to lowere case
    try:
        value = json_entry[key]
    except:
        if len(key) > 1 and key[0] == '_' and key[1:].isdigit() and key[1:] in json_entry:
            # This is the case of adding underscore to a column name to make it valid
            # Column name 40001 is changed to _40001
            value = json_entry[key[1:]]
        else:
            # Test if a mapping dictionary exists
            value = ""
            if not key in attr_names_map:   # The key can be in upper case or using a dot or a minus sign or a space
                # create a mapping between a lower case to the way it is represented in the JSON entry
                for json_key in json_entry.keys():
                    new_key = utils_data.reset_str_chars(json_key)
                    attr_names_map[new_key] = json_key  # Keep a mapping between lower to the format in the dictionary

            # If a dictionary exists or just created
            if len(attr_names_map):
                if key in attr_names_map.keys():
                    json_key = attr_names_map[key]
                    if json_key in json_entry.keys():  # try again with the upper case key
                        value = json_entry[json_key]
    return value


# ==================================================================
# Apply the function in the attribute name on the JSON Entry to get the column value
# For example: ["[lat]",",","[lon]"] --> get latitude , comma, longitude 
# ==================================================================
def get_value_by_function(function, json_entry, upper_lower_map):
    output = ""
    for element in function:
        if not isinstance(element, str):
            break
        if element[0] == '[' and element[-1] == ']':
            data_out = get_value_ignore_case(json_entry, element[1:-1], upper_lower_map)
        else:
            data_out = element

        output += str(data_out)

    return output


# ==================================================================
# Given relevent information generate INSERT statment
# ==================================================================
def generate_insert_stmt(status: process_status, dbms_name, table_name, insert_size, column_names, insert_rows):
    # check number of lines to insert
    if len(insert_rows) == 0:
        status.add_error(
            "Unable to generate insert statement for table: %s.%s, data file is empty" % (dbms_name, table_name))
        return ''

    # map to inserts per specific dbms
    insert_statements = db_info.get_insert_rows(status, dbms_name, table_name, insert_size, column_names, insert_rows)

    return insert_statements


# ==================================================================
# Code to convert JSON file to SQL INSERT statement
# ==================================================================
def map_json_file_to_insert(status: process_status, tsd_name, tsd_id, dbms_name, table_name, insert_size: int, json_file, sql_dir, instructions):
    sql_file_list = None
    rows_count = 0
    ret_val = True

    if instructions == "0":
        instruct = None
    else:
        # get instructions to map the file from JSON to a Table struct
        instruct = member_cmd.get_instructions(status, instructions)
        if not instruct:
            status.add_error("Failed to retrieve mapping instructions using the key: '%s' for file: '%s'" % (instructions, json_file))
            ret_val = False

    if ret_val:
        err_msg = ""
        json_file = os.path.expanduser(os.path.expandvars(json_file))
        if not os.path.isfile(json_file):
            status.add_error("Failed to generate INSERT statement - Path to JSON file not recognized: " + json_file)
            ret_val = False
        else:

            sql_dir = sql_dir.replace('\\', '/')
            json_file = json_file.replace('\\', '/')
            # crearte SQL file to store inserts in
            if sql_dir[-1] != "/":
                sql_dir += "/"
            sql_dir = os.path.expanduser(os.path.expandvars(sql_dir))
            if not os.path.isdir(sql_dir):
                status.add_error("Failed to generate INSERT statement - Invalid SQL dir: %s" % sql_dir)
                ret_val = False
            else:
                file_name_offset = json_file.rfind("/")
                if file_name_offset == -1:
                    file_name_offset = 0
                else:
                    file_name_offset += 1
                sql_file_name = json_file[file_name_offset:-5]

                json_data = utils_io.read_json_strings(status, json_file)
                if not json_data or not len(json_data):
                    status.add_error(err_msg)
                    sql_file_list = None
                    rows_count = 0
                else:
                    sql_file_list, rows_count = map_json_list_to_sql(status, tsd_name, tsd_id, dbms_name, table_name, insert_size, sql_dir, sql_file_name, instruct, json_data)
                    if rows_count:
                        process_log.add("File", "INSERT statement generated from file: %s" % json_file)
                    else:
                        process_log.add("File", "Failed to generate INSERT statement from file: %s" % json_file)

    return [sql_file_list, rows_count]  # return the names of the file with the SQL data and total number of SQL rows
# ==================================================================
#Convert JSON list to SQL INSERT statement
# ==================================================================
def map_json_list_to_sql(status: process_status, tsd_name, tsd_id, dbms_name, table_name, insert_size,
                            sql_dir, sql_file_name, instruct, json_data):

    if instruct and len(instruct):
        # Make a copy of the policy as it may be changed using compile_ in Mapping_policy.apply_policy_schema()
        policy = copy.deepcopy(instruct)
    else:
        policy = None


    columns_list = get_columns_list(status, dbms_name, table_name, policy)
    if not columns_list:
        err_msg = "Failed to generate INSERT statement - Columns are not recognized in the database schema for table: %s.%s" % (
            dbms_name, table_name)
        ret_val = False
    else:
        err_msg = ""
        ret_val = True

    if ret_val:
        if policy:
            ret_code = mapping_policy.validate(status, policy)
            if ret_code:
                err_msg = f"Error in mapping policy structure with dbms: '{dbms_name}' and table: '{table_name}'"
                ret_val = False
            else:
                # TEST if the JSON data provided is in the target format, or, mapping is needed
                policy_inner = policy["mapping"]
                policy_schema = policy_inner["schema"]
                if isinstance(json_data, list):
                    test_entry = json_data[0]       # Take the first entry from the list as a representatiive of the group
                elif  isinstance(json_data, dict):
                    test_entry = json_data
                else:
                    err_msg = f"Error in data format,  dbms: '{dbms_name}' and table: '{table_name}'"
                    ret_val = False

                if ret_val:
                    apply_policy = False
                    for attribute in test_entry:
                        if not attribute in policy_schema:
                            apply_policy = True     # Data structure is different than the target structure in the policy
                            break
                    if apply_policy:
                        # Modify the JSON file using the Policy
                        ret_code, dbms_name, table_name, data_source, json_data = mapping_policy.apply_policy_schema(status, dbms_name, table_name, policy_inner, policy_inner["id"], json_data, False, None)
                        if ret_code:
                            ret_val = False     # Mapping failed


    if ret_val:
        # insert_columns is a list - every entry in the list includes the columns of each row to be added
        insert_columns = map_columns(status, dbms_name, table_name, tsd_name, tsd_id, json_data, columns_list)
        rows_count = len(insert_columns)
        if not rows_count:
            status.add_keep_error(
                "Failed to map (time and value) from JSON file to database '%s' and table '%s' using existing schema or mapping policy" % (
                dbms_name, table_name))
            ret_val = False
        elif data_monitor.set_monitoring(dbms_name, table_name):    # Sets the monitored struct - or returns false if not monitored
            # Update the data_monitor struct
            data_monitor.process_monitored_data(dbms_name, table_name, columns_list, insert_columns)

    sql_file_list = []  # keep the list of sql files created
    if ret_val:

        while insert_columns:
            # Loop as long there is data to write
            # Every insert_data list contains data to a different partition
            ret_code, file_name, table_extension, other_data = partition_data(status, sql_dir, sql_file_name, dbms_name,
                                                                             table_name, columns_list, insert_columns)
            if ret_code:
                # Error code in processing data
                ret_val = False
                break

            if not sql_dir:
                # Streaming data with immediate flag - Update the local table with insert statement
                ret_val = insert_sql_to_table(status, file_name, dbms_name, table_name, table_extension, insert_size,
                                         columns_list, insert_columns)
                if not ret_val:
                    err_msg = "Failed to INSERT streaming data to local table with immediate flag: %s.%s" % (dbms_name, table_name)
            else:
                # Operator process
                # write the SQL file with insert stmt to a file with .sql extension
                ret_val = write_sql_file(status, file_name, dbms_name, table_name, table_extension, insert_size,
                                     columns_list, insert_columns)

            if not ret_val:
                break

            sql_file_list.append(file_name)
            insert_columns = other_data  # other_data contains data of a different partition


    if not ret_val:
        if not err_msg:
            err_msg = "Failed to generate INSERT statement for table: %s.%s" % (dbms_name, table_name)
        status.add_error(err_msg)
        rows_count = 0

    return [sql_file_list, rows_count]  # return the names of the file with the SQL data and total number of SQL rows


# -------------------------------------------------------------------------------------------
# Go over the data to do the following:
# 1) Determine if data is partitioned
# 1) Determine if data is partitioned
# 2) Provide file name as f (table name + partition)
# 3) provide the relevant partitioned data
# ___________________________________________________________________________________________
def partition_data(status, sql_dir, sql_file_name, dbms_name, table_name, columns_list, insert_columns):

    name_splitted = sql_file_name.split(".", 2)  # dbms name + table name + all the rest
    entries = len(name_splitted)
    table_extension = ""
    if not partitions.is_partitioned(dbms_name, table_name):
        # No partitions, one output file and no name extension
        # + add partition '0'
        if sql_dir:
            if entries > 2:
                file_name = sql_dir + dbms_name + "." + table_name + ".0." + name_splitted[2] + ".insert.sql"
            else:
                file_name = sql_dir + dbms_name + "." + table_name + ".0.insert.sql"
        else:
            file_name = None        # Immediate flag in streaming mode - the data is not added to a file
        ret_val = process_status.SUCCESS

        other_data = []
    else:
        par_info = partitions.get_par_info(dbms_name, table_name)
        par_column = par_info[2]
        column_id = db_info.get_column_id(columns_list, par_column)
        if column_id == -1:
            status.add_error(
                "Faild to map JSON to SQL: Partition field '%s' is not recognized in table metadata" % par_column)
            ret_val = process_status.Missing_metadata_info
            file_name = None
            other_data = None
        else:
            ret_val, other_data, table_extension = partitions.split_list(status, column_id, par_info[1], par_info[0],
                                                                         par_info[2], columns_list, insert_columns)

            if not ret_val and sql_dir != None:     # sql_dir is none if inserting individual rows
                # Place the partition after table name
                file_name = sql_dir + dbms_name + "." + table_name + '.' + table_extension
                if entries > 2:
                    file_name += '.' + name_splitted[2]  # The rest of the name fields

                file_name += ('.' + utils_io.get_unique_time_string() + ".insert.sql")  # get a unique prefix
            else:
                file_name = None    # Immediate flag in streaming mode - the data is not added to a file

    return [ret_val, file_name, table_extension, other_data]


# -------------------------------------------------------------------------------------------
# Write the Insert statements to a SQL file
# ___________________________________________________________________________________________
def write_sql_file(status, sql_file, dbms_name, table_name, par_name, insert_size, column_names, insert_columns):

    # Map columns data to insert stmt
    ret_val, insert_stmt = columns_to_insert_stmt(status, dbms_name, table_name, par_name, column_names, insert_size, insert_columns)

    if ret_val:
        io_handle = utils_io.IoHandle()
        tmp_name =  sql_file + '_tmp'    # Name changed not be processed by the main Operator thread
        if not io_handle.open_file("append", tmp_name):
            ret_val = False
        else:
            # File opened -> Append Data
            if not io_handle.append_data(insert_stmt):
                ret_val = False

            # Close file
            if not io_handle.close_file():
                ret_val = False

            if ret_val:
                ret_val = utils_io.rename_file(status, tmp_name, sql_file)  # Revert to .sql name so it will be processed
                if not ret_val:
                    ret_val = utils_io.delete_file(sql_file, False)        # Delete old file
                    if ret_val:
                        ret_val = utils_io.rename_file(status, tmp_name, sql_file)  # Try again after old file deleted




    return ret_val

# -------------------------------------------------------------------------------------------
# Insert to the local table - this process is called from streaming data with immediate flag
# ___________________________________________________________________________________________
def insert_sql_to_table(status, sql_file, dbms_name, table_name, par_name, insert_size, column_names, insert_columns):

    # Map columns data to insert stmt
    ret_val, insert_stmt = columns_to_insert_stmt(status, dbms_name, table_name, par_name, column_names, insert_size, insert_columns)

    if ret_val:
        insert_list = insert_stmt.split("\n")
        ret_val = db_info.insert_rows(status, dbms_name, table_name, insert_list)
        # Note: If partitioned and partition is not defined - it will switch to not-immidiate and operator will create the partition

    return ret_val
# -------------------------------------------------------------------------------------------
# Map the columns data to insert statements
# ___________________________________________________________________________________________
def columns_to_insert_stmt(status, dbms_name, table_name, par_name, column_names, insert_size, insert_columns):

    if par_name:
        t_name = "par_" + table_name + "_" + par_name  # extend name by partition
    else:
        t_name = table_name
    insert_stmt = generate_insert_stmt(status, dbms_name, t_name, insert_size, column_names, insert_columns)

    ret_val = insert_stmt != ''

    return [ret_val, insert_stmt]

# -------------------------------------------------------------------------------------------
# Map JSON streaming data to SQL Inserts
# ___________________________________________________________________________________________
def buffered_json_to_sql(status, dbms_name, table_name, source, instructions, tsd_id, json_data):


    sql_file_name = dbms_name + '.' + table_name
    sql_file_list, rows_count = map_json_list_to_sql(status, '0', tsd_id, dbms_name, table_name, 1,
                                                     None, sql_file_name, None, json_data)

    return [sql_file_list, rows_count]
