"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

from calendar import monthrange

import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.utils_sql as utils_sql
import edge_lake.generic.utils_columns as utils_columns

partition_struct = {}  # describes how data is partitioned

# The structure of the partition name:
# par _ [table_name] _ [date/time key] _ [type] [counter] _ [column name]
# [date/time key] - depending if partitioned by year, month, day, hour
# [type] - One char for the type of partition: year, month, day, hour --> y/m/d/h
# [counter] - ie. every 2 hours or every 4 days
# Example: par_test_data_2019_10_15_17_h01_timestamp

# ---------------------------------------------------------------
# Return True for partitioned table
# ---------------------------------------------------------------
def is_partitioned(dbms_name, table_name):
    if table_name[:4] == "par_":
        return False  # This is a partition of a different table - This table is not partitioned
    ret_val = dbms_name + '.' + table_name in partition_struct.keys()
    if not ret_val:
        ret_val = dbms_name + '.*' in partition_struct.keys()
        if not ret_val:
            ret_val = '*.*' in partition_struct.keys()
    return ret_val


# ---------------------------------------------------------------
# Return True The partition info
# ---------------------------------------------------------------
def get_par_info(dbms_name, table_name):
    key = dbms_name + '.' + table_name
    if key in partition_struct.keys():
        info = partition_struct[key]
    else:
        key = dbms_name + '.*'
        if key in partition_struct.keys():
            info = partition_struct[key]
        elif '*.*' in partition_struct.keys():
            info = partition_struct["*.*"]
        else:
            info = ""

    return info


# ---------------------------------------------------------------
# Return file name extension with partition by year
# ---------------------------------------------------------------
def get_year(status, date_str, units_count):
    if len(date_str) >= 10:
        if units_count == 1:
            date_name = date_str[:4]
        else:
            try:
                year = int(date_str[:4])
                date_name = str(int(year / units_count))
            except:
                status.add_error("Partition failed: Error in date provided: '%s'" % date_str)
                date_name = ""
    else:
        status.add_error("Partition failed: Error in date provided: '%s'" % date_str)
        date_name = ""

    return date_name


# ---------------------------------------------------------------
# Return file name extension with partition by month
# ---------------------------------------------------------------
def get_year_month(status, date_str, units_count):
    if len(date_str) >= 10:
        if units_count == 1:
            date_name = date_str[:4] + "_" + date_str[5:7]
        else:
            try:
                year = date_str[:4]
                month = date_str[5:7]
                if units_count == 1:
                    date_name = year + "_" + month
                else:
                    date_name = year + "_%02u" % int(
                        ((int(month) - 1) / units_count))  # if units_count is 2 - Jan + Feb will be in the same backet
            except:
                status.add_error("Partition failed: Error in date provided: '%s'" % date_str)
                date_name = ""
    else:
        status.add_error("Partition failed: Error in date provided: '%s'" % date_str)
        date_name = ""

    return date_name


# ---------------------------------------------------------------
# Return file name extension with partition by day
# ---------------------------------------------------------------
def get_year_month_day(status, date_str, units_count):
    if len(date_str) >= 10:
        if units_count == 1:
            date_name = date_str[:4] + "_" + date_str[5:7] + "_" + date_str[8:10]
        else:
            try:
                year = date_str[:4]
                month = date_str[5:7]
                day = int(date_str[8:10])
                date_name = year + "_" + month + "_%02u" % int((day - 1) / units_count)
            except:
                status.add_error("Partition failed: Error in date provided: '%s'" % date_str)
                date_name = ""
    else:
        status.add_error("Partition failed: Error in date provided: '%s'" % date_str)
        date_name = ""

    return date_name


# ---------------------------------------------------------------
# Return file name extension with partition by hour
# ---------------------------------------------------------------
def get_year_month_day_hour(status, date_str, units_count):
    if len(date_str) >= 13:
        if units_count == 1:
            date_name = date_str[:4] + "_" + date_str[5:7] + "_" + date_str[8:10] + "_" + date_str[11:13]
        else:
            try:
                year = date_str[:4]
                month = date_str[5:7]
                day = date_str[8:10]
                hour = int(date_str[11:13])
                date_name = year + "_" + month + "_" + day + "_%02u" % int(hour / units_count)
            except:
                status.add_error("Partition failed: Error in date provided: '%s'" % date_str)
                date_name = ""
    else:
        status.add_error("Partition failed: Error in date provided: '%s'" % date_str)
        date_name = ""

    return date_name


time_units = {
    #               Function                start end
    "year": (get_year, 0, 4, 1),
    "years": (get_year, 0, 4, 1),
    "month": (get_year_month, 5, 7, 2),
    "months": (get_year_month, 5, 7, 2),
    "day": (get_year_month_day, 8, 10, 3),
    "days": (get_year_month_day, 8, 10, 3),
    "hour": (get_year_month_day_hour, 11, 13, 4),
    "hours": (get_year_month_day_hour, 11, 13, 4),
}

time_units_list = [
    ("year", 0, 4),
    ("month", 5, 7),
    ("day", 8, 10),
    ("hour", 11, 13),
]

char_to_time_unit = {
    'y' : "year",
    'm' : "month",
    'd' : "day",
    'h' : "hour",
}
# ---------------------------------------------------------------
# Return the start date and end date
# Example "2019" --> ["2019-01-01" , "2019-12-31"]
# ---------------------------------------------------------------
def get_range_year(status, par_str, units_count):
    try:
        year = int(par_str)
        year *= units_count
        start_date = str(year) + "-01-01"
        end_date = str(year + units_count - 1) + "-12-%02u" % monthrange(year, 12)[1]
    except:
        status.add_error("Error in partition name: '%s' is not indicative of a year range" % par_str)
        start_date = ""
        end_date = ""

    reply_list = [start_date, end_date]
    return reply_list


# ---------------------------------------------------------------
# Return the start date and end date
# Example "2019-02" --> ["2019-02-01" , "2019-02-28"]
# ---------------------------------------------------------------
def get_range_month(status, par_str, units_count):
    try:
        year = par_str[:4]
        month = par_str[5:7]
        if units_count == 1:
            start_date = year + "-%s-01" % month
            end_date = year + "-%s-%02u" % (month, monthrange(int(year), int(month))[1])
        else:
            start_month = int(month) * units_count + 1
            end_month = int(month) * units_count + units_count
            start_date = year + "-%02u-01" % start_month
            end_date = year + "-%02u-%02u" % (end_month, monthrange(int(year), int(end_month))[1])
    except:
        status.add_error("Error in partition name: '%s' is not indicative of months range" % par_str)
        start_date = ""
        end_date = ""

    reply_list = [start_date, end_date]
    return reply_list


# ---------------------------------------------------------------
# Return the start date and end date
# Example "2019-02-01" --> ["2019-02-01" , "2019-02-01"]
# ---------------------------------------------------------------
def get_range_day(status, par_str, units_count):
    try:
        year_month = par_str[:4] + '-' + par_str[5:7]
        day = par_str[8:10]
        if units_count == 1:
            start_date = year_month + "-%s" % day
            end_date = start_date
        else:
            start_day = int(day) * units_count + 1
            end_day = (int(day) + 1) * units_count
            days_in_month = monthrange(int(par_str[:4]), int(par_str[5:7]))[1]
            if end_day > days_in_month:
                end_day = days_in_month
            start_date = year_month + "-%02u" % start_day
            end_date = year_month + "-%02u" % end_day
    except:
        status.add_error("Error in partition name: '%s' is not indicative of months range" % par_str)
        start_date = ""
        end_date = ""

    reply_list = [start_date, end_date]
    return reply_list


# ---------------------------------------------------------------
# Return the start date and end date
# Example "2019-02-01-12" --> ["2019-02-01-12" , "2019-02-01-12"]
# ---------------------------------------------------------------
def get_range_hour(status, par_str, units_count):
    try:
        year_month_day = par_str[:4] + '-' + par_str[5:7] + '-' + par_str[8:10]
        hour = int(par_str[11:13])
        start_hour = hour * units_count
        end_hour = start_hour + units_count - 1
        start_date = year_month_day + "-%02u" % hour
        end_date = year_month_day + "-%02u" % end_hour
    except:
        status.add_error("Error in partition name: '%s' is not indicative of months range" % par_str)
        start_date = ""
        end_date = ""

    reply_list = [start_date, end_date]
    return reply_list


reverse_par = {  # partition id to date range
    #               Function      Start   End
    "year": get_range_year,
    "years": get_range_year,
    "month": get_range_month,
    "months": get_range_month,
    "day": get_range_day,
    "days": get_range_day,
    "hour": get_range_hour,
    "hours": get_range_hour,
}


# ---------------------------------------------------------------
# Get the extension name of a table which is set into partitions
# ---------------------------------------------------------------
def get_table_extension(status, dbms_name, table_name, date_str):
    units_count, time_unit = get_partition_units(status, dbms_name, table_name)
    if units_count:
        extension = time_units[time_unit][0](status, date_str, units_count)
    else:
        extension = ""

    return extension


# ---------------------------------------------------------------
# Get the partitions units from partition_struct
# ---------------------------------------------------------------
def get_partition_units(status, dbms_name, table_name):
    key = dbms_name + '.' + table_name
    if key in partition_struct.keys():
        par_info = partition_struct[dbms_name + '.' + table_name]
    elif dbms_name in partition_struct.keys():
        # all tables of this dbms
        par_info = partition_struct[dbms_name]
    else:
        par_info = None

    if par_info:
        units_count = par_info[0]  # number of units (like: 7 days)
        time_unit = par_info[1]  # days / weeks / months/ year

    else:
        units_count = 0
        time_unit = ""

    reply_list = [units_count, time_unit]
    return reply_list


# ---------------------------------------------------------------
# Return True if the same partition
# ---------------------------------------------------------------
def is_same_partition(status, time_unit, units_count, date_old, date_new):
    info = time_units[time_unit]
    end_offset = info[2]  # the length of the time units
    if date_old[:end_offset] == date_new[:end_offset]:
        ret_val = True
    elif units_count == 1:
        ret_val = False  # dates at unit granularity not equal
    else:
        par_old = time_units[time_unit][0](status, date_old, units_count)
        par_new = time_units[time_unit][0](status, date_new, units_count)
        if par_old:
            # at least one name is not null
            ret_val = par_old == par_new
        else:
            ret_val = False
    return ret_val


# ---------------------------------------------------------------
# Extract the rows from the list that do not satisfy the provided partition info.
# Return a list of row which are to be assigned to a different partition
# ---------------------------------------------------------------
def split_list(status, column_id, time_unit, units_count, column_name, columns_list, insert_columns):
    other_list = []  # a list of data that is assigned to a different partition
    table_extension = ""
    ret_val = process_status.SUCCESS

    # Get the default date
    column_info = columns_list[column_id]
    default_declared = column_info[3]
    if default_declared == "current_timestamp":
        default_date = utils_columns.get_current_utc_time()
    else:
        default_date = default_declared

    # Determine the partition to organize by the first entry in the list
    date_column = insert_columns[0][column_id]

    # Get the date or the default value for the date
    first_date_str = date_column[1:-1] if date_column != "DEFAULT" else default_date

    if not is_valid_date_time(first_date_str, time_unit):
            # has a defualt value
        status.add_error(f"Invalid date value in JSON file: '{first_date_str}'")
        ret_val = process_status.ERR_wrong_json_structure
    else:
        # Pull all the entries assigned to the same partition
        for index, entry in enumerate(insert_columns):
            try:
                date_column = entry[column_id]
                date_new = date_column[1:-1] if date_column != "DEFAULT" else default_date
            except:
                status.add_error("Partition field has the wrong structure")
                ret_val = process_status.ERR_wrong_json_structure
                break

            if not is_same_partition(status, time_unit, units_count, first_date_str, date_new):
                other_list.append(entry)  # this entry is in a different partition
                insert_columns[index] = None

        if not ret_val:
            table_extension = "%s_%s%02u_%s" % (
            time_units[time_unit][0](status, first_date_str, units_count), time_unit[0], units_count, column_name)


    return [ret_val, other_list, table_extension]
# ---------------------------------------------------------------
# Partition a query to satisfy the physical partitioning of the data
# Return multiple queries against each partition
# ---------------------------------------------------------------
def partition_query(status, dbms_name, table_name, par_str, src_command, select_parsed):
    par_info = get_par_info(dbms_name, table_name)
    if par_info == "":
        status.add_error("Missing partition information on table: %s.%s" % (dbms_name, table_name))
        reply_list = [process_status.Missing_par_info, None]
        return reply_list

    # Get a list of partitions
    ret_val, par_list = par_str_to_list(status, dbms_name, table_name, par_str)
    if ret_val:
        reply_list = [ret_val, None]
        return reply_list

    # Get the values in the SQL stmt from which the partitining is derived
    par_field = par_info[2]  # The name of the partitioned field
    par_values = []  # An array with the values to consider
    partition_sql_by_field(status, par_field, src_command, select_parsed,
                           par_values)  # Split on the name of the partitioned field

    # Make a list of qiueries to execute
    sql_queries = make_partitioned_queries(status, par_info, dbms_name, table_name, par_list, par_values, src_command)

    reply_list = [ret_val, sql_queries]
    return reply_list


# ---------------------------------------------------------------
# Return a list of qiueries to execute
# ---------------------------------------------------------------
def make_partitioned_queries(status, par_info, dbms_name, table_name, par_list, sql_values, src_command):
    sql_queries = []

    index = src_command.find(" from ")

    if len(par_list) == 0 or index == -1:
        # No partitions or all partitions needs to be considered
        return sql_queries

    # Get offset of table name in the query - as table name needs to be replaced by the partition name

    offset_start, offset_end = utils_data.find_word_after(src_command, index + 6)  # GET Offset table name

    table_name_length = len(table_name)

    for partition in par_list:
        if "par_name" not in partition.keys():
            status.add_error("Error in the structure providing the list of partitions: %s" % str(par_list))
            return None
        par_name = partition["par_name"]
        if not sql_values:
            # The partition field is not part of the SQL - execute the SQL against all partitions
            sql_queries.append(src_command[:offset_start] + par_name + src_command[
                                                                       offset_end:])  # replace table name with partition name
        else:
            par_date = get_par_date_from_name(par_name, table_name_length)
            if is_par_with_data(status, sql_values, par_info, par_date):
                # Relevan partition
                sql_queries.append(src_command[:offset_start] + par_name + src_command[
                                                                           offset_end:])  # replace table name with partition name

    return sql_queries


# ---------------------------------------------------------------
# Test if the partition with relevant data
# sql_value - the date values in the SQL
# par_date - the time interval of the partition
# ---------------------------------------------------------------
def is_par_with_data(status, sql_info, par_info, par_date):
    start_date, end_date = par_date_to_range(status, par_info, par_date)

    # test if SQL dates are within the range of start_date and end_date
    ret_val = True  # no logic to fail the partition - try partition
    for conditions in sql_info:
        if len(conditions) == 1:
            # Only one date value in the SQL

            sql_opr = conditions[0][1]
            sql_value = conditions[0][2]
            ret_val = compare_one_date_to_par(sql_opr, sql_value, start_date, end_date)

        elif len(conditions) == 2:
            ret_val = True  # no logic to fail the partition - try partition

            if conditions[0][2] < conditions[1][2]:
                first_date = conditions[0][2]
                second_date = conditions[1][2]
                sql_opr_first = conditions[0][1][0]
                sql_opr_second = conditions[1][1][0]
            else:
                first_date = conditions[1][2]
                second_date = conditions[0][2]
                sql_opr_first = conditions[1][1][0]
                sql_opr_second = conditions[0][1][0]

            if sql_opr_first == sql_opr_second:
                # the 2 dates are < or > (note >= and <= are treated as > or <
                if sql_opr_first == '<':
                    ret_val = compare_one_date_to_par(sql_opr_first, first_date, start_date, end_date)
                elif sql_opr_first == '>':
                    ret_val = compare_one_date_to_par(sql_opr_first, second_date, start_date, end_date)
            elif sql_opr_first == '>' and sql_opr_second == '<':
                # a) Test if the SQL is within the partition
                # is first date in range
                ret_val = utils_columns.is_date_in_range(start_date, end_date, first_date)
                if not ret_val:
                    # is second date in range
                    ret_val = utils_columns.is_date_in_range(start_date, end_date, second_date)
                    if not ret_val:
                        # b) test if the partition is in the SQL range
                        ret_val = utils_columns.is_date_in_range(first_date, second_date, start_date)
                        if not ret_val:
                            # is second date in range
                            ret_val = utils_columns.is_date_in_range(first_date, second_date, end_date)
        if ret_val:
            break  # one of te conditions satisfy the partition - include the partition
    return ret_val


# ---------------------------------------------------------------
# Compare if the provided date/time fits to the partition
# ---------------------------------------------------------------
def compare_one_date_to_par(sql_opr, sql_value, start_date, end_date):
    if sql_opr == '=':
        ret_val = utils_columns.is_date_in_range(start_date, end_date, sql_value)
    elif sql_opr == '>' or sql_opr == '>=':
        ret_val = utils_columns.comarison_date_time[">="](end_date, sql_value)
    elif sql_opr == '<' or sql_opr == '<=':
        ret_val = utils_columns.comarison_date_time["<="](start_date, sql_value)

    return ret_val


# ---------------------------------------------------------------
# Transform the partitin date to start date and end date
# ---------------------------------------------------------------
def par_date_to_range(status, par_info, par_date):
    units_count = par_info[0]  # number of units (like: 7 days)
    time_unit = par_info[1]  # days / weeks / months/ year
    start_date, end_date = reverse_par[time_unit](status, par_date, units_count)
    return start_date, end_date


# ---------------------------------------------------------------
# Partition a SQL query by the partition field.
# Traverse the nodes of the where tree in select_parsed tree to find the values of the partition field
# Returns a list indicating the field values to be considered by the different partitions
# ---------------------------------------------------------------
def partition_sql_by_field(status, par_field, sql_stmt, select_parsed, par_values):
    root_node = select_parsed.get_where_tree()
    if root_node:
        # if root node is NULL, there is no where segment in the SQL
        root_node.traversal_where_tree(status, get_par_value, [par_field, par_values])


# ---------------------------------------------------------------
# Get the value of the partitioned field
# self represents a node in the where tree
# sql_segments is to be updated with the values to consider
# ---------------------------------------------------------------
def get_par_value(node, caller_data):
    par_field = caller_data[0]  # The name of the partition field
    sql_segments = caller_data[1]  # The array to update

    ret_val = False

    if node.and_array:
        par_conditions = []  # the conditions relating the partition field
        # since these are and conditions, it is sufficient that one condition is using the partition field
        for and_condition in node.and_array:
            # test if partition key exists with this node
            if is_with_par_field(and_condition, par_field):
                par_conditions.append(and_condition)
                ret_val = True
            else:
                sql_segments.clear()  # Remove all entries - Need to consider all partitions
                break  # Exit - no need to continue the traversal as all partitions needs to be visited
        if ret_val:
            # at least one condition is with the partition field
            ret_val = True
            sql_segments.append(par_conditions)
    else:
        if len(node.or_dict):
            # since these are 'OR' conditions, all the condition need to consider  the partition field
            sql_segments.clear()  # Remove all entries - Need to consider all partitions
            ret_val = False  # Exit - no need to continue the traversal as all partitions needs to be visited
        else:
            ret_val = True

    return ret_val


# ---------------------------------------------------------------
# Test if the partition field is in the condition considered
# ---------------------------------------------------------------
def is_with_par_field(condition, par_field):
    if condition[0] == par_field:
        return True
    index = condition[0].find(par_field)
    if index == -1:
        return False

    # Either space or brackets arround the par_field
    par_field_len = len(par_field)
    if index > 0 and (condition[0][index - 1] == ' ' or condition[0][index - 1] == '('):
        if len(condition[0]) > (index + par_field_len):
            if condition[0][index + par_field_len] == ' ' or condition[0][index + par_field_len] == ')':
                return True
    return False


# ---------------------------------------------------------------
# Base on the timeunit - validate the date - time
# YYYY-MM-DD HH
# ---------------------------------------------------------------
def is_valid_date_time(date_str, time_unit):

    if not date_str:
        return False

    if time_unit not in time_units.keys():
        return False

    components = time_units[time_unit][3]  # get the number of date components to validate

    if len(date_str) < 4:  # test year
        return False
    year = date_str[:4]
    if not year.isnumeric():
        return False
    year_int = int(year)
    if year_int < 1970 or year_int > 2500:
        return False
    if components == 1:
        return True

    if len(date_str) < 7:  # test month
        return False
    month = date_str[5:7]
    if not month.isnumeric():
        return False
    month_int = int(month)
    if month_int < 1 or month_int > 12:
        return False
    if components == 2:
        return True

    if len(date_str) < 10:  # test day
        return False
    day = date_str[8:10]
    if not day.isnumeric():
        return False
    day_int = int(day)
    if not day_int or day_int > monthrange(year_int, month_int)[1]:
        return False
    if components == 3:
        return True

    if len(date_str) < 13:  # test hour
        return False
    hour = date_str[11:13]
    if not hour.isnumeric():
        return False
    if int(hour) > 23:
        return False
    if components == 4:
        return True

    return False


# ---------------------------------------------------------------------------
# Map partition string to partition list
# ---------------------------------------------------------------------------
def par_str_to_list(status, dbms_name, table_name, par_str, key=None):
    # Get a JSON struct with the list of partitions
    par_json = utils_json.str_to_json(par_str)
    if not par_json:
        status.add_error("Failed to analyze partition information from table: %s.%s" % (dbms_name, table_name))
        reply_list = [process_status.Missing_par_info, None]
        return reply_list

    # Get a list of partitions
    par_list_dict = utils_json.get_value(par_json, "Partitions." + dbms_name + "." + table_name)
    if not par_list_dict:
        status.add_error("Failed to analyze partition information from table: %s.%s" % (dbms_name, table_name))
        reply_list =  [process_status.Missing_par_info, None]
        return reply_list

    if key:
        # Each entry is a dictionary, ket the value associated with the key
        par_list = []
        for entry in par_list_dict:
            par_list.append(entry[key])
    else:
        par_list = par_list_dict

    reply_list = [process_status.SUCCESS, par_list]
    return reply_list

# ---------------------------------------------------------------------------
# Filter a list of tables to extract the partition tables
# From the list provided, return a list that satisfies the partition definition
# The need to filter: A query of partitions from the dbms can pull table names that are not partitions
# Info type can be - all, first or last
# ---------------------------------------------------------------------------
def filter_list(status, dbms_name, table_name, par_str, info_type):
    par_info = get_par_info(dbms_name, table_name)
    if par_info == "":
        status.add_error("Missing partition information on table: %s.%s" % (dbms_name, table_name))
        reply_list = [process_status.Missing_par_info, ""]
        return reply_list

    ret_val, par_list = par_str_to_list(status, dbms_name, table_name, par_str)
    filtered_str = ""
    if not ret_val:
        filtered_list = []
        # Go over all entries to validate name
        counter = 0
        if info_type == "count":
            for par_name in par_list:
                if test_valid_name(par_info, dbms_name, table_name, par_name):
                    counter += 1
            filtered_str = str(counter)

        elif info_type == "last":
            # do in reverse order
            for par_name in reversed(par_list):
                if test_valid_name(par_info, dbms_name, table_name, par_name):
                    filtered_list.append(par_name)
                    filtered_str = par_name["par_name"]
                    break  # Only the last is needed

        elif info_type == "all" or info_type == "first":
            # Do all or first
            for par_name in par_list:
                if test_valid_name(par_info, dbms_name, table_name, par_name):
                    filtered_list.append(par_name)
                    if info_type == "first":
                        filtered_str = par_name["par_name"]
                        break  # Only the first is needed
        elif info_type == "dates":
            # Add the dates info
            table_name_length = len(table_name)
            for par_name in par_list:
                if test_valid_name(par_info, dbms_name, table_name, par_name):
                    par_date = get_par_date_from_name(par_name, table_name_length)
                    start_date, end_date = par_date_to_range(status, par_info, par_date)
                    par_name["start_date"] = start_date
                    par_name["end_date"] = end_date
                    filtered_list.append(par_name)
        else:
            ret_val = process_status.ERR_command_struct

        if not ret_val:
            if info_type != "last" and info_type != "first":
                if len(filtered_list):
                    satisfied_partitions = {"Partitions." + dbms_name + "." + table_name: filtered_list}
                    filtered_str = str(satisfied_partitions).replace("'", "\"")

    return ret_val, filtered_str


# ---------------------------------------------------------------------------
# Get info on the partitions -> returns an array with info on all partitions
# ---------------------------------------------------------------------------
def get_info_partitions(status, dbms_name, table_name, par_str):
    par_data = []
    par_info = get_par_info(dbms_name, table_name)
    if par_info == "":
        status.add_error("Missing partition information on table: %s.%s" % (dbms_name, table_name))
        return  None

    ret_val, par_list = par_str_to_list(status, dbms_name, table_name, par_str)
    if not par_list:
        status.add_error("Missing partition information on table: %s.%s" % (dbms_name, table_name))
        return  None

    if table_name != '*':
        table_name_length = len(table_name)
    for par_name in par_list:
        if table_name == '*':
            t_name = partition_name_to_table_name(par_name['par_name'], par_info)
            table_name_length = len(t_name)
        else:
            t_name = table_name
        if test_valid_name(par_info, dbms_name, t_name, par_name):
            par_date = get_par_date_from_name(par_name['par_name'], table_name_length)
            start_date, end_date = par_date_to_range(status, par_info, par_date)
            par_data.append([par_name['par_name'], start_date, end_date])

    return par_data

# ---------------------------------------------------------------------------
# Extract the table name from the partition name
# The structure of the partition name:
# par _ [table_name] _ [date/time key] _ [type] [counter] _ [column name]
# [date/time key] - depending if partitioned by year, month, day, hour
# [type] - One char for the type of partition: year, month, day, hour --> y/m/d/h
# [counter] - ie. every 2 hours or every 4 days
# Example: par_test_data_2019_10_15_17_h01_timestamp
# ---------------------------------------------------------------------------
def partition_name_to_table_name(par_name, par_info):
    global time_units

    key_type = par_info[1]      # year / month / day / hour
    suffix_length = 6           # the number of underscores to consider (after the table name) + 3 bytes of [type] + [counter] (i.e. every 3 days --> d03)
    suffix_length += time_units[key_type][2]    # adding the length of the data nd time string
    suffix_length += len(par_info[2])       # Add length of name of partitioned column
    table_name = par_name[4:-suffix_length]
    return table_name
# ---------------------------------------------------------------------------
# Get the table name from the partition name
# Returns: True if identified as partition + table name
# ---------------------------------------------------------------------------
def get_table_name_by_par_name(par_name):
    if par_name[:4] != "par_":
        reply_list = [False, par_name]         # not a partition
        return reply_list
    name_segments = par_name[4:].rsplit('_', 2)
    if len (name_segments) != 3:
        reply_list = [False, par_name]  # not a partition
        return reply_list
    # the segments are table_name + date/time key, [type] [counter], [column name]
    unit_char = name_segments[1][0]
    if unit_char not in char_to_time_unit:  # needs to be h / d / m / y
        reply_list = [False, par_name]  # not a partition
        return reply_list
    key_type = char_to_time_unit[unit_char]
    suffix_length = time_units[key_type][2] + 1
    table_name = name_segments[0][:-suffix_length]
    reply_list = [True, table_name]
    return reply_list
# ---------------------------------------------------------------------------
# Test if valid par name
# ---------------------------------------------------------------------------
def test_valid_name(par_info, dbms_name, table_name, par_name):
    global partition_struct

    column_name = par_info[2]       # The name of the column that makes the partition

    name = par_name["par_name"]
    index = len(name) - len(column_name) -1  # offset of the column name
    if index <= 5:
        return False

    name_length = index - 4  # Ignore the sufix
    par_prefix = "par_" + table_name
    offset = len(par_prefix)
    par_sufix = "%s%02u" % (par_info[1][0], par_info[0])  # EXAMPLE: d02 for every 2 days

    if not name.startswith(par_prefix) or not name[:index].endswith(par_sufix):
        ret_val = False
    else:
        time_unit = par_info[1]  # days / weeks / months/ year

        ret_val = True
        # cycle through time units in this order - year - month - day - hour to determine the component of each
        for entry in time_units_list:
            unit = entry[0]
            start_offset = entry[1]
            end_offset = entry[2]

            unit_length = (end_offset - start_offset) + 1
            needed_length = offset + unit_length

            if name_length < needed_length:
                # Must have the unit info
                ret_val = False
                break
            if name[offset] != "_":
                ret_val = False
                break
            if not name[offset + 1:needed_length].isnumeric():
                ret_val = False
                break
            if time_unit.startswith(unit):
                if name_length == needed_length:
                    break
                else:
                    ret_val = False
                    break
            offset += unit_length

    return ret_val


# ---------------------------------------------------------------------------
# Given a date range Partition the date to N intervals
# Logic:
# Get no. of seconds from epoc for each date,
# get the difference start to end
# divide the difference by intervals
# calculate the dates intervals
# ---------------------------------------------------------------------------
def partition_date_range(status, column_name, start_date, start_included, end_date, end_included, intervals):
    start_in_sec = utils_columns.get_time_in_seconds(start_date)
    if not start_in_sec:
        status.add_error("Failed to recognize the date value: '%s'" % start_date)
        reply_list = [process_status.ERR_SQL_failure, None]
        return reply_list

    end_in_sec = utils_columns.get_time_in_seconds(end_date)
    if not start_in_sec:
        status.add_error("Failed to recognize the date value: '%s'" % end_date)
        reply_list = [process_status.ERR_SQL_failure, None]
        return reply_list

    dates_list = []

    diff = end_in_sec - start_in_sec

    sec_split = diff / intervals
    remainder = diff - sec_split * intervals

    seconds = start_in_sec
    for i in range(intervals):

        if not i:
            # first date
            start = start_date
            if start_included:
                start_operator = ">="
            else:
                start_operator = ">"
        else:
            start = utils_columns.seconds_to_date(seconds)
            start_operator = ">="

        seconds += sec_split
        if remainder:
            seconds += 1
            remainder -= 1

        if i == intervals - 1:
            end = end_date
            if end_included:
                end_operator = "<="
            else:
                end_operator = "<"
        else:
            end = utils_columns.seconds_to_date(seconds)
            end_operator = "<"

        dates_list.append((start, start_operator, end, end_operator))

    reply_list = [process_status.SUCCESS, dates_list]
    return reply_list
# ---------------------------------------------------------------------------
# Get the partition date from the partition name
# The structure of the partition name is:
#  par_Table_Date_Type_Column
#  Example: 'par_ping_sensor_2019_07_01_d07_timestamp'
# ---------------------------------------------------------------------------
def get_par_date_from_name(par_name, table_name_length):
    index = par_name.rfind('_')  # offset to "_" before partition column name
    if index < (8 + table_name_length):
        par_date = ""
    else:
        par_date = par_name[5 + table_name_length: index - 4]

    return par_date
