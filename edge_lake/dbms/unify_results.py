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
import os
import sys
# from email.errors import NonASCIILocalPartDefect

import anylog_node.dbms.db_info as db_info
import anylog_node.dbms.projection_entry as projection_entry
import anylog_node.generic.utils_sql as utils_sql
import anylog_node.generic.utils_columns as utils_columns
import anylog_node.generic.utils_data as utils_data


# ==================================================================
# Create a dbms table, on the publisher node, per each issued query.
# The table name inclused the job number.
# The result from each operator is inserted to the table.
# When all results are returned (from all operators), query the table
# to return a unified result to the user/app.
# ==================================================================

# ==================================================================
# Map data types
# ==================================================================
unify_data_types = {
    "uuid": "character varying",
}


# ==================================================================
# Process a sql function that is translated to one function
# ==================================================================
def generic_function(select_parsed, projection, function_id, remote_dbms, remote_table, function, description, details,
                     as_name, new_type):
    if not description:
        return ["", ""]

    if description[-1] == ')':
        # Test for casting - Int16(column_name) --> get column_name
        name_pulled = utils_data.get_iner_brackets(description, '(', ')')
    else:
        name_pulled = description

    column_type = db_info.get_column_type(remote_dbms, remote_table, name_pulled)

    if column_type == "":
        return ["", ""]

    new_field_name = function + "_" + str(function_id)

    local_query = (function + "(" + new_field_name + ")")
    if select_parsed.get_dbms_type() == "oledb.pi":
        # PI specific format
        remote_query = "%s(cast(%s as %s))" % (function, description, "int32")
    else:
        remote_query = (function + "(" + description + ")")
    local_create = (new_field_name + " " + column_type)

    if as_name != "":
        remote_query += " as " + as_name
        local_query += " as " + as_name

    projection.set_projection_info(name_pulled, new_field_name, remote_query, remote_query, local_create, local_query, 1, as_name)

    return [new_field_name, column_type]

# ==================================================================
# Distinct function -
# The local query is changed to include column name
# And group by is updated with exxtended fields and the distinct column
# Query: run client () sql lsl_demo include=(percentagecpu_sensor) and extend=(@table_name as table) "SELECT DISTINCT(value) FROM ping_sensor"
# Local query: sql system_query select table_name, distinct_1  from query_13 group by table_name, distinct_1
# ==================================================================
def distinct_function(select_parsed, projection, function_id, remote_dbms, remote_table, function, description, details,
                     as_name, new_type):
    if description[-1] == ')':
        # Test for casting - Int16(column_name) --> get column_name
        name_pulled = utils_data.get_iner_brackets(description, '(', ')')
    else:
        name_pulled = description

    column_type = db_info.get_column_type(remote_dbms, remote_table, name_pulled)

    if column_type == "":
        return ["", ""]

    new_field_name = function + "_" + str(function_id)

    local_query = new_field_name # Without the function as we add group by

    projection.add_local_group_by_column(new_field_name, False) # Add the distinct key to the local group by column in the local query

    if select_parsed.get_dbms_type() == "oledb.pi":
        # PI specific format
        remote_query = "%s(cast(%s as %s))" % (function, description, "int32")
    else:
        remote_query = (function + "(" + description + ")")
    local_create = (new_field_name + " " + column_type)

    if as_name != "":
        remote_query += " as " + as_name
        local_query += " as " + as_name

    projection.set_projection_info(name_pulled, new_field_name, remote_query, remote_query, local_create, local_query, 1, as_name)

    return [new_field_name, column_type]

# ==================================================================
# Process a date_trunc function that is translated to one function
# ==================================================================
def date_trunc(select_parsed, projection, function_id, remote_dbms, remote_table, function, description, details,
               as_name, new_type):

    offset = description.find(',')      # Find a comma that splits between the the truncate type and the column name
    if offset > 0 and offset < (len(description) - 2):
        # For example: '\'hour\',timestamp'
        description_list = description.split(',')
        if len(description_list) == 2:
            column_name = description_list[1].strip()
            column_type = db_info.get_column_type(remote_dbms, remote_table, column_name)
            trunc_type = description_list[0].strip()
            if len(trunc_type) > 2 and trunc_type[0] == "'" and trunc_type[-1] == "'":
                trunc_type = trunc_type[1:-1]       # Remove quotation
        else:
            return ["", ""]
    else:
        # Get name + type separated by space
        description_list = description.split()
        if len(description_list) == 2:
            column_type = description_list[1]
            trunc_type = description_list[0]
        else:
            return ["", ""]

    new_field_name = function + "_" + str(function_id)

    local_query = function + "(" + trunc_type + new_field_name + ")"

    remote_query = (function + "(" + description + ")")
    local_create = (new_field_name + " " + column_type)

    if as_name != "":
        remote_query += " as " + as_name
        local_query += " as " + as_name

    projection.set_projection_info(description, new_field_name, remote_query, remote_query, local_create, local_query, 1, as_name)

    return [new_field_name, column_type]


# ==================================================================
# Process an extract function
# ==================================================================
def extract_function(select_parsed, projection, function_id, remote_dbms, remote_table, function, description, details,
                     as_name, new_type):
    description_list = description.split()

    if new_type:
        column_type = new_type
    else:
        if len(description_list) == 3:
            column_name = description_list[2]
            column_type = db_info.get_column_type(remote_dbms, remote_table, column_name)
        else:
            return ["", ""]

    new_field_name = function + "_" + str(function_id)

    local_query = new_field_name

    remote_query = (function + "(" + description + ")")
    local_create = (new_field_name + " " + column_type)

    if as_name != "":
        remote_query += " as " + as_name
        local_query += " as " + as_name

    projection.set_projection_info(description, new_field_name, remote_query, remote_query, local_create, local_query, 1, as_name)

    return [new_field_name, column_type]


# ==================================================================
# Replace the count field name
# ==================================================================
def count_sql(select_parsed, projection, function_id, remote_dbms, remote_table, function, description, details,
              as_name, new_type):
    if details.startswith("distinct ") or details.startswith("distinct("):

        if len(details) < 10:
            return ["", ""]

        column_type = db_info.get_column_type(remote_dbms, remote_table, description)
        if column_type == "":
            return ["", ""]

        remote_query = details

        new_field_name = "distinct_" + str(function_id)

        local_query = (" count (distinct " + new_field_name + ")")

        local_create = (new_field_name + " " + column_type)

    else:

        if description == "*":
            remote_query = function + "(*)"
            new_field_name = "count_all"
        else:
            remote_query = function + "(" + description + ")"
            new_field_name = "count_func_" + str(function_id)

        local_create = new_field_name + " integer "

        local_query = "sum(" + new_field_name + ")"

    if as_name != "":
        remote_query += " as " + as_name
        local_query += " as " + as_name

    projection.set_projection_info(description, new_field_name, remote_query, remote_query, local_create, local_query, 1, as_name)

    return [new_field_name, "integer"]


# ==================================================================
# process average
# ==================================================================
def avg_sql(select_parsed, projection, function_id, remote_dbms, remote_table, function, description, details, as_name,
            new_type):
    if select_parsed.is_replace_avg():
        # replace avg with count and sum

        column_name = description
        column_type = db_info.get_column_type(remote_dbms, remote_table, column_name)
        if column_type == "":
            return ["", ""]

        new_column_name = column_name.replace(".", "_")  # PI can have names with "."
        new_field_name = "SUM__" + new_column_name

        if select_parsed.get_dbms_type() == "oledb.pi":
            # PI specific format
            remote_query = ("SUM" + "(cast(" + column_name + " as int32)), COUNT(cast(" + column_name + " as int32))")
        else:
            remote_query = ("SUM" + "(" + column_name + "), COUNT(" + column_name + ")")

        local_create = (new_field_name + " " + "numeric" + ", COUNT__" + new_column_name + " integer")

        if db_info.get_db_type("system_query") == "sqlite":
            local_query = ("SUM( CAST(" + new_field_name + " AS FLOAT)) /NULLIF(SUM(COUNT__" + new_column_name + "),0)")
        else:
            local_query = ("SUM(" + new_field_name + ") /NULLIF(SUM(COUNT__" + new_column_name + "),0)")

        if as_name != "":
            remote_query += " as " + as_name
            local_query += " as " + as_name

        projection.set_projection_info(column_name, new_field_name, remote_query, remote_query, local_create, local_query, 2, as_name)

    else:
        # query is only on this node - avg returns float
        new_field_name, new_data_type = generic_function(select_parsed, projection, function_id, remote_dbms,
                                                         remote_table, function, description, details, as_name,
                                                         new_type)

    return [new_field_name, "float"]


# ==================================================================
# process RANGE - |MAX - MIN|
# ==================================================================
def range_function(select_parsed, projection, function_id, remote_dbms, remote_table, function, description, details,
                   as_name, new_type):
    column_name = description
    column_type = db_info.get_column_type(remote_dbms, remote_table, column_name)
    if column_type == "":
        return ""

    new_field_name = "RANGE__" + column_name

    remote_query = ("MIN" + "(" + column_name + "), MAX(" + column_name + ")")

    local_create = (new_field_name + "_MIN " + column_type + ", " + new_field_name + "_MAX " + column_type)

    if db_info.get_db_type("system_query") == "sqlite":
        local_query = ("abs(MAX (" + new_field_name + "_MAX ) - MIN (" + new_field_name + "_MIN ))")
    else:
        local_query = ("abs(MAX (" + new_field_name + "_MAX )::numeric - MIN (" + new_field_name + "_MIN )::numeric)")

    if as_name != "":
        remote_query += " as " + as_name
        local_query += " as " + as_name

    projection.set_projection_info(column_name, new_field_name, remote_query, remote_query, local_create, local_query, 2, as_name)

    return [new_field_name, column_type]


# ==================================================================
# process Increment - Group by time increments
# Function shows increment(time_unit, interval, column_name)
# For postgres - date_trunc('hour',column_name), (extract(minute FROM column_name)::int /5)
# Example Query:
# 'select date_trunc(\'day\',timestamp), (extract(hour FROM timestamp)::int / 1), max(timestamp), SUM(value), COUNT(value)
# from ping_sensor where timestamp >= \'2020-06-01 19:34:09\' and timestamp < \'2020-09-29 19:34:09\' group by 1,2'
# ==================================================================
def increment_function(select_parsed, projection, function_id, remote_dbms, remote_table, function, description,
                       details, as_name, new_type):
    time_interval = 0  # interval duration
    if description != "":
        details = description.replace(' ', '').split(",")  # remove spaces and split on the commas
        if len(details) == 3:
            if details[1].isdecimal():
                time_unit = details[0]  # second, minute, hour etc.
                if time_unit in utils_sql.increment_date_types:
                    column_name = details[2]
                    time_interval = int(details[1])
                    trunc_time = utils_sql.increment_date_types[time_unit]
                    if time_unit == "week":
                        time_unit = "day"
                        time_interval *= 7

    if not time_interval:
        return ["", ""]

    column_type = db_info.get_column_type(remote_dbms, remote_table, column_name)

    new_field_name = function + "_" + str(function_id)

    remote_query = get_remote_query_increment(select_parsed, time_unit, trunc_time, column_name, time_interval)

    user_order_by = select_parsed.get_order_by()
    if user_order_by:
        if not select_parsed.is_ascending():
            user_order_by += " desc, "
        else:
            user_order_by += ", "

    if time_unit != 'year':

        local_create = (new_field_name + "_trunc " + column_type + ", " + new_field_name + "_extract " + "integer")
        counter_local_fields = 2

        if as_name != "":
            local_query = ("%s_trunc as %s_%u, %s_extract as %s_%u" % (
            new_field_name, as_name, function_id, new_field_name, as_name, function_id))
            projection.set_local_group_by("group by 1,2")
            projection.set_local_order_by("order by %s1,2", user_order_by)
        else:
            local_query = ""  # no need in group by / order by keys
            projection.set_local_group_by("group by %s,%s" % (new_field_name + "_trunc", new_field_name + "_extract"))
            projection.set_local_order_by("order by %s%s,%s" % (user_order_by, new_field_name + "_trunc", new_field_name + "_extract"))

        projection.set_remote_group_by("group by 1,2")
    else:
        # year - no need in truncate

        local_create = (new_field_name + "_extract " + "integer")
        counter_local_fields = 1

        if as_name != "":
            local_query = (new_field_name + "_extract as %s_%u" % (as_name, function_id))
            projection.set_local_group_by("group by 1")
            projection.set_local_order_by("order by %s1", user_order_by)
        else:
            local_query = ""
            projection.set_local_group_by("group by " + new_field_name + "_extract")
            projection.set_local_order_by("order by " + user_order_by + new_field_name + "_extract")

        projection.set_remote_group_by("group by 1")

    g_query = function + "(" + description + ')'  # keep original call

    projection.set_projection_info(column_name, new_field_name, remote_query, g_query, local_create, local_query, counter_local_fields, "")

    return [new_field_name, ""]


# ==================================================================
# For a query using INCREMENT - Get the increment based on the remote dbms
# ==================================================================
def get_remote_query_increment(select_parsed, time_unit, trunc_time, column_name, time_interval):
    if select_parsed.get_dbms_type() == "sqlite":
        extract_format = utils_columns.get_time_extract_format(time_unit)
        if time_unit != 'year':
            trunc_format = utils_columns.get_time_trunc_format(trunc_time)
            remote_query = "strftime('%s',%s), (cast (strftime('%s',%s) as decimal) / %u)" % (
            trunc_format, column_name, extract_format, column_name, time_interval)
        else:
            remote_query = "(cast (strftime('%s',%s) as decimal) / %u)" % (extract_format, column_name, time_interval)
    else:
        # default is Posrgres
        if time_unit != 'year':
            remote_query = "date_trunc('%s',%s), (extract(%s FROM %s)::int / %u)" % (
            trunc_time, column_name, time_unit, column_name, time_interval)
        else:
            remote_query = "(extract(%s FROM %s)::int / %u)" % (time_unit, column_name, time_interval)

    return remote_query


# ==================================================================
# process a function without a name
# example: 'extract(minute from timestamp)::int / 5'
# ==================================================================
def no_name_function(select_parsed, projection, function_id, remote_dbms, remote_table, function, description, details,
                     as_name, new_type):
    source_sql = description.split()

    # 1) Get the sub function

    is_supported, is_function, name_pulled, sub_description, words_count, as_name = utils_sql.process_variable(
        source_sql)
    if not is_supported:
        return ["", ""]

    column_type = ""
    offset = len(name_pulled) + len(sub_description) + 2  # the 2 are for the parenthesis
    if offset + 2 < len(description):
        additional_info = True
        if description[offset:offset + 2] == "::":  # get the data type
            column_type = utils_sql.get_data_type(
                description[offset + 2:])  # example: get integer from 'extract(minute from timestamp)::int / 5'
    else:
        additional_info = False

    # 2) Process the sub function

    if name_pulled in sql_functions:
        new_field_name, new_data_type = sql_functions[name_pulled](projection, function_id, remote_dbms, remote_table,
                                                                   name_pulled, sub_description, "", as_name,
                                                                   column_type)
    else:
        return ["", ""]

    if additional_info:
        projection.add_remote_query_info(description[offset:])
        projection.add_generic_query_info(description[offset:])

    if as_name != "":
        projection.add_remote_query_info(" as " + as_name)
        projection.add_generic_query_info(" as " + as_name)
        projection.add_local_query_info(" as " + as_name)

    return [new_field_name, column_type]


# ==================================================================
# FUNCTIONS SUPPORTED
# ==================================================================
sql_functions = {
    '': no_name_function,
    'count': count_sql,
    'avg': avg_sql,
    'min': generic_function,
    'max': generic_function,
    'sum': generic_function,
    'distinct': distinct_function,
    'range': range_function,
    'increments': increment_function,
    'date_trunc': date_trunc,
    'extract': extract_function
}


# ==================================================================
# AnyLog timastamp function
# Example: run client (!ip 2048) sql lsl_demo "select * from ping_sensor where timestamp = date(date('now','start of month','+1 month','-1 day', '-2 hours', '+2 minuts'));"
# Example SELECT date('now','start of month','+1 month','-1 day', '-2 hours', '+2 minuts');
# ==================================================================
def al_timestamp_function(select_parsed, description, table_name, updated_column_names, local_table_name):
    # use AnyLog function to calculate the date
    calc_time = utils_columns.function_to_time(description)
    if calc_time:
        date_str = "'" + calc_time + "'"
    else:
        # not an AnyLog function - keep as is
        date_str = "timestamp(" + description + ")"

    return [date_str, date_str, date_str]


# ==================================================================
# AnyLog Date function
# Example: run client (!ip 2048) sql lsl_demo "select * from ping_sensor where timestamp = date(date('now','start of month','+1 month','-1 day', '-2 hours', '+2 minuts'));"
# Example SELECT date('now','start of month','+1 month','-1 day', '-2 hours', '+2 minuts');
# ==================================================================
def al_date_function(select_parsed, description, table_name, updated_column_names, local_table_name):
    # use AnyLog function to calculate the date
    calc_time = utils_columns.function_to_time(description)
    if calc_time:
        date_str = "'" + calc_time[0:10] + "'"
    else:
        # not an AnyLog function - keep as is
        date_str = "date(" + description + ")"

    return [date_str, date_str, date_str]


# ==================================================================
# Find the date range and organize it as a subselect
# Example: select max(time_column), function(column) from table_name where period(interval, count, date_time, time_column);
# Explanation:
# Use the date_time specified in “period” to find the first instance with date_time which is less than or equal to the date_time.
# Subtract (count * interval) from the date of the first instance and calculate the function over all the rows within the range.
# For Row Data - replace max(time_column) with time_column and  function(column) with column.
#
# The SQL:
# SELECT timestamp, value FROM ping_sensor
# WHERE timestamp < (SELECT MAX(timestamp) FROM ping_sensor WHERE timestamp < NOW())
#     and timestamp > (SELECT MAX(timestamp) FROM ping_sensor WHERE timestamp < NOW()) - interval '1 day'
# order by timestamp  desc limit 10;
#
# ==================================================================
def period_time_frame(select_parsed, description, table_name, updated_column_names, local_table_name):
    if not len(description):
        return ["", "", ""]
    # description needs to show: (interval, count, date_time, time_column)
    details = description.split(",")  # remove spaces and split on the commas

    filter = ""
    if len(details) != 4:
        if len(details) != 5:
            return ["", "", ""]
        filter = details[4]  # add filter criteria

    time_unit = details[0].strip()  # second, minute, hour etc.
    if time_unit not in utils_sql.increment_date_types:
        return ["", "", ""]
    count = details[1].strip()
    if time_unit == "week":
        time_unit = "day"
        count *= 7

    number = details[1].strip()
    if not number.isdecimal():
        return ["", "", ""]
    count = int(number)
    if not count:
        return ["", "", ""]

    date_time = details[2].strip()
    if date_time == "now()":
        end_date_time = '\'' + utils_columns.get_current_utc_time() + '\''  # date format  \'2019-07-29 19:34:00\'
    else:
        if len(date_time) > 11 and date_time[11] == 'T':
            end_date_time = date_time       # In UTC
        else:
            end_date_time = "'" + utils_columns.local_to_utc(date_time[1:-1]) + "'"

    column_name = details[3].strip()

    if select_parsed.with_leading_queries():
        # use leading query
        if filter == "":
            leading = "sql %s text select max(%s) from %s where %s <= %s" % (
            select_parsed.remote_dbms, column_name, table_name, column_name, end_date_time)
            sub_sql_remote = "%s > %s and %s <= %s" % (column_name, "%s", column_name, "%s")
        else:
            leading = "sql %s text select max(%s) from %s where %s <= %s %s" % (
            select_parsed.remote_dbms, column_name, table_name, column_name, end_date_time, filter)
            sub_sql_remote = "%s > %s and %s <= %s %s" % (column_name, "%s", column_name, "%s", filter)

        l_query = select_parsed.get_new_leading_query()  # get leading query
        l_query.set_leading_query(leading)
        l_query.set_period_function(time_unit, count)

        # Add MAX function to LEADING QUERY
        function = utils_columns.functions_classes["max"]("max", column_name, "date")
        l_query.get_projection_functions().update_projection_functions(column_name, function, 1)

    else:
        sub_sql_remote = get_remote_query_period(select_parsed, column_name, table_name, end_date_time, filter, count,
                                                 time_unit)

    if column_name in updated_column_names:
        if select_parsed.with_leading_queries():
            sub_sql_local = ""
        else:
            # change the local query with the where condition
            sub_sql_local = get_local_query_period(select_parsed, updated_column_names, column_name, local_table_name,
                                                   end_date_time, count, time_unit)
    else:
        sub_sql_local = ""

    return [sub_sql_remote, sub_sql_local, ""]


# ==================================================================
# For a query using PERIOD - Get the period where condition on the remote dbms
# ==================================================================
def get_remote_query_period(select_parsed, column_name, table_name, end_date_time, filter, count, time_unit):
    if select_parsed.get_dbms_type() == "sqlite":

        # without leading query
        if filter == "":
            sub_sql_remote = "%s > datetime ( (SELECT MAX(%s) FROM %s WHERE datetime(%s) <= datetime(%s)) , '-%u %s' ) and datetime(%s) <= datetime ( (SELECT MAX(%s) FROM %s WHERE datetime(%s) <= datetime(%s)) )" \
                             % (column_name, column_name, table_name, column_name, end_date_time, count, time_unit,
                                column_name, column_name, table_name, column_name, end_date_time)

        else:  # Filter example - ' and device_name = \'APC SMART X 3000\''
            sub_sql_remote = "%s > datetime ( (SELECT MAX(%s) FROM %s WHERE datetime(%s) <= datetime(%s) %s) , '-%u %s' ) and datetime(%s) <= datetime( (SELECT MAX(%s) FROM %s WHERE datetime(%s) <= datetime(%s) %s) ) %s" \
                             % (column_name, column_name, table_name, column_name, end_date_time, filter, count,
                                time_unit, column_name, column_name, table_name, column_name, end_date_time, filter,
                                filter)

    else:
        # default is Posrgres

        # timestamp > (SELECT MAX(timestamp) FROM ping_sensor WHERE timestamp <=
        # timestamp '2020-05-28 18:56:49.890199'  and device_name =
        # 'REMOTE-SERVER') - interval '1 minute' and timestamp <=
        # (SELECT MAX(timestamp) FROM ping_sensor WHERE timestamp <=
        # timestamp '2020-05-28 18:56:49.890199'  and device_name = 'REMOTE-SERVER')
        # and device_name = 'REMOTE-SERVER'

        # without leading query
        if filter == "":
            sub_sql_remote = "%s > (SELECT MAX(%s) FROM %s WHERE %s <= timestamp %s) - interval '%u %s' and %s <= (SELECT MAX(%s) FROM %s WHERE %s <= timestamp %s)" \
                             % (column_name, column_name, table_name, column_name, end_date_time, count, time_unit,
                                column_name, column_name, table_name, column_name, end_date_time)

        else:  # Filter example - ' and device_name = \'APC SMART X 3000\''
            sub_sql_remote = "%s > (SELECT MAX(%s) FROM %s WHERE %s <= timestamp %s %s) - interval '%u %s' and %s <= (SELECT MAX(%s) FROM %s WHERE %s <= timestamp %s %s) %s" \
                             % (
                             column_name, column_name, table_name, column_name, end_date_time, filter, count, time_unit,
                             column_name, column_name, table_name, column_name, end_date_time, filter, filter)

    return sub_sql_remote


# ==================================================================
# For a query using PERIOD - Get the period where condition on the local dbms
# ==================================================================
def get_local_query_period(select_parsed, updated_column_names, column_name, local_table_name, end_date_time, count,
                           time_unit):

    # create a local query
    if select_parsed.get_dbms_type() == "sqlite":
        # from query_8 where max_1 >
        # datetime( (SELECT MAX(max_1) FROM query_8 WHERE max_1 <=
        # '2020-05-26 12:17:42.201668') , '-1 minutes' ) and max_1 <=
        # (SELECT MAX(max_1) FROM query_8 WHERE max_1 <= '2020-05-26 12:17:42.201668');

        local_column_name = updated_column_names[column_name]
        sub_sql_local = "%s > datetime( (SELECT MAX(%s) FROM %s WHERE %s <= %s) , '-%u %s') and %s <= (SELECT MAX(%s) FROM %s WHERE %s <= %s)" \
                        % (local_column_name, local_column_name, local_table_name, local_column_name,
                           end_date_time, count, time_unit, local_column_name, local_column_name,
                           local_table_name, local_column_name, end_date_time)
    else:
        # default is Posrgres

        # from query_8 where max_1 >
        # (SELECT MAX(max_1) FROM query_8 WHERE max_1 <=
        # timestamp '2020-05-26 12:17:42.201668') - interval '1 minute' and max_1 <=
        # (SELECT MAX(max_1) FROM query_8 WHERE max_1 <= timestamp '2020-05-26 12:17:42.201668')

        local_column_name = updated_column_names[column_name]
        sub_sql_local = "%s > (SELECT MAX(%s) FROM %s WHERE %s <= timestamp %s) - interval '%u %s' and %s <= (SELECT MAX(%s) FROM %s WHERE %s <= timestamp %s)" \
                        % (local_column_name, local_column_name, local_table_name, local_column_name,
                           end_date_time, count, time_unit, local_column_name, local_column_name,
                           local_table_name, local_column_name, end_date_time)


    return sub_sql_local


# ==================================================================
# Replace NOW() with Current Date and Time
# ==================================================================
def set_current_date_time(select_parsed, description, table_name, updated_column_names, local_table_name):
    current_time = utils_columns.get_current_utc_time()
    return [current_time, "", current_time]


# ==================================================================
# FUNCTIONS SUPPORTED on the WHERE CLAUSE
# ==================================================================
where_functions = {
    'period': period_time_frame,
    'now': set_current_date_time,
    'date': al_date_function,  # proprietry management of date
    'timestamp': al_timestamp_function  # proprietry management of timestamp
}


# ==================================================================
# Based on the issued query - create the needed SQL:
# 1) For the Operators - an updated SQL to process (Average is replaced to count and sum)
# 2) For the locla database: The create table for the local database
# 3) For the local database: The query to issue

# Note: source_sql is formatted in utils_sql.format_select_sql()

# ==================================================================
def make_sql_stmt(status, select_parsed, is_suport_join):
    '''

    select_parsed - a structure containing the parsed info
    is_suport_join - enabled for specific dbms options (not a generic property)
    '''
    updated_column_names = {}  # hash table that maps source column names to new names
    projection_list = []

    projection_count = len(select_parsed.projection_parsed)  # generic functions
    proprietry_count = len(select_parsed.proprietary_functions)  # proprietry functions like increments
    total_projections = projection_count + proprietry_count
    function_id = 0
    output_counter = 0
    extended_group_by = ""
    extended_list = []          # the list of extendec columns
    projection_id = 0  # the id of the field which will be projected to the caller
    counter_local_fields = 0

    pass_through = True             # Remains True if no functions involved and no order by or group by

    projection_names = {}              # The dictionary prevent same names on the projection list

    extended_columns = select_parsed.get_extended_columns()
    if extended_columns:
        # Extend the column values returned in the query with the extended values based on status and state values in the node processing the query
        # Example: extend = (@ip as Node IP, @port, @DBMS, @table, !!disk_space.int as space)
        ret_val, query_title, query_data_types, query_columns = process_extended_columns(status, extended_columns)
        if not ret_val:
            return False

        if select_parsed.is_select_all():
            # "SELECT * from ..." - does not need to include columns names in the projection list
            include_projection = False
        else:
            include_projection = True   # Add extended columns to the projection list


        for index, column_name in enumerate(query_columns):
            # Add the columns from the extend directive
            projection = projection_entry.ProjectionEntry(output_counter)
            projection.set_projection_info(column_name, "", "", "", "%s %s" % (column_name, query_data_types[index]) , column_name, 1, "")
            projection.set_projection_flag(include_projection)
            projection_list.append(projection)
            extended_group_by += (column_name + ',')
            extended_list.append(column_name)
    else:
        query_title = ""  # the list of fields participating in the query
        query_data_types = []
        query_columns = []


    j = 0
    for i in range(total_projections):

        title_name = ""     # the projected title for the column (could be column name, or as name or function name)
        projection = projection_entry.ProjectionEntry(output_counter)

        if i < proprietry_count:
            # take the fumction from the proprietry functions (like increments)
            projection_col_info = select_parsed.get_proprietry_func(i)
            function_name = projection_col_info[0]
            name_pulled = projection_col_info[1]
            as_name = ""
            details = ""
        else:
            # take a generic function like min, max
            projection_col_info = select_parsed.get_projection_col_info(j)
            j += 1
            name_pulled = projection_col_info[0]
            function_name = projection_col_info[1]
            as_name = projection_col_info[2]  # projection_col_info is organized in utils_sql.process_projection()
            details = projection_col_info[3]

        if function_name:
            # This is a function
            pass_through = False
            function_id += 1
            projection.set_function(function_name)

            if not function_name in sql_functions:
                # keep the function as is
                new_column_name, new_data_type = generic_function(select_parsed, projection, function_id,
                                                                  select_parsed.remote_dbms, select_parsed.remote_table,
                                                                  function_name, name_pulled, "", as_name, "")
            else:
                new_column_name, new_data_type = sql_functions[function_name](select_parsed, projection, function_id,
                                                                              select_parsed.remote_dbms,
                                                                              select_parsed.remote_table, function_name,
                                                                              name_pulled, details, as_name, "")

            if new_data_type:
                # increment function is not projected to the user and is not included in the data types of the columns retrieved
                query_data_types.append(new_data_type)

            if new_column_name == "" or new_column_name.find(' ') != -1:
                # need to provide a column name which is a single word
                err_msg = "Non supported SQL: Error parsing SQL stmt near function: \"%s(%s)\"" % (
                function_name, name_pulled)
                status.add_error(err_msg)
                status.keep_error(err_msg)
                return False

            if projection.is_function_projected():
                if query_title:
                    query_title += ","
                if as_name != "":
                    title_name = as_name
                    query_title += as_name
                elif details:
                    title_name = (function_name + "(" + details + ")")
                    query_title += title_name
                else:
                    title_name = (function_name + "(" + name_pulled + ")")
                    query_title += title_name
                projection_id += 1

            if name_pulled.find(' ') == -1:  # description is a single word
                if not name_pulled in updated_column_names:
                    # Map the column name to the column name in the local table
                    updated_column_names[name_pulled] = new_column_name

        else:
            # column name or all columns

            if name_pulled == '*':

                counter_local_fields += db_info.get_counter_fields(select_parsed.remote_dbms,
                                                                   select_parsed.remote_table)

                l_create = db_info.get_create_fields(select_parsed.remote_dbms, select_parsed.remote_table,
                                                     unify_data_types)

                title, table_data_types, projection_id = db_info.get_fields_names_types(projection_id,
                                                                                        select_parsed.remote_dbms,
                                                                                        select_parsed.remote_table,
                                                                                        False)
                query_data_types = query_data_types +table_data_types       # add to the extended columns

                if query_title:
                    query_title += (',' + title)
                else:
                    query_title += title

                projection.set_projection_info("", "", "*", "*", l_create, "*", 0, "")

                i += 1

            else:
                if select_parsed.is_use_view():  # returns True if metadata maintained in view
                    # Get the local column name and the data type from the view
                    index, view_column_info = db_info.get_view_column_info(select_parsed.remote_dbms,
                                                                           select_parsed.remote_table, name_pulled)
                    if not view_column_info:
                        err_msg = "Non supported SQL: Column '%s' is not recognized in view: %s.%s" % (
                        name_pulled, select_parsed.remote_dbms, select_parsed.remote_table)
                        status.add_keep_error(err_msg)
                        return False
                    local_name = view_column_info[2]  # Get the Local name of the column from the VIEW declaration
                    if not local_name:
                        continue  # Skip this column
                    column_type = view_column_info[1]  # Get the data type of the column from the VIEW declaration
                else:
                    column_type = db_info.get_column_type(select_parsed.remote_dbms, select_parsed.remote_table, name_pulled)

                    if column_type == "":
                        if name_pulled in query_columns:        # query_columns is the list of the extended columns
                            continue
                        else:
                            err_msg = "Non supported SQL: Column '%s' is not recognized in table: %s.%s" % (
                            name_pulled, select_parsed.remote_dbms, select_parsed.remote_table)
                            status.add_error(err_msg)
                            status.keep_error(err_msg)
                            return False

                    local_name = name_pulled.replace(".", "_")  # PI can have names with "."

                local_query_field = local_name + " "
                remote_query_field = local_name + " "
                generic_query_field = remote_query_field
                if as_name:
                    title_name = as_name
                    remote_query_field += "as " + as_name + " "
                else:
                    title_name = local_name

                if column_type in unify_data_types.keys():
                    column_type = unify_data_types[column_type]

                l_create = local_name + " " + column_type  # in case of a view, it will take the as_name

                if query_title:
                    query_title += ","
                if as_name != "":
                    query_title += as_name
                else:
                    query_title += name_pulled
                query_data_types.append(column_type)

                counter_local_fields += 1

                projection_id += 1
                projection.set_projection_info(name_pulled, local_name, remote_query_field, generic_query_field, l_create, local_query_field, 0, as_name)



        # Prevent multiple names on the projection list
        if title_name:
            if title_name in projection_names:
                status.add_keep_error("Duplicate projected column names: '%s'" % title_name)
                return False
            projection_names[title_name] = True


        projection_list.append(projection)
        output_counter += 1

    if select_parsed.is_distinct():
        remote_query = "select distinct "
        generic_query = "select distinct "
        local_query = "select distinct "
        pass_through = False
    else:
        remote_query = "select "
        generic_query = "select "
        local_query = "select "
    local_create = "create table " + select_parsed.get_local_table() + " ("
    local_fields_start = len(local_create)      # The offset to the fields names

    l_sql_order_by = ""  # order by needed for the local server
    r_sql_group_by = ""  # group needed for the remote server
    l_sql_group_by = ""  # group by needed for the local server

    for projection in projection_list:
        remote_query = projection.get_remote_query(remote_query)
        generic_query = projection.get_generic_query(generic_query)
        local_create = projection.get_local_create(local_create)
        local_query = projection.get_local_query(local_query)

        l_sql_order_by += projection.get_local_order_by()  # note: no need to order by in the remote
        r_sql_group_by += projection.get_remote_group_by()  # This group by was added in the function, for example in increment_function
        l_sql_group_by += projection.get_local_group_by()

        counter_local_fields += projection.get_counter_local_fields()  # number of fields on the local create

    local_create += ");"
    local_fields_end = len(local_create) - 1 # The offset to the fields names

    select_parsed.set_local_fields(local_create[local_fields_start:local_fields_end])  # Keep the info of the local table that maintains the replies from all nodes

    # different table names

    remote_query += " from " + select_parsed.remote_table
    generic_query += " from " + select_parsed.remote_table
    local_query += " from " + select_parsed.get_local_table()

    if is_suport_join:
        # depending on the dbms engine
        remote_query += select_parsed.join_str
    generic_query += select_parsed.join_str

    where_condition = utils_data.split_quotations(select_parsed.get_where())
    # where_condition = select_parsed.get_where().split()
    where_count = len(where_condition)

    where_condition_local = None

    if where_count:
        # with where conditions - no need on local query
        remote_query += " where"
        generic_query += " where"
        i = 0
        while i < where_count:

            words_count, date_string = utils_sql.process_date_time(where_condition, i, True)        # pull a date or a date function and set as a UTC date string
            if words_count:
                # replace with current date - time
                where_offset = i + words_count - 1
                if where_offset < len(where_condition) and where_condition[where_offset] == "and":
                    # Multiple where conditions
                    word = " '%s' and" % date_string
                else:
                    word = " '%s'" % date_string
                remote_query += word
                generic_query +=  word
                if where_condition_local:
                    local_query += " " + word  # adding the date ('2019-10-15') in the example: select * from test_data where date(timestamp) = '2019-10-15'

                i += words_count
            else:

                is_supported, is_function, name_pulled, description, words_count, as_name = utils_sql.process_variable(where_condition[i:])
                if is_function and name_pulled in where_functions:
                    where_condition_remote, where_condition_local, where_condition_generic = where_functions[name_pulled](
                        select_parsed, description, select_parsed.remote_table, updated_column_names,
                        select_parsed.get_local_table())
                    if where_condition_remote == "":
                        err_msg = "Inconsistent arguments for function: '%s'" % name_pulled
                        status.add_error(err_msg)
                        status.keep_error(err_msg)
                        return False
                    remote_query += " " + where_condition_remote
                    if where_condition_generic:
                        generic_query += " " + where_condition_generic
                    else:
                        generic_query += " " + name_pulled + " (" + description + ")"
                    if where_condition_local:
                        local_query += " where " + where_condition_local
                    i += words_count
                else:
                    # column name - not a function
                    word = where_condition[i]  # get a word from the where condition
                    if select_parsed.is_use_view():  # returns True if metadata maintained in view
                        # Get the local column name and the data type from the view
                        index, view_column_info = db_info.get_view_column_info(select_parsed.remote_dbms,
                                                                               select_parsed.remote_table, word)
                        if view_column_info:
                            word = view_column_info[2]  # Get the word from the view
                        remote_query += " " + word  # only from the where condition (without the join data)
                        generic_query += " " + where_condition[i]
                    else:
                        remote_query += " " + word
                        generic_query += " " + word
                        if where_condition_local:
                            local_query += " " + word   # adding the = sign in the example: select * from test_data where date(timestamp) = '2019-10-15'

                    i = i + 1

    if select_parsed.is_consider_ha():
        # Add th where condition that considers data sync with nodes in the network
        ha_where = select_parsed.get_ha_where_cond()
        if not where_count:
            remote_query += f" where {ha_where}"
        else:
            remote_query += f" and ({ha_where})"

    if select_parsed.is_group_by():
        group_by_columns = select_parsed.get_group_by()
        group_by_no_extended = remove_extended_columns(group_by_columns, extended_list)          # Remote query ignore the extended columns

        if group_by_no_extended:
            remote_query += " group by %s" % group_by_no_extended
            generic_query += " group by %s" % group_by_no_extended

        l_group_fields = remote_name_to_local_name(projection_list, group_by_columns, None)

        with_group_by = True
        pass_through = False
    else:
        l_group_fields = ""
        with_group_by = False

    if r_sql_group_by != "":
        if with_group_by:
            remote_query += ", " + r_sql_group_by[9:]
        else:
            remote_query += " " + r_sql_group_by

    # Create the group by for the local query
    local_group_by = ""
    if function_id and extended_group_by:
        # if query with functions - First group by is the extended fields (like table_name)
        local_group_by =   extended_group_by[:-1]

    if l_sql_group_by:
        # second group by are based on functions like distinct that are represented as group by in the local query
        if local_group_by:
            local_group_by += ", %s" % l_sql_group_by[9:]  # remove the "group by " prefix
        else:
            local_group_by = l_sql_group_by[9:]  # remove the "group by " prefix

    if l_group_fields:
        # Third - add other group by fields from the sql stmt
        if local_group_by:
            local_group_by += ", %s" % l_group_fields
        else:
            local_group_by = l_group_fields

    if local_group_by:
        local_query += " group by %s" % local_group_by


    if l_sql_order_by != "":
       local_query += " " + l_sql_order_by  # l_sql_order_by may be different from the original SQL

    if select_parsed.is_order_by():
        order_by_columns = select_parsed.get_order_by()
        if select_parsed.is_ascending():
            ordering = ""
        else:
            ordering = "desc"
        order_columns = remove_extend_columns(extended_list, order_by_columns)       # remove the columns which are extended (as the sql table that executes the SQL does not see these columns)

        if order_columns:
            remote_query += " order by %s %s" % (order_by_columns, ordering)
            generic_query += " order by %s %s" % (order_by_columns, ordering)

        # Order by for local query
        if l_sql_order_by == "":
            # order by not determined by functions
            order_fields = remote_name_to_local_name(projection_list, order_by_columns, extended_list)
            local_query += " order by %s %s" % (order_fields.replace(".", "_"), ordering)  # PI can bring names with "."
        pass_through = False

    else:
        if select_parsed.get_per_column():  # If not null, AnyLog calculates the limit - used with extended tables and associating the limit to each table
            pass_through = False



    limit = select_parsed.get_limit()
    if limit:
        max_rows = " limit %u" % limit
        generic_query += max_rows

        if not select_parsed.get_per_column():
            local_query += max_rows

        if select_parsed.get_dbms_type() == "oledb.pi":
            query_prefix = "select top %u " % limit
            remote_query = query_prefix + remote_query[7:]
        else:
            remote_query += max_rows

    select_parsed.functions_counter = function_id
    select_parsed.remote_query = remote_query
    select_parsed.generic_query = generic_query
    select_parsed.local_create = local_create
    select_parsed.local_query = local_query

    select_parsed.set_columns_info(query_title, query_data_types)

    select_parsed.set_counter_local_fields(counter_local_fields)  # Number of fields ob the local create

    select_parsed.set_pass_through(pass_through)

    return True


# ==================================================================
# Remove the extended columns from the comma separated list
# ==================================================================
def remove_extend_columns(extended_list, columns_string):

    columns_list = columns_string.split(',')
    new_string = ""
    for column_name in columns_list:
        if column_name in extended_list:
            continue
        if not new_string:
            new_string = column_name
        else:
            new_string += (", " + column_name)
    return new_string
# ==================================================================
# Remove the extended columns - Remote query ignore the extended columns
# ==================================================================
def remove_extended_columns(group_by_columns, extended_list):
    '''
    group_by_columns - comma seperated list of user provided columns
    extended_list - the extended columns
    '''

    if not len(extended_list):
        reply_columns = group_by_columns
    else:
        columns_list = group_by_columns.split()
        reply_columns = ""
        for column in columns_list:
            if column in extended_list:
                continue
            if reply_columns:
                reply_columns += ", %s" % column
            else:
                reply_columns = column

    return reply_columns


# ==================================================================
# replace a name or function of the remote query with the local name
# Return the order by columns (or the group by columns) using the new names
# used in the local table.
# ==================================================================
def remote_name_to_local_name(projection_list, remote_name_list, extended_list):
    '''
    projection_list - the analyzed projection info
    remote_name_list = Column names to transform to the user SQL name to th column name on the local query
    extended_list - extended column names - are not transformed
    '''
    remote_names = remote_name_list.split(',')  # a list of column names
    local_name = ""

    for entry in projection_list:
        for index, one_name in enumerate(remote_names):
            column_name = one_name.strip()
            # column_name the name in the order by list or projection list

            if extended_list and column_name in extended_list:
                # The name remains to be the name in the extended list
                if local_name:
                    local_name += ("," + column_name)
                else:
                    local_name = column_name
                del remote_names[index]
                break

            remote_query = entry.remote_query.rstrip()
            if remote_query == '*':
                local_name = remote_name_list
                break
            index = remote_query.find(" as ")
            if index > 0:
                # find the "as" keyword to pull the column name
                remote_column_name = remote_query[index+4:].strip()
            else:
                remote_column_name = remote_query
            offset_function = remote_column_name.find('(')
            if offset_function > 0:
                remote_column_name = remote_column_name[:offset_function].strip()   # Remove the function name parenthesis
            if  remote_column_name == column_name:
                if entry.as_name:
                    local_column = entry.as_name
                else:
                    local_column = entry.local_name
                # Get the name of the field or function provided to the local query.
                # For example: 'date(timestamp)' --> 'date(date_1)'
                if local_name:
                    local_name += ("," + local_column)
                else:
                    local_name = local_column
                break
            elif entry.is_function and entry.column_name == column_name:
                # The column name is within a function
                if local_name:
                    local_name += ("," + entry.local_name)
                else:
                    local_name = entry.local_name
                break
        if not len(remote_names):
            break

    return local_name


# ==================================================================
# test if a field name is within a function
# ==================================================================
def test_function(sql_word: str, func_word: str):
    if sql_word == func_word:
        return True
    if sql_word.startswith(func_word):
        if sql_word[len(func_word)] == '(':
            return True
    return False


# ==================================================================
# Replace the min_max_sum field name
# ==================================================================
def get_min_max_sum_sql(filed_name, data_type):
    remote_query = filed_name

    new_field_name = filed_name[:3] + "_"
    new_field_name += filed_name[4: len(filed_name) - 1]

    local_create = new_field_name + " " + data_type + " "

    local_query = filed_name[:3] + "(" + new_field_name + ")"

    return [remote_query, local_create, local_query]

# ==================================================================
# Returned result set includes additional information defined in the extend directive
# Example: extend = (@ip as Node IP, @port, @DBMS, @table, !!disk_space.int as space)
# ==================================================================
def  process_extended_columns(status, extended_columns):

    query_title = ""  # the list of fields participating in the query
    query_data_types = []
    query_columns = []
    ret_val = True

    for column in extended_columns:

        index = column.find(".")
        if index != -1:
            # String includes the data type
            name_str = column[:index]
            index_next =  column.find(" ", index + 1)
            if index_next == -1:
                data_type = column[index + 1:]
            else:
                data_type = column[index + 1:index_next]
            offset_as = index_next + 1
        else:
            data_type = "varchar"      # The default
            offset_as = 0
            name_str = None


        query_data_types.append(data_type)

        index = column.find(" as ", offset_as)
        if index != -1:
            if name_str:
                column_name = name_str.strip()
            else:
                column_name = column[:index].strip()
            column_title = column[index + 4:].strip()
        else:
            if name_str:
                column_name = name_str.strip()
            else:
                column_name = column.strip()
            column_title = column_name


        if column_title[0] == '@' or column_title[0] == '+':
            query_title += (column_title[1:]  + ',')   # Predefined name like @IP @Table
        else:
            query_title += (column_title  + ',')

        if column_name[0] == '@' or column_name[0] == "+":
            query_columns.append(column_name[1:])  # Predefined name like @IP @Table
        else:
            status.add_error("Extend directive in SQL command: Wrong variable name prefix: %s" % column_title)
            ret_val = False
            break


    return [ret_val, query_title[:-1], query_data_types, query_columns]
