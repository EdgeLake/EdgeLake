"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.utils_sql as utils_sql
import edge_lake.generic.utils_columns as utils_columns
import edge_lake.job.leading_query as leading_query
import edge_lake.generic.process_log as process_log

# -----------------------------------------------------------
# A structure representing a select statement
# -----------------------------------------------------------
class SelectParsed():

    def __init__(self):
        self.src_stmt = ""  # The original statement
        self.offset_select = 0  # The offset of the word after the "SELECT " keyword
        self.offset_from = 0  # Offset to the FROM keyword
        self.offset_table_name = 0  # Offset to table name
        self.offset_where = 0
        self.offset_join = 0
        self.local_dbms = ""  # Dbms name on the local server
        self.local_table = ""  # Table name on the local server
        self.task_local_table = ""  # A task that is using a unique name for the local table
        self.counter_local_fields = 0  # Number of fields ob the local create

        self.leading_queries = []  # an array for queries issued before the main query. The leading query result is an input to the main query
        self.leading_issued = 0  # a counter representing the number of leading queries that have been issued
        self.leading_processed = 0  # a counter representing the number of leading queries that have been processed

        self.project_func = utils_columns.ProjectionFunctions()
        self.reset(False, False)
        self.dbms_type = "psql"  # default
        self.ascending = True  # for order by clause

    def reset(self, replace_avg, use_leading):

        # Leading queries are needed only of more than 1 server participates in the query
        self.use_leading_queries = use_leading
        self.replace_avg = replace_avg  # Set to True if Average is to be replaced by count and sum
        self.join_str = ""  # The join part of the sql
        self.dbms_name = ""
        self.table_name = ""
        self.projection = ""
        self.where = ""
        self.cluster_conditions = ""        # The part of the where condition to that relates to HA
        self.order_by = ""
        self.group_by = ""
        self.limit = 0
        self.projection_parsed = []  # array with the projection list
        self.is_func_calls = False  # are projected field values or functions
        self.proprietary_functions = []  # array with anylog functions (like increments)
        self.increment_info = ""        # Time unit and interval - used in trace mode by Grafana
        self.where_tree = None  # a TREE structure representing the where stmt
        self.distinct = False
        self.casting_list = []  # A list of casting for each column retrieved like ::float(3))
        self.casting_columns = [] # A list of the columns with custing

        self.remote_dbms = ""  # Dbms name on the remote server
        self.remote_table = ""  # Table name on the remote server

        self.task_local_table = ""  # A task that is using a unique name for the local table

        self.with_view = False  # Set to True if the table has a user defined view

        self.functions_counter = 0  # number of functions with the sql stmt
        self.remote_query = ""
        self.generic_query = ""
        self.local_create = ""
        self.local_query = ""
        self.query_title = None  # a list of the output columns
        self.date_types = []  # A list of the time data types
        self.query_data_types = None  # a list of the output columns data types

        self.input_queries = None  # leading queries
        self.input_issued = False  # Set to TRue after leading queries issued

        self.local_fields = []    # Column names and data types of the fields in the local table in system_query dbms

        self.project_func.reset()

        self.extended_columns = None # list that extends the returned column values in the result set based on extend directive in the command line
        self.pass_through = False
        self.per_column = None      # A field name used in local query (with extended tables) to specify limit per table

    # =======================================================================================================================
    # Increment function info - used in trace mode by Grafana
    # =======================================================================================================================
    def set_increment_info(self, info):
        self.increment_info = info

    # =======================================================================================================================
    # Increment function info - used in trace mode by Grafana
    # =======================================================================================================================
    def get_increment_info(self):
        return self.increment_info

    # =======================================================================================================================
    # A local query will do the limit locally (not in the SQL query)
    # =======================================================================================================================
    def get_per_column(self):
        return self.per_column      # A field name used in local query (with extended tables) to specify limit per table

    # =======================================================================================================================
    # True if no functions involved and no order by or group by - in this case returned result set  to the query node does not update a dbms
    # =======================================================================================================================
    def set_pass_through(self, pass_through):
        self.pass_through = pass_through

    # =======================================================================================================================
    # True if no functions involved and no order by or group by - in this case returned result set  to the query node does not update a dbms
    # =======================================================================================================================
    def get_pass_through(self):
        return self.pass_through

    # =======================================================================================================================
    # Extend the column values returned in the query with the extended values based on status and state values in the node processing the query
    # Example: extend = (@ip, @port, @DBMS, @table, !!disk_space.int)
    # =======================================================================================================================
    def set_extended_columns(self, extended_columns):
        self.extended_columns =  extended_columns
    # =======================================================================================================================
    # Return the list that extends the returned column values in the result set
    # =======================================================================================================================
    def get_extended_columns(self):
        return self.extended_columns

    # =======================================================================================================================
    # Return the number of extended results
    # =======================================================================================================================
    def get_counter_extended(self):
        if not self.extended_columns:
            return 0
        return len(self.extended_columns)

    # =======================================================================================================================
    # Parse the sql_stmt
    # Format the SQL part of the select statement
    # remove leading and trailing spaces and comma & to lower other than data in quotations
    # add space after comma remove extra spaces and remove space after parenthesis
    # =======================================================================================================================
    def parse_sql(self, status, database_name, sql_satement):

        self.dbms_name = database_name

        # remove leading and trailing spaces and comma & to lower other than data in quotations
        self.src_stmt = sql_satement

        if self.src_stmt[0:7] == "select ":
            self.offset_select = 7
        else:
            self.offset_select = self.src_stmt.find(" select ")
            if self.offset_select == -1:
                return False  # not a select statement

            self.offset_select += 8  # after the select

        self.offset_select = utils_data.find_non_space_offset(self.src_stmt, self.offset_select)
        if self.offset_select == -1:
            return False

        if self.src_stmt[self.offset_select:self.offset_select + 9] == "distinct ":
            self.offset_select += 9
            self.offset_select = utils_data.find_non_space_offset(self.src_stmt, self.offset_select)
            self.distinct = True

        self.offset_from = 0
        self.offset_table_name = 0

        # add space after comma remove extra spaces and remove space after parenthesis
        column_list = ""
        previous = ""
        old = ""  # 2 chars away
        bytes_to_copy = 0
        parenthesis = 0
        offset_copied = self.offset_select
        string_length = len(self.src_stmt)
        i = self.offset_select
        while (i < string_length):

            char = self.src_stmt[i]
            if char == '(':
                parenthesis += 1
            elif char == ')':
                parenthesis -= 1
            elif char == 'f' and parenthesis == 0 and previous == ' ':
                # test from (which is not in parenthesis
                if self.src_stmt[i:].startswith("from "):
                    self.offset_from = i
                    self.offset_table_name = utils_data.find_non_space_offset(self.src_stmt, self.offset_from + 5)
                    table_name_length = utils_data.get_word_length(self.src_stmt, self.offset_table_name)
                    self.table_name = self.src_stmt[self.offset_table_name:self.offset_table_name + table_name_length]
                    if self.offset_table_name == -1:
                        return False
                    break

            copy_length, skip_length, delete_length, add_space, replace_char = utils_sql.test_string(old, previous,
                                                                                                     char)

            if add_space:
                column_list += self.src_stmt[offset_copied:offset_copied + bytes_to_copy] + " " + char
                bytes_to_copy = 0
                offset_copied = i + 1
            elif delete_length:
                column_list = (column_list + self.src_stmt[offset_copied:offset_copied + bytes_to_copy])[:-1] + char
                bytes_to_copy = 0
                offset_copied = i + 1
            elif skip_length:
                column_list += self.src_stmt[offset_copied:offset_copied + bytes_to_copy]
                bytes_to_copy = 0
                offset_copied = i + 1
            elif replace_char:
                column_list += self.src_stmt[offset_copied:offset_copied + bytes_to_copy] + replace_char
                bytes_to_copy = 0
                offset_copied = i + 1
            else:
                bytes_to_copy += copy_length

            old = previous
            previous = char

            i += 1

        if not self.offset_from:
            return False  # not a select statement

        column_list += self.src_stmt[offset_copied:offset_copied + bytes_to_copy]
        self.set_projection(column_list[:-1])

        self.offset_where = self.src_stmt[self.offset_from:].find(" where ")
        if self.offset_where == -1:
            self.offset_where = self.src_stmt[self.offset_from:].find(" where(")

        self.offset_join = self.offset_table_name + table_name_length
        self.join_str = self.src_stmt[self.offset_join:self.offset_from + self.offset_where + 1]

        if self.offset_where == -1:
            where_condition = False
            self.offset_where = string_length
        else:
            where_condition = True
            self.offset_where += self.offset_from
            if self.src_stmt[self.offset_where + 6] == ' ':  # " where "
                offset_cond = 7
            elif self.src_stmt[self.offset_where + 6] == '(':  # " where("
                offset_cond = 6
            else:
                where_condition = False
                self.offset_where = string_length

        # Format the where part

        if where_condition:
            # organize the where section

            where_string = utils_sql.orgaize_where_condition(self.src_stmt, self.offset_where + offset_cond)

        else:
            where_string = ""

        if where_condition:
            sql_str = where_string
            end_where = len(where_string)
        else:
            sql_str = self.src_stmt
            end_where = 0

        start_offset = sql_str.rfind(" group by ")
        if start_offset != -1:
            start, end = utils_sql.get_comma_seperated_units(sql_str, False, start_offset + 10)
            self.set_group_by(sql_str[start:end])
            if start_offset < end_where:
                end_where = start_offset  # end of the where condition

        start_offset = sql_str.rfind(" order by ")
        if start_offset != -1:
            start, end = utils_sql.get_comma_seperated_units(sql_str, False, start_offset + 10)
            self.set_order_by(sql_str[start:end])
            if start_offset < end_where:
                end_where = start_offset  # end of the where condition
            start, end = utils_data.find_word_after(sql_str, end)
            if start != -1 and sql_str[start:end] == "desc":
                self.ascending = False
            else:
                self.ascending = True

        start_offset = sql_str.rfind(" limit ")
        if start_offset != -1:
            start, end = utils_sql.get_sql_unit(sql_str, False, start_offset + 7)
            limit = sql_str[start:end]
            if limit.isdigit():
                self.set_limit(int(limit))
            else:
                status.add_error("SQL parsing error: LIMIT is not a decimal value: '%s'" % limit)
                return False
            if start_offset < end_where:
                end_where = start_offset  # end of the where condition

            # find "per table_name" --> this is a single query to multiple (extended tables) and the limit is per each table
            # Example: run client () sql lsl_demo extend=(@table_name as table) "SELECT table_name, timestamp, value FROM ping_sensor WHERE order by timestamp desc limit 1 per table_name;"
            start, end =  utils_data.find_word_after(sql_str, end)
            if sql_str[start:end] == "per":
                start, end = utils_data.find_word_after(sql_str, end)
                if start == -1:
                    status.add_error("SQL parsing error: Missing column name after 'per' keyword")
                    return False
                else:
                    self.per_column = sql_str[start:end]
        if where_condition:
            self.set_where(where_string[7:end_where])
        else:
            self.set_where("")

        return True

    # =======================================================================================================================
    # Prepare local and remore queries - translate the query by the specific database engine
    # =======================================================================================================================
    def prep_sql_stmt(status, engine_type, local_query, remote_query):
        # self.prep_projection_list()
        # prep_where_cond()
        pass

    # =======================================================================================================================
    # Update the tree structure representing the where condition
    # =======================================================================================================================
    def set_where_tree(self, root_node):
        self.where_tree = root_node

    # =======================================================================================================================
    # Return the root node of a tree structure representing the where condition
    # =======================================================================================================================
    def get_where_tree(self):
        return self.where_tree

    # =======================================================================================================================
    # add a proprietary function (like increments)
    # =======================================================================================================================
    def add_proprietary_function(self, name, details):
        self.proprietary_functions.append((name, details))

    # =======================================================================================================================
    # Get the proprietary function (like increments)
    # =======================================================================================================================
    def get_proprietary_functions(self):
        return self.proprietary_functions

    # =======================================================================================================================
    # Get one function
    # =======================================================================================================================
    def get_proprietry_func(self, i):
        return self.proprietary_functions[i]

    # =======================================================================================================================
    # is select all
    # =======================================================================================================================
    def is_select_all(self):
        if len(self.projection_parsed) == 1:
            if self.projection_parsed[0][0] == '*' and self.projection_parsed[0][1] == '': # if self.projection_parsed[0][1] - it is a function (like count(*)
                return True
        return False

    # =======================================================================================================================
    # Add entry to the projection list. Set is_func_calls - True if the call is to functions like MIN, MAX, AVG etc.
    # =======================================================================================================================
    def get_projection_parsed(self):
        return self.projection_parsed

    # =======================================================================================================================
    # Get info on a particular column projected
    # =======================================================================================================================
    def get_projection_col_info(self, id):
        return self.projection_parsed[id]

    # =======================================================================================================================
    # Add a casting string as f(column retrieved) like ::float(3)
    # =======================================================================================================================
    def add_casting(self, column_id, casting_string):

        self.casting_columns.append(str(column_id))  # A list of the columns IDs which include custing
        column_custing = casting_string.split("::")  # A list of castings for this column i.e. int::format(":,") --> [int, format(":,"]
        self.casting_list.append(column_custing)    # A list of the casting to apply on each column

    # =======================================================================================================================
    # Get the list of the casting to apply on each row
    # =======================================================================================================================
    def get_casting_list(self):
        return self.casting_list
    # =======================================================================================================================
    # Get the list of columns with casting functions
    # =======================================================================================================================
    def get_casting_columns(self):
        return self.casting_columns

    # =======================================================================================================================
    # Add entry to the projection list. Set is_func_calls - True if the call is to functions like MIN, MAX, AVG etc.
    # Example: max((cast(a.Value as Int32)))
    #        field_name: a.Value
    #        function_name: max
    #        as_name
    #        field_extended: (a.Value as Int32)
    # =======================================================================================================================
    def add_projection(self, is_function, field_name, function_name, as_name, field_extended):
        self.is_func_calls = is_function
        self.projection_parsed.append((field_name, function_name, as_name, field_extended))

    # =======================================================================================================================
    # Test if projhection includes the specified column
    # =======================================================================================================================
    def is_field_projected(self, field_name):
        for entry in self.projection_parsed:
            if entry[0] == field_name:
                return True
        return False

    # =======================================================================================================================
    # True if the call is to column values (vs. functions like MIN, MAX, AVG etc.)
    # =======================================================================================================================
    def is_columns_values(self):
        return not self.is_func_calls

    # =======================================================================================================================
    # True if the call is to functions like MIN, MAX, AVG etc.
    # =======================================================================================================================
    def is_functions_values(self):
        return self.is_func_calls

    def set_dbms_name(self, name):
        self.dbms_name = name

    def get_dbms_name(self):
        return self.dbms_name

    def set_table_name(self, name):
        self.table_name = name

    def get_table_name(self):
        return self.table_name

    def set_projection(self, projection_fields):
        self.projection = projection_fields

    def set_join_str(self, join_string):
        self.join_str = join_string

    def get_join_str(self):
        return self.join_str

    def get_projection(self):
        return self.projection

    def set_where(self, where_cond):
        self.where = where_cond

    def get_where(self):
        return self.where

    def set_order_by(self, order_by):
        self.order_by = order_by

    def is_order_by(self):
        return self.order_by != ""

    def get_order_by(self):
        return self.order_by

    def is_group_by(self):
        return self.group_by != ""

    def get_group_by(self):
        return self.group_by

    def is_ascending(self):
        return self.ascending

    def set_group_by(self, group_by):
        self.group_by = group_by

    def set_limit(self, limit):
        self.limit = limit

    def get_limit(self):
        return self.limit

    def is_distinct(self):
        return self.distinct

    # =======================================================================================================================
    # Find value in the where condition string
    # =======================================================================================================================
    def find_value_in_where_condition(self, sub_string, operand):

        index = self.where.find(sub_string)
        if index == -1:
            return ""
        index += len(sub_string)

        # Get the operand
        index = utils_data.find_non_space_offset(self.where, index)
        if index == -1:
            return ""

        length_operand = len(operand)
        if self.where[index:index + length_operand] != operand:
            return ""

        # find  the value
        index += length_operand
        index = utils_data.find_non_space_offset(self.where, index)
        if index == -1:
            return ""

        length = utils_data.get_word_length(self.where, index)

        value = utils_data.remove_quotations(self.where[index: index + length])

        return value

    # =======================================================================================================================
    # Return a SQL select statement from the parsed data
    # =======================================================================================================================
    def make_select_stmt(self, status, columns_info: dict, with_join: bool):

        ret_val = process_status.SUCCESS
        select_stmt = ""

        if not columns_info:
            projection = self.projection
            where_cond = self.where
        else:
            ret_val, projection = utils_sql.make_new_projection(status, columns_info, self.projection)
            ret_val, where_cond = utils_sql.make_new_where(status, columns_info, self.where)

        if with_join:
            join_str = " " + self.get_join_str() + " "
        else:
            join_str = ""

        if not ret_val:
            select_stmt = "select " + projection + " from " + self.table_name + join_str + " where " + where_cond

        return [ret_val, select_stmt]

    # =======================================================================================================================
    # Set the local and remote database and table names
    # =======================================================================================================================
    def set_target_names(self, remote_dbms, remote_table):
        self.remote_dbms = remote_dbms  # Dbms name on the remote server
        self.remote_table = remote_table  # Table name on the remote server

    # =======================================================================================================================
    # Set Flag to True with user defined view
    # =======================================================================================================================
    def set_view(self, status):
        self.with_view = status

    # =======================================================================================================================
    # Is a user defined view over the table
    # =======================================================================================================================
    def is_use_view(self):
        return self.with_view

    # =======================================================================================================================
    # Return the remote query
    # =======================================================================================================================
    def get_remote_query(self):
        return self.remote_query


    # =======================================================================================================================
    # Return the source query - this is the query provided by the user with small formatting changes
    # =======================================================================================================================
    def get_source_query(self):
        return self.src_stmt

    # ====