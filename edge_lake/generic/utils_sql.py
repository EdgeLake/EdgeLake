"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import datetime
import decimal
import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.utils_columns as utils_columns
import edge_lake.generic.utils_print as utils_print


trace_level_ = 0
trace_command_ = ""     # Limit trace to this command

increment_date_types = {
    "second": "minute",
    "minute": "hour",
    "hour": "day",
    "day": "month",
    "week": "month",
    "month": "year",
    "year": ""
}

supported_instruct = {
    "where": 1,
    "order": 1,
    "group": 1,
    "limit": 1,
    "join": 1,
    "inner": 1,
    "outer": 1,
}

map_data_types = {
    "serial": "integer",
    "timestamp": "timestamp without time zone",
    "varchar": "character varying",
}

al_data_type = {

    "timestamp without time zone" : "timestamp",
    "character varying" : "varchar",
}


end_unit_char = {  # these chars mark the end of a word unit
    ' ': "",
    '=': "",
    '>': "",
    '<': "",
    '(': ")",
}

projection_functions = {  # these are functions that impact the projections
    "increments": True
}

operand_values = {
    '=': 0,
    '>': 1,
    '<': 2,
    '!=': 3,
    '<=': 4,
    '>=': 5
}
add_operands = [
    #    =     >     <     !=   <=    >=
    ['=', '>=', '<=', None, '<=', '>='],  # =
    ['>=', '>', '!=', '!=', None, '>='],  # >
    ['<=', '!=', '<', '!=', '<=', None],  # <
    [None, '!=', '!=', '!=', None, None],  # !=
    ['<=', None, '<=', None, '<=', None],  # <=
    ['>=', '>=', None, None, None, '>=']  # >=
]

fast_date_time_filter_ = {
    '\'': 1,  # Date string in quotations
    'n': 1,  # now()
    'd': 1,  # date()
    't': 1,  # time()
    '+': 1,  # add time()
    '-': 1,  # subtract time()
}
# -----------------------------------------------------------
# Represent a value as a flot
# -----------------------------------------------------------
def rep_value_as_float( value ):
    if value.find('.') == -1:
        rep_val = value + ".0"      # the value represented as float
    else:
        rep_val = value
    return rep_val

# -----------------------------------------------------------
# Represent a value as an int
# -----------------------------------------------------------
def rep_value_as_int( value ):
    return value

# -----------------------------------------------------------
# Represent a value as a bool
# -----------------------------------------------------------
def rep_value_as_bool( value ):
    if value and (value[0] == 'T' or value[0] == 't'):
        rep_val = "true"
    else:
        rep_val = "false"
    return rep_val

# -----------------------------------------------------------
# Represent a value as a string
# -----------------------------------------------------------
def rep_value_as_str( value ):
    return  "\"" + value + "\""


get_value_by_type_ = {
    "float" :               rep_value_as_float,
    "decimal" :             rep_value_as_float,
    "integer"   :           rep_value_as_int,
    "int"   :               rep_value_as_int,
    "bool"  :               rep_value_as_bool,
    "boolean"  :            rep_value_as_bool,
    "string" :              rep_value_as_str,
    "str" :                 rep_value_as_str,
    "char" :                rep_value_as_str,
    "varchar" :             rep_value_as_str,

}

# -----------------------------------------------------------
# Nodes that create a tree representing a where structure
# -----------------------------------------------------------
class ParsedNode():
    def __init__(self, stmt):
        self.stmt = stmt
        self.left_node = None
        self.right_node = None
        self.condition = ""  # is set to AND or OR
        self.is_leaf = False
        self.offset_paren = find_comparison_brackets(stmt)
        self.offset_or = stmt.find(' or ')
        self.and_array = []  # an Array on the leaf with AND statements to execute
        self.direction = 0  # used in traversal - set 1 when moved left and 2 when moved right
        self.or_dict = {}  # a dictionary on the leaf representing OR statements to execute
        self.use_or_dict = False  # Flag indicating hash table to represent multiple OR on the same node
        self.or_key = ""  # The key used when a dictionary represents the or options on the node

    def is_use_or_dict(self):
        return self.use_or_dict

    def get_or_key(self):
        return self.or_key

    def is_leaf_node(self):
        return self.is_leaf

    def get_left_node(self):
        return self.left_node

    def get_right_node(self):
        return self.right_node

    def get_and_array(self):
        return self.and_array

    # -----------------------------------------------------------
    # Test if a node that can change a decision by evaluating conditions
    # -----------------------------------------------------------
    def is_with_conditions(self):
        if self.use_or_dict:
            return len(self.or_dict) > 0

        return len(self.and_array) > 0

    # -----------------------------------------------------------
    # Reset direction to 0
    # -----------------------------------------------------------
    def reset_direction(self):
        self.direction = 0

    # -----------------------------------------------------------
    # get 1 when moved left and 2 when moved right
    # -----------------------------------------------------------
    def get_direction(self):
        return self.direction

    # -----------------------------------------------------------
    # set 1 when moved left and 2 when moved right
    # -----------------------------------------------------------
    def next_direction(self):
        self.direction += 1
        return self.direction

    # -----------------------------------------------------------
    # only and in the statement - simple parse
    # -----------------------------------------------------------
    def is_only_and_stmt(self):
        return self.offset_paren == -1 and self.offset_or == -1

    # -----------------------------------------------------------
    # First parse by parenthesis, otherwise by Or
    # -----------------------------------------------------------
    def parse(self, status, node_type):

        if self.offset_paren > -1:
            # First parse by parenthesis
            if self.offset_paren == 0:
                # stmt starts with parenthesis
                counter, end_offset = count_parenthesis(self.stmt, 0)
                self.left_node = collection_nodes[node_type](self.stmt[1:end_offset])
                ret_value = self.left_node.parse(status, node_type)
                if ret_value:
                    return ret_value

                if end_offset < len(self.stmt) - 1:
                    right_stmt = self.stmt[end_offset + 1:]
                    length, self.condition = test_and_or(right_stmt, True)
                    if not length:
                        return process_status.Failed_to_parse_sql  # 'and' or 'or' not identifies in the statement
                    self.right_node = collection_nodes[node_type](right_stmt[length:])
                    ret_value = self.right_node.parse(status, node_type)
                    if ret_value:
                        return ret_value
            else:
                left_stmt = self.stmt[:self.offset_paren]
                length, self.condition = test_and_or(left_stmt, False)
                if not length:
                    return process_status.Failed_to_parse_sql  # 'and' or 'or' not identifies in the statement

                self.left_node = collection_nodes[node_type](left_stmt[:-length])
                self.right_node = collection_nodes[node_type](self.stmt[self.offset_paren:])
                ret_value = self.left_node.parse(status, node_type)
                if ret_value:
                    return ret_value
                ret_value = self.right_node.parse(status, node_type)
                if ret_value:
                    return ret_value
        elif self.offset_or > -1:
            # parse on the or part
            if self.offset_or == 0:
                return process_status.Failed_to_parse_sql
            self.condition = "or"
            if self.stmt.find(' and ') == -1:
                # ONLY OR statement - find if can be represented by a hash structure
                if self.represent_as_hash():
                    return process_status.SUCCESS

            self.left_node = collection_nodes[node_type](self.stmt[:self.offset_or])
            if self.offset_or >= len(self.stmt) - 4:
                return process_status.Failed_to_parse_sql
            self.right_node = collection_nodes[node_type](self.stmt[self.offset_or + 4:])
            ret_value = self.left_node.parse(status, node_type)
            if ret_value:
                return ret_value
            ret_value = self.right_node.parse(status, node_type)
            if ret_value:
                return ret_value
        else:
            self.is_leaf = True  # only AND stmt
            self.condition = "and"
            ret_value = update_and_stmt(status, self.and_array, self.stmt)

        return ret_value

    # -----------------------------------------------------------
    # This node includes only or statements - try to represent as a hash table
    # Return True - if node is represented as hash
    # -----------------------------------------------------------
    def represent_as_hash(self):

        or_conditions = self.stmt.split(' or ')
        if len(or_conditions) > 2:
            # minimum 3 OR stmts
            key_size = or_conditions[0].find(" = ")
            if key_size == -1:
                # has to be an equal operator
                return False
            key_size += 3
            key = or_conditions[0][:key_size]
            for entry in or_conditions[1:]:
                if entry[:key_size] != key:
                    return False  # a condition that does not share the same key
                if entry[key_size:].find('*') != -1:
                    return False  # prefix search can not be represented in the hash

            # all conditions share the same key
            # update to hash

            for entry in or_conditions:
                # update the value in the hash or update the operand in the hash
                value = utils_data.remove_quotations(entry[key_size:]).lower()
                if value not in self.or_dict.keys():
                    self.or_dict[value] = 1  # keep the operand as f(value)

            self.or_key = key[:-3]  # remove the equal operand and spaces (' = ')
            self.use_or_dict = True
            return True

        return False

    # -----------------------------------------------------------
    # Traversal on the SQL WHERE condition tree
    # Call the function with every entry in the tree
    # -----------------------------------------------------------
    def traversal_where_tree(self, status, caller_function, caller_data):

        self.reset_direction()  # flag - this node was not visited
        ret_val = True
        while 1:
            direction = self.next_direction()  # flag - this node was not visited
            if direction == 1:
                next_node = self.get_left_node()
            elif direction == 2:
                next_node = self.get_right_node()
            else:

                if not caller_function(self, caller_data):
                    ret_val = False  # exit - no need to continue the traversal
                break

            if next_node:
                if not next_node.traversal_where_tree(status, caller_function, caller_data):
                    ret_val = False  # exit - no need to continue the traversal
                    break

        return ret_val

    # -----------------------------------------------------------
    # Traversal on the SQL WHERE condition tree
    # Return a list with all the conditions
    # -----------------------------------------------------------
    def traversal_get_where_conditions(self, status, conditions_list):

        self.reset_direction()  # flag - this node was not visited
        ret_val = True
        while 1:
            direction = self.next_direction()  # flag - this node was not visited
            if direction == 1:
                next_node = self.get_left_node()
            elif direction == 2:

                next_node = self.get_right_node()
            else:
                if self.use_or_dict:
                    conditions_list.append(self.or_dict)
                elif len(self.and_array):
                    conditions_list.append(self.and_array)
                break

            if next_node:
                if not next_node.traversal_get_where_conditions(status, conditions_list):
                    ret_val = False  # exit - no need to continue the traversal
                    break

        return ret_val

# -----------------------------------------------------------
# Set the trace level
# if command is specified, it can include SQL command like INSERT or SELECT.
# If command provided, trace is limited to the specified command
# -----------------------------------------------------------
def set_trace_level(level, command):
    global trace_level_
    global trace_command_

    trace_level_ = level
    if command:
        trace_command_ = command        # Limit to the specific command
    else:
        trace_command_ = ""             # All commands

# -----------------------------------------------------------
# Get the trace level
# -----------------------------------------------------------
def get_trace_level():
    global trace_level_
    return trace_level_

# -----------------------------------------------------------
# Get the trace command
# -----------------------------------------------------------
def get_trace_command():
    global trace_command_
    return trace_command_

# -----------------------------------------------------------
# Return True if trace level is not 0
# -----------------------------------------------------------
def is_trace_sql():
    global trace_level_
    return trace_level_ != 0
# -----------------------------------------------------------
# Show the SQL string and  the result
# If trace_command_ has a value, only trace a specific sql command
# -----------------------------------------------------------
def trace_sql(dbms_type, sql_stmt, ret_val, ignore_error, error_msg):
    '''
    dbms_type - Physical dbms like PSQL, SQLite, PI etc.
    sql_stmt - the executed statement
    ret_val - True or False indicating execution result
    ignore_error - status of flag
    '''

    formatted_sql = sql_stmt.replace("\r"," ").replace("\n"," ").replace("  "," ")

    output_trace = True
    if trace_command_:
        if not formatted_sql[:len(trace_command_)].lower() == trace_command_:
            output_trace = False

    if output_trace:
        out_msg = "\r\n[SQL: %s] [Result: %s]" % (dbms_type, str(ret_val))
        if ignore_error:
            out_msg += " [Ignore Error]"
        if error_msg:
            out_msg += " [Err MSG: %s]" % error_msg

        out_msg += " [%s]" % formatted_sql
        utils_print.output(out_msg, True)
# -----------------------------------------------------------
# Where copndition node with extra info for PI
# -----------------------------------------------------------
class PI_NODE(ParsedNode):
    def __init__(self, stmt):
        ParsedNode.__init__(self, stmt)
        self.and_array_pi = []  # An array with statemnents that use the PI naming and the data types
        self.pi_attr_name_or = ""  # PI attribute name represented by OR dictionary
        self.pi_attr_type_or = ""  # PI attribute type represented by OR dictionary
        self.pi_attr_level_or = 0  # PI attribute level represented by OR dictionary

    # -----------------------------------------------------------
    # The name of the attribute in PI - when the node represents and OR condition
    # that is represented in a Hash table
    # -----------------------------------------------------------
    def set_pi_or_dict_info(self, attr_name, pi_attribute_type, level):
        self.pi_attr_name_or = attr_name
        self.pi_attr_type_or = pi_attribute_type
        self.pi_attr_level_or = level

    # -----------------------------------------------------------
    # Optimize PI query process by considering the search tree
    # -----------------------------------------------------------
    def optimize_pi_query(self, element_lookup, sensor_lookup):

        ret_value = True
        self.reset_direction()  # flag - this node was not visited
        while 1:
            direction = self.next_direction()  # flag - this node was not visited
            if direction == 1:
                next_node = self.get_left_node()
            elif direction == 2:
                next_node = self.get_right_node()
            else:
                if self.is_with_conditions():
                    if self.use_or_dict:
                        ret_value = False
                    else:
                        for entry in self.and_array_pi:
                            if entry[0] == "Id" and entry[1] == '=':
                                # an ID of a sensor or an element
                                if entry[4] == 1:
                                    # sensor
                                    sensor_lookup.append(entry[2])
                                else:
                                    # element
                                    element_lookup.append(entry[2])
                break

            if next_node:
                ret_value = next_node.optimize_pi_query(element_lookup, sensor_lookup)
                if ret_value == False:
                    if self.condition == "and":
                        # no point to continue to the Right Subtree
                        break
                elif self.condition == "or":
                    # no point to continue to the Right Subtree
                    break

        return ret_value

    # -----------------------------------------------------------
    # Traversal to test conditions on PI server
    # is_sql_where is True when we filter the path to the data from the SQL WHERE clause
    # is_sql_where is False when we filter the path to the data from the blockchain Traversal conditions
    # -----------------------------------------------------------
    def traversal_pi_where_tree(self, is_sql_where, tree_depth, json_data, ignore_missing_pi_column):

        ret_value = True
        self.reset_direction()  # flag - this node was not visited
        while 1:
            direction = self.next_direction()  # flag - this node was not visited
            if direction == 1:
                next_node = self.get_left_node()
            elif direction == 2:
                next_node = self.get_right_node()
            else:
                if self.is_with_conditions():
                    ret_value = self.test_pi_where_conditions(is_sql_where, tree_depth, json_data,
                                                              ignore_missing_pi_column)
                break

            if next_node:
                ret_value = next_node.traversal_pi_where_tree(is_sql_where, tree_depth, json_data,
                                                              ignore_missing_pi_column)

                if ret_value == False:
                    if self.condition == "and":
                        # no point to continue to the Right Subtree
                        break
                elif self.condition == "or":
                    # no point to continue to the Right Subtree
                    break

        return ret_value

    # ==================================================================
    # Test if the where conditions on the current node are satisfied
    # is_sql_where is True when we filter the path to the data from the SQL WHERE clause
    # is_sql_where is False when we filter the path to the data from the blockcjain Traversal conditions
    # ==================================================================
    def test_pi_where_conditions(self, is_sql_where, tree_depth, json_data, ignore_missing_pi_column):

        ret_val = True

        if is_sql_where:
            # SQL Where condition

            if self.use_or_dict:
                # a dctionary representing the OR values
                if self.pi_attr_level_or == tree_depth:
                    ret_val, pi_value = utils_columns.get_pi_column_val(json_data, self.pi_attr_name_or)
                    if ret_val:
                        if not isinstance(pi_value, str):
                            pi_value_str = str(pi_value)
                        else:
                            pi_value_str = pi_value

                        # Test if key is in hash dictionary
                        if pi_value_str in self.or_dict.keys():
                            ret_val = True  # value in dictionary, condition satisfied
                        else:
                            ret_val = False  # value not in dictionary, condition not satisfied
                    elif ignore_missing_pi_column:
                        # the needed column name is not available to process the where condition - ignore the structure
                        ret_val = True
            else:
                # a LIST representing AND values
                # entry struct: (pi_attr_name, operand, value, pi_attribute_type, level, with_prefix, prefix_size, is_navigation))

                for entry in self.and_array_pi:

                    if entry[4] == tree_depth:  # depth 0 - readings, depth 1 - sensor, depth > 1 - element
                        # map to tree depth
                        ret_val, pi_value = utils_columns.get_pi_column_val(json_data, entry[0])
                        if ret_val:
                            if isinstance(pi_value, str):
                                if entry[3] == "varchar":
                                    pi_value = pi_value.lower()
                            ret_val = utils_columns.compare(pi_value, entry[2], entry[1], entry[3])
                            if ret_val == False:
                                break
                        elif ignore_missing_pi_column:
                            # the needed column name is not available to process the where condition - ignore the structure
                            ret_val = True

        else:
            # Blockchain Traversal conditions

            if self.use_or_dict:
                # a dctionary representing the OR values

                if self.pi_attr_level_or == tree_depth:
                    if isinstance(json_data, dict):
                        if not self.pi_attr_name_or in json_data.keys():
                            pi_value = ""
                        else:
                            pi_value = json_data[self.pi_attr_name_or]
                    else:
                        pi_value = getattr(json_data, self.pi_attr_name_or, "")

                    if pi_value:

                        if not isinstance(pi_value, str):
                            pi_value_str = str(pi_value)
                        else:
                            pi_value_str = pi_value

                        # Test if key is in hash dictionary
                        if pi_value_str.lower() in self.or_dict.keys():
                            ret_val = True  # value in dictionary, condition satisfied
                        else:
                            ret_val = False  # value not in dictionary, condition not satisfied

                    else:
                        ret_val = False

            else:
                # AND conditions
                # entry struct: (pi_attr_name, operand, value, pi_attribute_type, level, with_prefix, prefix_size, is_navigation))

                for entry in self.and_array_pi:
                    layer = entry[4]
                    if layer == tree_depth or not layer:  # layer 0 may be during connect
                        pi_attr_name = entry[0]
                        if isinstance(json_data, dict):
                            if pi_attr_name in json_data.keys():

                                ret_val = compare_values(entry[5], entry[6], entry[2], json_data[pi_attr_name].lower())
                                if not ret_val:
                                    break
                        elif hasattr(json_data, pi_attr_name):
                            ret_val = compare_values(entry[5], entry[6], entry[2], json_data[pi_attr_name].lower())
                            if not ret_val:
                                break

        return ret_val

    # -----------------------------------------------------------
    # Update an Array with the info of the where conditions
    # The array includes statements that all needed to be executed (AND CLAUSE)
    # -----------------------------------------------------------
    def update_pi_and_array(self, pi_cursor, pi_attr_name, operand, value, pi_attribute_type, level_in_pi_hierarchy,
                            with_prefix, prefix_size, is_navigation):
        '''
        :param pi_attr_name: The name in the PI structure
        :param operand: one of the following: =, >, <, <=
        :param value: in string format, can include a prefix with a *
        :param pi_attribute_type: the data type
        :param level_in_pi_hierarchy:
        :param with_prefix: True for prefix matching on the key value
        :param prefix_size: The size of the prefix
        :param is_navigation: True to process the call in the hierarchical navigation
        :return:
        '''

        self.and_array_pi.append((pi_attr_name, operand, value, pi_attribute_type, level_in_pi_hierarchy, with_prefix,
                                  prefix_size, is_navigation))

    # -----------------------------------------------------------
    # Traversal on the SQL WHERE condition tree to find the keys that would
    # be provided to PI to determine the traversal in PI hierarchy.
    # These keys are used in the method pi_query (in pi_dbms.py) as the cmd_params values
    # These keys determine the search criteria in PI.
    # -----------------------------------------------------------
    def traversal_update_pi_keys(self, status, pi_cursor):

        self.reset_direction()  # flag - this node was not visited
        while 1:
            if self.condition != "and":
                return True  # with or, the values are not candidates to PI cmd_params
            direction = self.next_direction()  # flag - this node was not visited
            if direction == 1:
                next_node = self.get_left_node()
            elif direction == 2:
                next_node = self.get_right_node()
            else:
                for entry in self.and_array_pi:
                    # consider all the conditions to find conditions for PI search
                    if entry[3].startswith("time"):  # entry[3] is pi_attribute_type
                        # build the key for the search based on the time
                        value_formatted = utils_columns.unify_time_format(status,
                                                                          entry[2])  # entry[2] is the value to test
                        pi_cursor.set_search_key("time", entry[1], value_formatted)  # entry[1] is the operand

                break

            if next_node:
                if not next_node.traversal_update_pi_keys(status, pi_cursor):
                    return False
        return True


# -----------------------------------------------------------
# An Array with the types of nodes that can be placed on the tree
# 0 is geberic node, 1 is PI node
# -----------------------------------------------------------
#                       SQL         PI
collection_nodes = [ParsedNode, PI_NODE]

# =======================================================================================================================
# Identify if a select statement
# Return select without the leading and trailing spaces and offset to first word after the "select"
# =======================================================================================================================
def get_select_stmt(status, command_str):

    ret_val = process_status.SUCCESS
    is_select = True

    # remove leading and trailing spaces and comma & to lower other than data in quotations
    new_string = sql_to_standard_format(command_str.strip(" ;"))

    if new_string[0:7] == "select ":
        offset_select = 7
    else:
        if new_string.startswith("sql "):
            offset_select = new_string.find(" select ")
            if offset_select == -1:
                is_select = False       # SQL which is not select
            else:
                offset_select += 8  # after the select
        else:
            offset_select = 0
            is_select = False

    if is_select:
        offset_select = utils_data.find_non_space_offset(new_string, offset_select) # Only select without info
        if offset_select == -1:
            status.add_error("Parsing SQL Select failed: Missing SELECT components after 'select' keyword in SQL: '%s'" % new_string)
            ret_val = process_status.ERR_in_select_stmt

    return [ret_val, is_select, new_string, offset_select]

# =======================================================================================================================
# Format the SQL part of the select statement
# remove leading and trailing spaces and comma & to lower other than data in quotations
# add space after comma remove extra spaces and remove space after parenthesis
# =======================================================================================================================
def format_select_sql(status, new_string, offset_select, select_parsed):

    offset_from = 0
    offset_table_name = 0

    # add space after comma remove extra spaces and remove space after parenthesis
    column_list = ""
    previous = ""
    old = ""  # 2 chars away
    bytes_to_copy = 0
    parenthesis = 0
    offset_copied = offset_select
    string_length = len(new_string)
    i = offset_select

    while (i < string_length):

        char = new_string[i]

        if char != " " and not previous or previous == ' ':
            function_length = extract_functions(select_parsed, new_string, i)  # Extract functions like TOP, Limit
            if function_length:
                # remove the function
                new_string = new_string[:i] + new_string[i + function_length:]
                char = new_string[i]  # if the index moved to after the function

        if char == '(':
            parenthesis += 1
        elif char == ')':
            parenthesis -= 1
        elif char == 'f' and parenthesis == 0 and previous == ' ':
            # test from (which is not in parenthesis
            if new_string[i:].startswith("from "):
                offset_from = i
                offset_table_name, offset_end_name = utils_data.find_word_after(new_string, offset_from + 5)
                if offset_table_name == -1:
                    status.add_error("Parsing SQL failed: Missing SELECT statement components after 'select' keyword in SQL: '%s'" % new_string)
                    return [process_status.ERR_in_select_stmt, None]
                next_word = utils_data.get_word_after(new_string, offset_end_name)
                # Test if next word after the table name is recoginize
                if not next_word or next_word in supported_instruct.keys():
                    break
                if new_string.find(" join ", offset_end_name) != -1:
                    break  # Join can have prefix info
                status.add_keep_error("Parsing SQL failed: Unrecognized SELECT statement components after 'select' keyword in SQL: '%s'" % new_string)
                return [process_status.ERR_in_select_stmt, None]  # Syntax is not recognized or not supported

        copy_length, skip_length, delete_length, add_space, replace_char = test_string(old, previous, char)

        if add_space:
            column_list += new_string[offset_copied:offset_copied + bytes_to_copy] + " " + char
            bytes_to_copy = 0
            offset_copied = i + 1
        elif delete_length:
            column_list = (column_list + new_string[offset_copied:offset_copied + bytes_to_copy])[:-1] + char
            bytes_to_copy = 0
            offset_copied = i + 1
        elif skip_length:
            column_list += new_string[offset_copied:offset_copied + bytes_to_copy]
            bytes_to_copy = 0
            offset_copied = i + 1
        elif replace_char:
            column_list += new_string[offset_copied:offset_copied + bytes_to_copy] + replace_char
            bytes_to_copy = 0
            offset_copied = i + 1
        else:
            bytes_to_copy += copy_length

        old = previous
        previous = char

        i += 1

    if not offset_from:
        status.add_error("Parsing SQL failed: Missing 'FROM' in SELECT statement in SQL: '%s'" % new_string)
        return [process_status.ERR_in_select_stmt, None]

    column_list += new_string[offset_copied:offset_copied + bytes_to_copy]

    offset_where = new_string[offset_from:].find(" where")

    offset_join = offset_from + 5 + utils_data.get_word_length(new_string, offset_from + 5)
    join_string = new_string[offset_join:offset_from + offset_where + 1]

    if offset_where == -1:
        where_condition = False
        offset_where = string_length
    else:
        where_condition = True
        offset_where += offset_from

        if offset_where + 6 >= string_length:
            status.add_error("Parsing SQL failed: Failed to process the 'WHERE' condition in SELECT statement in SQL: '%s'" % new_string)
            return [process_status.ERR_in_select_stmt, None]  # error with select statement

        if new_string[offset_where + 6] == ' ':  # " where "
            offset_cond = 7
        elif new_string[offset_where + 6] == '(':  # " where("
            offset_cond = 6
        else:
            where_condition = False
            offset_where = string_length

    # Format the where part

    if where_condition:
        # organize the where section
        where_string = orgaize_where_condition(new_string, offset_where + offset_cond)
        # Find leading queries
        where_string, input_queries = get_leading_queries(where_string)  # test if a value is based on leading queries
    else:
        where_string = ""
        input_queries = None

    if select_parsed:
        set_select_parsed(select_parsed, new_string, where_condition, where_string, offset_where, offset_table_name,
                          column_list, join_string)  # set the select info on a structure
        select_parsed.set_input_queries(input_queries)

    new_string = new_string[:offset_select] + column_list + new_string[offset_from:offset_where] + where_string

    return [process_status.SUCCESS, new_string]


# -----------------------------------------------------------
# Extract functions like TOP, Limit
# Return offset after the function
# -----------------------------------------------------------
def extract_functions(select_parsed, stmt_string, i):
    function_length = 0  # number of chars to advance
    if stmt_string[i] == 't' and stmt_string.startswith("top ", i, i + 4):
        # Top
        offset = i + 4
    elif stmt_string[i] == 'l' and stmt_string.startswith("limit ", i, i + 6):
        # max
        offset = i + 4
    else:
        offset = 0

    if offset:
        # Top or Max was found
        offset = utils_data.get_word_start(stmt_string, offset)  # Get offset to the value
        if offset != -1:
            value_length = utils_data.get_word_length(stmt_string, offset)
            if value_length:
                value_str = stmt_string[offset:offset + value_length]
                if value_str.isnumeric():
                    select_parsed.set_limit(int(value_str))
                    offset += value_length
                    function_length = offset - i

    return function_length


# -----------------------------------------------------------
# Find if there is a leading query in the where condition
# Leading queries format: {Query (field_name))
# -----------------------------------------------------------
def get_leading_queries(where_string):
    sub_where = where_string.split("{")
    if len(sub_where) > 1:
        # with leading queries
        leading_queries = []
        updated_where = sub_where[0]
        for entry in sub_where[1:]:
            # Go over the leading queries and extract the query text and the column name
            leading_flag = False
            end_leading = entry.rfind("}")
            if end_leading > 1:
                # found leading query
                leading_end = entry.rfind('}')
                if leading_end > 2:
                    column_start = entry.rfind("(", 0, leading_end)  # find the column name
                    if column_start != -1:
                        column_end = entry.find(")", column_start + 1, leading_end)
                        if column_end > column_start + 1:
                            column_name = entry[column_start + 1:column_end]
                            leading_query = entry[:column_start]
                            updated_where += "%s" % "%s"  # Update the where condition such that the value of the leading query can be applied
                            if len(entry) > end_leading + 1:
                                updated_where += entry[end_leading + 1:]
                            leading_queries.append((leading_query, column_name))
                            leading_flag = True
            if not leading_flag:
                updated_where += entry
        if not len(leading_queries):
            leading_queries = None
    else:
        updated_where = where_string
        leading_queries = None

    return [updated_where, leading_queries]


# -----------------------------------------------------------
# Test for prefix or suffix with end or or
# -----------------------------------------------------------
def test_and_or(stmt, is_prefix):
    if is_prefix:
        # test AND or OR in the prefix of the statement
        if stmt[0:5] == " and ":
            length = 5
            condition = "and"
        elif stmt[0:4] == " or ":
            length = 4
            condition = "or"
        elif stmt[0:5] == " and(":
            length = 4
            condition = "and"
        elif stmt[0:4] == " or(":
            length = 3
            condition = "or"
        elif stmt[0:4] == "and ":
            length = 4
            condition = "and"
        elif stmt[0:3] == "or ":
            length = 3
            condition = "or"
        elif stmt[0:4] == "and(":
            length = 3
            condition = "and"
        elif stmt[0:3] == "or(":
            length = 2
            condition = "or"
        else:
            length = 0
            condition = ""
    else:
        # test AND or OR in the sufix of the statement
        if stmt[-5:] == " and ":
            length = 5
            condition = "and"
        elif stmt[-4:] == " or ":
            length = 4
            condition = "or"
        elif stmt[-5:] == " and(":
            length = 4
            condition = "and"
        elif stmt[-4:] == " or(":
            length = 3
            condition = "or"
        elif stmt[-4:] == " and":
            length = 4
            condition = "and"
        elif stmt[-3:] == " or":
            length = 3
            condition = "or"
        elif stmt[-4:] == "and(":
            length = 3
            condition = "and"
        elif stmt[-3:] == "or(":
            length = 2
            condition = "or"
        else:
            length = 0
            condition = ""

    return [length, condition]


# =======================================================================================================================
# Organize the WHERE CONDITION
# =======================================================================================================================
def orgaize_where_condition(source_string, offset):
    string_length = len(source_string)
    where_string = " where "
    offset_copied = offset
    old = ""  # 2 chars away
    bytes_to_copy = 0

    if source_string[offset] == ' ':
        previous = " "  # taken from the space from the " where " - remove extra space
    else:
        previous = ""

    while (offset < string_length):

        char = source_string[offset]

        copy_length, skip_length, delete_length, add_space, replace_char = test_string(old, previous, char)
        if add_space:
            where_string += source_string[offset_copied:offset_copied + bytes_to_copy] + " " + char
            bytes_to_copy = 0
            offset_copied = offset + 1
        elif delete_length:
            if (offset_copied + bytes_to_copy) < 5 or not source_string[offset_copied + bytes_to_copy - 5:].startswith(
                    ' and ('):
                # The space in the case of "and (...)" is npt removed
                where_string = (where_string + source_string[offset_copied:offset_copied + bytes_to_copy])[:-1] + char
                bytes_to_copy = 0
                offset_copied = offset + 1
            else:
                bytes_to_copy += copy_length
        elif skip_length:
            where_string += source_string[offset_copied:offset_copied + bytes_to_copy]
            bytes_to_copy = 0
            offset_copied = offset + 1
        elif replace_char:
            where_string += source_string[offset_copied:offset_copied + bytes_to_copy] + replace_char
            bytes_to_copy = 0
            offset_copied = offset + 1
        else:
            bytes_to_copy += copy_length

        old = previous
        previous = char

        offset += 1

    where_string += source_string[offset_copied:offset_copied + bytes_to_copy]

    return where_string


# =======================================================================================================================
# Organize the parsed SQL in a structure
# =======================================================================================================================
def set_select_parsed(select_parsed, new_string, where_condition, where_string, offset_where, offset_table_name,
                      column_list, join_string):
    # update the structure
    select_parsed.set_projection(column_list[:-1])
    select_parsed.set_join_str(join_string)

    # get the table name
    start_offset, end_offset = get_sql_unit(new_string, False, offset_table_name)
    select_parsed.set_table_name(new_string[start_offset:end_offset])

    if where_condition:
        sql_str = where_string
        end_where = len(where_string)
    else:
        sql_str = new_string
        end_where = 0

    start_offset = sql_str.rfind(" group by ")
    if start_offset != -1:
        start, end = get_comma_seperated_units(sql_str, False, start_offset + 10)
        select_parsed.set_group_by(sql_str[start:end])
        if start_offset < end_where:
            end_where = start_offset  # end of the where condition

    start_offset = sql_str.rfind(" order by ")
    if start_offset != -1:
        start, end = get_comma_seperated_units(sql_str, False, start_offset + 10)
        select_parsed.set_order_by(sql_str[start:end])
        if start_offset < end_where:
            end_where = start_offset  # end of the where condition

    start_offset = sql_str.rfind(" limit ")
    if start_offset != -1:
        start, end = get_sql_unit(sql_str, False, start_offset + 7)
        limit = sql_str[start:end]
        if limit.isdigit():
            select_parsed.set_limit(int(limit))
        if start_offset < end_where:
            end_where = start_offset  # end of the where condition

    if where_condition:
        select_parsed.set_where(where_string[7:end_where])


# =======================================================================================================================
# Get unit after the search key from the get_where condition
# =======================================================================================================================
def get_from_where_condition(select_parsed, search_key):
    where_str = select_parsed.get_where()
    start_offset = where_str.rfind(search_key)
    start_offset, end_offset = get_sql_unit(where_str, False, start_offset + len(search_key))
    return where_str[start_offset:end_offset]


# =======================================================================================================================
# Create a structure representing the where condition
# build a structure with (left_operand, operation, right_operand)
# =======================================================================================================================
def process_where_condition(status, select_parsed):
    where_str = select_parsed.get_where()

    ret_value = process_status.SUCCESS

    if not where_str:
        return ret_value

    ret_value, root_node = make_where_tree(status, 0, where_str)  # 1 determines to use PI_NODE in the where tree

    select_parsed.set_where_tree(root_node)

    return ret_value


# =======================================================================================================================
# Create a structure representing the projection list

# The function split_consider_paren returns:
# a) individual functions
# b) offset to parenthesis start
# c) offset to parenthesis end
# d) offset to casting  - casting starts with ::
# e) end of casting

# =======================================================================================================================
def process_projection(status, select_parsed):
    '''
    select_parsed includes the select info
    '''
    ret_val = True
    counter_instruct_func = 0       # Counter to names on the projection list which are not generating an output column
    counter_extended = select_parsed.get_counter_extended()     # Counter for extended results like table name or up of data source node
    projection = select_parsed.get_projection()
    projection_list = split_consider_paren(projection, ",")
    for index, projection_unit in enumerate(projection_list):

        entry = projection_unit[0]
        casting_start =  projection_unit[3]
        if casting_start != -1:
            # Remove the casting from the entry
            casting_end = projection_unit[4]
            projection_name = entry[:casting_start]
            if len(entry) > casting_end + 1:
                projection_name += entry[casting_end:]
            casting_str = entry[casting_start + 2: casting_end]
        else:
            projection_name = entry
            casting_str = None

        paren_start = projection_unit[1]
        if paren_start == -1:
            # not a function - get field name
            start_offset, end_offset = get_sql_unit(projection_name, False, 0)
            field_name = projection_name[start_offset:end_offset]
            inner_name = field_name
            is_function = False
            function_name = ""
            as_name = get_as_name(projection_name, end_offset)

        else:
            # get function name and field name
            paren_end = projection_unit[2]  # offset matching parenthesis
            if paren_end <= paren_start + 1:
                status.add_error("Parsing of SQL projection failed (missing value in parenthesis): '%s'" % projection)
                ret_val = False
                break
            as_name = get_as_name(projection_name, paren_end)
            function_name = projection_name[0:paren_start].strip()
            if function_name in projection_functions.keys():
                counter_instruct_func += 1
                select_parsed.add_proprietary_function(function_name, projection_name[paren_start + 1:paren_end])
                continue  # add proprietry function like increments
            is_function = True
            field_name = projection_name[paren_start + 1:paren_end]
            if not field_name:
                ret_val = False
                break
            if field_name[-1] == ')':
                # for example, PI can have a function such as:  max((cast(a.Value as Int32)))
                inner_name = utils_data.get_iner_brackets(field_name, '(', ')')
                word_length = utils_data.get_word_length(inner_name, 0)
                if len(inner_name) != word_length:
                    inner_name = inner_name[
                                 :word_length]  # take word up to space: max((cast(a.Value as Int32))) --> a.Value
            else:
                inner_name = field_name
            if inner_name.startswith("distinct "):
                inner_name = inner_name[9:]

        select_parsed.add_projection(is_function, inner_name, function_name, as_name, field_name)
        if casting_str:
            select_parsed.add_casting(index - counter_instruct_func + counter_extended, casting_str)

    return ret_val
# =======================================================================================================================
# Create the title for the projections based on the SQL projection list
# =======================================================================================================================
def make_title_list(projection_string) :

    title_list = []

    projection_list = projection_string.split(',')      # Every entry is a projection
    for projection in projection_list:
        index = projection.find(" as ")
        if index != -1:
            # "as" determines the column title
            title_list.append(projection[index + 4:].strip())
        else:
            projection_name =  projection.strip()   # column name or a function over a column name
            if projection_name == '*':
                # select * retrieves the title list from unify_results.make_sql_stmt()
                title_list = None
                break
            title_list.append(projection_name)

    return title_list


# =======================================================================================================================
# find the SQL "AS" name from the string provided
# =======================================================================================================================
def get_as_name(entry, offset):
    index = entry.find(" as ", offset)
    if index != -1:
        offset_start, offset_end = utils_data.find_word_after(entry, index + 4)
        if offset_start != -1:
            as_name = entry[offset_start:offset_end]
        else:
            as_name = ""
    else:
        as_name = ""
    return as_name


# =======================================================================================================================
# Get N units seperated by a comma
# =======================================================================================================================
def get_comma_seperated_units(sql_stmt, ignore_functions, from_offset, to_offset=0):
    start = -1
    end = -1

    if to_offset:
        stmt_length = to_offset  # consider sub-stringt plul origin ms-dev

    else:
        stmt_length = len(sql_stmt)  # consider the entire string
    offset = from_offset

    while 1:
        start_offset, end_offset = get_sql_unit(sql_stmt, ignore_functions, offset, stmt_length)
        if start_offset == end_offset:
            break  # No more units
        if start == -1:
            start = start_offset  # Offset to first unit
        end = end_offset
        if sql_stmt[end - 1] == ',':
            offset = end
        else:

            start_offset, end_offset = utils_data.find_word_after(sql_stmt, end_offset)
            if start_offset == -1:
                break
            if sql_stmt[start_offset] != ',' or len(sql_stmt) == (start_offset + 1):
                # no more units
                break
            offset = start_offset + 1  # find next unit

    return [start, end]


# =======================================================================================================================
# find start offset (which is not a space) and end offset (which is a space or end of stmt) of word or multiple words in a SQL stmt
# =======================================================================================================================
def get_sql_unit(sql_stmt, ignore_functions, from_offset, to_offset=0):
    first_flag = False

    if to_offset:
        stmt_length = to_offset  # consider sub-string
    else:
        stmt_length = len(sql_stmt)  # consider the entire string

    end_offset = stmt_length - from_offset
    if from_offset >= stmt_length:
        return [0, 0]
    previous_char = ""
    index_skip = 0
    for index, ch in enumerate(sql_stmt[from_offset:]):
        if (from_offset + index) < index_skip:
            continue
        if not first_flag:

            if ch == '=':
                start_offset = index
                end_offset = index + 1
                break

            if ch == '\'' or ch == '"' or ch == '`':
                # find matching parenthesis
                start_offset = index
                end_offset = sql_stmt[from_offset + index + 1:].find(ch)
                if end_offset == -1:
                    end_offset = start_offset + 1
                else:
                    end_offset += (start_offset + 2)
                break

            if ch == '!' and (index + 1) < end_offset and sql_stmt[from_offset + index + 1] == '=':
                end_offset = index + 2
                start_offset = index
                break

            if ch == '>' or ch == '<':
                if (index + 1) < end_offset:  # test for >= or <=
                    if sql_stmt[from_offset + index + 1] == '=':
                        end_offset = index + 2
                    else:
                        end_offset = index + 1
                    start_offset = index
                    break

            if ch != ' ':
                first_flag = True  # first char found
                start_offset = index  # offset of first char
            previous_char = ch
            continue

        if ch in end_unit_char.keys():
            if (ch == '(' or ch == ')') and ignore_functions:
                # brackets of a function does not start or stop a unit
                pass
            else:
                if ch == '=' and previous_char == '!':
                    # not equal
                    end_offset = index + 1
                if ch == '(':
                    index_skip, counter_left, counter_right = utils_data.find_parentheses_offset(sql_stmt, from_offset + index, '(', ')')
                    if sql_stmt.find("and", from_offset + index, index_skip) == -1:
                        if sql_stmt.find("or", from_offset + index, index_skip) == -1:
                            index_skip += 1
                            continue  # there is no And or Or inside the parenthesis - skip the parenthesis. Example: now()
                    index_skip = 0
                else:
                    end_offset = index
                break

    return [start_offset + from_offset, end_offset + from_offset]


# =======================================================================================================================
# 1) make all lower case
# 2) replace tab and new line to space
# =======================================================================================================================
def sql_to_standard_format(source_str: str):
    length = len(source_str)
    offset = 0
    offset_copied = 0
    dest_str = ""
    while offset < length:
        char = source_str[offset]
        if char == "\'" or char == "\"":
            if offset and source_str[offset - 1] == 'N':
                # This is a control char in PI: N'XXX' - should keep the capital N
                dest_str += (source_str[offset_copied:offset - 1].lower() + 'N')
            else:
                dest_str += source_str[offset_copied:offset].lower()
            index = source_str[offset + 1:].find(char)  # find matching quotes
            if index == -1:
                offset_copied = offset
                break
            offset_copied = offset + index + 1
            dest_str += source_str[offset:offset_copied]
            offset = offset_copied
        elif char == '\n' or char == '\t':
            dest_str += (source_str[offset_copied:offset].lower() + ' ')  # replace tab or new line with space
            offset_copied = offset + 1
        offset += 1
    dest_str += source_str[offset_copied:length].lower()

    return dest_str


# =======================================================================================================================
# Consider characters in the sql string to organize sql text in dest_sql_str
# old is 2 chars away, previous is one char away, char = current char
# =======================================================================================================================
def test_string(old: str, previous: str, char: str):
    skip_length = 0  # bytes to skip
    delete_length = 0  # bytes to delete
    add_space = False
    replace_char = 0

    if char == ' ':
        if previous == ' ' or previous == '(':
            skip_length = 1
            copy_length = 0  # bytes to copy
        else:
            copy_length = 1

    elif (char == '(' or char == ')') and previous == ' ':
        delete_length = 1
        copy_length = 1

    elif char == ',' and previous == ' ':
        delete_length = 1
        copy_length = 1

    elif previous == ',' and old == ')':
        add_space = True  # sum(field),max(field) --> sum(field), max(field)
        copy_length = 1

    elif char == "=" and previous != ' ' and previous != '!' and previous != '>' and previous != '<':
        add_space = True  # value= 240 -> value = 240
        copy_length = 1

    elif previous == '=' and char != ' ':
        add_space = True  # value =240 -> value = 240
        copy_length = 1

    elif char == '\t':
        replace_char = ' '  # replace the tab with space (tab was set on the message to make a non spaced string)
        copy_length = 1
    else:

        copy_length = 1

    return [copy_length, skip_length, delete_length, add_space, replace_char]


# =======================================================================================================================
# Count the number of fields in parenthesis
# return 0 with error in sql
# index shows word prefixed by left parenthesis, count words to the word suffixed by right parenthesis
# =======================================================================================================================
def count_in_parenthesis(source_sql: list, index: int, offset: int):
    """

    :param source_sql: list with the SQL words
    :param index: index to the list showing the first select field
    :param offset: offset of the parenthesis
    :return: mumber of column names within the function
    """
    if source_sql[index][offset:offset + 1] != '(':
        return 0  # index is on first parenthesis

    counter = 0

    for word in list(source_sql[index:]):

        counter += 1
        if word[-1] == ')':
            return counter
        length = len(word)
        if word[-1] == ',' and word[length - 2] == ')':
            return counter

    return 0


# ==================================================================
# get the column name without the function around the name
# ==================================================================
def get_column_name(word):
    offset_start = is_name_with_function(word)

    if word[-1] == ')':
        offset_end = -1
    else:
        offset_end = len(word)

    return word[offset_start:offset_end]


# ==================================================================
# is with function arround column name
# return offset after the function and parenthesis
# ==================================================================
def is_name_with_function(word):
    if word.startswith("min(") or word.startswith("max(") or word.startswith("sum(") or word.startswith("avg("):
        return 4

    if word.startswith("distinct("):
        return 9

    return 0


# ==================================================================
# Get the projection list from the string
# Example date_trunc(\'month\',timestamp), (extract(day FROM timestamp)::int / 1), parentelement , min(timestamp), max(timestamp)
# --> "date_trunc(\'month\',timestamp)", "(extract(day FROM timestamp)::int / 1)", "parentelement" , "min(timestamp)", "max(timestamp)"
# ==================================================================
def get_projection_list(sql_substr):

    proj_list = []
    start_offset = 0
    entry_flag = False
    left_paren = 0
    for index, char in enumerate(sql_substr):
        if char == ' ' and not entry_flag:
            continue        # Remove leading spaces
        if not entry_flag:
            entry_flag = True
            start_offset = index
        if char == '(':
            left_paren += 1
            continue
        if char == ')':
            left_paren -= 1
            continue
        if left_paren:
            continue        # continue until parenthesis are closed
        if char == ',':
            # add to projection list
            proj_list.append(sql_substr[start_offset:index].rstrip())   # remove trailing spaces
            entry_flag = False
            start_offset = index + 1

    if entry_flag:
        proj_list.append(sql_substr[start_offset:].rstrip())  # remove trailing spaces

    return proj_list



# ==================================================================
# Get the function around the column name
# ==================================================================
def get_column_function(word):
    length = is_name_with_function(word)
    if not length:
        return None
    return word[:length - 1].upper()  # postgres wants upper case


# ==================================================================
# Find the table name which is not in parenthesis
# ====================================================================
def get_remote_table_name(sql_words: list):
    table_name = ""
    previous_word = ""
    for word in reversed(sql_words):
        if word == "from":
            table_name = previous_word
            break
        previous_word = word

    return table_name
# ==================================================================
# Get the table name from SQL string
# ====================================================================
def get_table_name_from_sql(status:process_status, sql_str:str, offset_from:int):

    if not offset_from:
        offset_from = sql_str.find(" from ")

    if offset_from == -1:
        table_name = ""
        status.add_error("Missing table name in SQL command: '%s'" % sql_str)
        ret_val = process_status.ERR_table_name
    else:
        offset_table_name, offset_end_name = utils_data.find_word_after(sql_str, offset_from + 5)
        if offset_table_name == -1:
            status.add_error("Missing table name in SQL command: '%s'" % sql_str)
            ret_val = process_status.ERR_table_name
        else:
            table_name = sql_str[offset_table_name : offset_end_name]
            ret_val = process_status.SUCCESS


    return [ret_val, table_name]


# ==================================================================
# Process a sql function to determine:
# 1) is supported
# 2) is a function
# 3) the function name
# 4) the description of the function
# 5) how many words describe the function
# 6) Is the data determined by the function
# 7) Is the function output is given a name (function followed by "AS"
# ====================================================================
def process_variable(function_words):
    is_supported = True
    is_function = False
    use_name = ""
    description = ""  # the inside of the function parenthesis
    as_name = ""
    words_considered = 1
    words_count = len(function_words)

    word = function_words[0]
    offset = word.find("(")

    if words_count <= 1 and offset == -1:
        is_supported = False  # sql is not complete
    else:
        if offset == -1:
            # not a function

            if word[-1] == ',':  # remove separeted comma
                use_name = word[:-1]
            else:
                use_name = word
            if words_count >= 3:
                if function_words[1] == "as":
                    as_name = function_words[2]
                    words_considered = 3
        else:
            is_function = True
            use_name = word[:offset]
            is_supported, words_considered, end_offset = find_parenthesis_end(function_words, offset)
            if is_supported:
                description = get_function_description(function_words, offset, words_considered, end_offset)
                if words_count >= (words_considered + 2):
                    # find "AS" for this function_name
                    if function_words[words_considered] == "as":
                        as_name = function_words[words_considered + 1]
                        words_considered += 2

    if as_name != "" and as_name[-1] == ',':
        as_name = as_name[:-1]

    return [is_supported, is_function, use_name, description, words_considered, as_name]
# ==========================================================================================
# Given an array of words - return a date-time string + number of words that make the string
# Supported formats:
# 1) 'YY-MM-DD...' - Some date/time string in single quotation
# 2) now()
# 3) date('now','start of month','+1 month','-1 day', '-2 hours', '+2 minutes')
# 4) timestamp('now','start of month','+1 month','-1 day', '-2 hours', '+2 minuts')
# 5) date(some convertible string)
# 6) timestamp(some convertible string)
# 7)        Each of the above with added or removed time - +- x unit for example +3d
# 8) +- x unit for example +3d --> from current
# =========================================================================================
def process_date_time(date_words, offset_first, is_utc):

    global fast_date_time_filter_

    first_char = date_words[offset_first][0]
    if not first_char in fast_date_time_filter_:
        return [0, None]        # A fast filter that this is not a date string

    if is_utc:
        date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    else:
        date_format = "%Y-%m-%d %H:%M:%S.%f"

    offset = offset_first       # offset of first word to consider
    offset_last = len(date_words)
    time_words = 0              # Number of words that make the time-string
    date_time_string = ""
    only_date = False

    while offset < offset_last:

        word = date_words[offset]
        if not time_words and word[0] == '\'':
            date_time_string = utils_columns.get_date_time_str(word, is_utc)
            if date_time_string:
                time_words = 1
                offset += 1
            else:
                break       # Not a date - time string
        elif not time_words and (word == "now()" or word == "date()"):
            if is_utc:
                date_time_string = utils_columns.get_current_utc_time()
            else:
                date_time_string = utils_columns.get_current_time()
            time_words = 1
            offset += 1
            if word[0] == 'd':
                only_date = True

        elif word[0] == '+' or word[0] == '-':
            words_count, diff_str = utils_columns.get_diff_str(date_words, offset) # Transform multiple words representing value added or subtracted to date to one word
            if words_count:
                date_time_string = utils_columns.get_date_time_as_difference(diff_str, date_time_string, is_utc, date_format)
                time_words += words_count
                offset += words_count
                continue
            break
        elif not time_words and word[:5] == "date(" and word[-1] == ')':
            only_date = True
            date_str = word[5:-1].strip()
            date_time_string = utils_columns.function_to_time(date_str)  # Example:  date('now','start of month','+1 month','-1 day', '-2 hours', '+2 minuts')
            if date_time_string:
                date_time_string = date_time_string[:10]    # Only the date
                time_words += 1
                break
            else:
                date_time_string = utils_columns.get_date_time_str(date_str, is_utc)
                if date_time_string:
                    time_words += 1
                    offset += 1
                else:
                    # test for 'YYYY-MM-DD' string
                    date_str = utils_data.remove_quotations(date_str)
                    if utils_data.check_date(date_str):
                        date_time_string = date_str
                        time_words += 1
                        offset += 1
                    break
        elif not time_words and word[:10] == "timestamp(" and word[-1] == ')':
            date_time_string = utils_columns.function_to_time(word[10:-1])
            if date_time_string:
                time_words += 1
                break
            else:
                # test the string inside timestamp("2020-01-01 ...")
                date_time_string = utils_columns.get_date_time_str(word[10:-1], is_utc)
                if not date_time_string:
                    break
                time_words += 1
                offset += 1
        else:
            break

    if only_date and time_words:
        date_time_string = date_time_string[:10]    # return date only

    return [time_words, date_time_string]

# ==================================================================
# Return an array with
# a) individual functions
# b) offset to parenthesis start
# c) offset to parenthesis end
# d) offset to casting  - casting starts with ::
# e) end of casting
# ====================================================================
def split_consider_paren(src_str, split_char):
    split_array = []  # an array with the sub_arrays
    index = 0
    start_offset = 0
    paren_start = -1
    paren_end = -1
    casting_start = -1
    casting_end = -1
    array_len = len(src_str)
    find_first_char = False
    while index < array_len:
        ch = src_str[index]
        if not find_first_char:
            # the first char of the split was not found
            if ch == ' ':
                index += 1
                start_offset += 1
                continue
            find_first_char = True  # first chat which is not space was found
        if ch == split_char:
            if paren_start == -1:
                split_array.append((src_str[start_offset:index].strip(), -1, -1, casting_start, casting_end))
                casting_start = -1
                casting_end = -1
            else:
                split_array.append((src_str[start_offset:index].strip(), paren_start - start_offset, paren_end - start_offset, casting_start, casting_end))
                paren_start = -1
                paren_end = -1
                casting_start = -1
                casting_end = -1
            start_offset = index + 1  # start_offset is set to after the split_char
            find_first_char = False
        elif ch == '(':
            paren_start = index  # keep position of parenthesis
            index, counter_left, counter_right = utils_data.find_parentheses_offset(src_str, index, '(', ')')
            paren_end = index
        elif ch == ':' and index > 2 and src_str[index -1] == ':':
            # casting, start at ::
            casting_end = get_casting_length(src_str, index + 1) # index oints to the offset after ::
            if casting_end > 0:
                # casting was found, it ends at offset == casting_end
                casting_start = index -1 - start_offset
                index = casting_end - 1 # set index to the end of the casting
                casting_end = casting_end - start_offset

        index += 1

    if start_offset < array_len:
        if paren_start == -1:
            split_array.append((src_str[start_offset:index].strip(), -1, -1, casting_start, casting_end))
        else:
            split_array.append(
                (src_str[start_offset:index].strip(), paren_start - start_offset, paren_end - start_offset, casting_start, casting_end))
    return split_array

# ==================================================================
# Return the length of the casting or -1 if not casting
# Example ::float ( 2 ) returns 13
# ==================================================================
def get_casting_length(src_str, start_offset):
    index = start_offset
    end_offset = src_str.find(',', start_offset)
    last_casting_offset = src_str.rfind("::", start_offset, end_offset)
    if last_casting_offset > 0:
        # Multiple casting like ::int::format(":,")
        index = last_casting_offset + 2  # Start from the last casing
    str_len = len(src_str)
    if index >= (str_len - 1)  or src_str[index] == ' ':
        return -1       # Missing :: or space after ::

    while index < str_len:
        ch = src_str[index]
        if ch == '(' or ch == ' ' or ch == ',':
             break   # found the end parenthesis - end process

        index += 1

    if ch == '(':
        index, counter_left, counter_right = utils_data.find_parentheses_offset(src_str, index, '(', ')')
        if counter_left != counter_right:
            index = -1  # Failed to find closing paren
        else:
            index += 1  # After the closing paren

    return index

# ==================================================================
# Find the closing parenthesis on this array
# return the index to the word in the array and the offset in the word
# ====================================================================
def find_parenthesis_end(words, start_offset):
    words_count = len(words)
    offset = start_offset  # first word can start from a location after the function name
    parenthesis_counter = 0
    is_supported = False
    words_considered = words_count  # this is to place a value if parenthesis_counter is never 0 and the value is not set for the return.
    for index in range(words_count):

        counter, end_offset = count_parenthesis(words[index], offset)
        parenthesis_counter += counter

        if parenthesis_counter == 0:
            # processed all that is in the brackets
            is_supported = True
            words_considered = index + 1
            break

        offset = 0

    return is_supported, words_considered, end_offset


# ====================================================================================
# Find the closing parenthesis in a word, return the offset of the closing parenthesis
# =====================================================================================
def count_parenthesis(word, start_offset):
    counter = 0
    end_offset = 0
    index = start_offset
    for ch in word[start_offset:]:
        if ch == '(':
            counter += 1
        elif ch == ')':
            counter -= 1
            end_offset = index
            if not counter:
                break
        index += 1

    return [counter, end_offset]


# ====================================================================================
# get the function from inside the parenthesis
# =====================================================================================
def get_function_description(function_words, start_offset, words_for_function, end_offset):
    description = ""
    index = 0
    while 1:

        word = function_words[index]

        if index == 0:
            if words_for_function == 1:
                description += word[start_offset + 1:end_offset]  # no need in the closing parenthesis
            else:
                description += word[start_offset + 1:]
        elif index == (words_for_function - 1):
            if end_offset:
                description += word[:end_offset]  # no need in the closing parenthesis
        else:
            description += word

        index += 1
        if index >= words_for_function:
            break
        description += " "

    return description


# ====================================================================================
# get the data type within the string
# =====================================================================================
def get_data_type(string):
    type_name = ""
    index = 0
    for ch in string:
        if ch < 'a' or ch > 'z':
            break
        index += 1

    if index:
        type_name = string[:index]
        if type_name == "int":
            type_name = "integer"

    return type_name


# ====================================================================================
# organize the query title - the list of projected fields
# =====================================================================================
def set_query_title(projection_id, new_title: str):
    if not projection_id:
        query_title = "\"" + str(projection_id) + "\":\"" + new_title + "\""
    else:
        query_title = ",\"" + str(projection_id) + "\":\"" + new_title + "\""

    return query_title


# ====================================================================================
# Get the title string as a list
# =====================================================================================
def get_title_list(title_string: str):
    title_data = title_string[12:-4]
    title_data = title_data.replace("\",\"", "\":\"")
    title_array = title_data.split("\":\"")
    projection_list = title_array[1::2]  # some_list[start:stop:step]
    return projection_list


# ====================================================================================
# Create a row in JSON format
# =====================================================================================
def make_output_row(row_number, title_list, data_types_list, query_data):
    '''
    Transform the row info to a JSON structure with proper column names
    row_number - the number of rows to deliver
    title_list - a list with the colums name
    is_tring_output - a list with a True/False value if need to represent a string (with double quotation)
    query_data - the data to transform
    '''
    global get_value_by_type_

    json_row = ""
    if isinstance(query_data, dict) and "Query" in query_data:
        data_list = query_data["Query"]
        if len(data_list):
            if row_number == 1:
                json_row = "{\"Query\":[{"
            else:
                json_row = ",{"

            data = data_list[0]

            columns_count = len(title_list)
            for x in range(columns_count):
                if x:
                    json_row += ','
                json_row += ("\"" + title_list[x] + "\":")
                key = str(x)
                if key in data.keys():
                    data_type = data_types_list[x]
                    if data_type in get_value_by_type_:
                        json_row += get_value_by_type_[data_type](str(data[str(x)]))    # The outer str is because of casting
                    else:
                        json_row += get_value_by_type_["string"](data[str(x)])
                else:
                    json_row += "NULL"  # add NULL if field not provided

            json_row += "}"

    return json_row


# ====================================================================================
# Create an insert statement from the rows returned in a query
# =====================================================================================
def make_insert_row(row_number, table_name, counter_local_fields, local_fields, query_data):
    '''
    row_number - sequential number from the caller
    table_name - the local table in the system_query dbms
    row_number - the number of columns returned
    local_fields - the rows name and data type
    query_data - a dictionary with the columns to insert
    '''
    sql_stmt = ""
    if isinstance(query_data, dict) and "Query" in query_data:
        data_list = query_data["Query"]
        if len(data_list):

            sql_stmt = "INSERT INTO " + table_name + " values "

            data = data_list[0]

            sql_stmt += "("
            columns_count = counter_local_fields  # the number of fields on the create stmt (or 0 for select *)

            for x in range(columns_count):
                if x:
                    sql_stmt += ','
                key = str(x)
                if key in data.keys():
                    column_val = data[str(x)]
                    if column_val == "":
                        sql_stmt += "null"  # empty value replaced with explicit null
                    else:
                        sql_stmt += "'" + column_val + "'"
                else:
                    sql_stmt += "null"  # add NULL if field not provided

            sql_stmt += ");"

    return sql_stmt


# ==================================================================
# To String format - format a string reply
#      json_data = [list((db_cursor.description[i][0], value)\
#                    for i, value in enumerate(row)) for row in rows_data ]
# ==================================================================
def format_db_rows(status, db_cursor, output_prefix, rows_data, title_list, type_list):
    '''
    Organize a reply as a JSON Structure
    status - process status
    db_cursor - The object reresenting the physical database
    output_prefix - the root of the JSON to set
    rows_data - the data to include in the JSON structure
    title_list - the projections column names
    type_list - the projection data types
    '''

    global get_value_by_type_

    data = "{\"" + output_prefix + "\":[{"

    entries_title = len(title_list)

    formatted_row = ""
    first_row = True

    if entries_title == 1 and isinstance(title_list[0],int):
        # Only one entry and the entry is an int --> start counting from this value
        first_column = title_list[0]
        entries_title = 0
    else:
        first_column = 0        # start counting from 0

    for row in rows_data:

        if not first_row:
            formatted_row = ",{"

        for i, value in enumerate(row):

            new_value = unify_data_format(value)

            if output_prefix == "Query":
                if i < entries_title:
                    description = title_list[i]
                else:
                    description = str(first_column + i)  # this makes column names smaller + avoids non-unique names
            elif db_cursor:
                description = db_cursor.description[i][0]
            else:
                if i == 0:
                    description = "column_name"
                else:
                    description = "data_type"

            if new_value == None:
                formatted_row += "\"" + description + "\":" + "\"\""
            else:
                if type_list:
                    # format according to data type
                    data_type = type_list[i]

                    if data_type in get_value_by_type_:
                        method = get_value_by_type_[data_type]
                    else:
                        method = get_value_by_type_["string"]

                else:
                    method = get_value_by_type_["string"]

                str_value = str(new_value)

                if id(method) == id(rep_value_as_str) and '"' in str_value:
                    #  use \\ to escape double quotations
                    str_value = esc_quotations(str_value)


                projected_value = method(str_value)

                formatted_row += "\"" + description + "\":" + projected_value

            data += formatted_row
            formatted_row = ","

        first_row = False
        data += "}"
    data += "]}"

    return data


# ==================================================================
#  use \\ to escape double quotations - othrwise JSON formats fail
# i.e.:  "10 seconds" -->  \\"10 seconds\\"
# ==================================================================
def esc_quotations(str_val):
    new_string = ""
    index = 0
    offset = 0
    while 1:
        next_offset = str_val[index:].find('"')
        if next_offset == -1:
            new_string += str_val[offset:]      # Add remainder
            break
        index += next_offset
        if not index or str_val[index-1] != '\\':
            new_string += str_val[offset:index] + '\\'
            offset = index
        index += 1

    return new_string

# ==================================================================
# Unify data format
# ==================================================================
def unify_data_format(value):

    if isinstance(value, int):
        new_value = value
    elif isinstance(value, decimal.Decimal):
        new_value = float(value)
    elif isinstance(value, datetime.datetime):
        new_value = value.strftime('%Y-%m-%d %H:%M:%S.%f')
    elif isinstance(value, datetime.time):
        new_value = value.strftime('%H:%M:%S.%f')
    elif isinstance(value, datetime.date):
        new_value = value.strftime('%Y-%m-%d')
    else:
        new_value = value

    return new_value
# ==================================================================
# Unify data types - data_types an array with the def of the data type
# return the unified data type
# ==================================================================
def unify_data_type(data_type):
    global map_data_types
    new_type = data_type[0].lower()
    if new_type in map_data_types.keys():
        new_type = map_data_types[new_type]
    return new_type
# ==================================================================
# Get the data types to be used on the blockchain in  the create stmt
# ==================================================================
def get_al_data_types(data_type:str):
    global al_data_type
    if data_type.lower() in al_data_type:
        new_type = al_data_type[data_type]
    else:
        new_type = data_type
    return new_type
# ==================================================================
# Parse the where condition
# '(sensor.name = \'voltage quality\' or sensor.name = \'voltage\') and element.name = mtr_k1e2h313056'
# ==================================================================
def make_where_tree(status, node_type, where_str):
    root_node = collection_nodes[node_type](where_str)
    ret_value = root_node.parse(status, node_type)

    return [ret_value, root_node]  # return root of tree


# ==================================================================
# Create an array that includes AND statements
# Example: sensor.name = \'voltage\') and element.name = mtr_k1e2h313056'
# ==================================================================
def update_and_stmt(status, and_array, where_str):
    ret_value = process_status.SUCCESS
    end_offset = 0
    len_where = len(where_str)

    while 1:
        if end_offset:
            # not the first time here - need to see an ;'and' or an 'or'
            start_offset, end_offset = get_sql_unit(where_str, False, end_offset)
            if not start_offset:
                break  # no more data
            if where_str[start_offset:end_offset] != "and":
                ret_value = process_status.Failed_to_parse_sql
                status.add_keep_error(f"SQL parsing failed: Missing 'and' before {where_str[start_offset:end_offset]} in the WHERE condition: '{where_str}'")
                break

        start_offset, end_offset = get_sql_unit(where_str, True, end_offset)
        if not end_offset:
            ret_value = process_status.Failed_to_parse_sql
            status.add_keep_error("Process failed with parsing of WHERE condition: '%s'" % where_str)
            break

        if where_str[start_offset] == '(':
            # since this is a statement with only 'and' - parenthesis can be ignored
            left_operand = where_str[start_offset + 1:end_offset]
        else:
            left_operand = where_str[start_offset:end_offset]

        start_offset, end_offset = get_sql_unit(where_str, False, end_offset)
        if not end_offset:
            ret_value = process_status.Failed_to_parse_sql
            status.add_keep_error("Process failed with parsing of WHERE condition: '%s'" % where_str)
            break  # no more data - but missong operands
        operation = where_str[start_offset:end_offset]

        start_offset, end_offset = get_sql_unit(where_str, False, end_offset)
        if not end_offset:
            ret_value = process_status.Failed_to_parse_sql
            status.add_keep_error("Process failed with parsing of WHERE condition: '%s'" % where_str)
            break  # no more data- but missong operands

        if where_str[start_offset] == '(':
            # Right operand is a function
            end_offset, counter_left, counter_right = utils_data.find_parentheses_offset(where_str, start_offset, '(', ')')
            if end_offset <= start_offset:
                ret_value = process_status.Failed_to_parse_sql
                status.add_keep_error("Process failed with parsing of WHERE condition: '%s'" % where_str)
                break  # no more data- but missong operands
            end_offset += 1
            right_operand = where_str[start_offset:end_offset]
        else:
            right_operand = where_str[start_offset:end_offset]
            if end_offset < len_where and where_str[end_offset] == ')':
                # since this is a statement with only 'and' - parenthesis can be ignored
                end_offset += 1

        if not ret_value:
            if utils_columns.is_valid_operator(operation):
                right_operand = utils_data.remove_quotations(right_operand)
                and_array.append((left_operand, operation, right_operand))
            else:
                ret_value = process_status.Failed_to_parse_sql
                break

    return ret_value


# =======================================================================================================================
# Update the tree structure representing the where condition with PI Info
# =======================================================================================================================
def make_pi_conditions(status, node, pi_cursor):
    node.reset_direction()  # flag - this node was not visited
    while node:
        direction = node.next_direction()  # flag - this node was not visited
        if direction == 1:
            next_node = node.get_left_node()
        elif direction == 2:
            next_node = node.get_right_node()
        else:
            if node.is_use_or_dict():
                ret_value = update_pi_info_or(status, node, pi_cursor)
            else:
                ret_value = update_pi_info_and(status, node, pi_cursor)
            break

        if next_node:
            ret_value = make_pi_conditions(status, next_node, pi_cursor)
            if ret_value:
                break

    return ret_value


# ==================================================================
# Update PI Info on the nodes of the tree representing the WHERE conditions
# in the case that the OR statement is represented as a HASH
# ==================================================================
def update_pi_info_or(status, node, pi_cursor):
    or_key = node.get_or_key()
    pi_column_info = pi_cursor.get_pi_column_info(or_key)
    if pi_column_info[0]:  # test for pi_attr_name
        node.set_pi_or_dict_info(pi_column_info[0], pi_column_info[3],
                                 pi_column_info[4])  # keep the PI name + type + level
        ret_val = process_status.SUCCESS
    else:
        ret_val, key, value_string, layer, prefix_search, key_length = get_traversal_info(status, or_key,
                                                                                          "")  # attribute name, operand, value --> entry[0], entry[1], entry[2]
        if not ret_val:
            node.set_pi_or_dict_info(key, "string", layer)

    return ret_val


# ==================================================================
# Update PI Info on the nodes of the tree representing the WHERE conditions
# Make PI where conditions based on the SQL where conditions
# identify time condition and make a time array that is used in the readings
# Identify pi instructions for the traversal - these are PI LAYER NAME + DOt + VALUE
# ==================================================================
def update_pi_info_and(status, node, pi_cursor):
    ret_value = process_status.SUCCESS

    where_parsed = node.get_and_array()  # this is an array of conditions that needs to exist (with AND clause)
    for entry in where_parsed:
        pi_column_info = pi_cursor.get_pi_column_info(entry[0])  # Get PI attribute info by SQL field name
        if pi_column_info[0]:  # test for pi_attr_name
            # Map the SQL Name to PI
            pi_attribute_type = pi_column_info[3]
            # append left operand (with pi name), condition, right operand, attribute type
            operand = entry[1]
            value = utils_data.remove_quotations(entry[2])
            prefix_search, key_length, value_string = is_prefix_search(value)

            node.update_pi_and_array(pi_cursor, pi_column_info[0], operand, value_string, pi_attribute_type,
                                     pi_column_info[4], prefix_search, key_length,
                                     False)  # pi_column_info[4] has the level in the hierarchy

            # if pi_attribute_type.startswith("time"):
            #    # build the key for the search based on the time
            #    value_formatted = utils_columns.get_pi_time_format(status, value)
            #
            #    pi_cursor.set_search_key("time", operand, value_formatted)
        else:
            # Look for traversal conditions

            ret_val, key, value_string, layer, prefix_search, key_length = get_traversal_info(status, entry[0], entry[
                2])  # attribute name, value --> entry[0], entry[2]

            if not ret_val and layer:
                node.update_pi_and_array(pi_cursor, key, entry[1], value_string, "string",
                                         layer, prefix_search, key_length,
                                         True)  # pi_column_info[3] has the level in the hierarchy

            else:
                status.add_keep_error(
                    "PI select: The column '%s' in the WHERE condition is not mapped to a PI attribute name" % entry[0])
                ret_value = process_status.Failed_to_parse_sql
                break

    return ret_value


# ==================================================================
# Identify * in string for prefix search
# ==================================================================
def is_prefix_search(value):
    index = value.find("*")
    if index != -1:
        # prefix match
        prefix = value[:index]
        prefix_search = True
    else:
        # full keyword match
        prefix = value
        prefix_search = False

    return [prefix_search, index, prefix]


# ==================================================================
# Compare values of prefixes
# ==================================================================
def compare_values(is_prefix, prefix_length, value1, value2):
    if is_prefix == True:
        # prefix search
        if value1 != value2[:prefix_length]:
            return False
    else:
        # Full keyword match
        if value1 != value2:
            return False
    return True


# ==================================================================
# Extend existing operand by new operand.
# Example > + = --> >=
# ==================================================================
def extend_operand(x_operand, y_operand):
    x_value = operand_values[x_operand]
    y_value = operand_values[y_operand]
    return add_operands[x_value][y_value]


# ==================================================================
# Get the traversal info from the JSON in the blockchain
# ==================================================================
def get_traversal_info(status, layer_name, value):
    ret_value = process_status.SUCCESS
    offset = layer_name.find('.')
    if offset > 0 and offset < (len(layer_name) - 2):
        # the condition is a traversal condition like -- reading.name = voltage
        # test for - layer in digits (dot) key
        if layer_name[:offset].isdigit():
            layer = int(layer_name[:offset])
            key = layer_name[offset + 1:]
            ret_val = True
        else:
            ret_val = False

        if not ret_val:
            ret_value = process_status.Failed_to_parse_sql
            status.add_keep_error(
                "PI select: The column '%s' in the WHERE condition is not mapped to a PI attribute name" % layer_name)
        else:

            if value:
                value = utils_data.remove_quotations(value).lower()
                prefix_search, key_length, value_string = is_prefix_search(value)
            else:
                prefix_search = False
                key_length = 0
                value_string = ""

            return [ret_value, key, value_string, layer, prefix_search, key_length]

    return [ret_value, "", "", 0, False, 0]


# ==================================================================
# Update dbms and table names on SQL select stmt
# sql_stmt starts with sql dbms_name text select ....
# ==================================================================
def update_dbms_table_names(dbms_name, table_name, sql_stmt):
    length = utils_data.get_word_length(sql_stmt, 4)
    if sql_stmt[4: 4 + length] != dbms_name:
        # replace database name
        new_stmt = "sql " + dbms_name + sql_stmt[4 + length:]
    else:
        new_stmt = sql_stmt

    if table_name:
        # replace table name
        index = new_stmt.find("\tfrom\t")
        if index > -1:
            length = utils_data.get_word_length(new_stmt, index + 6, '\t')
            if new_stmt[index + 6: index + 6 + length] != table_name:
                new_stmt = new_stmt[0: index + 6] + table_name + new_stmt[index + 6 + length:]

    return new_stmt


# ==================================================================
# Update table names on SQL create stmt
# extend_name - extends the name of the table and indexes to address the partition
# ==================================================================
def change_table_name_to_par(par_name, sql_stmt):
    index = sql_stmt.find("(")
    if index > 0:
        # table name is the word before parenthesis --> CREATE TABLE if not exists TABLE_NAME (
        offset_start, offset_end = utils_data.find_word_before(sql_stmt, index)

        if offset_start and offset_end:
            # Change the craete table name
            new_stmt = sql_stmt[:offset_start] + par_name + sql_stmt[offset_end:]

            table_name = sql_stmt[offset_start:offset_end]

            # Change the CREATE INDEX part
            stmt_parts = new_stmt.split("CREATE INDEX")
            if len(stmt_parts) > 1:
                # change the table name on the indexes
                # Example: CREATE INDEX ping_sensor_insert_timestamp_index ON ping_sensor(insert_timestamp);
                new_stmt = stmt_parts[0]  # The create table part
                for entry in stmt_parts[1:]:
                    index = entry.find("(")
                    if index > 0:
                        offset_start, offset_end = utils_data.find_word_before(entry, index)
                        if offset_start and offset_end:
                            index_stmt = entry[:offset_start] + par_name + entry[offset_end:]  # Add CREATE INDEX
                            index = index_stmt.find("ON")
                            if index > 0:
                                offset_start, offset_end = utils_data.find_word_before(index_stmt, index)
                                index_stmt = index_stmt[:offset_start] + par_name + index_stmt[
                                                                                    offset_start + len(table_name):]
                                new_stmt += ("CREATE INDEX" + index_stmt)
        else:
            new_stmt = ""
    else:
        new_stmt = ""
    return new_stmt


# ==================================================================
# Get table name from Select stamt
# ==================================================================
def get_select_table_name(stmt):
    index = stmt.find(" from ")
    if index > -1:
        offset_start, offset_end = utils_data.find_word_after(stmt, index + 6)
        if offset_start != -1:
            if stmt[offset_end - 1] == ';':
                offset_end -= 1  # ignore the comma at the end of the table name (if exists)
            table_name = stmt[offset_start:offset_end]
        else:
            table_name = ""
    else:
        table_name = ""
    return table_name


# ==================================================================
# Get the offset of the table name
# ==================================================================
def get_offsets_table_name(stmt):
    index = stmt.find(" from ")
    if index > -1:
        offset_start, offset_end = utils_data.find_word_after(stmt, index + 6)
        if offset_start != -1:
            if stmt[offset_end - 1] == ';':
                offset_end -= 1  # ignore the comma at the end of the table name (if exists)
    else:
        offset_start = -1
        offset_end = -1

    return [offset_start, offset_end]


# ==================================================================
# Get location for where condition (if where is not in the sql string)
# ==================================================================
def get_offset_after_table_name(stmt):
    index = stmt.find(" from ")
    if index > -1:
        offset_start, offset_end = utils_data.find_word_after(stmt, index + 6)
        offset = offset_end + 1  # offset after table name + space
    else:
        offset = -1
    return offset


# ==================================================================
# In a SQL statement portion at the position next to the operand,
# return the operand and the value - in a staement of "a > b" and
# an offset before ">" , return ">" and "b"
# ==================================================================
def get_operator_value(sql_stmt, sql_offset):
    sql_length = len(sql_stmt)
    offset = sql_offset
    while offset < sql_length:
        if sql_stmt[offset] != ' ':
            break  # the offset to the operand
        offset += 1

    # find the operand
    operand = ""
    for key in operand_values.keys():
        if key == sql_stmt[offset:offset + len(key)]:
            operand = key
            break

    if not operand:
        return ["", ""]

    offset += len(key)

    # get the VALUE
    offset_start, offset_end = utils_data.find_word_after(sql_stmt, offset)
    if offset_start == -1:
        return ["", ""]

    value = sql_stmt[offset_start:offset_end]
    return [operand, value]


# ==================================================================
# In a CREATE TABLE or CREATE VIEW - return the list of columns and their type
# ==================================================================
def create_to_column_info(status, create_stmt):
    offset_start, offset_end = utils_data.find_word_after(create_stmt, 7)
    create_type = create_stmt[offset_start:offset_end]
    if create_type == "view":
        create_view = True
    else:
        create_view = False

    # fields are in the create stmt between parentheses
    offset_columns = create_stmt.find("(")  # find parentheses after table name
    if offset_columns == -1:
        status.add_error("Error in 'create' statement - failed to find parenthesis with column info: %s" % create_stmt)
        return [process_status.Failed_to_parse_sql, "", None]

    offset_start, offset_end = utils_data.find_word_before(create_stmt, offset_columns)

    if not offset_start or offset_end <= offset_start:
        status.add_error("Error in 'create' statement - failed to identify table/view name: %s" % create_stmt)
        return [process_status.Failed_to_parse_sql, "", None]

    table_name = create_stmt[offset_start:offset_end].lower()

    offset_end, counter_left, counter_right = utils_data.find_parentheses_offset(create_stmt, offset_columns, '(', ')')
    if offset_end == -1:
        status.add_error("Error in 'create' statement: '%s'" % create_stmt)
        return [process_status.Failed_to_parse_sql, "", None]

    fields_list = create_stmt[offset_columns + 1:offset_end].split(',')
    if not len(fields_list):
        status.add_error(
            "Error in 'create' statemnt of table '%s' (error in columns info): '%s' - " % (table_name, create_stmt))
        return [process_status.Failed_to_parse_sql, "", None]

    schema_list = []
    for entry in fields_list:
        words = entry.split()
        len_col_info = len(words)
        if len_col_info < 2:
            status.add_error("Error in 'create '%s' statemnt of '%s': column '%s' is missing data type" % (
            create_type, table_name, words[0]))
            return [process_status.Failed_to_parse_sql, "", None]  # has to have field name and type

        if create_view:
            # take the mapping information
            if words[1] == "using":
                # this is the mapping info
                if len_col_info < 4:
                    status.add_error(
                        "Error in 'create view' statemnt of '%s': missing mapping info for column: '%s' - error in fields info" % (
                        table_name, words[0]))
                    return [process_status.Failed_to_parse_sql, "", None]
                mapped_field = words[2]
                field_type = unify_data_type(words[3:])
            elif utils_data.test_text(words, 1, ["not", "used"]):
                mapped_field = ""
                field_type = ""
            else:
                mapped_field = words[0]  # Same field name
                field_type = unify_data_type(words[1:])
            schema_list.append((words[0], field_type, mapped_field))
        else:
            # Create table
            field_type = unify_data_type(words[1:])
            schema_list.append((words[0], field_type))

    return [process_status.SUCCESS, table_name, schema_list]


# ==================================================================
# Make new projection list based on the column info
# With a view, the column dictionary is set with:
# (view_id), (column_type), (table column name) as f (view column_name)
# ==================================================================
def make_new_projection(status, columns_dict, src_projection):
    ret_val = process_status.SUCCESS
    projection = ""
    src_list = src_projection.split(',')

    for entry in src_list:
        # go over the words to determine if the words are defined columns
        offset_start, offset_end = utils_data.find_word_after(entry, 0)
        column_name = entry[offset_start:offset_end]

        if column_name in columns_dict.keys():
            # column recognized
            column_to_use = columns_dict[column_name][2]  # The mapped name
            if not column_to_use:
                # Ignore this column
                column_to_use = "''"  # It allows to return NULL for this field on the result set
        else:
            column_to_use = column_name

        if projection:
            projection += ", "
        projection += column_to_use

    return [ret_val, projection]


# ==================================================================
# Make new projection list based on the mapping of columns in the view declaration
# ==================================================================
def make_new_where(status, columns_dict, src_where):
    ret_val = process_status.SUCCESS
    where_cond = ""
    src_list = src_where.split(' and ')

    for entry in src_list:
        # go over the words to determine if the words are defined columns
        left_operand, operation, right_operand = split_compare_components(entry)
        if left_operand in columns_dict.keys():
            # column recognized
            column_to_use = columns_dict[left_operand][2]  # The mapped name
            if not column_to_use:
                # Ignore this column
                continue
        else:
            column_to_use = left_operand

        if where_cond:
            where_cond += " and "

        where_cond += column_to_use + " " + operation + right_operand

    return [ret_val, where_cond]


# ==================================================================
# Get comparison components. For exampl: a > b return: "a" ">" "b"
# ==================================================================
def split_compare_components(compare_str):
    left_operand = ""
    right_operand = ""
    operation = ""

    step = 0  # changes 0 -> 1 -> 2 to retrieve: left -> operation -> right
    for index, ch in enumerate(compare_str):
        if step == 0:
            # Set Left Operand
            if ch == ' ':
                if left_operand:
                    step += 1  # Get operation
                continue
            if ch == '=' or ch == '>' or ch == '<':
                step += 1
                operation = ch
                continue
            left_operand += ch
            continue
        if step == 1:
            # Set Operation
            if ch == '=' or ch == '>' or ch == '<':
                operation += ch
                continue
            if ch == ' ':
                if not operation:
                    continue
            right_operand = compare_str[index:].strip()  # take the rest
            break
    return [left_operand, operation, right_operand]


# ==================================================================
# Get offset on where to plug values in the SQL statment
# For example: "select * from table where a = %s and b = %s" -
# Return an array with [("a","=",offset_a_val),("b","=",offset_b_val)]
# ==================================================================
def get_input_offsets(status, sql_command):
    info_array = []
    offset_search = 0
    ret_val = process_status.SUCCESS

    while 1:
        # Find the column name and operation that the data is associated with query_info
        offset_variable = sql_command.find("%s", offset_search)
        if offset_variable == -1:
            break

        # find the token that represents the column value
        offset_var_start, offset_var_end = utils_data.get_word_offsets(sql_command, offset_variable)
        if sql_command[offset_var_end - 1] == ')':
            offset_var_end -= 1
        column_value = sql_command[offset_var_start:offset_var_end]

        # find operation
        offset_start, offset_end = utils_data.find_word_before(sql_command, offset_var_start)
        if offset_start < offset_end:
            operation = sql_command[offset_start:offset_end]
        else:
            status.add_keep_error("Failed to merge input data to SQL statement: %s" % sql_command)
            ret_val = process_status.ERR_SQL_failure
            break

        # find column name
        offset_col_name, offset_end = utils_data.find_word_before(sql_command, offset_start)

        index = sql_command.rfind('(', offset_col_name, offset_end)
        if index != -1:
            offset_col_name = (index + 1)
        if offset_col_name < offset_end:
            column_name = sql_command[offset_col_name:offset_end]
        else:
            status.add_keep_error("Failed to merge input data to SQL statement: %s" % sql_command)
            ret_val = process_status.ERR_SQL_failure
            break

        info_array.append((offset_col_name, column_name, operation, offset_var_start, offset_var_end))

        offset_search = offset_var_end  # search from the end of thhe current

    return [ret_val, info_array]


# ==================================================================
# find location of brackets containing a comparison
# where a > b and (c > d or e < f) --> find the offset to the "(" brackets
# ==================================================================
def find_comparison_brackets(where_condition):
    start_offset = where_condition.find("(")
    if start_offset > -1:
        counter, end_offset = count_parenthesis(where_condition, start_offset)
        if end_offset > -1:
            if where_condition.find(" and ", start_offset, end_offset) == -1:
                if where_condition.find(" or ", start_offset, end_offset) == -1:
                    start_offset = -1  # Not a comparison inside the brackets
        else:
            start_offset = -1  # No comparison inside the brackets

    return start_offset
