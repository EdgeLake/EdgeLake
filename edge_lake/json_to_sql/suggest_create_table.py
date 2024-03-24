"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

# import json
import os

import edge_lake.generic.utils_io as utils_io
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.process_status as process_status
import edge_lake.json_to_sql.mapping_policy as mapping_policy

# ==================================================================
#
# Iterate through dictionary and figure out the data type of each column.
# Based on the data type, store proper datatype to be used in CREATE. 
# If data type not found (ie isn't an if/else option) use VARCHAR
#
# Dictionary key - column name | Dictionary value - list of values for given column 
# 
# Example: 
#    Input: {'col1': ['2000-01-01 00:00:00', '2000-01-01 00:00:01', '2000-01-01 00:00:02'], 
#            'col2': [1, 2, 3], 
#            'col3': ['a', 'aa', 'aaa']
#           } 
#    Output: {'col1': 'TIMESTAMP NOT NULL DEFAULT NOW()', 'col2': 'INT', 'col3': 'VARCHAR'} 
# 
# ==================================================================
def get_column_types(status: process_status, data: list):
    """
    Understand the columns and types
    :args:
       data:dict - data generated from file
    :param:
       columns:dict - column name and data type statement
    :return:
       column names and types
    """

    try:  # check if data type is valid (ie expect length)
        if len(data) == 0:
            status.add_error("Column types can't be determined: empty file")
            return None
    except:
        status.add_error("Column types can't be determined: file data is not available")
        return None

    columns = {}
    # Go over all the rows and determine column name and column type
    if isinstance(data, list) is True or isinstance(data, tuple) is True:
        for index, line in enumerate(data):  # Go over all entries in the file
            jdata = utils_io.format_reading(status, line)  # make each entry a dictionary where columns names ate the keys
            if jdata == None:
                status.add_error("Json data in line #%u is not formatted correctly" % index)
                return None  # failed to create a JSON
            for key in jdata:
                set_column_name_type(status, columns, key, jdata[key])

    return columns


# ==================================================================
#  Based on the data in a file, determine the data type
# ==================================================================
def set_column_name_type(status, columns, column_name, column_value):
    if column_name in columns.keys():
        existing_type = columns[column_name]
    else:
        existing_type = ""

    if isinstance(column_name, str) and len(column_name):

        data_type = get_column_type_by_value(column_value)  # send the columndata from the row to determine the data type

        if existing_type:
            if existing_type != data_type:
                resolved_type = resolve_data_type(existing_type, data_type)
                if resolved_type != existing_type:
                    columns[
                        column_name] = resolved_type  # change data type (because of a value that does not fit the existing type)
        else:
            columns[column_name] = data_type


# ==================================================================
# Determine the data type to use when data types conflict
# ==================================================================
def resolve_data_type(one_type, second_type):
    if one_type.startswith("CHAR("):
        count_chars = 1
    else:
        count_chars = 0
    if second_type.startswith("CHAR("):
        count_chars += 1

    if count_chars:
        if count_chars == 2:
            # both are chars
            if int(one_type[5:-1]) > int(second_type[5:-1]):
                data_type = one_type
            else:
                data_type = second_type
        else:
            if one_type == 'VARCHAR' or second_type == 'VARCHAR':
                data_type = 'VARCHAR'
            else:
                data_type = "CHAR(32)"
    else:
        # alphabetically organize such that only one combination is tested
        if one_type < second_type:
            data_type_1 = one_type
            data_type_2 = second_type
        else:
            data_type_1 = second_type
            data_type_2 = one_type

        data_type = 'VARCHAR'  # default

        if data_type_1 == 'DECIMAL':
            if data_type_2 == 'INT':
                data_type = 'DECIMAL'
            elif data_type_2 == 'FLOAT':
                data_type = 'FLOAT'
        elif data_type_1 == 'FLOAT':
            data_type = "FLOAT"
        elif data_type_1 == "BIGINT":
            if data_type_2 == 'INT' or data_type_2 == 'DECIMAL':
                data_type = 'BIGINT'

    return data_type


# ==================================================================
# Determine the data type based on a string value
# ==================================================================
def get_column_type_by_value(value):

    if isinstance(value, bool):
        # Needs to be before INT as bool is a subclass of INT
        data_type = 'BOOLEAN'
    elif isinstance(value, int):
        if abs(value) > 0x5F5E0FF:     # t needs to be 0x7fff ffff - but we assume big int if the numbers are near this range
            data_type = 'BIGINT'
        else:
            data_type = 'INT'

    elif isinstance(value, float):
        if value and len(str(value).split(".")[-1]) >= 1 and len(str(value).split(".")[-1]) <= 5:
            data_type = 'DECIMAL'
        else:
            data_type = 'FLOAT'
    elif isinstance(value, str):
        if not value:
            data_type = 'VARCHAR'
        elif utils_data.check_uuid(value) is True:
            data_type = 'UUID'
            # data_type = 'UUID DEFAULT uuid_generate_v4()'
        elif utils_data.check_timestamp(value) is True:
            data_type = 'TIMESTAMP NOT NULL DEFAULT NOW()'
        elif utils_data.check_date(value) is True:
            data_type = 'DATE NOT NULL DEFAULT NOW()'
        elif utils_data.check_time(value) is True:
            data_type = 'TIME NOT NULL DEFAULT NOW()'
        elif utils_data.check_ip_address(value) is True:
            data_type = "CIDR"
        else:
            data_len = len(value)
            if data_len <= 19 and value.find('.') != -1 and utils_data.isfloat(value):
                data_type = 'FLOAT'
            elif value.isdigit() or (value[0] == '-' and data_len > 1 and value[1:].isdigit()):
                if data_len < 9:      # 0x5F5E0FF it needs to be 0x7fff ffff - but we assume big int if the numbers are near this range
                    data_type = 'INT'
                elif utils_data.isint(value):
                    data_type = 'BIGINT'
                else:
                    data_type = 'VARCHAR'
            elif data_len <= 8:
                data_type = 'CHAR(%u)' % data_len
            else:
                data_type = 'VARCHAR'
    else:
        # Data type like list or dictionary are set to 'VARCHAR'
        data_type = 'VARCHAR'

    return data_type
# ==================================================================
# 
# Given table_name and columns withh data types (from get_column_types)
# generate create table
# 
# Example: 
#   table_name: data
#   columns: {'col1': 'TIMESTAMP NOT NULL DEFAULT NOW()', 'col2': 'INT', 'col3': 'VARCHAR'}
#   output: 
#      CREATE TABLE data(
#         col1 TIMESTAMP NOT NULL DEAFULT NOW(), 
#         col2 INT,
#         col3 VARCHAR
#      ); 
#
# ==================================================================
def create_table_sql(table_name: str, columns: dict, with_tsd_info:bool):
    """
    CREATE table stmt
    :args:
       table_name:str - table_name
       columns:dict - dict of columns and they datatype stmts
    :param:
       create_table:str - create table stmt
       primmary_key_list:str - List of PRIMARY KEY columns (UUID type by default)
       timestamp_key:list - for datetime or timestamp columns set to be KEYs
    :return:
       create_table stmt
    """
    index_list = []

    create_table = "CREATE TABLE IF NOT EXISTS %s(\n\trow_id SERIAL PRIMARY KEY,\n\tinsert_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),\n\ttsd_name CHAR(3),\n\ttsd_id INT," % table_name
    column_string = "\n\t%s %s,"
    index_key = "\nCREATE INDEX %s_%s_index ON %s(%s);"

    if with_tsd_info:
        tsd_index =  "\nCREATE INDEX %s_tsd_index ON %s(tsd_name, tsd_id);" % (table_name, table_name) # An index on the fields that map to the TSD tables
        index_list.append(tsd_index)

    uuid_id = False
    for name, val in columns.items():
        if name.isdigit():
            column_name = "_" + name
        else:
            column_name = name.lower()
        column_val = val.lower()
        create_table += column_string % (column_name, column_val)
        if "uuid" in column_val and uuid_id is False:
            # create_table = 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";\n' + create_table
            index_list.append(index_key % (table_name, column_name, table_name, column_name))
            uuid_id = True
        if ('timestamp' in column_val) or ('datetime' in column_val):
            index_list.append(index_key % (table_name, column_name, table_name, column_name))
        if 'sensor_id' in column_name:
            index_list.append(index_key % (table_name, column_name, table_name, column_name))


    create_table = create_table.rsplit(",", 1)[0] + "\n);"
    for index in sorted(index_list):
        create_table += index
    create_table += index_key % (table_name, "insert_timestamp", table_name, "insert_timestamp")
    return create_table

# ==================================================================
# 
# Process creates a CREATE TABLE statement based on file name 
# 
# ==================================================================
def suggest_create_table(status: process_status, file_name: str, dbms_name:str, table_name: str, with_tsd_info:bool, instructions):
    """
    From JSON convert to CREATE TABLE
    :args:
       file_name:str - file to use
    :return:
       create table stmt
    """

    file_name = os.path.expanduser(os.path.expandvars(file_name))
    if os.path.isfile(file_name) is False:
        status.add_error("Failed to create table: File (`%s`) doesn't exist" % file_name)
        return ""

    if instructions and "schema" in instructions["mapping"].keys():
        # use the Schema for the create stmt
        schema = instructions["mapping"]["schema"]
        if "table" in instructions.keys():
            tb = instructions["table"]  # get table name from instruction
        else:
            tb = utils_io.get_table_name_from_file_name(file_name)
        if tb == "":
            status.add_error("Failed to create table: Failed to get table name from file: %s" % file_name)
            return ""

        columns = {}
        ret_val = mapping_policy.policy_to_columns_dict(status, dbms_name, table_name, instructions, columns)
        if ret_val:
            return ""       # failed to create schema
        create_stmt = create_table_sql(tb, columns, with_tsd_info)

    else:
        # Use the data to generate the create stmt

        data = utils_io.read_all_lines_in_file(status, file_name)
        if data == None:
            status.add_error("Failed to create table: File (`%s`) has no data" % file_name)
            return ""
        utils_data.organize_list_entries(data)  # remove empty lines and suffix and prefix spaces

        if table_name:
            tb = table_name
        else:
            tb = utils_io.get_table_name_from_file_name(file_name)
            if tb == "":
                status.add_error("Failed to create table: Failed to get table name from file: %s" % file_name)
                return ""
        columns = get_column_types(status, data)
        if columns == None:
            status.add_error("Failed to create table: Failed to get column types from file: %s" % file_name)
            return ""

        create_stmt = create_table_sql(tb, columns, with_tsd_info)

    return create_stmt


# ==================================================================
#
# Get columns list from the mapping policy
# Update an array with:
#           Source Attr. Name, Column Name, Data Type, Default Value
# ==================================================================
def policy_to_columns_list(status, dbms_name, table_name, instruct, columns):
    '''
    instruct - the mapping policy
    columns - a list to be updated with the info
    '''

    schema = instruct["mapping"]["schema"]

    # 2 columns
    columns.append((None, "row_id", "SERIAL PRIMARY KEY", None))  # Unique auto incr ID
    columns.append((None, "insert_timestamp", "TIMESTAMP", "now"))  # timestamp by system
    columns.append((None, "tsd_name", "char(3)", "   "))  # The name of the TSD table
    columns.append((None, "tsd_id", "int", 0))  # The ID of the JSON file descriptor in the TSD table

    for column_name, column_val in schema.items():

        if isinstance(column_val, dict):
            column_list = [column_val]  # Make a list with one attribute
        elif isinstance(column_val, list):
            # Multiple dictionaries in the list
            column_list = column_val
        else:
            policy_id = instruct["mapping"]["id"] if "id" in instruct["mapping"] else ""
            status.add_error("Wrong column info in 'schema' in mapping policy '%s'" % (policy_id))
            ret_val = process_status.ERR_wrong_json_structure
            break

        for column_info in column_list:

            if "source_name" in column_info.keys():
                source_name = column_info["source_name"]
            else:
                source_name = column_name

            if "dbms" in column_info and column_info["dbms"] != dbms_name:
                continue  # Incorrect option, get to the next option
            if "table" in column_info and column_info["table"] != table_name:
                continue  # Incorrect option, get to the next option

            if "type" in column_info.keys():
                data_type = column_info["type"].lower()
            else:
                data_type = None

            if "default" in column_info.keys():
                default_value = column_info["default"]
            else:
                default_value = None

            columns.append((source_name, column_name, data_type, default_value))
            break   # data type was found

