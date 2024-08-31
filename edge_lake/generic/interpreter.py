"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.params as params
import edge_lake.generic.process_status as process_status
from edge_lake.generic.utils_columns import input_to_date
from edge_lake.generic.utils_io import test_dir_exists_and_writeable

# =======================================================================================================================
# Interprets AnyLog commands
# =======================================================================================================================

# =======================================================================================================================
# Creates a dictionary of key value pairs from a statement a = b and c = d etc.
# If key_list is provided, test that the keys are available in the dictionary which is returned
# If exact_match is True - only keys in the list can appear in the returned dictionary.
#
# cmd_keys is structured as a dictionary, every entry includes:
# key: keyword,
# 0) data type
# 1) flag if must exists
# 2) a flag to add to a counter which is returned to the caller
# 3) is unique - True allows only one instance
# =======================================================================================================================
def get_dict_from_words(status, cmd_words, offset_first, end_offset, cmd_keys, exact_match):
    ret_val = process_status.SUCCESS
    returned_counter = 0  # counter of the entries that are flagged to be counted

    dictionary = {}

    if end_offset:
        words_count = end_offset
    else:
        words_count = len(cmd_words)

    offset = offset_first
    while offset < words_count:
        offset, key = get_term_from_cmd(status, "", cmd_words, offset, words_count, "=")
        if offset == -1:
            # failed to get a key with eaual sign afterwards
            ret_val = process_status.ERR_command_struct
            break

        if cmd_keys:
            if key not in cmd_keys.keys():
                # no arbitrary keys
                status.add_error("Unknown key in AnyLog command: %s" % key)
                ret_val = process_status.ERR_command_struct
                break
            else:
                # with info on key
                info = cmd_keys[key]
        else:
            info = None

        offset, value = get_term_from_cmd(status, key, cmd_words, offset, words_count, "and")
        if offset == -1:
            # failed to get a value with 'and' afterwards
            ret_val = process_status.ERR_command_struct
            break

        ret_val, counter = place_value_in_dict(status, dictionary, key, value, info)
        if ret_val:
            break

        returned_counter += counter  # count the occurrence of this value

    if not ret_val and cmd_keys:
        if exact_match:
            if len(dictionary) != len(cmd_keys):
                status.add_error("Redundant values in AnyLog command: %s" % ' '.join(cmd_words))
                ret_val = process_status.ERR_command_struct
        else:
            for key, value in cmd_keys.items():
                if value[1]:
                    # this entry must exists
                    if not key in dictionary.keys():
                        status.add_error("Missing '%s' in command variables" % key)
                        ret_val = process_status.ERR_command_struct
                        break
                    if dictionary[key][0] == "":
                        status.add_error("Missing value for key '%s' in command variables" % key)
                        ret_val = process_status.ERR_command_struct
                        break

    return [ret_val, returned_counter, dictionary]


# =======================================================================================================================
# Add the value to the value array
# =======================================================================================================================
def place_value_in_dict(status, dictionary, key, value, info):
    ret_val = process_status.SUCCESS
    returned_counter = 0
    if isinstance(value, str) and value[0] == '(' and value[-1] == ')' and key != "sort":
        if info == None:
            # Take as a single string
            if not add_value(dictionary, key, value):
                # This is a unique key which gets multiple values
                status.add_error("Duplicate key value pair: %s : %s" % (key, value))
                ret_val = process_status.ERR_command_struct
        elif info[0] == "nested":
            # The value of the key represents a dictionary
            ret_val = process_nested_dict(status, dictionary, key, value)

        else:
            # Multiple comma seperated values in a parenthesis
            multiple_values = value[1:-1].split(',')
            if not len(multiple_values):
                # This is a unique key which gets multiple values
                status.add_error("Missing value to key: %s" % key)
                ret_val = process_status.ERR_command_struct
            else:
                for value in multiple_values:
                    value = value.strip()
                    if info:
                        ret_val, value = process_value(status, key, value, info[0])
                        if ret_val:
                            break

                    if not add_value(dictionary, key, value):
                        # This is a unique key which gets multiple values
                        status.add_error("Duplicate key value pair: %s : %s" % (key, value))
                        ret_val = process_status.ERR_command_struct
                        break
                    if info and info[2]:
                        # count the occurrence of this value
                        returned_counter += 1

    else:
        if info:
            if info[0] == "nested":
                status.add_error("Missing parenthesis to represent the value for the key: '%s'" % key)
                ret_val = process_status.ERR_command_struct
            else:
                ret_val, value = process_value(status, key, str(value), info[0])
        if not ret_val:
            if not add_value(dictionary, key, value):
                # This is a unique key which gets multiple values
                status.add_error(f"Duplicate key value pair: {key} : {value}")
                ret_val = process_status.ERR_command_struct
            else:
                if info and info[2]:
                    # count the occurrence of this value
                    returned_counter += 1
    if not ret_val:
        if info and info[3]:
            if len(dictionary[key]) > 1:
                # This is a unique key which gets multiple values
                status.add_error("Multiple values to key: %s" % key)
                ret_val = process_status.ERR_command_struct

    return [ret_val, returned_counter]

# =======================================================================================================================
# Process a nested dictionary
# For example, Topic is nested: run mqtt client where ... and topic=(name=!mqtt_topic and dbms=!company_name and table = !table_name)
# =======================================================================================================================
def process_nested_dict(status, dictionary, key, value):
    # run the dictionary again on these attribute value pairs.
    # example: run mqtt client where broker = "mqtt.eclipse.org" and topic = (name = $SYS/# and dbms = lsl_demo and table =ping_sensot and qos = 3)
    multiple_values = value[1:-1]
    sub_str, left_brackets, right_brakets = utils_data.cmd_line_to_list_with_json(status, multiple_values, 0, 0)
    if left_brackets != right_brakets:
        status.add_error("Wrong number of brackets used by the key: '%s' and value: %s" % (key, value))
        ret_val = process_status.ERR_command_struct
    elif sub_str == None:
        status.add_error("Error in the representation of key: '%s' and value: %s" % (key, value))
        ret_val = process_status.ERR_command_struct
    else:
        ret_val, returned_counter, value = get_dict_from_words(status, sub_str, 0, 0, None, False)
        if ret_val:
            status.add_error("The value for the key '%s' is not structured correctly: '%s'" % (key, value))
            ret_val = process_status.ERR_command_struct

    if not ret_val:
        if not add_value(dictionary, key, value):
            # This is a unique key which gets multiple values
            status.add_error("Duplicate key value pair: '%s' : '%s'" % (key, multiple_values))
            ret_val = process_status.ERR_command_struct
        for nested_key, nested_val in value.items():
            # Replace variable with a value
            if isinstance(nested_val, list):
                for counter, entry in enumerate(nested_val):
                    if isinstance(entry, str):
                        new_val = params.get_value_if_available(entry)
                        if not new_val:
                            status.add_error("Missing value for the name: '%s'" % entry)
                            ret_val = process_status.ERR_command_struct
                            break
                        if new_val != entry:
                            nested_val[counter] = new_val  # Replace with a value from the dictionary
            elif isinstance(nested_val, str):
                new_val = params.get_value_if_available(nested_val)
                if not new_val:
                    status.add_error("Missing value for the name: '%s'" % nested_val)
                    ret_val = process_status.ERR_command_struct
                else:
                    if new_val != nested_val:
                        value[nested_key] = new_val
    return ret_val
# =======================================================================================================================
# Find the key - up to the equal sign and return key in offset
# =======================================================================================================================
def get_term_from_cmd(status, key, cmd_words, start_offset, end_offset, end_word):
    term = ""
    offset = -2
    for offset, word in enumerate(cmd_words[start_offset:end_offset]):
        if word == end_word:
            break
        if not word:
            status.add_error("Wrong command structure after: %s" % ' '.join(cmd_words[:start_offset + offset]))
            offset = -1
            break
        if word[0] == '!' or word[0] == '$':
            # use dictionary to replace with value or get value assigned to environment variable
            value = params.get_value_if_available(word)
        else:
            value = word
        if not value:
            status.add_error("Missing dictionary definition for: '%s'" % word)
            offset = -1
            break
        if not isinstance(value, str) or not isinstance(term, str):
            # value pulled in not a string
            term = value # Retrieved from the dictionary with a data type which is not a string (!anylog_server_port.int)
        elif offset:
            term += (' ' + str(value))
        else:
            term += str(value)


    if offset >= 0:
        if end_word == '=' and word != '=':
            status.add_error("Missing '=' sign for key: '%s'" % term)
            offset = -1
        elif isinstance(term, str) and not term:        # was npt pulled from the dictionary
            if key:
                status.add_error("Missing value for key: '%s'" % key)
            else:
                status.add_error("Wrong key value pairs in command: '%s'" % ' '.join(cmd_words))
            offset = -1  # error
        else:
            # return the location after the end_word ('and' or equal sign)
            offset = start_offset + offset + 1
    elif offset == -2:
        status.add_error("Wrong key value pairs using key: '%s'" % key)
        offset = -1  # error value tested by the caller

    return [offset, term]

# =======================================================================================================================
# Test the data type
# =======================================================================================================================
def process_value(status, key, input_value, data_type):
    value = params.get_value_if_available(input_value)
    ret_val = process_status.SUCCESS
    new_value = None
    if data_type == "str":
        if isinstance(value,str):
            new_value = value
        else:
            status.add_error("Value '%s' (for key '%s') is not of type string" % (value, key))
            ret_val = process_status.ERR_command_struct
    elif data_type == "int":
        if not value.isnumeric():
            status.add_error("Value '%s' (for key '%s') is not numeric" % (value, key))
            ret_val = process_status.ERR_command_struct
        else:
            new_value = int(value)
    elif data_type == "dir":
        # directory structure
        if isinstance(value, str):
            ret_val = test_dir_exists_and_writeable(status, value, True)
            separator = params.get_path_separator()
            if not ret_val:
                # Add the path separator at the end of the directory name
                if value[-1] == '/' or value == '\\':
                    if value[-1] != separator:
                        new_value = value[:-1] + separator
                    else:
                        new_value = separator
                else:
                    new_value = value + separator
        else:
            status.add_error("Directory name '%s' (for key '%s') is not of type string" % (value, key))
            ret_val = process_status.ERR_command_struct

    elif data_type == "bool":
        if value.lower() == "true":
            new_value = True
        elif value.lower() == "false":
            new_value = False
        else:
            status.add_error("Value '%s' (for key '%s') is not boolean" % (value, key))
            ret_val = process_status.ERR_command_struct
    elif data_type == "int.time":
        # translate to seconds
        time_array = value.split()  # needs to show number + units
        if len(time_array) != 2:
            status.add_error("Key: '%s' showing wrong time value and units: '%s'" % (key, value))
            ret_val = process_status.ERR_command_struct
        else:
            new_value = utils_data.time_to_seconds(time_array[0], time_array[1])
            if new_value == None:
                status.add_error("Wrong time unit: Key: '%s' is provided with the wrong time value: '%s'" % (key, value))
                ret_val = process_status.ERR_command_struct
    elif data_type == "int.storage":
        # translate to bytes from KB, MB, GB
        ret_val, new_value = get_storage(status, key, value)
    elif data_type == "ip.port":
        index = value.find(':')
        if index > 1 and index < (len(value) - 1) and value[index + 1:].isnumeric():
            new_value = value
        else:
            status.add_error("Key: '%s' is provided with the wrong IP:Port value: '%s'" % (key, value))
            ret_val = process_status.ERR_command_struct
    elif data_type.startswith("int.from."):
        # for example: data type is: 'int.from.file_name' and value needs to be 'file_name[X]'
        # in this example - get X
        if value.startswith(data_type[9:]):
            offset_value = len(data_type[9:])
            if value[offset_value] == '[' and value[-1] == ']':
                if value[offset_value + 1:-1].isnumeric():
                    new_value = int(value[offset_value + 1:-1])
                elif len(value[offset_value + 1:-1]) > 1 and (
                        value[offset_value + 1:offset_value + 2] == '-' and value[offset_value + 2:-1].isnumeric()):
                    new_value = int(value[offset_value + 1:-1])  # negative value in brackets
        if new_value == None:
            status.add_error("Key: '%s' is provided with the wrong value: '%s'" % (key, value))
            ret_val = process_status.ERR_command_struct
    elif data_type == "date":
        # get a date string or value that is subtracted from current
        ret_code, new_value = input_to_date(status, value)
        if not ret_code:
            ret_val = process_status.ERR_command_struct
    elif data_type == "format":
        if value != "json" and value != "table":
            status.add_error("Key: '%s' is provided with the wrong value: '%s', optional values are 'json' or 'table'" % (key, value))
            ret_val = process_status.ERR_command_struct
        else:
            new_value = value
    elif data_type == "float":

        try:
            new_value = float(value)
        except:
            status.add_error("Value '%s' (for key '%s') is not of type float" % (value, key))
            ret_val = process_status.ERR_command_struct
    elif data_type == "sort":
        # Used in: get data nodes where sort = (2,3)
        if len(value) <=2 or value[0] != '(' or value[-1] != ')':
            status.add_error("Sort values '%s' (for key '%s') are not in the correct format" % (value, key))
            ret_val = process_status.ERR_command_struct
        else:
            new_value = value

    return [ret_val, new_value]
# =======================================================================================================================
# Translate data storage units to bytes
# =======================================================================================================================
def get_storage(status, key, value):
    ret_val = process_status.SUCCESS
    len_val = len(value)
    if len_val > 2 and (value[-1] == 'B' or value[-1] == 'b'):
        v_word = value[-2].upper()
        if v_word == 'K':
            multipier = 1024
            number_length = -2
        elif v_word == 'M':
            multipier = 1048576
            number_length = -2
        elif v_word == 'G':
            multipier = 1073741824
            number_length = -2
        elif v_word == 'T':
            multipier = 1099511627776
            number_length = -2
        else:
            status.add_error("The value for the key '%s' needs to be a number or KB or MB or GB" % key)
            ret_val = process_status.ERR_command_struct
    else:
        multipier = 1
        number_length = len_val

    if value[:number_length].isnumeric():
        new_value = int(value[:number_length]) * multipier
    else:
        status.add_error("Value '%s' (for key '%s') is not numeric" % (value, key))
        new_value = ""
        ret_val = process_status.ERR_command_struct

    return [ret_val, new_value]


# =======================================================================================================================
# Test if the key exists and compare the value provided to the first in the array
# =======================================================================================================================
def test_one_value(conditions, key, test_value):
    if key in conditions:
        if isinstance(conditions[key], list):
            ret_val = test_value in conditions[key]  # conditions[key] is the array with the values for the key
        else:
            value = conditions[key]
            if type(test_value) == type(value):
                ret_val = test_value == value
            else:
                ret_val = False
    else:
        ret_val = False

    return ret_val


# =======================================================================================================================
# Test that the values match the allowed values
# =======================================================================================================================
def test_values(status, conditions:dict, key:str, allowed_values:dict):
    '''
    conditions - the dictionary with keys and values
    key - the key to test
    allowed_values - test that the values assigned to the key are allowed values
    '''

    ret_val = process_status.SUCCESS
    if key in conditions:
        values = conditions[key]

        for value in values:
            if value in allowed_values:
                break
            else:
                index = value.find('@')
                if index > 0 and value[:index + 1] in allowed_values:
                    break       # allowed

                status.add_error("The value '%s' is not allowed for the key '%s'" % (value, key))
                ret_val = process_status.Wrong_key_value
                break

    return ret_val
# =======================================================================================================================
# Test if the key exists and return the first value from the array
# If the key does not exists, return None
# =======================================================================================================================
def get_one_value(conditions, key):
    if key in conditions:
        if isinstance(conditions[key], list):
            value = conditions[key][0]  # conditions[key] is the array with the values for the key
        else:
            value = conditions[key]
        if isinstance(value, str):
            return params.get_value_if_available(value)
        return value
    return None

# =======================================================================================================================
# get one value if exists, test the data type, return value if data type validated
# If the key does not exists, return None
# =======================================================================================================================
def get_test_one_value(conditions, key, data_type):
    if key in conditions:
        if isinstance(conditions[key], list):
            value = conditions[key][0]  # conditions[key] is the array with the values for the key
        else:
            value = conditions[key]
        if isinstance(value, str):
            if data_type == str:
                return params.get_value_if_available(value)
            return None
        elif isinstance(value, data_type):
            return value
    return None
# =======================================================================================================================
# Test if the key exists and return the first value from the array
# If the key does not exists, return None
# =======================================================================================================================
def get_one_value_or_default(conditions, key, default):

    if conditions:
        if key in conditions:
            value = conditions[key][0]  # conditions[key] is the array with the values for the key
            if isinstance(value, str):
                return params.get_value_if_available(value)
            return value
        if isinstance(default, str):
            value = params.get_value_if_available(default)
        else:
            value = default
    else:
        if isinstance(default, str):
            value = params.get_value_if_available(default)
        else:
            value = default
    return value
# =======================================================================================================================
# Add one value
# =======================================================================================================================
def add_value(conditions, key, value):
    if not key in conditions:
        conditions[key] = []
    else:
        if value in conditions[key]:
            return False

    conditions[key].append(value)
    return True

# =======================================================================================================================
# Test if value exists or add a value
# =======================================================================================================================
def add_value_if_missing(status, conditions, key, value):
    # test that err_dir is defined
    ret_val = process_status.SUCCESS
    existing_value = get_one_value(conditions, key)
    if not existing_value:
        if isinstance(value, str):
            new_value = params.get_value_if_available(value)
        else:
            new_value = value
        if new_value == None or (isinstance(new_value,str) and new_value == ""):
            status.add_error("Param declaration is missing definition for key \'%s\'" % key)
            ret_val = process_status.Missing_configuration
        else:
            add_value(conditions, key, new_value)
    return ret_val

# =======================================================================================================================
# Add value and validate data type
# =======================================================================================================================
def test_add_value(status, conditions, key, value, data_type):
    # test that err_dir is defined
    ret_val = process_status.SUCCESS
    existing_value = get_one_value(conditions, key)
    if not existing_value:
        ret_val, new_value = process_value(status, key, value, data_type)
        if not ret_val:
            add_value(conditions, key, new_value)
    return ret_val



# =======================================================================================================================
# Update / Add values to existing dictionary
# =======================================================================================================================
def update_dict(all_values, new_values):
    for key, value in new_values.items():
        all_values[key] = value  # replace with new


# =======================================================================================================================
# Given a name string, if the name string is in the format: [X], whereas X is a number, the process returns entry X from the array
# =======================================================================================================================
def get_string_from_offset(conditions, key, source_struct: list):
    name_str = get_one_value(conditions, key)
    if name_str:
        if len(name_str) > 2 and name_str[0] == '[' and name_str[-1] == ']' and name_str[1:-1].isnumeric():
            index = int(name_str[1:-1])
            if len(source_struct) > index:
                return source_struct[index]  # return the string in the source_struct which is addressed by index
        name_str = params.get_value_if_available(name_str)

    return name_str


# =======================================================================================================================
# Test that all conditions are satisfied with a unique value
# =======================================================================================================================
def test_unique_values(conditions):
    for value in conditions.values():
        if len(value) > 1:
            return False  # more than one value
    return True  # only single values
# =======================================================================================================================
# Return an array where each entry is the value that satisfies the key in the keys_list
# =======================================================================================================================
def get_multiple_values(conditions, keys_list, default_list):
    values_list = []
    for index, key in enumerate(keys_list):
        value = get_one_value(conditions, key)
        if value == None and default_list:
            value = default_list[index]  # Get the default
        values_list.append(value)
    return values_list
# =======================================================================================================================
# Return all values assigned to the key or NULL
# =======================================================================================================================
def get_all_values(conditions, key, default = None):

    if key in conditions:
        values = conditions[key]
    else:
        values = default
    return values
# =======================================================================================================================
# Test the nested dictionaries maintain the needed keys
# =======================================================================================================================
def test_nested_data(status, conditions, key, nested_info ):  # test all attribute pairs exists

    ret_val = process_status.SUCCESS
    list_of_dict = conditions[key]   # An array of dictionaries
    if not isinstance(list_of_dict, list):
        status.add_error("Command values are not properly provided for the key: '%s'" % key)
        ret_val = process_status.ERR_command_struct
    else:
        for entry in list_of_dict:
            if not isinstance(entry, dict):
                status.add_error("Command values are not properly provided for the key: '%s'" % key)
                ret_val = process_status.ERR_command_struct
                break
            for test_info in nested_info:
                # Every test_info includes: [key name, data type, exact_match]
                tested_key = test_info[0]
                tested_type = test_info[1]
                exact_match = test_info[2]
                # test that all keys exists
                if tested_key not in entry.keys():
                    if exact_match:
                        status.add_error("The key '%s' is missing with the declaration of '%s'" % (tested_key, key))
                        ret_val = process_status.ERR_command_struct
                        break
                else:
                    for value in entry[tested_key]:
                        if not isinstance(value, tested_type):
                            status.add_error("The value '%s' is not of data type '%s'" % (str(entry[tested_key]), str(tested_type)))
                            ret_val = process_status.ERR_command_struct
                            break
                if ret_val:
                    break
            if ret_val:
                break

    return ret_val
# =======================================================================================================================
# For a nested key - value pairs: Add default value if value is missing
# =======================================================================================================================
def set_nested_default(status, conditions, key, nested_key, default_value):
    ret_val = process_status.SUCCESS
    list_of_dict = conditions[key]  # An array of dictionaries
    if list_of_dict:
        for entry in list_of_dict:
            if not isinstance(entry, dict):
                status.add_error("Command values are not properly provided for the key: '%s'" % key)
                ret_val = process_status.ERR_command_struct
                break
            if not nested_key in entry:
                entry[nested_key] = [default_value]
    return ret_val
# =======================================================================================================================
# Test values within the nested dictionaries maintain the needed range
# =======================================================================================================================
def test_nested_range(status, conditions, key, nested_key, range ):

    ret_val = process_status.SUCCESS
    list_of_dict = conditions[key]  # An array of dictionaries
    if not isinstance(list_of_dict, list):
        status.add_error("Command values are not properly provided for the key: '%s'" % key)
        ret_val = process_status.ERR_command_struct
    else:
        for entry in list_of_dict:
            if not isinstance(entry, dict):
                status.add_error("Command values are not properly provided for the key: '%s'" % key)
                ret_val = process_status.ERR_command_struct
                break
            if nested_key in entry.keys():
                values_array = entry[nested_key]
                for tested_value in values_array:
                    try:
                        value = int(tested_value)
                    except:
                        status.add_error("The value '%s' for the key '%s' is not a number" % (str(tested_value), key))
                        ret_val = process_status.ERR_command_struct
                        break

                    if value < range[0] or value > range[1]:
                        status.add_error("Command value set for '%s' and '%s' (%u) are not within the needed range" % (key, nested_key, int(value)))
                        ret_val = process_status.ERR_command_struct
                        break
    return ret_val
# =======================================================================================================================
# Add the default directories to the conditions variables
# =======================================================================================================================
def add_defualt_dir(status, conditions, directories:list):
    '''
    For these directories - If the key is not in the dictionary it will be added
    '''
    ret_val = process_status.SUCCESS
    for directory in directories:
        ret_val = test_add_value(status, conditions, directory, f"!{directory}", "dir")
        if ret_val:
            break

    return ret_val
# =======================================================================================================================
# Get the list of values as a dictionary with attribute name  value
# =======================================================================================================================
def get_value_dict(conditions, name_list):

    value_diict = {}
    for attr_name in name_list:
        if attr_name in conditions:
            value_diict[attr_name] = conditions[attr_name][0]
    return value_diict

