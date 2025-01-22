"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import os
import sys
import threading
import re

import edge_lake.tcpip.tcpip_server as tcpip_server
import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_columns as utils_columns
import edge_lake.cmd.member_cmd as member_cmd

# List of chars that terminate a key
end_key_chars = {
    ".": 1,
    "(": 1,
    ")": 1,
    "[": 1,
    "]": 1,
    "=": 1,
    " ": 1,
    "+": 1,
}

cond_signs = {
    "==" : 1,
    ">"  : 1,
    ">=" : 1,
    "<"  : 1,
    "<=" : 1,
    "!=" : 1,
    "not" : 1,
    "contains" : 1,
}

al_func_ = {        # ANyLog Functions
             # Size      Extention      Data type  Output_Flag (Format)
    '.int' : (4,         ".int",        int,       0),
    '.str' : (4,         ".str",        str,       0),
    'bool' : (5,         ".bool",       bool,      0),
    'loat' : (6,         ".float",      float,     0),
    '.len' : (4,         ".len",        int,       2),
    'name' : (5,         ".name",       str,       1),        # To lower and replace space with underscore
    'lies' : (8,         ".replies",    int,       3),        # Number of nodes replied
    'diff':  (5,         ".diff",       int,       4),        # Number of nodes missing (len - replies)
}


TCP_BUFFER_SIZE = 2048

DEFAULT_IP = "127.0.0.1"

user_defined = dict()
job_process_ = dict()    # Dictionary pointing to values in job handle - every entry is a key pointing to Job ID and Unique Job ID

if sys.platform.startswith('win'):
    os_wind = True
    path_separator = '\\'
    path_niu = '/'      # Not in use sign
else:
    path_separator = '/'
    path_niu = '\\'      # Not in use sign

    os_wind = False

def init(home_path):
    add_param("ip",tcpip_server.get_ip() )  # add default IP
    add_param("external_ip", tcpip_server.get_external_ip())  # add external IP
    add_param("anylog_server_port", str(2048))  # add sever default port
    add_param("anylog_rest_port", str(2148))  # add RESTful API port

    add_param("io_buff_size", str(TCP_BUFFER_SIZE))  # add default port

    user_name = os.path.expanduser(os.path.expandvars('$HOME')).split("/")[-1]
    add_param("db_user", '%s@127.0.0.1:demo' % user_name)
    add_param("db_port", '5432')

    set_directory_locations(home_path)


# =======================================================================================================================
# Return the path separator ('/' or '\\') based on the OS
# =======================================================================================================================
def get_path_separator():
    global path_separator
    return path_separator
# =======================================================================================================================
# Return the Non In Use separator
# =======================================================================================================================
def get_reverse_separator():
    global path_niu
    return path_niu

# =======================================================================================================================
# Test if a directory - Ignore the OS
# =======================================================================================================================
def is_directory(dir_name):
    global path_separator
    global path_niu
    return dir_name[-1] == path_separator or dir_name[-1] == path_niu

# =======================================================================================================================
# Set all directory locations
# =======================================================================================================================
def set_directory_locations(home_path):

    global path_separator
    global path_niu


    if home_path[-1] == '\\' or  home_path[-1] == '/':
        home_path = home_path[:-1]
    home_path = os.path.expanduser(os.path.expandvars(home_path))
    network_path = home_path + path_separator + "EdgeLake" + path_separator

    add_param("edgelake_path", home_path)  # Path to EdgeLake

    add_param("blockchain_sql", "%sblockchain%sblockchain.sql" % (network_path, path_separator))  # Blockchain file
    add_param("blockchain_file", "%sblockchain%sblockchain.json" % (network_path, path_separator))  # Blockchain file
    add_param("blockchain_new", "%sblockchain%sblockchain.new" % (network_path, path_separator))  # Blockchain file

    add_param("blockchain_dir", "%sblockchain" % network_path)  # Blockchain file

    add_param("local_scripts", "%sscripts" % network_path)  # Location for User Scripts

    add_param("prep_dir",
              "%sdata%sprep" % (network_path, path_separator))  # dir where data is being preped to be used by the node
    add_param("watch_dir",
              "%sdata%swatch" % (network_path, path_separator))  # data placed in this directory is ready to be used ("or sent) by the node
    add_param("bkup_dir", "%sdata%sbkup" % (network_path, path_separator))  # data that's been used ("or sent) by the node
    add_param("err_dir", "%sdata%serror" % (network_path, path_separator))  # dir where that returns an error is stored
    add_param("dbms_dir", "%sdata%sdbms" % (network_path, path_separator))  # default dbms dir
    add_param("archive_dir", "%sdata%sarchive" % (network_path, path_separator))  # default archive dir
    add_param("distr_dir", "%sdata%sdistr" % (network_path, path_separator))
    add_param("pem_dir", "%sdata%spem" % (network_path, path_separator))    # default dir for PEM files
    add_param("test_dir", "%sdata%stest" % (network_path, path_separator))  # default dir for test files
    add_param("blobs_dir", "%sdata%sblobs" % (network_path, path_separator))  # default dir for video files
    add_param("bwatch_dir", "%sdata%sbwatch" % (network_path, path_separator))  # default dir for blob info
    add_param("tmp_dir", "%sdata%stmp" % (network_path, path_separator))  # TMP dir

    add_param("id_dir", "%sanylog" % network_path)  # default archive dir

# =======================================================================================================================
# Save the name of the thread doing the keyboard input
# =======================================================================================================================
def save_input_thread():
    add_param("input_thread", threading.current_thread().name)

# =======================================================================================================================
# Test if current thread is doing the input
# =======================================================================================================================
def is_input_thread(thread_name):
    input_thread = get_value_if_available("!input_thread")
    return input_thread == thread_name

# =======================================================================================================================
# Add value to dictionary
# =======================================================================================================================
def key_to_job(assignment, job_id, unique_job_id):

    if len(assignment) > 2 and assignment[-2:] == '[]' or assignment[-2:] == '{}':
        key = assignment[:-2]
        job_process_[key] = (job_id, unique_job_id)
        if key in user_defined:
            del user_defined[key]  # Delete current value from User dictionary

# =======================================================================================================================
# Add value to dictionary
# =======================================================================================================================
def add_param(key: str, value):

    if not isinstance(key,str) or not len(key):
        return  # key cant be null or non-string

    if len(key) >= 2 and (key[-2:] =='[]' or key[-2:] == "{}"):
        # remove brackets
        key = key[:-2]
        if not key:
            return      # Only brackets are not allowed

    if key in job_process_:
        # Any value entered to the dictionary, removes the value that links to the Job Instance
        del job_process_[key]  # Link to data in job instance

    value_str = str(value)
    if len(value_str) > 1 and (value_str[0] == '!' or value_str[0] == '$'):
        value_str = get_value_if_available(value_str)

    value_str_len = len(value_str)
    if isinstance(value, str):
        if not value or not value_str_len or (value_str_len == 2 and value_str[0] == value_str[1] and (value[0] == '"' or value[0] == "'")):
            # value is null - remove from dictionary if exists
            if key in user_defined:
                del user_defined[key]       # User dictionary

            return

    # If into a dictionary - update the dictionary
    ret_val = process_status.SUCCESS
    dict_updated = False
    if key[-1] == ']':
        # Test if dictionary assignment
        offset = key.find('[')
        if offset > 0 and offset < len(key) - 2:
            dict_name = key[:offset]         # This is an update of a dictionary or a new dictionary
            dict_new_key = key[offset:]
            # Set a dictionary struct (as a JSON object) in the local dictionary
            ret_val = set_dictionary(None, dict_name, [dict_new_key, '=', value])
            dict_updated = True

    if not ret_val and not dict_updated:
        user_defined[key] = value_str


# =======================================================================================================================
# Delete Param
# =======================================================================================================================
def del_param(key: str):
    if key in user_defined:
        del user_defined[key]
    if key in job_process_:
        del job_process_[key]       # Link to data in job instance
    if key in job_process_:
        del job_process_[key]       # Link to data in job instance
# =======================================================================================================================
# Test that key is in dictionary and the value provided is the assigned value for the key
# =======================================================================================================================
def test_value_by_key(key, tested_value):
    value = get_param(key)
    if not value:
        return False  # no such key
    if value == tested_value:
        return True
    return False
# =======================================================================================================================
# Get value by key, return empty string if the key is not in the dictionary
# =======================================================================================================================
def get_param(key):

    try:
        value = user_defined[key]
    except:
        if key in job_process_:
            # Get the data from the Job Handle
            value = member_cmd.get_data_struct_from_job(None, job_process_[key][0],job_process_[key][1] )
        else:
            value = ""


    return value

# =======================================================================================================================
# Test if key is in dictionary
# =======================================================================================================================
def validate_param(key):
    if "!" in key[0]:
        test_key = key[1:]
    else:
        test_key = key
    return test_key in user_defined
# =======================================================================================================================
#  return a formatted string with keys and values
# =======================================================================================================================
def get_params_text():
    return utils_print.format_dictionary(user_defined, True, False, True, None)
# =======================================================================================================================
#  Return the dictionary as a formatted table
# =======================================================================================================================
def get_dict_as_table(substr):
    '''
    :param substr: If substr is not null, find substr in keys
    :return:    The key values pairs
    '''
    out_table = []
    for key, value in user_defined.items():
        if not substr or substr in key:
            out_table.append((key,value))
    out_table.sort()
    title = ["Key", "Value"]
    reply = utils_print.output_nested_lists(out_table, "", title, True)
    return reply
# =======================================================================================================================
#  Return the dictionary in JSON
# =======================================================================================================================
def get_dict_as_json(substr):
    '''
    :param substr: If substr is not null, find substr in keys
    :return:    The key values pairs
    '''

    if not substr:
        reply = utils_json.to_string(user_defined)
    else:
        sub_dictionary = {}     # only with keys satisfying the substring
        for key, value in user_defined.items():
            if substr in key:
                sub_dictionary[key] = value
        reply = utils_json.to_string(sub_dictionary)

    return reply


def print_params():
    utils_print.format_dictionary(user_defined, True, True, False, None)


# =======================================================================================================================
#  return an ip and port from the provided string
# =======================================================================================================================
def get_ip_port(key_str):
    '''
    Key_str can be in the format ip:port or !name:!port or !name:port or ip:!port
    '''

    if key_str[0] == ':' or key_str[-1] == ':':
        ip_port = ""
    else:

        index = key_str.find(":")

        if index > 1:
            ip_str = key_str[:index]
            port_str = key_str[index+1:]
            ip_port = get_value_if_available(ip_str) + ':' + get_value_if_available(port_str)
        else:
            ip_port = get_value_if_available(key_str)

    return ip_port
# =======================================================================================================================
#  return a directory name (assigned to the key) with a separator at the end
# =======================================================================================================================
def get_directory(key_type:str):

    directory = get_value_if_available(key_type)
    if not directory:
        directory = key_type

    if directory:
        if directory[-1] != path_separator:
            directory += path_separator
    return directory

# =======================================================================================================================
#  Retrieve the value by the provided key
# =======================================================================================================================
def get_value_if_available(key_type: str):
    global os_wind
    # replace word if available in dictionary

    data_type = None
    output_flag = 0

    str_len = len(key_type)
    if str_len > 4 and key_type[-4:] in al_func_ :     # test for int or float and not !! assignment (to be set on a different node)
        func_info = al_func_[key_type[-4:]]
        min_word_size = func_info[0]
        if len(key_type) > min_word_size and func_info[1] == key_type[-min_word_size:]:
            key = key_type[:str_len - min_word_size]
            data_type = func_info[2]
            output_flag = func_info[3]
            # Output Flag:
            # 1 - change to lower and underscore,
            # 2 - Get the length of the object
            # 3 - Get the number of nodes replied
            # 4 - Get the number of nodes missing
        else:
            key = key_type

    else:
        key = key_type

    if len(key):
        if key[0] == '!':
            value = get_param(key[1:])
            if not value:
                if len(key) > 2 and key[:2] == "!!":
                    # Keep one exclamation point to the destination node (value to be translated in destination)
                    value = key[1:]
                else:
                    value = get_path(key)
        elif key[0] == '$':
            # enviornment variable
            value = get_env_var(key)
            if not value:
                value = get_path(key)
            #if not value:
                # Removed because "if $abc" returns True whereas $abc is not set
                #value = key  # needs to be kept as source as $X may be processed by the OS even without system param defined
        elif key[0] == '\\' and key[1] == '"':
            # translate including quotations
            if len(key) > 5 and key[2] == '!' and key[-2:] == '\\"':
                # translate word and keep quotation
                value = '"' + get_param(key[3:-2]) + '"'
            else:
                value = key
        else:
            value = key  # keep text


        if data_type:
            if output_flag == 1:
                # str + replace space with underscore + lower case
                type_value = value.replace(' ', '_').lower()
            elif output_flag == 2 or output_flag == 3 or output_flag == 4:
                # 2 - get str length or object length
                # 3 - get nodes replied
                type_value = -1
                if len(value) > 2:
                    if value[0] == '[':
                        list_obj = utils_json.str_to_list(value)
                        if list_obj:
                            if output_flag == 2:
                                type_value = len(list_obj)      # Entries in object
                            else:
                                # count nodes replied
                                type_value = 0
                                for entry in list_obj:
                                    if entry[1]:
                                        type_value += 1  # A reply is found
                                if output_flag == 4:
                                    # Calculate no. of nodes that did not not reply
                                    type_value = len(list_obj) - type_value
                    elif value[0] == '{':
                        json_obj = utils_json.str_to_json(value)
                        if json_obj:
                            if output_flag == 2:
                                type_value = len(json_obj)      # Entries in object
                            else:
                                # dictionary
                                type_value = 0
                                for entry in json_obj.values():
                                    if entry:
                                        type_value += 1  # A reply is found
                                if output_flag == 4:
                                    # Calculate no. of nodes that did not not reply
                                    type_value = len(json_obj) - type_value
                if type_value == -1:
                    # not an object
                    if output_flag == 2:
                        # length of string
                        type_value = len(value)
                    else:
                        type_value = 0    # No nodes replied

            # Convert to a different data type
            elif data_type == bool:
                if value == "false" or not value:
                    type_value = False
                else:
                    type_value = True
            else:
                # int / str / float
                try:
                    type_value = data_type(value)
                except:
                    type_value = ""   # Return Null string
        else:
            type_value = value
    else:
        type_value = ""

    return type_value

# =======================================================================================================================
# Set enviornment variable
# =======================================================================================================================
def set_env_var(key, value):

    try:
        os.environ[key[1:]] = value
    except:
        pass

# =======================================================================================================================
# Get environment variable value
# Key[0] has the $ sign
# =======================================================================================================================
def get_env_var(key):
    global os_wind
    if os_wind:
        value = os.getenv(key[1:])
    else:
        value = os.environ.get(key[1:])
    return value

# =======================================================================================================================
# Return complete path by replacing the encoded prefix by a value from the dictionary or a system variable.
# For example: !data_dir/abc ...
# For example: $HOME/abc ...
# =======================================================================================================================
def get_path(path_encoded):

    local_path = path_encoded.replace(path_niu, path_separator)  # Organize the path to the local OS

    offsets = [0]
    for m in re.finditer("[!$./\\\\]", local_path): # note [] is a set
        offset_point = m.start()
        if offset_point:
            offsets.append(offset_point)  # Start or end point
            offsets.append(offset_point)       # End or start point

    offsets.append(len(local_path))         # Final point

    # Pull the substr and apply dictionary or sys variables
    start_substr = -1
    path = ""
    for offset in offsets:
        if start_substr == -1:
            start_substr = offset
        else:
            substr = local_path[start_substr: offset]

            if substr[0] == '!':
                value = get_param(substr[1:])
                if not value:
                    path = ""
                    break       # Not translated return error string
            elif substr[0] == '$':
                value = get_env_var(substr)
                if not value:
                    path = ""
                    break       # Not translated return error string
            else:
                value = substr

            path += value
            start_substr = -1

    return path



# =======================================================================================================================
# break conditions with multiple "and" statements
# The process updates the conditions_list with if conditions and returns:
# a) ret_val
# b) offset then
# c) True if the if condition had parenthesis
# =======================================================================================================================
def analyze_if(status, cmd_words, offset_start, conditions_list):
    '''
    conditions_list - empty list
    '''

    try:
        index = cmd_words[offset_start + 1:].index("then")
    except:
        # Not then - result is set to stdout or a variable
        offset_then = len(cmd_words)
    else:
        offset_then = offset_start + 1 + index

    # organizing a list - each entry is a list that is tested independently
    cond_x = 0
    conditions_list.append( ["and"] )
    with_paren = False
    need_end_or = False
    ret_val = process_status.SUCCESS

    offset_cond = 0         # and/or if a > b  --> 0 is on and/or, 1 is on a, 2 is on b, 3 is on c
    for offset in range(offset_start + 1, offset_then):
        word = cmd_words[offset]
        offset_cond += 1
        if word == 'and' or word == 'or':
            and_or_word = True
            need_end_or = False
            offset_cond = 0
        elif word == "not":
            offset_cond = 2
            and_or_word = False
        else:
            and_or_word = False

        if offset_cond == 2:
            if not word in cond_signs:
                status.add_error("Failed to parse in \"if\" stmt, missing comparison operator: %s" % ' '.join(cmd_words[offset_start:]))
                ret_val = process_status.Failed_to_parse_if
                break

        if need_end_or and not and_or_word:
            status.add_error(f"Failed to parse in \"if\" stmt, missing end or else: {' '.join(cmd_words[offset_start:])}")
            ret_val = process_status.Failed_to_parse_if
            break

        if and_or_word:
            cond_x += 1
            conditions_list.append([])
        elif word and word[0] == '(':
            with_paren = True

        if word and word[-1] == ')':
            need_end_or = True      # next word (after parenthesis) needs to be ANd or OR

        conditions_list[cond_x].append(word)

    reply = [ret_val, offset_then, with_paren]
    return reply

# =======================================================================================================================
# Process the if condition after it was analyzed in - analyze_if()
# Returns a list with 2 values:
# a) Next word,
# b) compare result -1 (error) or 0 (False) or 1 (True)
# =======================================================================================================================
def process_analyzed_if(status, cmd_words, offset_start, offset_then, with_paren, conditions_list, json_struct):
    '''
    cmd_words - the if stmt
    offset_start - the "if" offset
    offset_then - the "then" offset
    with_paren - if parenthesis are used
    conditions_list - the if conditions broken to inner parenthesis
    json_struct - a policy to use
    '''

    if with_paren:

        compare_result = -1

        for counter, conditions in enumerate(conditions_list):
            # Go over each condition in parenthesis
            cond_counter = len(conditions)
            if cond_counter < 2:
                # Error,
                status.add_error("Failed to parse in \"if\" stmt: %s" % ' '.join(cmd_words[offset_start:]))
                return [0, -1]

            if compare_result == 1:
                if conditions[0] == "or":
                    break  # Previous cond is satisfied  - no need to test this or condition
            else:
                if counter and conditions[0] == "and":
                    break  # previous conditions not satisfied but required

            if len(conditions) == 2:
                cond_str = conditions[1]
                if cond_str[0] == '(':
                    # remove parenthesis
                    cond_words, left_brackets, right_brakets = utils_data.cmd_line_to_list_with_json(status, cond_str[1:-1], 0, 0)
                    if left_brackets != right_brakets:
                        status.add_error("Failed to parse in \"if\" stmt, missing parenthesis: %s" % ' '.join(cmd_words[offset_start:]))
                        return [0, -1]
                else:
                    cond_words, left_brackets, right_brakets = utils_data.cmd_line_to_list_with_json(status, cond_str, 0, 0)
            else:
                cond_words = conditions[1:]

            # Adding if + then --> Make consistent with test_conditions_sequentially()
            if not isinstance(cond_words, list):
                status.add_error("Failed to parse in \"if\" stmt, missing parenthesis: %s" % ' '.join(cmd_words[offset_start:]))
                return [0, -1]
            compare_result = test_conditions_sequentially(status, ["if"] + cond_words + ["then"], 0, len(cond_words), json_struct)

            if compare_result == 1:
                if conditions[0] == "or":
                    break       # Or condition satisfied
            else:
                if compare_result == -1:
                    break  # Error
                if counter and conditions[0] == "and":
                    break   #  not satisfied but required

    else:
        compare_result = test_conditions_sequentially(status, cmd_words, offset_start, offset_then,json_struct)

    reply = [offset_start + offset_then, compare_result]

    return reply # Return next word + compare result -1 (error) or 0 (False) or 1 (True)
# =======================================================================================================================
# break conditions with multiple "and" statements
# =======================================================================================================================
def test_conditions_sequentially(status, cmd_words, offset_start, offset_then, json_struct):

    words_count = len(cmd_words)
    from_word = offset_start + 1
    counter_words = 0
    compare_result = -1
    for offset in range (from_word, words_count):

        word = cmd_words[offset]

        if word == 'or':
            compare_result = test_condition(status, cmd_words, from_word, counter_words, json_struct)
            if compare_result == 1:
                # Up to 'or' was success
                counter_words = offset_then - counter_words
                break;
            # Go to condition after 'or' keyword
            from_word += (counter_words + 1)  # Skip "or""
            counter_words = 0

        elif word == "then" or word == "and" or (offset + 1) == words_count:
            if (offset + 1) == words_count and word != "then":
                counter_words += 1      # include last word

            compare_result = test_condition(status, cmd_words, from_word, counter_words, json_struct)
            if compare_result <= 0 or word == "then": # failed (0) or error (1):
                break   # Comparison failed
            # New comparison
            from_word += (counter_words + 1)    # Skip "and""
            counter_words = 0
        else:
            counter_words += 1      # words to consider in the condition

    return compare_result

# =======================================================================================================================
# Return ret_val, the data type and value. For example !a.int returns the dictionary value for !a and "int"
# =======================================================================================================================
def get_value_type(status, word, json_struct):
    ret_val = process_status.SUCCESS
    value  = get_value_if_available(word)
    if isinstance(value, str):
        data_type = "str"
    elif isinstance(value, bool):
        data_type = "bool"              # Need to be tested before int because bool is a subclass of int
    elif isinstance(value, int):
        data_type = "int"
    elif isinstance(value, float):
        data_type = "float"

    else:
        data_type = "unknown"
        ret_val = process_status.Unrecognized_data_type

    if not ret_val and data_type == "str" and len(value) and value[0] == '[' and json_struct:
        is_key, key, is_list, next_key = utils_json.make_pull_keys(value)
        if is_key:
            ret_val, new_word = utils_json.get_object_data(status, json_struct, key, False, "", False, None, False, "", False, 1)
        else:
            new_word = value
    else:
        new_word = value

    return [ret_val, data_type, new_word]
# =======================================================================================================================
# Get the length of the entry
# =======================================================================================================================
def get_entry_length( entry ):

    value = 0
    dict_value = get_value_if_available(entry)
    len_value = len(dict_value)
    if len_value:
        if dict_value[0] == '{':
            # count elements in the dictionary
            structure = utils_json.str_to_json(dict_value)
        elif dict_value[0] == '[':
            # count elements in the list
            structure = utils_json.str_to_list(dict_value)
        else:
            # Get string size
            structure = dict_value

        if structure:
            value = len(structure)

    return value

# =======================================================================================================================
# Test condition on an array (up to the end_word):
# If A then
# If Not A Then
# If A > B Then  -->    > or < or >= or <= or == or !=
# returns words considered AND -1 - error, 0 - False, 1 - True
# =======================================================================================================================
def test_condition(status, cmd_words, offset_start, words_count, json_struct):

    # json_struct is a json policy

    offset = offset_start

    if words_count == 1:
        # this is the case of If A then
        ret_val, data_type, value = get_value_type(status, cmd_words[offset], json_struct)
        if ret_val:
            return ret_val
        is_value = test_value(data_type, value)
        return is_value

    if words_count == 2:
        # The case of "NOT X"
        if cmd_words[offset] != "not":
            err_msg = "Comparison of values failed: Missing 'not' with stmt: '%s'" % (' '.join(cmd_words))
            status.add_error(err_msg)
            return -1

        ret_val, data_type, value = get_value_type(status, cmd_words[offset+1], json_struct)
        if ret_val:
            return ret_val
        is_value = test_value(data_type, value)  # returns 1 for numeric which is not 0 or non null string
        if is_value == 1:
            is_value = 0
        elif is_value == 0:
            is_value = 1
        return is_value

    if words_count == 3:
        # a > b or a < b
        opr = cmd_words[offset + 1]
        if opr != 'not' and opr in cond_signs:
            word1 = cmd_words[offset]
            word2 = cmd_words[offset + 2]
        else:
            err_msg = "Comparison of values failed: Comparison operator '%s' is not recognized: '%s'" % (opr, ' '.join(cmd_words[1:-1]))
            status.add_error(err_msg)
            return -1
    elif words_count == 4:
        # a == b, a !=b, a >= b or a <= b
        opr = cmd_words[offset + 1] + cmd_words[offset + 2]
        if opr == "==":
            opr = "="  # the comparison id
        word1 = cmd_words[offset]
        word2 = cmd_words[offset + 3]
    else:
        err_msg = "Comparison of values failed with stmt: '%s'" % ' '.join(cmd_words)

        status.add_error(err_msg)
        return -1


    ret_val, data_type1, value1 = get_value_type(status, word1, json_struct)
    if ret_val:
        return ret_val
    ret_val, data_type2, value2 = get_value_type(status, word2, json_struct)
    if ret_val:
        return ret_val

    if data_type1 == data_type2:
        data_type = data_type1
    elif data_type1 == "float" or data_type2 == "float":
        data_type = "float"
    elif data_type1 == "int" or data_type2 == "int":
        data_type = "int"
    elif data_type1 == "bool":
        if isinstance(data_type2, str):
            if value2 == "true":
                value2 = True
            elif value2 == "false":
                value2 = False
            else:
                return -1        # both needs to be the same type
        else:
            return -1  # both needs to be the same type
        data_type = "bool"
    elif data_type2 == "bool":
        if isinstance(data_type1, str):
            if value1 == "true":
                value1 = True
            elif value1 == "false":
                value1 = False
            else:
                return -1        # both needs to be the same type
        else:
            return -1  # both needs to be the same type
        data_type = "bool"
    else:
        data_type = "str"  # the default


    if data_type == "int":
        try:
            number1 = int(value1)
        except:
            status.add_error("Value provided '%s' is not numeric" % word1)
            return -1
        try:
            number2 = int(value2)
        except:
            status.add_error("Value provided '%s' is not numeric" % word2)
            return -1

        ret_val = utils_columns.comarison[opr](number1, number2)

    elif data_type == "bool":
        try:
            bool1 = bool(value1)
        except:
            status.add_error("Value provided '%s' is not a bool" % word1)
            return -1
        try:
            bool2 = bool(value2)
        except:
            status.add_error("Value provided '%s' is not a bool" % word2)
            return -1

        ret_val = utils_columns.comarison[opr](bool1, bool2)

    elif data_type == "float":
        try:
            number1 = float(value1)
        except:
            status.add_error("Value provided '%s' is not a float" % word1)
            return -1
        try:
            number2 = float(value2)
        except:
            status.add_error("Value provided '%s' is not a float" % word2)
            return -1

        ret_val = utils_columns.comarison[opr](number1, number2)

    else:
        if len(value1) > 2 and value1[0] =='\'' and value1[-1] == '\'':
            # This is because utils_data.cmd_line_to_list_with_json() will keep the single quotation on multiple words
            value1 = value1[1:-1]
        if len(value2) > 2 and value2[0] =='\'' and value2[-1] == '\'':
            # This is because utils_data.cmd_line_to_list_with_json() will keep the single quotation on multiple words
            value2 = value2[1:-1]
        try:
            ret_val = utils_columns.comarison[opr](value1.lower(), value2.lower())
        except:
            err_msg = "Comparison of values failed with stmt: '%s'" % ' '.join(cmd_words)
            status.add_error(err_msg)
            return -1

    if ret_val:
        compare_result = 1
    else:
        compare_result = 0

    return compare_result

# =======================================================================================================================
# In the case of int - return 1 if value is not 0, 0 if the value is 0, and -1 if the number is not numeric
# In the case of a string - return 1 if not null and 0 if null
# =======================================================================================================================
def test_value(data_type, value):

    if isinstance(value,str):
        ret_val = 1 if value != "" else 0
    elif isinstance(value,bool):
        ret_val = int(value)
    elif isinstance(value, int):
        ret_val = 1 if value != 0 else 0
    elif isinstance(value, float):
        ret_val = 1 if value != 0 else 0
    else:
        ret_val = -1

    return ret_val

# =======================================================================================================================
# Compare 2 params
# =======================================================================================================================
def compare(is_int, word1, word2, opr):
    value1 = get_value_if_available(word1)
    value2 = get_value_if_available(word2)

    if is_int:
        try:
            number1 = int(value1)
        except:
            return False
        try:
            number2 = int(value2)
        except:
            return False

        ret_val = utils_columns.comarison[opr](number1, number2)
    else:
        ret_val = utils_columns.comarison[opr](value1.lower(), value2.lower())

    return ret_val


# =======================================================================================================================
# Compare 2 params
# =======================================================================================================================
def isEqual(word1, word2):
    value1 = get_value_if_available(word1)
    value2 = get_value_if_available(word2)
    return value1.lower() == value2.lower()


def print_param(key: str):
    value = get_value_if_available(key)
    if (value == ""):
        utils_print.output("No value fo key " + key, True)
    else:
        utils_print.output(value, True)


# =======================================================================================================================
# Concatenate words to a string such that it can be excuted using eval- no space between words
# =======================================================================================================================
def make_eval_command(my_array, from_entry, to_enty=0):
    if to_enty:
        array_len = to_enty
    else:
        array_len = len(my_array)
    if array_len <= from_entry:
        return ""
    new_string = ""

    is_string_text = True

    for x in range(from_entry, array_len):

        if my_array[x] == '+':
            new_substring = "+"
            is_string_text = True  # next is string value
        else:
            new_substring, is_string_text = replace_by_dictionary(my_array[x],
                                                                  is_string_text)  # search for "!" and replace with dictionary

        new_string += new_substring

    return new_string

# =======================================================================================================================
# Replace keys with values from dictionary
# =======================================================================================================================
def make_python_command(command_string: str):
    new_string = ""

    index = 0
    command_length = len(command_string)
    non_dictionary_length = 0
    while index < command_length:
        char = command_string[index]
        if char == '!':
            if non_dictionary_length:
                new_string += command_string[index - non_dictionary_length: index]  # copy the python part
            length = utils_data.get_length_for_dictionary(command_string, index)
            new_word = get_value_if_available(command_string[index:index + length])
            new_word = new_word.replace('\n', ' ')  # New line makes eval return an error - EOL while scanning string literal
            new_word = new_word.replace('\r',' ')  # New line makes eval return an error - EOL while scanning string literal
            if len(new_word) and \
                    ((new_word[0] == '"' and new_word[-1] == '"') or
                     (new_word[0] == "'" and new_word[-1] == "'")):
                new_string += new_word
            else:
                new_string += "'" + new_word + "'"
            index += length
            non_dictionary_length = 0
        else:
            non_dictionary_length += 1
            index += 1

    if non_dictionary_length:
        new_string += command_string[index - non_dictionary_length: index]  # copy the python part

    return new_string


# =======================================================================================================================
# Return a string - replace text after exclamation point with the dictionary value
# =======================================================================================================================
def replace_by_dictionary(string: str, is_string_text: bool):
    prefix_considered = 0  # number of bytes considered
    new_string = ""

    while 1:

        if is_string_text:
            new_string += '\''

        index = string[prefix_considered:].find("!")
        if index == -1:
            break
        if index:
            new_string += string[
                          prefix_considered:prefix_considered + index]  # add string portion up to the exclamation point
            prefix_considered += index

        length = utils_data.get_length_for_dictionary(string, index)
        prefix_considered += length
        new_string += get_value_if_available(string[index:index + length])

        if is_string_text:
            new_string += '\''

        is_string_text = not is_string_text

    if prefix_considered < len(string):
        new_string += string[prefix_considered:]  # add the rest that has no exclamation point
        if is_string_text:
            new_string += '\''
        is_string_text = not is_string_text

    reply_list = [new_string, is_string_text]
    return reply_list


# =======================================================================================================================
# Return a string from an array of words, use the dictionary when key is in dictionary
# =======================================================================================================================
def get_translated_string(my_array, from_entry, to_enty, space_to_tab):
    """
    Build new string from the words array
    :param my_array:
    :param from_entry:
    :return:
    """
    if to_enty:
        array_len = to_enty
    else:
        array_len = len(my_array)
    if array_len <= from_entry:
        return ""
    new_string = ""

    for x in range(from_entry, array_len):
        if (x > from_entry):
            new_string += ' '

        word = my_array[x]
        if len(word) > 5 and word[0:3] == '\\"!' and word[-2:] == '\\"':
            # translate word and keep quotation
            translated_string = '"' + get_value_if_available(word[2:-2]) + '"'
        else:
            extracted_value = get_value_if_available(word)
            translated_string = extracted_value if isinstance(extracted_value, str) else str(extracted_value)

        if len(translated_string):
            if translated_string[0] == "{":
                # jason string
                translated_string = json_str_replace_key_with_value(translated_string)
            elif space_to_tab:
                # On a messsage - will be replaced back to space
                translated_string = translated_string.replace(' ', '\t')

            new_string += translated_string
        else:
            new_string = ""
            break           # Was not translated

    return new_string


# =======================================================================================================================
# Return a string from an array of words with + sign between substrings
# =======================================================================================================================
def get_added_string(my_array, from_entry, to_enty=0):
    """
    Build new string from the words array
    :param my_array:
    :param from_entry:
    :return:
    """
    if to_enty:
        array_len = to_enty
    else:
        array_len = len(my_array)
    if array_len <= from_entry:
        return ""
    new_string = ""

    need_plus = False

    for x in range(from_entry, array_len):

        sub_str = my_array[x]
        if need_plus:  # has to have the + sign
            if sub_str != '+':
                new_string = ""  # did not find the plus sign
                break

            need_plus = False
            continue

        if len(sub_str) > 1 and (sub_str[0] == '!' or sub_str[1] == '$'):
            new_string += str(get_value_if_available(sub_str))
        else:
            new_string += sub_str
        need_plus = True

    if need_plus == False:
        new_string = ""  # ended with a plus

    return new_string


# ======================================================================================================================
# Replace dictionary keys with values for a given JSON string
# note: \s means ignore space, \w means alphanumeric and underscore
# ======================================================================================================================
def json_str_replace_key_with_value(data_str: str):
    offsets = []
    start_quot = False
    for m in re.finditer(r'[!"]', data_str):        # Find exclamation point or quotation
        offset = m.end()
        char = data_str[offset-1]                   # ! or "
        if char == '"':
            start_quot = not start_quot
        elif start_quot:
            continue             # Ignore exclamation inside quote

        offsets.append(offset)

    start_quot = False                              # Determine if replacing a string inside quotations
    for x in reversed(offsets):
        if data_str[x-1] == '"':
            start_quot = not start_quot
            continue
        if x >= 2:
            previous_char = data_str[x - 2]  # the char before the exclamation point
            if previous_char == ' ' or previous_char == ':' or previous_char == '[' or previous_char == '{':
                word_length = get_dictionary_word_length(data_str, x)
                word = data_str[x - 1:x + word_length]
                new_word = get_value_if_available(word)
                if isinstance(new_word, str):
                    if start_quot:
                        # The new_word is added inside double quotation, replace double quotation with single quotation
                        str_word = new_word.replace('"', "'")
                    else:
                        str_word = utils_json.replace_non_supported_chars(new_word)
                    is_str = True
                else:
                    str_word = str(new_word)
                    is_str = False              # Int or float

                if str_word and word != str_word:
                    if (str_word[0] == '[' and str_word[-1] == ']') or (str_word[0] == '{' and str_word[-1] == '}') or not is_str or start_quot:
                        data_str = data_str[:x - 1] + str_word + data_str[x + word_length:]
                    else:
                        data_str = data_str[:x - 1] + '"' + str_word + '"' + data_str[x + word_length:]

    return data_str


# ======================================================================================================================
# Return the length of a word in a string
# ======================================================================================================================
def get_dictionary_word_length(data_str: str, offset: int):
    str_length = len(data_str)
    if offset >= str_length:
        return 0
    for x in range(offset, str_length):
        if not is_dictionary_char(data_str[x]):
            return x - offset
    return 0


# ======================================================================================================================
# Test char value if can be used in a word in the dictionary
# ======================================================================================================================
def is_dictionary_char(char):
    if char >= 'a' and char <= 'z':
        return True
    if char >= 'A' and char <= 'Z':
        return True
    if char >= '0' and char <= '9':
        return True
    if char == '_' or char == '.':
        return True

    return False


# ======================================================================================================================
# with a param_name and index like: value(index) - return the value and the index
# reply(3) returns: [reply,3]
# ======================================================================================================================
def get_key_and_index(word):
    offset = word.rfind('(')
    if offset > 0 and word[-1] == ')':
        param_name = word[:offset]
        index_word = word[offset + 1:-1]
        value = get_value_if_available(index_word)
        if value.isnumeric():
            index = int(value)
        else:
            param_name = ""
            index = 0
    else:
        param_name = ""
        index = 0

    reply_list = [param_name, index]
    return reply_list


# ======================================================================================================================
# Apply dictionary on substrings
# For example:  " if !value.upper().find('A') != -1" --> !value is replaced by dictionary
# stop_dict are chars to stop scan
# ======================================================================================================================
def apply_dictionary(source_str: str, start_offset: int):
    str_len = len(source_str)
    offset = start_offset
    updated_str = source_str

    while 1:
        offset = updated_str.find('!', offset)
        if offset == -1:
            break
        key_len = get_key_len(updated_str, offset, str_len)
        if key_len == 1:
            # For example: !=
            offset += 1
        else:
            value = get_value_if_available(updated_str[offset: offset + key_len])
            if value:
                updated_str = updated_str[:offset] + '\'' + value + '\'' + updated_str[offset + key_len:]
                offset += (len(value) + 2)
            else:
                offset += 1
    return updated_str


# ======================================================================================================================
# Calculate the length of a key that starts at a given offset
# ======================================================================================================================
def get_key_len(source_str, offset_start, offset_end):
    offset = offset_start
    while offset < offset_end:
        ch = source_str[offset]
        if ch in end_key_chars.keys():
            break
        offset += 1

    return offset - offset_start


# ======================================================================================================================
# Return all environment variables
# ======================================================================================================================
def environment_var(format_type):

    if format_type == "json":
        out_dict = {}
    else:
        out_list = []

    for k, v in sorted(os.environ.items()):

        if format_type == "json":
            out_dict[k] = v
        else:
            out_list.append((k,v))

    if format_type == "json":
        reply = utils_json.to_string(out_dict)
    else:
        title = ["Key", "Value"]
        reply = utils_print.output_nested_lists(out_list, "", title, True)

    return reply


# ----------------------------------------------------------------------
# Get value word from a command line - return a value object - like a list, dict, or string
# ----------------------------------------------------------------------
def word_to_value_object(source_word):

    value_object = None
    if isinstance(source_word, str):

        if len(source_word) > 1:

            value_word = get_value_if_available(source_word)  # Value from command line

            if value_word:
                value_object = value_word

            if isinstance(value_word, str) and len(value_word) > 1:     # get_value_if_available can return int or float

                ch = value_word[0]

                if ch == '\'':
                    # Remove single quotation
                    if ch == value_word[-1]:
                        value_object = value_word[1:-1]  # Remove quotation but ignore params.get_value
                elif ch == '{' and value_word[-1] == '}':
                    # Get a Dictionary
                    value_object = utils_json.str_to_json(value_word)
                elif ch == '[' and value_word[-1] == ']':
                    # Get a listynetynet
                    value_object = utils_json.str_to_list(value_word)
                    if value_object:
                        # Remove leading and trailing single quotations
                        for index, entry in enumerate(value_object):
                            if len(entry) > 2 and entry[0] == '\'' and entry[-1] == '\'':
                                value_object[index] = entry[1:-1]       # Remove single quotation
            else:
                value_object = value_word

    if value_object == None:
        value_object = source_word

    return value_object

# ----------------------------------------------------------------------
# Assign a value to a dictionary
# Example: set config_policy [config] = {}
# ----------------------------------------------------------------------
def set_dictionary(status, dict_key:str, key_value:list):
    '''
    Example: config_policy[config][ip] = !external_ip
    dict_key is config_policy
    dict_index is [config][ip]
    new_value is {}
    '''

    ret_val = process_status.SUCCESS


    policy_str = get_param(dict_key)      # Get the string value of the policy from the dictionary
    if not policy_str:
        # New policy
        policy = {}
    else:
        policy = utils_json.str_to_json(policy_str)
        if  policy == None:
            if status:
                status.add_error("Set Policy failed: The value assigned to '%s' is not in JSON structure" % dict_key)
            ret_val = process_status.ERR_wrong_json_structure

    if not ret_val:

        words_count = len(key_value)

        for index in range (0, words_count, 4 ):
            # Multiple assignments - this is in "set policy" command
            # Example: set policy config_policy [config][port] = 7848 and [config][restport] = 7849 and [config][broker_port] = 7850
            if index > 3:
                if key_value[index - 1] != "and":
                    if status:
                        status.add_error("Error in 'set policy' command - miissing 'and' keyword between assignments")
                    ret_val = process_status.ERR_command_struct
                    break

            if (index + 3) > words_count:
                if status:
                    status.add_error("Error in 'set policy' command - assignment info is not complete")
                ret_val = process_status.ERR_command_struct
                break

            key_string = key_value[index]
            operation = "assign"
            if key_value[index + 1] != '=':
                if key_value[index + 1] == '+':
                    # Append to a list
                    operation = "append"
                else:
                    if status:
                        status.add_error("Set Policy failed: Missing equal sign (=) in JSON assignment")
                    ret_val = process_status.ERR_command_struct
                    break

            # SET the value assigned to the policy
            value = word_to_value_object(key_value[index + 2])

            key_list = key_string.split(']')
            existing_keys = len(key_list) - 2       # The number of keys to the location of the update

            # Get the inner json and assign the value
            inner_json = policy
            for depth, key_section in enumerate(key_list):

                try:
                    key_str = key_section[1:] if key_section[1] != "'" else key_section[2:-1] # Remove parenthesis if needed
                except:
                    if status:
                        status.add_error("Command 'set policy' failed: Wrong key structure in assignment")
                    ret_val = process_status.ERR_command_struct
                    break

                key = get_value_if_available(key_str)
                if key in inner_json and depth < existing_keys:
                    inner_json = inner_json[key]
                else:
                    break

            if ret_val:
                break

            if depth != existing_keys:
                if status:
                    status.add_error("Command 'set policy' failed: Error in depth of assignment key")
                ret_val = process_status.ERR_command_struct
                break

            if operation == "assign":
                if isinstance(inner_json, dict):
                    inner_json[key] = value
                else:
                    if status:
                        status.add_error(f"Command 'set policy' failed: The key: '{key}' does not lead to a dictionary")
                    ret_val = process_status.ERR_command_struct
                    break
            elif operation == "append":
                # append to a list
                if isinstance(inner_json[key], list):
                    inner_json[key].append(value)
                else:
                    if status:
                        status.add_error(f"Command 'set policy' failed: The key: '{key}' does not lead to a list")
                    ret_val = process_status.ERR_command_struct
                    break

    if not ret_val:
        updated_policy = utils_json.to_string(policy)
        add_param(dict_key, updated_policy)

    return ret_val

# ----------------------------------------------------------------------
# Add a result set to a dictionary or a list

# Example result set: [From Node 10.0.0.78:7848] AnyLog@73.202.142.172:7848 running
# The result set is broken to: [10.0.0.78:7848] and [AnyLog@73.202.142.172:7848 running]
# ----------------------------------------------------------------------
def create_result_struct(assignment, out_obj):
    '''
    status - thread status object
    assignment - a key that ends with [] representing a list or {} representing a dictionary
    out_obj - the result set to save in a dictionary or a table
    is_dict - the struct type
    '''

    key = assignment[:-2]       # The name of the key

    value = utils_json.to_string(out_obj)

    add_param(key, value)

