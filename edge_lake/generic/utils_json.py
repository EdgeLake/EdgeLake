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
import json
import re
import sys

import anylog_node.generic.process_log as process_log
import anylog_node.generic.process_status as process_status
import anylog_node.generic.utils_python as utils_python
import anylog_node.generic.utils_data as utils_data
import anylog_node.cmd.member_cmd as member_cmd
# =======================================================================================================================
# Chars not supported in JSON string
# =======================================================================================================================
replaced_chars = {
    '"': '|',
    '\'': '|',
    '\n': ' ',
    '\t': ' ',
}
control_chars = {
    '\n': True,
    '\t': True,
    '\r': True,
}
map_control_chars = {
    '\\n': '\n',
    '\\r': '\r',
    '\\t': '\t',
    '\'\\n\'' : '\n',
    '\'\\r\'':  '\r',
    '\'\\t\'': '\t',
}
bring_types_ = {
    "bring":    1,
    "unique":   2,
    "recent":   4,
    "last":     4,      # same as recent
    "first":    8,
    "json":     16,
    "count" :   32,
    "table" :   64,
    "null"  :   128,
    "sort"  :   256,
    "ip_port"  :   512,
    "min"   :   1024,
    "max"   :   2048,
}

pattern_bring = re.compile('[ []{}:"]')  # Find any of these occurrences - to void the bring

# =======================================================================================================================
# Validate Policy Structure
# =======================================================================================================================
def is_policy(json_str):

    policy = str_to_json(json_str)
    if not policy:
        ret_val = False
    elif len(policy) != 1:
        ret_val = False     # Only one key at the root is allowed
    else:
        ret_val = True
    return ret_val

# ==================================================================
# Given a list of entries, test that the entries are JSON
# ==================================================================
def test_json_list(status, list_data):
    lines = len(list_data)
    for line in range(lines):
        data = list_data[line]
        if line == (lines - 1) and data == "":
            break  # ignore last empty line
        error_msg = test_json_instance(data)  # convert data to dict
        if error_msg:  # check if failed
            if not isinstance(data, str):
                data = ""
            status.add_keep_error(
                "Wrong JSON structure at line number %u with error:'%s' over data: %s" % (line + 1, error_msg, data))
            return False
    return True


# =======================================================================================================================
# Validate JSON Format. Return error message
# =======================================================================================================================
def test_json_instance(data):
    error_msg = ""
    try:
        json_object = json.loads(data, strict=False)
    except ValueError as error:
        error_msg = str(error)
    return error_msg


# =======================================================================================================================
# Map a JSON object to a string.
# =======================================================================================================================
def to_string(json_obj):
    try:
        json_str = json.dumps(json_obj)
    except TypeError:
        errno, value = sys.exc_info()[:2]
        return ""

    return json_str

# =======================================================================================================================
# Map a JSON object to a string - formatted.
# =======================================================================================================================
def to_formatted_string(json_obj, intent_bytes):
    try:
        json_str = json.dumps(json_obj, indent=intent_bytes)
    except TypeError:
        return ""

    return json_str

# =======================================================================================================================
# String to list
# =======================================================================================================================
def str_to_list(data: str):

    try:
        list_obj = list(eval(data))
    except:
        list_obj = None
    return list_obj

# =======================================================================================================================
# Validate JSON Format. Return True if a vakid JSON format
# JSON requires double quotes for its strings -  there are no single quotes in a JSON string
# =======================================================================================================================
def str_to_json(data: str):
    fix_json = False
    try:
        json_object = json.loads(data, strict=False)
    except ValueError as error:
        if not data:
            err_msg = "Empty data string provided as JSON data"
            process_log.add("Error", err_msg)  # Log Error
            json_object = None
        else:
            err_msg = "JSON value error (line and column are of the JSON struct): " + str(error)
            fix_json = True
            json_object = None
    except TypeError as error:
        err_msg = "JSON type error (line and column are of the JSON struct): " + str(error)
        process_log.add("Error", err_msg)  # Log Error
        json_object = None
    except:
        fix_json = False
        err_msg = "Error", "Failed to map string to JSON"
        process_log.add("Error", err_msg)  # Log Error
        json_object = None

    if fix_json:
        modified = change_json_data(data)
        modified = utils_data.replace_string_chars(True, modified, None)
        try:
            json_object = json.loads(modified, strict=False)
        except ValueError as error:
            process_log.add("Error", err_msg)  # Log Error
            err_msg = utils_data.get_str_to_json_error(err_msg, data)
            if err_msg:
                process_log.add("Error", err_msg)
            json_object = None
        except:
            process_log.add("Error", err_msg)  # Log Error
            err_msg = utils_data.get_str_to_json_error(err_msg, data)
            if err_msg:
                process_log.add("Error", err_msg)
            json_object = None

    return json_object


# =======================================================================================================================
# Try to fix the JSON data:
# 1) Connect strings on multiple lines - parenthesis without commas are connected
# 2) Replace "\\" with "."
# 3 Replace single quotes with double quotes
# =======================================================================================================================
def change_json_data(data_str):

    data = data_str
    ch = data[0]
    if ch == '\'' or ch == '"' or ch == '`':
        # remove quotation over the string
        if len(data) > 1 and data[-1] == ch:
            data = data_str[1:-1]   # remove the quotations

    paren_status = 0
    modified = ""
    copied_offset = 0
    for offset, char in enumerate(data):

        if char == '\\':
            modified += (data[copied_offset:offset] + ".")
            copied_offset = offset + 1  # next copy will start after the slash
            continue

        if not paren_status and char == "'":
            # replace single quote with double quotes
            modified += (data[copied_offset:offset] + "\"")
            copied_offset = offset + 1  # next copy will start after the slash
            continue

        if char == '"':
            paren_status += 1
            if paren_status == 1:
                continue
            if paren_status == 2:
                # closing parenthesis - copy up to parenthesis as rest may be in next line
                modified += data[copied_offset:offset]  # copy without parenthesis
                copied_offset = offset  # copied_offset on the closing parenthesis
                continue
            if paren_status == 3:
                # the rest with new parenthesis
                copied_offset = offset + 1
                paren_status = 1  # treat like start parenthesis
                continue

        if paren_status >= 2:
            if char == ' ' or char in control_chars.keys():
                copied_offset = offset + 1  # ignore the chars
            else:
                modified += '"'  # close the parenthesis
                copied_offset = offset
                paren_status = 0  # restart

    if len(data) > copied_offset:
        modified += data[copied_offset:]

    return modified


# =======================================================================================================================
# Validate JSON Format. Return True if a vakid JSON format
# =======================================================================================================================
def validate(json_data):
    try:
        dict_object = json.dumps(json_data)
    except ValueError as error:
        return False
    except:
        return False
    return True


# =======================================================================================================================
# Set attribute values - replace dictionary items with their values
# =======================================================================================================================
def set_attribute_names(user_params, json_obj: dict):
    if isinstance(json_obj, dict):
        for x in json_obj:
            attribute_value = json_obj[x]
            if isinstance(attribute_value, dict):
                set_attribute_names(user_params, attribute_value)
            if attribute_value[0] == '!':  # replace from dictionary
                json_obj[x] = user_params.get_value_if_available(attribute_value)


# =======================================================================================================================
# Convert str to timestamp if valid
# =======================================================================================================================
def set_timestamp_value(json_value: str):
    try:
        re.search('[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])T([0-1][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])',
                  json_value).group()
    except Exception:
        return json_value
    else:
        return json_value.replace("T", " ").split(".")[0].replace("Z", "")


# ======================================================================================================================
# Replace characters that are not supported in JSON string
# ======================================================================================================================
def replace_non_supported_chars(source_str):

    source = source_str
    if len(source_str) > 2:
        # remove quotations
        ch = source_str[0]
        if ch == '\'' or ch == '`':
            if source_str[-1] == ch:
                source = source_str[1:-1]

    length = len(source)
    dest = ""
    copied = 0
    for x in range(length):
        if source[x] in replaced_chars.keys():
            dest += source[copied:x]
            dest += replaced_chars[source[x]]
            copied = x + 1
    if dest != "":
        # some chars were replaced
        if copied < length:
            dest += source[copied:]
    else:
        dest = source
    return dest


# =======================================================================================================================
# Get the info to bring: bring.unique, bring,recent, bring.json
# Bring unique - one occurrence of the key + value
# Bring recent - the last occurrence of the value
# Make a number representing the output options
# =======================================================================================================================
def get_bring_type(status, bring_word):
    global bring_types_

    sort_fields = None
    bring_type = 0
    bring_list = bring_word.split('.')
    for entry in bring_list:
        if entry in bring_types_:
            bring_type |= bring_types_[entry]
        elif entry[:5] == "sort(" and entry[-1] == ')':
            # sort by fields (first field is 0)
            sort_fields = entry[4:]
            bring_type |= bring_types_["sort"]
        else:
            bring_type = -1
            status.add_error("Unrecognized 'bring' directive: '%s'" % bring_word)
            break  # Non recognized type

    if not bring_type:
        status.add_error("Missing 'bring' directive information")
        bring_type = -1  # return error

    if is_bring("table", bring_type):
        # Always add the JSON bit if the table bit is set - because data is pulled in JSON and converted to table
        bring_type |= bring_types_["json"]
        bring_type |= bring_types_["null"]      # Add null keys to the json

    return [bring_type, sort_fields]


# =======================================================================================================================
# Test the Bring flags
# =======================================================================================================================
def is_bring(bring_name, bring_value):
    global bring_types_

    bring_bit = bring_types_[bring_name]
    return ((bring_value & bring_bit) > 0)


# ======================================================================================================================
# Pull data from the JSON object
# Pull_instruct is an array showing what to pull from the JSON and what info to add
# separator - between occurrences
#bring_types = {
#    "bring": 1,
#    "bring.unique": 2,
#    "bring.recent": 3,
#    "bring.first": 4,
#    "bring.json": 5,
#}
# ======================================================================================================================
def pull_info(status, json_data, pull_instruct, conditions, bring_type):
    ret_val = process_status.SUCCESS

    out_json = is_bring("json", bring_type)

    dynamic_string = ""
    if is_bring("count", bring_type):
        if not len(pull_instruct) and not conditions:
            # return count of entries
            count = len(json_data)
            if out_json:
                dynamic_string = "[{\"count\" : %u }]" % count
            else:
                dynamic_string = str(count)
            return [process_status.SUCCESS, dynamic_string]
        else:
            out_json = True # Must have JSON format to the count to work


    if not len(pull_instruct) and not conditions:
        # Only apply the bring
        if is_bring("recent", bring_type):
            dynamic_string = "[" + to_string(json_data[0]) + "]"
        elif is_bring("first", bring_type):
            dynamic_string = "[" + to_string(json_data[0]) + "]"
        elif is_bring("unique", bring_type) or is_bring("json", bring_type):
            dynamic_string = "["
            for counter, entry in enumerate(json_data):
                if counter:
                    dynamic_string += ','
                dynamic_string += to_string(entry)
            dynamic_string += "]"
        elif is_bring("ip_port", bring_type):       # Bring an IP and port string - considering the destination address to use
            dynamic_string = member_cmd.get_valid_ip_port(json_data)

        return [process_status.SUCCESS, dynamic_string]


    is_unique = is_bring("unique", bring_type)          # Bring unique value
    is_first = is_bring("first", bring_type)            # Bring first occurrence
    is_min = is_bring("min", bring_type)  # Bring first occurrence
    is_max = is_bring("max", bring_type)  # Bring first occurrence

    unique_struct = {}  # keeps entry to determine Unique occurrences
    python_cmds = None
    add_separator = False
    separator = ""
    if conditions:
        if not out_json:
            if "separator" in conditions.keys():
                add_separator = True  # Add separator with multiple results
                separator = conditions["separator"][0]
                if separator in map_control_chars.keys():
                    separator = map_control_chars[separator]
                elif len(separator) >= 3 and separator[0] == '"' and separator[-1] == '"':
                    separator = separator[1:-1]

        if "python" in conditions.keys():
            ret_val, python_cmds = utils_python.make_cmd_array(status, conditions["python"])
            if ret_val:
                return [ret_val, ""]

    counter_fields = len(pull_instruct)

    if out_json:
        # output a JSON collection - string based on a list with json struct instances
        dynamic_list = []
        dynamic_string = "["  # Output a string - build the string as needed
    else:
        dynamic_string = ""  # Output a string - build the string as needed

    for obj in json_data:  # Json_data is a list
        # Every obj is a new JSON instance
        new_json = {}
        new_string = ""

        for x in range(counter_fields):
            separator_event = False
            if not x and add_separator:  # before first field starting at the second instance
                if dynamic_string:  # not the first instance
                    separator_event = True

            word_str = pull_instruct[x]

            if word_str == "[]":
                # Pull the policy type
                is_key = True
                key = ""
                is_list = False     # Returned value is the policy type not a list
                next_key = ""
            else:
                is_key, key, is_list, next_key = make_pull_keys(word_str)  # validate the structure of the key used to pull from the JSON

            if is_key:
                ret_val, out_value = get_object_data(status, obj, key, is_list, next_key, out_json, new_json, separator_event, separator, False, bring_type)
                if ret_val:
                    break
                if not out_value:
                    continue        # No such data
                if not out_json and out_value:
                    if is_max or is_min:
                        if not dynamic_string or is_replace_result(dynamic_string, out_value, is_max):    # Replace by min or max
                            dynamic_string = str(out_value)
                        continue
                    else:
                        new_string += str(out_value)

            elif not out_json:
                if separator_event:
                    new_string += separator  # before first field starting at the second instance and not the first instance
                if len(word_str) >= 3 and word_str[0] == '"' and word_str[-1] == '"':
                    # remove quotations
                    new_string += word_str[1:-1]
                else:
                    if word_str in map_control_chars.keys():
                        word_str = map_control_chars[word_str]  # replace "\\n" with "\n"
                    new_string += word_str

        if not ret_val:
            if out_json:
                if len(new_json):
                    json_str = str(new_json)
                    if not is_unique or test_unique(json_str, False, None, None, unique_struct):
                        dynamic_list.append(json_str)
            elif new_string != "":
                if python_cmds:
                    # exec python command on each string retrieved
                    # Example: blockchain get table bring [table][create] separator = "\n" and python = { {data} { offset = !data.find('(') } { !data[int(!offset):] }}
                    ret_val, new_string = utils_python.execute_on_data(status, python_cmds, new_string)
                    if ret_val:
                        break

                if not is_unique or test_unique(new_string, add_separator, separator, dynamic_string, unique_struct):
                    # no need to test if unique
                    dynamic_string += new_string

        if is_first:
            break       # Only one occurrence

    if not ret_val:
        if is_bring("count", bring_type):
            if is_unique:
                count = len(unique_struct)
            else:
                count = len(dynamic_list)

            if is_bring("json", bring_type):
                dynamic_string = "[{\"count\" : %u }]" % count
            else:
                dynamic_string = str(count)

        elif out_json:
            for entry in dynamic_list:
                dynamic_string += entry + ','
            if dynamic_string == '[':
                # No data
                dynamic_string = "[]"
            else:
                dynamic_string = dynamic_string[:-1] + ']'  # remove last comma and close brackets

    return ret_val, dynamic_string

# ======================================================================================================================
# Replace the returned result to set it to the min or max value
# Compare the existing value to a new value
# ======================================================================================================================
def is_replace_result(existing_value, new_value, is_max):

    try:
        existing = float(existing_value)
        new = float(new_value)
    except:
        # Compare as strings
        existing = existing_value
        new = new_value

    if is_max:
        ret_val =  True if new > existing else False
    else:
        # User asked for min
        ret_val = True if new < existing else False

    return ret_val

# ======================================================================================================================
# Get data from a JSON object, given an object and a key
# ======================================================================================================================
def get_object_data(status, obj, key, is_list, next_key, out_json, new_json, separator_event, separator, add_quotation, bring_type):

    ret_val = process_status.SUCCESS
    new_string = ""

    if is_bring("null", bring_type):
        add_null_val = True     # Add null values to the JSON
    else:
        add_null_val = False
    if key[:5] == "[\"*\"]":
        if len(key) == 5:
            # Only get the policy type
            key = ""
        else:
            # Replace * with policy type
            policy_type = get_policy_type(obj)
            if policy_type:
                key = "[\"%s\"]" % policy_type + key[5:]

    try:
        if key == "":
            # if bring key was [] --> get the policy type
            out_value = next(iter(obj))
        else:
            # Pull from policy
            out_value = eval("obj" + key)
    except:
        # No value for the searched attribute name
        if out_json and add_null_val:
            # Add null values to the JSON struct
            json_key = get_key_string(key)
            new_json[json_key] = ""

    else:
        if is_list:
            # The value pulled is a list
            if not isinstance(out_value, list):
                status.add_error("Failed to retrieve a list from the JSON structure using the key: '%s'" % key)
                ret_val = process_status.ERR_wrong_json_structure
            else:
                if next_key:
                    is_key, key, is_list, new_key = make_pull_keys(next_key)  # validate the structure of the key used to pull from the JSON
                    if is_key:
                        # Pull the attribute value from each of the key entries
                        new_string = "["
                        for index, entry in enumerate(out_value):
                            ret_val, list_str = get_object_data(status, entry, key, is_list, next_key, out_json, new_json, separator_event, separator, True, bring_type)
                            new_string += list_str + ','
                        new_string += "]"
                else:
                    new_string = str(out_value)
        else:
            if out_value is not None:
                if not isinstance(out_value, str):
                    data_out = str(out_value)
                else:
                    if add_quotation:
                        data_out = '"' + out_value + '"'
                    else:
                        data_out = out_value

                if data_out:
                    if out_json:
                        if key == "":
                            json_key = "policy"
                        else:
                            json_key = get_key_string(key)
                        new_json[json_key] = data_out
                    else:
                        if separator_event:
                            new_string += separator  # before first field starting at the second instance and not the first instance
                        new_string += str(data_out)


    return [ret_val, new_string]

# ======================================================================================================================
# Test that the provided string is unique, otherwise update the unique dictionary
# ======================================================================================================================
def test_unique(test_str, add_separator, separator, dynamic_string, unique_struct):

    if not test_str in unique_struct.keys():
        # first occurence
        if add_separator and separator and not dynamic_string:
            # first instance - add separator to the hashed entry
            unique_struct[separator + test_str] = True  # next time this string would be ignored
        else:
            unique_struct[test_str] = True
        ret_val = True      # First occurrence of the test_str
    else:
        ret_val = False     # Not first occurrence
    return ret_val

# ======================================================================================================================
# Get the key for a new JSON structure
# Transform the object to create a key for the output JSON: ["operator"]["ip"] --> "ip"
# ======================================================================================================================
def get_key_string(key):
    new_key = key
    offset_end = key.rfind(']')
    if offset_end > 0:
        offset_start = key.rfind('[', 0, offset_end)
        if offset_start != -1 and (offset_end - offset_start) > 3:
            new_key = key[offset_start + 2: offset_end - 1]
    return new_key


# ======================================================================================================================
# Make a JSON struct from a where condition:
# where dbms = !dbms_name and table = !table_name --> { "dbms" : !dbms_name }
# example blockchain get operator where dbms = lsl_demo
# ======================================================================================================================
def make_jon_struct_from_where(cmd_words, offset_first):
    offset = offset_first
    words_count = len(cmd_words)

    if offset + 2 >= words_count:
        return process_status.ERR_command_struct, offset, ""

    ret_val = process_status.SUCCESS
    json_data = "{"

    while offset + 2 < words_count:

        key = cmd_words[offset]

        value = cmd_words[offset + 2]
        if len(value):
            if value[0] != '!':
                value = "\"%s\"" % value  # wrap with quotations

        if cmd_words[offset + 1] == '=':
            # Exact match
            json_data += ("\"%s\" : %s" % (key, value))
        elif cmd_words[offset + 1] == 'with':
            # Search a value within an array that may have multiple values
            if len(value) > 4 and value[0:2] == '"[' and value[-2:] == ']"':
                json_data += ("\"%s\" : %s" % (key, value[1:-1]))
            else:
                json_data += ("\"%s\" : [%s]" % (key, value))
        else:
            ret_val = process_status.ERR_command_struct
            break

        offset += 3

        if offset + 3 < words_count:
            if cmd_words[offset] == "and":
                offset += 1
                json_data += ','
                continue  # test for another key value pair
        break  # no end or not sufficient words

    if not ret_val:
        json_data += "}"
    else:
        json_data = ""

    return ret_val, offset, json_data


# ======================================================================================================================
# Given a list of JSON, maintain return the olddest considering a date/time field
# without a date attribute, return the first entry
# ======================================================================================================================
def get_first_instance(blockchain_out):
    first = None
    for entry in blockchain_out:
        if not first:
            first = entry
        else:
            # compare dates
            if "date" in entry.keys() and "date" in first.keys() and first["date"] > entry["date"]:
                first = entry
    return [first]  # return new array


# ======================================================================================================================
# Given a list of JSON, maintain the most recent considering a date/time field
# without a date attribute, return the last entry
# ======================================================================================================================
def get_recent_instance(blockchain_out):
    last = None
    for entry in reversed(blockchain_out):
        if not last:
            last = entry
        else:
            # compare dates
            if "date" in entry.keys() and "date" in last.keys() and last["date"] < entry["date"]:
                last = entry
    return [last]  # return new array


# ======================================================================================================================
# Get the ID from the JSON object.
# AnyLog objects are organized as key value leading to an object that has attributes and values including an ID.
# ======================================================================================================================
def get_object_id(json):
    try:
        key = next(iter(json))
    except:
        id = ""
    else:
        if "id" in json[key].keys():
            id = json[key]["id"]
        else:
            id = ""

    return id

# ======================================================================================================================
# test keys used in pull command and if needed use quotations:
# blockchain get table bring [table][name] "\t" [table][create] separator = "\n"
# --> blockchain get table bring ["table"]["name"] "\t" ["table"]["create"] separator = "\n"
# ======================================================================================================================
def make_pull_keys(word_str):

    is_list = False
    next_key = ""
    if len(word_str) > 1 and word_str[0] == '[' and word_str[-1] == ']' and word_str[1] != '{':     # not a list of policies

        key_string = ""
        is_key = True       # Flag that the string is an index to the JSON object

        key_list = word_str.split(']')
        entries_count = len(key_list) - 1       # Last entry in null
        for index in range (entries_count):
            entry = key_list[index]
            entry_length = len(entry)

            if not entry_length or entry[0] != '[':
                is_key = False
                break       # no brackets

            if entry_length == 1:
                # empty brackets --> list
                for list_index in range(index + 1, entries_count):
                    # get the key to pull from the list
                    entry_length = len(entry)
                    if not entry_length or entry[0] != '[':
                        is_key = False
                        break  # no brackets
                    next_key += (key_list[list_index] + ']')
                is_list = True
                break       # Pull up to the list and use next_key to bring the data

            if entry[1] == '\'' or entry[1] == '"':
                # test quotations inside the brackets
                if entry_length < 4 or entry[-1] != entry[1]:
                    is_key = False
                    break  # make sure close brackets

                # Ignore the spaces because quotations
                if re.search("[{}:]", entry):
                    is_key = False  # wrong CHAR SET used
                    break
                key_string += "[\"%s\"]" % entry[2:-1]
            else:
                # Include spaces in the re,search test
                if re.search("[ {}:]", entry):
                    is_key = False  # wrong CHAR SET used
                    break
                # Add quortation if not an index to a table
                if entry[1:].isdecimal():
                    key_string += entry + ']'
                else:
                    # Add quotation
                    key_string += "[\"%s\"]" %  entry[1:]
    else:
        is_key = False

    if not is_key:
        key_string = word_str

    return [is_key, key_string, is_list, next_key]

# ------------------------------------------------------------
# Given a JSON object, and nested keys - test the value
# ------------------------------------------------------------
def test_nested_key_value(src_obj, key, value):

    ret_val, key_string, is_list, next_key  = make_pull_keys(key)   # the key to pull from the JSON structure

    if ret_val:
        ret_val = False
        if isinstance(src_obj, list):
            for obj in src_obj:
                ret_val = test_dict_key_val(obj, key_string, value)
                if ret_val == True:
                    break   # One entry in the obj satisfies the condition
        elif isinstance(src_obj, dict):
            ret_val = test_dict_key_val(src_obj, key_string, value)
    return ret_val
# ------------------------------------------------------------
# Validate and test a single key value
# Key needs to be in the format: ["key"]
# ------------------------------------------------------------
def test_dict_key_val(json_obj, key, value):
    try:
        data_out = eval("json_obj" + key)
    except:
        ret_val = False  # No value for the searched attribute name
    else:
        if data_out == value:
            ret_val = True  # One entry satisfies the condition
        else:
            ret_val = False
    return ret_val
# ------------------------------------------------------------
# Given a JSON object, test key and value
# if the key address a list, test that the list includes the value
# ------------------------------------------------------------
def test_key_value(obj, key, value):
    if not key in obj.keys():
        return False

    sub_obj = obj[key]

    if sub_obj == '*':
        # all values qualify
        return True

    if isinstance(sub_obj, str):
        ret_val = value == sub_obj
    elif isinstance(sub_obj, list):
        # test all entries
        ret_val = value in sub_obj
    else:
        ret_val = False

    return ret_val


# ------------------------------------------------------------
# Given a JSON object and a key - return the value
# ------------------------------------------------------------
def get_value(obj, key):
    try:
        value = obj[key]
    except:
        value = None
    return value


# ------------------------------------------------------------
# Given a row in JSON - Return a list of the keys and the values
# ------------------------------------------------------------
def split_row_key_value(json_data):
    key_list = []
    value_list = []

    for key, value in json_data.items():
        key_list.append(key)
        value_list.append(value)

    return [key_list, value_list]
# ======================================================================================================================
# Get the inner object from the JSON object.
# ======================================================================================================================
def get_inner(json):
    try:
        key = next(iter(json))
    except:
        json_oject = None
    else:
        try:
            json_oject =  json[key]
        except:
            json_oject = None

    return json_oject
# ======================================================================================================================
# Get the entries within the object
# ======================================================================================================================
def get_policy_type(json):
    try:
        key = next(iter(json))
    except:
        key = None
    else:
        if len(json) != 1:
            key = None          # Multiple values in layer 1 - there is no policy type!

    return key

# ======================================================================================================================
# Get the policy type and the id
# Return a list with 2 values: [policy_type, policy_id]
# ======================================================================================================================
def get_policy_type_id(json):

    try:
        policy_type = next(iter(json))
    except:
        reply_list = [None, None]
    else:
        if len(json) != 1:
            reply_list = [None, None]          # Multiple values in layer 1 - there is no policy type!
        else:
            if not "id" in json[policy_type]:
                reply_list = [None, None]
            else:
                policy_id = json[policy_type]["id"]
                reply_list = [policy_type, policy_id]

    return reply_list   # Return [policy_type, policy_id]


# ======================================================================================================================
# Compare policies dates, return 1 if greater, 0 if equal and -1 if older
# ======================================================================================================================
def compare_policies_dates(policy1, policy1_type, policy2, policy2_type):

    policy1_date = get_policy_value(policy1, policy1_type, "date", "0000-00-00")
    policy2_date = get_policy_value(policy2, policy2_type, "date", "0000-00-00")

    if policy1_date > policy2_date:
        ret_val = 1
    elif policy1_date < policy2_date:
        ret_val = -1
    else:
        ret_val = 0
    return ret_val
# ======================================================================================================================
# Get the policy date
# ======================================================================================================================
def get_policy_value(policy, policy_type, attr_name, default_val):

    if not policy_type:
        # get the type if not provided
        try:
            key = next(iter(policy))
        except:
            key = None
            policy_date = default_val
    else:
        key = policy_type

    if key:
        # Get the date
        try:
            policy_date = policy[key][attr_name]
        except:
            policy_date = default_val

    return policy_date

# ======================================================================================================================
# Make JSON rows - organize JSON data as rows - each JSON unit is a string with a single '\n' at the end of the string
# ======================================================================================================================
def make_json_rows(status, source_data):
    pattern_any = re.compile('[{}"\'\n\t\r]')  # Find any of these occurrences
    pattern_include_comma = re.compile('[{}"\'\n\t\r,]')  # Find any of these occurrences
    pattern_single = re.compile('[\'\n\t\r]')  # Single queotation
    pattern_double = re.compile('["\n\t\r]')  # Single queotation

    row_counter = 0
    add_new_line = False
    paren_counter = 0
    copied_offset = 0
    updated_data = ""
    ret_val = process_status.SUCCESS

    offset = 0
    length = len(source_data)

    if length:
        if source_data[0] != '{':
            offset = source_data.find('{')
            if offset == -1:
                status.add_keep_error("Failed to identify JSON data in Put message body")
                ret_val = process_status.ERR_wrong_json_structure
                offset = length  # will not go into the loop
            else:
                copied_offset = offset  # Starting offset to scan the data
                length = source_data.rfind("}")
                if length == -1:
                    status.add_keep_error("Failed to identify JSON data in Put message body")
                    ret_val = process_status.ERR_wrong_json_structure
                    offset = length  # will not go into the loop
                else:
                    length += 1

        while offset < length:

            if add_new_line:
                obj = pattern_include_comma.search(source_data, offset)
            else:
                obj = pattern_any.search(source_data, offset)
            if not obj:
                break
            ch = obj.group()
            offset = obj.start()

            if ch == '\n' or ch == '\t' or ch == '\r' or ch == ',':
                # replace with space
                if ch == '\n' and add_new_line:
                    # Found as needed - a new line at the end of the row
                    add_new_line = False
                else:
                    updated_data += (source_data[copied_offset: offset] + ' ')
                    copied_offset = offset + 1
            elif ch == '{':
                if add_new_line:
                    updated_data += (source_data[copied_offset: offset] + '\n{')
                    copied_offset = offset + 1
                    add_new_line = False
                paren_counter += 1
            elif ch == '}':
                if not paren_counter:
                    status.add_keep_error(
                        "Missing parenthesis of type '}' in the JSON structure at offset: %u" % obj.start())
                    ret_val = process_status.ERR_wrong_json_structure
                    break
                row_counter += 1
                paren_counter -= 1
                if not paren_counter:
                    # New row - need to fine a new line (or add a new line
                    add_new_line = True
            elif ch == '\'':
                # find matching up to '\n' or '\r'
                obj = pattern_single.search(source_data, offset + 1)
                if not obj:
                    status.add_keep_error("Missing single quotation in the JSON structure at offset: %u" % (offset + 1))
                    ret_val = process_status.ERR_wrong_json_structure
                    break
                ch = obj.group()
                if ch != '\'':
                    status.add_keep_error("Missing single quotation in the JSON structure at offset: %u" % obj.start())
                    ret_val = process_status.ERR_wrong_json_structure
                    break
                offset = obj.start()
            elif ch == '"':
                # find matching up to '\n' or '\r'
                obj = pattern_double.search(source_data, offset + 1)
                if not obj:
                    status.add_keep_error("Missing double quotation in the JSON structure at offset: %u" % (offset + 1))
                    ret_val = process_status.ERR_wrong_json_structure
                    break
                ch = obj.group()
                if ch != '"':
                    status.add_keep_error("Missing double quotation in the JSON structure at offset: %u" % obj.start())
                    ret_val = process_status.ERR_wrong_json_structure
                    break
                offset = obj.start()
            offset += 1

    if not ret_val:
        if not copied_offset:
            updated_data = source_data  # Source data was set as needed
        elif copied_offset < offset:
            updated_data += source_data[copied_offset: offset]

    return [ret_val, updated_data, row_counter]

# ----------------------------------------------------------------------
# Get value from policy
# ----------------------------------------------------------------------
def get_policy_val(status, json_obj, policy_id, key, data_type, add_type_err, add_value_err):
    '''
    status - status object
    json_obj - needs to be a dictionary
    policy_id - the ID of the policy processed
    key - key that delivers the value
    data_type - the value data type (to be validated)
    add_type_err - add error if data type is wrong
    add_value_err - if value not found, add error
    '''
    ret_val = process_status.SUCCESS
    if key in json_obj:
        value = json_obj[key]
        if data_type:
            if not isinstance(value, data_type):
                if add_type_err:
                    status.add_error("The value for the key '%s' in JSON object (associated to policy '%s') is not of type %s" % (key, policy_id, str(data_type)))
                    ret_val = process_status.ERR_wrong_json_structure
    elif add_value_err:
        value = None
        status.add_error("The key '%s' (derived from policy '%s') is missing in JSON object" % (key, policy_id))
        ret_val = process_status.ERR_wrong_json_structure
    else:
        value = None
    return [ret_val, value]