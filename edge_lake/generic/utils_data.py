"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

from datetime import datetime
import ipaddress
import uuid
import hashlib
import re

import edge_lake.generic.process_status as process_status
import edge_lake.generic.process_log as process_log

# Define the pattern for characters to replace - 1) single quote 2) double quote 3) any control characters (both lower than space and higher than tilde ~).
basic_pattern_ = re.compile(r"['\"\x00-\x1F\x7F-\x9F]")

# Conversion table to allow proper dbms names and table names
translate_dict_ = {}
for x in range (256):

    char_x = chr(x)
    if char_x == '_' or char_x == ' ' or char_x == '-' or char_x == '.' or char_x == '/' or char_x == '\\' or char_x == ':':
        char_y = '_'
    elif (char_x >= 'A' and char_x <= 'Z'):
        char_y = char_x.lower()
    else:
        if (char_x >= 'a' and char_x <= 'z') or (char_x >= '0' and char_x <= '9'):
           continue                 # No need to translate

        char_y = "0x%02x" % x    # The string showing x in hex

    translate_dict_[x] = char_y


data_types_unifier_ = {

    "str" :     "varchar",
    "string" :  "varchar",
    'char varying' : "varchar",
    "uuid"   :  "uuid",
    "int":      "int",
    "bigint":   "bigint",
    "integer":  "int",
    "float":    "float",
    "float64":  "float",
    "decimal" : "float",
    "numeric" : "float", # https://www.postgresql.org/docs/current/datatype-numeric.html
    "double" :  "float",
    "char" :    "char",
    "character" : "char",
    "bool" :    "bool",
    "boolean" : "bool",
    "varchar" : "varchar",
    "timestamp" : "timestamp",
    "date"      : "date",
    "time"      : "time"
}

# Data types that require quotations on the value ot the default value
quotation_data_types_ = {
    "varchar"   : True,     # Default val requires quotations
    "char"      : True,     # Default val requires quotations
    "character" : True,     # Default val requires quotations
    "string"    : True,     # Default val requires quotations
    "str"       : True,     # Default val requires quotations
    "date"      : False,    # default may be a time function like now() and not requiring quotation
    "time"      : False,    # default may be a time function like now() and not requiring quotation
    "timestamp" : False,    # default may be a time function like now() and not requiring quotation
}

quotation_types_ = {
    "\""    :   True,
    "'"     :   True,
}
supported_data_types_ = {}      # A dictionary to the supported data types
for value in data_types_unifier_.values():
    supported_data_types_[value] = True


time_to_sec_ = {
    "second": 1,
    "seconds": 1,
    "minute": 60,
    "minutes": 60,
    "hour": 3600,
    "hours": 3600,
    "day": 86400,
    "days": 86400
}

url_chars_ = {  "20": ' ',
                "22": '"',
                "3c": '<',
                "3e": '>',
                "23": '#',
                "25": '%',
                "27": '\'',
                "7b": '{',
                "7d": '}',
                "7c": '|',
                "5c": '\\',
                "5e": '^',
                "7e": '~',
                "5b": '[',
                "5d": ']',
                "60": '`',
              }

hex_digits_ = "0123456789abcdef"

skip_chars_ = {
    ' ' :  1,
    '\t' : 1,
    '\r' : 1,
    '\n' : 1,
}

# -------------------------------------------------------------------------
#   Check if a string is hexadecimal
# -------------------------------------------------------------------------
def is_hex_str(hex_string):
    global hex_digits_
    return set(hex_string).issubset(hex_digits_)
# -------------------------------------------------------------------------
#   Transform a url string to string
# -------------------------------------------------------------------------
def url_to_str(url_str):

    url_entries = url_str.split('%')
    for index, entry in enumerate(url_entries):
        if len(entry) >= 2:
            sub_str = entry[:2].lower()
            if sub_str in url_chars_:
                # replace to the ascii char
                url_entries[index] = url_chars_[ sub_str ] + entry[2:]
            else:
                url_entries[index] = ('%' + url_entries[index]) # Return Original char
        else:
            url_entries[index] = ('%' + url_entries[index])  # Return Original char

    updated_str = ''.join(url_entries)
    return updated_str

# -------------------------------------------------------------------------
#   Test if quotation is needed with this data type and string
# -------------------------------------------------------------------------
def is_quotation_required(data_type, str_val):
    if data_type in quotation_data_types_:
        # Needed
        is_always = quotation_data_types_[data_type]
        if is_always or '(' not in str_val:
            # always needed in string, but time can be represented as a function (like now()) and then - not needed
            ret_val = False if str_val[0] in quotation_types_ else True
        else:
            ret_val = False     # now() --> return False
    else:
        ret_val = False        # Not needed
    return ret_val
# -------------------------------------------------------------------------
#   Test if quotation is required on data value
#   The case of now() does not need to be tested
# -------------------------------------------------------------------------
def is_add_quotation(data_type, data_val):
    if len(data_val) and data_val[0] == "\"":
        return False        # Quotation is on the string. For example, blob file name appears with quotation
    return  data_type in quotation_data_types_

# -------------------------------------------------------------------------
#   Transform time to seconds
# -------------------------------------------------------------------------
def time_to_seconds(counter, time_unit):
    '''
    Return time in seconds
    counter - the number of time units
    time_unit - seconds, minutes, hours, days
    '''
    global time_to_sec_

    if time_unit in time_to_sec_:
        seconds = time_to_sec_[time_unit]
        if counter.isnumeric():
            seconds *= int(counter)
        else:
            seconds = None
    else:
        seconds = None

    return seconds
# -------------------------------------------------------------------------
#   Map to a unified data type name
# -------------------------------------------------------------------------
def unify_data_type( status, data_type_name ):

    global data_types_unifier_

    ret_val = process_status.SUCCESS
    type_name = data_type_name.lower()

    index = type_name.find(' ')
    if index != -1:
        key = type_name[:index]
    else:
        key = type_name

    if key in data_types_unifier_:
        data_type = data_types_unifier_[key]
        if index != -1:
            data_type += type_name[index:]
    else:
        if type_name.startswith("char("):
            data_type = type_name
        elif type_name.startswith("character("):
            data_type = "char(" + type_name[10:]        # change "character" to "char"
        else:
            status.add_error(f"Data type '{type_name}' not supported")
            data_type = None
            ret_val = process_status.Data_type_not_supported

    return [ret_val, data_type]

# -------------------------------------------------------------------------
#   Return a unified data type from the data_types_unifier_
# -------------------------------------------------------------------------
def get_unified_data_type( data_type_name ):

    global data_types_unifier_

    data_type = data_type_name.strip()
    if len(data_type):
        if data_type in data_types_unifier_:
            return data_types_unifier_[data_type]
        if data_type.startswith("timestamp "):
            return "timestamp"
        if data_type.startswith("character"):
            data_type = data_type.strip()   # Make the parenthesis without spaces (before and after)
            return data_type.replace("character", "char", 1)

    return None
# -------------------------------------------------------------------------
# Return true if the data type is in the supported dictionary
# -------------------------------------------------------------------------
def is_supported_data_type(data_type):

    global supported_data_types_
    index = data_type.find(' ')
    if index != -1:
        key = data_type[:index].lower()
    else:
        key = data_type.lower()

    if  key in supported_data_types_:
        return True
    if key.startswith("char("):
        return True
    return False
# -------------------------------------------------------------------------
# Remove special chars that conflict with naming convention
# Database names and table names are set with alphanumeric characters
# -------------------------------------------------------------------------
def reset_str_chars( source_str ):
    global translate_dict_
    return source_str.translate ( translate_dict_ )

# ======================================================================================================================
# Remove control chars (replace with space) - keep printable chars
# Replace the following:
# '\'': '`', '"': '`',  chars lower than ' ' or greater than  '~' with space
# ======================================================================================================================
def prep_data_string(src_str):

    # Define the replacement function
    def replacement(match):
        ch = match.group(0)
        if ch == '\'' or  ch == '"':
            return '`'
        return ' '

    # Perform the replacement
    result = basic_pattern_.sub(replacement, src_str)

    return result

# ======================================================================================================================
# Remove control chars (replace with space) - keep printable chars
# Replace_chars is a dictionary to replace key with value
# If fix_json is True, make bool value lower case and remove comma at the end of the string
# ======================================================================================================================
def replace_string_chars(remove_control_chars, src_str, replace_chars, fix_json = False):
    offset = 0
    new_str = ""
    is_string = False
    for index, ch in enumerate(src_str):

        if ch == '"':
            is_string = not is_string

        if replace_chars and ch in replace_chars.keys():
            # ch is a key in the dictionary
            new_str += (src_str[offset:index] + replace_chars[ch])
            offset = index + 1
        elif remove_control_chars and ch < ' ' or ch > '~':
            # ch is a control char
            new_str += (src_str[offset:index] + ' ')  # replace char with space
            offset = index + 1
        elif fix_json and not is_string:
            if ch.isupper():
                new_str += (src_str[offset:index] + ch.lower())  # replace char with space
                offset = index + 1
    if offset:
        new_str += src_str[offset:]
    else:
        new_str = src_str

    if fix_json:
        new_str = new_str.strip()   # remove leading and trailing spaces
        if new_str and new_str[-1] == ',':
            new_str = new_str[:-1]  # Remove the comma at the end

    return new_str


def get_ip_addr(ip_string):
    """
    :param ip_string:
    :return: return ip address or null if string is not an ip address
    """
    index = 0

    while (is_ip_char(ip_string[index])):
        index += 1
        if index > 15:
            return ""

    if index < 7:
        return ""
    return ip_string[:index]


# ==================================================================
#
# Validate if datatype (such as dict, list, tuple ) is empty
# 
# ==================================================================
def is_empty(any_structure):
    if any_structure:
        return False
    else:
        return True


def is_ip_char(one_char):
    if one_char >= '0' and one_char <= '9':
        return True
    if one_char == '.':
        return True
    return False


# =======================================================================================================================
# Test Array Text - return true if string contains the tested text
# =======================================================================================================================
def test_text(my_array, from_entry, tested_text):
    if len(my_array) < from_entry + len(tested_text):
        return False
    index = from_entry
    for entry in tested_text:
        if my_array[index] != entry:
            return False
        index += 1
    return True


# =======================================================================================================================
# Concatenate words to a string - no space between words
# =======================================================================================================================
def concat_words(my_array, from_entry, to_enty=0):
    if to_enty:
        array_len = to_enty
    else:
        array_len = len(my_array)
    if array_len <= from_entry:
        return ""
    new_string = ""
    for x in range(from_entry, array_len):
        new_string += my_array[x]

    return new_string


# =======================================================================================================================
# Concatenate words to a string - add space between words
# =======================================================================================================================
def get_str_from_array(my_array, from_entry, to_enty=0):
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
        if len(my_array[x]) == 0:
            continue
        if (x > from_entry):
            new_string += ' '

        if (my_array[x][0] == '(' and my_array[x][-1] == ')') \
                or (my_array[x][0] == '{' and my_array[x][-1] == '}') \
                or (my_array[x][0] == '[' and my_array[x][-1] == ']') \
                or (my_array[x][0] == '"' and my_array[x][-1] == '"'):
            quotations = False
        else:
            quotations = True
        if quotations and my_array[x].find(' ') != - 1:  # with space
            new_string += ('\"' + my_array[x] + '\"')
        else:
            new_string += my_array[x]

    return new_string


# =======================================================================================================================
# Find the location of an extry in a list, or return -1 if not in the list
# =======================================================================================================================
def list_find(my_list, my_entry):
    try:
        index = my_list.index(my_entry)
    except:
        index = -1
    return index


# JSON to SQL supprot function
# =======================================================================================================================
# Test if a date field
# Example: '2012-12-01T00:30:00'
# Example: '2019-10-11T17:13:39.0430145Z'
# =======================================================================================================================
def check_timestamp(line: str):

    ret_val = False
    str_length = len(line)

    if str_length > 10:
        if line[-6] == '+' and datetime.strptime(line, '%m/%d/%Y %I:%M:%S %p %z'):
            ret_val = True
        else:
            if line[-3] == ':' and (line[-6] == '+' or line[-6] == '-') and line[-2:].isnumeric() and  line[-5:-3].isnumeric():
                # UTC offset format like 2011-11-04T00:05:23+04:00
                line = line[:-6]
                str_length -= 6

            if line[-1] == 'Z':
                # remove the Z char
                date_length = str_length - 1
            else:
                date_length = str_length

            if line[4] == '-' and line[7] == '-' and line[date_length - 1].isdigit():
                if date_length >= 27:
                    date_length = 26  # the function strptime doesn't take 27 chars
                if line[10] == 'T':
                    # Remove the 'T' - Format like '2019-10-11T17:13:39.0430145Z'
                    date_str = line[:10] + " " + line[11:date_length]
                else:


                    # test utc offset - example:  2021-11-04 00:05:23+04:00
                    if line[-6] == '+' and int(line[-2:]) >=0 and int(line[-5:-3]) >=0:
                        date_length -= 6

                    date_str = line[:date_length]

                if date_length == 19:
                    # no milliseconds
                    try:
                        datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')  # returns this format if line is correct
                        ret_val = True
                    except ValueError as err:
                        pass
                    except:
                        pass
                elif date_length > 19 and date_str[19] == '.' and len(date_str) >= 21 and len(date_str) <= 27:
                    # if longer than 26 characters: '2019-10-11T18:02:48.0370025Z'
                    try:
                        datetime.strptime(date_str,
                                                   '%Y-%m-%d %H:%M:%S.%f')  # returns this format if line is correct
                        ret_val = True
                    except ValueError as err:
                        pass
                    except:
                        pass
    return ret_val


def set_subsecond_six(timestamp: str):
    """
    if the length of sub_seconds is > round up to the 6th digit
    """
    if "." in timestamp:
        sub_second = timestamp.split(".")[-1]
        if len(sub_second) > 6:
            timestamp = timestamp.replace(sub_second, sub_second[:6]) + "'"
    return timestamp


def check_date(line: str):
    if len(line) == 10 and line[4] == '-' and line[7] == '-':
        try:
            datetime.strptime(line, '%Y-%m-%d')
        except ValueError as err:
            ret_val = False
        except:
            ret_val = False
        else:
            ret_val = True
    else:
        ret_val = False

    return ret_val


def check_time(line: str):
    """
    Check if line is time format
    :args:
       line:str - line to convert
    :return:
       True if valid
       False if not
    """
    try:
        datetime.strptime(line.split(".")[0], '%H:%M:%S')
    except:
        return False
    else:
        return True


def check_uuid(line: str = ''):
    """
    check if value is UUID
    :return:
       True if valid
       False if not
    """
    for v in [1, 3, 4, 5, 6]:
        try:
            uuid.UUID(line, version=v)
        except:
            pass
        else:
            return True
    return False


def check_ip_address(line: str):
    """
    Check whether value type is IP or not
    :param:
       line value
    :return:
       True if valid
       False if not
    """
    try:
        ipaddress.ip_address(line)
    except:
        return False
    return True

# ==================================================================
#
# Organize command string in a list. Identifies multiple commands
# on the same string and moves a second command to status object.
# Identify JSON structures if brackets_lefts is not -1
#
# ==================================================================
def cmd_line_to_list_with_json(status: process_status, command: str, brackets_lefts: int, brackets_right: int,
                               keep_quotation: bool = False):
    """

    :param status: An object to contain a Next Command - which is identtified by and and (&) sign
    :param command: The command to process
    :param brackets_lefts: A counter for left brackets to identify a JSON structure (-1 to ignore JSON structs)
    :param brackets_right:  A counter for right brakets
    :return:
    """

    counter_left = brackets_lefts  # number of open brackets
    counter_right = brackets_right  # number of close brackets

    words_list = []

    length = len(command)
    start_word = False
    word_offset = 0
    index = 0

    while index < length:
        ch = command[index]

        if not start_word:
            word_offset = index       # save the initial offset of the word to consider

        if brackets_lefts != -1:  # -1 ignore JSON

            if counter_left != counter_right:
                # in the process of pulling JSON data
                index, counter_left, counter_right = find_parentheses_offset(command, index, '{', '}', None, counter_left, counter_right)
                index = length
                word = command[word_offset:index]
                if word[-1] == '\n':
                    word = word[:-1]
                word = word.replace('\t', ' ')
                words_list.append(word)  # Add the rest
                break

            if ch == '{' or ch == '[':

                if start_word and ch == '{':
                    # With brakets type - '[' : need to see a space before previous
                    words_list.append(command[word_offset:index])  # save previous word in list
                    word_offset = index
                elif index and command[index - 1] == ' ':
                    word_offset = index

                start_word = False
                if ch == '{':
                    index, counter_left, counter_right = find_parentheses_offset(command, index, '{', '}', None, counter_left, counter_right)
                else:
                    index, counter_left, counter_right = find_parentheses_offset(command, index, '[', ']', '[:', counter_left, counter_right)

                if index == -1:
                    # no matching:
                    index = length
                    word = command[word_offset:index]
                    if word[-1] == '\n':
                        word = word[:-1]
                    word = word.replace('\t', ' ')
                    words_list.append(word)   # Add the rest
                    break
                else:
                    word = command[word_offset:index + 1].replace('\t', ' ')
                    if word == "{}" and len(words_list) == 1 and command[word_offset - 1] != ' ' and command[word_offset - 1] != '=':
                        # This is the case of key{} = value --> kep "key{}" as one ord
                        words_list[0] = words_list[0] + "{}"
                    else:
                        words_list.append(word)  # Add parentheses
                    index += 1
                    continue


        if ch == '\r':
            # take the rest as a single word
            # Used in "set script autoexec data" for the data section
            if start_word:
                words_list.append(command[word_offset:index])  # save word in list
            words_list.append(command[index + 1:])  # save word in list (without the \r)
            start_word = False
            break

        if ch == ' ' or ch == '\n' or ch == '\t':
            if start_word:
                words_list.append(command[word_offset:index])  # save word in list
            start_word = False
            index += 1
            continue  # ignore space

        if ch == '!':  # separate words
            if (index + 1) < length and command[index + 1] == '=':
                # Considering != (not equal)
                if start_word:
                    words_list.append(command[word_offset:index])  # save word in list
                words_list.append('!=')
                start_word = False
                index += 2
                continue

        if (ch == '=' or ch == '+'):  # separate words
            if start_word:
                words_list.append(command[word_offset:index])  # save word in list

            start_word = False
            index += 1

            if ch == '=' and index < length and command[index] == '=':
                # This is the case of "=="
                index += 1
                words_list.append('==')
            else:
                words_list.append(ch)
            continue

        if (ch == '<' or ch == '>'):  # seperate words
            if start_word:
                if command[word_offset:index] == '-':
                    # This is the case of instructions to the local node like: -> where max_time = 10 second
                    words_list.append("->")
                    start_word = False
                    index += 1
                    continue

                words_list.append(command[word_offset:index])  # save word in list

            start_word = False
            index += 1

            if index < length and command[index] == '=':  # >= or <=
                index += 1
                words_list.append(ch + '=')
            else:
                words_list.append(ch)
            continue

        if ch == '&':  # new command
            if start_word:
                words_list.append(command[word_offset:index])  # save word in list
            if index + 1 < length:
                if status:
                    status.set_next_command(command[index + 1:])  # move next command to status object
                else:
                    words_list.append(
                        '&')  # thisis the case where a script is processed - recreating a single line string with multiple commands
            break

        if ch == '(' and length > index + 1:

            end_offset, count_left, count_right = find_parentheses_offset(command, index + 1, '(', ')', ignore_chars = None, left_counter = 1, right_counter = 0)
            if count_left != count_right:
                status.add_error("Missing closing parenthesis with statement: %s" % command[index:])
                words_list = None
                break  # no matching quotes

            if start_word == False:
                words_list.append(command[index:end_offset + 1])  # include the ( ) signes

            # if not start_word - copy inside the brackets as is
            index = end_offset + 1
            continue

        if start_word == False:
            if ch == '\"':
                if length > index + 1:
                    end_offset = command.find('\"', index + 1, length)  # get end of quotes
                    if end_offset == -1:
                        status.add_error("Missing double quotations after offset %u" % (index + 1))
                        words_list = None
                        break  # no matching quotes
                    if keep_quotation:
                        words_list.append(command[index:end_offset + 1])
                    else:
                        words_list.append(command[index + 1:end_offset])
                    index = end_offset + 1
                    continue

            if ch == '\'' or ch == '`':
                # These quotations are to be kept
                if length > index + 1:
                    end_offset = command.find(ch, index + 1, length)  # get end of quotes
                    if end_offset == -1:
                        status.add_error("Missing single quotations after offset %u" % (index + 1))
                        words_list = None
                        break  # no matching quotes
                    words_list.append(command[index:end_offset + 1])
                    index = end_offset + 1
                    continue

            if ch == '\\':
                # if next char is quotation - copy exact up to the end of the quotation
                if length > index + 3:  # need to find \" and a matching \"
                    if command[index + 1] == '"':
                        end_offset = command.find('\\"', index + 2, length)
                        if end_offset:
                            words_list.append(command[index:end_offset + 2])  # include the \" signes
                            index = end_offset + 2
                            continue

            if ch == '#':
                start_word = False
                break  # comment - ignore the rest

            start_word = True
            word_offset = index
        index += 1

    if words_list != None and start_word:  # manage last word in command line
        words_list.append(command[word_offset:])

    if brackets_lefts != -1:
        return [words_list, counter_left, counter_right]  # with JSON
    else:
        return words_list  # ignore JSON


# ======================================================================================================================
# Test if a string represents a float value
# ======================================================================================================================
def isfloat(value_str):
    try:
        float(value_str)
        return True
    except:
        return False
# ======================================================================================================================
# Test if a string represents an int value
# ======================================================================================================================
def isint(value_str):

    try:
        int(value_str)
        return True
    except:
        return False
# ======================================================================================================================
# Transform the string to float if possible
# ======================================================================================================================
def str_to_float(value_str, err_val):

    try:
        value = float(value_str)
    except:
        value = err_val

    return value

# ======================================================================================================================
# Removes entries in list (removed entries match 'entry' text
# ======================================================================================================================
def organize_list_entries(string_list: list):
    list_size = len(string_list)
    i = 0
    while i < list_size:
        entry = string_list[i]
        if entry == "":
            del string_list[i]
            list_size -= 1
        elif entry[0] == " ":
            # Remove leading and trailing spaces
            entry = entry.strip()
            if entry == "":
                del string_list[i]
                list_size -= 1
            else:
                string_list[i]=entry
                i += 1
        elif entry[-1] == " ":
            # No leading spaces, only trailing - remove trailing spaces
            entry = entry.rstrip()
            if entry == "":
                del string_list[i]
                list_size -= 1
            else:
                string_list[i] = entry
                i += 1
        else:
            i += 1


# ======================================================================================================================
# Map a string to a dictionary
# ======================================================================================================================
def string_to_dictionary(str_data: str):
    try:
        dict_data = eval(str_data)
    except (SyntaxError, NameError, TypeError, ZeroDivisionError) as error:
        dict_data = None
        err_msg = "Failed eval process on JSON string to struct: " + str(error)
        process_log.add("Error", err_msg)  # Log original message
        err_msg = get_str_to_json_error(err_msg, str_data)
        if err_msg:
            process_log.add("Error", err_msg)  # Log new  message
    except:
        dict_data = None

    return dict_data

# ======================================================================================================================
# Try to identify the error word in the mapping between string to JSON
# ======================================================================================================================
def get_str_to_json_error(err_msg, data):
    start_offset = err_msg.rfind("column")
    if start_offset == -1:
        error_txt = ""
    else:
        start_offset += 7       # Set on the column number
        end_offset = err_msg.find(" ", start_offset)   # search for the end of the column offset
        if end_offset == -1:
            error_txt = ""
        else:
            column_offset = err_msg[start_offset:end_offset]
            if not column_offset.isdecimal():
                error_txt = ""
            else:
                index = int(column_offset)
                if index > 0:
                    index -= 1
                error_txt = "JSON error starting at: [" + data[index:index + 30] + " ... ]"

    return error_txt
# ======================================================================================================================
# Convert special chars like \n to one byte
# ======================================================================================================================
def get_compiled_str(source_str):
    compiled_str = ""

    length = len(source_str)
    offset = 0

    while offset < length:
        special_char = False
        first_char = source_str[offset:offset + 1]
        compiled_char = first_char
        if first_char == '\\':
            if offset + 1 < length:
                second_char = source_str[offset + 1:offset + 2]
                if second_char == 'n':
                    compiled_char = '\n'
                    special_char = True

        compiled_str += compiled_char

        if special_char:
            offset += 2
        else:
            offset += 1

    return compiled_str


# ======================================================================================================================
# Remove chars from string
# ======================================================================================================================
def remove_chars(source_str: str, removed_chars: str):
    for ch in removed_chars:
        source_str = source_str.replace(ch, '')

    return source_str


# ======================================================================================================================
# Remove comment in a string
# ======================================================================================================================
def remove_comment(source_str: str):
    index = source_str.find("#")
    reply_str = source_str[:index]  # if index is -1, only the new line ("\n") is removed

    return reply_str


# ======================================================================================================================
# Find the length of a word that can be considered in the dictionary
# ======================================================================================================================
def get_length_for_dictionary(source_str: str, offset: int):
    i = 0
    for char in source_str[offset:]:

        if (char >= 'a' and char <= 'z') or (char >= 'A' and char <= 'Z'):
            i += 1
            continue
        if char == '_' or char == '-' or char == '@':
            i += 1
            continue
        if i == 0 and char == '!':
            i += 1
            continue  # firs char is !

        break

    return i


# ======================================================================================================================
# Remove extra spaces in string
# ======================================================================================================================
def make_single_space_string(source: str):
    if source == "":
        return source

    words = source.split()
    dest = ""
    first = True
    for one_word in words:
        if first:
            dest += one_word
            first = False
        else:
            dest += (" " + one_word)
    return dest


# ======================================================================================================================
# Get offset to substring based on matching parentheses
# starting point is the first parenthesis
# ======================================================================================================================
def find_parentheses_offset(test_str, from_offset, open_char, close_char, ignore_chars = None, left_counter = 0, right_counter = 0):
    stop_offset = len(test_str)

    counter_left = left_counter
    counter_right = right_counter
    position = -1
    skip_char = False
    for offset in range(from_offset, stop_offset):
        if skip_char:
            skip_char = False
            continue
        if test_str[offset] == open_char:
            counter_left += 1
        elif test_str[offset] == close_char:
            counter_right += 1
            if ignore_chars and offset <  (stop_offset - 1) and test_str[offset + 1] in ignore_chars:
                # ignore_char is a char after the closing char - to allow [operator][id] to be a single string
                if test_str[offset + 1] != open_char:
                    # This is the case of an OR string: [tags][name]:[tags][host] returned as : "[tags][name]:[tags][host]"
                    skip_char = True
                continue
        if counter_left == counter_right:
            position = offset
            break
    return [position, counter_left, counter_right]


# ======================================================================================================================
# Find first non-space offset
# return -1 if not found
# ======================================================================================================================
def find_non_space_offset(test_str, from_offset):
    str_len = len(test_str)
    offset = from_offset
    ret_offset = -1  # returned if no space
    while offset < str_len:
        # go to first position which is not a space
        if test_str[offset] != ' ':
            ret_offset = offset
            break
        offset += 1
    return ret_offset


# ======================================================================================================================
# Compare if array at offset includes the second array
# ======================================================================================================================
def is_sub_array(source_array, offset, sub_array):
    test_length = len(sub_array)
    if test_length + offset > len(source_array):
        return False
    return source_array[offset:offset + test_length] == sub_array

# ======================================================================================================================
# For a given string, determine if enclosed in the specified parenthesis.
# Used to determine the type of structure represented by the string: { } represents a dictionary and [ ] for a list.
# Ignore trailing \r\n
# Returns dict or list or none
# ======================================================================================================================
def get_str_obj(tested_str):

    if tested_str:
        str_len = len(tested_str)
        if str_len > 1:
            first_char = tested_str[0]
            last_char = tested_str[-1]
            if first_char == '{' or first_char == '[': # Dictionary or list
                if first_char == '{' and last_char == '}':
                    return "dict"
                elif first_char == '[' and last_char == ']':
                    return "list"

            offset_first = 0
            while first_char in skip_chars_:
                offset_first += 1
                if offset_first >= str_len:
                    return "none"
                first_char = tested_str[offset_first]
            offset_last = 1     # -1 is the last char
            while last_char in skip_chars_:
                offset_last += 1
                if (offset_last + offset_first + 1) >= str_len:
                    return "none"
                last_char = tested_str[-offset_last]
            if first_char == '{' and last_char == '}':
                return "dict"
            elif first_char == '[' and last_char == ']':
                return "list"
    return "none"

# ======================================================================================================================
# Get word length - return length to space or end of line
# ======================================================================================================================
def get_word_length(source_string, offset, end_word_char = ' '):
    # In most cases end word char is space.
    # When data is send between nodes - it can be tab (space is replaced with tab)

    index = source_string.find(end_word_char, offset)  # offset to next spece char
    if index == -1:
        index = len(source_string)  # length to last char
    return index - offset


# ======================================================================================================================
# Get word start
# ======================================================================================================================
def get_word_start(source_string, offset):
    length = len(source_string)
    for index in range(offset, length):
        if source_string[index] != ' ':
            return index

    return -1


# ======================================================================================================================
# Change to lower case, ignore substring in quotations
# ======================================================================================================================
def to_lower_ignore_quoted_substr(source_string):
    quotation = "!"
    start = 0
    new_string = ""
    for index, ch in enumerate(source_string):

        if quotation == "!":
            if ch == '\'' or ch == '"' or ch == '`':
                # found the starting point
                quotation = ch
                if index > start:
                    new_string += source_string[start:index].lower()
                new_string += quotation
                start = index
        elif quotation == ch:
            # found ending point
            if index >= (start + 2):
                new_string += source_string[start + 1:index]
            new_string += quotation  # unify quotations
            quotation = '!'
            start = index + 1  # after the quotation

    if len(source_string) > start:
        new_string += source_string[start:].lower()
    return new_string


# ======================================================================================================================
# Get an entry from a hash table - go over the entries - find the entry with key equals provided value
# ======================================================================================================================
def get_from_dict(hash_table, key, value):
    for hash_value in hash_table.keys():
        entry = hash_table[hash_value]
        if isinstance(entry, dict):
            if key in entry.keys():
                if entry[key] == value:
                    return entry
    return None


# ======================================================================================================================
# Get an entry from a list - go over the entries - find the entry with key equals provided value
# If value not provided, return the attribute that is indexed by the key
# ======================================================================================================================
def get_from_list(list_table, key, value):
    for entry in list_table:
        if isinstance(entry, dict):
            if key in entry.keys():
                if value:
                    if entry[key] == value:
                        return entry
                else:
                    return entry[key]
    return None


# ======================================================================================================================
# Calculate data buffer Hash value
# ======================================================================================================================
def get_string_hash(hash_type, data, prefix_data):
    if hash_type == "sha256":
        str_hash = hashlib.sha256()  # Create the hash object, can use something other than `.sha256()` if you wish
    else:
        str_hash = hashlib.md5()

        if prefix_data:
            # Prefixed data can be dbms name and table name that are considered in the hash
            str_hash.update(prefix_data.encode())  # Update the hash with the prefix data+

        str_hash.update(data.encode())  # Update the hash

    return str_hash.hexdigest()  # Get the hexadecimal digest of the hash

# ======================================================================================================================
# Get ID from a list of string - needs to be compatible with get_hash_value(status, file_name, lock_key)
# ======================================================================================================================
def get_hash_from_list(data_list):

    list_hash = hashlib.md5()

    for entry in data_list:
        list_hash.update(entry.encode())  # Update the hash


    return list_hash.hexdigest()  # Get the hexadecimal digest of the hash

# ======================================================================================================================
# Test a sequence of words
# ======================================================================================================================
def test_words(words_array, offset, test_array):
    if offset + len(test_array) > len(words_array):
        return False  # Missing words in words_array

    for i in range(0, len(test_array)):
        if words_array[offset + i] != test_array[i]:
            return False
    return True


# ======================================================================================================================
# Split -  Split consider quotations as one word
# ======================================================================================================================
def split_quotations(command):
    array = []
    start = 0
    step = 0
    open_brackets = 0
    close_brackets = 0
    with_tab = False            # Changed to True when tab is inside a word
    for index, char in enumerate(command):

        if not step:
            if char == '\'' and start == index:
                step = 11  # start of a single quotations that are kept
                continue
            elif char == '(':
                open_brackets += 1
                step = 21  # start of a single brackets
                continue
            elif char == '"':
                # start quotations without the slash
                step = 100
                continue

        if step == 11:
            # inside single quotations
            if char == '\'':
                with_tab = _append_word(array, command[start:index + 1], with_tab)
                step = 0
                start = index + 2
            continue
        elif step == 21:
            # inside parenthesis

            if char == ')':
                close_brackets += 1
                if open_brackets == close_brackets:
                    with_tab = _append_word(array, command[start:index + 1], with_tab)
                    step = 0
                    start = index + 1
                    if len(command) > start and command[start] == ' ':
                        # space after parenthesis are closed. 2 use cases:
                        # a) insert_timestamp > now() - 1 day"
                        # b) insert_timestamp > now()- 1 day"  # no space after now()
                        start += 1

            elif char == '(':
                open_brackets += 1
            continue

        if step == 100:
            # process up to and including end quotation (slash is not considered)
            if char == '"':
                # end quotation found
                with_tab = _append_word(array, command[start + 1:index], with_tab)
                step = 0
                start = index + 2
            continue
        if char == '\\':
            step += 1  # find quotation
            continue
        if char == '\"':
            step += 1  # find matching quotation
            if step == 4:
                # end quotation
                with_tab = _append_word(array, command[start:index - 1], with_tab)
                step = 0
            start = index + 1  # step 2 and 4
            continue
        if step == 2:
            continue  # find end of quotation
        step = 0
        if char == ' ':
            if index > start:
                if index > start:
                    with_tab = _append_word(array, command[start:index], with_tab)
                start = index + 1
        elif char == '\t':
            # Flag with tabs - need to bereplaced back to space"
            with_tab = True

    if len(command) > start:
        _append_word(array, command[start:], with_tab)
        
    return array

# ======================================================================================================================
# append word to array - replace tab with space
# ======================================================================================================================
def _append_word(array, word, with_tab):
    if with_tab:
        new_word = word.replace('\t', ' ')
    else:
        new_word = word

    array.append(new_word)
    return False        # always sets with_tab to False

# ======================================================================================================================
# Get comma separated string from the array
# comma can be at a start of a word or end of a word
# ======================================================================================================================
def get_comma_separated_str(words_array, offset):
    need_comma = False
    new_str = ""
    for index, word in enumerate(words_array[offset:]):

        if not need_comma:
            if word[0] == ',':
                break
        else:
            if word[0] != ',':
                break
        new_str += word
        if word[-1] == ',':
            need_comma = False
        else:
            need_comma = True

    return [offset + index, new_str]



# ======================================================================================================================
# Find the start offset and the end offset of the word before an offset
# " name (" --> find_word_before( string, 7) --> "name" --> returns [1,5]
# ======================================================================================================================
def find_word_before(data_str, offset):
    index = offset - 1
    offset_end = 0
    offset_start = 0
    while index:
        if data_str[index] == ' ':
            # either space before the parenthesis or space ti previous word
            if offset_end:
                offset_start = index + 1
                break
        else:
            if not offset_end:
                offset_end = index + 1  # First char before parenthesis
        index -= 1
    if not index and data_str[0] == ' ':
        offset_start += 1

    return [offset_start, offset_end]


# ======================================================================================================================
# Find the next word on or after the provided offset
# Offset start is returned as -1 if no such word
# ======================================================================================================================
def find_word_after(data_str, offset):
    index = offset
    offset_end = len(data_str)
    offset_start = -1
    while index < offset_end:
        if data_str[index] == ' ':
            # end of word
            if offset_start != -1:
                offset_end = index
                break  # space at the end of the serached word
        else:
            if offset_start == -1:
                offset_start = index  # First char which is not a space
        index += 1

    return [offset_start, offset_end]


# ======================================================================================================================
# Get the word after current word or space
# ======================================================================================================================
def get_word_after(data_str, offset):
    offset_start, offset_end = find_word_after(data_str, offset)
    if offset_start == -1:
        word = ""
    else:
        word = data_str[offset_start:offset_end]
    return word


# ======================================================================================================================
# find word offset
# if positioned on a word, find offset-start and offset-end of the word
# ======================================================================================================================
def get_word_offsets(data_str, offset):
    offset_start = data_str.rfind(' ', 0, offset)
    offset_start += 1
    offset_end = data_str.find(' ', offset)
    if offset_end == -1:
        offset_end += 1
    return [offset_start, offset_end]


# ======================================================================================================================
# Get the content of the inner brackets: a(b(c))) --> c
# ======================================================================================================================
def get_iner_brackets(data_str, left_bracket, right_bracket):
    inner_str = 0
    offset_start = data_str.rfind(left_bracket)
    if offset_start != -1:
        offset_end = data_str.find(right_bracket, offset_start + 1)
        if offset_end != -1:
            inner_str = data_str[offset_start + 1:offset_end]
    return inner_str


# ======================================================================================================================
# change total time in seconds to hours - minutes - Seconds
# ======================================================================================================================
def seconds_to_hms(total_time):
    seconds_time = total_time
    hours_time = int(seconds_time / 3600)
    seconds_time -= (hours_time * 3600)
    minutes_time = int(seconds_time / 60)
    seconds_time -= (minutes_time * 60)

    return [hours_time, minutes_time, seconds_time]

# ======================================================================================================================
# get a string with hh:mm:ss
# ======================================================================================================================
def get_formatted_hms(total_time):
    hours_time, minutes_time, seconds_time = seconds_to_hms(total_time)
    return "%02u:%02u:%02u" % (hours_time, minutes_time, seconds_time)

# ======================================================================================================================
# Go over entries in array and replace chars
# For example, in a message, a space is replaced with a tab
# ======================================================================================================================
def replace_chars_in_list(words_array, from_enytry, old_val, new_val):
    for index, entry in enumerate(words_array[from_enytry:]):
        offset = entry.find(old_val)
        if offset != -1:
            # old_val exists - replace all occurences
            words_array[from_enytry + index] = entry.replace(old_val, new_val)


# ======================================================================================================================
# Convert Byte array to Hex
# ======================================================================================================================
def bytes_to_hex(byte_array):
    try:
        hex_bytes = ''.join(["%02X" % x for x in byte_array])
    except:
        hex_bytes = ""
    return hex_bytes


# ======================================================================================================================
# Convert Hex string to byte array
# ======================================================================================================================
def hex_to_bytes(hex_str):
    bytes = []

    for i in range(0, len(hex_str), 2):
        bytes.append(chr(int(hex_str[i:i + 2], 16)))

    return ''.join(bytes)


# ======================================================================================================================
# Remove quotations over a string
# ======================================================================================================================
def remove_quotations(test_str):

    if len(test_str) > 2:
        first_char = test_str[0]
        if first_char == test_str[-1]:
            if first_char == "'" or first_char == '"' or first_char == '`':
                test_str = test_str[1:-1]
    return test_str
# ======================================================================================================================
# Test double quotation and remove
# ======================================================================================================================
def test_remove_quotations(status, test_str, must_exists, process_name):
    '''
    status - process object
    test_str - the string with the parenthesis
    must_exists - parenthesis are required on this string
    process_name - provided to the error log message
    '''

    ret_val = process_status.SUCCESS
    if len(test_str) > 2:
        first_char = test_str[0]
        if first_char == "'" or first_char == '"' or first_char == '`':
            if first_char == test_str[-1]:
                test_str = test_str[1:-1]
            else:
                status.add_error(f"Missing parenthesis in {process_name}: {test_str}")
                ret_val = process_status.Error_str_format
        elif must_exists:
            status.add_error(f"Missing parenthesis in {process_name}: {test_str}")
            ret_val = process_status.Error_str_format
    elif must_exists:
        status.add_error(f"Wrong string format or missing parenthesis in {process_name}: {test_str}")
        ret_val = process_status.Error_str_format
    return [ret_val, test_str]

# ======================================================================================================================
# Test if double quotations are arround the word.
# All types of quotations are evaluated
# ======================================================================================================================
def is_with_double_quotation(word):
    if len(word) < 2:
        return False
    if word[0] == '"' or word[0] == '”' or word[0] == '“':  # these are 3 different values
        if word[-1] == '"' or word[-1] == '”' or word[-1] == '“':
            return True
    return False
# ======================================================================================================================
# get the number value from the parameter
# ======================================================================================================================
def get_number_value( number_value ):

    if isinstance(number_value, int):
        number = number_value
    elif isinstance(number_value, float):
        number = number_value
    elif isinstance(number_value, str):
        if number_value.isdecimal():
            number = int(number_value)
        elif isfloat(number_value):
            number = float(number_value)
        else:
            number = None
    else:
        number = None
    return number

# ======================================================================================================================
# Given a string, find occurrences of words in string and return the word to follow
# For example: in the string: "sql aiops format= json" given ["format", "="] return json
# ======================================================================================================================
def find_next_word(data_str, start_offset, end_offset, words_list):

    first_word = words_list[0]

    offset = start_offset

    word = ""

    while (offset + len(first_word)) < end_offset:
        offset = data_str.find(first_word, offset, end_offset)
        if offset == -1:
            break
        offset += len(first_word)
        found = True
        for next_word in  words_list[1:]:
            index = data_str.find(next_word, offset, end_offset)
            if index == -1:
                found = False
                break       # next word not found
            for i in range(offset, index):
                if data_str[i] != ' ':
                    found = False
                    break       # next_word is not consecutive
            offset += len(next_word) + (index - offset)     # Skeep spaces + size of next word

        if found:
            word = get_word_after(data_str, offset)
            if word:
                break
    return word

# ======================================================================================================================
# Given a string with x = y, validate x = and return y
# ======================================================================================================================
def get_equal_value(test_str, key):

    entry = test_str.strip()
    key_len = len(key)

    value = None
    if entry[:key_len] == key:
        subentry = entry[key_len:].lstrip()
        if len(subentry) and subentry[0] == '=':
            subentry = subentry[1:].lstrip()
            if len(subentry):
                value = subentry
    return value

# =======================================================================================================================
# Test Unix timestamp, representing the number of seconds that have elapsed since the Unix epoch (January 1, 1970, 00:00:00 UTC). T
# =======================================================================================================================
def is_valid_timestamp(timestamp):
    try:
        datetime.utcfromtimestamp(int(timestamp))
        return True
    except:
        return False