"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import sys

import edge_lake.generic.process_status as process_status
import edge_lake.generic.params as params
import datetime  # may be needed to user calls


# -----------------------------------------------------------
# Connect the AnyLog scripts with Python
# -----------------------------------------------------------

# -----------------------------------------------------------
# Make an array of commands to execute
# -----------------------------------------------------------
def execute_on_data(status, python_cmds, data):
    for entry in python_cmds:
        if entry.isalnum():
            params.add_param(entry, data)  # A name entry is the key in the dictionary assigned with the data
            new_string = data
            ret_val = process_status.SUCCESS
        else:
            assign = False
            index = entry.find("=")
            if index > 0:
                # do assignment
                key = entry[:index].strip()
                if key.isalnum():
                    assign = True
                    index += 1
            else:
                index = 0  # -1 is changed to 0
            ret_val, new_string = al_python(status, entry[index:])  # Execute python on AnyLog variables
            if assign:
                # save result
                params.add_param(key, new_string)
            if ret_val:
                break

    return [ret_val, new_string]


# -----------------------------------------------------------
# Make an array of commands to execute
# this pre-process the commands provided by the user
# {command A} {command B} {command C} {command D}  --> [command A, command B ...]
# -----------------------------------------------------------
def make_cmd_array(status, user_code):
    commands = []

    if user_code[0] == '{' and user_code[-1] == '}':
        code_str = user_code[1:-1]
        offset_start = 0
        offset_end = 0
        code_len = len(code_str)
        while offset_start < code_len:
            offset_start = code_str[offset_start:].find('{')
            if offset_start == -1:
                # no more commands identified by brackets
                if offset_end < code_len:
                    if code_str[offset_end:].strip():
                        # extra commands which are not recognized
                        status.add_error("Unrecognized structure in user python command")
                        return [process_status.ERR_command_struct, commands]
                break
            offset_start += offset_end
            offset_end = code_str[offset_start:].find('}')
            if offset_end == -1:
                status.add_error("Missing brackets in user python command")
                return [process_status.ERR_command_struct, commands]
            cmd = code_str[offset_start + 1:offset_start + offset_end].strip()
            commands.append(cmd)
            offset_end += (offset_start + 1)
            offset_start = offset_end
    else:
        commands.append(user_code)

    return [process_status.SUCCESS, commands]


# -----------------------------------------------------------
# Call python with AnyLog Data and vars
# Examples: sensor_data_file = ~/data/test.0000_00_00_00_00_00.abcd.table_name.json
# dbms_name = python !sensor_data_file.split("/")[-1].split(".")[0]
# !dbms_name --> table_name
# -----------------------------------------------------------
def al_python(status, pre_string):
    cmd_string = params.make_python_command(pre_string)  # create a sting that can be processed using eval

    ret_val, reply_val = exec_eval(status, cmd_string, None, None, None)
    if not ret_val:
        new_string = str(reply_val)
    else:
        new_string = ""

    return [ret_val, new_string]


# -----------------------------------------------------------
# Run eval with local and global variables
# -----------------------------------------------------------
def exec_eval(status, python_cmd, name, value, not_compiled):
    local_vars = {}

    if name:
        # modify or reference
        local_name = name
        local_vars["name"] = local_name

    if value:
        local_value = value
        local_vars["value"] = local_value

    formated_cmd = python_cmd.replace('\\','/')     # the '\\' char is treated as prefix to the following char: \\N -> returns an error as \N

    try:
        if not local_vars:
            reply_val = eval(formated_cmd)
        else:
            reply_val = eval(formated_cmd, {}, local_vars)
    except Exception as e:
        reply_val = None
        if not_compiled:
            # if python_cmd is compiled - place the non-compiled command in the error message
            status.add_error("Failed to process compiled string: '%s' with error: '%s'" % (not_compiled, str(e)))
        else:
            status.add_error("Failed to process string: '%s' with error: '%s'" % (formated_cmd, str(e)))
        ret_val = process_status.Failed_python_call
    else:
        ret_val = process_status.SUCCESS

    return [ret_val, reply_val]  # add name and value which coold be modified


# -----------------------------------------------------------
# Get a compiled version of the code
# https://realpython.com/python-eval-function/#using-pythons-eval-with-input
# -----------------------------------------------------------
def get_compiled(status, python_src):
    try:
        code = compile(python_src, "<string>", "eval")
    except:
        status.add_error("Failed to compile python code: %s" % python_src)
        code = None
    return code
