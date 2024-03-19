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

# process instructions declared in a JSON format

import edge_lake.generic.utils_python as utils_python
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.params as params

compiled_code = {}

# ---------------------------------------------------------------
# JSON attribute name transformation to Table column name
# 1) JSON attribute name may be different than Table column name
# 2) Test data type
# 3) May have default value
#
# Example:
#   "attributes": {
#      "device_name": {
#         "name": "device",
#         "type": "int",
#         "default": "0"
#      }
#   }
# ---------------------------------------------------------------
def mapping_attr_to_col(status, rules, attr_name, attr_val):
    new_val = attr_val

    # Test if column name is different than the JSON attribute Name
    if 'name' in rules.keys():
        # Instruction to change the column name
        new_name = rules['name']
    else:
        new_name = attr_name

    # Test the JSON value against the rules
    if 'type' in rules.keys():

        data_type = rules['type']
        if 'default' in rules.keys():
            default = rules['default']
            if not default.isnumeric():
                default = "0"  # the default does not satisfy the data type
        else:
            default = "0"

        if data_type == "int":
            # change to INT or set the default (or 0 with no default)
            try:
                float_val = float(attr_val)
                new_val = int(float_val)
            except:
                new_val = default

    return [new_name, new_val]


# ---------------------------------------------------------------
# Go over a list of rules that will map attributes to columns
# ---------------------------------------------------------------
def rules_attr_to_col(status, rules, attr_name, attr_val):
    new_name = attr_name
    new_value = attr_val
    ret_val = True
    for rule in rules:

        ret_val, reply_val, new_name, new_value = process_one_rule(status, True, rule, attr_name, attr_val)
        if not ret_val:
            break
        if reply_val:
            # An if condition was executed and returned True -> the function changing the value was applied
            break

    return [ret_val, new_name, new_value]


# ---------------------------------------------------------------
# Process one rule - a single stmt or if then stmt
# ---------------------------------------------------------------
def process_one_rule(status, compile_code, source_rule, name, value):
    ret_val = True
    new_name = name
    new_value = value

    rule = params.apply_dictionary(source_rule, 0)

    if rule.startswith("if "):

        index = rule.find(" then ", 4)
        if index == -1:
            if_status = 1  # Only if statement
            test_str = rule[3:].strip()
        else:
            if_status = 2  # if - then statement
            test_str = rule[3:index].strip()
    else:
        if_status = 0  # Not an "if" statement
        test_str = rule.strip()

    # if this code was executed - take the compiled code
    if compile_code:
        code = get_compiled_code(status, test_str)
    else:
        code = test_str
    if not code:
        ret_val = False  # failed to compile
    else:
        # Execute the the code
        ret_code, reply_val = utils_python.exec_eval(status, code, name, value, test_str)
        if ret_code:
            ret_val = False  # Python call failed
        else:
            if if_status == 2:
                if reply_val:
                    # if statement returned True --> execute the then
                    exec_string = rule[index + 6:].strip()
                    ret_val, new_name, new_value = process_string(status, compile_code, exec_string, new_name,
                                                                  new_value)

    return [ret_val, reply_val, new_name, new_value]


# ---------------------------------------------------------------
# Execute instruction
# ---------------------------------------------------------------
def process_string(status, compile_code, exec_string, name, value):
    ret_val = False
    new_name = name
    new_value = value
    if exec_string.startswith("value"):
        # apply instructions on the value
        string_length = 5
    elif exec_string.startswith("name"):
        string_length = 4
    else:
        string_length = 0

    if string_length:
        index = utils_data.find_non_space_offset(exec_string, string_length)
        if index != -1:
            if exec_string[index] == '=':
                index += 1
                command = exec_string[index:].strip()
                if compile_code:
                    code = get_compiled_code(status, command)
                else:
                    code = command
                if code:
                    ret_code, reply_val = utils_python.exec_eval(status, code, name, value)
                    if not ret_code:
                        if string_length == 5:
                            # update value
                            new_value = str(reply_val)
                        else:
                            new_name = str(reply_val)
                        ret_val = True

    return [ret_val, new_name, new_value]


# ---------------------------------------------------------------
# Get compiled code - first test if code is in dictionary
# Or compile and place in dictionary
# ---------------------------------------------------------------
def get_compiled_code(status, code_str):
    if code_str in compiled_code.keys():
        code = compiled_code[code_str]
    else:
        # compile and save code
        code = utils_python.get_compiled(status, code_str)
        if code:
            compiled_code[code_str] = code  # not failed to compile
    return code
