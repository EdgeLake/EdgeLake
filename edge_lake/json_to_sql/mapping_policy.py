"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import json
import re

try:
    import base64
except:
    base64_installed_ = False
else:
    base64_installed_ = True

try:
    import cv2
except:
    cv2_installed_ = False
else:
    cv2_installed_ = True

try:
    import numpy
except:
    numpy_installed_ = False
else:
    numpy_installed_ = True

try:
    import ast
except:
    ast_installed_ = False
else:
    ast_installed_ = True



import sys
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_columns as utils_columns
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.params as params
import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_io as utils_io
import edge_lake.generic.trace_func as trace_func
import edge_lake.dbms.partitions as partitions
import edge_lake.dbms.db_info as db_info
import edge_lake.cmd.member_cmd as member_cmd


excluded_defaults_ = {
    "default" : "Reserved word",
}

re_group_pattern_ = r're\.group\((\d+)\)'  # Define the regular expression pattern

# ==================================================================
#
# Get columns list from the mapping policy
# Update a dictionary:
#           with column type as f(column name)
# ==================================================================
def policy_to_columns_dict(status, dbms_name, table_name, instruct, columns):
    '''
    instruct - the mapping policy
    columns - a dictionary to be updated with the info
    '''

    if not "mapping" in instruct or not isinstance(instruct["mapping"], dict):
        if "id" in instruct:
            policy_id = instruct["id"]
        else:
            policy_id = ""
        status.add_error("Policy provided '%s' is not of type 'mapping'" % (policy_id))
        return process_status.ERR_wrong_json_structure

    policy_inner = instruct["mapping"]

    if not "schema" in policy_inner or not isinstance(policy_inner["schema"], dict):
        if "id" in instruct:
            policy_id = instruct["id"]
        else:
            policy_id = ""

        status.add_error("Missing 'schema' in mapping policy '%s'" % (policy_id))
        return process_status.ERR_wrong_json_structure


    ret_val = process_status.SUCCESS

    schema = policy_inner["schema"]

    for column_name, column_data in schema.items():

        if isinstance(column_data, dict):
            column_list = [column_data]  # Make a list with one attribute
        elif isinstance(column_data, list):
            # Multiple dictionaries in the list
            column_list = column_data
        else:
            policy_id = policy_inner["id"] if "id" in policy_inner else ""
            status.add_error("Wrong column info in 'schema' in mapping policy '%s'" % (policy_id))
            ret_val = process_status.ERR_wrong_json_structure
            break

        for column_info in column_list:     # May have multiple options

            if "type" in column_info:
                if "dbms" in column_info and column_info["dbms"] != dbms_name:
                    continue               # Incorrect option, get to the next option
                if "table" in column_info and column_info["table"] != table_name:
                    continue               # Incorrect option, get to the next option
                data_type = column_info["type"].lower()

                if "default" in column_info.keys():
                    default_value = column_info["default"]
                    if default_value:
                        if default_value in excluded_defaults_:
                            status.add_error(f"Policy default value {default_value} is not allowed: {excluded_defaults_[default_value]}")
                            ret_val = process_status.ERR_command_struct
                            break
                        else:
                            if utils_data.is_quotation_required(data_type, default_value):
                                # Add quotation - if not exists
                                data_type = "%s NOT NULL DEFAULT '%s'" % (data_type, default_value)
                            else:
                                data_type = "%s NOT NULL DEFAULT %s" % (data_type, default_value)

                ret_val, unified_data_type = utils_data.unify_data_type( status, data_type )
                if ret_val:
                    break

                columns[column_name] = unified_data_type.upper()
                break           # Data type that satisfies the dbms and table was found

            if ret_val:
                break
        if ret_val:
            break

    return ret_val

# ----------------------------------------------------------------------------------------
# Map the source data by the policy schema
# ----------------------------------------------------------------------------------------
def apply_policy_schema(status, source_dbms, source_table, policy_inner, policy_id, json_data, is_dest_string, blobs_dir):
    '''
    status - status object
    source_dbms - used to extract the schema if the table is not declared
    source_table - used to extract the schema if the table is not declared
    policy_inner - a dictionary with an attribute "schema"
    policy_id - the ID of the policy
    json_data - the source data organized as a list
    is_dest_string - if True, returns a list of JSON strings, if False, returns a list of JSON objects
    blobs_dir - the directory to write the blob data
    '''

    global tables_schemas_

    # Provided by the caller
    dbms_name = source_dbms
    table_name = source_table

    working_policy = policy_inner       # this policy may change using the import policies

    if blobs_dir and blobs_dir[-1] != params.get_path_separator():
        blobs_dir += params.get_path_separator()

    data_source = "0"
    insert_list = None


    if "source" in working_policy:
        # Get the data source - i.e of a device generating the data (data source is used in the file name)
        source_info = working_policy["source"]
        ret_val, data_source = bring_and_default_to_data(status, source_info, policy_id, "source", json_data)
        if not data_source or not isinstance(data_source, str):
            data_source = '0'
        else:
            data_source = data_source.lower().replace(' ', '_') # Make lower case + no spaces
    else:
        ret_val = process_status.SUCCESS


    if not ret_val:
        # A key (from the schema) to pull a list of readings from the message
        ret_val, readings_key = get_policy_info(status, working_policy, policy_id, "readings", False)
        if ret_val or not readings_key:
            # there is no list, just the json msg
            if isinstance(json_data, list):
                readings = json_data
            else:
                readings = [json_data]
            ret_val = process_status.SUCCESS        # The entire JSON representa a reading
        else:
            if not readings_key in json_data:
                status.add_error("Missing '%s' in mapping policy '%s'" % (readings_key, policy_id))
                ret_val = process_status.ERR_wrong_json_structure
            else:
                readings = json_data[readings_key]
                if not isinstance(readings, list):
                    readings = [readings]       # May have a single dictionary - not in a list -Make a list with a single entry

    if not ret_val:

        if not len(readings):
            # Empty readings
            readings = [{}]     # This setup would loop over the schema as an entry can be created from the root of the readings


        while (True):
            if not "schema" in working_policy or not isinstance(working_policy["schema"], dict):
                status.add_error("Missing 'schema' in mapping policy '%s'" % (policy_id))
                ret_val = process_status.ERR_wrong_json_structure
                break
            else:
                schema = working_policy["schema"]

            insert_list = []  # Make a list of rows from the topic info
            # Use a different policy and restart

            if not dbms_name:

                # provided by the policy
                # Get dbms name if specified in the policy or from the JSON data using bring command
                ret_val, value = get_value_by_key(status, "dbms", policy_inner, json_data, policy_id)
                if ret_val:
                    break
                if value:
                    dbms_name = utils_data.reset_str_chars(value)

            if not table_name:
                # Get table name if specified in the policy or from the JSON data using bring command


                ret_val, value = get_value_by_key(status, "table", policy_inner, json_data, policy_id)
                if ret_val:
                    break
                if value:
                    table_name = utils_data.reset_str_chars(value)

            if not dbms_name or not table_name:
                err_str = "DBMS" if not dbms_name else "Table"
                status.add_error(f"Missing {err_str} in mapping policy '{policy_id}'")
                ret_val = process_status.ERR_wrong_json_structure
                break

            if partitions.is_partitioned(dbms_name, table_name):
                par_info = partitions.get_par_info(dbms_name, table_name)
                column_name = par_info[2]  # The name of the column that makes the partition
                if column_name != "insert_timestamp":
                    # insert_timestamp is added during the mapping of JSON to Insert and does not need to be included on the policy
                    if not column_name in schema:
                        status.add_error(f"Policy '{policy_id}' for table '{dbms_name}.{table_name}' is missing partitioned column: '{column_name}'")
                        ret_val = process_status.Wrong_policy_structure
                        break



            file_name_prefix = f"{dbms_name}.{table_name}."


            ret_val, working_policy = process_event(status, readings, working_policy, policy_id, dbms_name, table_name, file_name_prefix, blobs_dir, is_dest_string, json_data, insert_list)
            if ret_val != process_status.CHANGE_POLICY:
                break

    if trace_func.get_func_trace_level("mapping"):
        # Show the process output (the mapped data)
        show_insert_list(ret_val, insert_list)


    return [ret_val, dbms_name, table_name, data_source, insert_list]   # dbms name and table name can be determined by the policy

# ----------------------------------------------------------------------
# Process an Event - go over all the attributes of the schema and apply logic on the event (row)
# ----------------------------------------------------------------------
def process_event(status, readings, policy_inner, policy_id, dbms_name, table_name, file_name_prefix, blobs_dir, is_dest_string, json_data, insert_list):

    working_policy = None

    if not "schema" in policy_inner:
        status.add_error(f"Missing 'schema' in policy {policy_id}")
        return [process_status.Wrong_policy_structure, None]

    schema = policy_inner["schema"]

    secondary_index = 0                # Counter if * is used to add all columns
    for index, data_entry in enumerate(readings):  # data_entry is one reading from a list of readings

        for attr_name, attr_data in schema.items():
            # Go over all the columns in the schema and extract the data

            if attr_name == '*':
                # bring all columns
                if isinstance(attr_data, dict) and "bring" in attr_data:
                    bring_list = attr_data["bring"]
                    if isinstance(bring_list, list):
                        # Bring all the columns
                        for bring_key in bring_list:
                            # Get all columns in the root level or in a subtree rooted by the values in the list.
                            # Example:
                            # "schema" : {
                            #           "*": {
                            #               "type": "*",
                            #               "bring": ["*"]  # Bring can be #    "bring": ["fields", "tags"]
                            #                }
                            if bring_key == '*' or (isinstance(data_entry, dict) and bring_key in data_entry):
                                if bring_key == '*':
                                    source_columns = data_entry
                                else:
                                    source_columns = data_entry[bring_key]   # The attribute values to include
                                if isinstance(source_columns, str) and len(source_columns):
                                    input_columns = utils_json.str_to_json(source_columns)
                                else:
                                    input_columns = source_columns

                                if isinstance(input_columns, dict):
                                    for attr_name, column_val in input_columns.items():
                                        data_type = type(column_val).__name__
                                        ret_val, unified_data_type = utils_data.unify_data_type(status, data_type)
                                        if ret_val:
                                            break

                                        # add column to the relational table
                                        col_str = attr_name if bring_key == '*' else  f"{bring_key}_{attr_name}"    # Extend the name if pulled from a sub key
                                        column_name = utils_data.reset_str_chars(col_str)
                                        ret_val = add_column_to_list(status, insert_list, index + secondary_index,column_name, unified_data_type, column_val, None, is_dest_string)
                                        if ret_val:
                                            break

                            if ret_val:
                                break
                if ret_val:
                    break
                secondary_index += 1  # Count rows added by *
                continue


            if isinstance(attr_data, dict):
                attr_list = [attr_data]  # Make a list with one attribute
            elif isinstance(attr_data, list):
                # Multiple dictionaries in the list
                attr_list = attr_data
            else:
                status.add_error("Wrong column info in 'schema' in mapping policy '%s'" % (policy_id))
                ret_val = process_status.ERR_wrong_json_structure
                break

            is_apply = 0
            for column_info in attr_list:
                # Because of condition, could have multiple column infos for a single attr_name
                if is_apply:
                    # The if condition returned True and the column_info was processed
                    break
                if "dbms" in column_info and column_info["dbms"] != dbms_name:
                    continue  # Incorrect option, get to the next option
                if "table" in column_info and column_info["table"] != table_name:
                    continue  # Incorrect option, get to the next option

                if not isinstance(column_info, dict):
                    status.add_error("Schema info is not in a dictionary format in Mapping Policy '%s'" % (policy_id))
                    ret_val = process_status.ERR_wrong_json_structure
                    break

                if "script" in column_info:
                    reply_code, info_if = process_if_code(status, column_info, policy_id, "script", data_entry)
                    if reply_code:
                        if reply_code == process_status.IGNORE_ATTRIBUTE:
                            # if ... then skip attribute - Skip this column
                            continue
                        if reply_code == process_status.IGNORE_EVENT:
                            # if ... then skip event - Skip this JSON
                            ret_val = process_status.SUCCESS
                            break  # Get next JSON
                        if reply_code == process_status.CHANGE_POLICY:
                            # Change the processing policy
                            if "import_" in working_policy:
                                import_dictionary = working_policy["import_"]
                                if isinstance(import_dictionary, dict) and info_if in import_dictionary:
                                    working_policy = import_dictionary[info_if]  # Change the policy and restart
                                    break

                        ret_val = reply_code  # An error
                        schema["__error__"] = f"Failed processing of policy: '{policy_id}' 'script' of attribute: '{attr_name}' returned: '{process_status.get_status_text(ret_val)}'"
                        break

                ret_val, data_type = get_policy_info(status, column_info, policy_id, "type", True)
                if ret_val:
                    schema["__error__"] = f"Failed processing of policy: '{policy_id}' 'type' from attribute: '{attr_name}' returned: '{process_status.get_status_text(ret_val)}'"
                    break

                source_attr_name = attr_name
                if "value" in column_info:
                    ret_val, bring_cmd = get_policy_info(status, column_info, policy_id, "value", True)
                    if ret_val:
                        schema["__error__"] = f"Failed processing of policy: '{policy_id}' 'value' from attribute: '{attr_name}' returned: '{process_status.get_status_text(ret_val)}'"
                        break

                    ret_val, bring_key = get_bring_key(status, bring_cmd, policy_id)
                    if ret_val:
                        break
                    if bring_key:
                        # Is a bring command - change source_attr_name
                        source_attr_name = bring_key


                if "root" in column_info and isinstance(column_info["root"], bool) and column_info["root"] == True:
                    # Take the info from a different the root (rather than the reading instance)
                    ret_val, attr_val = bring_and_default_to_data(status, column_info, policy_id, source_attr_name,
                                                                  json_data)
                else:
                    ret_val, attr_val = bring_and_default_to_data(status, column_info, policy_id, source_attr_name,
                                                                  data_entry)

                if ret_val:
                    break

                if "blob" in column_info and isinstance(column_info["blob"], bool) and column_info["blob"] == True:
                    # This is a file/blob column

                    # 1) Write the blob in the blobs dir
                    # 2) Calculate the hash value to use as the file name + place on the SQL table if column
                    #    includes:     hash = true

                    if not blobs_dir:
                        status.add_error(f"Missing blob dir definition when policy '{policy_id}' is processed")
                        ret_val = process_status.Blobs_dir_does_not_exists
                        break

                    ret_val, hash_type = get_policy_info(status, column_info, policy_id, "hash", True)
                    if ret_val:
                        break
                    if isinstance(hash_type, str) and hash_type == "md5":
                        #  - the hash value string is added to the relational table

                        hash_value = utils_data.get_string_hash('md5', attr_val, dbms_name + '.' + table_name)
                    else:
                        status.add_error(
                            f"Policy '{policy_id}' includes Hash Type '{hash_type}' which is not recognized")
                        ret_val = process_status.Wrong_policy_structure
                        break

                    if "extension" in column_info and isinstance(column_info["extension"], str):
                        extension = column_info["extension"]  # The file type (added to the hash value)
                    else:
                        extension = None

                    if "apply" in column_info:
                        apply = column_info["apply"]  # Apply encoding or decoding
                    else:
                        apply = None

                    ret_val, column_val = blob_to_file(status, file_name_prefix, hash_value, attr_val, blobs_dir,
                                                       extension, apply)
                    if ret_val:
                        break

                    policy_inner["bwatch_dir"] = True

                else:
                    if "apply" in column_info:
                        # Apply a function on the value
                        ret_val, column_val = get_applied_val(status, column_info["apply"], attr_val)
                        if ret_val:
                            break
                    else:
                        column_val = attr_val

                ret_val, unified_data_type = utils_data.unify_data_type(status, data_type)
                if ret_val:
                    break

                # add column to the relational table
                ret_val = add_column_to_list(status, insert_list, index + secondary_index, attr_name, unified_data_type, column_val, None, is_dest_string)
                if ret_val:
                    break

            if ret_val:
                break

        if ret_val:
            break

    return [ret_val, working_policy]


# ----------------------------------------------------------------------
# Get value from a JSON ENtry by a key, consider bring command
# ----------------------------------------------------------------------
def get_value_by_key(status, key, json_policy, json_data, policy_id):

    if key in json_policy:
        ret_str = json_policy[key]
        if len(ret_str) >= 8 and ret_str[:5] == "bring" and (ret_str[5] == ' ' or ret_str[5] == '['):
            cmd_words, left_brackets, right_brakets = utils_data.cmd_line_to_list_with_json(status, ret_str, 0, 0)  # a list with words in command line
            ret_val, value = member_cmd.process_bring(status, [json_data], cmd_words, 1, 0)
        else:
            ret_val = process_status.SUCCESS  # No such attr
            value = ret_str
    else:
        ret_val = process_status.SUCCESS        # No such attr
        value = None

    return [ret_val, value]

# ----------------------------------------------------------------------
# Test if bring command, if True - return the bring key
# 'bring [Operation]' --> return Operation, otherwise, return NULL
# ----------------------------------------------------------------------
def get_bring_key(status, bring_string, policy_id):
    ret_val = process_status.SUCCESS
    if len(bring_string) >= 8 and bring_string[:5] == "bring" and (bring_string[5] == ' ' or bring_string[5] == '['):
        # find the key in the source data
        bring_cmd = bring_string[5:].strip()
        if len(bring_cmd) >= 3 and bring_cmd[0] == '[' and bring_cmd[-1] == ']':
            bring_key = bring_cmd[1:-1]  # Get the attribute name from the bring command
        else:
            # Error in bring command
            status.add_error(f"Wrong BRING command in value associated with attribute {bring_string} in mapping policy {policy_id}")
            ret_val = process_status.ERR_wrong_json_structure
            bring_key = None
    else:
        # Not a bring command
        bring_key = None
    return [ret_val, bring_key]
# ----------------------------------------------------------------------
# Show the insert list if user specified:  trace level = 1 mapping
# ----------------------------------------------------------------------
def show_insert_list(ret_val, insert_list):
    # Show the process output (the mapped data)
    if ret_val:
        utils_print.output("\nMapping failed with error %u: %s" % (ret_val, process_status.get_status_text(ret_val)),
                           True)
    else:
        for entry in insert_list:
            utils_print.struct_print(entry, True, True, 1000)


# ----------------------------------------------------------------------
# Write blob data to a file
# ----------------------------------------------------------------------
def blob_to_file(status, file_name_prefix, hash_value, blob_data, blobs_dir, file_type, apply):
    '''
    The file name (written on disk) includes a prefix of dbms+table+source+hash+type
    The hash+type are returned and written in the data row
    apply - can be base64decoding
    '''

    ret_val = process_status.SUCCESS
    if not blobs_dir:
        blob_file_name = None
        status.add_error("Missing blobs directory in global dictionary (!blobs_dir)")
        ret_val = process_status.Missing_key_in_dictionary
    else:
        # Write blob file
        blob_file_name = hash_value if not file_type else hash_value + '.' + file_type

        if apply:
            if apply == "base64decoding":
                if not base64_installed_:
                    status.add_error(f"Failed to import base64 library")
                    ret_val = process_status.Failed_to_import_lib
                else:
                    try:
                        base64_bytes = blob_data.encode('ascii')
                        blobs_info = base64.b64decode(base64_bytes)
                    except:
                        status.add_error(f"Failed to apply Base64 decoding on data")
                        ret_val = process_status.Failed_to_encode_data
            elif apply == "opencv":
                if not numpy_installed_:
                    status.add_error(f"Failed to import Numpy library")
                    ret_val = process_status.Failed_to_import_lib
                elif not cv2_installed_:
                    status.add_error(f"Failed to import OpenCV library")
                    ret_val = process_status.Failed_to_import_lib
                elif not ast_installed_:
                    status.add_error(f"Failed to import ast library")
                    ret_val = process_status.Failed_to_import_lib
                else:
                    try:
                        if not isinstance(blob_data, numpy.ndarray) or not isinstance(blob_data, list):
                            content = ast.literal_eval(blob_data)
                        else:
                            content = blob_data

                        if not isinstance(content, numpy.ndarray):
                            ndarry_content = numpy.array(content)  # convert back to numpy.ndarray
                        else:
                            ndarry_content = content
                    except:
                        status.add_error(f"Failed to apply OpenCV decoding on data")
                        ret_val = process_status.Failed_to_encode_data
            else:
                blobs_info = blob_data
        else:
            blobs_info = blob_data

        if not ret_val:
            file_path_name = blobs_dir + file_name_prefix + blob_file_name

            if apply == "opencv":
                try:
                    ret_code = cv2.imwrite(file_path_name, ndarry_content)
                except:
                    errno, value = sys.exc_info()[:2]
                    message = "Failed to write file: '%s' with error %s: %s" % (file_path_name, str(errno), str(value))
                    status.add_error(message)
                    ret_val = process_status.File_write_failed
                    ret_code = True     # Avoid the second error message below

            elif isinstance(blobs_info,bytes):
                ret_code = utils_io.write_data_block(file_path_name, True, blobs_info)
            else:
                ret_code = utils_io.write_str_to_file(status, blob_data, file_path_name)

            if not ret_code:
                status.add_error("Failed to write blob data to blobs dir at: %s" % blobs_dir + file_name_prefix + blob_file_name)
                ret_val = process_status.File_write_failed


    return [ret_val, blob_file_name]


# ----------------------------------------------------------------------
# Apply a function on the value
# ----------------------------------------------------------------------
def get_applied_val(status, applied_func, attr_val):


    ret_val = process_status.SUCCESS
    if attr_val:

        if applied_func == "epoch_to_datetime":
            # Convert Epoch to datetime
            if attr_val and attr_val.isdigit():
                value = utils_columns.epoch_to_date_time(attr_val)
            else:
                # Keep source value if not epoch
                value = attr_val
        elif applied_func == "json_dump":
            # Convert into a JSON-formatted string
            try:
                value = json.dumps(attr_val)
            except:
                value = None
        else:
            value = None
            message = f"Unrecognized mapping function '{applied_func}' on value: {str(attr_val)}"
            status.add_error(message)
            ret_val = process_status.Wrong_policy_structure

        if not value and ret_val == process_status.SUCCESS:
            message = f"Failed to apply mapping using function '{applied_func}' on value: {str(attr_val)}"
            status.add_error(message)
            ret_val = process_status.Wrong_policy_structure
    else:
        value = attr_val

    return [ret_val, value]
# ----------------------------------------------------------------------
# Use the bring command to get the attribute value. If the bring doesn't
# return data - use the default key.
# If bring is available - place the compiled bring in the policy for next time usage
# ----------------------------------------------------------------------
def bring_and_default_to_data(status, column_info, policy_id, attr_name, data_entry):
    attr_val = None
    ret_val, bring_cmd = utils_json.get_policy_val(status, column_info, policy_id, "bring", str, True, False)
    if not ret_val:
        if "compiled_bring" in column_info:
            # get the bring command which was already processed using utils_data.cmd_line_to_list_with_json
            bring_list = column_info["compiled_bring"]
        else:
            if not bring_cmd:
                bring_cmd = "[%s]" % attr_name  # Use the attribute name to bring the data

            bring_list, left_brackets, right_brakets = utils_data.cmd_line_to_list_with_json(status, bring_cmd, 0, 0)
            if left_brackets != right_brakets:
                status.add_error("Wrong bring command in column '%s' in mapping policy '%s'" % (attr_name, policy_id))
                ret_val = process_status.ERR_wrong_json_structure
            else:
                column_info["compiled_bring"] = bring_list  # save for next row

    if not ret_val:
        ret_val, attr_val = utils_json.pull_info(status, [data_entry], bring_list, None, 0)
        if not ret_val and attr_val == "":
            # Get the default
            data_type_name = column_info["type"] if "type" in column_info else None
            data_type = utils_columns.get_instance_by_name(data_type_name)
            ret_val, attr_val = utils_json.get_policy_val(status, column_info, policy_id, "default", data_type, True,  True)
            if not ret_val:
                if isinstance(attr_val, str) and attr_val == "now()":
                    attr_val = utils_columns.get_current_utc_time()

    return [ret_val, attr_val]
# ----------------------------------------------------------------------
# Process AnyLog code on the policy
# In the first call - analyze_if is called and stored on the policy such that it is available to reuse.
# ----------------------------------------------------------------------
def process_if_code(status, mapping, policy_id, key, json_msg):
    '''
    status - object status
    mapping - a dictionary with the if statement
    policy_id - the id of the policy with the if statement
    key - the key to retieve the if statement
    json_msg - The dictionary with the data to validate
    '''

    ret_val = process_status.SUCCESS
    info_if = ""

    compiled_key = "compiled_%s" % key

    if not compiled_key in mapping:

        non_compiled = mapping["script"]
        ret_val, compiled_list = member_cmd.compile_commands(status, non_compiled, policy_id)
        if not ret_val:
            mapping[compiled_key] = compiled_list  # save for next time
    else:
        compiled_list = mapping[compiled_key]     # Get the compiled script

    if not ret_val:
        io_buff_in =status.get_io_buff()        # Use the thread IO buffer

        for command_info in compiled_list:
            cmd_key, cmd_words, cmd_exec, offset_then, with_paren, conditions_list = command_info
            if offset_then:
                # This is an if statement
                next_word, ret_code = params.process_analyzed_if(status, cmd_words, 0, offset_then, with_paren, conditions_list, json_msg)
            else:
                ret_code = 1        # Not an if statement, process the cmd

            if ret_code == 1:
                if not cmd_key:
                    # Assignment
                    if not member_cmd.data_assignment(status, 2, cmd_exec):  # exit or dictionary assignments
                        status.add_error(f"Failed to process policy script: assignment process failed with '{cmd_exec}' with policy: '{policy_id}'")
                        ret_val = process_status.ERR_command_struct
                else:
                    ret_val = member_cmd.commands[cmd_key]['command'](status, io_buff_in, cmd_exec, 0)
                if ret_val:
                    # Including process_status.SKIP_ATTRIBUTE and process_status.SKIP_EVENT
                    if ret_val == process_status.IGNORE_SCRIPT:
                        ret_val = process_status.SUCCESS    # Only exit the script but continue processing
                    elif ret_val == process_status.CHANGE_POLICY:
                        info_if = cmd_exec[-1]  # Return the name of the new policy in the "import_" dictionary
                    break

    return [ret_val, info_if]

# ----------------------------------------------------------------------
# Get info from Policy
# Given a key - retrieve the value from the json_obj.
# ----------------------------------------------------------------------
def get_policy_info(status, json_obj, policy_id, key, must_exists):
    '''
    status - thread status object.
    json_obj - the data to process
    policy_id - the unique policy ID
    key - the key to apply on the json_obj
    must_exist - if True, an error code is return if the key is not represented in the json_obj
    '''

    if key in json_obj:
        ret_val = process_status.SUCCESS

        policy_value = json_obj[key]
        if isinstance(policy_value,str) or isinstance(policy_value,bool):
            value = policy_value
        elif isinstance(policy_value,dict):
            # get the BRING command
            ret_val, value = get_policy_info(status, policy_value, policy_id, "bring", True)
            if not ret_val and not isinstance(value,str):
                ret_val = process_status.ERR_wrong_json_structure
    else:
        ret_val = process_status.ERR_wrong_json_structure


    if ret_val:
        value = None
        if must_exists:
            status.add_error("Missing info for attribute '%s' in mapping policy '%s'" % (key, policy_id))


    return [ret_val, value]


# ----------------------------------------------------------------------
# Given a list of column values, add a new column value to the list
# Attr constant could be a repeatable date using the key now
# ----------------------------------------------------------------------
def add_column_to_list(status, insert_list, index, bring_attr_name, data_type, attr_val, attr_constant, is_dest_string):
    '''
    bring_attr_name is the name set in the bring command.
    If the brings pulls a dictionary with name and value, the attribute name is modified by the dictionary in
    the method get_formatted_val

    dest_string - if True, returns a list of JSON strings, if False, returns a list of JSON objects
    '''

    ret_val, column_val_str, dict_attr_name = get_formatted_val(status, data_type, attr_val)
    attr_name = dict_attr_name or bring_attr_name

    if not ret_val:
        if index >= len(insert_list):
            # A new entry to the list
            if is_dest_string:      # is_dest_string - if True, returns a list of JSON strings, if False, returns a list of JSON objects
                # Organize as a string
                if attr_constant:
                    insert_list.append(attr_constant + "\"" + attr_name + "\":" + column_val_str + "}")
                elif utils_data.is_add_quotation(data_type, column_val_str):
                    insert_list.append("{\"" + attr_name + "\":\"" + column_val_str + "\"}")    # added because of now()
                else:
                    insert_list.append( "{\"" + attr_name + "\":" + column_val_str + "}" )
            else:
                # Organize as object
                insert_list.append( { attr_name : column_val_str })
        else:
            if is_dest_string:      # is_dest_string - if True, returns a list of JSON strings, if False, returns a list of JSON objects
                if utils_data.is_add_quotation(data_type, column_val_str):
                    insert_list[index] = insert_list[index][:-1] + "," + "\"" + attr_name + "\":\"" + column_val_str + "\"}"
                else:
                    insert_list[index] = insert_list[index][:-1] + "," + "\"" + attr_name + "\":" + column_val_str + "}"
            else:
                insert_list[index] [attr_name] =  column_val_str

    return ret_val
# ----------------------------------------------------------------------
# Format the value string by the data type
# ----------------------------------------------------------------------
def get_formatted_val(status, data_type, pulled_val):
    ret_val = process_status.SUCCESS
    value_str = None
    time_str = None

    if isinstance(pulled_val,str) and len(pulled_val) > 3 and pulled_val[0] == '{' and pulled_val[-1] == '}':
        json_dict = utils_json.str_to_json(pulled_val)
        if json_dict:
            # a dictionary with a name - value pair
            attr_name = next(iter(json_dict))      # Get the name from the JSON dictionary
            attr_val = json_dict[attr_name]
            attr_name = utils_data.reset_str_chars(attr_name.lower())
        else:
            attr_name = None
            attr_val = pulled_val
    else:
        attr_name = None
        attr_val = pulled_val

    if data_type.startswith("varchar") or data_type == "uuid":
        if len (attr_val) < 2 or attr_val[0] != '"' or attr_val[-1] != '"':
            value_str = "\"%s\"" % attr_val
        else:
            value_str = attr_val        # Already formatted with quotation (json,dump was applied)
    elif data_type == "int":
        value_str = str(attr_val)
    elif data_type == "float":
        value_str = str(attr_val)
        if 'e' in value_str:
            # Change scientific notation
            if utils_data.isfloat(value_str):
                value_str = str(float(value_str))

    elif data_type == "timestamp":
        if isinstance(attr_val, str):
            if attr_val.isdecimal():
                time_val = int(attr_val)
            elif attr_val.isdigit():
                time_val = int(float(attr_val))
            else:
                time_val = None
                if len(attr_val) > 10 and attr_val[-3] == ':' and (attr_val[-6] == '+' or attr_val[-6] == '-'):
                    # Format like: '2011-11-04T00:05:23+04:00' - https://docs.python.org/3/library/datetime.html
                    time_str = utils_columns.time_iso_format(attr_val)
                else:
                    time_format =  utils_columns.get_utc_time_format(attr_val)
                    if utils_columns.validate_date_string(attr_val, time_format):
                        time_str = attr_val
                    else:
                        time_format = utils_columns.get_local_time_format(attr_val)
                        if utils_columns.validate_date_string(attr_val, time_format):
                            time_str = attr_val
                        else:
                            time_str = None

        elif isinstance(attr_val, int):
            time_val = attr_val
        elif isinstance(attr_val, float):
            time_val = int(attr_val)
        else:
            time_val = None

        if time_val:
            # Map second to time string
            seconds = int(time_val/1000)
            time_str = utils_columns.seconds_to_date(seconds)


        if not time_str:
            status.add_error("MQTT failure: Failed to retrieve date and time from message attribute value: %s" % str(attr_val))
            ret_val = process_status.MQTT_info_err
        else:
            value_str = time_str
    elif data_type == "bool":
        value_str = str(attr_val).lower()
    else:
        value_str = str(attr_val)

    return [ret_val, value_str, attr_name]

# ----------------------------------------------------------------------
# Validate the policy structure
# ----------------------------------------------------------------------
def validate(status, policy):

    ret_val = process_status.ERR_wrong_json_structure
    if not isinstance(policy,dict):
        status.add_error("Error in policy structure")
    elif not "mapping" in policy:
        status.add_error("Mapping policy does not include the key 'mapping' in the root")
    else:
        policy_inner = policy["mapping"]
        if not "id" in policy_inner:
            status.add_error("Mapping policy without an ID")
        elif not "schema" in policy_inner:
            status.add_error("Mapping policy without a schema")
        else:
            ret_val = process_status.SUCCESS

    return ret_val


# ------------------------------------------------------------------------------
# Archive blob fIle
# Write to a DBMS, move file to archive directory
# ------------------------------------------------------------------------------
def archive_blob_file(status, dbms_name, table_name, blob_data):

    blobs_dir = params.get_value_if_available("!blobs_dir")
    err_dir = params.get_value_if_available("!err_dir")
    blob_file_name = ""
    if not blobs_dir or not err_dir:
        if not blobs_dir:
            status.add_error("Missing blobs directory in global dictionary (!blobs_dir)")
        else:
            status.add_error("Missing error directory in global dictionary (!err_dir)")
        ret_val = process_status.Missing_key_in_dictionary
    else:
        if blobs_dir[-1] != params.get_path_separator():
            blobs_dir += params.get_path_separator()
        if err_dir[-1] != params.get_path_separator():
            err_dir += params.get_path_separator()


        file_name_prefix = f"{dbms_name}.{table_name}."
        blob_hash_value = utils_data.get_string_hash('md5', blob_data, dbms_name + '.' + table_name)
        # Write the blob to a blob dir and return the ID of the blob
        ret_val, blob_file_name = blob_to_file(status, file_name_prefix, blob_hash_value, blob_data, blobs_dir,
                                                            "blob", None)
        if not ret_val:
            # Insert the Blob to a DBMS
            utc_time = utils_columns.get_current_utc_time("%Y-%m-%dT%H:%M:%S.%fZ")
            date_time_key = utils_io.utc_timestamp_to_key(utc_time)
            db_info.store_file(status, "blobs_" + dbms_name, table_name, blobs_dir,
                                         blob_hash_value + '.blob', blob_hash_value, date_time_key[:6], True, True, 0)

            file_id_name = file_name_prefix + blob_file_name  # database + table + source + name

            absolute_name = blobs_dir + file_id_name

            # Move the file to archive + compress -> if archiving fails - move to err_dir
            ret_val = utils_io.archive_file(status, "*", blobs_dir, err_dir, absolute_name, False, date_time_key)
            if ret_val:
                status.add_keep_error("Failed to archive blob '%s.%s' " % (blobs_dir, file_id_name))

    return [ret_val, blob_file_name]

# ------------------------------------------------------------------------------
# Add columns to the JSON row
# ------------------------------------------------------------------------------
def get_new_row(status, current_time, dbms_name, table_name, policy_inner, policy_id, json_data, re_match):
    '''
    policy_inner - the policy without the type (mapping or transform)
    policy_id - the ID of the policy used
    json_data - the user data (i.e. PLC data)
    json_row - the new row to update
    re_match - match used to retrieve the column name
    '''

    ret_val = process_status.SUCCESS
    if not "schema" in policy_inner:
        is_new, row_to_update = get_table_row(status, current_time, policy_inner["__new_rows_from_plc__"], dbms_name, table_name, None)
    else:
        schema = policy_inner["schema"]
        if "id" in schema:
            # The ID column is tested first as it creates the key that hosts the row
            # This ID is used to create a row instance when multiple rows of the same table are generated from a plc pannel
            attr_data = schema["id"]
            ret_val, not_used, attr_val = get_name_val(status, policy_id, json_data, attr_data, re_match)
            with_id = True
        else:
            attr_val = False
            with_id = False

        is_new, row_to_update = get_table_row(status, current_time, policy_inner["__new_rows_from_plc__"], dbms_name, table_name, attr_val)

        if is_new:
            if with_id:
                row_to_update["id"] = attr_val

            for attr_name, attr_data in schema.items():
                # Update the columns from the Schema
                if attr_name == "id":
                    continue        # Was processed

                ret_val, not_used, attr_val = get_name_val(status, policy_id, json_data, attr_data, re_match)

                if ret_val:
                    break

                row_to_update[attr_name] = attr_val


    return [ret_val, row_to_update]
#----------------------------------------------------------------------
# Get the row being updated for this table.
# The rows are maintained in the policy as f(dbms and table)
# The structure of the new rows in the policy:
# "new_rows_from_plc" --> dictionary as f(dbms.table) -> dict to row and time
#----------------------------------------------------------------------
def get_table_row(status, current_time, new_rows, dbms_name, table_name, row_id):
    '''
    current_time - the current time on the node
    new_rows - a dict to maintain all new rows as f (dbms and table)
    dbms_name - dbms assigned to the column considered
    table_name - table assigned to the column considered
    row_id - when a PLCs generates multiple rows to the same table - like: generator_1, generator_2 eyc
    '''

    table_id = f"{dbms_name}.{table_name}.{row_id}" if row_id else f"{dbms_name}.{table_name}"

    if table_id in new_rows:
        row_to_update = new_rows[table_id]
        is_new = False      # this row was created in a previous itteration
    else:
        # Create a new row
        row_to_update = {}
        new_rows[table_id] = row_to_update
        is_new = True

    return [is_new, row_to_update]

# ------------------------------------------------------------------------------
# Get Attr name + Attr value from policy and the user data
# ------------------------------------------------------------------------------
def get_name_val(status, policy_id, json_data, attr_data, re_match):

    attr_name = None        # The derived name
    attr_val = None

    if not isinstance(attr_data, dict):
        status.add_error("Schema info is not in a dictionary format in Transform Policy '%s'" % (policy_id))
        ret_val = process_status.ERR_wrong_json_structure
    else:

        ret_val, data_type = get_policy_info(status, attr_data, policy_id, "type", True)
        if ret_val:
            status.add_error(f"Failed processing of policy: '{policy_id}' 'type' from attribute: '{attr_name}' returned: '{process_status.get_status_text(ret_val)}")
            ret_val = process_status.ERR_wrong_json_structure
        else:

            attr_val = None
            source_attr_name = attr_name
            if "value" in attr_data:
                ret_val, bring_cmd = get_policy_info(status, attr_data, policy_id, "value", True)
                if ret_val:
                    status.add_error(f"Failed processing of policy: '{policy_id}' 'value' from attribute: '{attr_name}' returned: '{process_status.get_status_text(ret_val)}")
                    ret_val = process_status.ERR_wrong_json_structure

                if not ret_val:

                    ret_val, bring_key = get_bring_key(status, bring_cmd, policy_id)
                    if not ret_val:

                        if bring_key:
                            # Is a bring command - change source_attr_name
                            source_attr_name = bring_key
                        else:
                            # Get the value from the column name (used to map PLCs)
                            attr_val = get_re_match_value(status, re_match, bring_cmd)

            if not attr_val:
                ret_val, attr_val = bring_and_default_to_data(status, attr_data, policy_id, source_attr_name, json_data)

    return [ret_val, attr_name, attr_val]


# ------------------------------------------------------------------------------
# A policy can pull a value from a string using re
# The value is determined by the re.group(X) command that is expressed as a string in the policy.
# This method pulls X
# ------------------------------------------------------------------------------
def get_re_match_value(status, re_match, key):
    '''
    re_match - the object returned to the call:  re_match = re.match(re_compiled_pattern, attr_name)
    key - the value in the policy
    '''

    match = re.search(re_group_pattern_, key)  # Search for the pattern in the string
    if match:
        # The policy includes re.group(X)
        try:
            group_id = int(match.group(1))  # The number X is in Group 1 - Extract and return the number as an integer
            # Get the value
            derived_value = re_match.group(group_id)  # the name derived from the attribute name of the plc column

        except:
            status.add_error(f"Failed to retrieve name using key '{key}' from pattern '{re_group_pattern_}' ")
            derived_value = None
    else:
        derived_value = key

    return derived_value