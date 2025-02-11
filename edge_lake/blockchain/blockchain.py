"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import copy
import json
import os
import time

import edge_lake.blockchain.bsync as bsync

import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_io as utils_io
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_print as utils_print
import edge_lake.tcpip.net_utils as net_utils
import edge_lake.members.policies as policies
import edge_lake.generic.params as params

from edge_lake.generic.utils_columns import compare

main_ledger_ = []       # The ledger with updated policies

update_master_ = ["run", "client", None, "blockchain", "push ignore", None]     # Ignore errors when pushing into the dbms
delete_master_ = ["run", "client", None, "blockchain", "drop", "policy", "where", "id", "=", None]
update_blockchain_ = ["blockchain", "commit", "to", None, None]
delete_blockchain_ = ["blockchain", "drop", "policy", "from", None, "where", "id", "=", None]


merge_counter_ = 0  # Counter for the number of time merge was done

# ==================================================================
# Delete the main ledger - Mutex is done on the caller
# ==================================================================
def delete_ledger():
    global main_ledger_
    main_ledger_ = []

# ==================================================================
# Get the number of policies in the ledger
# ==================================================================
def get_policies_count():
    global main_ledger_
    return len(main_ledger_)

# ==================================================================
# Maintain Operator Info
# ==================================================================
class OperatorInfo():
    def __init__(self):
        self.reset()

    def reset(self):
        self.ip = ""
        self.port = ""
        self.type = ""
        self.dbms = ""
        self.table = ""
        self.counter_conditions = 0  # number of conditions satisfied
        self.active = False

    def set_info(self, ip, port, dbms_name, table_name, is_active):
        self.ip = ip
        self.port = port
        self.dbms = dbms_name
        self.table = table_name
        self.active = is_active

# ==================================================================
# Given a non-empty dict option, write it to blockchain file + Update the global main_ledger_ (the memory image)
# ==================================================================
def blockchain_write(status: process_status, file_name: str, policy: dict, is_mutex:bool):
    """
    Convert dict object to JSON and write to blockchain file
    :args:
       file_name:str - blockchain file w/ path
       policy:dict - data to store into dict
    :return:
       False if fails at some point otherwise True
    """

    global main_ledger_

    file_name = os.path.expanduser(os.path.expandvars(file_name))

    # check policy object
    if isinstance(policy, dict) is False:
        status.add_error("Invalid datatype to store in blockchain")
        return False
    if policy == {}:
        status.add_error("Empty data set to store in blockchain - invalid")
        return False

    # convert data to JSON
    try:
        data = json.dumps(policy)
    except:
        status.add_error("Unable to convert JSON policy to string in order to store in local file")
        return False

    if is_mutex:
        utils_io.write_lock("blockchain")

    ret_val = utils_io.append_data_to_file(status, file_name, "blockchain", data)

    if ret_val:
        # Update the memory image
        if not len(main_ledger_):
            # First write
            main_ledger_ = __load_ledger(status, file_name)
        else:
            main_ledger_.append(copy.deepcopy(policy))      # Make a copy such that is the policy is changed outside, it has no impacy on the policy

    if is_mutex:
        utils_io.write_unlock("blockchain")

    return ret_val

# ==================================================================
# Given an object, check if it's a dictionary or not. If not, 
# try converting it into a dictionary.
# If passes (and not empty) return dict, else return None
# ==================================================================
def restore_json(status: process_status, data: str):
    if isinstance(data, dict):
        jdata = data
    else:
        try:
            jdata = json.loads(data)
        except ValueError as error:
            status.add_error("Unable to restore data from JSON: %s" % str(error))
            return None
        except:
            status.add_error("Unable to restore data back to dict for blockchain")
            return None

    if jdata == {}:
        status.add_error("Missing JSON data")
        return None

    return jdata



# ==================================================================
# Given a file_name, read all data and store into a list - return the list
# ==================================================================
def blockchain_get_all(status: process_status, file_name: str):
    if utils_io.is_path_exists(file_name) is False:
        return None

    utils_io.read_lock("blockchain")
    list_data = utils_io.read_all_lines_in_file(status, file_name)
    utils_io.read_unlock("blockchain")

    if not list_data or not len(list_data):
        return None

    return list_data


# =======================================================================================================================
# Read the disk Ledger file to Memory
# =======================================================================================================================
def read_ledger_to_mem(status, is_modified, file_name, trace):
    '''
    If there is no file in RAM or the fule was modified, replace the file
    '''
    global main_ledger_
    utils_io.read_lock("blockchain")
    ledger_list = __load_ledger(status, file_name)
    utils_io.read_unlock("blockchain")

    utils_io.write_lock("blockchain")
    if len(ledger_list) and (is_modified or not len(main_ledger_)):
        if trace:
            message = f"Ledger with {len(ledger_list)} policies updated locally (replacing older ledger with {len(main_ledger_)} policies)"
        main_ledger_ = ledger_list  # Update the ledger list - if it is not updated
    elif trace:
        message = f"Current ledger with {len(main_ledger_)} policies is not replaced: Modified flag is {str(is_modified)}, new ledger has {len(ledger_list)} policies"
    utils_io.write_unlock("blockchain")

    if  trace:
        utils_print.output("\r\n[Update Ledger] [%s]" % (message), True)

# =======================================================================================================================
# For a given key and a set of key value pairs - place the satisfying JSONs in a list
# =======================================================================================================================
def blockchain_search(status: process_status, file_name, operation, key, value_pairs, where_cond):
    '''
    status - process status object
    blockchain_file - path to the json file
    operation - get (from memory objects) or read (from json file - representing the source)
    key - policy type
    value_pairs - key value pairs that satisfy the search
    where_cond - a where condition string (if provided, value_pairs is NULL)
    '''
    global main_ledger_

    if not key or not isinstance(key, str):
        ret_val = process_status.Wrong_key_value
        status.add_error("Invalid key value type: '%s'" % str(key))
        return [ret_val, None]

    if value_pairs:
        if isinstance(value_pairs, dict) is False and isinstance(value_pairs, set) is False:
            ret_val = process_status.Wrong_key_value
            status.add_error("Invalid value_pair value type: '%s'" % str(value_pairs))
            return [ret_val, None]

    # Get data by key and value pairs
    if operation == "get":
        if not len(main_ledger_):
            # load the ledger to RAM - this process would only happened once
            read_ledger_to_mem(status, False, file_name, 0)

        utils_io.read_lock("blockchain")

        ret_val, json_list = __get_json(status, key, value_pairs, where_cond)        # Get from the memory image at main_ledger_

        utils_io.read_unlock("blockchain")


    else:
        # READ line by line from file

        utils_io.read_lock("blockchain")

        ret_val, json_list = __read_json(status, file_name, key, value_pairs, where_cond)

        utils_io.read_unlock("blockchain")

    return [ret_val, json_list]


# ==================================================================
# Return the list of operators as a comma separated string
# ==================================================================
def get_operators_ip_string(status, fname, search_keys, include_tables):

    operators = get_operators_objects(status, fname, search_keys, include_tables)
    info_string = ""
    separator = ""
    if operators:
        for entry in operators:
            info_string += (separator + (entry.ip + ":" + entry.port))
            separator = ','
    return info_string


# ==================================================================
# Get operators that satisfy the conditions in the array
# ==================================================================
def get_operators_objects(status, fname, search_keys, include_tables):
    utils_io.read_lock("blockchain")

    operators_list = get_operators_objects_list(status, fname, search_keys, include_tables)

    utils_io.read_unlock("blockchain")

    return operators_list
# ==================================================================
# Get operators that satisfy the conditions in the array
# ==================================================================
def get_operators_objects_list(status, fname, search_keys, include_tables):
    '''
    Return a list of objects, every object represents an operator with the following attributes:
    counter_conditions - used internaly by the method to reuse existing objects
    dbms
    ip
    port
    table
    type - the type of database used by the operator for this data - i.e. psql
    '''
    file_name = os.path.expanduser(os.path.expandvars(fname))
    if file_name == "":
        status.add_error("Invalid blockchain file: %s" % fname)
        return None

    # The pool maintains multiple list_key_value enries
    lookup_pool = []  # The pool acts as an OR - every entry in the pool is an array which acts as AND

    list_key_value = []  # Each list_key_value are conditions that must exists (like AND condition) to provide an operator
    for key_value in search_keys:
        offset = key_value.find("=")  # split to key and value
        if offset < 1 or offset == (len(key_value) - 1):
            status.add_error("Unrecognized key value pairs to determine operators. Missing '=' in '%s'" % search_keys)
            return None
        key = key_value[0:offset].strip()
        value = key_value[offset + 1:].strip()
        list_key_value.append((key, value))  # add key value to the list

    lookup_pool.append(list_key_value)

    if include_tables:
        # include additional lookup tables (tables with different names but the same structure)
        include_list = include_tables.split(',')
        for entry in include_list:
            # the format is dbms.table
            dbms_table = entry.split('.')
            if len(dbms_table) != 2:
                status.add_error("Wromg format for dbms.table in include list: '%s'" % dbms_table)
            lookup_pool.append([("dbms", dbms_table[0]), ("table", dbms_table[1])])

    operators_list = []

    # for each row check if key is valid
    try:
        with open(file_name, 'r') as f:
            line_counter = 0
            for line in f.readlines():
                if not line_counter or operator_info.counter_conditions:
                    # Get new operator_info if the first time in the loop or if previous operator_info was used.
                    operator_info = OperatorInfo()
                else:
                    operator_info.reset()

                line_counter += 1
                if line == "\n":
                    continue
                json_description = restore_json(status, line)
                if json_description is None:
                    status.add_error(
                        "Failed to process data from blockchain at line %u at file at: %s" % (line_counter, file_name))
                    return None

                if "operator" in json_description.keys():
                    operator_json = json_description[
                        "operator"]  # the info from the Blockchain for the Operator to validate
                    if isinstance(operator_json, dict):
                        # test that key and value exists in object
                        for list_key_value in lookup_pool:
                            in_json = True
                            for key_value in list_key_value:
                                key = key_value[0]
                                value = key_value[1]
                                # test key value in JSON
                                if value and value != '*' and not utils_json.test_key_value(operator_json, key, value):
                                    in_json = False
                                    break  # get another set if available
                                if key == "dbms":
                                    operator_info.dbms = value
                                elif key == "table":
                                    operator_info.table = value

                            if not in_json:
                                continue  # some key is missing - test in the include tables
                            # all keys found --> add the operator to the list
                            pull_operator_info(operator_info, operator_json)

                            if not operator_info.ip or not operator_info.port:
                                break  # missing IP or port - Ignore operator

                            operator_info.counter_conditions = len(list_key_value)

                            #  a database and a table
                            for entry in operators_list:  # Test that there are no duplicate messages to the same node
                                if operator_info.ip == entry.ip and operator_info.port == entry.port \
                                        and operator_info.dbms == entry.dbms and operator_info.table == entry.table:
                                    operator_info.counter_conditions = 0  # this value allows to reuse the entry
                                    break  # duplicate entry
                            if operator_info.counter_conditions:  # was not reset with a duplicate
                                operators_list.append(operator_info)
                            break  # -> This entry was added to operators_list or is a duplicate -> move to next

    except:
        status.add_error("Failed to process data from blockchain file at: %s" % file_name)
        return None

    return operators_list


# ==================================================================
# Retrieve from the operator object (on the blockchain), needed data to the structure operator_info
# type of info: IP and port
# ==================================================================
def pull_operator_info(operator_info, operator_json):
    if "type" in operator_json.keys():
        operator_info.type = operator_json["type"]
    else:
        operator_info.type = "psql"  # SET THE DEFAULT

    ip, port = net_utils.get_dest_ip_port(operator_json)    # get the external or local ip and port to use in order to send a message to this operator
    if ip:
        operator_info.ip = ip
    if port:
        operator_info.port = port

# -----------------------------------------------------------------
# Given a list of operators, every entry in the list is the Operator Policy:
# Get a list of Operators' IP and port
# if operator_type == "all" -> all operators are included
# if operator_type == "logger" -> only logger operators are included
# If ignore_type is with value, the operator with that type is not added to the list.
# -----------------------------------------------------------------
def get_ip_port_list(status, operators, operator_type, ignore_type):

    ip_port_list = []
    ret_val = process_status.SUCCESS
    for operator in operators:
        operator_obj = operator["operator"]
        if "ip" in operator_obj.keys() and  "port" in operator_obj.keys():
            if "type" in operator_obj.keys():
                object_type = operator_obj["type"]
            else:
                object_type = ""
            if operator_type == "all" or operator_type == object_type:
                if "id" in operator_obj.keys():
                    policy_id = operator_obj["id"]
                else:
                    policy_id = "0"
                if not ignore_type or object_type != ignore_type:           # Test if a particular type of Operator is not to be included
                    if "member" not in operator_obj.keys():
                        status.add_error("Policy Error: Missing 'member' in Operator Policy: %s" % policy_id)
                        ret_val = process_status.Incomplete_policy
                        break

                    member = operator_obj["member"]       # Sequential number that is associated with the sequence of the Operator in the Cluster
                    if not isinstance(member, int):
                        status.add_error("Policy Error: Wrong value for 'member' in Operator Policy: %s" % policy_id)
                        ret_val = process_status.Wrong_policy_structure
                        break

                    ip, port = net_utils.get_dest_ip_port(operator_obj)  # get the external or local ip and port to use in order to send a message to this operator

                    if not ip:
                        status.add_error("Policy Error: Missing 'ip' in Operator Policy: %s" % policy_id)
                        ret_val = process_status.Incomplete_policy
                        break

                    if not port:
                        status.add_error("Policy Error: Missing 'port' in Operator Policy: %s" % policy_id)
                        ret_val = process_status.Incomplete_policy
                        break

                    ip_port_list.append((ip, port, member))
        else:
            if "id" in operator_obj["id"].keys():
                id = operator_obj["id"]
            else:
                id = "'Not provided'"
            status.add_eror("Missing IP and Port information for Operator where id = %u" % id)
            ret_val = process_status.Incomplete_policy
            break

    return [ret_val, ip_port_list]

# ==================================================================
# Transform a comma seperated keys to a list
# Example: "(operator,master, query)" --> [ "operator" , "master" , "query"]
# ==================================================================
def get_search_keys(key):

    if key[0] == '(' and key[-1] == ')':
        # Multiple types
        key_list = key[1:-1].split(',')
        for i in range(len(key_list)):
            if key_list[i][0] == ' ' or key_list[i][-1] == ' ':
                key_list[i] = key_list[i].strip()  # remove leading and trailing spaces from the keys
    else:
        key_list = [key]

    return key_list

# ==================================================================
# Read from the source JSON file
# Given a key, find the JSON that satisfies the key
# Return a list of all JSONs that satisfy the key.
# ==================================================================
def __get_json(status: process_status, key: str, value_pair, where_cond):
    '''
     status - process status object
     blockchain_file - path to the json file
     key - policy type
     json_serach - key value pairs that satisfy the search
     '''
    global main_ledger_

    ret_val = process_status.SUCCESS

    updated_data_list = []

    key_list = get_search_keys(key) # Transform a comma seperated keys to a list

    if where_cond:
        conditions_list = []
        ret_val, offset_then, with_paren = params.analyze_if(status, where_cond, 0, conditions_list)
        if ret_val:
            return [ret_val, updated_data_list]    # On failure - returns empty list


    # for each row check if key is valid
    for json_description in main_ledger_:

        for key_val in key_list:

            if where_cond:
                # A where condition string
                # Filter policies by the WHERE condition
                if key_val == '*' or key_val in json_description:
                    # test_condition returns: a) offset to then, b) 0 (False), 1 (True), -1 (Error)
                    json_inner = utils_json.get_inner(json_description)
                    next_word, ret_code = params.process_analyzed_if(status, params, where_cond, 0, offset_then, with_paren,
                                                                     conditions_list, json_inner)
                    if ret_code == 1:
                        # This Policy satisfies the condition
                        updated_data_list.append(json_description)

            elif value_pair == None:
                # Get all policies of type X or Get all policies
                if key_val in json_description or key_val == '*':
                    updated_data_list.append(json_description)  # a list of all JSONs that satisfy the key.
            else:
                # Filter policies by the WHERE condition
                if key_val in json_description:
                    json_inner = json_description[key_val]          # Search by the key-value pair
                elif key_val == '*':
                    json_inner = utils_json.get_inner(json_description)
                else:
                    json_inner = None

                if json_inner:
                    if __validate_value_pair(json_inner, value_pair):
                        updated_data_list.append(json_description)  # a list of JSON that satisfies the key and the value pair


    return [ret_val, updated_data_list]

# ==================================================================
# Read from the source JSON file
# Given a key, find the JSON that satisfies the key
# Return a list of all JSONs that satisfy the key.
# ==================================================================
def __read_json(status: process_status, fname: str, key: str, value_pair, where_cond):
    '''
     status - process status object
     blockchain_file - path to the json file
     key - policy type
     json_serach - key value pairs that satisfy the search
     '''

    ret_val = process_status.SUCCESS

    file_name = os.path.expanduser(os.path.expandvars(fname))
    if file_name == "":
        status.add_error("Invalid blockchain file: %s" % fname)
        ret_val = process_status.BLOCKCHAIN_not_recognized
        return [ret_val, None]

    updated_data_list = []

    key_list = get_search_keys(key)  # Transform a comma seperated keys to a list

    if where_cond:
        conditions_list = []
        ret_val, offset_then, with_paren = params.analyze_if(status, where_cond, 0, conditions_list)
        if ret_val:
            return [ret_val, updated_data_list]    # On failure - returns empty list


    # for each row check if key is valid
    try:
        with open(file_name, 'r') as f:
            line_counter = 0
            for line in f.readlines():
                line_counter += 1
                if line == "\n":
                    continue
                json_description = restore_json(status, line)
                if json_description is None:
                    status.add_error("Failed to process data from blockchain at line %u at file at: %s" % (line_counter, file_name))
                    ret_val = process_status.Wrong_blockchain_struct
                    return [ret_val, None]

                for key_val in key_list:

                    if where_cond:
                        # A where condition string
                        # Filter policies by the WHERE condition
                        if key_val == '*' or key_val in json_description:
                            # test_condition returns: a) offset to then, b) 0 (False), 1 (True), -1 (Error)
                            json_inner = utils_json.get_inner(json_description)
                            next_word, ret_code = params.process_analyzed_if(status, params, where_cond, 0, offset_then,
                                                                             with_paren,
                                                                             conditions_list, json_inner)
                            if ret_code == 1:
                                # This Policy satisfies the condition
                                updated_data_list.append(json_description)

                    elif value_pair == None:
                        # Get all policies of type X or Get all policies
                        if key_val in json_description or key_val == '*':
                            updated_data_list.append(json_description)  # a list of all JSONs that satisfy the key.
                    else:
                        # Filter policies by the WHERE condition
                        if key_val in json_description:
                            json_inner = json_description[key_val]          # Search by the key-value pair
                        elif key_val == '*':
                            json_inner = utils_json.get_inner(json_description)
                        else:
                            json_inner = None

                        if json_inner:
                            if __validate_value_pair(json_inner, value_pair):
                                updated_data_list.append(json_description)  # a list of JSON that satisfies the key and the value pair

    except IOError:
        status.add_error("Blockchain file does not exists: %s" % file_name)
        ret_val = process_status.No_local_blockchain_file
    except:
        status.add_error("Failed to process data from blockchain file at: %s" % file_name)
        ret_val = process_status.File_read_failed

    if not ret_val:
            # if empty set warning
        if updated_data_list == []:  # warning if no data is found
            if not value_pair:
                status.add_warning("Blockchain Read: Empty data set for given key: \"%s\"" % key)
            else:
                status.add_warning(
                    "Blockchain Read: Empty data set for given key and value pairs: \"%s\":\"%s\"" % (key, value_pair))

    return [ret_val, updated_data_list]
# ==================================================================
# Load the JSON policies to RAM + Update ANMP policies
# ==================================================================
def __load_ledger(status: process_status, fname: str):
    '''
     status - process status object
     blockchain_file - path to the json file
     '''

    ledger_list = []                            # a list with the blockchain objects - dynamically organized in this process
    anmp_list = []                              # A list with AnyLog Network Management Policies
    index_by_id = {}                            # a dictionary to the policy by ID

    file_name = os.path.expanduser(os.path.expandvars(fname))
    if file_name == "":
        status.add_error("Invalid blockchain file: %s" % fname)
        return ledger_list

    # for each row check if key is valid
    try:
        with open(file_name, 'r') as f:
            line_counter = 0
            for line in f.readlines():
                line_counter += 1
                if line == "\n":
                    continue
                policy = restore_json(status, line)

                if policy is None:
                    status.add_error("Failed to process data from blockchain at line %u at file at: %s" % (line_counter, file_name))
                    break
                policy_type, policy_id = utils_json.get_policy_type_id(policy)

                if policy_type and policy_id:
                    # Update memory structures: a) A list of all policies, b) a list of ANMP policies c) index by ID to the policies
                    if policy_type == "anmp":
                        # AnyLog Network Management Policy
                        # Put in a side list
                        anmp_list.append(policy)
                    else:
                        index_by_id[policy_id] = policy
                    ledger_list.append(policy)      # All policies are added (including ANMP)



    except IOError:
        status.add_error("Blockchain file does not exists: %s" % file_name)
    except:
        status.add_error("Failed to process data from blockchain file at: %s" % file_name)
    else:
        merge_anmp(status, anmp_list, ledger_list, index_by_id)  # Merge ANMP policies with the ledger


    return ledger_list

# ==================================================================
# Return true if the list of key values appear in the JSON
# Both the search value can be a list and the JSON values can be a list
# ==================================================================
def __validate_value_pair(json_description, value_pair):

    for nested_key, test_value in value_pair.items():

        # test that key and value are in the tested JSON
        child_key = ""
        key = nested_key
        if nested_key[-1] == ']':       # Could be a key like table[name]
            # extract the prefix key: table[name] --> table
            index = nested_key.find('[')
            if index > 0:
                key = nested_key[:index]
                child_key = nested_key[index:]
            elif index == 0:
                # remove the square brackets
                index = nested_key.find(']')
                if index < 2:
                    ret_val = False  # Wrong key - missing closing brackets or no value in brakets
                    break
                key = nested_key[1:index]
                if len(nested_key) > index:
                    child_key = nested_key[index + 1:]

        if not key in json_description:
            ret_val = False  # key is not in tested JSON
            break
        else:
            ret_val = True
            json_value = json_description[key]

            if isinstance(test_value, str):
                # Tested value is a string - Change to lower case
                value = test_value.lower()

                if child_key:
                    if len(child_key) <= 2 or child_key[0] != '[' or child_key[-1] != ']':
                        ret_val = False     # Is not a pull from a dictionary
                        break
                    pull_key = child_key[1:-1]
                    # Test for a nested dictionary
                    if isinstance(json_value, list):
                        # Go over all list entries
                        ret_val = False
                        for entry in json_value:
                            if isinstance(entry, dict) and pull_key in entry:
                                nested_value = entry[pull_key]
                                ret_val = True
                                break       # Found a match
                        if not ret_val:
                            break   # No such key
                    elif not isinstance(json_value, dict) or not pull_key in json_value:
                        ret_val = False
                        break
                    else:
                        # Pull the value from the nested dictionary and compare to user provided "value"
                        nested_value = json_value[pull_key]

                    if not isinstance(nested_value, str) or nested_value.lower() != value:
                        ret_val = False
                        break
                elif isinstance(json_value, str):
                    # Blockchain value is a string - Change to lower case
                    if json_value != '*':
                        if json_value.lower() != value:
                            ret_val = False
                            break
                elif isinstance(json_value, list):
                    # Blockchain value is a list - Find match in the list
                    if child_key:
                        if not utils_json.test_nested_key_value(params, json_value, child_key, value):
                            ret_val = False
                            break
                    elif value not in json_value:
                        ret_val = False
                        break
                elif isinstance(json_value, int):
                    if child_key or not compare(json_value, test_value, "=", "int"):
                        ret_val = False
                        break
                elif isinstance(json_value, float):
                    if child_key or not compare(json_value, test_value, "=", "float"):
                        ret_val = False
                        break
                else:
                    ret_val = False
                    break
            else:
                # if the tested value in a list - the call was using 'with': blockchain get assignment where members with ...
                if isinstance(test_value, list):
                    ret_val = False # Find one occurence which is True
                    for entry in test_value:        # Go over each list entry
                        if isinstance(json_value, list):
                            if entry in json_value or entry.lower() in json_value:
                                ret_val = True
                                break
                        elif isinstance(json_value, str):
                            if  entry == json_value or entry.lower() == json_value.lower():
                                ret_val = True
                                break
                else:
                    ret_val = False
                    break

    return ret_val


# ==================================================================
# Validate the blockchain structure
# Read the blockchain local file and test that the structure is in JSON
# ==================================================================
def validate_struct(status, b_file):

    unique_dict = {}        # test unique values in the file
    ret_val = process_status.SUCCESS

    data_list = blockchain_get_all(status, b_file)
    if not data_list:
        ret_val = process_status.Empty_Local_blockchain_file
    else:
        # test local blockchain file
        for index, entry in enumerate(data_list):
            if entry:

                ret_val, json_obj = get_policy_object(status, index, entry)
                if not ret_val:
                    ret_val = policies.validate_policy(status, json_obj, unique_dict)
                    if ret_val:
                        err_message ="Policy %u in file %s failed: Validation failure" % (index + 1, b_file)
                        status.add_error(err_message)
                        utils_print.output(err_message, True)
    return ret_val

# ==================================================================
# Get Policy object or output Error
# ==================================================================
def get_policy_object(status, index, policy_str ):

    policy_obj = utils_json.str_to_json(policy_str)
    if not policy_obj:
        message = "Policy validation failed: Policy is not represented as a JSON structure - entry number %u: '%s'" % (index + 1, policy_str)
        utils_print.output(message, True)
        status.add_error(message)
        ret_val = process_status.Wrong_blockchain_struct
    else:
        ret_val = process_status.SUCCESS

    return [ret_val, policy_obj]

# ==================================================================
# Return True if the blockchain contains the given ID
# ==================================================================
def validate_id(status, b_file, object_id):
    ret_val = False

    utils_io.read_lock("blockchain")
    for policy in main_ledger_:
        # Go over all policies to determine if ID exists
        if object_id == utils_json.get_object_id(policy):
            ret_val = True
            break
    utils_io.read_unlock("blockchain")

    return ret_val

# ==================================================================
# Return True if the blockchain contains the given ID
# ==================================================================
def delete_policy(status, b_file, policy_id):

    global main_ledger_
    ret_val = False

    utils_io.write_lock("blockchain")
    for index, tested_id in enumerate(main_ledger_):
        # Go over all policies to determine if ID exists
        if policy_id == utils_json.get_object_id(tested_id):
            del main_ledger_[index]
            ret_val = True
            break
    utils_io.write_unlock("blockchain")

    if ret_val:
        file_name = os.path.expanduser(os.path.expandvars(b_file))

        # This policy is written to the local file and flagged as deleted
        deleted_policy = '{"deleted": { "id" : "%s", "ledger" : "local" }}' % policy_id  # During the merge with a new file - flag that the policy is deleted


        utils_io.write_lock("blockchain")

        ret_val = utils_io.append_data_to_file(status, file_name, "blockchain", deleted_policy)

        if ret_val:
            # Update the memory image
            if not len(main_ledger_):
                # First write
                main_ledger_ = __load_ledger(status, file_name)


        utils_io.write_unlock("blockchain")

    return ret_val
# ==================================================================
# Wait for a local copy of the blockchain - return an error if no copy found
# ==================================================================
def wait_for_blockchain(status, blockchain_file, max_time):
    wait_time = 0
    while 1:
        ret_val = validate_struct(status, blockchain_file)
        if not ret_val:
            break  # File exists
        if ret_val == process_status.Empty_Local_blockchain_file:
            # The file was not yet received, wait for next time
            wait_time += 5
            if wait_time > max_time:
                status.add_error("No local copy of the blockchain, wait time of %u seconds expired" % max_time)
                break
            time.sleep(5)
        else:
            break  # Error with the file

    return ret_val

# ==================================================================
# Merge 2 ledgers
# existing_ledgerr is with policies flagged "local".
# These policies are tested to exist in the new_ledger.
# If they do not exist - the policies are added to the new_ledger
# and the blockchain platform is updated with the policies.
# ==================================================================
def merge_ledgers(status, io_buff_in, new_ledger, existing_ledger, trace):
    '''
    # Caller:
    # events.use_new_blockchain_file() after a new blockchain file was copied from a master node

    # Params:
    source_path - the path to the source ledger (with policies flagged local)
    dest_path - the path to the destination ledger
    '''

    global update_master_
    global delete_master_
    global update_blockchain_
    global delete_blockchain_
    global main_ledger_
    global merge_counter_                       # Counter for the number of time merge was done

    ledger_list = []                            # a list with the blockchain objects - dynamically organized in this process
    anmp_list = []                                  # A list with AnyLog Network Management Policies
    index_by_id = {}                                # a dictionary to the policy by ID
    ret_val, local_list = blockchain_search(status, existing_ledger, "read", '*',  {"ledger" : "local"}, None)   # get all the local policies

    if not ret_val or ret_val == process_status.No_local_blockchain_file:
        # if ret_val is process_status.No_local_blockchain_file - a new file starting

        try:

            data_list = blockchain_get_all(status, new_ledger)     # read all rows in new file
            if not data_list:
                ret_val = process_status.Empty_Local_blockchain_file
            else:
                unique_dict = {}  # test unique values in the file
                ret_val = process_status.SUCCESS

                if bsync.is_running():
                    platform_sync = True        # With Blockchain or Master Node
                    mem_view = memoryview(io_buff_in)
                    if bsync.is_master():
                        platform = "master"
                        ip_port = bsync.get_connection()
                        update_master_[2] = "(" + ip_port + ")"
                        delete_master_[2] = "(" + ip_port + ")"
                    else:
                        platform = "blockchain"
                        blockchain_name = bsync.get_connection()
                        update_blockchain_[3] = blockchain_name
                        delete_blockchain_[4] = blockchain_name
                else:
                    platform_sync = False


                # test the new ledger file
                # Data List is the new ledger from the Master or Blockchain platform
                for index, entry in enumerate(data_list):   # Go over all entries in the new ledger
                    if entry:
                        ret_val, global_policy = get_policy_object(status, index, entry)
                        if not ret_val:
                            # Test that the format of the policy is correct
                            ret_val = policies.validate_policy(status, global_policy, unique_dict)
                            if ret_val:
                                # Error in policy
                                err_message ="Policy %u in file %s failed: Merge failure" % (index + 1, new_ledger)
                                status.add_error(err_message)
                                utils_print.output(err_message, True)
                            else:
                                policy_type, policy_id = utils_json.get_policy_type_id(global_policy)

                                if policy_type and policy_id:
                                    # Update memory structures: a) A list of all policies, b) a list of ANMP policies c) index by ID to the policies
                                    if policy_type == "anmp":
                                        # AnyLog Network Management Policy
                                        # Put in a side list
                                        anmp_list.append(global_policy)
                                    else:
                                        index_by_id[policy_id] = global_policy
                                    ledger_list.append(global_policy)      # All policies are added (including ANMP)

                                if local_list:

                                    # test that policy in the local list exists in the new file.
                                    # For policy that is added - If the policy doesn't exist - it will be updated to the new file.
                                    # For a policy that was deleted - If the policy exists, remove from the new file

                                    for policy_offset, local_policy in enumerate(local_list):
                                        if utils_json.get_policy_type(local_policy) != "deleted":
                                            if policies.compare_policies(local_policy, global_policy):
                                                # this policy is represented in the new ledger - remove from the list
                                                del local_list[policy_offset]  # this policy to remove
                                                break


                if local_list:      # local_list includes all the policies in the current active blockchain.json
                    # The policies which are not on the new_ledger are:
                    # 1) Added to the new ledger
                    # 2) Resend to the blockchain platform
                    for local_policy in local_list:
                        # Update the new ledge
                        policy_type, policy_id = utils_json.get_policy_type_id(local_policy)

                        if policy_type != "deleted":
                            ret_val = policies.validate_policy(status, local_policy, unique_dict)
                            if ret_val:
                                # the policy to merge is not valid
                                err_message = "Policy to merge is not valid: %s" % str(local_policy)
                                status.add_error(err_message)
                                utils_print.output(err_message, True)
                                continue
                        else:
                            # Deleted policy
                            if not policy_id in index_by_id:
                                # index_by_id is a dictionary to all the active policies
                                # The policy id deleted from the main ledger
                                continue

                            for index, tested_id in enumerate(main_ledger_):
                                # Go over all policies to remove the deleted policy
                                if policy_id == utils_json.get_object_id(tested_id):
                                    if index >= len(ledger_list):
                                        break               # Same policy delete multiple times
                                    del ledger_list[index]

                                    del index_by_id[policy_id]  # Avoid sending the delete message twice
                                    break
                            # Will add next to the new ledger (in blockchain_write)

                        if not blockchain_write(status, new_ledger, local_policy, False): # no need to mutex during the write as the process is mutexed
                            ret_val = process_status.ERR_process_failure
                            break


                        if policy_type and policy_id and policy_type != "deleted":
                            # Update memory structures: a) A list of all policies, b) a list of ANMP policies c) index by ID to the policies
                            if policy_type == "anmp":
                                # AnyLog Network Management Policy
                                # Put in a side list
                                anmp_list.append(local_policy)
                            else:
                                index_by_id[policy_id] = local_policy
                            ledger_list.append(local_policy)      # All policies are added (including ANMP)

                        # Update the platform
                        if platform_sync:

                            local_policy[policy_type]["ledger"] = "global"

                            policy_str = utils_json.to_string(local_policy)

                            if platform == "master":
                                if policy_type == "deleted":
                                    # Delete a policy from the master
                                    delete_master_[9] = policy_id
                                    reply_val = member_cmd.run_client(status, mem_view, delete_master_, 0)  # We ignore errors as the master may be down
                                else:
                                    # Add a new policy to the master
                                    update_master_[5] = policy_str
                                    # This is an update to the database  of the master + ignore the errors
                                    reply_val = member_cmd.run_client(status, mem_view, update_master_, 0)  # We ignore errors as the master may be down
                            elif platform == "blockchain":
                                if policy_type == "deleted":
                                    # Delete a policy from the master
                                    delete_blockchain_[8] = policy_id
                                    reply_val, reply = member_cmd.blockchain_commit(status, mem_view, delete_blockchain_, 0, None)  # We ignore errors as the network may be down
                                else:
                                    update_blockchain_[4] = policy_str
                                    reply_val, reply = member_cmd.blockchain_commit(status, mem_view, update_blockchain_, 0, None)  # We ignore errors as the network may be down
        except:
            err_message = "Merge ledgers process failed"
            status.add_error(err_message)
            utils_print.output(err_message, True)


        if not ret_val:
            merge_anmp(status, anmp_list, ledger_list, index_by_id)        # Merge ANMP policies with the ledger
            utils_io.write_lock("blockchain")

            if trace:
                current_entries = len(main_ledger_) if main_ledger_ else 0 # The number of entries before the merge
                new_entries = len(ledger_list) if ledger_list else 0 # The number of new entries

            main_ledger_ = ledger_list          # Update the ledger list

            utils_io.write_unlock("blockchain")

    merge_counter_ += 1  # Counter for the number of time merge was done

    return ret_val

# ==================================================================
# Return the merge counter
# ==================================================================
def get_merge_counter():
    global merge_counter_  # Counter for the number of time merge was done
    return merge_counter_
# ==================================================================
# Merge AnyLog Network Management Policies with the in-memory ledger
# ==================================================================
def  merge_anmp(status, anmp_list, ledger_list, index_by_id):
    '''
    status - process status object
    anmp_list - list of ANMP policies
    ledger_list = the ledger policies
    index_by_id - an index to the policies by their IDs
    '''

    for anmp_policy in anmp_list:

        policies = anmp_policy["anmp"]

        if "date" in policies:
            policy_date = policies["date"]      # This date is validated such that new ANMP policies will update older ones
        else:
            policy_date = ""

        for policy_id, policy_new_values in policies.items():

            if policy_id in index_by_id:
                # Modify the policy by the values
                src_policy = index_by_id[policy_id]     # This is the policy to modify
                src_policy_inner = utils_json.get_inner(src_policy) # the policy values

                if not "anmp" in  src_policy_inner:
                    src_policy_inner["anmp"] = {}       # this dictionary keeps the date relating the update

                for key, new_value in policy_new_values.items():
                    update_val = True
                    if key in src_policy_inner["anmp"]:
                        previous_date = src_policy_inner["anmp"][key]
                        if previous_date > policy_date:
                            update_val = False          # A more updated value in the policy

                    if update_val:
                        src_policy_inner["anmp"][key] = policy_date
                        src_policy_inner[key] = new_value

