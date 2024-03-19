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


import anylog_node.generic.process_status as process_status
import anylog_node.tcpip.message_header as message_header
import anylog_node.generic.interpreter as interpreter
import anylog_node.cmd.member_cmd as member_cmd
import anylog_node.generic.utils_print as utils_print
import anylog_node.generic.utils_json as utils_json

'''
Apply conditions on the streaming data
The following are the relevant commands:


set streaming condition where dbms = X and table = Y if condition then result
get streaming conditions
reset streaming conditions where dbmms = a and table = y
reset streaming condition 
'''

conditions_ = {}  # Conditions as f (dbms + table)

# =======================================================================================================================
# Set a condition as f (dbms + table)
# Example: set streaming condition where dbms = test and table = and_data if [value] > 10 then send sms to 6508147334 where gateway = tmomail.net
# =======================================================================================================================
def set_streaming_condition(status, io_buff_in, cmd_words, trace):

    global conditions_

    words_count = len(cmd_words)
    if words_count < 2:
        return process_status.ERR_command_struct

    ret_val = process_status.SUCCESS

    if cmd_words[1] == "message":  # a tcp sql message
        mem_view = memoryview(io_buff_in)
        command = message_header.get_command(mem_view)
        words_array = command.split()
        message = True
    else:
        words_array = cmd_words
        message = False

    if_offset = 0
    if words_array[3] != "where":
        status.add_error("Missing 'where' in 'set streaming condition' command")
        ret_val = process_status.ERR_command_struct
    elif words_array[11] == "if":
        if_offset = 11
    elif len(words_array) > 16 and words_array[15] == "if":
        if_offset = 15
    else:
        status.add_error("Missing 'if' in 'set streaming condition' command")
        ret_val = process_status.ERR_command_struct

    if not ret_val:

        # get the conditions to execute the JOB
        #                                      Must     Add      Is
        #                                      exists   Counter  Unique

        keywords = {"dbms": ("str", True, False, True),
                    "table": ("str", True, False, True),  # A name to provide to the message
                    "limit": ("int", False, False, True),  # A cap on the number of times the 'then' statement is executed
                   }

        ret_val, counter, conditions = interpreter.get_dict_from_words(status, words_array, 4, if_offset, keywords,
                                                                       False)
        if not ret_val:
            # conditions are satisfied by keywords or command structure

            # The limit sets a cap. For example: to avoid thousands of emails send
            limit = interpreter.get_one_value_or_default(conditions, "limit", 0)  # A cap on the number of times the "then" statements is executed

            dbms_name = interpreter.get_one_value(conditions, "dbms")

            if not dbms_name in conditions_:
                conditions_[dbms_name] = {}

            table_name = interpreter.get_one_value(conditions, "table")

            if not table_name in conditions_[dbms_name]:
                conditions_[dbms_name][table_name] = []     # A list for the conditions

            # Test if duplicated
            if_stmt = words_array[if_offset:]
            conditions_list = conditions_[dbms_name][table_name]
            for one_condition in conditions_list:
                if if_stmt == one_condition[0]:
                    status.add_error(f"Duplicate condition for DBMS '{dbms_name}' and table '{table_name}': {' '.join(words_array[if_offset:])}")
                    ret_val = process_status.Duplicate_condition
                    break
            if not ret_val:
                # Each entry includes: the command, if execution has a limit (i.e. max number of emails send), number of times executed
                conditions_[dbms_name][table_name].append([if_stmt, limit, 0])


    if ret_val and message:
        member_cmd.error_message(status, io_buff_in, ret_val, message_header.BLOCK_INFO_COMMAND, f"echo Failed to set a condition with: {' '.join(words_array)}", status.get_saved_error())

    return ret_val

# =======================================================================================================================
# get conditions as f (dbms + table)
# Example: get streaming conditions where dbms = test and table = rand_data
# Example get streaming conditions
# =======================================================================================================================
def get_streaming_condition(status, io_buff_in, cmd_words, trace):

    global conditions_

    words_count = len(cmd_words)

    reply = ""
    ret_val = process_status.SUCCESS

    if words_count == 3:
        dbms_name = '*'
        table_name = '*'
    elif words_count >= 7 and cmd_words[3] == "where":
        keywords = {"dbms": ("str", False, False, True),
                    "table": ("str", False, False, True),
                    }

        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
        if not ret_val:
            # conditions are satisfied by keywords or command structure

            dbms_name = interpreter.get_one_value_or_default(conditions, "dbms", '*')
            table_name = interpreter.get_one_value_or_default(conditions, "table", '*')

    else:
        ret_val = process_status.ERR_command_struct


    reply_list = []
    if not ret_val:
        if dbms_name == '*':
            # Get all
            for dbms_entry in conditions_:
                get_tables_conditions(reply_list, dbms_entry, table_name)       # Add each database
        else:
            get_tables_conditions(reply_list, dbms_name, table_name)            # Add one database

        reply = utils_print.output_nested_lists(reply_list, "", ["DBMS", "Table", "ID", "Condition"], True)

    return [ret_val, reply, "table"]

# =======================================================================================================================
# Get the conditions of all tables of a particular database
# =======================================================================================================================
def  get_tables_conditions(reply_list, dbms_name, table_name):
    '''
    conditions_list - the nested list to update with the info
    '''

    global conditions_

    if not  dbms_name in conditions_:
        # Add dbms name, table name,  ID (empty), condition or comment
        reply_list.append(( dbms_name, "", "", "No Conditions"))
    else:
        tables_dict = conditions_[dbms_name]
        if table_name == '*':
            # Get all
            for table_entry in tables_dict:
                get_one_table_conditions(reply_list, dbms_name, table_entry)  # Add each database
        else:
            get_one_table_conditions(reply_list, dbms_name, table_name)  # Add one database

# =======================================================================================================================
# Get the conditions from one table of a particular database
# =======================================================================================================================
def  get_one_table_conditions(reply_list, dbms_name, table_name):
    '''
    conditions_list - the nested list to update with the info
    '''

    global conditions_
    if not  table_name in conditions_[dbms_name]:
        # Add dbms name, table name,  ID (empty), condition or comment
        reply_list.append((dbms_name, table_name, "", "No conditions assigned to the table"))
    else:
        conditions_list = conditions_[dbms_name][table_name]

        for index, cond_item in enumerate(conditions_list):
            if not index:
                print_dbms = dbms_name      # Show in output only once
                print_table = table_name
            else:
                print_dbms = ""
                print_table = ""
            reply_list.append((print_dbms, print_table, index + 1, ' '.join(cond_item[0])))


# =======================================================================================================================
# reset conditions as f (dbms + table)
# Example: reset streaming conditions where dbms = test and table = rand_data and id = 1 and id = 3
# Example reset streaming conditions
# =======================================================================================================================
def reset_streaming_condition(status, io_buff_in, cmd_words, trace):

    global conditions_

    words_count = len(cmd_words)

    ret_val = process_status.SUCCESS

    if words_count == 3:
        dbms_name = '*'
        table_name = '*'
    elif words_count >= 7 and cmd_words[3] == "where":
        keywords = {"dbms": ("str", False, False, True),
                    "table": ("str", False, False, True),
                    "id": ("int", False, False, False),
                    }

        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
        if not ret_val:
            # conditions are satisfied by keywords or command structure

            dbms_name = interpreter.get_one_value_or_default(conditions, "dbms", '*')
            table_name = interpreter.get_one_value_or_default(conditions, "table", '*')
            id_list = interpreter.get_all_values(conditions, "id", None)

    if not ret_val:

        if dbms_name == '*':
            if table_name == '*' and id_list == None:
                conditions_ == {}   # Remove all
            else:
                ret_val = process_status.ERR_command_struct
        elif table_name == '*':
            if id_list != None:
                ret_val = process_status.ERR_command_struct
            elif not dbms_name in conditions_:
                status.add_error(f"No conditions for DBMS: '{dbms_name}'")
                ret_val = process_status.ERR_dbms_name
            else:
                del conditions_[dbms_name]
        elif id_list == None:
            # dbms and table are available
            if dbms_name not in conditions_ or table_name not in conditions_[dbms_name]:
                status.add_error(f"No conditions for DBMS: '{dbms_name}' and Table: '{table_name}'")
                ret_val = process_status.ERR_table_name
            else:
                del conditions_[dbms_name][table_name]
                if not len(conditions_[dbms_name]):
                    # all data deleted
                    del conditions_[dbms_name]
        else:
            if dbms_name not in conditions_ or table_name not in conditions_[dbms_name]:
                status.add_error(f"No conditions for DBMS '{dbms_name}' and Table: '{table_name}'")
                ret_val = process_status.ERR_table_name
            else:
                conditions_list = conditions_[dbms_name][table_name]
                id_list.sort(reverse = True)
                for entry in id_list:
                    list_id = entry - 1
                    if list_id >= 0 and list_id < len(conditions_list):
                        del  conditions_list[list_id]
                if not len(conditions_list):
                    # All was deleted
                    del conditions_[dbms_name][table_name]  # Delete the table
                    if not len(conditions_[dbms_name]):
                        del conditions_[dbms_name] # Delete the DBMS

    else:
        ret_val = process_status.ERR_command_struct


    return ret_val


# =======================================================================================================================
# Return True if conditions are available
# =======================================================================================================================
def is_with_conditions(dbms_name, table_name):
    global conditions_
    return dbms_name in conditions_ and table_name in conditions_[dbms_name]

# =======================================================================================================================
# Apply conditions on the message data
# =======================================================================================================================
def apply_conditions(status, dbms_name, table_name, msg_data):

    global conditions_

    ret_val = process_status.SUCCESS

    data_list = msg_data.split('\n')

    io_buff = status.get_io_buff()

    delete_entries = []

    for entry_id, entry in enumerate(data_list):

        json_struct = utils_json.str_to_json(entry)
        if json_struct:
            conditions_list = conditions_[dbms_name][table_name]
            # Process each condition
            for one_condition in conditions_list:

                limit = one_condition[1]        # User may say a limit on executions (for example limit on emails send)
                if not limit or limit > one_condition[2]:

                    ret_val = member_cmd._process_if(status, io_buff, one_condition[0], 0, json_struct)

                    if ret_val:
                        if ret_val == process_status.IGNORE_ENTRY:
                            # remove the entry from the processed data
                            # Example: if val < 3 then return ignore entry
                            delete_entries.append(entry_id)
                            ret_val = process_status.SUCCESS    # Not an error that stops processing the next entries
                        break

                    if limit:
                        result = status.get_if_result()
                        if result:
                            one_condition[2] += 1       # Count executions returning True

        if ret_val:
            break

    if len(delete_entries) and not ret_val:
        # Delete the entries and return a new buffer
        for entry_id in reversed(delete_entries):
            del data_list[entry_id]

        if not len(data_list):
            new_msg_data = ""       # all deleted
        else:
            new_msg_data = utils_json.to_string(data_list)      # with removed entries
    else:
        new_msg_data = msg_data # No entry removals

    return [ret_val,  new_msg_data]



