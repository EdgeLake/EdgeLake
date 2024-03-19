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

# ============================================================================================
# Create tables and partitions as needed
# If a table in the database dosed not exists - create the table
# If a partition does not exist - create the partition
# ============================================================================================

import edge_lake.dbms.db_info as db_info
import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_print as utils_print
import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.generic.utils_sql as utils_sql
import edge_lake.generic.utils_data as utils_data
import edge_lake.json_to_sql.suggest_create_table as suggest_create_table
import edge_lake.tcpip.net_utils as net_utils


operator_active_ = False
platform_ = ""                  # BLOCKCHAIN OR Master
blockchain_file_ = ""
master_node_ = None
is_create_table_ = False        # Operator flag to determine if a new table is to be created automatically
status_msg_ = "Operator not active"

# ----------------------------------------------------------
# Set variables by operator - if operator is not configured,
# process will exit
# ----------------------------------------------------------
def set_generic_params(operator_status, config):
    '''
    operator_status - True is Operator is operational
    config - the operator files configuration object
    '''
    global operator_active_
    global platform_
    global blockchain_file_
    global status_msg_
    global is_create_table_
    global master_node_

    operator_active_ = False
    if operator_status and config:
        # if a declaration is missing - set operator_active_ to False
        status_msg_ = ""
        blockchain_file_ = config.blockchain_file
        if not blockchain_file_:
            status_msg_ = "Create Table: Operator is missing local blockchain file location"
            operator_active_ = False
        elif not config.platform:
            status_msg_ = "Create Table: Operator is missing platform type info"
        else:
            if config.platform == "master":
                platform_ = None  # not using a blockchain
            else:
                platform_ = [config.platform]

            is_create_table_ = config.is_create_table
            master_node_ = config.master_node
            if not platform_ and not master_node_:
                status_msg_ = "Create Table: Operator is missing IP and Port of a master node"
            else:
                operator_active_ = True

    else:
        status_msg_ = "Operator not active"
        operator_active_ = False


# ----------------------------------------------------------
# Create table if doesn't exists
# If the table exists in the database: Test that the table in in the blockchain
# ----------------------------------------------------------
def validate_table(status, io_buff,dbms_name, table_name, with_tsd_info, instructions, is_partitioned, par_name, file_name, trace_level):

    global blockchain_file_
    global is_create_table_

    if not operator_active_:
        status.add_error(status_msg_)
        ret_val = process_status.Operator_not_enabled
    else:

        if is_partitioned:
            t_name = "par_" + table_name + "_" + par_name
        else:
            t_name = table_name

        if db_info.is_table_exists(status, dbms_name, t_name):
            if trace_level > 1:
                utils_print.output("[Operator] [Table exists on local database] [%s.%s]" % (dbms_name, t_name), True)

            cmd_get_table = ["blockchain", "get", "table", "where", "dbms", "=", dbms_name, "and", "name", "=", table_name, \
                             "bring", "[table][create]"]

            # This table (or partition) exists in the database - it has to be in the blockchain or just created and in RAM

            # test if the table exists on the blockchain
            ret_val, create_stmt = member_cmd.blockchain_get(status, cmd_get_table, blockchain_file_, True)
            if ret_val:
                # Error in blockchain process
                status.add_error( "Operator failed to retrieve info from blockchain on table '%s.%s'" % (dbms_name, table_name))
            else:
                if create_stmt:
                    if trace_level > 1:
                        utils_print.output("[Operator] [Table exists on blockchain] [%s.%s]" % (dbms_name, t_name), True)

                    ret_val = process_status.SUCCESS  # The table is in the blockchain
                else:
                    # The table is not in the blockchain
                    # Create from the local database schema
                    if trace_level > 1:
                        utils_print.output("[Operator] [Table does not exists on blockchain] [%s.%s]" % (dbms_name, t_name), True)

                    if is_create_table_:
                        create_stmt = db_info.generate_create_stmt(status, dbms_name, table_name)
                        if create_stmt:
                            # Add create statement on the blockchain
                            ret_val = add_table_on_blockchain(status, io_buff, table_name, dbms_name, create_stmt, "Local schema from %s" %  net_utils.get_external_ip_port(), trace_level)
                            if trace_level > 1:
                                utils_print.output("[Operator] [Table created on blockchain] [%s.%s]" % (dbms_name, t_name), True)

                        if ret_val or not create_stmt:
                            status.add_error("Local database includes table '%s.%s' and blockchain does not include the table definition" % (dbms_name, table_name))
                            ret_val = process_status.Local_table_not_in_blockchain
                    else:
                        status.add_error("Operator 'create_table' flag is set to 'false' with table: '%s.%s' not declared on the blockchain" % (
                            dbms_name, table_name))
                        ret_val = process_status.Operator_not_allowed_with_table

        else:
            # Test automated creation of tables
            if is_create_table_:
                if trace_level > 1:
                    utils_print.output("[Operator] [Create table on local database and blockchain] [%s.%s]" % (dbms_name, t_name), True)
                # Create the table if it does not exists - This process is done for every processed file (SQL or JSON)
                ret_val = create_new_table(status, io_buff, file_name, dbms_name, table_name, is_partitioned, par_name, with_tsd_info, instructions, trace_level)
            else:
                status.add_error("Operator failed to identify table and 'create_table' flag is set to 'false' with table: '%s.%s'" % (dbms_name, table_name))
                ret_val = process_status.Operator_not_allowed_with_table

    return ret_val


# -----------------------------------------------------------------
# Create a new table
# cmd_get_table = ["blockchain", "get", "table", "where", "dbms", "=", "dbms", "and", "name", "=", "table",\
#                  "bring", "[table][create]"]

# blockchain_push = ["run", "client",  "(operators)", "blockchain", "push" "json"]
# -----------------------------------------------------------------
def create_new_table(status, io_buff, file_name, dbms_name, table_name, is_partitioned, par_name, with_tsd_info, instructions, trace_level):
    global blockchain_file_

    cmd_get_table = ["blockchain", "get", "table", "where", "dbms", "=", dbms_name, "and", "name", "=", table_name, \
                     "bring", "[table][create]"]

    # test if the table exists on the blockchain
    ret_val, create_stmt = member_cmd.blockchain_get(status, cmd_get_table, blockchain_file_, True)

    if ret_val:
        # Table exists in blockchain
        status.add_error("Operator failed to retrieve info from blockchain on table %s.%s" % (dbms_name, table_name))
        return process_status.Inconsistent_declarations

    if create_stmt:
        declare_table = False
    else:

        if is_partitioned:
            # The table should be created based on the JSON file - it fails here on the SQL file
            status.add_error(
                "Operator failed to retrieve info from blockchain on table %s.%s" % (dbms_name, table_name))
            return process_status.Inconsistent_declarations

        if instructions and instructions != '0':
            # test if instructions define how to create the table
            instruct = member_cmd.get_instructions(status, instructions)
            if not instruct:
                # Failed to retrieve the mapping policy
                return process_status.Failed_to_retrieve_instructions
        else:
            instruct = None

        # The table does not exists in the blockchain - declare table based on JSON file
        create_stmt = suggest_create_table.suggest_create_table(status, file_name, dbms_name, table_name, with_tsd_info, instruct)

        if trace_level > 1:
            utils_print.output("[Operator] [Create Table from Data] [" + create_stmt + "]", True)

        if not create_stmt:
            return process_status.Failed_to_analyze_json

        create_stmt = utils_data.replace_string_chars(True, create_stmt, {'|': '"'})
        declare_table = True


    if declare_table:
        ret_val = add_table_on_blockchain(status, io_buff, table_name, dbms_name, create_stmt, "Processing JSON file", trace_level)

    if not ret_val:
        # Create the table on the local database
        if is_partitioned:
            sql_stmt = utils_sql.change_table_name_to_par("par_" + table_name + "_" + par_name, create_stmt)
        else:
            sql_stmt = create_stmt
        if not db_info.process_contained_sql_stmt(status, dbms_name, sql_stmt):
            status.add_error("Failed to prepare a query with a SQL stmt: " + sql_stmt)
            ret_val = process_status.ERR_SQL_failure

    return ret_val


# ------------------------------------------------------------------------------
# Add table declaration to the blockchain
# ------------------------------------------------------------------------------
def add_table_on_blockchain(status, io_buff, table_name, dbms_name, create_stmt, source_info, trace_level):

    global blockchain_file_
    global master_node_

    table_info = {"table": {  # The JSON table to update the blockchain
    }
    }

    # Note: if is_partitioned is True, declare_table is always set to False
    table_info["table"]["name"] = table_name
    table_info["table"]["dbms"] = dbms_name
    table_info["table"]["create"] = create_stmt
    table_info["table"]["source"] = source_info     # Info on the source that generated the schema


    ret_val = member_cmd.blockchain_insert_all(status, io_buff, table_info, True, blockchain_file_, [master_node_], platform_, False)

    return ret_val
