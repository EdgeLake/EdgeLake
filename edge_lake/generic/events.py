"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import time

import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.tcpip.message_header as message_header
import edge_lake.generic.params as params
import edge_lake.generic.process_status as process_status
import edge_lake.dbms.db_info as db_info
import edge_lake.generic.utils_io as utils_io
import edge_lake.blockchain.blockchain as blockchain
import edge_lake.generic.interpreter as interpreter
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_print as utils_print
import edge_lake.blockchain.metadata as metadata
import edge_lake.dbms.ha as ha

from edge_lake.generic.utils_threads import seconds_sleep
#                                  Must     Add      Is
#                                  exists   Counter  Unique
drop_par = {"dbms_name": ("str", True, False, True),
            "max_size": ("int.storage", True, False, True),
            }

in_merge_ = False         # A flag indicating a merge process

partitions_dropped = {}  # statistics on partitions dropped

blockchain_transfers = {} # keeps hash value and time as f(ip:port)

copy_id_on_master = 0  # a unique ID identifying the file being copied (such that multiple files are not overwritten because of race conditions)
# ----------------------------------------------------------
# Process on the Master node - Copy the blockchain to destination node
# This event is triggered when a node sends a "get_blockchain" message from bsync.synchronizer

# Detailed debug: trace level = 2 run blockchain sync
# ----------------------------------------------------------
def blockchain_master_to_node(status, io_buff_in, data, trace):
    '''
    On the Master node:
    1) Pull the ledger data from the database
    2) Compare the hash value of the ledger retrieved to the hash value on the node to determine if different
        - if not different no need to copy the ledger
    3) Copy the ledger to the requesting node
    4) Send an event (blockchain_use_new) to notify that the blockchain was copied
    '''

    global blockchain_transfers     # keeps hash value and time as f(ip:port)
    global copy_id_on_master # a unique ID identifying the file being copied (such that multiple files are not overwritten because of race conditions)


    trace_cmd = ["run", "client", "(destination)", "print", "trace msg"] # reply with trace info - the caller is configured with: "trace level = 2 run blockchain sync"
    trace_message = ""

    mem_view = memoryview(io_buff_in)
    ip, port = message_header.get_source_ip_port(mem_view)

    source_file = params.get_value_if_available("!blockchain_file")
    if not source_file:
        status.add_error("Param \"blockchain_file\" is not defined and coded event \"get_blockchain\" failed")
        ret_val = process_status.No_local_blockchain_file
        caller_trace_level = 0
    else:

        if not data or len(data) != 3:
            status.add_error("Coded event \"get_blockchain\" failed: destination file name not provided")
            ret_val = process_status.Missing_file_name
            caller_trace_level = 0
        else:
            caller_file_name = data[0]  # The location of the JSON file ob the caller machine
            caller_hash_value = data[1]  # The hash value of the existing JSON file on the caller machine
            caller_trace_level = data[2]    # Trace level on the caller - the caller is configured with: "trace level = 2 run blockchain sync"

            file_path, file_name, file_type = utils_io.extract_path_name_type(source_file)
            new_file = file_path + file_name + ".new"

            # Pull the ledger data from the database


            if db_info.blockchain_select(status, "json", new_file, "new"):
                # Pull the blockchain data from a local database
                 # test struct of puled blockchain
                ret_val = blockchain.validate_struct(status, new_file)
                if not ret_val:
                    # Lock the blockchain.new file and determine the hash value
                    ret_code, new_hash_value = utils_io.get_hash_value(status, new_file, "new", None)
                    if ret_code == True and new_hash_value and new_hash_value != caller_hash_value:
                        # Copy the blockchain - if the hash value of the file is different than the Hash value of the file at the caller node

                        # because of race condition: File is not copied if the same hash value was delivered within the last 15 seconds
                        ip_port = ip + ":" + str(port)
                        if ip_port in blockchain_transfers:
                            # not the first transfer to this node
                            transferred_hash, transferred_time = blockchain_transfers[ip_port]
                            if new_hash_value == transferred_hash and (int(time.time()) - transferred_time) <= 5:
                                time.sleep(5)  # The caller has no file - give time for the previous update to avoid race conditions

                        blockchain_transfers[ip_port] = (new_hash_value, int(time.time()))

                        if copy_id_on_master >= 9:
                            copy_id_on_master = 1    # Restart as we want to limit files on disk (if files are not deleted)
                        else:
                            copy_id_on_master += 1  # a unique ID identifying the file being copied (such that multiple files are not overwritten because of race conditions)

                        f_type = utils_io.extract_file_type(caller_file_name)
                        len_type = len(f_type)
                        if len_type:
                            name_on_dest = caller_file_name[:-len_type] + "master_id_%u.%s" % (copy_id_on_master, f_type)
                        else:
                            name_on_dest = caller_file_name + ".master_id_%u" % copy_id_on_master

                        ret_val = member_cmd.copy_file(status, [(ip, str(port))], [new_file, name_on_dest, "ledger"], 0, trace)
                        if not ret_val:
                            if caller_trace_level == '2':
                                trace_message = f"Master copied blockchain file with hash value: {new_hash_value}"
                        elif caller_trace_level == '2':
                            err_text = process_status.get_status_text(ret_val)
                            trace_message = f"Master Process: Failed to coy file from Master to Node with error {ret_val} ({err_text})"
                    else:
                        if caller_trace_level == '2':
                            if ret_code == False or not new_hash_value:
                                trace_message = "Master Process: Failed to calculate Hash value of ledger"
                            elif new_hash_value == caller_hash_value:
                                trace_message = f"Master Process: Hash value of ledger was not changed: '{new_hash_value}'"
                else:
                    if caller_trace_level == '2':
                        trace_message = "Master Process: Ledger in Master DBMS is with invalid format"
            else:
                ret_val = process_status.Failed_to_pull_blockchain
                if caller_trace_level == '2':
                    trace_message = "Master Process: Failed to pull ledger from Master local DBMS"

    if caller_trace_level == '2':
        if trace_message:
            trace_cmd[2] = "(%s:%s)" % (ip, str(port))
            trace_cmd[-1] = trace_message
            member_cmd.run_client(status, io_buff_in, trace_cmd, trace)
        
    if ret_val:
        info_string = process_status.get_status_text(ret_val)  # send the error to the caller node
        ret_val = member_cmd.send_display_message(status, ret_val, "echo", io_buff_in, info_string, False)

    return ret_val


# ----------------------------------------------------------
#  Process on the Local node - use the new ledger
#  Info on the active ledger which is missing on the new ledger is copied to the new ledger (and send to the blockchain platform)
#  Active ledger is renamed to .old: blockchain.json --> blockchain old
#  New ledger becomes the active: blockchain.new --> blockchain.json
# ----------------------------------------------------------
def blockchain_use_new(status, io_buff_in, info, trace):
    '''
    A message send from the master node when a new copy of the ledger was copied to the node.

    The new copied ledger has a unique name.
    This process makes the new ledger the active ledger
    '''
    blockchain_file = params.get_value_if_available("!blockchain_file")
    if not blockchain_file:
        status.add_error("Param \"blockchain_file\" is not defined and coded event \"get_blockchain\" failed")
        ret_val = process_status.No_local_blockchain_file
    else:
        # move the blockchain to destination
        path, name, type = utils_io.extract_path_name_type(blockchain_file)
        old_file = path + name + ".old"
        new_file = info[0]
        ret_val = use_new_blockchain_file(status, io_buff_in, old_file, blockchain_file, new_file, True, trace)

        ret_val = member_cmd.blockchain_load(status, ["blockchain", "get", "cluster"], False, trace)    # Build the metadata layer with the new blockchain file

    return ret_val


# --------------------------------------------------------------
# File Sync - Replace the local ledger with a new ledger.
# On the file system, 3 files are maintained: an old ledger, a new ledger and the active ledger.
# The process:
# Delete old ledger
# move active ledger to old.
# move new to active.
# If merge ledger is on - copy the "local" policies to the new ledger.
# The local policies are policies updated localy but not represented on the new blockchain.
# Update the source platform with the local policies.
# --------------------------------------------------------------
def use_new_blockchain_file(status, io_buff_in, old_file, blockchain_file, new_file, merge_ledgers, trace):
    '''
    A message to replace the local ledger with a new ledger

    status - the process status object
    old_file - path to the oldest copy of the ledger
    blockchain_file - the path to the active local ledger
    new_file - the path to the new copy of the ledger
    merge_ledgers - move policies with "local" value (key "ledger") from the
                    active ledger to the new ledger + update source platform
    '''

    global in_merge_

    if trace:
        utils_print.output( f"New Blockchain: Sync with new ledger at: {new_file}", True)

    utils_io.write_lock("new")

    if in_merge_:
        # A different thread is doing merge
        # We don't want this thread to be on wait as if multiple threads are on wait, we may get them in the wrong order.
        # Ignore this process and wait to the next thread
        utils_io.write_unlock("new")
        return process_status.SUCCESS   # The next sync will get the updates

    in_merge_ = True

    utils_io.write_unlock("new")

    if merge_ledgers:
        # Test if the old file includes updates not included on the new file.
        # If the answer is Yes --> add the info to the new file + update the blockchain platform
        # This process validates the structure of the new file
        ret_val = blockchain.merge_ledgers(status, io_buff_in, new_file, blockchain_file, trace)
        if trace:
            message = process_status.get_status_text(ret_val)
            utils_print.output(f"New Blockchain: Merged status: {message}", True)

    else:
        ret_val = blockchain.validate_struct(status, new_file)


    if ret_val:
        if ret_val == process_status.Empty_Local_blockchain_file:
            # The file was not yet received, wait for next time
            ret_val = process_status.SUCCESS
        else:
            status.add_error("Event blockchain_use_new: Error or missing new blockchain file at '%s'" % new_file)
    else:
        # new file structure validated
        if utils_io.is_path_exists(old_file):
            # delete the old if the new file exists

            # Validate if a file exists on the OS
            for _ in range(2):
                test_result = utils_io.is_path_exists(new_file)
                if not test_result:
                    # The OS system does not recognize the file - can be timing issue
                    seconds_sleep("Wait for new blockchain file to be recognized", 1)
                else:
                    break
            if not test_result:
                # The blockchain file is not available even after 2 seconds new_file
                status.add_error("New blockchain file is not available: %s" % new_file)
                ret_val = process_status.ERR_file_does_not_exists
            else:
                if not utils_io.delete_file(old_file):
                    status.add_error("Event blockchain_use_new: Failed to delete file: %s" % old_file)
                    ret_val = process_status.Failed_to_delete_file

        if not ret_val:

            utils_io.write_lock("blockchain")

            if utils_io.is_path_exists(blockchain_file):
                # move current --> old
                if not utils_io.rename_file(status, blockchain_file, old_file):
                    status.add_error("Event blockchain_use_new: Faild to rename file: %s" % old_file)
                    ret_val = process_status.Failed_to_rename_file

            if not ret_val:
                # move "new" --> blockchain

                if not utils_io.rename_file(status, new_file, blockchain_file):
                    # could be OS timing issue
                    seconds_sleep("Rename of new blockchain file failed - wait and retry", 2) # Address a bug in windows
                    if not utils_io.rename_file(status, new_file, blockchain_file):
                        utils_io.rename_file(status, old_file, blockchain_file) # Revert the files
                        status.add_error("Event blockchain_use_new: Failed to rename file: %s to active blockchain file, precess reverted" % old_file)
                        ret_val = process_status.Failed_to_rename_file


            utils_io.write_unlock("blockchain")


    in_merge_ = False

    return ret_val

# ----------------------------------------------------------
# Process to delete the oldest partitions
# command: event drop_old_partitions where dbms_name = lsl_demo and max_size = 10MB
# ----------------------------------------------------------
def drop_old_partitions(status, io_buff_in, data, trace):
    global drop_par
    global partitions_dropped

    if not data or data[0] != "where":
        return process_status.ERR_command_struct

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, data, 1, 0, drop_par, False)

    if ret_val:
        return ret_val

    dbms_name = interpreter.get_one_value(conditions, "dbms_name")
    max_size = interpreter.get_one_value(conditions, "max_size")

    # get the size of the data in the database
    reply = db_info.get_dbms_size(status, dbms_name)
    if reply.isnumeric():
        dbms_size = int(reply)
        if dbms_size > max_size:
            # drop old tables:
            tables_str = db_info.get_database_tables(status, dbms_name)
            if tables_str:
                tables_json = utils_json.str_to_json(tables_str)
                if not tables_json:
                    status.add_error("Failed to retrieve tables for DBMS: %s" % dbms_name)
                    return process_status.ERR_wrong_json_structure
                tables_list = tables_json["Structure.Tables." + dbms_name]
                for table in tables_list:
                    if "table_name" not in table.keys():
                        status.add_error("Failed to retrieve tables for DBMS: %s" % dbms_name)
                        return process_status.ERR_wrong_json_structure
                    table_name = table["table_name"]
                    # Get the number of partitions for the table
                    info_string = db_info.get_table_partitions(status, dbms_name, table_name, "count")
                    if info_string and info_string.isnumeric() and int(info_string) > 1:
                        # with partitioned data and more than one partition to this table
                        first_partition = db_info.get_table_partitions(status, dbms_name, table_name, "first")
                        if first_partition:
                            if not db_info.drop_table(status, dbms_name, first_partition):
                                status.add_error("Failed to drop partition: %s.%s" % (dbms_name, first_partition))
                                ret_val = process_status.Drop_partition_failed
                                # we do not break, as it will continue to next table
                            else:
                                # add statistics:
                                key = dbms_name + "." + table_name
                                if key in partitions_dropped.keys():
                                    partitions_dropped[key] += 1
                                else:
                                    partitions_dropped[key] = 1
    db_info.reclaim_dbms_space(status, dbms_name)

    return ret_val

# ----------------------------------------------------------
# Send a message to the operator nodes to stop processing a query
# Job_location - the location of the job in the list containing all jobs
# job_id - a unique id of the job
# ----------------------------------------------------------
def message_stop_job(status, io_buff_in, operators_list, job_location, job_id):

    event_stop_job = ["run", "client", "(destination)", "event", "job_stop", str(job_location), str(job_id)]

    for ip_port in operators_list:
        event_stop_job[2] = ip_port
        ret_val = member_cmd.run_client(status, io_buff_in, event_stop_job, 0)  # Send a message to stop the job
# ----------------------------------------------------------
# A message received on the operator to stop processing a message
# Job_location - the location of the job in the list containing all jobs
# job_id - a unique id of the job
# ----------------------------------------------------------
def job_stop(status, io_buff_in, data, trace):
    Job_location, job_id = data
    source_ip = message_header.get_source_ip(io_buff_in)
    member_cmd.set_job_stop(source_ip, int(Job_location), int(job_id))   # set a structure indicating that the job need to stop