"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import sys
import threading

import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.generic.params as params
import edge_lake.generic.process_status as process_status
import edge_lake.generic.process_log as process_log
import edge_lake.generic.interpreter as interpreter
import edge_lake.generic.utils_io as utils_io
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.utils_queue as utils_queue
import edge_lake.generic.events as events
from edge_lake.generic.utils_columns import get_current_time_in_sec
from edge_lake.dbms.db_info import blockchain_select
import edge_lake.blockchain.bplatform as bplatform

sync_time_ = -1

counter_process_ = 0       # The number of times the sync loop was processed

status_queue_ = utils_queue.MsgQueue(0)

source_ = "Not declared"        # Should change to master or blockchain
connection_ = "Not declared" # Should change to IP and port

# ------------------------------------------------------------------------------------------------
# Blockchain Stat is a class that determines if the blockchain data on the local file was modified
# ------------------------------------------------------------------------------------------------
class BlockchainStat():
    def __init__(self, blockchain_file, buff_counter):
        self.status_mutex = threading.Lock()  # mutex to avoid same id to multiple scripts
        self.state_buff = [None] *   buff_counter   # An array to maintain a state
        self.buff_counter = buff_counter            # the number of buffers to store user data
        self.blockchain_file = blockchain_file  # Path and file name
        self.file_time = 0  # The time the file was last modified
        self.hash_value = '0'  # The hash value of the file
        self.process_time = 0  # avoid updating status within time intervals less than 30 seconds
        self.force_load = False # Set to true when node is transformed to an Operator and need to consider the policy ID
        for x in range (self.buff_counter):
            self.state_buff[x] = None

    # ------------------------------------------------------------------------------------------------
    # Test if the file was modified by considering the file time.
    # If the file time was changed -> calculate the hash value to find if a new version of the file
    # If file was modified - updte the hash value representig the file.
    # ------------------------------------------------------------------------------------------------
    def update_status(self, status):
        ret_val = False
        current_time = get_current_time_in_sec()
        if (current_time - self.process_time) > 30:       # More than 30 seconds than last validation
            self.process_time = current_time
            # get the time that the local file was modified
            new_time = utils_io.get_file_modified_time(self.blockchain_file)
            if self.file_time != new_time or not self.file_time:
                # Take a mutex such that 2 competing processes, one would get true and the second false.
                self.status_mutex.acquire()
                new_time = utils_io.get_file_modified_time(self.blockchain_file)    # Try again with the mutex taken
                if self.file_time != new_time or not self.file_time:
                    # The local JSON file was updated
                    self.file_time = new_time
                    got_hash, new_hash_value = utils_io.get_hash_value(status, self.blockchain_file, "blockchain", None)
                    if new_hash_value != self.hash_value or not new_hash_value:
                        # Update the local database
                        ret_val = True
                    self.hash_value = new_hash_value

                self.status_mutex.release()

        return ret_val

    # ------------------------------------------------------------------------------------------------
    # Update file status
    # ------------------------------------------------------------------------------------------------
    def new_file_reset(self, status, blockchain_file):

        self.blockchain_file = blockchain_file  # Path and file name

        # The time the file was last modified
        self.file_time = utils_io.get_file_modified_time(self.blockchain_file)

        # The hash value of the file
        got_hash, self.hash_value = utils_io.get_hash_value(status, self.blockchain_file, "blockchain", None)

        # avoid updating status within time intervals less than 30 seconds
        self.process_time = get_current_time_in_sec()

        for x in range (self.buff_counter):
            self.state_buff[x] = None       # Reset the buffers

    # ------------------------------------------------------------------------------------------------
    # Return the Hash value of the blockchain file
    # ------------------------------------------------------------------------------------------------
    def get_hash_value(self):
        return self.hash_value
    # ------------------------------------------------------------------------------------------------
    # Return the Path and Name of the local blockchain file
    # ------------------------------------------------------------------------------------------------
    def get_blockchain_file(self):
        return self.blockchain_file
    # ------------------------------------------------------------------------------------------------
    # Store a sate on the object
    # ------------------------------------------------------------------------------------------------
    def set_state(self, index, state_obj):
        self.state_buff[index] = state_obj
    # ------------------------------------------------------------------------------------------------
    # Get the state from the object
    # ------------------------------------------------------------------------------------------------
    def get_state(self, index):
        return self.state_buff[index]


    # ------------------------------------------------------------------------------------------------
    # When this node is declared as an operator - the metadata is forced to be reloaded
    # ------------------------------------------------------------------------------------------------
    def set_force_load(self):
        self.force_load = True

    # ------------------------------------------------------------------------------------------------
    # If file name is replaced, reset values
    # ------------------------------------------------------------------------------------------------
    def set_blockchain_file(self, status, blockchain_file):

        if self.force_load:
            # Set to true when node is transformed to an Operator and need to consider the policy ID
            self.force_load = False
            was_modified = True
        else:
            if not self.blockchain_file or self.blockchain_file != blockchain_file:
                # Blockchain file name not available or modified
                self.new_file_reset(status, blockchain_file)
                was_modified = True
            else:
                was_modified = self.update_status(status) # test the file time to see if it was modified

        return was_modified


blockchain_stat = BlockchainStat(None, 0)  # An object that monitors if the blockchain file was modified

# ------------------------------------------------------------------------------------------------
# Replace the connection info. This process is the result of the command:
#  blockchain switch network where master = 23.239.12.151:2048
# ------------------------------------------------------------------------------------------------
def switch_network(connect_ip_port):
    global  connection_
    connection_ = str(connect_ip_port)
# --------------------------------------------------------------
# Return the connection info: Blockchain platform or IP and port of master node
# --------------------------------------------------------------
def get_connection():
    global connection_
    return connection_
# --------------------------------------------------------------
# Return the hash value of the blockchain JSON file as the version number
# --------------------------------------------------------------
def get_version():
    return blockchain_stat.get_hash_value()
# --------------------------------------------------------------
# Return True if running
# --------------------------------------------------------------
def is_running():
    global sync_time_
    return  sync_time_ != -1
# --------------------------------------------------------------
# Return the sync time
# --------------------------------------------------------------
def get_sync_time():
    global sync_time_
    return  sync_time_
# ------------------------------------------------------------------
# Return Info on the service - called by the commands:
# get processes
# get synchronizer
# ------------------------------------------------------------------
def get_info(status):
    global sync_time_
    global source_          # Master or Blockchain
    global connection_      # IP and Port

    if is_running():
        info_str = status_queue_.get_indexed_msg( [("{A1}",str(sync_time_)),  ("{A2}",source_) , ("{A3}",connection_) ])
    else:
        info_str = ""
    return info_str
# --------------------------------------------------------------
# Synchronize the blockchain data -
# retrieve the data from a source: Blockchain, Node, Database
# Using a Master Node - the message is processed in events.blockchain_master_to_node()
# Example: run blockchain sync where source = master and time = 15 seconds and dest = file and connection = !ip_port
#
# run blockchain sync where source = blockchain and platform = ethereum and time = 30 seconds and dest = file
#
# Debug using:  trace leve = 2 run blockchain sync
# 1) Level 2 debug is placed on the message to the MASTER
# 2) When the master gets the message, it replies from events.blockchain_master_to_node()
#               with:  "Master copied blockchain file with hash value: 5e36884570b0f4f7e5212366f1cd3229"
# 3) When file is copied, member_cmd.write_data_block() prints a message:


# --------------------------------------------------------------
def synchronizer(dummy: str, conditions: dict):
    '''
    A background process to retrieve the ledger from the blockchain platform (or a master node)
    '''
    global sync_time_
    global source_
    global connection_
    global counter_process_

    status = process_status.ProcessStat()

    status_info = [
        "Sync every {A1} seconds with {A2} using: {A3}",
        "Failed to connect to {A2} using: {A3}",
    ]

    status_queue_.set_static_queue(status_info)

    destination = conditions["dest"][0]  # destination of the sync - file, dbms or both
    is_file = interpreter.test_one_value(conditions, "dest", "file")  # is sync to file
    is_dbms = interpreter.test_one_value(conditions, "dest", "dbms")  # is sync to dbms

    # get the location of the blockchain file - either provided by the conditions or use !blockchain_file
    blockchain_file = interpreter.get_one_value(conditions, "file")
    # from the blockchain file, get the filename and location for the file copied from the Master
    ret_val, dest_file = utils_io.get_writeable_file_name(status, blockchain_file, "new")
    if ret_val:
        process_log.add_and_print("event", "synchronizer process failed - error with blockchain destination")
        return

    dest_file = dest_file.replace(' ', '\t')

    source_ = conditions["source"][0]
    if source_ == "blockchain":
        connection_ = interpreter.get_one_value(conditions, "platform") # The name of the blockchain platform
        master_msg_array = ["blockchain", "checkout", "from", connection_]


    elif source_ == "master":
        connection_ = interpreter.get_one_value(conditions, "connection")  # IP and port to the Master

        # This is a message to bring the blockchain data to a destination file on the local node
        master_msg = "run client (%s) event get_blockchain %s 0 0" % (connection_, dest_file)
        master_msg_array = master_msg.split(' ')

    if is_dbms:
        update_dbms = ["blockchain", "update", "dbms", blockchain_file, "ignore", "message"]

    sync_time_ = conditions["time"][0]  # set in a global variables only of process starts

    size = params.get_param("io_buff_size")
    buff_size = int(size)
    data_buffer = bytearray(buff_size)

    blockchain_stat = BlockchainStat(blockchain_file, 0)   # An object that monitors if the blockchain file was modified
    trn_count = 0       # number of transactions on the blockchain - to determine if sync is needed
    while 1:


        if process_status.is_exit("synchronizer"):
            process_log.add_and_print("event", "synchronizer process terminated")
            sync_time_ = -1
            break

        trace_level = member_cmd.commands["run blockchain sync"]['trace']
        if source_ == "master":
            if trace_level == 2:
                # Set trace on the Master Node when this message is processed
                master_msg_array[-1] = '2'
            else:
                # Disable trace on the Master
                master_msg_array[-1] = '0'


        hash_value = blockchain_stat.get_hash_value()       # The Hash value of the current blockchain file

        if source_ == "master":

            # The mutex Prevent race condition with events.use_new_blockchain_file()
            # Waiting for finishing previous new ledger processing before asking for a ledger update
            utils_io.write_lock("new")   # This mutex prevent a request for a file when previous file is processes

            try:

                master_msg_array[2] = "(%s)" % connection_      # conection may change using "blockchain switch netrun blockchaiwork" command

                master_msg_array[-2] =  hash_value # The hash value of the existing file is send to the master such that it is not send back if unchanged
                ret_val = member_cmd.run_client(status, data_buffer, master_msg_array, trace_level)  # --> Request the Blockchain from the Master Node
                if ret_val:
                    status_queue_.set_index(1)      # Set status on message: "Failed to connect to master on: {2}"
                    error_msg = f"Synchronizer process failed to connect to Master using {connection_} with error: '{process_status.get_status_text(ret_val)}'"
                    if member_cmd.echo_queue:
                        member_cmd.echo_queue.add_msg(error_msg)
                    else:
                        utils_print.output("\r\n" + error_msg, True)  # print error message
                else:
                    status_queue_.set_index(0)  # Set status on message: "Sync every {A1} seconds with {A2} on: {A3}"
            except:
                errno, value = sys.exc_info()[:2]
                message = f"Synchronizer failure in the process requesting a new ledger from the Master (bsync.synchronizer()) errno: '{str(errno)}', value: '{str(value)}'"
                status.add_error(message)
                utils_print.output(message, True)

            utils_io.write_unlock("new")

        elif source_ == "blockchain":
            ret_val = sync_by_blockchain(status, data_buffer, master_msg_array)

        if is_dbms:
            is_modified = blockchain_stat.update_status(status)
            if is_modified:
                # If the blockchain file was modified -> Update the local blockchain file with the new policies
                member_cmd.blockchain_update_dbms(status, update_dbms, blockchain_file, 0)

        counter_process_ += 1          # Count the number of looping that was done

        process_status.sleep_test_signal(sync_time_, "synchronizer")


# --------------------------------------------------------------
# On the Master node
# Master Synchronizer - UPDATE the local blockchain JSON file from the local dbms
# Example: run blockchain sync where source = dbms and time = 30 seconds and dest = file
# --------------------------------------------------------------
def master_synchronizer(dummy: str, conditions: dict):
    global sync_time_
    global counter_process_
    global source_
    global connection_

    hash_val = None

    status = process_status.ProcessStat()

    status_info = [
        "Sync every {A1} seconds to local blockchain file",
    ]

    status_queue_.set_static_queue(status_info)

    sync_time_ = conditions["time"][0]  # set in a global variables only of process starts
    source_ = "DBMS"
    connection_ = "table blockchain.ledger"

    blockchain_file = interpreter.get_one_value(conditions, "file")

    # move the blockchain to destination
    path, name, type = utils_io.extract_path_name_type(blockchain_file)
    new_file = path + name + ".local"
    old_file = path + name + ".old"

    while 1:

        if process_status.is_exit("synchronizer"):
            ret_val = process_status.EXIT
            break

        if not blockchain_select(status, "json", new_file, None):
            ret_val = process_status.BLOCKCHAIN_operation_failed
            break

        got_hash, new_hash_val = utils_io.get_hash_value(status, new_file, None, None)

        if hash_val == None or new_hash_val != hash_val:
            # New blockchain file
            ret_val = events.use_new_blockchain_file(status, None, old_file, blockchain_file, new_file, False, 0)
            if ret_val:
                break
            ret_val, dummy = member_cmd.blockchain_load_metadata(status, None, None, 0, None)
            if ret_val:
                if ret_val != process_status.Needed_policy_not_available:
                    break
                else:
                    # No need to exit on the master as the master does not need the cluster policies
                    ret_val = 0
            hash_val = new_hash_val

        counter_process_ += 1

        process_status.sleep_test_signal(sync_time_, "synchronizer")


    sync_time_ = -1
    process_log.add_and_print("event", "Synchronizer process terminated: %s" % process_status.get_status_text(ret_val))

# --------------------------------------------------------------
# Sync the local metadata by the blockchain platform
# --------------------------------------------------------------
def sync_by_blockchain(status, data_buffer, master_msg_array):

    blockchain_file = params.get_value_if_available("!blockchain_file")  # The local file name
    if not blockchain_file:
        status.add_error("Param \"blockchain_file\" is not defined")
        ret_val = process_status.No_local_blockchain_file
    else:
        # blockchain checkout from ethereum
        master_msg_array[3] = connection_
        ret_val, data = member_cmd.blockchain_checkout(status, data_buffer, master_msg_array, 0,
                                                       [0, 0, 0])  # The hash value determines a file copy is needed
        if ret_val:
            status_queue_.set_index(1)  # Set status on message: "Failed to connect to master on: {2}"
        else:
            status_queue_.set_index(0)  # Set status on message: "Sync every {A1} seconds with {A2} on: {A3}"

            # Write the data to a new file in the local blockchain dir, example name: 'D:\\Node\\EdgeLake\\blockchain\\blockchain.ethereum_id_2.new'
            path, name, type = utils_io.extract_path_name_type(blockchain_file)
            tmp_file_name = f"{path}{connection_}.bchain.id_{counter_process_ % 10 + 1}.new"     # Intermidiary file for the blockchain data
            if not utils_io.write_str_to_file(status, data, tmp_file_name):
                status.add_error(f"Failed to write ledger data to file {tmp_file_name}")
                ret_val = process_status.File_write_failed
            else:
                info = [tmp_file_name]
                ret_val = events.blockchain_use_new(status, data_buffer, info, 0)

    return ret_val
# --------------------------------------------------------------
# Is Master node
# --------------------------------------------------------------
def is_master():
    global source_
    return source_ == "master"
# --------------------------------------------------------------
# Return Master or Blockchain or Not declared
# --------------------------------------------------------------
def get_source():
    global source_
    return source_
# --------------------------------------------------------------
# Return the counter of sync processes
# --------------------------------------------------------------
def get_counter_process():
    global counter_process_
    return counter_process_