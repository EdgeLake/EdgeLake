"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import edge_lake.generic.process_status as process_status
import edge_lake.generic.version as version

# =======================================================================================================================
# Maintains the data and information on a node that participates in the query
# =======================================================================================================================

REPLY_HEADER = 0
REPLY_DATA = 1


class JobMember:

    def __init__(self):
        self.replies = ["" for y in range(2)]  # reply from each returned node

        self.returned_txt = ""          # A text message which is a reply to a command
        self.returned_json = False      # Set to True if text message is in JSON
        self.reset()

    def reset(self):
        self.returned_code = process_status.No_reply  # -1 indicates no replies

        self.last_block = False  # When last block is returned, this flag is set to True
        self.first_block = False    # is first block in the sequence
        self.ip = ""  # The member IP
        self.port = ""  # the member port
        self.incomplete_segment = ""  # A block segment which is incomplete (and will be comp[leted with the next block)

        self.message_counter = 0  # number of blocks transferred
        self.process_time = 0  # the time when the last reply was received
        self.time_dbms = 0  # time in seconds spend in the DBMS
        self.time_send = 0  # time in seconds representing the send time
        self.data_par = 1  # On par #0 - The number of partitions of the data with the operator
        self.counter_replies = 0  # On par #0 - The number of partitions that replied
        self.entries = 0  # the number of entries returned from the partition - ie. the number of rows
        self.password = None    # Decrypted using the provate key to enable symetric decryption of the data
        self.salt = None
        self.length_encrypt_key = 0 # The length of the key used to encrypt and decrypt the data
        self.data_encrypted = False # Will be set to True if the node returns an encryption key
        self.f_object = None    # Once the password and dalst are derived using async encryption, f_object is used to decrypt the data

        if len(self.returned_txt):
            self.returned_txt = ""     # A text message which is a reply to a command

    # =======================================================================================================================
    # Returns msg returned from node (string / JSON)
    # =======================================================================================================================
    def get_reply_msg(self):
        return self.returned_txt
    # =======================================================================================================================
    # Returns True if message returned from node in JSON format
    # =======================================================================================================================
    def is_msg_json(self):
        return self.returned_json
    # =======================================================================================================================
    # Save a node reply to AnyLog cmd which is not DATA (i.e. get status)
    # =======================================================================================================================
    def save_print_reply(self, node_ret_val, is_json, print_msg):

        self.returned_code = node_ret_val
        self.returned_json = is_json
        self.returned_txt += print_msg
        self.message_counter += 1

    # =======================================================================================================================
    # Retrieve from the first reply - the key to decrypt the data
    # =======================================================================================================================
    def set_decryption_passwords(self, status, encryption_key):

        password_text = version.al_auth_decrypt_node_message(status, encryption_key)
        if password_text:
            ret_val = process_status.SUCCESS
            self.password =  password_text[:16]
            self.salt = password_text[16:]
            self.length_encrypt_key = len(encryption_key)
            self.data_encrypted = True
            self.f_object = version.al_auth_generate_fernet(status, self.password, self.salt.encode())
        else:
            status.add_error("Failed to decrypt key received from node")
            ret_val = process_status.Decryption_failed

        return ret_val
    # =======================================================================================================================
    # Return the length of the encryption key.
    # =======================================================================================================================
    def get_length_encrypt_key(self):
        return  self.length_encrypt_key

    # =======================================================================================================================
    # Decrypt the data using Fernet
    # =======================================================================================================================
    def decrypt_data(self, status, data):
        return version.al_auth_symetric_decryption(status, self.f_object, data)
    # =======================================================================================================================
    # Return the length of the encryption key.
    # =======================================================================================================================
    def is_data_encrypted(self):
        return  self.data_encrypted
    # =======================================================================================================================
    # Update the number of entries returned from the partition - ie. the number of rows
    # =======================================================================================================================
    def set_reply_entries(self, counter):
        self.entries += counter

    # =======================================================================================================================
    # Get the number of entries returned from the partition - ie. the number of rows
    # =======================================================================================================================
    def get_reply_entries(self):
        return self.entries

    # =======================================================================================================================
    # On Par #0 - Count the number of partition replied
    # Return total number
    # =======================================================================================================================
    def count_replies(self):
        self.counter_replies += 1
        return self.counter_replies

    # =======================================================================================================================
    # Set the number of data partitions with this operator
    # =======================================================================================================================
    def set_data_partitions(self, counter_par):
        self.data_par = counter_par

    # =======================================================================================================================
    # Get the number of data partitions with this operator
    # =======================================================================================================================
    def get_data_partitions(self):
        return self.data_par  # Updated on partition #0

    # =======================================================================================================================
    # Set reply header
    # =======================================================================================================================
    def set_reply_header(self, reply_header):
        self.replies[REPLY_HEADER] = reply_header

    # =======================================================================================================================
    # Set reply data
    # =======================================================================================================================
    def set_reply_data(self, reply_data):
        self.replies[REPLY_DATA] = reply_data

    # =======================================================================================================================
    # get reply header
    # =======================================================================================================================
    def get_reply_header(self):
        return self.replies[REPLY_HEADER]

    # =======================================================================================================================
    # Set reply data
    # =======================================================================================================================
    def get_reply_data(self):
        return self.replies[REPLY_DATA]

    # =======================================================================================================================
    # Add time spend to query the database
    # =======================================================================================================================
    def set_dbms_time(self, dbms_time):
        self.time_dbms = dbms_time

    # =======================================================================================================================
    # Get total time spend to query the database
    # =======================================================================================================================
    def get_dbms_time(self):
        return self.time_dbms

    # =======================================================================================================================
    # set the time spend to send messages from the operator
    # =======================================================================================================================
    def set_send_time(self, send_time):
        self.time_send = send_time

    # =======================================================================================================================
    # Get time spend to send messages from the operatoe
    # =======================================================================================================================
    def get_send_time(self):
        return self.time_send

    # =======================================================================================================================
    # Set reurned code
    # =======================================================================================================================
    def set_returned_code(self, ret_code: int):
        self.returned_code = ret_code

    # =======================================================================================================================
    # Get reurned code
    # =======================================================================================================================
    def get_returned_code(self):
        return self.returned_code

    # =======================================================================================================================
    # Set the time that the last reply was received
    # =======================================================================================================================
    def set_process_time(self, ptime):
        self.process_time = int(ptime)

    # =======================================================================================================================
    # Get the time that the last reply was received
    # =======================================================================================================================
    def get_process_time(self):
        return self.process_time

    # =======================================================================================================================
    # Increase the count of messages received from this node
    # =======================================================================================================================
    def incr_message_counter(self):
        self.message_counter += 1

    # =======================================================================================================================
    # Get the count of messages received from this node
    # =======================================================================================================================
    def get_message_counter(self):
        return self.message_counter

    # =======================================================================================================================
    # Is the first block to be processed
    # =======================================================================================================================
    def set_is_first_block(self, status):
        self.first_block = status

    # =======================================================================================================================
    # Return True if the block is the first block in the sequence
    # =======================================================================================================================
    def is_first_block(self):
        return self.first_block

    # =======================================================================================================================
    # Set block as last block to be returned with True, or a block which is not last in a sequence of blocks with False
    # =======================================================================================================================
    def set_is_last_block(self, status: bool):
        self.last_block = status

    # =======================================================================================================================
    # Return True if the block is the last block in the sequence
    # =======================================================================================================================
    def is_last_block(self):
        return self.last_block

    # =======================================================================================================================
    # set the IP and port
    # =======================================================================================================================
    def set_ip_port(self, operator_ip, operator_port):
        self.ip = operator_ip
        self.port = operator_port

    # =======================================================================================================================
    # Get the IP
    # =======================================================================================================================
    def get_ip(self):
        return self.ip

    # =======================================================================================================================
    # Get the Port
    # =======================================================================================================================
    def get_port(self):
        return self.port

    # =======================================================================================================================
    # Get the IP and Port
    # =======================================================================================================================
    def get_ip_port(self):
        return self.ip + ":" + self.port

    # =======================================================================================================================
    # Add data to incomplete segment - block x provided incomplete segment, block x + 1 adding the missing data
    # =======================================================================================================================
    def add_to_incomplete_segment(self, added_data):
        self.incomplete_segment += added_data

    # =======================================================================================================================
    # Get the incomplete segment
    # =======================================================================================================================
    def get_incomplete_segment(self):
        return self.incomplete_segment

    # =======================================================================================================================
    # Reset the incomplete segment
    # =======================================================================================================================
    def reset_incomplete_segment(self):
        self.incomplete_segment = ""
