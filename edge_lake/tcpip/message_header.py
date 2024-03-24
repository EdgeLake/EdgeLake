"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import edge_lake.generic.process_status as process_status
import edge_lake.generic.version as version

# Error codes are in process_status.py:

# =======================================================================================================================
# Header structure:
# A message is split into 3 parts - 1) Header 2) command 3) Data
# The header structure is defined below
# =======================================================================================================================
MAX_IP_LENGTH = 128
HASH_LENGTH = 16  # MD5 is using 128 bits (16 bytes)

OFFSET_BLOCK_HASH = 0  # 16 bytes with a hash value of the block
OFFSET_SOCKET_ID = OFFSET_BLOCK_HASH + HASH_LENGTH  # 2 bytes with an ID representing the socket on the sender node
OFFSET_SOCKET_TIME_ID = OFFSET_SOCKET_ID + 2  # 8 bytes as a timestamp
OFFSET_SOCKET_COUNTER = OFFSET_SOCKET_TIME_ID + 8  # 4 bytes for a counter of messages send from this socket
OFFSET_BLOCK_COUNTER = OFFSET_SOCKET_COUNTER + 4  # 4 bytes - a number representing the message counter
OFFSET_BLOCK_DATA_STRUCT = OFFSET_BLOCK_COUNTER + 4  # 1 byte which is a code representing the structure of the data in the block
OFFSET_FLAG_LAST = OFFSET_BLOCK_DATA_STRUCT + 1  # 1 byte set to a value if this is the last block in the sequence of blocks returned
OFFSET_FLAG_INFO_TYPE = OFFSET_FLAG_LAST + 1  # the type of info in the block: Query/Structure ...
OFFSET_ERROR = OFFSET_FLAG_INFO_TYPE + 1  # 2 byte indicating error
OFFSET_COMMAND_LENGTH = OFFSET_ERROR + 2  # 2 bytes indicating the size of the command
OFFSET_AUTHENTICATION_LENGTH = OFFSET_COMMAND_LENGTH + 2  # 2 bytes indicating the size of the authentication
OFFSET_DATA_LENGTH = OFFSET_AUTHENTICATION_LENGTH + 2  # 4 bytes indicating the size of the data part
OFFSET_COUNTER_JSONS = OFFSET_DATA_LENGTH + 4  # 2 bytes - counter for the number of JSON structures in the block

OFFSET_REPLY_FLAG = OFFSET_COUNTER_JSONS + 2  # 1 byte - set to a value which is not 0 if this is a reply to a message
OFFSET_JOB_LOCATION = OFFSET_REPLY_FLAG + 1  # 2 bytes - the location of the info in active_jobs array (value between 0 to JOB_INSTANCES)
OFFSET_JOB_ID = OFFSET_JOB_LOCATION + 2  # 4 bytes - unique job id (on this node)
OFFSET_COUNTER_RECEIVERS = OFFSET_JOB_ID + 4  # 2 bytes - the number of nodes received the message
OFFSET_RECEIVER_ID = OFFSET_COUNTER_RECEIVERS + 2  # 2 bytes - the id as a receiver (i.e. node #2 from 4 receivers)

OFFSET_PAR_USED = OFFSET_RECEIVER_ID + 2  # 2 bytes - the number of physical partitions that are used on the operator node to satisfy the query
OFFSET_PAR_ID = OFFSET_PAR_USED + 2  # 2 bytes - the ID of the physical partition
OFFSET_DBMS_TIME = OFFSET_PAR_ID + 2  # 4 bytes - the time in milliseconds that the database was processed
OFFSET_SEND_TIME = OFFSET_DBMS_TIME + 4  # 4 bytes - the time in milliseconds that messages are delivered

OFFSET_SOURCE_IP_LENGTH = OFFSET_SEND_TIME + 4  # 1 byte for source IP length
OFFSET_SOURCE_IP = OFFSET_SOURCE_IP_LENGTH + 1  # 39 bytes (max size for IPV6 - but aws and azure give a larger string
OFFSET_SOURCE_PORT = OFFSET_SOURCE_IP + MAX_IP_LENGTH  # 4 bytes for the source port

OFFSET_GENERIC_FLAG = OFFSET_SOURCE_PORT + 4  # 2 bytes as a generic flag

SIZE_OF_HEADER = OFFSET_GENERIC_FLAG + 2

GENERIC_USE_WATCH_DIR = 1       # Placed in the generic flag
LEDGER_FILE = 2                 # Ledger file copied
LARGE_MSG = 3                   # Message that does not fit a single block, transferred as a file

GENERIC_STRING_FORMAT = 1       # Print message is in string format
GENERIC_JSON_FORMAT = 2         # Print message is in JSON format


space = ord(' ')  # value for space
tab = ord('\t')  # Value for Tab

INFO_PREFIX_LENGTH = 3  # number of bytes for info that describes data being copied
OFFSET_SEGMENT_LENGTH = 0  # 2 bytes keeping the size of the structure copied
OFFSET_SEGMENT_COMPLETED = 2  # 1 byte keeping a flag if the entire structure is in the current block (1 - yes, 0 - no)

BLOCK_STRUCT_JSON_SINGLE = 1  # Flag indicating the block data is with a single JSON structure
BLOCK_STRUCT_JSON_MULTIPLE = 2  # Flag indicating the block data is with multiple JSON structures

BLOCK_INFO_RESULT_SET = 1  # the info in the block is the result set of a query
BLOCK_INFO_TABLE_STRUCT = 2  # the info in the block is the structure of a table
BLOCK_INFO_CONFIGURATION = 3
BLOCK_INFO_COMMAND = 4  # command (like SQL) send to the nodes
BLOCK_INFO_TABLES_IN_DBMS = 5  # the list of tables in a database
BLOCK_INFO_AUTHENTICATE = 6  # If authentication is enabled - requires user authentication

SEGMENT_COMPLETE = (b'\x00')
SEGMENT_PREFIX = (b'\x01')
SEGMENT_SUFIX = (b'\x02')


# =======================================================================================================================
# copy data to a buffer, return the size of the data copied
# This method is used when data is added to the block incrementally
# The structure of the data in the block:
# 2 bytes - the size of the data
# 1 byte with a flag: 0 - if only part of the data segment fits in the block. 1 - if all data segment fits in the block
# =======================================================================================================================
def copy_data(mem_view, data_encoded, bytes_not_copied):


    data_length = len(data_encoded)

    if bytes_not_copied:
        data_encoded = data_encoded[data_length - bytes_not_copied:]
        data_length = bytes_not_copied

    start_data_length = get_data_length(mem_view)  # the offset of the data in the block (after the SQL/command)

    start_offset = start_data_length + SIZE_OF_HEADER

    end_offset = start_offset + data_length + INFO_PREFIX_LENGTH

    if (start_offset + 10) > len(mem_view):
        return [True, data_length]  # return a flag if the block is full and number of bytes were not copied

    if end_offset > len(mem_view):
        copy_length = len(mem_view) - (start_offset + INFO_PREFIX_LENGTH)
        block_full = True

        mem_view[
        start_offset + OFFSET_SEGMENT_COMPLETED: start_offset + OFFSET_SEGMENT_COMPLETED + 1] = SEGMENT_PREFIX  # part of the data is in the block
        bytes_remain = end_offset - len(mem_view)
    else:
        copy_length = data_length
        if end_offset + 10 > len(mem_view):
            block_full = True  # need at least 10 bytes for the next segment
        else:
            block_full = False

        if bytes_not_copied:
            mem_view[
            start_offset + OFFSET_SEGMENT_COMPLETED: start_offset + OFFSET_SEGMENT_COMPLETED + 1] = SEGMENT_SUFIX  # The remainder from previous block
        else:
            mem_view[
            start_offset + OFFSET_SEGMENT_COMPLETED: start_offset + OFFSET_SEGMENT_COMPLETED + 1] = SEGMENT_COMPLETE  # entire segment is in the block
        bytes_remain = 0

    # place the size of the data segment in the block
    mem_view[start_offset + OFFSET_SEGMENT_LENGTH: start_offset + OFFSET_SEGMENT_LENGTH + 2] = copy_length.to_bytes(2,
                                                                                                                    byteorder='big',
                                                                                                                    signed=False)

    # Copy the data to the block
    mem_view[start_offset + INFO_PREFIX_LENGTH: start_offset + INFO_PREFIX_LENGTH + copy_length] = data_encoded[
                                                                                                   :copy_length]

    used_length = start_data_length + copy_length + INFO_PREFIX_LENGTH

    mem_view[OFFSET_DATA_LENGTH: OFFSET_DATA_LENGTH + 4] = used_length.to_bytes(4, byteorder='big', signed=False)

    return [block_full, bytes_remain]  # return a flag if the block is full and number of bytes were not copied


# =======================================================================================================================
# Return True if the first entry is the suffix of a row from the previous block
# =======================================================================================================================
def is_with_row_suffix(mem_view):
    # offset of the first segment in the block
    location = get_command_length(mem_view) + SIZE_OF_HEADER
    # 1 byte is indicating if the entire JSON in contained in the block
    segment_stat = int.from_bytes(
        mem_view[location + OFFSET_SEGMENT_COMPLETED: location + OFFSET_SEGMENT_COMPLETED + 1], byteorder='big',
        signed=False)
    if segment_stat == 2:
        # this is an extension of the previous block
        ret_val = True
    else:
        ret_val = False
    return ret_val


# =======================================================================================================================
# Flag indicating how the data is organized inside the block
# =======================================================================================================================
def set_block_struct(mem_view, block_struct_flag):
    mem_view[OFFSET_BLOCK_DATA_STRUCT:OFFSET_BLOCK_DATA_STRUCT + 1] = block_struct_flag.to_bytes(1, byteorder='big',
                                                                                                 signed=False)


# =======================================================================================================================
# Get the flag indicating how the data is organized inside the block
# =======================================================================================================================
def get_block_struct(mem_view):
    return int.from_bytes(mem_view[OFFSET_BLOCK_DATA_STRUCT:OFFSET_BLOCK_DATA_STRUCT + 1], byteorder='big',
                          signed=False)


# =======================================================================================================================
# Test if command does not fit into a block
# =======================================================================================================================
def is_large_command(mem_view, command, data, auth_data):
    return True if (SIZE_OF_HEADER + len(command) + len(data) + len(auth_data)) >= len(mem_view) else False

# =======================================================================================================================
# Encode command and place command in the command segment
# =======================================================================================================================
def prep_command(mem_view, command):
    """
    :param mem_view: Buffer to send to server
    :param command: Command to execute on server
    :return: Offset to place the data in the buffer
    """
    command_encoded = command.encode()
    command_length = len(command_encoded)

    # the length of command
    mem_view[OFFSET_COMMAND_LENGTH: OFFSET_COMMAND_LENGTH + 2] = command_length.to_bytes(2, byteorder='big',
                                                                                         signed=False)
    # the length of data segment
    mem_view[OFFSET_DATA_LENGTH:OFFSET_DATA_LENGTH + 4] = command_length.to_bytes(4, byteorder='big', signed=False)

    # Reset the authentication segment
    reset_authentication(mem_view)

    if SIZE_OF_HEADER + command_length > len(mem_view):
        set_error(mem_view, process_status.ERROR_DATA_SIZE_LARGER_THAN_BUFFER)
    else:
        mem_view[SIZE_OF_HEADER:SIZE_OF_HEADER + command_length] = command_encoded  # add command
    return SIZE_OF_HEADER + command_length  # return location of data

# =======================================================================================================================
# Reset the authentication information in the block
# =======================================================================================================================
def reset_authentication(mem_view):
    mem_view[OFFSET_AUTHENTICATION_LENGTH: OFFSET_AUTHENTICATION_LENGTH + 1] = (b'\x00')
    mem_view[OFFSET_AUTHENTICATION_LENGTH + 1: OFFSET_AUTHENTICATION_LENGTH + 2] = (b'\x00')


# =======================================================================================================================
# Get the length of the authentication segment
# =======================================================================================================================
def get_authentication_length(mem_view):
    return int.from_bytes(mem_view[OFFSET_AUTHENTICATION_LENGTH: OFFSET_AUTHENTICATION_LENGTH + 2], byteorder='big',
                          signed=False)
# =======================================================================================================================
# Get the public key from the authentication string (the prefix of the authentication string)
# =======================================================================================================================
def get_public_str(mem_view):

    length = int.from_bytes(mem_view[OFFSET_AUTHENTICATION_LENGTH: OFFSET_AUTHENTICATION_LENGTH + 2], byteorder='big',
                          signed=False)

    if length < version.get_public_key_chars():
        public_str = None
    else:
        start = get_data_offset_after_command(mem_view)

        public_str = bytes(mem_view[start:start + version.get_public_key_length()]).decode()

    return public_str
# =======================================================================================================================
# set authentication
# Set Authentication is organized as follows: Public string of the sender + signature of the sender  + [ip + port + time]
# =======================================================================================================================
def set_authentication(mem_view, auth_data):
    command_length = get_command_length(mem_view)

    auth_encoded = auth_data.encode()

    auth_length = len(auth_encoded)

    data_length = command_length + auth_length

    # the length of authentication segment
    mem_view[OFFSET_AUTHENTICATION_LENGTH:OFFSET_AUTHENTICATION_LENGTH + 2] = auth_length.to_bytes(2, byteorder='big',
                                                                                                   signed=False)
    if (SIZE_OF_HEADER + command_length + auth_length) > len(mem_view):
        set_error(mem_view, process_status.ERROR_DATA_SIZE_LARGER_THAN_BUFFER)
        ret_val = False
    else:
        mem_view[SIZE_OF_HEADER + command_length: SIZE_OF_HEADER + data_length] = auth_encoded  # add authentication
        mem_view[OFFSET_DATA_LENGTH: OFFSET_DATA_LENGTH + 4] = data_length.to_bytes(4, byteorder='big', signed=False)
        ret_val = True

    return ret_val  # return location of data

# =======================================================================================================================
# Add command to block, if command is larger than block, add the size of command that fits
# =======================================================================================================================
def insert_encoded_data(mem_view, data_encoded):
    if data_encoded:

        data_length = len(data_encoded)

        # Current size of the data segment
        # offset_new_data = int.from_bytes(mem_view[OFFSET_DATA_LENGTH : OFFSET_DATA_LENGTH + 4], byteorder='big',signed=False)
        command_length = get_command_length(mem_view)

        auth_length = get_authentication_length(mem_view)

        position_data = command_length + auth_length  # data is set after authentication

        if SIZE_OF_HEADER + position_data + data_length > len(mem_view):
            insert_length = len(mem_view) - (SIZE_OF_HEADER + position_data)
            offset = insert_length  # length added to block
        else:
            insert_length = data_length  # all fits to a block
            offset = 0

        mem_view[SIZE_OF_HEADER + position_data: SIZE_OF_HEADER + position_data + insert_length] = data_encoded[
                                                                                                   :insert_length]

        # updated size of the data segment
        updated_segment_length = position_data + insert_length

        mem_view[OFFSET_DATA_LENGTH: OFFSET_DATA_LENGTH + 4] = updated_segment_length.to_bytes(4, byteorder='big',
                                                                                               signed=False)  # add length cof command

        return offset


# =======================================================================================================================
# Encode data and place data in the data segment
# =======================================================================================================================
def prep_data(mem_view, data):
    if data:
        data_encoded = data.encode()
        data_length = len(data_encoded)

        # Current size of the data segment
        offset_new_data = int.from_bytes(mem_view[OFFSET_DATA_LENGTH: OFFSET_DATA_LENGTH + 4], byteorder='big',
                                         signed=False)

        # updated size of the data segment
        updated_segment_length = data_length + offset_new_data

        mem_view[OFFSET_DATA_LENGTH: OFFSET_DATA_LENGTH + 4] = updated_segment_length.to_bytes(4, byteorder='big',
                                                                                               signed=False)  # add length cof command

        if SIZE_OF_HEADER + updated_segment_length > len(mem_view):
            set_error(mem_view, process_status.ERROR_DATA_SIZE_LARGER_THAN_BUFFER)
        else:
            mem_view[
            SIZE_OF_HEADER + offset_new_data: SIZE_OF_HEADER + updated_segment_length] = data_encoded  # add the data to the data segment


# =======================================================================================================================
# Increase the size of the data segment
# =======================================================================================================================
def incr_data_segment_size(mem_view, length):
    updated_length = length + int.from_bytes(mem_view[OFFSET_DATA_LENGTH: OFFSET_DATA_LENGTH + 4], byteorder='big',
                                             signed=False)
    mem_view[OFFSET_DATA_LENGTH:OFFSET_DATA_LENGTH + 4] = updated_length.to_bytes(4, byteorder='big',
                                                                                  signed=False)  # add length cof data


# =======================================================================================================================
# Ireset the block
# =======================================================================================================================
def reset_block(mem_view):
    to_offset = get_block_size_used(mem_view)
    for x in range(to_offset):
        mem_view[x:x + 1] = (b'\xff')


# =======================================================================================================================
# Ireset the data segment
# =======================================================================================================================
def reset_data_segment(mem_view):
    from_offset = get_data_offset_after_command(mem_view)
    to_offset = get_block_size_used(mem_view)
    for x in range(from_offset, to_offset):
        mem_view[x:x + 1] = (b'\x00')


def get_command_length(mem_view):
    return int.from_bytes(mem_view[OFFSET_COMMAND_LENGTH: OFFSET_COMMAND_LENGTH + 2], byteorder='big', signed=False)


# =======================================================================================================================
# Get the length of the data segment
# =======================================================================================================================
def get_data_length(mem_view):
    return int.from_bytes(mem_view[OFFSET_DATA_LENGTH: OFFSET_DATA_LENGTH + 4], byteorder='big', signed=False)


# =======================================================================================================================
# Get the length of the block used
# =======================================================================================================================
def get_block_size_used(mem_view):
    return SIZE_OF_HEADER + int.from_bytes(mem_view[OFFSET_DATA_LENGTH: OFFSET_DATA_LENGTH + 4], byteorder='big',
                                           signed=False)


# =======================================================================================================================
# Copy block header and data from src to dest
# =======================================================================================================================
def copy_block(dest, src):
    length = SIZE_OF_HEADER + int.from_bytes(src[OFFSET_DATA_LENGTH: OFFSET_DATA_LENGTH + 4], byteorder='big',
                                             signed=False)
    dest[:length] = src[:length]


def get_command(mem_view):
    command_length = get_command_length(mem_view)
    return bytes(mem_view[SIZE_OF_HEADER:SIZE_OF_HEADER + command_length]).decode()


# =======================================================================================================================
# Find sthe first non-space occurence. return -1 if not found
# =======================================================================================================================
def get_word_offset(mem_view, start_offset):
    command_length = get_command_length(mem_view)
    for index in range(start_offset, command_length):
        if mem_view[SIZE_OF_HEADER + index] != space:
            return index
    return -1


# =======================================================================================================================
# Get the message text sarting at the specified offset
# =======================================================================================================================
def get_message_text(mem_view, start_offset):
    message_size = get_command_length(mem_view)

    if start_offset == -1 or start_offset >= message_size:
        return ""

    return bytes(mem_view[SIZE_OF_HEADER + start_offset:SIZE_OF_HEADER + message_size]).decode()


# =======================================================================================================================
# Get a single word from the array array. Words are seperated by space or TAB.
# This method is used to pull command word after word from the buffer
# =======================================================================================================================
def get_word_from_array(mem_view, start_offset):
    command_length = get_command_length(mem_view)
    if command_length <= start_offset:
        return ""

    # Start at start_offset and find the end of the word by a space or tab.
    for index in range(start_offset, command_length):  # starts after the term sql
        if mem_view[SIZE_OF_HEADER + index] == tab or mem_view[SIZE_OF_HEADER + index] == space:
            break
    if index + 1 == command_length and mem_view[SIZE_OF_HEADER + index] != tab and mem_view[
        SIZE_OF_HEADER + index] != space:
        index += 1  # last word in the buffer
    return bytes(mem_view[SIZE_OF_HEADER + start_offset:SIZE_OF_HEADER + index]).decode()


# =======================================================================================================================
# Get a string from the array - a string can include tabs and it ends with a space.
# This method is used to pull file names that may have space in the path
# =======================================================================================================================
def get_string_from_array(mem_view, start_offset):
    command_length = get_command_length(mem_view)
    if command_length <= start_offset:
        return ""

    # Start at start_offset and find the end of the string by a space.
    for index in range(start_offset, command_length):  # starts after the term sql
        byte_val =  mem_view[SIZE_OF_HEADER + index]
        if byte_val == space or byte_val == 0:
            break
    if index + 1 == command_length and mem_view[SIZE_OF_HEADER + index] != space:
        index += 1  # last word in the buffer
    return bytes(mem_view[SIZE_OF_HEADER + start_offset:SIZE_OF_HEADER + index]).decode()


# =======================================================================================================================
# Get Authentication String
# =======================================================================================================================
def get_auth_str_decoded(mem_view):
    start = get_data_offset_after_command(mem_view)
    length = get_authentication_length(mem_view)
    if not length:
        return None
    end = start + length
    return bytes(mem_view[start:end]).decode()
# =======================================================================================================================
# Get Data in decoded format
# =======================================================================================================================
def get_data_decoded(mem_view):
    start = get_data_offset_after_authentication(mem_view)
    end = get_block_size_used(mem_view)
    return bytes(mem_view[start:end]).decode()


# =======================================================================================================================
# Return the data offset after the command
# =======================================================================================================================
def get_data_offset_after_command(mem_view):
    offset = SIZE_OF_HEADER + get_command_length(mem_view)
    return offset


# =======================================================================================================================
# Return the data offset after authentication
# =======================================================================================================================
def get_data_offset_after_authentication(mem_view):
    offset = SIZE_OF_HEADER
    # add command length
    offset += int.from_bytes(mem_view[OFFSET_COMMAND_LENGTH: OFFSET_COMMAND_LENGTH + 2], byteorder='big', signed=False)
    # add Authentication length
    offset += int.from_bytes(mem_view[OFFSET_AUTHENTICATION_LENGTH: OFFSET_AUTHENTICATION_LENGTH + 2], byteorder='big',
                             signed=False)

    return offset


# =======================================================================================================================
# Flag that this is a single block message
# =======================================================================================================================
def prep_single_block(mem_view, counter, is_last):
    """

    :param mem_view: The memory location of the data to transfer
    :param counter:  The sequence number of the block
    :param is_last:  True - if this is the last block in the sequence
    """

    mem_view[OFFSET_BLOCK_COUNTER:OFFSET_BLOCK_COUNTER + 4] = counter.to_bytes(4, byteorder='big', signed=False)
    if is_last:
        mem_view[OFFSET_FLAG_LAST:OFFSET_FLAG_LAST + 1] = (b'\x01')
    else:
        mem_view[OFFSET_FLAG_LAST:OFFSET_FLAG_LAST + 1] = (b'\x00')


# =======================================================================================================================
# Test if the last block message by considering OFFSET_FLAG_LAST
# =======================================================================================================================
def is_last_block(mem_view):
    if mem_view[OFFSET_FLAG_LAST:OFFSET_FLAG_LAST + 1] == (b'\x01'):
        return True
    return False


# =======================================================================================================================
# Test first block to be 1
# =======================================================================================================================
def is_first_block(mem_view):
    if int.from_bytes(mem_view[OFFSET_BLOCK_COUNTER: OFFSET_BLOCK_COUNTER + 4], byteorder='big', signed=False) == 1:
        return True
    return False


# =======================================================================================================================
# Flag the block as last block
# =======================================================================================================================
def set_last_block(mem_view):
    mem_view[OFFSET_FLAG_LAST:OFFSET_FLAG_LAST + 1] = (b'\x01')


# =======================================================================================================================
# Set the hash value of the block
# =======================================================================================================================
def set_hash_value(mem_view, hash_value):
    mem_view[OFFSET_BLOCK_HASH:OFFSET_BLOCK_HASH + HASH_LENGTH] = hash_value


# =======================================================================================================================
# Get the hash value of the block
# =======================================================================================================================
def get_hash_value(mem_view):
    return mem_view[OFFSET_BLOCK_HASH:OFFSET_BLOCK_HASH + HASH_LENGTH]


# =======================================================================================================================
# Update the message header - Set the block number and a flag representing last block
# =======================================================================================================================
def set_block_number(mem_view, block_number, last_block):
    mem_view[OFFSET_BLOCK_COUNTER:OFFSET_BLOCK_COUNTER + 4] = block_number.to_bytes(4, byteorder='big', signed=False)

    if last_block:
        mem_view[OFFSET_FLAG_LAST:OFFSET_FLAG_LAST + 1] = (b'\x01')
    else:
        mem_view[OFFSET_FLAG_LAST:OFFSET_FLAG_LAST + 1] = (b'\x00')


# =======================================================================================================================
# Store info on the sender socket - socket ID and a counter representing the ID of messages sent with this socket
# =======================================================================================================================
def set_send_socket_info(mem_view, socket_id, time_id, send_counter):
    mem_view[OFFSET_SOCKET_ID:OFFSET_SOCKET_ID + 2] = socket_id.to_bytes(2, byteorder='big', signed=False)
    if time_id:
        mem_view[OFFSET_SOCKET_TIME_ID:OFFSET_SOCKET_TIME_ID + 8] = time_id
    mem_view[OFFSET_SOCKET_COUNTER:OFFSET_SOCKET_COUNTER + 4] = send_counter.to_bytes(4, byteorder='big', signed=False)


# =======================================================================================================================
# Get the info on the sender socket - socket ID and a counter representing the ID of messages sent with this socket
# =======================================================================================================================
def get_send_socket_info(mem_view):
    socket_id = int.from_bytes(mem_view[OFFSET_SOCKET_ID: OFFSET_SOCKET_ID + 2], byteorder='big', signed=False)
    time_id = bytes(mem_view[OFFSET_SOCKET_TIME_ID:OFFSET_SOCKET_TIME_ID + 8])
    send_counter = int.from_bytes(mem_view[OFFSET_SOCKET_COUNTER: OFFSET_SOCKET_COUNTER + 4], byteorder='big',
                                  signed=False)
    return [socket_id, time_id, send_counter]


# =======================================================================================================================
# Flag a reply message
# =======================================================================================================================
def set_reply_message(mem_view, is_reply):
    if is_reply:
        mem_view[OFFSET_FLAG_LAST:OFFSET_FLAG_LAST + 1] = (b'\x01')  # non 0 value indicates a reply
    else:
        mem_view[OFFSET_FLAG_LAST:OFFSET_FLAG_LAST + 1] = (b'\x00')


# =======================================================================================================================
# is a reply message - test reply flag
# =======================================================================================================================
def is_reply_message(mem_view, is_reply):
    if mem_view[OFFSET_REPLY_FLAG:OFFSET_REPLY_FLAG + 1] == (b'\x01'):
        return True
    return False


# =======================================================================================================================
# Set the amount of time in milliseconds that the DBMS was processed and the time messages were send
# =======================================================================================================================
def set_operator_time(mem_view, dbms_time, send_time):
    mem_view[OFFSET_DBMS_TIME:OFFSET_DBMS_TIME + 4] = dbms_time.to_bytes(4, byteorder='big', signed=False)
    mem_view[OFFSET_SEND_TIME:OFFSET_SEND_TIME + 4] = send_time.to_bytes(4, byteorder='big', signed=False)


# =======================================================================================================================
# Get the amount of time in milliseconds that the operator spend on DBMS processing
# =======================================================================================================================
def get_dbms_time(mem_view):
    return int.from_bytes(mem_view[OFFSET_DBMS_TIME: OFFSET_DBMS_TIME + 4], byteorder='big', signed=False)


# =======================================================================================================================
# Get the amount of time in milliseconds that the operator spent to send messages
# =======================================================================================================================
def get_send_time(mem_view):
    return int.from_bytes(mem_view[OFFSET_SEND_TIME: OFFSET_SEND_TIME + 4], byteorder='big', signed=False)


# =======================================================================================================================
# Set the info on the current job: location in the active_jobs arry and a unique id identifying the job.
# =======================================================================================================================
def set_job_info(mem_view, job_location, unique_job_id):
    mem_view[OFFSET_JOB_LOCATION:OFFSET_JOB_LOCATION + 2] = job_location.to_bytes(2, byteorder='big', signed=False)
    mem_view[OFFSET_JOB_ID:OFFSET_JOB_ID + 4] = unique_job_id.to_bytes(4, byteorder='big', signed=False)


# =======================================================================================================================
# Get the BLOCK id - a unique id identifying the Block.
# =======================================================================================================================
def get_block_id(mem_view):
    return int.from_bytes(mem_view[OFFSET_BLOCK_COUNTER: OFFSET_BLOCK_COUNTER + 4], byteorder='big', signed=False)


# =======================================================================================================================
# Get the job id - a unique id identifying the job.
# =======================================================================================================================
def get_job_id(mem_view):
    return int.from_bytes(mem_view[OFFSET_JOB_ID: OFFSET_JOB_ID + 4], byteorder='big', signed=False)


# =======================================================================================================================
# Get the job location - the location of the job in the active_jobs array
# =======================================================================================================================
def get_job_location(mem_view):
    return int.from_bytes(mem_view[OFFSET_JOB_LOCATION: OFFSET_JOB_LOCATION + 2], byteorder='big', signed=False)


# =======================================================================================================================
# Set the number of JSON structures inside a block
# =======================================================================================================================
def set_counter_jsons(mem_view, counter):
    mem_view[OFFSET_COUNTER_JSONS:OFFSET_COUNTER_JSONS + 2] = counter.to_bytes(2, byteorder='big', signed=False)


# =======================================================================================================================
# Get the number of JSON structures inside a block
# =======================================================================================================================
def get_counter_jsons(mem_view):
    return int.from_bytes(mem_view[OFFSET_COUNTER_JSONS: OFFSET_COUNTER_JSONS + 2], byteorder='big', signed=False)


# =======================================================================================================================
# Set the number of nodes receiving this message
# =======================================================================================================================
def set_counter_receivers(mem_view, counter):
    mem_view[OFFSET_COUNTER_RECEIVERS:OFFSET_COUNTER_RECEIVERS + 2] = counter.to_bytes(2, byteorder='big', signed=False)


# =======================================================================================================================
# Get the number of nodes receiving this message
# =======================================================================================================================
def get_counter_receivers(mem_view):
    return int.from_bytes(mem_view[OFFSET_COUNTER_RECEIVERS: OFFSET_COUNTER_RECEIVERS + 2], byteorder='big',
                          signed=False)


# =======================================================================================================================
# Use the GENERIC_FLAG to signal if print/echo message is in JSON
# =======================================================================================================================
def set_message_format(mem_view, is_json):
    if is_json:
        flag = GENERIC_JSON_FORMAT
    else:
        flag = GENERIC_STRING_FORMAT

    set_generic_flag(mem_view, flag)

# =======================================================================================================================
# Get the GENERIC_FLAG value to determine if print/echo message is in JSON
# =======================================================================================================================
def is_json_message_format(mem_view):
    flag = get_generic_flag(mem_view)
    if flag == GENERIC_JSON_FORMAT:
        ret_val = True
    else:
        ret_val = False
    return ret_val

# =======================================================================================================================
# Get Generic Flag - 2 bytes flag - used by specific processes
# When file is written - it indicates if to use the watch dir
# If a print message is send - it indicates if the print format is JSON
# =======================================================================================================================
def get_generic_flag(mem_view):
    return int.from_bytes(mem_view[OFFSET_GENERIC_FLAG: OFFSET_GENERIC_FLAG + 2], byteorder='big', signed=False)

# =======================================================================================================================
# Set Generic Flag - 2 bytes flag - used by specific processes
# =======================================================================================================================
def set_generic_flag(mem_view, flag):
    mem_view[OFFSET_GENERIC_FLAG:OFFSET_GENERIC_FLAG + 2] = flag.to_bytes(2, byteorder='big', signed=False)

# =======================================================================================================================
# Set the receiver id (node x out of y receivers)
# =======================================================================================================================
def set_receiver_id(mem_view, id):
    mem_view[OFFSET_RECEIVER_ID:OFFSET_RECEIVER_ID + 2] = id.to_bytes(2, byteorder='big', signed=False)

# =======================================================================================================================
# Get the receiver id (node x out of y receivers)
# =======================================================================================================================
def get_receiver_id(mem_view):
    return int.from_bytes(mem_view[OFFSET_RECEIVER_ID: OFFSET_RECEIVER_ID + 2], byteorder='big', signed=False)

# =======================================================================================================================
# Set the receiver ip and port
# =======================================================================================================================
def set_source_ip_port(mem_view, ip, port):

    if add_ip(mem_view, ip):
        # add the port
        mem_view[OFFSET_SOURCE_PORT:OFFSET_SOURCE_PORT + 4] = port.to_bytes(4, byteorder='big', signed=False)
        ret_val = True
    else:
        # IP length is over max
        ret_val = False

    return ret_val

# =======================================================================================================================
# Get the receiver ip (str) and port (int)
# =======================================================================================================================
def get_source_ip_port(mem_view):
    ip_length = int.from_bytes(mem_view[OFFSET_SOURCE_IP_LENGTH: OFFSET_SOURCE_IP_LENGTH + 1], byteorder='big',
                               signed=False)

    ip = bytes(mem_view[OFFSET_SOURCE_IP:OFFSET_SOURCE_IP + ip_length]).decode()

    port = int.from_bytes(mem_view[OFFSET_SOURCE_PORT: OFFSET_SOURCE_PORT + 4], byteorder='big', signed=False)

    return [ip, port]

# =======================================================================================================================
# Get IP:Port string from the header data
# =======================================================================================================================
def get_ip_port(mem_view):
    ip_length = int.from_bytes(mem_view[OFFSET_SOURCE_IP_LENGTH: OFFSET_SOURCE_IP_LENGTH + 1], byteorder='big',
                               signed=False)

    ip = bytes(mem_view[OFFSET_SOURCE_IP:OFFSET_SOURCE_IP + ip_length]).decode()

    port = int.from_bytes(mem_view[OFFSET_SOURCE_PORT: OFFSET_SOURCE_PORT + 4], byteorder='big', signed=False)

    return f"{ip}:{str(port)}"


# =======================================================================================================================
# Test if source IP included in the block. Returns 0 if not included
# =======================================================================================================================
def is_source_ip_included(mem_view):
    ip_length = int.from_bytes(mem_view[OFFSET_SOURCE_IP_LENGTH: OFFSET_SOURCE_IP_LENGTH + 1], byteorder='big', signed=False)
    ip = bytes(mem_view[OFFSET_SOURCE_IP:OFFSET_SOURCE_IP + ip_length]).decode()
    return not ip == "0.0.0.0"
# =======================================================================================================================
# Add the source IP to the block
# =======================================================================================================================
def add_ip(mem_view, ip):

    ip_encoded = ip.encode()
    ip_length = len(ip_encoded)

    if ip_length > MAX_IP_LENGTH:
        return False

    mem_view[OFFSET_SOURCE_IP_LENGTH: OFFSET_SOURCE_IP_LENGTH + 1] = ip_length.to_bytes(1, byteorder='big',
                                                                                        signed=False)  # add length IP
    mem_view[OFFSET_SOURCE_IP:OFFSET_SOURCE_IP + ip_length] = ip_encoded  # add IP

    return True
# =======================================================================================================================
# Get the receiver ip (str) and port (int)
# =======================================================================================================================
def get_source_ip_port_string(mem_view):

    ip, port = get_source_ip_port(mem_view)

    return ip + ":" + str(port)

# =======================================================================================================================
# Test if the provioded IP and Port are the same as the IP and Port in th message header
# =======================================================================================================================
def is_source_ip_port(data_buffer, ip_port_str):
    ip, port = get_source_ip_port(data_buffer)

    return ip + ":" + str(port) == ip_port_str


# =======================================================================================================================
# Get the receiver ip (str)
# =======================================================================================================================
def get_source_ip(mem_view):
    ip_length = int.from_bytes(mem_view[OFFSET_SOURCE_IP_LENGTH: OFFSET_SOURCE_IP_LENGTH + 1], byteorder='big',
                               signed=False)
    ip = bytes(mem_view[OFFSET_SOURCE_IP:OFFSET_SOURCE_IP + ip_length]).decode()
    return ip


# =======================================================================================================================
# Set the type of info in a block
# =======================================================================================================================
def set_info_type(mem_view, info_type):
    mem_view[OFFSET_FLAG_INFO_TYPE: OFFSET_FLAG_INFO_TYPE + 1] = info_type.to_bytes(1, byteorder='big', signed=False)


# =======================================================================================================================
# Get the type of info in a block
# =======================================================================================================================
def get_info_type(mem_view):
    return int.from_bytes(mem_view[OFFSET_FLAG_INFO_TYPE: OFFSET_FLAG_INFO_TYPE + 1], byteorder='big', signed=False)


# =======================================================================================================================
# Set an Error Code, or ) for no error
# =======================================================================================================================
def set_error(mem_view, error_code):
    mem_view[OFFSET_ERROR:OFFSET_ERROR + 2] = error_code.to_bytes(2, byteorder='big', signed=False)


# =======================================================================================================================
# Get the error value
# =======================================================================================================================
def get_error(mem_view):
    return int.from_bytes(mem_view[OFFSET_ERROR: OFFSET_ERROR + 2], byteorder='big', signed=False)


# =======================================================================================================================
# Reset block such that the size used includes the command
# =======================================================================================================================
def set_data_segment_to_command(mem_view):
    command_length = get_command_length(mem_view)

    mem_view[OFFSET_DATA_LENGTH: OFFSET_DATA_LENGTH + 4] = int(command_length).to_bytes(4, byteorder='big',
                                                                                        signed=False)
    reset_authentication(mem_view)

    return SIZE_OF_HEADER + command_length

# =======================================================================================================================
# update the number of physical partitions that are used to satisfy the query
# =======================================================================================================================
def set_partitions_used(mem_view, par_used):
    mem_view[OFFSET_PAR_USED:OFFSET_PAR_USED + 2] = par_used.to_bytes(2, byteorder='big', signed=False)


# =======================================================================================================================
# Get the number of physical partitions that are used to satisfy the query
# =======================================================================================================================
def get_partitions_used(mem_view):
    return int.from_bytes(mem_view[OFFSET_PAR_USED: OFFSET_PAR_USED + 2], byteorder='big', signed=False)


# =======================================================================================================================
# update the ID of the physical partition with the result set
# =======================================================================================================================
def set_partition_id(mem_view, par_id):
    mem_view[OFFSET_PAR_ID:OFFSET_PAR_ID + 2] = par_id.to_bytes(2, byteorder='big', signed=False)


# =======================================================================================================================
# Get the ID of the physical partition on the Operator node
# =======================================================================================================================
def get_partition_id(mem_view):
    return int.from_bytes(mem_view[OFFSET_PAR_ID: OFFSET_PAR_ID + 2], byteorder='big', signed=False)


# =======================================================================================================================
# Set partition ID to 0 and partitions used to 1
# =======================================================================================================================
def reset_partitions(mem_view):
    set_partitions_used(mem_view, 1)
    set_partition_id(mem_view, 0)
