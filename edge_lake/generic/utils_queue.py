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
import threading
import edge_lake.generic.params as params
import edge_lake.generic.utils_print as utils_print
from edge_lake.generic.utils_columns import get_current_time
import edge_lake.generic.node_info as node_info

# ------------------------------------------------------------------------------
# Maintain a message Queue
# ------------------------------------------------------------------------------
class MsgQueue():
    def __init__(self, queue_size):

        if queue_size:
            self.queue = [None] * queue_size       # A dynamic buffer of messages which are accumulated int he queue
        else:
            self.queue = None

        self.static_queue = None                    # This queue is updated once with a fixed set of messages

        self.queue_offset = 0                       # The location for the next message
        self.queue_size = 0
        self.queue_mutex = threading.Lock()
        self.queue_counter = 0                      # message number
        self.index = 0                              # This is an index on a particular message of the queue


    # =======================================================================================================================
    # Get the queue size
    # =======================================================================================================================
    def get_queue_size(self):
        if not self.queue:
            size = 0
        else:
            size = len(self.queue)
        return size
    # =======================================================================================================================
    # Set the index on a particular message
    # =======================================================================================================================
    def set_index(self, value:int):
        self.index = value
    # =======================================================================================================================
    # Get the indexed message
    # Values is an array with key + value, if the key is found in the string, the key is replaced with the value
    # =======================================================================================================================
    def get_indexed_msg(self, msg_val:list = None):
        message_txt = self.static_queue[self.index]     # The message to return

        if msg_val:       # Add a value to the message
            for key_val in msg_val:
                key = key_val[0]        # Search for this key in the message
                val = key_val[1]        # replace the key with this value
                message_txt = message_txt.replace(key, val)  # Add the input value to the message
        return message_txt

    # =======================================================================================================================
    # Set the messages to maintain
    # =======================================================================================================================
    def set_static_queue(self, queue):
        self.index = 0
        self.static_queue = queue
    # =======================================================================================================================
    # Flag the user if there is data in the queue using the command prompt (switch between EL > to AL +>)
    # =======================================================================================================================
    def update_prompt(self, data_in_buff):
        if node_info.queue_msg_ != data_in_buff:
            # Prompt state is different
            node_info.queue_msg_  = False  # Flag no messages in the queue
            utils_print.print_prompt()  # Add/Remove the queue sign

    # =======================================================================================================================
    # Place messages in a message queue
    # =======================================================================================================================
    def add_msg(self, message):

        self.queue_mutex.acquire()

        if not self.queue_counter:
            # First message
            first_message = True
        else:
            first_message = False

        self.queue_counter += 1

        self.queue[self.queue_offset] = (self.queue_counter, get_current_time(), message)
        self.queue_offset += 1
        if self.queue_offset >= len(self.queue):  # Test overflow against the size of the list
            self.queue_offset = 0

        self.queue_mutex.release()

        if first_message:
            node_info.queue_msg_  = True  # Flag new messages in the queue
            utils_print.output("Messages in Echo Queue! ...", True)
        elif not node_info.queue_msg_:
            # Not the first message but the queue was printed
            node_info.queue_msg_ = True  # Flag new messages in the queue
            utils_print.print_prompt()  # Add the queue sign
        return True  # Placed in the Queue

    # =======================================================================================================================
    # Return the Queue messages formatted
    # =======================================================================================================================
    def get_mssages(self):

        self.queue_mutex.acquire()

        self.queue.sort(key=echo_queue_sort)

        reply = utils_print.output_nested_lists(self.queue, "\r\nMessage Queue:",
                                        ["Counter", "Time", "Message"], True)

        self.queue_offset = 0  # Next message will go to offset 0 not to overwrite a new message (which will be at the end of the list)

        node_info.queue_msg_  = False  # Flag no new messages in the queue
        self.queue_mutex.release()

        return reply

# =======================================================================================================================
# A method to sort the echo queue
# =======================================================================================================================
def echo_queue_sort(entry):
    if not entry:
        return 0
    return entry[0]     # message ID