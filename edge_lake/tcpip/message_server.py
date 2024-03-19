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

import socket
import sys
import time

import edge_lake.tcpip.net_utils as net_utils
import edge_lake.generic.utils_threads as utils_threads
import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.process_log as process_log
import edge_lake.tcpip.mqtt_client as mqtt_client
import edge_lake.generic.interpreter as interpreter
import edge_lake.generic.version as version
from edge_lake.generic.utils_columns import seconds_to_date, format_syslog_date
from edge_lake.generic.streaming_data import add_data
from edge_lake.generic.params import get_param




# The MQTT protocol: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901041

MESSAGE_NOT_COMPLETE = 2001
END_SESSION = 2002

mqtt_topics_ = {}       # Topics registered pointing to the client ID (client_id as a f(topic))
counter_events_ = {}    #Global counter for event messages supporting the command - get messages
info_title_ = ["Protocol", "IP", "Event", "Success", "Last message time", "Error", "Last error time", "Error Code", "Details"]
workers_pool_ = None


msg_rules_ = {}     # Mapping rules for arbitrary messages - this structure is a dictionary - the key is the rule name and the value is the user rules info
ip_rules_ = {}      # This structure is a dictionary. the key are the ip+port and the value is a dictionary with keys that are the rule names.


mapping_types_ = {
    "bsd"   :       [
                        # Attribute name    offset  start Char    End char      Format,            Length
                        ("priority",          -1,      '<',          '>',       None,                0),
                        ("timestamp",         -1,      None,         None,      '%b %d %H:%M:%S',   15),
                        ("hostname",          -1,      ' ',          ' ',       None,                0),
                        ("tag",               -1,      None,          ' ',      None,                0),     # process or app name
                        ("message",           -1,      None,          None,     None,                0),     # process or app name
                    ],

    "ietf": [
                        # Attribute name  offset    start Char    End char   Format,            Length
                        ("priority",      -1,           '<',           '>',       None,               0),
                        ("Version",       -1,           None,          None,      None,               1),
                        ("timestamp",     -1,           None,          None,      '%b %d %H:%M:%S',   15),
                        ("hostname",      -1,           ' ',           ' ',       None,               0),
                        ("application",   -1,           None,          ' ',       None,               0),
                        ("pid",           -1,           None,          ' ',       None,               0),
                        ("mid",           -1,           None,          ' ',       None,               0),
                        ("struct",       -1,            '[',           ']',       None,               0),
                        ("message",     None,           None,          None,               0)
                    ]

}
class SESSION():
    def __init__(self, clientSoc, protocol_name, msg_size, rules_obj, trace_level):

        self.clientSoc = clientSoc
        self.protocol_name = protocol_name
        self.message_size = msg_size        # The size of the message processed
        self.rules_obj = rules_obj          # Provided by the user in the "set msg rules" command
        self.trace_level = trace_level
        self.keep_alive = 60  # Default value for closing the session if no communication for so many seconds
        self.sessions_counter = 0
        self.msg_counter = 0  # Counter messages on this connection

        try:
            self.peer_ip = clientSoc.getpeername()[0]  # The IP of the peer node
        except:
            self.peer_ip = "Not determined"

        self.reset()
    # ----------------------------------------------------------------
    # The trace level is tested with every new message
    # ----------------------------------------------------------------
    def set_trace_level(self, trace_level):
        self.trace_level = trace_level

    # ----------------------------------------------------------------
    # The trace level is tested with every new message
    # ----------------------------------------------------------------
    def get_msg_counter(self):
        return self.msg_counter

    # ----------------------------------------------------------------
    # It is the maximum time interval in seconds that is permitted to elapse between the point at which the Client finishes
    # transmitting one Control Packet and the point it starts sending the next.
    # ----------------------------------------------------------------
    def get_keep_alive(self):
        return self.keep_alive


class MQTT_MESSAGES(SESSION):

    def reset(self):
        self.msg_type = -1
        self.msg_len = 0
        self.msg_size = 0
        self.msg_flags = 0
        self.fixed_hdr_len = 0  # Length of fixed header
        self.sessions_counter += 1  # number of times this session was called
        self.clean_session = 0
        self.will_flag = 0
        self.will_qos = 0
        self.will_retain = 0
        self.password_flag = 0
        self.user_name_flag = 0
        self.user_name = ""
        self.password = ""
        self.authenticated = False

        self.version = 0  # MQTT Version
        self.client_id = ""  # Each Client connecting to the Server has a unique ClientId
        self.will_topic = ""
        self.will_msg = None
        self.packet_identifier = 0xffffff  # A unique number provided by the sender in a publish message - will be set with 2 bytes value
        self.userdata = -1  # The value that is send to the mqtt client to identify how the topic was registered.
        self.payload = None  # Updated with the message data on a PUBLISH Message
        self.topic_name = None  # Updated with the topic name on a PUBLISH Message

    def get_seesion_id(self):
        # The Client Identifier is the session ID
        return self.client_id

    def get_msg_len(self):
        # Length as determined by the message header
        return self.msg_len
    # ----------------------------------------------------------------
    # Process one MQTT message
    # https://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html
    '''
    Reserved	0	Reserved
    CONNECT	1	Client request to connect to Server
    CONNACK	2	Connect Acknowledgment
    PUBLISH	3	Publish message
    PUBACK	4	Publish Acknowledgment
    PUBREC	5	Publish Received (assured delivery part 1)
    PUBREL	6	Publish Release (assured delivery part 2)
    PUBCOMP	7	Publish Complete (assured delivery part 3)
    SUBSCRIBE	8	Client Subscribe request
    SUBACK	9	Subscribe Acknowledgment
    UNSUBSCRIBE	10	Client Unsubscribe request
    UNSUBACK	11	Unsubscribe Acknowledgment
    PINGREQ	12	PING Request
    PINGRESP	13	PING Response
    DISCONNECT	14	Client is Disconnecting
    Reserved	15	Reserved
    '''
    # ----------------------------------------------------------------
    def process_msg(self, status, mem_view, length):


        remaining_length = length                 # The amount of data left in the buffer to process

        while True:

            if remaining_length < 3:
                ret_val = MESSAGE_NOT_COMPLETE
                break                            # The minimum is 3 bytes - (type + size + message)

            ret_val = self.process_fixed_header(mem_view, remaining_length)
            if ret_val:
                break

            self.msg_counter += 1

            # Could be multiple messages on the mem_view buffer
            multiple_messages = remaining_length > self.msg_len  # Test if the size of the data received is larger than the message size

            if self.msg_type == 1:
                # connect
                if self.msg_counter != 1:
                    # The Server MUST process a second CONNECT Packet sent from a Client as a protocol violation and disconnect the Client [MQTT-3.1.0-2].
                    ret_val = END_SESSION
                else:
                    ret_val = self.connect_msg( status, mem_view, multiple_messages )
                    count_event("MQTT", self.peer_ip, "CONNECT", ret_val, "")
            elif self.msg_type == 3:
                # Message   - MQTT Control Packet type (3)
                ret_val = self.publish_msg( mem_view )

                if ret_val == process_status.Unrecognized_mqtt_topic:
                    details = self.topic_name
                else:
                    details = ""
                count_event("MQTT", self.peer_ip, "PUBLISH", ret_val, details)

            elif self.msg_type == 6:
                # A PUBREL Packet is the response to a PUBREC Packet. It is the third packet of the QoS 2 protocol exchange.
                ret_val = self.pubrel_msg( mem_view )
            elif self.msg_type == 8:
                # Register to a topic
                ret_val = self.subscribe_msg( mem_view )
            elif self.msg_type == 10:
                # An UNSUBSCRIBE message is sent by the client to the server to unsubscribe from named topics.
                # Needs to return Unsubscribe acknowledgment (UNSUBACK)
                ret_val = self.unsubscribe_mqtt( mem_view )

            elif self.msg_type == 12:
                # Request that the Server responds to confirm that it is alive
                ret_val = self.ping_msg()
            else:
                err_msg = "Non supported MQTT message type %u" % self.msg_type
                utils_print.output_box(err_msg)
                ret_val = END_SESSION
                break
            if ret_val:
                remaining_length = 0
                break

            if multiple_messages:
                remaining_length = self.shift_data( mem_view, remaining_length)  # Set next message to processing
            else:
                remaining_length = 0
                break                           # No more messages in mem_view

        return [ret_val, remaining_length]

    # ----------------------------------------------------------------
    # Shift the next message to beginning of the buffer and return the remaining lenth
    # ----------------------------------------------------------------
    def shift_data( self, mem_view, remaining_length):

        next_message_length = remaining_length - self.msg_len
        mem_view[:next_message_length] = mem_view[self.msg_len: self.msg_len + next_message_length]

        return next_message_length

    # ----------------------------------------------------------------
    # Process Fixed Header
    # ----------------------------------------------------------------
    def process_fixed_header(self, mem_view, remaining_length):

        self.msg_type = mem_view[0] >> 4  # 1st 4 bits - MQTT Control Packet types
        self.msg_flags = mem_view[0] & 7  # low 4 bits -  Flags specific to each MQTT Control Packet types

        message_size, bytes_used = msg_get_var_int(mem_view, 1) # (section 3.1.1) length of the Variable Header plus the length of the Payload
        self.fixed_hdr_len = 1 + bytes_used
        self.msg_len = ( message_size + self.fixed_hdr_len)        # Add the length of the FIXED HEADER

        if self.msg_len > remaining_length:         # remaining_length is the data in the buffer
            ret_val = MESSAGE_NOT_COMPLETE             # get more data
        else:
            ret_val = process_status.SUCCESS
        return ret_val


    # ----------------------------------------------------------------
    # Process PINGREQ  - Request that the Server responds to confirm that it is alive
    # ----------------------------------------------------------------
    def ping_msg(self):

        # The server needs to respond with PINGRESP
        list_response = [0xd0, 0x0,  # Fixed header with PINGRESP
                    ]

        msg_response = bytearray(list_response)
        ret_val = send_msg("MQTT", self.clientSoc, msg_response)
        return ret_val

    # ----------------------------------------------------------------
    # Process SUBSCRIBE - Subscribe to topics
    # ----------------------------------------------------------------
    def subscribe_msg(self, mem_view):

        ret_val = process_status.SUCCESS
        # Fixed header - 2 bytes
        remaining_length, bytes_used = msg_get_var_int(mem_view, 1) # This is the length of Variable Header plus the length of the Payload
        message_length = remaining_length + bytes_used + 1          # length of Header + var Header + Payload

        offset = 1 + bytes_used     # Offset to Var Header
        packet_identifier = msg_get_int(mem_view, offset, 2)
        if not packet_identifier:
            ret_val = process_status.Err_in_broker_msg_format
        else:
            offset += 2         # Offset to Payload

            reply_var_header = [0,0]        # Placeholder for the Packet Identifier

            while offset < message_length:
                topic_length = msg_get_int(mem_view, offset, 2)
                if not topic_length:
                    ret_val = process_status.Err_in_broker_msg_format
                    break
                topic_name = msg_get_bytes(mem_view, offset + 2, topic_length)
                if not topic_name:
                    ret_val = process_status.Err_in_broker_msg_format
                    break
                offset += (2 + topic_length)
                qos = mem_view[offset]

                reply_var_header.append(qos)        # need to return the QoS for every Topic

                offset += 1

                if self.trace_level:
                    message = "\r\nSubscribe to topic: [%s]" % (topic_name)
                    utils_print.output(message, True)

        if not ret_val:
            # reply with SUBACK

            list_ack = [   0x90,    ]            # Fixed header ( SUBACK )

            counter = len(reply_var_header)     # Number of topics
            if counter >= 128:
                # 2 bytes for the length for the length of var header + payload
                low_bits = counter & 0x7f
                low_bits |= 0x80
                high_bits = counter >> 7
                list_ack.append(low_bits)
                list_ack.append(high_bits)
            else:
                # One byte for the length of var header + payload
                list_ack.append(counter)

            list_ack += reply_var_header

            msg_ack = bytearray(list_ack)
            msg_ack[2:4] = packet_identifier.to_bytes(2, byteorder='big', signed=False)  # set packet identifier

            ret_val = send_msg("MQTT", self.clientSoc, msg_ack)     # send reply message

        return ret_val

    # ----------------------------------------------------------------
    # Process CONNECT MESSAGE
    # send acknowledgement to the client
    # Details at https://docs.solace.com/MQTT-311-Prtl-Conformance-Spec/MQTT%20Control%20Packets.htm
    # The Server MUST acknowledge the CONNECT packet with a CONNACK packet containing a 1075 0x00 (Success) Reason Code [MQTT-3.1.4-5].
    # ----------------------------------------------------------------
    def connect_msg(self, status, mem_view,  multiple_messages):

        offset_version = self.fixed_hdr_len + 6     # 2 bytes protocol name length + len("MQTT")
        self.version = mem_view[offset_version]

        offset_connect = offset_version + 1
        connect_flags = mem_view[offset_connect]

        # If CleanSession is set to 0, the Server MUST resume communications with the Client
        # based on state from the current Session (as identified by the Client identifier).
        # If there is no Session associated with the Client identifier the Server MUST create a new Session.
        # The Client and Server MUST store the Session after the Client and Server are disconnected [MQTT-3.1.2-4].
        # After the disconnection of a Session that had CleanSession set to 0,
        # the Server MUST store further QoS 1 and QoS 2 messages that match any subscriptions that the client
        # had at the time of disconnection as part of the Session state.
        # If CleanSession is set to 1, the Client and Server MUST discard any previous Session and start a new one.
        # This Session lasts as long as the Network Connection. State data associated with this Session MUST NOT be reused in any subsequent Session
        self.clean_session = (connect_flags & 0x2) >> 1

        # Keep the last message and last QoS in case there is disconnect.
        # When the bit of Will Flag is 1, Will QoS and Will Retain will be read. At this time,
        # the specific contents of Will Topic and Will Message will appear in the message body,
        # otherwise the Will QoS and Will Retain will be ignored.
        # When the Will Flag bit is 0, Will Qos and Will Retain are invalid.
        self.will_flag =  (connect_flags & 0x4) >> 2

        #  2 bits specify the QoS level to be used when publishing the Will Message
        self.will_qos = (connect_flags & 0x18) >> 3

        # If Will Retain is set to 0, the Server MUST publish the Will Message as a non-retained message [MQTT-3.1.2-16].
        # If Will Retain is set to 1, the Server MUST publish the Will Message as a retained message
        self.will_retain = (connect_flags & 0x20) >> 5

        self.password_flag = (connect_flags & 0x40) >> 6
        self.user_name_flag = (connect_flags & 0x80) >> 7

        # It is the maximum time interval in seconds that is permitted to elapse between the point at which the Client finishes
        # transmitting one Control Packet and the point it starts sending the next.
        self.keep_alive = msg_get_int(mem_view, offset_connect + 1, 2)

        offset = offset_connect + 3         # offset payload

        # Each Client connecting to the Server has a unique ClientId
        client_id_length = msg_get_int(mem_view, offset, 2)
        self.client_id = msg_get_bytes(mem_view, offset + 2, client_id_length)
        offset += (client_id_length + 2)

        if self.will_flag:
            will_topic_length = msg_get_int(mem_view, offset, 2)
            self.will_topic = msg_get_bytes(mem_view, offset + 2, will_topic_length)
            offset += (will_topic_length + 2)

            will_msg_length = msg_get_int(mem_view, offset, 2)
            self.will_msg = mem_view[offset + 2: offset + 2 + will_msg_length]
            offset += (will_msg_length + 2)

        if self.user_name_flag:
            name_length = msg_get_int(mem_view, offset, 2)
            user_name = msg_get_bytes(mem_view, offset + 2, name_length)
            offset += (name_length + 2)
        else:
            user_name = ""

        if self.password_flag:
            password_length = msg_get_int(mem_view, offset, 2)
            password = msg_get_bytes(mem_view, offset + 2, password_length)
            offset +=2
        else:
            password = ""

        ret_val = self.register_user(status, user_name, password)  # with a new user - authenticate the user
        if not ret_val:

            if self.trace_level:
                message = "\r\nConnect from user: [%s]" % (self.user_name)
                utils_print.output(message, True)


            list_ack = [   0x20,        0x2,                # Fixed header ( CONNACK - 1119)
                        # Variable Header - Section 3.1.2
                           0x00,            # Byte 1 / 1132 is the "Connect Acknowledge Flags : 0 for new session
                           0x00,            # Byte 2 / 1165 Connect return Code : 0 for Connection Accepted
                       ]

            msg_ack = bytearray(list_ack)

            if multiple_messages:
                # No need to acknowledge if received multiple messages on the buffer
                ret_val = process_status.SUCCESS
            else:
                # Callback for MQTT is explained here - http://www.steves-internet-guide.com/mqtt-python-callbacks/
                ret_val = send_msg("MQTT", self.clientSoc, msg_ack)

        return ret_val
    # ----------------------------------------------------------------
    # Process Publish MESSAGE
    # https://openlabpro.com/guide/mqtt-packet-format/
    '''
    The process for QoS 2
    1. A receiver gets a QoS 2 PUBLISH packet from a sender
    2 -The receiver sends a PUBREC message 
    3. If the sender doesn’t receive an acknowledgement (PUBREC)  it will resend the message with the DUP flag set.
    4. When the sender receives an acknowledgement message PUBREC it then sends a message release message (PUBREL). The message can be deleted from the queue.
    5. If the receiver doesn’t receive the PUBREL it will resend the PUBREC message
    5. When the receiver receives the PUBREL message it can now forward the message onto any subscribers.
    6. The receiver then send a publish complete (PUBCOMP) .
    7. If the sender doesn’t receive the PUBCOMP message it will resend the PUBREL message.
    8. When the sender receives the PUBCOMP the process is complete and it can delete the message from the outbound queue, and also the message state.
    '''
    # ----------------------------------------------------------------
    def publish_msg(self, mem_view):

        global mqtt_topics_

        ret_val = process_status.SUCCESS

        # Section 3.3.1 PUBLISH Fixed Header

        dup_flag = mem_view[0] & 8      # If the bit is on = re-delivery of the message
        qos_level = (mem_view[0] & 6) >> 1   # 2 bits for QoS
        retain_flag = mem_view[0] & 1   # Keep the last data published

        # Section 3.3.2 PUBLISH Variable Header
        # The Variable Header of the PUBLISH Packet contains the following fields in the order: Topic Name,
        # Packet Identifier, and Properties. The rules for encoding Properties are described in section 2.2.2.

        offset_var_header = self.fixed_hdr_len
        topic_name_length = msg_get_int(mem_view, offset_var_header, 2)
        self.topic_name = msg_get_bytes( mem_view, offset_var_header + 2, topic_name_length)

        offset_var_header += (2 + topic_name_length)

        new_message = True
        if qos_level:
            # If QoS > 0 --> reply to sender
            in_identifier =  msg_get_int(mem_view, offset_var_header, 2)    # Section 2.2.1 Packet Identifier
            offset_var_header += 2
            if dup_flag and in_identifier == self.packet_identifier:
                # The same message delivered again
                new_message = False       # repeat of a message that was received
            else:
                self.packet_identifier = in_identifier  # new message

        if new_message:  # not a message repeat

            offset_payload = offset_var_header
            length_payload = self.msg_len - offset_payload

            self.payload = msg_get_bytes(mem_view, offset_payload, length_payload)
            if not self.payload:
                ret_val = process_status.Err_in_broker_msg_format

            if self.topic_name in mqtt_topics_: # there is a subscription to the topic
                # The topic name was registered on the local broker
                # Move the published data to the streamer through the MQTT Client on_message process
                self.userdata = mqtt_topics_[self.topic_name]
            else:
                self.userdata = 0
                ret_val = process_status.Unrecognized_mqtt_topic
        if not ret_val:
            # Respond to the PUBLISH packet:
            # If QoS is 0 - None
            # If QoS is 1 - PUBACK Packet   --> Publish Acknowledgement
            # If QoS is 2 - PUBREC Packet   --> Publish Received
            if not qos_level:
                if self.userdata:   # if self.userdata is 0 --> no subscription to the topic
                    ret_val = mqtt_client.process_message(self.topic_name, self.userdata, self.payload)
            elif self.packet_identifier <= 0xffff:
                if qos_level == 1:
                    # Publish Acknowledgement First
                    ret_val = self.send_puback()
                    if not ret_val:
                        if self.userdata:  # if self.userdata is 0 --> no subscription to the topic
                            ret_val = mqtt_client.process_message(self.topic_name, self.userdata, self.payload)
                elif qos_level == 2:
                    # Publish after PUBREL message from the sender of the data
                    ret_val = self.send_pubrec()

        if self.trace_level:
            if not new_message:
                msg_status = "Repeat of processed msg: PUBLISH"
            else:
                msg_status = "Publish a new msg: PUBLISH"

            if not self.userdata:   # if self.userdata is 0 --> no subscription to the topic
                msg_result = "Non subscribed topic"
            else:
                msg_result = "With subscription"
            if ret_val < 2000:
                err_msg = process_status.get_status_text(ret_val)
            else:
                err_msg = str(ret_val)

            trace_msg = "\r\n[MQTT] [%s] [Topic: %s] [QoS: %u] [--> %s] [--> %s] [payload: %s] " % (msg_status, self.topic_name, qos_level, msg_result, err_msg, self.payload)

            utils_print.output(trace_msg, True)

        return ret_val

    # ----------------------------------------------------------------
    # A PUBREL Packet is the response to a PUBREC Packet.
    #
    #  With this packet - forward the message onto any subscribers.
    #
    # It is the third packet of the QoS 2 protocol exchange.
    # http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718043
    # ----------------------------------------------------------------
    def pubrel_msg(self, mem_view):
        # Fixed header - 2 bytes
        remaining_length, bytes_used = msg_get_var_int(mem_view, 1) # This is the length of Variable Header plus the length of the Payload

        purel_identifier = msg_get_int(mem_view, bytes_used + 1, 2)

        if purel_identifier == self.packet_identifier:
            # Got confirmatiom for the PUBREL Packet
            if self.userdata:  # if self.userdata is 0 --> no subscription to the topic
                ret_val = mqtt_client.process_message(self.topic_name, self.userdata, self.payload)  # Transfer data to MQTT Client
                if not ret_val:
                    ret_val = self.send_pubcomp()   # the PUBCOMP Packet is the response to a PUBREL Packet. It is the fourth and final packet of the QoS 2 protocol exchange.
        else:
            ret_val = process_status.SUCCESS        # Ignore the message
            if self.trace_level:
                message = "\r\n[MQTT] [Publish] [QoS 2] [Packet confirmation returned wrong value]"
                utils_print.output(message, True)

        return ret_val
    # ----------------------------------------------------------------
    # the PUBCOMP Packet is the response to a PUBREL Packet.
    # It is the fourth and final packet of the QoS 2 protocol exchange.
    # ----------------------------------------------------------------
    def send_pubcomp(self):
        list_pubcomp = [0x70, 0x2,  # Fixed header ( PUBACK )
                    # Variable Header
                    0x00,
                    0x00,
                    ]

        list_pubcomp[2] = self.packet_identifier >> 8
        list_pubcomp[3] = self.packet_identifier & 0xff

        msg_pubcomp = bytearray(list_pubcomp)
        ret_val = send_msg("MQTT", self.clientSoc, msg_pubcomp)
        return ret_val

    # ----------------------------------------------------------------
    # A PUBREC Packet is the response to a PUBLISH Packet with QoS 2.
    # It is the second packet of the QoS 2 protocol exchange.
    # ----------------------------------------------------------------
    def send_pubrec(self):

        list_pubrec = [0x50, 0x2,  # Fixed header ( PUBACK )
                    # Variable Header
                    0x00,
                    0x00,
                    ]

        list_pubrec[2] = self.packet_identifier >> 8
        list_pubrec[3] = self.packet_identifier & 0xff

        msg_pubrec = bytearray(list_pubrec)
        ret_val = send_msg("MQTT", self.clientSoc, msg_pubrec)
        return ret_val

    # ----------------------------------------------------------------
    # A PUBACK Packet is the response to a PUBLISH Packet with QoS level 1.
    # ----------------------------------------------------------------
    def send_puback(self):

        list_puback = [0x50, 0x2,  # Fixed header ( PUBACK )
                    # Variable Header
                    0x00,
                    0x00,
                    ]

        list_puback[2] = self.packet_identifier >> 8
        list_puback[3] = self.packet_identifier & 0xff


        msg_puback = bytearray(list_puback)
        ret_val = send_msg("MQTT", self.clientSoc, msg_puback)
        return ret_val
    # ----------------------------------------------------------------
    # Validate user with basic authentication
    # with a new user - authenticate the user
    # ----------------------------------------------------------------
    def register_user(self, status, user_name, password):

        if user_name == self.user_name and password == self.password and self.authenticated:
            ret_val = process_status.SUCCESS       # Same user as before and was authenticated
        else:
            if not version.al_auth_validate_basic_auth(status, user_name, password):
                ret_val = process_status.Failed_message_authentication
            else:
                # Authenticated
                self.user_name = user_name
                self.password = password
                self.authenticated = True
                ret_val = process_status.SUCCESS
        return ret_val

    # ----------------------------------------------------------------
    # An UNSUBSCRIBE message is sent by the client to the server to unsubscribe from named topics.
    # UNSUBSCRIBE messages use QoS level 1 to acknowledge multiple unsubscribe requests. The corresponding UNSUBACK message
    # is identified by the Message ID. Retries are handled in the same way as PUBLISH messages.
    # Details in section 3.10 - https://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#unsubscribe
    # ----------------------------------------------------------------
    def unsubscribe_mqtt(self, mem_view):

        unsuback = [0x0b, 0x0,  # Fixed header ( UNSUBACK  - 1100)
                    # Variable Header - Section 3.1.2
                    0x00,  # 0
                    0x02,  # Remaining length (2)
                    0x00,  # 	Message ID MSB (0)
                    0x10   #    Message ID LSB (10)
                    ]

        msg_ack = bytearray(unsuback)


        # Callback for MQTT is explained here - http://www.steves-internet-guide.com/mqtt-python-callbacks/
        ret_val = send_msg("MQTT", self.clientSoc, msg_ack)

        return ret_val

# ----------------------------------------------------------------
# --------------------- -------------------------------------------
# RSYSLOG MESSAGES - No explicit header
# ----------------------------------------------------------------
# ----------------------------------------------------------------

class GENERIC_MSG(SESSION):

    def reset(self):
        self.len_added_info = 0 # the length of al.sl.header.[dbms_mame].[table_name]

        mapping_cond = self.rules_obj[1]        # The user mapping info
        self.syslog_format = interpreter.get_one_value(mapping_cond, "format")

        structure = interpreter.get_one_value(mapping_cond, "structure")    # If structure value is included, first row includes the structure
        if structure:
            self.mapping_attributes = None  # will be determined by the first row
        else:
            self.mapping_attributes = mapping_types_[self.syslog_format]

        # With header, user data starts after the header
        self.header_len = 0 if not "header" in self.rules_obj[0] else (len(self.rules_obj[0]["header"][0]) + 1)
        self.topic_name = None if not "topic" in self.rules_obj[1] else self.rules_obj[1]["topic"][0]
        if self.topic_name and self.topic_name in mqtt_topics_:  # there is a subscription to the topic
            # The topic name was registered on the local broker
            # Move the published data to the streamer through the MQTT Client on_message process
            self.userdata = mqtt_topics_[self.topic_name]
        else:
            self.userdata = 0

        self.dbms_name = mapping_cond["dbms"][0]
        self.table_name = mapping_cond["table"][0]

        self.prep_dir = get_param("prep_dir")
        self.watch_dir = get_param("watch_dir")
        self.err_dir = get_param("err_dir")

        self.statistics = self.rules_obj[2] # Number of batches processed, Counter rows processed, Counter errors, Error message

    def get_seesion_id(self):
        # The Client Identifier is the session ID
        return 0

    def get_msg_len(self):
        # Length as determined by the message header
        return 0

    # ----------------------------------------------------------------
    # Process the messages on the data block
    # ----------------------------------------------------------------
    def process_msg(self, status, mem_view, length):

        self.statistics[0] += 1         # Number of batches processed

        byte_message = bytearray(mem_view)
        str_message = byte_message[:length].decode('utf-8')
        msg_list = str_message.split('\n')


        if len(byte_message) == self.message_size:
            # get more data
            ret_val = MESSAGE_NOT_COMPLETE
            index_last = len(msg_list) - 1      # Do not process the last message as it may not be a complete message
            remaining_length = len(msg_list[-1])  # Size of last message
            # Set sufix of next message to process
            mem_view[:remaining_length] = mem_view[self.message_size-remaining_length: self.message_size]
        else:
            # session completed
            ret_val = END_SESSION
            index_last = len(msg_list)
            remaining_length = 0

        if self.topic_name and not self.userdata:
            self.statistics[2] += 1  # Number of errors
            self.statistics[3] = f"The topic '{self.topic_name}' is not registered on the local broker"
            return [process_status.Unrecognized_mqtt_topic, remaining_length]

        if not self.mapping_attributes:
            # determine the structure from the first row
            self.mapping_attributes = set_structure_from_row(msg_list[0], self.header_len)
            index = 1   # Start from the second row
        else:
            index = 0   # start from the first row

        last_id = len(self.mapping_attributes) - 1

        while index < index_last:
            json_msg = "{"
            message = msg_list[index]
            if not message:
                break

            msg_offset = self.header_len      # the offset considered - start at 0 or after the header that was added to syslog gen.
            for attr_id, attr_info in enumerate(self.mapping_attributes):
                # Get the attr info
                attr_val = ""
                attr_name = attr_info[0]
                attr_offset = attr_info[1]
                start_char = attr_info[2]
                end_char = attr_info[3]
                attr_format = attr_info[4]
                attr_length = attr_info[5]

                if attr_offset != -1:
                    # if  FIXED SIZE columns
                    msg_offset = self.header_len + attr_offset
                else:
                    if start_char:
                        # For example < sigh or [ sign
                        offset_start = message.find(start_char, msg_offset)
                        if offset_start == -1:
                            self.statistics[2] += 1  # Number of errors
                            self.statistics[3] = f"Missing start attribute sign '{start_char}' for column '{attr_name}' - column ignored"
                            continue        # no start_char
                        msg_offset = offset_start + 1

                    if end_char:
                        offset_end = message.find(end_char, msg_offset)
                        if offset_end == -1:
                            self.statistics[2] += 1  # Number of errors
                            self.statistics[3] = f"Missing end attribute sign '{start_char}' for column '{attr_name}' - column ignored"
                            continue        # no end_char

                        attr_val = message[msg_offset:offset_end]

                        msg_offset = offset_end + 1

                if not attr_val:
                    if attr_length:
                        attr_val = message[msg_offset:msg_offset + attr_length].strip() # Remove spaces to make it in original size
                        msg_offset += attr_length
                    elif attr_id == last_id:
                        # Last ID - take the entire message
                        if message[-1] == '\r':
                            # Ignore the \r
                            attr_val = message[msg_offset:-1]
                        else:
                            attr_val = message[msg_offset:]


                if attr_val and attr_format:
                    # Change the format
                    formatted_val = format_syslog_date(attr_val)
                    if not formatted_val:
                        self.statistics[2] += 1  # Number of errors
                        self.statistics[3] = f"Failed to format column '{attr_name}'"
                        continue
                else:
                    formatted_val = attr_val

                if formatted_val:
                    if len(json_msg) == 1:
                        # Without leading comma
                        json_msg += f"\"{attr_name}\" : \"{formatted_val}\""
                    else:
                        json_msg += f",\"{attr_name}\" : \"{formatted_val}\""

            json_msg += '}'
            self.msg_counter += 1

            if self.topic_name:
                ret_val = mqtt_client.process_message(self.topic_name, self.userdata, json_msg)  # Transfer data to MQTT Client
            else:
                ret_val, hash_value = add_data(status, "streaming", 1, self.prep_dir, self.watch_dir, self.err_dir, self.dbms_name, self.table_name, '0', '0', "json", json_msg)

            if not ret_val:
                self.statistics[1] += 1  # Rows processed
            else:
                self.statistics[2] += 1  # Number of errors
                self.statistics[3] = process_status.get_status_text(ret_val)

            index += 1

        return [ret_val, remaining_length]

# ----------------------------------------------------------------
# Set the structure from the first row.
# Assuming fixed column size up to the message data
# Creating the following columns:  Attribute name  start Char    End char   Format,            Length
# Example Data: al.sl.header.test.syslog Timestamp                       Thread     Type        Activity             PID    TTL
# ----------------------------------------------------------------
def set_structure_from_row( first_event, header_length ):
    '''
    first event determines the structure
    Header_length - size of header to ignore - in the example above, ignore al.sl.header.test.syslog
    '''

    private_struct = []

    # Get offset when column name starts
    event_len = len(first_event)
    offset = header_length
    attr_offset = offset
    previous_char = ' '
    attr_name = ""
    while offset < event_len:
        char = first_event[offset]

        if char == ' ':
            if previous_char != ' ':
                attr_name = first_event[attr_offset:offset]
        elif previous_char == ' ':
            if attr_name:
                # Avoid first call
                attr_size = offset - attr_offset -1
                private_struct.append((attr_name.lower(), attr_offset - header_length, None, None,  None,  attr_size))

                attr_offset = offset

        previous_char = char
        offset += 1

    # Column before Message
    attr_size = offset - attr_offset - 1
    attr_pos = attr_offset - header_length
    private_struct.append((attr_name.lower(),attr_pos , None, None, None, attr_size))

    # Add the message attribute
    private_struct.append(("message", attr_pos + attr_size + 1, None, None, None, 0))

    return private_struct


sessions_types = {
    "MQTT"  : MQTT_MESSAGES,
    "GENERIC_MSG" : GENERIC_MSG,                  # Syslog messages no header
}
# ----------------------------------------------------------------
# Init mqtt server
# The server protocol - https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.pdf
# Edgex setup - https://github.com/oshadmon/Udemy/blob/master/EdgeX/Lab_Setup.md
# Examole python HBMQTT https://hbmqtt.readthedocs.io/en/latest/
# ----------------------------------------------------------------
def message_broker( host: str, port: int, is_bind:bool, workers_count, trace ):
    global workers_pool_

    # Set a pool of workers threads
    workers_pool_ = utils_threads.WorkersPool("Message", workers_count)

    net_utils.message_server("Message Broker", "broker", host, port, 2048, workers_pool_, rceive_data, is_bind, trace)

    net_utils.remove_connection(2)

    workers_pool_ = None


# ----------------------------------------------------------------------------------
# Set rules to identify and mapp arbitrary messages like SysLog
# ----------------------------------------------------------------------------------
def set_msg_rules(status, rule_name, identify_cond, mapping_cond):

    remove_rule(rule_name)  # remove the old rue of exists

    source_ip = interpreter.get_one_value(identify_cond, "ip")
    source_port = interpreter.get_one_value(identify_cond, "port")

    msg_rules_stat = [
        0,                  # Number of batches processed
        0,                  # Counter for rows processed
        0,                  # Counter errors
        "",                 # Error message
    ]

    msg_rules_[rule_name] = (identify_cond, mapping_cond, msg_rules_stat)   # Keep the new rule

    ip_port_key = f"{source_ip}:{source_port}"            # Keep the rule name as f(IP and Port)

    if not ip_port_key in ip_rules_:
       ip_rules_[ip_port_key] = {}

    ip_rules_[ip_port_key][rule_name] = True

# ----------------------------------------------------------------------------------
# Get the message rules
# Satisfying the user command: get msg rules
# ----------------------------------------------------------------------------------
def get_msg_rules(status, io_buff_in, cmd_words, trace):

    output_table = []

    for rule_name, entry in msg_rules_.items():
        identify_cond = entry[0]
        mapping_cond = entry[1]
        statistics = entry[2]

        source_ip = interpreter.get_one_value_or_default(identify_cond, "ip", "*")
        source_port = interpreter.get_one_value_or_default(identify_cond, "port", 0)
        header = interpreter.get_one_value_or_default(identify_cond, "header", "")  # The offset in the header to validate

        dbms_name =  interpreter.get_one_value_or_default(mapping_cond, "dbms", "")
        table_name = interpreter.get_one_value_or_default(mapping_cond, "table", "")
        is_syslog = interpreter.get_one_value_or_default(mapping_cond, "syslog", False)
        topic = interpreter.get_one_value_or_default(mapping_cond, "topic", "")
        structure = interpreter.get_one_value_or_default(mapping_cond, "structure", "")

        batches = statistics[0]
        counter = statistics[1]
        counter_err= statistics[2]
        err_msg = statistics[3]

        output_table.append((rule_name, source_ip, source_port, header, dbms_name, table_name, is_syslog, topic, structure, batches, counter, counter_err, err_msg))


    reply = utils_print.output_nested_lists(output_table, "",
                       ["Name", "IF\nSource IP", "IF\nPort", "IF\nHeader", "THEN\nDBMS", "THEN\nTable", "THEN\nSysLog", "THEN\nTopic", "THEN\nStructure", "Batches", "Events", "Errors", "Error Msg"], True)

    return [process_status.SUCCESS, reply, "table"]

# ----------------------------------------------------------------------------------
# Remove entry from the rules tables
# Example: reset msg rule syslog
# ----------------------------------------------------------------------------------
def reset_msg_rules(status, io_buff_in, cmd_words, trace):
    return remove_rule (cmd_words[3])

# ----------------------------------------------------------------------------------
# Remove entry from the rules tables - 3 steps
# Remove the rule name from the structure representing the rules for an IP and Port
# Remove the IP and Port if no rules assigned
# ----------------------------------------------------------------------------------
def remove_rule(rule_name):

    if rule_name in msg_rules_:
        # Get the rule Obj as f(rule name)
        rules_object =  msg_rules_[rule_name]
        source_ip = interpreter.get_one_value_or_default(rules_object[0], "ip", "*")
        source_port = interpreter.get_one_value_or_default(rules_object[0], "port", '*')

        ip_port_key = f"{source_ip}:{source_port}"
        if ip_port_key in ip_rules_:    # The rule name by IP
            rules_names =  ip_rules_[ip_port_key]
            if rule_name in rules_names:
                rules_names[rule_name] = False  # Remove the entry as f(ip_port) - we don't delete to allow concurrency
        del msg_rules_[rule_name]
        ret_val = process_status.SUCCESS
    else:
        ret_val = process_status.Wrong_rule_name


    return ret_val

# ----------------------------------------------------------------------------------
# Determine the protocol to use
# ----------------------------------------------------------------------------------
def is_mqtt(mem_view):

    ret_val = False
    protocol_name_length = msg_get_int(mem_view, 2, 2)
    if protocol_name_length < 20:
        protocol_name = msg_get_bytes(mem_view, 4, protocol_name_length).lower()
        if protocol_name and (protocol_name == "mqtt" or protocol_name == "mqisdp'"):
            # In MQTT 3.1 the protocol name is "MQISDP". In MQTT 3.1.1 the protocol name is represented as "MQTT".
            # https://www.oasis-open.org/committees/download.php/55095/mqtt-diffs-v1.0-wd01.doc
            ret_val = True

    return ret_val

# ----------------------------------------------------------------------------------
# Determine if a rule applies to the message
# ----------------------------------------------------------------------------------
def get_rule_obj(clientSoc, mem_view):

    remote_ip, remote_port = clientSoc.getpeername()

    from_ip_port = f"{remote_ip}:{remote_port}"
    if from_ip_port in ip_rules_:        # Get the rule as f(IP and Port)
        rules_dict = ip_rules_[from_ip_port]
    else:
        source_ip_port = f"{remote_ip}:*"
        if source_ip_port in ip_rules_:
            rules_dict = ip_rules_[source_ip_port]
        else:
            if "*:*" in ip_rules_:
                rules_dict = ip_rules_["*:*"]
            else:
                rules_dict = None

    rules_object = None     # No rules associated with the data

    if rules_dict:
        # A dictionary with potential multiple rules for this IP and Port, find the header that match
        for rule_name, is_active in rules_dict.items():
            if is_active:       # User can remove rule using the command: reset msg rule [rule name]
                try:
                    rules_object_candidate =  msg_rules_[rule_name]
                except:
                    pass  # No rules associated with the data
                else:
                    identify_cond = rules_object_candidate[0]
                    prefix_header = interpreter.get_one_value(identify_cond, "header")  # The offset in the header to validate
                    if prefix_header:
                        # with text to test
                        header_data = msg_get_bytes(mem_view, 0, len(prefix_header))
                        if header_data == prefix_header:
                            rules_object = rules_object_candidate   # Overwrite a generic rule (with no conditions)
                            break
                    else:
                        # No condition, potentially the needed object
                        rules_object = rules_object_candidate

    return rules_object

# ----------------------------------------------------------------------------------
# Determine the protocol is SYSLOG with header formatting
# Generated by rsyslog
# MAC
# Example: (log show --info --start '2023-11-30 16:50:00' --end '2023-12-01 16:51:00' | awk '{print "al.sl.header.test.syslog", $0}') | nc -w 1 10.0.0.78 7850
# Linux
# journalctl -o json --since "2023-12-18 01:56:39" | jq  -c '{__REALTIME_TIMESTAMP, _HOSTNAME, SYSLOG_IDENTIFIER, PRIORITY, MESSAGE} | { "header": "al.sl.header.new_company.syslog", "data": . }' | nc -w 1 73.202.142.172 7849
# ----------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------
# Get bytes from mqtt message buffer
# ----------------------------------------------------------------------------------
def msg_get_bytes(msg_buff, offset, length):

    try:
        ret_bytes = bytes(msg_buff[offset:offset + length]).decode()
    except:
        errno, value = sys.exc_info()[:2]

        err_msg = "Failed to decode message withe error: %s : %s".format(str(errno), str(value))

        process_log.add("Error", err_msg)

        ret_bytes = None

    return ret_bytes
# ----------------------------------------------------------------------------------
# Get int from mqtt message buffer
# ----------------------------------------------------------------------------------
def msg_get_int(msg_buff, offset, length):

    try:
        int_value = int.from_bytes(msg_buff[offset: offset + length], byteorder='big', signed=False)
    except:
        errno, value = sys.exc_info()[:2]

        err_msg = "Failed to decode message withe error: %s : %s".format(str(errno), str(value))

        process_log.add("Error", err_msg)

        int_value = None
    return int_value


# ----------------------------------------------------------------------------------
# Get variable int from mqtt message buffer
# Explained in section 1.5.5 ( Line 268 )
#The Variable Byte Integer is encoded using an encoding scheme which uses a single byte for values up 299 to 127.
# Larger values are handled as follows:
# The least significant seven bits of each byte encode the data,
# and the most significant bit is used to indicate whether there are bytes following in the representation.
# ----------------------------------------------------------------------------------
def msg_get_var_int(msg_buff, offset):

    # else:
    multiplier = 1
    value = 0
    for i in range (4):          # up to 3 bytes
        encodedByte = msg_buff[offset + i]
        value += (encodedByte & 127) * multiplier
        if encodedByte <= 127:
            break
        multiplier *= 128

    return [value, i + 1]       # return value and size

# ----------------------------------------------------------------------------------
# send message to the client
# ----------------------------------------------------------------------------------
def send_msg(server_type, clientSoc, message_data):

    while 1:
        try:
            clientSoc.sendall(message_data)
        except:
            errno, value = sys.exc_info()[:2]
            err_msg = "{0} Server failed to send a messgae: {1} : {2}".format(server_type, str(errno), str(value))
            process_log.add("Error", err_msg)
            ret_val = process_status.MQTT_server_error
            break
        else:
            ret_val = process_status.SUCCESS
            break

    return ret_val

# ----------------------------------------------------------------------------------
# Log the message info
# ----------------------------------------------------------------------------------
def log_msg(mem_view):

    message_type = mem_view[0] >> 4  # 1st 4 bits - MQTT Control Packet types
    message_flags = mem_view[0] & 7  # low 4 bits -  Flags specific to each MQTT Control Packet types

    remaining_length, var_int_length = msg_get_var_int(mem_view, 1)  # Section 2.1.4 ( Line 416 )
    packet_length = remaining_length + 1  # 1 byte for  the message type (4 bits) and message flags (4 bits)
    packet_length += var_int_length  # size of the var int

    protocol_name_length = msg_get_int(mem_view, var_int_length + 1, 2)
    if protocol_name_length:
        protocol_name = msg_get_bytes(mem_view, var_int_length + 3, protocol_name_length)
        if protocol_name:
            char_str = ""
            for x in range (packet_length):
                byte_val = mem_view[x]
                if byte_val >= 0x20:
                    char_str += chr(byte_val)
                else:
                    char_str += '[%u:0x%s]' % (x,str(byte_val))

            log_buff = "\r\nMQTT: Type: %u Flags: %u length: %u  Protocol: %s String: %s" % (message_type, message_flags, packet_length, protocol_name, char_str)

            utils_print.output(log_buff, True)

# ----------------------------------------------------------------
# Register MQTT Topics when broker is "local"
# Command example:
# run mqtt client where broker = local and ...
# ----------------------------------------------------------------
def subscribe_mqtt_topics(status, client_id):

    global mqtt_topics_

    # Get the topics associated with this client id
    topics_dict = mqtt_client.get_topics_by_id( client_id )

    if not topics_dict:
        status.add_error("Message Broker Error: MQTT process: No topics associated with client id %u" % client_id)
        ret_val = process_status.ERR_process_failure
    else:
        ret_val = process_status.SUCCESS
        for topic in topics_dict:
            mqtt_topics_[topic] = client_id

    return ret_val
# ----------------------------------------------------------------
# unregister MQTT Topics when broker is "local"
# When "exit mqtt" is called
# ----------------------------------------------------------------
def unsubscribe_mqtt_topics(status, client_id):
    global mqtt_topics_

    # Get the topics associated with this client id
    topics_dict = mqtt_client.get_topics_by_id(client_id)

    if not topics_dict:
        status.add_error("Message Broker Error: MQTT process: No topics associated with client id %u" % client_id)
        ret_val = process_status.ERR_process_failure
    else:
        ret_val = process_status.SUCCESS
        for topic in topics_dict:
            if topic in mqtt_topics_ and mqtt_topics_[topic] == client_id:
                del mqtt_topics_[topic]

    return ret_val
# ----------------------------------------------------------------
# Count message events to support AnyLog command - "get messages"
# ----------------------------------------------------------------
def count_event(msg_protocol, msg_ip, msg_event_name, ret_code, details):
    global counter_events_  # protocol --> client --> event name --> success counter --> date --> error counter  --> date --> error_code

    # Get or set the protocol entry
    if msg_protocol not in counter_events_:
        protocol = {}
        counter_events_[msg_protocol] = protocol
    else:
        protocol = counter_events_[msg_protocol]

    # Get or set the IP entry
    if msg_ip not in protocol:
        ip = {}
        protocol[msg_ip] = ip
    else:
        ip = protocol[msg_ip]

    # Get or set the event name entry
    if msg_event_name not in ip:
        info_stat = [0,0,0,0,0,""]        # success counter --> date --> error counter  --> date --> error_code --> details
        ip[msg_event_name] = info_stat
    else:
        info_stat = ip[msg_event_name]

    # Update the info
    if ret_code:
        info_stat[2] += 1   # Error
        info_stat[3] = int(time.time())
        info_stat[4] = ret_code
        info_stat[5] = details
    else:
        info_stat[0] += 1   # Success
        info_stat[1] = int(time.time())

# ----------------------------------------------------------------
# Get info to support AnyLog command - "get messages"
# ----------------------------------------------------------------
def show_info():

    global counter_events_  # protocol --> client --> event name --> success counter --> date --> error counter  --> date --> error_code
    global info_title_

    info_table = []
    for protocol, protocol_info in counter_events_.items():
        for ip, ip_info in protocol_info.items():
            for event_name, event_info in ip_info.items():
                if event_info[1]:
                    success_date = seconds_to_date(event_info[1], "%Y-%m-%d %H:%M:%S")
                else:
                    success_date = ""
                if event_info[3]:
                    error_date = seconds_to_date(event_info[3], "%Y-%m-%d %H:%M:%S")
                else:
                    error_date = ""

                error_code = event_info[4]
                if not error_code:
                    err_msg = ""
                elif error_code < 2000:
                    err_msg = process_status.get_status_text(event_info[4])
                else:
                    err_msg = str(error_code)
                counter_success = format(event_info[0], ",")
                counter_error = format(event_info[2], ",")
                details = event_info[5]
                info_table.append([protocol, ip, event_name, counter_success, success_date, counter_error, error_date, err_msg, details])

    reply = utils_print.output_nested_lists(info_table, "Message Broker Stat", info_title_, True)
    return reply

# ------------------------------------------------------------------
# Return info on the TCP Server in command - show processes
# ------------------------------------------------------------------
def get_info( status = None ):
    global workers_pool_

    info_str = net_utils.get_connection_info(2)
    if workers_pool_:
        info_str += ", Threads Pool: %u" %  workers_pool_.get_number_of_threds()

    return info_str

# ----------------------------------------------------------------
# Receive data and process
# MQTT Example is here - http://www.steves-internet-guide.com/publishing-messages-mqtt-client/
# Echo Server:  https://realpython.com/python-sockets/
# https://github.com/eclipse/paho.mqtt.python/blob/master/tests/testsupport/broker.py
# ----------------------------------------------------------------
def rceive_data(status, mem_view, params, clientSoc, ip_in, port_in, thread_buff_size):

    #clientSoc.setblocking(1)  # wait for the data to be received (equal to soc.settimeout(None)

    clientSoc.settimeout(10)

    ret_val = process_status.SUCCESS
    msg_size = 0

    known_protocol = False
    session = None        # AN object created once the protocol is identified
    counter = 0
    rules_obj = None
    err_msg = ""
    protocol_name = ""

    data_buffer = mem_view
    data_buff_size = thread_buff_size

    while True:

        counter += 1

        trace_level = member_cmd.commands["run message broker"]['trace']

        if known_protocol:
            session.set_trace_level(trace_level)

        if ret_val == MESSAGE_NOT_COMPLETE:
            # This is not the first loop to get the message
            # Not all the data of the message is in the buffer
            if not session.get_msg_len():
                # Message length not determined
                if not rules_obj:
                    err_msg = "Message Broker terminated, large message and max message size not defined"
                    process_log.add("Error", err_msg)
                    break
                if data_buff_size < 2000000:
                    # Buffer can be increased up to 2 MB
                    data_buff_size += 2048
                    new_buffer = memoryview(bytearray(data_buff_size))
                    new_buffer[:msg_size] = data_buffer[:msg_size]  # Copy the message prefix to the new buffer
                    data_buffer = new_buffer
                else:
                    err_msg = "Message size larger than max allowed (2MB)"
                    process_log.add("Error", err_msg)
                    break

            elif session.get_msg_len() > data_buff_size:
                # Need to increase the buffer size to contain the message
                data_buff_size = session.get_msg_len()
                new_buffer = memoryview(bytearray(data_buff_size))
                new_buffer[:msg_size] = data_buffer[:msg_size]  # Copy the message prefix to the new buffer
                data_buffer = new_buffer

        try:
            #length = clientSoc.recv_into(data_buffer[msg_size:], data_buff_size - msg_size)
            # The send is in client.py line # 2335
            # In client.py - place breakpoints on line 664 (self._sock_recv) and line 667 (self._sock.send)
            length, address = clientSoc.recvfrom_into(data_buffer[msg_size:], data_buff_size - msg_size)

        except:
            if not net_utils.is_active_connection(2) or process_status.is_exit("broker", False):
                # is_active_connection is tested as "exit broker" may have reset the exit flag
                ret_val = process_status.SUCCESS
                break           # The main broker thread exited or all terminated

            errno, value = sys.exc_info()[:2]
            if errno == socket.timeout:
                ret_val = process_status.SUCCESS

                if not known_protocol:
                    if counter < 4:
                        continue        # wait more time before closing the socket
                    break              # No message showed up

                if not session or (message_time + session.get_keep_alive()) < int(time.time()):
                    break       # If no messages arrived for 50 seconds (default) --> close connections
                continue


            err_msg = "Message Broker disconnected from: %s with error: %s" % (str(clientSoc), str(value))
            process_log.add("Error", err_msg)
            ret_val = process_status.ERR_network
            break
        else:
            if trace_level > 1:
                try:
                    remote_ip, remote_port = clientSoc.getpeername()  # The IP and port of the peer node
                except:
                    remote_ip = "Not Recognized"
                    remote_port = 0
                msg_data = msg_get_bytes(mem_view, 0, 100)
                data_prefix = " ..." if msg_data[-1] != "" else ""       # A flag to indicate if all data is printed
                msg_data = msg_data.replace("\n","\\n").replace("\r", "\\r")    # Remove \n\r from the 100 bytes
                utils_print.output(f"[Message Broker Received {length} Bytes] [Source: {remote_ip}:{remote_port}] [Data: {msg_data}{data_prefix}]", True)

            if not length:
                ret_val = process_status.SUCCESS
                break

            #if len == 2048:
                #continue

            if not net_utils.is_active_connection(2) or process_status.is_exit("broker", False):
                # is_active_connection is tested as "exit broker" may have reset the exit flag
                ret_val = process_status.SUCCESS
                break           # The main broker thread exited or all terminated


            message_time = int(time.time())         # Get the time of the message received

            msg_size += length
            if msg_size < 3:
                continue                            # The minimum is 3 bytes - (type + size + message)
            if not known_protocol:
                # First call with this connection
                if msg_size < 8:
                    protocol_name = "not_complete"
                    continue        # Need more data - MQTT protocol requires 8 bytes
                if is_mqtt(mem_view):
                    protocol_name = "MQTT"
                    rules_obj = None
                else:
                    rules_obj = get_rule_obj(clientSoc, mem_view)
                    if rules_obj:
                        protocol_name = "GENERIC_MSG"    # sys log messages
                    else:
                        protocol_name = "UNKNOWN"
                        if trace_level > 1:
                            utils_print.output(f"[Exit] [Unrecognized Message Protocol]", True)
                        break

                known_protocol = True

                if session:
                    session.reset()
                else:
                    session = sessions_types[protocol_name](clientSoc, protocol_name, msg_size, rules_obj, trace_level)

            # process the protocol
            ret_val, msg_size = session.process_msg(status, data_buffer, msg_size)

            if ret_val == END_SESSION:
                break

            if ret_val == MESSAGE_NOT_COMPLETE:
                continue        # Not all the data of the message is available

    if trace_level:
        try:
            remote_ip, remote_port = clientSoc.getpeername()  # The IP and port of the peer node
        except:
            remote_ip = "Not Recognized"
            remote_port = 0

        if err_msg:
            err_msg = f" [Err: {err_msg}]"
        utils_print.output_box(f"Message Broker thread exists [Protocol: {protocol_name}] [Source: {remote_ip}:{remote_port}]{err_msg}")

    return ret_val

# ------------------------------------------------------------------
# Return info on the workers pool
# returns info when calling - get msg pool
# ------------------------------------------------------------------
def get_threads_obj():

    global workers_pool_
    return workers_pool_



