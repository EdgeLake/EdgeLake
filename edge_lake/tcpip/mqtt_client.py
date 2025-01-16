"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""


# Simple example - https://medium.com/python-point/mqtt-basics-with-python-examples-7c758e605d4

# Google:
# Using Paho with Google - https://cloud.google.com/iot/docs/how-tos/mqtt-bridge
# + blog: https://blog.kornelondigital.com/2019/12/28/google-cloud-iot-step-by-step-temperature-and-humidity-monitoring/
# Google connect example - https://stackoverflow.com/questions/49907529/google-cloud-iot-invalid-mqtt-publish-topic
# Google API - https://googleapis.dev/python/pubsub/latest/index.html
#For Google:
# CLIENT_ID = f"projects/{PROJECT_ID}/locations/{GCP_LOCATION}/registries/{REGISTRY_ID}/devices/{DEVICE_ID}"
# _MQTT_TOPIC = f"/devices/{DEVICE_ID}/events"

# Cloud MQTT Example - https://github.com/CloudMQTT/python-mqtt-example
import re
import sys
import threading
import copy

try:
    import paho.mqtt.client as mqtt
except:
    paho_installed = False
else:
    paho_installed = True

import time
from datetime import datetime, timedelta

try:
    import jwt  # Package version pyjwt==1.7.1
except:
    jwt_installed = False
else:
    jwt_installed = True

import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.interpreter as interpreter
import edge_lake.generic.process_log as process_log
import edge_lake.generic.process_status as process_status
import edge_lake.generic.streaming_data as streaming_data
import edge_lake.generic.utils_columns as utils_columns
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.utils_threads as utils_threads
from edge_lake.generic.params import get_value_if_available
import edge_lake.tcpip.net_utils as net_utils
import edge_lake.json_to_sql.mapping_policy as mapping_policy
from edge_lake.generic.node_info import get_node_name

if paho_installed == True:
    MQTT_ERR_AGAIN = mqtt.MQTT_ERR_AGAIN
    MQTT_ERR_SUCCESS = mqtt.MQTT_ERR_SUCCESS
    MQTT_ERR_PROTOCOL = mqtt.MQTT_ERR_PROTOCOL
    MQTT_ERR_INVAL= mqtt.MQTT_ERR_INVAL
    MQTT_ERR_NO_CONN = mqtt.MQTT_ERR_NO_CONN
    MQTT_ERR_CONN_REFUSED = mqtt.MQTT_ERR_CONN_REFUSED
    MQTT_ERR_NOT_FOUND = mqtt.MQTT_ERR_NOT_FOUND
    MQTT_ERR_TLS = mqtt.MQTT_ERR_TLS
    MQTT_ERR_PAYLOAD_SIZE = mqtt.MQTT_ERR_PAYLOAD_SIZE
    MQTT_ERR_NOT_SUPPORTED = mqtt.MQTT_ERR_NOT_SUPPORTED
    MQTT_ERR_AUTH = mqtt.MQTT_ERR_AUTH
    MQTT_ERR_ERRNO = mqtt.MQTT_ERR_ERRNO
else:
    MQTT_ERR_AGAIN = -1
    MQTT_ERR_SUCCESS = 0
    MQTT_ERR_PROTOCOL = 2
    MQTT_ERR_INVAL= 3
    MQTT_ERR_NO_CONN = 4
    MQTT_ERR_CONN_REFUSED = 5
    MQTT_ERR_NOT_FOUND = 6
    MQTT_ERR_TLS = 8
    MQTT_ERR_PAYLOAD_SIZE = 9
    MQTT_ERR_NOT_SUPPORTED = 10
    MQTT_ERR_AUTH = 11
    MQTT_ERR_ERRNO = 14

mqtt_errors = {

    MQTT_ERR_AGAIN : "MQTT_ERR_AGAIN",
    MQTT_ERR_PROTOCOL : "MQTT_ERR_PROTOCOL",
    MQTT_ERR_INVAL : "MQTT_ERR_INVAL",
    MQTT_ERR_NO_CONN : "MQTT_ERR_NO_CONN",
    MQTT_ERR_CONN_REFUSED : "MQTT_ERR_CONN_REFUSED",
    MQTT_ERR_NOT_FOUND : "MQTT_ERR_NOT_FOUND",
    MQTT_ERR_TLS : "MQTT_ERR_TLS",
    MQTT_ERR_PAYLOAD_SIZE : "MQTT_ERR_PAYLOAD_SIZE",
    MQTT_ERR_NOT_SUPPORTED : "MQTT_ERR_NOT_SUPPORTED",
    MQTT_ERR_AUTH : "MQTT_ERR_AUTH",
    MQTT_ERR_ERRNO : "MQTT_ERR_ERRNO",
}

clients_counter = 0     # Number of active clients
node_client_id = 0           # A unique id for each call to "run mqtt client"

#  broker_to_topic[ip_port][topic] = [client_id,  counter_msg, counter_err]  # Update the list of topics as f(broker) -> every entry includes the client ID + counter messages + counter errrors
broker_to_topic = {}    # A dictionary that maps each broker to the list of topics
subscriptions = {}      # A dictionary that provides the mapping from the user data to the DBMS tables as f(id + topic)
subscript_mutex = [None]    # AN array of read-writemutexes to protect subscriptions

debug_mode = False      # True will print the message data

non_subscribed_users_ = {}  # a dictionary of users connecting to the broker which are not subscribed to the broker.
                            # For example, a user that only publish data


al_state_not_connected_ = 0
al_state_connected_ = 1
al_state_not_published_ = 2
al_state_published_ = 2

# ----------------------------------------------------------------------
# A class that maintains the subscription info
# ----------------------------------------------------------------------
class SubscriptionInfo:

    def __init__(self):
        self.client_id = 0
        self.local_broker = False       # This flag is set to True if the connected to the Message Server
        self.kafka_consumer = False
        self.user_name = ""             # The user name provided to the connection call
        self.prep_dir = None
        self.watch_dir = None
        self.bwatch_dir = None
        self.err_dir = None
        self.blobs_dir = None
        self.topics = {}
        self.status = None
        self.message_counter = 0
        self.error_counter = 0
        self.last_error = 0
        self.msg_time = ""
        self.error_time = ""
        self.exit_client = False       # This flag is set to True when exit is called
        self.ip_port = None
        self.ip_name = None            # This name is used if message_flush is set to True, it determines the file name
        self.log_error = False         # True value indicates that messages that are not processed correctly are logged
        self.connect_rc = MQTT_ERR_NO_CONN     # Updated with the return value from on_connect()
        self.user_agent = None         # user-agent determines a REST header associated with the mqtt client
        self.statistics = {}           # as f(topic) maintain: counter messages success, counter messages failed, last error time, last error message
        self.connect_counter = 0
        self.disconnected_flag = False  # Set to True after the call to client.disconnect()

    def get_client_id(self):
        return self.client_id

    def new_client_id(self):
        '''
        Provide a unique client ID - this is done when the broker is registered - resister_by_broker()
        '''
        global node_client_id
        node_client_id += 1
        self.client_id = node_client_id
        return self.client_id

    def get_disconnect_flag(self):
        '''
        the Flag is set to True after the call to client.disconnect()
        '''
        return self.disconnected_flag

    def flag_disconnected(self):
        '''
        Set to True after the call to client.disconnect()
        '''
        self.disconnected_flag = True

    def count_connect_attempts(self):
        '''
        Count the number of connection attempts
        '''
        self.connect_counter += 1
        return self.connect_counter

    def reset_connect_counter(self):
        '''
        Reset the counter that determines the number of failures in connection attempts
        '''
        self.connect_counter = 0

    def set_statistics(self, topic, ret_val):
        '''
        Statistics per client as f(broker) - counter success, counter error, last error, last error time
        '''
        if not topic in self.statistics:
            self.statistics[topic] = [0,0,0,""]
        if not ret_val:
            self.statistics[topic][0] += 1
        else:
            self.statistics[topic][1] += 1
            self.statistics[topic][2] = ret_val     # Keep last error
            self.statistics[topic][3] = utils_columns.get_current_time("%Y-%m-%d %H:%M:%S")

    def get_statistics(self):
        '''
        Return client stat
        '''
        return self.statistics

    def is_local(self):
        # Returns True if subscribed to the local message server
        return self.local_broker

    def set_local(self):
        '''
        Set to true when the client is called from the local message broker or from REST calls
        '''
        self.local_broker = True

    def is_kafka(self):
        # Returns True if configured as Kafka Consumer
        return self.kafka_consumer

    def set_kafka(self):
        # Flag to indicate configured as Kafka Consumer
        self.kafka_consumer = True

    def set_user_agent(self,user_agent):
        '''
        Set with value when REST calls are calling the MQTT client.
        user-agent determines the header associated with the mqtt client
        '''
        self.user_agent = user_agent

    def get_user_agent(self):
        return self.user_agent

    def set_connect_rc(self, rc):
        self.connect_rc = rc

    def get_connect_rc(self):
        return self.connect_rc

    def set_user_name(self, user_name):
        self.user_name = user_name

    def get_user_name(self):
        return self.user_name

    def is_exit(self):
        return self.exit_client

    def set_exit(self):
        self.exit_client = True

    def set_ip_port(self, ip_port):
        self.ip_port = ip_port
        ip_name = ip_port.replace('.','_')
        self.ip_name = ip_name.replace(':','_') # This name is used if message_flush is set to True, it determines the file name

    def set_log_error(self, value:bool):
        self.log_error = value

    def is_log_error(self):
        return self.log_error

    def get_ip_port(self):
        return self.ip_port

    def get_ip_name(self):
        return self.ip_name

    def set_work_dirs(self, prep_dir, watch_dir, bwatch_dir, err_dir, blobs_dir):
        self.prep_dir = prep_dir
        self.watch_dir = watch_dir
        self.bwatch_dir = bwatch_dir     # Directory for JSON describing the blobs
        self.err_dir = err_dir
        self.blobs_dir = blobs_dir        # Directory for blob storage

    def get_work_dirs(self):
        return [self.prep_dir, self.watch_dir, self.bwatch_dir, self.err_dir, self.blobs_dir]

    def get_topics_dict(self):
        return self.topics

    def set_status(self, status):
        self.status = status

    def get_status(self):
        return self.status

    def get_topic_info(self, topic):
        if topic in self.topics:
            topic_info = self.topics[topic]
        else:
            topic_info = None
        return topic_info

    def is_message_flush(self, topic):
        '''
        The message has no bring info and no policy info - source message written to file, but not processed
        Registration with bring
       self.topics[topic] = (dbms, table, qos, mapping, None)     # Update the subscription info for this client
        or registration with policy
        self.topics[topic] = (None, None, qos, None, policies_info)  # Add topics to list
        '''
        if not topic in self.topics:
            return False
        topic_info = self.topics[topic]
        return topic_info[5]   # Bring not defined and policy not defined

    def is_with_policies(self, topic):
        '''
        The message has no bring info and no policy info - source message written to file, but not processed
        Registration with bring
        self.topics[topic] = (dbms, table, qos, mapping, None)     # Update the subscription info for this client
        or registration with policy
        self.topics[topic] = (None, None, qos, None, policies_info)  # Add topics to list
        '''
        if not topic in self.topics:
            return False

        topic_info = self.topics[topic]
        return (topic_info[4] != None)   # Policy is defined

    def get_message_counter(self):
       return self.message_counter

    def get_error_counter(self):
       return self.error_counter

    def get_last_error(self):
       return self.last_error

    def get_msg_time(self):
        return self.msg_time

    def get_error_time(self):
        return self.error_time

    def count_messages(self, ret_value):
        self.message_counter += 1
        self.msg_time = utils_columns.get_current_time("%Y-%m-%d %H:%M:%S")
        if ret_value:
            self.error_counter += 1
            self.last_error = ret_value
            self.error_time = self.msg_time

    # ----------------------------------------------------------------------
    # Add a new topic to the list of topics of this client
    # ----------------------------------------------------------------------
    def add_topic(self, name, dbms, table, qos, mapping, policies, persist):
        '''
        name - topic name
        dbms - dbms name (if mapping instructions)
        table - table name (if mapping instructions)
        qos - Quality of service
        mapping - mapping instructions
        policies - a list of policies assigned (optional and replacing name, dbms and mapping)
        persist - if True, only write to log file
        '''

        self.topics[name] = (dbms, table, qos, mapping, policies, persist)

# ----------------------------------------------------------------------
# Test if needed lib in installed
# ----------------------------------------------------------------------
def is_installed():
    global paho_installed
    return paho_installed

# ----------------------------------------------------------------------
# Satisfy command: get msg client statistics
# Show the list of client subscriptions and stat per client
# ----------------------------------------------------------------------
def show_stat( client_id ):
    global node_client_id


    if not len(subscriptions):
        reply = "\r\nNo message client subscriptions"
    else:

        if client_id:
            start_client = client_id
            end_client = client_id + 1
        else:
            start_client = 1
            end_client = node_client_id + 1

        reply = ""
        for i in range(start_client, end_client):
            if i in subscriptions:
                reply += get_subscriptions_stat(i)

                try:
                    topics_dict = subscriptions[i].get_statistics()  # The stat as f(topic)
                except:
                    continue    # No such client
                else:
                    # get the dict info
                    out_list = []
                    for topic_name, topic_stat in topics_dict.items():
                        # Stat includes: counter success, counter error, last error, last error time
                        success = topic_stat[0]
                        failure = topic_stat[1]
                        last_error = topic_stat[2]
                        if last_error:
                            err_msg = process_status.get_status_text(last_error)
                            last_error_time = topic_stat[3]
                        else:
                            err_msg = ""
                            last_error_time = ""

                        out_list.append((topic_name, success, failure, last_error_time, err_msg))
                    reply += utils_print.output_nested_lists(out_list, "\n\r", ["Topic", "Success", "Failure", "Last Error Time", "Last Error"], True, "     ")

    return reply
# ----------------------------------------------------------------------
# Satisfy command: get msg client
# Show the list of subscriptions
# mqtt_map[topic] = [dbms_name, table_name, qos, 0]
# ----------------------------------------------------------------------
def show_info( client_id, is_detailed, broker, topic ):

    global clients_counter
    global node_client_id

    if not len(subscriptions):
        reply = "\r\nNo message client subscriptions"
    else:
        if client_id:
            # show specific subscription
            if not client_id in subscriptions:
                reply = "\r\nSubscription %u is not in use" % client_id
            else:
                reply = get_subscriptions_info(client_id, is_detailed, broker, topic)
        else:
            reply = ""
            for i in range(1, node_client_id + 1):
                if i in subscriptions:
                    reply += get_subscriptions_info(i, is_detailed, broker, topic)

    return reply

# ----------------------------------------------------------------------
# Return a list with all subscriptions to the topic
# ----------------------------------------------------------------------
def get_subscriptions_by_topic(topic:str):
    global subscriptions

    clients_list =[]
    for topics_dict in broker_to_topic.values():
        if topic in topics_dict:
            topic_info = topics_dict[topic]
            clients_list.append(topic_info[0])

    return clients_list

# ----------------------------------------------------------------------
# Get the list of topics for a client_id
# ----------------------------------------------------------------------
def get_topics_by_id( client_id ):

    if client_id in subscriptions:
        client_sbscr = subscriptions[client_id]  # The object with the subscription info
        topics_dict = client_sbscr.get_topics_dict()
    else:
        topics_dict = None

    return topics_dict

# ----------------------------------------------------------------------
# Return the info on a single subscription statistics
# ----------------------------------------------------------------------
def get_subscriptions_stat(client_id):

    client_sbscr = subscriptions[client_id]  # The object with the subscription info


    if client_sbscr.is_local():
        # Connected to Message Server
        connect_stat = "Connected to local Message Server"
    elif client_sbscr.is_kafka():
        connect_stat = "Kafka Consumer"
    else:
        connection_rc = client_sbscr.get_connect_rc()  # The returned code of the connection
        if not connection_rc:
            connect_stat = "Connected"
        else:
            connect_stat = "Not connected"
            if connection_rc in mqtt_errors.keys():
                connect_stat += ": %s" % mqtt_errors[connection_rc]
            if client_sbscr.get_disconnect_flag():
                connect_stat += ": Disconnected from server"


    info_str = "\r\nSubscription ID: %04u\r\nUser:         %s\r\nBroker:       %s\r\nConnection:   %s\r\n"\
               % (client_id, client_sbscr.get_user_name(), client_sbscr.get_ip_port(), connect_stat)

    msg_counter = client_sbscr.get_message_counter()
    err_counter = client_sbscr.get_error_counter()
    success_counter = msg_counter - err_counter
    last_err = client_sbscr.get_last_error()
    msg_time = client_sbscr.get_msg_time()

    info_str += "\r\n     Messages    Success     Errors      Last message time    Last error time      Last Error"
    info_str += "\r\n     ----------  ----------  ----------  -------------------  -------------------  ----------------------------------\r\n"

    info_str += str(msg_counter).rjust(15)
    info_str += str(success_counter).rjust(12)
    info_str += str(err_counter).rjust(12)
    info_str += "  %s" % msg_time
    if err_counter:
        err_time = client_sbscr.get_error_time()
        err_msg = process_status.get_status_text(last_err)
        info_str += "  %s  %s" % (err_time, err_msg)
    return info_str

# ----------------------------------------------------------------------
# Return the info on a single subscription
# ----------------------------------------------------------------------
def get_subscriptions_info(client_id, is_detailed, broker_ip_port, topic_requested):
    global subscriptions

    if broker_ip_port:
        ip_port = subscriptions[client_id].get_ip_port()
        if not ip_port or ip_port!= broker_ip_port:
            return ""   # No such broker


    client_sbscr = subscriptions[client_id]  # The object with the subscription info

    topics_dict = client_sbscr.get_topics_dict()

    if topic_requested and not topic_requested in topics_dict:
        return ""           # no such topic in this broker

    info_str = get_subscriptions_stat(client_id)

    # Get the Topics info

    subscription_array = []
    # Get the topics info
    for topic, value in topics_dict.items():
        if topic_requested and topic_requested != topic:
            continue
        if client_sbscr.is_message_flush(topic):
            # Only flushing the message data
            qos = value[2]
            subscription_array.append((topic, qos, "*******", "*******", "", "", "","", None))
        elif  client_sbscr.is_with_policies(topic):
            # Mapping is based on policies
            qos = value[2]
            cmd_policies = get_cmd_policies(value[4], False)   # Get the blockchain command to retrieve the policies


            subscription_array.append((topic, qos, "", "", "", "", "", "", cmd_policies))
        else:
            dbms = value[0]
            table = value[1]
            qos = value[2]
            mapping  = value[3]
            for entry in mapping:
                # every column name is an entry
                subscription_array.append((topic, qos, dbms, table, entry[0], entry[1], str(entry[2]), str(entry[3]),None))
                dbms = ""
                table = ""
                qos = ""
                topic = ""

    info_str += utils_print.output_nested_lists(subscription_array, "\r\n     Subscribed Topics:", ["Topic", "QOS", "DBMS", "Table", "Column name", "Column Type", "Mapping Function", "Optional", "Policies"], True, "     ")

    if is_detailed:
        # Get directories used
        dir_array = []
        prep_dir, watch_dir, bwatch_dir, err_dir, blobs_dir = client_sbscr.get_work_dirs()
        dir_array.append(("Prep Dir", prep_dir))
        dir_array.append(("Watch Dir", watch_dir))
        dir_array.append(("Bwatch Dir", bwatch_dir))
        dir_array.append(("Error Dir", err_dir))
        dir_array.append(("Blobs Dir", blobs_dir))

        info_str += utils_print.output_nested_lists(dir_array, "\r\n     Directories Used:",
                                                ["Directory Name", "Location"], True, "     ")

    return info_str
# ----------------------------------------------------------------------
# Get the blockchain command to retrieve the policies
# ----------------------------------------------------------------------
def get_cmd_policies( policies, is_id_list ):

    command = "blockchain get (mapping,transform) where"
    for index, policy in enumerate(policies):
        if index:
            command += " or"
        if is_id_list:
            # A list of IDs
            policy_id = policy
        else:
            # Get the ids from the policies
            try:
                policy_id = policy["mapping"]["id"] if "mapping" in policy else policy["transform"]["id"]
            except:
                command += " [Failed to identify ID in policy]"
                break

        command += " [id] == %s" % policy_id
    return command


# ----------------------------------------------------------------------
# Show the list of brokers and the subscribed topics and their clients
# ----------------------------------------------------------------------
def show_brokers():
    global broker_to_topic

    if not len(broker_to_topic):
        info_str = "\r\nNo clients assigned to brokers"
    else:
        table_info = []
        for broker, topics_dict in broker_to_topic.items():
            for name, topic_info in topics_dict.items():
                client_id = topic_info[0]
                counter_msg = topic_info[1]
                counter_err = topic_info[2]
                table_info.append((broker, name, client_id, counter_msg, counter_msg - counter_err,  counter_err))

        info_str = utils_print.output_nested_lists(table_info, "",
                                                ["Broker", "Topic", "Client ID", "Messages", "Success", "Failure"], True, "     ")
    return info_str

# ----------------------------------------------------------------------
# Test if MQTT Client process is running
# ----------------------------------------------------------------------
def is_running():
    # At least one subscription
    return len(subscriptions) > 0

# ----------------------------------------------------------------------
# Place Error message on the AnyLog Log
# ----------------------------------------------------------------------
def place_error_msg(error_code, method_name, is_print):
    if error_code in mqtt_errors.keys():
        message = "MQTT/%s Error: %s" % (method_name, mqtt_errors[error_code])
    else:
        message = "MQTT/%s Error: Unrecognized error value: %u" % (method_name, error_code)

    if is_print:
        process_log.add_and_print("Error", message)
    else:
        process_log.add("Error", message)

# ----------------------------------------------------------------------
# Get the list of topics and QoS, build a map of topics to tables
# ----------------------------------------------------------------------
def set_subscription(client_id):

    subscription_array = []

    client_sbscr = subscriptions[client_id]     # The object with the subscription info
    topics_dict = client_sbscr.get_topics_dict()
    for topic, value in topics_dict.items():
        qos = value[2]
        subscription_array.append((topic, qos))

    return subscription_array

# ----------------------------------------------------------------------
# MQTT callbacks - https://pypi.org/project/paho-mqtt/#callbacks
# ----------------------------------------------------------------------
def on_log(client, userdata, level, buff):
    utils_print.output("\r\nMQTT Log: %s" % buff, True)

# ----------------------------------------------------------------------
# MQTT callbacks - https://pypi.org/project/paho-mqtt/#callbacks
# ----------------------------------------------------------------------
def on_message(client, userdata, message):
    # print("received message: " ,str(message.payload.decode("utf-8")))
    global debug_mode

    ret_val = process_status.MQTT_data_err

    topic = message.topic       # The topic on the message
    # Get the user message
    user_msg = message.payload.decode("utf-8")

    process_message( topic, userdata, user_msg)

# ----------------------------------------------------------------------
# Process a message send by the broker
# ----------------------------------------------------------------------
def process_message( topic, user_id, user_msg):
    '''
    topic - the topic associated with the data
    user_id - the AnyLog  user ID
    user_msg - the data delivered
    '''

    global subscript_mutex

    ret_val = process_status.SUCCESS
    mutex_taken = False

    try:
        client_sbscr = subscriptions[user_id]  # The mapping info as f(topic)
    except:
        user_msg = "MQTT message without subscription for topic: '%s' and client id: %s" % (topic, str(user_id))
        ret_val = process_status.MQTT_wrong_client_id
    else:
        # Find the subscription info on this message.
        topic_info = client_sbscr.get_topic_info(topic)

        if not topic_info:
            user_msg = "Unrecognized topic in MQTT process: %s" % topic
            ret_val = process_status.Unrecognized_mqtt_topic


    if ret_val:
        process_log.add("Error", user_msg)
    else:

        if not isinstance(user_id, int) or len(subscript_mutex) <= user_id:
            user_msg = "MQTT message with unrecognized user data for topic: '%s' and client id: %s" % (topic, str(user_id))
            ret_val = process_status.MQTT_wrong_client_id
        else:
            ret_val = process_status.SUCCESS

            # take a mutex (readlock) to protect the registration to the topic
            subscript_mutex[user_id].acquire_read()
            mutex_taken = True

            status = client_sbscr.get_status()
            prep_dir, watch_dir, bwatch_dir, err_dir, blobs_dir = client_sbscr.get_work_dirs()

            if client_sbscr.is_message_flush(topic):
                # Only write messages without processing
                dbms_name = client_sbscr.get_ip_name()  # A name based on the IP address of the broker
                table_name = utils_data.reset_str_chars(topic)
                insert_list = [user_msg]
                ret_val = process_status.SUCCESS
            else:
                # Pull needed data from the JSON
                if not user_msg:
                    process_log.add("Error", f"MQTT missing message data with topic '{topic}'")
                    ret_val = process_status.MQTT_not_in_json
                else:
                    struct_type = utils_data.get_str_obj(user_msg)  # Returns "dict" or "list" or "none"
                    if struct_type == "dict":
                        json_msg = utils_json.str_to_json(user_msg)
                        if not json_msg:
                            process_log.add("Error", f"MQTT message is not in JSON format (Failed to map string to JSON)")
                            ret_val = process_status.MQTT_not_in_json
                    elif struct_type == "list":
                        json_list = utils_json.str_to_list(user_msg)
                        if not json_list:
                            process_log.add("Error", f"MQTT format error (Failed to map string to JSON List)")
                            ret_val = process_status.MQTT_not_in_json
                        else:
                            json_msg = None
                    else:
                        process_log.add("Error", f"MQTT message format is not recognized (data assigned to topic '{topic}')")
                        ret_val = process_status.MQTT_not_in_json


                if not ret_val:
                    # Find the subscription info on this message.
                    topic_info = client_sbscr.get_topic_info(topic)

                    if not topic_info:
                        process_log.add("Error", "Unrecognized topic in MQTT process: %s" % topic)
                        ret_val = process_status.Unrecognized_mqtt_topic
                    else:

                        if client_sbscr.is_with_policies(topic):
                            # Use Policies
                            if json_msg:
                                # A single entry
                                ret_val = process_using_policies(status, topic_info, topic, json_msg, prep_dir, watch_dir, bwatch_dir, err_dir, blobs_dir)
                            else:
                                # List of entries
                                for entry in json_list:
                                    if not isinstance(entry, dict):
                                        process_log.add("Error", f"MQTT message assigned to policy in a list is not in JSON format (List entry is not in JSON format)")
                                        ret_val = process_status.MQTT_not_in_json
                                    else:
                                        ret_val = process_using_policies(status, topic_info, topic, entry, prep_dir, watch_dir, bwatch_dir, err_dir, blobs_dir)
                                        if ret_val:
                                            break
                        else:
                            # Use the BRING command
                            if json_msg:
                                # A single entry
                                ret_val = process_using_bring(status, topic_info, topic, json_msg, prep_dir, watch_dir, err_dir)
                            else:
                                for entry in json_list:
                                    if not isinstance(entry, dict):
                                        process_log.add("Error", f"MQTT message assigned to bring command in a list is not in JSON format (List entry is not in JSON format)")
                                        ret_val = process_status.MQTT_not_in_json
                                    else:
                                        ret_val = process_using_bring(status, topic_info, topic, entry, prep_dir, watch_dir, err_dir)
                                        if ret_val:
                                            break
            # Update stats
            if not client_sbscr.is_message_flush(topic):
                # Stat per client
                client_sbscr.set_statistics(topic, ret_val)

                # Stat per broker
                ip_port = client_sbscr.get_ip_port()
                topics_dict = broker_to_topic[ip_port]      # List of topics for this broker
                if topic in topics_dict:
                    topic_info = topics_dict[topic]
                else:
                    topic, topic_info = get_partial_match_topic(topics_dict, topic)
                    if not topic_info:
                        status.add_error("Received message without subscription on topic: '%s'" % topic)
                        ret_val = process_status.MQTT_non_subscribed_err

                if topic_info:
                    topic_info[1] += 1      # Counter messages

                if ret_val:
                    if topic_info:
                        topic_info[2] += 1  # Counter Errors
                    if client_sbscr.is_log_error():
                        # Write the user data to an error directory
                        # This option is enables by setting "log_error = true" on the command line
                        dbms_name = "err_" + client_sbscr.get_ip_name()  # A name based on the IP address of the broker
                        table_name = topic
                        try:
                            ret_val, hash_value = streaming_data.add_data(status, "streaming", 1, err_dir, err_dir, err_dir, dbms_name, table_name, '0', '0', 'json', user_msg)
                        except:
                            utils_print.output_box("Failed to stream data to err dir at mqtt_client call to streaming_data.add_data(): '%s' with topic: '%s'" % (user_msg, topic))
                            ret_val = process_status.MQTT_data_err

    client_sbscr.count_messages(ret_val)

    if debug_mode:
        # print message - enabled using the command: set mqtt debug
        debug_msg = "\r\nMQTT Status: [%02u] Message: [%s]" % (ret_val, user_msg)
        utils_print.output(debug_msg, True)

    if mutex_taken:
        subscript_mutex[user_id].release_read()

    return ret_val


# ----------------------------------------------------------------------
# Get a topic by comparison up to the hashtag (if exists)
# ----------------------------------------------------------------------
def get_partial_match_topic(topics_dict, topic):
    """
    topics_dict - the dictionary with info as f(topic). these topics can include hashtag
    topic - the topic published withhou the hashtag
    """

    # Find if topics with partial match
    topic_info = None
    updated_topic = topic
    for key in topics_dict:
        if key[-1] == '#':
            # Match prefix
            key_len = len(key)
            if key_len == 1:
                topic_info = topics_dict[key]  # MAP All topics
                updated_topic = key            # The topic with the hashtag
                break
            elif key[:-1] == topic[:key_len - 1]:
                topic_info = topics_dict[key]  # Prefix match
                updated_topic = key
                break

    return [updated_topic, topic_info]

# ----------------------------------------------------------------------
# Process a message using the bring command
# ----------------------------------------------------------------------
def process_using_bring(status, topic_info, topic, json_msg, prep_dir, watch_dir, err_dir):

    msg_list = [json_msg]

    bring_dbms = topic_info[0]
    if is_name_from_reading(bring_dbms):
        get_dbms_name = True  # Get the dbms name from the reading data (the reading data is a list of entries and can have a different dbms for every entry)
    else:
        get_dbms_name = False
        ret_val, dbms_name = get_dest_name(status, topic, bring_dbms, msg_list, "dbms")  # Get the database name

    if not ret_val:
        bring_table = topic_info[1]
        if is_name_from_reading(bring_table):
            get_table_name = True  # Get the table name from the reading data (the reading data is a list of entries and can have a different table name for every entry)
        else:
            get_table_name = False
            ret_val, table_name = get_dest_name(status, topic, bring_table, msg_list, "table")  # Get the table name

    if not ret_val:
        try:
            ret_val, insert_list = get_msg_data(status, topic, topic_info, msg_list)
        except:
            utils_print.output_box(
                "Failed to process message at mqtt_client.get_msg_data(): '%s' with topic: '%s'" % (utils_json.to_string(json_msg), topic))
            ret_val = process_status.MQTT_data_err


    if not ret_val:
        for index, msg_data in enumerate(insert_list):

            if get_dbms_name:
                key_to_readings, bring_keys = update_bring(bring_dbms)
                if len(key_to_readings) and len(bring_keys) and key_to_readings in msg_list[0]:
                    reading_info = msg_list[0][key_to_readings][index]
                    ret_val, dbms_name = get_dest_name(status, topic, bring_keys, [reading_info],
                                                       "dbms")  # Get the database name
                else:
                    process_log.add("Error", "Failed to retrieve dbms name from JSON data")
                    ret_val = process_status.MQTT_info_err
                    break
            if get_table_name:
                key_to_readings, bring_keys = update_bring(bring_table)
                if len(key_to_readings) and len(bring_keys) and key_to_readings in msg_list[0]:
                    reading_info = msg_list[0][key_to_readings][index]
                    ret_val, table_name = get_dest_name(status, topic, bring_keys, [reading_info],
                                                        "table")  # Get the database name
                else:
                    process_log.add("Error", "Failed to retrieve table name from JSON data")
                    ret_val = process_status.MQTT_info_err
                    break

            try:
                ret_val, hash_value = streaming_data.add_data(status, "streaming", 1, prep_dir, watch_dir, err_dir,
                                                              dbms_name, table_name, '0', '0', 'json', msg_data)
            except:
                utils_print.output_box(
                    "Failed to process message at mqtt_client call to streaming_data.add_data(): '%s' with topic: '%s'" % (
                        utils_json.to_string(json_msg), topic))
                ret_val = process_status.MQTT_data_err
            if ret_val:
                break

    return ret_val
# ----------------------------------------------------------------------
# Process a message using the bring command
# ----------------------------------------------------------------------
def process_using_policies(status, topic_info, topic, json_msg, prep_dir, watch_dir, bwatch_dir, err_dir, blobs_dir):

    ret_val = process_status.SUCCESS

    policies = topic_info[4]        # The mapping policies

    for policy in policies:
        if "mapping" in policy:
            # Map the policy to a single table
            ret_val = process_mapping(status, policy, json_msg, prep_dir, watch_dir, bwatch_dir, err_dir, blobs_dir)
        elif "transform" in policy:
            ret_val = process_transform(status, policy, json_msg, prep_dir, watch_dir, bwatch_dir, err_dir, blobs_dir)
        else:
            status.add_error(f"Wrong mapping/transform policy used with topic: '{topic}'")
            ret_val = process_status.Wrong_policy_structure
            break

    return ret_val
#----------------------------------------------------------------------
# Process a transform a policy (used to map a PLC with many readings that are broken to tables
#----------------------------------------------------------------------
def process_transform(status, policy, json_msg, prep_dir, watch_dir, bwatch_dir, err_dir, blobs_dir):

    ret_val = process_status.SUCCESS
    policy_inner = policy["transform"]
    policy_id = policy_inner["id"] if "id" in policy_inner else "Not provided"

    policy_inner["__new_rows_from_plc__"] = {}  # This is a dictionary to all the rows created from the PLC reading

    re_compiled_pattern = None
    if "re_match" in policy_inner:
        raw_pattern = fr"{policy_inner['re_match']}"    # Raw string ignore backslash chars

        try:
            re_compiled_pattern = re.compile(raw_pattern) # the r treats backslashes (\) as literal characters
        except:
            pass

    current_time = utils_columns.get_current_time_in_sec()

    for attr_name, attr_val in json_msg.items():
        # Go over the PLC data
        # For each reading:
        # Get the DBMS from the PLC attr name
        if re_compiled_pattern:
            re_match = re.match(re_compiled_pattern, attr_name)

        dbms_name = mapping_policy.get_re_match_value(status, re_match, policy_inner["dbms"])
        if not dbms_name:
            #status.add_error(f"Failed to derive DBMS Name using {policy_inner['dbms']} from Transform policy: {policy_id}")
            #ret_val = process_status.Wrong_policy_structure
            continue    # Ignore this attribute
        dbms_name = utils_data.reset_str_chars(dbms_name.strip())

        table_name = mapping_policy.get_re_match_value(status, re_match, policy_inner["table"])
        if not table_name:
            # status.add_error(f"Failed to derive Table Name using {policy_inner['table']} from Transform policy: {policy_id}")
            # ret_val = process_status.Wrong_policy_structure
            continue    # Ignore this attribute
        table_name = utils_data.reset_str_chars(table_name.strip())

        # Add the static rows to the table if not added in a previous attr_val
        ret_val, row_to_update = mapping_policy.get_new_row(status, current_time, dbms_name, table_name, policy_inner, policy_id, json_msg, re_match)
        if ret_val:
            break

        # Add new column
        column_name = mapping_policy.get_re_match_value(status, re_match, policy_inner["column"])
        if not column_name:
            #status.add_error(f"Failed to derive Column Name using {policy_inner['column']} from Transform policy: {policy_id}")
            #ret_val = process_status.Wrong_policy_structure
            continue    # Ignore this attribute

        column_name = utils_data.reset_str_chars(column_name.strip())

        row_to_update[column_name] = attr_val        # Update the PLC value in the needed table

    data_source = "0"       # The ID of the PLC generating the data

    rows_dict = policy_inner["__new_rows_from_plc__"]
    for dbms_table, mapped_row_dict in rows_dict.items():
        dbms_table_list = dbms_table.split('.')
        dbms_name = dbms_table_list[0]
        table_name = dbms_table_list[1]
        mapped_row = utils_json.to_string(mapped_row_dict)
        if not mapped_row:
            status.add_error("Failed to generate rows in policy Transform process with policy: %s" % policy_id)
            ret_val = process_status.ERR_process_failure
            break

        ret_val, hash_value = streaming_data.add_data(status, "streaming", 1, prep_dir, watch_dir, err_dir,
                                                      dbms_name, table_name, data_source, policy_id, 'json', mapped_row)
        if ret_val:
            break


    return ret_val

#----------------------------------------------------------------------
# Process a mapping policy
#----------------------------------------------------------------------
def process_mapping(status, policy, json_msg, prep_dir, watch_dir, bwatch_dir, err_dir, blobs_dir):

    ret_val = process_status.SUCCESS
    policy_inner = policy["mapping"]
    policy_id = policy_inner["id"]  # no need to validate as this is the select criteria
    is_apply = True  # If with script - validate that skip event is not returned
    if "script" in policy_inner:
        # Get the condition if the policy applies
        reply_code, info_if = mapping_policy.process_if_code(status, policy_inner, policy_id, "condition", json_msg)
        if reply_code == process_status.IGNORE_EVENT:
            # Script instruction - Skip this entry
            is_apply = False

    if is_apply:  # 1 value means if returned true
        # condition satisfied
        ret_val = process_policy(status, policy_inner, policy_id, json_msg, prep_dir, watch_dir, bwatch_dir, err_dir, blobs_dir)

    return ret_val

#----------------------------------------------------------------------
# Process a policy against the JSON data
# Get the dbms name and table name from the policy, then apply the policy logic on the data
#----------------------------------------------------------------------
def process_policy(status, policy_inner, policy_id, json_msg, prep_dir, watch_dir, bwatch_dir, err_dir, blobs_dir):

    # Get the DBMS Name
    ret_val, policy_info = mapping_policy.get_policy_info(status, policy_inner, policy_id, "dbms", True)

    if not ret_val:
        # Apply the policy logic
        ret_val, dbms_name, table_name, data_source, insert_list = mapping_policy.apply_policy_schema(status, "", "", policy_inner, policy_id, json_msg, True, blobs_dir)

        if not ret_val:
            # policy_inner["bwatch_dir"] = True is updated inside: mapping_policy.apply_policy_schema()
            if "bwatch_dir" in policy_inner:
                write_dir = bwatch_dir      # The data includes reference to images (blobs)
                mode = "file"               # Flag - write to file
            else:
                write_dir = watch_dir
                mode = "streaming"


            for mapped_row in insert_list:
                ret_val, hash_value = streaming_data.add_data(status, mode, 1, prep_dir, write_dir, err_dir,
                                              dbms_name, table_name, data_source, policy_id, 'json',mapped_row)

    return ret_val
# ----------------------------------------------------------------------
# Test if the name of the dbms or the table is taken from the readings entries.
# For example, a message with multiple readings and the tabkle name is on the reading structure:
'''
<message = {"id":"b360a56e-136c-4eb9-b78e-0df1f832ebe2","device":"Modbus TCP test device","created":1623025405293,"origin":1623025405289878204,"readings":[
    {"id":"73874b98-0066-4cfc-9edd-b94056f68e61","origin":1623025405285688179,"device":"Modbus TCP test device","name":"OperationMode","value":"Cool","valueType":"String"},
    {"id":"ddb08704-dcc7-4726-97cc-692ee39d07df","origin":1623025405286995857,"device":"Modbus TCP test device","name":"FanSpeed","value":"Low","valueType":"String"},
    {"id":"264eb1e4-483c-47f8-aef8-b37296562a96","origin":1623025405288438346,"device":"Modbus TCP test device","name":"Temperature","value":"0.000000e+00","valueType":"Float64","floatEncoding":"eNotation"}
]}>
'''
# ----------------------------------------------------------------------
def is_name_from_reading(object_bring):
    # Get the dbms name from the user data
    for entry in object_bring:
        if "[]" in entry:
            return True
    return False
# ----------------------------------------------------------------------
# Update the bring keys to address the individual rows:
# [readings][][value] --> [value]
# ----------------------------------------------------------------------
def update_bring(object_bring):
    # Get the dbms name from the user data
    new_object = []
    key_to_readings = None
    for entry in object_bring:
        index = entry.find("[]")
        if index != -1:
            new_object.append(entry[index + 2:])    # [readings][][value] --> [value]
            if index >= 3:
                key_to_readings = entry[1:index-1]
        else:
            new_object.append(entry)
    return [key_to_readings, new_object]


# ----------------------------------------------------------------------
# Construct the data to be transferred to AnyLog
# ----------------------------------------------------------------------
def get_msg_data(status, topic, topic_info, message):

    insert_list = []          # Make a list of rows from the topic info

    ret_val = process_status.SUCCESS

    attr_constant = ""

    columns_list = topic_info[3]        # The list of columns, every entry includes column name + column extract instructions

    for column_info in columns_list:
        attr_name = column_info[0]
        data_type_info = column_info[1]
        retrieve_info = column_info[2]
        is_optional = column_info[3]        # If set to True and the data is missing, ignores the value

        if data_type_info[:6] == "bring ":
            # The data type is in the JSON data - apply bring command to retrieve the data type
            ret_val, data_type_str = utils_json.pull_info(status, message, [data_type_info[6:].lstrip()], None, 0)
            data_type_list = utils_json.str_to_list(data_type_str)
            if not data_type_list:
                status.add_error("Failed to map message data types to AnyLog format- from topic: %s and message: %s" % (topic, message))
                ret_val = process_status.MQTT_data_err
                break

        else:
            data_type_list = None
            data_type = data_type_info

        if not ret_val:
            if isinstance(retrieve_info, list):
                # retrieve_info is BRING COMMAND
                ret_val, attr_val = utils_json.pull_info(status, message, retrieve_info, None, 0)
                if ret_val:
                    break

                if not attr_val:
                    if is_optional:
                        continue        # ignore this value
                    status.add_error(f"Failed to pull info from message - topic '{topic} pulls '{str(retrieve_info)}' which is missing in message - Add: 'opttional = true'")
                    ret_val = process_status.MQTT_data_err
                    break

                if isinstance(attr_val, str) and len(attr_val) > 2 and attr_val[0] == '[' and attr_val[-1] == "]":
                    # A list of values
                    values_list = utils_json.str_to_list(attr_val)
                    if not values_list:
                        if is_optional:
                            continue  # ignore this value
                        status.add_error("Failed to map message list to AnyLog format- from topic: %s and message: %s" % (topic, message))
                        ret_val = process_status.MQTT_data_err
                        break
                    if data_type_list and len(values_list) != len(data_type_list):
                        status.add_error("Missing data type in JSON data - from topic: %s and message: %s" % (topic, message))
                        ret_val = process_status.MQTT_data_err
                        break

                    for index, entry in enumerate(values_list):
                        if data_type_list:
                            ret_val, data_type = utils_data.unify_data_type(status, data_type_list[index])
                            if ret_val:
                                break


                        ret_val = mapping_policy.add_column_to_list(status, insert_list, index, attr_name, data_type, entry, attr_constant, True)
                        if ret_val:
                            break
                else:
                    ret_val, unified_data_type = utils_data.unify_data_type(status, data_type)
                    if ret_val:
                        break

                    ret_val = mapping_policy.add_column_to_list(status, insert_list, 0, attr_name, unified_data_type, attr_val, attr_constant, True)
                    if ret_val:
                        break

            elif retrieve_info == "now()":
                # place current time:
                attr_constant = "{\"" + attr_name + "\":\"" + utils_columns.get_current_utc_time() + "\","
            else:
                ret_val, unified_data_type = utils_data.unify_data_type(status, data_type)
                if ret_val:
                    break

                ret_val = mapping_policy.add_column_to_list(status, insert_list, 0, attr_name, unified_data_type, retrieve_info, None, True)
                if ret_val:
                    break

    return [ret_val, insert_list]
# ----------------------------------------------------------------------
# Determine the destination database and table name based on the topic received
# ----------------------------------------------------------------------
def get_dest_name(status, topic, object_bring, message, object_type):


    ret_val = process_status.SUCCESS
    object_name = ""

    if isinstance(object_bring, list):

        ret_val, object_name = utils_json.pull_info(status, message, object_bring, None, 0)
        if not object_name:
            status.add_error("MQTT: Failed to apply 'bring' command to retrieve %s name" % object_type)
            if object_type == "dbms":
                ret_val = process_status.MQTT_err_dbms_name
            else:
                ret_val = process_status.MQTT_err_table_name

    elif isinstance(object_bring, str):
        object_name = object_bring
    else:
        status.add_error("MQTT failure: failed to recognize %s name" % object_type)
        ret_val = process_status.MQTT_info_err


    if not ret_val:
        # To lower case and replace space with underscore
        object_name = object_name.strip().lower()
        object_name = utils_data.reset_str_chars(object_name)


    return [ret_val, object_name]
# ----------------------------------------------------------------------
# The callback for when the client receives a CONNACK response from the server.
# http://www.steves-internet-guide.com/subscribing-topics-mqtt-client/
# ----------------------------------------------------------------------
def on_connect(client, userdata, flags, rc):

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # The subscribe function returns a tuple to indicate success, and also a message id which is used as a tracking code.
    # The subscribe method accepts 2 parameters – A topic or topics and a QOS (quality of Service)
    # subscribe(topic, qos=0)
    # client1.subscribe([(“house/bulb3”,2),(“house/bulb4”,1),(“house/bulb5”,0)])

    global subscriptions
    global non_subscribed_users_

    result = rc  # Get the returned code
    if rc:
        output_msg = True

    if userdata in subscriptions:
        msg_name = "Connect from subscription to a topic"
        subscriptions[userdata].set_connect_rc(rc)

        if not rc:

            subscription = set_subscription(userdata)     # Map the anylog structure to the MQTT structure

            # client.subscribe can be called as a List of string and integer tuples
            #         ---------------------------------
            #         e.g. subscribe([("my/topic", 0), ("another/topic", 2)])
            #
            #         This allows multiple topic subscriptions in a single SUBSCRIPTION
            #         command, which is more efficient than using multiple calls to
            #         subscribe().

            try:
                result, message_id = client.subscribe(subscription)
            except:
                result = -1     # Will trigger an error message

            subscriptions[userdata].reset_connect_counter()    # Because connection succeeded, reset the attempts counter

    elif userdata in non_subscribed_users_:
        if result:
            msg_name = "Connect from publishing data"
        non_subscribed_users_[userdata] = al_state_connected_     # Connected
    else:
        msg_name = "Connect from unrecognized source"
        output_msg = True
        if not result:
            result = -1  # Will trigger an error message

    if result:
        # some error
        place_error_msg(result, msg_name, output_msg)
        counter = subscriptions[userdata].count_connect_attempts()    # Count the connection attempts
        subscriptions[userdata].flag_disconnected()
        if counter >= 3:
            try:
                client.disconnect()
            except:
                pass
            utils_print.output_box(f"Disconnected from MQTT Client #{userdata} after {counter} failed connection attempts")

# ----------------------------------------------------------------------
# The MQTT broker/server will acknowledge subscriptions which will then generate an on_subscribe callback.
# The "mid" value can be compared with the one returned by the function call to check for successful subscription requests.
# ----------------------------------------------------------------------
def on_subscribe(client, userdata, mid, granted_qos):
    pass
# ----------------------------------------------------------------------
# The MQTT broker/server will acknowledge subscriptions which will then generate an on_subscribe callback.
# The "mid" value can be compared with the one returned by the function call to check for successful subscription requests.
# ----------------------------------------------------------------------
def on_publish(client, userdata, mid):
    global non_subscribed_users_

    if userdata in non_subscribed_users_:
        non_subscribed_users_[userdata] = al_state_published_

# ------------------------------------------------------------------------------------------------------------
# Set a time based password using PK
# ------------------------------------------------------------------------------------------------------------
def assign_password(client, password, user_name, project_id, private_key):

    if password:
        client.username_pw_set(user_name, password)
        ret_val = True
    elif not private_key:
        # No need in password
        ret_val = True
    elif not project_id:
        # Must have project ID to get the password
        process_log.add("Error", "MQTT clienet did not connect: Missing project ID")
        ret_val = False
    else:
        password = generate_password(project_id, private_key)
        if password:
            client.username_pw_set(user_name, password)
            ret_val = True
        else:
            process_log.add("Error", "MQTT did not connect: Failed to generate password from private key")
            ret_val = False
    return ret_val
# ------------------------------------------------------------------------------------------------------------
# Generate the password
# ------------------------------------------------------------------------------------------------------------
def generate_password(project_id, private_key):

    now = datetime.utcnow()
    time_exp = now + timedelta(minutes=60)
    token = {
        'iat': now,
        'exp': time_exp,
        'aud': project_id
    }

    if jwt_installed:
        try:
            password = jwt.encode(token, private_key, "RS256")
        except:
            password = None
    else:
        password = None
        process_log.add("Error", "MQTT clienet did not connect: 'pyjwt' package not installed")

    return password

# ------------------------------------------------------------------------------------------------------------
# Declare a MQTT client that subscribes to a broker
# Example: run mqtt client where broker = "mqtt.eclipse.org" and topic = (name = $SYS/# and dbms = lsl_demo and table =ping_sensot and qos = 2)
# https://pypi.org/project/paho-mqtt/#callbacks
# Run the Broker client.
# To exit call: exit mqtt
# ------------------------------------------------------------------------------------------------------------
def subscribe(dummy: str, conditions: dict, client_id:int):
    global clients_counter
    global subscriptions

    client = connect_to_broker("subscribe", conditions, client_id, None)
    if not client:
        utils_print.output("Failed to connect to MQTT broker", True)
    else:

        clients_counter += 1        # Count active processes

        client_sbscr = subscriptions[client_id]

        #     client.loop_forever()
        client.loop_start()

        while True:
            time.sleep(10)
            if client_sbscr.is_exit():
                break

    subscript_mutex[client_id].acquire_write()     # Wait for processing to completed (message server might be sending data)

    if client:
        # Will skip if client failed to initiate
        client.loop_stop()                              # Stop the dedicated thread

        client.disconnect()

    # Need to be after disconnect - Update the list of topics for each broker
    end_subscription(client_id, False)

    subscript_mutex[client_id].release_write()

    process_log.add_and_print("event", "MQTT Client process terminated")


# ------------------------------------------------------------------------------------------------------------
# End subscription
# ------------------------------------------------------------------------------------------------------------
def end_subscription(client_id, is_mutex):
    '''
    client_id - the client removed from the suscribers list
    is_mutex - needs to be set to true unless mutex is done by the caller
    '''
    global clients_counter
    global subscriptions

    if is_mutex:
        subscript_mutex[client_id].acquire_write()

    client_sbscr = subscriptions[client_id]

    # Need to be after disconnect - Update the list of topics for each broker
    ip_port = client_sbscr.get_ip_port()
    topics_dict = client_sbscr.get_topics_dict()
    remove_topics(ip_port, topics_dict)

    del subscriptions[client_id]  # Update a global mapping dictionary

    clients_counter -= 1

    if is_mutex:
        subscript_mutex[client_id].release_write()

# ------------------------------------------------------------------------------------------------------------
# Remove the topics of this client from the broker
# ------------------------------------------------------------------------------------------------------------
def remove_topics(ip_port, topics_dict):

    if ip_port in broker_to_topic:
        broker_topics = broker_to_topic[ip_port]  # List of topics for this broker

        for topic in topics_dict:       # Go over the list of topics of this customer
            if topic in broker_topics:  # The topic is with the broker
                del broker_topics[topic]

# ------------------------------------------------------------------------------------------------------------
# Publish a message
# ------------------------------------------------------------------------------------------------------------
def publish(status, conditions):
    global broker_to_topic

    thread_name = threading.current_thread().name

    local_broker = interpreter.get_one_value(conditions, "broker") == "local"       # Set to True if a local broker

    client = connect_to_broker("publish", conditions, None, thread_name)

    if not client:
        ret_val = process_status.MQTT_connect_failure
    else:
        message = None
        topic = interpreter.get_one_value(conditions, "topic")
        message_info = interpreter.get_one_value(conditions, "message")
        qos_level = interpreter.get_one_value_or_default(conditions, "qos", 0)
        if message_info and topic:

            if message_info[0] == '[' and message_info[-1] == ']':
                # send each list entry
                list_object = utils_json.str_to_list(message_info)
            else:
                json_obj = utils_json.str_to_json(message_info)
                if json_obj:
                    list_object = [json_obj]    # Make a list
                else:
                    list_object = None
            if not list_object:
                status.add_error("Published data format is not JSON")
                ret_val = process_status.ERR_wrong_json_structure
            else:
                ret_val = process_status.SUCCESS
                for entry in list_object:
                    if ret_val:
                        break       # Previous message publishing failed

                    message = utils_json.to_string(entry)

                    if local_broker:
                        # Deliver to the local process without the network
                        try:
                            user_id = broker_to_topic["local"][topic][0]    # The user registered as
                        except:
                            status.add_error("Non-subscribed topic: %s" % topic)
                            ret_val = process_status.MQTT_info_err
                        else:
                            ret_val = process_message(topic, user_id, message)
                    else:
                        # deliver to the broker using the network

                        if qos_level:
                            non_subscribed_users_[thread_name] = al_state_not_published_

                        client.loop_start()

                        try:
                            delivery_info = client.publish(topic=topic,payload=message, qos=qos_level)
                        except ValueError as error:
                            status.add_error("MQTT publish failed with error: %s" % str(error))
                            ret_val = process_status.MQTT_publish_failed
                        except:
                            errno, value = sys.exc_info()[:2]
                            status.add_error("MQTT publish failed with Error: (%s) %s" % (str(errno), str(value)))
                            ret_val = process_status.MQTT_publish_failed
                        else:
                            if delivery_info.rc == MQTT_ERR_SUCCESS:
                                ret_val = process_status.SUCCESS
                            else:
                                status.add_error("MQTT publish failed")
                                ret_val = process_status.MQTT_publish_failed

                        if qos_level:
                            counter = 0
                            while (counter < 6):
                                # connect with in a publish process
                                if non_subscribed_users_[thread_name] == al_state_published_:
                                    break
                                time.sleep(1)
                                counter += 1

                            if counter >= 6:
                                status.add_error("MQTT publish failed - no reply from broker with QoS 1")
                                ret_val = process_status.MQTT_publish_failed

                        client.loop_stop()

        else:
            if not topic:
                status.add_error("MQTT publish failed: no topic is assigned to '%s'" % conditions["topic"][0])
            if not message:
                status.add_error("MQTT publish failed: no message is assigned to '%s'" % conditions["message"][0])
            ret_val = process_status.MQTT_publish_failed

    return ret_val
# ------------------------------------------------------------------------------------------------------------
# Connect to an MQTT broker
# user_id is an identification of the subscription info
# Examples connections: http://www.steves-internet-guide.com/client-connections-python-mqtt/
# ------------------------------------------------------------------------------------------------------------
def connect_to_broker(operation_type, conditions:dict, user_id, thread_name):

    global non_subscribed_users_
    global al_state_not_connected_


    # Get user params
    mqtt_broker = interpreter.get_one_value(conditions, "broker")  # is sync to dbms
    port = interpreter.get_one_value_or_default(conditions, "port", 1883)  # 1883 is the default Port


    user_name = interpreter.get_one_value_or_default(conditions, "user", "unused")
    password = interpreter.get_one_value(conditions, "password")
    if password:
        del conditions["password"]   # Remove the password from the structure as conditions is passed to the callbacks

    use_log = interpreter.get_one_value_or_default(conditions, "log", False)

    client_id = interpreter.get_one_value_or_default(conditions, "client_id", None)
    project_id = interpreter.get_one_value_or_default(conditions, "project_id", None)

    private_key = interpreter.get_one_value_or_default(conditions, "private_key", None)
    if private_key:
        del conditions["private_key"]   # Remove the PK from the structure as conditions is passed to the callbacks


    if user_id:
        user_data = user_id
        subscriptions[user_data].set_connect_rc( MQTT_ERR_NO_CONN )     # Updated with the return value from on_connect()
    else:
        user_data = thread_name
        non_subscribed_users_[thread_name] = al_state_not_connected_      # this is a structure that will be updated on the on_connect

    if not client_id:
        client_id = get_node_name() + "." + str(user_data)              # Make unique name at the broker side

    if mqtt_broker == "local":
        # The local machine is the broker - no need in network transfer
        client = client_id
    else:
        # Use the network

        client = mqtt.Client(client_id=client_id, clean_session=True, userdata=user_data)  # assign conditions to the user data

        # Define the cllback methods
        if operation_type == "subscribe":
            client.on_connect = on_connect
            client.on_message = on_message
            client.on_subscribe = on_subscribe
        elif operation_type == "publish":
            client.on_publish = on_publish
            client.on_connect = on_connect

        if use_log:
            # If user specified to log process
            client.on_log = on_log

        ret_val = assign_password(client, password, user_name, project_id, private_key)
        if ret_val:


            try:
                reply = client.connect(host=mqtt_broker, port=port, keepalive=60)
            except:
                process_log.add("Error", "Failed to connect to MQTT broker at: %s:%u" % (mqtt_broker, port))
                client = None
            else:
                counter = 0     # Test returned code a few times
                if user_id:
                    # connect with Subscribe to a topic

                    # # The loop_start() starts a new thread, that calls the loop method at regular intervals for you. It also handles re-connects automatically.
                    # To stop the loop use the loop_stop() method.
                    try:
                        client.loop_start()     # <- starts a dedicated thread
                    except:
                        client = None  # A new thread failed
                    else:
                        while (counter < 6):
                            ret_code = subscriptions[user_data].get_connect_rc()
                            if ret_code != MQTT_ERR_NO_CONN:
                                break           # Connected
                            time.sleep(1)
                            counter += 1

                        if counter >= 6:
                            client.loop_stop()      # An error stop the thread
                            client = None           # on_connect was not called
                else:
                    # connect with in a publish process
                    # No need to start a new thread - the main thread will publish
                    # # The loop_start() starts a new thread, that calls the loop method at regular intervals for you. It also handles re-connects automatically.
                    # To stop the loop use the loop_stop() method.
                    try:
                        client.loop_start()     # <- starts a dedicated thread
                    except:
                        client = None  # A new thread failed
                    else:

                        while (counter < 6):

                            if non_subscribed_users_[thread_name] == al_state_connected_:
                                break       # Successfully published
                            time.sleep(1)
                            counter += 1

                        client.loop_stop()  # Stop the thread (if the data was published or if an error)
                        if counter >= 6:
                            client = None  # connect failed
        else:
            client = None

    return client

# ------------------------------------------------------------------------------------------------------------
# Pre process the conditions set by the user to create a dictionary that manage the subscription and the mapping
# Between the user data and the database tables.
# Returns a key that identifies the mapping dictionary. This key is passed to every on_message call.

#  if message_flush is True - Topic info is not provided in parentheses - data will be flushed to disk.
# ------------------------------------------------------------------------------------------------------------
def register(status, conditions):

    global subscriptions
    global subscript_mutex

    ret_val = process_status.SUCCESS

    client_sbscr = SubscriptionInfo()  # A class that maintains the subscription info

    # Maintain a table to show topics as f(broker) and avoid registrations to the same topic on the same broker multiple times)
    mqtt_broker = interpreter.get_one_value(conditions, "broker")

    if mqtt_broker == "rest":
        ip_port = "rest"  # Connected to the rest calls with the specified user-agent
        user_agent = interpreter.get_one_value(conditions, "user-agent")
        client_sbscr.set_user_agent(user_agent)
        client_sbscr.set_local()
    elif mqtt_broker == "local":
        ip_port = "local"           # Connected to the local Message Server
        client_sbscr.set_local()
    elif mqtt_broker == "kafka":
        ip_port = "%s:%s" % (interpreter.get_one_value(conditions, "ip"),  interpreter.get_one_value(conditions, "port"))
        client_sbscr.set_kafka()
    else:
        port = interpreter.get_one_value_or_default(conditions, "port", 1883)  # 1883 is the default Port
        ip_port = mqtt_broker + ':' + str(port)
        if ip_port == net_utils.get_ip_port(2,2):
            # Same IP and Port as the message server - transform to "local"
            ip_port = "local"  # Connected to the local Message Server
            client_sbscr.set_local()

    if not ip_port in broker_to_topic:
        broker_to_topic[ip_port] = {}       # AN array to include the list of topics on this IP and port

    client_sbscr.set_ip_port(ip_port)

    user_name = interpreter.get_one_value_or_default(conditions, "user", "unused")
    client_sbscr.set_user_name(user_name)


    # Get the data locations on this node
    client_sbscr.set_work_dirs( prep_dir = interpreter.get_one_value(conditions, "prep_dir"),\
                                watch_dir = interpreter.get_one_value(conditions, "watch_dir"), \
                                bwatch_dir=interpreter.get_one_value(conditions, "bwatch_dir"), \
                                err_dir = interpreter.get_one_value(conditions, "err_dir"), \
                                blobs_dir = interpreter.get_one_value(conditions, "blobs_dir"))

    log_error = interpreter.get_one_value_or_default(conditions, "log_error", False)
    client_sbscr.set_log_error(log_error)   # A true value will log the data if it is not processed correctly

    client_topics = conditions["topic"]  # ALl the subscribed topics of this client

    topics_dict = {}        # Collect the topics that were configured such that it can be undone in a failure

    for topic in client_topics:

        with_policies = "policy" in topic
        message_flush = "flush" in topic        # Only flush the topic data to disk

        if with_policies:
            # register the policies on each topic
            # Only flush the topic data to disk
            qos = interpreter.get_one_value_or_default(conditions, "qos", 0)

            name = interpreter.get_test_one_value(topic, "name", str)
            if not name:
                status.add_error("Wrong msg client declaration for topic: %s" % str(topic))
                ret_val = process_status.MQTT_info_err
                break

            if not "policy" in topic or not isinstance(topic["policy"], list):
                status.add_error("Missing policies for topic: %s" % str(topic))
                ret_val = process_status.MQTT_info_err
                break
            policies = topic["policy"]

            command = get_cmd_policies( policies, True )

            ret_val, policies_info = member_cmd.blockchain_get(status, command.split(), None, True)
            if ret_val:
                status.add_error("Failed to read mapping policies using the command: %s" % command)
                break
            if not policies_info:
                status.add_error("Mapping policies using the command '%s' are not available" % command)
                ret_val = process_status.MQTT_info_err
                break
            if len(policies_info) != len(policies):
                status.add_error("The command '%s' returned %s policies from %s policies requested" % (command, len(policies_info), len(policies)))
                ret_val = process_status.MQTT_info_err
                break

            ret_val = resister_by_broker(status, ip_port, name, client_sbscr)  # Register the topic as f(broker)
            if ret_val:
                break
            topics_dict[name] = 1  # Keep the (registered) topic name to allow undo

            policies_copy = copy.deepcopy(policies_info) # Make a copy of the policy as it may be changed using compile_ in Mapping_policy.apply_policy_schema()
            client_sbscr.add_topic(name, None, None, qos, None, policies_copy, False)  # Add topics to list

        elif message_flush:
            # Only flush the topic data to disk
            qos = interpreter.get_one_value_or_default(conditions, "qos", 0)

            name = interpreter.get_test_one_value(topic, "name", str)
            if not name:
                status.add_error("Wrong msg client declaration for topic: %s" % str(topic))
                ret_val = process_status.MQTT_info_err
                break

            ret_val = resister_by_broker(status, ip_port, name, client_sbscr) # Register the topic as f(broker)
            if ret_val:
                break
            topics_dict[name] = 1  # Keep the (registered) topic name to allow undo

            client_sbscr.add_topic(name, None, None, qos, None, None, False)     # Add topics to list
        else:
            qos = 0     # Default
            name = None
            dbms = None
            table = None


            mapping = []
            persist = False
            for key, value in topic.items():
                optional = False            # The default value - data must have the column, unless optional is set to True
                if key == "name":
                    name = value[0]
                elif key == "dbms":
                    # get the dbms name from the message data
                    ret_val, dbms_info = utils_data.test_remove_quotations(status, value[0], False, "Subscribe to a broker (dbms name)")
                    if ret_val:
                        break
                    if dbms_info.startswith("bring "):
                        # Get the mapping instructions from the message to the dbms name
                        ret_val, dbms = get_cmd_bring(status, dbms_info)
                        if ret_val:
                            break
                    else:
                        dbms = dbms_info        # The dbms name without mapping
                elif key == "table":
                    # get the table name from the message data
                    ret_val, table_info = utils_data.test_remove_quotations(status, value[0], False, "Subscribe to a broker (table name)")
                    if ret_val:
                        break

                    if table_info.startswith("bring "):
                        ret_val, table = get_cmd_bring(status, table_info)
                        if ret_val:
                            break
                    else:
                        table = table_info
                elif key == "qos":
                    qos = value[0]              # Quality of service
                elif key.startswith("column."):
                    # First option:  column.[column name].[column type]
                    ret_val, column_name, column_type = get_column_name_type(status, key)
                    if not ret_val and column_type:
                        column_value = value[0]
                    else:
                        if column_name:         # Provided in step one from the call to get_column_name_type
                            # Second option:  column.[column name] = (type = [data type or bring command] and value = [bring command] and optional = true/false)
                            ret_val, column_type, column_value, optional = get_column_type_value(status, value[0])
                            if not ret_val:
                                ret_val, column_type = utils_data.test_remove_quotations(status, column_type, False, "Subscribe to a broker (column type)")
                                if not ret_val:
                                    ret_val, column_value = utils_data.test_remove_quotations(status, column_value, False, "Subscribe to a broker (column value)")
                        if ret_val:
                            break
                    ret_val, column_instruct = utils_data.test_remove_quotations(status, column_value, False, "Subscribe to a broker (column instruction)")
                    if ret_val:
                        break
                    if column_instruct.startswith("bring ") or column_instruct.startswith("bring.json "):   # can ask to return multiple values
                        insert_first = False
                        ret_val, column_val = get_cmd_bring(status, column_instruct)
                        if ret_val:
                            break
                    elif column_instruct == "now":
                        column_val = "now()"        # Place current time and date
                        insert_first = True         # This datestamp is a constant to multiple values and needs to be first in the list
                    else:
                        if isinstance(column_instruct, str):
                            insert_first = False
                            column_val = column_instruct
                        else:
                            status.add_error("Unrecognized function assigned to column '%s'" % column_name)
                            ret_val = process_status.MQTT_info_err
                            break
                    if insert_first:
                        mapping.insert(0, ( column_name, column_type, column_val, optional))
                    else:
                        mapping.append(( column_name, column_type, column_val, optional))
                elif key == "persist":
                    # Only Write the data to a file
                    # Example:  run mqtt client where broker = local and port = 32150 and log= false and topic = (name = edgexpert-anylog and persist = true)
                    persist = True if value[0] == "true" else False
                else:
                    status.add_error("Unrecognized topic component for MQTT mapping: '%s" % key)
                    ret_val = process_status.MQTT_info_err
                    break

            if ret_val:
                break

            if not name:
                status.add_error("Missing topic name")
                ret_val = process_status.MQTT_info_err
                break

            if not dbms:
                status.add_error("Missing mapping instruction for DBMS")
                ret_val = process_status.MQTT_info_err
                break

            if not table:
                status.add_error("Missing mapping instructions for table")
                ret_val = process_status.MQTT_info_err
                break

            if not persist:
                # If persist, data is written to file, mapping is optional
                if not len(mapping):
                    status.add_error("Missing mapping instructions for message in topic: '%s" % topic)
                    ret_val = process_status.MQTT_info_err
                    break

            ret_val = resister_by_broker(status, ip_port, name, client_sbscr) # Register the topic as f(broker)
            if ret_val:
                break
            topics_dict[name] = 1  # Keep the (registered) topic name to allow undo

            client_sbscr.add_topic(name, dbms, table, qos, mapping, None, persist)     # Update the subscription info for this client

    if not ret_val:
        status = process_status.ProcessStat()
        client_sbscr.set_status(status)
        # add the client info to the subscriptions dictionary as f(client_id)
        subscriptions[node_client_id] = client_sbscr
        while len(subscript_mutex) < (node_client_id + 1):
            # Add a mutex to an array that protects each subscription
            subscript_mutex.append(utils_threads.ReadWriteLock())

        client_id = client_sbscr.get_client_id()
    else:
        if len(topics_dict):
            remove_topics(ip_port, topics_dict) # Undo the assignments before the failure
        client_id = 0

    return [ret_val, client_id]

# -----------------------------------------------------------------------
# Register the topic in the a dictionary that shows, for each nroker, the list of registered topics
# -----------------------------------------------------------------------
def resister_by_broker(status, ip_port, name, client_sbscr):
    global broker_to_topic

    topics_dict = broker_to_topic[ip_port]  # The list of topics on this broker
    if name in topics_dict:
        status.add_error("MQTT Error: The Topic '%s' was subscribed by client #%u on this broker" % (name, topics_dict[name][0]))
        ret_val = process_status.MQTT_info_err
    else:
        client_id = client_sbscr.new_client_id()            # Set a unique client ID
        topics_dict[name] = [client_id, 0, 0]  # Update the list of topics as f(broker) -> every entry includes the client ID + counter messages + counter errrors
        ret_val = process_status.SUCCESS
    return ret_val

# -----------------------------------------------------------------------
# Extract the command that is used to pull the info from the message data
# -----------------------------------------------------------------------
def get_cmd_bring(status, bring_str):

    # Get the mapping instructions from the message to the dbms name
    cmd_words, left_brackets, right_brakets = utils_data.cmd_line_to_list_with_json(status, bring_str, 0,  0)  # a list with words in command line

    if not cmd_words or not isinstance(cmd_words, list) or len(cmd_words) < 2 or cmd_words[0][:5] != "bring":
        status.add_error("Wrong 'bring' command in MQTT mapping: %s" % bring_str)
        ret_val = process_status.MQTT_info_err
    else:
        ret_val = process_status.SUCCESS

    return [ret_val, cmd_words[1:]] # return without the bring keyword

# -----------------------------------------------------------------------
# Extract the column name and the data type from the key.
# column.column_name.column_type
# -----------------------------------------------------------------------
def get_column_name_type(status, key):
    column_name = None
    column_type = None
    ret_val = process_status.SUCCESS
    if len(key) <= 7:
        status.add_error("Wrong column name in MQTT mapping: %s" % key)
        ret_val = process_status.MQTT_info_err
    else:
        index = key.find('.', 7)        # find the suffix of the data type
        if index <= 7:
            if len(key) > 7 and key[6] == '.':
                column_name = key[7:]   # Only column name, column type in the parenthesis (see option 2)
            else:
                status.add_error("Missing column type and column name in MQTT mapping: %s" % key)
                ret_val = process_status.MQTT_info_err
        else:
            column_name = key[7:index]
            if not column_name:
                status.add_error("Wrong column name in MQTT mapping: %s" % key)
                ret_val = process_status.MQTT_info_err
            else:
                column_type = key[index + 1:]
                if not column_type or (column_type != "str" and column_type != "int" and column_type != "timestamp" and column_type != "float"):
                    status.add_error("Wrong column type in MQTT mapping: %s" % key)
                    ret_val = process_status.MQTT_info_err

    return [ret_val, column_name, column_type]
# -----------------------------------------------------------------------
# Extract the column name and the data type and value from the key.
# column.column_name = (type = [data type or bring command] [value = bring command]
# Example:
# topic=(name=anylogedgex and dbms=edgex and table="bring [readings][][name]" and column.timestamp.timestamp=now and column.value = (value="bring [readings][][value]" and type="bring [readings][][valueType]") and optional = true)
# -----------------------------------------------------------------------
def get_column_type_value(status, value):
    column_type = None
    column_value = None
    optional = False
    ret_val = process_status.SUCCESS

    if not len(value) or value[0] != '(' or value[-1] != ')':
        status.add_error("Wrong format in MQTT mapping: %s" % value)
        ret_val = process_status.MQTT_info_err
    else:
        value_list = value[1:-1].split(" and ")
        if len(value_list) < 2:
            status.add_error("Missing column type and column name in MQTT mapping: %s" % value)
            ret_val = process_status.MQTT_info_err
        else:
            value_list.sort()
            index = 0
            list_entry = value_list[index]
            optional_val = utils_data.get_equal_value(list_entry, "optional")
            if optional_val:
                index += 1
                if len(optional_val) > 1 and optional_val[0] == '!':
                    optional_val = get_value_if_available(optional_val)
                if optional_val == "true":
                    optional = True
                elif optional_val == "false":
                    optional = False
                else:
                    status.add_error("Optional value in mapping is not correctly set (to true or false): %s" % value)
                    ret_val = process_status.MQTT_info_err
            if not ret_val:
                if index:
                    # if we optional keyword - get the next instruction
                    list_entry = value_list[index]
                column_type = utils_data.get_equal_value(list_entry, "type")
                if not column_type:
                    status.add_error("Missing column type in MQTT mapping from: %s" % list_entry)
                    ret_val = process_status.MQTT_info_err
                elif len(column_type) > 1 and column_type[0] == '!':
                    column_type = get_value_if_available(column_type)

                if not ret_val:
                    index += 1
                    list_entry = value_list[index]
                    column_value = utils_data.get_equal_value(list_entry, "value")
                    if not column_value:
                        status.add_error("Missing column mapping instructions in MQTT declaration from: %s" % list_entry)
                        ret_val = process_status.MQTT_info_err
                    elif len(column_value) > 1 and column_value[0] == '!':
                        column_value = get_value_if_available(column_value)

    return [ret_val, column_type, column_value, optional]
# -----------------------------------------------------------------------
# Set the debug mode - True will print incomming messages
# -----------------------------------------------------------------------
def set_debug( mode ):
    global debug_mode
    debug_mode = mode
# -----------------------------------------------------------------------
# Exit one or more clients
# -----------------------------------------------------------------------
def exit(client_id):

    global subscriptions
    ret_val = process_status.SUCCESS
    if not client_id:
        # exit all
        for client_sbscr in subscriptions.values():
            client_sbscr.set_exit()
    else:
        if client_id in subscriptions:
            subscriptions[client_id].set_exit()             # The object with the subscription info
        else:
            ret_val = process_status.MQTT_wrong_client_id
    return ret_val
# -----------------------------------------------------------------------
# Test valid subscription
# -----------------------------------------------------------------------
def is_subscription(client_id):
    global subscriptions
    return client_id in subscriptions
# -----------------------------------------------------------------------
# Test if subscription is set to the local message server
# -----------------------------------------------------------------------
def is_local_subscription(client_id):
    global subscriptions
    return subscriptions[client_id].is_local()
