"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

# Documentation https://kafka-python.readthedocs.io/en/master/
# Example: https://github.com/dpkp/kafka-python/blob/master/example.py

# Confluent - https://docs.confluent.io/clients-confluent-kafka-python/current/overview.html
# https://www.tutorialsbuddy.com/confluent-kafka-python-producer-example
# https://www.tutorialsbuddy.com/confluent-kafka-python-consumer-example

import sys
import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_threads as utils_threads
import edge_lake.generic.interpreter as interpreter
import edge_lake.generic.utils_print as utils_print
import edge_lake.tcpip.mqtt_client as mqtt_client
import edge_lake.generic.process_log as process_log

msg_counter_ = 0        # Number of messages received
counter_by_topic_ = {}
is_running = False
ip_port_ = ""

try:
    import kafka
    #import confluent_kafka
except:
    kafka_installed = False
else:
    kafka_installed = True

# ---------------------------------------------------------------------------------------------
# Return True if package installed
# ---------------------------------------------------------------------------------------------
def is_installed():
    return kafka_installed

# ---------------------------------------------------------------------------------------------
# Get Kafka producer object
# Using this object data is send to kafka
# ---------------------------------------------------------------------------------------------
def get_producer( status, ip_port ):

    if kafka_installed:


        try:
            producer = kafka.KafkaProducer(bootstrap_servers=ip_port)
        except:
            errno, value = sys.exc_info()[:2]
            message = "Failed to connect to Kafka using: %s with error: %s : %s" % (ip_port, str(errno), str(value))
            status.add_error(message)
            producer = None
    else:
        producer = None
        status.add_error("Kafka not installed")

    return producer


# ---------------------------------------------------------------------------------------------
# End producer connection
# ---------------------------------------------------------------------------------------------
def close_connection(status, kafka_object):
    '''
    status - the thread status object
    kafka_object - producer object or consumer object
    '''
    try:
        kafka_object.close()
    except:
        errno, value = sys.exc_info()[:2]
        message = "Failed to close kafka connection with error:  %s : %s" % (str(errno), str(value))
        status.add_error(message)


# ---------------------------------------------------------------------------------------------
# Send data to Kafka using the producer
# ---------------------------------------------------------------------------------------------
def send_data(status, producer, topic, data):

    try:
        producer.send(topic, value=data.encode())
    except:
        errno, value = sys.exc_info()[:2]
        message = "Failed to send data to kafka with error:  %s : %s" % (str(errno), str(value))
        status.add_error(message)
        ret_val = process_status.ERR_write_stream
    else:
        ret_val = process_status.SUCCESS

    return ret_val

# ---------------------------------------------------------------------------------------------
# Flush Data
# ---------------------------------------------------------------------------------------------
def flush_data(status, producer):

    try:
        producer.flush()
    except:
        errno, value = sys.exc_info()[:2]
        message = "Failed to flush data to kafka with error:  %s : %s" % (str(errno), str(value))
        status.add_error(message)
        ret_val = process_status.ERR_write_stream
    else:
        ret_val = process_status.SUCCESS

    return ret_val


# ---------------------------------------------------------------------------------------------
# Get Kafka consumer object
# Using this object data is send to kafka
# ---------------------------------------------------------------------------------------------
def get_consumer( ip_port , auto_offset_reset ):

    '''
    ip_port - the kafka server IP and Port string
    auto_offset_reset (str) – A policy for resetting offsets on OffsetOutOfRange errors:
    ‘earliest’ will move to the oldest available message,
    ‘latest’ will move to the most recent.
    Any other value will raise the exception. Default: ‘latest’.
    '''

    if kafka_installed:


        try:
            consumer = kafka.KafkaConsumer(bootstrap_servers=ip_port,
                                     auto_offset_reset=auto_offset_reset,
                                     consumer_timeout_ms=2000)
        except:
            errno, value = sys.exc_info()[:2]
            message = "\r\nFailed to connect to Kafka using: %s with error: %s : %s" % (ip_port, str(errno), str(value))
            utils_print.output(message, True)
            consumer = None
    else:
        consumer = None
        message = "\r\nKafka not installed"
        utils_print.output(message, True)

    return consumer


# ---------------------------------------------------------------------------------------------
# Run a Kafka consumer - continuously pull data from kafka and process the data using the kafka_workers
# Example: run kafka consumer where ip = 198.74.50.131 and port = 9092 and topic = (name = abc and dbms = lsl_demo and table = ping_sensor and column.timestamp.timestamp = "bring [timestamp]" and column.value.int = "bring [value]")
# ---------------------------------------------------------------------------------------------
def pull_data(user_id:int, conditions: dict):
    '''
    user_id - a user ID associated with the topics
    conditions - the info from the command line including the topics
    '''

    global msg_counter_
    global counter_by_topic_
    global ip_port_
    global is_running

    ip = interpreter.get_one_value(conditions, "ip")
    port = interpreter.get_one_value(conditions, "port")
    auto_offset_reset = interpreter.get_one_value_or_default(conditions, "reset", "latest") # can be set to earliest

    ret_val = process_status.SUCCESS

    consumer = get_consumer(ip + ":" + port, auto_offset_reset)

    ip_port_ = ip + ":" + port      # used to provide info

    if consumer:

        topics_list = []
        topics_info = conditions["topic"]
        for entry in topics_info:
            topics_list.append(entry["name"][0])

        workers_count = interpreter.get_one_value_or_default(conditions, "threads", 3)      # No. of worjers threads

        # Set a pool of workers threads
        workers_pool = utils_threads.WorkersPool("Kafka Pool", workers_count)

        is_running = True

        while 1:
            # Get Message
            if process_status.is_exit("kafka"):
                ret_val = process_status.EXIT
                break  # all threads stopped by user

            try:
                consumer.subscribe(topics_list)
            except:
                message = "\r\nFailed to subscribe to topics: %s" % (str(topics_list))
                utils_print.output(message, True)
                break
            else:
                for message in consumer:
                    # Give message to thread
                    task = workers_pool.get_free_task()
                    task.set_cmd(kafka_worker, [user_id, message])
                    workers_pool.add_new_task(task)

                    msg_counter_ += 1       # Global message counter
                    if message.topic in counter_by_topic_:
                        counter_by_topic_[message.topic][0] += 1
                    else:
                        counter_by_topic_[message.topic] = [1, 0, 0, ""]    # counter messages + couter failures, last error, last error txt

        workers_pool.exit()

    mqtt_client.end_subscription(user_id, True)
    process_log.add_and_print("event", "Kafka consumer process terminated: %s" % process_status.get_status_text(ret_val))
    is_running = False

# ---------------------------------------------------------------------------------------------
# Run a Kafka worker thread to process the data
# ---------------------------------------------------------------------------------------------
def kafka_worker(status, mem_view, user_id, message):

    value = message.value
    try:
        decoded_msg = value.decode('utf-8')
    except:
        errno, value = sys.exc_info()[:2]
        message = "\r\nFailed to decode message: %s : %s" % (str(errno), str(value))
        utils_print.output(message, True)
    else:
        ret_val = mqtt_client.process_message(message.topic, user_id, decoded_msg)

        if ret_val:
            # Update failures
            counter_by_topic_[message.topic][1] += 1        # count failure
            counter_by_topic_[message.topic][1] = ret_val   # Last error
            counter_by_topic_[message.topic][2] = process_status.get_status_text(ret_val)  # Last error text

# ------------------------------------------------------------------------------
# Return an info string on the kafka client state
# ------------------------------------------------------------------------------
def get_info(status):
    global is_running
    global ip_port_

    if is_running:
        info_str = "Kafka consumer configured to %s" % ip_port_
    else:
        info_str = ""
    return info_str

def is_active():
    return is_running