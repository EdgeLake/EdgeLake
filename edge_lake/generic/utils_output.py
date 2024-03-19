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

import smtplib
import ssl
import threading

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import edge_lake.generic.params as params
import edge_lake.generic.utils_io as utils_io
import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.interpreter as interpreter
from edge_lake.generic.node_info import get_node_name


smtp_client_ = None     # Client object
smtp_server_ = ""       # URL of the server
smtp_port_ = 0
sender_email_ = ""        # Sender email
sender_password_ = ""
ssl_connect_ = False
smtp_mutex_ = threading.Lock()  # mutex to allow one message at a time

# -----------------------------------------------------------------------------------------------------------------------
# Test SMTP Client
# -----------------------------------------------------------------------------------------------------------------------
def is_smtp_running():
    return not smtp_client_ == None

# -----------------------------------------------------------------------------------------------------------------------
# Return info for the get processes command
# -----------------------------------------------------------------------------------------------------------------------
def get_smtp_info(status):

    if smtp_client_:
        message = "Using '%s' with email: '%s'" % (smtp_server_, sender_email_)
    else:
        message = ""
    return message

# -----------------------------------------------------------------------------------------------------------------------
# Exit the SMTP Client
# -----------------------------------------------------------------------------------------------------------------------
def exit_smtp():
    global smtp_client_
    global smtp_mutex_

    if smtp_client_:

        smtp_mutex_.acquire()

        try:
            smtp_client_.quit()
        except:
            pass
        finally:
            smtp_client_ = None

        smtp_mutex_.release()

    return process_status.SUCCESS
# -----------------------------------------------------------------------------------------------------------------------
# Start SMTP Client
# If specified, `host' is the name of the remote host to which to
#        connect.  If specified, `port' specifies the port to which to connect.
#        By default, smtplib.SMTP_PORT is used.
# run smtp client where host = [host name] and port = [port name] and email = [email address] and password = [email password] and ssl = [true / false]
# run smtp client where email = anylog.iot@gmail.com and password = google4anylog
# -----------------------------------------------------------------------------------------------------------------------
def start_smtp_client(status, io_buff_in, cmd_words, trace):

    global smtp_client_
    global smtp_server_        # URL of the server
    global smtp_port_
    global sender_email_
    global sender_password_
    global ssl_connect_


    if len(cmd_words) < 11:
        return process_status.ERR_command_struct

    if cmd_words[3]!= "where":
        return process_status.ERR_command_struct

    if smtp_client_:
        status.add_error("SMTP client is running using %s with email %s" % (smtp_server_, sender_email_))
        return process_status.ERR_process_failure

    keywords = {"host": ("str", False, False, True),
                "port": ("str", False, False, True),
                "email": ("str", True, False, True),
                "password": ("str", True, False, True),
                "ssl": ("bool", False, False, True),            # Use SSL
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return ret_val

    host_name = interpreter.get_one_value(conditions, "host")
    host_port = interpreter.get_one_value(conditions, "port")
    sender_email_ = interpreter.get_one_value(conditions, "email")
    sender_password_ = interpreter.get_one_value(conditions, "password")
    ssl_connect_ = interpreter.get_one_value(conditions, "ssl")

    if not host_name:
        smtp_server_ = "smtp.gmail.com"
    else:
        smtp_server_ = host_name

    if not host_port:
        if ssl_connect_:
            smtp_port_ = 465  # For SSL
        else:
            smtp_port_ = 587
    else:
        smtp_port_ = host_port

    ret_val = start_smtp_connection(status)

    return ret_val

# -----------------------------------------------------------------------------------------------------------------------
# Start new SMTP connection
# -----------------------------------------------------------------------------------------------------------------------
def start_smtp_connection(status):

    global smtp_client_
    global smtp_server_        # URL of the server
    global smtp_port_
    global sender_email_
    global sender_password_
    global smtp_mutex_

    smtp_mutex_.acquire()

    if smtp_client_:
        status.add_error("SMTP client is running using %s with email %s" % (smtp_server_, sender_email_))
        ret_val =  process_status.ERR_process_failure
    else:
        try:
            if ssl_connect_:
                # Create a secure SSL context
                context = ssl.create_default_context()
                smtp_client_ = smtplib.SMTP_SSL(smtp_server_, smtp_port_, context=context)
            else:
                smtp_client_ = smtplib.SMTP(smtp_server_, smtp_port_)
                # Starting the server
                smtp_client_.starttls()
            #  login
            smtp_client_.login(sender_email_, sender_password_)
        except smtplib.SMTPException as error:
            smtp_client_ = None
            err_msg = "Failed to initiate SMTP server using '%s' with error: '%s'" % (smtp_server_, error)
            status.add_error(err_msg)
            ret_val = process_status.ERR_process_failure
        except:
            smtp_client_ = None
            err_msg = "Failed to initiate SMTP server using %s" % smtp_server_
            status.add_error(err_msg)
            ret_val = process_status.ERR_process_failure
        else:
            ret_val = process_status.SUCCESS

    smtp_mutex_.release()

    return ret_val

# -----------------------------------------------------------------------------------------------------------------------
# Test connection and reconnect if connection closed
# -----------------------------------------------------------------------------------------------------------------------
def validate_smtp_connect(status):

    try:
        ret_code = smtp_client_.noop()[0]
    except:
        ret_code = -1

    if ret_code == 250:
        # WIth connection
        ret_val = process_status.SUCCESS
    else:
        # reconnect
        ret_val = start_smtp_connection(status)

    return ret_val
# -----------------------------------------------------------------------------------------------------------------------
# Send an email
# Example how to make a complete connection is available here - https://developers.google.com/gmail/api/quickstart/python
#'usage': 'email to [receiver email] where subject = [message subject] and message = [message text]',
# 'example': 'email to my_name@my_company.com where subject = "anylog alert" and message = "message text"',
# -----------------------------------------------------------------------------------------------------------------------
def email_send(status, io_buff_in, cmd_words, trace):

    global smtp_client_
    global smtp_server_
    global sender_email_
    global smtp_mutex_

    if len(cmd_words) < 3:
        return process_status.ERR_command_struct

    if not smtp_client_:
        status.add_error("SMTP client is not running")
        return process_status.ERR_process_failure

    if cmd_words[1] != "to":
        return process_status.ERR_command_struct

    subject = "AnyLog Alert"        # Default subject
    message = "AnyLog Network Alert from Node: %s" % get_node_name() # default message

    if len(cmd_words) > 3:
        if cmd_words[3] != "where":
            return process_status.ERR_command_struct

        #                                Must     Add      Is
        #                                exists   Counter  Unique
        keywords = {
                    "subject": ("str",  False, True, True),
                    "message": ("str",  False, True, False),
                    }

        ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
        if ret_val:
            # conditions not satisfied by keywords or command structure
            return ret_val

        subject = interpreter.get_one_value_or_default(conditions, "subject", subject)
        message = get_message_lines(conditions)

    receiver_email = params.get_value_if_available(cmd_words[2])


    sender_message = "Subject: %s\n\n%s" % (subject, message)

    ret_val = validate_smtp_connect(status)     # validate connection

    if not ret_val:

        smtp_mutex_.acquire()

        try:
            smtp_client_.sendmail(sender_email_, receiver_email, sender_message)
        except smtplib.SMTPException as error:
            err_msg = "Failed to send an email message using '%s' with error: '%s'" % (smtp_server_, error)
            status.add_error(err_msg)
            ret_val = process_status.ERR_process_failure
        except:
            err_msg = "Failed to send an email message using '%s'" % smtp_server_
            status.add_error(err_msg)
            ret_val = process_status.ERR_process_failure
        else:
            ret_val = process_status.SUCCESS

        smtp_mutex_.release()

    return ret_val
# -----------------------------------------------------------------------------------------------------------------------
# Make a message from multiple lines
# -----------------------------------------------------------------------------------------------------------------------
def get_message_lines(conditions):
    message = ""
    if "message" in conditions:
        # One or multiple message lines
        message_lines = conditions["message"]
        for msg_line in message_lines:
            message += (msg_line + '\n')

    return message
# -----------------------------------------------------------------------------------------------------------------------
# Example: https://dev.to/mraza007/sending-sms-using-python-jkd
'''
SMS Gateways for each Carrier
AT&T: [number]@txt.att.net
Sprint: [number]@messaging.sprintpcs.com or [number]@pm .sprint.com
T-Mobile: [number]@tmomail.net
Verizon: [number]@vtext.com
Boost Mobile: [number]@myboostmobile.com
Cricket: [number]@sms.mycricket.com
Metro PCS: [number]@mymetropcs.com
Tracfone: [number]@mmst5.tracfone.com
U.S. Cellular: [number]@email.uscc.net
Virgin Mobile: [number]@vmobl.com
'''
# sms to 6508147334 where gateway = [SMS gateway] and subject = [message subject] and message = [message text]',
# sms to 6508147334 where gateway = tmomail.net
# -----------------------------------------------------------------------------------------------------------------------
def sms_send(status, io_buff_in, cmd_words, trace):

    global smtp_client_
    global smtp_server_
    global sender_email_
    global smtp_mutex_

    if len(cmd_words) < 7:
        return process_status.ERR_command_struct

    if not smtp_client_:
        status.add_error("SMTP client is not running")
        return process_status.ERR_process_failure

    if cmd_words[1] != "to" or cmd_words[3] != "where":
        return process_status.ERR_command_struct

    #                           Must     Add      Is
    #                           exists   Counter  Unique
    keywords = {
                "gateway": ("str", True, False, True),
                "subject": ("str",  False, True, True),
                "message": ("str",  False, True, True),
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return ret_val

    receiver_phone = params.get_value_if_available(cmd_words[2])

    gateway = interpreter.get_one_value(conditions, "gateway")
    subject = interpreter.get_one_value_or_default(conditions, "subject", "AnyLog Alert")
    message = interpreter.get_one_value_or_default(conditions, "message","AnyLog Network Alert from Node: '%s'" % get_node_name())

    sms_gateway = receiver_phone + '@' + gateway

    ret_val = validate_smtp_connect(status)  # validate connection

    if not ret_val:

        smtp_mutex_.acquire()
        # Use the MIME module to structure our message.
        try:
            msg = MIMEMultipart()
            msg['From'] = sender_email_
            msg['To'] = sms_gateway
            msg['Subject'] = subject
            msg.attach(MIMEText(message + '\n', 'plain'))

            sms = msg.as_string()

            smtp_client_.sendmail(sender_email_, sms_gateway, sms)
        except smtplib.SMTPException as error:
            err_msg = "Failed to send an sms message using '%s' with error: '%s'" % (smtp_server_, error)
            status.add_error(err_msg)
            ret_val = process_status.ERR_process_failure
        except:
            err_msg = "Failed to send an sms message using '%s'" % smtp_server_
            status.add_error(err_msg)
            ret_val = process_status.ERR_process_failure
        else:
            ret_val = process_status.SUCCESS

        smtp_mutex_.release()

    return ret_val

# =======================================================================================================================
# Place data to a dictionary or a file
# # Place data to a dictionary or a file (if dest looks like: [file=!prep_dir/london.readings.json, key=results, show= true]
# File - file location
# Key - use the specified key into the object
# Show - use a bar to show status of data write
# Example: [file=!prep_dir/london.readings.json, key=results, show= true] = rest get where url = https://datahub.io/core/london-air-quality/r/1.json
# =======================================================================================================================
def save_data(status, destination, data_obj):
    if destination[0] == '[' and destination[-1] == ']':
        # Output instructions inside brackets
        show_stat = False  # Suppress write output status
        key = None
        file_dest = None
        instructions = destination[1:-1].split(',')  # Multiple instructions are splitted by comma inside the brackets
        for entry in instructions:
            instruct = entry.split('=')
            if len(instruct) != 2:
                status.add_error("Data destination is not formatted correctly: %s" % destination)
                ret_val = process_status.ERR_command_struct
                break

            if instruct[0].strip() == "file":
                # write data to file
                file_dest = params.get_value_if_available(instruct[1].strip())
            elif instruct[0].strip() == "key":
                # Only the data of the provided key are of interest
                key = params.get_value_if_available(instruct[1].strip())
            elif instruct[0].strip() == "show":
                # write data to file
                if instruct[1].strip().lower() == 'true':
                    show_stat = True  # Print output status
            else:
                status.add_error("Data destination is not formatted correctly: %s" % destination)
                ret_val = process_status.ERR_command_struct
                break
        if file_dest:
            if key and isinstance(data_obj, dict) and key in data_obj.keys():
                write_obj = data_obj[key]
            else:
                write_obj = data_obj
            ret_val = write_object_to_file(status, file_dest, write_obj, show_stat)
        else:
            status.add_error("Missing output file name with the following instructions: %s" % destination)
            ret_val = process_status.ERR_process_failure
    else:
        # Keep in global dictionary
        ret_val = process_status.SUCCESS
        params.add_param(destination, str(data_obj))

    return ret_val


# =======================================================================================================================
# Write the data object to a file
# =======================================================================================================================
def write_object_to_file(status, file_dest, data_obj, show_stat):
    ret_val = process_status.SUCCESS

    io_handle = utils_io.IoHandle()
    if not io_handle.open_file("append", file_dest):
        status.add_error("Failed to open output file: %s" % file_dest)
        ret_val = process_status.ERR_process_failure

    if isinstance(data_obj, dict):
        str_data = utils_json.to_string(data_obj)
        if str_data:
            str_data_formated = str_data.replace("}, ","},\n")    # Add new lines
            if not io_handle.append_data(str_data_formated + '\n'):
                status.add_error("Failed to write to output file: %s" % file_dest)
                ret_val = process_status.ERR_process_failure

    elif isinstance(data_obj, list):

        if show_stat:
            # print write status
            elements = len(data_obj)

        for index, entry in enumerate(data_obj):
            str_data = utils_json.to_string(entry)
            if str_data:
                if not io_handle.append_data(str_data + '\n'):
                    status.add_error("Failed to write to output file: %s" % file_dest)
                    ret_val = process_status.ERR_process_failure
                    break
            if show_stat:
                utils_print.print_status(elements, 50, index)
    else:
        status.add_error("Failed to recognize data structure for output file: %s" % file_dest)
        ret_val = process_status.ERR_wrong_json_structure

    if not io_handle.close_file():
        status.add_error("Failed to close output file: %s" % file_dest)
        ret_val = process_status.ERR_process_failure

    return ret_val
