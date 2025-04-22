"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

# HTTP documentation: https://docs.python.org/3/library/http.server.html
# Adding thread POOL Example: https://code.activestate.com/recipes/574454-thread-pool-mixin-class-for-use-with-socketservert/


import sys
import ssl
import os
import traceback
with_profiler_ = True if os.getenv("PROFILER", "False").lower() == "true" else False      # Needs to return True - otherwise will be False
if with_profiler_:
    import edge_lake.generic.profiler as profiler

try:
    import OpenSSL
    from OpenSSL import crypto
except:
    with_open_ssl_ = False
else:
    with_open_ssl_ = True


from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn

import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.generic.params as params
import edge_lake.generic.version as version
import edge_lake.generic.process_status as process_status
import edge_lake.generic.process_log as process_log
import edge_lake.job.job_scheduler as job_scheduler
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.utils_io as utils_io
import edge_lake.generic.utils_data as utils_data
import edge_lake.api.al_grafana as al_grafana
import edge_lake.cmd.native_api as native_api
import edge_lake.generic.interpreter as interpreter
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.streaming_data as streaming_data
import edge_lake.tcpip.net_utils as net_utils
import edge_lake.generic.utils_threads as utils_threads
import edge_lake.tcpip.mqtt_client as mqtt_client
import edge_lake.tcpip.html_reply as html_reply
from edge_lake.generic.utils_columns import get_current_time


# REST with server and client authentication requests - https://requests.readthedocs.io/en/master/user/advanced/
# https://requests.readthedocs.io/en/master/user/quickstart/
# REST DOCUMENTATION - https://tools.ietf.org/html/rfc7231
# REST returned codes - explained here - https://restfulapi.net/http-status-codes/
REST_OK = 200
REST_BAD_REQUEST = 400
REST_UNAUTHORIZED = 401
REST_WRONG_DETAILS = 410
REST_INTERNAL_SERVER_ERROR = 500
REST_NOT_IMPLEMENTED = 501

# AnyLog returned codes

REST_API_EXIT = -1
REST_API_OK = 0
REST_API_BAD_REQUEST = 1
REST_API_WRONG_DETAILS = 2
REST_API_WRONG_TYPE = 3
REST_API_MISSING_BODY = 4

# Mapping AnyLog codes to REST returned values.  see https://tools.ietf.org/html/rfc7231
rest_returned_code = [
    REST_OK,  # 0:  REST_API_OK --> REST_OK
    REST_BAD_REQUEST,  # 1:  REST_API_BAD_REQUEST --> REST_BAD_REQUEST
    REST_BAD_REQUEST,  # 2:  REST_API_WRONG_DETAILS --> REST_BAD_REQUEST
    REST_BAD_REQUEST,  # 3:  REST_API_WRONG_TYPE --> REST_BAD_REQUEST
    REST_BAD_REQUEST,  # 4:  REST_API_WRONG_TYPE --> REST_BAD_REQUEST
]

rest_errors_msg = [
    "{\"AnyLog.result\":\"OK\"",  # 0
    "{\"AnyLog.error\":{\"Error Type\":\"Error in REST request\"",  # 1
    "{\"AnyLog.error\":{\"Error Type\":\"Error in REST header 'details'\"",  # 2
    "{\"AnyLog.error\":{\"Error Type\":\"Error in REST header 'type'\"",  # 3
    "{\"AnyLog.error\":{\"Error Type\":\"Error in REST body - no data'\"",  # 4

]

declared_ip_port = ""

user_agent_ = {
    "anylog"    :   ("product", 0, None),
    "grafana"   :   ("product", 0, None),
}

http_methods_ = {
#   Lookup key
    "sql"       :       "get*post",  # SQL can be post and get (i.e. insert/create is post and query is get)
    "help"      :       "get",
    "get"       :       "get",
    "blockchain get":   "get",
    "blockchain read":  "get",
    "blockchain drop":  "post",
    "query status":     "get",
    "query explain":    "get",
    "query destination":    "get",
    "analyze output":   "get",
    "time file get":    "get",
    "time file summary":    "get",
    "job status":       "get",
    "job active":       "get",      # The list of jobs which are not completed
    "job run":          "post",     #  Executes the specified job command
    "job stop":         "post",     # stops the execution of a particular running job or stops a scheduled job
    "file get":         "get",
    "file retrieve":    "get",
    "test":             "get",
    "wait":             "get*post",
    "subprocess":       "get*post",
}

rest_stat_ = {}         # Collect statistics

with_traceback_ = False     # On an exception - show error file and line, enabled with: set exception traceback on

# ---------------------------------------------------------
# Get the IP and port which is used by the server
# ---------------------------------------------------------
def get_declared_ip_port():
    global declared_ip_port
    return declared_ip_port

# =======================================================================================================================
# Provide the parameters to call the AnyLog instance
# =======================================================================================================================
class http_server_info():
    server = None
    connection = ""
    is_bind = True              # Determines if listening to a single IP or all
    timeout = 0
    buff_size = 0
    trace = False
    is_ssl = False
    workers_count = 0           # Number of threads in the workers pool
    workers_pool = None         # Threads Pool
    ca_public_key = None        # The CA public key file
    node_cr = None              # The node Certificate Request file
    node_private_key = None     # The node private key file
    lib_open_ssl = with_open_ssl_   # Test if library is loaded
    streaming_log = False       # Sets to true to enable logging

def get_workers_pool():
    return http_server_info.workers_pool

def is_ssl():
    global http_server_info
    return http_server_info.is_ssl

# =======================================================================================================================
# Structure that maintains the parameters of the call
# =======================================================================================================================
class request_info():
    rest_status = REST_API_OK
    send_to_operators = False
    command_type = ""
    details = ""
    command = ""
    dbms_name = ""
    table_name = ""
    servers = ""  # ip and ports of the servers to use to process the command
    instructions = ""  # instruction relating the sql query such as:
    # include - list of databases and tables to include in the query
    # table - a name for an output table to maintain the query reslt set
    # drop - Indicate if the output data is to be added to an existing result set
    is_select = False
    api_command_flag = False  # set to True if command is resolved on the API (like 'show peer')
    api_command = ""  # the details of the api command
    detailed_error = ""  # API error msg


# =======================================================================================================================
# Structure to debug code
# =======================================================================================================================
class debug_rest_flow():
    code_path = "\n"
    step = 0
    is_failed = False

# =======================================================================================================================

# documentation: https://docs.python.org/3.4/library/socketserver.html

# =======================================================================================================================

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """An HTTP Server that handle each request in a new thread"""
    # Example is here - https://docs.aws.amazon.com/polly/latest/dg/example-Python-server-code.html
    #
    # The ThreadingMixIn class defines an attribute daemon_threads,
    # which indicates whether or not the server should wait for thread termination.
    #  You should set the flag explicitly if you would like threads to behave autonomously;
    #  the default is False, meaning that Python will not exit until all threads created by ThreadingMixIn have exited.
    daemon_threads = True


    # --------------------------------------------------------------------
    # Overrides the method in class ThreadingMixIn in socketserver.py
    # Setp into serve_forever
    # Setep into _handle_request_noblock
    # Comment out the implementation to disable the OVERRIDE
    # --------------------------------------------------------------------
    def process_request(self, request, client_address):
        """Start a new thread to process the request."""

        # Provide the socket to a new thread
        task = http_server_info.workers_pool.get_free_task()
        task.set_cmd(self.process_request_thread, [request, client_address])
        http_server_info.workers_pool.add_new_task(task)

        '''
        # Source Code
        t = threading.Thread(target = self.process_request_thread,
                             args = (request, client_address))
        t.daemon = self.daemon_threads
        if not t.daemon and self.block_on_close:
            if self._threads is None:
                self._threads = []
            self._threads.append(t)
        t.start()
        '''

    # =======================================================================================================================
    #  overridden handle_error in socketserver.pi
    # =======================================================================================================================
    def handle_error(self, request, client_address):
        """Handle an error gracefully.  May be overridden.

        The default is to print a traceback and continue.

        """
        try:
            message1 = "REST server on %s received unrecognized message" % http_server_info.connection
        except:
            message1 = "REST server received unrecognized message"
            pass

        try:
            message2 = " from %s" % str(client_address)
        except:
            message2 = " from unrecognized client"


        err_msg = message1 + message2

        utils_print.output(err_msg, True)
        process_log.add("Error", err_msg)


# =======================================================================================================================
# Defining a HTTP request Handler class
# test with - curl -X GET 10.0.0.41:8080/store.json
#
# curl --header "type":"info" --header "dbms":"anylog_test" --header "details":"info dbms anylog_test tables" --request GET 10.0.0.41:8080
# curl --header "type":"info" --header "dbms":"anylog_test" --header "details":"info table anylog_test ping_sensor columns" --request GET 10.0.0.41:8080
# curl --header "type":"sql" --header "dbms":"anylog_test" --header "details":"SELECT * from ping_sensor limit 3;" --request GET 10.0.0.41:8080
# https://docs.python.org/3/library/http.server.html
# =======================================================================================================================
class ChunkedHTTPRequestHandler(BaseHTTPRequestHandler):

    # =======================================================================================================================
    # Print message if trace level is set: trace level = 1 run rest server
    # This is called before the end of the execution
    # =======================================================================================================================
    def log_message(self, format, *args):

        trace_level = member_cmd.commands["run rest server"]['trace']
        if trace_level:
            # Trace leve is 1 - print incoming message source
            try:
                command = get_value_from_headers(self.al_headers, "command")
            except:
                command = "Undetermined"

            message = "\r\n%s - - [%s] [Command: %s] %s" %(self.address_string(), self.log_date_time_string(), command, format % args)
            if trace_level == 1:
                utils_print.output(message, True)
            else:
                utils_print.output(message, False)  # More printouts to come, wait with AL prompt


    # =======================================================================================================================
    # Log execution: trace level = 1 run rest server
    # =======================================================================================================================
    def log_streaming(self, ret_val):
        global http_server_info

        trace_level = member_cmd.commands["run rest server"]['trace']
        if trace_level > 1:

            # Trace leve is 2 - print command and ret value
            try:
                command = get_value_from_headers(self.al_headers, "command")
            except:
                command = "Undetermined"

            status_text = process_status.get_status_text(ret_val)

            message_cmd = "\r\n%s - - [%s] [%s]" % (self.address_string(), command, status_text)

            if trace_level > 2:
                # Trace leve is 2 - print incoming message source + msg_body
                message = "\r\n%s - - HEADERS: %s\r\nBODY: %s\r\n" % (self.address_string(), str(self.headers._headers), str(self.msg_body))
                utils_print.output(message, False)

            utils_print.output(message_cmd, True)

        if http_server_info.streaming_log or ret_val:
            # Place every call in the log or if an error
            # Trace leve is 2 - print command and ret value
            try:
                command = get_value_from_headers(self.al_headers, "command")
            except:
                command = "Undetermined"

            status_text = process_status.get_status_text(ret_val)
            message = "%s [Command: %s] [(%u) %s]" % (self.address_string(), command, ret_val, status_text)
            if self.msg_body:
                message += "\r\nHEADERS: %s\r\nBODY: %s" % (str(self.headers._headers), str(self.msg_body))
            else:
                message += "\r\nHEADERS: %s" % (str(self.headers._headers))
            process_log.secondary_log("streaming", message)



    # =======================================================================================================================
    #  private error
    # =======================================================================================================================
    def handle_exception(self, status, rest_type, errno, value, stack_trace):

        try:
            stack_list = traceback.format_tb(stack_trace)
            lines_to_print = 2 if len(stack_list) > 1 else 1
            part_stack = "".join(stack_list[-lines_to_print:])   # Print 1 or 2 lines

            utils_print.output_box(f"\nFailed line: {part_stack}")

            if with_traceback_:
                # This option is enabled by the command:
                #       set exception traceback on
                full_traceback = ''.join(stack_list)
                utils_print.output("\r\n" + full_traceback, True)
        except:
            pass

        try:
            message = "HTTP %s call exception from IP: %s" % (rest_type, str(self.client_address))
            process_log.add("Error", message)
        except:
            message = "HTTP Exception"

        message += " [errno: %s] [value: %s] " % (str(errno), str(value))

        try:
            command = get_value_from_headers(self.al_headers, "command")
            process_log.add("Error", "REST Error: %s [Exception] Failed to process command: [%s]" % (message, command))
        except:
            command = "Undetermined"

        utils_print.output_box("%s\nFailed to process command: %s" % (message, command))

        process_log.secondary_log("streaming", "%s [Exception] Failed to process command: [%s]" % (message, command))

        process_status.stack_to_location(stack_trace)       # Print the code location

        if self.msg_body:
            utils_print.output("Body: %s" % (self.msg_body), True)

    # =======================================================================================================================
    #  private error
    # =======================================================================================================================
    def handle_error(self, status, err_message, rest_type, errno, value):

        command = get_value_from_headers(self.al_headers, "command")
        if not command:
            command = ""
        message = "HTTP %s/%s call from IP: %s failed: %s" % (rest_type, command, str(self.client_address), err_message)

        message += " [errno: %s] [value: %s] " % (str(errno), str(value))

        utils_print.output_box(message)

        status.add_error(message)       # Print the code location

    # =======================================================================================================================
    # Logs Errors
    # =======================================================================================================================
    def log_error(self, format, *args):

        try:
            message1 = "REST server on %s received unrecognized message" % http_server_info.connection
        except:
            message1 = "REST server received unrecognized message"
            pass

        utils_print.output(message1, True)
        process_log.add("Error", message1)

        try:
            ip_source = str(self.address_string())
            date_time = str(self.log_date_time_string())
        except:
            ip_source = "Not recognized"
            date_time = get_current_time("%Y-%m-%d %H:%M:%S")

        try:
            error_name = str(args[0])
            error_txt = str(args[1]).replace("\\x00", "")
        except:
            error_name = "HTTP Request Error"
            error_txt = "Failed to retrieve failure info"

        utils_print.output("%s: [Source: %s] [Date: %s] [Message: %s]" % (error_name, ip_source, date_time, error_txt), True)  # We saw UnicodeEncodeError - an error caused by a Chinese character encoding problem in Python, mainly caused by the character \u200e

        try:
            message3 = "REST error using: %s" % str(self.connection)
        except:
            pass
        else:
            utils_print.output(message3, True)
            process_log.add("Error", message3)


    # =======================================================================================================================
    # Send reply headers + message - update the msg size in header
    # =======================================================================================================================
    def write_headers_and_msg(self, status, response_code, content_type, message ):

        ret_val = process_status.SUCCESS
        html_policy = None
        into_output = get_value_from_headers(self.al_headers, "into")   # Push the output as HTML

        if not into_output:
            encoded_message = message.encode(encoding='UTF-8')
        else:
            # Return HTML
            html_info = get_value_from_headers(self.al_headers, "html")
            if html_info:
                if html_info.startswith("blockchain "):
                    ret_val, html_policy = native_api.get_from_blockchain(status, html_info)
                    if ret_val or not html_policy:
                        encoded_message = "Failed to retrieve Ploicy using '{html_info}'".encode(encoding='UTF-8')
                        ret_val = process_status.ERR_process_failure
                else:
                    html_policy, err_msg = html_reply.url_to_json(status, html_info)  # Get the user info for the HTML page
                    if err_msg or not html_policy:
                        encoded_message = err_msg.encode(encoding='UTF-8')
                        ret_val = process_status.ERR_wrong_json_structure
                    elif not isinstance(html_policy,dict):
                        status.add_error("Failed to generate a JSON structure from user's HTML instructions - wrong JSON input structure")
                        ret_val = process_status.ERR_wrong_json_structure

            if not ret_val:
                is_pdf = get_value_from_headers(self.al_headers, "pdf", "bool")
                encoded_message = html_reply.to_html(status, message, content_type, into_output, is_pdf, html_policy).encode(encoding='UTF-8')
                content_type =  "text/html"


        ret_val = self.send_reply_headers(status, response_code, None, False, content_type, len(encoded_message), False, None)
        if not ret_val:
            write_ret_value = utils_io.write_encoded_to_stream(status, self.wfile, encoded_message)

        return ret_val

    # =======================================================================================================================
    # Send reply headers
    # Details at https://docs.python.org/3/library/http.server.html
    # =======================================================================================================================
    def send_reply_headers(self, status, response_code, message, echo_message, content_type, content_length, is_chunked, accept_ranges):
        '''
        status - thread object
        response_code - REST reply
        message - message to deliver
        echo_message - message to the node echo queue
        content_length - messages which are nor queries to data and length can be determined
        is_chunked - bool value to represent reply with data (or with info with undetermined length)
        '''

        try:
            # defining all the headers
            if message:
                message += "\r\n"
            self.send_response(response_code, message)
            self.send_header('Content-Type', content_type)
            if is_chunked:
                self.send_header('Transfer-Encoding', 'chunked') # https://en.wikipedia.org/wiki/Chunked_transfer_encoding
            elif content_length:
                self.send_header('Content-Length', str(content_length))  # https://datatracker.ietf.org/doc/html/rfc9110#name-content-length

            if accept_ranges:
                # bytes: This indicates that the server supports range requests and can return partial content using the Range header.
                # The Range header specifies the byte range of the content the client wants to retrieve.
                # For example, a client can send a request with the Range header set to bytes=0-999 to retrieve the first 1000 bytes of a resource.
                self.send_header("Accept-Ranges", accept_ranges)
                # public - the response can be stored by any cache (on the client)
                # max-age=3600 - the response can be cached for one hour
                self.send_header("Cache-Control", "public, max-age=3600")


            self.end_headers()      # The buffered headers are written to the output stream
        except:
            error_type, error_value = sys.exc_info()[:2]
            err_msg = "REST failed to write reply header: {} - {}".format(error_type, error_value)
            status.add_error(err_msg)
            utils_print.output_box(err_msg)
            ret_val = process_status.REST_header_err
        else:
            ret_val = process_status.SUCCESS

        if echo_message and message:
            self.echo_error(status, message)
        return ret_val
    # =======================================================================================================================
    # Echo error message and connection info
    # =======================================================================================================================
    def echo_error(self, status, message):

        echo_queue = member_cmd.echo_queue  # Get the method as the user may Null the method
        if echo_queue:
            queue_msg = "REST call error"
            if message:
                queue_msg += ": <%s>" % message
            queue_msg += " using connection: <%s>" % str(self.connection)
            echo_queue.add_msg(queue_msg)

    # =======================================================================================================================
    # Authenticate the user or certificate with the request
    # https://requests.readthedocs.io/en/master/user/advanced/
    # =======================================================================================================================
    def authenticate_request(self, status, rest_type, command):
        '''
        status - process status object
        rest_type - REST call: GET, PUT, POST etc.
        command - the AnyLog command
        '''

        if http_server_info.is_ssl:
            if with_open_ssl_:
                ret_val = self.process_user_certificate(status, rest_type, command)
            else:
                ret_val = False
        else:
            ret_val = True  # Ignore certificate

        if ret_val:
            if version.al_auth_is_user_authentication():
                # use basic authentication
                key = get_value_from_headers(self.al_headers, "Authorization")
                ret_val = version.al_auth_validate_user(status, key, False)

        if not ret_val:
            # Reply with Unauthorized
            error_msg = "Failed AnyLog Client Authentication"
            status.add_error(error_msg)
            self.send_reply_headers(status, REST_UNAUTHORIZED, error_msg, True, 'text/json', 0, True, None)

        return ret_val

    # =======================================================================================================================
    # Each ANyLog command is assigned to an HTTP method. The default method is POST
    # =======================================================================================================================
    def is_correct_method(self, status, http_method, command_list):
        global http_methods_

        ret_val = False
        # Test 2 words key

        if len(command_list) >= 2:
            key = command_list[0].lower() + ' ' + command_list[1].lower()
            if key in http_methods_:
                if http_methods_[key] == http_method:
                    # correct key was used
                    ret_val = True
            elif len(command_list) >= 3:
                key += (' ' + command_list[2].lower())
                if key in http_methods_:
                    if http_methods_[key] == http_method:
                        # correct key was used
                        ret_val = True
            if not ret_val:
                # Test 1 word key
                key = command_list[0].lower()
                if key in http_methods_:
                    if http_methods_[key] == http_method:
                        # correct key was used
                        ret_val = True
                    elif http_method in http_methods_[key]:
                        # SQL can be post and get (i.e. insert/create is post and query is get)
                        ret_val = True

                else:
                    if http_method == "post":
                        ret_val = True          # the default
        else:
            if len(command_list) == 1:
                key = command_list[0]  # One word command
                if key in http_methods_:
                    if http_methods_[key] == http_method:
                        # correct key was used
                        ret_val = True
            else:
                key = "Command not provided"

        if not ret_val:
            # Error message
            error_msg = "Wrong HTTP method (%s) used with AnyLog command: '%s'" % (http_method.upper(), key)
            status.add_error(error_msg)

        return ret_val

    # =======================================================================================================================
    # Failed to process with AnyLog command
    # =======================================================================================================================
    def error_failed_process(self, status, with_wait, error_code, http_method, into_output, command, err_msg):

        err_reply = {
            "method": http_method,
            "node": self.address_string(),
            "err_code": error_code,
            "err_text": process_status.get_status_text(error_code)  # get the generic message

        }

        if err_msg:
            # Get the operator error
            err_reply["operaor_msg"] = err_msg

        # the call to AnyLog is with an error
        err_msg = status.get_saved_error()  # try detailed message
        if err_msg:
            err_reply["local_msg"] = err_msg

        reply = utils_json.to_string(err_reply)
        if not reply:
            reply = str(err_reply)

        if into_output:

            # Place error in the html and send as HTMP
            encoded_message = html_reply.to_html(status, reply, "text/html", into_output, False, None).encode(encoding='UTF-8')

            content_type = "text/html"
            ret_val = self.send_reply_headers(status, REST_OK, None, False, content_type, len(encoded_message),
                                              False, None)
            if not ret_val:
                write_ret_value = utils_io.write_encoded_to_stream(status, self.wfile, encoded_message)

        else:

            if not with_wait:
                # If with_wait it True - a SQL query is processed and different threads satisfy the query
                # The headers were send before the process started

                if reply:
                    content_type = get_result_set_type(reply)  # returns 'text/json' or "text"
                else:
                    reply = "AnyLog command returned an error"  # Generic reply
                    content_type = 'text'

                self.send_reply_headers(status, REST_BAD_REQUEST, reply, True, content_type, 0, True, None)

            write_ret_value = utils_io.write_to_stream(status, self.wfile, reply, False, True)

    # =======================================================================================================================
    # Mismatch between REST CALL and AnyLog command - return an error
    # =======================================================================================================================
    def error_wrong_method(self, status, http_method, command):

        if http_method == "post" and command == 'data':
            # missing run mqtt client where broker = rest
            ret_val = process_status.Missing_data_ingest_mapping
        else:
            ret_val = process_status.Wrong_http_metod

        err_reply = {
            "method": http_method,
            "node": self.address_string(),
            "err_code": ret_val,
            "err_text": process_status.get_status_text(ret_val)  # get the generic message
        }
        reply = utils_json.to_string(err_reply)

        self.write_headers_and_msg(status, REST_BAD_REQUEST, "json", reply)

        return ret_val

    # =======================================================================================================================
    # Process user certificate - This process is done by authenticating the user using the certificate authority public
    # key (the file ca-public-key.pem) in the validation process when the message arrives.
    # =======================================================================================================================
    def process_user_certificate(self, status, rest_type, command):
        '''
        status - process status object
        rest_type - REST call: GET, PUT, POST etc.
        command - the AnyLog command
        '''

        try:
            cert = self.connection.getpeercert(True)
            cert = crypto.load_certificate(crypto.FILETYPE_ASN1, cert)
        except:
            errno, value = sys.exc_info()[:2]
            err_message = "Failed to load certificate from connection with Error: %s and Value: %s" % (str(errno), str(value))
            self.handle_error(status, err_message, rest_type, errno, value)
            ret_val = False
        else:

            if cert.has_expired():
                self.handle_error(status, "Certificate expired", rest_type, "Certificate Info", "Expired Certificate")
                ret_val = False
            else:
                try:
                    public_key = crypto.dump_publickey(crypto.FILETYPE_PEM,cert.get_pubkey()).decode()
                except:
                    errno, value = sys.exc_info()[:2]
                    err_message = "Failed to retrieve public key from certificate - Error: %s and Value: %s" % (str(errno), str(value))
                    self.handle_error(status, err_message, rest_type, errno, value)
                    ret_val = False
                else:
                    ret_val = True
                    if get_value_from_headers(self.al_headers, "trace_certificate") == "true":
                        # the header includes trace_certificate : True

                        # print(crypto.dump_publickey(crypto.FILETYPE_PEM,cert.get_pubkey()))
                        # print("Issuer: ", cert.get_issuer())
                        subject_list = cert.get_subject().get_components()
                        cert_byte_arr_decoded = []
                        for item in subject_list:
                            key = item[0].decode('utf-8')
                            if key == "CN":
                                details = "Common Name"
                            elif key == "GN":
                                details = "Identifier"
                            else:
                                details = ""

                            value = item[1].decode('utf-8')
                            cert_byte_arr_decoded.append( (key, value, details) )

                        utils_print.output_nested_lists(cert_byte_arr_decoded, "\r\nClient Certificate Info", ["Key", "Value", "Details"], False, "")

                        # end_date = datetime.strptime(str(cert.get_notAfter().decode('utf-8')), "%Y%m%d%H%M%SZ")
                        # print("Not After (UTC Time): ", end_date)
                        # diff = end_date - datetime.now()
                        # print('Summary: "{}" SSL certificate expires on {} i.e. {} days.'.format(host, end_date, diff.days))

                        # x509_cert = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_ASN1, self.connection.getpeercert(True))

        if ret_val:
            # Use the autherization policies to determine if autherized
            ret_code = version.permissions_authenticate_rest_message(status, public_key)
            if ret_code:
                ret_val = False

        return ret_val

    # =======================================================================================================================
    # GET Method Definition
    # =======================================================================================================================
    def do_GET(self):

        ret_val = process_status.SUCCESS

        if with_profiler_:
            profiler.manage("get")  # Stop, Start and reset the profiler

        self.msg_body = None
        self.al_headers = set_al_headers(self.headers._headers)
        status = process_status.ProcessStat()

        user_agent, version, user_id = self.get_client("get")

        if not user_agent and self.raw_requestline:
            # This is an option to place AnyLog header on the URL line.
            # For example: http://10.0.0.78:7849/?command = get_status? User-Agent  = 'AnyLog/1.23',
            ret_val = self.header_from_raw_requestline(status)
            if not ret_val:
                user_agent, version, user_id = self.get_client("get")        # Try again for the needed info

        command = get_value_from_headers(self.al_headers, "command")

        if self.authenticate_request(status, "GET", command):

            if user_agent == "grafana":

                ret_val = al_grafana.grafana_get(status, self)

            elif command and user_agent == "anylog":

                try:
                    ret_val = self.al_exec(status, "get", command)      # Updated version
                except:
                    errno, value, stack_trace = sys.exc_info()[:3]
                    self.handle_exception(status, "GET", errno, value, stack_trace)
                    ret_val = process_status.REST_call_err
            else:
                # Treat like ping - return OK
                self.send_reply_headers(status, REST_OK, "", False, 'text/json', 0, True, None)

            native_api.end_job(status)  # need to end Job regardless of an error. for example in time-out to release the job
        else:
            ret_val = process_status.Failed_message_authentication

        update_stats("GET", user_agent, ret_val, self.client_address)

        self.log_streaming(ret_val)

        if with_profiler_:
            profiler.stop("get")     # Force Profiler because thread goes to sleep


    # =======================================================================================================================
    # An option to place AnyLog header on the URL line.
    # For example: http://10.0.0.78:7849/?User-Agent  = AnyLog/1.23 ? command = get status
    # http://10.0.0.78:7849/?User-Agent  = AnyLog/1.23 ? destination = network ? command = sql lsl_demo format = table select * from ping_sensor
    # =======================================================================================================================
    def header_from_raw_requestline(self, status):


        ret_val = process_status.SUCCESS

        request_str = self.raw_requestline.decode('utf-8')

        commands_str = utils_data.url_to_str(request_str)
        offset_headers = commands_str.find('?')
        index = commands_str.rfind("HTTP")          # The string ends with HTTP
        if offset_headers > 0 and (index == -1 or index > offset_headers):
            self.al_headers["url"] = True
            offset_headers += 1     # Skip the question mark
            headers_list = commands_str[offset_headers:index].split('?')
            for header in headers_list:
                if header and header != ' ':
                    offset_equal = header.find("=")     # Commands are split by the equal sign
                    if offset_equal < 1 or offset_equal == (len(header) - 1):
                        err_msg = f"Error in header value {header}"
                        ret_val = process_status.Wrong_header_struct
                        break
                    else:
                        key = header[:offset_equal].strip().lower()
                        value = header[offset_equal+1:].strip()
                        self.al_headers[key] = value
        if ret_val:
            status.add_error(err_msg)
            self.send_reply_headers(status, REST_BAD_REQUEST, err_msg, True, 'text', 0, True, None)

        return ret_val
    # =======================================================================================================================
    # Test is Grafana call
    # =======================================================================================================================
    def get_client(self, rest_call):

        user_id = 0
        user_agent = get_value_from_headers(self.al_headers, 'User-Agent')

        if not user_agent:
            product = None
            version = 0
        else:

            index = user_agent.find('/')
            if index == -1 or index == (len(user_agent) - 1):
                version = 0
            else:
                version = user_agent[index + 1:].strip().lower()

            if not index:
                # Slash as first char
                product = None
            elif index == -1:
                # No slash
                product = user_agent.strip().lower()
            else:
                product = user_agent[:index].strip().lower()

            if product not in user_agent_:
                if product == "curl":
                    # user did not specify a product - use "anylog"
                    product = "anylog"
                    if rest_call == "get" and not "command" in self.al_headers:
                        self.al_headers["command"] = "get status"   # Default command
                else:
                    # The product is not recognized
                    product = None
            else:
                if user_agent_[product][0] == "mqtt":
                    user_id = user_agent_[product][1]

        return [product, version, user_id]

    # =======================================================================================================================
    # VIEW
    # =======================================================================================================================
    def do_VIEW(self):

        status = process_status.ProcessStat()
        self.msg_body = None
        self.al_headers = set_al_headers(self.headers._headers)
        self.send_reply_headers(status, REST_NOT_IMPLEMENTED, "VIEW NOT SUPPORTED", True, 'text/json', 0, True, None)

        try:
            message = "Server received a non-supported REST request (VIEW) using: %s" % str(self.connection)
        except:
            utils_print.output("Server received a non-supported REST request (VIEW)", True)
        else:
            utils_print.output(message, True)
            process_log.add("Error", message)

            write_ret_value = utils_io.write_to_stream(status, self.wfile, message)

    # =======================================================================================================================
    # HEAD
    # =======================================================================================================================
    def do_HEAD(self):

        status = process_status.ProcessStat()
        self.msg_body = None
        self.al_headers = set_al_headers(self.headers._headers)


        self.send_reply_headers(status, REST_NOT_IMPLEMENTED, "HEAD NOT SUPPORTED", True, 'text/json', 0, False, None)

        try:
            message = "Server received a non-supported REST request (HEAD) using: %s" % str(self.connection)
        except:
            utils_print.output("Server received a non-supported REST request (HEAD)", True)
        else:
            utils_print.output(message, True)
            process_log.add("Error", message)


    # =======================================================================================================================
    # POST
    # https://docs.python.org/3/library/http.client.html
    # =======================================================================================================================
    def do_POST(self):

        if with_profiler_:
            profiler.manage("post")     # Stop, Start and reset the profiler

        self.msg_body = None
        self.al_headers = set_al_headers(self.headers._headers)
        status = process_status.ProcessStat()
        err_msg = None
        user_agent, version, user_id = self.get_client("post")

        header_delivered = False
        command = get_value_from_headers(self.al_headers, "command")
        if command and len(command) == 4 and command.lower() == "data":
            mqtt_data = True
        else:
            mqtt_data = False

        if self.authenticate_request(status, "POST", command):

            if user_agent == "anylog":
                if mqtt_data:
                    if user_id:
                        try:
                            # Send to the MQTT client if registered to the topic
                            ret_val = self.to_mqtt_client(status, user_agent, user_id)
                        except:
                            errno, value, stack_trace = sys.exc_info()[:3]
                            self.handle_exception(status, "POST", errno, value, stack_trace)
                            ret_val = process_status.REST_call_err
                    else:
                        err_msg = "{\"Error\" : \"HTTP server received POST message to add data and not able to determine the message broker user ID\" }"
                        status.add_error(err_msg)
                        ret_val = process_status.Unknown_user_agent
                else:
                    # Execute command
                    try:
                        ret_val = self.al_exec(status, "post", command)
                    except:
                        errno, value, stack_trace = sys.exc_info()[:3]
                        self.handle_exception(status, "POST", errno, value, stack_trace)
                        ret_val = process_status.REST_call_err
                    else:
                        header_delivered = True     # Inside self.al_exec

            elif user_agent == "grafana":

                ret_val, msg_body, err_msg = self.get_msg_body(status)
                if not ret_val:
                    try:
                        ret_val = al_grafana.grafana_post(status, self, msg_body, http_server_info.timeout)
                    except:
                        errno, value, stack_trace = sys.exc_info()[:3]
                        self.handle_exception(status, "POST", errno, value, stack_trace)
                        ret_val = process_status.REST_call_err

            elif mqtt_data and user_id:
                # Send to the MQTT client if registered to the topic
                ret_val = self.to_mqtt_client(status, user_agent, user_id)

            else:
                err_msg = "{\"Error\" : \"HTTP server received POST message from unknown 'User-Agent': '%s'\", \"Source\" : \"%s\"}" % (user_agent, self.address_string())
                status.add_error(err_msg)
                ret_val = process_status.Unknown_user_agent

            native_api.end_job(status)  # need to end Job regardless of an error. for example in time-out to release the job
        else:
            ret_val = process_status.Failed_message_authentication

        if not header_delivered:
            if ret_val:
                if not err_msg:
                    err_txt = process_status.get_status_text(ret_val)
                    err_msg = "{\"Error\" : \"HTTP POST error\" : \"%s\", \"source\" : \"%s\" }" % (err_txt, self.address_string())
                    content_type = 'text/json'
                else:
                    content_type = get_result_set_type(err_msg)  # returns 'text/json' or "text"

                if ret_val == process_status.Missing_data_ingest_mapping:
                    # A post command  to add data - but there is no client mapping to address the data
                    message = "No client mappint to User-Agent: AnyLog -- use the command 'run mqtt client where broker=rest and user-agent=anylog/1.23 ...' to assign a process the data"
                    status.add_error(err_msg)
                    self.write_headers_and_msg(status, REST_BAD_REQUEST, content_type, message)
                else:
                    self.send_reply_headers(status, REST_BAD_REQUEST, err_msg, True, content_type, 0, False, None)
            else:
                if not header_delivered:
                    self.send_reply_headers(status, REST_OK, "", True, 'text/json', 0, False, None)

        update_stats("POST", user_agent, ret_val, self.client_address)

        self.log_streaming(ret_val)

        if with_profiler_:
            profiler.stop("post")     # Force Profiler because thread goes to sleep

    # =======================================================================================================================
    # send info to MQTT CLIENT
    # If a client is registered for this info. (using broker=rest and user-agent determines the calls assigned to the MQTT)
    # Example: run mqtt client where broker=rest and user-agent=python and topic=(name=foglamp  and dbms="aiops" and table="bring [asset]" and column.timestamp.timestamp="bring [timestamp]" and column.value.float = "bring [readings]")
    # =======================================================================================================================
    def to_mqtt_client(self, status, product, user_id):

        global user_agent_

        topic = get_value_from_headers(self.al_headers, "topic")
        if not topic:
            # get the default topic
            topic = user_agent_[product][2]
        if not topic:
            status.add_error("Data provided is missing a topic")
            ret_val = process_status.ERR_wrong_data_structure
        else:

            # Publish the data as an MQTT topic
            ret_val, msg_body, err_msg = self.get_msg_body(status)
            if not ret_val:

                if msg_body and msg_body[0] == '[':
                    messages_list = utils_json.str_to_list(msg_body)
                    if messages_list:
                        # List of messages
                        for message in messages_list:
                            msg_str = utils_json.to_string(message)

                            try:
                                ret_val = mqtt_client.process_message(topic, user_id, msg_str)
                            except:
                                errno, value, stack_trace = sys.exc_info()[:3]
                                self.handle_exception(status, "POST", errno, value, stack_trace)
                                ret_val = process_status.REST_call_err
                                if len(message) > 200:
                                    utils_print.output_box("Failed to process message: %s" % message[:200] + ' . . .')
                                else:
                                    utils_print.output_box("Failed to process message: %s" % message)

                            if ret_val:
                                break
                    else:
                        status.add_error("Failed to map data to a list")
                        ret_val = process_status.ERR_wrong_data_structure
                else:
                    # One message
                    ret_val = mqtt_client.process_message(topic, user_id, msg_body)
                if ret_val:
                    ret_code = REST_BAD_REQUEST
                else:
                    ret_code = REST_OK

        return ret_val

    # =======================================================================================================================
    # AnyLog Get or Post
    # =======================================================================================================================
    def al_exec(self, status, http_method, command):

        ret_val = process_status.SUCCESS

        if not command:
            self.write_headers_and_msg(status, REST_BAD_REQUEST, "text", "Missing 'command' attribute in header")
        else:
            cmd_words = utils_data.str_to_list(command, 3)

            if cmd_words[0] != "body" and not self.is_correct_method(status, http_method, cmd_words):
                # when command sas "body", one or mode anylog commands are passed in the message body

                ret_val = self.error_wrong_method(status, http_method, command)

            else:
                
                # Update commands_list with a list of commands (main command and commands placed in the msg body)
                commands_list = []
                into_output = get_value_from_headers(self.al_headers, "into")  # Push the output as HTML
                ret_val, with_wait, content_type, is_select, is_stream, file_data = self.prepare_commands(status, command, cmd_words, commands_list, into_output)

                if not ret_val:
                    if with_wait:
                        if not into_output:     # If not as HTML - HTML is organized as a single write to the caller
                            # Send the headers first
                            self.send_reply_headers(status, REST_OK, "", True, content_type, 0, True, None)

                    buff_size = int(params.get_param("io_buff_size"))
                    io_buff = bytearray(buff_size)


                    ret_val = self.execute_al_commands(status, io_buff, commands_list, into_output, file_data)


                j_handle = status.get_active_job_handle()  # Need to be done after the execution of the commands
                if ret_val != process_status.SUCCESS and ret_val < process_status.NON_ERROR_RET_VALUE:
                    # Operator returned an error

                    err_msg = j_handle.get_operator_error_txt()         # Error returned by operators
                    self.error_failed_process(status, with_wait, ret_val, http_method, into_output, command, err_msg)


                elif not with_wait and is_select:
                    # local query on this node.
                    # send_reply_headers is within the called method

                    job_id = status.get_job_id()
                    j_instance = job_scheduler.get_job(job_id)
                    nodes_count = j_instance.get_nodes_participating()
                    nodes_replied = j_instance.get_nodes_replied()


                    write_ret_value = self.local_table_query(status, j_handle, with_wait, nodes_count, nodes_replied)

                    j_instance.set_not_active()

                elif with_wait and is_select:
                    # This is a select command that was executed against other network nodes

                    job_id = status.get_job_id()
                    if (job_id != process_status.JOB_INSTANCE_NOT_USED):
                        # job instance was used
                        j_instance = job_scheduler.get_job(job_id)
                        # Mutex such that the job instance is not reused by a different thread in the query process
                        j_instance.data_mutex_aquire(status, 'W')
                        if j_instance.is_job_active() and j_instance.get_unique_job_id() == status.get_unique_job_id():
                            if not j_instance.is_pass_through():
                                # Otherwise the data was written to the caller
                                nodes_count = j_instance.get_nodes_participating()
                                nodes_replied = j_instance.get_nodes_replied()
                                write_ret_value = self.local_table_query(status, j_handle, with_wait, nodes_count, nodes_replied)
                                if into_output and not write_ret_value:
                                    # Write the HTML including header and data
                                    ret_val = self.write_headers_and_msg(status, REST_OK, content_type, j_handle.get_output_buff())

                            if j_handle.is_subset():
                                # Reply with partial nodes

                                if not j_handle.is_query_completed():
                                    member_cmd.query_summary(status, io_buff, j_instance, self.wfile)

                                # provide summary of reply
                                error_list = j_instance.get_nodes_error_list()
                                if error_list:
                                    # not all nodes replied
                                    # Include: IP + Port + Partition + RetCode + Text
                                    reply = utils_print.output_nested_lists(error_list, "\r\nReply errors:", ["Node IP", "Port", "Par", "Ret-Code", "Error Message"], True)
                                    write_ret_value = utils_io.write_to_stream(status, self.wfile, reply, True, True)
                        j_instance.data_mutex_release(status, 'W')

                        j_instance.set_not_active()
                elif with_wait and not is_select:
                    # This is a print message returned from multiple nodes

                    job_id = status.get_job_id()
                    if (job_id != process_status.JOB_INSTANCE_NOT_USED):
                        # job instance was used
                        j_instance = job_scheduler.get_job(job_id)
                        # Mutex such that the job instance is not reused by a different thread in the query process
                        j_instance.data_mutex_aquire(status, 'W')
                        if j_instance.is_job_active() and j_instance.get_unique_job_id() == status.get_unique_job_id():
                            result_set = j_instance.get_nodes_print_message()
                            # raw_requestline represents ? in command:
                            # http://10.0.0.78:7849/?User-Agent=AnyLog/1.23?destination=10.0.0.78:7848?subset=true?timeout=2?command=get status
                            transfer_encoding = False if "url" in self.al_headers else True    # self.al_headers["url"] is set in header_from_raw_requestline()
                            write_ret_value = utils_io.write_to_stream(status, self.wfile, result_set, transfer_encoding, True)
                        j_instance.data_mutex_release(status, 'W')

                elif is_stream:
                    # stream data to an app or browser
                    stream_file = j_handle.get_stream_file()
                    ret_val = self.deliver_stream_file(status, stream_file, content_type)

                else:
                    # result was placed on the job_handle
                    result_set = j_handle.get_result_set()
                    if isinstance(result_set, list):
                        result_set = "\r\n".join([str(item) for item in result_set])
                    content_type = get_result_set_type(result_set)  # returns 'text/json' or "text"

                    if not with_wait:
                        if result_set:
                            ret_val = self.write_headers_and_msg(status, REST_OK, content_type, result_set )
                        else:
                            is_chunked = True if http_method == "get" else False
                            self.send_reply_headers(status, REST_OK, "", True,content_type, 0, is_chunked, None)

                    elif result_set:
                        write_ret_value = utils_io.write_to_stream(status, self.wfile, result_set, True, True)


        return ret_val

    # =======================================================================================================================
    # Update commands_list with a list of commands:
    # 1) the main command in the header
    # 2) Include secondary commands from the body
    # =======================================================================================================================
    def prepare_commands(self, status, command, rest_cmd_words, commands_list, into_output):
        '''
        status - status object
        command - the command from the header
        rest_cmd_words - split() over command
        command_list - a list to be updated with all commands
        into_output - for example into htm - it disables pass_through
        '''
        
        with_wait = False  # No wait for a reply from a different node
        is_select = False
        is_stream = False
        content_type = 'text/json'
        file_data = None  # File via a call like: curl -X POST -H "command: file store where dest = !prep_dir/file2.txt" -F "file=@testdata.txt" http://10.0.0.78:7849
        is_binary_data = get_value_from_headers(self.al_headers, "Content-Type") == "application/octet-stream"

        ret_val, run_client = self.get_run_client()
        if not ret_val:

            ret_val, msg_body, err_msg = self.get_msg_body(status)
            if not ret_val:

                if rest_cmd_words[0] == "body":
                    # The command is passed in the message body. The example is with the command: set scrippt [file name] [script data]
                    command = msg_body
                    msg_body = None
                    cmd_words = utils_data.str_to_list(command, 3)
                else:
                    cmd_words = rest_cmd_words


                is_select = False
                if cmd_words[0] == "sql":
                    cmd_lower = command[4:].lower()
                    index = cmd_lower.find("select ", 4)
                    if index > 0:
                        char_before = cmd_lower[index - 1]
                        if char_before == ' ' or char_before == '"':
                            is_select = True
                            # A select stmt - find output format to determine content-type
                            out_format = utils_data.find_next_word(cmd_lower, 4, index, ["format", "="])
                            if out_format == "table":
                                content_type = "text"
                elif len(cmd_words) >= 3:
                    if utils_data.test_words(cmd_words, 0, ["file", "retrieve", "where"]):
                        # If streaming - Need to deliver headers first:
                        index = command.find(" stream", 19)
                        if index > -1:
                            # Test if command includes stream = true
                            stream_text = command[index + 1:].replace(" ","").lower()       # remove spaces
                            if stream_text[:11] == "stream=true":
                                content_type = "video/mp4"
                                is_stream = True
                    elif cmd_words[0] == "file" and (cmd_words[1] == "store" or cmd_words[1] == "to"):
                        # The file can be provided in 2 ways:
                        # 1) By identifying a source file: 'command': f'file store where dbms = {dbms} and table = {table} and source = {filename}'
                        # 2) by a buffer in the message body
                        if msg_body:
                            if is_binary_data:
                                file_data = msg_body        # Transfer binary data as is
                            else:
                                # If with msg body byu not binary
                                # The path is provided from the msg body
                                ret_val, content_type, file_data = get_user_file_data(status, msg_body)   # the message body is set on the status object as it includes the file to write
                                msg_body = None         # Message was pushed to the status object

                # determine if wait is needed
                if run_client:
                    # For SQL command
                    if command[:5] != "file ":
                        # No wait for file copy
                        # if data goes to HTML, it needs to go into a file to be pulled to one HTML doc.
                        with_wait = True  # Place thread on wait for reply

                # Execute the command (or commands in message body)
                if msg_body and not is_binary_data:
                    # These are assignments of values or pre=processed commands
                    self.get_msg_body_cmds(status, commands_list, msg_body)

                commands_list.append((run_client + command, with_wait))  # Add a flag if needed to wait for a reply
            
        return [ret_val, with_wait, content_type, is_select, is_stream, file_data]

    # =======================================================================================================================
    # Get the get_run_client info:
    # IP, Port List
    # Blockchain command
    # subset + timeout instructions: subset=true, timeout=10
    # =======================================================================================================================
    def get_run_client(self):

        destination = get_value_from_headers(self.al_headers, "destination")

        if not destination or destination == "local":
            run_client = ""             # Not a network call - apply on the query node
            ret_val = process_status.SUCCESS
        else:
            run_client = "run client ("
            if destination != "network":
                run_client += destination

            subset = get_value_from_headers(self.al_headers, "subset")  # A bool value that determines if partial results can be returned (subset = True)
            if subset:
                if run_client[-1] == '(':
                    run_client += f"subset={str(subset).lower()}"
                else:
                    run_client += f" ,subset={str(subset).lower()}"

            ret_val, sec_timeout = self.get_timeout()  # Timeout is based on the default defined in - run rest server command, or, provided in the header
            if not ret_val:
                if sec_timeout:
                    # max timeout in seconds for execution completion
                    if run_client[-1] == '(':
                        run_client += f"timeout={sec_timeout}"
                    else:
                        run_client += f" ,timeout={sec_timeout}"
            run_client += ')'


        return [ret_val, run_client]

    # =======================================================================================================================
    # Get the commands and assignments in the REST Body Part
    # Every line is a command or if a line starts with <, find the end with >
    # =======================================================================================================================
    def get_msg_body_cmds(self, status, commands_list, msg_body):

        offset = 0
        commands_string = msg_body.replace('\r',"").replace('\t',"")
        body_lngth = len(commands_string) - 1
        while offset < body_lngth:
            if commands_string[offset] == '<':
                end_char = '>'
                offset += 1
            else:
                end_char = '\n'

            index = commands_string.find(end_char, offset)

            if index == -1:
                # Take all the rest and exit
                command = commands_string[offset:]
                commands_list.append(command)
                break
            else:
                command = commands_string[offset:index]
                commands_list.append((command, False))      # The false indicates no wait for reply
                offset = index + 1

    # =======================================================================================================================
    # Execute the commands on the command_list and the commandon the header
    # =======================================================================================================================
    def execute_al_commands(self, status, io_buff, commands_list, into_output, file_data):
        '''
        into_output - when output is organized as HTML
        '''

        ret_val = process_status.SUCCESS

        for index, entry in enumerate (commands_list):
            command = entry[0]
            with_reply = entry[1]       # Indicate if the thread needs to wait for a reply (i.e. SQL query)
            if with_reply:
                ret_val = native_api.exec_al_cmd(status, command, self.wfile, into_output, 20)   # Timeout for a list of commands is the default
            else:
                ret_val = native_api.exec_no_wait(status, command, io_buff, file_data, self.wfile)
            if ret_val:
                if index != (len(commands_list) - 1):
                    # the command that failed is in the message body (not the header)
                    status.add_keep_error("Error with command #%u in the message body: '%s'" % (index +1, command))
                break

        return ret_val

    # =======================================================================================================================
    # Get the timeout in case of a wait
    # Timeout is based on the default defined in - run rest server command, or, provided in the header
    # =======================================================================================================================
    def get_timeout(self):

        ret_val = process_status.SUCCESS
        sec_timeout = 0
        timeout = get_value_from_headers(self.al_headers, 'timeout')  # overwrite the default timeout
        if timeout:
            # timeout can be a number or number + unit
            time_info = timeout.split(' ')
            if len(time_info) >= 1:  # can be a number or a number + a unit
                counter = time_info[0].strip()
                try:
                    sec_timeout = int(counter)
                except:
                    sec_timeout = 0
                    ret_val = process_status.Error_timeout_val
                else:
                    if len(time_info) > 1:
                        # get the units type
                        units = time_info[1].strip()
                        sec_timeout = utils_data.time_to_seconds(counter, units)
                        if not sec_timeout:
                            ret_val = process_status.Error_timeout_val
        else:
            sec_timeout = http_server_info.timeout

        return [ret_val, sec_timeout]

    # =======================================================================================================================
    # STream a file to a browser or APP
    # =======================================================================================================================
    def deliver_stream_file(self, status, stream_file, content_type):

        file_size = utils_io.get_file_size(status, stream_file)
        if file_size > 0:
            self.send_reply_headers(status, REST_OK, "", True, content_type, file_size, False, "bytes")

            # Stream and delete the file
            ret_val = utils_io.stream_to_browser(status, stream_file, self.wfile, True)
        else:
            status.add_error(f"Error with file that is streamed to the app: {stream_file}")
            ret_val = process_status.File_open_failed

        return ret_val
    # =======================================================================================================================
    # PUT method def
    # =======================================================================================================================
    def do_PUT(self):

        if with_profiler_:
            profiler.manage("put")     # Stop, Start and reset the profiler

        status = process_status.ProcessStat()
        self.msg_body = None
        self.al_headers = set_al_headers(self.headers._headers)
        user_agent, version, user_id = self.get_client("put")

        if self.authenticate_request(status, "PUT", "Data"):


            try:
                ret_val = self.al_put(status)  # Updated version
            except:
                errno, value, stack_trace = sys.exc_info()[:3]
                self.handle_exception(status, "PUT", errno, value, stack_trace)
                ret_val = process_status.REST_call_err

        else:
            ret_val = process_status.Failed_message_authentication

        update_stats("PUT", user_agent, ret_val, self.client_address)

        self.log_streaming(ret_val)

        if with_profiler_:
            profiler.stop("put")     # Force Profiler because thread goes to sleep

    # =======================================================================================================================
    # AnyLog PUT method def - write data
    # =======================================================================================================================
    def al_put(self, status):


        content_type = 'text/json'

        headers_info = request_info()

        headers_info.rest_status = REST_API_OK

        ret_val, msg_body, err_msg = self.get_msg_body(status)
        if not ret_val:
            ret_val, msg_data, row_counter = utils_json.make_json_rows(status, msg_body)

        if ret_val:
            if not err_msg:
                if not msg_body:
                    err_msg = "Failed to process JSON data: Message Body is empty"
                else:
                    err_msg = "Failed to process JSON data"
                status.add_error(err_msg)
            headers_info.rest_status = REST_API_MISSING_BODY
            content_type = 'text'
        elif not msg_data:
            err_msg = "REST PUT command without new data"
            content_type = 'text'
            status.add_error(err_msg)
            headers_info.rest_status = REST_API_MISSING_BODY
            ret_val = process_status.Failed_to_analyze_json
        else:
            prep_dir = params.get_value_if_available("!prep_dir")  # dir where the data will be written
            watch_dir = params.get_value_if_available("!watch_dir")  # dir where the data will be written
            err_dir = params.get_value_if_available("!err_dir")

            ret_val = test_work_dir(status, prep_dir, watch_dir, err_dir)
            if not ret_val:
                ret_val, dbms_name, table_name, source, instructions, file_type = put_params_from_header(status,
                                                                                                         self.al_headers)
                if not ret_val:

                    mode = get_put_processing_method(self.al_headers)

                    ret_val, hash_value = streaming_data.add_data(status, mode, row_counter, prep_dir, watch_dir, err_dir, dbms_name, table_name, source, instructions, file_type, msg_data)


        # determine returned message
        if ret_val:
            response_code = REST_INTERNAL_SERVER_ERROR

            echo_msg = process_status.get_status_text(ret_val)      # The value to return with the headers
            if not err_msg:
                err_msg = status.get_saved_error()  # try detailed message
                if not err_msg:
                    err_msg = "{\"AnyLog.error on REST server\":\"%s\"}" % (echo_msg) # get generic message
            result_set = err_msg
        else:
            result_set = "{\"AnyLog.status\":\"Success\", \"AnyLog.hash\": \"%s\" }" % hash_value   # Return the hash of the added file
            echo_msg = None
            response_code = REST_OK


        encoded_result_set = result_set.encode(encoding='UTF-8')
        ret_val = self.send_reply_headers(status, response_code, echo_msg, True, content_type, len(encoded_result_set), False, None)
        if not ret_val:
            write_ret_value = utils_io.write_encoded_to_stream(status, self.wfile, encoded_result_set)

      # =======================================================================================================================
    # DELETE
    # =======================================================================================================================
    def do_DELETE(self):

        status = process_status.ProcessStat()
        self.msg_body = None
        self.al_headers = set_al_headers(self.headers._headers)

        self.send_reply_headers(status, REST_NOT_IMPLEMENTED, "DELETE NOT SUPPORTED", True, 'text/json', 0, True, None)

    # =======================================================================================================================
    # Get the message body
    # =======================================================================================================================
    def get_msg_body(self, status):

        self.msg_body = None
        err_msg = None
        ret_val = process_status.SUCCESS

        msg_body_length = get_value_from_headers(self.al_headers, 'Content-Length') # Get the message lengthe from the headers

        if msg_body_length and msg_body_length.isnumeric():
            msg_info = utils_io.read_from_stream(status, self.rfile, int(msg_body_length))


            if len(msg_info) == 2:
                # The message body is a list with 2 entries:
                # msg_info[0] is the Content length
                # msg_info[1] is the content
                if get_value_from_headers(self.al_headers, "Content-Type") == "application/octet-stream":
                    # Specify binary content  - keep unchanged
                    self.msg_body = msg_info[1]
                else:
                    try:
                        self.msg_body = msg_info[1].decode('iso-8859-1')    # Save the decoded body - being used in case of an error to provide error info
                    except:
                        self_command = (self.command + "\r\n") if hasattr(self, 'command') else ""
                        err_msg = f"Failed to decode message body:{self_command}\r\n'Content-Length': {msg_body_length}\r\nmsg_info[0]: {msg_info[0]}\r\nmsg_info[1]: {msg_info[1]}"
                        status.add_error(err_msg)
                        utils_print.output_box(err_msg)
                        ret_val = process_status.HTTP_failed_to_decode
                    else:
                        if len(self.msg_body) and (self.msg_body[0] == ' ' or self.msg_body[-1] == ' '):
                            self.msg_body = self.msg_body.strip()


        return [ret_val, self.msg_body, err_msg]

    # =======================================================================================================================
    # Send headers and Query the local database
    # =======================================================================================================================
    def local_table_query(self, status, j_handle, with_wait, nodes_count, nodes_replied):

        logical_dbms, table_name, conditions, sql_command = j_handle.get_local_query_params()

        if not with_wait:
            # If with wait is True - headers already delivered (before the call to execute the query - to allow job recivers to deliver data)
            if interpreter.test_one_value(conditions, "format", "table"):
                content_type = 'text'
            else:
                content_type = 'text/json'

            ret_val = self.send_reply_headers(status, REST_OK, "", True, content_type, 0, True, None)
        else:
            ret_val = process_status.SUCCESS
            if not logical_dbms:
                # This is the case of subset and not all nodes replied
                logical_dbms = "system_query"
                sql_command = j_handle.select_parsed.local_query
                table_name = j_handle.select_parsed.get_local_table()
                conditions["dest"] = ["rest"]


        if not ret_val:
            try:
                ret_val = member_cmd.query_local_dbms(status, None, logical_dbms, table_name, conditions, sql_command, nodes_count, nodes_replied)
            except:
                ret_val = process_status.Failed_query_process

        return ret_val


# =======================================================================================================================
# Get value from REST headers
# Headers were reorganized as a dictionary is set_al_headers()
# Given a key. return the value
# =======================================================================================================================
def get_value_from_headers(al_headers, test_key, data_type = None):
    key = test_key.lower()
    if key in al_headers:
        value = al_headers[key]
        if data_type == "bool":
            if value.lower().strip() == "true":
                value = True
            else:
                value = False
    else:
        if data_type == "bool":
            value = False
        else:
            value = None

    return value
# =======================================================================================================================
# Debug path
# =======================================================================================================================
def test_code(step, command, debug_rest, io_stream, value, data):
    debug_rest.step = step

    if not debug_rest.is_failed:

        stream_stat = utils_io.test_stream_socket(io_stream)
        message = "Step: <%u> Stat: <%s> Val: <%s> * " % (step, str(stream_stat), str(value))
        debug_rest.code_path += message

        if not stream_stat:
            # print only in first time
            debug_rest.is_failed = True
            message = debug_rest.code_path + " ---> Details: <" + command + ">"
            utils_print.output(message, True)
            if data:
                if not isinstance(data, str):
                    message = "\nData is not str with length: %u - %s" % (len(data), str(data))
                else:
                    message = "\nData type is: %s with value %s" % (type(data), str(data))
                utils_print.output(message, True)


# =======================================================================================================================
# Update REST statistics
# =======================================================================================================================
def update_stats(do_cmd, user_agent, ret_val, client_address):
    global rest_stat_

    if not user_agent:
        user_agent = "AnyLog-V1"

    if not user_agent in rest_stat_:
        rest_stat_[user_agent] = {}

    agents = rest_stat_[user_agent]             # The client products interacting with the network

    if not do_cmd in agents:

        agents[do_cmd] = {
            "Processed" : 0,
            "Errors"    : 0,
            "Last Error" : 0,
            "First Call" : get_current_time("%Y-%m-%d %H:%M:%S"),
            "Last Call"  : "",
            "Last Client" : ""
        }

    rest_cmd_stat = agents[do_cmd]

    rest_cmd_stat["Processed"] += 1
    rest_cmd_stat["Last Call"] = get_current_time("%Y-%m-%d %H:%M:%S")
    if ret_val:
        # some error
        rest_cmd_stat["Errors"] += 1
        rest_cmd_stat["Last Error"] = ret_val   # Keep last errpr value
    if client_address and isinstance(client_address,tuple) and len(client_address) == 2:
        rest_cmd_stat["Last Client"] = str(client_address[0]) + ':' + str(client_address[1])

# ------------------------------------------------------------------
# Return info on the REST calls statisfying "get rest" command
# ------------------------------------------------------------------
def show_info():

    global rest_stat_

    info_table = []
    for user_agent, rest_info in rest_stat_.items():
        caller = user_agent
        for do_cmd, info in rest_info.items():
            last_error = info["Last Error"]
            if last_error:
                text_error = process_status.get_status_text(last_error)
            else:
                text_error = ""
            info_table.append((caller, do_cmd, info["Processed"], info["Errors"], text_error, info["First Call"], info["Last Call"], info["Last Client"]))
            caller = ""

    title = ["Caller", "Call", "Processed", "Errors", "Last Error", "First Call", "Last Call", "Last Caller"]
    info_string = utils_print.output_nested_lists(info_table, "\r\nStatistics", title, True, "")

    return info_string

# ------------------------------------------------------------------
# Return info on the workers pool
# returns info when calling - get rest pool
# ------------------------------------------------------------------
def get_threads_obj():

    global http_server_info
    return http_server_info.workers_pool
# ------------------------------------------------------------------
# Return info on http_server_info
# returns to the command: get rest server info
# ------------------------------------------------------------------
def get_server_info(status, io_buff_in, cmd_words, trace):

    global http_server_info

    info_list = []
    for attr in dir(http_server_info):
        if attr[:2] != "__":
            # Exclude private methods
            attr_val = str( getattr(http_server_info,attr))
            if not len(attr_val) or attr_val[0] != "<":
                # Exclude methods
                info_list.append((attr, " " + attr_val + " "))

    server_info = utils_print.output_nested_lists(info_list, "", ["Attribute", "Value"], True)

    return [process_status.SUCCESS, server_info]

# ------------------------------------------------------------------
# Set timeout - set the timeout for a REST call. 0 value sets no timeout
# Satisfying command: set rest timeout 2 minutes
# ------------------------------------------------------------------
def set_rest_timeout(status, io_buff_in, cmd_words, trace):

    words_count = len(cmd_words)
    ret_val = process_status.SUCCESS
    if words_count == 4:
        # units not provided - use seconds
        counter = cmd_words[3]
        units = "seconds"
    elif words_count == 5:
        # units provided
        counter = cmd_words[3]
        units = cmd_words[4]
    else:
        ret_val = process_status.ERR_command_struct

    if not ret_val:
        timeout = utils_data.time_to_seconds(counter, units)
        if timeout == None:
            ret_val = process_status.ERR_command_struct
        else:
            http_server_info.timeout = timeout

    return ret_val

# ------------------------------------------------------------------
# Return info on the TCP Server in command - show processes
# ------------------------------------------------------------------
def get_info( status = None ):
    global http_server_info

    info_str = net_utils.get_connection_info(1)
    if http_server_info.workers_pool:
        info_str += ", Threads Pool: %u" % http_server_info.workers_pool.get_number_of_threds()
    info_str += ", Timeout: %u, SSL: %s" % (http_server_info.timeout, http_server_info.is_ssl)

    return info_str

# =======================================================================================================================
# Invoke the server - Example: https://www.techbeamers.com/python-tutorial-write-multithreaded-python-server/
# =======================================================================================================================
def rest_server(params: params, host: str, port: int, is_bind:bool, conditions):
    global declared_ip_port

    # init variables

    http_server_info.is_ssl = interpreter.get_one_value_or_default(conditions, "ssl", False)
    if http_server_info.is_ssl:
        http_server_info.connection = "https://" + host + ":" + str(port)
    else:
        http_server_info.connection = "http://" + host + ":" + str(port)

    http_server_info.is_bind = is_bind

    timeout = interpreter.get_one_value_or_default(conditions, "timeout", 20)

    http_server_info.timeout = timeout  # max wait for a response in seconds

    http_server_info.buff_size = int(params.get_param("io_buff_size"))

    http_server_info.workers_count = interpreter.get_one_value_or_default(conditions, "threads", 5)

    http_server_info.workers_pool = utils_threads.WorkersPool("REST", http_server_info.workers_count, False)

    declared_ip_port = host + ":" + str(port)

    if is_bind:
        bind_host = host
    else:
        bind_host = ""

    try:
        # Server Initialization
        http_server_info.server = ThreadedHTTPServer((bind_host, port), ChunkedHTTPRequestHandler)
    except:
        errno, value = sys.exc_info()[:2]
        net_utils.remove_connection(1)      # Remove the REST connection IP/Port
        process_log.add_and_print("Error", "Failed to open a REST server on %s:%u [Error: %s] [Value: %s]" % (host, port, str(errno), str(value)))
        return

    if http_server_info.is_ssl:
        if with_open_ssl_:
            # Adding SSL
            ca_org = interpreter.get_one_value(conditions, "ca_org")      # Organization to locate the server certificate
            server_org = interpreter.get_one_value(conditions, "server_org")  # Organization to locate the server certificate
            ret_val = connect_ssl(ca_org, server_org)
            if not ret_val:
                process_log.add_and_print("Error", "Failed to start REST server with SSL due to missing key files")
        else:
            message = "Failed to load pyOpenSSL library"
            utils_print.output_box(message)
            process_log.add(message)
            ret_val = False
    else:
        ret_val = True

    if ret_val:
        process_log.add("Event", "REST server initiated on %s:%u" % (host, port))

        http_server_info.server.serve_forever()

    # -------------------------------
    # on exit close socket
    # -------------------------------
    try:
        http_server_info.server.socket.close()
    except:
        pass

    net_utils.remove_connection(1)      # Remove the REST connection IP/Port

    http_server_info.workers_pool.exit()

    http_server_info.workers_pool = None

    process_log.add_and_print("event", "Terminating REST Server %s:%u" % (host, port))


# =======================================================================================================================
# Create a connection using SSL
# Example: run rest server !ip 2149 where ssl = true
# =======================================================================================================================
def connect_ssl(ca_org, server_org):

    file_key = server_org.strip().lower().replace(" ", "-").replace("'","")  # replace spaces with '-' sign

    directory_name = params.get_value_if_available("!pem_dir") + params.get_path_separator()

    server_public_key_file = directory_name + "server-%s-public-key.crt" % file_key        # The signed certificate request
    server_private_key_file = directory_name + "server-%s-private-key.key" % file_key      # The Private Key issued to the server by the CA


    # Get the file name of the Certificate Authority public Key
    file_key = ca_org.strip().lower().replace(" ", "-").replace("'","")   # replace spaces with '-' sign
    ca_public_key_file = directory_name + 'ca-%s-public-key.crt' % file_key

    # Test that the files exists
    if not utils_io.is_path_exists(server_public_key_file):
        missing_file = server_public_key_file
        ret_val = False
    elif not utils_io.is_path_exists(server_private_key_file):
        missing_file = server_private_key_file
        ret_val = False
    elif not utils_io.is_path_exists(ca_public_key_file):
        missing_file = ca_public_key_file
        ret_val = False
    else:
        ret_val = True

    if not ret_val:
        process_log.add_and_print("Error", "Missing file for ssl process: %s" % missing_file)
    else:

        try:
            # params are explained here - https://www.kite.com/python/docs/ssl.wrap_socket

            try:
                _create_unverified_https_context = ssl._create_unverified_context
            except AttributeError:
                # Legacy Python that doesn't verify HTTPS certificates by default
                pass
            else:
                # Handle target environment that doesn't support HTTPS verification
                ssl._create_default_https_context = _create_unverified_https_context

            context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            context.verify_mode = ssl.CERT_REQUIRED
            context.check_hostname = False

            context.load_cert_chain(certfile=server_public_key_file, keyfile=server_private_key_file) #### This is the certificate and private key of the server
            context.load_verify_locations(cafile=ca_public_key_file) ### This is the certificate (public key) of the CA

            ssl_socket = context.wrap_socket(http_server_info.server.socket, server_side=True)
        except:
            error_type, error_value = sys.exc_info()[:2]
            process_log.add_and_print("Error", "SSL socket error - type: '%s' value: '%s " % (str(error_type), str(error_value)))
            ret_val = False
        else:
            # Close original socket
            try:
                http_server_info.server.socket.close()
            except:
                pass

            http_server_info.ca_public_key = ca_public_key_file  # The CA public key file
            http_server_info.node_cr = server_public_key_file  # The node Certificate Request file
            http_server_info.node_private_key = server_private_key_file  # The node private key file

            # Set new socket
            ssl_socket.verify_mode = ssl.CERT_REQUIRED      # ssl.CERT_OPTIONAL Or ssl.CERT_REQUIRED

            http_server_info.server.socket = ssl_socket


    return ret_val

# =======================================================================================================================
# Review the headers from the Rest message and return a ret code and the message
# =======================================================================================================================
def get_command_from_rest_header(rest_headers, headers_info):
    if not isinstance(rest_headers, list):
        headers_info.rest_status = REST_API_BAD_REQUEST
    else:
        headers_info.rest_status = REST_API_OK

        for entry in rest_headers:
            if isinstance(entry, tuple) and len(entry) == 2:
                key = entry[0]
                if key.lower() == "type":
                    if headers_info.command_type != "":
                        headers_info.rest_status = REST_API_BAD_REQUEST  # multiple identical keys
                        headers_info.detailed_error += "<Duplicate REST 'type' in header>"
                        break
                    headers_info.command_type = entry[1].lower()  # sql or "info dbms" or "onfo table" etc
                elif key.lower() == "details":
                    if headers_info.details != "":
                        headers_info.rest_status = REST_API_BAD_REQUEST  # multiple identical keys
                        headers_info.detailed_error += "<Duplicate REST 'details' in header>"
                        break
                    headers_info.details = utils_data.to_lower_ignore_quoted_substr(entry[1])
                elif key.lower() == "dbms":
                    if headers_info.dbms_name != "":
                        headers_info.detailed_error += "<Duplicate REST 'dbms' in header>"
                        headers_info.rest_status = REST_API_BAD_REQUEST  # multiple identical keys
                        break
                    headers_info.dbms_name = entry[1].lower()
                elif key.lower() == "table":
                    if headers_info.table_name != "":
                        headers_info.detailed_error += "<Duplicate REST 'table' in header>"
                        headers_info.rest_status = REST_API_BAD_REQUEST  # multiple identical keys
                        break
                    headers_info.table_name = entry[1].lower()
                elif key.lower() == "servers":
                    # if headers_info.servers != "":
                    #    headers_info.rest_status = REST_API_BAD_REQUEST  # multiple identical keys
                    #    break
                    headers_info.servers = entry[1].lower()
                elif key.lower() == "instructions":
                    # instrction can include:
                    # include - include different databases/tables in the sql query
                    # table - # an output table for the select returned data
                    # drop - # a flag to indicate if new data is added to the existing data in the output table
                    # example: table: looker drop:false include: dbms_name1.table_name1, dbms_name2.table_name2
                    if headers_info.instructions:
                        headers_info.instructions += " " + entry[1].lower()
                    else:
                        headers_info.instructions = entry[1].lower()

        if headers_info.rest_status == REST_API_OK:
            headers_info.command_type = utils_data.make_single_space_string(headers_info.command_type)
            if headers_info.command_type == "sql":
                test_sql_select(headers_info)
            elif headers_info.command_type == "json file" or headers_info.command_type == "sql file" or headers_info.command_type == "json to sql file":
                test_input_command(headers_info)
            elif headers_info.command_type == "info":
                test_info_command(headers_info)
            elif headers_info.command_type == "exit":
                headers_info.rest_status = REST_API_EXIT
            else:
                headers_info.detailed_error += "<Type in REST header not recognized: '%s'>" % headers_info.command_type
                headers_info.api_command_flag = True  # this command needs to be resolved on the API - NOT AnyLog request
                return

        if headers_info.rest_status == REST_API_OK:
            if headers_info.command == "":
                headers_info.detailed_error += "<Missing 'command' after processing REST header>"
                headers_info.rest_status = REST_API_BAD_REQUEST
            if headers_info.command_type == "":
                headers_info.detailed_error += "<Missing 'type' after processing REST header>"
                headers_info.rest_status = REST_API_BAD_REQUEST



# =======================================================================================================================
# test info command
# =======================================================================================================================
def test_input_command(headers_info):
    if not len(headers_info.dbms_name) or not len(headers_info.table_name):
        headers_info.command = ""
        headers_info.rest_status = REST_API_BAD_REQUEST
        if not len(headers_info.dbms_name):
            headers_info.detailed_error += "<Missing dbms name in REST header>"
        if not len(headers_info.table_name):
            headers_info.detailed_error += "<Missing table name in REST header>"
    elif headers_info.details == "":
        headers_info.rest_status = REST_API_WRONG_DETAILS
        headers_info.detailed_error += "<Missing details in REST header>"
    else:
        headers_info.command = "{\"type\":\"input\",\"dbms\":\"" + headers_info.dbms_name + "\",\"table\":\"" + headers_info.table_name + "\"}"


# =======================================================================================================================
# test info command
# =======================================================================================================================
def test_info_command(headers_info):
    if headers_info.details.lower().startswith("get ") or headers_info.details.lower().startswith(
            "job ") or headers_info.details.lower().startswith("show "):
        headers_info.command = headers_info.details
        cmd_words = headers_info.details.lower().split()
        words_count = len(cmd_words)
        if words_count >= 2 and cmd_words[0] == "show" and cmd_words[1] == "peer":
            # resolve:
            # 1) Show peer
            # 2) show peer commands on/off
            if words_count == 2:
                headers_info.api_command_flag = True  # this comand needs to be resolved on the API
                headers_info.api_command = "show peer"
            elif words_count == 4 and cmd_words[2] == "command":
                if cmd_words[3] == "on":
                    headers_info.api_command_flag = True  # this comand needs to be resolved on the API
                    http_server_info.trace = True  # print executed commands
                    headers_info.api_command = "show peer command"
                elif cmd_words[3] == "off":
                    headers_info.api_command_flag = True  # this comand needs to be resolved on the API
                    http_server_info.trace = False
                    headers_info.api_command = "show peer command"
                else:
                    headers_info.detailed_error += "<Error in details in REST header>"
                    headers_info.rest_status = REST_API_WRONG_DETAILS
            else:
                headers_info.detailed_error += "<Error in details in REST header>"
                headers_info.rest_status = REST_API_WRONG_DETAILS

    elif headers_info.dbms_name != "" and headers_info.details.lower().startswith("info "):
        headers_info.command = "\"" + headers_info.details + "\""
        headers_info.send_to_operators = True
    elif headers_info.details.startswith("blockchain "):
        if headers_info.details[11:].lstrip().startswith("get "):
            # search on the JSON file of the local node
            headers_info.command = headers_info.details
            headers_info.send_to_operators = False
        elif headers_info.details[11:].strip() == "test":
            # test the local blockchain structure
            headers_info.command = headers_info.details
            headers_info.send_to_operators = False
    elif headers_info.details.startswith("test "):
        # Test connection, test table
        headers_info.command = headers_info.details
        headers_info.send_to_operators = False
    elif headers_info.details.startswith("mqtt publish"):
        # Test connection, test table
        headers_info.command = headers_info.details
        headers_info.send_to_operators = False
    else:
        headers_info.command = ""
        headers_info.detailed_error += "<Error in details in REST header>"
        headers_info.rest_status = REST_API_WRONG_DETAILS


# =======================================================================================================================
# test SQL select command
# =======================================================================================================================
def test_sql_select(headers_info):
    if headers_info.details != "" and headers_info.dbms_name != "":
        if not headers_info.details.lower().startswith("select "):
            headers_info.rest_status = REST_API_WRONG_DETAILS
            headers_info.detailed_error += "<Missing 'select' in REST header>"
        else:
            sql_instructions = "\"sql " + headers_info.dbms_name + " dest = rest "
            if headers_info.instructions != "":
                sql_instructions += (headers_info.instructions)

            headers_info.command = sql_instructions + " \" + \"" + headers_info.details.replace("\"", "'") + "\""
            headers_info.send_to_operators = True
            headers_info.is_select = True
    else:
        if headers_info.details == "":
            headers_info.detailed_error += "<Missing details in REST header>"
        if headers_info.dbms_name == "":
            headers_info.detailed_error += "<Missing dbms name in REST header>"
        headers_info.rest_status = REST_API_BAD_REQUEST


# =======================================================================================================================
# Signal the rest server to exit
# =======================================================================================================================
def signal_rest_server():

    try:
        http_server_info.server.shutdown()  # requests.request("GET", http_server_info.connection, headers=exit_header)
    except:
        pass  # did not connect to REST server


# =======================================================================================================================
# Get the REST API MESSAGE
# =======================================================================================================================
def get_rest_stat_msg(headers_info):
    message = rest_errors_msg[headers_info.rest_status]
    if headers_info.detailed_error != "":
        message += (",\"Error info\":\"" + headers_info.detailed_error + "\"}}")
    else:
        message += "}"
    return message

# =======================================================================================================================
# Organize the AnyLog needed header values in a dict for a fast lookup
# =======================================================================================================================
def set_al_headers(input_headers):
    header_dict = {}
    for entry in input_headers:
        if len(entry) == 2:
            header_dict[entry[0].lower().strip()] = entry[1].strip()

    return header_dict

# =======================================================================================================================
# Test the PREP DIR and WATCH DIR - test that dir exists and are writeable
# =======================================================================================================================
def test_work_dir(status, prep_dir, watch_dir, err_dir):
    if not prep_dir:
        ret_val = process_status.Missing_configuration
        status.keep_error("Missing definition for 'prep_dir'")
    else:
        ret_val = utils_io.test_dir_exists_and_writeable(status, prep_dir, True)
        if not ret_val:
            if not watch_dir:
                ret_val = process_status.Missing_configuration
                status.keep_error("Missing definition for 'watch_dir'")
            else:
                ret_val = utils_io.test_dir_exists_and_writeable(status, watch_dir, True)
                if not ret_val:
                    if not err_dir:
                        ret_val = process_status.Missing_configuration
                        status.keep_error("Missing definition for 'err_dir'")
                    else:
                        ret_val = utils_io.test_dir_exists_and_writeable(status, err_dir, True)

    return ret_val


# =======================================================================================================================
# Retrieve params from header to support the PUT command
# =======================================================================================================================
def put_params_from_header(status, al_headers):
    ret_val = process_status.SUCCESS

    table_name = ""
    source = "0"
    instructions = "0"
    file_type = "json"

    db_name = get_value_from_headers(al_headers, "dbms")
    if not db_name:
        ret_val = process_status.Missing_dbms_name
        status.keep_error("Missing 'dbms' name in REST PUT command")
    else:
        dbms_name = db_name.lower()
        tb_name = get_value_from_headers(al_headers, "table")
        if not tb_name:
            ret_val = process_status.ERR_table_name
            status.keep_error("Missing 'table' name in REST PUT command")
        else:
            table_name = tb_name.lower()
            source = get_value_from_headers(al_headers, "source")
            if not source:
                source = "0"
            instructions = get_value_from_headers(al_headers, "instructions")
            if not instructions:
                instructions = "0"

            file_type = get_value_from_headers(al_headers, "type")
            if not file_type:
                file_type = "json"

    return [ret_val, dbms_name, table_name, source, instructions, file_type]


# =======================================================================================================================
# Processing method is one of the 2:
# File - the file is transferred and moved to the watch directory (default)
# Streaming - data is accumulated and transferred using file size and time thresholds
# =======================================================================================================================
def get_put_processing_method(al_headers):
    mode = get_value_from_headers(al_headers, "mode")
    if not mode or mode.lower() != "streaming":
        mode = "file"  # default
    else:
        mode = "streaming"  # change to lower case
    return mode


# =======================================================================================================================
# Assign data with the user-agent header to treat the data posted as MQTT call
# =======================================================================================================================
def assign_user_agent_to_mqtt(user_agent, user_id, default_topic):
    '''
    user_agent - this user-agent in the header is transformed to publish data
    user_id is the id of the MQTT subscribed user
    '''

    global  user_agent_

    user_agent_[user_agent] = ("mqtt", user_id, default_topic)


# ------------------------------------------------------------------------
# set rest log on/off
# enables/disables a log that collects last REST calls
# ------------------------------------------------------------------------
def set_streaming_log(status, io_buff_in, cmd_words, trace):
    global http_server_info
    ret_val = process_status.SUCCESS

    if cmd_words[3] == "on":
        http_server_info.streaming_log = True
    elif  cmd_words[3] == "off":
        http_server_info.streaming_log = False
    else:
        ret_val = process_status.ERR_command_struct
    return ret_val

# ------------------------------------------------------------------------
# Return "text" or 'text/json' based on the content of the message
# ------------------------------------------------------------------------
def get_result_set_type(result_set):
    if result_set and (result_set[0] == '{' or result_set[0] == '['):
        content_type = 'text/json'
    else:
        content_type = 'text'
    return content_type

# ------------------------------------------------------------------------
# Get the info from a message body, for example when file is copied vie rest
# ------------------------------------------------------------------------
def get_user_file_data(status, msg_body):

    ret_val = process_status.Failed_to_parse_input_file
    content_type = None
    file_data = None

    if msg_body:
        # Initialize counters
        count = 0
        offsets = []

        # Find the 3rd and 4th occurrences of \r\n
        split_key = '\r\n' if msg_body[-2:] == "\r\n" else '\n'     # Captures windows or linux
        len_split_key = len(split_key)

        pos = 0
        while count < 4:
            pos = msg_body.find(split_key, pos)  # Find next occurrence, advance position
            if pos > 0:  # If found
                pos += 2
                offsets.append(pos)
                count += 1
            else:
                break

        if count == 4:
            # All entries up to the file content were found.
            # Get end of file position:
            data_offset_end = msg_body.rfind(split_key, 0, -1)     # the offset of the end of the file data in msg_bdy
            if data_offset_end > offsets[3]:
                offsets.append(pos)

                metadata = msg_body[offsets[0]:offsets[1]-len_split_key]

                content_type = msg_body[offsets[1]:offsets[2]-len_split_key]
                if content_type.startswith("Content-Type: "):
                    content_type = content_type[14:]
                data_offset_start = offsets[3]
                file_data = msg_body[data_offset_start:data_offset_end]
                ret_val = process_status.SUCCESS


    return [ret_val, content_type, file_data]
