"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

# import the JSON parser
import os
import requests
from requests import Request, Session

requests.packages.urllib3.disable_warnings()
from requests.exceptions import HTTPError
import base64

import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_output as utils_output
import edge_lake.generic.interpreter as interpreter

#                          Must     Add      Is
#                          exists   Counter  Unique
keywords_get = {"url": ("str", True, False, True),
                "user": ("str", False, False, True),
                "password": ("str", False, False, True),
                "type": ("str", False, False, True),
                "details": ("str", False, False, True),
                "command" : ("str", False, False, True),
                "destination" : ("str", False, False, True),
                "User-Agent" : ("str", False, False, True),
                "subset" : ("str", False, False, True),
                "validate" : ("str", False, False, True),       # Only validate the url page
                "Authorization"  : ("str", False, False, True), # Used for authentication.
                "Accept"  : ("str", False, False, True), # Specifies the response format.
                "Cache-Control"  : ("str", False, False, True), #  Controls caching behavior
                "X-Vault-Token"  : ("str", False, False, True), #  Specific to HashiCorp Vault.
                "Content-Type" : ("str", False, False, True), #  Usually not required for GET but sometimes used when sending query parameters in the body (though unconventional)
                }
#                          Must     Add      Is
#                          exists   Counter  Unique
keywords_put = {"url": ("str", True, False, True),
                "user": ("str", False, False, True),
                "password": ("str", False, False, True),
                "dbms": ("str", False, False, True),
                "table": ("str", False, False, True),
                "type": ("str", False, False, True),
                "mode": ("str", False, False, True),
                "body": ("str", False, False, True),
                }
#                          Must     Add      Is
#                          exists   Counter  Unique
keywords_post = {"url": ("str", True, False, True),
                "headers": ("str", False, False, True),
                "user": ("str", False, False, True),
                "password": ("str", False, False, True),
                "dbms": ("str", False, False, True),
                "table": ("str", False, False, True),
                "type": ("str", False, False, True),
                "mode": ("str", False, False, True),
                "body": ("str", False, False, True),
                }

#                          Must     Add      Is
#                          exists   Counter  Unique
keywords_delete={"url": ("str", True, False, True),
                "user": ("str", False, False, True),
                "password": ("str", False, False, True),
                }

# =======================================================================================================================
# Send DELETE requests
# example: rest delete where url=http://139.144.8.104:59880/api/v2/event/age/86400000000000
# =======================================================================================================================
def do_DELETE(status, cmd_words):
    global keywords_put
    # Get params from command line

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3, 0, keywords_delete, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return ret_val

    headers = {
        # 'cache-control': "no-cache",
    }

    url = ""
    user = ""
    password = ""
    body = ""

    for key, values in conditions.items():
        if key == "url":
            url = values[0]
        elif key == "user":
            user = values[0]
        elif key == "password":
            password = values[0]
        else:
            headers[key] = values[0]

    if not url:
        status.add_error("REST DELETE error, URL not provided")
        return process_status.REST_call_err

    if user and password:
        encoded = get_auth_token(user, password)
    else:
        encoded = None

    if encoded:
        headers['Authorization'] = "Basic " + encoded

    headers["Content-Type"] = "text/plain"

    response = delete_request(status, url, headers, body, 10)

    ret_val, reply_data = response_to_json(status, response)

    if not ret_val:
        if reply_data:
            if isinstance(reply_data, dict):
                utils_print.jput(reply_data, True)
            else:
                utils_print.output(reply_data, True)
    return ret_val


# =======================================================================================================================
# Send Put requests
# example: rest put where url=http://10.0.0.25:2049 and dbms = lsl_demo
# =======================================================================================================================
def do_PUT(status, cmd_words):
    global keywords_put
    # Get params from command line

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3, 0, keywords_put, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return ret_val

    if not "Content-Type" in conditions:
        conditions["Content-Type"] = ["text/plain"]     # Default

    ret_val, url, headers, body = get_rest_headers(status, "POST", conditions)
    if ret_val:
        return ret_val

    response = put_request(status, url, headers, body, 10)

    ret_val, json_data = response_to_json(status, response)

    if not ret_val:
        if json_data:
            utils_print.jput(json_data, True)

    return ret_val

# =======================================================================================================================
# Send VIEW requests
# example: rest view  where url=http://10.0.0.25:2049
# =======================================================================================================================
def do_VIEW(status, cmd_words):
    # Get params from command line

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3, 0, keywords_put, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return ret_val

    if not "Content-Type" in conditions:
        conditions["Content-Type"] = ["text/plain"]     # Default

    ret_val, url, headers, body = get_rest_headers(status, "POST", conditions)
    if ret_val:
        return ret_val

    response = view_request(status, url, None, None)

    ret_val, reply_data = response_to_json(status, response)

    if not ret_val:
        if reply_data:
            if isinstance(reply_data, dict):
                utils_print.jput(reply_data, True)
            else:
                utils_print.output(reply_data, True)
        else:
            utils_print.output("REST returned error %u" % response.status_code, True)

    return ret_val
# =======================================================================================================================
# Send VIEW requests
# example: rest view  where url=http://10.0.0.25:2049
# =======================================================================================================================
def do_POST(status, cmd_words):
    # Get params from command line

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3, 0, keywords_post, False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return ret_val

    if not "Content-Type" in conditions:
        conditions["Content-Type"] = ["text/plain"]     # Default

    ret_val, url, headers, body = get_rest_headers(status, "POST", conditions)
    if ret_val:
        return ret_val

    response = post_request(status, url, headers, body)

    ret_val, reply_data = response_to_json(status, response)

    if not ret_val:
        if reply_data:
            if isinstance(reply_data, dict):
                utils_print.jput(reply_data, True)
            else:
                utils_print.output(str(reply_data), True)
        else:
            utils_print.output("REST returned error %u" % response.status_code, True)

    return ret_val

# =======================================================================================================================
# Send REST requests
# example: rest get where user=Moshe.Shadmon password="Station#6" url=https://MetersAPI.Transpara.com/piwebapi/assetservers
# =======================================================================================================================
def do_GET(status, cmd_words, offset):
    global keywords_get
    # Get params from command line

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3 + offset, 0, keywords_get,
                                                                   False)
    if ret_val:
        # conditions not satisfied by keywords or command structure
        return ret_val

    ret_val, url, headers, body = get_rest_headers(status, "GET", conditions)
    if ret_val:
        return ret_val

    response = get_request(status, url, headers, 20)

    if headers and "validate" in headers and headers["validate"] == "true":
        # only validate URL exists
        if response and hasattr(response,"status_code") and  response.status_code == 200:
            reply_data = "true"            # Valid URL
        else:
            reply_data = "false"        # Broken URL
        ret_val = process_status.SUCCESS

    else:
        ret_val, reply_data = response_to_json(status, response)

    if not ret_val:
        if reply_data:
            if isinstance(reply_data, dict):
                if offset:
                    # Place data to a dictionary or a file (if cmd_words[0] looks like: [file=!prep_dir/london.readings.json, key=results, show= true]
                    ret_val = utils_output.save_data(status, cmd_words[0], reply_data)
                else:
                    utils_print.jput(reply_data, True)
            else:
                if offset:
                    # Place data to a dictionary or a file (if cmd_words[0] looks like: [file=!prep_dir/london.readings.json, key=results, show= true]
                    ret_val = utils_output.save_data(status, cmd_words[0], reply_data)
                else:
                    out_str = str(reply_data)
                    if out_str[0] == '{' and out_str[-1] == '}':
                        utils_print.print_row(out_str, True)
                    else:
                        utils_print.output(out_str, True)
        else:
            utils_print.output("REST returned error %u" % response.status_code, True)

    return ret_val

# =======================================================================================================================
# Get the REST headers from Conditions values
# =======================================================================================================================
def get_rest_headers(status, get_name, conditions):
    '''
    get_name - GET, PUT, POST
    conditions is the array returned from interpreter
    '''

    ret_val = process_status.SUCCESS
    url = ""
    user = ""
    password = ""
    body = None

    if "headers" in conditions:
        headers_str = conditions["headers"][0]
        headers = utils_json.str_to_json(headers_str)
        if not headers:
            status.add_error(f"Failed to get a JSON header from command using: '{headers_str}'")
            ret_val = process_status.REST_call_err
    else:
         headers = {}

    if not ret_val:

        for key, values in conditions.items():
            if key == "url":
                url = values[0]
            elif key == "user":
                user = values[0]
            elif key == "password":
                password = values[0]
            elif key == "body":
                body = str(values[0])
            else:
                headers[key] = values[0]

        if not len(headers):
            headers = None

        if not url:
            status.add_error(f"REST {get_name} error, URL not provided")
            ret_val = process_status.REST_call_err
        else:
            if user and password:
                encoded = get_auth_token(user, password)
                if encoded:
                    headers['Authorization'] = "Basic " + encoded

    return [ret_val, url, headers, body]

# =======================================================================================================================
# Return Json data from the URL
# =======================================================================================================================
def get_jdata(status, connect_str, headers, auth_token, timeout):
    if auth_token:
        headers['Authorization'] = "Basic " + auth_token

    response = get_request(status, connect_str, headers, timeout)

    ret_val, json_data = response_to_json(status, response)

    return [ret_val, json_data]


# =======================================================================================================================
# Get the Json data from the response
# =======================================================================================================================
def response_to_json(status, response):
    if response == None:
        ret_val = process_status.REST_call_err
        reply_data = None
        status.add_error("Failure to retrieve data from REST call")
    else:
        ret_val = process_status.SUCCESS

        if response.status_code == 200:
            try:
                reply_data = response.json()
            except ValueError as err:
                if hasattr(response, "text"):
                    reply_data = response.text
                else:
                    status.add_error("Can't decode reply from the REST response: %s" % str(err))
                    ret_val = process_status.REST_call_err
                    reply_data = None
            except KeyError as err:
                status.add_error("Reply from the REST call has no 'message' key: %s" % str(err))
                ret_val = process_status.REST_call_err
                reply_data = None
            except:
                ret_val = process_status.REST_call_err
                reply_data = None
        else:
            if hasattr(response, "text"):
                reply_data = response.text
            else:
                reply_data = None

    return [ret_val, reply_data]
# =======================================================================================================================
# Execute a rest GET request
# =======================================================================================================================
def get_request(status, url, headers_data, timeout):
    try:
        response = requests.get(url=url, params=None, verify=False, timeout=timeout, headers=headers_data)
    except HTTPError as http_err:
        error_msg = "REST GET HTTPError Error: %s" % str(http_err)
        status.add_error(error_msg)
        response = None
    except Exception as err:
        error_msg = "REST GET Error: %s" % str(err)
        status.add_error(error_msg)
        response = None

    return response
# =======================================================================================================================
# Execute a REST PUT request
# =======================================================================================================================
def put_request(status, url, headers_data, body, timeout):
    r = None

    try:
        r = requests.put(url=url, headers=headers_data, data=body)
    except HTTPError as http_err:
        error_msg = "REST Put Error: %s" % str(http_err)
        status.add_error(error_msg)
    except Exception as err:
        error_msg = "REST Put Error: %s" % str(err)
        status.add_error(error_msg)
    return r

# =======================================================================================================================
# Execute a REST PUT request
# =======================================================================================================================
def delete_request(status, url, headers_data, body, timeout):
    r = None

    try:
        r = requests.delete(url=url, headers=headers_data, data=body)
    except HTTPError as http_err:
        error_msg = "REST Delete Error: %s" % str(http_err)
        status.add_error(error_msg)
    except Exception as err:
        error_msg = "REST Delete Error: %s" % str(err)
        status.add_error(error_msg)
    return r


# =======================================================================================================================
# Execute a rest Post request
# authentication = HTTPDigestAuth('Moshe.Shadmon','Station#6' )
# =======================================================================================================================
def post_request(status, url, headers, data):
    r = None

    try:
        r = requests.post(url=url, headers=headers, data=data)
    except HTTPError as http_err:
        error_msg = "REST Post Error: %s" % str(http_err)
        status.add_error(error_msg)
    except Exception as err:
        error_msg = "REST Post Error: %s" % str(err)
        status.add_error(error_msg)
    return r
# =======================================================================================================================
# Execute a view request
# details at - https://requests.readthedocs.io/en/master/user/advanced/
# =======================================================================================================================
def view_request(status, url, headers, data):
    r = None

    try:

        s = Session()

        req = Request('VIEW',  url)
        prepped = s.prepare_request(req)

        # do something with prepped.body
        prepped.body = 'test body'

        # do something with prepped.headers
        prepped.headers['test-header'] = '123'

        resp = s.send(prepped)

    except HTTPError as http_err:
        error_msg = "REST VIEW Error: %s" % str(http_err)
        status.add_error(error_msg)
        resp = None
    except Exception as err:
        error_msg = "REST VIEW Error: %s" % str(err)
        status.add_error(error_msg)
        resp = None

    return resp


# =======================================================================================================================
# Get an encrypted token based on user name and password
# =======================================================================================================================
def get_auth_token(user, password):
    try:
        token = base64.b64encode((user + ":" + password).encode()).decode("utf-8")
    except:
        token = ""

    return token


def get_ip(status):
    """
    Get network IPv4 through whatismyip.com
    :return:
       IPv4 address
    """
    try:
        r = requests.get('https://ipv4bot.whatismyipaddress.com')
    except Exception as e:
        status.add_error('Failed to get IP address [Error: %s]' % e)
        return False
    return r.text


def get_location(status, ip: str):
    """
    Based on the IP get location info
    :args:
       ip:str - IPv4 address to get location info for
    :return:
       location info (as JSON), if fails return False
    :Sample output:
    {
       "ip": "67.169.64.26",
       "country_code": "US",
       "country_name": "United States",
       "region_code": "CA",
       "region_name": "California",
       "city": "San Mateo",
       "zip_code": "94401",
       "time_zone": "America/Los_Angeles",
       "latitude": 37.5742,
       "longitude": -122.325,
       "metro_code": 807
    }
    """
    try:
        output = os.popen('curl -X GET https://freegeoip.live/json/%s 2> /dev/null' % ip).read()
    except Exception as e:
        status.add_error('Failed to retrive location info based on ip % [Error: %s]' % (ip, e))
        return False

    return output.replace('\n', '')
