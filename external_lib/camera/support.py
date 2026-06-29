import sys
import re
import xmltodict

import edge_lake.generic.process_status as process_status
from edge_lake.generic.utils_lazy_import import lazy_import


"""
Read content from camera via REST call 
"""
def rest_call(status, method:str, url:str, user:str, password:str, header:dict={}, data={}, payload:dict={},
              timeout:float=30, stream:bool=False):
    ret_val = process_status.SUCCESS
    response = None
    err_msg = ""

    requests, error = lazy_import("requests")
    if error:
        status.add_error(error)
        return process_status.Failed_to_import_lib, None

    auth = ()
    if user and password:
        auth = (user, password)

    try:
        if method.lower() == 'get':
            response = requests.get(url=url, headers=header, auth=auth, data=data, json=payload, timeout=timeout, stream=stream)
        elif method.lower() == 'post':
            response = requests.get(url=url, headers=header, auth=auth, data=data, json=payload, timeout=timeout, stream=stream)
        if not 200 <= int(response.status_code) < 300:
            err_msg = f"Failed to get data from camera (Network Error: {response.status_code})"
            ret_val = process_status.NETWORK_CONNECTION_FAILED
            response =None
    except:
        errno, value = sys.exc_info()[:2]
        err_msg = f'Failed to get data from camera: {value}'
        ret_val = process_status.NETWORK_CONNECTION_FAILED
        response = None

    if err_msg:
        status.add_error(err_msg)

    return ret_val, response

def convert_xml(status, content:str)->(process_status, dict):
    """
    Convert XML content to dict
    :args:
        content:str - XML content in string fromat
    :return:
        content as dict
    """
    err_msg = ""
    ret_val = process_status.SUCCESS
    updated_content = None
    try:
        updated_content = xmltodict.parse(content)
    except:
        errno, value = sys.exc_info()[:2]
        err_msg = f'Failed to decode content from camera: {value}'
        ret_val = process_status.Decoding_failed

    if err_msg:
        status.add_error(err_msg)

    return ret_val, updated_content

def parse_logs(status, log_text):
    ret_val = process_status.SUCCESS
    parsed_logs = []
    try:
        raw_dicts = [{"line_number": i + 1, "message": line} for i, line in enumerate(log_text) if
                     line not in ["", "----- System log -----"]]

        log_pattern = re.compile(
            r"^(?:(?P<timestamp>\S+) )?"
            r"(?:(?P<host>\S+) )?"
            r"(?:\[ (?P<level>\w+) \] )?"
            r"(?:(?P<subsystem>\w+): )?"
            r"(?P<message>.+)$"
        )


        for raw_dict in raw_dicts:
            match = log_pattern.match(raw_dict['message'])
            if match:
                parsed_logs.append(match.groupdict())
    except:
        errno, value = sys.exc_info()[:2]
        err_msg = f'Failed to extract logs from camera: {value}'
        ret_val = process_status.Uninitialized_variable

    return ret_val, parsed_logs
