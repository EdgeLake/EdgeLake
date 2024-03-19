import subprocess

import anylog_node.generic.process_status as process_status

def get_data(status):

    os_string = "cleos get info"
    process = subprocess.Popen(os_string, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if len(stderr):
        error_msg = stderr.decode("utf-8")
        status.add_error("EOS call error: %s" % error_msg)
        ret_val = process_status.BLOCKCHAIN_operation_failed
    else:
        ret_val = process_status.SUCCESS

    return ret_val