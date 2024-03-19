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
import time



forward_info_ = {

}

import sys
try:
    import portforward
except:
    portforward_installed_ = False
else:
    portforward_installed_ = True

try:
    from kubernetes import client, config
except:
    kubernetes_installed_ = False
else:
    kubernetes_installed_ = True


import edge_lake.generic.process_status as process_status
import edge_lake.generic.process_log as process_log
import edge_lake.generic.interpreter as interpreter
import edge_lake.generic.utils_print as utils_print
from enum import Enum


# https://pypi.org/project/portforward/
# https://portforward.readthedocs.io/en/latest/
# https://github.com/pytogo/portforward/blob/main/tests/test_portforward.py

class LogLevel(Enum):
    DEBUG = 0
    INFO = 1
    WARN = 2
    ERROR = 3
    OFF = 4

# ----------------------------------------------------------------------
# Test if needed lib in installed
# ----------------------------------------------------------------------
def is_installed():
    global portforward_installed_
    return portforward_installed_
# --------------------------------------------------------------
'''
# Replacing
kubectl port-forward service/grpc-server 50051:50051 
'''

# Configure port forwarding
# Example: set port forwarding where pod_name = "pod_name" and namespace = "namespace" and local_port = 8080 and pod_port=80
# Example: set port forwarding where pod_name = "kubearmor-relay-577df7b5-dq86l" and namespace = "kubearmor" and local_port = 8080 and pod_port=32767
# --------------------------------------------------------------
def configure(dummy: str, conditions: dict):

    status = process_status.ProcessStat()
    exit_msg = "Port Forwarding Client process terminated"

    pod_name = conditions["pod_name"][0]
    namespace = conditions["namespace"][0]
    local_port = conditions["local_port"][0]
    pod_port = conditions["pod_port"][0]

    # set_kubernetes_client()

    port_forward(status, pod_name, namespace, local_port, pod_port)

    process_log.add_and_print("event", exit_msg)
# --------------------------------------------------------------
# Do the port forwarding
"""
    Connects to a **pod or service** and tunnels traffic from a local port to
    this target. It uses the kubectl kube config from the home dir if no path
    is provided.

    The libary will figure out for you if it has to target a pod or service.

    It will fall back to in-cluster-config in case no kube config file exists.

    Caution: Go and the port-forwarding needs some ms to be ready. ``waiting``
    can be used to wait until the port-forward is ready.

    (Best consumed as context manager.)

    Example:
        >>> import portforward
        >>> with portforward.forward("test", "web-svc", 9000, 80):
        >>>     # Do work
        >>>
        >>> # Or without context manager
        >>>
        >>> forwarder = portforward.forward("test", "some-pod", 9000, 80)
        >>> # Do work
        >>> forwarder.stop()

    :param namespace: Target namespace
    :param pod_or_service: Name of target Pod or service
    :param from_port: Local port
    :param to_port: Port inside the pod
    :param config_path: Path for loading kube config
    :param waiting: Delay in seconds
    :param log_level: Level of logging
    :param kube_context: Target kubernetes context (fallback is current context)
    :return: forwarder to manual stop the forwarding
"""
"""
Example:

NAMESPACE     NAME                                        READY   STATUS    RESTARTS        AGE
kubearmor     pod/kubearmor-none-docker-d4651-mhgsj       1/1     Running   2 (7m23s ago)   11d

NAMESPACE     NAME                                           TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)                  AGE
kubearmor     service/kubearmor                              ClusterIP   10.103.194.213   <none>        32767/TCP                11d

set port forward where namespace = kubearmor and pod_name = kubearmor and local_port = 32769 and pod_port = 32767

"""

# --------------------------------------------------------------
def port_forward(status, pod_name, namespace, local_port, pod_port):
    '''
    Parameters:
    namespace – Target namespace
    pod_or_service – Name of target Pod or service
    from_port – Local port
    to_port – Port inside the pod
    config_path – Path for loading kube config
    waiting – Delay in seconds
    log_level – Level of logging
    kube_context – Target kubernetes context (fallback is current context)
    Returns:
    forwarder to manual stop the forwarding
    '''
    config_path = None
    waiting: float = 0.1
    log_level: LogLevel = LogLevel.DEBUG
    kube_context: str = ""


    try:
        # No path to kube config provided - will use default from $HOME/.kube/config
        forwarder = portforward.forward(namespace, pod_name, local_port, pod_port, config_path, waiting, log_level, kube_context)
    except:
        errno, value = sys.exc_info()[:2]
        status.add_error(f"Failed to set Kubernetes port-forwarding (Error: ({errno}) {value})")
    else:
        key = f"{namespace}.{pod_name}"
        forward_info_[key] = [forwarder, local_port, pod_port]

    while (1):
        time.sleep(3)

# --------------------------------------------------------------
# Reset the port forwarding
# Example: reset port forward where namespace = test and pod = web-svc
# --------------------------------------------------------------
def reset_port_forward(status, io_buff_in, cmd_words, trace):


    if not is_installed():
        # portforward lib is not installed
        status.add_error("portforward Lib failed to import")
        return process_status.Failed_to_import_lib

    #                               Must     Add      Is
    #                               exists   Counter  Unique
    keywords = {"namespace": ("str", True, False, True),
                "pod":       ("str", True, False, False),
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words,
                                                                   4, 0, keywords,
                                                                   False)

    if ret_val:
        return ret_val

    namespace = interpreter.get_one_value(conditions, "namespace")
    pod_name = interpreter.get_one_value(conditions, "pod")
    key = f"{namespace}.{pod_name}"


    if not key in forward_info_:
        status.add_error(f"No port forwarding for namespace: '{namespace}' and pod: '{pod_name}'")
        return process_status.ERR_command_struct

    forwarder = forward_info_[key][0]
    ret_val = process_status.SUCCESS

    try:
        portforward._portforward.stop(namespace, pod_name, 32767, 4)       # 4 is LogLevel as OFF
        del forward_info_[key]
    except:
        errno, value = sys.exc_info()[:2]
        status.add_error(f"Faild to stop Kubernetes port-forwarding (Error: ({errno}) {value})")
        ret_val = process_status.ERR_process_failure

    return ret_val

# --------------------------------------------------------------
# Get the port forwarding info
# Example: get port forward
# --------------------------------------------------------------
def get_port_forward(status, io_buff_in, cmd_words, trace):

    if not is_installed():
        # portforward lib is not installed
        reply_str = "portforward Lib failed to import"
    else:

        if not len(forward_info_):
            reply_str = "No Kubernetes port-forwarding settings"
        else:
            reply_list = []
            for key, val in forward_info_.items():
                name_pod = key.split(".")
                if len(name_pod) == 2:
                    namespace = name_pod[0]
                    pod_name = name_pod[1]
                    local_port = val[1]
                    pod_port = val[2]
                    reply_list.append((namespace, pod_name, local_port, pod_port))

            reply_str = utils_print.output_nested_lists(reply_list, "", ["Namespace", "Pod Name", "Local Port", "Pod Port"], True)

    return [process_status.SUCCESS, reply_str]


# https://www.velotio.com/engineering-blog/kubernetes-python-client
def set_kubernetes_client():
    # Configure API server access
    configuration = client.Configuration()
    configuration.host = "https://10.0.0.251:49277"  # Assuming the default API server port is 6443

    configuration.verify_ssl = False  # Only use this if the server uses self-signed certificates

    client.Configuration.set_default(configuration)

    # Create a Kubernetes API client
    v1 = client.CoreV1Api()

    # List nodes
    node_list = v1.list_node()

    # List namespaces
    namespace_list = v1.list_namespace()

    # Print the namespace names
    for namespace in namespace_list.items:
        print(f"Namespace: {namespace.metadata.name}")


    # Specify the namespace
    namespace = "kubearmor"

    # List Pods in the specified namespace
    pod_list = v1.list_namespaced_pod(namespace)

    # Print the pod names
    for pod in pod_list.items:
        print(f"Pod Name: {pod.metadata.name}")

    pass