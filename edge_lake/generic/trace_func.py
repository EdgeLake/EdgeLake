"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

traced_func_ = {
    "grafana"   :   0,
    "mapping"   :   0,          # Trace Mapping policy - in mapping policy.py
    "period"    :   0,          # period function in unify_result.py
}


# =======================================================================================================================
# get the trace level of a function
# =======================================================================================================================
def get_func_trace_level(function_name):
    return traced_func_[function_name]


# =======================================================================================================================
# is traced function
# =======================================================================================================================
def is_traced(function_name):
    return function_name in traced_func_

# =======================================================================================================================
# Set a trace level
# =======================================================================================================================
def set_trace_level(function_name, trace_level):
    traced_func_[function_name] = trace_level