"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import datetime
import decimal
import json
import os
import sys
import re


import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_json as utils_json

# ==================================================================
# Based on the query type, execute and get results
# ==================================================================
def get_raw_data(status: process_status, active_dbms, query_type: str, query: str):
    """
    Based on the query type, execute and get results
    :args:
       active_dbms - connection to database
       query_type:str - whether the query is as a text or in file
       query:str - actual query
    :return:
       result of query. If fails returns False
    """
    if query_type == 'text':
        raw_data = active_dbms.execute_sql_stmt(status, query)
    elif query_type == 'file':
        raw_data, rows_counter = active_dbms.execute_sql_file(status, query)
    else:
        status.add_warning("Unsuporrted query type - `%s`" % query_type)
        return False  # return empty list

    return raw_data


# ==================================================================
# Convert date/time/datetime objects in a given dict to string format
# ==================================================================
def format_timestamp(line: dict):
    """
    Convert date/time/datetime objects in a given dict to string format
    :args:
        line:dict - dict containg raw data that needs to be validated
    :return:
       properlly formated line
    """
    for key in line:
        if isinstance(line[key], datetime.time):
            line[key] = line[key].strftime('%H:%M:%S.%s')
        elif isinstance(line[key], datetime.date):
            line[key] = line[key].strftime('%Y-%m-%d')
        elif isinstance(line[key], datetime.datetime):
            line[key] = line[key].strftime('%Y-%m-%d %H:%M:%S.%s')
        elif isinstance(line[key], decimal.Decimal):
            line[key] = float(line[key])

    return line


# ==================================================================
# Itterate through the steps to generate query results
# ==================================================================
def execute_query(status: process_status, active_dbms, dbms_id: str, query_type: str, query: str):
    """
    Execute query and print results
    :args:
       active_dbms:dict - dict of dbms nodes
       dbms_id:str - DBMS connection identifier
       query_type:str - type of query (text or file)
       query:str - query to execute
    :param:
       data:list - query results
    :return:
       query results as a JSON object (string)
    """
    # Get raw data
    data = get_raw_data(status, active_dbms, query_type, query)
    if data is False:
        status.add_error("Failed to execute SELECT query.")
        return ""

    # convert raw data to propeer format
    for i in range(len(data)):
        data[i] = format_timestamp(data[i])

    table_name = query.lower().split('from ', 1)[-1].split(' ')[0].split(';')[0]
    # convert result to JSON
    data = utils_json.to_string({"Query." + dbms_id + "." + table_name: data})
    if data is False:
        return ""

    return data


# ==================================================================
# Set query cursor
# ==================================================================
def execute_sql_stmt(status: process_status, active_dbms, dbms_id: str, query_type: str, query: str):
    ret_val = active_dbms.execute_sql_stmt(status, query)

    return ret_val
