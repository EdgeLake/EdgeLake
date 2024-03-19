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
import anylog_node.generic.process_status as process_status
from anylog_node.json_to_sql.map_json_to_insert import map_columns


# ============================================================================
# generate multi-row insert  based on list of dicts and corresponding columns 
# ============================================================================
def create_insert(status: process_status, table_name: str, data: list, columns: list):
    """
    generate multi-row insert  based on list of dicts and corresponding columns
    :args:
       data:list - list of dictionary objects from JSON
       columns:list of columns
    :return:
       INSERT statement (str)
    """
    stmt = "INSERT INTO " + table_name + " values "
    # generate pre-rows insert based on columns

    # generate rows to insert using map_json_to_insert.create_insert_rows
    data = map_columns(status, "", "", "", 0, data, columns)

    if data == None:
        return ""

    rows_total = len(data)
    row_count = 0
    # add rows to insert into INSERT stmt
    for row in data:
        stmt += "\n\t(" + row + ")"
        row_count += 1
        if row_count == rows_total:
            stmt += ";"  # for end of stmt
        else:
            stmt += ","  # for extended insert

    return stmt


# =======================================
# main to generate insert from results 
# ======================================
def map_results_to_insert_main(status: process_status, table_name: str, json_object: dict):
    """
    Given a JSON result, convert to INSERT statement
    :args:
       json_object:str - object containing JSON data
    :return:
       For each set of results in json_object, create an INSERT stmt
       Results are in multi-row format
    """

    # Place the metadata (the table structure) on the tables dictionary
    try:

        for key in json_object:  # only one key in the dictionary
            columns = list(json_object[key][0].keys())
            return create_insert(status, table_name, json_object[key], columns)


    except:
        return ""
