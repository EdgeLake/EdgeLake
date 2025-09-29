"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""
import edge_lake.generic.params as params
import edge_lake.dbms.db_info as db_info


# Manage the distribution of the data

sql_helpers_ = None
default_dir_ = ""

# ------------------------------------------------------------------
# Test if helpers are used
# ------------------------------------------------------------------
def is_with_helpers():
    global sql_helpers_

    return True if sql_helpers_ else False
# ------------------------------------------------------------------
# Using multiple processes to do the insert
# When the helpers are set, this process keeps the info as f(dbms type)
# ------------------------------------------------------------------
def use_sql_helpers(dbms_type, helpers_count, process_dir):
    '''
    dbms_type - like psql
    helpers_count - how many helpers are defined
    process_dir - the root directory from which the helpers are located
    '''
    global sql_helpers_
    global default_dir_

    default_dir_ = process_dir

    helpers_setup = {
        "current" : 1,        # The helper to use
        "helpers_count" : helpers_count,  # Number of helpers
        "dir" : [None],
        "usage" : [0] * (helpers_count + 1),
        "dbms_type" : dbms_type,
    }
    # Details the dirs to use
    for helper_id in range(1, int(helpers_count) + 1):
        helpers_setup["dir"].append(process_dir + params.get_path_separator() + dbms_type + f"_{helper_id}" + params.get_path_separator())

    if sql_helpers_ is None:
        sql_helpers_ = {}

    sql_helpers_[dbms_type] = helpers_setup

# ----------------------------------------------------------
# With helper processes, distribute the file to the next helper process
# ----------------------------------------------------------
def get_next_helper_dir(dbms_name, table_name):
    global sql_helpers_
    global default_dir_

    dbms_type = db_info.get_db_type(dbms_name)
    if dbms_type in sql_helpers_:       # Test that the helper support this the dbms used
        # The table exists in the metadata and on the dbms
        helper_id = sql_helpers_[dbms_type]["current"]    # The helper to use
        helper_dir = sql_helpers_[dbms_type]["dir"][helper_id]
        sql_helpers_[dbms_type]["usage"][helper_id] += 1        # count files given to helper
        helper_id += 1      # Set on next helper
        sql_helpers_[dbms_type]["current"] = helper_id if helper_id <= sql_helpers_[dbms_type]["helpers_count"] else 1
    else:
        helper_dir = default_dir_
    return helper_dir


