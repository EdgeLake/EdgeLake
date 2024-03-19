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
from edge_lake.dbms.psql_dbms import PSQL
from edge_lake.dbms.pi_dbms import PIDB
from edge_lake.dbms.sqlite_dbms import SQLITE
from edge_lake.dbms.oledb_dbms import OLEDB
from edge_lake.dbms.mongodb_dbms import MONGODB

import edge_lake.generic.process_status as process_status

supported_databases_ = {
                        # Data type, name extention
    "psql"      :       ["sql",         "",         ],
    "sqlite"    :       ["sql",         "",         ],
    "mongo"     :       ["blobs",       "blobs_",   ],
}

# ==================================================================
# Get real logical database name:
# database that maintains blobs has "blobs_" extension
# ==================================================================
def get_real_dbms_name( dbms_type, dbms_name ):
    global supported_databases_

    if dbms_type in supported_databases_:
        prefix = supported_databases_[dbms_type][1]     # i.e.  blobs_
        if dbms_name.startswith(prefix):
            # connect DBMS added the prefix
            real_name = dbms_name
        else:
            real_name = "blobs_" + dbms_name        # Add the prefix
    else:
        real_name = dbms_name
    return real_name

# ==================================================================
# Test if the database type supports blobs
# ==================================================================
def is_blobs_dbms( dbms_type ):
    global supported_databases_

    if dbms_type in supported_databases_ and supported_databases_[dbms_type][0] == "blobs":
        ret_val = True
    else:
        ret_val = False

    return ret_val
# ==================================================================
#
# select which database to use 
#
# ==================================================================
def select_dbms(status: process_status, db_type: str, connect_str: str, port: int, dbn: str, in_ram: bool,
                engine_string: str):
    """
    Select DBMS
    :args:
       status:process_status - process_status
       db_type:str - database to connect to
       usr:str - database user
       port:int - database port
       dbn:str - database name
       log_file:str - database log file
    :param:
       dbms_id:str - database connection name
    :return:
       connection to database
    """

    # format port to be int
    try:
        port = int(port)
    except:
        status.add_error('Invalid database connection port')
        return None

    if ":" not in connect_str or "@" not in connect_str:
        status.add_error("Invalid database connection info format (user@host:password)")
        return None

    if isinstance(dbn, str) is False:
        status.add_error("Invalid logical database name.")
        return None

    try:
        user = connect_str.split("@")[0]
    except:
        status.add_error("Invalid user info to connect to database.")
        return None
    try:
        passwd = connect_str.rsplit(":", 1)[1]
    except:
        status.add_error("Invalid user info to connect to database.")
        return None
    try:
        host = connect_str.split("@", 1)[1].rsplit(":", 1)[0]
    except:
        status.add_error("Invalid user info to connect ot database.")
        return None

    return connect_dbms(status, dbn, db_type, user, passwd, host, port, in_ram, engine_string, None)

# ==================================================================
#
# Connect a logical database to a physical database
#
# ==================================================================
def connect_dbms(status, dbms_name, db_type, user, passwd, host, port, in_ram, engine_string, conditions):
    # connect DBMS
    db_type_name = db_type.lower()
    if db_type_name == "psql":
        dbms = PSQL()
    #   elif db_type_name == "pi":
    #      dbms = PIDB()
    elif db_type_name == "sqlite":
        dbms = SQLITE(in_ram)
    elif db_type_name == "mongo":
        dbms = MONGODB()
    elif db_type_name.startswith("oledb."):
        dbms = OLEDB(engine_string, db_type_name)
    else:
        if db_type_name == "oledb":
            status.add_error("Database type '%s' requires an extension, for example oledb.pi" % db_type)
        else:
            status.add_error("Database type '%s' is not supported" % db_type)
        return None

    if port:
        if not isinstance(port, int):
            try:
                port_val = int(port)
            except:
                status.add_error(f"Wrong port value for dbms '{dbms_name}' ({db_type})")
                return None
        else:
            port_val = port
    else:
        port_val = port

    if not dbms.connect_to_db(status, user, passwd, host, port_val, dbms_name, conditions):
        dbms = None

    return dbms
