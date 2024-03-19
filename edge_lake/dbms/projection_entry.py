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


# ==================================================================
# Info on the entries in the projection list in a SQL_query
# ==================================================================

class ProjectionEntry:

    def __init__(self, output_counter):
        self.output_counter = output_counter  # an id that determines the projected field id, stating at 0
        self.remote_query = ""  # SELECT SQL stmt send to operator
        self.generic_query = ""  # SELECT Generic stmt send to operator
        self.local_create = ""  # CREATE stmt on local server
        self.counter_local_fields = 0  # the number of local fields crearted
        self.local_query = ""  # SELECT stmt on local server
        self.is_function = False
        self.function_name = ""     # separate the function name from the function + column name
        self.column_name = ""       # separate the column name from the function + column name
        self.local_name = ""  # The column name in the local table that aggregates the results
        self.as_name = ""       # The projection name

        self.local_order_by = ""
        self.local_group_by = ""
        self.remote_group_by = ""

        self.projection_flag = True

    def set_projection_info(self, pulled_name, local_name, r_query, g_query, l_create, l_query, counter_local_fields, as_name):
        self.column_name = pulled_name.strip()
        self.local_name = local_name
        self.remote_query = r_query
        self.generic_query = g_query
        self.local_create = l_create
        self.local_query = l_query
        self.set_counter_local_fields(counter_local_fields)  # number of fields added for the local create
        self.as_name = as_name
    # ==================================================================
    # set the umber of fields added for the local create
    # ==================================================================
    def set_counter_local_fields(self, counter):
        self.counter_local_fields = counter

    # ==================================================================
    # The projection_flag determines if to add the column name to the projection list.
    # With "SELECT * FROM ...", extended fields (like @table_name) are not added to the local query
    # ==================================================================
    def set_projection_flag(self, flag):
        self.projection_flag = flag

    # ==================================================================
    # Set the number of fields added (for the local create stmt) to support this function
    # ==================================================================
    def set_counter_local_fields(self, counter):
        self.counter_local_fields = counter

    # ==================================================================
    # Get the number of fields added (for the local create stmt) to support this function
    # ==================================================================
    def get_counter_local_fields(self):
        return self.counter_local_fields

    # ==================================================================
    # Set only for function calls (will not be called in select * or select column)
    # ==================================================================
    def set_function(self, f_name):
        self.is_function = True
        self.function_name = f_name

    # ==================================================================
    # Return SQL for the operator node
    # ==================================================================
    def get_remote_query(self, current_query: str):

        if self.remote_query == "":
            return current_query  # nothing to add

        if test_no_projection(current_query):  # test if first projected field added
            # not first projected field
            return current_query + self.remote_query

        return current_query + ", " + self.remote_query

    # ==================================================================
    # Return generic query to the operator node
    # ==================================================================
    def get_generic_query(self, current_query: str):

        if self.generic_query == "":
            return current_query  # nothing to add

        if test_no_projection(current_query):  # test if first projected field added
            # not first projected field
            return current_query + self.generic_query

        return current_query + ", " + self.generic_query

    # ==================================================================
    # Return SQL to create the unifying table
    # ==================================================================
    def get_local_create(self, current_create):

        if self.local_create == "":
            return current_create  # nothing to add

        if current_create[-1] == '(':  # first created field added
            # not first projected field
            return current_create + self.local_create

        return current_create + ", " + self.local_create

    # ==================================================================
    # Return SQL to query the local table
    # ==================================================================
    def get_local_query(self, current_query):

        if self.local_query == "" or not self.projection_flag:
            return current_query  # nothing to add

        if test_no_projection(current_query):  # test if first projected field added
            # not first projected field
            return current_query + self.local_query

        return current_query + ", " + self.local_query

    # ==================================================================
    # Return True if the function is projected to the user (there is a select of this field)
    # ==================================================================
    def is_function_projected(self):
        return self.local_query != ""

    # ==================================================================
    # set local order by
    # ==================================================================
    def set_local_order_by(self, order_by):
        self.local_order_by = order_by

    # ==================================================================
    # set local group by
    # ==================================================================
    def set_local_group_by(self, group_by):
        self.local_group_by = group_by

    # ==================================================================
    # Add column to group by
    # ==================================================================
    def add_local_group_by_column(self, group_by_column, is_first):
        '''
        group_by_column - the name of the column
        is_first - True value will add the column to be first in the group by list. Else, will be added last.
        '''
        if not self.local_group_by:
            self.local_group_by = "group by %s" % group_by_column
        elif is_first:
            # add first
            self.local_group_by = "group by %s, %s" % (group_by_column, self.local_group_by[9:])
        else:
            # add Last
            self.local_group_by += ", %s" % group_by_column


    # ==================================================================
    # set remote group by
    # ==================================================================
    def set_remote_group_by(self, group_by):
        self.remote_group_by = group_by

    # ==================================================================
    # Functions may require group
    # ==================================================================
    def get_local_group_by(self):
        return self.local_group_by

    # ==================================================================
    # Functions may require group
    # ==================================================================
    def get_remote_group_by(self):
        return self.remote_group_by

    # ==================================================================
    # Functions may require order by
    # ==================================================================
    def get_local_order_by(self):
        return self.local_order_by

    # ==================================================================
    # Add info to remote query
    # ==================================================================
    def add_remote_query_info(self, info):
        self.remote_query += info

    # ==================================================================
    # Add info to generic query
    # ==================================================================
    def add_generic_query_info(self, info):
        self.generic_query += info

    # ==================================================================
    # Add info to remote query
    # ==================================================================
    def add_local_query_info(self, info):
        self.local_query += info

# ==================================================================
# Returns TRUE if query with no projection info
# ==================================================================
def test_no_projection(sql_query):
    if sql_query.endswith("select "):
        ret_val = True
    elif sql_query.endswith("select distinct "):
        ret_val = True
    else:
        ret_val = False
    return ret_val
