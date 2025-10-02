"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

# https://blog.jonathanmccall.com/2018/10/09/creating-a-grafana-datasource-using-flask-and-the-simplejson-plugin/

# Examples: http://oz123.github.io/writings/2019-06-16-Visualize-almost-anything-with-Grafana-and-Python/index.html

import time

import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.generic.utils_io as utils_io
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_columns as utils_columns
import edge_lake.generic.utils_sql as utils_sql
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.process_status as process_status
import edge_lake.generic.trace_func as trace_func
import edge_lake.cmd.native_api as native_api

# -----------------------------------------------------------------------------------
# AnyLog JSON Connector to Grafana
# Grafana documentation -  https://grafana.com/grafana/plugins/simpod-json-datasource
#                          https://grafana.com/docs/grafana/latest/developers/plugins/legacy/data-sources/
#                          https://docs.huihoo.com/grafana/2.6/reference/http_api/index.html
# -----------------------------------------------------------------------------------


# Grafana day and month names
_week_day_name = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_month_name = [None,
               "Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def format_date_time(timestamp):
    year, month, day, hh, mm, ss, wd, y, z = time.gmtime(timestamp)
    return "%s, %02d %3s %4d %02d:%02d:%02d GMT" % (
        _week_day_name[wd], day, _month_name[month], year, hh, mm, ss
    )

grafana_data_types_ = {
    "string"    :       True,
    "timestamp" :       True,
    "number"    :       True,
}

aggregation_func_ = ["count", "avg", "min", "max" ]
# -----------------------------------------------------------------------------------
# An object that have the parameters to query and process AnyLog data
# -----------------------------------------------------------------------------------
class AlQueryParams:

    # =======================================================================================================================
    #  Get the info provided from Grafana and add the info declared by the user in the 'Additional JSON Data' section.
    #  The info describes the query and the data transferred back to Grafana
    # =======================================================================================================================
    def __init__(self, status, dbms_name, body_info):

        self.dbms_name = dbms_name  # DBMS name from the Connection
        self.table_name = "Unknown"
        self.sql_stmt = ""
        self.where_cond = ""
        self.functions = None       # Details the functions like Min, Max to overwrite the defaults
        self.column_time = ""       # The name of the column used for the time values
        self.column_value = ""
        self.instructions = ""
        self.timeseries = False
        self.request_type = ""
        self.provided_sql = False       # Set to True if SQL is provided from the Grafana Payload
        self.members = None
        self.sql_query = False
        self.with_time_range = True
        self.offset_for_where = -1  # offset to location after the table name
        self.offset_where = -1
        self.increment_offset = -1
        self.interval_time = -1
        self.executed_stmt = {}  # A dictionary with the executed statements
        self.nodes_info = []  # An array to maintain info on nodes in the network
        self.metric = []
        self.attribute = []
        self.trace_level = trace_func.get_func_trace_level("grafana")   # Could change by a dashboard's panel info
        self.timezone = None    # Timezone from Grafana
        self.include_tables = None      # List of tables to be treated as table selected
        self.extend_keys = None              # Added info pushed to the query
        self.fixed_points = False  # Use fixed data points in increment function to show fixed intervals
        self.grafana_start_ms = 0
        self.grafana_end_ms = 0
        self.grafana_interval_ms = 0
        self.user_limit = 0
        if "scopedVars" in body_info and "__interval_ms" in body_info["scopedVars"] and 'value' in body_info["scopedVars"]["__interval_ms"]:
            self.grafana_interval_ms =  body_info["scopedVars"]["__interval_ms"]["value"]
            grafana_seconds = self.grafana_interval_ms / 1000
            if grafana_seconds < 1:
                # Smallest supported
                self.grafana_interval_time = 1
                self.grafana_interval_unit = "second"
            else:
                self.grafana_interval_time, self.grafana_interval_unit = utils_columns.seconds_to_time(grafana_seconds)

        else:
            self.grafana_interval_time = None
            self.grafana_interval_unit = None

        # time interval for the query
        if "interval" in body_info.keys():
            interval = body_info["interval"]
            if len(interval) > 1:
                if interval[-2:] == 'ms':
                    self.interval_unit = "second"
                    self.interval_time = 1  # smallest interval unit
                elif interval[-1] == 's':
                    self.interval_unit = "second"
                elif interval[-1] == 'm':
                    self.interval_unit = "minute"
                elif interval[-1] == 'h':
                    self.interval_unit = "hour"
                elif interval[-1] == 'd':
                    self.interval_unit = "day"
                else:
                    status.add_error("Failed to interpret query interval time")
                    self.interval_unit = "Unknown"
            else:
                status.add_error("Failed to interpret query interval time")
                self.interval_unit = "Unknown"

            if self.interval_time == -1:
                if interval[:-1].isnumeric():
                    self.interval_time = int(interval[:-1])
                else:
                    status.add_error("Failed to interpret query interval time")
        else:
            # In a Table query, time interval in not needed
            self.interval_unit = "Unknown"

        # Get the time range - Note: The Z stands for the Zero timezone
        if "range" in body_info.keys():
            time_range = body_info["range"]

            if 'from' in time_range.keys():
                self.time_start = time_range['from']
                # if self.time_start[-1] == 'Z':   # Zero time
                # self.time_start = utils_columns.utc_to_local(self.time_start)
            else:
                self.time_start = utils_columns.get_current_time()

            #self.grafana_start_ms = utils_columns.get_ms_from_date_time(self.time_start)
            self.grafana_start_ms = utils_columns.get_utc_to_ms(self.time_start)

            if 'to' in time_range.keys():
                self.time_end = time_range['to']
                # if self.time_end[-1] == 'Z':   # Zero time
                # self.time_end = utils_columns.utc_to_local(self.time_end)
            else:
                self.time_end = utils_columns.get_current_time()

            #self.grafana_end_ms = utils_columns.get_ms_from_date_time(self.time_end)
            self.grafana_end_ms = utils_columns.get_utc_to_ms(self.time_end)

        else:
            self.time_start = "Not provided"
            self.time_end = "Not provided"

        if "maxDataPoints" in body_info.keys():
            self.limit = body_info["maxDataPoints"]
        else:
            self.limit = 0

        if "timezone" in body_info.keys():
            self.timezone = body_info["timezone"]

        if 'targets' in body_info.keys():
            # Every target describes a query
            self.targets = body_info['targets']
            self.counter_targets = len(self.targets)
        else:
            self.targets = None

    # =======================================================================================================================
    # Return the number of targets (queries to process)
    # =======================================================================================================================
    def get_counter_targets(self):
        return self.counter_targets

    # =======================================================================================================================
    # Set the info of a particular target
    # Every target describes a query - Grafana transfers each query info as an entry in the body_info['targets'] array
    # =======================================================================================================================
    def set_target_info(self, status, target_id):

        ret_val = process_status.SUCCESS

        err_msg = ""
        self.servers = ""
        self.details = ""
        self.default_query = False
        self.timeseries = False
        self.data_points = 0

        if 'target' in self.targets[target_id].keys():
            gr_struct = self.targets[target_id]
            self.table_name = gr_struct['target']  # Table name from the Grafana menu
            index = self.table_name.find('.')  # Table name may be dbms_name.table_name
            if index > 0 and index < (len(self.table_name) - 1):
                # Get the dbms name and the table name from the lookup for the list of tables \
                # This process is under - if request_handler.path.startswith("/search"):
                self.dbms_name = self.table_name[:index]  # DBMS name from the menu showing yable name + dbms name
                self.table_name = self.table_name[index + 1:]

            if "data" in gr_struct.keys():
                # Overide defaults
                # has info provided in the JSON input in the Grafana panel
                al_data = gr_struct['data']
            elif "payload" in gr_struct.keys():
                # In version 9.5.2 of Grafana
                al_data = gr_struct['payload']
                if al_data and not isinstance(al_data, dict):
                    al_data = None  # not usable
                    err_msg = "Data Source info is not in JSON format"
                    ret_val = process_status.Err_grafana_payload
            else:
                al_data = None
                err_msg = "Missing Data Source info"
                ret_val = process_status.Err_grafana_payload

            if ret_val:
                return [ret_val, err_msg]

            if not target_id:
                # THe type of presentation is determined by the first query
                if 'type' in gr_struct:
                    # take the type from the Grafana "FORMAT AS" option on the Panel
                    if gr_struct['type'] == "timeseries":
                        self.timeseries = True
                else:
                    # The case that 'type' is not provided - we saw instances without "format as" on the grafana panel
                    if al_data and "grafana" in al_data:
                        if "format_as" in al_data["grafana"]:
                            if al_data["grafana"]["format_as"] == "timeseries": # Define in the Payload
                                self.timeseries = True
                        if "data_points" in al_data["grafana"]:
                            data_points = al_data["grafana"]["data_points"] # Number of points to return
                            if isinstance(data_points, int):
                                self.data_points = data_points
            if al_data:

                if "timezone" in al_data:
                    self.timezone = al_data["timezone"]
                if "include" in al_data:
                    # List of tables to be treated as table selected
                    if not isinstance(al_data["include"], list):
                        err_msg = "Grafana Payload: 'include' value is not a list"
                        return [process_status.Err_grafana_payload, err_msg]
                    include_list = al_data["include"]
                    if not all(isinstance(item, str) for item in include_list):
                        err_msg = "Grafana Payload: 'include' entries needs to be strings representing table names"
                        return [process_status.Err_grafana_payload, err_msg]
                    self.include_tables = f"({','.join(include_list)})"
                if "extend" in al_data:
                    # Added info pushed to the query
                    if not isinstance(al_data["extend"], list):
                        err_msg = "Grafana Payload: 'extend' value is not a list"
                        return [process_status.Err_grafana_payload, err_msg]
                    extend_list = al_data["extend"]
                    if not all(isinstance(item, str) for item in extend_list):
                        err_msg = "Grafana Payload: 'extend' entries needs to be strings"
                        return [process_status.Err_grafana_payload, err_msg]
                    self.extend_keys = f"({','.join(extend_list)})"

                if "dbms" in al_data.keys():
                    self.dbms_name = str(al_data["dbms"])  # DBMS name mention explicitly in the JSON
                if "table" in al_data.keys():
                    self.table_name = str(al_data["table"])  # Table name mention explicitly in the JSON
                    table_provided = True
                    index = self.table_name.find('.')  # Table name may be dbms_name.table_name
                    if index > 0 and index < (len(self.table_name) - 1):
                        # Get the dbms name and the table name from the lookup for the list of tables \
                        # This process is under - if request_handler.path.startswith("/search"):
                        self.dbms_name = self.table_name[:index]  # DBMS name from the menu showing yable name + dbms name
                        self.table_name = self.table_name[index + 1:]
                else:
                    table_provided = False

                if "type" in al_data.keys():
                    self.request_type = str(al_data["type"])
                    if self.request_type == "period":
                        self.default_query = True
                        self.sql_query = True
                    elif self.request_type == "increments":
                        self.default_query = True
                        self.sql_query = True
                        # Engine will set intervals
                        self.interval_unit = None
                        self.interval_time = -1
                        if "grafana" in al_data and "data_points" in al_data["grafana"] and al_data["grafana"]["data_points"] == "fixed":
                            # Grafana is configured: data_points" : "fixed"
                            self.fixed_points = True # Use fixed data points in increment function

                if "interval" in al_data and al_data["interval"] != "optimize":
                    # Overwrite the interval optimized/default intervals provided by AnyLog
                    interval = al_data["interval"].strip()

                    if len(interval) > 5:
                        if interval == "dashboard" and self.grafana_interval_unit:
                            # Take the interval from Grafana's dashboard
                            self.interval_time = int(self.grafana_interval_time)
                            self.interval_unit = self.grafana_interval_unit
                        else:
                            if interval[-1] == 's':
                                # remove plural
                                interval = interval[:-1]
                            index = interval.find(' ')      # The space between the number and units
                            if index > 0:
                                if interval[:index].isnumeric():
                                    interval_unit = interval[index+1:].lstrip()
                                    if interval_unit in utils_sql.increment_date_types:
                                        self.interval_unit = interval_unit
                                        self.interval_time = int(interval[:index])


                if "sql" in al_data.keys():
                    self.provided_sql = True        # SQL provided from the dashboards - process reply as increment or period
                    if not self.request_type:
                        # can be set on increment or period
                        self.request_type = "sql"   # Process reply as a SQL query

                    self.sql_query = True
                    self.sql_stmt = str(al_data["sql"]).strip()

                    if len(self.sql_stmt) < 10:
                        return [process_status.Failed_to_parse_sql, "Error in SQL stmt format"]

                    if self.sql_stmt[-1] == ';':
                        self.sql_stmt = self.sql_stmt[:-1]  # remove the end of stmt comma as the stmt can be extended

                    # get the table name in lower case to find the table name location and the WHERE location
                    sql_lower = self.sql_stmt.lower()  # Can not change self.sql_stmt to lower - some values needs to be in capital letters
                    if sql_lower[:7] != "select ":
                        return [process_status.Failed_to_parse_sql, "Failed to identify 'select' in SQL stmt"]

                    # Test if increments
                    if not self.default_query:
                        # User did nit specify "type" : "increment" in the payload
                        for index in range(7, len(sql_lower)):
                            if sql_lower[index] != ' ':
                                if sql_lower[index:index+10] == "increments":
                                    self.default_query = True
                                    self.request_type = "increments"
                                break

                    if not table_provided:
                        # get the table name from the sql statement
                        self.table_name = utils_sql.get_select_table_name(sql_lower)  # Table name mention explicitly in the SQL
                    else:
                        # change the table name pn the SQL using the table name in the JSON
                        offset_start, offset_end = utils_sql.get_offsets_table_name(sql_lower)
                        if offset_start > 0 and offset_end > 0:
                            self.sql_stmt = self.sql_stmt[:offset_start] + self.table_name + self.sql_stmt[
                                                                                             offset_end:]

                    self.offset_where = sql_lower.find(" where ")  # offset to the where condition
                    if self.offset_where > -1:
                        self.offset_where += 7  # to the first word after the where
                    else:
                        # Get offset for adding a where condition
                        self.offset_for_where = utils_sql.get_offset_after_table_name(sql_lower)
                    if self.interval_time > 0:
                        self.increment_offset = sql_lower.find("increments(),")  # increment without data
                if "member" in al_data.keys():
                    # A list, each entry is a member
                    self.members = al_data["member"]
                if "metric" in al_data.keys():
                    # A list, each entry is a number representing a color on the map
                    if isinstance(al_data["metric"], list):
                        if all(isinstance(x,int) for x in al_data["metric"]):
                            self.metric = al_data["metric"]
                if "attribute" in al_data.keys():
                    if isinstance(al_data["attribute"], list):
                        if all(isinstance(x, str) for x in al_data["attribute"]):
                            self.attribute = al_data["attribute"]
                if "where" in al_data.keys():
                    self.where_cond = str(al_data["where"])
                if "functions" in al_data.keys() and isinstance(al_data["functions"],list):
                    if len(al_data["functions"]):
                        self.functions = []
                        for function in al_data["functions"]:       # Specify Min, Max etc. to overwrite the default
                            self.functions.append(function.lower())
                if "time_column" in al_data.keys():
                    self.column_time = str(al_data["time_column"])
                if "value_column" in al_data.keys():
                    values =  al_data["value_column"]
                    if isinstance(values, list):
                        # multiple_columns
                        self.column_value = ' and value_column = '.join(values)
                    else:
                        self.column_value = str(al_data["value_column"])
                if "limit" in al_data.keys():
                    if isinstance(al_data["limit"], int):
                        self.user_limit = al_data["limit"]
                if "servers" in al_data.keys():
                    servers = al_data["servers"]
                    if isinstance(servers,str):
                        self.servers = servers
                    elif isinstance(servers, list):
                        self.servers = ",".join(map(str, servers))
                    else:
                        err_msg = f"Error in 'servers' list: '{servers}' is not a valid servers list"
                        status.add_error(err_msg)
                        ret_val = process_status.ERR_command_struct
                        return [ret_val, err_msg]

                if "instructions" in al_data.keys():
                    self.instructions = str(al_data["instructions"])
                if "time_range" in al_data.keys():
                    if isinstance(al_data["time_range"], bool):
                        self.with_time_range = al_data["time_range"]
                if "trace_level" in al_data.keys():
                    if isinstance(al_data["trace_level"], int):
                        self.trace_level = al_data["trace_level"]


                if self.request_type == "info" and "details" in al_data.keys():
                    # Any other command
                    self.details = str(al_data["details"])
                    self.timeseries = False  # The info is data from the network
                elif self.request_type == "aggregations":
                    # Aggregation function
                    self.column_time = "timestamp"
                    self.timeseries = True
                    if not self.functions:
                        self.functions = aggregation_func_      # Use all functions
                        func_str = ""
                    else:
                        # User defined functions
                        missing = [col_name for col_name in self.functions if col_name not in aggregation_func_]
                        if missing:
                            err_msg = f"Error in aggregation function - '{missing}' is not a valid aggregation function, use: {aggregation_func_}  "
                            status.add_error(err_msg)
                            ret_val = process_status.ERR_command_struct
                            return [ret_val, err_msg]

                        func_str = "and " + " and ".join(f"function = {col}" for col in self.functions) + " "

                    value_column_name = self.get_value_column()     # COuld be one or more
                    if not value_column_name:
                        value_column_name = "value"     # Default

                    limit = self.get_user_limit()
                    if limit:
                        limit_stmt = f" and limit = {limit}"
                    else:
                        limit_stmt = ""

                    self.details = f"get aggregations by time where dbms = {self.dbms_name} and table = {self.table_name} and value_column = {value_column_name} {func_str}and timezone = utc and format = json{limit_stmt}"

        if self.request_type != "aggregations":     # With aggregations column names are known
            # If column names not provided - try best guess
            if not self.column_time or (not self.column_value and not self.sql_stmt):
                if self.dbms_name and self.table_name:
                    ret_val, self.column_time, self.column_value = member_cmd.get_time_value_columns(status, self.dbms_name,
                                                                                                     self.table_name,
                                                                                                     self.column_time,
                                                                                                     self.column_value,
                                                                                                     self.trace_level)

        return [ret_val, err_msg]

    # =======================================================================================================================
    # Return True for time series data
    # =======================================================================================================================
    def is_tsd(self):
        return self.timeseries

    # =======================================================================================================================
    # Return True for default query
    # =======================================================================================================================
    def is_default_query(self):
        return self.default_query

    # =======================================================================================================================
    # Return the time range of the query
    # =======================================================================================================================
    def get_time_range(self):
        return [self.time_start, self.time_end]

    # =======================================================================================================================
    # Return true if time range is to be considered
    # =======================================================================================================================
    def use_time_range(self):
        return self.with_time_range

    # =======================================================================================================================
    # For a Time Series Query - Get the interval unit (seconds, minutes, etc.) and value
    # =======================================================================================================================
    def get_interval_info(self):
        return [self.interval_unit, self.interval_time]

    # =======================================================================================================================
    # For an increment query - the info for fixed points display
    # =======================================================================================================================
    def get_fixed_points_info(self):
        return [self.fixed_points, self.grafana_start_ms, self.grafana_end_ms, self.grafana_interval_ms]    # Used in increments if Grafana is configured: data_points" : "fixed"

    # =======================================================================================================================
    # For a Time Series Query - Return the name of the time and value columns
    # =======================================================================================================================
    def get_time_value_columns_names(self):
        return [self.column_time, self.column_value]

    # =======================================================================================================================
    # Return the name of the time column 0 - it identifies the name of the time column in timeseries reply
    # =======================================================================================================================
    def get_time_column(self):
        return self.column_time
    # =======================================================================================================================
    # Return the name of the column identified as the value_column
    # =======================================================================================================================
    def get_value_column(self):
        return self.column_value

    # =======================================================================================================================
    # Return the number of data points in an increment function
    # =======================================================================================================================
    def get_data_points(self):
        return self.data_points

    # =======================================================================================================================
    # Return the dbms name and table name
    # =======================================================================================================================
    def get_dbms_table(self):
        return [self.dbms_name, self.table_name]

    # =======================================================================================================================
    # Return the table name
    # =======================================================================================================================
    def get_table_name(self):
        return self.table_name

    # =======================================================================================================================
    # Max rows returned to Grafana = a grafana limit
    # =======================================================================================================================
    def get_limit(self):
        return self.limit

    # =======================================================================================================================
    # A user limit in the payload
    # =======================================================================================================================
    def get_user_limit(self):
        return self.user_limit

    # =======================================================================================================================
    # Additional info to the wgere condition
    # =======================================================================================================================
    def get_where_cond(self):
        return self.where_cond

    # =======================================================================================================================
    # Overwite the default Min, Max Avg
    # =======================================================================================================================
    def get_functions(self):
        return self.functions

    # =======================================================================================================================
    # Return the SQL stmt provided by the user
    # =======================================================================================================================
    def get_user_stmt(self):
        return self.sql_stmt

    # =======================================================================================================================
    # Determins the type of request:
    # a) SQL
    # b) prebuild queries (like: increments or period)
    # c) Info requests
    # =======================================================================================================================
    def get_request_type(self):
        return self.request_type

    # =======================================================================================================================
    # Test if the SQL was provided from the Grafana JSON Payload
    # =======================================================================================================================
    def is_provided_sql(self):
        return self.provided_sql

    # =======================================================================================================================
    # Return the offset of the where condition
    # =======================================================================================================================
    def get_offset_where(self):
        return self.offset_where

    # =======================================================================================================================
    # In the case that the sql has no where condition - Return the offset to add a WHERE Condition
    # =======================================================================================================================
    def get_offset_add_where(self):
        return self.offset_for_where

    # =======================================================================================================================
    # If the SQL contains increment without info, will be added by system
    # =======================================================================================================================
    def get_increment_offset(self):
        return self.increment_offset

    # =======================================================================================================================
    # Get the IP and Port of specific destination servers to use
    # =======================================================================================================================
    def get_destination_servers(self):
        return self.servers

    # =======================================================================================================================
    # Get a request which is not SQL
    # =======================================================================================================================
    def get_details(self):
        return self.details

    # =======================================================================================================================
    # Get the database name
    # =======================================================================================================================
    def get_dbms_name(self):
        return self.dbms_name
    # =======================================================================================================================
    # Get the timezone
    # =======================================================================================================================
    def get_timezone(self):
        return "utc"            # return self.timezone
    # =======================================================================================================================
    # Get a list of keys representing information added to the query.
    # For example @table_name
    # =======================================================================================================================
    def get_extend(self):
        return self.extend_keys

    # =======================================================================================================================
    # Get a list of tables to be considered like the main table
    # =======================================================================================================================
    def get_include(self):
        return self.include_tables

    # =======================================================================================================================
    # Return True for SQL query over the user data
    # =======================================================================================================================
    def is_sql_query(self):
        return self.sql_query

    # =======================================================================================================================
    # Update a dictionaru with statements executed - rteturn False if the statement exists in the dictionary
    # =======================================================================================================================
    def new_statement(self, stmt):
        if stmt in self.executed_stmt.keys():
            ret_val = False  # This query was executed
        else:
            ret_val = True
            self.executed_stmt[stmt] = 1
        return ret_val

    # =======================================================================================================================
    # Return the type of member
    # =======================================================================================================================
    def get_members_types(self):
        return self.members

    # =======================================================================================================================
    # Return an array of metrics - an integer value that determines the color
    # =======================================================================================================================
    def get_nodes_metric(self):
        return self.metric

    # =======================================================================================================================
    # Return an array of attribute names - an attribute names that determine what is displayed when cursor hovers on the circle
    # =======================================================================================================================
    def get_nodes_attr(self):
        return self.attribute

    # =======================================================================================================================
    # Return an array with info on member nodes
    # =======================================================================================================================
    def get_nodes_info(self):
        return self.nodes_info

    # =======================================================================================================================
    # Get the trace level
    # =======================================================================================================================
    def get_trace_level(self):
        return self.trace_level

# -----------------------------------------------------------------------------------
# AnyLog JSON Connector to Grafana
# -----------------------------------------------------------------------------------
def grafana_get(status, request_handler):
    ret_val = send_preamble(status, request_handler)

    if not ret_val:
        access_ctr = 'Access-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\nAccess-Control-Allow-Headers: Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token\r\nContent-Length: 2\r\nContent-Type: text/html; charset=UTF-8\r\n\r\n'
        info_string = access_ctr.encode('iso-8859-1')
        ret_val = utils_io.write_encoded_to_stream(status, request_handler.wfile, info_string)

        if not ret_val:
            info_string = "OK".encode('iso-8859-1')
            ret_val = utils_io.write_encoded_to_stream(status, request_handler.wfile, info_string)

    return ret_val


# -----------------------------------------------------------------------------------
# POST REQUEST - request to publish list of tables or data per table
# Example data points written:
# b'Content-Type: application/json\r\nContent-Length: 38399\r\n\r\n'
# b'["series A", "series B"]'
# b'[{"target": "series B", "datapoints": [[-0.5799358614273129, 1598992658000],  [-0.9281617508387254, 1599014215647], [0.18279045401831143, 1599014236823]]}]'
# -----------------------------------------------------------------------------------
def grafana_post(status, request_handler, decode_body, timeout):
    send_preamble(status, request_handler)

    if request_handler.path.startswith("/search") or request_handler.path.startswith("/metrics"):
        # RETURN A LIST OF TABLES for the DBMS - /search is used in Grafana V7 and metrics in V8
        dbms_name = get_info_from_headers(request_handler.headers._headers, "al.dbms.")
        if not dbms_name:
            tables_array = get_dbms_table_list(status)
        else:
            tables_array = get_table_list(status, dbms_name)
            if not tables_array:
                # Ge t all databases and tables
                tables_array = get_dbms_table_list(status)

        # Organize the list of tables in a string with the following format: '["table A", "table B"]'
        if not tables_array:
            data_tables = '["Error: No table connected"]'
        else:
            data_tables = "["
            for index, table_name in enumerate(tables_array):
                if index:
                    data_tables += ", \"" + table_name + "\""
                else:
                    data_tables += "\"" + table_name + "\""
            data_tables += "]"

        ret_val = stream_data_to_grafana(status, request_handler, data_tables)

    elif request_handler.path.startswith("/query"):
        # RETURN DATA
        dbms_name = get_info_from_headers(request_handler.headers._headers, "al.dbms.")

        ret_val, data_str = process_queries(status, dbms_name, request_handler, decode_body, timeout)  # process one or more queries

        if ret_val:
            # Return an error message - data_str is the error message
            status_txt = process_status.get_status_text(ret_val)
            data_str = f'{{"message": "{data_str}", "status": "{status_txt} ({ret_val})"}}'

        ret_val = stream_data_to_grafana(status, request_handler, data_str)

    else:
        grafana_request = utils_json.str_to_json(decode_body)
        if isinstance(grafana_request,dict) and "payload" in grafana_request:
            ret_val, data_str = process_other_request(status, grafana_request)

            if ret_val:
                status_txt = process_status.get_status_text(ret_val)
                data_str = f'{{"message": "{data_str}", "status": "{status_txt} ({ret_val})"}}'

            ret_val = stream_data_to_grafana(status, request_handler, data_str)

        else:
            ret_val = process_status.REST_call_err

        if ret_val:
            status.add_error("[Grafana API Error] [Unrecognized key provided by Grafana to POST request: '%s']" % request_handler.path)

    return ret_val
# -----------------------------------------------------------------------------------
# Get the list of tables for all database
# -----------------------------------------------------------------------------------
def get_dbms_table_list(status):
    get_cmd = "blockchain get table bring.unique ['table']['dbms'] \".\" ['table']['name'] separator = ,"
    ret_value, tables = member_cmd.blockchain_get(status, get_cmd.split(), "", True)
    if not tables:
        tables_array = None
    else:
        tables_array = tables.split(',')
        tables_array.sort()
    return tables_array


# -----------------------------------------------------------------------------------
# Get the list of tables for a specific database
# -----------------------------------------------------------------------------------
def get_table_list(status, dbms_name):
    get_cmd = "blockchain get table where dbms = %s bring ['table']['name'] separator = ," % dbms_name
    ret_value, tables = member_cmd.blockchain_get(status, get_cmd.split(), "", True)
    if not tables:
        tables_array = None
    else:
        tables_array = tables.split(',')
        tables_array.sort()
    return tables_array


# -----------------------------------------------------------------------------------
# Stream headers and data to Grafana
# -----------------------------------------------------------------------------------
def stream_data_to_grafana(status, request_handler, data_str):
    data_encoded = data_str.encode('iso-8859-1')

    headers = 'Content-Type: application/json\r\nContent-Length: %u\r\n\r\n' % len(data_encoded)

    headers_encoded = headers.encode('iso-8859-1')

    # send headers with length
    ret_val = utils_io.write_encoded_to_stream(status, request_handler.wfile, headers_encoded)

    if not ret_val:
        ret_val = utils_io.write_encoded_to_stream(status, request_handler.wfile, data_encoded)

    return ret_val



# -----------------------------------------------------------------------------------
# Send preamble headers to Grafana
# -----------------------------------------------------------------------------------
def send_preamble(status, request_handler):
    # Protocol version
    if hasattr(request_handler, 'protocol_version'):
        connection = request_handler.protocol_version + " 200 OK\r\n"
    else:
        connection = 'HTTP/1.0 200 OK\r\n'

    info_string = connection.encode('iso-8859-1')
    ret_val = utils_io.write_encoded_to_stream(status, request_handler.wfile, info_string)

    if not ret_val:
        # date and time
        date_str = 'Date: %s\r\n' % format_date_time(time.time())

        info_string = date_str.encode('iso-8859-1')
        ret_val = utils_io.write_encoded_to_stream(status, request_handler.wfile, info_string)

        if not ret_val:
            # Software version
            server = 'Server: WSGIServer/0.2 CPython/3.8.3\r\n'
            server = 'Server: AnyLog/1.2\r\n'

            info_string = server.encode('iso-8859-1')
            ret_val = utils_io.write_encoded_to_stream(status, request_handler.wfile, info_string)

    return ret_val


# =======================================================================================================================
# Get info from REST headers
# With Grafana, we use header to provide info. For example: al.dbms.lsl_demo -> we search for "al.dbms." and return -> lsl_demo
# =======================================================================================================================
def get_info_from_headers(input_headers, key_prefix):
    key_sufix = None
    for entry in input_headers:
        if entry[0].lower().startswith(key_prefix):
            key_sufix = entry[0][len(key_prefix):]
            break
    return key_sufix


# =======================================================================================================================
# Organize the reply data in the Grafana format for time-series
# Example: data_str = '[{"target": "series B", "datapoints": [[-0.5799358614273129, 1598992658000], [-0.9281617508387254, 1599014215647], [0.18279045401831143, 1599014236823]]}]'
# Explained here - https://grafana.com/grafana/plugins/simpod-json-datasource
# Internal DOc - edge_lake/api/grafana/GRAFANA.md
# =======================================================================================================================
def set_timeseries_struct(status, dbms_name, table_name, query_params, reply_data):
    j_handle = status.get_active_job_handle()
    select_parsed = j_handle.get_select_parsed()
    title_list = select_parsed.get_query_title()
    data_types = select_parsed.get_query_data_types()
    grafana_data_types = []


    # Organize the data
    reply_json = utils_json.str_to_json(reply_data)

    if reply_json:
        rows = reply_json["Query"]
    else:
        rows = None

    for entry in data_types:
        grafana_data_types.append(get_grafana_data_type(entry))

    base_column_id = -1  # The ID of the time column, or ID of a column used for the X Axies
    is_time = False

    # Identify the base column - time column, or without the time column, the x column

    # Option 1 - the time column is provided by the AnyLog JSON script using "time_column"
    base_column_name = query_params.get_time_column()

    if base_column_name:
        for index, column_name in enumerate(title_list):
            # test for a column name or a function over the column name
            if column_name == base_column_name or (column_name.find("(" + base_column_name + ")") > 0):
                # with time column that can be graphed
                base_column_name = column_name
                base_column_id = index
                is_time = True  # X axis is time value
                break


    # Option 2 find a field with time data type
    if base_column_id == -1 and rows and len(rows):
        # Take the first entry - and see if any column includes time
        first_row = rows[0]
        for index, attr_name in enumerate(first_row.keys()):
            grafana_dt = grafana_data_types[index]
            if grafana_dt.startswith("time"):
                base_column_name = attr_name
                base_column_id = index
                is_time = True  # X axis is time value
                break

    return get_timeseries_response(status, dbms_name, table_name, title_list, None, grafana_data_types, rows, base_column_name, base_column_id, is_time)

# --------------------------------------------------------------------------------------------------------
# Reformat aggregation reply to a query result set
# --------------------------------------------------------------------------------------------------------
def reformat_aggregations(status, is_dest_servers, dbms_name, table_name, reply_data):
    '''
    is_dest_servers - True if was sent to a server and reply contains server ID
    '''
    gr_str= ""
    reply_json = utils_json.str_to_json(reply_data)
    if reply_json and len(reply_json):
        query_list = []         # Organize aggregation output as a query output

        if is_dest_servers:
            node_data = utils_json.get_inner(reply_json)        # Remove the node ID
        else:
            node_data = reply_json

        if len(node_data):                                  # With data received from the node
            target_counter = 0
            for target, agg_values in node_data.items():        # Target is the column name
                if not len(agg_values):
                    break       # No data - only date

                first_entry = agg_values[0]
                title_list = list(first_entry.keys())         # Keys in each row
                project_names = ["timestamp" if entry == "timestamp" else (f"{entry}({target})") for entry in first_entry]   # Names to project
                grafana_data_types = ["timestamp"] + ["number"] * (len(title_list) - 1)

                rows = []
                index = 1
                for single_entry in agg_values:
                    columns = {}
                    columns[target] = index
                    index += 1
                    for name, value in single_entry.items():
                        columns[name] = value
                    rows.append(columns)

                if target_counter:
                    gr_str += ", "

                gr_str += get_timeseries_response(status, dbms_name, table_name, title_list, project_names, grafana_data_types, rows, "timestamp", 0, True)
                target_counter += 1
    return gr_str
# --------------------------------------------------------------------------------------------------------
# Organize the timeseries response
'''
Example timeseries response - pairs of values - the first is the Y value (measured) the second is the X value (time value OR group by value)

[
  {
    "target":"upper_75", // The field being queried for
    "datapoints":[
      [622,1450754160000],  // Metric value as a float , unixtimestamp in milliseconds
      [365,1450754220000]
    ]
  },
  {
    "target":"upper_90",
    "datapoints":[
      [861,1450754160000],
      [767,1450754220000]
    ]
  }
]
'''
# --------------------------------------------------------------------------------------------------------
def get_timeseries_response(status, dbms_name, table_name, title_list, project_names, grafana_data_types, rows, base_column_name, base_column_id, is_time):
    '''
    status - the status object
    dbms_name
    table_name
    title_list:list - the names of the columns
    project_names: list - optional, names to return to grafana.
    grafana_data_types:list - the grafana data types per each column
    rows - the list of rows
    base_column_name - the x-axis column name
    base_column_id - the x-axis column id
    is_time - is x-axis represents timestamp
    '''

    targets = []
    if base_column_id == -1:
        # Time field was not identified
        error_msg = "[Grafana] [Query Error] [Timeseries column not identified] [Table: %s.%s] [Include 'time_column' attribute and value in the Dashboard's JSON Payload]" % (dbms_name, table_name)
        status.add_error(error_msg)
    else:
        error_msg = ""
        # determibe the list of columns that are to be send to Grafana
        if rows:
            first = True
            for row_counter, row in enumerate(rows):
                if base_column_name not in row:
                    continue
                base_value = row[base_column_name]
                if is_time:
                    x_value = utils_columns.get_utc_to_ms(base_value)     # timestamp to milliseconds
                else:
                    x_value = f"\"{base_value}\""

                for index, column_name in enumerate(title_list):
                    if index == base_column_id:
                        # Ignore this colummn as it is the X axis (time, or base)
                        #if first:
                        targets.append(None)       # This entry is the base (time or X axis) - not in use
                        continue

                    column_value = row[column_name]
                    if isinstance(column_value,bool):
                        column_value = "1" if column_value else "0"     # change bool to 0 / 1
                    elif isinstance(column_value,str):
                        column_value = "0" if (not column_value or column_value == "False") else "1"
                    grafana_dt = grafana_data_types[index]
                    if grafana_dt == "string":
                        column_value = f"\"{column_value}\""
                    if row_counter:
                        targets[index] += (f", [{column_value}, {x_value}]")
                    else:
                        # First row
                        if is_time:
                            target_name = project_names[index] if project_names else title_list[index]    # The field being queried for
                        else:
                            if base_column_name in row:
                                target_name = base_column_name
                            else:
                                target_name = title_list[index]  # The field being queried for

                        if first:
                            # First set
                            # Without a prefixed comma
                            targets.append('{"target": "%s", "datapoints": [' % target_name)
                            first = False
                        else:
                            targets.append(', {"target": "%s", "datapoints": [' % target_name)

                        targets[index] += (f"[{column_value}, {x_value}]")


    if len(targets):
        gr_str = ""
        for target in targets:
            if target != None:
                gr_str += target + ']}'
    else:
        # No results retuned
        gr_str = set_grafana_error(dbms_name, table_name, error_msg)

    return gr_str

# =======================================================================================================================
# Set an error message that is returned to Grafana - Query inspector / Refresh
# =======================================================================================================================
def set_grafana_error(dbms_name, table_name, error_msg):
    return f'{{"target": \"{dbms_name}.{table_name}", "datapoints": [], "error" : "{error_msg}" }}'


# =======================================================================================================================
# Return the Grafana time series stat with the extended information
# Example string returned:
# {"target": "t99<avg>", "datapoints": [[32.85971006253553,1744945516387], [19.105,1745016747623], [31.436796116504848,1745018351697], [32.055238095238096,1745044903053]]},{"target": "t99<min>", "datapoints": [[0.02,1744945516387], [8.65,1745016747623], [0.39,1745018351697], [0.03,1745044903053]]},{"target": "t99<max>", "datapoints": [[62.9,1744945516387], [29.56,1745016747623], [63.75,1745018351697], [63.37,1745044903053]]}
# =======================================================================================================================
def set_extended_timeseries_struct(table_name, extend, reply_data, functions, is_fixed_points, grafana_start_time, grafana_end_time, interval_time, trace_level):
    '''
    table_name - the table to process
    extend - the list of extended info, for example: @dbms_name, @table_name
    reply_data - the data returned by the dbms
    functions - the functions returned to grafana
    is_fixed_points - if "data_points" are set to "fixed" in the grafana JSON - using the grafana points vs. AnyLog points
    grafana_start_time - the start time by grafana
    interval_time - the interval time by grafana
    '''

    if extend:
        extend_list = extend[1:-1].split(',')
        keys_list =  [item[1:] for item in extend_list]     # These are the column names with the additional info
    else:
        keys_list = None

    if not functions:
        # The default: Min Max Avg
        with_min = True
        with_max = True
        with_avg = True
        with_range = False
        with_count = False
    else:
        with_min = "min" in functions
        with_max = "max" in functions
        with_avg = "avg" in functions
        with_range = "range" in functions
        with_count = "count" in functions

    prep_struct = {}

    reply_json = utils_json.str_to_json(reply_data)

    if reply_json:
        rows = reply_json["Query"]
        index = -1  # Needed for the trace command below if no data returned

        for index, entry in enumerate(rows):
            attr_time = entry["timestamp"]
            # attr_ms = utils_columns.get_ms_from_date_time(attr_time)

            attr_ms = utils_columns.get_utc_to_ms(attr_time)

            if keys_list:
                # Could be the table name + dbms name
                info_key = ".".join(str(entry[key]) for key in keys_list)
            else:
                info_key = table_name   # all is of the same table


            if not info_key in prep_struct:
                # First time
                info_bucket = {}
                info_bucket["counter"] = 0
                prep_struct[info_key] = info_bucket
                info_bucket["grafana_time"] = grafana_start_time

                if with_avg:
                    info_bucket["gr_avg"] = {
                        "target" : f"{info_key}.<avg>",
                        "datapoints" : []
                    }
                if with_min:
                    info_bucket["gr_min"] = {
                        "target": f"{info_key}.<min>",
                        "datapoints": []
                    }
                if with_max:
                    info_bucket["gr_max"] = {
                        "target": f"{info_key}.<max>",
                        "datapoints": []
                    }
                if with_range:
                    info_bucket["gr_range"] = {
                        "target": f"{info_key}.<range>",
                        "datapoints": []
                    }
                if with_count:
                    info_bucket["gr_count"] = {
                        "target": f"{info_key}.<count>",
                        "datapoints": []
                    }
                    # gr_count = '{"target": "%s", "datapoints": [' % (table_name + "<count>")  # Count values string

            info_bucket = prep_struct[info_key]       # The new setup for this extended info
            info_bucket["counter"] += 1               # count instances with this info key
            grafana_time = info_bucket["grafana_time"]

            if is_fixed_points:
                # return fixed points - regardless if dbms includes the data or not
                while (grafana_time + interval_time) <= attr_ms:
                    # return nulls for missing info
                    if info_bucket["counter"] > 1:      # Ignore the first entry
                        if with_avg:
                            info_bucket["gr_avg"]["datapoints"].append([None, grafana_time])
                        if with_min:
                            info_bucket["gr_min"]["datapoints"].append([None, grafana_time])
                        if with_max:
                            info_bucket["gr_max"]["datapoints"].append([None, grafana_time])
                        if with_range:
                            info_bucket["gr_range"]["datapoints"].append([None, grafana_time])
                        if with_count:
                            info_bucket["gr_count"]["datapoints"].append([None, grafana_time])

                    grafana_time += interval_time


                attr_ms = grafana_time      # use the grafana time
                grafana_time += interval_time
                info_bucket["grafana_time"] = grafana_time

            try:
                if with_avg:
                    attr_avg = entry["avg_val"]
                    info_bucket["gr_avg"]["datapoints"].append([attr_avg, attr_ms])
                if with_min:
                    attr_min = entry["min_val"]
                    info_bucket["gr_min"]["datapoints"].append([float(attr_min), attr_ms])
                if with_max:
                    attr_max = entry["max_val"]
                    info_bucket["gr_max"]["datapoints"].append([float(attr_max), attr_ms])
                if with_range:
                    attr_range = entry["range_val"]
                    info_bucket["gr_range"]["datapoints"].append([float(attr_range), attr_ms])
                if with_count:
                    attr_count = entry["count_val"]
                    info_bucket["gr_count"]["datapoints"].append([attr_count, attr_ms])
            except:
                return ""


        if trace_level > 1:
            utils_print.output("\r\n[Grafana] [increments returned %u rows]" % (index + 1), True)

    gr_str = ""
    is_first = True
    for target_info in prep_struct.values():
        # create the grafana info for each table returned (note multiple tables are returned with include and extend)
        for target_key, target_values in target_info.items():
            if not target_key.startswith("gr_"):
                continue        # Not info returned to grafana
            if is_first:
                is_first = False
            else:
                gr_str += ','

            gr_str += f'{{"target": "{target_values["target"]}", "datapoints": '
            datapoints =  target_values["datapoints"]
            gr_str += f"{datapoints}"
            gr_str += "}"

    if is_fixed_points:
        gr_str = gr_str.replace(" [None,", " [null,")

    return gr_str  # return string in grafana format

# =======================================================================================================================
# Organize the reply data (to the default query) in the Grafana format to time series view
# Example: data_str = '[{"target": "series B", "datapoints": [[-0.5799358614273129, 1598992658000], [-0.9281617508387254, 1599014215647], [0.18279045401831143, 1599014236823]]}]'
# Explained here - https://grafana.com/grafana/plugins/simpod-json-datasource
# =======================================================================================================================
def set_default_timeseries_struct(table_name, reply_data, functions, is_fixed_points, grafana_start_time, grafana_end_time, interval_time, trace_level):
    '''
    table_name - the table to process
    reply_data - the data returned by the dbms
    functions - the functions returned to grafana
    is_fixed_points - if "data_points" are set to "fixed" in the grafana JSON - using the grafana points vs. AnyLog points
    grafana_start_time - the start time by grafana
    interval_time - the interval time by grafana
    '''

    if not functions:
        # The default: Min Max Avg
        with_min = True
        with_max = True
        with_avg = True
        with_range = False
        with_count = False
    else:
        with_min = "min" in functions
        with_max = "max" in functions
        with_avg = "avg" in functions
        with_range = "range" in functions
        with_count = "count" in functions

    if with_avg:
        gr_avg = '{"target": "%s", "datapoints": [' % (table_name + "<avg>")  # Average values string
    if with_min:
        gr_min = '{"target": "%s", "datapoints": [' % (table_name + "<min>")  # Min values string
    if with_max:
        gr_max = '{"target": "%s", "datapoints": [' % (table_name + "<max>")  # Max values string
    if with_range:
        gr_range = '{"target": "%s", "datapoints": [' % (table_name + "<range>")  # Max - Min values string
    if with_count:
        gr_count = '{"target": "%s", "datapoints": [' % (table_name + "<count>")  # Count values string

    grafana_time = grafana_start_time
    reply_json = utils_json.str_to_json(reply_data)

    if reply_json:
        rows = reply_json["Query"]
        index = -1  # Needed for the trace command below if no data returned

        comma = ""      # No comma with first entries
        for index, entry in enumerate(rows):
            attr_time = entry["timestamp"]
            # attr_ms = utils_columns.get_ms_from_date_time(attr_time)

            attr_ms = utils_columns.get_utc_to_ms(attr_time)

            if is_fixed_points:
                # return fixed points - regardless if dbms includes the data or not
                while (grafana_time + interval_time) <= attr_ms:
                    # return nulls for missing info
                    if comma:
                        if with_avg:
                            gr_avg += f"{comma}[null,{grafana_time}]"
                        if with_min:
                            gr_min += f"{comma}[null,{grafana_time}]"
                        if with_max:
                            gr_max += f"{comma}[null,{grafana_time}]"
                        if with_range:
                            gr_range += f"{comma}[null,{grafana_time}]"
                        if with_count:
                            gr_count += f"{comma}[null,{grafana_time}]"

                    grafana_time += interval_time


                attr_ms = grafana_time      # use the grafana time
                grafana_time += interval_time


            if with_avg:
                attr_avg = entry["avg_val"]
            if with_min:
                attr_min = entry["min_val"]
            if with_max:
                attr_max = entry["max_val"]
            if with_range:
                attr_range = entry["range_val"]
            if with_count:
                attr_count = entry["count_val"]

            if with_avg:
                gr_avg += f"{comma}[{attr_avg},{attr_ms}]"
            if with_min:
                gr_min += f"{comma}[{attr_min},{attr_ms}]"
            if with_max:
                gr_max += f"{comma}[{attr_max},{attr_ms}]"
            if with_range:
                gr_range += f"{comma}[{attr_range},{attr_ms}]"
            if with_count:
                gr_count += f"{comma}[{attr_count},{attr_ms}]"

            if not index:
                comma = ", "


        if trace_level > 1:
            utils_print.output("\r\n[Grafana] [increments returned %u rows]" % (index + 1), True)

    if with_avg:
        gr_str = gr_avg + "]},"
    else:
        gr_str = ""
    if with_min:
        gr_str +=  gr_min + "]},"
    if with_max:
        gr_str +=  gr_max + "]},"
    if with_range:
        gr_str += gr_range + "]},"
    if with_count:
        gr_str += gr_count + "]}"
    else:
        gr_str = gr_str[:-1]    # Remove suffix comma

    return gr_str  # return string in grafana format
# =======================================================================================================================
# Organize the reply data (to the default query) in the Grafana format to time series view
# Example: data_str = '[{"target": "series B", "datapoints": [[-0.5799358614273129, 1598992658000], [-0.9281617508387254, 1599014215647], [0.18279045401831143, 1599014236823]]}]'
# Explained here - https://grafana.com/grafana/plugins/simpod-json-datasource

# THIS IS AN Increment function specified by the user in a "SQL" attribute in the Grafana Payload
# =======================================================================================================================
def set_sql_timeseries_struct(status, table_name, reply_data, is_fixed_points, grafana_start_time, grafana_end_time, interval_time, trace_level):
    '''
    table_name - the table to process
    reply_data - the data returned by the dbms
    time_column - the time column name
    is_fixed_points - if "data_points" are set to "fixed" in the grafana JSON - using the grafana points vs. AnyLog points
    grafana_start_time - the start time by grafana
    interval_time - the interval time by grafana
    '''


    grafana_time = grafana_start_time
    reply_json = utils_json.str_to_json(reply_data)
    response = []

    if reply_json:
        rows = reply_json["Query"]
        if len(rows):
            # With data
            first_row = rows[0]
            if len(first_row) < 2:
                status.add_error(f"Missing columns in 'increments' query projection list (at least time column and one value column are required)")
                return ""

            for index, col_name in enumerate(first_row):
                # Go over the columns which are not the timestamp
                if not index:
                    time_column = col_name
                    continue    # Skip the time column
                response.append({
                    "target" : f"{col_name}",
                    "datapoints": []
                })


            # Go over all rows
            for index, entry in enumerate(rows):
                attr_time = entry[time_column]
                attr_ms = utils_columns.get_utc_to_ms(attr_time)

                target_id = 0
                for col_name, col_val in entry.items():
                    # Go over the columns returned
                    if col_name == time_column:
                        continue
                    response_points = response[target_id]["datapoints"]
                    target_id+= 1

                    if is_fixed_points:
                        # return fixed points - regardless if dbms includes the data or not
                        # Option: "data_points" : "fixed"  In the Payload
                        while (grafana_time + interval_time) <= attr_ms:
                            response_points.append(["null", grafana_time])
                            grafana_time += interval_time

                        attr_ms = grafana_time  # use the grafana time
                        grafana_time += interval_time

                    if isinstance(col_val,str):
                        try:
                            if '.' in col_val:
                                value =  float(col_val)
                            else:
                                value = int(col_val)
                        except:
                            value = col_val
                    else:
                        value = col_val


                    response_points.append([value, attr_ms])


        if trace_level > 1:
            utils_print.output("\r\n[Grafana] [increments returned %u rows]" % (len(rows)), True)

    gr_str = utils_json.to_string(response)

    return gr_str[1:-1]  # return syting in grafana format

# =======================================================================================================================
# Map the query data to the Grafana structure
# =======================================================================================================================
def set_sql_table_struct(status, dbms_name, table_name, reply_data):
    # Organize the metadata of the data transferred

    j_handle = status.get_active_job_handle()
    title_list = j_handle.get_select_parsed().get_query_title()

    gr_reply = '[{"columns":['

    query_dt = j_handle.get_select_parsed().get_query_data_types()

    for index, title in enumerate(title_list):
        source_data_type = query_dt[index]
        data_type = get_grafana_data_type(source_data_type)

        if index:
            gr_reply += f',{{"text":"{title}", "type":"{data_type}"}}'
        else:
            gr_reply += f'{{"text":"{title}", "type":"{data_type}"}}'

    gr_reply += '], "rows":['

    # Organize the data

    reply_json = utils_json.str_to_json(reply_data)

    if reply_json:
        rows = reply_json["Query"]
        for index1, entry in enumerate(rows):
            row_str = ""
            for index2, value in enumerate(entry.values()):
                if index2:
                    row_str += f',\"{value}\"'
                else:
                    row_str += f'\"{value}\"'
            if index1:
                gr_reply += f',[{row_str}]'
            else:
                gr_reply += f'[{row_str}]'

    gr_reply += '], "type":"table"}]'

    return gr_reply


# =======================================================================================================================
# Organize a JSON structure such that it can be provided as a table structure to Grafana
# =======================================================================================================================
def json_to_grafana_table(status, command, reply_data, multiple_servers):
    # multiple_servers - if reply is a list of multiple nodes replies

    if isinstance(reply_data, str):
        json_struct = utils_json.str_to_json(reply_data)
        if not json_struct:
            status.add_error("Grafana Interface - data provided is not representative of a JSON structure")
        if multiple_servers and isinstance(json_struct, dict):
            # it is organized as a dict and the reply from each node is a func (dict)
            updated_reply = []
            for node_ip, val in json_struct.items():
                if isinstance(val, dict):
                    val["source_node"] = node_ip
                    updated_reply.append(val)
                elif isinstance(val, list):
                    # For example: "get streaming where format = json" from multiple nodes returns a list
                    if not len(val):
                        val.append({})      # this entry would show a reply from this server
                    for entry in val:
                        if isinstance(entry, dict):
                            entry["source_node"] = node_ip
                            updated_reply.append(entry)

            json_struct = updated_reply
    elif isinstance(reply_data, list):
        json_struct = reply_data
    else:
        status.add_error("Grafana Interface - data provided is not in a recogbized format")
        json_struct = None

    if json_struct:
        gr_reply = '[{"columns":['

        # Get the Title List from the JSON instances
        title_list = []
        if isinstance(json_struct, dict):
            json_struct = [json_struct]
        if isinstance(json_struct, list):
            for entry in json_struct:
                if isinstance(entry, dict):
                    al_object = utils_json.get_inner(entry)  # get the AnyLog Object with the needed entries
                    if isinstance(al_object, dict):
                        for key in al_object.keys():
                            if key not in title_list:
                                title_list.append(key)
                    elif isinstance(al_object, str):
                        for key in entry.keys():
                            if key not in title_list:
                                title_list.append(key)

                if multiple_servers and title_list[-1] != "source_node":
                    # source_node is added after title is set
                    title_list.append("source_node")
                break # Get title from first entry

        if len(title_list):

            for index, title in enumerate(title_list):
                if index:
                    gr_reply += ',{"text":"%s", "type":"string"}' % title
                else:
                    gr_reply += '{"text":"%s", "type":"string"}' % title

        gr_reply += '], "rows":['

        # Organize the data

        if isinstance(json_struct, list):
            for index, entry in enumerate(json_struct):
                if isinstance(entry, dict):
                    al_object = utils_json.get_inner(entry)  # get the AnyLog Object with the needed entries
                    if isinstance(al_object, dict):
                        if "source_node" in entry:
                            # move from outside the iner JSON to the inside of the JSON:
                            # {'Queries Statistics': {'Up to  1 sec.': '0', 'Up to  2 sec.': '0', 'Up to  3 sec.': '0', 'Up to  4 sec.': '0', 'Up to  5 sec.': '0', 'Up to  6 sec.': '0', 'Up to  7 sec.': '0', 'Up to  8 sec.': '0', 'Up to  9 sec.': '0', 'Over   9 sec.': '0', 'Total queries': '0', 'Time interval': '222 (sec.) : 0:3:42 (H:M:S)'},
                            # 'source_node': '127.0.0.78:7848'}
                            source_node = entry["source_node"]
                            al_object["source_node"] = source_node
                        row_str = get_row_from_dict(status, al_object, title_list)
                    elif isinstance(al_object, str):
                        row_str = get_row_from_dict(status, entry, title_list)
                    else:
                        row_str = ""
                    if row_str:
                        if index:
                            gr_reply += f',[{row_str}]'
                        else:
                            gr_reply += f'[{row_str}]'

        gr_reply += '], "type":"table"}]'
    else:
        status.add_error("Failed to map command results to Grafana table structure: %s" % command)
        gr_reply = ""

    return gr_reply


# =======================================================================================================================
# Transform a dictionary structure to a Row structure
# =======================================================================================================================
def get_row_from_dict(status, al_object, title_list):
    row_str = ""
    # Go over the column names from the title and get the values
    for index, key in enumerate(title_list):
        try:
            value = str(al_object[key])
            str_val = '\"' + value + '\"'
        except:
            str_val = "\"-\""

        if index:
            row_str += ',' + str_val
        else:
            row_str += str_val

    return row_str


# =======================================================================================================================
#    https://corvus.inf.ufpr.br/grafana/plugins/grafana-worldmap-panel/edit
#    https://github.com/grafana/worldmap-panel#table-data-with-latitude-and-longitude-columns
#    JSON result as the Data Source - Need - Warp 10 via grafana-warp10-datasource plugin
# It supports any datasource capable of generating a JSON response with a a custom list of locations (the same format that for the JSON enpoint).
# https://github.com/ovh/ovh-warp10-datasource
#
# Geohash lib  - https://pypi.org/project/geolib/
# A more advanced lib is here - https://geopy.readthedocs.io/en/latest/
# =======================================================================================================================
def grafana_world_map(nodes_list):
    all_nodes = []

    for node in nodes_list:
        node_info = {}

        node_info["hostname"] = node[0]
        node_info["latitude"] = node[1]
        node_info["longitude"] = node[2]
        node_info["metric"] = node[3]

        '''
        node_info["columns"] = [
           "time",
           "metric"
        ]
        node_info["values"] = [
           [
              1529762933815,
              75.654324173059
           ]
        ]
        '''

        all_nodes.append(node_info)

    data_str = '[' + str(all_nodes).replace("'", "\"") + ']'

    return data_str


# =======================================================================================================================
# Organize the data of the default query in the Grafana format
# Example: data_str = [
#   {
#     "columns":[
#       {"text":"Time","type":"time"},
#       {"text":"Country","type":"string"},
#       {"text":"Number","type":"number"}
#     ],
#     "rows":[
#       [1234567,"SE",123],
#       [1234567,"DE",231],
#       [1234567,"US",321]
#     ],
#     "type":"table"
#   }
# ]
# Explained here - https://grafana.com/grafana/plugins/simpod-json-datasource
# =======================================================================================================================
def set_default_table_struct(table_name, user_functions, reply_data):

    if user_functions and len(user_functions):
        functions = user_functions
        defaults = False
    else:
        # Use defaults
        defaults = True
        functions = ["avg", "min", "max"]

    gr_reply =  '[' \
                '{"columns":[' \
                '{"text":"Date","type":"time"}'

    for func in functions:
        gr_reply += ', {"text":"%s%s", "type": "string"}' % (func[0].upper(), func[1:])

    gr_reply += '], "rows":['


    reply_json = utils_json.str_to_json(reply_data)

    if reply_json:
        rows = reply_json["Query"]
        for index, entry in enumerate(rows):
            attr_time = entry["timestamp"]
            attr_ms = int(utils_columns.string_to_seconds(attr_time, None) * 1000)

            if defaults:    # Avg + Min + Max
                attr_avg = entry["avg_val"]
                attr_min = entry["min_val"]
                attr_max = entry["max_val"]
                if index:
                    gr_reply += ',[%u,"%s","%s","%s"]' % (attr_ms, attr_avg, attr_min, attr_max)
                else:
                    gr_reply += '[%u,"%s","%s","%s"]' % (attr_ms, attr_avg, attr_min, attr_max)
            else:
                if index:
                    gr_reply += ',[%u' % attr_ms
                else:
                    gr_reply += '[%u' % attr_ms
                for func in functions:
                    key = '%s_val' % func
                    gr_reply += ',"%s"' % entry[key]
                gr_reply += ']'

    gr_reply += '], "type":"table"}]'

    return gr_reply


# =======================================================================================================================
# Return empty reply to Grafana
# =======================================================================================================================
def set_empty_reply():
    return "[]"


# =======================================================================================================================
# Process a Grafana one or more queries and organize the data for a reply
# 2 tpes of queries: a) Time Series b) Table
# =======================================================================================================================
def process_queries(status, dbms_name, request_handler, decode_body, timeout):


    ret_val = process_status.SUCCESS
    body_info = utils_json.str_to_json(decode_body)
    data_str = ""

    if body_info:

        query_params = AlQueryParams(status, dbms_name, body_info)

        queries_count = query_params.get_counter_targets()

        for target_id in range(queries_count):  # Execute one or more queries on the Grafana Panel

            ret_val, err_msg = query_params.set_target_info(status, target_id)  # Get the query info
            if ret_val:
                if queries_count == 1:
                    data_str = err_msg
                    break  # With a single query - return the error message

                ret_val = process_status.SUCCESS    # ignore the error
                continue        # Error - try next query

            servers = query_params.get_destination_servers()  # Get the IP and Port of specific destination servers to use

            # 3 Types of requets: SQL - with a SQL query, Info - AnyLog command, Map - Info for Grafana Map
            # Other - SQL requests with types: SQL - user SQL, Increments - build in Increment Query, Period - build in Period Query

            if query_params.is_sql_query():
                # User SQL or INCREMENTS or period PERIOD

                statement = make_anylog_query(status, query_params)
                if not statement:
                    data_str = "Failed to create SQL stmt from Payload data"
                    status.add_error(data_str)
                    ret_val = process_status.Failed_to_parse_sql
                    break

                dbms_name = query_params.get_dbms_name()
                if not dbms_name:
                    data_str = "DBMS name not provided by Grafana to POST request: '%s'" % request_handler.path
                    status.add_error(data_str)
                    ret_val = process_status.Missing_dbms_name
                    break

                if not query_params.new_statement(statement):
                    continue  # This query was executed

                trace_level = query_params.get_trace_level()

                timezone = query_params.get_timezone()
                if not timezone or timezone == "browser":
                    data_str = "Timezone not provided by Grafana POST request" if not timezone else "Timezone 'Browser' not supported, select timezone"
                    status.add_error(data_str)
                    ret_val = process_status.ERR_timezone
                    break

                # Set pass_throgh to False because result set is manipulated by this API
                conditions = f"timezone = {timezone} and pass_through = false"

                include_tables = query_params.get_include()     # Get a list of tables that are queries with the main table
                if include_tables:
                    conditions += f" and include = {include_tables}"

                extend_list = query_params.get_extend()     # Get a list of keys that extend the query info
                if extend_list:
                    conditions += f" and extend = {extend_list}"

                # Execute and wait for completion
                ret_val = native_api.exec_sql_stmt(status, servers, dbms_name, conditions, statement, timeout)

                if ret_val:
                    err_msg = f"[Grafana] [Query Process Error] [DBMS: {dbms_name}] [Table: {query_params.get_table_name()}] [Error: {process_status.get_status_text(ret_val)}] [Query: {statement}]"
                    status.add_error(err_msg)

                    if trace_level:
                        show_grafana_process(status, query_params, trace_level, decode_body, "query", ret_val, -1, servers, conditions, dbms_name, statement)

                    if queries_count == 1:
                        data_str = err_msg
                        break       # With a single query - return the error message

                    if target_id:
                        data_str += ","  # Not the first query

                    # Add an error msg
                    data_str += set_grafana_error(dbms_name, query_params.get_table_name(), f"[Error: {process_status.get_status_text(ret_val)}]")
                    ret_val = process_status.SUCCESS  # ignore the error
                    continue

                ret_val, reply_data, rows_counter = native_api.get_sql_reply_data(status, dbms_name, None)

                if trace_level:
                    show_grafana_process(status, query_params, trace_level, decode_body, "query", ret_val, rows_counter, servers, conditions, dbms_name, statement)

                if ret_val:
                    if queries_count == 1:
                        data_str = "Grafana-EdgeLake API failed to process query reply: %s" % statement
                        break       # With a single query - return the error message

                    ret_val = process_status.SUCCESS  # ignore the error
                    continue  # Ignore this query and get to the next

                if reply_data:
                    # PROCESS DATA REQUEST - With data
                    data_str = map_sql_replies(status, query_params, target_id, data_str, reply_data, trace_level)

                    if not data_str:
                        data_str = f"Failed to map Data Source returned data to Grafana with statement: {statement}"
                        ret_val = process_status.Mapping_to_garafana_Error
                        break  # With a single query - return the error message


            elif query_params.get_request_type() == "info" or query_params.get_request_type() == "aggregations":

                # AnyLog command
                statement = query_params.get_details()
                if not query_params.new_statement(statement):
                    continue  # This query was executed

                trace_level = query_params.get_trace_level()

                # Run the AnyLog COMMAND
                ret_val = native_api.exec_native_cmd(status, servers, statement, timeout)

                if trace_level:
                    show_grafana_process(status, query_params, trace_level, decode_body, "info", ret_val, 0, servers, None, None, statement)

                if ret_val:
                    if queries_count == 1:
                        data_str = "Grafana-EdgeLake API failed to process info request: %s" % statement
                        break  # With a single query - return the error message
                    ret_val = process_status.SUCCESS  # ignore the error
                    continue

                # Non SQL command - AnyLog Native Command
                if servers:
                    # data collected from all replying servers
                    ret_val, reply_data = native_api.get_network_reply(status)
                    if ret_val:
                        if queries_count == 1:
                            data_str = f"Failed to retrieve reply from Network nodes using: run client ({servers}) {statement}"
                            break  # With a single query - return the error message
                        ret_val = process_status.SUCCESS  # ignore the error
                        continue
                    multiple_servers = True
                else:
                    # local command
                    # result was placed on the job_handle
                    reply_data = status.get_active_job_handle().get_result_set()
                    multiple_servers = False

                if query_params.get_request_type() == "aggregations":
                    dbms_name = query_params.get_dbms_name()
                    table_name = query_params.get_table_name()
                    is_dest_servers = True if query_params.get_destination_servers() else False  # If was send to a server or executed locally
                    data_str = reformat_aggregations(status, is_dest_servers, dbms_name, table_name, reply_data)
                else:
                    data_str = json_to_grafana_table(status, statement, reply_data, multiple_servers)

            elif query_params.get_request_type() == 'map':
                # Data for the Grafana Map
                # Get the info on the members to be added
                nodes_info = query_params.get_nodes_info()  # an array to store info returned from the blockchain on nodes in the network
                members_types = query_params.get_members_types()
                nodes_metric = query_params.get_nodes_metric()
                nodes_attribute = query_params.get_nodes_attr()
                ret_val = get_map_info(status, members_types, nodes_attribute, nodes_metric, nodes_info, query_params.get_trace_level())
                if ret_val:
                    ret_val = process_status.SUCCESS  # ignore the error
                    continue
                data_str += grafana_world_map(nodes_info)  # TEMPORARY CODE

            else:
                trace_level = trace_func.get_func_trace_level("grafana")
                if trace_level:
                    show_grafana_process(status, query_params, trace_level, decode_body, "not-recognized", ret_val, 0, 0, None, None, None)
                if queries_count == 1:
                    data_str = "Unrecognized request from Grafana JSON Payload to the data source"
                    ret_val = process_status.ERR_wrong_json_structure
                    break  # With a single query - return the error message

    if not ret_val:
        # No error
        if data_str:
            if query_params.is_tsd():
                # Time series view needs completion, table view has all returned data
                data_str = '[' + data_str + ']'
        else:
            data_str = set_empty_reply()

    return [ret_val, data_str]

# =======================================================================================================================
# Debug Queries - needs to set debug level.
# Either in Grafana: trace_level : 1
# Or as a command: trace level = 1 grafana
# =======================================================================================================================
def show_grafana_process(status, query_params,  trace_level, decode_body, call_type, ret_val, rows_returned, servers, conditions, dbms_name, statement ):
    '''
    call_type is query, command or map
    '''

    if trace_level > 1:
        # show the Grafana structure
        utils_print.output("\rGrafana JSON:\r\n" + decode_body +"\r\nNetwork Call:", True)


    msg_text = process_status.get_status_text(ret_val)

    if call_type == "query":
        if query_params.get_request_type() == "increments":
            # add query details
            j_handle = status.get_active_job_handle()
            if j_handle.get_select_parsed():
                details = j_handle.get_select_parsed().get_increment_info()
            else:
                details = ""
        else:
            details = ""

        al_cmd = f"run client ({servers}) sql {dbms_name} {conditions} {statement}"

        print_msg = f"\rProcess: [{ret_val}:{msg_text}] Rows: [{rows_returned}] Details: [{details}]\r\nStmt: [{al_cmd}]"

        utils_print.output(print_msg, True)
    elif call_type == "info":
        if servers:
            al_cmd = f"run client ({servers}) {statement}"
        else:
            al_cmd = f"{statement}"
        print_msg = f"\rProcess: [{ret_val}:{msg_text}] Stmt: [{al_cmd}]"
        utils_print.output(print_msg, True)
    else:
        print_msg = f"\rUnrecognized Grafana Call"
        utils_print.output(print_msg, True)

# =======================================================================================================================
# Get the info on the members which are presented on the map
# =======================================================================================================================
def get_map_info(status, members_types, nodes_attribute, nodes_metric, map_data, trace_level):
    ret_val = process_status.SUCCESS
    if isinstance(members_types, list):
        # this is a list of the members to bring. Each entry is an array with the member type and the metic assigned
        for index, member in enumerate(members_types):
            if isinstance(member, str):
                attribute = None  # an attributes names, from which a value is taken to be displayed when hovered on the circle
                if isinstance(nodes_attribute, list) and len(nodes_attribute) > index:
                    attribute = nodes_attribute[index]

                metric = 0
                if isinstance(nodes_metric, list) and len(nodes_metric) > index:
                    metric = nodes_metric[index]
                if isinstance(member, str):
                    ret_val = get_node_info(status, member, attribute, metric, map_data, trace_level)
                    if ret_val:
                        continue
                else:
                    continue
            else:
                continue
    else:
        ret_val = process_status.ERR_wrong_json_structure
    return ret_val


# =======================================================================================================================
# Get the node locations and info for the Grafana Map
# =======================================================================================================================
def get_node_info(status, member, attribute, metric, map_data, trace_level):

    if attribute:
        # Get the value of the attribute name
        get_cmd = "blockchain get %s bring [%s][%s] \\t [%s]['loc'] separator = \\n" % (member, member, attribute, member)
    else:
        get_cmd = "blockchain get %s bring -- \\t [%s]['loc'] separator = \\n" % (member, member)   # Without attribute value


    ret_val, nodes_str = member_cmd.blockchain_get(status, get_cmd.split(), "", True)

    if trace_level > 1:
        if ret_val:
            result = "Error: %s" % process_status.get_status_text(ret_val)
        elif not nodes_str or not len(nodes_str):
            result = "No data set"
        else:
            result = "Returned data"
        utils_print.output("\r\n[Grafana] [Command] [%s] --> [%s]" % (get_cmd, result), True)


    if not ret_val:
        nodes_list = nodes_str.split('\n')

        for node in nodes_list:
            index = node.find('\t')  # Split between the name and the location
            if index > 0 and index < (len(node) - 1):
                node_name = node[:index]
                node_location = node[index + 1:].split(',')
                if len(node_location) == 2:
                    latitude = node_location[0]
                    longitude = node_location[1]
                    map_data.append((node_name, latitude, longitude, metric))  # metric determines the color of the node

    return ret_val


# =======================================================================================================================
# Organize the query data in the Grafana format
# =======================================================================================================================
def map_sql_replies(status, query_params, target_id, data_str, reply_data, trace_level):
    dbms_name = query_params.get_dbms_name()
    table_name = query_params.get_table_name()

    if query_params.is_tsd():
        if target_id:
            data_str += ","
        if query_params.is_default_query():
            functions = query_params.get_functions()    # Get the SQL functions
            is_fixed_points, start_time_ms, end_time_ms, interval_time_ms = query_params.get_fixed_points_info()
            if query_params.is_provided_sql():
                # Using the user SQL query (in the PayLoad)
                data_str += set_sql_timeseries_struct(status, table_name, reply_data, is_fixed_points, start_time_ms, end_time_ms, interval_time_ms, trace_level)  # Organize in the grafana format
            else:
                # SQL was created by the JSON Payload
                extend = query_params.get_extend()          # A list of keys that extend the result set - like @table_name
                if extend:
                    data_str += set_extended_timeseries_struct(table_name, extend, reply_data, functions, is_fixed_points, start_time_ms, end_time_ms, interval_time_ms, trace_level)  # Organize in the grafana format
                else:
                    data_str += set_default_timeseries_struct(table_name, reply_data, functions, is_fixed_points, start_time_ms, end_time_ms, interval_time_ms, trace_level)  # Organize in the grafana format
        else:
            data_str += set_timeseries_struct(status, dbms_name, table_name, query_params,
                                              reply_data)  # Organize in the grafana format
    else:
        functions = query_params.get_functions()
        if functions and query_params.is_default_query():
            data_str = set_default_table_struct(table_name, functions, reply_data)  # Organize in the grafana format
        else:
            data_str = set_sql_table_struct(status, dbms_name, table_name, reply_data)

    return data_str

# =======================================================================================================================
# Using the Grafaba provided info construct a call to AnyLog
# =======================================================================================================================
def make_anylog_query(status, query_params):
    # Get dbms and table name
    dbms_name, table_name = query_params.get_dbms_table()
    limit = query_params.get_limit()
    where_cond = query_params.get_where_cond()  # added to each SQL query
    functions = query_params.get_functions()    # User overwrites default functions
    offset_where = query_params.get_offset_where()  # The offset to the WHERE CLAUSE in the user sql (or -1 with no where condition)
    offset_for_where = query_params.get_offset_add_where()  # The offset after the table name - where WHERE condition can be added

    # Get the time range
    start_time, end_time = query_params.get_time_range()

    use_time_range = query_params.use_time_range()

    # Get the interval unit and time
    interval_unit, interval_time = query_params.get_interval_info()

    # Get the columns to present
    time_column, value_column = query_params.get_time_value_columns_names()

    if query_params.is_default_query():
        if query_params.get_request_type() == "period":
            user_stmt = query_params.get_user_stmt()
            sql_stmt = get_period_timeseries_stmt(status, user_stmt, interval_unit, interval_time, time_column, value_column, table_name,
                                                  start_time, end_time, where_cond, functions, limit)
        elif query_params.get_request_type() == "increments":
            data_points = query_params.get_data_points()
            if data_points:
                # AnyLog will calculate the increment variables:
                incr_stmt = f"SELECT increments({time_column},{data_points}), "
            else:
                if not interval_unit:
                    # AnyLog will set optimized values
                    incr_stmt = f"SELECT increments({time_column}), "
                else:
                    # Predefined or Grafana values
                    incr_stmt = f"SELECT increments({interval_unit},{interval_time},{time_column}), "

            if query_params.is_provided_sql():
                # Update the user provided SQL stmt
                user_stmt = query_params.get_user_stmt()
                sql_stmt = update_increments_stmt(status, user_stmt, incr_stmt, interval_unit, interval_time, time_column, table_name, start_time, end_time, limit, offset_where, offset_for_where)
            else:
                # Create a SQL stmt from the Grafana payload
                sql_stmt = get_increments_timeseries_stmt(incr_stmt, interval_unit, interval_time, time_column, value_column,
                                                      table_name, start_time, end_time, where_cond, functions, limit)
    else:
        increment_offset = query_params.get_increment_offset()
        user_stmt = query_params.get_user_stmt()
        sql_stmt = update_user_sql_stmt(increment_offset, interval_unit, interval_time, user_stmt, use_time_range,
                                        time_column, value_column, start_time, end_time, offset_where, offset_for_where,
                                        where_cond, limit)

    return sql_stmt


# =======================================================================================================================
# Make a timeseries SQL statement to pull the data for time series graph
# =======================================================================================================================
def get_increments_timeseries_stmt(sql_prefix, interval_unit, interval_time, time_column, value_column, table_name, start_time,
                                   end_time, where_cond, functions, limit):


    if functions:
        # User detailed functions
        source_stmt = "%smax(%s) as timestamp "
        for func_name in functions:
            source_stmt += ", %s(%s) as %s_val " % (func_name, value_column, func_name)
        source_stmt +=  "from %s " \
                        f"where {time_column} >= '%s' and {time_column} <= '%s' " \
                        "%s" \
                        "limit %u;"
    else:
        source_stmt = "%s" \
                      "max(%s) as timestamp, " \
                      "avg(%s) as avg_val, " \
                      "min(%s) as min_val, " \
                      "max(%s) as max_val " \
                      "from %s " \
                      f"where {time_column} >= '%s' and {time_column} <= '%s' " \
                      "%s" \
                      "limit %u;"

    if where_cond:
        where_stmt = "and (" + where_cond + ") "
    else:
        where_stmt = ""

    if functions:
        sql_stmt = source_stmt % (
        sql_prefix, time_column, table_name,
        start_time, end_time, where_stmt, limit)
    else:
        sql_stmt = source_stmt % (
        sql_prefix, time_column, value_column, value_column, value_column, table_name,
        start_time, end_time, where_stmt, limit)

    return sql_stmt

# =======================================================================================================================
# Update a user provided increment stmt
# =======================================================================================================================
def update_increments_stmt(status, user_stmt, incr_stmt, interval_unit, interval_time, time_column, table_name, start_time, end_time, limit, offset_where, offset_for_where):
    """
    offset_where - The offset to the WHERE CLAUSE in the user sql (or -1 with no where condition)
    offset_for_where - The offset after the table name - where WHERE condition can be added
    """

    if offset_where != -1:
        # Add to existing where
        sql_stmt = user_stmt[:offset_where] + f"{time_column} >= '{start_time}' and {time_column} <= '{end_time}' and {user_stmt[offset_where:]} limit {limit};"
    else:
        # add where condition
        if len(user_stmt) > offset_for_where:
            sql_prefix = user_stmt[offset_for_where:]
        else:
            sql_prefix = ""
        sql_stmt = user_stmt[: offset_for_where] + f" where {time_column} >= '{start_time}' and {time_column} <= '{end_time}' {sql_prefix} limit {limit};"



    if interval_unit:
        # Grafana values for increment (otherwise AnyLog will optimize)
        offset = sql_stmt.find(')', 18)         # Find the end of the increment statement in the user_stmt
        if offset == -1:
            status.add_error("Grafana API: Failed to generate an 'increment query' - missing parentheses in user provided SQL stmt")
            return None
        offset = sql_stmt.find(',', offset+1)  # Find the comma after the increment function
        if offset == -1:
            status.add_error("Grafana API: Failed to generate an 'increment query' - missing comma in user provided SQL stmt")
            return None

        sql_stmt = incr_stmt + sql_stmt[offset+1:]

    return sql_stmt

# =======================================================================================================================
# Make a timeseries SQL statement to pull the last data provided
# SELECT MIN(value) AS min, AVG(value) AS avg, MAX(value) AS max FROM ping_sensor WHERE period(day, 1, now(), timestamp) AND device_name = '${device_name}'
# =======================================================================================================================
def get_period_timeseries_stmt(status, user_stmt, interval_unit, interval_time, time_column, value_column, table_name, start_time,
                               end_time, where_cond, functions, limit):

    if where_cond:
        where_stmt = ", and " + where_cond
    else:
        where_stmt = ""

    if user_stmt:
        # User specifies the SQL
        """
        {
          "type": "period",
          "sql": "select max(insert_timestamp) as insert_timestamp, avg(hw_influent) as hw_influent from wwp_analog",
          "time_column": "insert_timestamp",
          "grafana": {
            "format_as": "table"
          },
          "trace_level" : 1
        }
        """
        period_func = f" where period({interval_unit}, {interval_time}, '{end_time}',{time_column} {where_stmt});"

        sql_stmt = user_stmt + period_func

    elif functions:
        source_stmt = "SELECT max(%s) as timestamp"
        for func_name in functions:
            source_stmt += ", %s(%s) as %s_val" % (func_name, value_column, func_name)

        source_stmt += " from %s where period(%s, %u, '%s', %s %s);"

        sql_stmt = source_stmt % (time_column, table_name, interval_unit, interval_time, end_time, time_column, where_stmt)


    else:
        source_stmt = "SELECT " \
                      "max(%s) as timestamp, " \
                      "avg(%s) as avg_val, " \
                      "min(%s) as min_val, " \
                      "max(%s) as max_val " \
                      "from %s " \
                      "where period(%s, %u, '%s', %s %s);"


        sql_stmt = source_stmt % (
            time_column, value_column, value_column, value_column, table_name, interval_unit, interval_time, end_time, time_column, where_stmt)

    return sql_stmt


# =======================================================================================================================
# Make a time series SQL statement from the user statement
# =======================================================================================================================
def update_user_sql_stmt(increment_offset, interval_unit, interval_time, user_stmt, use_time_range, time_column,
                         value_column, start_time, end_time, offset_where, offset_for_where, where_cond, limit):
    if use_time_range or where_cond:
        # update the where condition:
        if use_time_range and time_column and where_cond:
            updated_where = "%s and %s >= '%s' and %s <= '%s' " % (
            where_cond, time_column, start_time, time_column, end_time)
        elif use_time_range and time_column:
            updated_where = "%s >= '%s' and %s <= '%s' " % (time_column, start_time, time_column, end_time)
        else:
            updated_where = where_cond

        if offset_where > 0:
            # Add to existing where
            sql_stmt = user_stmt[:offset_where] + updated_where + "and " + user_stmt[offset_where:]
        else:
            if offset_for_where > 0:
                sql_stmt = user_stmt[:offset_for_where] + ' where ' + updated_where
                if offset_for_where < len(user_stmt):
                    sql_stmt += user_stmt[offset_for_where:]
            else:
                sql_stmt = user_stmt
    else:
        sql_stmt = user_stmt


    offset_limit = sql_stmt.rfind(" ") - 10
    if sql_stmt.find(" limit ", offset_limit) == -1:
        # Add default limit if not in the SQL
        sql_stmt = sql_stmt + " limit %u" % limit

    if increment_offset > 0:
        sql_stmt = "SELECT increments(%s, %u, %s), %s" % (
        interval_unit, interval_time, time_column, sql_stmt[increment_offset + 13:])

    return sql_stmt

# =======================================================================================================================
# Determine if the data type can be represented as a value
# =======================================================================================================================
def is_graphed_data_type(data_type):

    if data_type in grafana_data_types_:
        return True
    if data_type.startswith("int"):
        return True
    if data_type.startswith("time"):
        return True
    if data_type.startswith("num"):
        return True
    if data_type == "float":
        return True

    return False

# =======================================================================================================================
# Value to data type
# =======================================================================================================================
def val_to_data_type(data_val):

    if isinstance(data_val, int):
        data_type = "number"
    elif isinstance(data_val, float):
        data_type = "number"
    else:
        data_type = "string"


    return data_type

# =======================================================================================================================
# Replace AnyLog data type with grafana data type
# =======================================================================================================================
def get_grafana_data_type(source_data_type):

    if source_data_type in grafana_data_types_:
        data_type = source_data_type
    elif source_data_type.startswith("timestamp "):
        data_type = "timestamp"
    elif source_data_type.startswith("char") or source_data_type.startswith("var"):
        data_type = "string"
    elif source_data_type.startswith("int") or source_data_type.startswith("float") or source_data_type.startswith("big"):
        data_type = "number"
    else:
        data_type = source_data_type
    return data_type

# =======================================================================================================================
# Grafana Other Request
# =======================================================================================================================
def process_other_request(status, grafana_request):

    ret_val = process_status.Err_grafana_payload
    gr_reply = None

    if "target" in grafana_request["payload"]:
        statement = grafana_request["payload"]["target"]
        if isinstance(statement, str):
            # Execute and wait for completion
            ret_val = native_api.exec_al_cmd(status, statement, None, None, 5)
            if ret_val:
                gr_reply = f"Failed to execute '{statement}'"
            else:
                j_handle = status.get_active_job_handle()  # Need to be done after the execution of the commands
                result_set = j_handle.get_result_set()

                #gr_reply = '[{ "text": "Server 1", "value": "server1" }, { "text": "Server 2", "value": "server2" }]'

                gr_reply = result_set.replace("'",'"')
    else:
        gr_reply = "Missing Target in Grafana Payload"

    return [ret_val, gr_reply]
