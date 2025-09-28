"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import requests
from requests.exceptions import HTTPError

import edge_lake.dbms.db_info as db_info
import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.generic.process_status as process_status
import edge_lake.generic.params as params
import edge_lake.generic.al_parser as al_parser
import edge_lake.generic.utils_sql as utils_sql
import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.utils_columns as utils_columns
import edge_lake.tcpip.message_header as message_header
import edge_lake.tcpip.net_client as net_client
import edge_lake.tcpip.http_client as http_client

PI_DEBUG = False

# Connect to the pi DBMS:
# Xompass API: connect dbms pi !db_user 7400 transpara
# AnyLog API: connect dbms pi Moshe.Shadmon@https://MetersAPI.Transpara.com/piwebapi/assetservers:Station#6 0 transpara
# rest get user=Moshe.Shadmon password=Station#6 url=
# pi transpara text "select * from g30 limit 100"
# pi transpara text "select element_name, sensor_name from sub_t01 limit 100"
# pi transpara text "select element_name from sub_t01 limit 100"

# killall -9 npm ; screen -Sd xompass -m bash -c "cd ~/xompass-pi-webapi-browser/ ; npm start > /tmp/npm_out.err"

# Documentation:
# https://techsupport.osisoft.com/Documentation/PI-Web-API/help/controllers/stream/actions/getsummary.html
# Example Process:


"""
Pi Hierarchy:
AF
Databases -> sync with a database
Elements -> maintain the hierarchy of physical or logical enteties (uniquely named in their path). i.e. Transformer
Attributes -> within elements, contain names and values pairs (uniquely named in their path). i.e. Capacity, Level, Volume
Tables - Created in AF databases to provide Table Lookup Data Reference
"""


# ==================================================================
# Set debug ON/OFF
# ==================================================================
def set_debug(is_on: bool):
    global PI_DEBUG
    PI_DEBUG = is_on


# ==================================================================
# Prepare an array of the functions that needs to be processed
# ==================================================================
def prepare_functions(status, pi_cursor, select_parsed):
    # Prepare the proprietary functions
    projection = select_parsed.get_projection_parsed()

    with_increments = False
    proprietary = select_parsed.get_proprietary_functions()  # these are non-sql functions like increments
    if proprietary:
        for p_entry in proprietary:
            if p_entry[0] == "increments":  # the function process data in increments
                with_increments = True
                details = p_entry[1].split(",")
                start_date = pi_cursor.get_start_date()
                if not start_date:
                    status.add_keep_error("Pi error: Function 'increments' does not have a start date")
                    return False
                interval_type = details[0].strip()  # seconds, ninutes, hours etc
                interval_units = details[1].strip()
                if not interval_units.isdecimal():
                    status.add_keep_error(
                        "Pi error: Function 'increments' does not identify time units: '%s'" % details)
                    return False

                time_field_name = details[2].strip()
                time_field_name, pi_attribute_type = pi_cursor.get_pi_name_type(
                    time_field_name)  # Get PI attribute name by SQL field name
                if not pi_cursor.set_increments_query(interval_type, start_date, int(interval_units), time_field_name):
                    status.add_keep_error("Pi error: Function 'increments' failed to set function: '%s'" % details)
                    return False
                # the next 2 functions extract a date such that results can be grouped with data of other machines
                if interval_type != "year":
                    # with year, group by is only using the date_extract value
                    function = utils_columns.functions_classes["date_truncate"]("date_truncate", time_field_name,
                                                                                pi_attribute_type)
                    function.set_increment()
                    truncate_type = utils_columns.get_next_time_unit(
                        interval_type)  # change seconds to minutes, minutes to hours etc.
                    function.set_time_unit(truncate_type)
                    pi_cursor.get_project_func().update_projection_functions(time_field_name, function, 0)
                function = utils_columns.functions_classes["date_extract"]("date_extract", time_field_name,
                                                                           pi_attribute_type)
                function.set_increment()
                function.set_time_unit(interval_type, interval_units)
                pi_cursor.get_project_func().update_projection_functions(time_field_name, function, 0)

    # Prepare the functions from the SQL statement
    ret_val = True

    for entry in projection:
        function_name = entry[1]
        pi_info = pi_cursor.get_pi_column_info(entry[0])  # Get PI info by SQL field name
        pi_attr_name = pi_info[0]
        if not pi_attr_name:
            if entry[0] == "*" and entry[1] == "count":  # test for select count(*)
                pi_attr_name = "all"
                pi_attribute_type = "all"
                pi_layer = 0  # last layer
            else:
                status.add_keep_error("Pi error: Column Name '%s' in projection list is not mapped to PI" % entry[0])
                ret_val = False
                break
        else:
            pi_attribute_type = pi_info[3]
            pi_layer = pi_info[4]

        if function_name not in utils_columns.functions_classes.keys():
            status.add_keep_error("Pi error: Function '%s' not supported" % function_name)
            ret_val = False
            break

        function = utils_columns.functions_classes[function_name](function_name, pi_attr_name, pi_attribute_type)
        if with_increments:
            function.set_increment()

        pi_cursor.get_project_func().update_projection_functions(pi_attr_name, function, pi_layer)

    return ret_val


# ==================================================================
# Traversal based on self.element_tree
# PI_CURSOR.element_tree is pointing to entries in PIDB.elements
# ==================================================================
def pull_data_from_pi(status, pi_cursor):
    if pi_cursor.get_interface() == "web_api":
        elements = pi_cursor.get_root_elements()
    else:
        elements_tree = pi_cursor.get_element_tree()  # get the list of elements in this layer
        elements = elements_tree[2]

    table_path = pi_cursor.get_table_path()  # an array with the traversal paths

    ret_data = ""

    if len(elements):
        for element in elements:

            if pi_cursor.is_interface("xompass"):
                url = 'http://%s/api/children_elements' % pi_cursor.get_host_port()
                params = {'eid': element["Id"]}
                ret_val, element = get_xompass_data(status, url, params)  # Get the parent with children info

            # visit all the elements in this layer of the tree
            ret_val, pi_backtrack, data = visit_children(status, pi_cursor, 1, table_path, element)
            if ret_val or pi_backtrack:
                break
    else:
        ret_val = process_status.DBMS_unexpected_reply
        status.add_keep_error(
            "Query process is not able to identify elements in database %s" % pi_cursor.get_dbms_name())

    reply_list = [ret_val, ret_data]
    return reply_list


# -----------------------------------------
# Determine if current element is in the path
# return path options for next layer
# -----------------------------------------
def get_path_keys(path_options, element_path):
    new_options = []
    index = element_path.rfind("\\")

    pi_points = False
    pi_children = False
    pi_backtrack = True

    for option in path_options:  # go over the path options to see if continue retrieving children

        sensors, continue_path, backtrack, offset_option = is_included(option, element_path[index:])
        if offset_option:
            new_options.append(option[offset_option:])
        elif continue_path:
            # include all childrens
            new_options.append("\\*")
        if sensors:
            pi_points = True
        if continue_path:
            pi_children = True
        if not backtrack:
            pi_backtrack = False

    reply_list = [pi_points, pi_children, pi_backtrack, new_options]
    return reply_list


# -----------------------------------------
# Determine if path satisfies the table key
# Return:
#  sensors - True if the sensor data of this element is included
#  continue_path  - True: Continue to children, False: Continue to sibling
#  backtrack - Return to parent
#  offset_option - the key offset of the child element
# -----------------------------------------
def is_included(option, path):
    if option[1] == '*':
        reply_list = [True, True, False, 0]  # include all children
        return reply_list

    len_o = len(option)
    len_p = len(path)
    if path == option[:len_p]:
        if len_o == len_p:
            reply_list = [True, False, False, 0]
            return reply_list

    if option.startswith(path):
        reply_list = [False, True, False, len_p]
        return reply_list

    if len_p < len_o:
        length = len_p

    # compare byte by byte
    for i in range(1, length):
        o_char = option[i]
        if o_char == '*':
            reply_list = [True, True, False, 0]
            return reply_list
        if o_char != path[i]:
            break

    reply_list = [False, False, False, 0]  # ignore sensors and children
    return reply_list
# -----------------------------------------
# Given an element - visit the children
# -----------------------------------------
def visit_children(status, pi_cursor, depth, path_keys, parent):
    ret_data = ""

    ret_val = process_status.SUCCESS

    pi_points, pi_children, pi_backtrack, child_keys = get_path_keys(path_keys, parent[
        "Path"])  # test if the path satisfies the key defined for the table
    if pi_backtrack:
        reply_list = [ret_val, True, ret_data]
        return reply_list

    pi_cursor.place_in_hierarchy(1, parent)  # add to the hierarchy of elements and sensors - root to leaf

    # get the sensors of the parent element
    if pi_points:
        ret_val, data = visit_sensors(status, pi_cursor, depth + 1, parent)

    if not ret_val and pi_children:
        # pi_children is True according to the metadata key of the table

        if len(child_keys):
            if pi_cursor.is_interface("xompass"):
                children = xompass_get_children(parent)
            else:
                ret_val, children = web_api_get_children(status, pi_cursor, parent)

            if PI_DEBUG:
                print_debug(ret_val, "element", parent, children)

            if not ret_val:
                for child in children:
                    if pi_cursor.is_interface("xompass"):
                        ret_val, element = xompass_add_children(status, pi_cursor, child)
                        if ret_val:
                            break
                    else:
                        element = child

                    ret_val, pi_backtrack, data = visit_children(status, pi_cursor, depth + 1, child_keys, element)
                    if ret_val or pi_backtrack:
                        break

    reply_list = [ret_val, False, ret_data]
    return reply_list

# -----------------------------------------
# web interface - given an element - get the list of children
# -----------------------------------------
def web_api_get_children(status, pi_cursor, parent):
    ret_val = process_status.SUCCESS
    children = None
    if "HasChildren" in parent.keys() and parent["HasChildren"]:
        if "Links" in parent.keys():
            links = parent["Links"]
            if "Elements" in links.keys():
                url = links["Elements"]
                url += pi_cursor.get_selected_fields()
                ret_val, ret_struct = http_client.get_jdata(status, url, {}, pi_cursor.get_auth_token(), 10)
                if not ret_val:
                    if "Items" in ret_struct:
                        children = ret_struct["Items"]

    if children == None:
        children = []  # no childrens or error

    reply_list = [ret_val, children]
    return reply_list

# -------------------------------------------------------------------------
# web interface - given an element - get the next layer using the link info
# For example, given an element, get the attributes
# -------------------------------------------------------------------------
def web_api_read_link(status, pi_cursor, element, link_name):
    ret_val = process_status.SUCCESS
    link_data = None
    if "Links" in element.keys():
        links = element["Links"]
        if link_name in links.keys():
            url = links[link_name]
            ret_val, ret_struct = http_client.get_jdata(status, url, {}, pi_cursor.get_auth_token(), 10)
            if not ret_val:
                if "Items" in ret_struct:
                    link_data = ret_struct["Items"]

    if link_data == None:
        link_data = []  # no childrens or error

    reply_list = [ret_val, link_data]
    return reply_list


# -----------------------------------------
# Xompass - given an element - get the list of children
# -----------------------------------------
def xompass_get_children(parent):
    if "HasChildren" in parent.keys() and parent["HasChildren"] and "children" in parent.keys():
        children = parent["children"]
    else:
        children = []  # no childrens
    return children


# -----------------------------------------
# Xompass - get element with children array
# -----------------------------------------
def xompass_add_children(status, pi_cursor, child):
    url = 'http://%s/api/children_elements' % pi_cursor.get_host_port()
    params = {'eid': child}
    ret_val, element = get_xompass_data(status, url, params)  # Get the parent with children info

    reply_list = [ret_val, element]
    return reply_list


# -----------------------------------------
# STEP 3
# Given an element - get sensors per element
# -----------------------------------------
def visit_sensors(status, pi_cursor, depth, element):
    ret_data = ""

    if not pi_cursor.is_with_sensors():
        # The traversal considers the sensor as the end point
        # No need to visiy the sensor and readings
        if pi_cursor.get_select_parsed().is_functions_values():
            # send the data to a function
            ret_val = execute_functions(status, 1, [None], pi_cursor)
        else:
            ret_val = deliver_hierarchical_data(status, 1, pi_cursor)
        reply_list = [ret_val, ret_data]
        return reply_list

    if pi_cursor.is_interface("xompass"):
        eid = element["Id"]
        url = 'http://%s/api/get_sensors_in_element' % pi_cursor.get_host_port()
        params = {'eid': eid}

        ret_val, sensors_dict = get_xompass_data(status, url, params)
        if not ret_val:
            sensors = list(sensors_dict.values())  # make a list to be consistant with webapi
    else:
        ret_val, sensors = web_api_read_link(status, pi_cursor, element, "Attributes")

    if PI_DEBUG:
        print_debug(ret_val, "sensor", element, sensors)

    if not ret_val:

        for sensor in sensors:
            # Filter with level 1 --> sensor data
            if filter_by_where_condition(1, sensor, pi_cursor, False):

                pi_cursor.place_in_hierarchy(depth,
                                             sensor)  # add to the hierarchy of elements and sensors - root to leaf

                pi_cursor.count_sensors_visited()

                if pi_cursor.is_with_readings():
                    # get values from sensors
                    ret_val, data = get_sensor_reading(status, pi_cursor, sensor)
                else:
                    if pi_cursor.get_select_parsed().is_functions_values():
                        # send the data to a function
                        ret_val = execute_functions(status, 1, [None], pi_cursor)
                    else:
                        # Get data only from elements
                        ret_val = deliver_hierarchical_data(status, 0, pi_cursor)
                if ret_val:
                    break

                if pi_cursor.all_query_sensors_visited():
                    ret_val = process_status.TRAVERSAL_COMPLETED
                    break

    reply_list = [ret_val, ret_data]
    return reply_list


# -----------------------------------------
# Given a sensor - get the readings
# -----------------------------------------
def get_sensor_reading(status, pi_cursor, sensor):
    if pi_cursor.is_interface("xompass"):
        ret_val, items = xompass_get_values(status, pi_cursor, sensor["Id"])
    else:
        ret_val, items = web_api_get_values(status, pi_cursor, sensor)

    if PI_DEBUG:
        print_debug(ret_val, "readings", sensor, items)

    if not ret_val and len(items):
        dest = pi_cursor.get_out_instruct().get_output_dest()
        ret_val, data = process_sensor_reading(status, pi_cursor, items, dest)
    else:
        data = ""

    reply_list = [ret_val, data]
    return reply_list

# =======================================================================================================================
# Get Values from Xompass
# =======================================================================================================================
def xompass_get_values(status, pi_cursor, sensor):
    items = []
    url = pi_cursor.get_date_cmd()
    params = pi_cursor.get_date_cmd_params()
    params["sid"] = sensor

    # url = 'http://%s/api/get_sensor_data_range' % pi_cursor.get_host_port()
    # params = {'stime' : "01/01/70", 'etime' : "*", 'sid' : sensor}

    ret_val, readings = get_xompass_data(status, url, params)

    if not ret_val:
        if readings:
            if "Items" in readings.keys():
                items = readings["Items"]
            else:
                items = readings

    reply_list = [ret_val, items]
    return reply_list

# =======================================================================================================================
# Get Values from web API
# from: tail -f /tmp/npm_out.err
# "https://MetersAPI.Transpara.com/piwebapi/streams/F1AbESMnPyLii5UK-yRJ2zq7VBQ2Aab0dFb6hGggODdQJRkVAByTR5yHYjVQM9sN-HCJmIwR0NQLVZLUEktU1FMLUFcQU1JNlxTVUJcU1VCLlQwMVxTVUIuVDAxLkMxXFNVQi5UMDEuQzEuUFhcU1VCLlQwMS5DMS5QWC5QVDAyXFNVQi5UMDEuQzEuUFguUFQwMi5NMDAxfENJUkNVSVQ"\
# "/recorded?startTime=2019-01-29T18:11:03.031&endTime=*&selectedFields=Items.Value.Timestamp;Items.Value.Value;Items.Timestamp"
# =======================================================================================================================
def web_api_get_values(status, pi_cursor, sensor):
    if "Links" in sensor.keys():
        links = sensor["Links"]
        if "Value" in links.keys():
            url = links["Value"]
            if url.endswith('/value'):
                url = url[:-6]
            url += "/recorded?" + pi_cursor.get_search_string()  # + "&selectedFields=Items.Value.Timestamp;Items.Value.Value;Items.Timestamp"

            ret_val, ret_struct = http_client.get_jdata(status, url, {}, pi_cursor.get_auth_token(), 10)

            if not ret_val and ret_struct and "Items" in ret_struct:
                items = ret_struct["Items"]
            else:
                items = []
        else:
            ret_val = process_status.SUCCESS
            items = []
    else:
        ret_val = process_status.SUCCESS
        items = []

    reply_list = [ret_val, items]
    return reply_list


# =======================================================================================================================
# Process Sensor reading
# =======================================================================================================================
def process_sensor_reading(status, pi_cursor, items, dest):
    data = ""
    if pi_cursor.get_select_parsed().is_functions_values():
        # send the data to a function
        ret_val = execute_functions(status, 0, items, pi_cursor)
    else:
        # send the data to the caller
        ret_val = deliver_data(status, 0, items, pi_cursor)

    reply_list = [ret_val, data]
    return reply_list

# =======================================================================================================================
# Prepare output data and place the data in the IO_BUFF
# data_node shows the path to the data
# =======================================================================================================================
def deliver_data(status, tree_depth, json_data, pi_cursor):
    ret_val = process_status.SUCCESS
    out_instruct = pi_cursor.get_out_instruct()

    data_dest = out_instruct.get_output_dest()
    if data_dest == "network":
        add_column_name = False
        socket = out_instruct.get_socket()
    else:
        add_column_name = True
        socket = None

    io_buff = pi_cursor.get_io_buff_in()
    block_counter = pi_cursor.get_blocks_counter()  # counter for the block being prepared
    rows_counter = pi_cursor.get_rows_counter()  # number of rows are currently in the block
    limit = out_instruct.get_limit()

    # Get the list that shows the output order:
    # the list entries has 3 values -
    # 1) Location in the hierarchy (1 is the bottom)
    # 2) Column number in SQL output
    # 3) and PI column name
    out_order = pi_cursor.get_out_order()

    if isinstance(json_data, dict):
        # a single row in the result_set --> set it as a list
        data_list = [json_data]  # organize the single row as a lits
    else:
        data_list = json_data

    for counter, row in enumerate(data_list):

        if filter_by_where_condition(tree_depth, row, pi_cursor, False):

            row_data = make_output_row(status, pi_cursor, 0, out_order, add_column_name, row)

            ret_val, block_counter, rows_counter = transfer_output_row(status, pi_cursor, data_dest, socket, io_buff,
                                                                       row_data, block_counter, rows_counter, limit)
            if ret_val:
                break

    pi_cursor.set_blocks_counter(block_counter)
    pi_cursor.set_rows_counter(rows_counter)  # counter for rows inside the block

    return ret_val


# =======================================================================================================================
# Print PI debug info on nodes visited
# =======================================================================================================================
def print_debug(ret_val, type_node, parent, children):
    if children:
        counter_children = len(children)
    else:
        counter_children = 0

    print_str = "\n%s -> (%u) %s" % (type_node, counter_children, parent["Path"])
    if ret_val:
        print_str = "\nError #u: %s -> (%u) %s" % (ret_val, type_node, counter_children, parent["Path"])

    utils_print.output(print_str, False)


# =======================================================================================================================
# Prepare output data from the hierarchy. The query does not need the pi values
# data_node shows the path to the data
# =======================================================================================================================
def deliver_hierarchical_data(status, from_leaf, pi_cursor):
    out_instruct = pi_cursor.get_out_instruct()

    data_dest = out_instruct.get_output_dest()
    if data_dest == "network":
        add_column_name = False
        socket = out_instruct.get_socket()
    else:
        add_column_name = True
        socket = None

    io_buff = pi_cursor.get_io_buff_in()
    block_counter = pi_cursor.get_blocks_counter()  # counter for the block being prepared
    rows_counter = pi_cursor.get_rows_counter()  # number of rows are currently in the block
    limit = out_instruct.get_limit()

    # Get the list that shows the output order:
    # the list entries has 3 values -
    # 1) Location in the hierarchy (1 is the bottom)
    # 2) Column number in SQL output
    # 3) and PI column name
    out_order = pi_cursor.get_out_order()

    row_data = make_output_row(status, pi_cursor, from_leaf, out_order, add_column_name, "")

    ret_val, block_counter, rows_counter = transfer_output_row(status, pi_cursor, data_dest, socket, io_buff, row_data,
                                                               block_counter, rows_counter, limit)

    return ret_val


# -----------------------------------------
# Transfer output row by the network or stdout
# -----------------------------------------
def transfer_output_row(status, pi_cursor, data_dest, socket, io_buff, row_data, block_counter, rows_counter, limit):
    # send to network or print
    if data_dest == "network":
        ret_val, block_counter, rows_counter = member_cmd.add_row_to_netowk_block(status, socket, io_buff, row_data,
                                                                                  block_counter, rows_counter)
        pi_cursor.set_blocks_counter(block_counter)
        pi_cursor.set_rows_counter(rows_counter)  # counter for rows inside the block
    else:
        ret_val = process_status.SUCCESS
        output_data = utils_json.str_to_json(row_data)
        if output_data:
            utils_print.jput(output_data, False)
        else:
            utils_print.output("Error in display of data retrieved from PI", True)

    if not ret_val:
        if pi_cursor.count_row_test_limit(limit):  # counts overall rows - and compare to limit
            ret_val = process_status.AT_LIMIT

    reply_list = [ret_val, block_counter, rows_counter]
    return reply_list

# -----------------------------------------
# Create the output row
# -----------------------------------------
def make_output_row(status, pi_cursor, from_leaf, out_order, add_column_name, row):
    row_data = "{\"Query\":[{"

    for entry in out_order:  # out_order determines the output columns
        out_column_number = entry[1]
        pi_column_name = entry[2]
        distance = entry[0]  # look at the hierarchy of elements to provide the value
        if not distance:
            # the data is in the row provided
            is_available, pi_value = utils_columns.get_pi_column_val(row, pi_column_name)
            if not is_available:
                # get data from the hierarchy
                pi_value = pi_cursor.get_from_hierarchy(1, pi_column_name)
        else:
            pi_value = pi_cursor.get_from_hierarchy(distance - from_leaf, pi_column_name)

        field_info = pi_cursor.get_sql_field_info(entry[0], pi_column_name)  # Get the data type, entry[0] is the layer
        if field_info:  # firld info may be None if the row does not exists in SQL
            is_correct_type, transfer_value = utils_columns.transform_by_data_type(pi_value, field_info[3])
            if not is_correct_type:
                transfer_value = pi_value
            if add_column_name:
                attr_name = field_info[2]
            else:
                attr_name = out_column_number
            row_data += "\"" + attr_name + "\":\"" + str(transfer_value) + "\","

    row_data = row_data[0:-1] + "}]}"

    return row_data


# -----------------------------------------
# Info on the output structure and destination
# -----------------------------------------
class OutInstruct():
    def __init__(self):
        self.data_dest = "network"  # destination format, use "json" to print output
        self.socket = None
        self.limit = 0

    def set_limit(self, limit):
        self.limit = limit

    def set_socket(self, socket):
        self.socket = socket

    def get_socket(self):
        return self.socket

    def get_limit(self):
        return self.limit

    # -----------------------------------------
    # Set data destination
    # -----------------------------------------
    def set_output_dest(self, data_dest):
        self.data_dest = data_dest  # destination for the data

    # -----------------------------------------
    # Get destination format
    # -----------------------------------------
    def get_output_dest(self):
        return self.data_dest

    # =======================================================================================================================
    # Test valif format: "json" or "list" or "network"
    # =======================================================================================================================
    def is_valid_output_format(self):
        if self.data_dest == "network":
            valid = True  # send data to the caller
        elif self.data_dest == "list":
            valid = True
        elif self.data_dest == "json":
            valid = True
        else:
            valid = False
        return valid


# =======================================================================================================================
# Get PI data using a REST call
# =======================================================================================================================
def get_xompass_data(status, command, cmd_params):
    ret_val = process_status.SUCCESS

    reply = process_request(status, command, cmd_params)

    if not reply:
        reply_list = [process_status.DBMS_error, None]
        return reply_list

    if reply.status_code != 200:
        reply_list = [process_status.Empty_data_set, None]
        return reply_list

    try:
        json_data = reply.json()
    except:
        ret_val = process_status.DBMS_unexpected_reply
        json_data = None

    if not ret_val:
        if isinstance(json_data, dict):
            if "statusCode" in json_data.keys():
                status_code = json_data["statusCode"]
                if isinstance(status_code, int):
                    if status_code != 200:
                        reply_list = [process_status.Empty_data_set, None]
                        return reply_list

    reply_list = [ret_val, json_data]
    return reply_list


# =======================================================================================================================
# Organize the elements to represent the table
# =======================================================================================================================
def organize_elements(status, pi_cursor, json_data):
    for entry in json_data:
        # go over the entries and map to the table structure

        pi_cursor.add_entry_to_table(status, pi_cursor, json_data[entry])


# =======================================================================================================================
# At the end of the query process - Update a block with the functions data
# =======================================================================================================================
def update_block_with_functions_data(status, pi_cursor):
    field_counter = 0

    out_instruct = pi_cursor.get_out_instruct()

    socket = out_instruct.get_socket()

    io_buff = pi_cursor.get_io_buff_in()

    rows_data = "{\"Query\":[{"

    functions_array = pi_cursor.get_project_func().get_functions()
    for entry in functions_array:
        results = entry[1].get_results()  # result is an array with the result of the function (1 or 2 results)
        for value in results:
            rows_data += "\"" + str(field_counter) + "\":\"" + str(value) + "\","
            field_counter += 1
    rows_data = rows_data[0:-1] + "}]}"
    ret_val, block_counter, rows_counter = member_cmd.add_row_to_netowk_block(status, socket, io_buff, rows_data, 1, 0)

    pi_cursor.set_blocks_counter(block_counter)
    pi_cursor.set_rows_counter(rows_counter)  # counter for rows inside the block

    return ret_val


# =======================================================================================================================
# STEP 5
# # At the end of the query process - Update a block with the functions data organized by increments
# =======================================================================================================================
def update_block_with_increments_data(status, pi_cursor):
    out_instruct = pi_cursor.get_out_instruct()

    socket = out_instruct.get_socket()

    io_buff = pi_cursor.get_io_buff_in()

    functions_array = pi_cursor.get_project_func().get_functions()
    main_entry = functions_array[0]  # Get the first function
    hash_values = main_entry[1].get_hash_values()  # Get a list of all the hash values

    block_counter = 1
    rows_counter = 0

    for hash in hash_values:
        rows_data = "{\"Query\":[{"
        field_counter = 0
        for entry in functions_array:
            function = entry[1].get_function_by_hash(hash,
                                                     "")  # result is an array with the result of the function (1 or 2 results)
            results = function.get_results()
            for value in results:
                rows_data += "\"" + str(field_counter) + "\":\"" + str(value) + "\","
                field_counter += 1
        rows_data = rows_data[0:-1] + "}]}"
        ret_val, block_counter, rows_counter = member_cmd.add_row_to_netowk_block(status, socket, io_buff, rows_data,
                                                                                  block_counter, rows_counter)

    pi_cursor.set_blocks_counter(block_counter)
    pi_cursor.set_rows_counter(rows_counter)  # counter for rows inside the block


# =======================================================================================================================
# apply value to function
# =======================================================================================================================
def apply_value_to_function(status, pi_cursor, pi_key, pi_value):
    ret_val = process_status.SUCCESS

    functions_array = pi_cursor.get_project_func().get_functions()  # get an array showing field name + function

    for entry in functions_array:  # Note: same value can apply to multiple types
        pi_column_name = entry[0]
        function = entry[1]
        if pi_column_name == "all":
            # field name is * - like a select count(*)
            function.process_value(1)  # count(*) translate to count rows considered
        else:
            if function.get_column_name() == pi_key:
                field_info = pi_cursor.get_sql_field_info(entry[2], pi_key)
                if field_info:
                    is_correct_type, value = utils_columns.transform_by_data_type(pi_value, field_info[3])
                    if is_correct_type:
                        function.process_value(value)

    return ret_val


# =======================================================================================================================
# STEP 4 - Execute functions - Count, Min, Max etc
# =======================================================================================================================
def execute_functions(status, tree_depth, json_data, pi_cursor):
    ret_val = process_status.SUCCESS

    functions_array = pi_cursor.get_project_func().get_functions()  # get an array showing field name + function

    if pi_cursor.is_with_increments():
        # get the time from the row and calculate the time unit
        increments = pi_cursor.get_increments()
        time_field_name = increments.get_time_field_name()
        with_increments = True
    else:
        with_increments = False

    limit = pi_cursor.get_out_instruct().get_limit()

    for counter, row in enumerate(json_data):

        # row is None when there is no need to filter
        if row == None or filter_by_where_condition(tree_depth, row, pi_cursor, False):

            if with_increments:
                # get the time from the row and calculate the time unit
                time_field_value = search_for_value_recursively(time_field_name, row)
                hash_value = increments.get_hash_value(
                    time_field_value)  # get a value which is a key for the increments (based on the date)

            for entry in functions_array:
                pi_column_name = entry[0]
                layer = entry[2]
                if with_increments:
                    function = entry[1].get_function_by_hash(hash_value,
                                                             time_field_value)  # get a function relating the id of the increment
                else:
                    function = entry[1]
                if pi_column_name == "all":
                    # field name is * - like a select count(*)
                    function.process_value(1)  # count(*) translate to count rows considered
                else:
                    if layer == 0:
                        # get the value from the row
                        pi_value = search_for_value_recursively(pi_column_name, row)
                    else:
                        # get the value from the hierarchy
                        pi_value = pi_cursor.get_from_hierarchy(tree_depth, pi_column_name)

                    if pi_value:
                        field_info = pi_cursor.get_sql_field_info(layer, pi_column_name)
                        if field_info:
                            is_correct_type, pi_value = utils_columns.transform_by_data_type(pi_value, field_info[3])
                            if is_correct_type:
                                function.process_value(pi_value)

        if pi_cursor.count_row_test_limit(limit):  # counts overall rows - and compare to limit
            ret_val = process_status.AT_LIMIT
            break

    return ret_val


# =======================================================================================================================
# Search a dictionary recursively to return the needed value.
# if not found, return ""
# =======================================================================================================================
def search_for_value_recursively(key, dict_root):
    if key in dict_root.keys():
        # this is a fast lookup for the one hierarchy case
        value = dict_root[key]
        if isinstance(value, dict):
            return search_for_value_recursively(key, value)
        return value

    for entry in dict_root.keys():
        dict_child = dict_root[entry]
        if isinstance(dict_child, dict):
            value = search_for_value_recursively(key, dict_child)
            if value:
                return value

    return ""


# =======================================================================================================================
# save data if needs to be projected or returned
# =======================================================================================================================
def save_hierarchy_data(status, tree_depth, pi_cursor, json_data):
    # Get the list that shows the output order:
    # the list entries has 3 values -
    # 1) Location in the hierarchy (1 is the bottom)
    # 2) Column number in SQL output
    # 3) and PI column name
    layer = pi_cursor.get_layer(tree_depth)
    if layer:
        out_order = pi_cursor.get_out_order()
        for entry in out_order:
            if entry[0] == layer:
                out_column_number = entry[1]
                pi_column_name = entry[2]
                if pi_column_name in json_data.keys():
                    pi_cursor.save_hierarchy_data(out_column_number, json_data[pi_column_name])


# =======================================================================================================================
# filter the data by the where condition
# =======================================================================================================================
def filter_by_where_condition(tree_depth, row, pi_cursor, ignore_missing_pi_column):
    root_node = pi_cursor.get_select_parsed().get_where_tree()

    if root_node:
        ret_value = root_node.traversal_pi_where_tree(True, tree_depth, row, ignore_missing_pi_column)
    else:
        ret_value = True

    return ret_value


# =======================================================================================================================
# Traversal in PI is at a leaf with KEY and Value
# =======================================================================================================================
def process_value(status, pi_cursor, pi_key, pi_value):
    ret_val = process_status.SUCCESS

    out_instruct = pi_cursor.get_out_instruct()

    if out_instruct.get_output_dest() == "network":
        if pi_cursor.get_select_parsed().is_functions_values():
            # send the data to a function
            ret_val = apply_value_to_function(status, pi_cursor, pi_key, pi_value)
        data = ""

    else:
        # output result

        if out_instruct.get_output_dest() == "json":
            end_line = "\","
        else:
            end_line = "\"\n"

        data = "\"" + str(pi_key) + ":\"" + str(pi_value) + end_line

    reply_list = [ret_val, data]
    return reply_list


# =======================================================================================================================
# Get and register the PI databases
# databases = register pi dbms 10.0.0.41 7400
# =======================================================================================================================
def info_dbms(status, host, port):
    command_prefix = 'http://' + host + ':' + port
    # GET List of Asset Frameworks

    command = command_prefix + "/api/af_list"  # get the list of asset frameworks
    af = process_request(status, command, None)
    if not af:
        return ""

    af_data = af.json()
    af_keys = list(af_data.keys())  # transform the previous response object keys into array

    json_dbms_prefix = "{\"dbms\": {\"type\":\"pi\",\"ip\":\"" + host + "\",\"port\":\"" + port + "\""

    databases = ""
    for af_key in af_keys:
        params = {'afid': af_data[af_key]["Id"]}  # get the first key of elements and pass it as url parameter
        command = command_prefix + '/api/af_databases'
        af_dbms = process_request(status, command, params)
        # get the list of databases
        af_dbms_data = af_dbms.json()  # dictionary of all databases
        for db in af_dbms_data.keys():
            if len(databases):
                databases += ',\n'  # not the first
            databases += json_dbms_prefix
            dbms = af_dbms_data[db]
            for db_key in dbms.keys():
                key = str(db_key)
                if key == 'Path':
                    value = str(dbms[db_key])
                    value = value.replace('\\\\', '/')
                    value = value.replace('\\', '/')
                else:
                    value = str(dbms[db_key])
                databases += ",\"" + key + "\":\"" + value + "\""
            databases += '}}'

    if databases != "":
        databases = "[" + databases + "]"
        # databases = "{\"PI databases\":[" + databases + "]}"
    return databases


# =======================================================================================================================
# Get elements in database or info of a particular element
# =======================================================================================================================
def info_request(status, url, vars, element_id):
    reply = process_request(status, url, vars)

    str_data = ""
    if reply != None:
        try:
            json_data = reply.json()
        except:
            str_data = ""
            error_msg = "REST reply from PI not in JSON format"
            status.add_error(error_msg)
        else:
            if element_id and element_id in json_data.keys():
                json_data = json_data[element_id]

            if isinstance(json_data, dict):
                for key in json_data:
                    value = json_data[key]
                    if isinstance(value, dict) or isinstance(value, list):
                        str_data += "\"" + str(key) + "\":\"{}\"\n"
                    else:
                        str_data += "\"" + str(key) + "\":\"" + str(value) + "\"\n"

            else:
                if isinstance(json_data, list):
                    for entry in json_data:
                        str_data += str(entry) + '\n'
                else:
                    str_data = str(json_data)

    return str_data


# =======================================================================================================================
# Get and register the PI schema
# =======================================================================================================================
def query(status, url, query_params, secondary_id):
    if len(query_params) < 3 or query_params[0] != '{' or query_params[-1] != '}':
        error_msg = "Query params provided to PI are not declared within brackets: {...}"
        status.add_error(error_msg)
        str_data = ""
    else:
        vars = query_params[1:-1].replace(" ", "")  # remove spaces
        vars = vars.replace("+", "&")
        url += vars
        str_data = info_request(status, url, None, secondary_id)

    return str_data


# =======================================================================================================================
# connect to the provided Asse Framework and to the Database
# =======================================================================================================================
def connect_to_pi(status, host_port, connect_str, auth_token, af_name, db_name):
    root = None
    af_id = ""
    db_id = ""

    # GET List of Asset Frameworks
    if connect_str:
        # new PI interface
        ret_val, af_reply = http_client.get_jdata(status, connect_str, {}, auth_token, 10)
        if (len(af_reply) and "Items" in af_reply.keys()):
            af_data = af_reply["Items"]
        else:
            return {"", "", None}

        asset_framework = utils_data.get_from_list(af_data, "Name", af_name)  # get an entry where "Name" is af_name
        # Get the needed DBMS
        if asset_framework:
            af_id, db_id, root = connect_pi(status, host_port, asset_framework, db_name, auth_token)

    else:
        # Xompass Interfcae

        url = 'http://%s/api/af_list' % host_port

        ret_val, af_data = get_xompass_data(status, url, None)

        if not ret_val:
            # GET the needed AF
            asset_framework = utils_data.get_from_dict(af_data, "Name", af_name)  # get an entry where "Name" is af_name
            # Get the needed DBMS
            if asset_framework:
                af_id, db_id, root = connect_xompass(status, host_port, asset_framework, db_name)

    reply_list = [af_id, db_id, root]
    return reply_list


# =======================================================================================================================
# Connect with PI interface - Get first layer of ELEMENTS for the needed DBMS
# =======================================================================================================================
def connect_pi(status, host_port, asset_framework, db_name, auth_token):
    root = None
    af_id = ""
    db_id = ""

    if "Id" in asset_framework.keys():
        af_id = asset_framework["Id"]

    af_reply = get_from_pi(status, asset_framework, "Links", "Databases", auth_token)
    if af_reply:
        # af_reply is a list with first entry being an int
        db_list = utils_data.get_from_list(af_reply, "Items", None)
        if db_list:
            af_db = utils_data.get_from_list(db_list, "Name", db_name)  # Get the DBMS that maintains the needed data
            if "Id" in af_db.keys():
                db_id = af_db["Id"]
                elements = get_from_pi(status, af_db, "Links", "Elements", auth_token)
                root = utils_data.get_from_list(elements, "Items", None)

    reply_list = [af_id, db_id, root]
    return reply_list


# =======================================================================================================================
# Move in the PI hierarchy and return the relevant JSON
# =======================================================================================================================
def get_from_pi(status, src_json, attribute_key, url_key, auth_token):
    dest_json = None
    if attribute_key in src_json.keys():
        struct = src_json[attribute_key]
        if url_key in struct.keys():
            url = struct[url_key]
            if isinstance(url, str):
                dest_json = http_client.get_jdata(status, url, {}, auth_token, 10)
    return dest_json


# =======================================================================================================================
# Connect with Xompass interface - Get first layer of ELEMENTS for the needed DBMS
# =======================================================================================================================
def connect_xompass(status, host_port, asset_framework, db_name):
    root = None
    af_id = ""
    db_id = ""

    url = 'http://%s/api/af_databases' % host_port
    if "Id" in asset_framework.keys():
        af_id = asset_framework["Id"]
        params = {'afid': af_id}

        ret_val, af_db = get_xompass_data(status, url, params)

        # Get the element
        if not ret_val:
            # get a dbms entry where "Name" is db_name
            af_db = utils_data.get_from_dict(af_db, "Name", db_name)

            if af_db:

                if "Id" in af_db.keys():
                    db_id = af_db["Id"]

                    # get the root element
                    url = 'http://%s/api/af_root_elements' % host_port
                    params = {'dbid': db_id}

                    ret_val, root = get_xompass_data(status, url, params)

    reply_list = [af_id, db_id, root]
    return reply_list
# =======================================================================================================================
# Get and register the PI schema - Use port 7400
# =======================================================================================================================
def test_pi(status, host, port):
    try:

        # GET List of Asset Frameworks

        URL = 'http://%s:%u/api/af_list' % (host, port)

        utils_print.output(URL, False)

        r = requests.get(url=URL, timeout=3)

        utils_print.output(" --> %s\n" % str(r), True)

        data = r.json()

        utils_print.output(data, True)

        data_keys = list(data.keys())  # transform the previous response object keys into array

        # GET AF dabases of first AF

        PARAMS = {'afid': data[data_keys[0]]["Id"]}  # get the first key of elements and pass it as url parameter
        URL = 'http://%s:%u/api/af_databases' % (host, port)

        utils_print.output(URL + "?" + "afid=" + PARAMS["afid"], False)

        r = requests.get(url=URL, params=PARAMS)

        utils_print.output(" --> %s\n" % str(r), True)

        af_db_data = r.json()

        utils_print.output(af_db_data, True)

        # Sync with one database - 'dee1a62d-9eac-4945-82e1-4a1e9baa9d8e'
        # PARAMS = {'dbid': list(af_db_data.keys())[2]}  # get the first key of elements and pass it as url parameter
        # URL = 'http://%s:%u/api/af_sync' % (host, port)

        # utils_print.output(URL + "?" + "dbid=" + PARAMS["dbid"], True)

        # r = requests.get(url=URL, params=PARAMS)

        # the list of elements in a database

        PARAMS = {'afid': data[data_keys[0]]["Id"]}  # get the first key of elements and pass it as url parameter
        URL = 'http://%s:%u/api/get_element_list' % (host, port)

        utils_print.output(URL + "?" + "afid=" + PARAMS["afid"], False)

        r = requests.get(url=URL, params=PARAMS)

        utils_print.output(" --> %s\n" % str(r), True)

        elem_in_dbms = r.json()

        utils_print.output(elem_in_dbms, True)

        # Get ROOT Element

        PARAMS = {
            'dbid': "32c64d82-9788-441d-b9e5-72a670e5b1a9"}  # get the first key of elements and pass it as url parameter
        URL = 'http://%s:%u/api/af_root_elements' % (host, port)

        utils_print.output(URL + "?" + "dbid=" + PARAMS["dbid"], False)

        r = requests.get(url=URL, params=PARAMS)

        utils_print.output(" --> %s\n" % str(r), True)

        root = r.json()

        # Get Child of Element

        PARAMS = {
            'eid': "7548a7d9-5bd2-11ea-a080-e0dd40946454"}  # get the first key of elements and pass it as url parameter
        URL = 'http://%s:%u/api/children_elements' % (host, port)

        utils_print.output(URL + "?" + "eid=" + PARAMS["eid"], False)

        r = requests.get(url=URL, params=PARAMS)

        utils_print.output(" --> %s\n" % str(r), True)

        children = r.json()

        # "http://10.0.0.25:7400/api/af_root_elements?dbid=32c64d82-9788-441d-b9e5-72a670e5b1a9"
        # "http://10.0.0.25:7400/api/children_elements?eid=07c7c759-527c-11ea-a078-b05249f3e730"

        # Example 1 - given an element - attributes names and data type of each attribute

        PARAMS = {'eid': list(elem_in_dbms.keys())[3]}  # get the first key of elements and pass it as url parameter
        URL = 'http://%s:%u/api/get_sensors_in_element' % (host, port)
        r = requests.get(url=URL, params=PARAMS)
        af_db_attr = r.json()
        print(af_db_attr)
        # note: CategoryName is a key to the sensot that was set by the customer

        # "http://10.0.0.25:7400/api/af_root_elements?dbid=32c64d82-9788-441d-b9e5-72a670e5b1a9"
        # "http://10.0.0.25:7400/api/children_elements?eid=07c7c759-527c-11ea-a078-b05249f3e730"

        # Example 2 - given a sensor - the readings of the sensor

        PARAMS = {'time': '*-1h', 'sid': list(af_db_attr.keys())[
            0]}  # (1 --> 0) get the first key of elements and pass it as url parameter
        URL = 'http://%s:%u/api/get_sensor_data' % (host, port)

        r = requests.get(url=URL, params=PARAMS)
        af_sensor_reading = r.json()
        print(af_sensor_reading)

        # READING CURRENT TIME
        PARAMS = {'time': '*', 'sid': list(af_db_attr.keys())[
            0]}  # (1-->0) get the first key of elements and pass it as url parameter
        URL = 'http://%s:%u/api/get_sensor_data' % (host, port)

        r = requests.get(url=URL, params=PARAMS)
        af_sensor_reading = r.json()
        print(af_sensor_reading)

        PARAMS = {'stime': '*-1h', 'etime': '*', 'sid': list(af_db_attr.keys())[
            0]}  # (1->0) get the first key of elements and pass it as url parameter
        URL = 'http://%s:%u/api/get_sensor_data_range' % (host, port)
        r = requests.get(url=URL, params=PARAMS)
        af_sensor_reading = r.json()
        print(af_sensor_reading)

        # OTHER TESTS

        PARAMS = {'time': '2019-10-15T00:00:00.0000000Z', 'sid': 'd67079ff-f13b-5a74-2336-969d2cfc08e2'}
        URL = 'http://%s:%u/api/get_sensor_data' % (host, port)
        r = requests.get(url=URL, params=PARAMS)
        af_sensor_reading = r.json()
        print(af_sensor_reading)

        PARAMS = {'stime': '2019-10-14T23:59:00.0000000Z', 'etime': '2019-10-15T00:00:01.0000000Z',
                  'sid': 'd67079ff-f13b-5a74-2336-969d2cfc08e2'}
        URL = 'http://%s:%u/api/get_sensor_data_range' % (host, port)
        r = requests.get(url=URL, params=PARAMS)
        af_sensor_reading = r.json()
        print(af_sensor_reading)


    except:
        utils_print.output(" --> Failed", True)


# =======================================================================================================================
# PI CALL - Execute a rest request
# =======================================================================================================================
def process_request(status, command, cmd_params):
    r = None
    try:
        r = requests.get(url=command, params=cmd_params, timeout=10)
    except HTTPError as http_err:
        error_msg = "REST client error connecting with PI: %s" % str(http_err)
        status.add_error(error_msg)
    except Exception as err:
        error_msg = "REST client error processing with PI: %s" % str(err)
        status.add_error(error_msg)
    return r


# =======================================================================================================================
# PI Cursor
# =======================================================================================================================
class PI_CURSOR:
    description = [[""] * 2 for i in range(2)]

    def __init__(self, host_port, dbms_name):
        self.host_port = host_port
        self.sql_dbms_name = dbms_name  # the dbms name of the sql table
        self.pi_dbms_id = ""  # The PI DBMS ID
        self.pi_af_id = ""  # The PI AF ID
        self.pi_dbms_name = ""  # The PI DBMS name
        self.pi_af_name = ""  # The PI AF name
        self.select_parsed = al_parser.SelectParsed()
        self.io_buff_in = None
        self.network_block_counter = 0  # counter for blocks transferred over the network
        self.out_instruct = OutInstruct()  # output instructions
        self.block_counter = 1  # counter for the block being prepared
        self.rows_counter = 0  # counter for the number of rows in the block being updated
        self.rows_returned = 0  # number of rows returned to the caller
        self.map_column_pi_sql = {}  # a dictionary that maps the PI attribute name to the table column names
        self.map_column_sql_pi = {}  # a dictionary to map sql names to PI key (level + PI attribute Name)
        self.pi_where_parsed = []  # PI where condition (based on the SQL where condition) -> [pi name][condition][value][data type]
        self.pi_search_keys = []  # specifies the time search conditions like: 'time', 'stime' 'etime'
        self.out_order = []  # an array that list the columns that needs to be send by the order it needs to be send
        self.data_buffer = []  # a list to save data pulled while traversing the hierarchy, ordered bty the output
        self.hierarchy = []  # hierarchy data as f(layer)
        self.date_cmd_params = {}  # the time search key for the PI readings
        self.date_cmd = ""  # The URL for the serach key of PI reading in a time range
        self.project_func = utils_columns.ProjectionFunctions()
        self.increments = None
        self.with_increments = False
        self.table_path = None  # An array representing the path which maps to a table
        self.element_tree = []  # a tree structure that contains the structure and the hierarchical elements
        self.path_prefix = ""  # the af name + dbms name
        self.sensor_lookup = []  # an array with element ids and sensor ids that satisfy the query
        self.element_lookup = []  # an array with element ids that satisfy the query
        self.sensors_visited = 0  # a counter for the number of sensors visited
        self.elements_visited = 0  # a counter for the number of elements visited
        self.pi_interface = ""  # xompass or web_api
        self.root_elements = None  # first elements that are provided in the connect
        self.usr_auth_token = ""  # A token that is based ob the PI dbms user and password
        self.search_string = ""  # A string that contains the suffix of the URL for the Web API
        self.with_readings = False  # If value is True - need to visit the PI Values in the traversal
        self.with_sensors = False  # If value is True - need to visit the senors in the traversal
        # List of fields to retrieve from the elements
        self.selected_fields = "?selectedFields=Items.links.Elements;Items.links.Attributes;Items.Name;Items.Path;Items.WebId;" \
                               "Items.HasChildren"

    # ==================================================================
    # Get the element fields to return
    # ==================================================================
    def get_selected_fields(self):
        return self.selected_fields

    # ==================================================================
    # The traversal process needs to visit the readings at the leaf of the hierarchy
    # ==================================================================
    def is_with_readings(self):
        return self.with_readings

    # ==================================================================
    # The traversal process needs to visit the sensors (that leads to the eadings)
    # ==================================================================
    def is_with_sensors(self):
        return self.with_sensors

    # ==================================================================
    # Save the output destination: "network", "stdout"
    # ==================================================================
    def set_output_dest(self, output_dest):
        self.out_instruct.set_output_dest(output_dest)

    # ==================================================================
    # Save the output destination: "network", "stdout"
    # ==================================================================
    def set_output_dest(self, output_dest):
        self.out_instruct.set_output_dest(output_dest)

    # ==================================================================
    # Return the search string for the WEB API
    # ==================================================================
    def get_search_string(self):
        # the last char is an '&' which allows to extend the key but is not part of the search
        return self.search_string[:-1]

    def set_auth_token(self, auth_token):
        self.usr_auth_token = auth_token

    def get_auth_token(self):
        return self.usr_auth_token

    # ==================================================================
    # Store the root elements that relate to the DBMS and AF
    # ==================================================================
    def set_root_elements(self, root):
        self.root_elements = root

    # ==================================================================
    # Get the root elements that relate to the DBMS and AF
    # ==================================================================
    def get_root_elements(self):
        return self.root_elements

    def set_interface(self, pi_interface):
        self.pi_interface = pi_interface

    def get_interface(self):
        return self.pi_interface

    def is_interface(self, interface):
        return self.pi_interface == interface

    # ==================================================================
    # Return an array with sensor ids that satisfy the query.
    # This array is set from the query where condition.
    # ==================================================================
    def get_sensor_lookup(self):
        return self.sensor_lookup

    # ==================================================================
    # Return an array with element ids that satisfy the query.
    # This array is set from the query where condition.
    # ==================================================================
    def get_element_lookup(self):
        return self.element_lookup

    def count_sensors_visited(self):
        self.sensors_visited += 1

    def count_elements_visited(self):
        self.elements_visited += 1

    # ==================================================================
    # Test if all sensors visited by comparing sensors in the where condition
    # to the sensors visited
    # ==================================================================
    def all_query_sensors_visited(self):
        if len(self.sensor_lookup):
            if self.sensors_visited >= len(self.sensor_lookup):
                return True
        return False

    # ==================================================================
    # Make the date search key
    # ==================================================================
    def make_date_pi_key(self):

        search_keys = self.get_search_keys()

        with_time = False
        start_time = ""
        end_time = ""
        exact_time = False
        for pi_search_info in search_keys:
            search_type = pi_search_info[0]
            operand = pi_search_info[1]

            if self.is_interface("web_api"):
                value = utils_columns.unify_time_format(None, pi_search_info[2])
            else:
                value = pi_search_info[2]

            if search_type == "time":
                with_time = True
                if operand == '=':
                    exact_time = True
                    start_time = value

                    self.search_string += "Time=%s&" % value

                elif operand == '<':
                    end_time = value

                    self.search_string += "EndTime=%s&" % value

                elif operand == "<=":
                    end_time = value

                    self.search_string += "EndTime=%s&" % value

                elif operand == '>':
                    start_time = value

                    self.search_string += "StartTime=%s&" % value

                elif operand == ">=":
                    start_time = value

                    self.search_string += "StartTime=%s&" % value

            elif search_type == "sensor_id":
                pass

        if not with_time:
            self.search_string += "StartTime=1970-01-01&EndTime=*&"  # include all

        if self.is_interface("xompass"):
            self.set_xompass_date_cmd(with_time, exact_time, start_time, end_time)

    # -----------------------------------------
    # Set the date paramns for the xompass interface
    # -----------------------------------------
    def set_xompass_date_cmd(self, with_time, exact_time, start_time, end_time):
        if with_time:
            if exact_time == True:
                # search by a specific time - search for data equals to time
                self.date_cmd = "http://%s/api/get_sensor_data" % self.get_host_port()
                self.date_cmd_params['time'] = start_time
            else:
                # need to have stime and etime
                if end_time and not start_time:
                    # find the nearest time before the time provided
                    self.date_cmd = "http://%s/api/get_sensor_data" % self.get_host_port()
                    self.date_cmd_params['time'] = end_time
                    self.date_cmd_params['retrievalmode'] = "Before"
                else:
                    # the default is a range search
                    if not start_time:
                        start_time = "01/01/70"
                    if not end_time:
                        end_time = "*"
                    self.date_cmd = "http://%s/api/get_sensor_data_range" % self.get_host_port()
                    self.date_cmd_params['stime'] = start_time
                    self.date_cmd_params['etime'] = end_time
        else:
            # the default is a range search
            self.date_cmd = "http://%s/api/get_sensor_data_range" % self.get_host_port()
            self.date_cmd_params['stime'] = "01/01/70"
            self.date_cmd_params['etime'] = "*"

    # -----------------------------------------
    # Get the url for the date search
    # -----------------------------------------
    def get_date_cmd(self):
        return self.date_cmd

    # -----------------------------------------
    # Get the time range key
    # -----------------------------------------
    def get_date_cmd_params(self):
        return self.date_cmd_params

    # -----------------------------------------
    # The hierarchy of elements and sensors - root to leaf
    # -----------------------------------------
    def place_in_hierarchy(self, depth, element):
        if depth > len(self.hierarchy):
            self.hierarchy.append(element)
        else:
            self.hierarchy[depth - 1] = element

    # -----------------------------------------
    # get value from hierarchy
    # -----------------------------------------
    def get_from_hierarchy(self, location, key):
        depth = len(self.hierarchy)  # number of elements in the hierarchy
        if location <= depth:
            index = depth - location
            if key in self.hierarchy[index].keys():
                return str(self.hierarchy[index][key])
        return ""

    # -----------------------------------------
    # Return the path prefix - AF + DBMS name
    # -----------------------------------------
    def get_path_prefix(self):
        return self.path_prefix

    # -----------------------------------------
    # Return the path that represent a table
    # -----------------------------------------
    def get_table_path(self):
        return self.table_path

    # -----------------------------------------
    # Return a tree structure that contains the logical structure and the hierarchical elements
    # -----------------------------------------
    def get_element_tree(self):
        return self.element_tree

    # -----------------------------------------
    # Update the path which mapps to the table
    # Create a meta-table that describes the table structure
    # -----------------------------------------
    def set_table_path(self, table_path):
        self.table_path = table_path
        for path in table_path:
            offset = 1
            previous_offset = 0
            segment_length = 0

            t_struct = self.element_tree

            while segment_length != -1:
                segment_length = path[offset:].find("\\")
                if segment_length == -1:
                    offset = len(path)
                else:
                    offset += segment_length

                index = path[previous_offset:offset].find("*")

                if (index != -1):
                    key = '*'
                    suffix = path[previous_offset:previous_offset + index]  # save the prefix
                else:
                    key = path[previous_offset:offset]  # Get the sub-key
                    suffix = ""

                previous_offset = offset

                if not len(t_struct):
                    # create new entry
                    t_struct.append(
                        {})  # 1st entry is a dictionary to the next layer, Every entry in the dictionary is a key to an array
                    t_struct.append(
                        {})  # 2st entry is a dictionary to the next layer, Every entry in the dictionary is a suffix to an array
                    t_struct.append([])  # 3nd entry is an array to elements that satisfy the tree condition

                if suffix:
                    if not suffix in t_struct[1].keys():
                        t_struct[1][suffix] = []  # create sub-key
                    t_struct = t_struct[1][suffix]  # this sub-key exists or created
                else:
                    if not key in t_struct[0].keys():
                        t_struct[0][key] = []  # create sub-key
                    t_struct = t_struct[0][key]  # this sub-key exists or created
                offset += 1

        # the af name + dbms name
        self.path_prefix = "\\\\" + self.pi_af_name + "\\" + self.pi_dbms_name + "\\"

    # =======================================================================================================================
    # Test if the entry makes part of the table - if so add to the table
    # Key can be string, or a prefix of a key followed by '*'
    # Create a neseted dictionary representing the hierarchy with the PI elements.
    # =======================================================================================================================
    def add_entry_to_table(self, status, pi_cursor, entry):

        offset = len(self.path_prefix)  # ignote the AF name and the dbms name
        path = entry["Path"]
        if len(path) <= offset:
            return  # ignore AF and DBMS entries
        elif self.path_prefix != path[:offset]:
            return  # different AF or different database

        previous_offset = offset - 1
        t_struct = self.element_tree
        segment_length = 0

        while segment_length != -1:
            segment_length = path[offset:].find("\\")
            if segment_length == -1:
                offset = len(path)
            else:
                offset += segment_length

            key = path[previous_offset:offset]  # Get the sub-key
            previous_offset = offset
            if key in t_struct[0].keys():
                if segment_length == -1:
                    # key maintains the entire path - keep a link to the Element
                    t_struct[2].append(entry)  # save the entry
                    break
                t_struct = t_struct[0][key]  # continue to next layer as f(key)
                offset += 1
                continue
            elif len(t_struct[1]):
                # Prefix Array
                prefix_tree = t_struct[1]
                for prefix in prefix_tree.keys():
                    if prefix == key[:len(prefix)]:
                        if segment_length == -1:
                            # key maintains the entire path - keep a link to the Element
                            t_struct[2].append(entry)  # save the entry
                            offset += 1
                            break  # done with the entry
                        else:
                            t_struct = t_struct[1]
                            offset += 1
                            continue

            break  # not being used

    # -----------------------------------------
    # Return the database name
    # -----------------------------------------
    def get_dbms_name(self):
        return self.pi_dbms_name

    # -----------------------------------------
    # Set the dbms id
    # -----------------------------------------
    def set_dbms_id_name(self, dbms_id, dbms_name):
        self.pi_dbms_id = dbms_id
        self.pi_dbms_name = dbms_name

    # -----------------------------------------
    # Set Asset Framework ID
    # -----------------------------------------
    def set_af_id_name(self, af_id, af_name):
        self.pi_af_id = af_id
        self.pi_af_name = af_name

    # -----------------------------------------
    # Return Asset Framework ID
    # -----------------------------------------
    def get_af_id(self):
        return self.pi_af_id

    # -----------------------------------------
    # Is a Query that is using increments
    # -----------------------------------------
    def is_with_increments(self):
        return self.with_increments

    # -----------------------------------------
    # Return the increment object
    # -----------------------------------------
    def get_increments(self):
        return self.increments

    # -----------------------------------------
    # This query organizes the results in increments
    # return 0 if failed to set the increment values
    # -----------------------------------------
    def set_increments_query(self, interval_type, start_date, interval_units, time_field_name):

        self.with_increments = True
        self.increments = utils_columns.INCREMENTS(interval_type, start_date, interval_units, time_field_name)
        return self.increments.set_increments_base()

    # -----------------------------------------
    # Return the object with the Projection Functions (MIN, MAX, SUM)
    # -----------------------------------------
    def get_project_func(self):
        return self.project_func

    # -----------------------------------------
    # Save data from the hierarchy of PI as a f(output position)
    # -----------------------------------------
    def save_hierarchy_data(self, out_column_number, data_value):
        self.data_buffer[int(out_column_number)] = data_value

    # -----------------------------------------
    # Get data from the hierarchy of PI as a f(output position)
    # -----------------------------------------
    def get_hierarchy_data(self, out_column_number):
        return self.data_buffer[int(out_column_number)]

    # -----------------------------------------
    # set data buffer for the hierarchy data
    # -----------------------------------------
    def set_data_buffer(self, counter_out_columns):
        for i in range(counter_out_columns):
            self.data_buffer.append(None)

    # -----------------------------------------
    # Based on the PI search keys - find the start_date
    # -----------------------------------------
    def get_start_date(self):
        for entry in self.pi_search_keys:
            if entry[0] == "time":
                operand = entry[1]
                if operand == ">" or operand == ">=" or operand == "=":
                    return entry[2]
        return ""

    # -----------------------------------------
    # Set the search key - a key to retrieve readings
    # -----------------------------------------
    def set_search_key(self, pi_search_type, operand, value):
        self.pi_search_keys.append((pi_search_type, operand, value))

    # -----------------------------------------
    # Get the search key - a key to retrieve readings
    # -----------------------------------------
    def get_search_keys(self):
        return self.pi_search_keys

    # -----------------------------------------
    # Get PI where condition (based on the SQL where condition)
    # structure is: [pi name][condition][value][data type]
    # -----------------------------------------
    def get_pi_where_parsed(self):
        return self.pi_where_parsed

    # -----------------------------------------
    # Get PI attribute name and type by SQL field name
    # -----------------------------------------
    def get_pi_name_type(self, sql_field_name):

        if sql_field_name in self.map_column_sql_pi.keys():
            # sql field name was mapped to PI
            pi_layer_name = self.map_column_sql_pi[sql_field_name]
            info = self.map_column_pi_sql[pi_layer_name]
            reply_list = [info[0], info[3]]  # return PI Name + Type
            return reply_list

        reply_list = ["", ""]
        return reply_list

    # -----------------------------------------
    # Get PI Column Info by SQL field name
    # -----------------------------------------
    def get_pi_column_info(self, sql_field_name):

        if sql_field_name in self.map_column_sql_pi.keys():
            # sql field name was mapped to PI
            pi_layer_name = self.map_column_sql_pi[sql_field_name]
            return self.map_column_pi_sql[pi_layer_name]

        reply_list = [None]
        return reply_list

    # -----------------------------------------
    # Get PI attribute name by SQL field name
    # -----------------------------------------
    def get_pi_name(self, sql_field_name):

        if sql_field_name in self.map_column_sql_pi.keys():
            # sql field name was mapped to PI
            pi_layer_name = self.map_column_sql_pi[sql_field_name]
            return self.map_column_pi_sql[pi_layer_name][0]

        return ""

    # -----------------------------------------
    # Get PI layer and attribute name by SQL field name
    # -----------------------------------------
    def get_pi_layer_name(self, sql_field_name):

        if sql_field_name in self.map_column_sql_pi.keys():
            # sql field name was mapped to PI
            pi_layer_name = self.map_column_sql_pi[sql_field_name]
            info = self.map_column_pi_sql[pi_layer_name]
            reply_list = [info[4], info[0]]  # return layer + PI Attribute Name
            return reply_list

        reply_list = [0, None]
        return reply_list

    # -----------------------------------------
    # Get PI layer and attribute name by SQL field name
    # -----------------------------------------
    def get_pi_layer(self, sql_field_name):

        if sql_field_name in self.map_column_sql_pi.keys():
            # sql field name was mapped to PI
            pi_layer_name = self.map_column_sql_pi[sql_field_name]
            return self.map_column_pi_sql[pi_layer_name][4]

        return 0

    # -----------------------------------------
    # Get map_column_pi_sql - a dictionary that maps the PI column names to the table column names
    # -----------------------------------------
    def get_map_column_pi_sql(self):
        return self.map_column_pi_sql

    # -----------------------------------------
    # Map PI columns name to sql
    # Set the SQL info as f(PI column name)
    # PI columns names are defined in the blockchain under attributes,
    # -----------------------------------------
    def map_pi_column_names(self, pi_column_name, column_number, sql_column_name, column_type, layer):

        key = str(
            layer) + '.' + pi_column_name  # With PI, there are the same names in different layers - therefore we add layer to make the key unique

        if key in self.map_column_pi_sql.keys():
            return False

        self.map_column_pi_sql[key] = (pi_column_name, str(column_number), sql_column_name, column_type, layer)

        self.map_column_sql_pi[sql_column_name] = key

        return True

    # -----------------------------------------
    # Get the list that shows the output order.
    # the list entries has 3 values -
    # Location in the hierarchy (1 is the bottom)
    # Column number in the output
    # PI column name
    # -----------------------------------------
    def get_out_order(self):
        return self.out_order

    # -----------------------------------------
    # Set a list of PI columns that needs to send to the user.
    # The list is in the order of output.
    # The order is determined by the SQL select.
    # For select *, the order is the order of the columns in the table.
    # -----------------------------------------
    def set_reply_info(self, table_struct, sql_parsed):
        '''
        :param table_struct: The Columns as defined in SQL
        :param sql_parsed: The info requested by the user
        '''
        column_number = 0
        if sql_parsed.is_columns_values():  # for function on the projection, this process is not needed
            if sql_parsed.is_select_all():
                for column_number, column_info in enumerate(table_struct):
                    sql_name = column_info["column_name"]
                    self.set_out_order(column_number, sql_name)  # change sql name to pi name and store in a struct
            else:
                projection_parsed = sql_parsed.get_projection_parsed()
                for column_number, column_info in enumerate(projection_parsed):
                    sql_name = column_info[0]
                    self.set_out_order(column_number, sql_name)  # change sql name to pi name and store in a struct
        else:
            # functions
            projection_parsed = sql_parsed.get_projection_parsed()
            for column_number, column_info in enumerate(projection_parsed):
                sql_name = column_info[0]
                layer, pi_name = self.get_pi_layer_name(sql_name)
                self.set_pi_nodes_to_visit(layer, pi_name)

        return column_number  # return number of columns

    # -----------------------------------------
    # set output order -
    # given a sql column name, find the pi column name and set in the out_order structure
    # -----------------------------------------
    def set_out_order(self, column_number, sql_name):

        layer, pi_name = self.get_pi_layer_name(sql_name)
        self.out_order.append((layer, str(column_number), pi_name))  # get field value from the last layer
        self.set_pi_nodes_to_visit(layer, pi_name)

    # -----------------------------------------
    # set output order -
    # given a sql column name, find the pi column name and set in the out_order structure
    # -----------------------------------------
    def set_pi_nodes_to_visit(self, layer, pi_name):
        # set the nodes to visit
        if layer == 0:
            self.with_readings = True  # need to visit the PI Values
            self.with_sensors = True  # need to visit the senors
        elif layer == 1:
            self.with_sensors = True

        if layer:
            # Layer 0 maintains the readings and not needed in the selected fields string
            # these are the fields in PI which are in the elements and beinged mapped to the table
            self.selected_fields += ";Items." + pi_name

    # -----------------------------------------
    # Given a PI column name, return the SQL info
    # -----------------------------------------
    def get_sql_field_info(self, layer, pi_column_name):

        if pi_column_name:
            key = str(layer) + '.' + pi_column_name

            try:
                return self.map_column_pi_sql[key]
            except:
                return None
        return None  # field that does not exists in PI

    # -----------------------------------------
    # A counter for the number of rows in the block being updated
    # -----------------------------------------
    def get_rows_counter(self):
        return self.rows_counter

    # -----------------------------------------
    # A counter for the number of rows in the block being updated
    # -----------------------------------------
    def set_rows_counter(self, counter):
        self.rows_counter = counter

    # -----------------------------------------
    # counter for the block being prepared
    # -----------------------------------------
    def get_blocks_counter(self):
        return self.block_counter

    # -----------------------------------------
    # counter for the block being prepared
    # -----------------------------------------
    def set_blocks_counter(self, counter):
        self.block_counter = counter

    # -----------------------------------------
    # Count the number of rows returned and compare to limit
    # -----------------------------------------
    def count_row_test_limit(self, limit):
        self.rows_returned += 1
        if limit and self.rows_returned >= limit:
            return True
        return False

    # ====================================================
    # Get the Output instructions
    # ====================================================
    def get_out_instruct(self):
        return self.out_instruct

    # ====================================================
    # Count the number of blocks transferred
    # ====================================================
    def add_network_block_counter(self):
        self.network_block_counter += 1
        return self.network_block_counter

    # ====================================================
    # Get the number of blocks transferred
    # ====================================================
    def get_network_block_counter(self):
        self.network_block_counter += 1

    # ====================================================
    # Get the information parsed from the select statement
    # ====================================================
    def get_select_parsed(self):
        return self.select_parsed

    # ====================================================
    # Set the network block buffer
    # ====================================================
    def set_io_buff_in(self, buff_in):
        self.io_buff_in = buff_in

    # ====================================================
    # Return the network block buffer
    # ====================================================
    def get_io_buff_in(self):
        return self.io_buff_in

    # ====================================================
    # return the DBMS name - this value was set when the cursor was initialized
    # ====================================================
    def get_dbms_name(self):
        return self.sql_dbms_name

    # ====================================================
    # return the DBMS ID - this value was set when the cursor was initialized
    # ====================================================
    def get_dbms_id(self):
        return self.pi_dbms_id

    # ====================================================
    # return the connection info
    # ====================================================
    def get_host_port(self):
        return self.host_port


# =======================================================================================================================
# PI Instance
# =======================================================================================================================
class PIDB:
    def __init__(self):
        self.ip = ""
        self.port = 0
        self.pi_url = ""  # connection url to pi
        self.pi_interface = ""  # xompass or webapi
        self.pi_user = ""
        self.pi_passwd = ""
        self.usr_auth_token = ""  # based on base64.b64encode(user + password) that is provided with the requests to PI

        self.blockchain_info = None
        self.dbms_name = ""

        self.af_name = ""
        self.af_id = ""  # AF ID
        self.pi_db_name = ""
        self.pi_db_id = ""  # DBMS ID

        self.elements = None  # the root elements

    # =======================================================================================================================
    #  Return the special configuration for "get databases" command
    # =======================================================================================================================
    def get_config(self, status):

        return ""

    def get_dbms_name(self):
        return self.dbms_name

    # =======================================================================================================================
    #  Get IP and Port of the DBMS -  used in show dbms command
    # =======================================================================================================================
    def get_ip_port(self):

        if self.port == 0:
            return "Web API"

        return self.ip + ':' + str(self.port)

    # =======================================================================================================================
    #  Return the storage type -  used in show dbms command
    # =======================================================================================================================
    def get_storage_type(self):
        if self.port == 0:
            return self.pi_url
        return "Persistent"

    # ==================================================================
    # Get the root elements that relate to the DBMS and AF
    # ==================================================================
    def get_root_elements(self):
        return self.elements

    # ==================================================================
    # Commit
    # ==================================================================blockchain show operator
    def commit(self, status, db_cursor):
        pass

    # =======================================================================================================================
    #  SETUP Calls - These are issued when the database is created
    # =======================================================================================================================
    def exec_setup_stmt(self, status):
        return True

    # =======================================================================================================================
    # In PI, one connection for all threads
    # =======================================================================================================================
    def is_thread_connection(self):
        return False

    # ==================================================================
    # Return True if a table exists
    # ==================================================================
    def is_table_exists(self, status: process_status, table_name: str):
        return False

    # =======================================================================================================================
    # Return the Asset Framework ID - valoue was initialized in the connect_to_db
    # =======================================================================================================================
    def get_af_id(self):
        return self.af_id

    # =======================================================================================================================
    # Return the DBMS ID - valoue was initialized in the connect_to_db
    # =======================================================================================================================
    def get_dbms_id(self):
        return self.pi_db_id

    # =======================================================================================================================
    # When row are retrieved, there is no need to call query_row_by_row()
    # =======================================================================================================================
    def is_row_by_row(self):
        return False

    # =======================================================================================================================
    # Connect to the pi DBMS
    # Xompass API: connect dbms pi !db_user 7400 transpara
    # AnyLog API: connect dbms pi Moshe.Shadmon@https://MetersAPI.Transpara.com/piwebapi/assetservers:Station#6 0 transpara
    # =======================================================================================================================
    def connect_to_db(self, status, user, passwd, host, port, dbn, conditions):

        if port == 0:
            # new interface
            self.pi_url = host
            self.pi_user = user
            self.pi_passwd = passwd
            self.usr_auth_token = http_client.get_auth_token(user, passwd)
            self.pi_interface = "web_api"  # use the PI web API
        else:
            # Xompass interface
            self.pi_interface = "xompass"
            self.ip = host
            self.port = port

        self.dbms_name = dbn

        pi_cursor = self.get_cursor(status)

        blockchain_file = params.get_value_if_available("!blockchain_file")
        if blockchain_file == "":
            status.add_keep_error("PI connect: Missing dictionary definition for \'blockchain_file\'")
            return False  # no blockchain file def

        # get from the blockchain the PI connection info to dbn
        cmd = "blockchain get operator where ip = !ip and port = !anylog_server_port and type = pi and dbms = %s" % dbn
        ret_val, self.blockchain_info = member_cmd.blockchain_get(status, cmd.split(), blockchain_file, True)
        if ret_val or not self.blockchain_info:
            machine_ip = params.get_value_if_available("!ip")
            machine_port = params.get_value_if_available("!anylog_server_port")
            status.add_keep_error("PI connect: Blockchain metadata on node %s:%s does not include connection to PI" % (
            machine_ip, machine_port))
            return False

        try:
            self.af_name = self.blockchain_info[0]['operator']['mapping']['af.name']
        except:
            status.add_keep_error("PI connect: blockchain metadata does not include ['operator']['mapping']['af.name']")
            return False
        try:
            self.pi_db_name = self.blockchain_info[0]['operator']['mapping']['af.dbms']
        except:
            status.add_keep_error("PI connect: blockchain metadata does not include ['operator']['mapping']['af.dbms']")
            return False

        # connect to pi on the IP and port

        self.af_id, self.pi_db_id, self.elements = connect_to_pi(status, pi_cursor.get_host_port(), self.pi_url,
                                                                 self.usr_auth_token, self.af_name, self.pi_db_name)

        if self.elements:
            pi_cursor.set_af_id_name(self.af_id, self.af_name)
            pi_cursor.set_dbms_id_name(self.pi_db_id, self.pi_db_name)


        else:
            status.add_keep_error(
                "PI connect: Failed to sync with AF: %s and DBMS: %s" % (self.af_name, self.pi_db_name))
            return False

        return True

    # =======================================================================================================================
    # Return info on the table - info is taken from the blockchain data
    # =======================================================================================================================
    def get_table_info(self, status, dbms_name, table_name, info_type):

        ret_val = True
        output_prefix = "Structure." + dbms_name + "." + table_name
        pi_cursor = self.get_cursor(status)

        if info_type == "columns":
            # load table metadata
            if db_info.get_table_structure(dbms_name, table_name) == None:
                if member_cmd.load_table_metadata_from_blockchain(status, pi_cursor, output_prefix, dbms_name,
                                                                  table_name):
                    reply_list = [False, ""]  # error with the metadata
                    return reply_list

            string_data = db_info.get_table_structure_as_str(dbms_name, table_name)
        elif info_type == "tables":
            # get tables for the dbms
            string_data = member_cmd.load_table_list_from_blockchain(status, pi_cursor, output_prefix, None,
                                                                     dbms_name)
        else:
            ret_val = False
            string_data = ""

        reply_list = [True, string_data]
        return reply_list

    # =======================================================================================================================
    # Return a cursor
    # =======================================================================================================================
    def get_cursor(self, status):

        cursor = PI_CURSOR(self.ip + ":" + str(self.port), self.dbms_name)
        cursor.set_af_id_name(self.af_id, self.af_name)
        cursor.set_dbms_id_name(self.pi_db_id, self.pi_db_name)
        cursor.set_interface(self.pi_interface)
        if self.pi_interface == "web_api":
            cursor.set_root_elements(self.get_root_elements())
            cursor.set_auth_token(self.usr_auth_token)
        return cursor

    # =======================================================================================================================
    # Close a cursor
    # =======================================================================================================================
    def close_cursor(self, status, db_cursor):
        pass

    # =======================================================================================================================
    # Return the IP and Port of the DBMS
    # =======================================================================================================================
    def get_ip_port(self):
        return self.ip + ":" + str(self.port)

    # =======================================================================================================================
    # Execute commands from SQL FIle
    # =======================================================================================================================
    def execute_sql_file(self, status, dbms_name, table_name, db_cursor, file_path):
        status.add_keep_error("PI process error: SQL commands from file are not supported")
        return process_status.SQL_not_supported

    # =======================================================================================================================
    # STEP 1 - Execute SQL stmt
    # Place the logic of the execution on the Cursor
    # =======================================================================================================================
    def execute_sql_stmt(self, status, pi_cursor, sql_command):

        select_parsed = pi_cursor.get_select_parsed()  # get an object for query info

        ret_val, is_select, new_string, offset_select = utils_sql.get_select_stmt(status, sql_command)
        if not is_select or ret_val:
            # Not a select statement or ERROR in the select statement
            reply_list = [ret_val, "", sql_command]
            return reply_list

        ret_val, sql_command = utils_sql.format_select_sql(status, new_string, offset_select,
                                                           select_parsed)  # clean the sql stmt

        if ret_val:
            # Not a select statement or ERROR in the select statement
            reply_list = [ret_val, "", sql_command]
            return reply_list


        if not is_select:
            status.add_keep_error("PI process error: SQL \"%s\" not supported" % sql_command)
            return False

        if utils_sql.process_where_condition(status, select_parsed):
            return False

        if not utils_sql.process_projection(status, select_parsed):
            return False

        # get dbms name and table name of the SQL structure
        dbms_name = pi_cursor.get_dbms_name()
        table_name = select_parsed.get_table_name()

        if db_info.get_table_structure(dbms_name, table_name) == None:
            output_prefix = "Structure." + dbms_name + "." + table_name
            if member_cmd.load_table_metadata_from_blockchain(status, pi_cursor, output_prefix, dbms_name, table_name):
                return False  # error with the metadata

        out_instruct = pi_cursor.get_out_instruct()  # instructions on the output of the query

        out_instruct.set_limit(select_parsed.get_limit())  # set max rows to return

        if out_instruct.get_output_dest() == "network":
            # send the data back to the query node with the TCP server
            io_buff_in = pi_cursor.get_io_buff_in()

            mem_view = memoryview(io_buff_in)
            ip, port = message_header.get_source_ip_port(mem_view)
            soc = net_client.socket_open(ip, port, sql_command, 6, 3)
            if soc == None:
                status.add_error("PI query process failed to open socket on: %s:%u" % (ip, port))
                return process_status.ERR_network

            out_instruct.set_socket(soc)

            message_header.set_info_type(mem_view, message_header.BLOCK_INFO_RESULT_SET)
            message_header.set_block_struct(mem_view, message_header.BLOCK_STRUCT_JSON_MULTIPLE)
            message_header.prep_command(mem_view, "job reply")  # add command to the send buffer
            message_header.set_data_segment_to_command(mem_view)

        # get the relevant metadata
        blockchain_file = params.get_value_if_available("!blockchain_file")
        if blockchain_file == "":
            status.add_keep_error("PI select: Missing dictionary definition for \'blockchain_file\'")
            return False  # no blockchain file def

        cmd = "blockchain get operator where dbms = %s and type = pi and table = %s " \
              "bring ['operator']['mapping']" % (dbms_name, table_name)

        ret_val, mapping = member_cmd.blockchain_get(status, cmd.split(), blockchain_file, True)
        if ret_val or not mapping:
            status.add_keep_error("PI select: Missing metadata information for query")
            return False
        else:
            ret_val = True

        blockchain_map = utils_json.str_to_json(mapping.replace('\'', '"'))
        if not blockchain_map:
            status.add_keep_error("PI select: Metadata not in JSON format")
            return False

        # get the conditions that needs to be met for a result set
        if not self.set_mapping_info(status, pi_cursor, dbms_name, table_name, blockchain_map):
            return False

        if self.pi_interface == "xompass":
            # organize elements retrieved to tree structure
            organize_elements(status, pi_cursor, self.elements)

        table_struct = db_info.get_table_structure(dbms_name, table_name)
        counter_out_columns = pi_cursor.set_reply_info(table_struct,
                                                       select_parsed)  # return number of columns to output

        root_node = select_parsed.get_where_tree()
        if root_node:
            # traversal to change column names to PI and update info such it can be tested while visiting the PI hierarchi
            ret_value = utils_sql.make_pi_conditions(status, root_node, pi_cursor)
            if ret_value:
                return False

            # These keys determine the search criteria in PI
            if not root_node.traversal_update_pi_keys(status, pi_cursor):
                return False

            # Make changes to the traversal based on the where condition
            root_node.optimize_pi_query(pi_cursor.get_element_lookup(), pi_cursor.get_sensor_lookup())

        pi_cursor.make_date_pi_key()  # make the key that will be used for the date range

        if select_parsed.is_functions_values():
            ret_val = prepare_functions(status, pi_cursor, select_parsed)

        if not ret_val:
            status.add_keep_error("PI process error: SQL \"%s\" not supported on PI" % sql_command)

        return ret_val

    # ==================================================================
    # STEP 2 - Fetch rows
    # if fetch_size is 0, execute fetchall()
    # Return String
    # ==================================================================
    def fetch_rows(self, status, pi_cursor, output_prefix, fetch_size, title_list):

        ret_val = True

        # execute a query
        out_instruct = pi_cursor.get_out_instruct()  # instructions on the output of the query

        ret_code, string_data = pull_data_from_pi(status, pi_cursor)

        if not ret_code or ret_code > process_status.NON_ERROR_RET_VALUE:
            # completed no errors - or at fot to limit (1007)
            if pi_cursor.get_select_parsed().is_functions_values():
                # place functions in a block and send to caller
                if pi_cursor.is_with_increments():
                    update_block_with_increments_data(status, pi_cursor)
                else:
                    update_block_with_functions_data(status, pi_cursor)

            # flush block with rows or function valoues
            if pi_cursor.get_out_instruct().get_output_dest() == "network":
                if (member_cmd.flush_network_block(status, out_instruct.get_socket(), pi_cursor.get_io_buff_in(),
                                                   pi_cursor.get_blocks_counter(), pi_cursor.get_rows_counter())):
                    ret_val = False

        reply_list = [ret_val, string_data]
        return reply_list

    # ==================================================================
    # Prepare the mapping and conditions that apply for the PI tree that is visited.
    # and the mapping between PI atributes to column names
    # these conditions impact the traversal - if a condition is not satified
    # the traversal ignores the sub-tree
    # ==================================================================
    def set_mapping_info(self, status, pi_cursor, dbms_name, table_name, blockchain_map):

        if "attributes" not in blockchain_map.keys():
            status.add_keep_error("PI process error: Missing 'attributes' definition in JSON metadata")
            return False

        if "table" in blockchain_map.keys():
            table_path = blockchain_map["table"]  # the path used to map data to the table structure
            pi_cursor.set_table_path(table_path)

        # Set the attribute that are terurned to the app and the mapping to the sql column names

        map_column_pi_sql = blockchain_map["attributes"]
        for layer_attribute, sql_column_name in map_column_pi_sql.items():
            # structure to map [pi attribute name] to [table column name]
            ret_val, layer, pi_column_name = utils_columns.get_pi_layer_and_value(status, layer_attribute)
            if not ret_val:
                return False

            column_number, column_type = db_info.get_table_column_number_and_type(dbms_name, table_name,
                                                                                  sql_column_name)

            # Set the SQL info as f(PI column name)
            if not pi_cursor.map_pi_column_names(pi_column_name, column_number, sql_column_name, column_type, layer):
                status.add_keep_error("PI select: duplicate attributes names in metadata: %s" % pi_column_name)
                return False

        return True

    # =======================================
    # A DBMS that takes PostgreSQL calls returns True
    # A DBMS like PI that considers the generic calls returns False
    # ======================================
    def is_default_sql_command(self):
        return False
