"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""


# Organize the output as follows:

# Organize format:
# In JSON format
# In table Format

# Organize Output destination:
# To stdout
# To FIle
# To REST
# Buffer results

# Add count and time statistics

import edge_lake.generic.utils_json as utils_json
import edge_lake.generic.utils_io as utils_io
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.utils_sql as utils_sql
import edge_lake.generic.process_status as process_status
import edge_lake.generic.params as params
from edge_lake.generic.interpreter import get_multiple_values, get_one_value
from edge_lake.generic.utils_data import seconds_to_hms
from edge_lake.generic.utils_columns import get_current_time
import edge_lake.cmd.member_cmd as member_cmd
import edge_lake.api.al_kafka as al_kafka
import edge_lake.tcpip.html_reply as html_reply

class OutputManager():

    def __init__(self, conditions, rest_socket, output_into, add_stat, nodes_count):
        '''
        conditions = a dictionary with the following:
            destination - where output data is directed: stdout / REST
            file_name - a name of a file if the output is directed to a file
            assign_key - a name of a variable if the data is assigned to a variable
            format_type - json / table
            title - a title to use for the output
            test - a bool value to determine if output is using a test format - (Header section, Output section and Stat section)
            option - different comparison options
        rest_socket - the REST socket if used
        output_into - if organized as HTML
        add_stat - Bool val to add statistics info
        nodes_count - number of nodes participating in the query process
        '''

        destination, format_type, file_name, trusted_file, title, test, assign_key, topic = get_multiple_values(
            conditions,
            ["dest",    "format", "file", "source", "title", "test", "output_key", "topic"],
            ["stdout",  "json",   "",      "",      "",      False,  "", None])

        if "include" in conditions:
            include_list = conditions["include"] # A list of tables included in the query
            self.include_tables = ','.join(include_list)
        else:
            self.include_tables = None

        if "extend" in conditions:
            extend_list = conditions["extend"] # A list of columns extending the columns in the table
            self.extend_columns = ','.join(extend_list)
        else:
            self.extend_columns = None

        self.new_line = "\n"
        if destination == "stdout" and rest_socket:
            destination = "rest"

        self.ip_port = None         # IP and Port for destination of data (i.e. Kafka)
        if destination[:6] == "kafka@":
            # kafka would be registered as kafka@ip:port
            self.destination = "kafka"
            if rest_socket:
                self.source = "rest"
            else:
                self.source = "kafka"
            if len(destination) > 6:
                self.ip_port = params.get_ip_port(destination[6:])
        else:
            self.source = destination       # In the case of file - destination is changed - we keep the source destination

            if file_name:
                self.destination = "file"
            elif assign_key:
                self.destination = "buffer"
            else:
                self.destination = destination  # stdout / REST /
                if self.destination == "stdout":
                    self.new_line = "\r\n"

        if "option" in conditions:
            self.options = conditions["option"]       # Different options for testing (i.e option = time, option = dest.stdout, option = dbms.qa.test_cases)
        else:
            self.options = None

        self.output_into = output_into      # HTML structure to generate for the output

        self.format_type = format_type      # JSON / Table
        self.assign_key = assign_key        # Assign buffer to this dictionary key
        self.topic = topic                  # If the query data is send to a broker, use this topic

        self.title = title
        self.test = test
        self.add_stat = add_stat          # Flag to include statistics on output
        self.nodes_count = nodes_count    # Nodes participating

        self.out_list_counter = 0         # Counter for numbers of rows added to the table stucture
        self.out_list = []                # A structure to collect the data in a table format
        self.command = ""                 # The command processed
        self.file_name = file_name        # the name of the file if output is directed to file
        self.trusted_file = trusted_file     # A file name with complete output results for validation
        self.io_handle = None             # The file output handle - if output to file
        self.rest_socket = rest_socket    # The output socket to the REST caller (if used)
        self.output_str = "["             # If data is buffered - Format: [{'Query': {'0': '18774'}}, {'Query': {'0': '18774'}}]
        self.title_list = None          # the projected columns
        self.data_types_list = None     # The projected data types
        self.dbms_name = None           # The database name
        self.kafka_producer = None      # Used to transfer the data to Kafka
        self.result_set = ""             # Maintain the result set, for example, in the case of HTML


    # --------------------------------------------------------------------------------------
    # Initiate the object based on the output type and format type
    # --------------------------------------------------------------------------------------
    def init(self, status, command, title_list, data_types_list, dbms_name):
        '''
        status - the thread status object
        command - the SQL query being processed
        title_list - the projected names of the columns
        data_types_list - the projected data types
        '''
        ret_val = process_status.SUCCESS
        self.command = command
        self.title_list = title_list
        self.data_types_list = data_types_list
        self.dbms_name = dbms_name

        if self.format_type == "table" and title_list:
            # If format is table - Set the title columns on the table_list in the first entry
            for index, entry in enumerate(title_list):
                self.out_list.append([])
                self.out_list[index].append(entry)

        if self.file_name:
            self.io_handle = utils_io.IoHandle()

            file_dir, name_prefix, type_prefix = utils_io.extract_path_name_type(self.file_name)

            # Provide unique name if file name ends with asterisk
            if not name_prefix:
                f_name = utils_io.get_unique_time_string()
            elif name_prefix[-1] == '*':
                f_name = name_prefix[:-1] + utils_io.get_unique_time_string()  # create a unique file name for the output
            else:
                f_name = name_prefix

            if not type_prefix:
                t_name = "out"
            else:
                t_name = type_prefix

            self.file_name = "%s%s.%s" % (file_dir, f_name, t_name)


            if not self.io_handle.open_file("append", self.file_name):
                status.add_error("Query process failed to open output file: %s" % self.file_name)
                ret_val = process_status.File_open_failed

        if self.destination == "kafka":
            # transfer the data to Kafka
            self.kafka_producer = al_kafka.get_producer(status, self.ip_port)
            if not self.kafka_producer:
                ret_val = process_status.Failed_kafka_process

        if not ret_val and self.trusted_file:
            if self.destination != "file":
                status.add_error("Validate process is only supported when process output is directed to file")
                ret_val = process_status.ERR_command_struct

        return ret_val
    # =======================================================================================================================
    # Output the title - or the header in a test query
    # =======================================================================================================================
    def output_header(self, status):

        if self.test:
            # Output a TEST HEADER
            ret_val = self.output_test_header(status)
        else:
            # Output title
            if self.title:
                ret_val = self.output_info_string(status, self.new_line + self.title, None, False)
            else:
                ret_val = process_status.SUCCESS

        return ret_val
    # =======================================================================================================================
    # Return True with title or a test format (with header)
    # =======================================================================================================================
    def is_output_header(self):
        return (self.title or self.test)
    # =======================================================================================================================
    # TEST Format - a format used for comparison between files to validate correct output
    # Test Format Header - contains the title, the data and time and the command used
    # =======================================================================================================================
    def output_test_header(self, status):

        if self.destination == "stdout":
            new_line = self.new_line        # start with new line on stdout
        else:
            new_line = ""

        ret_val = self.output_separator(status, new_line, "", False)

        if not ret_val:
            info_str = ""
            if self.title:
                info_str += "%sTitle:      %s" % (self.new_line, self.title)
            info_str +=     "%sDate:       %s" % (self.new_line, get_current_time())
            if self.command:
                info_str += "%sCommand:    %s" % (self.new_line, self.command)
            if self.dbms_name:
                info_str += "%sDBMS:       %s" % (self.new_line, self.dbms_name)

            if self.include_tables:
                info_str += "%sInclude:    (%s)" % (self.new_line, self.include_tables)

            if self.extend_columns:
                info_str += "%sExtend:     (%s)" % (self.new_line, self.extend_columns)


            info_str += "%sFormat:     %s" % (self.new_line, self.format_type)

            ret_val = self.output_info_string(status, info_str, None, False)

            if not ret_val:
                ret_val = self.output_separator(status, self.new_line, "", False)        # separator between the test sections

        return ret_val
    # =======================================================================================================================
    # TEST Format - a format used for comparison between files to validate correct output
    # Test Format Header - contains the title, the data and time and the command used
    # =======================================================================================================================
    def output_test_footer(self, status, rows, run_time):

        ret_val = self.output_separator(status, self.new_line, "", False)

        if not ret_val:
            info_str = "%sRows:     %s%sRun Time: %s" % (self.new_line, rows, self.new_line, run_time)

            ret_val = self.output_info_string(status, info_str, None, False)

            if not ret_val:
                ret_val = self.output_separator(status, self.new_line, self.new_line, True)        # separator between the test sections

        return ret_val


    # =======================================================================================================================
    # Add a line separator between the sections
    # =======================================================================================================================
    def output_separator(self, status, start_new_line, end_new_line, is_last):

        return self.output_info_string(status, "%s==========================================================================%s" % (start_new_line, end_new_line), None, is_last)

    # =======================================================================================================================
    # Output the info_str to the destination output
    # =======================================================================================================================
    def output_info_string(self, status, info_str, dest, is_last):
        '''
        status - the process status object
        info_str - the output str
        dest - if sending the message to a different destination than self.destination. i.e write to file but some messages fo to stdout
        is_last - is last output string
        '''

        if dest:
            out_dest = dest
        else:
            out_dest = self.destination

        ret_val = process_status.SUCCESS
        if out_dest == "file":
            if not self.io_handle.append_data(info_str):
                status.add_error("Failed to write process output file: %s" % self.file_name)
                ret_val = process_status.File_write_failed
        elif out_dest == "rest":
           ret_val = self.rest_write(status, self.rest_socket, info_str, True, is_last, self.output_into)
        elif out_dest == "buffer":
            self.output_str += info_str
        elif out_dest == "kafka":
            ret_val = al_kafka.send_data(status, self.kafka_producer, self.topic, info_str)
        else:
            if info_str[0] == "{" and info_str[-1] == "}":
                json_struct = utils_json.str_to_json(info_str)
                if json_struct:
                    was_printed = True
                    utils_print.jput(json_struct, True, indent = 4)
                else:
                    was_printed = False
            else:
                was_printed = False
            if not was_printed:
                utils_print.output(info_str, False)

        return ret_val
    # --------------------------------------------------------------------------------------
    # Add a new Row
    # --------------------------------------------------------------------------------------
    def new_rows(self, status, rows_data, json_data, offset_row, is_mutex):
        '''
        status - the thread status object

        Data is provided in one of these options:

        rows_data - the rows in string format
        json data - the rows in JSON format
        offset_row - the offset to start print

        is_mutex - bool to represent if multiple threads are processing (printing) concurrently
        '''

        ret_val = process_status.SUCCESS

        if self.format_type == "table":
            # use a table format
            self.out_list_counter += process_table_list(self.data_types_list, self.out_list, rows_data, json_data, self.out_list_counter + 1)
            if self.out_list_counter >= 25:
                ret_val = self.output_table_entries(status, is_mutex, False)
        else:
            # use JSON format
            if self.format_type == "json:output" or self.format_type == "json:list":
                # Clean JSON rows
                if not rows_data:
                    # Data is not organized as a string
                    out_data = utils_sql.make_output_row(1, self.title_list, self.data_types_list, json_data)
                    if not offset_row:
                        if self.format_type == "json:list":
                            offset_new = 9      # Keep the [ parenthesis
                        else:
                            # 10 is used because of the transformation from JSON
                            offset_new = 10 # Ignore the "{"Query":[" prefix
                    else:
                        offset_new = offset_row
                    # not rhe first row - add comma to seperate between the JSON entries
                    str_info = out_data[offset_new:]
                else:
                    if not offset_row:
                        # 11 is used because of the AnyLog format
                        if self.format_type == "json:list":
                            offset_new = 10      # Keep the [ parenthesis
                        else:
                            offset_new = 11 # Ignore the "{"Query":[" prefix
                    else:
                        offset_new = offset_row
                    str_info = rows_data[offset_new:-2]

                if offset_row:
                    # not the first row
                    if self.destination != "kafka":     # Kafka does not need the new line
                        if self.format_type == "json:output":
                            str_info = "%s%s" % (self.new_line, str_info)
                        else:
                            str_info = ",%s%s" % (self.new_line, str_info)
                else:
                    if self.destination == "stdout" or self.test:
                        # On the first row with stdout - add a new line
                        str_info = "%s%s" % (self.new_line, str_info)

                if self.destination == "buffer":
                    self.output_str += str_info  # Add {"Query": + data + suffix

                elif self.destination == "file":
                    if not self.io_handle.append_data(str_info):
                            status.add_error("Query process failed to write to output file: %s" % self.file_name)
                            ret_val = process_status.File_write_failed

                elif self.destination == "rest":
                    ret_val = self.rest_write(status, self.rest_socket, str_info, True, False, self.output_into)

                elif self.destination == "kafka":
                    ret_val = al_kafka.send_data(status, self.kafka_producer, self.topic, str_info)

                else:
                    # STDOUT
                    if is_mutex:
                        utils_print.print_lock()  # avoid multiple threads printing at the same time

                    self.output_info_string(status, str_info, None, False) # only new line at the end of the row

                    if is_mutex:
                        utils_print.print_unlock()

            else:
                # JSON with Query Prefix
                if self.test:
                    # add new line
                    if self.destination == "file":
                        if offset_row:
                            separator = ','   # not first row
                        else:
                            separator = ""
                    else:
                        if offset_row:
                            separator = ',' + self.new_line  # not first row
                        else:
                            separator = self.new_line
                else:
                    if offset_row:
                        separator = ',' # not first row
                    else:
                        separator = ''
                if not rows_data:
                    # Data is not organized as a string
                    out_data = utils_sql.make_output_row(1, self.title_list, self.data_types_list, json_data)
                    # not rhe first row - add comma to separate between the JSON entries
                    rows_str = out_data[offset_row:]
                    offset_parenthesis = 10
                else:
                    rows_str = rows_data[offset_row:-2]
                    offset_parenthesis = 11

                if self.destination == "buffer":
                    self.output_str += (separator + rows_str)  # Add {"Query": + data + suffix
                elif self.destination == "file":
                    if not offset_row:
                        # First row - with the - ""Query": [" prefix
                        if self.format_type == "json":
                            str_info = rows_str  # Provide the prefix for the first row
                        else:
                            str_info = rows_str[offset_parenthesis:]    # Skip the prefix
                    else:
                        str_info = rows_str
                    if not self.io_handle.append_data(separator + self.new_line + str_info):
                            status.add_error("Query process failed to write to output file: %s" % self.file_name)
                            ret_val = process_status.File_write_failed
                elif self.destination == "rest":
                    ret_val = self.rest_write(status, self.rest_socket, separator + rows_str, True, False, self.output_into)
                else:
                    # STDOUT
                    if is_mutex:
                        utils_print.print_lock()  # avoid multiple threads printing at the same time
                    if not offset_row:
                        # First row - with the - ""Query": [" prefix
                        utils_print.print_row(rows_str, False)
                    else:
                        utils_print.output("," + self.new_line, False) # Add comma and new line
                        if rows_str[0] == ',':
                            # print without the prefixed comma - it was printed
                            data_offset = 1
                        else:
                            data_offset = 0

                        utils_print.print_row_internal(rows_str, offset_row, data_offset, len(rows_str), True)
                    if is_mutex:
                        utils_print.print_unlock()

        return ret_val

    # =======================================================================================================================
    # Print to the output destination all the table entries that aare maintained in self.out_list
    # =======================================================================================================================
    def output_table_entries(self, status, is_mutex, is_last):
        '''
        is_mutex- a flag to indicate the print is from pass_through where multiple threads provide output concurrently
        is_last - is the last printout
        '''

        if self.destination == "stdout":
            ret_val = process_status.SUCCESS
            if is_mutex:
                utils_print.print_lock()  # avoid multiple threads printing at the same time
            utils_print.print_data_list(self.out_list, self.out_list_counter + 1, True, False)
            if is_mutex:
                utils_print.print_unlock()
        else:
            info_str = utils_print.print_data_list(self.out_list, self.out_list_counter + 1, True, True, self.new_line)
            ret_val = self.output_info_string(status, info_str, None, is_last)

        self.out_list_counter = 0 # reset self.out_list

        return ret_val
    # =======================================================================================================================
    # Finalize output process
    # =======================================================================================================================
    def finalize(self, status, io_buff_in, rows_counter, dbms_time, is_mutex, is_query_end, nodes_replied):
        '''
        status - the processing thread status object
        io_buff_in - the thread message buff
        rows_counter - the number of rows included in self.out_list
        dbms_time - processing query time
        is_mutex - bool to represent if multiple threads are processing (printing) concurrently
        is_query_end - This call is done at the end of the query (vs. a thread representing data set from a node that delivered all the data)
        nodes_replied = the number of nodes participating in the query process
        '''

        ret_val = process_status.SUCCESS

        # Complete the output of whatever is left in the buggers

        if self.destination == "buffer":
            # assign the output:
            if self.format_type == "Table":
                params.add_param(self.assign_key, (self.output_str[:-1] + "]"))
            else:
                # JSON - End the query info
                params.add_param(self.assign_key, (self.output_str + "]}]"))
        elif  self.format_type == "table":
            if self.out_list_counter:
                # Print whatever is left in the table struct
                is_last = True if (not self.add_stat and rows_counter and is_query_end) else False  # No more printouts after this one
                ret_val = self.output_table_entries(status, is_mutex, is_last)

            if not ret_val:
                if self.add_stat:
                    # Output Statistics
                    if self.test:
                        prefix_str = ""
                    else:
                        prefix_str = self.new_line + "{"
                    ret_val = self.out_statistics(status, rows_counter, dbms_time, False, prefix_str, "", True, nodes_replied)
                else:
                    if not rows_counter and is_query_end:
                        self.output_info_string(status, "{\"reply\" : \"Empty data set\"}", None, True)

        else:
            # JSON format

            if not rows_counter:
                # Output Statistics
                if self.add_stat:
                    if self.test:
                        prefix_str = ""
                    else:
                        prefix_str = self.new_line + "{"
                    ret_val = self.out_statistics(status, rows_counter, dbms_time, False, prefix_str, "", True, nodes_replied)
                else:
                    self.output_info_string(status, "{\"reply\" : \"Empty data set\"}", None, True)

            elif self.format_type[:4] == "json":
                if self.add_stat:
                    prefix_str = ""
                    double_lines_stat = False  # Place stat on a single line
                    if self.format_type == "json":
                        if self.destination == "stdout":
                            double_lines_stat = True    # Place stat on 2 line
                        if self.test:
                            prefix_str = "]}" + self.new_line   # Complete the JSON as the stat is seperated in the footer
                        else:
                            prefix_str = "]," + self.new_line
                    else:
                        if self.format_type == "json:list":
                            if self.test:
                                prefix_str = "]"
                            else:
                                prefix_str = "]" + self.new_line + '{'
                        else:
                            # self.format_type == "json:output":
                            if not self.test:
                                prefix_str = self.new_line + '{'        # Prefix of statistics
                    ret_val = self.out_statistics(status, rows_counter, dbms_time, False, prefix_str, "", double_lines_stat, nodes_replied)
                else:
                    # complete last row

                    if self.format_type == "json":
                        end_struct = "]}"
                    elif self.format_type == "json:list":  # not JSON:output
                        end_struct = "]"
                    elif self.format_type == "json:output":  # not JSON:output
                        end_struct = ""

                    if self.destination == "rest":
                        ret_val = self.rest_write(status, self.rest_socket, end_struct, True, True, self.output_into)
                    elif self.destination == "stdout":
                        utils_print.output(end_struct, False)


        # Close output file / Kafka connection
        self.close_handles(status)

        if self.destination == "file":
            # Close file

            if self.trusted_file:
                # Do file compare
                if ret_val:
                    utils_print.output("Failed [Error '%s' in process] - %s" % (process_status.get_status_text(ret_val), self.title), True)
                else:
                    # validate the output file to the file assigned to validate
                    result, compare_message = utils_io.analyze_file(status, self.trusted_file, self.file_name, self.options)

                    # Deliver the results to destination
                    ret_val = self.deliver_compare_results(status, io_buff_in, compare_message)

            else:
                if self.source == "rest":
                    ret_val = self.output_info_string(status, "{\"Reply\" : \"Query output written to file\", \"Rows\" : %s}" % rows_counter, self.source, False)

        elif self.destination == "kafka":
            if self.source == "rest":
                ret_val = self.output_info_string(status, "{\"Reply\" : \"Query output send to kafka\", \"Rows\" : %s}" % rows_counter, self.source, False)

        if self.source == "stdout" and is_query_end:
            utils_print.output(self.new_line, True)

        return ret_val

    # --------------------------------------------------------------------------------------
    # Close the file if open
    # --------------------------------------------------------------------------------------
    def close_handles(self, status):

        if self.io_handle:
            self.io_handle.close_file()
            self.io_handle = None

        if self.kafka_producer:
            al_kafka.flush_data(status, self.kafka_producer)
            al_kafka.close_connection(status, self.kafka_producer)
            self.kafka_producer = None

    # =======================================================================================================================
    # Add statistics - showing the query execution time
    # =======================================================================================================================
    def out_statistics(self, status, rows_counter, dbms_time, is_prompt, prefix_str, suffix_str, is_2lines, nodes_replied):

        ret_val = process_status.SUCCESS
        # Show query statistics
        hours_time, minutes_time, seconds_time = seconds_to_hms(dbms_time)

        if is_2lines:
            # Make stat on 2 lines
            mid_str = self.new_line + "                "
        else:
            mid_str = ""

        if not self.test:

            if nodes_replied != self.nodes_count:
                # Not all the nodes replied
                nodes_stat = f"{mid_str}\"Target Nodes\": {self.nodes_count},{mid_str}\"Participating Nodes\": {nodes_replied}"
            else:
                nodes_stat = f"{mid_str}\"Nodes\": {self.nodes_count}"

            info_str = "%s\"Statistics\":[{\"Count\": %u,%s\"Time\":\"%02u:%02u:%02u\",%s}]}%s" % (
                prefix_str, rows_counter, mid_str, hours_time, minutes_time, seconds_time, nodes_stat, suffix_str)


            self.output_info_string(status, info_str, None, True)

        else:
            if prefix_str:
                self.output_info_string(status, prefix_str, None, False)
            self.output_test_footer(status, rows_counter, "%02u:%02u:%02u" % (hours_time, minutes_time, seconds_time))

        if is_prompt and self.source == "stdout":
            utils_print.output(self.new_line, True)

        return ret_val

    # =======================================================================================================================
    # Send the reults of the comparison to one or more destinations
    # example: and inform = stdout and inform = stdout@!qa_node and inform = dbms.qa.testing@!qa_node
    # =======================================================================================================================
    def deliver_compare_results(self, status, io_buff_in, compare_message):
        '''
        status - the thread status object
        io_buff_in - the thread message buff
        compare_message - the output returned from utils_io.analyze_file
        '''

        ret_val = process_status.SUCCESS
        if not self.options:
            # Only deliver to requestor output
            ret_val = self.output_info_string(status, compare_message, self.source, False)
        else:
            for dest_type in self.options:
                if dest_type == "stdout":
                    ret_code = self.output_info_string(status, compare_message, self.source, False)
                    if ret_code:
                        ret_val = ret_code  # Continue even if one output device is with error
                else:
                    index = dest_type.find('@')
                    if index > 1:
                        # with destination node
                        node = params.get_ip_port(dest_type[index + 1:])
                        if node:
                            if dest_type[:index] == "stdout":
                                # echo to destination
                                cmd_words = ["run", "client", node, "echo",  compare_message]
                                ret_code = member_cmd.run_client(status, io_buff_in, cmd_words, 0)
                                if ret_code:
                                    ret_val = ret_code  # Continue even if one output device is with error
                            elif dest_type[:5] == "dbms.":
                                # Send to a dbms
                                dbms_table = dest_type[5:].split('.')
                                if len(dbms_table) == 2:
                                    dbms_name = dbms_table[0]
                                    table_name = dbms_table[1]
                                    cmd_words = ["rest", "put", "where", "url", "=", "http://%s" % node, "and", "dbms", "=", dbms_name, "and", "table", "=", table_name, \
                                                 "and", "mode", "=", "streaming", "and", "body", "=", compare_message]
                                    ret_code = member_cmd._rest_client(status, io_buff_in, cmd_words, 0)
                                    if ret_code:
                                        ret_val = ret_code  # Continue even if one output device is with error
                                else:
                                    status.add_error("DBMS name and table name are not properly provided to update test case dbms: %s" % dest_type)
                                    ret_val = process_status.ERR_command_struct

        return ret_val

    # =======================================================================================================================
    # Return True if result sets are maintained in a buffer - for example in the case of HTML
    # =======================================================================================================================
    def is_with_result_set(self):
        return True if self.output_into else False
    # =======================================================================================================================
    # Return the result sets (if maintained in a buffer, for example if output is HTML)
    # =======================================================================================================================
    def get_result_set(self):
        return self.result_set
    # =======================================================================================================================
    # Reply via rest = there are 2 options:
    # 1) ALl writes are of user data
    # 2) With HTML - wrapping in an HTML doc and modifing the strings to HTM format
    # =======================================================================================================================
    def rest_write(self, status, io_stream, send_data, transfer_encoding, transfer_final, output_into):
        '''
        status - thread status object
        io_stream - io handle
        send_data - the database returned data
        transfer_encoding - true with messages with undetermined size: self.send_header('Transfer-Encoding', 'chunked') # https://en.wikipedia.org/wiki/Chunked_transfer_encoding
        transfer_final - true for last output to stream
        output_into - html form
        '''
        if output_into:

            self.result_set += send_data  # Maintain the output for HTML
            ret_val = process_status.SUCCESS

        else:
            ret_val = utils_io.write_to_stream(status, io_stream, send_data, transfer_encoding, transfer_final)

        return ret_val


# =======================================================================================================================
# Update a list with the row information. After Max Rows - Print the list
# location starts at 1 as location 0 maintains the title
# =======================================================================================================================
def process_table_list(data_types_list, table_list, rows_data, json_data, location):
    '''
    Add rows to the table_list
    table_list - a list to contain the data organized as a table
    rows_data - the data in a string format
    json_data - the data in JSON format
    location - the location in the table_list to use

    return the number of rows added to the table
    '''

    # Add row to a table
    rows_counter = 0
    if not json_data:
        json_struct = utils_json.str_to_json(rows_data)
    else:
        # The data is available in a dictionary format
        json_struct = json_data

    if json_struct:
        rows_list = json_struct['Query']
        for row in rows_list:
            if (location + rows_counter) >= len(table_list[0]):
                # add new list
                for index, column_val in enumerate(row.values()):
                    print_val = str(column_val)
                    if not print_val:
                        print_val = "null"
                    elif data_types_list and data_types_list[index][:4] == "bool":
                        if print_val == "0":
                            print_val = "false"
                        else:
                            print_val = "true"
                    table_list[index].append(print_val)
            else:
                for index, column_val in enumerate(row.values()):
                    # reuse existing list
                    print_val = str(column_val)
                    if not print_val:
                        print_val = "null"
                    elif data_types_list and data_types_list[index][:4] == "bool":
                        if print_val == "0":
                            print_val = "false"
                        else:
                            print_val = "true"
                    table_list[index][location + rows_counter] = (print_val)
            rows_counter += 1
    else:
        rows_counter += 1
    return rows_counter  # return number of rows added to the table





