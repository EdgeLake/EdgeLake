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
import pprint
import threading
import time
import edge_lake.generic.params as params
import sys
from edge_lake.generic.utils_json import str_to_json, str_to_list
from edge_lake.generic.node_info import get_prompt as get_prompt

try:
    if sys.platform.startswith('win'):
        import colorama # Makes ANSI escape character sequences (for producing colored terminal text and cursor positioning) work under MS Windows.
        colorama.init()
except:
    pass

print_mutex = threading.Lock()  # variable on the class level are static
print_mutex2 = threading.Lock()  # variable on the class level are static

new_line_counter = 0  # counts new lines

# =======================================================================================================================
# A nested table created by individual steps that create the title and columns
# =======================================================================================================================
class PrintTable():
    def __init__(self, missing_value):
        '''
        missing_value - the cell printout if a cell value is missing
        '''
        self.missing_value = missing_value
        self.title = []     # The table title
        self.table = []     # The data table
        self.x_keys = {}    # Keys on the X axis
        self.y_keys = {}    # Keys on the Y axis
        self.rows = 0
        self.columns = 0

    # =======================================================================================================================
    # Set a value in coordinates determined by the values associated with x_key and y_key.
    # If x_key, y_key are missing, add to the keys and assign a value to each
    # =======================================================================================================================
    def add_entry(self, x_key, y_key, value):

        x = self.update_x_key(x_key)

        if y_key in self.y_keys:
            y = self.y_keys[y_key]
        else:
            y = self.rows  # New location
            self.rows += 1
            self.y_keys[y_key] = y
            self.table.append([self.missing_value for _ in range(self.columns)]) # Add another row

        self.table[y][x] = value

    # =======================================================================================================================
    # Get a value From the table
    # =======================================================================================================================
    def get_entry(self, x_key, y_key):

        value = ""
        if x_key in self.x_keys:
            x = self.x_keys[x_key]

            if y_key in self.y_keys:
                y = self.y_keys[y_key]

                # If both x and y are available
                value = self.table[y][x-1]
        return value

    # =======================================================================================================================
    # Set a value for the title associated with the x_key
    # If x_key, iss missing, update the x_keys and assign a value
    # =======================================================================================================================
    def add_title(self, x_key, title):
        x = self.update_x_key(x_key)
        while len(self.title) <= x:
            self.title.append(self.missing_value)
        self.title[x] = title

    # =======================================================================================================================
    # Return the value on the x axis or update x axis and the table entries in case of a new key.
    # =======================================================================================================================
    def update_x_key(self, x_key):
        if x_key in self.x_keys:
            x = self.x_keys[x_key]
        else:
            x = self.columns  # New location
            self.columns += 1
            self.x_keys[x_key] = x
            # Add another column to all rows
            for entry in self.table:
                entry.append(self.missing_value)
        return x
    # =======================================================================================================================
    # Output formatted table info
    # =======================================================================================================================
    def output(self, header, get_info_str, line_prefix):
        '''
        header - printout header
        get_info_str - true reurns the output string, false prints the output
        line_prefix - shifts the output to the right
        '''
        return output_nested_lists(self.table, header, self.title, get_info_str, line_prefix)

# =======================================================================================================================
# A second mutex for competing threads with multiple calls to output()
# =======================================================================================================================
def print_lock():
    global print_mutex2
    print_mutex2.acquire()

# =======================================================================================================================
# A second mutex for competing threads with multiple calls to output()
# =======================================================================================================================
def print_unlock():
    global print_mutex2
    print_mutex2.release()

# =======================================================================================================================
# Use control chars to move the cursor up
# =======================================================================================================================
def move_lines_up(counter_lines, clear_lines):
    print_mutex.acquire()
    for _ in range (counter_lines):
        if clear_lines:
            sys.stdout.write("\x1b[2K")
        sys.stdout.write("\033[A")
    if clear_lines:
        sys.stdout.write("\x1b[2K")
    sys.stdout.write("\r")      # goto start of line
    print_mutex.release()
# =======================================================================================================================
# Print to stdout
# =======================================================================================================================
def output(info, is_newLine):
    global new_line_counter

    print_mutex.acquire()
    try:
        if (is_newLine):
            print_len = len(info)
            offset = 0
            while print_len:
                if print_len <= 16000:
                    done = True
                    end_with = '\r\n'
                else:
                    done = False
                    end_with = ''

                print(info[offset:offset + 16000], end=end_with, flush=False)
                if done:
                    break
                print_len -= 16000
                offset += 16000

            new_line_counter += 1
            prompt = get_prompt()
            print(prompt, end='', flush=True)
        else:
            # unify \n\r in the prefix and suffix of the string
            start_offset = 0
            end_offset = len(info)
            if end_offset:
                if info[0] == '\n':
                    start_offset = 1  # missing \r
                if (end_offset > 1):
                    if info[-1] == '\n' and info[-2] != '\r':
                        end_offset = -1
            if start_offset or end_offset:
                # add '\r\n at prefix or suffix
                if start_offset:
                    print('\r\n', end='', flush=False)
                print(info[start_offset:end_offset], end='', flush=True)
                if end_offset == -1:
                    print('\r\n', end='', flush=False)
            else:
                print(info, end='', flush=True)
    except:
        pass

    finally:
        print_mutex.release()   # always release the mutex


# =======================================================================================================================
# Output box messages
# =======================================================================================================================
def output_box(info_string):

    # remove '\r' and split by line
    info_array = info_string.replace("\r","").split('\n')

    # Get the longest line
    box_length = 0
    for line in info_array:
        if len(line) > box_length:
            box_length = len(line)

    print_mutex.acquire()

    try:

        # Print upper frame
        print("\r\n|", end='', flush=False)
        for x in range(box_length):
            print('=', end='', flush=False)

        # Print content
        for line in info_array:
            print_line = line.ljust(box_length)
            print("|\r\n|%s" % print_line, end='', flush=False)

        print("|\r\n|", end='', flush=False)
        for x in range(box_length):
            print('=', end='', flush=False)
        print("|\r\n", end='', flush=True)
    except:
        pass

    finally:
        print_mutex.release()

    output("", True)


# =======================================================================================================================
# Print JSON
# =======================================================================================================================
def jput(json_data, is_newLine):
    global new_line_counter

    print_mutex.acquire()

    try:
        if (is_newLine):
            pprint.pprint(json_data)
            new_line_counter += 1
            prompt = get_prompt()
            print(prompt, end='', flush=True)
        else:
            pprint.pprint(json_data)
    except:
        pass

    finally:
        print_mutex.release()

# =======================================================================================================================
# Print AnyLog Row in a formatted way
# =======================================================================================================================
def print_row(data_row, is_newLine=False):
    index = data_row.find('[')
    if index == -1:
        output(data_row, True)
    else:
        end_offset = len(data_row)
        offset = index + 1
        output('\r\n' + data_row[:offset], False)  # print the first line
        index += 1

        print_row_internal(data_row, index, offset, end_offset, False)

    if is_newLine:
        print_prompt()

# =======================================================================================================================
# Print the internal part of a row
# =======================================================================================================================
def print_row_internal(data_row, index, offset, end_offset, print_spaces):
    '''
    data_row - The row to print
    index - spaces from left border
    offset - start print offset
    end_offset - end print offset
    print_spaces - make spaces before print
    '''

    while offset < end_offset:
        segment_length = find_key_val_comma(data_row, offset)
        if not segment_length:
            segment_length = end_offset - offset

        if offset + segment_length >= end_offset:
            is_last_column = True
        else:
            is_last_column = False
        print_column(data_row, index, offset, segment_length, print_spaces, is_last_column)
        offset += segment_length
        print_spaces = True


# =======================================================================================================================
# Find the comma at the end of the key-value pair (vs. a comma that can be in a number like 1,000,000
# =======================================================================================================================
def find_key_val_comma(data_row, offset):

    index = offset
    str_length = len(data_row)
    quotation = False
    ch = 0
    while index < str_length:
        ch = data_row[index]
        if ch == '"':
            quotation = not quotation   # Avoid the comma inside a number like "1,000,000"
        else:
            if ch == ',':
                if not quotation:
                    index = index + 1 - offset  # return after the comma
                    break
        index += 1
    if ch != ',':
        index = 0
    return index



# =======================================================================================================================
# Print One column from a row
# =======================================================================================================================
def print_column(data_row, index, offset, segment_length, print_spaces, is_last_column):
    print_length = segment_length

    if print_length > 80:
        key_length = data_row.find(':')  # find position of print if broken to second line
        if key_length != - 1:
            key_length += 1
        else:
            key_length = 0
    else:
        key_length = 0
    add_spaces = 0

    while print_length > 80:  # print long segments
        if print_spaces:
            output_char(' ', index + add_spaces, False)
        else:
            print_spaces = True
        output(data_row[offset:offset + 80] + '\r\n', False)
        print_length -= 80
        offset += 80
        add_spaces = key_length  # push second line

    if print_length:
        if print_spaces:
            output_char(' ', index + add_spaces, False)  # set next line
        if not is_last_column or data_row[offset + print_length -1] == ',':
            output(data_row[offset:offset + print_length] + '\r\n', False)
        else:
            # last column without comma seperated - avoid new line - next output will add comma or end structure
            output(data_row[offset:offset + print_length], False)

# =======================================================================================================================
# Print char multiple times
# =======================================================================================================================
def output_char(char, count, is_newLine):
    print_mutex.acquire()

    try:
        for x in range(count):
            print(char, end='', flush=False)

        if (is_newLine):
            prompt = get_prompt()
            print(prompt, end='', flush=True)
        else:
            print("", end='', flush=True)

    except:
        pass

    finally:
        print_mutex.release()


# =======================================================================================================================
# Print prompt
# =======================================================================================================================
def print_prompt():
    prompt = get_prompt()
    output(prompt, False)


def get_new_line_counter():
    return new_line_counter


def print_list(the_list: list):
    index = 1
    for entry in the_list:
        output(str(index).ljust(40) + " : " + entry, False)
        output("\r\n", False)
        index = index + 1
    output('AL > ', False)


# --------------------------------------------------------------------
# Print the values in a dictionary
# --------------------------------------------------------------------
def format_dictionary(the_dictionary: dict, key_first, is_print, is_sort, title):
    # find the max length of the first printed value
    reply_str = "\r\n"
    if title:
        if key_first:
            max_length = len(title[0])
        else:
            max_length = len(title[1])
    else:
        max_length = 0

    for key_entry, val in the_dictionary.items():
        key = str(key_entry)
        value = str(val)
        if key_first:
            if len(key) > max_length:
                max_length = len(key)
        else:
            if len(value) > max_length:
                max_length = len(value)

    # PRINT TITLE (IF AVAILABLE)

    if title:
        if key_first:
            entry = title[0].ljust(max_length) + "   " + title[1] + "\r\n"
            next_len = len(title[1])
        else:
            entry = title[1].ljust(max_length) + "   " + title[0] + "\r\n"
            next_len = len(title[0])
        reply_str += entry

        reply_str += "-".ljust(max_length, '-') + '   ' + "-".ljust(next_len, '-') + "\r\n"

    # print KEY : VALUE or VALUE : KEY


    keys_list = list(the_dictionary.keys())
    for index, entry in enumerate(keys_list):
        # change values to strings
        if not isinstance(entry, str):
            entry[index] = str(entry)

    if is_sort:
        keys_list.sort()

    for key in keys_list:
        value = str(the_dictionary[key])
        if key_first:
            entry = key.ljust(max_length) + " : " + value + "\r\n"
            if is_print:
                output(entry, False)
            else:
                reply_str += entry
        else:
            entry = value.ljust(max_length) + " : " + key + "\r\n"
            if is_print:
                output(entry, False)
            else:
                reply_str += entry

    return reply_str


def print_string_prefix(string, substring, is_newLine):
    if not isinstance(substring, str):
        return  # has to be a string
    try:
        offset = string.find(substring)
    except:
        return
    if offset == -1:
        output(string, is_newLine)  # no substring - print string
    else:
        sub_length = len(substring)
        output(string[0:offset + sub_length], is_newLine)  # print including substring


# --------------------------------------------------------------------
# Print nested lists - first pass - get the field size, second print
# --------------------------------------------------------------------
def output_nested_lists(nested_lists: list, header, title, get_info_str: bool, line_prefix = "", capital_title = False):
    '''
    capital_title - change first char of title to capital letter
    '''
    columns_length = []  # maintain the max length for each colum
    ret_str = ""

    title1 = None
    title2 = None

    if title:
        title_lines = 1
        # Go over the title - an determine if title is one line or 2 lines
        for index, entry in enumerate(title):
            offset = entry.find('\n')
            if offset == -1:
                if title1:
                    title1.append(entry)
                    title2.append("")
                length = len(entry)
                columns_length.append(length)
            else:
                # Split to 2 title lines
                if not title1:
                    # Create title1 and title2
                    title_lines = 2
                    title1 = []
                    title2 = []
                    for i in range (index):
                        title1.append(title[i])
                        title2.append("")

                entry_size_1 = offset
                entry_size_2 = length = len(entry) - offset
                title1.append(entry[:offset])
                title2.append(entry[offset + 1:])
                if entry_size_1 > entry_size_2:
                    columns_length.append(entry_size_1)
                else:
                    columns_length.append(entry_size_2)
    else:
        title_lines = 0

    # Find the length of the columns
    if nested_lists:
        for columns in nested_lists:
            if columns:
                for index, col_data in enumerate(columns):
                    length = len(str(col_data))
                    if length > 100:
                        length = 100
                    if index >= len(columns_length):
                        columns_length.append(length)  # first value
                    elif length > columns_length[index]:
                        columns_length[index] = length  # replace with larger value

    if header:
        out_str = "\r\n" + line_prefix + header
    else:
        out_str = ""

    # print title
    if title:
        if title_lines == 1:
            use_title = title           # Use the origin title
        else:
            use_title = title1          # First line title

        for _ in range (title_lines):

            out_str += ("\r\n" + line_prefix)
            # Go over the title and print title
            for index, entry in enumerate(use_title):
                length = columns_length[index]
                if capital_title and len(entry):
                    # change first char to capital letter
                    out_str += (entry[0].upper() + entry[1:]).ljust(length + 1)
                else:
                    # Keep title as is
                    out_str += entry.ljust(length + 1)

            if get_info_str:
                ret_str += out_str
            else:
                output(out_str, False)
            use_title = title2
            out_str = ""

        out_str = ("\r\n" + line_prefix)
        # Go over the title and print title's underline

        for index, entry in enumerate(title):
            length = columns_length[index]
            out_str += "-".ljust(length, '-') + '|'

        if get_info_str:
            ret_str += out_str
        else:
            output(out_str, False)

    # print columns
    if nested_lists:
        for columns in nested_lists:
            if columns:
                out_str = ("\r\n" + line_prefix)
                prefix = "\r\n"  # prefix is used for a column greater than 100 that is printed in multiple lines
                for index, col_data in enumerate(columns):
                    length = columns_length[index]
                    if col_data != None:
                        if isinstance(col_data, bool):
                            out_str += str(col_data).ljust(length) + '|'
                        elif is_right_shift(col_data):
                            # value = format(col_data, ",")
                            # out_str += value.rjust(length) + '|'
                            out_str += str(col_data).rjust(length) + '|'
                        else:
                            # String
                            data_str = str(col_data).replace('\r','').replace('\n',' ').replace('\t',' ')
                            if len(data_str) <= 100:
                                out_str +=data_str.ljust(length) + '|'
                            else:
                                # print multiple lines
                                offset = 0
                                while offset < len(data_str):
                                    if offset:
                                        out_str += prefix           # Empty lines to get to the column position
                                    out_str += str(data_str[offset:offset + 100]).ljust(100) + '|'
                                    offset += 100
                        prefix += ' '.ljust(length) + '|'
                    else:
                        extension = ' '.ljust(length) + '|'
                        prefix += extension
                        out_str += extension
                if get_info_str:
                    ret_str += out_str
                else:
                    output(out_str, False)

    if not get_info_str:
        output('\r\n', True)
    else:
        ret_str += "\r\n"

    return ret_str

# --------------------------------------------------------------------
# test if a number or a comma seperated number to allighn to the right
# --------------------------------------------------------------------
def is_right_shift(col_data):
    if isinstance(col_data, int) or isinstance(col_data, float) or (isinstance(col_data, str) and len(col_data) < 20 and is_formated_number(col_data)):
        return True
    return False
# --------------------------------------------------------------------
# test if the string is a formatted number
# --------------------------------------------------------------------
def is_formated_number(col_data):
    with_number = False
    for char in col_data:
        if char >= '0' and char <= '9':
            with_number = True
        elif char == '-':
            if with_number:
                return False        # - sign needs to be once before the nuber itself
            with_number = True
        elif char != '.' and char != ',':
            return False            # Ignored
    return True

# --------------------------------------------------------------------
# Print list
# --------------------------------------------------------------------
def output_list(data_list: list, title: str, get_info_str: bool):
    if title:
        out_str = "\r\n" + title + "\r\n"
        out_str += "-".ljust(len(title), '-')
    else:
        out_str = ""

    if not get_info_str:
        output(out_str, False)

    for entry in data_list:
        if not get_info_str:
            output("\r\n" + entry, False)
        else:
            out_str += "\r\n" + entry

    if not get_info_str:
        output('', True)

    return out_str


# =======================================================================================================================
# Add structure to every printed line
# =======================================================================================================================
def output_lines(title, info, new_line_prefix, add_al_prompt, get_info_str):

    updated_info = new_line_prefix + info.replace("\n", new_line_prefix)

    if get_info_str:
        out_str = title + updated_info
    else:
        out_str = ""
        output(title, False)
        output(updated_info, False)

        if add_al_prompt:
            output("\r\n", True)

    return out_str

# =======================================================================================================================
# Print star fro wait time
# =======================================================================================================================
def time_star_print(text, sleep_time, intervals, elements):
    '''
    Text - message
    sleep_time - wait time
    intervals - how many time intevals to keep
    elements - total stars
    '''

    if text:
        output(f"\n\r{text}\r\n", False)
    interval_time = sleep_time / intervals
    counter = int(elements / intervals)

    star_count = 0
    while star_count <= elements:
        print_status(elements, elements, star_count, False)
        time.sleep(interval_time)
        star_count += counter

    output("\r\n", True)
# =======================================================================================================================
# Print a fix number of chars (prints_count) to represent status
# An output in the format of [******     ] showing state
# =======================================================================================================================
def print_status(elements, prints_count, index, get_info_str: bool = False):
    '''
    Elements - number of elements that make the 100%
    prints_count - how many signs represent all elements
    index - the value to show on the bar
    get_info_str - True is the bar string is returned
    '''
    info_str = ""

    counter = int(elements / prints_count)  # counter for index value per print

    if not counter:
        counter = 1
        max_prints = elements
    else:
        max_prints = prints_count

    if get_info_str:
        star_count = int (index / counter)
        if star_count < 2 and index:
            star_count = 2  # Show one star
        info_str = '['.ljust(star_count, '*').ljust(max_prints, ' ')
        info_str += "]"
    else:

        count = index % counter

        if not count:       # Avoid too many prints that will take all the cpu
            print_mutex.acquire()

            print('\r[', end='', flush=False)

            for i in range(max_prints):
                if i * counter <= index:
                    ch = '*'
                else:
                    ch = ' '

                print(ch, end='', flush=False)

            print(']', end='', flush=True)

            print_mutex.release()

        if index == (elements - 1):
            output("\r\n", True)

    return info_str
# =======================================================================================================================
# Print a list whereas every entry is a list of strings
# =======================================================================================================================
def print_data_list(out_list, rows_count, print_title, get_info_str, new_line = "\r\n"):
    '''
    out_list -  a list to print
    rows_count - number of entries to print
    print_title - a bool flag to print the firt row in the list as a title
    get_info_str - a bool value to return the output info as a string
    new_line - the chars representing a new line
    '''

    info_str = ""

    columns_count = len(out_list)
    columns_length = [0] * columns_count  # maintain the max length for each column

    # Get the max size for each column
    for index, columns in enumerate(out_list):
        for column in columns:
            if len(column) > columns_length[index]:
                columns_length[index] = len(column)

    # print the data
    for row_counter in range(rows_count):  # rows_count in the number of rows to print
        for i in range(2):  # for title do twice
            if get_info_str:
                info_str += new_line
            else:
                output(new_line, False)
            last_column = columns_count - 1
            for column_number in range(columns_count):
                length = columns_length[column_number]
                if i == 1:
                    out_str = "-".ljust(length, '-') + ' '
                else:
                    column_data = out_list[column_number][row_counter]

                    if is_right_shift(column_data):
                        out_str = str(column_data).rjust(length) + ' '
                    else:
                        if column_number < last_column:
                            # Does not increase the size of the last data column
                            out_str = str(column_data).ljust(length + 1)
                        else:
                            out_str = column_data

                if get_info_str:
                    info_str += out_str
                else:
                    output(out_str, False)

            if row_counter or not print_title:
                break

    if get_info_str:
        info_str += new_line
    else:
        output(new_line, False)
    return info_str

# =======================================================================================================================
# Dynamic Tab
# =======================================================================================================================
def dynamic_tab(new_data, new_line, offset_tab, is_print):

    info = ""

    if new_line:
        info = "\r\n"

    if offset_tab:
        info += " ".rjust(offset_tab, ' ')

    info += new_data

    if is_print:
        output(info, False)
        ret_str = ""        # No need to return data
    else:
        ret_str = info

    return ret_str
# =======================================================================================================================
# Print string or List or JSON structure
# =======================================================================================================================
def struct_print(struct_data, is_first, is_last, max_str = 0):
    '''
    if max_str has a value - will not print strings larger than specified size (used to eliminate print of blobs like video
    '''

    is_number = isinstance(struct_data, int) or isinstance(struct_data, float)

    if struct_data or is_number:
        if is_first:
            # First message
            output("\r\n", False)
        elif  isinstance(struct_data,str) and struct_data.find('\n',0,512) == -1:
            # the message is a string and is not formatted to be printed on the monitor
            output("\r\n", False)

        if is_number:
            print(struct_data, end='', flush=True)
        else:
            _print_recursive(0, struct_data, max_str)

        if is_last:
            output("\r\n", True)

# =======================================================================================================================
# Print depending on the data structure
# =======================================================================================================================
def _print_recursive(offset, struct_data, max_str):
    if struct_data != None:
        is_printed = False

        # print a dictionary
        if isinstance(struct_data, dict):
            print_dictionary(offset, struct_data, max_str)
            #jput(struct_data, False)
            is_printed = True

        # print a list
        elif isinstance(struct_data, list):
            output('[', False)
            for index, entry in enumerate(struct_data):
                if index:
                    output(',\r\n' +  ' ' * (offset + 1), False)
                _print_recursive(offset + 1, entry, max_str)
            output(']', False)
            is_printed = True

        # print a string
        elif isinstance(struct_data, str):
            if len(struct_data):
                if struct_data[0] == '{':
                    json_struct = str_to_json(struct_data)
                    if json_struct:
                        _print_recursive(offset, json_struct, max_str)
                        is_printed = True
                elif struct_data[0] == '[':
                    list_struct = str_to_list(struct_data)
                    if list_struct:
                        _print_recursive(offset, list_struct, max_str)
                        is_printed = True

        if not is_printed:
            print_data = str(struct_data)
            if print_data.find('\n') != -1:
                # The data has formatting chars
                if print_data.find('\r') == -1:
                    # add \r
                    print_data = print_data.replace('\n','\r\n')
            else:
                print_data = break_string(print_data, offset, 0, max_str)
            output(print_data, False)
    return offset

# =======================================================================================================================
# Print JSON Object
# =======================================================================================================================
def print_dictionary(offset, struct_data, max_str):

    print_str = "{"
    offset += 1
    index = 0
    for key, value in struct_data.items():
        if index:
            print_str += ',\r\n' + ' ' * offset
        print_str += "'%s' : " % key

        # Try if string is an object
        if isinstance(value, str) and len(value) > 2:
            if value[0] == '[' and value[-1] == ']':
                object = str_to_list(value)
                if object:
                    value = object
            elif value[0] == '{' and value[-1] == '}':
                object = str_to_json(value)
                if object:
                    value = object

        if isinstance(value, str):
            # print a string + format to max chars per line
            # new line every 64 bytes
            print_str += break_string(value, offset, len(key), max_str)

        # If an entry is a list or a dictionary
        elif isinstance(value, dict) or isinstance(value, list):
            output(print_str, False)
            print_str = ""
            _print_recursive(offset + len(key) + 5, value, max_str)
        else:
            # everything else
            print_str += "%s" % value

        index += 1

    print_str += "}"
    output(print_str, False)

    return offset

# =======================================================================================================================
# break string - long string are broken to multiple lines
# =======================================================================================================================
def break_string(str_val, offset, key_len, max_str):


    str_len = len(str_val)
    if max_str and str_len >= max_str:
        # Only print a representative of the string
        print_str = str_val[:10] + " ... " + str_val[-10:]
    else:
        print_str = ""
        str_offset = 0

        # test if string has \n character (i.e. Public or Private key)
        str_list = str_val.replace('\r','').split('\n')
        list_size = len(str_list)
        list_index = 0

        while True:
            if list_index >= list_size:
                break

            if list_size == 1:
                # New Line every 64 chars
                print_str += "'%s'" % str_val[str_offset:str_offset + 64]
                str_offset += 64
                if str_offset >= str_len:
                    break
            else:
                # Print from the list
                if not str_list[list_index]:
                    list_index += 1
                    continue            # Empty line

                print_str += "'%s'" % str_list[list_index]
                list_index += 1

                if list_index == (list_size - 1) and  str_list[list_index] == "":
                    # Last one is empty
                    break

            if not print_str and key_len:
                # First line - print in the same line of the key:
                print_str += ' ' * (offset + 3)
            else:
                print_str += '\r\n' + ' ' * (offset + key_len + 5)

    return print_str

# =======================================================================================================================
# Print debug
# =======================================================================================================================
def print_debug(function_name, data_str):

    print_str = "[Debug] [%s] [%s]" % (function_name, data_str)
    output(print_str, True)

# =======================================================================================================================
# Print dictionary as a table. The table columns are taken from the attr_names list.
# If an attr name is missing in the dict_struct, the value is taken from info_struct
# =======================================================================================================================
def print_dict_as_table(key, dict_list, info_struct, attr_names, get_info_str, line_prefix, title, capital_title):
    '''
    key - the policy type
    dict_list - a list of dictionaries, each dictionary is printed, entries in the dictionary can be lists
    info_struct - additional info in a dictionary
    attr_names - title list
    get_info_str - True to return as a string
    line_prefix - border spaces
    title - print title
    capital_title - change first char of title to capital letter

    '''

    print_list = []

    for dict_struct in dict_list:
        dict_position = len(print_list)     # The location of the dictionary in the list
        print_list.append(["" for _ in range(len(attr_names))])

        for col_id, col_name in enumerate(attr_names):
            col_name = col_name.lower()
            if key and key in dict_struct and col_name in dict_struct[key]:
                col_val = dict_struct[key][col_name]
            elif col_name in dict_struct:
                col_val = dict_struct[col_name]
            elif info_struct and col_name in  info_struct:
                col_val = info_struct[col_name]
            else:
                col_val = ""

            if isinstance(col_val, list):
                for index, entry in enumerate(col_val):
                    if (dict_position + index) >= len(print_list):
                        print_list.append(["" for _ in range(len(attr_names))])
                    print_list[dict_position + index][col_id] = entry

            elif col_val or isinstance(col_val, int):
                print_list[dict_position][col_id] = col_val

    reply_data = output_nested_lists(print_list, title, attr_names, get_info_str, line_prefix, capital_title)

    return reply_data
