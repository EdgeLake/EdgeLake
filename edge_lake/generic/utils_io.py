"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import os
import sys
import shutil
import hashlib
import time
import gzip
from os import walk
import base64

import edge_lake.generic.process_log as process_log
import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_threads as utils_threads
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.params as params

from edge_lake.generic.utils_columns import seconds_to_date, get_current_utc_time
from edge_lake.generic.utils_json import str_to_json


last_time_stamp = 0  # value based on time providing unique string

crlf_ = "\r\n".encode(encoding='UTF-8')
end_msg_ = "0\r\n\r\n".encode(encoding='UTF-8')

# a global read/write lock on files, like the blockchain local file
file_locker = {
    "blockchain": utils_threads.ReadWriteLock(),
    "new": utils_threads.ReadWriteLock(),
    "sql": utils_threads.ReadWriteLock(),
    "json": utils_threads.ReadWriteLock(),
}

class FileMetadata:

    def __init__(self):
        self.reset()

    def reset(self):
        self.file_name = None
        self.no_path_name = ""      # File name without path
        self.file_type = None
        self.dbms_name = None
        self.table_name = None
        self.par_name = None
        self.data_source = '0'  # ID of the device
        self.hash_value = '0'
        self.instructions = '0'  # Blockchain instructions
        self.tsd_member = None  # The member delivering the file
        self.tsd_row_id = '0'  # The row id in the TSD table
        self.tsd_date = None  # The date associated with the file

        self.tsd_status1 = ""  # Status field that can be added to the TSD table
        self.tsd_status2 = ""  # Status field that can be added to the TSD table

    def get_file_name(self):
        return self.file_name       # Path + name

    def get_no_path_name(self):
        return self.no_path_name   # File name without path

    def get_dbms_name(self):
        return self.dbms_name

    def get_table_name(self):
        return self.table_name

    def get_partition_name(self):
        return self.par_name

    def get_data_source(self):
        return self.data_source

    def get_instructions(self):
        return self.instructions


    # =======================================================================================================================
    # Map file name to the object elements
    # =======================================================================================================================
    def set_file_name_metadata(self, status, path_file: str):

        self.reset()

        ret_val = process_status.Error_file_name_struct

        self.tsd_status1 = ""  # Status field that can be added to the TSD table
        self.tsd_status2 = ""  # Status field that can be added to the TSD table

        self.file_name = path_file

        path, file_name, self.file_type = extract_path_name_type(path_file)
        self.no_path_name = file_name + "." + self.file_type

        try:
            file_sections = file_name.split('.')
        except:
            file_sections = []

        counter_sections = len(file_sections)
        if counter_sections < 2:
            status.add_error("JSON file name is missing dbms.table section: '%s'" % file_name)
        elif self.file_type == "json" and counter_sections > 5 and counter_sections != 8:
            status.add_error("JSON file name is not structured correctly: '%s'" % file_name)
        else:
            self.dbms_name = set_val_or_default(file_sections, 0, str, None)
            self.table_name = set_val_or_default(file_sections, 1, str, None)
            if self.file_type == "sql":
                # The next section for SQL is the partition ID
                self.par_name = set_val_or_default(file_sections, 2, str, '0')
                section_id = 3
            else:
                # No partition ID
                self.par_name = None
                section_id = 2

            self.data_source = set_val_or_default(file_sections, section_id, str, '0')
            self.hash_value = set_val_or_default(file_sections, section_id + 1, str, '0')
            self.instructions = set_val_or_default(file_sections, section_id + 2, str, '0')

            if self.file_type == "sql":
                self.tsd_member = None  # The member delivering the file
                self.tsd_row_id = '0'  # The row id in the TSD table
                self.tsd_date = None  # The date associated with the file
                ret_val = process_status.SUCCESS
            else:
                offset = section_id + 3
                if len(file_sections) >  offset and file_sections[offset] == '@':
                    # From streaming data with write immediate flag
                    self.tsd_member = '@'
                else:
                    # Data is set as a file or from streaming data with write immediate set to FALSE
                    self.tsd_member = set_val_or_default(file_sections, section_id + 3, int, None)
                self.tsd_row_id = set_val_or_default(file_sections, section_id + 4, str, '0')
                self.tsd_date = set_val_or_default(file_sections, section_id + 5, str, None)

                if self.tsd_date and len(self.tsd_date) < 12:
                    status.add_error("JSON file name: time section with incorrect length: '%s'" % file_name)
                if counter_sections == 8:
                    if self.tsd_member == None:
                        status.add_error("JSON file name: TSD member section is with unrecognized value: '%s'" % file_name)
                    elif self.tsd_row_id == '0':
                        status.add_error("JSON file name: TSD member section is missing a row ID value: '%s'" % file_name)
                    else:
                        ret_val = process_status.SUCCESS
                else:
                    ret_val = process_status.SUCCESS

        return ret_val

    # =======================================================================================================================
    # return the file type
    # =======================================================================================================================
    def get_file_type(self):
        return self.file_type

    # =======================================================================================================================
    # Rename the file to be the same as the structure data
    # =======================================================================================================================
    def make_json_file(self, status):

        file_path, current_name, file_type = extract_path_name_type(self.file_name)

        new_file = "%s%s.%s.%s.%s.%s.%s.%s.%s.%s" % (file_path, self.dbms_name, self.table_name, self.data_source, self.hash_value,\
                                                     self.instructions, self.tsd_member, self.tsd_row_id, self.tsd_date, file_type)

        was_renamed = rename_file(status, self.file_name, new_file)

        reply_list = [was_renamed, new_file]
        return reply_list

    # =======================================================================================================================
    # Return True/False for hash_value
    # =======================================================================================================================
    def with_hash_value(self):
        return self.hash_value != '0'
    # =======================================================================================================================
    # Calculate the hash value of the file
    # =======================================================================================================================
    def add_hash_value(self, status, dbms_name, table_name):

        got_hash, ret_hash = get_hash_value(status, self.file_name, "", dbms_name + '.' + table_name)
        if not got_hash:
            status.add_error("Failed to access or calculate hash value for file: %s" % self.file_name)
            ret_val = process_status.ERR_file_does_not_exists
        else:
            self.hash_value = ret_hash
            ret_val = process_status.SUCCESS

        return ret_val
    # =======================================================================================================================
    # Set the current time in the metadata object
    # =======================================================================================================================
    def set_time_key(self):
        utc_time = get_current_utc_time("%Y-%m-%d %H:%M:%S")
        self.tsd_date = utc_time[2:4] + utc_time[5:7] + utc_time[8:10] + utc_time[11:13] + utc_time[14:16] + utc_time[17:19]

    # =======================================================================================================================
    # Add a status field that will be inserted to the row in the TSD table
    # =======================================================================================================================
    def set_status1(self, status):
        self.tsd_status1 = status  # Status field that can be added to the TSD table
    # =======================================================================================================================
    # Add a status field that will be inserted to the row in the TSD table
    # =======================================================================================================================
    def set_status2(self, status):
        self.tsd_status1 = status  # Status field that can be added to the TSD table
    # =======================================================================================================================
    # Add the member delivering the file
    # =======================================================================================================================
    def set_member_id(self, member_id):
        self.tsd_member = member_id

    def set_row_id(self, row_id):
        self.tsd_row_id = row_id

    # =======================================================================================================================
    # Return True if the file is from a member node
    # =======================================================================================================================
    def is_from_cluster_member(self, current_member_id):
        if self.file_type != "json" or not self.tsd_member or self.tsd_member == '@' or self.tsd_member == current_member_id:
            return False
        return True



def set_val_or_default(entries_list, offset , data_type, default):
    if offset < len(entries_list):
        try:
             value = data_type(entries_list[offset])
        except:
            value = default
    else:
        value = default

    return value


class IoHandle:

    def __init__(self):
        self.file_name = None
        self.file_type = None  # read or write
        self.file_object = None
        self.last_message = None
        self.file_stat = 0  # 0 is close, 1 is open
        self.status = False  # open sets to true, failure sets to false

    def get_file_name(self):
        return self.file_name

    def open_file(self, file_type: str, fname: str):

        ret_val = True

        self.file_name = os.path.expanduser(os.path.expandvars(fname))
        self.file_type = file_type  # read or write

        if self.file_type == "get":
            open_mode = 'r'
        elif self.file_type == "append":
            open_mode = 'a'
        elif self.file_type == "new":
            open_mode = 'w'     # Delete and write
        elif self.file_type == "write":
            open_mode = 'wb'  # write binary
        elif self.file_type == "read":
            open_mode = 'rb'  # read binary
        else:
            self.last_message = "I/O open file error - unrecognized file type"
            return False

        try:
            self.file_object = open(self.file_name, open_mode)  # read will return data
        except IOError as e:
            errno, strerror = e.args
            self.last_message = "I/O open file error ({0:d}: {1})".format(errno, strerror)
            process_log.add("Error", self.last_message)
            self.file_stat = -1  # file error
            ret_val = False

        if ret_val == True:
            self.file_stat = 1  # file open

        return ret_val

    def is_with_error(self):
        if self.file_stat == -1:
            return True
        return False

    def close_file(self):
        self.file_stat = 0
        ret_val = True
        try:
            self.file_object.close()
        except IOError as e:
            errno, strerror = e.args
            self.last_message = "I/O close file error ({0:d}: {1})".format(errno, strerror)
            process_log.add("Error", self.last_message)
            self.file_stat = -1  # file error
            ret_val = False
        return ret_val

    # -----------------------------------------------------------------------
    # Read all the lines and returns a list
    # -----------------------------------------------------------------------
    def read_lines(self):

        try:
            lines_read = self.file_object.readlines()
        except:
            errno, value = sys.exc_info()[:2]
            self.last_message = "I/O read data error %s: %s" % (str(errno), str(value))
            process_log.add("Error", self.last_message)
            self.file_stat = -1  # file error
            lines_read = None

        return lines_read

    # -----------------------------------------------------------------------
    # Reads a single line and return as a string
    # -----------------------------------------------------------------------
    def read_one_line(self):

        try:
            line_read = self.file_object.readline()
        except IOError as e:
            errno, strerror = e.args
            self.last_message = "I/O read data error ({0:d}: {1})".format(errno, strerror)
            process_log.add("Error", self.last_message)
            self.file_stat = -1  # file error
            line_read = ""

        return line_read

    def read_data(self, size):

        try:
            data_read = self.file_object.read(size)
        except IOError as e:
            errno, strerror = e.args
            self.last_message = "I/O read data error ({0:d}: {1})".format(errno, strerror)
            process_log.add("Error", self.last_message)
            self.file_stat = -1  # file error

        return data_read

    def append_data(self, data: str):

        ret_val = True
        try:
            self.file_object.write(data)
        except IOError as e:
            errno, strerror = e.args
            self.last_message = "I/O append data error ({0:d}: {1})".format(errno, strerror)
            process_log.add("Error", self.last_message)
            self.file_stat = -1  # file error
            ret_val = False
        return ret_val

    def read_into_buffer(self, buffer):
        """
        Read up to len(block) bytes into block, and return the number of bytes read
        :param buffer: should be pre-allocated array of bytes or memoryview
        :return: Number of bytes read
        """

        try:
            bytes_read = self.file_object.readinto(buffer)
        except IOError as e:
            errno, strerror = e.args
            self.last_message = ("I/O read into buffer error ({0}: {1})".format(errno, strerror))
            process_log.add("Error", self.last_message)
            self.file_stat = -1  # file error

        return bytes_read

    def write_from_buffer(self, buffer):
        """
        Write size bytes from buffer to the file
        :param buffer: should be pre-allocated array of bytes or memoryview
        :return: True if success write
        """

        ret_val = True
        try:
            bytes_write = self.file_object.write(buffer)
        except IOError as e:
            errno, strerror = e.args
            self.last_message = ("I/O write into buffer error ({0}: {1})".format(errno, strerror))
            process_log.add("Error", self.last_message)
            self.file_stat = -1  # file error
            ret_val = False

        return ret_val

# =======================================================================================================================
# Return True to existing path
# =======================================================================================================================
def is_path_exists(file_name):
    try:
        fname = os.path.expandvars(file_name)
        ret_val = os.path.lexists(fname)
    except:
        ret_val = False
    return ret_val
# =======================================================================================================================
# Return the file size
# =======================================================================================================================
def get_file_size(status: process_status, file_name):
    fname = os.path.expandvars(file_name)
    try:
        size = os.path.getsize(fname)
    except IOError as e:
        errno, strerror = e.args
        status.add_eror_message = ("I/O error({0}: fo file: {1} - Error messag: {2}".format(errno, file_name, strerror))
        size = -1

    return size


# =======================================================================================================================
# Given a path, update the provided list with subdirectories or file names in the path
# =======================================================================================================================
def directory_walk(status: process_status, name_list: list, dir_path: str, info_type: str):
    """

    :param name_list: list to update with names
    :param dir_path:  Path to directory consider
    :param info_type: "file" or "dir" - object to retrieve
    :return: false with no data
    """
    ret_value = False
    try:
        path = os.path.expanduser(os.path.expandvars(dir_path))
        for (path, dirnames, filenames) in walk(path):
            if info_type == "file":
                ret_value = True
                name_list.extend(filenames)
                break  # ignore recursive
            elif info_type == "dir":
                ret_value = True
                name_list.extend(dirnames)
                break  # ignore recursive
            else:
                ret_value = False
                status.add_error("Directory walk eror: '%s' is not recognized - use file or dir objects" % info_type)
    except:
        ret_value = False
        status.add_error("Directory walk failed on path: '%s'" % dir_path)

    return ret_value
# ==================================================================
#
# get a list of all directories recursively
#
# ==================================================================
def get_directories_list(status, root_dir):
    """
    returns:
        ret_val
        the length of the root dir - as it may change after the call to expanduser
        a sorted directories list
    """

    ret_val = process_status.SUCCESS
    directories = []

    try:
        root_dirictory = os.path.expanduser(os.path.expandvars(root_dir))
        root_length = len(root_dirictory)

        for dirpath, dirnames, _ in os.walk(root_dirictory):    # filenames not needed
            for dirname in dirnames:
                directories.append(os.path.join(dirpath, dirname))
        directories.sort()  # os.walk() in Python is not guaranteed to be sorted
    except:
        errno, value = sys.exc_info()[:2]
        message = f"Failed to list directories from path: {root_dir}, errno: {errno}, [{value}]"
        status.add_error(message)
        ret_val = process_status.ERR_process_failure
        root_length = 0
    return [ret_val, root_length, directories]

# ==================================================================
#
# Test if a directory exists. If flag is on, write an error if doesn't exists
#
# ==================================================================
def test_dir_exists(status: process_status, dir_name: str, add_error):

    path = os.path.expanduser(os.path.expandvars(dir_name))

    try:
        if os.path.isdir(path) is False:
            ret_val = process_status.ERR_dir_does_not_exists
        else:
            ret_val = process_status.SUCCESS
    except:
        ret_val = process_status.ERR_dir_does_not_exists

    if ret_val and add_error:
        status.add_keep_error("Directory is not accessible or does not exists: " + dir_name)

    return ret_val

# ==================================================================
#
# Test if a directory is writeable
#
# ==================================================================
def test_dir_writeable(status: process_status, dir_name: str, add_error):
    path = os.path.expanduser(os.path.expandvars(dir_name))
    if os.access(path, os.W_OK) is False:
        if add_error:
            status.add_keep_error("Directory is not writeable: " + dir_name)
        ret_val = process_status.Directory_not_writeable
    else:
        ret_val = process_status.SUCCESS

    return ret_val

# ==================================================================
#
# Get permissions of a file or directory
# Command: get permissions my_file/my_dir
# ==================================================================
def get_access(status, io_buff_in, words_array, trace):

    if words_array[1] == '=':
        offset = 2
    else:
        offset = 0

    test_path = params.get_value_if_available(words_array[offset + 2])
    try:
        is_exists = str(os.access(test_path, os.F_OK)).ljust(6)  # Check for existence of file
        is_read = str(os.access(test_path, os.R_OK)).ljust(6)  # Check for read access
        is_write = str(os.access(test_path, os.W_OK)).ljust(6)  # Check for write access
        is_exec = str(os.access(test_path, os.X_OK)).ljust(6)  # Check for execution access
    except OSError as error:
        reply = "Failed to test permissions: Error: {0}".format(error)
        status.add_error(reply)
        ret_val = process_status.ERR_process_failure
    except:
        reply = "Unable to test permissions for path: %s - [%s]" % (words_array[offset + 2], test_path)
        status.add_error(reply)
        ret_val = process_status.ERR_process_failure
    else:
        reply = "\r\nExits Read  Write Exec\r\n" + is_exists + is_read + is_write + is_exec
        ret_val = process_status.SUCCESS

    reply_list = [ret_val, reply]
    return reply_list
# ==================================================================
#
# Test if a directory exists and is writeable
#
# ==================================================================
def test_dir_exists_and_writeable(status: process_status, dir_name: str, add_error):
    ret_val = test_dir_exists(status, dir_name, add_error)
    if ret_val:
        return ret_val

    ret_val = test_dir_writeable(status, dir_name, add_error)
    return ret_val


# ==================================================================
#
# create directory if not exists
#
# ==================================================================
def create_dir(status: process_status, dir_name: str):
    """
    :args:
       status:process_status - status code
       root_dir:str - directory to create
    :return:
       if failed return False, else return True
    """

    path = os.path.expanduser(os.path.expandvars(dir_name))
    if os.path.isdir(path) is False:
        try:
            os.makedirs(path)
        except OSError as error:
            err_msg = "Failed to create directory: Error: {0}".format(error)
            status.add_error(err_msg)
            ret_val = False
        except:
            status.add_error("Unable to create directory")
            ret_val = False
        else:
            ret_val = True

    return ret_val

# ==================================================================
#
# Given  a file, read the data and return a string
#
# ==================================================================
def read_to_string(status: process_status, file_name: str):
    """
    Read data from file to str
    :args:
       file_name:str - file containing JSON data
    :return:
       The file data as a string
    """
    file_name = os.path.expanduser(os.path.expandvars(file_name))

    try:
        with open(file_name, 'r') as f:
            data = f.read()
    except (IOError, EOFError) as e:
        errno, value = sys.exc_info()[:2]
        message = "Failed to read from file: '%s' with error %s: %s" % (file_name, str(errno), str(value))
        status.add_error(message)
        data = None

    return data


# ==================================================================
#
# Given  a file, read the data and store its information as a list
#
# ==================================================================
def read_all_lines_in_file(status: process_status, file_name: str):
    """
    Read file to a list
       file_name:str - file containing JSON data
    :return:
       list of data from file
    """
    file_name = os.path.expanduser(os.path.expandvars(file_name))

    try:
        with open(file_name, 'r') as f:
            data = f.read().split("\n")
    except (IOError, EOFError) as e:
        status.add_warning("In Read - Testing multiple exceptions. {}".format(e.args[-1]))
        return None

    return data

# ==================================================================
#
# write a string to a file
#
# ==================================================================
def write_str_to_file(status: process_status, data: str, fname: str):

    file_name = get_path(fname)

    if file_name:

        try:
            with open(file_name, 'w') as f:
                f.write(data)
        except:
            errno, value = sys.exc_info()[:2]
            message = "Failed to open and write file: '%s' with error %s: %s" % (file_name, str(errno), str(value))
            process_log.add("Error", message)
            ret_val = False
        else:
            ret_val = True
    else:
        status.add_error("File location '%s' is not recognized" % fname)
        ret_val = False

    return ret_val

# ==================================================================
# Write nested lists to a file
# ==================================================================
def output_nested_lists(status: process_status, nested_lists: list, title: list, fname: str):
    data = utils_print.output_nested_lists(nested_lists, "", title, True)
    ret_val = write_str_to_file(status, data[1:], fname)

    return ret_val


# ==================================================================
#
# Given data in json format, write the data to a file
#
# ==================================================================
def write_list_to_file(status: process_status, data: list, fname: str, lock_key):

    file_name = params.get_value_if_available(fname)
    if not file_name:
        status.add_error("File name using the key '%s' is not declared" % fname)
        ret_val = False
    else:
        file_name = os.path.expanduser(os.path.expandvars(file_name))
        ret_val = True

        if lock_key:
            write_lock(lock_key)  # prevent multiple writters

        try:
            with open(file_name, 'w') as f:
                for entry in data:
                    f.write(str(entry) + "\n")
        except (IOError, EOFError) as e:
            error_message = "IO Error - failed to write JSON data to file: {0} to file {1} err.".format(file_name,
                                                                                                        e.args[-1])
            status.add_error(error_message)
            ret_val = False  # return an empty list

        if lock_key:
            write_unlock(lock_key)  # prevent multiple writters

    return ret_val


# ==================================================================
#
# Given data in json format, write the data to a file to satisfy insert stmts
#
# ==================================================================
def write_tuples_to_file(status: process_status, insert_stmt: str, columns_list: list, data: list, fname: str,
                         lock_key: str):
    file_name = os.path.expanduser(os.path.expandvars(fname))

    if lock_key:
        write_lock(lock_key)  # prevent multiple writters

    try:
        file_object = open(file_name, "w")  # read will return data
    except IOError as e:
        errno, strerror = e.args
        message = "Write tuples to file: I/O open file error ({0:d}: {1}) : {2}".format(errno, strerror, file_name)
        process_log.add("Error", message)
        ret_val = False
        file_opened = False
    else:
        ret_val = True
        file_opened = True

    if ret_val:
        for row in data:
            columns_values = "("
            column_id = 0
            for column in row:
                columns_values += get_column_data_as_str(column_id, columns_list[column_id], str(column))
                column_id += 1
            columns_values += ");"
            try:
                file_object.write(insert_stmt + columns_values + "\n")
            except (IOError, EOFError) as e:
                error_message = "IO Error - failed to write tuples with error: {0} to file {1} err. {}".format(
                    file_name, e.args[-1])
                status.add_error(error_message)
                ret_val = False  # return an empty list
                break

    if file_opened:
        try:
            file_object.close()
        except IOError as e:
            errno, strerror = e.args
            message = "Write tuples to file: I/O close file error ({0:d}: {1})".format(errno, strerror)
            process_log.add("Error", message)
            ret_val = False

    if lock_key:
        write_unlock(lock_key)  # prevent multiple writters

    return ret_val


# ==================================================================
# Set the column data as a string based on the data type.
# Prefix with comma if not first column
# ==================================================================
def get_column_data_as_str(column_id: int, column_info: dict, column_data: str):
    # determine if quotations are needed
    if column_data == "None":
        column_str = "\'\'"
    else:
        column_str = ""
        if "data_type" in column_info:
            if column_info["data_type"] == 'int':
                column_str = column_data

        if column_str == "":
            column_str = '\'' + column_data + '\''

    if column_id:  # not the first - add comma
        column_str = ", " + column_str

    return column_str


# ==================================================================
#
# Extract the file name
#
# ==================================================================
def get_offset_file_name(path_file_name: str):
    # Could be / or \
    separator = params.get_path_separator()
    reverse = params.get_reverse_separator()
    offset1 = path_file_name.rfind(separator)
    offset2 = path_file_name.rfind(reverse, offset1 + 1)
    if offset2 > -1:
        offset_name = offset2 + 1
    else:
        offset_name = offset1 + 1

    return offset_name


# ==================================================================
#
# Extract the path, file name and file type from the provided name
#
# ==================================================================
def extract_path_name_type(path_file: str):

    separator = params.get_path_separator()
    reverse = params.get_reverse_separator()    # The Non-In_Use separator

    complete_name = path_file.replace(reverse, separator)  # Unify path

    offset_name = complete_name.rfind(separator)
    if offset_name == -1:
        file_path = ""
        file_name_type = complete_name
    else:
        file_path = complete_name[:offset_name + 1]  # including the "/" sign
        if offset_name < (len(complete_name) - 1):
            file_name_type = complete_name[offset_name + 1:]
        else:
            file_name_type = ""

    # extract the file type
    if file_name_type:
        file_name, file_type = extract_name_type(file_name_type)
    else:
        file_name = ""
        file_type = ""

    reply_list = [file_path, file_name, file_type]
    return reply_list

# -------------------------------------------------------
# Given a file name - extract the name and type
# -------------------------------------------------------
def extract_name_type(name: str):

    separator = params.get_path_separator()
    offset_name = name.rfind(separator)
    if offset_name == -1:
        # no path
        file_name_type = name
    else:
        if offset_name < (len(name) - 1):
            # get the offset after the '/' sign
            file_name_type = name[offset_name + 1:]
        else:
            # no file name
            file_name_type = ""

    offset_type = file_name_type.rfind('.')
    if offset_type == -1:
        file_name = file_name_type
        file_type = ""
    else:
        file_name = file_name_type[:offset_type]

        if offset_type < len(file_name_type) - 1:
            file_type = file_name_type[offset_type + 1:]
        else:
            file_type = ""

    reply_list = [file_name, file_type]
    return reply_list
# -------------------------------------------------------
# Given a file name - extract the file type
# -------------------------------------------------------
def extract_file_type(file_name: str):
    offset_type = file_name.rfind('.')
    if offset_type == -1 or offset_type == (len(file_name) -1):
        file_type = ""
    else:
        file_type = file_name[offset_type + 1:]
    return file_type
# -------------------------------------------------------
# Test file type
# -------------------------------------------------------
def test_file_type(file_name: str, file_type:str):
    return extract_file_type(file_name) == file_type
# ==================================================================
#
# Given a file_name with path return the table name (second segment in the name)
#
# Example:
#    $HOME/EdgeLake/data/contractor/in/lsl_demo.ping.12345.json --> ping
#
# ==================================================================
def get_table_name_from_file_name(file_name: str):
    try:
        table_name = file_name.split(".", 2)[1]
    except:
        table_name = ""
    return table_name.lower()

# ==================================================================
# Write a data block to file
# If the data block is first, truncate file
# ==================================================================
def write_data_block(fname: str, is_new_file: bool, data_block):

    file_name = os.path.expanduser(os.path.expandvars(fname))

    if is_new_file:
        open_mode = 'wb'  # binary file open and truncated
    else:
        open_mode = 'ab'  # binary file, append only mode

    try:
        with open(file_name, open_mode) as file_handle:
            file_handle.write(data_block)
    except:
        errno, value = sys.exc_info()[:2]
        message = "Failed to open and write file: '%s' with error %s: %s" % (file_name, str(errno), str(value))
        process_log.add("Error", message)
        ret_val = False
    else:
        ret_val = True

    if not ret_val:
        utils_print.output_box("Failed to write data block to file (open mode: %s): '%s'\nError: %s" % (open_mode, str(file_name), message))
        process_log.add("Error", "Error in write to file %s" % str(fname))
    return ret_val
# ==================================================================
# Change file Mode
# 0o444 - read only
# ==================================================================
def change_mode(fname, mode_val):
    try:
        os.chmod(fname, mode_val)
    except:
        ret_val = False
    else:
        ret_val = True
    return ret_val
# ==================================================================
# delete a directory along with all its contents
# ==================================================================
def delete_dir_with_content(status, directory_path):
    ret_val = process_status.SUCCESS
    try:
        if os.path.exists(directory_path):
            shutil.rmtree(directory_path)
    except:
        errno, value = sys.exc_info()[:2]
        err_msg = "Failed to delete directory with content %s: %s" % (str(errno), str(value))
        status.add_error(err_msg)
        ret_val = process_status.ERR_process_failure
    return ret_val

# ==================================================================
# Delete File
# ==================================================================
def delete_file(fname:str, add_error:bool = True):
    file_name = os.path.expanduser(os.path.expandvars(fname))

    ret_val = True
    if os.path.exists(file_name):
        try:
            os.remove(file_name)
        except IOError as e:
            if add_error:
                errno, strerror = e.args
                message = ("I/O error({0}: {1}".format(errno, strerror))
                process_log.add("Error", message)
            ret_val = False
    else:
        if add_error:
            message = "Delete file failed - file doesn\'t exists: %s" % file_name  # returns True
            process_log.add("Error", message)
        ret_val = False

    return ret_val

# ==================================================================
# Move multiple files on the same machine
# ==================================================================
def move_multiple_files(status, source, dest):

    dest_dir = params.get_value_if_available(dest)
    if not params.is_directory(dest_dir):
        dest_dir += params.get_path_separator()

    source_files = params.get_value_if_available(source)
    src_dir, src_name, src_type = extract_path_name_type(source_files)
    name_prefix, type_prefix = get_name_type_prefix(src_name, src_type)

    files_list = []
    ret_val = search_files_in_dir(status, files_list, src_dir, name_prefix, type_prefix, False, False)
    if not ret_val:
        if not len(files_list):
            ret_val = process_status.No_files_to_copy
        else:
            for file_name in files_list:
                file_source = src_dir + file_name

                if not move_file(file_source, dest_dir):
                    status.add_error("Failed to move file <%s> to <%s>" % (file_source, dest_dir))
                    ret_val = process_status.File_move_failed
                    break

    return ret_val

# ==================================================================
# Move File
# ==================================================================
def move_file(src_fname, dest_dir):
    file_name = os.path.expanduser(os.path.expandvars(src_fname))
    if not os.path.exists(file_name):
        message = "Move file failed - file doesn\'t exists: %s" % file_name  # returns True
        process_log.add("Error", message)
        ret_val = False
    else:
        dest_name = os.path.expanduser(os.path.expandvars(dest_dir))
        try:
            shutil.move(file_name, dest_name)
        except:
            errno, value = sys.exc_info()[:2]
            err_msg = "Failed to move file with error %s: %s" % (str(errno), str(value))
            process_log.add("Error", err_msg)
            ret_val = False
        else:
            ret_val = True

    return ret_val

# ==================================================================
# Copy multiple files on the same machine
# ==================================================================
def copy_multiple_files(status, source, dest):


    dest_dir = params.get_value_if_available(dest)
    if not params.is_directory(dest_dir):
        status.add_error("Destination directory is missing a separator (%s) at the end of the directory name" % params.get_path_separator())
        ret_val = process_status.Err_dir_name
    else:
        source_files = params.get_value_if_available(source)
        src_dir, src_name, src_type = extract_path_name_type(source_files)
        name_prefix, type_prefix = get_name_type_prefix(src_name, src_type)

        files_list = []
        ret_val = search_files_in_dir(status, files_list, src_dir, name_prefix, type_prefix, False, False)
        if not ret_val:
            if not len(files_list):
                ret_val = process_status.No_files_to_copy
            else:
                for file_name in files_list:
                    file_source = src_dir + file_name
                    file_dest = dest_dir + file_name

                    if not copy_file(file_source, file_dest):
                        status.add_error("Failed to copy file from <%s> to <%s>" % (file_source, file_dest))
                        ret_val = process_status.File_copy_failed
                        break

    return ret_val

# ==================================================================
# Set the file path separator depending on the OS
# ==================================================================
def set_path_separator(file_name):
    os_separator = params.get_path_separator()
    rv_separator = params.get_reverse_separator()
    return file_name.replace(rv_separator, os_separator)    # Make separator consistant with the OS

# ==================================================================
# Copy File
# ==================================================================
def copy_file(src_fname, dest_fname):

    separator = params.get_path_separator()
    reverse = params.get_reverse_separator()    # The Non-In_Use separator

    file_name = src_fname.replace(reverse, separator)  # Unify path
    file_name = os.path.expanduser(os.path.expandvars(file_name))
    if not os.path.exists(file_name):
        message = "Copy file failed - file doesn\'t exists: %s" % file_name  # returns True
        process_log.add("Error", message)
        ret_val = False
    else:
        dest = dest_fname.replace(reverse, separator)  # Unify path
        dest = os.path.expanduser(os.path.expandvars(dest))
        if dest[-1] == separator:
            # add source file name
            f_name, f_type = extract_name_type(file_name)
            dest += f"{f_name}.{f_type}"

        try:
            shutil.copyfile(file_name, dest)
        except:
            errno, value = sys.exc_info()[:2]
            err_msg = "Failed to copy file with error %s: %s" % (str(errno), str(value))
            process_log.add("Error", err_msg)
            ret_val = False
        else:
            ret_val = True

    return ret_val


# ==================================================================
# Get OS path
# ==================================================================
def get_os_path(fname):
    path = os.path.expanduser(os.path.expandvars(fname))
    return path


# ==================================================================
# Get Disk Space
# ==================================================================
def get_disk_stat(dir_path):
    path = get_path(dir_path)
    if path:
        try:
            disk_stat = shutil.disk_usage(path)
        except:
            disk_stat = None
    else:
        disk_stat = None
    return disk_stat


# ==================================================================
# Stream IO - Write IO Stream
# source_id is an id of the caller. Last id is 10
# See https://docs.python.org/3/library/http.client.html for Transfer-Encoding
# ==================================================================
def write_to_stream(status, io_stream, send_data, transfer_encoding, transfer_final):
    '''
    status - the process objext
    io_stream - the socket
    send_data - data to deliver
    transfer_encoding - true with messages with undetermined size: self.send_header('Transfer-Encoding', 'chunked') # https://en.wikipedia.org/wiki/Chunked_transfer_encoding
    '''

    global crlf_            # encoded CRLF ("\r\n")
    global end_msg_         # encoded 0 + CRLF + CRLF
    ret_val = process_status.SUCCESS
    try:
        if transfer_encoding:
            # Add the write size (in hex) in the buffer
            encoded_data = send_data.encode(encoding='UTF-8')
            write_data = f"{hex(len(encoded_data))[2:]}\r\n".encode(encoding='UTF-8') + encoded_data + crlf_  # Length of encoded data in hex - 'Transfer-Encoding', 'chunked' requests that value

            if transfer_final:
                # with Transfer-Encoding, write the final bytes:
                write_data += end_msg_
        else:
            write_data = send_data.encode(encoding='UTF-8')


        bytes_written = io_stream.write( write_data )

    except:
        errno, value = sys.exc_info()[:2]
        err_msg = "Failed to write to stream with error %s: %s" % (str(errno), str(value))
        status.add_error(err_msg)
        ret_val = process_status.ERR_write_stream

    return ret_val
# ------------------------------------------------------------------
# Stream to the user browser
# Example browser:
'''
<!DOCTYPE html>
<html>
<head>
    <title>Video Stream</title>
</head>
<body>
    <video width="640" height="480" controls autoplay>
        <source src="http://10.0.0.78:7849/?User-Agent=AnyLog/1.23?command=stream" type="video/mp4">
    </video>
</body>
</html>
'''
# ------------------------------------------------------------------
def stream_to_browser(status, stream_file, rest_wfile, delete_flag):
    '''
    status - thread status object
    stream_file - path and file name to the file to stream
    rest_wfile - the rest Socket Writer
    delete_file - True to delete the file after the stream
    '''
    '''
    self.send_response(200)
    self.send_header('Content-type', 'video/mp4')
    self.end_headers()
    '''
    try:
        with open(stream_file, 'rb') as video_file:
            rest_wfile.write(video_file.read())
    except:
        errno, value = sys.exc_info()[:2]
        err_msg = f"Failed to stream to the REST Socket with error #{str(errno)}: {str(value)}"
        status.add_error(err_msg)
        ret_val = process_status.ERR_write_stream
    else:
        ret_val = process_status.SUCCESS

    if delete_flag:
        # Delete the file even with error on the stream process
        delete_file(stream_file, False)

    return ret_val
# =======================================================================================================================
# Get message body data by reads until EOF is returned
# https://docs.python.org/3/library/io.html#io.BufferedIOBase
# =======================================================================================================================
def read_from_stream(status, io_stream, content_length):
    ret_val = process_status.SUCCESS
    data = None

    try:
        data = io_stream.read(content_length)
    except:
        errno, value = sys.exc_info()[:2]
        err_msg = f"Failed to read from stream with error {str(errno)}: {value}"
        status.add_error(err_msg)
        ret_val = process_status.ERR_write_stream

    reply_list = [ret_val, data]
    return reply_list
# ==================================================================
# Stream IO - Write IO Stream
# source_id is an id of the caller. Last id is 10
# ==================================================================
def write_encoded_to_stream(status, io_stream, send_data):
    ret_val = process_status.SUCCESS

    try:
        io_stream.write(send_data)
    except:
        errno, value = sys.exc_info()[:2]
        err_msg = "Write to stream failed with error: %s" % str(value)
        status.add_error(err_msg)
        ret_val = process_status.ERR_write_stream

    return ret_val


# =======================================================================================================================
# Stream IO - Get the source server IP and port
# =======================================================================================================================
def get_peer_name(io_stream):
    try:
        peer = str(io_stream._sock.getpeername())
        peer = peer.replace(",", ":")
    except:
        peer = "can't be determined"
    return peer


# =======================================================================================================================
# Stream IO - Get the current server IP and port
# =======================================================================================================================
def get_sock_name(io_stream):
    try:
        sockname = str(io_stream._sock.getsockname())
        sockname = sockname.replace(",", ":")
    except:
        sockname = "can't be determined"
    return sockname


# =======================================================================================================================
# Stream IO - Test socket
# =======================================================================================================================
def test_stream_socket(io_stream):
    try:
        peer = io_stream._sock.getpeername()
        ret_value = True
    except:
        ret_value = False
    return ret_value


# =======================================================================================================================
# Rename file
# =======================================================================================================================
def rename_file(status, src_name, dest_name):

    separator = params.get_path_separator()
    src = get_path(src_name)
    if src == "":
        status.add_error("Rename failed: wrong source file name: %s" % src_name)
        return False

    destination = dest_name.rsplit(separator, 1)
    if destination[0] != dest_name:
        # with directory - check that the directory exists
        directory = get_path(destination[0])
        if directory == "":
            status.add_error("Rename failed: wrong destination directory: %s" % directory)
            return False
        dest = directory + separator + destination[1]
    else:
        dest = destination[0]

    if src != dest:
        try:
            os.rename(src, dest)
        except OSError as e:
            status.add_error("Rename failed: %s" % str(e))
            ret_val = False
        else:
            ret_val = True
    else:
        ret_val = True

    return ret_val


# =======================================================================================================================
# Get path
# =======================================================================================================================
def get_path(provided_path):
    try:
        os_path = os.path.expanduser(os.path.expandvars(provided_path))
    except OSError as e:
        os_path = ""
    return os_path

# =======================================================================================================================
# Get Hash Value of a file
# if lock_key is provided, the file is locked until hash is calculated
# If prefix_data is provided, it is added to the hash - it is used to add a dbms name and a table name to the hash value
# =======================================================================================================================
def get_hash_value(status, file_name, lock_key, prefix_data):
    BLOCK_SIZE = 65536  # The size of each read from the file

    file_name = os.path.expanduser(os.path.expandvars(file_name))
    # file_hash = hashlib.sha256()  # Create the hash object, can use something other than `.sha256()` if you wish
    file_hash = hashlib.md5()

    if lock_key:
        read_lock(lock_key)

    if prefix_data:
        # Prefixed data can be dbms name and table name that are considered in the hash
        file_hash.update(prefix_data.encode())  # Update the hash with the prefix data+

    try:
        with open(file_name, 'rb') as f:
            fb = f.read(BLOCK_SIZE)  # Read from the file. Take in the amount declared above
            while len(fb) > 0:  # While there is still data being read from the file
                file_hash.update(fb)  # Update the hash
                fb = f.read(BLOCK_SIZE)  # Read the next block from the file
    except (IOError, EOFError) as e:
        status.add_warning("Failed to read a file to calculate hash value Error: [%s] File: %s" % (str(e), file_name))
        ret_val = False  # return an empty list
        hash_value = '0'
    else:
        ret_val = True
        hash_value = file_hash.hexdigest()  # Get the hexadecimal digest of the hash

    if lock_key:
        read_unlock(lock_key)

    return [ret_val, hash_value]
# =======================================================================================================================
# Given a file name:
# test that the directory exists
# test that the directory is writeable
# change the type in the file name
# =======================================================================================================================
def get_writeable_file_name(status, path_file_name, new_type):
    dir_name, file_name, file_type = extract_path_name_type(path_file_name)

    if not dir_name or not file_name or not file_type:
        status.add_error("Failed to extract directory, file name and type from name provided: " + path_file_name)
        ret_val = process_status.Failed_to_extract_file_components
        reply_list = [ret_val, ""]
        return reply_list

    # test directory exists
    ret_val = test_dir_exists(status, dir_name, True)
    if ret_val:
        reply_list = [ret_val, ""]
        return reply_list

    # test directory is writeable
    ret_val = test_dir_writeable(status, dir_name, True)
    if ret_val:
        reply_list = [ret_val, ""]
        return reply_list

    new_file = dir_name + file_name + "." + new_type

    reply_list = [process_status.SUCCESS, new_file]
    return reply_list

# =======================================================================================================================
# Lock a file - write
# =======================================================================================================================
def write_lock(file_name):
    file_locker[file_name].acquire_write()
# =======================================================================================================================
# UnLock a file - write
# =======================================================================================================================
def write_unlock(file_name):
    file_locker[file_name].release_write()
# =======================================================================================================================
# Lock a file - read
# =======================================================================================================================
def read_lock(file_name):
    file_locker[file_name].acquire_read()
# =======================================================================================================================
# UnLock a file - read
# =======================================================================================================================
def read_unlock(file_name):
    file_locker[file_name].release_read()

# =======================================================================================================================
# Get the dbms name and the table name from the file name
# negative numbers pull from the end
# file_sections is a list with section ID for dbms_name, table_name etc..
# Default file struct: [dbms name].[table name].[data source].[hash value].[instructions].[TSD member].[TSD ID].[TSD date]
# =======================================================================================================================
def query_file_name(path_file:str, name_sections:list):

    dbms_section = name_sections[0]
    table_section = name_sections[1]
    source_section = name_sections[2]
    hash_section = name_sections[3]
    instruct_section = name_sections[4]
    if len(name_sections) >= 8:
        tsd_member_section = name_sections[5]
        tsd_id_section = name_sections[6]
        tsd_date_section = name_sections[7]
    else:
        tsd_member_section   = None
        tsd_id_section = None
        tsd_date_section = None


    path, file_name, file_type = extract_path_name_type(path_file)
    dbms_name = ""
    table_name = ""
    source = "0"  # The ID of the device generated the data
    instructions = "0"  # instructions to the mapping of JSON attributes to Table columns
    hash_value = "0"
    tsd_member = None
    tsd_row_id = None
    tsd_date = None

    if file_name:
        file_sections = file_name.split('.')
        sections_count = len(file_sections)

        if dbms_section >= 0:  # count from first section
            if dbms_section < sections_count:
                dbms_name = file_sections[dbms_section].lower()
        else:  # count from end, -1 is the last
            dbms_section = abs(dbms_section)
            if dbms_section <= sections_count:
                dbms_name = file_sections[sections_count - dbms_section].lower()

        if table_section >= 0:
            if table_section < sections_count:
                table_name = file_sections[table_section].lower()
        else:
            table_section = abs(table_section)
            if table_section <= sections_count:
                table_name = file_sections[sections_count - table_section].lower()

        if source_section:  # The id of the device generating the data
            if source_section >= 0:
                if source_section < sections_count:
                    source = file_sections[source_section].lower()
            else:
                source_section = abs(source_section)
                if source_section <= sections_count:
                    source = file_sections[sections_count - source_section].lower()

        if hash_section:  # The id of the device generating the data
            if hash_section >= 0:
                if hash_section < sections_count:
                    hash_value = file_sections[hash_section].lower()
            else:
                hash_section = abs(hash_section)
                if hash_section <= sections_count:
                    hash_value = file_sections[sections_count - hash_section].lower()

        if instruct_section:  # The id of the device generating the data
            if instruct_section >= 0:
                if instruct_section < sections_count:
                    instructions = file_sections[instruct_section].lower()
            else:
                instruct_section = abs(instruct_section)
                if instruct_section <= sections_count:
                    instructions = file_sections[sections_count - instruct_section].lower()

        if tsd_member_section:  # The id of the member in the cluster
            if tsd_member_section >= 0:
                if tsd_member_section < sections_count:
                    tsd_member = file_sections[tsd_member_section].lower()
            else:
                tsd_member_section = abs(tsd_member_section)
                if tsd_member_section <= sections_count:
                    tsd_member = file_sections[sections_count - tsd_member_section].lower()

        if tsd_id_section:  # The row id in the tsd table
            if tsd_id_section >= 0:
                if tsd_id_section < sections_count:
                    tsd_row_id = file_sections[tsd_id_section].lower()
            else:
                tsd_id_section = abs(tsd_id_section)
                if tsd_id_section <= sections_count:
                    tsd_row_id = file_sections[sections_count - tsd_id_section].lower()

        if tsd_date_section:  # The row id in the tsd table
            if tsd_date_section >= 0:
                if tsd_date_section < sections_count:
                    tsd_date = file_sections[tsd_date_section].lower()
            else:
                tsd_date_section = abs(tsd_date_section)
                if tsd_date_section <= sections_count:
                    tsd_date = file_sections[sections_count - tsd_date_section].lower()

    reply_list = [dbms_name, table_name, source, hash_value, instructions, tsd_member, tsd_row_id, tsd_date]
    return reply_list


# =======================================================================================================================
# Get a section from a file name
# =======================================================================================================================
def get_section_name(path_file, section):
    section_name = ""
    path, file_name, file_type = extract_path_name_type(path_file)
    if file_name:
        file_sections = file_name.split('.')
        if section >= 0:  # count from first section
            if section < len(file_sections):
                section_name = file_sections[section].lower()
        else:  # count from end, -1 is the last
            section_id = abs(section)
            if section_id <= len(file_sections):
                section_name = file_sections[len(file_sections) - section_id].lower()
    return section_name


# =======================================================================================================================
# Return a file name with path as follows:
# 1) If the file_name includes path - return the provided file and path
# 2) If the file_name does not includes path - use the path of the file_with_path
# =======================================================================================================================
def add_path_to_file_name(file_name, file_with_path):
    separator = params.get_path_separator()

    if file_name.find(separator) != -1:
        # file include path
        new_file_name = file_name
    else:
        index = file_with_path.rfind(separator)
        if index != -1:
            new_file_name = file_with_path[:index + 1] + file_name
        else:
            new_file_name = file_name
    return new_file_name


# ==================================================================
# Copy multiple files on the same machine
# ==================================================================
def operate_multiple_files(status, operation, source_path_file, destination):
    '''
    source_path_file - a string representative of the path and files to compress or decompress
    destination - destination directory
    '''
    ret_val = process_status.SUCCESS
    source_files = params.get_value_if_available(source_path_file)
    src_dir, src_name, src_type = extract_path_name_type(source_files)
    name_prefix, type_prefix = get_name_type_prefix(src_name, src_type)

    if destination:
        dest_dir = params.get_value_if_available(destination)
        if not params.is_directory(dest_dir):
            status.add_error("Destination directory is missing a separator (%s) at the end of the directory name" % params.get_path_separator())
            ret_val = process_status.Err_dir_name
    else:
        dest_dir = None

    if not ret_val:
        files_list = []
        ret_val = search_files_in_dir(status, files_list, src_dir, name_prefix, type_prefix, False, False)
        if not ret_val:
            if not len(files_list):
                ret_val = process_status.Files_not_available_in_dir
            else:
                for file_name in files_list:
                    file_source = src_dir + file_name
                    if dest_dir:
                        file_dest = dest_dir + file_name
                    else:
                        file_dest = None

                    if operation == "compress":
                        if file_dest and (file_source == file_dest or not file_dest.endswith(".gz")):
                            file_dest += ".gz"
                        ret_val = compress(status, file_source, file_dest)
                    elif operation == "decompress":
                        if file_dest and file_dest.endswith(".gz"):
                            file_dest = file_dest[:-3]
                        ret_val = decompress(status, file_source, file_dest)
                    elif operation == "encode":
                        ret_val = encode(status, file_source, file_dest)
                    elif operation == "decode":
                        ret_val = decode(status, file_source, file_dest)

                    if ret_val:
                        break

    return ret_val


# =======================================================================================================================
# Zip s a file
# =======================================================================================================================
def compress(status, file_name_in, file_name_out=None):
    file_in = os.path.expanduser(os.path.expandvars(file_name_in))

    if not file_name_out:
        file_out = file_in + ".gz"
    else:
        file_name = add_path_to_file_name(file_name_out, file_name_in)
        file_out = os.path.expanduser(os.path.expandvars(file_name))

    try:
        with open(file_in, 'rb') as f_in:
            with gzip.open(file_out, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        ret_val = process_status.SUCCESS
    except:
        status.add_error("Failed to compress file: %s" % file_in)
        ret_val = process_status.Compression_failed

    return ret_val

# =======================================================================================================================
# UnZip s a file
# =======================================================================================================================
def decompress(status, file_name_in, file_name_out=None):
    file_in = os.path.expanduser(os.path.expandvars(file_name_in))

    if not file_name_out:
        if file_in.endswith(".gz"):
            file_out = file_in[:-3]     # remove the ".gz" type
        else:
            file_out = file_in + ".dat"
    else:
        file_name = add_path_to_file_name(file_name_out, file_name_in)
        file_out = os.path.expanduser(os.path.expandvars(file_name))

    try:
        with open(file_out, 'wb') as f_out:
            with open(file_in, 'rb') as f_in:
                f_out.write(gzip.decompress(f_in.read()))
        ret_val = process_status.SUCCESS
    except:
        errno, value = sys.exc_info()[:2]
        status.add_error("Failed to decompress file: '%s' with error %s : %s" % (file_in, str(errno), str(value)))
        ret_val = process_status.Decompression_failed

    return ret_val

# =======================================================================================================================
# Base64 encoding - exmple usage at https://www.educative.io/answers/how-to-use-base64b64encode-in-python
# =======================================================================================================================
def encode(status, file_name_in, file_name_out=None):
    file_in = os.path.expanduser(os.path.expandvars(file_name_in))

    if not file_name_out:
        file_out = file_in + ".msg"
    else:
        file_name = add_path_to_file_name(file_name_out, file_name_in)
        file_out = os.path.expanduser(os.path.expandvars(file_name))

    try:
        with open(file_out, 'wb') as f_out:
            with open(file_in, 'rb') as f_in:
                bytes_read = f_in.read()
                # ecode in base 64
                encoded = base64.b64encode(bytes_read)
                f_out.write(encoded)
        ret_val = process_status.SUCCESS

    except:
        status.add_error("Failed to encode file: %s" % file_in)
        ret_val = process_status.encoding_failed

    return ret_val

# =======================================================================================================================
# Base64 decode
# =======================================================================================================================
def decode(status, file_name_in, file_name_out=None):
    file_in = os.path.expanduser(os.path.expandvars(file_name_in))

    if not file_name_out:
            file_out = file_in + ".png"
    else:
        file_name = add_path_to_file_name(file_name_out, file_name_in)
        file_out = os.path.expanduser(os.path.expandvars(file_name))

    try:
        with open(file_out, 'wb') as f_out:
            with open(file_in, 'r') as f_in:
                bytes_read = f_in.read()
                base64_bytes = bytes_read.encode('ascii')
                blobs_info = base64.b64decode(base64_bytes)
                f_out.write(blobs_info)
        ret_val = process_status.SUCCESS
    except:
        errno, value = sys.exc_info()[:2]
        status.add_error("Failed to decompress file: '%s' with error %s : %s" % (file_in, str(errno), str(value)))
        ret_val = process_status.Decoding_failed

    return ret_val

# =======================================================================================================================
# Change a directory name to a file
# =======================================================================================================================
def change_dir(file_path_name, new_dir):
    separator = params.get_path_separator()

    index = file_path_name.rfind(separator)
    if index == -1:
        new = new_dir + file_path_name
    else:
        new = new_dir + file_path_name[index + 1:]

    return new


# ----------------------------------------------------------
# Move file to the error directory
# If make_unique is true, add a string that makes the file name unique
# ----------------------------------------------------------
def file_to_dir(status, dir_name, path_file_name, extend_name, make_unique):

    separator = params.get_path_separator()
    file_name, file_type = extract_name_type(path_file_name)

    if extend_name:
        fname = file_name + "." + extend_name
    else:
        fname = file_name

    if dir_name[-1] != '/' and dir_name[-1] != '\\':
        dir_name += separator

    if make_unique:
        dest_name = dir_name + fname + "." + get_unique_time_string() + "." + file_type
    else:
        dest_name = dir_name + fname + "." + file_type

    ret_val = rename_file(status, path_file_name, dest_name)
    if not ret_val:
        status.add_error("Failed to move file '%s' to directory: '%s'" % (file_name, dir_name))
    return ret_val


# ----------------------------------------------------------
# Get a string based on seconds from 1970 that makes a filename unique
# ----------------------------------------------------------
def get_unique_time_string():
    global last_time_stamp

    current_time = int(time.time())
    if current_time <= last_time_stamp:
        last_time_stamp += 1
    else:
        last_time_stamp = current_time
    return str(last_time_stamp)


# ----------------------------------------------------------
# Manipulate file through an array of operations.
# Operations can be: delete, compress, move
# Add a unique timestamp such that move and compress do not fail on duplicate names
# ----------------------------------------------------------
def manipulate_file(status, file_path_name, is_compress, is_move, is_delete, dest_dir, extend_name, make_unique):
    dir_name, file_name, file_type = extract_path_name_type(file_path_name)

    if dest_dir:
        if dest_dir[-1] == '/' or dest_dir[-1] == '\\':
            dest_location = dest_dir
        else:
            dest_location = dest_dir + params.get_path_separator()
    else:
        dest_location = dir_name    # If dest_dir is not provided - use the dir of the source file

    if is_compress:
        if extend_name:
            key_str = "." + extend_name
        else:
            key_str = ''
        if make_unique:
            unique_str =  "." + get_unique_time_string()
        else:
            unique_str = ""

        zip_file = "%s%s.%s%s%s.gz" % (dest_location, file_name, file_type, key_str, unique_str)
        ret_val = compress(status, file_path_name, zip_file)
        if not ret_val:
            is_delete = True  # delete the source file as compression succeeded
            is_move = False
    else:
        ret_val = process_status.SUCCESS

    if is_move:
        if extend_name:
            key_str = "." + extend_name
        else:
            key_str = ''
        if make_unique:
            unique_str = "." + get_unique_time_string()
        else:
            unique_str = ""


        new_name = "%s%s%s%s.%s" % (dest_location, file_name, key_str, unique_str, file_type)
        ret_val = rename_file(status, file_path_name, new_name)
        if not ret_val:  # if move failed
            is_delete = False
            ret_val = process_status.Failed_to_rename_file
        else:
            ret_val = process_status.SUCCESS

    if is_delete:
        if not delete_file(file_path_name):
            ret_val = process_status.Failed_to_delete_file

    return ret_val
# =======================================================================================================================
# 1) Uncompress the file
# 2) delete the compressed copy
# 3) Rename the uncompressed file to new_name
# If process failes, files are moved to err_dir
# =======================================================================================================================
def uncompress_del_rename(status, source_file_name, dest_file_name, err_dir):

    unzipped_file_name = source_file_name + ".uzp"
    ret_val = decompress(status, source_file_name, unzipped_file_name)     # The unzipped version
    if not ret_val:
        if delete_file(source_file_name):
            if not rename_file(status, unzipped_file_name, dest_file_name):
                # failed to change name to destination name
                ret_val = process_status.Failed_to_rename_file
                extend_name = "err_%u" % ret_val
                file_to_dir(status, err_dir, unzipped_file_name, extend_name, True)
        else:
            # failed to delete the compressed file
            ret_val = process_status.Failed_to_delete_file
            extend_name = "err_%u" % ret_val
            file_to_dir(status, err_dir, source_file_name, extend_name, True)
            file_to_dir(status, err_dir, unzipped_file_name, extend_name, True)
    else:
        # Failed to uncompress
        ret_val = process_status.Decompression_failed
        extend_name = "err_%u" % ret_val
        file_to_dir(status, err_dir, source_file_name, extend_name, True)

    return ret_val

# =======================================================================================================================
# Find the first file in the list with one of the provided file types
# Ignore files which are with non-valid names
# =======================================================================================================================
def get_valid_file(name_list: list, file_types: list, ignore_dict):
    valid_name = ""
    for file_name_type in name_list:
        file_name, file_type = extract_name_type(file_name_type)
        if not file_name:
            continue  # Missing name
        if ignore_dict and file_name_type in ignore_dict:
            continue                    # Ignore this file
        name_segments = file_name.split('.')
        if "" in name_segments:
            continue  # file name like ...json
        if file_types[0] == '*':
            valid_name = file_name_type
            break
        if file_type in file_types:
            valid_name = file_name_type
            break

    return valid_name

# =======================================================================================================================
# Given a list of files, return a list that satisfies the conditions
# Ignore files which are with non-valid names
# =======================================================================================================================
def update_file_list(name_list: list, file_types: list, ignore_dict, dir_path:str):
    updated_list = []       # New list that includes the file names

    for file_name_type in name_list:
        # Go over all files
        file_name, file_type = extract_name_type(file_name_type)
        if not file_name:
            continue  # Missing name
        if ignore_dict and file_name_type in ignore_dict:
            continue                    # Ignore this file
        name_segments = file_name.split('.')
        if "" in name_segments:
            continue  # file name like ...json
        if file_types[0] == '*':
            updated_list.append(dir_path + file_name_type)
        if file_type in file_types:
            updated_list.append(dir_path + file_name_type)

    return updated_list
# =======================================================================================================================
# Make a JSON file:
# 1) Organize file name by the standard
# 2) Calculate the hash value of the file and add to file name
# =======================================================================================================================
def make_json_file(status, source_file, dbms_name, table_name, source, hash_value, instructions):
    if not hash_value or hash_value == "0":
        got_hash, hash_val = get_hash_value(status, source_file, "", dbms_name + '.' + table_name)
        if not got_hash:
            status.add_error("Failed to access or calculate hash value for file: %s" % source_file)
            reply_list = [False, ""]
            return reply_list
    else:
        hash_val = hash_value

    file_path, file_name, file_type = extract_path_name_type(source_file)

    new_file = "%s%s.%s.%s.%s.%s.%s" % (file_path, dbms_name, table_name, source, hash_val, instructions, file_type)

    was_renamed = rename_file(status, source_file, new_file)

    reply_list = [was_renamed, new_file]
    return reply_list
# =======================================================================================================================
# Compare hash files
# Each file contains a list of hash values - the hash lists are sorted by hash value and the differences are returned to the caller method
# the occurences that appear in file1 and are missing from file2 are written to file_out
# =======================================================================================================================
def compare_hash_files(status, file1, file2, file_out):
    handle1 = IoHandle()
    handle2 = IoHandle()
    handle3 = IoHandle()

    if not handle1.open_file("get", file1):
        return False

    if not handle2.open_file("get", file2):
        handle1.close_file()
        return False

    # write the differences to file3
    if not handle3.open_file("append", file_out):
        handle1.close_file()
        handle2.close_file()
        return False

    ret_val = True
    line2 = ""
    while 1:
        line1 = handle1.read_one_line()
        if not line1:
            break

        while line1 > line2:
            line2 = handle2.read_one_line()
            if line2 == "":
                # no more data in file 2 -> put the rest from file1 to file3
                while 1:
                    line1 = handle1.read_one_line()
                    if line1 == "":
                        break
                    handle3.write_from_buffer(line1)
                break

        if line2 > line1:
            # line1 is missing from file2
            ret_val = handle3.write_from_buffer(line1)
            if not ret_val:
                status.add_error("Failed to write hash value into %s" % file_out)
                ret_val = False
                break

    handle1.close_file()
    handle2.close_file()
    handle3.close_file()

    return ret_val


# -----------------------------------------------------------------
# Given a directory, provide a list of all the files that satisfy the conditions
# -----------------------------------------------------------------
def get_files_from_dir(status: process_status, source_dir: str, hash_values, file_types):
    files_list = []
    filtered_list = []  # the list after applying the hash value or the file type lists
    if not directory_walk(status, files_list, source_dir, "file"):
        ret_val = process_status.Failed_to_read_files_from_dir
    else:
        ret_val = process_status.SUCCESS
        if hash_values or file_types:
            # go over the list and filter
            for file in files_list:
                if hash_values:
                    # split file to: dbms . table . source . hash . .... type
                    file_segments = file.split('.')
                    if len(file_segments) >= 4:
                        if not file_types or file_segments[-1] in file_types:
                            # take all file types or the file type is included in the file_types list
                            if file_segments[3] in hash_values:
                                filtered_list.append(file)
                else:
                    index = file.rfind('.')
                    if index > 0 and index < (len(file) - 1):
                        if file[index + 1:] in file_types:
                            filtered_list.append(file)
        else:
            filtered_list = files_list

    reply_list = [ret_val, filtered_list]
    return reply_list

# -----------------------------------------------------------------
# Given a directory, search for the files by file name prefix and type prefix
# for example, all files with ab*.cd*
# -----------------------------------------------------------------
def search_files_in_dir(status: process_status, filtered_list:list, source_dir: str, name_prefix:str, type_prefix:str, inc_path:bool, inc_sub_dir:bool):
    '''
    status - thread process status
    filtered_list - list updated with file names
    source_dir - path to folder to search
    name_prefix - any file name prefixed with asteristik
    type_prefix - any file type prefixed with asteristik
    inc_path - include path in returned list
    inc_sub_dir - search in sub-directories

    return ret_val + list of files
    '''

    files_list = []
    if not directory_walk(status, files_list, source_dir, "file"):
        ret_val = process_status.Failed_to_read_files_from_dir
    else:
        ret_val = process_status.SUCCESS
        if name_prefix or type_prefix:
            # go over the list and filter
            if name_prefix:
                len_name_prefix = len(name_prefix)
            if type_prefix:
                len_type_prefix = len(type_prefix)

            for file in files_list:
                file_segments = file.rsplit('.', 1)     # split the name from the type
                file_name = file_segments[0]
                if len(file_segments) == 2:
                    file_type = file_segments[1]
                else:
                    # no file type
                    file_type = None

                if name_prefix:
                    if file_name[:len_name_prefix] != name_prefix:
                        continue        # name does not match
                if type_prefix:
                    if not file_type or file_type[:len_type_prefix] != type_prefix:
                        continue  # type does not match

                if inc_path:
                    # Add path
                    filtered_list.append(source_dir + file)
                else:
                    filtered_list.append(file)
        else:

            for file in files_list:
                if inc_path:
                # Add path
                    filtered_list.append(source_dir + file)
                else:
                    filtered_list.append(file)

    if inc_sub_dir and not ret_val:
        # Get from sub-directories
        files_list = []
        if not directory_walk(status, files_list, source_dir, "dir"):
            ret_val = process_status.Failed_to_read_files_from_dir
        else:
            ret_val = process_status.SUCCESS
        for folder in files_list:
            ret_val = search_files_in_dir(status, filtered_list, source_dir + folder + params.get_path_separator(), name_prefix, type_prefix, inc_path, inc_sub_dir)


    return ret_val

# -----------------------------------------------------------------
# Given a file name and a file type - return the prefix name and type
# For example: abc* jhi* -->  abd, jhi
# -----------------------------------------------------------------
def get_name_type_prefix(file_name, file_type):

    index_name = file_name.find('*')
    if index_name != -1:
        name_prefix = file_name[:index_name]  # Copy all files that share the prefix
    else:
        name_prefix = file_name

    if file_type:
        index_type = file_type.find('*')
        if index_type != -1:
            type_prefix = file_type[:index_type]  # Copy all files that share the prefix
        else:
            type_prefix = file_type
    else:
        type_prefix = ""

    reply_list = [name_prefix, type_prefix]
    return reply_list
# -----------------------------------------------------------------
# Make path and file name.
# test that the directory is valid
# -----------------------------------------------------------------
def make_path_file_name(status, dir_name, file_name, file_type):
    ret_value = test_dir_exists(status, dir_name, True)
    if not ret_value:
        if dir_name[-1] == '/' or dir_name[-1] == '\\':
            dest_location = dir_name
        else:
            dest_location = dir_name + params.get_path_separator()

        if file_type:
            pf_name = dest_location + file_name + '.' + file_type
        else:
            pf_name = dest_location + file_name
    else:
        pf_name = ""

    reply_list = [ret_value, pf_name]
    return reply_list

# -----------------------------------------------------------------
# Append a row to a file
# -----------------------------------------------------------------
def append_data_to_file(status, file_name, message_name, data):
    ret_val = True
    io_handle = IoHandle()
    if not io_handle.open_file("append", file_name):
        status.add_error("Failed to open %s file: %s" % (message_name, file_name))
        ret_val = False
    elif not io_handle.append_data(data + "\n"):
        status.add_error("Failed to write to %s file: %s" % (message_name, file_name))
        ret_val = False
    elif not io_handle.close_file():
        status.add_error("Failed to close %s file: %s" % (message_name, file_name))
        ret_val = False

    return ret_val
# -----------------------------------------------------------------
# Get the time a file was modified
# -----------------------------------------------------------------
def get_file_modified_time(file_name):
    try:
        file_date = os.path.getmtime(file_name)     # Float value (seconds from epoc)
    except:
        file_date = 0

    return file_date
# -----------------------------------------------------------------
# Get the time a file was modified - as a key: YYMMDDHHMMSS
# -----------------------------------------------------------------
def get_file_modified_time_key(file_name):

    file_time = get_file_modified_time(file_name)
    if file_time:
        utc_time = seconds_to_date(file_time, format_string="%Y-%m-%dT%H:%M:%S.%fZ")
        if utc_time:
            # Example string: '2019-07-29T19:34:00.123Z'
            date_time = utc_timestamp_to_key(utc_time)
        else:
            date_time = "0"
    else:
        date_time = "0"

    return date_time
# -----------------------------------------------------------------
# timetsamp %Y-%m-%dT%H:%M:%S.%fZ" to date key: YYMMDDHHMMSS
# -----------------------------------------------------------------
def utc_timestamp_to_key(utc_time):
    if len(utc_time) >= 19:
        date_key = utc_time[2:4] + utc_time[5:7] + utc_time[8:10] + utc_time[11:13] + utc_time[14:16] + utc_time[17:19]
    else:
        date_key = 0
    return date_key
# -----------------------------------------------------------------
# File date key: YYMMDDHHMMSS to %Y-%m-%dT%H:%M:%S.%fZ"
# -----------------------------------------------------------------
def key_to_utc_timestamp(key_date):

    if len(key_date) < 12:
        date_time = "0"
    else:
        date_time = "20%s-%s-%sT%s:%s:%s.0" % (key_date[0:2],key_date[2:4],key_date[4:6],key_date[6:8],key_date[8:10],key_date[10:12])
    return date_time
# -----------------------------------------------------------------
# Move file to archival
# The file type determine the archival location
# If file type is not on the mame string, return an error.
# The field before the type is the processing time in the format: YYMMDDHHMMSS (12 digits)
# If it is not found - use file creation day and time
# Example file name: [dbms name].[table name].[data source].[hash value].[instructions].[YYMMDDHHMMSS].json
#
# The archival directory:
# Archive -> year -> month -> day -> JSON file (Source Data)
# Archive -> year -> partition type (ie. 2 weeks) -> Member ID -> backup file (Partition Data)
# -----------------------------------------------------------------
def archive_file(status, file_type, archive_dir, err_dir, file_name, compress_file, archive_date = None):

    if not archive_date:
        ret_val, archive_date = get_archive_date(status, file_type, file_name)
    else:
        ret_val = process_status.SUCCESS        # The archive date is provided by the caller

    separator = params.get_path_separator()

    if not archive_dir:
        status.add_error("Archiving failed, archive directory is not available")
        ret_val = process_status.File_move_failed

    if not ret_val:
        # test the year subdir or create the directory
        if archive_dir[-1] == "/" or archive_dir[-1] == "\\":
            dir_name =  archive_dir + archive_date[0:2]    # represent the year
        else:
            dir_name = archive_dir + separator + archive_date[0:2]    # represent the year
        ret_val = test_create_dir(status, dir_name)
        if not ret_val:
            if file_type == "json" or file_type == '*':
                dir_name += (separator + archive_date[2:4])       # represent the month
                ret_val = test_create_dir(status, dir_name)
                if not ret_val:
                    dir_name += (separator + archive_date[4:6])       # represent the day
                    ret_val = test_create_dir(status, dir_name)
                    if not ret_val:
                        if not compress_file:
                            # Move without compression
                            if not move_file(file_name, dir_name):
                                status.add_error("File move to archive failed: %s" % file_name)
                                if file_to_dir(status, err_dir, file_name, "err_%u" % process_status.Move_to_archive_failed, True):
                                    # Probably duplicate file name
                                    status.add_error("Archiving failed, file moved to err_dir: %s" % file_name)
                                else:
                                    ret_val = process_status.File_move_failed
                        else:
                            # compress to the archive dir and delete the non compressed file
                            ret_val = manipulate_file(status, file_name, True, True, True, dir_name, None, False)
    return ret_val
# -----------------------------------------------------------------
# Return full path to the file based on the file name and type
# The archival directory:
# Archive -> year -> month -> day -> JSON file (Source Data)
# Archive -> year -> partition type (ie. 2 weeks) -> Member ID -> backup file (Partition Data)
# -----------------------------------------------------------------
def get_archival_file_dir(status, file_type, archive_dir, file_name):

    ret_val = process_status.Failed_to_extract_file_components
    file_path = ""

    file_segments = file_name.rsplit('.',2)
    if len(file_segments) != 3:
        status.add_error("JSON file name structure is incorrect: %s" % file_name)
    else:
        if file_type == "json":
            date_str = file_segments[1]
            if len(date_str) != 12:     # njeeds to be YYMMDDHHMMSS
                status.add_error("Wrond Date in JSON file name: %s" % date_str)
            else:
                separator = params.get_path_separator()
                sub_dirs = date_str[:2] + separator + date_str[2:4] + separator + date_str[4:6]
                file_path = archive_dir + separator + sub_dirs
                ret_val = process_status.SUCCESS

    reply_list = [ret_val, file_path]
    return reply_list
# -----------------------------------------------------------------
# Create directory if does not exists
# -----------------------------------------------------------------
def test_create_dir(status, dir_name):

    ret_val = process_status.SUCCESS
    if test_dir_exists(status, dir_name, False) == process_status.ERR_dir_does_not_exists:
        if not create_dir(status, dir_name):
            status.add_error("Failed to create archival directory: %s" % dir_name)
            ret_val = process_status.Failed_to_create_dir

    return ret_val
# -----------------------------------------------------------------
# Return a string representing the time that the file was processed:
# If file type is not on the mame string, return an error.
# The field before the type is the processing time in the format: YYMMDDHHMMSS (12 digits)
# If it is not found - use file creation day and time
# Example file name: [dbms name].[table name].[data source].[hash value].[instructions].[YYMMDDHHMMSS].json
# -----------------------------------------------------------------
def get_archive_date(status, file_type, file_name):

    ret_val = process_status.SUCCESS
    date_time = ""

    if file_type == '*':
        index = file_name.rfind(".")
        if index > 0 and len(file_name) > (index + 1):
            file_type = file_name[index + 1:]   # Get the file type
    else:
        index = file_name.rfind("." + file_type)

    if index > 0 and ((index + len(file_type) + 1) == len(file_name) or file_name[index + len(file_type) + 1] == '.'):
        # test for: abc.json or abc.json.jz
        with_time = False
        if index > 12:
            # test for the date and time string
            date_time = file_name[index - 12:index]
            if date_time.isdecimal():
                month = int(date_time[2:4])
                day = int(date_time[4:6])
                hour = int(date_time[6:8])
                minute = int(date_time[8:10])
                second = int(date_time[10:12])
                if month >= 1 and month <= 12 and day >=1 and day <= 31 and hour <=24 and minute <= 60 and second <= 60:
                    with_time = True        # Use the time on the file name string

        if not with_time:
            # Use the file time
            date_time = get_file_modified_time_key(file_name)
            if date_time == "0":
                status.add_error("Unable to retrieve time associated to the file: %s" % file_name)
                ret_val = process_status.File_time_not_accessible
    else:
        status.add_error("Archived file is not using the correct file type")
        ret_val = process_status.Wrong_file_type

    reply_list = [ret_val, date_time]
    return reply_list

# ------------------------------------------
# Return info on the get disk info command
# Command: get disk [usage] d:
# ------------------------------------------
def get_disk_info(status, io_buff_in, words_array, trace):

    if words_array[1] == '=':
        offset = 2
    else:
        offset = 0

    ret_val = process_status.SUCCESS
    path = params.get_value_if_available(words_array[3 + offset])
    disk_stat = get_disk_stat(path)
    if disk_stat:
        info_type = words_array[2 + offset]
        if info_type == "usage":
            if status.get_active_job_handle().is_rest_caller():
                ip = params.get_value_if_available("!ip")
                reply = "[{'node':'%s', 'total':'%s','used':'%s','free':'%s'}]" % (
                ip, "{:,d}".format(disk_stat.total), "{:,d}".format(disk_stat.used), "{:,d}".format(disk_stat.free))
            else:
                reply = str(disk_stat)
        elif info_type == "free":
            reply = str(disk_stat.free)
        elif info_type == "total":
            reply = str(disk_stat.total)
        elif info_type == "used":
            reply = str(disk_stat.used)
        elif info_type == "percentage":
            if disk_stat.total:
                reply = "{:.2f}".format(disk_stat.free / disk_stat.total * 100)
            else:
                reply = ""
                status.add_error("Failed to retrieve disk space from storage device")
                ret_val = process_status.Wrong_path
        else:
            reply = ""
            ret_val = process_status.ERR_command_struct
    else:
        reply = ""
        ret_val = process_status.Wrong_path

    reply_list = [ret_val, reply]
    return reply_list

# ------------------------------------------
# Get the list of objects (files or directories) from a directory
# object_type is "file" or "dir"
# ------------------------------------------
def get_from_dir(status, object_type, directory_name):

    info_string = ""
    dir_path = params.get_directory(directory_name)

    if not test_dir_exists(status, dir_path, False):

        name_list = []
        if not directory_walk(status, name_list, dir_path, object_type):    # object_type is "file" or "dir"
            info_string = "Failed to retrieve files from '%s'" % dir_path
        else:
            if not len(name_list):
                if object_type == "dir":
                    info_string = "No sub directories with path provided: '%s'" % dir_path
                else:
                    info_string = "No files with path provided: '%s'" % dir_path
            else:
                for entry in name_list:
                    info_string += ("\r\n" + dir_path + entry)
    else:
        info_string = "Directory does not exists or not accessible: '%s'" % dir_path

    return info_string

# ------------------------------------------
# Write to a file by the following process:
# Write to a .new file
# Delete .old file
# Move existing file to .old
# move .new to the file written
# ------------------------------------------
def write_protected_file(status, f_name, data):

    ret_val = process_status.SUCCESS
    path, file_name, file_type = extract_path_name_type(f_name)

    # write to .new file

    file_name_new = "%s/%s.new" % (path, file_name)

    try:
        with open(file_name_new, "wb") as key_file:
            key_file.write(data)
    except:
        status.add_error("Failed to write to file: %s" % file_name_new)
        ret_val = process_status.File_write_failed
    else:
        # delete old file
        file_name_old = "%s/%s.old" % (path, file_name)
        delete_file(file_name_old)

        # Move existing file to old
        if is_path_exists(f_name):
            was_renamed = rename_file(status, f_name, file_name_old)
            if not was_renamed:
                status.add_error("Failed to rename %s to %s" % (file_name, file_name_old))
                ret_val = process_status.Failed_to_rename_file

        if not ret_val:

            # delete existing
            delete_file(f_name)

            # move new to filename
            was_renamed = rename_file(status, file_name_new, f_name)
            if not was_renamed:
                status.add_error("Failed to rename %s to %s" % (file_name_new, file_name))
                ret_val = process_status.Failed_to_rename_file

    return ret_val
# ------------------------------------------
# Open the file in binary mode and return content
# ------------------------------------------
def read_protected_file(status, file_name):

    try:
        with open(file_name, "rb") as key_file:
            data = key_file.read()
    except:
        data = None
        ret_val = process_status.File_read_failed
    else:
        ret_val = process_status.SUCCESS

    reply_list = [ret_val, data]
    return reply_list

# ------------------------------------------
# Change the type on the name of a file
# ------------------------------------------
def change_name_type(file_name, new_type):

    index = file_name.rfind('.')    # Find offset to old type
    if index == -1:
        # No type for file name - add the type
        new_name = file_name + "." + new_type
    else:
        new_name = file_name[:index + 1] + new_type
    return new_name

# =======================================================================================================================
# Create the work directories
# These directories are created with the user command: "create work directories"
# =======================================================================================================================
def _create_anylog_dirs(status, io_buff_in, cmd_words, trace):

    directory_list = [
        "!archive_dir",
        "!bkup_dir",
        "!blockchain_dir",
        "!dbms_dir",
        "!distr_dir",
        "!id_dir",
        "!err_dir",
        "!prep_dir",
        "!watch_dir",
        "!pem_dir",
        "!test_dir",
        "!bwatch_dir",
        "!blobs_dir",
        "!tmp_dir",
    ]

    for directory in directory_list:
        message = None
        dir_name = params.get_value_if_available(directory)
        if not dir_name:
            message = "Missing file path declaration for '%s'" % directory
        else:
            if test_dir_exists(status, dir_name, True):
                if not create_dir(status, dir_name):
                    message = "Failed to create directory for '%s' at '%s'" % (directory, dir_name)

        if message:
            utils_print.output(message, True)

    return process_status.SUCCESS
# =======================================================================================================================
# Get the size of the directory and sub - directories
# Satisfy the command: get size !archive_dir
# =======================================================================================================================
def get_directory_size(status:process_status, dir_name:str, dir_list:list, is_root:bool):

    total = 0               # Disk space in bytes
    counter = 0             # Number of files
    ret_val = process_status.SUCCESS

    try:
        # print("[+] Getting the size of", directory)
        for entry in os.scandir(dir_name):
            if entry.is_file():
                # if it's a file, use stat() function
                counter += 1
                total += entry.stat().st_size
            elif entry.is_dir():
                # if it's a directory, recursively call this function
                ret_val, counter_files, dir_size = get_directory_size(status, entry.path, dir_list, False)
                if ret_val:
                    break
                dir_list.append((entry.path, format(counter_files, ","), format(dir_size, ",")))
                total += dir_size
                counter += counter_files
    except PermissionError:
        # if for whatever reason we can't open the folder, return 0
        status.add_error("Failed permissions at: %s" % dir_name)
        ret_val = process_status.ERR_process_failure
    except:
        errno, value = sys.exc_info()[:2]
        message = "Failed to retrieve directory size with error: %s: %s location: '%s'" % (str(errno), str(value), dir_name)
        status.add_error(message)
        ret_val = process_status.ERR_process_failure
    else:
        if is_root:
            dir_list.append((dir_name, format(counter, ","), format(total, ",")))

    reply_list = [ret_val, counter, total]
    return reply_list

# =======================================================================================================================
# Get the list of files in the archive
# Satisfy the command: get archived files 2021-02-21
# =======================================================================================================================
def get_archived(status, io_buff_in, cmd_words, trace):

    date_str = params.get_value_if_available(cmd_words[3])
    if len(date_str) == 10:
        year_str = date_str[2:4]
        month_str = date_str[5:7]
        day_str = date_str[8:10]

        separator = params.get_path_separator()
        archive_dir = params.get_directory("!archive_dir")
        path_name = "%s%s%s%s%s%s" % (archive_dir, year_str, separator, month_str, separator, day_str)

        reply = get_from_dir(status, "file", path_name)
        ret_val = process_status.SUCCESS
    else:
        reply = None
        status.add_error("The date provided '%s' or the value assigned to 'archive_dir' do not address archived data" % date_str)
        ret_val = process_status.Wrong_path

    reply_list = [ret_val, reply]
    return reply_list
# =======================================================================================================================
# Compare 2 files
# If files are not identical or with an error - return false
# =======================================================================================================================
def file_compare(file_name1, file_name2):

    f_name1= os.path.expanduser(os.path.expandvars(file_name1))
    f_name2 = os.path.expanduser(os.path.expandvars(file_name2))

    try:
        with open(f_name1, 'r') as file1:
            with open(f_name2, 'r') as file2:
                difference = set(file1).difference(file2)

        difference.discard('\n')

    except:
        ret_val = False
    else:
        ret_val = (len(difference) == 0)


    return ret_val

# =======================================================================================================================
# Analyze the structure of 2 test files.
# The structure of each file is with 3 sections:
# Info - which is not compared
# Data - which is compared line by line
# Footer - which is optionally compared
# =======================================================================================================================
def analyze_file(status, trusted, file_name, options):
    '''
    status - the thread status object
    trusted - the file whose contents are trusted
    file_name - the file to compare to the trusted
    option - compare options - i.e compare execution time

    return - the line of difference (in the data section),
                -1 - if error,
                -2 - if time is enabled in options and validated is slower
                0 - if equal and not slower
            The compare result message

    The compare result message is a JSON string with the following:
    Result: "Passed" or "Failed" or "Slower"
    Title: The title on the trusted file
    Reason: "Failed to open file" or "Failed to open trusted" or "Failed compare in line X"
    Diff: - the difference
    File: - the tested file path and name
    trusted - the trusted file path and name
    '''

    compare_result = 0
    validated_line = 0
    run_time_trusted = ""
    run_time_validated = ""

    handle1 = IoHandle()
    handle2 = IoHandle()

    title = ""
    compare_info = "{\"result\" : \"%s\", " \
                      "\"Title\" : \"%s\", " \
                      "\"Reason\" : \"%s\", " \
                      "\"File\" : \"%s\", " \
                      "\"Trusted\" : \"%s\"}"

    if not handle1.open_file("get", trusted):
        compare_message = compare_info % ("Failed", title, "Failed to open file", file_name, trusted)
        reply_list = [-1, compare_message]
        return reply_list

    if not handle2.open_file("get", file_name):
        compare_message = compare_info % ("Failed", title, "Failed to open trusted", file_name, trusted)
        handle1.close_file()
        reply_list = [-1, compare_message]
        return reply_list

    trusted_section = 0             # Section 0 is the header Comparison is on section 1
    validated_section = 0
    section2_line = 0                # Line number of section 2
    # Read and compare section 1 and some info from section 2
    while 1:

        if trusted_section <= validated_section:
            row_trusted = handle1.read_one_line()
            if row_trusted.startswith("======================="):
                trusted_section += 1
                continue
            if trusted_section == 1 and row_trusted.startswith("Title:"):
                title = row_trusted[6:].strip()

        if validated_section <= trusted_section:
            validated_line += 1
            row_validated = handle2.read_one_line()
            if row_validated.startswith("======================="):
                validated_section += 1
                continue

        if validated_section != trusted_section or not validated_section:
            continue        # get to section 2

        if not row_trusted or not row_validated:
            break       # at least one file has no more data to read

        if validated_section == 2:
            # Data section to compare
            section2_line += 1
            if row_trusted != row_validated:
                compare_result = section2_line
                reason = analyze_row_diff(row_trusted, row_validated, section2_line)
                compare_message = compare_info % ("Failed", title, reason, file_name, trusted)
                break   # Comparison failed
        elif validated_section == 3:
            # Info section to compare
            if row_trusted.startswith("Run Time:"):
                run_time_trusted = row_trusted[10:]
            if row_validated.startswith("Run Time:"):
                run_time_validated = row_validated[10:]

    handle1.close_file()
    handle2.close_file()

    if not compare_result and options and "time" in options:
        # Compare the time
        if len (run_time_validated) > len (run_time_trusted) or run_time_validated > run_time_trusted:
            reason = "[Source: %s] > [File: %s]" % (run_time_trusted, run_time_validated)
            compare_message = compare_info % ("Slower", title, reason, file_name, trusted)
            compare_result = -2     # Slower run

    if not compare_result:
        compare_message = compare_info % ("Passed", title, "", file_name, trusted)

    reply_list = [compare_result, compare_message]
    return reply_list

# =======================================================================================================================
# Analyze the difference in a row and return a message with the diff info
# =======================================================================================================================
def analyze_row_diff(row_trusted, row_validated, line_number):

    reason = ""
    trusted = str_to_json(row_trusted)
    if trusted:
        # Do a JSON compare of the key and values
        row = str_to_json(row_validated)
        if row:
            if row and trusted:
                # Compare the JSONs
                for key, value in trusted.items():
                    if key not in row:
                        reason = "The key '%s' in line %u is missing" % (key, line_number)
                        break
                    if row[key] != value:
                        reason = "The value for the key '%s' in line %u is different: '%s' vs. '%s" % (key, line_number, row[key], value)
                        break
            if not reason:
                for key, value in row.items():
                    if key not in trusted:
                        reason = "The key '%s' in line %u is not part of the source" % (key, line_number)
                        break

        else:
            reason = "Line %u is not in JSON format" % line_number
    else:
        # Output not in JSON - print the prefix  of each row
        if len(row_trusted) > 50:
            row_source = row_trusted[:50] + " ..."
        else:
            row_source = row_trusted
        if len(row_validated) > 50:
            row_tested = row_validated[:50] + " ..."
        else:
            row_tested = row_validated

        for index, char in enumerate(row_trusted):
            if char != row_validated[index]:
                reason = "Difference in line %u and offset %u: [%s] [%s]" % (line_number, index + 1, row_tested, row_source)
                break

        if not reason:
            if len(row_trusted) != len(row_validated):
                reason = "Difference in output length in line %u: [%s] [%s]" % (line_number, row_tested, row_source)

    return reason

# =======================================================================================================================
# Return the info from the header of a test file
# =======================================================================================================================
def get_header_info(status, file_name):
    '''
    status - the thread status object
    file_name - an output file of a test query

    return a list with the header info (as key-value pairs)
    '''

    handle = IoHandle()


    if not handle.open_file("get", file_name):
        err_message = "Failed to open file: [%s]" % file_name
        status.add_error(err_message)
        return None

    # Read data from header section
    header_info = {}
    section = 0
    while 1:
        row = handle.read_one_line()
        if row.startswith("======================="):
            section += 1
        if not section:
            continue
        if section > 1:
            break   # Header completed

        # header section
        index = row.find(':')
        if index > 0 and index < len(row):
            key = row[:index]
            value = row[index + 1:].strip()
            header_info[key] = value

    return header_info


# ==================================================================
# Read a sequential JSON strings
# Return a list object with a JSON Object (Dictionary) in each entry
# ==================================================================
def read_json_strings(status: process_status, json_file: str):
    """
    Convert JSON objects (in file) to dict objects
    :args:
       json_file:str - JSON file
    :param:
       json_data:list - list of converted data from file
    """
    json_data = []
    data = read_all_lines_in_file(status, json_file)
    if not data or len(data) == 0:
        status.add_error("JSON file without data: '%s'" % json_file)
        return json_data

    for line in data:
        if line != "":  # ignore empty lines
            updated_line = format_reading(status, line)  # Convert line JSON->dict and remove sub-dict objects
            if updated_line is not None:
                json_data.append(updated_line)

    if json_data is []:
        status.add_error("Failed to generate INSERT statement from JSON file: '%s'" % json_file)

    return json_data
# ==================================================================
#
# Given a dictionary, check if it has dictionaries within it. If so,
# separate the dictionary such that we only have information regarding
# the sub-dictionary
#
# Example:
#    Given:  {'reading: {'bandwidth_in': 3, 'bandwidth_out':4}}
#    Output: {'bandwidth_in': 3, 'bandwidth_out':4}
#
# ==================================================================
def format_reading(status: process_status, data):
    """
    itterate through list is an object has a sub-dict (example {key:{}}) break it appart
    :args:
       data:dict - dictionary of raw data or string that can be converted to dict
    :return:
       data properly formated
    """
    remove_data = []
    update_data = {}

    if not isinstance(data, dict):
        jdata = str_to_json(data)
        if not isinstance(jdata, dict):
            status.add_error(f"Invalid input JSON entry processed - Not convertible to JSON format: {data}")
            return None
        if not len(jdata):
            status.add_error(f"Empty entry in JSON input file:  {data}")
            return None
    else:
        status.add_error(f"Invalid input - JSON entry is not a dictionary: {data}")
        return None

    # Find keys with dict values & note them
    for key in jdata:
        if isinstance(jdata[key], dict):
            for ky in jdata[key]:
                if key.lower() == ky.lower():  # this is a nested JSON object - find the needed value
                    update_data[key] = jdata[key][ky]
            remove_data.append(key)

            # remove dict keys convert to their own key-value pairs
    for key in remove_data:
        del jdata[key]

        # add subkeys/values as their own
    jdata.update(update_data)

    return jdata
# ==================================================================
# Determine if the path include a file or only a directory
# ==================================================================
def determine_name_type(status, test_string):

    # Get the destination location or destination file
    file_path, current_name, file_type = extract_path_name_type(test_string)
    if not file_path:
        str_type = "file"
    else:
        if current_name and file_type:
            # Path and file name was provided
            str_type = "file"
        elif current_name and not file_type:
            # Test a directory exists - use the directory
            ret_val = test_dir_exists_and_writeable(status, file_path + current_name, False)
            if not ret_val:
                str_type = "dir"
            else:
                str_type = "file"
        else:
            # No current_name and no file type
            str_type = "dir"

    return str_type