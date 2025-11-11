"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""
import os

import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.utils_io as utils_io
from edge_lake.generic import interpreter
import edge_lake.dbms.akave as akave


providers_ = {
    # List of providers that were imported
}

try:
    # import to akave libraries
    providers_["akave"] = True
except:
    pass

# ============================================================================================
# Stores files into buckets (i.e. Filecoin / S3)
# ============================================================================================

registry_ = {           # Registries for all buckets and their info

}
# ============================================================================================
# Test that the group was declared
# ============================================================================================
def is_group_declared(group_name):
    global registry_
    return group_name in registry_
# ============================================================================================
# Bucket connect to a bucket provider
# Example: bucket provider connect where group = my_buckets and provider = akave and id = 123 and password = abc and region = US
# ============================================================================================
def bucket_connect(status, io_buff_in, cmd_words, trace):

    global providers_
    global registry_

    #                              Must     Add      Is
    #                              exists   Counter  Unique

    keywords = {"group": ("str", True, False, True),
                "provider": ("str", True, False, True),
                "id":     ("str", True, False, True),
                "password": ("str", False, False, True),
                "access_key": ("str", False, False, True),
                "secret_key": ("str", False, False, True),
                "region": ("str", True, False, True),
                "endpoint_url": ("str", True, False, True),
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0,
                                                                   keywords, False)
    if ret_val:
        return [ret_val, None]

    bucket_group = interpreter.get_one_value(conditions, "group")
    if bucket_group in registry_:
        status.add_error(f"Duplicate Bucket Name: {bucket_group}")
        return [process_status.ERR_process_failure, None]

    bucket_provider = interpreter.get_one_value(conditions, "provider")
    if not bucket_provider in providers_:
        status.add_error(f"Failed to import provider libraries or wrong provider name: {bucket_provider}")
        return [process_status.Failed_to_import_lib, None]

    bucket_id = interpreter.get_one_value(conditions, "id")
    bucket_pswrd = interpreter.get_one_value(conditions, "password")
    bucket_region = interpreter.get_one_value_or_default(conditions, "region", None)
    bucket_access_key = interpreter.get_one_value_or_default(conditions, "access_key", None)
    bucket_secret_key = interpreter.get_one_value_or_default(conditions, "secret_key", None)
    endpoint_url = interpreter.get_one_value_or_default(conditions, "endpoint_url", None)


    bucket_info = {
        "provider": bucket_provider,
        "endpoint_url": endpoint_url,
        "id": bucket_id,
        "pswrd": bucket_pswrd,
        "region": bucket_region,
        "access_key": bucket_access_key,
        "secret_key": bucket_secret_key,
    }

    # Test connection to the buckets
    reply = None
    obj = None

    if bucket_provider.lower() == "akave":
        # ret_val = akave(["bucket", "view", bucket_group], bucket_region, bucket_pswrd)

        ret_val = akave.test_lib_installed(status)
        if ret_val:
            # The Akave bucket not installed
            return [ret_val, None]

        obj = akave.AkaveConnector(endpoint_url, bucket_region, bucket_access_key, bucket_secret_key)
        ret_val, reply = obj.list_buckets(status) # returns a string
        if ret_val:
            status.add_error(f"Failed to connect to Akave at {endpoint_url} and Region {bucket_region}")
            ret_val = process_status.Failed_to_declare_bucket_group
        if not ret_val:
            bucket_info["bucket_obj"] = obj
            registry_[bucket_group] = bucket_info  # Store info as f(name)
            reply = "Connected to bucket '%s' in Akave" % bucket_group
            status.add_message("Connected to bucket '%s' in Akave" % bucket_group)
    else:
        status.add_error("Bucket provider '%s' not supported" % (bucket_provider))

    return [ret_val, reply]

# ============================================================================================
# bucket provider disconnect where group = my_buckets
# ============================================================================================
def bucket_disconnect(status, io_buff_in, cmd_words, trace):
    global registry_

    #                              Must     Add      Is
    #                              exists   Counter  Unique

    keywords = {
                "group":     ("str", True, False, True),
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0,
                                                                   keywords, False)
    if ret_val:
        return [ret_val, None]


    group_name = interpreter.get_one_value(conditions, "group")

    if group_name in registry_:
        del registry_[group_name]
        ret_val = process_status.SUCCESS
        output_text = "Bucket disconnect success"
    else:
        output_text = "Bucket disconnect failed"
        status.add_error(f"Bucket disconnect failed: Bucket Group: {group_name} not found")
        ret_val = process_status.ERR_process_failure

    return [ret_val, output_text]


# ============================================================================================
# get bucket groups    -> return the list of bucket groups
# ============================================================================================
def get_connected_groups(status, io_buff_in, cmd_words, trace):

    global registry_

    if len(registry_):
        first_entry = registry_[next(iter(registry_))]
        title = ["Group Name"] + [k for k in first_entry.keys() if k not in ("pswrd", "secret_key", "access_key")]
        struct_info = []
        for bucket_name, bucket_info in registry_.items():
            struct_info.append(
                [bucket_name] + [v for k, v in bucket_info.items() if k not in ("pswrd", "secret_key", "access_key")]
            )

        reply = utils_print.output_nested_lists(struct_info, "", title, True)
    else:
        reply = "No bucket groups are connected"

    return [process_status.SUCCESS, reply]

# ============================================================================================
# Bucket Create ...
# Example: bucket create where group = my_group and name = my_bucket
# ============================================================================================
def bucket_create(status, io_buff_in, cmd_words, trace):

    #                              Must     Add      Is
    #                              exists   Counter  Unique

    keywords = {"group": ("str", True, False, True),
                "name":     ("str", True, False, True),
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3, 0,
                                                                   keywords, False)
    if ret_val:
        return [ret_val, None]

    bucket_group = interpreter.get_one_value(conditions, "group")
    reply = None
    if not bucket_group in registry_:
        status.add_error(f"Bucket Group: {bucket_group} not declared or not connected")
        ret_val = process_status.Bucket_group_not_declared
        reply = "Bucket group not connected"
    else:
        bucket_name = interpreter.get_one_value(conditions, "name")
        # Create the bucket in the group and return success or process_status.Failed_bucket_create with the reason
        obj = registry_[bucket_group].get("bucket_obj")
        if obj is None:
            status.add_error(f"Bucket Group Not Connected: ...")
            reply = "Bucket group not connected"
            ret_val = process_status.Bucket_group_not_declared

        if not ret_val:

            ret_val, reply = obj.create_bucket(status, bucket_name)

    return [ret_val, reply]

# ============================================================================================
# Bucket Drop ...
# Example: bucket drop where group = my_group and name = my_bucket and delete_all = true
# Note: You cannot drop a non-empty bucket. Thus, prompt user to specify whether to delete all files or not.
# ============================================================================================
def bucket_drop(status, io_buff_in, cmd_words, trace):

    #                              Must     Add      Is
    #                              exists   Counter  Unique

    keywords = {"group": ("str", True, False, True),
                "name":     ("str", True, False, True),
                "delete_all": ("bool", True, False, True), # signal for deleting non-empty bucket
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3, 0,
                                                                   keywords, False)
    if ret_val:
        return [ret_val, None]

    bucket_group = interpreter.get_one_value(conditions, "group")
    reply = None
    if not bucket_group in registry_:
        status.add_error(f"Bucket Group: {bucket_group} not declared or not connected")
        reply = "Bucket group not connected"
        ret_val = process_status.Bucket_group_not_declared
    else:
        bucket_name = interpreter.get_one_value(conditions, "name")
        # drop the bucket in the group and return success or process_status.Failed_bucket_drop with the reason
        obj = registry_[bucket_group].get("bucket_obj")
        if obj is None:
            reply = "Bucket group not connected"
            status.add_error(f"Bucket Group Not Connected: ...")
            ret_val = process_status.Bucket_group_not_declared
        if not ret_val:

            delete_all = interpreter.get_one_value(conditions, "delete_all")
            ret_val, reply = obj.delete_bucket(status, bucket_name, delete_all)

    return [ret_val, reply]

# ============================================================================================
# Get the list of files in the bucket
# Examople: get bucket files where group = my_group and name = my_bucket
# ============================================================================================
def get_files(status, io_buff_in, cmd_words, trace):

    #                              Must     Add      Is
    #                              exists   Counter  Unique

    keywords = {"group":    ("str", True, False, True),
                "name":     ("str", True, False, True),
                "format":   ("str", False, False, True),
                "prefix": ("str", False, False, True),
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0,
                                                                   keywords, False)
    if ret_val:
        return [ret_val, None]

    reply = None
    bucket_group = interpreter.get_one_value(conditions, "group")
    if not bucket_group in registry_:
        status.add_error(f"Bucket Group: {bucket_group} not declared or not connected")
        ret_val = process_status.Bucket_group_not_declared
    else:
        bucket_name = interpreter.get_one_value(conditions, "name")
        # Return the list of the buckets or:
        obj = registry_[bucket_group].get("bucket_obj")
        if obj is None:
            status.add_error(f"Bucket Group Not Connected: ...")
            ret_val = process_status.Bucket_group_not_declared
        if not ret_val:
            prefix = interpreter.get_one_value(conditions, "prefix")
            ret_val, files_list = obj.list_files(status, bucket_name, prefix=prefix or "")

            if not ret_val:
                output_format = interpreter.get_one_value_or_default(conditions, "format", "table")
                if output_format == "table":
                    output_table = [[bucket_group, bucket_name, entry] for entry in files_list]

                    title = ["Bucket Group", "Bucket Name", "File Name"]
                    reply = utils_print.output_nested_lists(output_table, "", title, True)
                else:
                    reply = files_list


    return [ret_val, reply]


# ============================================================================================
# Get the list of files in the bucket
# Example: get bucket names where group = my_group
# ============================================================================================
def get_names(status, io_buff_in, cmd_words, trace):
    
    #                              Must     Add      Is
    #                              exists   Counter  Unique

    keywords = {"group":    ("str", True, False, True),
                "format":   ("str", False, False, True),
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0,
                                                                   keywords, False)
    if ret_val:
        return [ret_val, None]

    reply = None
    bucket_group = interpreter.get_one_value(conditions, "group")
    if not bucket_group in registry_:
        status.add_error(f"Bucket Group: {bucket_group} not declared or not connected")
        ret_val = process_status.Bucket_group_not_declared
    else:
        # Return the list of the buckets or:
        obj = registry_[bucket_group].get("bucket_obj")
        if obj is None:
            status.add_error(f"Bucket Group Not Connected: ...")
            ret_val = process_status.Bucket_group_not_declared
        if not ret_val:
            ret_val, bucket_names = obj.list_buckets(status)

            output_table = [[bucket_group, entry] for entry in bucket_names]

            title = ["Bucket Group", "Bucket Name"]
            reply = utils_print.output_nested_lists(output_table, "", title, True)

    return [ret_val, reply]


# ============================================================================================
# Upload a file to a bucket
# Example: bucket file upload where group = my_group and name = my_bucket and source_dir = my_dir and file_name = file_name
# ============================================================================================
def file_upload(status, io_buff_in, cmd_words, trace):

    #                              Must     Add      Is
    #                              exists   Counter  Unique

    keywords = {"group": ("str", True, False, True),
                "name":     ("str", True, False, True),
                "source_dir": ("str", True, False, True),
                "file_name": ("str", True, False, True),
                "key": ("str", True, False, True),
                "encryption_key": ("str", False, False, True),
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0,
                                                                   keywords, False)
    if ret_val:
        return [ret_val, None]

    bucket_group = interpreter.get_one_value(conditions, "group")
    reply = None
    if not bucket_group in registry_:
        status.add_error(f"Bucket Group: {bucket_group} not declared or not connected")
        reply = "Bucket group not connected"
        ret_val = process_status.Bucket_group_not_declared
    else:
        bucket_name = interpreter.get_one_value(conditions, "name")
        # Test that the bucket exists + load the file
        obj = registry_[bucket_group].get("bucket_obj")
        if obj is None:
            status.add_error(f"Bucket Group Not Connected: ...")
            reply = "Bucket Group Not Connected"
            ret_val = process_status.Bucket_group_not_declared
        if not ret_val:
            filename = interpreter.get_one_value(conditions, "file_name")
            source_dir = interpreter.get_one_value(conditions, "source_dir")
            key = interpreter.get_one_value(conditions, "key")
            encryption_key = interpreter.get_one_value(conditions, "encryption_key")
            ret_val = utils_io.test_dir_exists(status, source_dir, True)
            if not ret_val:
                # source dir exists - Error message in the function
                file_path = os.path.join(source_dir, filename)
                # check if key exists
                ret_val, reply = obj.list_files(status, bucket_name, key)
                if not ret_val:
                    if key in reply:
                        ret_val = process_status.Already_exists
                        status.add_error(f"Bucket Key Already Exists: {key}")
                    else:
                        ret_val, reply = obj.upload_file(status, bucket_name, file_path, key, encryption_key)

    return [ret_val, reply]

# ============================================================================================
# Upload a file to a bucket
# Example: bucket file download where group = my_group and name = my_bucket and dest_dir = my_dir and file_name = file_name'
# ============================================================================================
def file_download(status, io_buff_in, cmd_words, trace):


    #                              Must     Add      Is
    #                              exists   Counter  Unique

    keywords = {"group": ("str", True, False, True),
                "name":     ("str", True, False, True),
                "dest_dir": ("str", True, False, True),
                "key": ("str", True, False, True),
                "file_name": ("str", True, False, True),
                "encryption_key": ("str", False, False, True),
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0,
                                                                   keywords, False)
    if ret_val:
        return [ret_val, None]

    bucket_group = interpreter.get_one_value(conditions, "group")
    reply = None
    if not bucket_group in registry_:
        status.add_error(f"Bucket Group: {bucket_group} not declared or not connected")
        ret_val = process_status.Bucket_group_not_declared
        reply = "Bucket Group Not Connected"
    else:
        bucket_name = interpreter.get_one_value(conditions, "name")
        obj = registry_[bucket_group].get("bucket_obj")
        if obj is None:
            status.add_error(f"Bucket Group Not Connected: ...")
            reply = "Bucket Group Not Connected"
            ret_val = process_status.Bucket_group_not_declared
        if not ret_val:
            dest_dir = interpreter.get_one_value(conditions, "dest_dir")
            file_name = interpreter.get_one_value(conditions, "file_name")
            key = interpreter.get_one_value(conditions, "key")
            encryption_key = interpreter.get_one_value(conditions, "encryption_key")

            ret_val, reply = obj.download_file(status, bucket_name, key, file_name, dest_dir, encryption_key)

    return [ret_val, reply]

# ============================================================================================
# Delete a file from a bucket by key or prefix
# Example: bucket file delete where group = my_group and name = my_bucket and key = my-key and prefix = dir1'
# ============================================================================================
def file_delete(status, io_buff_in, cmd_words, trace):


    #                              Must     Add      Is
    #                              exists   Counter  Unique

    keywords = {"group": ("str", True, False, True),
                "name":     ("str", True, False, True),
                "key": ("str", False, False, True),
                "prefix": ("str", False, False, True),
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0,
                                                                   keywords, False)
    if ret_val:
        return [ret_val, None]

    bucket_group = interpreter.get_one_value(conditions, "group")
    reply = None
    if not bucket_group in registry_:
        status.add_error(f"Bucket Group: {bucket_group} not declared or not connected")
        ret_val = process_status.Bucket_group_not_declared
        reply = "Bucket Group Not Connected"
    else:
        bucket_name = interpreter.get_one_value(conditions, "name")
        obj = registry_[bucket_group].get("bucket_obj")
        if obj is None:
            status.add_error(f"Bucket Group Not Connected: ...")
            reply = "Bucket Group Not Connected"
            ret_val = process_status.Bucket_group_not_declared
        if not ret_val:

            key = interpreter.get_one_value(conditions, "key")
            prefix = interpreter.get_one_value(conditions, "prefix")

            if not (key or prefix): # must provide a key or a prefix
                status.add_error(f"File key or file prefix not specified.")
                ret_val = process_status.Failed_to_delete_file
            else:
                if key:
                    ret_val, reply = obj.delete_file(status, bucket_name, key)
                if not ret_val:
                    if prefix:
                        ret_val, reply = obj.delete_file_by_prefix(status, bucket_name, prefix)

    return [ret_val, reply]

  
# ============================================================================================
# Get a file's info
# Example: get bucket file info where group = my_group and name = my_bucket and key = my-key'
# ============================================================================================
def file_info(status, io_buff_in, cmd_words, trace):


    #                              Must     Add      Is
    #                              exists   Counter  Unique

    keywords = {"group": ("str", True, False, True),
                "name":     ("str", True, False, True),
                "key": ("str", True, False, True),
                }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 5, 0,
                                                                   keywords, False)
    if ret_val:
        return [ret_val, None]

    bucket_group = interpreter.get_one_value(conditions, "group")
    reply = None
    if not bucket_group in registry_:
        status.add_error(f"Bucket Group: {bucket_group} not declared or not connected")
        ret_val = process_status.Bucket_group_not_declared
        reply = "Bucket Group Not Connected"
    else:
        bucket_name = interpreter.get_one_value(conditions, "name")
        obj = registry_[bucket_group].get("bucket_obj")
        if obj is None:
            status.add_error(f"Bucket Group Not Connected: ...")
            reply = "Bucket Group Not Connected"
            ret_val = process_status.Bucket_group_not_declared
        if not ret_val:

            key = interpreter.get_one_value(conditions, "key")

            if not key:
                status.add_error(f"File key not specified.")
                ret_val = process_status.Failed_to_extract_file_components
            if not ret_val:
                ret_val, reply = obj.file_info(status, bucket_name, key)

    return [ret_val, reply]

  