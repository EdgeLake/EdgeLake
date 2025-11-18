"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_print as utils_print
import edge_lake.generic.interpreter as interpreter
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.func_references as func_references
import edge_lake.generic.params as params
from edge_lake.generic.utils_data import camera_url_builder


from edge_lake.video.generic_client import GenericVideoStreaming
from edge_lake.video.video import get_stats

STREAMING_PROTOCOLS = {
    "generic": ['rtsp', 'rtmp', 'http', 'https', 'hls', 'srt']
}

connected_ = {}     # The list of connected cameras and the camera object


# ---------------------------------------------------------------------------------
# Connect to a Video stream
# Example:
'''
<video connect where
    name = youtube and
    protocol = https and
    interface = url and
    address = "https://www.youtube.com/watch?v=rnXIjl_Rzy4" and
    dbms = video_dbms and 
    table = ny_camera
>
'''
# ---------------------------------------------------------------------------------
def connect(status, io_buff_in, cmd_words, trace):
    global connected_

    #                          Must     Add      Is
    #                          exists   Counter  Unique`
    keywords = {"name": ("str", True, False, True),
                "protocol": ("str", True, False, True),
                "interface": ("str", False, False, True), # UPC and TCP   -- If interface == "url" - take a video feed
                "address": ("str", True, False, True),
                "user": ("str", False, False, True),
                "password": ("str", False, False, True),
                "video_dir": ("str", False, False, True),       # Directory to place the videos captured - by default - blobs_dir
                "video_dbms" : ("str", False, True, True),      # The dbms to maintain the info on each video (the blobs dbms is blobs_[video_dbms]
                "video_table": ("str", False, True, True),      # The dbms to maintain the info on each table (the blobs dbms is blobs_[video_dbms]
                "detection_dbms": ("str", False, False, True),   # The dbms to maintain the detection info
                "detection_table": ("str", False, False, True),  # The table to maintain the detection info
                "detection_column" : ("str", False, False, False), # The detection columns

                "recording_segment_time": ("int", False, False, True),  # The length of thevideo segment to store (in minutes)
                "detection_ignore_time" : ("int", False, False, True),  # The time to ignore mutiple detections if they have the same value (in seconds)
                }


    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3, 0, keywords, False)
    if ret_val:
        return [ret_val, None]

    if counter and counter != 2:
        status.add_error("Video connect error: Either both 'video_dbms' and 'table_dbms' names must be provided, or neither")
        return [process_status.ERR_command_struct, None]

    connect_name = interpreter.get_one_value(conditions, "name")  # The name of the connection
    protocol = interpreter.get_one_value(conditions, "protocol")  # The name of the connection
    interface = interpreter.get_one_value(conditions, "interface")  # The name of the connection
    address = interpreter.get_one_value(conditions, "address")  # The name of the connection
    user = interpreter.get_one_value(conditions, "user")  # The name of the connection
    password = interpreter.get_one_value(conditions, "password")  # The name of the connection

    video_dbms = interpreter.get_one_value(conditions, "video_dbms")  # The dbms to maintain the info on each video (the blobs dbms is blobs_[video_dbms]
    video_table = interpreter.get_one_value(conditions, "video_table") # The table to maintain the info on each video
    detection_dbms = interpreter.get_one_value(conditions, "detection_dbms")  # The dbms to maintain the detection info
    detection_table = interpreter.get_one_value(conditions, "detection_table")  # The detection columns

    detection_columns = conditions.get("detection_column")        # Get multiple columns

    recording_segment_time = interpreter.get_one_value_or_default(conditions, "recording_segment_time", 5)    # Default is 5 minute recordings
    detection_ignore_time = interpreter.get_one_value_or_default(conditions, "detection_ignore_time", 2)      # Default is 2 second


    if detection_dbms or detection_table or detection_columns:
        if not detection_dbms or not detection_table or not detection_columns:
            # All or none needs to be with a value
            status.add_error("Video connect error: Either 'detection_dbms' and 'detection_table' and 'detection_columns' must be provided, or neither")
            return [process_status.ERR_command_struct, None]


    ret_val, watch_dir, bwatch_dir, err_dir, prep_dir = params.get_values(status, "video connect",
                                                              ["!watch_dir", "!bwatch_dir", "!err_dir", "!prep_dir"])

    if ret_val:
        return [ret_val, None]

    video_dir = interpreter.get_one_value_or_default(conditions, "blobs_dir", "!blobs_dir")
    if not video_dir:
        status.add_error("Camera connect: 'video_dir' is not provided - update 'video_dir' in command or assign value to 'blobs_dir' in the dictionary")
        ret_val = process_status.Error_command_params
        return [ret_val, None]

    if connect_name in connected_:
        ret_val = process_status.Duplicate_connection_name
        return [ret_val, None]

    # check params for protocol and interface
    if protocol not in STREAMING_PROTOCOLS['generic']:
        status.add_error(f"Invalid video streaming protocol {protocol} - Valid protocol options {','.join(STREAMING_PROTOCOLS['generic'])}")
        ret_val = process_status.NOT_supported_Protocol
    elif protocol == 'rtsp':
        if not interface in ['tcp', 'udp']:
            status.add_error("Invalid interface option for RTSP - supported interfaces TCP and UDP")
            ret_val = process_status.NOT_supported_Protocol
    if ret_val != process_status.SUCCESS:
        return [ret_val, None]


    camera_url = camera_url_builder(address, user, password, protocol)
    if not camera_url:
        status.add_error(f"Camera connect: Invalid URL for {protocol} connection to {connect_name}")
        ret_val = process_status.Arguments_mismatch
        return [ret_val, None]

    if interface == "url":
        # --- New Code: overwrite URL for consistent video feed ---
        # Abbey Road London: https://www.youtube.com/watch?v=57w2gYXjRic
        # Times Square: https://www.youtube.com/watch?v=rnXIjl_Rzy4

        # supports live feed from
        # - Youtube
        # - twitch
        # - Facebook
        # - instagram
        # - tikTok
        # - Vimo
        # - linkedin
        ret_val, url = utils_data.hls_url(status=status, url=camera_url)
    else:
        url = camera_url


    if not ret_val:
        video_client = GenericVideoStreaming       # Get the CAMERA API implementation



        connection = video_client(status=status, source_name=connect_name, url=url, user=user, password=password, protocol= protocol,
                                   interface=interface, prep_dir=prep_dir, watch_dir=watch_dir, bwatch_dir=bwatch_dir, err_dir=err_dir, video_dir=video_dir,
                                  video_dbms=video_dbms, video_table=video_table,
                                  detection_dbms=detection_dbms, detection_table=detection_table, detection_columns=detection_columns,
                                  recording_segment_time=recording_segment_time,
                                  detection_ignore_time=detection_ignore_time
                                  )

        ret_val = connection.test_dir_exists(status)  # Test all dirs exists - prep, watch, bwatch, blobs
        if not ret_val:
            ret_val = connection.open_connection(status)
            if not ret_val:
                connected_[connect_name] = connection


    return [ret_val, None]


# ---------------------------------------------------------------------------------
# Get video configuration info
# get connected video streams
# Or
# get connected video url
# ---------------------------------------------------------------------------------
def get_connected_video_streams(status, io_buff_in, cmd_words, trace):
    global connected_

    ret_val = process_status.SUCCESS
    out_list = []
    if cmd_words[3] == "streams":
        for connect_object in connected_.values():
            conn_name = connect_object.get_source_name()
            video_dbms = connect_object.get_video_dbms()
            video_table = connect_object.get_video_table()
            detection_dbms = connect_object.get_detection_dbms()
            detection_table = connect_object.get_detection_table()
            conn_protocol = connect_object.get_protocol()
            conn_interface = connect_object.get_interface()
            conn_fps = connect_object.get_display_fps()
            video_time = connect_object.get_recording_segment_time()
            ignore_time = connect_object.get_detection_ignore_time()

                # ALl exited
            if connect_object.controller.is_exit():
                streaming_stat = "exited"
                storage_stat = "exited"
                display_stat = "exited"
            else:

                streaming_stat, storage_stat, display_stat = connect_object.get_threads_stat()

            ai_models = connect_object.controller.with_ai_models()

            out_list.append([conn_name, video_dbms, video_table, detection_dbms, detection_table, conn_protocol, conn_interface, conn_fps, video_time, ignore_time, streaming_stat, storage_stat, display_stat, ai_models])


        info_str = utils_print.output_nested_lists(out_list, "", ["Name", "Video DBMS", "Video Table", "Detection DBMS", "Detection Table", "Protocol", "Interface", "FPS", "Video time\n(minutes)", "Ignore time\n(seconds)", "Streaming", "Storage", "Display", "AI Models"], True)
    elif cmd_words[3] == "urls":
        for connect_object in connected_.values():
            conn_name = connect_object.get_source_name()
            video_dbms = connect_object.get_video_dbms()
            video_table = connect_object.get_video_table()
            detection_dbms = connect_object.get_detection_dbms()
            detection_table = connect_object.get_detection_table()
            conn_url = connect_object.get_conn_url()

            out_list.append([conn_name, video_dbms, video_table, detection_dbms, detection_table, conn_url])


        info_str = utils_print.output_nested_lists(out_list, "",
                                                       ["Name", "Video DBMS", "Video Table", "Detection DBMS",
                                                        "Detection Table","URL"], True)
    else:
        ret_val = process_status.ERR_command_struct
        info_str = None

    return [ret_val, info_str]

# ---------------------------------------------------------------------------------
# Get video statistics info
# command: get video streams stats
# ---------------------------------------------------------------------------------
def get_video_streams_stats(status, io_buff_in, cmd_words, trace):
    source_dict = get_stats()
    info_str = utils_print.print_flat_dict_as_table(source_dict, [], "Name", None, True)
    return [process_status.SUCCESS, info_str]

# ---------------------------------------------------------------------------------
# Stream from a camera
# Example: run video stream where name=axis
# ---------------------------------------------------------------------------------
def run_video_stream(status, io_buff_in, cmd_words, trace):
    """
    This code needs to be using a separate thread.
    Right now it's being called using `thread` function
    """
    global connected_

    #                          Must     Add      Is
    #                          exists   Counter  Unique`
    keywords = {
        "name": ("str", True, False, True),
        "import_detect": ("str", False, False, True),      # The library to use in the detection
        "import_display": ("str", False, False, True),  # The library to use in the display
    }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if not ret_val:

        connect_name = interpreter.get_one_value(conditions, "name")  # The name of the connection

        if connect_name not in connected_:
            status.add_error(f"Camera '{connect_name}' not declared")
            ret_val = process_status.Camera_not_declared
        elif  not ret_val:

            enabled_libs = {}
            for key in ["import_detect", "import_display"]:
                if key in conditions:
                    # Get libraries which are defined using the "import function where import_name = ..." command
                    external_libs = []
                    import_names = conditions[key]    # A list of functions to pass to start_stream_video()
                    for import_name in import_names:
                        function = func_references.get_func(import_name)
                        if not function:
                            status.add_error(f"The function '{import_name}' does not exist in the functions registry. Use 'import function' command to load the function")
                            return process_status.Lib_not_in_registry
                        # get the params
                        params = func_references.get_params(import_name)
                        if params:
                            # initiate the function
                            module = function(**params)
                        else:
                            module = function()
                        external_libs.append(module)
                    enabled_libs[key] = external_libs


            ret_val = connected_[connect_name].start_stream_video(status=status, external_func=enabled_libs)

    return ret_val

def run_video_stream_old(status, io_buff_in, cmd_words, trace):
    """
    This code needs to be using a separate thread.
    Right now it's being called using `thread` function
    """
    global connected_

    #                          Must     Add      Is
    #                          exists   Counter  Unique`
    keywords = {
        "name": ("str", True, False, True)
    }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if not ret_val:

        connect_name = interpreter.get_one_value(conditions, "name")  # The name of the connection

        if connect_name not in connected_:
            status.add_error(f"Camera '{connect_name}' not declared")
            ret_val = process_status.Camera_not_declared
        elif  not ret_val:
            ret_val = connected_[connect_name].start_stream_video(status=status)

    return ret_val


# ---------------------------------------------------------------------------------
# Stop stream process from a camera
# Example: video stream resume where name = aixs
#          video stream pause where name = aixs and module = storage
# ---------------------------------------------------------------------------------
def video_stream_function(status, io_buff_in, cmd_words, trace):
    """
    1. Close open window (if set)
    2. stop stream process
    """
    global connected_

    #                          Must     Add      Is
    #                          exists   Counter  Unique`
    keywords = {
        "name": ("str", True, False, True),
        "module": ("str", False, False, True)
    }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 4, 0, keywords, False)
    if not ret_val:

        connect_name = interpreter.get_one_value(conditions, "name")  # The name of the connection

        if connect_name not in connected_:
            status.add_error(f"Camera '{connect_name}' not declared")
            ret_val = process_status.Camera_not_declared
        elif  not ret_val:
            operation = cmd_words[2]
            if operation != "pause" and operation != "resume":
                status.add_error(f"Unrecognized operation ('{operation}') applied to video stream '{connect_name}'")
                ret_val = process_status.Error_command_params
            else:
                module = interpreter.get_one_value(conditions, "module")  # Pause or Resume
                if not module:
                    module = "stream"       # Pause or resume the stream
                elif module and not (module == "storage" or module == "display"):
                    status.add_error(f"Unrecognized stream module: '{module}' is not recognized")
                    ret_val = process_status.Error_command_params

        if not ret_val:

            is_set = connected_[connect_name].controller.get_state(module)    # With False - wait() will block. With True - wait() does not block

            if is_set:
                if operation == "resume":
                    status.add_error(f"Wrong sequence of operations - process {module} in stream {connect_name} is already running")
                    ret_val = process_status.ERR_command_sequence
                else:
                    connected_[connect_name].controller.pause_event(module)
            else:
                if operation == "pause":
                    status.add_error(f"Wrong sequence of operations - process {module} in stream {connect_name} is already paused")
                    ret_val = process_status.ERR_command_sequence
                else:
                    connected_[connect_name].controller.resume_event(module)

    return [ret_val, None]

# ---------------------------------------------------------------------------------
# exit video where name = video - Close the popup window and the video stream
# ---------------------------------------------------------------------------------
def exit_video(status, io_buff_in, cmd_words, trace):
    """
    function for closing camera popup window
    """
    global  connected_
    err_msg = ""
    keywords = {
        "name": ("str", True, False, True)
    }

    ret_val, counter, conditions = interpreter.get_dict_from_words(status, cmd_words, 3, 0, keywords, False)

    if not ret_val:

        connect_name = interpreter.get_one_value(conditions, "name")  # The name of the connection
        if not connect_name in connected_:
            status.add_error(f"Camera '{connect_name}' not declared")
            ret_val = process_status.Wrong_connection_name
        else:
            ret_val = connected_[connect_name].exit_stream_video(status=status)
            if not ret_val or ret_val == process_status.CAMERA_THREAD_NOT_ACTIVE:
                # ret_val is process_status.CAMERA_THREAD_NOT_ACTIVE - if the camera was shut - remove the object
                del connected_[connect_name]        # remove the instance
                ret_val = process_status.SUCCESS

    return ret_val


def is_running():
    return True if len(connected_) else False

def get_info(status):
    streams_counter = len(connected_)
    message = f"Video streams declared: {streams_counter}" if streams_counter else ""
    return message