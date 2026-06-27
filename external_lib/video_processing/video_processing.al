# working directory

on error ignore

import function where import_name = imshow and lib = external_lib.video_processing.cv2_stream_imshow and method = init_class
get imported functions

set function params where import_name = imshow and param_name = port and param_type = int and param_value = 8888
set function params where import_name = imshow and param_name = host and param_value = 0.0.0.0
get function params

import function where import_name = initiate_yolo and lib = external_lib.frame_modeling.yolo_detection and method = init_class
get imported functions


set function params where import_name = initiate_yolo and param_name = module_type and param_value = darknet
set function params where import_name = initiate_yolo and param_name = classes and param_type = list and param_value = []
set function params where import_name = initiate_yolo and param_name = module_path1 and param_value = https://github.com/AlexeyAB/darknet/releases/download/yolov4/yolov4-tiny.weights
set function params where import_name = initiate_yolo and param_name = module_path2 and param_value = https://raw.githubusercontent.com/AlexeyAB/darknet/refs/heads/master/cfg/yolov4-tiny.cfg
set function params where import_name = initiate_yolo and param_name = coco_path and param_value = https://raw.githubusercontent.com/pjreddie/darknet/master/data/coco.names
get function params


<video connect where
    name = youtube and
    protocol = https and
    interface = url and
    address = "https://www.youtube.com/watch?v=rnXIjl_Rzy4" and
    video_dbms = video_dbms and
    video_table = video_table and
    detection_dbms = video_dbms and
    detection_table = detection_table and
    detection_column = person and
    detection_column = car and
    detection_column = truck and
    detection_column = bus and
    recording_segment_time = 1 and
    detection_ignore_time = 10
>

get connected video streams

run video stream where name=youtube and import_detect = initiate_yolo and import_display = imshow
get video streams stats
