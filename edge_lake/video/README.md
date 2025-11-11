## Todo 
**Phase 1**: 
- connect / disconnect from Camera [DONE]
- process images [DONE]
- start / stop viewing image [DONE]

**Phase 2**: 
- (optional) bbox integration [code - DONE]
- remote-gui / cli to view video 

**Phase 3**: store to AnyLog / EdgeLake 
- insights stores 

# Testing RTSP service 

**Examples**: 
* http(s) -- cartoon movie 
```anylog 
<video connect where 
    name=cartoon and 
    protocol = https and 
    address = https://cdn.flowplayer.com/a30bd6bc-f98b-47bc-abf5-97633d4faea0/hls/de3f6ca7-2db3-4689-8160-0f574a5996ad/playlist.m3u8
>
# enable streaming 
run video stream where name=cartoon
# enable display 
run video display where name=cartoon

video stream function where name = video and type = pause

video stream function where name = video and type = resume

# hide display 
exit video display where name = video
exit video stream where name = video

# show status 
get connected cameras 
# hide display 
camera stream hide where name=video
# disable streaming - could have timeout error 
camera stream stop where name=video

# close connection - doesn't work (see above) 
camera disconnect video 
```

* YouTube - Live Feed [README.md](README.md)
```anylog
# Abbey Road London: https://www.youtube.com/watch?v=57w2gYXjRic
# Times Square: https://www.youtube.com/watch?v=rnXIjl_Rzy4
# twitch : https://www.twitch.tv/citywalking4k

<video connect where
    name = youtube and
    protocol = https and
    interface = url and
    address = "https://www.youtube.com/watch?v=rnXIjl_Rzy4"
>
run video stream where name=youtube
run video display where name=youtube
```

* RTSP -- Live Camera by Axis
```anylog 
<camera connect where
    name = axis and
    protocol = rtsp and
    interface = tcp and
    address = "166.143.227.89:554/axis-media/media.amp?videocodec=h264" and
    user = AnyLog and password = "OriIsThweBest#1!@"
>

# enable streaming 
run video stream where name=axis
# enable display 
camera stream show where name=axis
# show status 
get connected cameras 
# hide display 
camera stream hide where name=axis
# disable streaming 
camera stream stop where name=axis

# close connection - doesn't work 
camera disconnect video 
```


```

REPLACING:

from external_lib.frame_modeling.yolo_detection import YoloDetection

#--- New code: should be outside this class ---
self.module = YoloDetection(module_type='darknet', classes=[])
self.module.initiate_models(module_path1='https://github.com/AlexeyAB/darknet/releases/download/yolov4/yolov4-tiny.weights',
                            module_path2='https://raw.githubusercontent.com/AlexeyAB/darknet/refs/heads/master/cfg/yolov4-tiny.cfg',
                            coco_path='https://raw.githubusercontent.com/pjreddie/darknet/master/data/coco.names')

WITH

# define the function and init

import function where import_name = initiate_yolo and lib = external_lib.frame_modeling.yolo_detection and method = init_class
get imported functions

# Define params passed to init

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
    address = "https://www.youtube.com/watch?v=rnXIjl_Rzy4"
>
run video stream where name=youtube and import_name = initiate_yolo

        video stream pause where name = youtube
        video stream resume where name = youtube

get connected video streams


        video stream pause where name = video 
        video stream resume where name = video
        video stream pause where name = video and module = storage
        video stream resume where name = video and module = storage
        video stream pause where name = video and module = display
        video stream resume where name = video and module = display
        
exit video where name = youtube


```

# Drop example databases
drop table video_table where dbms = video_dbms
drop table detection_table where dbms = video_dbms


# For DISPLAY

# For a local window
# rreplace:
# from external_lib.video_processing.cv2_basic_imshow import ImShow
# with:
import function where import_name = imshow and lib = external_lib.video_processing.cv2_basic_imshow and method = ImShow
get imported functions

    # --------------------------------------------- 

# For a browser
# replace:
# from external_lib.video_processing.cv2_stream_imshow import ImShow
# with:
import function where import_name = imshow and lib = external_lib.video_processing.cv2_stream_imshow and method = init_class
get imported functions

set function params where import_name = imshow and param_name = port and param_type = int and param_value = 8888
set function params where import_name = imshow and param_name = host and param_value = 0.0.0.0
get function params

    # ---------------------------------------------

# For DETECTION

import function where import_name = initiate_yolo and lib = external_lib.frame_modeling.yolo_detection and method = init_class
get imported functions

# Define params passed to init

set function params where import_name = initiate_yolo and param_name = module_type and param_value = darknet
set function params where import_name = initiate_yolo and param_name = classes and param_type = list and param_value = []
set function params where import_name = initiate_yolo and param_name = module_path1 and param_value = https://github.com/AlexeyAB/darknet/releases/download/yolov4/yolov4-tiny.weights
set function params where import_name = initiate_yolo and param_name = module_path2 and param_value = https://raw.githubusercontent.com/AlexeyAB/darknet/refs/heads/master/cfg/yolov4-tiny.cfg
set function params where import_name = initiate_yolo and param_name = coco_path and param_value = https://raw.githubusercontent.com/pjreddie/darknet/master/data/coco.names
get function params

    # ---------------------------------------------


# https://www.youtube.com/watch?v=57w2gYXjRic
# https://www.youtube.com/watch?v=rnXIjl_Rzy4
# address = "https://www.youtube.com/watch?v=57w2gYXjRic" and

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

run video stream where name=youtube and
import_display = imshow
get video streams stats


exit video where name = youtube


# Video Data
sql video_dbms info = (dest_type = rest) and extend=(+country, +city, @ip, @port, @dbms_name, @table_name) and format = json and timezone = utc  select  file,  timestamp::ljust(19)  from video_table  where timestamp > now() - 1 day order by timestamp desc --> selection (columns: ip using ip and port using port and dbms using dbms_name and table using table_name and file using file)
# Detection Data
sql video_dbms info = (dest_type = rest) and extend=(+country, +city, @ip, @port, @dbms_name, @table_name::replace(detection by video)) and format = json and timezone = utc  select  file::str,  timestamp::ljust(19), person, car , truck, bus, from detection_table  where timestamp > now() - 1 day order by timestamp desc limit 5 --> selection (columns: ip using ip and port using port and dbms using dbms_name and table using table_name and file using file)

# Blobs
sql video_dbms extend=(+country, +city, @ip, @port, @dbms_name, @table_name::replace(detection by video)) and format = json and timezone = utc  select  file,timestamp::ljust(19), person, car , truck, bus, from detection_table  order by timestamp desc limit 10 --> selection (columns: ip using ip and port using port and dbms using dbms_name and table using table_name and file using file)



# Streaming
sql video_dbms info = (dest_type = rest) and extend=(+country, +city, @ip, @port, @dbms_name, @table_name) and format = json and timezone = utc  select  file,  timestamp::ljust(19), person, car , truck, bus, from ny_camera  where timestamp > now() - 1 day order by timestamp desc --> selection (columns: ip using ip and port using port and dbms using dbms_name and table using table_name and file using file)
# Blobs
sql video_dbms extend=(+country, +city, @ip, @port, @dbms_name, @table_name) and format = json and timezone = utc  select  file,timestamp::ljust(19), person, car , truck, bus, from ny_camera  order by timestamp --> selection (columns: ip using ip and port using port and dbms using dbms_name and table using table_name and file using file)