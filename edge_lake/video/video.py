
from abc import ABC, abstractmethod
import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_io as utils_io
import edge_lake.video.video_controller as controller   # flags to stop and resume


declared_connections_ = {}   # A dictionary with a list of connections that were created - this is used for performance optimization

statistics_ = {}

def get_stats(): return statistics_

class VideoStreaming(ABC):
    def __init__(self, status, source_name, url, user, password, protocol,
                 interface, prep_dir, watch_dir, bwatch_dir, err_dir, video_dir, video_dbms, video_table, detection_dbms, detection_table, detection_columns,
                 recording_segment_time, detection_ignore_time):
        global statistics_
        """
        Abstract class for video streaming
        :args:
            source_name:str - source name
            url:str - service url
            user:str, password:str - access credentials
            protocol:str - streaming type (view protocol-types for list of options)
            interface:str - data transport protocol (usually TCP or UDP)
            video_dir:str - video directory used to write the video streams
        :params:
            self.source_name
            self.url - URL for getting feed
            self.url:str - URL path
            self.connections:av.open - connection to video server
            self.configs:dict - service configuration options

            self.invalid_value:bool - for RTSP, invalid protocol type
        :protocol-types:
        | Protocol                                              | Typical Latency | Notes / Use Case                                                                                                         | Supported                          |
        | ----------------------------------------------------- | --------------- | ------------------------------------------------------------------------------------------------------------------------ | ---------------------------------- |
        | **WebRTC**                                            | < 500 ms        | Browser-native real-time streaming. Ideal for interactive apps (Zoom, Google Meet, etc.). Peer-to-peer or SFU-based.     | ⚪ (requires aiortc / conversion)   |
        | **RTMP (Real-Time Messaging Protocol)**               | ~1–2 sec        | Older Adobe Flash-era protocol. Still widely used for *ingesting* streams (e.g., YouTube Live, Twitch, OBS → server).    | ✅                                  |
        | **SRT (Secure Reliable Transport)**                   | ~1–2 sec        | Developed by Haivision; secure, low-latency, robust over unreliable networks. Great for professional contribution feeds. | ✅                                  |
        | **RIST (Reliable Internet Stream Transport)**         | ~1–2 sec        | Broadcast-grade, open standard alternative to SRT. Used in TV production.                                                | ⚪ (requires external RIST library) |
        | **RTSP (Real-Time Streaming Protocol)**               | ~1–5 sec        | Often used in IP cameras and surveillance systems. Can be converted for web playback.                                    | ✅                                  |
        | **WHIP / WHEP (WebRTC-HTTP Ingest/Egress Protocols)** | < 500 ms        | Emerging standards for connecting WebRTC streams to media servers or CDNs easily.                                        | ⚪ (requires WebRTC integration)    |
        | **HTTP / HTTPS (progressive / HLS)**                  | ~1–5 sec        | Standard HTTP streaming, HLS (m3u8 playlists) or progressive streams.                                                    | ✅                                  |
        | **MPEG-DASH**                                         | ~1–5 sec        | Adaptive streaming via `.mpd` manifests, mainly used for web VOD or live streams.                                        | ⚪ (not natively supported)         |
        | **RTMPS**                                             | ~1–2 sec        | Secure RTMP (RTMP over TLS). Often used for cloud services like YouTube Live ingest.                                     | ✅ (if URL + cert valid)            |
        | **Local video file (MP4, MOV, MKV, etc.)**            | 0 sec           | Stored video files on disk. Can be read directly by PyAV/FFmpeg.                                                         | ✅                                  |
        """

        self.controller = controller.Controller()

        self.source_name = source_name
        self.conn_url = url             # The URL to the camera
        self.user = user

        self.streaming = False # Determines if frames are captured from the camera

        self.connection = None
        self.configs = {}

        self.protocol = protocol.lower()
        self.interface = interface

        self._display_fps = 30  # target display fps - Frames Per Second

        self.display = False      # Flag display thread is active
        self.recorder = False    # Flag recorder thread is active

        self.prep_dir = prep_dir    # Whrer the inferences are wrotten + Where the JSON file describing the blob is written
        self.watch_dir = watch_dir    # The inference JSON files are moved to theis directory
        self.bwatch_dir = bwatch_dir  # The blob files are moved to theis directory
        self.err_dir = err_dir
        self.video_dir = video_dir     # The blobs_dir

        self.video_dbms = video_dbms    # The dbms to maintain the info on each video (the blobs dbms is blobs_[video_dbms]
        self.video_table = video_table  # The table to maintain the info on each video

        self.detection_dbms = detection_dbms    # The dbms to maintain the detection info
        self.detection_table = detection_table   # The table to maintain the detection info


        self.columns = detection_columns      # A list of the columns names used in the detection table

        self.detection_instance = {
            "timestamp" : None
        }
        if detection_columns:
            for col_name in detection_columns:
                self.detection_instance[col_name] = 0
        self.detection_instance["file"] = ""        # The name of the video file


        # Set configs as f(protocol)
        if self.protocol in ['rtsp', 'rtmp', 'http', 'https', 'hls']:
            self.configs['fflags'] = 'nobuffer'

        if self.protocol in ['rtsp', 'rtmp']:
            self.configs['max_delay'] = '0'

        if self.protocol == 'rtsp':
            self.configs['rtsp_transport'] = interface # TCP or UDP
            self.configs['stimeout'] = '5000000' # 5 seconds
        elif self.protocol == 'srt':
            self.configs['timeout'] = '5000'
            self.configs['latency'] = '200'
        elif self.protocol in ['http', 'https', 'hls']:
            self.configs['rw_timeout'] = '5000000'

        statistics_[self.source_name] = {
            "Video DBMS" : self.video_dbms,
            "Video Table" : self.video_table,
            "Detection DBMS": self.detection_dbms,
            "Detection Table": self.detection_table,
            "frames" : 0,
            "detections" : 0,       # How many detections done
            "considered": 0,        # How many detections considered (not as the previous one)
            "json files" :  0,      # How many JSON files were processed (to SQL)
            "video files" : 0,      # How many Video files were written
            "error"       : "",     # Last error identified
        }

        self.named_stats = statistics_[self.source_name]

        self.recording_segment_time = recording_segment_time        # The length in time of each recording on disk (in minutes)
        self.detection_ignore_time = detection_ignore_time          # Ignore identical detections within the time frame (in seconds)

    def get_video_dbms(self): return self.video_dbms # The dbms to maintain the info on each video (the blobs dbms is blobs_[video_dbms]
    def get_video_table(self): return self.video_table  # The table to maintain the info on each video
    def get_detection_dbms(self): return self.detection_dbms  # The dbms to maintain the detection info
    def get_detection_table(self): return self.detection_table  # The table to maintain the detection info

    def get_recording_segment_time(self): return self.recording_segment_time    # The length in time of each recording on disk (in minutes)
    def get_detection_ignore_time(self): return self.detection_ignore_time      # Ignore identical detections within the time frame (in seconds)

    def get_detection_struct(self):
        # Get a new prediction instance
        return self.detection_instance.copy()
    def is_connected(self):
        return True if self.connection else False  # If stream is connected

    def get_display_fps(self):
        # return Frames Per Second (FPS)
        return self._display_fps

    def is_recorder(self):
        # Return True to record frames to file
        return self.recorder

    def get_conn_url(self):
        """
        Return the Camera URL
        """
        return self.conn_url
    def get_source_name(self):
        """
        Return the connection name
        """
        return self.source_name
    def get_protocol(self):
        """
        Return the protocol name
        """
        return self.protocol
    def get_interface(self):
        """
        Return the interface name
        """
        return self.interface

    def get_threads_stat(self):
        '''
        return status of all threads
        '''
        if not self.streaming:
            # streaming not enabled
            status_table = ["Not Running", "Not Running", "Not Running"]
        else:
            status_table =  [
                ("running" if event.is_set() else "paused")
                for event in self.controller.modules.values()
            ]

        return status_table

    @abstractmethod
    def open_connection(self, status):
        """Open stream."""
        ret_val = process_status.NOT_SUPPORTED
        return ret_val

    @abstractmethod
    def close_connection(self, status):
        pass

    """
    start video stream (thread) 
    """
    @abstractmethod
    def start_stream_video(self, status, external_func):
        """Get frames from video."""
        ret_val = process_status.NOT_SUPPORTED
        return ret_val

    """
    stop video stream (thread) 
    """
    @abstractmethod
    def exit_stream_video(self, status):
        """stop video frame processing"""
        ret_val = process_status.NOT_SUPPORTED
        return ret_val

    """
    check stream time
    """
    # @abstractmethod
    # def stream_time(self, status):
    #     """return stream uptime"""
    #     ret_val = process_status.NOT_SUPPORTED
    #     return ret_val

    def test_dir_exists(self, status):
        # Test bwatch prep and blobs dir exists

        ret_val = utils_io.test_dir_exists(status, self.prep_dir, True)
        if not ret_val:
            ret_val = utils_io.test_dir_exists(status, self.bwatch_dir, True)
            if not ret_val:
                ret_val = utils_io.test_dir_exists(status, self.video_dir, True)
                if not ret_val:
                    ret_val = utils_io.test_dir_exists(status, self.watch_dir, True)
        return ret_val

