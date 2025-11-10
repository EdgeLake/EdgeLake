


#from external_lib.frame_modeling import detection

try:
    installed_lib_ = "av"
    import av
    installed_lib_ = "cv2"
    import cv2
    installed_lib_ = "numpy"
    import numpy as np
except:
    plugins_installed_ = False
else:
    plugins_installed_ = True

import threading
import sys
from queue import Queue, Full, Empty
from datetime import datetime, timezone
import time

import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_print as utils_print
import edge_lake.video.video_recorder as video_recorder
from edge_lake.video.video import VideoStreaming
from edge_lake.video.video_recorder import SegmentedVFRRecorder

def plugins_installed():
    return plugins_installed_

def test_lib_installed(status):
    '''
    return error and update the log if a lib is not installed
    '''
    global plugins_installed_

    if plugins_installed_:
        ret_val = process_status.SUCCESS
    else:
        status.add_error(f"Video library not installed: '{installed_lib_}'")
        ret_val = process_status.Failed_to_import_lib

    return ret_val

"""
:protocol-types:
| Protocol                                              | Typical Latency | Notes / Use Case                                                                                                         | Supported                          |
| ----------------------------------------------------- | --------------- | ------------------------------------------------------------------------------------------------------------------------ | ---------------------------------- |
| **RTMP (Real-Time Messaging Protocol)**               | ~1–2 sec        | Older Adobe Flash-era protocol. Still widely used for *ingesting* streams (e.g., YouTube Live, Twitch, OBS → server).    | ✅                                  |
| **SRT (Secure Reliable Transport)**                   | ~1–2 sec        | Developed by Haivision; secure, low-latency, robust over unreliable networks. Great for professional contribution feeds. | ✅                                  |
| **RTSP (Real-Time Streaming Protocol)**               | ~1–5 sec        | Often used in IP cameras and surveillance systems. Can be converted for web playback.                                    | ✅                                  |
| **HTTP / HTTPS (progressive / HLS)**                  | ~1–5 sec        | Standard HTTP streaming, HLS (m3u8 playlists) or progressive streams.                                                    | ✅                                  |
| **RTMPS**                                             | ~1–2 sec        | Secure RTMP (RTMP over TLS). Often used for cloud services like YouTube Live ingest.                                     | ✅ (if URL + cert valid)            |
| **Local video file (MP4, MOV, MKV, etc.)**            | 0 sec           | Stored video files on disk. Can be read directly by PyAV/FFmpeg.                                                         | ✅                                  |
"""
class GenericVideoStreaming(VideoStreaming):
    def __init__(self, status, source_name, url, user, password,
                 protocol, interface, prep_dir, watch_dir, bwatch_dir, err_dir, video_dir, video_dbms, video_table, detection_dbms, detection_table, detection_columns,
                 recording_segment_time, detection_ignore_time):

        super().__init__(status=status, source_name=source_name, url=url, user=user, password=password,
                         protocol=protocol,
                         interface=interface,
                         prep_dir=prep_dir,
                         watch_dir=watch_dir,
                         bwatch_dir=bwatch_dir,
                         err_dir=err_dir,
                         video_dir=video_dir,
                         video_dbms=video_dbms,
                         video_table=video_table,
                         detection_dbms=detection_dbms,
                         detection_table=detection_table,
                         detection_columns=detection_columns,
                         recording_segment_time = recording_segment_time,
                         detection_ignore_time = detection_ignore_time
                         )

        self._frame_lock = threading.Lock()  # With display and pushing frames to file, there is a race condition on the queue

        self._frame_queue = Queue(maxsize=2)  # small queue to limit buffering

        self._rec_q = Queue(maxsize=120)  # ~4s @30fps; tune as needed

        self.last_timestamp = None          # Last timestamp updated detection
        self.last_detection = {}            # Last detection info

        self.detection_time = None          # The last detection time
        self.previous_detection = None      # The last detection returned from AI inference
        self.detection_entries = []            # The DBMS input rows

        self.recorder = None

    # ----------------------------------------------------------------------------
    # This process continuously writes frames to disk
    # ----------------------------------------------------------------------------
    def flush_video_frames(self):


        ret_val = process_status.SUCCESS
        status = process_status.ProcessStat()
        process_msg = ""

        # ---- Init recorder (5-minute segments, dynamic speed)
        # (without "if"  -  as it may dynamically activated) ----
        self.recorder = SegmentedVFRRecorder(
            dbms_name=self.video_dbms,
            table_name=self.video_table,
            source_name=self.source_name,
            prep_dir = self.prep_dir,
            bwatch_dir=self.bwatch_dir,
            video_dir = self.video_dir,
            err_dir=self.err_dir,
            named_stats=self.named_stats,
            segment_minutes=self.recording_segment_time,     # THE WRITE TIME IN MINUTES
            codec="libx264",
            pix_fmt="yuv420p",
        )

        self.recorder.init_values()      # Restart values if restart

        try:
            counter = 0     # used to wait longer after 10 consecutive attempts
            while not self.controller.is_exit():

                self.controller.thread_stream_wait("storage")       # Paused until signaled

                try:
                    item = self._rec_q.get(timeout=(0.5 if counter < 10 else 2))
                except Empty:
                    counter += 1
                    continue
                else:
                    counter = 0

                if item is None:  # sentinel for clean shutdown
                    break

                ts_sec, img = item
                try:
                    ret_val = self.recorder.write_frame(status, img, ts_sec)
                    if ret_val:  # nonzero = error per your API
                        break
                except:
                    errno, value = sys.exc_info()[:2]
                    process_msg = f'Flush Video Stream Error: Failed to stream video to disk: {value}'
                    ret_val = process_status.ERR_write_stream

                if ret_val:
                    break
        finally:
            self.recorder.close(status=status)
            self.recorder = None


        exit_msg = f"Video Writter Exited: '{process_status.get_status_text(ret_val)}'"
        if process_msg:
            exit_msg += ("\n" + process_msg)

        if ret_val:
            # On Error - update err log
            status.add_error(exit_msg)
            color = "red"
        else:
            color = "green"

        utils_print.output_box(exit_msg, color)

        self.recorder = None  # record frames to file

    # ----------------------------------------------------------------------------
    # This process continuously reads frames from tne video camera
    # It is executed as an independent thread
    # ----------------------------------------------------------------------------
    def get_video_frames(self, external_func, snapshot_mode:bool=False):
        """
        Get frames from video
        Capture and push frames from the video source into the shared queue.
        This thread produces frames as fast as they are decoded, while the display
        thread controls playback speed.
        """
        ret_val = process_status.SUCCESS
        status = process_status.ProcessStat()
        process_msg = ""        # A message to display when the process exists
        previous_detections = None
        previous_insights = None

        # used for sleep process when ai not disabled
        fps = max(1.0, float(self._display_fps))  # guard bad values
        interval = 1.0 / fps

        if not self.connection:
            # Not open - try to re-open
            ret_val = self.open_connection(status)
            if ret_val:
                process_msg = "Video Stream: No connection available"
                ret_val = process_status.Failed_to_import_lib

        if not ret_val:
            try:
                if not getattr(self.connection, "streams", None) or not self.connection.streams.video:
                    process_msg = "Video Stream: No video streams available"
                    ret_val = process_status.Connection_error
                else:
                    if (external_func and len(external_func)):
                        self.controller.use_ai_models(True)
                        ai_module = external_func[0]
                    else:
                        self.controller.use_ai_models(False)


                    self.streaming = True       # Flag - streaming started
                    self.streaming_flag = 0     # This flag is set to -1 on exit and 1 for pause
                    self.is_terminated = False

                    last_decode_ts = None  # seconds, non-decreasing for recorder

                    stream = self.connection.streams.video[0]

                    try:
                        # Helps some RTSP cameras; ignored if not supported
                        stream.thread_type = "AUTO"
                    except Exception:
                        pass

                    for packet in self.connection.demux(stream):

                        self.controller.thread_stream_wait("stream")  # Paused until signaled
                        if self.controller.is_exit():
                            break

                        for frame in packet.decode():

                            """
                            # when detection will be enabled, the call would go here
                            img = frame.to_ndarray(format='bgr24')
                            self.latest_image = img_detection(img) 
                            """

                            self.named_stats["frames"] += 1

                            # --- Timestamp (prefer decoder time) ---
                            if getattr(frame, "time", None) is not None:
                                ts_sec = float(frame.time)  # already in seconds (stream time base)
                            elif getattr(frame, "pts", None) is not None and getattr(frame, "time_base", None):
                                ts_sec = float(frame.pts * frame.time_base)  # seconds
                            else:
                                ts_sec = time.time()  # wall clock fallback

                            # Enforce non-decreasing before passing to recorder
                            if last_decode_ts is not None and ts_sec < last_decode_ts:
                                ts_sec = last_decode_ts
                            last_decode_ts = ts_sec

                            img = frame.to_ndarray(format='bgr24')
                            # This lock is used when this thread pushes frames to the queue and at the same time a different thread pulls
                            # From the queue (to write on disk)

                            #----- New Code: detection ----
                            if self.controller.with_ai_models():
                                # previous_detections, previous_insights = self.process_ai(ai_module, False, img, previous_detections, previous_insights)
                                ret_val = self.process_ai(status, ai_module, img)
                                if ret_val:
                                    break

                                if len(self.detection_entries) >= 25:  #1000:
                                    self.named_stats["json files"] += 1  # How many detections were considered
                                    ret_val = video_recorder.write_predictions(status, self.prep_dir, self.watch_dir, self.err_dir, self.detection_dbms, self.detection_table, self.source_name, self.detection_entries )      # Write the streaming data
                                    self.detection_entries.clear()
                                    if ret_val:
                                        break

                            else:
                                # calculate sleep if AI / ML is disabled
                                sleep_time = ts_sec - last_decode_ts
                                time.sleep(max(interval, sleep_time))

                            # Lock around queue access
                            with self._frame_lock:
                                try:
                                    self._frame_queue.put_nowait(img)
                                except Full: # cleanup queue
                                    _ = self._frame_queue.get_nowait()      # REMOVE THE OLDEST TO FREE SPACE
                                    self._frame_queue.put_nowait(img)       # ADD the new frame to the queue

                            if self.is_recorder():

                                # Enqueue for recorder (bounded latency: drop-then-put)
                                try:
                                    self._rec_q.put_nowait((ts_sec, img))
                                except Full:
                                    try:
                                        _ = self._rec_q.get_nowait()
                                    except Empty:
                                        pass
                                    try:
                                        self._rec_q.put_nowait((ts_sec, img))
                                    except Full:
                                        pass  # still saturated; skip this frame

                        if ret_val:
                            break


            except:
                err_num, err_val = sys.exc_info()[:2]
                err_msg = f"Video Stream: Failed to stream video using connection '{self.get_source_name()}' error: {err_val}"
                status.add_error(err_msg)
                ret_val = process_status.Connection_error
            finally:

                # Flush the JSON IF WITH DATA
                if len(self.detection_entries):
                    self.named_stats["json files"] += 1  # How many detections were considered
                    ret_val = video_recorder.write_predictions(status, self.prep_dir, self.watch_dir, self.err_dir,
                                                       self.detection_dbms, self.detection_table, self.source_name,
                                                       self.detection_entries)  # Write the streaming data
                    self.detection_entries.clear()


                self.streaming = False              # Streaming terminated
                self.controller.set_exit()          # This flag will exit all threads
                self.controller.resume_all()        # exit all threads from pause mode

                if not ret_val:
                    ret_val = self.wait_for_threads_termination(status)  # wait for display and recorder thread to terminate before socket close
                    if not ret_val:
                        # ALl other threads using the stream terminated
                        self.close_connection(status)

        time.sleep(0.1)  # small sleep to avoid tight-loop if demux ends


        exit_msg = f"Video Stream Capture Stopped: '{process_status.get_status_text(ret_val)}'"
        if process_msg:
            exit_msg += ("\n" + process_msg)

        if ret_val:
            # On Error - update err log
            status.add_error(exit_msg)
            color = "red"
        else:
            color = "green"

        utils_print.output_box(exit_msg, color)


    # -----------------------------------------------------------------------------------------
    # Process AI Models
    # -----------------------------------------------------------------------------------------
    def process_ai(self, status, ai_module, img):

        # DO the detection
        try:
            detect_status, detections = ai_module.detect(frame=img)  # New detection
        except:
            status.add_error(f"Error in 'detect' Method in an external module used with video stream named: '{self.get_source_name()}'")
            ret_val = process_status.Error_external_lib
            detect_status = False # Failed to detect
        else:

            ret_val = process_status.SUCCESS


        if not ret_val and detect_status:
            # No error and detection was done with detection info

            # DO the bounding boxes
            try:
                ai_module.draw_bounding_box(detections=detections, frame=img)
            except:
                status.add_error( f"Error in 'draw_bounding_box' Method in an external module used with video stream named: '{self.get_source_name()}'")
                ret_val = process_status.Error_external_lib
            else:

                try:
                    # get insights based on bbox difference (tolerance = accuracy)
                    detect_status, detections_insight = ai_module.get_insights(detections=detections,
                                                                     previous_detections=self.previous_detection,
                                                                     tolerance=1, check_param='class')
                except:
                    status.add_error(f"Error in 'get_insights' method in an external module used with video stream named: '{self.get_source_name()}'")
                    ret_val = process_status.Error_external_lib
                    detect_status = False

                if detect_status and len(detections_insight):

                    self.named_stats["detections"] += 1

                    self.previous_detection = detections          # Save current detection

                    detection_instance = self.get_detection_struct()

                    for key, value in detections_insight.items():
                        # get key and value from detection
                        # Update detection_instance if column is needed
                        if key in detection_instance:
                            detection_instance[key] = value     # Update new struct

                    if self.recorder:
                        # If the recorder was initiated
                        try:
                            file_name = self.recorder.get_file_name()
                        except:
                            pass        # The existing file is shut
                        else:
                            if file_name:
                                # If recorder is not hosting file data - skip
                                current_ts = time.time()
                                utc_str = datetime.fromtimestamp(current_ts, tz=timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

                                # figure if there is a difference or more than 10 seconds elapsed
                                if not len(self.detection_entries) or (detection_instance != self.last_detection or (current_ts - self.detection_time) > self.detection_ignore_time):
                                    # 1) If first entry, or
                                    # 2) if self.detection_ignore_time is 10, wait for 10 seconds elapsed if detection is like previous
                                    # 3) a difference in the detection

                                    self.last_detection = detection_instance.copy()     # Used to compare against the next detection to avoid repeatable data
                                    detection_instance["file"] = file_name  # The name of the file that hosts the video
                                    detection_instance["timestamp"] = utc_str

                                    self.detection_entries.append(detection_instance)  # first instance

                                    self.named_stats["considered"] += 1     # How many detections were considered

                                self.detection_time = current_ts            # save current timestamp

        return ret_val


    # -----------------------------------------------------------------------------------------
    # Process AI Models
    # -----------------------------------------------------------------------------------------
    def _display_loop(self, display_libs):
        """Keep window responsive while CLI controls show/hide.
        Continuously retrieves frames from the queue and displays them in a window at the target FPS.
        Uses time.monotonic() f or drift-free timing, keeps the GUI responsive with cv2.waitKey(1),
        handles missing frames gracefully, and stops cleanly when display_flag is set or the window is closed.
        """
        status = process_status.ProcessStat()
        display_library = display_libs[0]

        fps = max(1.0, float(self._display_fps))  # guard bad values
        interval = 1.0 / fps
        self.display = True
        self.display_flag = 0

        window_title = f"Stream Source - {self.source_name.title()}"
        next_deadline = time.monotonic()

        ret_val = process_status.SUCCESS
        err_msg = ""

        last_frame = None  # keep last good frame for smoother display

        while not self.controller.is_exit():

            self.controller.thread_stream_wait("display")  # Paused until signaled

            try:
                # Try to get a fresh frame; time out at the frame interval
                frame = self._frame_queue.get(timeout=interval)
                last_frame = frame
            except Empty:
                frame = None
            except Exception as e:
                status.add_error(f"Frame pull from queue error for '{self.get_source_name()}': {e}")
                frame = None

            # --- minimal patch: drop backlog to keep real-time feel ---
            # If more than one frame is queued, discard older ones.
            while getattr(self._frame_queue, "qsize", lambda: 0)() > 1:
                try:
                    _ = self._frame_queue.get_nowait()
                except Empty:
                    break
            # ----------------------------------------------------------

            # Display either the new frame or the last frame if available
            to_show = frame if frame is not None else last_frame
            if to_show is not None:
                ret_val, err_val = display_library.imshow(window_title=window_title, source_name=self.get_source_name(), frame=to_show)
                if not ret_val and err_val == 'break':
                    break
                if ret_val:
                    status.add_error(f"Failed to display video for '{self.get_source_name()}': {err_val}")
                # try:
                #     cv2.imshow(window_title, to_show)
                #     # Keep GUI responsive; also allow quit via 'q' or ESC
                #     key = cv2.waitKey(1) & 0xFF
                #     if key in (27, ord('q')):
                #         break
                #
                #     # Detect manual window close
                #     if cv2.getWindowProperty(window_title, cv2.WND_PROP_VISIBLE) < 1:
                #         break
                # except Exception as e:
                #     status.add_error(f"Failed to display video for '{self.get_source_name()}': {e}")
            # else:
                # No frame yet; still pump GUI so window doesn't freeze
                # cv2.waitKey(1)

            # Monotonic, drift-free pacing to target FPS (minimal patched catch-up)
            now = time.monotonic()
            next_deadline += interval
            sleep_time = next_deadline - now
            if sleep_time > 0:
                time.sleep(sleep_time)
            else:
                # We're behind; realign to now to prevent runaway lag / fast playback
                next_deadline = now

        # Cleanup
        try:
            cv2.destroyWindow(window_title)
        except Exception:
            try:
                cv2.destroyAllWindows()
            except:
                errno, value = sys.exc_info()[:2]
                err_msg = f"Failed to close video window using connection '{self.get_source_name()}' error: {value}"
                status.add_error(err_msg)
                ret_val = process_status.ERR_process_failure

        self.display = False

        exit_msg = f"Video Stream display Stopped: '{process_status.get_status_text(ret_val)}'"
        if err_msg:
            exit_msg += ("\n" + err_msg)

        if ret_val:
            # On Error - update err log
            status.add_error(exit_msg)
            color = "red"
        else:
            color = "green"

        utils_print.output_box(exit_msg, color)

        exit_msg = f"Video Stream Display Exited"
        utils_print.output_box(exit_msg, "green")

    def open_connection(self, status):
        """Open stream."""

        if plugins_installed():
            try:
                self.connection = av.open(self.conn_url, options=self.configs, timeout=30)
            except:
                errno, value = sys.exc_info()[:2]
                status.add_error(f'Failed to establish connection with error: {value}')
                ret_val = process_status.Connection_error
            else:
                ret_val = process_status.SUCCESS
        else:
            status.add_error(f"Video library not installed: '{installed_lib_}'")
            ret_val = process_status.Failed_to_import_lib

        return ret_val

    def close_connection(self, status):
        try:
            if self.is_connected():
                self.connection.close()
        except:
            pass
        finally:
            self.connection = None
    def start_stream_video(self, status, external_func):
        """
        Initiating 2 threads - the first is a thread to capture video and the second is to flush the video to the file
        Start stream video thread
        """

        ret_val = test_lib_installed(status)
        if not ret_val:

            if self.streaming or self.recorder or self.display:
                status.add_error(f"Video Stream '{self.source_name}': Process is already running - use 'get connected streams' command to determine the status or call 'exit video' command")
                ret_val = process_status.Process_already_running
            else:

                self.controller.reset_exit()       # Reset exit flag
                self.controller.resume_all()       # Start un-paused

                # Main thread initialized
                # Start the thread to write the data to file
                # '''
                self.recorder = None  # record frames to file
                t = threading.Thread(target=self.flush_video_frames, daemon=True)
                t.start()
                # '''

                # Start the Video capture thread (with user libraries for detection)

                detect_libs =  external_func["import_detect"] if "import_detect" in external_func else None # Optional detection

                self.streaming = True
                t = threading.Thread(target=self.get_video_frames, args=(detect_libs,), daemon=True)
                t.start()

                # Start the Display Thread

                display_libs =  external_func["import_display"] if "import_display" in external_func else None # Mandatory lib for the frames display
                if display_libs:
                    self.display = True
                    t = threading.Thread(target=self._display_loop, args=(display_libs,), daemon=True)
                    t.start()

        return ret_val


    def exit_stream_video(self, status):
        """
        set streaming as disabled
        """
        if self.controller.is_exit():
            status.add_error(f"Streaming in connection '{self.get_source_name()}' is not enabled")
            ret_val = process_status.CAMERA_THREAD_NOT_ACTIVE
        else:
            # Flag the streaming thread


            self.controller.resume_event("storage")   # Exit from Pause (if applicable)
            self.controller.resume_event("display")  # Exit from Pause (if applicable)
            self.controller.resume_event("stream")  # Exit from Pause (if applicable)

            self.controller.set_exit()        # This flag will exit all threads

            time.sleep(1)

            # Wait for stream exit - stream threads waits for recorder and display threads to exit
            ret_val = self.wait_for_value_change(status=status, attr="streaming", new_value=False,
                                                 err_code=process_status.ERR_process_failure,
                                                 error_msg="Failed to terminate recorder")



        return  ret_val


    # ---------------------------------------------------------------------------
    # Wait for the recorder and display to terminate
    # ---------------------------------------------------------------------------
    def wait_for_threads_termination(self, status):
        # Wait for recorder exit
        ret_val = self.wait_for_value_change(status=status, attr="recorder", new_value=False,
                                             err_code=process_status.ERR_process_failure,
                                             error_msg="Failed to terminate recorder")
        if not ret_val:
            # Wait for stream exit
            ret_val = self.wait_for_value_change(status=status, attr="display", new_value=False,
                                                 err_code=process_status.ERR_process_failure,
                                                 error_msg="Failed to terminate display")

        return ret_val

    # ---------------------------------------------------------------------------
    # Wait for the value change
    # ---------------------------------------------------------------------------
    def wait_for_value_change(self, status, attr, new_value, pool= 1, max_loops = 10, err_code = 0, error_msg = ""):
        '''
        attr - the name of the attribute to validate
        new_value - the new value of the attribute
        pool - thread sleep time in seconds
        max_loops - determines the max wait time in seconds
        '''
        ret_val = process_status.SUCCESS
        counter = 0
        while getattr(self, attr) != new_value:
            time.sleep( pool )
            counter += 1
            if counter >= 10:
                status.add_error(f'Video error for connection: {self.get_source_name()} ' + error_msg)
                ret_val = err_code
                break
        return ret_val





