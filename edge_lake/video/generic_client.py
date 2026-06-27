plugins_installed_ = True

try:
    installed_lib_ = "InferenceClient"
    from external_lib.frame_modeling.InferenceClient import InferenceClient
except:
    plugins_installed_ = False

from edge_lake.tcpip.grpc_client import get_stub

try:
    installed_lib_ = "av"
    import av
    installed_lib_ = "cv2"
    import cv2
    installed_lib_ = "numpy"
    import numpy as np
except:
    plugins_installed_ = False

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

        # Track worker threads so shutdown logic can coordinate clean joins
        self._capture_thread = None
        self._recorder_thread = None
        self._display_thread = None

        # Guard against multiple threads attempting to tear down resources
        self._shutdown_lock = threading.Lock()
        self._shutdown_started = False

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

                try:
                    self.controller.thread_stream_wait("storage") # Paused until signaled
                except Exception as e:
                    process_msg = f"storage wait failed: {e}"
                    ret_val = process_status.ERR_write_stream
                    break

                try:
                    item = self._rec_q.get(timeout=(0.5 if counter < 10 else 2))
                except Empty:
                    counter += 1
                    continue
                else:
                    counter = 0

                if item is None:  # sentinel for clean shutdown
                    break

                ts_mono, ts_sec, img = item ## monotonic timestamp, frame timestamp, img vector
                try:
                    ret_val = self.recorder.write_frame(status, img, ts_sec, ts_mono)
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

    def get_video_frames(self, ai_module, snapshot_mode: bool = False):
        """
        Get frames from video and push to display/recorder queues.
        Supports video reconnect with 3 retries at 15-second intervals.
        """

        ret_val = process_status.SUCCESS
        status = process_status.ProcessStat()
        process_msg = ""

        # used for sleep process when ai not disabled
        fps = max(1.0, float(self._display_fps))
        interval = 1.0 / fps

        # ---- reconnect policy ----
        max_retries = 3
        retry_interval_s = 15
        retry_count = 0

        # runtime flags
        self.controller.use_ai_models(True if ai_module else False)
        self.streaming = True
        self.streaming_flag = 0
        self.is_terminated = False

        last_decode_ts = None

        while not self.controller.is_exit():
            # Ensure open connection
            if not self.connection:
                open_ret = self.open_connection(status)
                if open_ret:
                    ret_val = process_status.Failed_to_import_lib
                    process_msg = "Video Stream: No connection available"

                    retry_count += 1
                    if retry_count > max_retries:
                        break

                    status.add_error(
                        f"Open connection failed for '{self.get_source_name()}'. "
                        f"Retry {retry_count}/{max_retries} in {retry_interval_s}s..."
                    )
                    waited = 0
                    while waited < retry_interval_s and not self.controller.is_exit():
                        time.sleep(1)
                        waited += 1
                    continue

            try:
                if not getattr(self.connection, "streams", None) or not self.connection.streams.video:
                    raise RuntimeError("Video Stream: No video streams available")

                stream = self.connection.streams.video[0]
                try:
                    stream.thread_type = "AUTO"
                except Exception:
                    pass

                # successful stream attach resets retry counter
                retry_count = 0

                for packet in self.connection.demux(stream):
                    self.controller.thread_stream_wait("stream")
                    if self.controller.is_exit():
                        break

                    for frame in packet.decode():
                        if self.controller.is_exit():
                            break

                        self.named_stats["frames"] += 1

                        # Timestamp (prefer decoder time)
                        if getattr(frame, "time", None) is not None:
                            ts_sec = float(frame.time)
                        elif getattr(frame, "pts", None) is not None and getattr(frame, "time_base", None):
                            ts_sec = float(frame.pts * frame.time_base)
                        else:
                            ts_sec = time.time()

                        ts_mono = time.monotonic()

                        # Enforce non-decreasing
                        if last_decode_ts is not None and ts_sec < last_decode_ts:
                            ts_sec = last_decode_ts
                        last_decode_ts = ts_sec

                        img = frame.to_ndarray(format="bgr24")

                        # Detection
                        if self.controller.with_ai_models():
                            ret_val = self.process_ai(status, ai_module, img, interval)
                            if ret_val:
                                break

                            if len(self.detection_entries) >= 25:
                                self.named_stats["json files"] += 1
                                ret_val = video_recorder.write_predictions(
                                    status,
                                    self.prep_dir,
                                    self.watch_dir,
                                    self.err_dir,
                                    self.detection_dbms,
                                    self.detection_table,
                                    self.source_name,
                                    self.detection_entries,
                                )
                                self.detection_entries.clear()
                                if ret_val:
                                    break

                        # Keep CPU sane (simple pacing)
                        time.sleep(interval)

                        # Display queue: bounded latency (drop oldest)
                        with self._frame_lock:
                            try:
                                self._frame_queue.put_nowait(img)
                            except Full:
                                try:
                                    _ = self._frame_queue.get_nowait()
                                except Empty:
                                    pass
                                try:
                                    self._frame_queue.put_nowait(img)
                                except Full:
                                    pass

                        # Recorder queue: bounded latency (drop oldest)
                        if self.is_recorder():
                            try:
                                self._rec_q.put_nowait((ts_mono, ts_sec, img))
                            except Full:
                                try:
                                    _ = self._rec_q.get_nowait()
                                except Empty:
                                    pass
                                try:
                                    self._rec_q.put_nowait((ts_mono, ts_sec, img))
                                except Full:
                                    pass

                    if ret_val or self.controller.is_exit():
                        break

            except Exception:
                _, err_val = sys.exc_info()[:2]
                status.add_error(
                    f"Video Stream: Failed to stream video using connection '{self.get_source_name()}' error: {err_val}"
                )
                ret_val = process_status.Connection_error

            finally:
                # IMPORTANT: close only after decode loop unwinds (safer for PyAV)
                try:
                    if self.connection:
                        self.connection.close()
                except Exception:
                    pass
                self.connection = None

            if self.controller.is_exit():
                break

            # If stream failed, attempt reconnect
            if ret_val:
                retry_count += 1
                if retry_count > max_retries:
                    status.add_error(
                        f"Reconnect failed for '{self.get_source_name()}' after {max_retries} attempts."
                    )
                    break

                status.add_error(
                    f"Stream error on '{self.get_source_name()}'. "
                    f"Reconnect attempt {retry_count}/{max_retries} in {retry_interval_s}s..."
                )

                waited = 0
                while waited < retry_interval_s and not self.controller.is_exit():
                    time.sleep(1)
                    waited += 1

                # reset ret_val before next reconnect attempt
                ret_val = process_status.SUCCESS
                continue

        # final flush
        try:
            if len(self.detection_entries):
                self.named_stats["json files"] += 1
                _ = video_recorder.write_predictions(
                    status,
                    self.prep_dir,
                    self.watch_dir,
                    self.err_dir,
                    self.detection_dbms,
                    self.detection_table,
                    self.source_name,
                    self.detection_entries,
                )
                self.detection_entries.clear()
        except Exception:
            pass

        self.streaming = False

        time.sleep(0.1)

        exit_msg = f"Video Stream Capture Stopped: '{process_status.get_status_text(ret_val)}'"
        if process_msg:
            exit_msg += ("\n" + process_msg)

        # exit gracefully
        self.graceful_exit(status)

        if ret_val:
            status.add_error(exit_msg)
            color = "red"
        else:
            color = "green"

        utils_print.output_box(exit_msg, color)

    # -----------------------------------------------------------------------------------------
    # Process AI Models
    # -----------------------------------------------------------------------------------------
    def process_ai(self, status, ai_module, img, inference_rate):
        # DO the detection every frame
        detect_status = False
        new_inference = False
        detections_insight = {}
        try:
            # submit inference request to gRPC endpoint
            rid = ai_module.submit_ndarray(img, fmt="bgr24")

            # read latest response (non-blocking) in best effort
            resp = ai_module.get_latest_response()
            # only treat it as new if request_id changed
            if resp is not None and resp.request_id != getattr(self, "last_seen_rid", None):
                self.last_seen_rid = resp.request_id
                detections = resp.detections
                new_inference = True
                detect_status = True

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
                if new_inference: # updates the bounding box if new inference result was received
                    ai_module.update_cached_detections(detections, img)  # builds overlay once
                ai_module.apply_cached_boxes(img)  # applies bounding box overlay onto each frame (cheap)

            except:
                status.add_error( f"Error in 'draw_bounding_box' Method in an external module used with video stream named: '{self.get_source_name()}'")
                ret_val = process_status.Error_external_lib
            else:
                if new_inference:
                    try:
                        # get insights based on bbox difference (tolerance = accuracy)
                        detect_status, detections_insight = ai_module.get_insights(detections=detections,
                                                                                   previous_detections=self.previous_detection,
                                                                                   tolerance=1, check_param='class')
                    except:
                        status.add_error(f"Error in 'get_insights' method in an external module used with video stream named: '{self.get_source_name()}'")
                        ret_val = process_status.Error_external_lib
                        detect_status = False

                    finally:

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

            # Monotonic, drift-free pacing to target FPS (minimal patched catch-up)
            now = time.monotonic()
            next_deadline += interval
            sleep_time = next_deadline - now
            if sleep_time > 0:
                time.sleep(sleep_time)
            else:
                # We're behind; realign to now to prevent runaway lag / fast playback
                next_deadline = now

        ### Cleanup ###
        # check if AnyLog node is running headless or on a machine with a GUI
        if self.opencv_has_gui(status):
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

        # Graceful cleanup close connection and kill fastapi server
        self.graceful_exit(status)

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

        exit_msg = f"Video Stream Display Exited: 'Success'"
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

    def start_stream_video(self, status, external_func, grpc_name=None):
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

                # keep pointer to shutdown fastapi thread
                self.external_display_func = external_func['import_display'][0]

                # setup grpc endpoint for detections
                # throw error if grpc service is not initialized or detection_table not defined
                if grpc_name:
                    error = ''
                    if self.detection_table:
                        inference_stub = get_stub(status, grpc_name)
                        if inference_stub:
                            grpc_endpoint = InferenceClient(status, inference_stub)
                        else:
                            error += f"gRPC service {grpc_name} not initialized. "
                            ret_val = process_status.gRPC_process_failed
                    else:
                        error += f"detection table not defined in `video connect` command."
                        ret_val = process_status.gRPC_process_failed
                else:
                    grpc_endpoint = None

                if ret_val:
                    status.add_error(error)
                    self.graceful_exit(status)

                if not ret_val:
                    self.controller.reset_exit()       # Reset exit flag
                    self.controller.resume_all()       # Start un-paused

                    # Main thread initialized
                    # Start the thread to write the data to file
                    # '''
                    self.recorder = None  # record frames to file
                    self._recorder_thread = threading.Thread(target=self.flush_video_frames, daemon=True)
                    self._recorder_thread.start()
                    # '''

                    self.streaming = True
                    self._capture_thread = threading.Thread(target=self.get_video_frames, args=(grpc_endpoint,), daemon=True)
                    self._capture_thread.start()

                    # Start the Display Thread
                    display_libs =  external_func["import_display"] if "import_display" in external_func else None # Mandatory lib for the frames display
                    if display_libs:
                        self.display = True
                        self._display_thread = threading.Thread(target=self._display_loop, args=(display_libs,), daemon=True)
                        self._display_thread.start()

        return ret_val


    def exit_stream_video(self, status):
        """Signal all worker threads to exit and wait for them to finish."""
        ret_val = process_status.SUCCESS

        # Ensure threads are not blocked in pause mode
        self.controller.resume_event("storage")
        self.controller.resume_event("display")
        self.controller.resume_event("stream")

        self.controller.set_exit()

        threads = [
            getattr(self, "_capture_thread", None),
            getattr(self, "_recorder_thread", None),
            getattr(self, "_display_thread", None),
        ]

        for t in threads:
            if not t:
                continue
            t.join(timeout=5)
            if t.is_alive():
                status.add_error(f"Thread '{t.name}' did not exit cleanly for connection '{self.get_source_name()}'")
                ret_val = process_status.ERR_process_failure

        self._capture_thread = None
        self._recorder_thread = None
        self._display_thread = None
        self.streaming = False

        return ret_val


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


    def graceful_exit(self, status):
        with self._shutdown_lock:
            if self._shutdown_started:
                return process_status.SUCCESS
            self._shutdown_started = True

        ret_val = process_status.SUCCESS
        try:
            # Flush the JSON WITH DATA
            if len(self.detection_entries):
                self.named_stats["json files"] += 1  # How many detections were considered
                ret_val = video_recorder.write_predictions(status, self.prep_dir, self.watch_dir, self.err_dir,
                                                           self.detection_dbms, self.detection_table, self.source_name,
                                                           self.detection_entries)  # Write the streaming data
                self.detection_entries.clear()

            # Ensure all worker threads exit before closing shared resources
            exit_ret = self.exit_stream_video(status)
            if exit_ret:
                ret_val = exit_ret

            # Correct order to kill threads but add sleep to ensure no failure
            if getattr(self, "external_display_func", None):
                try:
                    self.external_display_func.stop_web_app()
                except Exception:
                    status.add_error(f"Failed to stop display server for connection '{self.get_source_name()}'")
                    ret_val = process_status.ERR_process_failure

            self.close_connection(status)

        except Exception:
            ret_val = process_status.ERR_process_failure
            status.add_error(f"Failed to terminate video stream: {self.get_source_name()}")

        return ret_val


    # ---------------------------------------------------------------------
    # Check if AnyLog is runnning in a headless environment (no display)
    # ---------------------------------------------------------------------
    def opencv_has_gui(self, status) -> bool:
        """
        Return True if this OpenCV build appears to include a GUI backend (HighGUI),
        meaning functions like cv2.imshow()/cv2.destroyAllWindows() are likely to work.

        Note:
        - In many Docker/server environments you install `opencv-python-headless`, which
          intentionally omits GUI support. In that case this should return False.
        - This is a heuristic: it inspects OpenCV's build info text for known backends.
        """
        # OpenCV exposes a big, human-readable build summary string (compile options,
        # enabled modules, linked libraries, etc.).
        info = cv2.getBuildInformation()

        # Look for common GUI backends that enable HighGUI window functionality:
        # - GTK / Qt: common on Linux desktops
        # - Cocoa: macOS
        # - Win32 UI: Windows
        #
        # If none of these are present, OpenCV was likely built without window support.
        return any(backend in info for backend in ("GTK", "Qt", "Cocoa", "Win32 UI"))



