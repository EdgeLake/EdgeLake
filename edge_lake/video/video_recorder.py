"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import os, time, datetime
import sys
import json
from fractions import Fraction

try:
    installed_lib_ = "av"
    import av
except Exception:
    plugins_installed_ = False
else:
    plugins_installed_ = True

import edge_lake.generic.process_status as process_status
import edge_lake.generic.utils_data as utils_data
import edge_lake.generic.utils_io as utils_io
import edge_lake.generic.streaming_data as streaming_data


# ----------------------------------------------------------------------------------------
# Write the video file to the blobs dir
# Write a JSON entry to the bwatch_dir, this entry would push the video into the storage
# ----------------------------------------------------------------------------------------
class SegmentedVFRRecorder:
    """
    Variable-frame-rate (VFR) recorder with rolling segments.
    Each segment is a new MP4 named by its start time.
    """
    def __init__(self, dbms_name, table_name, source_name, prep_dir, bwatch_dir, video_dir, err_dir, named_stats, segment_minutes, codec="libx264", pix_fmt="yuv420p"):

        self.dbms_name = dbms_name
        self.table_name = table_name
        self.source_name = source_name
        self.prep_dir = prep_dir        # Where the JSON file describing the blob is written
        self.bwatch_dir = bwatch_dir
        self.video_dir = video_dir  # Where the video is written to be pushed to storage
        self.err_dir = err_dir
        self.named_stats = named_stats      # Statistics on this process
        self.segment_seconds = int(segment_minutes * 60)
        self.codec = codec
        self.pix_fmt = pix_fmt
        self.init_values()

        self.prefix_name = f"{self.dbms_name}.{self.table_name}"



    def init_values(self):
        # init values for restart

        # ... your existing fields ...
        self._pkt_buf = []  # pending encoded packets
        self._pkt_buf_max = 12  # flush to disk every ~12 packets (tune)
        self._last_flush_mono = 0.0  # optional time-based flush anchor
        self._flush_interval = 0.25  # seconds (time-based flush, optional)

        self.container = None
        self.stream = None
        self.segment_start_ts = None
        self.segment_start_dt = None
        self.last_pts = None
        self.time_base = Fraction(1, 1000)  # milliseconds (stable for VFR)
        self.current_out_path = None
        self.frames_written = 0
        self._use_wallclock = False
        self._pts_origin = 0.0
        self._wall_start = 0.0
        self._wall_start_mono = 0.0  # NEW: monotonic anchor for wallclock mode

        # Expect width/height to be set by caller before first write/open.
        self.width = None
        self.height = None
        self.file_name = None       # The file name without dbms and table
        self.file_time = None
        self.real_time = None
        self.disk_file_name = None  # The disk file name (including dbms and table name)



    def get_file_name(self): return self.file_name  # Return the name of the file to be written

    def _open_new_segment(self, status, start_ts: float):
        """
        Open a new MP4 segment for VFR recording.
        """
        if not plugins_installed_:
            status.add_error(f"[Video Recorder] Video library not installed: '{installed_lib_}'")
            return process_status.Failed_to_import_lib if hasattr(process_status,
                                                            "Plugin_missing") else process_status.Config_Error

        # --- Validate geometry ---
        if not getattr(self, "width", None) or not getattr(self, "height", None):
            status.add_error("[Video Recorder] width/height not set before opening segment.")
            return process_status.Config_Error

        ## Reset mux timestamp state for each new segment
        self._pkt_buf.clear()
        self._last_mux_dts = None
        self.last_pts = None
        self.last_dts = None
        self.last_pkt_pts = None

        enc_w = int(self.width)
        enc_h = int(self.height)

        # Enforce even dims for yuv420p
        pixfmt = getattr(self, "pix_fmt", "yuv420p")
        if pixfmt == "yuv420p":
            enc_w &= ~1
            enc_h &= ~1

        # Encoder params (define BEFORE try so they're in scope for except messages)
        codec = getattr(self, "codec", "libx264")
        tb = getattr(self, "time_base", None) or Fraction(1, 1000)  # 1ms ticks


        # --- Build filename from wall-clock (not ts source) ---
        # wall_now = time.time()
        wall_now = time.monotonic() # set monotonic time
        wall_dt = datetime.datetime.fromtimestamp(wall_now, tz=datetime.timezone.utc)

        # THIS part of the name is used in the detection to identify the file (without dbms + table)
        # This file with this name is written to the video (blobs) dir
        self.real_time = wall_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'
        self.file_time =  wall_dt.strftime(f"%Y-%m-%d_%H-%M-%S")        # For file name - seperation using : will not work
        self.file_name = f"{self.source_name}.{self.file_time}" + ".mp4"
        # THIS part of the name is used to be on disk (adding dbms and table names)
        self.disk_file_name =  f"{self.prefix_name}.{self.file_name}"
        out_path = os.path.join(self.video_dir, self.disk_file_name)

        # --- Open container & configure stream ---
        try:
            self.container = av.open(out_path, mode="w", format="mp4", options={"movflags": "+faststart"})
            self.stream = self.container.add_stream(codec, rate=None)  # VFR

            # Stream configuration
            self.stream.width = enc_w
            self.stream.height = enc_h
            self.stream.pix_fmt = pixfmt
            self.stream.time_base = tb

            # Defensive: mirror into codec context (helps some builds)
            cc = self.stream.codec_context
            cc.width = enc_w
            cc.height = enc_h
            cc.pix_fmt = pixfmt
            cc.time_base = tb
            if hasattr(cc, "gop_size"):
                cc.gop_size = 30

        except Exception as e:
            # Clean up any half-open container
            try:
                if getattr(self, "container", None):
                    self.container.close()
            except Exception:
                pass
            self.container = None
            self.stream = None
            status.add_error(
                f"[Video Recorder] Failed to open/configure stream (codec='{codec}', pix_fmt='{pixfmt}'): {e}")
            return process_status.File_open_failed

        # --- Commit segment state only AFTER successful open/config ---
        self.current_out_path = out_path
        self.segment_start_dt = wall_dt
        self.last_pts = None
        self.frames_written = 0
        self.time_base = tb

        # Choose timestamp domain for this segment
        self._use_wallclock = (start_ts is None) or (start_ts > 1e6) or (start_ts <= 0)
        if self._use_wallclock:
            self._wall_start = wall_now
            self._wall_start_mono = time.monotonic()
            self._pts_origin = 0.0
        else:
            self._wall_start = 0.0
            self._wall_start_mono = 0.0
            self._pts_origin = float(start_ts)

        # For rotation/logging only (not used for PTS math)
        self.segment_start_ts = wall_now

        # Update stored geometry to match enforced values
        self.width = enc_w
        self.height = enc_h

        # Initialize flush timer so first buffer flush doesn't trigger immediately
        self._last_flush_mono = time.monotonic()

        return process_status.SUCCESS

    def _close_segment(self, status):
        """Safely flush buffered packets, flush encoder, and close/move the active segment."""
        ret_val = process_status.SUCCESS

        filename = self.file_name
        # Prevent detections from being associated with a file mid-rotation
        self.file_name = None

        if not self.container:
            # Reset state to be safe
            self.stream = None
            self.segment_start_ts = None
            self.last_pts = None
            self.current_out_path = None
            self.frames_written = 0
            # Clear any lingering buffered packets
            if hasattr(self, "_pkt_buf"):
                self._pkt_buf.clear()
            return ret_val

        try:
            # 1) Flush any buffered (already-encoded) packets first
            try:
                if hasattr(self, "_flush_pkt_buffer"):
                    ret_val = self._flush_pkt_buffer(status)
                    if ret_val:
                        # Error
                        return ret_val
                else:
                    # Failsafe: inline flush if helper wasn't added
                    if getattr(self, "_pkt_buf", None):
                        tb = self.stream.time_base
                        for pkt in self._pkt_buf:
                            pkt.time_base = tb
                            self.container.mux(pkt)
                        self._pkt_buf.clear()
            except Exception:
                errno, value = sys.exc_info()[:2]
                status.add_error(f'[Video Recorder] Buffered mux failed during close: {value}')
                return process_status.File_close_failed

            # 2) Flush the encoder (writes delayed frames such as B-frames)
            try:
                if self.stream is not None:
                    tb = self.stream.time_base
                    for pkt in self.stream.encode():
                        pkt.time_base = tb
                        self.container.mux(pkt)
                        self.frames_written += 1  # <-- count only after successful mux

            except Exception:
                errno, value = sys.exc_info()[:2]
                status.add_error(f'[Video Recorder] Encode/flush AVError: {value}')
                ret_val = process_status.File_close_failed

            # 3) Close the container (writes MP4 moov/trailer)
            try:
                self.container.close()
            except Exception:
                errno, value = sys.exc_info()[:2]
                status.add_error(f'[Video Recorder] Container close AVError: {value}')
                ret_val = process_status.File_close_failed
            else:
                # Delete empty file to avoid corrupt stubs
                try:
                    if getattr(self, "frames_written", 0) == 0 and getattr(self, "current_out_path", None):
                        try:
                            os.remove(self.current_out_path)
                        except FileNotFoundError:
                            pass
                        except Exception as e:
                            status.add_error(
                                f"[Video Recorder] Failed to delete empty segment '{self.current_out_path}': {e}")
                except Exception:
                    pass

                # Write a JSON file describing the video in the prep_dir and move to bwatch_dir
                file_size = utils_io.get_file_size(status, self.video_dir + os.sep + self.disk_file_name)

                blob_descriptor = {
                                "timestamp": self.real_time,
                                "name": self.source_name,
                                "minutes": self.segment_seconds /60,
                                "file": filename,
                                "file_size" : file_size,
                                "is_deleted" : '0',
                                }

                file_data = json.dumps(blob_descriptor)
                hash_value = utils_data.get_string_hash('md5', file_data, self.dbms_name + self.table_name)  # Get the Hash of the data that is written to a file

                ret_val = streaming_data.write_by_prep_move(status, self.prep_dir, self.bwatch_dir, self.err_dir, self.dbms_name,
                                                  self.table_name, self.source_name, hash_value, '0',
                                                  None, file_data, "json" )

                self.named_stats["video files"] += 1



        except Exception:
            errno, value = sys.exc_info()[:2]
            status.add_error(f'[Video Recorder] Write Frame failed using Mux (close path): {value}')
            ret_val = process_status.File_close_failed
        finally:
            # Reset per-segment state
            self.container = None
            self.stream = None
            self.segment_start_ts = None
            self.last_pts = None
            self.current_out_path = None
            self.frames_written = 0
            # Clear any remaining buffered packets
            if hasattr(self, "_pkt_buf"):
                self._pkt_buf.clear()
            if hasattr(self, "_last_flush_mono"):
                try:
                    self._last_flush_mono = time.monotonic()
                except Exception:
                    pass

        return ret_val

    def write_frame(self, status, img_bgr, ts_sec: float, ts_mono: float):
        """
        Write a single frame (BGR ndarray) with a timestamp in seconds.
        Handles segment rotation and PTS assignment (VFR-safe).
        Enforces monotonicity for frame PTS AND encoded packet DTS/PTS.
        """
        if not plugins_installed_:
            status.add_error(f"[Video Recorder] Video library not installed: '{installed_lib_}'")
            return process_status.Failed_to_import_lib if hasattr(process_status,
                                                                  "Plugin_missing") else process_status.Config_Error

        if ts_sec is None:
            status.add_error("[Video Recorder] write_frame called with ts_sec=None")
            return process_status.Config_Error

        # --- 1) Open or rotate segment ---
        if self.segment_start_ts is None:
            if self.width is None or self.height is None:
                h, w = img_bgr.shape[:2]
                # enforce even dims for yuv420p
                if (self.pix_fmt or "yuv420p") == "yuv420p":
                    w &= ~1
                    h &= ~1
                self.height, self.width = h, w
            ret = self._open_new_segment(status, ts_sec)
            if ret:
                return ret
        else:
            elapsed = self._compute_rel_sec(ts_mono) # compute monotonic timestamp difference for closing
            if elapsed >= (self.segment_seconds - 1e-6):
                # flush buffer before closing
                self._flush_pkt_buffer(status)
                ret = self._close_segment(status)
                # Clear buffer after close and before open
                self._pkt_buf.clear()
                if ret:
                    return ret
                ret = self._open_new_segment(status, ts_sec)
                if ret:
                    return ret

        # --- 2) Reopen safeguard ---
        if self.segment_start_ts is not None and (not self.container or not self.stream):
            ret = self._open_new_segment(status, ts_sec)
            if ret:
                status.add_error("[Video Recorder] Failed to reopen after missing container/stream")
                return ret

        # Ensure we have packet timestamp trackers
        # (initialize in __init__ ideally)
        if not hasattr(self, "last_dts") or self.last_dts is None:
            self.last_dts = None  # type: ignore
        if not hasattr(self, "last_pkt_pts") or self.last_pkt_pts is None:
            self.last_pkt_pts = None  # type: ignore

        # --- 3) Encode & buffer packets ---
        try:
            vf = av.VideoFrame.from_ndarray(img_bgr, format="bgr24")
            vf = vf.reformat(self.stream.width, self.stream.height, format=self.pix_fmt)

            tb = self.stream.time_base  # Fraction

            # Compute frame PTS in stream.time_base units
            # rel_sec = self._compute_rel_sec(ts_sec)
            rel_sec = self._compute_rel_sec(ts_mono)
            ### makesure rel_sec is positive
            pts = int(round(rel_sec * tb.denominator / tb.numerator))

            # Enforce strictly increasing frame PTS
            if self.last_pts is not None and pts <= self.last_pts:
                pts = self.last_pts + 1

            vf.time_base = tb
            vf.pts = pts
            self.last_pts = pts

            # Encode packets
            for pkt in self.stream.encode(vf):
                # Make sure packets carry the right time_base for muxing
                pkt.time_base = tb

                # ---- Enforce packet DTS/PTS monotonicity (critical) ----
                # Fill missing timestamps defensively
                if pkt.dts is None and pkt.pts is not None:
                    pkt.dts = pkt.pts
                if pkt.pts is None and pkt.dts is not None:
                    pkt.pts = pkt.dts

                # If both missing (rare, but can happen), synthesize from last_dts
                if pkt.dts is None and pkt.pts is None:
                    if self.last_dts is None:
                        pkt.dts = 0
                        pkt.pts = 0
                    else:
                        pkt.dts = self.last_dts + 1
                        pkt.pts = pkt.dts

                # Force strictly increasing DTS
                if self.last_dts is not None and pkt.dts <= self.last_dts:
                    pkt.dts = self.last_dts + 1
                    # Keep PTS consistent with DTS
                    if pkt.pts is None or pkt.pts < pkt.dts:
                        pkt.pts = pkt.dts

                # Also ensure PTS is non-decreasing vs previous packet PTS (optional but helpful)
                if self.last_pkt_pts is not None and pkt.pts is not None and pkt.pts < self.last_pkt_pts:
                    pkt.pts = self.last_pkt_pts
                    if pkt.dts is not None and pkt.pts < pkt.dts:
                        pkt.pts = pkt.dts

                self.last_dts = pkt.dts
                self.last_pkt_pts = pkt.pts

                self._pkt_buf.append(pkt)

                # Periodically flush buffer
                ret_val = self._maybe_flush_buffer(status)
                if ret_val:
                    return ret_val

        except Exception as e:
            status.add_error(f"[Video Recorder] Write Frame failed (encode): {e}")
            return process_status.File_write_failed

        return process_status.SUCCESS

    def close(self, status):
        return self._close_segment(status)

    def _compute_rel_sec(self, ts_sec: float) -> float:
        """
        Return seconds elapsed in the chosen timestamp domain for the current segment.
        - If wallclock mode: use monotonic time so system clock jumps don't break PTS.
        - If decoder-time mode: use provided ts_sec against the origin.
        """
        # if self._use_wallclock:
        #     return max(0.0, time.monotonic() - self._wall_start_mono)
        # return max(0.0, float(ts_sec) - self._pts_origin)
        return float(ts_sec) - float(self.segment_start_ts)

    def _flush_pkt_buffer(self, status):
        """
        Mux buffered packets to the container in timestamp order.

        Why this exists:
          - Encoders (esp. H.264 with B-frames) can output packets slightly out of order.
          - If we mux packets with non-monotonic DTS, many muxers/containers (e.g. MP4)
            will error ("non monotonically increasing dts") and the recording can crash.
          - Sorting helps with re-ordering; a final linear "clamp" guarantees strictly
            increasing DTS even if the input is still imperfect.

        Performance:
          - Sorting is O(n log n) where n = len(_pkt_buf). Keep _pkt_buf_max modest (e.g. 32–256)
            and/or flush by time to avoid big bursts.
          - The monotonic clamp is O(n) and very cheap.
        """
        if not self._pkt_buf:
            return process_status.SUCCESS

        try:
            # 1) Sort packets by DTS (preferred) or PTS as fallback.
            #    Packets with missing timestamps are pushed to the END (safer than -1),
            #    so we don't mux "unknown-time" packets ahead of known-time packets.
            def key(pkt):
                v = pkt.dts if pkt.dts is not None else pkt.pts
                return v if v is not None else 10 ** 18  # "missing" timestamps go last

            self._pkt_buf.sort(key=key)

            # 2) Track the last DTS we successfully muxed across flushes.
            #    This ensures monotonicity even when packets are flushed in multiple batches.
            last_dts = getattr(self, "_last_mux_dts", None)

            for pkt in self._pkt_buf:
                # IMPORTANT:
                # Only set pkt.time_base if you're sure the packet timestamps are expressed in that base.
                # If your encoder already sets the correct time_base, overwriting can cause errors.
                # If you know it's safe in your pipeline, you can uncomment the next line:
                # pkt.time_base = self.stream.time_base

                # 3) Fill missing timestamps defensively.
                #    Many muxers require DTS; PTS-only packets can be made muxable by setting DTS=PTS.
                if pkt.dts is None and pkt.pts is not None:
                    pkt.dts = pkt.pts
                if pkt.pts is None and pkt.dts is not None:
                    pkt.pts = pkt.dts

                # If still missing, synthesize a timestamp (rare, but prevents hard crash).
                if pkt.dts is None and pkt.pts is None:
                    pkt.dts = 0 if last_dts is None else last_dts + 1
                    pkt.pts = pkt.dts

                # At this point dts must exist
                dts = int(pkt.dts)

                # 4) Enforce strictly increasing DTS (the key muxing constraint).
                #    If current DTS is <= last DTS, bump it forward by 1 tick.
                if last_dts is not None and dts <= last_dts:
                    status.add_error(
                        f"[Mux] non-monotonic dts: last={last_dts} cur={dts} (pts={pkt.pts}) -> bump"
                    )
                    pkt.dts = last_dts + 1
                    dts = int(pkt.dts)

                # 5) Keep PTS consistent: PTS should not be < DTS for a packet.
                #    (If it is, bump PTS up to DTS.)
                if pkt.pts is None or int(pkt.pts) < dts:
                    pkt.pts = dts

                # 6) Mux the packet and advance last_dts.
                self.container.mux(pkt)
                self.frames_written += 1
                last_dts = dts

            # 7) Persist last_dts across flushes.
            self._last_mux_dts = last_dts

        except Exception as e:
            errno, value = sys.exc_info()[:2]
            status.add_error(f"[Video Recorder] Buffered mux failed: {value}")
            status.add_error(
                "[Video Recorder] Buffered mux failed: "
                f"{type(e).__name__}: {e} | "
                f"pts={pkt.pts} dts={pkt.dts} "
                f"time_base={getattr(pkt, 'time_base', None)} size={getattr(pkt, 'size', None)} "
                f"last_dts={last_dts}"
            )
            return process_status.File_write_failed

        finally:
            # Always clear buffer and update flush time even on failure,
            # to prevent re-muxing the same packets repeatedly.
            self._pkt_buf.clear()
            self._last_flush_mono = time.monotonic()

        return process_status.SUCCESS

    def _maybe_flush_buffer(self, status):
        """Flush by size and (optionally) by time."""
        if len(self._pkt_buf) >= self._pkt_buf_max:
            ret_val = self._flush_pkt_buffer(status)
            return ret_val
        if self._flush_interval and (time.monotonic() - self._last_flush_mono) >= self._flush_interval:
            ret_val = self._flush_pkt_buffer(status)
            return ret_val
        return process_status.SUCCESS

# --------------------------------------------------------------------------------------
# Write JSON predictions
# --------------------------------------------------------------------------------------
def write_predictions(status, prep_dir, watch_dir, err_dir, detection_dbms, detection_table, source_name, detection_entries ):

    row_counter = len(detection_entries)
    if row_counter:
        file_data = "\n".join(json.dumps(entry) for entry in detection_entries)
        hash_value = utils_data.get_string_hash('md5', file_data, detection_dbms + '.' + detection_table)  # Get the Hash of the data that is written to a file
        ret_val = streaming_data.write_by_prep_move(status, prep_dir, watch_dir, err_dir, detection_dbms, detection_table, source_name, hash_value,
                               '0', None, file_data, "json")
    else:
        ret_val = process_status.SUCCESS        # No rows to write
    return ret_val


