# client/inference_client.py
#
# Fixed Option B:
# - Starts ONE bidirectional PredictStream in a background thread (so init never blocks)
# - Request iterator blocks on Queue.get() (fine) and yields requests
# - Response loop updates latest response via on_response()
# - submit_ndarray() enqueues latest request ("latest wins")
# - Includes cached overlay helpers: update_cached_detections / apply_cached_boxes

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from queue import Queue, Empty, Full
from typing import Optional, Union, Callable, Any, Dict, Tuple
from collections import Counter
import sys

import grpc
import numpy as np
import cv2

from edge_lake.generic import utils_print

# ---- Robust stub imports ----
try:
    import external_lib.frame_modeling.infer_pb2_grpc as infer_pb2_grpc
    import external_lib.frame_modeling.infer_pb2 as infer_pb2
except ModuleNotFoundError:
    import infer_pb2  # type: ignore
    import infer_pb2_grpc  # type: ignore


@dataclass
class StreamConfig:
    address: str = "127.0.0.1:50051"
    max_msg_mb: int = 128
    insecure: bool = True
    reconnect: bool = True
    reconnect_backoff_s: float = 1.0


class InferenceClient:
    """
    Bidi-streaming inference client.

    Key properties:
      - submit_ndarray()/submit_frame_bytes() DO NOT block waiting for a response
      - get_latest_response() returns the most recent PredictResponse (or None)
      - internal background thread owns:
          - creating the PredictStream call
          - iterating responses
      - request iterator reads from a "latest wins" Queue(maxsize=1)
    """

    def __init__(
        self,
        status,
        stub=None,
        channel: Optional[grpc.Channel] = None,
        config: Optional[StreamConfig] = None,
        start: bool = True,
        on_response_cb: Optional[Callable[[Any], None]] = None,
    ):
        self.status = status # used to write to error log
        self._on_response_cb = on_response_cb
        self._next_request_id = 1
        self.last_acked_id = 0

        self._cfg = config or StreamConfig()
        self._max_bytes = int(self._cfg.max_msg_mb) * 1024 * 1024

        self._owned_channel = False
        self._channel = channel
        self._stub = stub

        # Latest-wins outbound request queue
        self._send_q: Queue = Queue(maxsize=1)

        # Latest inbound response
        self._latest_resp = None
        self._resp_lock = threading.Lock()

        # Stream state
        self._call = None
        self._stop_evt = threading.Event()
        self._io_thread: Optional[threading.Thread] = None
        self.last_stream_error: Optional[str] = None

        # Optional request_id -> frame pairing
        self._frames: "OrderedDict[int, np.ndarray]" = OrderedDict()

        # Cached overlay/detections for cheap redraw
        self._last_norm_dets: list[Dict[str, Any]] = []
        self._last_det_sig: Optional[Tuple] = None
        self._overlay: Optional[np.ndarray] = None
        self._overlay_shape: Optional[Tuple[int, int]] = None  # (H, W)

        # Build channel/stub if not provided
        # if self._stub is None:
        #     if self._channel is None:
        #         self._channel = grpc.insecure_channel(
        #             self._cfg.address,
        #             options=[
        #                 ("grpc.max_send_message_length", self._max_bytes),
        #                 ("grpc.max_receive_message_length", self._max_bytes),
        #             ],
        #         )
        #         self._owned_channel = True
        #     self._stub = infer_pb2_grpc.InferenceServiceStub(self._channel)

        if start:
            self.start()

    # ---------------------------
    # Lifecycle
    # ---------------------------
    def start(self):
        """Start background I/O thread (safe to call once)."""
        if self._io_thread is not None and self._io_thread.is_alive():
            return
        self._stop_evt.clear()
        self._io_thread = threading.Thread(target=self._io_loop, daemon=True)
        self._io_thread.start()

    def close(self):
        """Stop stream and close channel if owned."""
        self._stop_evt.set()

        # unblock request iterator if it's waiting
        try:
            self._send_q.put_nowait(None)
        except Exception:
            pass

        # cancel active call
        try:
            if self._call is not None:
                self._call.cancel()
        except Exception:
            pass

        # close channel
        if self._owned_channel and self._channel is not None:
            try:
                self._channel.close()
            except Exception:
                pass

    # ---------------------------
    # Logging / utils
    # ---------------------------
    def _log_error(self, msg: str):
        # if the error msg is the same as before, ignore it
        if self.last_stream_error == msg:
            return
        if self.status is not None:
            try:
                self.status.add_error(msg)
                self.last_stream_error = msg
                msg = f"{msg}. Video recording continuing, inference not"
                utils_print.output_box(msg,"red")
                return
            except Exception:
                pass

    def _fmt_to_enum(self, fmt: Union[str, int]) -> int:
        if isinstance(fmt, int):
            return fmt
        key = fmt.strip().lower()
        if key == "bgr24":
            return infer_pb2.BGR24
        if key == "rgb24":
            return infer_pb2.RGB24
        if key in ("jpeg", "jpg"):
            return infer_pb2.JPEG
        raise ValueError('fmt must be "bgr24", "rgb24", "jpeg", or a PixelFormat enum value')

    def _next_id(self) -> int:
        rid = self._next_request_id
        self._next_request_id += 1
        return rid

    # ---------------------------
    # Streaming internals
    # ---------------------------
    def _io_loop(self):
        """
        Owns the streaming call and response iteration.
        Retries if enabled.
        """
        while not self._stop_evt.is_set():
            try:
                # Create the call in THIS thread so it can block safely on request iterator
                self._call = self._stub.PredictStream(self._request_iterator())

                # Iterate responses (this drives the stream)
                for resp in self._call:
                    if self._stop_evt.is_set():
                        break
                    self.on_response(resp)

                # If loop exits without exception, stream ended gracefully
                if self._stop_evt.is_set():
                    break

                self._log_error("PredictStream ended without error (server closed stream).")

            except grpc.RpcError as e:
                self._log_error(f"PredictStream RpcError: {e.code()} {e.details()}")

            except Exception as e:
                self._log_error(f"PredictStream error: {type(e).__name__}: {e}")

            if not self._cfg.reconnect or self._stop_evt.is_set():
                break

            time.sleep(max(0.1, float(self._cfg.reconnect_backoff_s)))

    def _request_iterator(self):
        """
        gRPC pulls from this generator to send outbound messages.
        Blocks on the queue until submit_* enqueues a request.
        """
        while not self._stop_evt.is_set():
            req = self._send_q.get()
            if req is None:
                return
            # DEBUG: proves gRPC is pulling and attempting to send
            # print("sending rid", req.request_id, "bytes", len(req.frame))
            yield req

    def _put_latest(self, req) -> None:
        # drop old request if present
        while True:
            try:
                self._send_q.get_nowait()
            except Empty:
                break
        try:
            self._send_q.put_nowait(req)
        except Full:
            # race; drop
            pass

    # ---------------------------
    # Public send API
    # ---------------------------
    def submit_frame_bytes(
        self,
        frame_bytes: bytes,
        width: int,
        height: int,
        fmt: Union[str, int] = "bgr24",
        request_id: Optional[int] = None,
        store_for_viz: bool = False,
        frame_for_viz: Optional[np.ndarray] = None,
        max_keep: int = 50,
    ) -> int:
        rid = self._next_id() if request_id is None else int(request_id)

        if store_for_viz and frame_for_viz is not None:
            self.store_frame(rid, frame_for_viz, max_keep=max_keep)

        req = infer_pb2.PredictRequest(
            frame=frame_bytes,
            width=int(width),
            height=int(height),
            format=self._fmt_to_enum(fmt),
            request_id=int(rid),
        )
        self._put_latest(req)
        return rid

    def submit_ndarray(
        self,
        frame: np.ndarray,
        fmt: Union[str, int] = "bgr24",
        request_id: Optional[int] = None,
        store_for_viz: bool = False,
        max_keep: int = 50,
        jpeg_quality: int = 80,
    ) -> int:
        if frame.dtype != np.uint8:
            frame = frame.astype(np.uint8, copy=False)
        if not frame.flags["C_CONTIGUOUS"]:
            frame = np.ascontiguousarray(frame)

        h, w = frame.shape[:2]

        if isinstance(fmt, str) and fmt.strip().lower() in ("jpeg", "jpg"):
            payload = self.encode_jpeg(frame, quality=jpeg_quality)
            return self.submit_frame_bytes(
                frame_bytes=payload,
                width=w,
                height=h,
                fmt="jpeg",
                request_id=request_id,
                store_for_viz=store_for_viz,
                frame_for_viz=(frame.copy() if store_for_viz else None),
                max_keep=max_keep,
            )

        return self.submit_frame_bytes(
            frame_bytes=frame.tobytes(),
            width=w,
            height=h,
            fmt=fmt,
            request_id=request_id,
            store_for_viz=store_for_viz,
            frame_for_viz=(frame.copy() if store_for_viz else None),
            max_keep=max_keep,
        )

    def encode_jpeg(self, frame_bgr: np.ndarray, quality: int = 80) -> bytes:
        if frame_bgr.dtype != np.uint8:
            frame_bgr = frame_bgr.astype(np.uint8, copy=False)
        if not frame_bgr.flags["C_CONTIGUOUS"]:
            frame_bgr = np.ascontiguousarray(frame_bgr)

        ok, enc = cv2.imencode(".jpg", frame_bgr, [int(cv2.IMWRITE_JPEG_QUALITY), int(quality)])
        if not ok:
            raise RuntimeError("cv2.imencode('.jpg') failed")
        return enc.tobytes()

    # ---------------------------
    # Response handling
    # ---------------------------
    def on_response(self, resp) -> None:
        self.last_acked_id = int(resp.request_id)
        with self._resp_lock:
            self._latest_resp = resp

        if self._on_response_cb is not None:
            try:
                self._on_response_cb(resp)
            except Exception as e:
                self._log_error(f"on_response_cb error: {e}")

    def get_latest_response(self):
        with self._resp_lock:
            return self._latest_resp

    # ---------------------------
    # Optional exact pairing (request_id -> frame)
    # ---------------------------
    def store_frame(self, request_id: int, frame_bgr: np.ndarray, max_keep: int = 50):
        self._frames[int(request_id)] = frame_bgr
        while len(self._frames) > int(max_keep):
            self._frames.popitem(last=False)

    def pop_frame(self, request_id: int) -> Optional[np.ndarray]:
        return self._frames.pop(int(request_id), None)

    # ---------------------------
    # Cached overlay helpers
    # ---------------------------
    def _normalize_detections(self, detections) -> list[Dict[str, Any]]:
        out: list[Dict[str, Any]] = []
        for d in detections or []:
            if hasattr(d, "xmin") and hasattr(d, "xmax"):
                out.append(
                    {
                        "xmin": float(d.xmin),
                        "ymin": float(d.ymin),
                        "xmax": float(d.xmax),
                        "ymax": float(d.ymax),
                        "class": str(getattr(d, "class_name", "")),
                        "confidence": float(getattr(d, "confidence", 0.0)),
                    }
                )
                continue
            if isinstance(d, dict):
                cls = d.get("class", d.get("class_name", ""))
                conf = d.get("confidence", d.get("conf", d.get("score", 0.0)))
                if all(k in d for k in ("xmin", "ymin", "xmax", "ymax")):
                    out.append(
                        {
                            "xmin": float(d["xmin"]),
                            "ymin": float(d["ymin"]),
                            "xmax": float(d["xmax"]),
                            "ymax": float(d["ymax"]),
                            "class": str(cls),
                            "confidence": float(conf),
                        }
                    )
        return out

    def _detections_signature(self, norm_dets: list[Dict[str, Any]], tol_px: float = 2.0) -> Tuple:
        items = []
        for d in norm_dets:
            cls = str(d.get("class", ""))
            conf = float(d.get("confidence", 0.0) or 0.0)
            xmin = int(round(float(d["xmin"]) / tol_px))
            ymin = int(round(float(d["ymin"]) / tol_px))
            xmax = int(round(float(d["xmax"]) / tol_px))
            ymax = int(round(float(d["ymax"]) / tol_px))
            qconf = int(round(conf * 100))
            items.append((cls, qconf, xmin, ymin, xmax, ymax))
        items.sort()
        return tuple(items)

    def _build_overlay(self, frame_shape_hw: tuple[int, int], norm_dets: list[dict]) -> np.ndarray:
        """
        Build an overlay image containing boxes + label text on black background.
        frame_shape_hw is (H, W).
        """
        H, W = frame_shape_hw
        overlay = np.zeros((H, W, 3), dtype=np.uint8)

        for det in norm_dets:
            xmin = int(round(det["xmin"]))
            ymin = int(round(det["ymin"]))
            xmax = int(round(det["xmax"]))
            ymax = int(round(det["ymax"]))

            xmin = max(0, min(W - 1, xmin))
            xmax = max(0, min(W - 1, xmax))
            ymin = max(0, min(H - 1, ymin))
            ymax = max(0, min(H - 1, ymax))

            if xmax <= xmin or ymax <= ymin:
                continue

            cls = str(det.get("class", ""))
            conf = det.get("confidence", None)

            color = (0, 255, 0)

            # box
            cv2.rectangle(overlay, (xmin, ymin), (xmax, ymax), color, 2)

            # label text
            label = cls if cls else "obj"
            if conf is not None:
                try:
                    label = f"{label} {float(conf):.2f}"
                except Exception:
                    pass

            # Put text above the box if possible, else inside
            text_y = ymin - 6
            if text_y < 12:
                text_y = ymin + 14

            cv2.putText(
                overlay,
                label,
                (xmin, text_y),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.5,
                color,
                2,
                cv2.LINE_AA,
            )

        return overlay

    def update_cached_detections(self, detections, frame: np.ndarray, tol_px: float = 2.0) -> bool:
        try:
            H, W = frame.shape[:2]
        except Exception as e:
            self._log_error(f"Invalid frame: {e}")
            return False

        norm = self._normalize_detections(detections)
        sig = self._detections_signature(norm, tol_px=tol_px)

        if sig != self._last_det_sig or self._overlay is None or self._overlay_shape != (H, W):
            self._last_det_sig = sig
            self._last_norm_dets = norm
            self._overlay = self._build_overlay((H, W), norm)
            self._overlay_shape = (H, W)

        return True

    def apply_cached_boxes(self, frame: np.ndarray, alpha: float = 1.0):
        """
        Returns (applied: bool, out_frame: np.ndarray).
        out_frame may be a converted copy (e.g. gray->BGR, BGRA->BGR).
        """
        if self._overlay is None:
            return False, frame

        # Ensure frame is 3-channel BGR (WITHOUT illegal in-place shape assignment)
        if frame.ndim == 2:
            frame = cv2.cvtColor(frame, cv2.COLOR_GRAY2BGR)
        elif frame.ndim == 3 and frame.shape[2] == 4:
            frame = cv2.cvtColor(frame, cv2.COLOR_BGRA2BGR)
        elif frame.ndim != 3 or frame.shape[2] != 3:
            self._log_error(f"Unexpected frame shape: {frame.shape}")
            return False, frame

        H, W = frame.shape[:2]

        # Overlay must match exactly for addWeighted
        if self._overlay.shape != frame.shape:
            # Don't nuke the overlay; just refuse this frame (or rebuild on next new inference)
            return False, frame

        cv2.addWeighted(self._overlay, float(alpha), frame, 1.0, 0.0, dst=frame)
        return True, frame

    def get_insights(self, detections: list, previous_detections=None, tolerance=0.05, check_param: str = "class"):
        ret_val = True
        class_counts = {}

        def bbox_differs(bbox1, bbox2, tol):
            for i in range(4):
                base = bbox1[i]
                comp = bbox2[i]
                if base == 0:
                    if comp != 0:
                        return True
                else:
                    rel_diff = abs(base - comp) / abs(base)
                    if rel_diff > tol:
                        return True
            return False

        def _as_dict(d):
            if hasattr(d, "xmin") and hasattr(d, "xmax"):
                xmin = float(getattr(d, "xmin", 0.0))
                ymin = float(getattr(d, "ymin", 0.0))
                xmax = float(getattr(d, "xmax", 0.0))
                ymax = float(getattr(d, "ymax", 0.0))
                w = max(0.0, xmax - xmin)
                h = max(0.0, ymax - ymin)
                class_name = str(getattr(d, "class_name", ""))
                class_id = int(getattr(d, "class_id", 0))
                conf = float(getattr(d, "confidence", 0.0))
                return {
                    "class": class_name,
                    "class_name": class_name,
                    "class_id": class_id,
                    "confidence": conf,
                    "xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax,
                    "bbox": (xmin, ymin, w, h),
                }

            if isinstance(d, dict):
                bbox = d.get("bbox")
                if bbox is None and all(k in d for k in ("xmin", "ymin", "xmax", "ymax")):
                    xmin, ymin, xmax, ymax = d["xmin"], d["ymin"], d["xmax"], d["ymax"]
                    bbox = (xmin, ymin, xmax - xmin, ymax - ymin)
                cls = d.get("class", d.get("class_name", ""))
                return {**d, "class": cls, "bbox": bbox}

            return {}

        try:
            det_list = [_as_dict(d) for d in (detections or [])]
            det_list = [d for d in det_list if d]

            if previous_detections:
                detected_changes = det_list[:]
            else:
                detected_changes = det_list

            class_counts = Counter(d.get(check_param) for d in detected_changes if d.get(check_param) is not None)
            class_counts = dict(class_counts)

        except Exception:
            _, value = sys.exc_info()[:2]
            if getattr(self, "logger", None) is not None:
                self._log_error(f"Failed to process insights - {value}")
            else:
                print(f"Failed to process insights - {value}")
            ret_val = False

        return [ret_val, class_counts]

