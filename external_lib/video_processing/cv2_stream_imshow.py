# imshow.py
import logging
import time
import base64

try:
    import cv2
    _pkg_installed = True
except ImportError:
    _pkg_installed = False

from external_lib.video_processing.video_processing import VideoProcessing
from external_lib.websocket.server import VideoWebApp, publish_frame
from external_lib.websocket.api import init_fastapi_app, register_routes


def init_class(**kwargs):
    # --- New code: should be outside this class ---
    # module_type, classes, module_path1, module_path2, coco_path
    call_class = None
    required = ["host", "port", "err_level"]
    missing = [k for k in required if k not in kwargs]
    if "port" in missing:
        raise ValueError(f"Missing required params: {missing}")
    else:
        port = kwargs["port"]
    if "host" in missing:
        host = "0.0.0.0"
    else:
        host = kwargs["host"]

    """
    - DEBUG (10): Detailed information, typically of interest only when diagnosing problems. This level is usually used during development and debugging.
    - INFO (20): Confirmation that things are working as expected. These messages provide general information about the program's flow or significant events.
    - WARNING (30): An indication that something unexpected happened, or indicative of some problem in the near future (e.g., 'disk space low'). The software is still working as expected. This is the default logging level.
    - ERROR (40): Due to a more serious problem, the software has not been able to perform some function. These errors might not stop the program entirely but require attention.
    - CRITICAL (50): A serious error, indicating that the program itself may be unable to continue running. These are severe errors that often lead to program termination. 
    """
    if "err_level" in missing or kwargs['err_level'].upper() == 'DEBUG':
        err_level = logging.DEBUG
    elif kwargs['err_level'].upper() == 'INFO':
        err_level = logging.INFO
    elif kwargs['err_level'].upper() == 'WARNING':
        err_level = logging.WARNING
    elif kwargs['err_level'].upper() == 'ERROR':
        err_level = logging.ERROR
    elif kwargs['err_level'].upper() == 'CRITICAL':
        err_level = logging.CRITICAL
    else:
        raise ValueError(f"Invalid err_level: {kwargs['err_level']}")

    call_class = ImShow(err_level=err_level, host=host, port=port)
    call_class.launch_app() # Launch web application (using FastAPI) to view stream via browser
    return call_class

class ImShow(VideoProcessing):
    """
    ImShow publishes frames directly into the running in-process FastAPI server's frame buffer
    using publish_frame(). This avoids REST/HTTP overhead for same-process usage.
    """
    def __init__(self, err_level=logging.DEBUG, host="0.0.0.0", port=8888):
        super().__init__(class_name=self.__class__.__name__, err_level=err_level)
        # manage the webapp runner; the FastAPI object lives in api_server.fastapi_app
        self.webapp = VideoWebApp(host=host, port=port)
        self.webhook_url = f"http://{host}:{port}/video"  # kept for external compatibility
        self.app = init_fastapi_app(logger=self.logger)
        if self.app:
            register_routes(fastapi_app=self.app)


    """
    Launch web application (using FastAPI) to view stream via brwoser
    """
    def launch_app(self):
        """Start the API server in a background thread (if not already running)."""
        if self.webapp.thread is not None and self.webapp.thread.is_alive():
            self.logger.info("Web app is already running")
            return
        self.webapp.launch(logger=self.logger, app_obj=self.app)

    """
    Stop web application
    """
    def stop_web_app(self):
        self.webapp.stop_uvicorn(logger=self.logger)

    """
    Publish a frame into the in-process buffer. `frame` must be a BGR numpy array (cv2 format).
    Returns [0, None] on success, [1, error_string] on failure.
    """
    def imshow(self, window_title: str, source_name, frame):
        if not _pkg_installed:
            return [1, "cv2 package not installed"]

        try:
            # Direct in-process publish: fast and avoids JSON/base64/HTTP
            publish_frame(source_name, frame)
            return [0, None]
        except Exception as e:
            self.logger.error(f"Failed to publish frame: {e}")
            return [1, str(e)]

    # Optional: legacy helper to support external REST clients if needed
    def imshow_via_rest(self, window_title: str, source_name, frame):
        """Encode to JPEG, base64, and POST to /video (kept for compatibility)."""
        import requests
        try:
            _, buffer = cv2.imencode(".jpg", frame)
            jpg_as_text = base64.b64encode(buffer).decode("utf-8")
            payload = {
                "window_title": window_title,
                "path": source_name,
                "frame": jpg_as_text,
                "timestamp": time.time(),
            }
            response = requests.post(url=self.webhook_url, json=payload, timeout=5)
            if not 200 <= response.status_code < 300:
                self.logger.error(f"Failed to publish frame (Network Error: {response.status_code})")
                return [1, f"network:{response.status_code}"]
            return [0, None]
        except Exception as e:
            self.logger.error(f"Failed to publish frame via REST: {e}")
            return [1, str(e)]