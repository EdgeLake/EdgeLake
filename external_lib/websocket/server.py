# server.py
import logging
import threading
import sys
import time
from typing import Dict, Optional
from external_lib.log_processing import ListHandler

_pkg_status = {
    'requests': True,
    'uvicorn': True,
    'fastapi': True,
    'numpy': True,
    'cv2': True,
}

try:
    _pkg_name = "requests"
    import requests

    _pkg_name = "uvicorn"
    import uvicorn

    _pkg_name = 'numpy'
    import numpy as np

    _pkg_name = 'cv2'
    import cv2

except Exception:
    _pkg_status[_pkg_name] = False
    # Mark all later packages as failed
    sub_keys = list(_pkg_status)
    start_idx = sub_keys.index(_pkg_name) + 1
    for later_key in sub_keys[start_idx:]:
        _pkg_status[later_key] = False

# ---- thread-safe frame buffer ----
_frame_buffers: Dict[str, "np.ndarray"] = {}
_frame_lock = threading.Lock()


def publish_frame(path: str, frame: "np.ndarray") -> None:
    """Thread-safe publish of a BGR numpy frame into the buffer."""
    with _frame_lock:
        _frame_buffers[path] = frame


def get_frame(path: str):
    """Return current frame or None (no locking for read is acceptable)."""
    with _frame_lock:
        return _frame_buffers.get(path)


# ---- VideoWebApp class ----
class VideoWebApp:
    """
    Run a FastAPI app in a background thread using uvicorn.Server.
    Call launch(logger, fastapi_app) to start and stop_uvicorn(logger) to stop.
    """
    def __init__(self, class_name:str=None, host: str = "127.0.0.1", port: int = 8080, health_path: str = "/health", err_level=logging.CRITICAL):
        logger_name = class_name or self.__class__.__name__
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(err_level)

        # Attach a ListHandler exactly once per logger (avoid duplicates)
        existing = next((h for h in self.logger.handlers if isinstance(h, ListHandler)), None)
        if existing:
            self.list_handler = existing
        else:
            self.list_handler = ListHandler()
            formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
            self.list_handler.setFormatter(formatter)
            self.logger.addHandler(self.list_handler)


        self.host = host
        self.port = port
        self.thread: threading.Thread | None = None
        self._server = None
        self._running = threading.Event()
        self.health_path = health_path

    def _run_server(self, logger, app_obj):
        if _pkg_status['uvicorn']:
            try:
                config = uvicorn.Config(
                    app=app_obj,
                    host=self.host,
                    port=self.port,
                    log_level="critical",  # suppress INFO logs
                    access_log=False,
                    workers=1,
                    reload=False
                )
                server = uvicorn.Server(config=config)
                self._server = server
                self.logger.info("uvicorn server starting (thread)...")
                server.run()  # blocks inside the thread until shutdown
            except Exception:
                etype, value, _ = sys.exc_info()
                self.logger.error(f"Failed to start uvicorn - {value}")
            finally:
                self._running.clear()
                self.logger.info("uvicorn thread exiting")
        else:
            self.logger.error('Package uvicorn not installed')

    def launch(self, logger, app_obj):
        """Start the API server in a background thread."""
        if self.thread and self.thread.is_alive():
            self.logger.info("Web App already running")
            return
        if not _pkg_status['uvicorn']:
            self.logger.error("pip package uvicorn not installed")
            return

        self.thread = threading.Thread(target=self._run_server, args=(logger, app_obj), daemon=True)
        self._running.set()
        self.thread.start()
        self.health_check(logger)

    def health_check(self, logger):
        host_url = "127.0.0.1" if self.host == "0.0.0.0" else self.host
        url = f"http://{host_url}:{self.port}{self.health_path}"
        for _ in range(50):  # ~5 seconds
            try:
                import requests
                r = requests.get(url, timeout=0.2)
                if r.status_code == 200:
                    self.logger.info("Web App health check passed")
                    return
            except Exception:
                time.sleep(0.1)
        self.logger.error("Web App failed health check (server did not respond)")

    def stop_uvicorn(self, logger):
        try:
            if self._server is not None:
                self.logger.info("Signaling uvicorn server to stop")
                self._server.should_exit = True
        except Exception:
            etype, value, _ = sys.exc_info()
            self.logger.error(f"Error signaling server shutdown: {value}")

        if self.thread and self.thread.is_alive():
            self.logger.info("Waiting for uvicorn thread to join")
            self.thread.join(timeout=5)
        self.thread = None
        self._server = None
        self._running.clear()
        self.logger.info("Web application stopped")

    """
    Get logs 
    :return; 
        if err_level not set return dict 
        if err_level is set return list
    """

    def get_logs(self, err_level: Optional[str] = None) -> (list or dict):
        # Access captured logs from the ListHandler
        return self.list_handler.get_logs(level=err_level)

    """
    Reset logs 
    :params: 
        if err_level is set, then reset specific level 
        if err_level not set, then reset entire logs  
    """
    def clear_logs(self, err_level: Optional[str] = None):
        return self.list_handler.clear(level=err_level)




__all__ = ["VideoWebApp", "publish_frame", "get_frame"]
