# api.py
import sys
import time
import logging
import base64
import os

_pkg_status = {
    'uvicorn': True,
    'fastapi': True,
    'numpy': True,
    'cv2': False
}

from external_lib.websocket.server import VideoWebApp, get_frame, publish_frame

try:
    import numpy as np
    import cv2
    from fastapi import FastAPI, Request
    from fastapi.responses import StreamingResponse, Response
    from fastapi.middleware.cors import CORSMiddleware
except Exception:
    _pkg_status['numpy'] = False
    _pkg_status['cv2'] = False
    _pkg_status['fastapi'] = False

# ---- FastAPI app ----
def init_fastapi_app(logger):
    fastapi_app = FastAPI() if _pkg_status['fastapi'] else None
    if fastapi_app:
        """
        To enable - remote access of the App
        """
        try:
            front_url = os.getenv('FRONTEND_URL', '*')

            fastapi_app.add_middleware(
                CORSMiddleware,
                allow_origins=[front_url, "*"],  # Change this to your React app's URL for security
                allow_credentials=True,
                allow_methods=["*"],  # Allows GET, POST, PUT, DELETE, etc.
                allow_headers=["*"],  # Allows all headers
            )
        except:
            etype, value, _ = sys.exc_info()
            logger.error(f"[{etype.__name__}] video_feed error: {value}")
    return fastapi_app


# ---- frame generator ----
def gen_frames(source_name: str):
    debug_logger = logging.getLogger("video_webapp.gen_frames")
    while True:
        frame = get_frame(source_name)
        if frame is None:
            debug_logger.debug("no frame for %s — sleeping", source_name)
            time.sleep(0.01)
            continue
        try:
            if not isinstance(frame, np.ndarray):
                debug_logger.error("frame for %s is not ndarray: %r", source_name, type(frame))
                time.sleep(0.01)
                continue
            if frame.size == 0:
                debug_logger.error("frame for %s has zero size", source_name)
                time.sleep(0.01)
                continue
            if frame.dtype != np.uint8:
                frame = frame.astype(np.uint8)
            ret, buf = cv2.imencode(".jpg", frame)
            if not ret:
                debug_logger.error("cv2.imencode failed for %s", source_name)
                time.sleep(0.01)
                continue
            yield (b"--frame\r\n"
                   b"Content-Type: image/jpeg\r\n\r\n" + buf.tobytes() + b"\r\n")
        except Exception:
            etype, value, tb = sys.exc_info()
            debug_logger.exception("gen_frames exception for %s: %s", source_name, value)
            break

# ---- routes ----
# if fastapi_app:
def register_routes(fastapi_app:FastAPI):
    @fastapi_app.post("/video")
    async def receive_video(request: Request):
        if not _pkg_status['numpy'] or not _pkg_status['cv2']:
            return {"status": "Nok", "Error": "Missing dependencies"}
        try:
            data = await request.json()
            path = data.get("path")
            frame_b64 = data.get("frame")
            if not path or not frame_b64:
                return {"status": "Nok", "source": path or "Unknown"}
            frame_data = base64.b64decode(frame_b64)
            np_arr = np.frombuffer(frame_data, np.uint8)
            frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
            publish_frame(path, frame)
            return {"status": "ok", "source": path}
        except Exception:
            etype, value, _ = sys.exc_info()
            logging.getLogger("video_webapp").error(f"[{etype.__name__}] receive_video error: {value}")
            return {"status": f"error: {value}"}

    @fastapi_app.get("/health")
    def health_check():
        return {"status": "ok"}

    @fastapi_app.get("/stream/{source_name}")
    def video_feed(source_name: str):
        publish_frame(source_name, None)  # initialize thread-safe
        try:
            return StreamingResponse(gen_frames(source_name),
                                     media_type="multipart/x-mixed-replace; boundary=frame")
        except Exception:
            etype, value, _ = sys.exc_info()
            logging.getLogger("video_webapp").error(f"[{etype.__name__}] video_feed error: {value}")
            return Response(content="Stream error", status_code=500)

    @fastapi_app.get("/stream_test")
    def stream_test():
        img = np.zeros((480, 640, 3), dtype=np.uint8)
        img[:] = (255, 0, 0)
        def gen():
            for _ in range(1000):
                ret, buf = cv2.imencode(".jpg", img)
                yield (b"--frame\r\n"
                       b"Content-Type: image/jpeg\r\n\r\n" + buf.tobytes() + b"\r\n")
                time.sleep(0.05)
        return StreamingResponse(gen(), media_type="multipart/x-mixed-replace; boundary=frame")


# ---- quick local run ----
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("video_webapp")
    web = VideoWebApp(host="0.0.0.0", port=8888)
    fastapi_app=init_fastapi_app(logger=logger)
    if fastapi_app is not None:
        register_routes(fastapi_app=fastapi_app)
    web.launch(logger, fastapi_app)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        web.stop_uvicorn(logger)
