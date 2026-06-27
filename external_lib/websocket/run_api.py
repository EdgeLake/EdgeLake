import logging
import threading
from external_lib.websocket.api import run_api

def start_api(logger):
    web, fastapi_app = run_api(logger=logger)

    def run_server():
        try:
            web.launch(logger, fastapi_app)
        except Exception as e:
            logger.error(f"Failed to start web server: {e}")

    thread_web = threading.Thread(
        target=run_server,
        daemon=True  # important: auto-exit on program stop
    )
    thread_web.start()

    return web


def stop_api(logger, web):
    if web:
        web.stop_uvicorn(logger)


def main():
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("video_webapp")

    web = start_api(logger=logger)

    input("Press ENTER to stop server...\n")

    stop_api(logger, web)
