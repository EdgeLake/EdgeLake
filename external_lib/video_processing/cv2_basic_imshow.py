import logging
try:
    import cv2
except:
    _pkg_not_installed = False
else:
    _pkg_not_installed = True

import sys
from external_lib.video_processing.video_processing import VideoProcessing

def init_class(**kwargs):
    # --- New code: should be outside this class ---
    # module_type, classes, module_path1, module_path2, coco_path
    call_class = None
    required = ["err_level"]
    missing = [k for k in required if k not in kwargs]
    # if "port" in missing:
    #     raise ValueError(f"Missing required params: {missing}")
    # else:
    #     port = kwargs["port"]
    # if "host" in missing:
    #     host = "0.0.0.0"
    # else:
    #     host = kwargs["host"]

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

    call_class = ImShow(err_level=err_level)
    # call_class.launch_app() # Launch web application (using FastAPI) to view stream via browser
    return call_class


class ImShow(VideoProcessing):
    """
    Process show video stream as a pop-up object
    """
    def __init__(self, err_level=logging.DEBUG):
        super().__init__(class_name=self.__class__.__name__, err_level=err_level)

    def imshow(self, window_title:str, source_name, frame):
        ret_val = 0 # success
        err_val = ""
        if not _pkg_not_installed:
            ret_val = 1 # fails
            err_val = "cv2 package not installed"
        else:
            try:
                cv2.imshow(window_title, frame)
                # Keep GUI responsive; also allow quit via 'q' or ESC
                key = cv2.waitKey(1) & 0xFF
                if key in (27, ord('q')):
                    return [0, "break"]

                # Detect manual window close
                if cv2.getWindowProperty(window_title, cv2.WND_PROP_VISIBLE) < 1:
                    return [0, "break"]
            except:
                errno, err_val = sys.exc_info()[:2]
                ret_val = f"Failed to display video for '{source_name}': {err_val}"
                ret_val = 1 # fails

        if err_val:
            self.logger.error(err_val)

        return [ret_val, err_val]