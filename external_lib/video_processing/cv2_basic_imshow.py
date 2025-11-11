try:
    import cv2
except:
    _pkg_not_installed = False
else:
    _pkg_not_installed = True

import sys
from external_lib.video_processing.video_processing import VideoProcessing


class ImShow(VideoProcessing):
    """
    Process show video stream as a pop-up object
    """
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