import runpy as np
from abc import ABC
import logging
from external_lib.log_processing import ListHandler
import inspect
from typing import Optional


class VideoProcessing(ABC):
    def __init__(self, class_name: str = None, err_level=logging.DEBUG):
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


        # Avoid adding multiple handlers if already exists
        if not self.logger.handlers:
            self.list_handler = ListHandler()
            formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
            )
            self.list_handler.setFormatter(formatter)
            self.logger.addHandler(self.list_handler)

    """
    Get logs 
    :return; 
        if err_level not set return dict 
        if err_level is set return list
    """
    def get_logs(self, err_level:Optional[str]=None)->(list or dict):
        # Access captured logs from the ListHandler
        return self.list_handler.get_logs(level=err_level)

    """
    Reset logs 
    :params: 
        if err_level is set, then reset specific level 
        if err_level not set, then reset entire logs  
    """
    def clear_logs(self, err_level:Optional[str]=None):
        return self.list_handler.clear(level=err_level)

    """
    Process show video stream
    """
    def imshow(self, window_title:str, source_name, frame):
        ret_val = 1
        self.logger.error(f"Method {self.__class__.__name__}.{inspect.currentframe().f_code.co_name} not defined")
        return [ret_val, None]