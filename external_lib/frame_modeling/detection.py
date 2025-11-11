import datetime
from abc import ABC, abstractmethod
import sys
from typing import Optional, List

import numpy as np
from collections import Counter
import logging
from external_lib.log_processing import ListHandler
import inspect



class ObjectDetection(ABC):
    """
    Abstract base class for object detectors.
    | **Model Type**                                    | **Framework / Loader**                 | **File Inputs**              | **Model Example**          | **Backend Used**             | **Status**                                                | **Notes / Capabilities**                                                     |
    | ------------------------------------------------- | -------------------------------------- | ---------------------------- | -------------------------- | ---------------------------- | --------------------------------------------------------- | ---------------------------------------------------------------------------- |
    | **Darknet (YOLOv3 / YOLOv4)**                     | OpenCV DNN (`cv2.dnn.readNet`)         | `.weights`, `.cfg`, `.names` | YOLOv3, YOLOv4             | `cv2.dnn.DNN_BACKEND_OPENCV` | ✅ **Fully supported**                                     | Classic YOLO models via Darknet config/weights. Requires COCO `.names` file. |
    | **ONNX (YOLOv5 / YOLOv7 / YOLOv8 / others)**      | OpenCV DNN (`cv2.dnn.readNetFromONNX`) | `.onnx`                      | YOLOv5s.onnx, YOLOv8n.onnx | `cv2.dnn.DNN_BACKEND_OPENCV` | ✅ **Supported**                                           | ONNX models supported via OpenCV. Layer names not required.                  |
    | **COCO Classes File**                             | Plain text                             | `.names` or `.txt`           | `coco.names`               | —                            | ✅ **Supported**                                           | Used to map detection class IDs → human-readable labels.                     |
    | **Other OpenCV DNN Models (e.g. SSD, MobileNet)** | OpenCV DNN                             | `.pb`, `.prototxt`           | SSD-MobileNet              | ❌ **Not yet implemented**    | Could be added easily by extending `load_models()` logic. |                                                                              |
    | **TorchScript / PyTorch Models**                  | —                                      | `.pt`                        | YOLOv5 (PyTorch)           | —                            | ⚙️ **Not supported (future)**                             | Would require `torch` + subclass of abstract interface.                      |
    | **TensorFlow / TFLite Models**                    | —                                      | `.pb`, `.tflite`             | SSD MobileNet v2           | —                            | ⚙️ **Not supported (future)**                             | Could be added via `cv2.dnn.readNetFromTensorflow()` or TFLite Interpreter.  |
    """
    def __init__(self, module_type: str, classes:Optional[List[str]]=None, class_name:Optional[str]=None, err_level:int=logging.DEBUG):
        logger_name = class_name or self.__class__.__name__
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(err_level)

        # Attach a ListHandler exactly once per logger
        if not any(isinstance(h, ListHandler) for h in self.logger.handlers):
            self.list_handler = ListHandler()
            formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
            self.list_handler.setFormatter(formatter)
            self.logger.addHandler(self.list_handler)
        else:
            # find existing handler for access (avoid duplicating)
            self.list_handler = next(h for h in self.logger.handlers if isinstance(h, ListHandler))

        # domain fields
        self.module_type = module_type.lower()
        self.classes = [
            "person",  # humans
            "car",  # vehicles
            "truck",  # vehicles
            "bus",  # vehicles
            "bicycle",  # vehicles
            "motorbike",  # vehicles
            "dog",  # pets
            "cat",  # pets
            "sports ball"  # for objects like balls
        ] if not classes else classes

        self.net = None
        self.coco_classes = []

        # Preset for Yolo.v0-v4 (ie Darknet)
        self.conf_threshold = 0.25
        self.nmh_threshold = 0.45
        self.input_size = (640, 640)

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
    Update module params 
    detection module update where name=[module name] and {input_size=[input size] and conf=[conf threshold] and nmh=[nmh threshold]} 
    """
    def update_input_size(self, input_size:tuple):
        self.input_size = input_size

    def update_conf_threshold(self, conf_threshold: float):
        self.conf_threshold = conf_threshold

    def update_nmh_threshold(self, nmh_threshold: float):
        self.nmh_threshold = nmh_threshold

    """
    Get object count based on detections using  user - defined param (default: 'class')  
    """

    def get_insights(self, detections: list, previous_detections=None, tolerance=0.05, check_param: str = 'class'):
        ret_val = True
        class_counts = {}
        err_msg = ""

        def bbox_differs(bbox1, bbox2, tol):
            # Compare each dimension with relative tolerance
            for i in range(4):  # x, y, w, h
                base = bbox1[i]
                comp = bbox2[i]
                if base == 0:  # avoid division by zero
                    if comp != 0:
                        return True
                else:
                    rel_diff = abs(base - comp) / abs(base)
                    if rel_diff > tol:
                        return True
            return False

        try:
            if previous_detections:
                detected_changes = []
                for b in detections:
                    # if not any(
                    #         b["class"] == a["class"] and not bbox_differs(a["bbox"], b["bbox"], tolerance)
                    #         for a in previous_detections
                    # ):
                    #     detected_changes.append(b)
                    detected_changes.append(b)
            else:
                detected_changes = detections

            class_counts = Counter(d[check_param] for d in detected_changes)
            class_counts = dict(class_counts)

        except Exception:
            errno, value = sys.exc_info()[:2]
            self.logger.error(f"Failed to process insights - {value}")
            ret_val = False

        return [ret_val, class_counts]

    """Load model weights/configs and prepare network for inference."""
    @abstractmethod
    def load_model(self,  module_path1, module_path2:str=None):
        ret_val = False
        self.logger.error(f"Method {self.__class__.__name__}.{inspect.currentframe().f_code.co_name} not defined")
        return ret_val

    @abstractmethod
    def load_model_coco(self, coco_path:str):
        ret_val = False
        self.logger.error(f"Method {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        return ret_val

    """initiate model(s) and coco"""
    @abstractmethod
    def initiate_models(self, module_path1, module_path2:str=None, coco_path:str=None):
        ret_val = self.load_model(module_path1=module_path1, module_path2=module_path2)
        if ret_val:
            if coco_path:
                ret_val = self.load_model_coco(coco_path=coco_path)
        return ret_val


    @abstractmethod
    def preprocess(self, frame:np.ndarray):
        ret_val = False
        self.logger.error(f"Method {self.__class__.__name__}.{inspect.currentframe().f_code.co_name} not defined")
        return [ret_val, None]

    """Run inference and return raw detections."""
    @abstractmethod
    def infer(self, blob):
        ret_val = False
        return [ret_val, None]


    """
    Convert raw detections to standardized output.
    Returns list of dicts: [{"class": str, "confidence": float, "bbox": [x, y, w, h]}]
    """
    @abstractmethod
    def postprocess(self, detections, height, width):
        ret_val = False
        self.logger.error(f"Method {self.__class__.__name__}.{inspect.currentframe().f_code.co_name} not defined")
        return [ret_val, None]

    """
    Draw bounding box 
    """
    @abstractmethod
    def draw_bounding_box(self, detections, frame: np.ndarray):
        ret_val = False
        self.logger.error(f"Method {self.__class__.__name__}.{inspect.currentframe().f_code.co_name} not defined")
        return [ret_val, None]

    """
    Convenience method: combines preprocess → infer → postprocess
    """
    @abstractmethod
    def detect(self, frame:np.ndarray):
        ret_val = False
        self.logger.error(f"Method {self.__class__.__name__}.{inspect.currentframe().f_code.co_name} not defined")
        return [ret_val, None]
