import logging

import cv2
import sys

import numpy as np

from external_lib.frame_modeling.detection import ObjectDetection
from external_lib.frame_modeling.support import get_module_path

from external_lib.frame_modeling.yolo_detection_backend.yolo_torch import TorchBackend
from external_lib.frame_modeling.yolo_detection_backend.yolo_opencv import OpenCVBackend
from external_lib.frame_modeling.yolo_detection_backend.yolo_openvino import OpenVINOBackend


def auto_backend(logger):
    try: # Torch / GPU
        import torch
        if torch.cuda.is_available():
            logger.info("Detections logic is using pyTorch")
            return TorchBackend(logger=logger)
    except:
        pass

    try: # OpenVion / NPU
        from openvino.runtime import Core
        core = Core()
        if 'NPU' in core.available_devices or 'GPU' in core.available_devices:
            logger.info("Detections logic is using OpenVino")
            return OpenVINOBackend(logger=logger)
    except:
        pass

    logger.info("Detections logic is using OpenCV")
    return OpenCVBackend(logger=logger)



def init_class(**kwargs):
    # --- New code: should be outside this class ---
    # module_type, classes, module_path1, module_path2, coco_path


    required = ["module_type", "classes", "module_path1", "coco_path"]
    missing = [k for k in required if k not in kwargs]
    if missing:
        raise ValueError(f"Missing required params: {missing}")

    module_type  = kwargs["module_type"]
    classes      = kwargs["classes"]
    module_path1 = kwargs["module_path1"]
    module_path2 = kwargs.get("module_path2")  # optional
    coco_path    = kwargs["coco_path"]

    module = YoloDetection(module_type=module_type, classes=classes)
    module.initiate_models(
        module_path1=module_path1,
        module_path2=module_path2,
        coco_path=coco_path)

    return module

class YoloDetection(ObjectDetection):
    """
    Program logic:
    0. Declare
    frame model create where name=[model name] and model_type=[model type] and {objects=[comma separated object]}

    1. Setup models - load_model()
    frame models load where name=[model name] and model_path1=[model path] {and model_path2=[model path]}

    2. Set coco params - load_model_coco()
    frame  model coco where name=[model name] and  path=[coco path]

    3. Enable model - if enabled should update the img in video/generic_client.py (not sure where) by calling detect()
    frame model enable where name=[model name]

    3b. disable model - do not process using detect()
    frame model disable where name=[model name]


    | **Model Type**                                    | **Framework / Loader**                 | **File Inputs**              | **Model Example**          | **Backend Used**             | **Status**                  | **Notes / Capabilities**                                                     |
    | ------------------------------------------------- | -------------------------------------- | ---------------------------- | -------------------------- | ---------------------------- | ----------------------------|---------------------------------------------------------------------------- |
    | **Darknet (YOLOv3 / YOLOv4)**                     | OpenCV DNN (`cv2.dnn.readNet`)         | `.weights`, `.cfg`, `.names` | YOLOv3, YOLOv4             | `cv2.dnn.DNN_BACKEND_OPENCV` | ✅ **Fully supported**      | Classic YOLO models via Darknet config/weights. Requires COCO `.names` file. |
    | **ONNX (YOLOv5 / YOLOv7 / YOLOv8 / others)**      | OpenCV DNN (`cv2.dnn.readNetFromONNX`) | `.onnx`                      | YOLOv5s.onnx, YOLOv8n.onnx | `cv2.dnn.DNN_BACKEND_OPENCV` | ✅ **Supported**            | ONNX models supported via OpenCV. Layer names not required.                  |
    | **Other OpenCV DNN Models (SSD, MobileNet, TF)** | OpenCV DNN                             | `.pb`, `.prototxt`           | SSD-MobileNet, MobileNet   | `cv2.dnn.DNN_BACKEND_OPENCV` | ✅ **Supported**            | TensorFlow / OpenCV DNN models. Loadable via `cv2.dnn.readNetFromTensorflow`. |
    | **COCO Classes File**                             | Plain text                             | `.names` or `.txt`           | `coco.names`               | —                            | ✅ **Supported**            | Used to map detection class IDs → human-readable labels.                     |

    """

    """
    Initiate detection module
    detection module initiate where name = [module name] and module_type = [darknet or onnx] classes = [comma separated] 
    """
    def __init__(self, module_type:str, classes:list,  err_level=logging.ERROR):
        super().__init__(module_type=module_type, classes=classes, class_name=self.__class__.__name__, err_level=err_level)

        if self.module_type == 'onnx':
            self.input_size = (640, 640)
            self.conf_threshold = 0.25
            self.nmh_threshold = 0.45
        elif self.module_type in ['ssd', 'mobilenet', 'tensorflow']:
            self.input_size = (300, 300)  # common default for SSD/MobileNet, adjust if needed
            self.conf_threshold = 0.25
            self.nmh_threshold = 0.45

        self.backend = auto_backend(logger=self.logger)


    # ----------------- #
    #   Local Methods   #
    # ----------------- #
    """
    For a given frame get actual detections
    """
    def normalize_detections(self, detections):
        """
        Normalize and extract detections into lists: class_ids, bboxes, confidences.
        Accepts:
          - Darknet-style `detections` (list of arrays)
          - ONNX-style `detections` (single numpy array of shape (N, 85) or (1, N, 85))
        Returns: [ret_val, class_ids, bboxes, confidences]
        """
        ret_val = True
        err_msg = ""
        det_iterable = None

        if detections:
            try:
                # Normalize detections to an iterable of arrays
                if isinstance(detections, list):
                    # Darknet case: list of arrays
                    det_iterable = detections
                elif isinstance(detections, np.ndarray):
                    # ONNX case: could be (N,85) or (1,N,85)
                    if detections.ndim == 3 and detections.shape[0] == 1:
                        # (1, N, 85) -> take the [0] slice -> (N,85)
                        det_iterable = [detections[0]]
                    else:
                        # (N,85) -> make it a single "layer" for consistent iteration
                        det_iterable = [detections]
                elif isinstance(detections, tuple):
                    # Some backends return tuples
                    det_iterable = list(detections)
                else:
                    # Fallback: try to iterate
                    det_iterable = list(detections)
            except:
                errno, value = sys.exc_info()[:2]
                err_msg = f"Failed to process detection from module - {value}"
                ret_val = False
        else:
            err_msg = "No detections provided"
            ret_val = False

        if err_msg:
            self.logger.error(err_msg)

        return [ret_val, det_iterable]


    def collect_detections(self, detections, width, height):
        ret_val, det_iterable = self.normalize_detections(detections=detections)
        err_msg = ""
        class_ids = []
        bboxes = []
        confidences = []

        if ret_val and self.coco_classes:
            for output in det_iterable:
                # output expected shape: (M, >=5 + num_classes) e.g. (N,85)
                # each `detection` is a row: [cx, cy, w, h, obj_conf, class1, class2, ...]
                for detection in output:
                    try:
                        # Basic shape check
                        if detection.size >= 6:
                            # class scores start at index 5
                            scores = detection[5:]
                            if scores.size > 0:
                                class_id = int(np.argmax(scores))
                                confidence = float(scores[class_id])

                                # Bounds check for coco_classes
                                if 0 <= class_id < len(self.coco_classes):
                                    cls_name = self.coco_classes[class_id]

                                    # Filter by confidence threshold and class whitelist
                                    if confidence > self.conf_threshold and (not self.classes or cls_name in self.classes):
                                        # bbox coordinates: darkent/onnx top row are often relative cx,cy,w,h
                                        # detection[0:4] expected to be [cx, cy, w, h] normalized (0..1) or absolute
                                        # We assume detection values are weight_module (common with YOLO exports).
                                        # If they're already absolute, multiplying will still work if width/height match.
                                        cx, cy, bw, bh = (detection[0:4] * np.array([width, height, width, height])).astype(int)

                                        # clamp coordinates to frame
                                        x = max(0, int(cx - bw / 2))
                                        y = max(0, int(cy - bh / 2))
                                        bw = max(0, int(bw))
                                        bh = max(0, int(bh))

                                        bboxes.append([x, y, bw, bh])
                                        confidences.append(confidence)
                                        class_ids.append(class_id)
                        elif detection.size == 6 and self.module_type in ['ssd', 'mobilenet', 'tensorflow']:
                            x1, y1, x2, y2, score, class_id = detection
                            class_id = int(class_id)
                            confidence = float(score)
                            cls_name = self.coco_classes[class_id] if 0 <= class_id < len(self.coco_classes) else None

                            if confidence >= self.conf_threshold and (not self.classes or cls_name in self.classes):
                                x = max(0, int(x1))
                                y = max(0, int(y1))
                                w = max(0, int(x2 - x1))
                                h = max(0, int(y2 - y1))
                                bboxes.append([x, y, w, h])
                                confidences.append(confidence)
                                class_ids.append(class_id)
                        else:
                            ret_val = False
                            err_msg = "Failed to calculate detections"
                    except:
                        # Non-fatal: log and continue parsing other detections
                        errno, value = sys.exc_info()[:2]
                        self.logger.error(f"Failed to parse detection row: {value}")
                        ret_val = False
        elif not self.coco_classes:
            err_msg = "coco configurations not declared"
            ret_val = False

        if err_msg:
            self.logger.error(err_msg)

        # If nothing was extracted, that's not necessarily an error — return success but empty lists
        return [ret_val, class_ids, bboxes, confidences]


    """
    Apply Non-Max Suppression and return a list of formatted detection results
    """
    def apply_thresholds(self, bboxes, confidence):
        ret_val = True
        err_msg = ""
        np_indices = []
        try:
            indices = cv2.dnn.NMSBoxes(bboxes, confidence, self.conf_threshold, self.nmh_threshold)
            if indices is not None and len(indices):
                np_indices = np.array(indices).flatten().tolist()
        except:
            errno, value = sys.exc_info()[:2]
            err_msg = f"Failed to get indicies based on provided info - {value} "
            ret_val = False

        if err_msg:
            self.logger.error(err_msg)

        return [ret_val, np_indices]


    # ----------------- #
    #  Abstract Methods #
    # ----------------- #
    """
    Upload coco module 
    """
    def load_model_coco(self, coco_path:str):
        err_msg = ""
        ret_val, coco_module = get_module_path(logger=self.logger, module_path=coco_path)
        if ret_val:
            try:
                with open(coco_module, 'r') as f:
                    self.coco_classes = [line.strip() for line in f.readlines()]
            except:
                errno, value = sys.exc_info()[:2]
                err_msg = f'Failed to load module - {value}'
                ret_val = False

        if err_msg:
            self.logger.error(err_msg)

        return ret_val

    """
    Load module 
    - for Yolo.v0-v4 - input weight and cfg 
    - for Yolo.v5-Yolo.vX - input onnx_path 
    - for SSD, MobileNet and tenserflow - input tf_pb and tf__proto
    """
    def load_model(self, module_path1, module_path2:str=None):
        err_msg = ""
        weight_module = None
        cfg_module = None
        onnx_module = None
        tf_pb = None
        tf_proto = None

        if self.module_type == 'darknet':
            ret_val, weight_module = get_module_path(logger=self.logger, module_path=module_path1)
            if ret_val:
                ret_val, cfg_module = get_module_path(logger=self.logger, module_path=module_path2)
        elif self.module_type == 'onnx':
            ret_val, onnx_module = get_module_path(logger=self.logger, module_path=module_path1)
        elif self.module_type in ['ssd', 'mobilenet', 'tensorflow']:
            ret_val, tf_pb = get_module_path(logger=self.logger, module_path=module_path1)
            if ret_val:
                ret_val, tf_proto = get_module_path(logger=self.logger, module_path=module_path2)
        else:
            err_msg = "Unsupported module type - cannot read module file"
            ret_val = False


        if ret_val:
            try:
                if self.module_type == 'darknet':
                    self.net = cv2.dnn.readNet(weight_module, cfg_module)
                elif self.module_type == 'onnx':
                    self.net = cv2.dnn.readNetFromONNX(onnx_module)
                elif self.module_type in ['ssd', 'mobilenet', 'tensorflow']:
                    self.net = cv2.dnn.readNetFromTensorflow(tf_pb, tf_proto)
                else:
                    err_msg = f"Unsupported model type {self.module_type}"
                    ret_val = False
                if self.net:
                    self.net.setPreferableBackend(cv2.dnn.DNN_BACKEND_OPENCV)
                    self.net.setPreferableTarget(cv2.dnn.DNN_TARGET_CPU)
            except:
                errno, value = sys.exc_info()[:2]
                err_msg = f'Failed to load module - {value}'
                ret_val = False

        if err_msg:
            self.logger.error(err_msg)

        return ret_val


    """
    "main" to initiate models and coco process in a single call
    """
    def initiate_models(self, module_path1, module_path2:str=None, coco_path:str=None):
        ret_val = False
        ret_val_model = self.load_model(module_path1=module_path1, module_path2=module_path2)

        if coco_path:
            ret_val_coco = self.load_model_coco(coco_path=coco_path)
            if ret_val_model == ret_val_coco == True:
                ret_val = True
        else:
            ret_val = ret_val_model

        return ret_val

    """
    convert frame to blob
    """
    def preprocess(self, frame:np.ndarray):
        """
        Convert frame to blob for YOLO input
        """
        return self.backend.preprocess(frame)

        # ret_val = True
        # err_msg = ""
        # height, width, blob = None, None, None
        #
        # try:
        #     height, width = frame.shape[:2]
        #     scale = 1 / 255.0 if self.module_type in ['darknet', 'onnx'] else 1.0
        #     blob = cv2.dnn.blobFromImage(frame, scale, self.input_size, swapRB=True, crop=False)
        # except:
        #     errno, value = sys.exc_info()[:2]
        #     err_msg = f'Failed to convert frame to blob - {value}'
        #     ret_val = False
        #
        # if err_msg:
        #     self.logger.error(err_msg)
        # return [ret_val, height, width, blob]


    """
    Given the blob and  cv2.dnn.readNet (self) get detection 
    """
    def infer(self, blob):
        return self.infer(blob=blob)
        # ret_val = True
        # err_msg = ""
        # detections = None
        # try:
        #     self.net.setInput(blob)
        #     layer_names = self.net.getUnconnectedOutLayersNames() if self.module_type == 'darknet' else None
        #     if layer_names:
        #         detections = self.net.forward(layer_names)
        #     elif self.module_type in ['onnx', 'ssd', 'mobilenet', 'tensorflow']:
        #         detections = self.net.forward()
        #     else:
        #         err_msg = "Unable to generate detection"
        #         ret_val = False
        # except:
        #     errno, value = sys.exc_info()[:2]
        #     err_msg = f"Failed to process detection from module - {value}"
        #     ret_val = False
        #
        # if err_msg:
        #     self.logger.error(err_msg)
        #
        # return [ret_val, detections]


    def postprocess(self, detections, height, width):
        return self.backend.postprocess(detections, height, width)
        # indices = None
        # ret_val, class_ids, bboxes, confidences = self.collect_detections(detections=detections,
        #                                                                  width=width, height=height)
        # if ret_val:
        #     # in apply_thresholds the value is `indices` called `np_indicies`
        #     ret_val, indices = self.apply_thresholds(bboxes=bboxes, confidence=confidences)
        #
        # return [ret_val, bboxes, indices, class_ids, confidences]

    """
    Draw bounding box 
    """
    def __get_dynamic_color(self, frame:np.ndarray):
        try:
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            brightness = np.mean(gray)  # range: 0-255
            if brightness < 63.75:
                color = (255, 255, 255)  # white
            elif brightness > 191.25:
                color = (0, 0, 0)  # black
            else:
                color = (0, 0, 255)  # blue
        except:
            color = (255, 165, 0)  # fallback green

        return color


    def draw_bounding_box(self, detections, frame:np.ndarray, highlight_class=None):
        ret_val = True
        err_msg = ""
        if self.coco_classes:
            for detection in detections:
                # output expected shape: (M, >=5 + num_classes) e.g. (N,85)
                # each `detection` is a row: [cx, cy, w, h, obj_conf, class1, class2, ...]
                # for detection in output:
                x, y, w, h = detection["bbox"]
                # cls = detection["class"]
                # conf = detection["confidence"]
                try:
                    # Draw rectangle (bounding box)
                    bbox_color = (0, 255, 0) # default color is green
                    if detection['class'] == highlight_class:
                        bbox_color = self.__get_dynamic_color(frame)
                        
                    # Label with class name and confidence
                    cv2.rectangle(frame, (x, y), (x + w, y + h), bbox_color, 2)
                    cv2.putText(frame, detection['class'], (x, y - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, bbox_color, 2)
                    
                    # cv2.imshow(cls, frame)
                    # # Keep GUI responsive; also allow quit via 'q' or ESC
                    # key = cv2.waitKey(1) & 0xFF
                    # if key in (27, ord('q')):
                    #     break
                    #
                    # # Detect manual window close
                    # if cv2.getWindowProperty(label, cv2.WND_PROP_VISIBLE) < 1:
                    #     break
                except:
                    errno, value = sys.exc_info()[:2]
                    err_msg = f"Failed to process bbox as part of display - {value}"
                    ret_val = False
        else:
            err_msg = "COCO classes not loaded"
            ret_val = False

        if err_msg:
            self.logger.error(err_msg)

        return ret_val

    def detect(self, frame:np.ndarray):
        ret_val = True
        err_msg = ""
        results = []
        blob = None
        indices = []
        confidences = []
        bboxes = []
        class_ids = []
        detections = None
        height = None
        width = None

        if frame is None or not isinstance(frame, np.ndarray): # validate frame exists
            self.logger.error('Frame not provided - cannot generate detection')
            ret_val = False
            return [ret_val, None]
        elif not self.coco_classes:
            self.logger.error("COCO classes not loaded")
            return [False, None]

        # -------------------------------------------------
        # Step I: Preprocess frame → blob
        # -------------------------------------------------
        if ret_val:
            ret_val, height, width, blob = self.preprocess(frame=frame)


        # -------------------------------------------------
        # Step II: get inference based on blob
        # -------------------------------------------------
        if ret_val and self.net:
            ret_val, detections = self.infer(blob=blob)
        elif not self.net:
            err_msg = f'Module not loaded - cannot get inference'
            ret_val = False

        # -------------------------------------------------
        # Step 3: Collect detections
        # -------------------------------------------------
        if ret_val:
            ret_val, bboxes, indices, class_ids, confidences = self.postprocess(detections=detections,
                                                                                  width=width, height=height)

        # -------------------------------------------------
        # Step 4: Construct final results
        # -------------------------------------------------
        if ret_val:
            if indices:
                for indx in indices:
                    x, y, w_box, h_box = bboxes[indx]
                    cls_name = self.coco_classes[class_ids[indx]] if class_ids else None
                    conf = confidences[indx]
                    results.append({
                        "class": cls_name,
                        "confidence": conf,
                        "bbox": [x, y, w_box, h_box]
                    })

        if err_msg:
            self.logger.error(err_msg)

        return [ret_val, results]




