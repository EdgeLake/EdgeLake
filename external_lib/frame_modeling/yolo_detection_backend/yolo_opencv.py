try:
    import cv2
except:
    is_import = False
else:
    is_import = True

class OpenCVBackend:
    """
    OpenCV DNN backend. Supports:
      - Darknet: readNet(weights, cfg)
      - ONNX: readNetFromONNX(path)
      - TensorFlow: readNetFromTensorflow(pb, proto)
    """
    def __init__(self, logger):
        self.net = None
        self.input_size = (640, 640)
        self.logger = logger
        if not is_import:
            self.logger.warning("OpenCV-python (cv2) not installed, cannot be used for detections")

    def load(self, module_type, module_path1, module_path2=None):
        if not is_import:
            return False
        try:
            if module_type == 'darknet':
                # module_path1 = weights, module_path2 = cfg
                self.net = cv2.dnn.readNet(module_path1, module_path2)
            elif module_type == 'onnx':
                self.net = cv2.dnn.readNetFromONNX(module_path1)
            elif module_type in ['ssd', 'mobilenet', 'tensorflow']:
                # module_path1 = .pb, module_path2 = .prototxt
                self.net = cv2.dnn.readNetFromTensorflow(module_path1, module_path2)
            else:
                self.logger.error(f"OpenCVBackend cannot load module_type={module_type}")

            # default to CPU; user can change later if they build CUDA-enabled OpenCV
            self.net.setPreferableBackend(cv2.dnn.DNN_BACKEND_OPENCV)
            self.net.setPreferableTarget(cv2.dnn.DNN_TARGET_CPU)
            return True
        except Exception as e:
            self.logger.error(f"OpenCVBackend.load error: {e}")
            return False

    def infer(self, blob,  module_type):
        """
        Accepts:
          - preprocessed_img: resized BGR image (H,W,3)
          - blob: cv2.dnn blobFromImage result (NCHW)
        Returns:
          - detections in whatever shape OpenCV returns (list of arrays for darknet heads, or numpy array)
        """
        if not is_import:
            return False, None

        try:
            if self.net is None:
                self.logger.error("OpenCVBackend.net is None")

            # Use blob directly (this mirrors original code)
            self.net.setInput(blob)

            # Darknet-style: multiple unconnected out layers
            if module_type == 'darknet':
                layer_names = self.net.getUnconnectedOutLayersNames()
                detections = self.net.forward(layer_names)
            else:
                detections = self.net.forward()

            return True, detections
        except Exception as e:
            self.logger.error(f"OpenCVBackend.infer error: {e}")
            return False, None