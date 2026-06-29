try:
    from openvino.runtime import Core
except:
    is_opnevino = False
else:
    is_opnevino = True
try:
    import cv2
except:
    is_cv2 = False
else:
    is_cv2 = True


class OpenVINOBackend:
    """
    OpenVINO backend. Accepts IR (.xml/.bin) or ONNX models via Core.read_model.
    """
    def __init__(self, logger):
        self.logger = logger
        if not is_opnevino or not is_cv2:
            self.logger.error("OpenVino and/or OpenCV-python is missing, cannot use it detections")
        else:
            self.Core = __import__('openvino.runtime', fromlist=['Core']).Core
            self.device = "AUTO"  # "CPU", "GPU", "MYRIAD", "AUTO", etc.
            self.compiled = None
            self.input_layer = None
            self.input_size = (640, 640)

    def load(self,  module_path1):
        if not is_opnevino or not is_cv2:
            return False

        try:
            core = self.Core()
            # Core.read_model accepts ONNX or IR
            model = core.read_model(module_path1)
            self.compiled = core.compile_model(model, self.device)
            # store input shape if needed
            self.input_layer = list(self.compiled.inputs)[0]
            return True
        except Exception as e:
            logging.getLogger("YOLO").error(f"OpenVINOBackend.load error: {e}")
            return False

    def infer(self, preprocessed_img, input_size):
        """
        preprocessed_img: BGR resized image (H,W,3)
        Convert to NCHW float and run compiled model
        """
        if not is_opnevino or not is_cv2:
            return False, None

        try:
            if self.compiled is None:
                raise RuntimeError("OpenVINOBackend.compiled is None")

            img = cv2.resize(preprocessed_img, input_size)
            # OpenVINO expects NHWC or NCHW depending on model; get input shape
            # We'll transpose to NCHW like typical YOLO converted models.
            if img.dtype != np.float32:
                img = img.astype(np.float32)
            img = img.transpose(2, 0, 1)[None] / 255.0

            # Execute
            outputs = self.compiled([img])
            # outputs is a dict-like mapping; pick first output
            out = outputs[self.compiled.output(0)]
            return True, out
        except Exception as e:
            logging.getLogger("YOLO").error(f"OpenVINOBackend.infer error: {e}")
            return False, None
