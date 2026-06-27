try:
    import torch
except:
    is_torch = False
else:
    is_torch = True
try:
    import cv2
except:
    is_cv2 = False
else:
    is_cv2 = True

class TorchBackend:
    """
    PyTorch backend. Expects a TorchScript or nn.Module saved to disk.
    If you have ONNX, prefer OpenCV or an ONNX runtime backend.
    """
    def __init__(self, logger):
        self.logger = logger
        if not is_torch or not is_cv2:
            self.logger.error("pyTorch and/or OpenCV-python is missing, cannot use it detections")
        else:
            self.torch = __import__('torch')
            self.model = None
            self.device = "cuda" if self.torch.cuda.is_available() else "cpu"
            self.input_size = (640, 640)

    def load(self, module_type, module_path1, module_path2=None):
        if not is_torch or not is_cv2:
            return False 
        try:
            # We expect a torch.jit or state dict saved model. Prefer torch.jit.load for inference.
            # If the model is ONNX, TorchBackend is not ideal; prefer OpenCV or ONNXRuntime.
            self.model = self.torch.jit.load(module_path1).to(self.device)
            self.model.eval()
            return True
        except Exception as e:
            self.logger.error(f"TorchBackend.load error: {e}")
            # Try fallback: torch.load + state_dict (not ideal for plain inference)
            try:
                m = self.torch.load(module_path1, map_location=self.device)
                # If it's a state_dict, user must supply code to construct model - we cannot auto-wire that.
                self.model = m
                return True
            except Exception as e2:
                self.logger.error(f"TorchBackend.load fallback error: {e2}")
                return False

    def infer(self, preprocessed_img, blob, input_size, module_type):
        """
        preprocessed_img: BGR resized image (H,W,3)
        We'll convert to RGB, normalize to [0,1] and transpose to NCHW float
        """
        if not is_torch or not is_cv2:
            return False, None
        try:
            if self.model is None:
                raise RuntimeError("TorchBackend.model is None")

            img_rgb = cv2.cvtColor(preprocessed_img, cv2.COLOR_BGR2RGB)
            # If model expects letterbox or specific size, the preprocessed_img must match input_size
            img_resized = cv2.resize(img_rgb, input_size)

            t = self.torch.from_numpy(img_resized).float().permute(2, 0, 1).unsqueeze(0) / 255.0
            t = t.to(self.device)

            with self.torch.no_grad():
                # Many Torch models return tuple (pred, ) or a list; try to be flexible
                out = self.model(t)
                # out could be a tuple/list -- extract first useful tensor
                if isinstance(out, (list, tuple)):
                    out = out[0]
                # If out is a dict with 'pred' or 'output' keys:
                if hasattr(out, "detach"):
                    arr = out.detach().cpu().numpy()
                else:
                    # fallback: try conversion
                    arr = self.torch.tensor(out).cpu().numpy()

            # For consistency with rest of pipeline, return numpy array
            return True, arr
        except Exception as e:
            self.logger.error(f"TorchBackend.infer error: {e}")
            return False, None
