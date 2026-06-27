import os
import urllib.parse
import urllib.request
import sys


ROOT_DIR = os.path.dirname(os.path.expanduser(os.path.expandvars(__file__)))
MODELS = os.path.join(ROOT_DIR, 'models') # path: EdgeLake/edge_lake/frame_processing/module

if not os.path.isdir(MODELS):
    os.makedirs(MODELS)

"""
Tool: to download module and store to (local) file
"""
def download_module(logger, url:str, file_path:str):
    err_msg = ""
    ret_val = True
    try:
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme in ('http', 'https'):
            urllib.request.urlretrieve(url, file_path)
        else:
            err_msg = f"Invalid URL ({url}) - unsupported scheme"
            ret_val = False
    except:
        errno, value = sys.exc_info()[:2]
        err_msg = f'Failed to download module - {value}'
        ret_val = False

    if err_msg:
        logger.error(err_msg)

    return ret_val

"""
Check if module exists and if not attempt to download 
"""
def get_module_path(logger, module_path:str):
    ret_val = True
    err_msg = ""
    module_path = os.path.expandvars(os.path.expanduser(module_path))
    module_file_path = ""  # file path for module
    if not os.path.isfile(module_path):  # validate file exists and if not - attempt to download
        if os.path.isdir(module_path):
            err_msg = "Path provided is directory - cannot use for module"
            ret_val = False
        else:
            url_path = module_path
            module_file_path = os.path.join(MODELS, os.path.basename(url_path))
            ret_val = download_module(logger=logger, url=url_path, file_path=module_file_path)
    else:
        module_file_path = module_path

    if err_msg:
        logger.error(err_msg)

    return [ret_val, module_file_path]