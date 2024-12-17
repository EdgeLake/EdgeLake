import configparser
import os.path
import multiprocessing
import sys
import platform
import traceback

from setuptools import setup, Extension
from Cython.Build import cythonize
try:
    from poetry.masonry.builders import SdistBuilder
    from poetry.core.factory import Factory
except:
    is_poetry = False
else:
    is_poetry = True

ROOT_PATH = os.path.dirname(os.path.expanduser(os.path.expanduser(os.path.abspath(__file__))))
REQUIREMENTS = os.path.join(ROOT_PATH, 'docker_files', 'requirements.txt')
SOURCE_PATH = os.path.join(ROOT_PATH, 'anylog_node')
CONFIG_FILE = os.path.join(ROOT_PATH, 'setup.cfg')
IS_ALPINE = platform.system().lower() == 'alpine'


config = configparser.ConfigParser()
config.read(CONFIG_FILE)
PKG_NAME = config['metadata']['name']
PKG_VERSION = config['metadata']['version']
PKG_LICENSE = config['metadata']['license']
PKG_AUTHOR = config['metadata']['author']
PKG_CONTACT = config['metadata']['contact']
PKG_DESCRIPTION = config['metadata']['description']
PKG_SITE = config['metadata']['site']
PKG_DOCS = config['metadata']['docs']
PKG_SOURCE = config['metadata']['source']

ANYLOG_MODULES = [] # anylog python files
REQUIREMENT_PACKAGES = [] # pip packages to install (based on requirements.txt)
EXTENSIONS = [] # python file paths in format anylog_node.cmd.user_cmd instead of anylog_node/cmd/user_cmd.py
FILES = []

WORKERS = max(1, multiprocessing.cpu_count() // 2)

# https://cython.readthedocs.io/en/latest/src/reference/compilation.html#compiler-directives
COMPILER_DIRECTIVES={
   # Specifies the language level of the generated Python code (default - 2)
   "language_level": sys.version_info.major,
   # Enables or disables bounds checking for array indexing and slicing (default - True)
   "boundscheck": True,
   # Enables or disables checking for integer overflow during arithmetic operations (default - True)
   "wraparound": True,
   # Enables or disables C-style division behavior for integer division [rounding towards 0] (default - False)
   "cdivision": True,
   # Enables or disables runtime checks for NoneType (default - True)
   "nonecheck": False,
   # Cython will embed the Cython function signature in the generated C code. (default - False)
   "embedsignature": True,
   # generation of a Python binding for the compiled C code (defaultt - False)
   "binding": False,
   # runtime checks for initialized memory access (default - True)
   "initializedcheck": True,
   # Specifies the default C string type to use (default - unicode)
   "c_string_type": "unicode",
   # Specifies the default encoding to use for C strings (default - ascii)
   "c_string_encoding": "ascii"
}


for dirname in os.listdir(SOURCE_PATH):
    dir_path = os.path.join(SOURCE_PATH, dirname)
    if os.path.isdir(dir_path):
        for filename in os.listdir(dir_path):
            file_path = os.path.join(dir_path, filename)
            if os.path.isfile(file_path) and filename.rsplit('.')[-1] == 'py':
                ANYLOG_MODULES.append(file_path)

for file_path in ANYLOG_MODULES:
    if not file_path.endswith("__init__.py"):
        if os.name == "nt":
            ext_name = file_path.rsplit('.', 1)[0].replace("\\", '.').split(".AnyLog-Network.")[-1]
        else:
            ext_name = file_path.rsplit('.', 1)[0].replace(os.path.sep, '.').split(".AnyLog-Network.")[-1]
        ext = Extension(ext_name, [file_path])
        EXTENSIONS.append(ext)

try:
    setup(
        name=PKG_NAME,
        version=PKG_VERSION,
        author=PKG_AUTHOR,
        author_email=PKG_CONTACT,
        description=PKG_DESCRIPTION,
        install_requires=[
            'requests>=2.26.0',
            'pytz>=2021.3',
            'python-dateutil>=2.8.2',
            'cryptography>=3.4.8'
        ],
        ext_modules=cythonize(EXTENSIONS, compiler_directives=COMPILER_DIRECTIVES),
    )
except Exception as error:
    print(f"Failed to complete build (Error: {error})")
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback.print_exception(exc_type, exc_value, exc_traceback, limit=2, file=sys.stdout)
