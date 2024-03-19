# command: setup.py install
import configparser
import subprocess
import os
import sys

from Cython.Build import cythonize
from setuptools import setup, find_packages
from setuptools.command.install import install
from setuptools import Extension

ROOT_PATH = os.path.dirname(os.path.expanduser(os.path.expanduser(os.path.abspath(__file__))))
CONFIG_FILE = os.path.join(ROOT_PATH, 'setup.cfg')

EDGELAKE_PY = os.path.join(ROOT_PATH, 'edge_lake', 'edgelake.py')
if 'EdgeLake' not in ROOT_PATH:
    EDGELAKE_PY = os.path.join(ROOT_PATH, 'EdgeLake', 'edge_lake', 'edgelake.py')


config = configparser.ConfigParser()
config.read(CONFIG_FILE)
PKG_NAME = config['metadata']['name']
PKG_VERSION = config['metadata']['version']
PKG_AUTHOR = config['metadata']['author']
PKG_CONTACT = config['metadata']['contact']
PKG_DESCRIPTION = config['metadata']['description']

subprocess.run(["cython", "--embed", EDGELAKE_PY])


class InstallCommand(install):
    def run(self):
        # Run PyInstaller to create an executable
        if not os.path.isfile(EDGELAKE_PY):
            raise ValueError(f"Failed to locate {EDGELAKE_PY}")
        exe_extension = '.exe' if sys.platform == 'win32' else ''
        subprocess.run(["pyinstaller", "--onefile", f"--name=edgelake", EDGELAKE_PY])

        try:
            install.run(self)
        except Exception as error:
            print(f"install.run fails (Error: {error}) | AnyLog Path: {EDGELAKE_PY}")


ext_modules = [Extension("edge_lake.edgelake", [EDGELAKE_PY])]

try:
    setup(
        name=PKG_NAME,
        version=PKG_VERSION,
        author=PKG_AUTHOR,
        author_email=PKG_CONTACT,
        description=PKG_DESCRIPTION,
        packages=find_packages(),
        install_requires=[
            'requests>=2.26.0',
            'pytz>=2021.3',
            'python-dateutil>=2.8.2',
            'cryptography>=3.4.8'
        ],
        scripts=["edgelake.py"],  # This assumes that your main script is edgelake.py
        cmdclass={"install": InstallCommand},
        ext_modules=cythonize(ext_modules)
    )
except Exception as error:
    print(f"setup fails (Error: {error}) | AnyLog Path: {EDGELAKE_PY}")
