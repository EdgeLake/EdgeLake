import configparser
import os
import platform
import subprocess
import sys

from Cython.Build import cythonize
from setuptools import setup, find_packages, Extension
from setuptools.command.install import install

# Paths and Configurations
ROOT_PATH = os.path.abspath(os.path.dirname(__file__))
CONFIG_FILE = os.path.join(ROOT_PATH, 'setup.cfg')
REQUIREMENTS_FILE = os.path.join(ROOT_PATH, 'requirements.txt')

EDGELAKE_PY = os.path.join(ROOT_PATH, 'edge_lake', 'edgelake.py')

if not os.path.isfile(EDGELAKE_PY):
    raise ValueError(f"Failed to locate {EDGELAKE_PY}")

# Read Requirements
REQUIREMENTS = []
if os.path.isfile(REQUIREMENTS_FILE):
    with open(REQUIREMENTS_FILE, 'r') as f:
        REQUIREMENTS = [line.strip() for line in f if line.strip() and not line.startswith("#")]
else:
    REQUIREMENTS = [
        'requests>=2.26.0',
        'pytz>=2021.3',
        'python-dateutil>=2.8.2',
        'cryptography>=3.4.8',
        'ojson>=0.0',
    ]

# Read Metadata from Configuration
config = configparser.ConfigParser()
config.read(CONFIG_FILE)
PKG_NAME = config['metadata']['name']
PKG_VERSION = config['metadata']['version']
PKG_AUTHOR = config['metadata']['author']
PKG_CONTACT = config['metadata']['contact']
PKG_DESCRIPTION = config['metadata']['description']

hidden_imports = [
    f"--hidden-import=reportlab.graphics.barcode",
    f"--hidden-import=reportlab.graphics.barcode.usps",
    f"--hidden-import=reportlab.graphics.barcode.usps4s",
    f"--hidden-import=reportlab.graphics.barcode.ecc200datamatrix",
    f"--hidden-import=reportlab.graphics.barcode.code39",
    f"--hidden-import=reportlab.graphics.barcode.code93",
    f"--hidden-import=reportlab.graphics.barcode.code128",
]
for requirement in REQUIREMENTS:
    package = requirement.strip().split('>=')[0]
    hidden_imports.append(f"--hidden-import={package}")
    hidden_imports.append(f"--collect-submodules={package}")

# Determine CPU Type
CPU_TYPE = platform.machine()

# Cythonize the Script
subprocess.run(["cython", "--embed", EDGELAKE_PY], check=True)


class InstallCommand(install):
    """Custom install command to generate executable with PyInstaller."""

    def run(self):
        exe_extension = '.exe' if sys.platform == 'win32' else ''
        exe_name = f"edgelake_v{PKG_VERSION}_{CPU_TYPE}{exe_extension}"

        try:
            subprocess.run(
                ["pyinstaller", "--onefile", f"--name={exe_name}", *hidden_imports, EDGELAKE_PY],
                check=True
            )
        except subprocess.CalledProcessError as e:
            print(f"Error during PyInstaller execution: {e}")
            raise

        super().run()


# Extensions
ext_modules = [
    Extension("edge_lake.edgelake", [EDGELAKE_PY])
]

# Setup
setup(
    name=PKG_NAME,
    version=PKG_VERSION,
    author=PKG_AUTHOR,
    author_email=PKG_CONTACT,
    description=PKG_DESCRIPTION,
    packages=find_packages(),
    install_requires=REQUIREMENTS,
    scripts=[EDGELAKE_PY],  # Include the script in the package
    cmdclass={"install": InstallCommand},
    ext_modules=cythonize(ext_modules, compiler_directives={'language_level': '3'}),
)
