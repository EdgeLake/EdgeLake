"""
The following provides directions validating which pip (import) packages are being deployed and which are not.

    Dependency Validation
    Package                        Installed
    ------------------------------------------
    OpenSSL                        ✓
    anylog_enterprise              ✓
    edge_lake                    ✓
    av                             ✓
    boto3                          ✓
    botocore                       ✓
    colorama                       ✓
    cryptography                   ✓
    cv2                            ✓
    cython                         ✓
    dateutil                       ✓
    dns                            ✓
    docker                         ✓
    external_lib                   ✓
    fastapi                        ✓
    google                         ✓
    gridfs                         ✓
    grpc                           ✓
    grpc_reflection                ✓
    infer_pb2                      ✗
    infer_pb2_grpc                 ✗
    jwt                            ✓
    kafka                          ✓
    netifaces                      ✓
    numpy                          ✓
    opcua                          ✓
    openvino                       ✓
    orjson                         ✓
    paho                           ✓
    psutil                         ✓
    psycopg2                       ✓
    py4j                           ✓
    pycomm3                        ✓
    pymongo                        ✓
    pytz                           ✓
    requests                       ✓
    starlette                      ✓
    torch                          ✗
    uvicorn                        ✓
    vm_pb2                         ✗
    web3                           ✓
    win32evtlog                    ✓
    xhtml2pdf                      ✗
    xmltodict                      ✓
    yt_dlp                         ✓
"""
import os
import ast
import importlib.util
import sys

ROOT_DIR = os.path.dirname(os.path.abspath(__file__)).split("edge_lake")[0]

def get_imports_from_file(filepath):
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        tree = ast.parse(f.read(), filename=filepath)

    imports = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for name in node.names:
                imports.add(name.name.split(".")[0])

        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.add(node.module.split(".")[0])

    return imports


def scan_project(directory):
    imports = set()

    for root, dirs, files in os.walk(directory):
        if "__pycache__" in root or "venv" in root:
            continue

        for file in files:
            if file.endswith(".py"):
                filepath = os.path.join(root, file)
                try:
                    imports |= get_imports_from_file(filepath)
                except Exception as error:
                    raise Exception(f"Failed reading {filepath}: {error}")

    return imports


def is_installed(pkg):
    return importlib.util.find_spec(pkg) is not None


def print_table(packages):

    print("\nDependency Validation\n")
    print(f"{'Package':30} {'Installed'}")
    print("-" * 42)

    for pkg in sorted(packages):
        status = "✓" if is_installed(pkg) else "✗"
        print(f"{pkg:30} {status}")


def pkg_validation():
    # scan project
    imports = []
    for dirname in ["edge_lake", "anylog_enterprise", "external_lib"]:
        sub_imports = scan_project(os.path.join(ROOT_DIR, dirname))
        imports += sub_imports

    # remove stdlib modules
    imports = {pkg for pkg in imports if pkg not in sys.stdlib_module_names}

    # print result
    print_table(imports)

if __name__ == "__main__":
    pkg_validation()