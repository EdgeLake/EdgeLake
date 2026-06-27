"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

"""
Lazy Import Rules for Nuitka Onefile Builds
-------------------------------------------

A package must be lazy‑imported if importing it at module import time would
cause it to load `certifi` or other resource files *before* the CA override
runs in AnyLog's startup sequence. All other packages may be imported normally.

A lazy import is required only when ALL THREE conditions are true:

    1. The package loads certifi or resource files during import.
       Examples: requests, boto3, pymongo, grpc, OpenSSL.

    2. The package would otherwise be imported at module import time
       (i.e., at the top of the file, before any function or method call).

    3. The file containing that import is loaded before the CA override runs
       (this applies to modules imported early in AnyLog's startup chain).

If all three conditions are met → the package must be lazy‑imported.

Sample usage:

    from edge_lake.generic.utils_lazy_import import lazy_import

    def my_method(status):
        requests, error = lazy_import("requests")
        if error:
            status.add_error(error)
            return None

        response = requests.get("https://example.com")
        ...

Dotted module paths are supported (e.g. "grpc_reflection.v1alpha.reflection_pb2").
The LAZY_REQUIRED guard is enforced on the TOP‑LEVEL package name, so a single
entry like "grpc_reflection" allows any submodule under it.
"""

import sys
import importlib
import traceback

LAZY_REQUIRED = {
    "requests",
    "docker",
    "boto3",
    "botocore",
    "pymongo",
    "gridfs",
    "grpc",
    "grpc_reflection",
    "google",
    "OpenSSL",
}

_modules = {}
_errors = {}

def lazy_import(pkg):
    """
    Attempt to import a package once, cache success/failure, and return module or None.
    :args:
        pkg:str - module name (dotted paths allowed, e.g. "botocore.config")
    :global:
        LAZY_REQUIRED:set - top-level packages permitted for lazy import
        _modules:dict     - cached imported module(s)
        _errors:dict      - cached error message(s)
    :return:
        (module, None) on success || (None, error_message) on failure
    """
    if pkg in _modules:
        return _modules[pkg], _errors.get(pkg)

    # The allow-list is enforced on the top-level package so that one entry
    # ("grpc_reflection") covers every submodule under it.
    top = pkg.split(".", 1)[0]
    if top not in LAZY_REQUIRED:
        msg = f"Package '{pkg}' is not configured for lazy import"
        _modules[pkg] = None
        _errors[pkg] = msg
        return None, msg

    try:
        module = importlib.import_module(pkg)
    except Exception as e:
        # Capture the *full* traceback. Under Nuitka onefile, bare ImportError()
        # instances are common and str(e) is often empty — without the traceback
        # the caller would only see "<class 'ImportError'> :" with no detail.
        msg = (
            f"Failed to import package '{pkg}' error: "
            f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
        )
        _modules[pkg] = None
        _errors[pkg] = msg
        return None, msg

    _modules[pkg] = module
    _errors[pkg] = None
    return module, None
