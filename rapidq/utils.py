import importlib
import os
import sys


def import_module(module_name):
    current_path = os.getcwd()
    if current_path not in sys.path:
        sys.path.append(current_path)
    _module = importlib.import_module(module_name)
    return _module
