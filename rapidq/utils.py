import importlib
import os
import sys
from types import ModuleType


def import_module(module_name) -> ModuleType:
    current_path = os.getcwd()
    if current_path not in sys.path:
        sys.path.append(current_path)
    _module = importlib.import_module(module_name)
    return _module
