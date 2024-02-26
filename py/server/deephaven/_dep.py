#
#     Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
"""This module is an internal module to support dependency management"""

import importlib
from types import ModuleType
from typing import Optional


def soft_dependency(module_name: str) -> Optional[ModuleType]:
    """Attempt to import a module and return it if it exists, otherwise return None.
    Args:
        module_name (str): the name of the module to import
    Returns:
        the module if it exists, otherwise None
    """
    try:
        return importlib.import_module(module_name)
    except ImportError:
        return None
