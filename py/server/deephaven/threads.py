#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
"""
Support for running operations on JVM server threads, so that they can be given work from python.
"""

from typing import Callable, Dict
from concurrent.futures import ThreadPoolExecutor
import jpy
from .jcompat import j_runnable


_executors: Dict[str, Callable[[Callable[[], None]], None]] = {}


def has_named_executor(executor_name: str) -> bool:
    """
    Returns True if the named executor exists and can have tasks submitted to it.
    """
    return hasattr(_executors, executor_name)


def submit_task(executor_name: str, task: Callable[[], None]) -> None:
    """
    Submits a task to run on a named executor. If no such executor exists, raises KeyError.
    """
    _executors[executor_name](task)


def _register_named_java_executor(executor_name: str, java_executor: jpy.JType):
    """
    Provides a Java executor for user code to submit tasks to.
    """
    _executors[executor_name] = lambda task: java_executor.accept(j_runnable(task))


# def _register_named_executor(executor_name: str, executor: Callable[[Callable[[], None]], None]) -> None:
#     """
#     Provides a Python executor for user code to submit tasks to. Only intended to be used until Java threads
#     are available for this.
#     """
#     _executors[executor_name] = executor
#
#
# _register_named_executor('serial', ThreadPoolExecutor(1).submit)
# _register_named_executor('concurrent', ThreadPoolExecutor(4).submit)
