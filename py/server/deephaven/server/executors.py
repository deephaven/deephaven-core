#
# Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
"""
Support for running operations on JVM server threads, so that they can be given work from python. Initially, there
are two executors, "serial" and "concurrent". Any task that will take an exclusive UGP lock should use the serial
executor, otherwise the concurrent executor should be used. In the future there may be a "fast" executor, for use
when there is no chance of using either lock.
"""

from typing import Callable, Dict, List
import jpy
from deephaven.jcompat import j_runnable
from deephaven import DHError


_executors: Dict[str, Callable[[Callable[[], None]], None]] = {}


def has_executor(executor_name: str) -> bool:
    """
    Returns True if an executor exists with that name.
    """
    return executor_name in executor_names()


def executor_names() -> List[str]:
    """
    Returns: the List of known executor names
    """
    return list(_executors.keys())


def submit_task(executor_name: str, task: Callable[[], None]) -> None:
    """
    Submits a task to run on a named executor. If no such executor exists, raises KeyError. The provided task should
    take care to set up any execution context or liveness scope to ensure that the task runs as intended.

    Typically, tasks should not block on other threads. Ensure tasks never block on other tasks submitted to the same
    executor.

    Args:
        executor_name (str): the name of the executor to submit the task to
        task (Callable[[], None]): the function to run on the named executor

    Raises:
         KeyError if the executor name
    """
    _executors[executor_name](task)


def _register_named_java_executor(executor_name: str, java_executor: jpy.JType) -> None:
    """
    Provides a Java executor for user code to submit tasks to. Called during server startup.

    Args:
        executor_name (str): the name of the executor to register
        java_executor (jpy.JType): a Java Consumer<Runnable> instance

    Raises:
        DHError
    """
    if executor_name in executor_names():
        raise DHError(f"Executor with name {executor_name} already registered")
    _executors[executor_name] = lambda task: java_executor.accept(j_runnable(task))
