#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#
"""
Support for running operations on JVM server threads, so that they can be given work from python. There are two
executors, "serial" and "concurrent". Any task that will take an exclusive UGP lock should use the serial executor,
otherwise the concurrent executor should be used.

Both executors currently share one server thread, so tasks submitted to either one run one at a time, in the order
they were submitted. Tasks submitted to "concurrent" do not run in parallel. A later version may give the executors
separate thread pools, so don't rely on this ordering.
"""

from collections.abc import Callable

import jpy

from deephaven import DHError
from deephaven.jcompat import j_runnable

_executors: dict[str, Callable[[Callable[[], None]], None]] = {}


def has_executor(executor_name: str) -> bool:
    """
    Returns True if an executor exists with that name.
    """
    return executor_name in executor_names()


def executor_names() -> list[str]:
    """
    Returns: the List of known executor names
    """
    return list(_executors.keys())


def submit_task(executor_name: str, task: Callable[[], None]) -> None:
    """
    Submits a task to run on a named executor. If no such executor exists, raises KeyError. The provided task should
    take care to set up any execution context or liveness scope to ensure that the task runs as intended.

    Typically, tasks should not block on other threads. Ensure tasks never block on other tasks submitted to any
    executor. The executors share one thread, so a task that waits for another submitted task deadlocks, even when the
    two tasks were submitted to different executors.

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
