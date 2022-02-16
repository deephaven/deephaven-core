#
#   Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#
""" Tools to obtain internal, Deephaven logs as tables. These tables include query logs and performance logs. """
import jpy

from deephaven2 import DHError
from deephaven2.table import Table

_JTableLoggers = jpy.get_type("io.deephaven.engine.table.impl.util.TableLoggers")


def process_info_log() -> Table:
    """ Returns a static table with process information for the current Deephaven engine process.

    Returns:
        a Table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JTableLoggers.processInfoLog())
    except Exception as e:
        raise DHError(e, "failed to obtain the process info log table.") from e


def process_memory_log() -> Table:
    """ Returns a table with process memory utilization and garbage collection data.

    Returns:
        a Table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JTableLoggers.processMemoryLog())
    except Exception as e:
        raise DHError(e, "failed to obtain the process memory log table.") from e


def process_metrics_log() -> Table:
    """ Returns a table with metrics collected for the current Deephaven engine process.

    Returns:
        a Table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JTableLoggers.processMetricsLog())
    except Exception as e:
        raise DHError(e, "failed to obtain the process metrics log table.") from e


def query_operation_performance_log() -> Table:
    """ Returns a table with Deephaven performance data for individual subqueries.

    Returns:
        a Table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JTableLoggers.queryOperationPerformanceLog())
    except Exception as e:
        raise DHError(e, "failed to obtain the query operation performance log table.") from e


def query_performance_log() -> Table:
    """ Returns a table with Deephaven query performance data. Performance data for individual sub-operations is
    available from calling `query_operation_performance_log`.

    Returns:
        a Table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JTableLoggers.queryPerformanceLog())
    except Exception as e:
        raise DHError(e, "failed to obtain the query performance log table.") from e


def update_performance_log() -> Table:
    """ Returns a table with Deephaven update performance data.

    Returns
        a Table

    Raises:
        DHError
    """
    try:
        return Table(j_table=_JTableLoggers.updatePerformanceLog())
    except Exception as e:
        raise DHError(e, "failed to obtain the update performance log table.") from e
