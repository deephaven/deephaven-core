#
# Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
#

"""This package is a place for Deephaven experimental features."""

import jpy

from deephaven import DHError
from deephaven.table import Table
from deephaven.time import DurationLike, to_j_duration
from deephaven.update_graph import auto_locking_ctx

_JWindowCheck = jpy.get_type("io.deephaven.engine.util.WindowCheck")
_JTailInitializationFilter = jpy.get_type(
    "io.deephaven.engine.table.impl.util.TailInitializationFilter"
)


def time_window(table: Table, ts_col: str, window: int, bool_col: str) -> Table:
    """Creates a new table by applying a time window to the source table and adding a new Boolean column.

    The value of the new Boolean column is set to false when the timestamp column value is older than the window from
    now or true otherwise. If the timestamp column value is null, the Boolean column value will be null as well. The
    result table ticks whenever the source table ticks, or modifies a row when it passes out of the window.

    Args:
        table (Table): the source table
        ts_col (str): the timestamp column name
        window (int): the size of the window in nanoseconds
        bool_col (str): the name of the new Boolean column.

    Returns:
        a new Table

    Raises:
        DHError
    """
    try:
        with auto_locking_ctx(table):
            return Table(
                j_table=_JWindowCheck.addTimeWindow(
                    table.j_table, ts_col, window, bool_col
                )
            )
    except Exception as e:
        raise DHError(e, "failed to create a time window table.") from e


class TailInitializationFilter:
    """Filters an add-only source table down to only its most recent rows, keeping either a fixed time window or a
    fixed number of rows from each partition of the source. A partition is a region of the source's on-disk storage
    (e.g. a Parquet fragment) when the timestamp column is region-backed, otherwise each contiguous range of row keys,
    so a plain in-memory table is a single partition. Rows added to the source after filtering are passed through
    unchanged.
    """

    @staticmethod
    def most_recent(table: Table, ts_col: str, period: DurationLike) -> Table:
        """Filters an add-only table down to the most recent rows in each partition of its source.

        For each partition, the newest (last) timestamp is found, and rows whose timestamp falls within ``period`` before
        that newest timestamp are retained.

        Args:
            table (Table): the add-only source table to filter
            ts_col (str): the name of the timestamp column; each partition must be sorted ascending by this column with
                no null values
            period (DurationLike): the look-behind window measured from the newest timestamp in each partition

        Returns:
            a new Table

        Raises:
            DHError
        """
        try:
            j_duration = to_j_duration(period)
            if j_duration is None:
                raise ValueError("period must not be None")
            nanos = j_duration.toNanos()
            with auto_locking_ctx(table):
                return Table(
                    j_table=_JTailInitializationFilter.mostRecent(
                        table.j_table, ts_col, nanos
                    )
                )
        except Exception as e:
            raise DHError(e, "failed to apply tail initialization filter.") from e

    @staticmethod
    def most_recent_rows(table: Table, row_count: int) -> Table:
        """Filters an add-only table down to the last ``row_count`` rows in each partition of the source table.

        Args:
            table (Table): the add-only source table to filter
            row_count (int): the number of most-recent rows to keep per partition

        Returns:
            a new Table

        Raises:
            DHError
        """
        try:
            with auto_locking_ctx(table):
                return Table(
                    j_table=_JTailInitializationFilter.mostRecentRows(
                        table.j_table, row_count
                    )
                )
        except Exception as e:
            raise DHError(e, "failed to apply tail initialization filter.") from e
