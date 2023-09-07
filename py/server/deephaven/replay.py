#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" This module provides support for replaying historical data. """

from typing import Union
import jpy

import datetime
import numpy as np
import pandas as pd

from deephaven import dtypes, DHError, time
from deephaven._wrapper import JObjectWrapper
from deephaven.table import Table

_JReplayer = jpy.get_type("io.deephaven.engine.table.impl.replay.Replayer")


class TableReplayer(JObjectWrapper):
    """The TableReplayer is used to replay historical data.

    Tables to be replayed are registered with the replayer.  The resulting dynamic replay tables all update in sync,
    using the same simulated clock.  Each registered table must contain a timestamp column.
    """

    j_object_type = _JReplayer

    def __init__(self, start_time: Union[dtypes.Instant, int, str, datetime.datetime, np.datetime64, pd.Timestamp],
                 end_time: Union[dtypes.Instant, int, str, datetime.datetime, np.datetime64, pd.Timestamp]):
        """Initializes the replayer.

        Args:
             start_time (Union[dtypes.Instant, int, str, datetime.datetime, np.datetime64, pd.Timestamp]):
                replay start time.  Integer values are nanoseconds since the Epoch.
             end_time (Union[dtypes.Instant, int, str, datetime.datetime, np.datetime64, pd.Timestamp]):
                replay end time.  Integer values are nanoseconds since the Epoch.

        Raises:
            DHError
        """
        start_time = time.to_j_instant(start_time)
        end_time = time.to_j_instant(end_time)
        self.start_time = start_time
        self.end_time = end_time
        try:
            self._j_replayer = _JReplayer(start_time, end_time)
        except Exception as e:
            raise DHError(e, "failed to create a replayer.") from e

    @property
    def j_object(self) -> jpy.JType:
        return self._j_replayer

    def add_table(self, table: Table, col: str) -> Table:
        """Registers a table for replaying and returns the associated replay table.

        Args:
            table (Table): the table to be replayed
            col (str): column in the table containing timestamps

        Returns:
            a replay Table

        Raises:
            DHError
        """
        try:
            replay_table = Table(j_table=self._j_replayer.replay(table.j_table, col))
            return replay_table
        except Exception as e:
            raise DHError(e, "failed to add a historical table.") from e

    def start(self) -> None:
        """Starts replaying.

        Raises:
             DHError
        """
        try:
            self._j_replayer.start()
        except Exception as e:
            raise DHError(e, "failed to start the replayer.") from e

    def shutdown(self) -> None:
        """Shuts down and invalidates the replayer. After this call, the replayer can no longer be used."""
        try:
            self._j_replayer.shutdown()
        except Exception as e:
            raise DHError(e, "failed to shutdown the replayer.") from e
