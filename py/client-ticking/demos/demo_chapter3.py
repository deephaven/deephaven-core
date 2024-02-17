#
#  Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
#
from __future__ import annotations

import pydeephaven as pyd
import pyarrow as pa
from typing import Dict, List, Tuple
from pydeephaven import TableListener, TableUpdate, listen

session = pyd.Session(host="localhost", port=10000)

class MyDiffListener(TableListener):
    def on_update(self, update: TableUpdate) -> None:
        self._show_deltas("REMOVES", update.removed())
        self._show_deltas("ADDS", update.added())
        self._show_deltas("MODIFIED-PREV", update.modified_prev())
        self._show_deltas("MODIFIED", update.modified())

    def _show_deltas(self, what: str, dict: Dict[str, pa.Array]):
        if len(dict) == 0:
            return

        print(f"*** {what} ***")
        for name, data in dict.items():
            print(f"{name}: {data}")

def print_diffs():
    table_name = input("Please enter the table name: ")
    table = session.open_table(table_name)
    listener_handle = listen(table, MyDiffListener())
    listener_handle.start()
    return listener_handle

while True:
    listener_handle = print_diffs()

    while True:
        line = input("Enter 'stop' to stop: ")
        if line == "stop":
            break

    listener_handle.stop()
