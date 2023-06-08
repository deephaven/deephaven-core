from __future__ import annotations

import deephaven_client as dh
import deephaven_util as dhutil
import numpy as np
import numpy.typing as npt
from typing import Dict, Generator, List, Union

ColDictType = Dict[str, np.ndarray]
DictGeneratorType = Generator[ColDictType, None, None]
_DEFAULT_CHUNK_SIZE = 65536

def _make_generator(table: dh.Table,
                    rows: dh.RowSequence,
                    col_names: str | List[str] | None,
                    chunk_size: int | None,
                    reuse_buffers: bool | None) -> DictGeneratorType:

    col_names = dhutil.canonicalize_cols_param(table, col_names)
    if chunk_size is None:
        chunk_size = _DEFAULT_CHUNK_SIZE
    if reuse_buffers is None:
        reuse_buffers = False

    buffers: [np.ndarray] = [None] * len(col_names)

    while not rows.empty:
        these_rows = rows.take(chunk_size)
        rows = rows.drop(chunk_size)

        result : ColDictType = {}
        for i in range(len(col_names)):
            col_name = col_names[i]
            col = table.get_column_by_name(col_name, True)
            np_type = table.schema.get_numpy_type_by_name(col_name, True)
            data = buffers[i]
            if (not reuse_buffers) or (data is None):
                data = np.zeros(these_rows.size, dtype = np_type)
                buffers[i] = data

            data = data[0:these_rows.size]
            col.fill_chunk(these_rows, data, None)
            result[col_name] = data

        yield result


class TickingUpdateAdapter:
    update: dh.TickingUpdate

    def __init__(self, update: dh.TickingUpdate):
        self.update = update

    def removed(self,
                chunk_size: int | None = None,
                cols: str | List[str] | None = None,
                reuse_buffers: bool | None = None) -> DictGeneratorType:
        return _make_generator(
            self.update.before_removes, self.update.removed_rows,
            cols, chunk_size, reuse_buffers)

    def added(self,
              chunk_size: int | None = None,
              cols: str | List[str] | None = None,
              reuse_buffers: bool | None = None) -> DictGeneratorType:
        return _make_generator(
            self.update.after_adds, self.update.added_rows,
            cols, chunk_size, reuse_buffers)

    def modified_prev(self,
                      chunk_size: int | None = None,
                      cols: str | List[str] | None = None,
                      reuse_buffers: bool | None = None) -> DictGeneratorType:
        return _make_generator(
            self.update.before_modifies, self.update.all_modified_rows,
            cols, chunk_size, reuse_buffers)

    def modified(self,
                 chunk_size: int | None = None,
                 cols: str | List[str] | None = None,
                 reuse_buffers: bool | None = None) -> DictGeneratorType:
        return _make_generator(
            self.update.after_modifies, self.update.all_modified_rows,
            cols,chunk_size, reuse_buffers)


class CallbackHandler:
    _COL_NAME = "Int64Value"

    def __init__(self):
        self._int64_sum = 0

    def on_tick(self, update: dh.TickingUpdate):
        adapter = TickingUpdateAdapter(update)

        col_name = CallbackHandler._COL_NAME
        num_removes = self._process_deltas(adapter.removed(cols=col_name), -1)
        num_adds = self._process_deltas(adapter.added(cols=col_name), 1)
        self._process_deltas(adapter.modified_prev(cols=col_name), -1)
        num_modifies = self._process_deltas(adapter.modified(cols=col_name), 1)

        print(f"int64Sum is {self._int64_sum}, num adds={num_adds}, "
              f"num removes={num_removes}, num modifies={num_modifies}")

    def on_error(self, error: str):
        print(f"Error happened: {error}")
        
    def _process_deltas(self, col_chunks: DictGeneratorType, parity: int) -> int:
        num_processed = 0
        for dict in col_chunks:
            col = dict.get(CallbackHandler._COL_NAME)
            if col is None:
                continue

            for element in col:
                self._int64_sum += element * parity

            num_processed = num_processed + len(col)

        return num_processed


# run this on the IDE
# from deephaven import time_table
# demo3m = time_table("PT00:00:00.000001").update(["Int64Value = (long)(Math.random() * 1000)", "Bucket  = (long)(ii % 1_000_000)"]).last_by("Bucket")
# sum3m = demo3m.view(["Int64Value"]).sum_by()

client = dh.Client.connect("localhost:10000")
manager = client.get_manager()
handle = manager.fetch_table("demo3m")
subHandle = handle.subscribe(CallbackHandler())

line = input("Press enter to quit: ")
