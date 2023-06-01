import deephaven_client as dh
import numpy as np

class CallbackHandler:
    def __init__(self):
        self._int64_sum = 0

    def on_tick(self, update: dh.TickingUpdate):
        current = update.current
        col_index = current.get_column_index("Int64Value", True)

        self._process_deltas(update.before_removes, col_index, update.removed_rows, -1)
        self._process_deltas(update.after_adds, col_index, update.added_rows, 1)

        num_modifies = 0
        if (col_index < len(update.modified_rows)):
            row_sequence = update.modified_rows[col_index]
            self._process_deltas(update.before_modifies, col_index, row_sequence, -1)
            self._process_deltas(update.after_modifies, col_index, row_sequence, 1)
            num_modifies += row_sequence.size

        print(f"int64Sum is {self._int64_sum}, num adds={update.added_rows.size}, "
              f"num removes={update.removed_rows.size}, num modifies={num_modifies}")

    def on_error(self, error: str):
        print(f"Error happened: {error}")
        
    def _process_deltas(self, table: dh.Table, col_index: int,
                        rows: dh.RowSequence, parity: int):
        data_col = table.getColumn(col_index)
        chunk_size = 65536
        chunk = np.zeros(chunk_size, dtype=np.int64)

        while not rows.empty:
            these_rows = rows.take(chunk_size)
            rows = rows.drop(chunk_size)

            data_col.fill_chunk(these_rows, chunk, None)

            for i in range(these_rows.size):
                self._int64_sum += chunk.data[i] * parity

# run this on the IDE
# from deephaven import time_table
# demo3m = time_table("PT00:00:00.000001").update(["Int64Value = (long)(Math.random() * 1000)", "Bucket  = (long)(ii % 1_000_000)"]).last_by("Bucket")
# sum3m = demo3m.view(["Int64Value"]).sum_by()

client = dh.Client.connect("localhost:10000")
manager = client.get_manager()
handle = manager.fetch_table("demo3m")
subHandle = handle.subscribe(CallbackHandler())

line = input("Press enter to quit: ")
