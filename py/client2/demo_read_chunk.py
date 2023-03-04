import deephaven_client as dh
import numpy as np

class CallbackHandler:
    def on_tick(self, update: dh.TickingUpdate):
        current = update.current
        rows = current.get_row_sequence()
        col = current.get_column_by_name("II", True)

        chunk_size = 100
        data = np.zeros(chunk_size, dtype=np.int64)

        sum = 0

        while not rows.empty:
            these_rows = rows.take(chunk_size)
            rows = rows.drop(chunk_size)

            col.fill_chunk(these_rows, data, None)

            for i in range(these_rows.size):
                sum += data.data[i]

        print(current.to_string(True, True))
        print(f"sum is {sum}")

    def on_error(self, error: str):
        print(f"Error happened: {error}")

client = dh.Client.connect("localhost:10000")
manager = client.get_manager()
handle = manager.time_table(0, 1000000000).update(["II = ii"]).tail(10)
sub_handle = handle.subscribe(CallbackHandler())

line = input("Press enter to quit: ")
