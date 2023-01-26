import deephaven_client as dh
import array as array
import numpy as np

class CallbackHandler:
    def __init__(self):
        self.int64Sum_ = 0

    def onTick(self, update : dh.TickingUpdate):
        current = update.current
        addedRows = update.addedRows

        int64Index = current.getColumnIndex("Int64Value", True)

        self.processDeltas(update.beforeRemoves, int64Index, update.removedRows, -1)
        self.processDeltas(update.afterAdds, int64Index, update.addedRows, 1)

        numModifies = 0
        if (int64Index < len(update.modifiedRows)):
            rowSequence = update.modifiedRows[int64Index]
            self.processDeltas(update.beforeModifies, int64Index, rowSequence, -1)
            self.processDeltas(update.afterModifies, int64Index, rowSequence, 1)
            numModifies += rowSequence.size

        print(f"int64Sum is {self.int64Sum_}, num adds={update.addedRows.size}, "
              f"num removes={update.removedRows.size}, num modifies={numModifies}")

    def onError(self, error: str):
        print(f"Error happened: {error}")
        
    def processDeltas(self, table: dh.Table, int64Index: int,
                      rows: dh.RowSequence, parity: int):
        int64Col = table.getColumn(int64Index)
        chunkSize = 65536
        int64Chunk = np.zeros(chunkSize, dtype=np.int64)

        while not rows.empty:
            theseRows = rows.take(chunkSize)
            theseRowsSize = theseRows.size
            rows = rows.drop(chunkSize)

            int64Col.fillChunk(theseRows, int64Chunk, None)

            for i in range(theseRowsSize):
                self.int64Sum_ += int64Chunk.data[i] * parity

# run this on the IDE
# from deephaven import time_table
# demo3m = time_table("00:00:00.000001").update(["Int64Value = (long)(Math.random() * 1000)", "Bucket  = (long)(ii % 1_000_000)"]).last_by("Bucket")
# sum3m = demo3m.view(["Int64Value"]).sum_by()

client = dh.Client.connect("localhost:10000")
manager = client.getManager()
handle = manager.fetchTable("demo3m")
subHandle = handle.subscribe(CallbackHandler())

line = input("Press enter to quit: ")
