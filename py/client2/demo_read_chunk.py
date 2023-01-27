import deephaven_client as dh
import array as array
import numpy as np

class CallbackHandler:
    def onTick(self, update : dh.TickingUpdate):
        current = update.current
        rows = current.getRowSequence()
        int64Col = current.getColumnByName("II", True)

        chunkSize = 100
        int64Chunk = np.zeros(chunkSize, dtype=np.int64)

        sum = 0

        while not rows.empty:
            theseRows = rows.take(chunkSize)
            theseRowsSize = theseRows.size
            rows = rows.drop(chunkSize)

            int64Col.fillChunk(theseRows, int64Chunk, None)

            for i in range(theseRowsSize):
                sum += int64Chunk.data[i]

        print(current.toString(True, True))        
        print(f"sum is {sum}")

    def onError(self, error: str):
        print(f"Error happened: {error}")

client = dh.Client.connect("localhost:10000")
manager = client.getManager()
handle = manager.timeTable(0, 1000000000).update(["II = ii"]).tail(10)
subHandle = handle.subscribe(CallbackHandler())

line = input("Press enter to quit: ")
