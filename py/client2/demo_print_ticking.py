import deephaven_client as dh
import array as array
import numpy as np

class CallbackHandler:
    def onTick(self, update : dh.TickingUpdate):
        print(update.current.toString(True, True))

    def onError(self, error: str):
        print(f"Error happened: {error}")

client = dh.Client.connect("localhost:10000")
manager = client.getManager()
handle = manager.timeTable(0, 1000000000).update(["II = ii"]).tail(10)
subHandle = handle.subscribe(CallbackHandler())

line = input("Press enter to quit: ")
