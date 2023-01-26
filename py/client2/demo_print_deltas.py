import deephaven_client as dh
import array as array
import numpy as np

class CallbackHandler:
    def onTick(self, update : dh.TickingUpdate):
        if not update.removedRows.empty:
            print("=== REMOVES ===")
            print(update.beforeRemoves.toString(True, True, update.removedRows))

        if not update.addedRows.empty:
            print("=== ADDS ===")
            print(update.afterAdds.toString(True, True, update.addedRows))

        if len(update.modifiedRows) != 0:
            print("=== MODIFIES (before) ===")
            print(update.beforeModifies.toString(True, True, update.modifiedRows))
            print("=== MODIFIES (after) ===")
            print(update.afterModifies.toString(True, True, update.modifiedRows))

    def onError(self, error: str):
        print(f"Error happened: {error}")

client = dh.Client.connect("localhost:10000")
manager = client.getManager()
handle = manager.timeTable(0, 1000000000).update(["II = ii"]).tail(10)
subHandle = handle.subscribe(CallbackHandler())

line = input("Press enter to quit: ")
