import deephaven_client as dh

class CallbackHandler:
    def on_tick(self, update : dh.TickingUpdate):
        print(update.current.to_string(True, True))

    def on_error(self, error: str):
        print(f"Error happened: {error}")

client = dh.Client.connect("localhost:10000")
manager = client.get_manager()
handle = manager.time_table(0, 1000000000).update(["II = ii"]).tail(10)
sub_handle = handle.subscribe(CallbackHandler())

line = input("Press enter to quit: ")
