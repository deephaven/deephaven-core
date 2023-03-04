import deephaven_client as dh

class CallbackHandler:
    def on_tick(self, update : dh.TickingUpdate):
        if not update.removed_rows.empty:
            print("=== REMOVES ===")
            print(update.before_removes.to_string(True, True, update.removed_rows))

        if not update.added_rows.empty:
            print("=== ADDS ===")
            print(update.after_adds.to_string(True, True, update.added_rows))

        if len(update.modified_rows) != 0:
            print("=== MODIFIES (before) ===")
            print(update.before_modifies.to_string(True, True, update.modified_rows))
            print("=== MODIFIES (after) ===")
            print(update.after_modifies.to_string(True, True, update.modified_rows))

    def on_error(self, error: str):
        print(f"Error happened: {error}")

client = dh.Client.connect("localhost:10000")
manager = client.get_manager()
handle = manager.time_table(0, 1000000000).update(["II = ii"]).tail(10)
sub_handle = handle.subscribe(CallbackHandler())

line = input("Press enter to quit: ")
