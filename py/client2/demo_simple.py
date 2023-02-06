import deephaven_client as dh

client = dh.Client.connect("localhost:10000")
manager = client.get_manager()
handle = manager.empty_table(10).update(["II = ii"])
print(handle.toString(True))
