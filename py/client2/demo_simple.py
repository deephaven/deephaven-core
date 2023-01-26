import deephaven_client as dh

client = dh.Client.connect("localhost:10000")
manager = client.getManager()
handle = manager.emptyTable(10).update(["II = ii"])
print(handle.toString(True))
