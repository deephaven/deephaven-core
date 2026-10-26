---
title: Send data to Deephaven from a Python client
---

This guide shows how to send data to Deephaven from an external Python application using [`pydeephaven`](/core/client-api/python/code/pydeephaven.html) and input tables. Input tables allow a client to add rows to a Deephaven table. Keyed input tables also let the client update and delete rows by key.

> [!NOTE]
> If your data source can run directly on the Deephaven server, consider using [server-side input tables](./input-tables.md) or a [table publisher or `DynamicTableWriter`](./table-publisher.md) instead. Server-side ingestion is generally more efficient because it avoids network overhead.

## When to use client-side input tables

Use client-side input tables for:

- **Managing reference data**: Upload configuration, lookup tables, or other datasets that change infrequently.
- **Tracking state**: Maintain the latest status per entity (e.g., device status, order state, user preferences).
- **Interactive editing**: Allow users or external systems to add, update, or remove records.
- **Forwarding external data**: Relay data from message queues, APIs, or sensors running in separate processes.

## Basic pattern

The basic pattern for sending data from a Python client is:

1. **Create an input table on the server** using [`Session.input_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.input_table). See [Input table types](#input-table-types) to choose a keyed, append-only, or blink table.
2. **Prepare data** in your client application.
3. **Convert the data to a PyArrow table** and upload it with [`Session.import_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.import_table).
4. **Add to the input table** with [`InputTable.add`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.InputTable.add). For a [keyed table](#keyed), `add` inserts rows with new keys and replaces rows whose key already exists.
5. **Close the uploaded table** with [`Table.close`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table.close) to release server resources promptly.

Repeat steps 2-5 as needed.

## Complete example

The following example tracks device status using a keyed input table. Each device has a unique ID, and updates replace the previous status for that device.

The example connects to a server that uses pre-shared key authentication. Replace `YOUR_PASSWORD_HERE` with your pre-shared key. See the [Python client quickstart](../getting-started/pyclient-quickstart.md) for other connection and authentication options.

```python skip-test
import pyarrow as pa
from pydeephaven import Session
from datetime import datetime, timezone

# Define schema for device status
schema = pa.schema(
    [
        pa.field("DeviceId", pa.string()),
        pa.field("Status", pa.string()),
        pa.field("LastSeen", pa.timestamp("ns", tz="UTC")),
    ]
)

# Use context manager to ensure session is closed on success or failure
with Session(
    host="localhost",
    port=10000,
    auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler",
    auth_token="YOUR_PASSWORD_HERE",
) as session:
    # Create a keyed input table - DeviceId is the key
    # Adding a row with an existing DeviceId updates that row
    input_table = session.input_table(schema=schema, key_cols="DeviceId")

    # Bind to a name so it's visible in the web IDE (http://localhost:10000/ide)
    session.bind_table("device_status", input_table)

    def update_device(device_id: str, status: str):
        """Add or update a device's status."""
        pa_table = pa.table(
            {
                "DeviceId": [device_id],
                "Status": [status],
                "LastSeen": pa.array(
                    [datetime.now(timezone.utc)], type=pa.timestamp("ns", tz="UTC")
                ),
            }
        )

        uploaded = session.import_table(pa_table)
        try:
            input_table.add(uploaded)  # Inserts or updates based on DeviceId
        finally:
            uploaded.close()

    def remove_device(device_id: str):
        """Remove a device from the table."""
        keys = pa.table({"DeviceId": [device_id]})
        uploaded = session.import_table(keys)
        try:
            input_table.delete(uploaded)
        finally:
            uploaded.close()

    # Example usage
    update_device("sensor-001", "online")
    update_device("sensor-002", "online")
    update_device("sensor-003", "offline")

    # Update sensor-001's status (replaces the previous row)
    update_device("sensor-001", "maintenance")

    # Remove sensor-003
    remove_device("sensor-003")
```

This example sends one row per call for clarity. For higher volumes, send several rows in one upload, as shown in [Batch uploads](#batch-uploads).

## Input table types

The client supports three types of input tables. The snippets in this section run inside the session from the [complete example](#complete-example) and use the `pa`, `session`, and `schema` names it defines. The delete snippet also uses its keyed `input_table`.

### Keyed

Key columns identify each row. Adding a row with an existing key replaces that row. You can also delete rows by key.

```python skip-test
# Pass a list to key on more than one column; each key column must be in the schema
regional_schema = pa.schema(
    [
        pa.field("DeviceId", pa.string()),
        pa.field("Region", pa.string()),
        pa.field("Status", pa.string()),
        pa.field("LastSeen", pa.timestamp("ns", tz="UTC")),
    ]
)
regional_table = session.input_table(
    schema=regional_schema, key_cols=["DeviceId", "Region"]
)
```

To delete rows from a keyed table, pass a table that contains every key column and no other columns to [`InputTable.delete`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.InputTable.delete). The server rejects a delete table that is missing a key column or has any other column. Keys that aren't in the table are ignored:

```python skip-test
# Delete rows by key
keys_to_delete = pa.table({"DeviceId": ["sensor-001", "sensor-002"]})
uploaded_keys = session.import_table(keys_to_delete)
try:
    input_table.delete(uploaded_keys)
finally:
    uploaded_keys.close()
```

### Append-only

Each call to [`add`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.InputTable.add) appends rows to the end of the table. Append-only tables have no key columns and don't support updates or deletes. Use this when you need to keep every row, such as a log or an event history.

```python skip-test
# Append-only (default when no key_cols specified)
log_table = session.input_table(schema=schema)
```

### Blink

Rows in a [blink table](../conceptual/table-types.md#specialization-3-blink) are visible for only one [update graph](../conceptual/table-update-model.md) cycle, then disappear. Use this for event streams where you only need to process the latest batch.

```python skip-test
# Blink table
event_table = session.input_table(schema=schema, blink_table=True)
```

> [!NOTE]
> Blink input tables can't have key columns. `input_table` raises a `ValueError` if you pass both `key_cols` and `blink_table=True`.

## Batch uploads

For better performance, batch multiple rows into a single upload. This snippet runs inside the session from the [complete example](#complete-example) and uses its imports (`pa`, `datetime`, and `timezone`) and its `session`, `schema`, and `input_table` names:

```python skip-test
now = datetime.now(timezone.utc)
batch_data = {
    "DeviceId": ["sensor-001", "sensor-002", "sensor-003"],
    "Status": ["online", "offline", "maintenance"],
    "LastSeen": [now, now, now],
}

# Upload entire batch at once
pa_table = pa.table(batch_data, schema=schema)
uploaded = session.import_table(pa_table)
try:
    input_table.add(uploaded)
finally:
    uploaded.close()
```

## Memory management

Each call to [`import_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.import_table) creates a temporary table on the server. The client releases it when the Python object is garbage-collected, but that timing is not deterministic.

**Always call [`close`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table.close) on uploaded tables after adding data to the input table.** This releases the temporary table on the server and marks the local object as closed. Use `try`/`finally` to ensure cleanup even if the call to [`add`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.InputTable.add) fails. The `update_device` and `remove_device` helpers in the [complete example](#complete-example) follow this pattern.

## Related documentation

- [Input tables (server-side)](./input-tables.md)
- [Python client quickstart](../getting-started/pyclient-quickstart.md)
- [`pydeephaven` API reference](/core/client-api/python/code/pydeephaven.html)
- [Write data to an in-memory, real-time table](./table-publisher.md)
