---
title: Send data to Deephaven from a Python client
sidebar_label: Client input tables
---

This guide shows how to send data to Deephaven from an external Python application using `pydeephaven` and input tables. Input tables allow a client to add, update, and delete rows in a Deephaven table.

> [!NOTE]
> If your data source can run directly on the Deephaven server, consider using [server-side input tables](./input-tables.md) or [`DynamicTableWriter`](./table-publisher.md) instead. Server-side ingestion is generally more efficient because it avoids network overhead.

## When to use client-side input tables

Use client-side input tables when:

- **Managing reference data**: Upload configuration, lookup tables, or static datasets that may need updates.
- **Tracking state**: Maintain the latest status per entity (e.g., device status, order state, user preferences).
- **Interactive editing**: Allow users or external systems to add, update, or remove records.
- **Forwarding external data**: Relay data from message queues, APIs, or sensors running in separate processes.

## Basic pattern

The basic pattern for sending data from a Python client is:

1. **Create an input table on the server** using [`Session.input_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.input_table).
2. **Prepare data** in your client application.
3. **Convert to PyArrow** and upload with [`Session.import_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.import_table).
4. **Add to the input table** with [`InputTable.add`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.InputTable.add). For keyed tables, this inserts new rows or updates existing ones.
5. **Release the uploaded table** with [`Session.release`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.release) to prevent memory leaks.

Repeat steps 2-5 as needed.

## Complete example

The following example tracks device status using a keyed input table. Each device has a unique ID, and updates replace the previous status for that device.

```python skip-test
import pyarrow as pa
from pydeephaven import Session
from datetime import datetime, timezone

# Connect to Deephaven server (localhost:10000 is the default)
session = Session()

# Define schema for device status
schema = pa.schema(
    [
        pa.field("DeviceId", pa.string()),
        pa.field("Status", pa.string()),
        pa.field("LastSeen", pa.timestamp("ns", tz="UTC")),
    ]
)

# Create a keyed input table - DeviceId is the key
# Adding a row with an existing DeviceId updates that row
input_table = session.input_table(schema=schema, key_cols="DeviceId")

# Bind to a name so it's visible in the UI (check http://localhost:10000)
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
    input_table.add(uploaded)  # Inserts or updates based on DeviceId
    session.release(uploaded.ticket)


def remove_device(device_id: str):
    """Remove a device from the table."""
    keys = pa.table({"DeviceId": [device_id]})
    uploaded = session.import_table(keys)
    input_table.delete(uploaded)
    session.release(uploaded.ticket)


# Example usage
update_device("sensor-001", "online")
update_device("sensor-002", "online")
update_device("sensor-003", "offline")

# Update sensor-001's status (replaces the previous row)
update_device("sensor-001", "maintenance")

# Remove sensor-003
remove_device("sensor-003")

session.close()
```

## Memory management

Each call to `import_table` creates a temporary table on the server. If you don't release these tables, they accumulate and consume server memory.

**Always call `session.release(table.ticket)` after adding data to the input table.**

```python skip-test
# Upload data
uploaded = session.import_table(pa_table)

# Add to input table
input_table.add(uploaded)

# Release the temporary table
session.release(uploaded.ticket)
```

## Input table types

The client supports three types of input tables:

### Keyed

Rows are identified by key columns. Adding a row with an existing key replaces that row. Deletion is supported.

```python skip-test
# Keyed by DeviceId
input_table = session.input_table(schema=schema, key_cols="DeviceId")

# With multiple key columns
input_table = session.input_table(schema=schema, key_cols=["DeviceId", "Region"])
```

With keyed tables, you can delete rows by providing just the key values:

```python skip-test
# Delete rows by key
keys_to_delete = pa.table({"DeviceId": ["sensor-001", "sensor-003"]})
uploaded_keys = session.import_table(keys_to_delete)
input_table.delete(uploaded_keys)
session.release(uploaded_keys.ticket)
```

### Append-only

Rows are added to the end of the table. No key columns, no updates, no deletion. Use this when you need a simple log or event stream.

```python skip-test
# Append-only (default when no key_cols specified)
input_table = session.input_table(schema=schema)
```

### Blink

Rows are visible for only one [update graph](../conceptual/table-update-model.md) cycle, then disappear. Use this for event streams where you only need to process the latest batch.

```python skip-test
# Blink table
input_table = session.input_table(schema=schema, blink_table=True)
```

> [!NOTE]
> Blink tables cannot have key columns.

## Batch uploads

For better performance, batch multiple rows into a single upload:

```python skip-test
from datetime import datetime, timezone

now = datetime.now(timezone.utc)
batch_data = {
    "DeviceId": ["sensor-001", "sensor-002", "sensor-003"],
    "Status": ["online", "offline", "maintenance"],
    "LastSeen": [now, now, now],
}

# Upload entire batch at once
pa_table = pa.table(batch_data, schema=schema)
uploaded = session.import_table(pa_table)
input_table.add(uploaded)
session.release(uploaded.ticket)
```

## Related documentation

- [Input tables (server-side)](./input-tables.md)
- [Python client quickstart](../getting-started/pyclient-quickstart.md)
- [pydeephaven API reference](/core/client-api/python/code/pydeephaven.html)
- [DynamicTableWriter](./table-publisher.md)
