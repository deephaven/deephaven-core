---
title: Stream data from a Python client with input tables
sidebar_label: Client input tables
---

This guide shows how to stream data into Deephaven from an external Python application using `pydeephaven` and input tables. This is useful when your data source runs outside the Deephaven server — for example, a separate process collecting sensor data, receiving messages from a queue, or pulling from an external API.

> [!NOTE]
> If your data source can run directly on the Deephaven server, consider using [server-side input tables](./input-tables.md) or [`DynamicTableWriter`](./table-publisher.md) instead. Server-side ingestion is generally more efficient because it avoids network overhead.

## When to use client-side input tables

Use client-side input tables when:

- Your data source runs in a separate process or machine.
- You need to forward data from an external system (message queue, API, sensor) to Deephaven.
- You want to keep data collection logic separate from server-side analytics.

## The streaming pattern

The basic pattern for streaming data from a Python client is:

1. **Create an input table on the server** using [`Session.input_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.input_table).
2. **Receive data** in your client application.
3. **Convert to PyArrow** and upload with [`Session.import_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.import_table).
4. **Append to the input table** with [`InputTable.add`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.InputTable.add).
5. **Release the uploaded table** with [`Session.release`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.release) to prevent memory leaks.

Repeat steps 2-5 to continuously stream data.

## Complete example

The following example simulates a sensor data stream. The client generates random temperature readings and streams them to the server every second.

```python skip-test
import time
import pyarrow as pa
from pydeephaven import Session

# Connect to Deephaven server (localhost:10000 is the default)
session = Session()

# Define schema for sensor data
schema = pa.schema(
    [
        pa.field("Timestamp", pa.timestamp("ns", tz="UTC")),
        pa.field("SensorId", pa.string()),
        pa.field("Temperature", pa.float64()),
    ]
)

# Create an append-only input table on the server
input_table = session.input_table(schema=schema)

# Bind to a name so it's visible in the UI (check http://localhost:10000)
session.bind_table("sensor_data", input_table)

print("Streaming sensor data. Press Ctrl+C to stop.")

try:
    while True:
        # Simulate receiving sensor data
        import random
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        sensor_id = f"sensor_{random.randint(1, 5)}"
        temperature = 20.0 + random.gauss(0, 2)

        # Create a PyArrow table with the new data
        pa_table = pa.table(
            {
                "Timestamp": pa.array([now], type=pa.timestamp("ns", tz="UTC")),
                "SensorId": pa.array([sensor_id]),
                "Temperature": pa.array([temperature]),
            }
        )

        # Upload to server
        uploaded = session.import_table(pa_table)

        # Append to input table
        input_table.add(uploaded)

        # IMPORTANT: Release to prevent memory leak
        session.release(uploaded.ticket)

        print(f"{now}: {sensor_id} = {temperature:.1f}°C")
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopped.")
finally:
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

### Append-only

Rows are added to the end of the table. No key columns, no deletion support.

```python skip-test
# Append-only (default)
input_table = session.input_table(schema=schema)
```

### Keyed

Rows are identified by key columns. Adding a row with an existing key replaces that row. Deletion is supported.

```python skip-test
# Keyed by SensorId
input_table = session.input_table(schema=schema, key_cols="SensorId")

# With multiple key columns
input_table = session.input_table(schema=schema, key_cols=["SensorId", "Region"])
```

With keyed tables, you can also delete rows:

```python skip-test
# Delete rows by key
keys_to_delete = pa.table({"SensorId": ["sensor_1", "sensor_3"]})
uploaded_keys = session.import_table(keys_to_delete)
input_table.delete(uploaded_keys)
session.release(uploaded_keys.ticket)
```

### Blink

Rows are visible for only one [update graph](../conceptual/table-update-model.md) cycle, then disappear. This is useful for event streams where you only need to see the latest batch.

```python skip-test
# Blink table
input_table = session.input_table(schema=schema, blink_table=True)
```

> [!NOTE]
> Blink tables cannot have key columns.

## Batch uploads

For better performance when streaming high volumes of data, batch multiple rows into a single upload:

```python skip-test
# Collect multiple readings
batch_data = {
    "Timestamp": [],
    "SensorId": [],
    "Temperature": [],
}

for _ in range(100):
    # Collect data...
    batch_data["Timestamp"].append(now)
    batch_data["SensorId"].append(sensor_id)
    batch_data["Temperature"].append(temperature)

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
