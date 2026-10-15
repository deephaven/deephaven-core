---
title: Deephaven Community Core Quickstart for the Python Client
---

Deephaven's Python client [`pydeephaven`](https://pypi.org/project/pydeephaven/) connects to a Deephaven server from any Python process, whether the server runs on your own machine or on a remote host. This guide walks you through installing `pydeephaven`, connecting to a server, creating and querying tables on the server, retrieving data, running scripts, and streaming data to the server.

> [!NOTE]
> This guide assumes that you already have a Deephaven server running. See the [Quickstart](./quickstart.md) or the detailed installation guides for [Docker](./docker-install.md) or [pip](./pip-install.md) to get Deephaven installed. The examples connect to a server on port `10000` that uses [pre-shared key authentication](../how-to-guides/authentication/auth-psk.md), which is how the Quickstart starts Deephaven.

## 1. Install `pydeephaven` and connect to a running server

[`pydeephaven`](https://pypi.org/project/pydeephaven/) requires Python 3.9 or later. Install it with [pip](https://en.wikipedia.org/wiki/Pip_(package_manager)), ideally inside a Python [virtual environment](https://docs.python.org/3/library/venv.html) to isolate it and its dependencies from other Python installs:

```sh
pip3 install pydeephaven
```

Now, start a Python interpreter, and let's get started!

The Deephaven Python client creates and maintains a connection to the server through a [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session). A `Session` takes the server's hostname or IP address, its port, and its authentication settings. The hostname and port each have a default:

- `host`: the `DH_HOST` environment variable if it's set, otherwise `localhost`.
- `port`: the `DH_PORT` environment variable if it's set, otherwise `10000`.

The `auth_type` and `auth_token` arguments must match how the server authenticates clients:

- `"Anonymous"` (the default): the server doesn't need a token. Use this for a server started with [anonymous authentication](../how-to-guides/authentication/auth-anon.md).
- `"Basic"`: the token is a `"user:password"` string. Use this for a server started with [username/password authentication](../how-to-guides/authentication/auth-uname-pw.md).
- A handler class name: the token must match what that handler expects. For example, `"io.deephaven.authentication.psk.PskAuthenticationHandler"` selects [pre-shared key authentication](../how-to-guides/authentication/auth-psk.md), and its token is the key itself.

A server started with a pre-shared key, as in the Quickstart, needs the PSK handler class name and the key:

```python docker-config=pyclient test-set=1
from pydeephaven import Session

client_session = Session(
    host="deephaven.local",
    port=10000,
    auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler",
    auth_token="YOUR_PASSWORD_HERE",
)
```

Replace `deephaven.local` with your server's hostname — `localhost` if the server runs on your machine. Replace `YOUR_PASSWORD_HERE` with the pre-shared key you set when you started the server (see the [Quickstart](./quickstart.md#1-install-and-launch-deephaven)).

The [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session) constructor connects to the server immediately, so you're ready to start using the Deephaven Python client.

## 2. Python client tables are references to server tables

The [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session) instance created above is the client's main line of communication with the server. Its methods run scripts on the server, create and fetch tables, and more.

Many of these methods appear to return tables. Take, for example, the [`time_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.time_table) method:

```python docker-config=pyclient test-set=1
t = client_session.time_table("PT1s")
```

This code creates a new ticking table on the server. A [ticking table](../conceptual/table-types.md) updates as new data arrives.

However, `t` doesn't hold that table's data. Methods like [`time_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.time_table) return _references_ to tables on the server, not the data itself. The [`Table`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table) class represents these references, and its methods mirror the table operations of a server-side Deephaven table. To pull a table's data into your Python process, see [Retrieve data from the server](#5-retrieve-data-from-the-server).

## 3. Create, name, open, and import tables

The [`empty_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.empty_table) and [`time_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.time_table) methods create new tables directly on the server. Section 2 already created the ticking table `t` with `time_table`. The following code creates a static table with `empty_table` and adds a column to each table:

```python test-set=1
# creates a static table on the server and returns a reference
static_t_ref = client_session.empty_table(10).update(["X = ii"])

# adds a column to the ticking table 't' from section 2 and returns a new reference
ticking_t_ref = t.update(["X = ii"])
```

Tables created in this way have no names on the server, so you can't open them by name, use them in server-side scripts, or see them in the [Deephaven web IDE](./quickstart.md#2-the-deephaven-ide). To name them, use the [`bind_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.bind_table) method:

```python test-set=1
# the table referenced by 'static_t_ref' is called 'static_t' on the server
client_session.bind_table("static_t", static_t_ref)

# similarly, the ticking table is called 'ticking_t' on the server
client_session.bind_table("ticking_t", ticking_t_ref)
```

Use the [`open_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.open_table) method to retrieve a reference to a named table:

```python test-set=1
# both 'static_t_ref' and 'static_t_ref_again' refer to the same table 'static_t'
static_t_ref_again = client_session.open_table("static_t")
```

If you have a local Python data structure that you want to convert to a Deephaven table on the server, use the [`import_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.import_table) method. This method accepts only an [Arrow table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html), so convert your data to one before calling `import_table`:

```python test-set=1
import pyarrow as pa

local_data = {"Col1": [1, 2, 3], "Col2": ["a", "b", "c"]}
local_data_arrow = pa.Table.from_pydict(local_data)

# import new data to the server
t_from_local_ref = client_session.import_table(local_data_arrow)
# give the new table a name on the server
client_session.bind_table("t_from_local", t_from_local_ref)
```

## 4. Table operations with the Python client

As described in section 2, [`Table`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table) objects have methods that mirror Deephaven table operations. As a result, you can often use table references _as if_ they were tables.

> [!NOTE]
> This section doesn't survey Deephaven's table operations. It shows how to call them from the Python client. For a brief overview of table operations, check out the [Quickstart](./quickstart.md#4-work-with-deephaven-tables). For more details, visit the [table operations section of the Crash Course](./crash-course/table-ops.md).

The [`Table` Pydocs](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table) list every method the class supports, including basic table operations like [`update`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table.update), [`view`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table.view), [`where`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table.where), and [`sort`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table.sort):

```python test-set=1
# create new table on the server and add columns
table_ref = client_session.empty_table(5 * 24 * 60).update(
    [
        "Timestamp = '2021-01-01T00:00:00Z' + ii * MINUTE",
        "Group = randomBool() ? `A` : `B`",
        "X = randomGaussian(0, 1) + (Group == `B` ? 10 : 0)",
    ]
)

# select specific columns
no_time_ref = table_ref.view(["Group", "X"])

# filter by values in Group
group_a_ref = table_ref.where("Group == `A`")

# sort by Group first, then by Timestamp
sorted_ref = table_ref.sort(["Group", "Timestamp"])
```

To learn more about these table operations, see the guides on [choosing a select method](../how-to-guides/use-select-view-update.md), [filtering](../how-to-guides/use-filters.md), and [sorting](../how-to-guides/sort.md).

The [`agg_by`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table.agg_by) and [`update_by`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table.update_by) operations work with the functions from the [`pydeephaven.agg`](/core/client-api/python/code/pydeephaven.agg.html) and [`pydeephaven.updateby`](/core/client-api/python/code/pydeephaven.updateby.html) Python modules:

```python test-set=1
import pydeephaven.agg as agg
import pydeephaven.updateby as uby

# group-wise mean and standard deviation for entire column
group_stats = table_ref.agg_by([agg.avg("AvgX = X"), agg.std("StdX = X")], by="Group")

# group-wise rolling mean and standard deviation over a 30-minute window
rolling_stats = table_ref.update_by(
    [
        uby.rolling_avg_time("Timestamp", "AvgX = X", "PT30m"),
        uby.rolling_std_time("Timestamp", "StdX = X", "PT30m"),
    ],
    by="Group",
)
```

Check out the guides on [combined aggregations](../how-to-guides/combined-aggregations.md) and [rolling aggregations](../how-to-guides/rolling-aggregations.md) to learn more.

The client also supports table operations that take other tables as arguments, such as [`join`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table.join):

```python test-set=1
other_table_ref = client_session.empty_table(2).update(
    ["Group = ii % 2 == 0 ? `A` : `B`", "ValueToJoin = Group == `A` ? 1234 : 5678"]
)

# join on Group
joined_ref = table_ref.join(other_table_ref, on="Group", joins="ValueToJoin")
```

The client supports Deephaven's [time-series joins](../how-to-guides/joins-timeseries-range.md) too, such as [`aj`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table.aj) and [`raj`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table.raj):

```python test-set=1
other_time_table_ref = client_session.empty_table(5 * 24 * 60).update(
    [
        "Timestamp = '2021-01-01T00:00:00Z' + (ii * MINUTE) + randomInt(0, 59) * SECOND",
        "Group = randomBool() ? `A` : `B`",
        "Y = randomGaussian(0, 1) + (Group == `B` ? 10 : 0)",
    ]
)

# as-of join: match Group exactly, then the closest earlier or equal Timestamp
time_joined_ref = table_ref.aj(
    other_time_table_ref, on=["Group", "Timestamp"], joins="Y"
)
```

Learn more about Deephaven's join operations in the [exact join guide](../how-to-guides/joins-exact-relational.md) and [time-series join guide](../how-to-guides/joins-timeseries-range.md).

## 5. Retrieve data from the server

Table references don't hold data. To pull a table's data into your Python process, use the [`to_arrow`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table.to_arrow) method, which takes a snapshot of the table and returns it as a [`pyarrow.Table`](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html). For a ticking table, the snapshot reflects the table at the moment you call `to_arrow`. The following example snapshots the `group_stats` aggregation from section 4:

```python test-set=1
# snapshot the server-side table into a local pyarrow Table
local_arrow = group_stats.to_arrow()

# convert it to a pandas DataFrame for use with other Python libraries
local_df = local_arrow.to_pandas()
```

## 6. Run scripts

The [`run_script`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.run_script) method runs Python scripts on a Deephaven server that uses Python. These scripts can include all of the Deephaven functionality that the server-side Python API supports. For table operations that the [`Table`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table) class already supports, the client-side methods from section 4 are usually more convenient; `run_script` is for server-side Python that the client API doesn't cover. Pass each script to `run_script` as a string:

```python test-set=1
client_session.run_script("from deephaven import time_table")
client_session.run_script("ticking_table = time_table('PT1s').update('X = ii')")
```

Later scripts can refer by name to tables that earlier scripts created:

```python test-set=1
client_session.run_script(
    """
ticking_table_avg = (
    ticking_table.update('BinnedTimestamp = lowerBin(Timestamp, 5 * SECOND)')
    .drop_columns('Timestamp')
    .avg_by('BinnedTimestamp')
)
"""
)
```

Then, retrieve a reference to the resulting table with the [`open_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.open_table) method:

```python test-set=1
ticking_table_avg_ref = client_session.open_table("ticking_table_avg")
```

## 7. Stream data with input tables

An [input table](../how-to-guides/input-tables.md) is a table on the server that you can add rows to. Keyed input tables also let you update and delete rows by key. Input tables are useful when your data source runs outside the Deephaven server.

The Python client creates input tables with the [`input_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.input_table) method and streams data to them with [`add`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.InputTable.add). The following example builds its data with the `pa` module that section 3 imported. It uploads that data with [`import_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.import_table):

```python test-set=1
# define schema
schema = pa.schema(
    [
        pa.field("Symbol", pa.string()),
        pa.field("Price", pa.float64()),
    ]
)

# create input table on server
input_table = client_session.input_table(schema=schema)
client_session.bind_table("prices", input_table)

# stream data: create Arrow table, upload, add, release
data = pa.table({"Symbol": ["AAPL", "GOOG"], "Price": [150.0, 140.0]})
uploaded = client_session.import_table(data)
try:
    input_table.add(uploaded)
finally:
    uploaded.close()  # releases server resources and marks object closed
```

For a complete guide on streaming patterns, memory management, and input table types, see [Send data to Deephaven from a Python client](../how-to-guides/client-input-tables.md).

## 8. Close the session

When you're done, close the session with the [`close`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.close) method:

```python test-set=1
client_session.close()
```

You can also create a [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session) in a `with` statement, which closes the session automatically when the block exits:

```python docker-config=pyclient order=null
from pydeephaven import Session

with Session(
    host="deephaven.local",
    port=10000,
    auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler",
    auth_token="YOUR_PASSWORD_HERE",
) as session:
    session.empty_table(5).to_arrow()
```

## 9. What to do next

Now that you've gotten a brief introduction to the Deephaven Python client, we suggest heading to the [Crash Course in Deephaven](./crash-course/get-started.md) to learn more about Deephaven's real-time data platform. To go further with the Python client, see:

- [Send data to Deephaven from a Python client](../how-to-guides/client-input-tables.md) for more input table patterns.
- [Capture Python client tables with Barrage](../how-to-guides/capture-tables.md) to subscribe to server tables from another Deephaven server.
- [`pydeephaven` Pydocs](/core/client-api/python) for the full client API.
