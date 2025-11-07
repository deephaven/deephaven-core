---
title: Deephaven Community Core Quickstart for the Python Client
sidebar_label: Python Client Quickstart
---

Deephaven's Python client [`pydeephaven`](https://pypi.org/project/pydeephaven/) enables users to connect to a Deephaven server running anywhere in the world. This guide will walk you through installing [`pydeephaven`](https://pypi.org/project/pydeephaven/), connecting to a server, and using the client to perform some real-time data tasks.

> [!NOTE]
> This document assumes that you already have a Deephaven server running on your machine. See the [Quickstart](./quickstart.md) or the detailed installation guides for [Docker](./docker-install.md) or [pip](./pip-install.md) to get Deephaven installed.

## 1. Install pydeephaven and connect to a running server

> [!NOTE]
> For installing the Deephaven Python client, we recommend using a Python [virtual environment](https://docs.python.org/3/library/venv.html) to decouple and isolate Python installs and associated packages.

[`pydeephaven`](https://pypi.org/project/pydeephaven/) is easily installed with [pip](https://en.wikipedia.org/wiki/Pip_(package_manager)):

```sh
pip3 install pydeephaven
```

Now, fire up a Python session, and let's get started!

The Deephaven Python client creates and maintains [a connection to the remote Deephaven server](./pip-install.md#start-a-deephaven-server) through a [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session). To connect to a remote server with a [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session), you must supply it with information about the remote server's hostname or URL, port, and authentication requirements. Here's an example:

```python docker-config=pyclient test-set=1
from pydeephaven import Session

client_session = Session(
    host="deephaven.local",
    port=10000,
    auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler",
    auth_token="YOUR_PASSWORD_HERE",
)
```

Once the connection has been established, you're ready to start using the Deephaven Python client.

## 2. A brief overview of the Python client design

The [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session) instance created above is the client's main line of communication with the server. It's used to run scripts on the server, create and fetch tables, and more. This functionality is facilitated by [methods](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session) attached to the [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session) instance.

Many of these methods appear to return tables. Take, for example, the [`time_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.time_table) method:

```python docker-config=pyclient test-set=1
t = client_session.time_table("PT1s")
```

This code appears to create a new ticking table called `t`. However, this is not the case - the Python client can only return _references_ to tables on the server. These references are represented with a [`Table`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table) class, and that class has its own methods that mirror the table operations of a true Deephaven table.

## 3. Create tables and retrieve references

New tables are created directly on the server with the [`empty_table`](../reference/table-operations/create/emptyTable.md) and [`time_table`](../reference/table-operations/create/timeTable.md) methods:

```python test-set=1
# creates a static table on the server and returns a reference
static_t_ref = client_session.empty_table(10).update(["X = ii"])

# creates a ticking table on the server and returns a reference
ticking_t_ref = client_session.time_table("PT1s").update(["X = ii"])
```

Tables created in this way do not yet have names on the server, so some Deephaven operations will not be possible. To name them, use the [`bind_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.bind_table) method:

```python test-set=1
# the table referenced by 'static_t_ref' will be called 'static_t' on the server
client_session.bind_table("static_t", static_t_ref)

# similarly, the ticking table will be called 'ticking_t' on the server
client_session.bind_table("ticking_t", ticking_t_ref)
```

Use the [`open_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.open_table) method to retrieve a reference to a named table:

```python test-set=1
# both 'static_t_ref' and 'static_t_ref_again' refer to the same table 'static_t'
static_t_ref_again = client_session.open_table("static_t")
```

If you have a local Python data structure that you want to convert to a Deephaven table on the server, use the [`import_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.import_table) method. This method will only accept an [Arrow table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html), so you must make the conversion before calling [`import_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.import_table):

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

Table references represented as [`Tables`](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table) have methods that mirror Deephaven table operations. In this way, table references can often be used _as if_ they were tables.

> [!NOTE]
> The table operations here are not intended to demonstrate a broad overview of what Deephaven offers. They are only for demonstrating how such operations are used in the Python client context. For a brief overview of table operations, check out the [Quickstart](./quickstart.md#4-working-with-deephaven-tables). For more details, visit the [table operations section of the Crash Course](./crash-course/table-ops.md).

All of the methods that have been implemented can be found in the [Pydocs](/core/client-api/python/code/pydeephaven.table.html#pydeephaven.table.Table). These include basic table operations like [`update`](../reference/table-operations/select/update.md), [`view`](../reference/table-operations/select/view.md), [`where`](../reference/table-operations/filter/where.md), and [`sort`](../reference/table-operations/sort/sort.md):

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

The [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) and [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) operations work seamlessly with the functions from the [`pydeephaven.agg`](/core/client-api/python/code/pydeephaven.agg.html) and [`pydeephaven.updateby`](/core/client-api/python/code/pydeephaven.updateby.html) Python modules:

```python test-set=1
import pydeephaven.agg as agg
import pydeephaven.updateby as uby

# group-wise mean and standard deviation for entire column
group_stats = table_ref.agg_by([agg.avg("AvgX=X"), agg.std("StdX=X")], by="Group")

# group-wise rolling mean and standard deviation over 30 minute window
rolling_stats = table_ref.update_by(
    [
        uby.rolling_avg_time("Timestamp", "AvgX=X", "PT30m"),
        uby.rolling_std_time("Timestamp", "StdX=X", "PT30m"),
    ],
    by="Group",
)
```

Check out the guides on [`agg_by`](../how-to-guides/combined-aggregations.md) and [`update_by`](../how-to-guides/rolling-aggregations.md) to learn more.

Table operations that require other tables as arguments, like [`join`](../reference/table-operations/join/join.md), are supported:

```python test-set=1
other_table_ref = client_session.empty_table(2).update(
    ["Group = ii % 2 == 0 ? `A` : `B`", "ValueToJoin = Group == `A` ? 1234 : 5678"]
)

# join on Group
joined_ref = table_ref.join(other_table_ref, on="Group", joins="ValueToJoin")
```

Even Deephaven's time-series joins like [`aj`](../reference/table-operations/join/aj.md) and [`raj`](../reference/table-operations/join/raj.md) are supported:

```python test-set=1
other_time_table_ref = client_session.empty_table(5 * 24 * 60).update(
    [
        "Timestamp = '2021-01-01T00:00:00Z' + (ii * MINUTE) + randomInt(0, 59)",
        "Group = randomBool() ? `A` : `B`",
        "Y = randomGaussian(0, 1) + (Group == `B` ? 10 : 0)",
    ]
)

# inexact join on Timestamp
time_joined_ref = table_ref.aj(other_time_table_ref, on="Timestamp", joins="Y")
```

Learn more about Deephaven's join operations in the [exact join guide](../how-to-guides/joins-exact-relational.md) and [inexact join guide](../how-to-guides/joins-timeseries-range.md).

## 5. Run scripts

The Python client can execute Python scripts on the server with the [`run_script`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.run_script) method. These scripts can include all of the Deephaven functionality that the server-side Python API supports. They should be encapsulated in strings:

```python test-set=1
client_session.run_script("from deephaven import time_table")
client_session.run_script("ticking_table = time_table('PT1s').update('X = ii')")
```

Tables created with scripts can then be used directly in downstream queries:

```python test-set=1
client_session.run_script(
    "\
ticking_table_avg = ticking_table\
.update('BinnedTimestamp = lowerBin(Timestamp, 5 * SECOND)')\
.drop_columns('Timestamp')\
.avg_by('BinnedTimestamp')"
)
```

Then, retrieve a reference to the resulting table with the [`open_table`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.open_table) method:

```python test-set=1
ticking_table_avg_ref = client_session.open_table("ticking_table_avg")
```

The client-side table operations discussed above may be a more convenient way to perform such table operations.

## 6. What to do next

Now that you've gotten a brief introduction to the Deephaven Python client, we suggest heading to the [Crash Course in Deephaven](../getting-started/crash-course/get-started.md) to learn more about Deephaven's real-time data platform.
