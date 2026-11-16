---
title: Table data service
---

This guide covers using the Table Data Service API to integrate custom, partitioned, lazily loaded data from an external service into Deephaven workflows.

To use the Table Data Service API, you implement a backend that supplies your data, and then construct a Table Data Service from it. The API provides classes with abstract methods that serve as a template for your implementation. This guide covers each of the components you implement, along with an example implementation.

> [!NOTE]
> This feature is currently experimental. The API and its characteristics are subject to change.

## How the API fits together

The Python [`TableDataService`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataService) API lets you serve data from an external service or database to Deephaven as PyArrow tables, which can back Deephaven tables in both static and refreshing contexts.

You implement three classes and use a fourth:

- [`TableKey`](#tablekey) identifies a table.
- [`TableLocationKey`](#tablelocationkey) identifies one location (partition) of a table.
- [`TableDataServiceBackend`](#tabledataservicebackend) supplies schemas, locations, sizes, and column values for those keys.
- [`TableDataService`](#tabledataservice) takes your backend and creates Deephaven tables from it.

Refreshing tables from the Python API are [add-only](../../conceptual/table-types.md#specialization-2-add-only). A backend can add new table locations and append rows to existing ones, but it can't remove a location or remove rows from one.

The following sections list the methods each class requires and give an example implementation of the three classes you implement. The [Example](#example) section uses those implementations to build static and refreshing tables.

## `TableKey`

A [`TableKey`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableKey) is a unique identifier for a table. An implementation must have the following methods:

- `__hash__`: Create a hash of the key.
- `__eq__`: Compare two keys for equality. If `key1 == key2` is true, then `hash(key1) == hash(key2)` must also be true.

The following code block contains an example [`TableKey`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableKey) implementation named `TableKeyImpl`. It implements both of the required methods, along with a `__str__` method so the key is readable in log and error messages.

```python test-set=1 order=null
from deephaven.experimental.table_data_service import TableKey


class TableKeyImpl(TableKey):
    """
    A basic implementation of a TableKey.
    """

    def __init__(self, key: str):
        self.key = key

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        if not isinstance(other, TableKeyImpl):
            return NotImplemented
        return self.key == other.key

    def __str__(self):
        return f"TableKeyImpl{{{self.key}}}"
```

## `TableLocationKey`

A [`TableLocationKey`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableLocationKey) is a unique identifier for a table location. A table location is one partition of a table, and it holds a subset of the table's rows. If the table has partitioning columns, the location also holds the partitioning values that identify it. A location's size is its row count.

An implementation requires the same methods as a [`TableKey`](#tablekey) implementation:

- `__hash__`: Create a hash of the key.
- `__eq__`: Compare two keys for equality. If `key1 == key2` is true, then `hash(key1) == hash(key2)` must also be true.

The following code block contains an example [`TableLocationKey`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableLocationKey) implementation named `TableLocationKeyImpl`. It implements both of the required methods, along with a `__str__` method so the key is readable in log and error messages.

```python test-set=1 order=null
from deephaven.experimental.table_data_service import TableLocationKey


class TableLocationKeyImpl(TableLocationKey):
    """
    A basic implementation of a TableLocationKey.
    """

    def __init__(self, key: str):
        self.key = key

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        if not isinstance(other, TableLocationKeyImpl):
            return NotImplemented
        return self.key == other.key

    def __str__(self):
        return f"TableLocationKeyImpl{{{self.key}}}"
```

## `TableDataServiceBackend`

A [`TableDataServiceBackend`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataServiceBackend) is the interface you implement to supply data to a [`TableDataService`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataService). You must implement each of its abstract methods:

- [`table_schema`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataServiceBackend.table_schema): Fetch the schema of a table.
- [`table_locations`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataServiceBackend.table_locations): Fetch the locations of a table.
- [`table_location_size`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataServiceBackend.table_location_size): Fetch the size of a table location.
- [`column_values`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataServiceBackend.column_values): Fetch the values of one column at one location, starting at row `offset`.
- [`subscribe_to_table_locations`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataServiceBackend.subscribe_to_table_locations): Subscribe to existing and future table locations.
- [`subscribe_to_table_location_size`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataServiceBackend.subscribe_to_table_location_size): Subscribe to existing and future table location sizes.

The [`TableDataService`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataService) calls [`table_locations`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataServiceBackend.table_locations) and [`table_location_size`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataServiceBackend.table_location_size) for static tables (`refreshing=False` in [`make_table`](#tabledataservice) or [`make_partitioned_table`](#tabledataservice)). It calls [`subscribe_to_table_locations`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataServiceBackend.subscribe_to_table_locations) and [`subscribe_to_table_location_size`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataServiceBackend.subscribe_to_table_location_size) for refreshing tables (`refreshing=True`).

Backend methods don't return their results. Each one calls the callbacks that the service passes to it:

| Method                                                                                                                                                                                                   | Delivers its result to                                              | Then calls                                |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- | ----------------------------------------- |
| [`table_schema`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataServiceBackend.table_schema)                                         | `schema_cb(data_schema, partitioning_schema)`                       | Nothing                                   |
| [`table_locations`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataServiceBackend.table_locations)                                   | `location_cb(location_key, partitioning_values)`, once per location | `success_cb`                              |
| [`table_location_size`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataServiceBackend.table_location_size)                           | `size_cb(row_count)`                                                | Nothing                                   |
| [`column_values`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataServiceBackend.column_values)                                       | `values_cb(pyarrow_table)`                                          | Nothing                                   |
| [`subscribe_to_table_locations`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataServiceBackend.subscribe_to_table_locations)         | `location_cb`, for existing and future locations                    | `success_cb` after the existing locations |
| [`subscribe_to_table_location_size`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataServiceBackend.subscribe_to_table_location_size) | `size_cb`, for the current and future sizes                         | `success_cb` after the current size       |

Each method reports a failure by calling its `failure_cb` callback with an `Exception`. The two `subscribe_*` methods keep the callbacks for future changes and return a function that the service calls to unsubscribe.

When the service calls `table_schema`, `table_locations`, `table_location_size`, or `column_values`, it waits until the method calls one of its callbacks, so every code path in those methods must call either the result callback or `failure_cb`. A backend can call the callbacks from another thread, for example when new data arrives. However, a callback called from another thread while the backend method is still running can block until that method returns, so a backend method shouldn't wait for another thread to call its callbacks.

The callbacks take these arguments:

- `data_schema`: The PyArrow schema of the data columns. It doesn't need to include the partitioning columns.
- `partitioning_schema`: The PyArrow schema of the partitioning columns, or `None` if the table has none.
- `partitioning_values`: A one-row PyArrow table that matches `partitioning_schema` and holds the location's partitioning values, or `None` if the table has no partitioning columns. Any other row count is an error.
- The table passed to `values_cb`: One column, with at least `min_rows` and at most `max_rows` rows. Any other row count is an error. `min_rows` is never larger than the service's [`page_size`](#tabledataservice).

The following code blocks contain two example classes:

- `TestTable` is an in-memory data store that holds the backend's data. It is a plain Python class, not a Deephaven table. Its `add_table_location` and `append_table_location` methods add a location and append rows to an existing one.
- `TestBackend` implements all six required methods of [`TableDataServiceBackend`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataServiceBackend), plus an `add_table` method that registers a table with the backend.

<details>
<summary>An in-memory data store for the backend</summary>

```python test-set=1 order=null
from typing import Optional, Dict, Callable
import pyarrow as pa


class TestTable:
    """
    An in-memory data store for the backend.
    """

    class TestTableLocation:
        """
        A custom table location implementation for the backend.
        """

        def __init__(
            self, data_schema: pa.Schema, partitioning_values: Optional[pa.Table]
        ):
            self.partitioning_values = partitioning_values
            self.size_cb: Callable[[int], None] = lambda *x: x
            self.size_failure_cb: Callable[[Exception], None] = lambda *x: x
            self.data: pa.Table = data_schema.empty_table()

        def append_data(self, new_data: pa.Table):
            """
            Append data to the table in batches.
            """
            rbs = self.data.to_batches()
            for batch in new_data.to_batches():
                rbs.append(batch)
            self.data = pa.Table.from_batches(rbs)
            self.size_cb(self.data.num_rows)

    def __init__(
        self, data_schema: pa.Schema, partitioning_column_schema: Optional[pa.Schema]
    ):
        self.data_schema = data_schema
        self.partitioning_column_schema = partitioning_column_schema
        self.locations: Dict[TableLocationKey, self.TestTableLocation] = {}
        self.location_cb: Callable[[TableLocationKeyImpl, Optional[pa.Table]], None] = (
            lambda *x: x
        )
        self.location_failure_cb: Callable[[Exception], None] = lambda *x: x

    def add_table_location(
        self,
        table_location_key: TableLocationKeyImpl,
        partitioning_column_values: Optional[pa.Table],
        data_values: pa.Table,
    ):
        """
        Add a new location to the table.
        """
        if table_location_key in self.locations:
            raise ValueError(f"Table location {table_location_key} already exists")
        new_location = self.TestTableLocation(
            self.data_schema, partitioning_column_values
        )
        new_location.append_data(data_values)
        self.locations[table_location_key] = new_location
        self.location_cb(table_location_key, partitioning_column_values)

    def append_table_location(
        self, table_location_key: TableLocationKeyImpl, data_values: pa.Table
    ):
        """
        Append data to an existing location in the table.
        """
        if table_location_key not in self.locations:
            raise ValueError(
                f"Cannot append to non-existent table location {table_location_key}"
            )
        self.locations[table_location_key].append_data(data_values)
```

</details>

<details>
<summary>A `TableDataServiceBackend` implementation</summary>

```python test-set=1 order=null
from deephaven.experimental.table_data_service import (
    TableDataServiceBackend,
    TableKey,
    TableLocationKey,
)


class TestBackend(TableDataServiceBackend):
    """
    A custom implementation of the TableDataServiceBackend for testing.
    """

    def __init__(self):
        self.tables: Dict[TableKey, TestTable] = {}

    def add_table(self, table_key: TableKeyImpl, table: TestTable):
        """
        Add a new table to the backend.
        """
        if table_key in self.tables:
            raise ValueError(f"{table_key} already exists")
        self.tables[table_key] = table

    def table_schema(
        self,
        table_key: TableKeyImpl,
        schema_cb: Callable[[pa.Schema, Optional[pa.Schema]], None],
        failure_cb: Callable[[Exception], None],
    ) -> None:
        """
        Fetch the schema of a table with a callable.
        """
        if table_key not in self.tables:
            failure_cb(KeyError(f"{table_key} does not exist"))
            return

        table = self.tables[table_key]
        schema_cb(table.data_schema, table.partitioning_column_schema)

    def table_locations(
        self,
        table_key: TableKeyImpl,
        location_cb: Callable[[TableLocationKeyImpl, Optional[pa.Table]], None],
        success_cb: Callable[[], None],
        failure_cb: Callable[[Exception], None],
    ) -> None:
        """
        Fetch the locations of a table with a callable.
        """
        if table_key not in self.tables:
            failure_cb(KeyError(f"{table_key} does not exist"))
            return

        for key, location in self.tables[table_key].locations.items():
            location_cb(key, location.partitioning_values)
        success_cb()

    def table_location_size(
        self,
        table_key: TableKeyImpl,
        table_location_key: TableLocationKeyImpl,
        size_cb: Callable[[int], None],
        failure_cb: Callable[[Exception], None],
    ) -> None:
        """
        Fetch the size of a table location with a callable.
        """
        if table_key not in self.tables:
            failure_cb(KeyError(f"{table_key} does not exist"))
            return

        table = self.tables[table_key]
        if table_location_key not in table.locations:
            failure_cb(
                KeyError(
                    f"{table_location_key} does not exist in table_key {table_key}"
                )
            )
            return

        size_cb(table.locations[table_location_key].data.num_rows)

    def column_values(
        self,
        table_key: TableKeyImpl,
        table_location_key: TableLocationKeyImpl,
        col: str,
        offset: int,
        min_rows: int,
        max_rows: int,
        values_cb: Callable[[pa.Table], None],
        failure_cb: Callable[[Exception], None],
    ) -> None:
        """
        Fetch column values with a callable.
        """
        if table_key not in self.tables:
            failure_cb(KeyError(f"{table_key} does not exist"))
            return

        table = self.tables[table_key]
        if table_location_key not in table.locations:
            failure_cb(
                KeyError(
                    f"{table_location_key} does not exist in table_key {table_key}"
                )
            )
            return

        location = table.locations[table_location_key]
        values_cb(location.data.select([col]).slice(offset, min_rows))

    def subscribe_to_table_locations(
        self,
        table_key: TableKeyImpl,
        location_cb: Callable[[TableLocationKeyImpl, Optional[pa.Table]], None],
        success_cb: Callable[[], None],
        failure_cb: Callable[[Exception], None],
    ) -> Callable[[], None]:
        """
        Subscribe to table locations with a callable.
        """
        if table_key not in self.tables:
            failure_cb(KeyError(f"{table_key} does not exist"))
            return lambda *x: x

        table = self.tables[table_key]
        table.location_cb = location_cb
        table.location_failure_cb = failure_cb

        # send all existing locations straight away
        for key, location in table.locations.items():
            location_cb(key, location.partitioning_values)
        success_cb()

        def unsubscribe():
            table.location_cb = lambda *x: x
            table.location_failure_cb = lambda *x: x

        return unsubscribe

    def subscribe_to_table_location_size(
        self,
        table_key: TableKeyImpl,
        table_location_key: TableLocationKeyImpl,
        size_cb: Callable[[int], None],
        success_cb: Callable[[], None],
        failure_cb: Callable[[Exception], None],
    ) -> Callable[[], None]:
        """
        Subscribe to table location size with a callable.
        """
        if table_key not in self.tables:
            failure_cb(KeyError(f"{table_key} does not exist"))
            return lambda *x: x

        table = self.tables[table_key]
        if table_location_key not in table.locations:
            failure_cb(
                KeyError(
                    f"{table_location_key} does not exist in table_key {table_key}"
                )
            )
            return lambda *x: x

        location = table.locations[table_location_key]
        location.size_cb = size_cb
        location.size_failure_cb = failure_cb

        # send existing size
        size_cb(location.data.num_rows)
        success_cb()

        def unsubscribe():
            location.size_cb = lambda *x: x
            location.size_failure_cb = lambda *x: x

        return unsubscribe
```

</details>

## `TableDataService`

The [`TableDataService`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataService) is the class you interact with. To create one, pass your [`TableDataServiceBackend`](#tabledataservicebackend) implementation to its constructor, along with up to three optional keyword-only parameters:

- `chunk_reader_factory`: The [Barrage](../../conceptual/what-is-barrage.md) chunk reader factory used to decode the PyArrow data the backend returns. Most backends can omit it.
- `stream_reader_options`: The Barrage options the service uses when it reads the PyArrow data. If omitted, the service uses Barrage's default snapshot options. Most backends can omit it.
- `page_size`: The page size, in rows, that the service uses when it requests column values. If omitted, the service uses the `PythonTableDataService.defaultPageSize` configuration property, which defaults to 65,536 rows (64K).

The [`TableDataService`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataService) class has two methods:

- [`make_table`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataService.make_table)

  Creates a table backed by the data service backend. Pass it the [`TableKey`](#tablekey) that identifies the table, and set `refreshing` to choose a static or refreshing table.

- [`make_partitioned_table`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataService.make_partitioned_table)

  Creates a [partitioned table](../partitioned-tables.md) backed by the data service backend. Pass it the [`TableKey`](#tablekey) that identifies the table, and set `refreshing` to choose a static or refreshing table.

## Example

This example combines the `TableKeyImpl`, `TableLocationKeyImpl`, `TestTable`, and `TestBackend` classes from the [`TableKey`](#tablekey), [`TableLocationKey`](#tablelocationkey), and [`TableDataServiceBackend`](#tabledataservicebackend) sections into a working [`TableDataService`](/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataService). It shows how to:

- Create a custom table and add it to the backend.
- Manually add data to the backend's table locations.
- Fetch a table from that backend in both static and refreshing contexts.

The code below creates two tables from the same backend:

- `t_static` is a static table (`refreshing=False`). It holds the rows the backend had when the engine first read the table. In this example, that read happens when the IDE displays `t_static`.
- `t` is a refreshing table (`refreshing=True`). It keeps updating while something holds a reference to it.

The code creates `t` inside a [liveness scope](../../conceptual/liveness-scope-concept.md), which manages the table and its subscriptions to the backend.

- The example doesn't release the scope, so `t` keeps updating after the `with` block ends.
- When you no longer need `t`, call the scope's [`release`](/core/pydoc/code/deephaven.liveness_scope.html#deephaven.liveness_scope.LivenessScope.release) method to drop the scope's reference to the table.
- Once nothing else holds the table, the engine cancels its subscriptions to the backend. For example, the engine cancels them after you release the scope and close the table in the IDE.

```python test-set=1 order=t_static,t
from deephaven.experimental.table_data_service import TableDataService
from deephaven.time import to_j_instant
import deephaven.arrow as dharrow
from deephaven import new_table
from deephaven.column import (
    bool_col,
    byte_col,
    char_col,
    short_col,
    int_col,
    long_col,
    float_col,
    double_col,
    string_col,
    datetime_col,
)
import numpy as np

# generate the same data for each location; the data doesn't need to include the partitioning columns
location_cols = [
    bool_col(name="Boolean", data=[True, False]),
    byte_col(name="Byte", data=(1, -1)),
    char_col(name="Char", data="-1"),
    short_col(name="Short", data=[1, -1]),
    int_col(name="Int", data=[1, -1]),
    long_col(name="Long", data=[1, -1]),
    long_col(name="NPLong", data=np.array([1, -1], dtype=np.int8)),
    float_col(name="Float", data=[1.01, -1.01]),
    double_col(name="Double", data=[1.01, -1.01]),
    string_col(name="String", data=["foo", "bar"]),
    datetime_col(
        name="Datetime",
        data=[
            to_j_instant("2024-10-01T12:30:00 ET"),
            to_j_instant("2024-10-01T12:45:00 ET"),
        ],
    ),
]
location_data = dharrow.to_arrow(new_table(cols=location_cols))


def generate_partitioning_values(ticker: str, exchange: str) -> pa.Table:
    partitioning_cols = [
        string_col(name="Ticker", data=[ticker]),
        string_col(name="Exchange", data=[exchange]),
    ]
    return dharrow.to_arrow(new_table(cols=partitioning_cols))


backend = TestBackend()
data_service = TableDataService(backend)

# generate a simple table
backend.add_table(
    TableKeyImpl("sample"),
    TestTable(
        location_data.schema,
        generate_partitioning_values("DUMMY_VAL", "DUMMY_VAL").schema,
    ),
)


def add_ticker_data(ticker: str, exchange: str):
    table_key = TableKeyImpl("sample")
    table_location_key = TableLocationKeyImpl(ticker + ":" + exchange)
    if table_key not in backend.tables:
        raise ValueError(f"{table_key} does not exist")
    if table_location_key not in backend.tables[table_key].locations:
        backend.tables[table_key].add_table_location(
            table_location_key,
            generate_partitioning_values(ticker, exchange),
            location_data,
        )
    else:
        backend.tables[table_key].append_table_location(
            table_location_key, location_data
        )


# add just a tiny amount of data
add_ticker_data("GOOG", "NYSE")
add_ticker_data("MSFT", "BZX")
add_ticker_data("MSFT", "BZY")

# a static table calls table_locations and table_location_size
t_static = data_service.make_table(TableKeyImpl("sample"), refreshing=False)

from deephaven.liveness_scope import LivenessScope

scope = LivenessScope()

# a refreshing table subscribes to the table locations and their sizes
with scope.open():
    t = data_service.make_table(TableKeyImpl("sample"), refreshing=True)
```

While the tables are open in the IDE, call `add_ticker_data` again to append more data. The refreshing table `t` shows the new rows, and the static table `t_static` does not change.

```python test-set=1 order=null
# this adds a new table location to the already opened table
add_ticker_data("GOOG", "BZX")

# these append to existing table locations of the already opened table
add_ticker_data("MSFT", "BZX")
add_ticker_data("MSFT", "BZY")
```

Each location occupies its own range of row keys, so the rows appended to `MSFT:BZX` and `MSFT:BZY` appear in `t` before the rows of `GOOG:BZX`, the location added later. Because rows don't always arrive at the end, the table is [add-only](../../conceptual/table-types.md#specialization-2-add-only) but not [append-only](../../conceptual/table-types.md#specialization-1-append-only).

## Related documentation

- [Custom data sources](../../reference/community-questions/custom-data-sources.md)
- [Liveness scopes](../../conceptual/liveness-scope-concept.md)
- [Partitioned tables](../partitioned-tables.md)
- [`deephaven.experimental.table_data_service` Pydoc](/core/pydoc/code/deephaven.experimental.table_data_service.html)
