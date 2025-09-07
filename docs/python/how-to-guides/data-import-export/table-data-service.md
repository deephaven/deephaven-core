---
title: Table data service
---

This guide covers using the Table Data Service API to integrate custom data services into Deephaven workflows.

To use the Table Data Service API, you must create an implementation of its backend, which will be used to construct the Table Data Service itself. The API provides a series of classes with abstract methods that can be used as a template in a custom implementation. This guide covers all of the necessary components of the backend, along with an example implementation.

> [!NOTE]
> This feature is currently experimental. The API and its characteristics are subject to change.

### `TableDataService` API

The Python `TableDataService` API simplifies how the engine thinks about data being served by a remote service or database. It provides a way for users to consume data from external sources into Deephaven in the format of PyArrow tables, which can back Deephaven tables in both static and refreshing contexts.

Each class in the API has some requirements when creating an implementation. The following sections describe the requirements for each class and give an example.

### TableKey

A `TableKey` is a unique identifier for a table. An implementation must have the following methods:

- `__hash__`: Create a hash of the key.
- `__eq__`: Compare two keys for equality. If `TableKey1 == TableKey2` is true, then `hash(TableKey1) == hash(TableKey2)` must also be true.

The following code block contains an example implementation of a `TableKey`, aptly named `TableKeyImpl`. It implements both of the required methods, along with a `__str__` method for key validation.

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

### TableLocationKey

A `TableLocationKey` is a unique identifier for a location within a table. An implementation has the same methods required as a [`TableKey`](#tablekey) implementation:

- `__hash__`: Create a hash of the key.
- `__eq__`: Compare two keys for equality. If `TableLocationKey1 == TableLocationKey2` is true, then `hash(TableLocationKey1) == hash(TableLocationKey2)` must also be true.

The following code block contains an example implementation of a `TableLocationKey`, aptly named `TableLocationKeyImpl`. It implements both of the required methods, along with a `__str__` method for key validation.

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

### TableDataServiceBackend

A `TableDataServiceBackend` is the interface that must be implemented to provide data to the `TableDataService`. It has a series of abstract methods that must be implemented to provide the necessary information to the `TableDataService`. The methods are:

- `table_schema`: Fetch the schema of a table.
- `table_locations`: Fetch the locations of a table.
- `table_location_size`: Fetch the size of a table location.
- `column_values`: Fetch column values.
- `susbcribe_to_table_locations`: Subscribe to existing and future table locations.
- `subscribe_to_table_location_size`: Subscribe to existing and future table location sizes.

The following code blocks contain example implementations of a `TableDataServiceBackend`, named `TestBackend`, and `TestTable`, a custom table implementation used by the backend. The backend implements all six required methods, along with some additional methods to add tables and locations to the backend.

<details>
<summary>A custom table implementation for the backend</summary>

```python test-set=1 order=null
from typing import Optional, Dict, Callable
import pyarrow as pa


class TestTable:
    """
    A custom table implementation for the backend.
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
            self.size_failure_cb: Callable[[], None] = lambda *x: x
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
        self.location_failure_cb: Callable[[str], None] = lambda *x: x

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
            raise ValueError(
                f"Cannot add table location {table_location_key} already exists"
            )
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
        failure_cb: Callable[[str], None],
    ) -> None:
        """
        Fetch the schema of a table with a callable.
        """
        if table_key not in self.tables:
            failure_cb(f"{table_key} does not exist")
            return

        table = self.tables[table_key]
        schema_cb(table.data_schema, table.partitioning_column_schema)

    def table_locations(
        self,
        table_key: TableKeyImpl,
        location_cb: Callable[[TableLocationKeyImpl, Optional[pa.Table]], None],
        success_cb: Callable[[], None],
        failure_cb: Callable[[str], None],
    ) -> None:
        """
        Fetch the locations of a table with a callable.
        """
        if table_key not in self.tables:
            failure_cb(f"{table_key} does not exist")
            return

        for key, location in self.tables[table_key].locations:
            location_cb([key, location.partitioning_values])
        success_cb()

    def table_location_size(
        self,
        table_key: TableKeyImpl,
        table_location_key: TableLocationKeyImpl,
        size_cb: Callable[[int], None],
        failure_cb: Callable[[str], None],
    ) -> None:
        """
        Fetch the size of a table location with a callable.
        """
        if table_key not in self.tables:
            failure_cb(f"{table_key} does not exist")
            return

        table = self.tables[table_key]
        if table_location_key not in table.locations:
            failure_cb(f"{table_location_key} does not exist in table_key {table_key}")
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
        failure_cb: Callable[[str], None],
    ) -> None:
        """
        Fetch column values with a callable.
        """
        if table_key not in self.tables:
            failure_cb(f"{table_key} does not exist")
            return

        table = self.tables[table_key]
        if table_location_key not in table.locations:
            failure_cb(f"{table_location_key} does not exist in table_key {table_key}")
            return

        location = table.locations[table_location_key]
        values_cb(location.data.select([col]).slice(offset, min_rows))

    def subscribe_to_table_locations(
        self,
        table_key: TableKeyImpl,
        location_cb: Callable[[TableLocationKeyImpl, Optional[pa.Table]], None],
        success_cb: Callable[[], None],
        failure_cb: Callable[[str], None],
    ) -> Callable[[], None]:
        """
        Subscribe to table locations with a callable.
        """
        if table_key not in self.tables:
            failure_cb(f"{table_key} does not exist")
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
        failure_cb: Callable[[str], None],
    ) -> Callable[[], None]:
        """
        Subscribe to table location size with a callable.
        """
        if table_key not in self.tables:
            failure_cb(f"{table_key} does not exist")
            return lambda *x: x

        table = self.tables[table_key]
        if table_location_key not in table.locations:
            failure_cb(f"{table_location_key} does not exist in table_key {table_key}")
            return lambda *x: x

        location = table.locations[table_location_key]
        location.size_cb = size_cb
        location.failure_cb = failure_cb

        # send existing size
        size_cb(location.data.num_rows)
        success_cb()

        def unsubscribe():
            location.size_cb = lambda *x: x
            location.failure_cb = lambda *x: x

        return unsubscribe
```

</details>

### TableDataService

The `TableDataService` is the main class that users interact with. It is instantiated using the [`TableDataServiceBackend`](#tabledataservicebackend) implementation, along with other optional parameters.

The `TableDataService` class has two methods:

- [`make_table`](https://docs.deephaven.io/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataService.make_table)

  Creates a table backed by the data service backend. It is given a unique [`TableKey`](#tablekey) and is told whether the table is static or refreshing.

- [`make_partitioned_table`](https://docs.deephaven.io/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataService.make_partitioned_table)

  Creates a partitioned table backed by the data service backend. It is given a unique [`TableKey`](#tablekey) and is told whether the table is static or refreshing.

The [`usage`](#usage) section of the example below calls [`make_table`](https://docs.deephaven.io/core/pydoc/code/deephaven.experimental.table_data_service.html#deephaven.experimental.table_data_service.TableDataService.make_table) to create a table from a table data service.

## Example

The example presented in this guide uses the code presented in the sections above to demonstrate how to use the `TableDataService` API to fetch a table in both static and refreshing contexts. The example is split into two parts, each in its own subsection.

### Backend data service implementation

The following code block provides a backend implementation for a data service. It shows how to:

- Implement a sample [`TableDataServiceBackend`](#tabledataservicebackend).
- Manually manipulate the [`TableDataServiceBackend`](#tabledataservicebackend) to demonstrate the behavior of static and refreshing scenarios.
- Create a custom table implementation and add it to the backend.
- Fetch a table from that backend in both static and refreshing contexts.

<details>
<summary>A backend data service implementation</summary>

```python test-set=2 order=null
from deephaven.experimental.table_data_service import (
    TableDataServiceBackend,
    TableKey,
    TableLocationKey,
    TableDataService,
)
from typing import Callable, Optional, Dict
import pyarrow as pa


class TableKeyImpl(TableKey):
    """
    A simple implementation of a TableKey.
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


class TableLocationKeyImpl(TableLocationKey):
    """
    A simple implementation of a TableLocationKey.
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


class TestTable:
    """
    A custom table implementation for the backend.
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
            self.size_failure_cb: Callable[[], None] = lambda *x: x
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
        self.location_failure_cb: Callable[[str], None] = lambda *x: x

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
            raise ValueError(
                f"Cannot add table location {table_location_key} already exists"
            )
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
        failure_cb: Callable[[str], None],
    ) -> None:
        """
        Fetch the schema of a table with a callable.
        """
        if table_key not in self.tables:
            failure_cb(f"{table_key} does not exist")
            return

        table = self.tables[table_key]
        schema_cb(table.data_schema, table.partitioning_column_schema)

    def table_locations(
        self,
        table_key: TableKeyImpl,
        location_cb: Callable[[TableLocationKeyImpl, Optional[pa.Table]], None],
        success_cb: Callable[[], None],
        failure_cb: Callable[[str], None],
    ) -> None:
        """
        Fetch the locations of a table with a callable.
        """
        if table_key not in self.tables:
            failure_cb(f"{table_key} does not exist")
            return

        for key, location in self.tables[table_key].locations:
            location_cb([key, location.partitioning_values])
        success_cb()

    def table_location_size(
        self,
        table_key: TableKeyImpl,
        table_location_key: TableLocationKeyImpl,
        size_cb: Callable[[int], None],
        failure_cb: Callable[[str], None],
    ) -> None:
        """
        Fetch the size of a table location with a callable.
        """
        if table_key not in self.tables:
            failure_cb(f"{table_key} does not exist")
            return

        table = self.tables[table_key]
        if table_location_key not in table.locations:
            failure_cb(f"{table_location_key} does not exist in table_key {table_key}")
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
        failure_cb: Callable[[str], None],
    ) -> None:
        """
        Fetch column values with a callable.
        """
        if table_key not in self.tables:
            failure_cb(f"{table_key} does not exist")
            return

        table = self.tables[table_key]
        if table_location_key not in table.locations:
            failure_cb(f"{table_location_key} does not exist in table_key {table_key}")
            return

        location = table.locations[table_location_key]
        values_cb(location.data.select([col]).slice(offset, min_rows))

    def subscribe_to_table_locations(
        self,
        table_key: TableKeyImpl,
        location_cb: Callable[[TableLocationKeyImpl, Optional[pa.Table]], None],
        success_cb: Callable[[], None],
        failure_cb: Callable[[str], None],
    ) -> Callable[[], None]:
        """
        Subscribe to table locations with a callable.
        """
        if table_key not in self.tables:
            failure_cb(f"{table_key} does not exist")
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
        failure_cb: Callable[[str], None],
    ) -> Callable[[], None]:
        """
        Subscribe to table location size with a callable.
        """
        if table_key not in self.tables:
            failure_cb(f"{table_key} does not exist")
            return lambda *x: x

        table = self.tables[table_key]
        if table_location_key not in table.locations:
            failure_cb(f"{table_location_key} does not exist in table_key {table_key}")
            return lambda *x: x

        location = table.locations[table_location_key]
        location.size_cb = size_cb
        location.failure_cb = failure_cb

        # send existing size
        size_cb(location.data.num_rows)
        success_cb()

        def unsubscribe():
            location.size_cb = lambda *x: x
            location.failure_cb = lambda *x: x

        return unsubscribe
```

</details>

### Usage

The following code block demonstrates how to use the backend implementation to fetch a table in both static and refreshing contexts.

<details>
<summary>Usage of the example table data service backend</summary>

```python test-set=2 order=t
from deephaven.time import to_j_instant
import deephaven.arrow as dharrow
from deephaven import new_table
from deephaven.column import *
import numpy as np

# generate the same data for each location; noting that we do not need to include partitioning columns
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

from deephaven.liveness_scope import LivenessScope

scope = LivenessScope()

with scope.open():
    t = data_service.make_table(TableKeyImpl("sample"), refreshing=True)
```

With the table open and visible in the IDE, more data can be appended with subsequent calls to `add_ticker_data`.

```python test-set=2 order=t
# this adds a new table location to the already opened table
add_ticker_data("GOOG", "BZX")

# these append to existing table locations of the already opened table
add_ticker_data("MSFT", "BZX")
add_ticker_data("MSFT", "BZY")
```

</details>

<!-- TODO:
## Related documentation

Pydoc once 0.37 comes out. -->
