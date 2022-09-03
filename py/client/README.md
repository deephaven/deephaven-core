
# Deephaven Python Client 

Deephaven Python Client is a Python package created by Deephaven Data Labs. It is a client API that allows Python applications to remotely access Deephaven data servers.

## Source Directory

### From the deephaven-core repository root 
(clone from https://github.com/deephaven/deephaven-core)
``` shell
$ cd pyclient
```
## Dev environment setup
``` shell
$ pip3 install -r requirements.txt
```

## Build
``` shell
$ python3 setup.py bdist_wheel
```
## Run tests
``` shell
$ python3 -m unittest discover tests

```
## Run examples
``` shell
$ python3 -m examples.demo_table_ops
$ python3 -m examples.demo_query
$ python3 -m examples.demo_run_script
$ python3 -m examples.demo_merge_tables
$ python3 -m examples.demo_asof_join

```
## Install
``` shell
$ pip3 install dist/pydeephaven-0.17.0-py3-none-any.whl
```
## Quick start

```python    
    >>> from pydeephaven import Session
    >>> session = Session() # assuming Deephaven Community Edition is running locally with the default configuration
    >>> table1 = session.time_table(period=1000000000).update(formulas=["Col1 = i % 2"])
    >>> df = table1.snapshot().to_pandas()
    >>> print(df)
                        Timestamp  Col1
    0     1629681525690000000     0
    1     1629681525700000000     1
    2     1629681525710000000     0
    3     1629681525720000000     1
    4     1629681525730000000     0
    ...                   ...   ...
    1498  1629681540670000000     0
    1499  1629681540680000000     1
    1500  1629681540690000000     0
    1501  1629681540700000000     1
    1502  1629681540710000000     0

    >>> session.close()

```

## Initialize

The `Session` class is your connection to Deephaven. This is what allows your Python code to interact with a Deephaven server.

```
from pydeephaven import Session

session = Session()
```

## Ticking table

The `Session` class has many methods that create tables. This example creates a ticking time table and binds it to Deephaven.

```
from pydeephaven import Session

session = Session()

table = session.time_table(period=1000000000).update(formulas=["Col1 = i % 2"])
session.bind_table(name="my_table", table=table)
```

This is the general flow of how the Python client interacts with Deephaven. You create a table (new or existing), execute some operations on it, and then bind it to Deephaven. Binding the table gives it a named reference on the Deephaven server, so that it can be used from the Web API or other Sessions.

## Execute a query on a table

`table.update()` can be used to execute an update on a table. This updates a table with a query string.

```
from pydeephaven import Session

session = Session()

# Create a table with no columns and 3 rows
table = session.empty_table(3)
# Create derived table having a new column MyColumn populated with the row index "i"
table = table.update(["MyColumn = i"])
# Update the Deephaven Web Console with this new table
session.bind_table(name="my_table", table=table)
```

## Sort a table

`table.sort()` can be used to sort a table. This example sorts a table by one of its columns.

```
from pydeephaven import Session

session = Session()

table = session.empty_table(5)
table = table.update(["SortColumn = 4-i"])

table = table.sort(["SortColumn"])
session.bind_table(name="my_table", table=table)
```

## Filter a table

`table.where()` can be used to filter a table. This example filters a table using a filter string.

```
from pydeephaven import Session

session = Session()

table = session.empty_table(5)
table = table.update(["Values = i"])

table = table.where(["Values % 2 == 1"])
session.bind_table(name="my_table", table=table)
```

## Query objects

Query objects are a way to create and manage a sequence of Deephaven query operations as a single unit. Query objects have the potential to perform better than the corresponding individual queries, because the query object can be transmitted to the server in one request rather than several, and because the system can perform certain optimizations when it is able to see the whole sequence of queries at once. They are similar in spirit to prepared statements in SQL.

The general flow of using a query object is to construct a query with a table, call the table operations (sort, filter, update, etc.) on the query object, and then assign your table to the return value of `query.exec()`.

Any operation that can be executed on a table can also be executed on a query object. This example shows two operations that compute the same result, with the first one using the table updates and the second one using a query object.

```
from pydeephaven import Session

session = Session()

table = session.empty_table(10)

# executed immediately
table1= table.update(["MyColumn = i"]).sort(["MyColumn"]).where(["MyColumn > 5"]);

# create Query Object (execution is deferred until the "exec" statement)
query_obj = session.query(table).update(["MyColumn = i"]).sort(["MyColumn"]).where(["MyColumn > 5"]);
# Transmit the QueryObject to the server and execute it
table2 = query_obj.exec();

session.bind_table(name="my_table1", table=table1)
session.bind_table(name="my_table2", table=table2)
```

## Join two tables

`table.join()` is one of many operations that can join two tables, as shown below.

```
from pydeephaven import Session

session = Session()

table1 = session.empty_table(5)
table1 = table1.update(["Values1 = i", "Group = i"])
table2 = session.empty_table(5)
table2 = table2.update(["Values2 = i + 10", "Group = i"])

table = table1.join(table2, on=["Group"])
session.bind_table(name="my_table", table=table)
```

## Use a combo aggregation on a table

Combined aggregations can be executed on tables in the Python client. This example creates a combo aggregation that averages the `Count` column of a table, and aggregates it by the `Group` column.

```
from pydeephaven import Session, ComboAggregation

session = Session()

table = session.empty_table(10)
table = table.update(["Count = i", "Group = i % 2"])

my_agg = ComboAggregation()
my_agg = my_agg.avg(["Count"])

table = table.agg_by(my_agg, ["Group"])
session.bind_table(name="my_table", table=table)
```

## Convert a PyArrow table to a Deephaven table

Deephaven natively supports [PyArrow tables](https://arrow.apache.org/docs/python/index.html). This example converts between a PyArrow table and a Deephaven table.

```
import pyarrow
from pydeephaven import Session

session = Session()

arr = pyarrow.array([4,5,6], type=pyarrow.int32())
pyarrow_table = pyarrow.Table.from_arrays([arr], names=["Integers"])

table = session.import_table(pyarrow_table)
session.bind_table(name="my_table", table=table)

#Convert the Deephaven table back to a pyarrow table
pyarrow_table = table.snapshot()
```

## Execute a script server side

`session.run_script()` can be used to execute code on the Deephaven server. This is useful when operations cannot be done on the client-side, such as creating a dynamic table writer. This example shows how to execute a script server-side and retrieve a table generated from the script.

```
from pydeephaven import Session

session = Session()

script = """
from deephaven import empty_table

table = empty_table(8).update(["Index = i"])
"""

session.run_script(script)

table = session.open_table("table")
print(table.snapshot())
```

## Error handling

The `DHError` is thrown whenever the client package encounters an error. This example shows how to catch a `DHError`.

```
from pydeephaven import Session, DHError

try:
    session = Session(host="invalid_host")
except DHError as e:
    print("Deephaven error when connecting to session")
    print(e)
except Exception as e:
    print("Unknown error")
    print(e)
```

## Related documentation
* https://deephaven.io/
* https://arrow.apache.org/docs/python/index.html

## API Reference
[start here] https://deephaven.io/core/client-api/python/
