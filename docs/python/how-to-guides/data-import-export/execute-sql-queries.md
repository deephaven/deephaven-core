---
title: Execute SQL queries in Deephaven
---

[Structured Query Language (SQL)](https://en.wikipedia.org/wiki/SQL) is one of the most popular languages for database management and access. You can use SQL with Deephaven in two ways. Use the [`deephaven.dbc`](/core/pydoc/code/deephaven.dbc.html) Python package to run SQL against an external database and read the result into a Deephaven table. Use the experimental [`deephaven.experimental.sql`](/core/pydoc/code/deephaven.experimental.sql.html) module to run SQL against Deephaven tables in your session.

This guide shows you how to do both. SQL queries against Deephaven tables work on ticking tables too, and their results update as the source tables change.

## Query an external database

[`deephaven.dbc`](/core/pydoc/code/deephaven.dbc.html) provides three functions:

- [`read_sql`](../../reference/data-import-export/SQL/read_sql.md)
  - Executes a provided SQL query via a [supported driver](#supported-drivers-and-required-packages).
- [`odbc.read_cursor`](../../reference/data-import-export/SQL/odbc.md)
  - Reads query results from a [turbodbc](https://turbodbc.readthedocs.io/en/latest/) cursor over the [Open Database Connectivity (ODBC)](https://learn.microsoft.com/en-us/sql/odbc/reference/what-is-odbc?view=sql-server-ver16) interface.
- [`adbc.read_cursor`](../../reference/data-import-export/SQL/adbc.md)
  - Reads query results from a cursor over the [Apache Arrow Database Connectivity (ADBC)](https://arrow.apache.org/docs/dev/format/ADBC.html) interface.

### Choose the right function

Consider the following factors when you choose among these three functions.

#### High-level vs low-level

[`read_sql`](../../reference/data-import-export/SQL/read_sql.md) is a high-level function for executing a query against a SQL database. Pass the SQL query as `query`. Pass a connection string or an existing turbodbc or ADBC `Connection` object as `conn`. It returns a static table that holds the query's result.

[`odbc.read_cursor`](../../reference/data-import-export/SQL/odbc.md) and [`adbc.read_cursor`](../../reference/data-import-export/SQL/adbc.md) are lower-level functions because they read from a cursor that has already executed a query. You open the cursor and execute the query yourself, then pass the cursor to the function. Use one of these when you need control over the cursor itself, for example to execute a parameterized query.

#### ODBC vs ADBC

The two lower-level options can both achieve the same results. When choosing one or the other, consider:

- ODBC ([`odbc.read_cursor`](../../reference/data-import-export/SQL/odbc.md)) is a well-known specification that works with relational databases that have an ODBC driver.
- ADBC ([`adbc.read_cursor`](../../reference/data-import-export/SQL/adbc.md)) fetches data in Arrow format from databases that support Arrow natively, such as databases that serve the Arrow Flight SQL protocol. It also works with other databases that have an ADBC driver, such as PostgreSQL and SQLite.

### Supported drivers and required packages

[`read_sql`](../../reference/data-import-export/SQL/read_sql.md) can use one of three supported drivers, which you choose with its `driver` argument. To use a driver, [install the corresponding packages](../install-and-use-python-packages.md):

- `"connectorx"` ([ConnectorX](https://github.com/sfu-db/connector-x), the default)
  - [connectorx](https://pypi.org/project/connectorx/)
- `"odbc"` ([ODBC](https://learn.microsoft.com/en-us/sql/odbc/reference/what-is-odbc?view=sql-server-ver16))
  - [turbodbc](https://pypi.org/project/turbodbc/)
- `"adbc"` ([ADBC](https://arrow.apache.org/adbc/current/index.html))
  - [adbc-driver-manager](https://pypi.org/project/adbc-driver-manager/)
  - [adbc-driver-flightsql](https://pypi.org/project/adbc-driver-flightsql/) for Arrow Flight SQL
  - [adbc-driver-postgresql](https://pypi.org/project/adbc-driver-postgresql/) for PostgreSQL
  - [adbc-driver-sqlite](https://pypi.org/project/adbc-driver-sqlite/) for SQLite

If you pass a turbodbc or ADBC `Connection` object instead of a connection string, [`read_sql`](../../reference/data-import-export/SQL/read_sql.md) ignores `driver`.

[`odbc.read_cursor`](../../reference/data-import-export/SQL/odbc.md) and [`adbc.read_cursor`](../../reference/data-import-export/SQL/adbc.md) need the same packages as the `"odbc"` and `"adbc"` drivers, respectively.

For ODBC, you need a turbodbc build with Apache Arrow support, an ODBC driver manager, and your database's ODBC driver.

For ADBC, you always need `adbc-driver-manager`, along with the driver package for your database. With a connection string, [`read_sql(driver="adbc")`](../../reference/data-import-export/SQL/read_sql.md) supports only two databases:

- PostgreSQL, when the connection string is a `postgresql://` URI.
- SQLite, when the connection string is empty.

For other ADBC drivers, such as Flight SQL, pass an ADBC `Connection` object as `conn`, or use [`adbc.read_cursor`](../../reference/data-import-export/SQL/adbc.md).

### Execute a query

With the necessary packages installed, you can create a table from a SQL query with [`read_sql`](../../reference/data-import-export/SQL/read_sql.md), [`odbc.read_cursor`](../../reference/data-import-export/SQL/odbc.md), or [`adbc.read_cursor`](../../reference/data-import-export/SQL/adbc.md). Each example below pulls from a PostgreSQL database.

> [!TIP]
> It's best practice not to store credentials in plaintext. The following examples read the username, password, host, and port of the PostgreSQL database from environment variables.

#### `read_sql`

```python skip-test
from deephaven.dbc import read_sql
from urllib.parse import quote
import os

my_query = (
    "SELECT t_ts AS Timestamp, CAST(t_id AS text) AS Id, "
    "CAST(t_instrument AS text) AS Instrument, "
    "t_exchange AS Exchange, t_price AS Price, t_size AS Size "
    "FROM crypto_trades"
)

username = os.environ["POSTGRES_USERNAME"]
password = os.environ["POSTGRES_PASSWORD"]
host = os.environ["POSTGRES_HOST"]
port = os.environ["POSTGRES_PORT"]

sql_uri = f"postgresql://{host}:{port}/postgres?user={quote(username)}&password={quote(password)}"

crypto_trades = read_sql(conn=sql_uri, query=my_query, driver="connectorx")
```

#### `odbc.read_cursor`

Before you run this example, install the PostgreSQL ODBC driver and register it with the ODBC driver manager. The `Driver` value in the connection string must match the registered driver name (`PostgreSQL Unicode` in this example). `turbodbc.connect` takes an ODBC connection string rather than a PostgreSQL URI.

```python skip-test
from deephaven.dbc import odbc as dhodbc
import os
import turbodbc

username = os.environ["POSTGRES_USERNAME"]
password = os.environ["POSTGRES_PASSWORD"]
host = os.environ["POSTGRES_HOST"]
port = os.environ["POSTGRES_PORT"]

sql_query = (
    "SELECT t_ts AS Timestamp, CAST(t_id AS text) AS Id, "
    "CAST(t_instrument AS text) AS Instrument, "
    "t_exchange AS Exchange, t_price AS Price, t_size AS Size "
    "FROM crypto_trades"
)

connection_string = f"Driver={{PostgreSQL Unicode}};Server={host};Port={port};Database=postgres;Uid={username};Pwd={password};"

with turbodbc.connect(connection_string=connection_string) as conn:
    with conn.cursor() as cursor:
        cursor.execute(sql_query)
        crypto_trades = dhodbc.read_cursor(cursor)
```

#### `adbc.read_cursor`

```python skip-test
from deephaven.dbc import adbc as dhadbc
from adbc_driver_postgresql import dbapi
from urllib.parse import quote
import os

username = os.environ["POSTGRES_USERNAME"]
password = os.environ["POSTGRES_PASSWORD"]
host = os.environ["POSTGRES_HOST"]
port = os.environ["POSTGRES_PORT"]

sql_query = (
    "SELECT t_ts AS Timestamp, CAST(t_id AS text) AS Id, "
    "CAST(t_instrument AS text) AS Instrument, "
    "t_exchange AS Exchange, t_price AS Price, t_size AS Size "
    "FROM crypto_trades"
)

uri = f"postgresql://{host}:{port}/postgres?user={quote(username)}&password={quote(password)}"

with dbapi.connect(uri) as conn:
    with conn.cursor() as cursor:
        cursor.execute(sql_query)
        crypto_trades = dhadbc.read_cursor(cursor)
```

All three examples read the same rows into `crypto_trades`, although column types can differ between drivers. PostgreSQL converts unquoted identifiers such as `Timestamp` to lowercase, so the column names in the result are lowercase:

![The `crypto_trades` table, with lowercase columns timestamp, id, instrument, exchange, price, and size](../../assets/how-to/crypto-trades.png)

## Query Deephaven tables with SQL

The [`deephaven.experimental.sql`](/core/pydoc/code/deephaven.experimental.sql.html) module runs SQL queries against Deephaven tables instead of an external database. Its [`evaluate`](/core/pydoc/code/deephaven.experimental.sql.html#deephaven.experimental.sql.evaluate) function runs a query that refers to tables by their Python variable names in the calling scope and returns a new Deephaven table. If the source tables are ticking, so is the result.

> [!NOTE]
> SQL support for Deephaven tables is experimental. The [`deephaven.experimental.sql`](/core/pydoc/code/deephaven.experimental.sql.html) API and the set of supported SQL features are subject to change.

```python order=source,other,result
from deephaven import empty_table
from deephaven.experimental import sql

# Create some example tables
source = empty_table(10).update(["X = i", "Y = i * 2"])
other = empty_table(10).update(["X = i + 5", "Y = i * 3"])

# Execute a SQL query against the tables
result = sql.evaluate(
    "SELECT source.X, other.Y FROM source JOIN other ON source.X = other.X"
)
```

### Query a ticking table

Let's run SQL against a ticking table. The example below creates a [time table](../time-table.md) that adds a row every second, then counts its rows by the value of `X`. The result updates each time a new row arrives:

```python ticking-table order=null
from deephaven import time_table
from deephaven.experimental import sql

ticks = time_table("PT1s").update("X = ii % 5")

counts = sql.evaluate("SELECT X, COUNT(*) AS NumTicks FROM ticks GROUP BY X")
```

### Validate a query without executing it

To parse and validate a query without executing it, pass `dry_run=True` to [`evaluate`](/core/pydoc/code/deephaven.experimental.sql.html#deephaven.experimental.sql.evaluate). Instead of a table, `evaluate` then returns a Java [`TableSpec`](/core/javadoc/io/deephaven/qst/table/TableSpec.html) object that describes the query plan. It also writes the plan to the server log in [Graphviz](https://graphviz.org/) format. The example below prints that plan to the console with [`Graphviz.toDot`](/core/javadoc/io/deephaven/qst/table/Graphviz.html):

```python order=:log
from deephaven import empty_table
from deephaven.experimental import sql
import jpy

source = empty_table(10).update(["X = i", "Y = i * 2"])
other = empty_table(10).update(["X = i + 5", "Y = i * 3"])

# Parse the query without executing it
table_spec = sql.evaluate(
    "SELECT source.X, other.Y FROM source JOIN other ON source.X = other.X",
    dry_run=True,
)

# Print the query plan in Graphviz format
Graphviz = jpy.get_type("io.deephaven.qst.table.Graphviz")
print(Graphviz.toDot(table_spec))
```

### Supported SQL features

Table and column names are case-sensitive and must match the Deephaven names exactly, so `SELECT x FROM source` doesn't find a column named `X`. Use double quotes to reference a name that isn't a valid SQL identifier.

Deephaven's SQL implementation supports:

- `SELECT` statements with column selection and aliasing.
- `SELECT DISTINCT` for removing duplicate rows.
- `WHERE` clauses for [filtering](../use-filters.md).
- [Inner joins](../joins-exact-relational.md) with `JOIN` or `INNER JOIN`.
- `GROUP BY` with [aggregate functions](../dedicated-aggregations.md): `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`, `VARIANCE`, `ANY_VALUE`, `COUNT(*)`, `COUNT(DISTINCT col)`, and `APPROX_COUNT_DISTINCT`.
  - `DISTINCT` works only inside `COUNT`.
  - Unlike standard SQL, `COUNT(DISTINCT col)` counts null as a distinct value.
  - `APPROX_COUNT_DISTINCT` returns the same exact count as `COUNT(DISTINCT col)`.
- `ORDER BY` for [sorting](../sort.md).
- `LIMIT` for restricting the number of rows.
- `UNION` and `UNION ALL` for combining the rows of queries with matching columns.
- Arithmetic operators (`+`, `-`, `*`, `/`).
  - Unlike standard SQL, `/` on two integers returns a floating-point result.
- Comparison and logical operators (`=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`, `IS NULL`, `IS NOT NULL`).
  - Unlike standard SQL, `=` and `<>` treat null as an ordinary value, so two nulls are equal and `X <> 1` also matches rows where `X` is null. Use `IS NULL` and `IS NOT NULL` to test for null values.
- `CAST` to numeric types (`TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `REAL`, `FLOAT`, and `DOUBLE`).
- Common math functions such as `ABS`, `SQRT`, `POWER`, `ROUND` (one-argument form only), `FLOOR`, and `CEIL`.

To see how integer division differs from standard SQL, divide an `int` column by `2`. The `Half` column in the result is a `double`, so `3 / 2` is `1.5` rather than `1`:

```python order=result
from deephaven import empty_table
from deephaven.experimental import sql

numbers = empty_table(5).update("X = i")

result = sql.evaluate("SELECT X, X / 2 AS Half FROM numbers")
```

Deephaven doesn't support every SQL feature. For example, it doesn't support `OFFSET`, the remainder operator `%`, `COUNT(col)`, `CAST` to any other type (such as `DECIMAL` or `VARCHAR`), string functions and operators such as `LIKE`, or `ORDER BY` with `ASC NULLS LAST` or `DESC NULLS FIRST`. If a query uses one of these, `evaluate` raises a `DHError`.

> [!WARNING]
> Deephaven runs outer joins (`LEFT`, `RIGHT`, and `FULL`) as [inner joins](../joins-exact-relational.md) and does not raise an error, so the result drops unmatched rows.

## Related documentation

- [`read_sql`](../../reference/data-import-export/SQL/read_sql.md)
- [`adbc.read_cursor`](../../reference/data-import-export/SQL/adbc.md)
- [`odbc.read_cursor`](../../reference/data-import-export/SQL/odbc.md)
- [Install and use Python packages](../install-and-use-python-packages.md)
- [Pydoc for `deephaven.experimental.sql`](/core/pydoc/code/deephaven.experimental.sql.html)
