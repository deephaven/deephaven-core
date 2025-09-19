---
title: Execute SQL queries in Deephaven
sidebar_label: SQL queries in Deephaven
---

[Structured Query Language (SQL)](https://en.wikipedia.org/wiki/SQL) is the most popular programming language for database management and access. Its popularity can be attributed to a number of factors, including its simplicity, readability, and interoperability. Open-source options like MySQL and PostgreSQL have also played an important role in its popularity and advancement. SQL can be utilized from Deephaven with the `deephaven.dbc` Python submodule.

This guide will show you how to connect to a SQL database, execute a query on it, and read the result directly into a table.

## deephaven.dbc

Deephaven's Python submodule, `deephaven.dbc`, contains all of Deephaven's functionality for connecting to and using external databases from Deephaven. The submodule contains three pertinent items for using SQL. They are:

- [`read_sql`](../../reference/data-import-export/SQL/read_sql.md)
  - Executes a provided SQL query via a [supported driver](#supported-drivers).
- [`adbc.read_cursor`](../../reference/data-import-export/SQL/adbc.md)
  - Supports ingesting data from external databases via the [Apache Arrow Database Connectivity (ADBC)](https://arrow.apache.org/docs/dev/format/ADBC.html) interfaces.
- [`odbc.read_cursor`](../../reference/data-import-export/SQL/odbc.md)
  - Supports ingesting data from external relational databases via the [Open Database Connectivity (ODBC)](https://learn.microsoft.com/en-us/sql/odbc/reference/what-is-odbc?view=sql-server-ver16) interfaces by using the [turbodbc](https://turbodbc.readthedocs.io/en/latest/) module.

## Supported drivers

`read_sql` can use one of three supported drivers. Each driver can be used by [installing one of the following packages](../install-and-use-python-packages.md).

- [`connectorx`](https://github.com/sfu-db/connector-x)
  - [connectorx](https://pypi.org/project/connectorx/)
- [`ODBC`](https://learn.microsoft.com/en-us/sql/odbc/reference/what-is-odbc?view=sql-server-ver16)
  - [turbodbc](https://pypi.org/project/turbodbc/)
- [`ADBC`](https://arrow.apache.org/adbc/0.3.0/index.html)
  - [adbc-driver-manager](https://pypi.org/project/adbc-driver-manager/)
  - [adbc-driver-flightsql](https://pypi.org/project/adbc-driver-flightsql/) for Arrow Flight SQL database drivers
  - [adbc-driver-postgresql](https://pypi.org/project/adbc-driver-postgresql/) for ADBC PostgreSQL database drivers
  - [adbc-driver-sqlite](https://pypi.org/project/adbc-driver-sqlite/) for ADBC SQLite database drivers

## Execute a query

With the necessary package(s) installed, a SQL query can create a table via any of the three provided Deephaven methods. Each example below pulls from a Postgres DB.

> [!TIP]
> It's best practice not to store credentials in plaintext. In the following examples, the username, password, URL, and port of the Postgres DB are set via environment variables.

### read_sql

```python skip-test
from deephaven.dbc import read_sql
import os

my_query = "SELECT t_ts as Timestamp, CAST(t_id AS text) as Id, " +
    "CAST(t_instrument as text) as Instrument, " +
    "t_exchange as Exchange, t_price as Price, t_size as Size " +
    "FROM CRYPTO TRADES"

username = os.environ["POSTGRES_USERNAME"]
password = os.environ["POSTGRES_PASSWORD"]
url = os.environ["POSTGRES_URL"]
port = os.environ["POSTGRES_PORT"]

sql_uri = f"postgresql://{url}:{port}/postgres?user={username}&password={password}"

crypto_trades = read_sql(conn=sql_uri, query=my_query, driver="connectorx")
```

### odbc.read_cursor

```python skip-test
from deephaven.dbc import odbc as dhodbc
import turbodbc, os

username = os.environ["POSTGRES_USERNAME"]
password = os.environ["POSTGRES_PASSWORD"]
url = os.environ["POSTGRES_URL"]
port = os.environ["POSTGRES_PORT"]

sql_query = "SELECT t_ts as Timestamp, CAST(t_id AS text) as Id, " +
    "CAST(t_instrument as text) as Instrument, " +
    "t_exchange as Exchange, t_price as Price, t_size as Size " +
    "FROM CRYPTO TRADES"

uri = f"postgresql://{url}:{port}/postgres?user={username}&password={password}"

with turbodbc.connect(connection_string=uri) as conn:
    with conn.cursor() as cursor:
        cursor.execute(sql_query)
        crypto_trades = dhodbc.read_cursor(cursor)
```

### adbc.read_cursor

```python skip-test
from deephaven.dbc import adbc as dhadbc
from adbc_driver_postgresql import dbapi
import os

username = os.environ["POSTGRES_USERNAME"]
password = os.environ["POSTGRES_PASSWORD"]
url = os.environ["POSTGRES_URL"]
port = os.environ["POSTGRES_PORT"]

sql_query = "SELECT t_ts as Timestamp, CAST(t_id AS text) as Id, " +
    "CAST(t_instrument as text) as Instrument, " +
    "t_exchange as Exchange, t_price as Price, t_size as Size " +
    "FROM CRYPTO TRADES"

uri = f"postgresql://{url}:{port}/postgres?user={username}&password={password}"

with dbapi.connect(uri) as conn:
    with conn.cursor() as cursor:
        cursor.execute(sql_query)
        crypto_trades = dhadbc.read_cursor(cursor)
```

![The above `crypto_trades` table](../../assets/how-to/crypto-trades.png)

## Choose the right method

With three different methods that can achieve the same results, which is the best choice? Take into account the following factors when choosing.

### High-level vs low-level

[`read_sql`](../../reference/data-import-export/SQL/read_sql.md) is a high-level method for executing a query against a SQL database. It is a general purpose method with less flexibility than the other two.

[`adbc.read_cursor`](../../reference/data-import-export/SQL/adbc.md) and [`odbc.read_cursor`](../../reference/data-import-export/SQL/odbc.md) are lower-level methods because they require the use of a cursor to execute queries. If a use case requires more flexibility and options when executing queries, one of these should be used.

### ADBC vs ODBC

The two lower-level options can both achieve the same results. When choosing one or the other, consider:

- `ODBC` is a well-known and universal specification that can work with a wide variety of SQL database types. If flexibility is a more important factor, this is the best method to choose.
- `ADBC` is faster when working with a SQL database compatible with Apache Arrow (e.g., Arrow Flight SQL), so it should be used where this applies. It's not as compatible with as many database types, so some flexibility is lost.

## Related documentation

- [`read_sql`](../../reference/data-import-export/SQL/read_sql.md)
- [`adbc.read_cursor`](../../reference/data-import-export/SQL/adbc.md)
- [`odbc.read_cursor`](../../reference/data-import-export/SQL/odbc.md)
