---
title: odbc.read_cursor
---

`deephaven.dbc.odbc.read_cursor` converts the results of a provided cursor into a Deephaven table.

## Syntax

```
read_cursor(cursor: turbodbc.cursor.Cursor) -> Table
```

## Parameters

<ParamTable>
<Param name="cursor" type="turbodbc.cursor.Cursor">

A Turbodbc cursor. Prior to it being passed in, its `execute` method must be called to run a query operation that produces a result set.

</Param>
</ParamTable>

## Returns

A new in-memory table.

## Examples

The following example executes a SQL query on a Postgres DB and converts the result into a Deephaven table.

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

## Related documentation

- [How to execute SQL queries](../../../how-to-guides/data-import-export/execute-sql-queries.md)
- [`adbc.read_cursor`](./adbc.md)
- [`read_sql`](./read_sql.md)
- [Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.dbc.odbc.html#deephaven.dbc.odbc.read_cursor)
