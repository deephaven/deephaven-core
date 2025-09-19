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
import turbodbc

import os

driver = "PostgreSQL"
server = "postgres"
db_name = os.environ["POSTGRES_DB_NAME"]
username = os.environ["POSTGRES_USERNAME"]
password = os.environ["POSTGRES_PASSWORD"]
port = os.environ["POSTGRES_PORT"]

sql_query = "SELECT t_ts as Timestamp, CAST(t_id AS text) as Id, " +
    "CAST(t_instrument as text) as Instrument, " +
    "t_exchange as Exchange, t_price as Price, t_size as Size " +
    "FROM CRYPTO TRADES"

connection = f"Driver={{driver}};Server={server};Port={port};Database={db_name};Uid={username};Pwd={password}

with turbodbc.connect(connection_string=connection) as conn:
    with conn.cursor() as cursor:
        cursor.execute(sql_query)
        result = dhodbc.read_cursor(cursor)
```

## Related documentation

- [How to execute SQL queries](../../../how-to-guides/data-import-export/execute-sql-queries.md)
- [`adbc.read_cursor`](./adbc.md)
- [`read_sql`](./read_sql.md)
- [Pydoc](/core/pydoc/code/deephaven.dbc.odbc.html#deephaven.dbc.odbc.read_cursor)
