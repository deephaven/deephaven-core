---
title: adbc.read_cursor
---

`deephaven.dbc.adbc.read_cursor` converts the results of a provided cursor into a Deephaven table.

## Syntax

```
read_cursor(cursor: adbc_driver_manager.dbapi.Cursor) -> Table
```

## Parameters

<ParamTable>
<Param name="cursor" type="adbc_driver_manager.dbapi.Cursor">

An ADBC DB-API cursor. Prior to it being passed in, its `execute` method must be called to run a query operation that produces an Arrow table.

</Param>
</ParamTable>

## Returns

A new in-memory table.

## Examples

The following example executes a SQL query on a Postgres DB and converts the result into a Deephaven table.

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

uri = f"{url}:{port}/postgres?user={username}&password={password}"

with adbc_driver_postgresql.dbapi.connect(uri) as conn:
    with conn.cursor() as cursor:
        cursor.execute(sql_query)
        result = dhadbc.read_cursor(cursor)
```

## Related documentation

- [How to execute SQL queries](../../../how-to-guides/data-import-export/execute-sql-queries.md)
- [`odbc.read_cursor`](./odbc.md)
- [`read_sql`](./read_sql.md)
- [Pydoc](/core/pydoc/code/deephaven.dbc.adbc.html#deephaven.dbc.adbc.read_cursor)
