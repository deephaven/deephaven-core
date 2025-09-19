---
title: read_sql
---

`read_sql` is a method used to execute a SQL query on a database and read the result directly into a table.

## Syntax

```
read_sql(conn: Any, query: str, driver: str = "connectorx") -> Table
```

## Parameters

<ParamTable>
<Param name="conn" type="any">

The database connection. Can be either a connection string for the given driver or a Connection object.

</Param>
<Param name="query" type="str">

The SQL query to execute on the database.

</Param>
<Param name="driver" type="str" optional>

The driver to use. Supported drivers are `odbc`, `adbc`, and `connectorx`. `connectorx` is the default. This argument will be ignored if `conn` is a Connection object.

</Param>
</ParamTable>

## Returns

A new table.

## Examples

The following example uses `read_sql` to execute a SQL query on a Postgres DB via the `connectorx` driver. The result of the query is a new in-memory table.

```python skip-test
from deephaven.dbc import read_sql
import os

my_query = "SELECT t_ts as Timestamp, CAST(t_id AS text) as Id, " +
    "CAST(t_instrument as text) as Instrument, " +
    "t_exchange as Exchange, t_price as Price, t_size as Size " +
    "FROM CRYPTO TRADES"

my_username = os.environ["POSTGRES_USERNAME"]
my_password = os.environ["POSTGRES_PASSWORD"]

sql_uri = "postgresql://postgres.postgres.svc.cluster.local.:5432/postgres?" +
  f"user={my_username}&password={my_password}"

crypto_trades = read_sql(conn=sql_uri, query=my_query, driver="connectorx")
```

![The above `crypto_trades` table](../../../assets/how-to/crypto-trades.png)

## Related documentation

- [How to exectute SQL queries](../../../how-to-guides/data-import-export/execute-sql-queries.md)
- [`adbc.read_cursor`](./adbc.md)
- [`odbc.read_cursor`](./odbc.md)
- [Pydoc](/core/pydoc/code/deephaven.dbc.html#deephaven.dbc.read_sql)
