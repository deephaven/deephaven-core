---
title: Execute SQL queries in Deephaven
---

[Structured Query Language (SQL)](https://en.wikipedia.org/wiki/SQL) is one of the most popular languages for database management and access. Deephaven runs SQL queries against Deephaven tables, including ticking tables, with the experimental [`Sql`](/core/javadoc/io/deephaven/engine/sql/Sql.html) Java class, and parses them into query plans with [`SqlAdapter`](/core/javadoc/io/deephaven/sql/SqlAdapter.html). It can also read the results of SQL queries against an external database into a table with [`JdbcToTableAdapter`](/core/javadoc/io/deephaven/jdbc/JdbcToTableAdapter.html).

This guide shows you how to execute SQL queries on Deephaven tables in your script session and how to read query results from an external database into a table.

> [!NOTE]
> SQL support for Deephaven tables is experimental. The [`Sql`](/core/javadoc/io/deephaven/engine/sql/Sql.html) and [`SqlAdapter`](/core/javadoc/io/deephaven/sql/SqlAdapter.html) APIs and the set of supported SQL features are subject to change.

## Choose the right method

To run a SQL query against tables in your script session, use [`Sql.evaluate`](#execute-a-query-with-evaluate). It is the simplest and most direct option. To check a query without running it, use [`Sql.dryRun`](#validate-queries-with-dryrun). To give the query an explicit list of tables instead of your script session's [query scope](../query-scope.md), use [`SqlAdapter.parseSql`](#advanced-usage-with-sqladapter). This is useful when you build custom SQL tooling or query tables that aren't in your script session.

To query an external database instead, use [`JdbcToTableAdapter.readJdbc`](#read-query-results-from-an-external-database).

## The `Sql` class

The [`Sql`](/core/javadoc/io/deephaven/engine/sql/Sql.html) class provides high-level methods for executing SQL queries against tables in your script session. It offers two main methods:

- [`evaluate`](../../reference/data-import-export/SQL/evaluate.md): Executes a SQL query and returns a Deephaven table.
- [`dryRun`](../../reference/data-import-export/SQL/dryRun.md): Parses a SQL query into a [`TableSpec`](/core/javadoc/io/deephaven/qst/table/TableSpec.html) without executing it, which lets you validate the query.

When a SQL query names a table, these methods look it up among the tables in your script session's [query scope](../query-scope.md). The set of tables a SQL query can reference is called its catalog.

### Execute a query with `evaluate`

This example runs a SQL query against two tables with [`evaluate`](../../reference/data-import-export/SQL/evaluate.md):

```groovy order=source,other,result
import io.deephaven.engine.sql.Sql

// Create some example tables in the script session
source = emptyTable(10).update("X = i", "Y = i * 2")
other = emptyTable(10).update("X = i + 5", "Y = i * 3")

// Execute a SQL query against the tables
result = Sql.evaluate("SELECT source.X, other.Y FROM source JOIN other ON source.X = other.X")
```

The query references tables by their variable names in the script session (`source` and `other`). The result is a new Deephaven table. If the source tables are ticking, so is the result.

### Query a ticking table

Let's run SQL against a ticking table. The example below creates a [time table](../time-table.md) that adds a row every second, then counts its rows by the value of `X`. The result updates each time a new row arrives:

```groovy ticking-table order=null
import io.deephaven.engine.sql.Sql

ticks = timeTable("PT1s").update("X = ii % 5")

counts = Sql.evaluate("SELECT X, COUNT(*) AS NumTicks FROM ticks GROUP BY X")
```

### Validate queries with `dryRun`

Use [`dryRun`](../../reference/data-import-export/SQL/dryRun.md) to validate SQL syntax and parse the query without executing it. It returns a [`TableSpec`](/core/javadoc/io/deephaven/qst/table/TableSpec.html) that describes the query plan. It also writes the plan to the server log in [Graphviz](https://graphviz.org/) format. The example below prints that plan to the console with [`Graphviz.toDot`](/core/javadoc/io/deephaven/qst/table/Graphviz.html):

```groovy order=:log
import io.deephaven.engine.sql.Sql
import io.deephaven.qst.table.Graphviz

source = emptyTable(10).update("X = i", "Y = i * 2")
other = emptyTable(10).update("X = i + 5", "Y = i * 3")

// Parse the query without executing it
tableSpec = Sql.dryRun("SELECT source.X, other.Y FROM source JOIN other ON source.X = other.X")

// Print the query plan in Graphviz format
println Graphviz.toDot(tableSpec)
```

## Supported SQL features

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

```groovy order=result
import io.deephaven.engine.sql.Sql

numbers = emptyTable(5).update("X = i")

result = Sql.evaluate("SELECT X, X / 2 AS Half FROM numbers")
```

Deephaven doesn't support every SQL feature. A query that uses `OFFSET`, `COUNT(col)`, `CAST` to any other type (such as `DECIMAL` or `VARCHAR`), a string function or operator such as `LIKE`, or `ORDER BY` with `ASC NULLS LAST` or `DESC NULLS FIRST` throws an `UnsupportedOperationException`. A query that uses the remainder operator `%` fails to parse.

> [!WARNING]
> Outer joins (`LEFT`, `RIGHT`, and `FULL`) do not throw. Deephaven runs them as [inner joins](../joins-exact-relational.md), so the result drops unmatched rows.

## Advanced usage with `SqlAdapter`

For more control over SQL parsing, use [`SqlAdapter.parseSql`](../../reference/data-import-export/SQL/parseSql.md), which takes an explicit [`Scope`](/core/javadoc/io/deephaven/sql/Scope.html) instead of the script session's [query scope](../query-scope.md). The `Scope` is the catalog of tables the query can reference. Each catalog entry gives:

- The table name.
- The table's column names and types, as a [`TableHeader`](/core/javadoc/io/deephaven/qst/table/TableHeader.html).
- A [`TableSpec`](/core/javadoc/io/deephaven/qst/table/TableSpec.html) that stands in for the table. This example uses a [`TicketTable`](/core/javadoc/io/deephaven/qst/table/TicketTable.html) placeholder.

[`parseSql`](../../reference/data-import-export/SQL/parseSql.md) returns a [`TableSpec`](/core/javadoc/io/deephaven/qst/table/TableSpec.html) that refers to tables only through those placeholders. To get a result, map each placeholder back to its real table and create the table from the spec.

> [!WARNING]
> This example uses `TableCreatorTicketInterceptor`, an internal engine class, to turn the [`TableSpec`](/core/javadoc/io/deephaven/qst/table/TableSpec.html) into a table. It is not part of the public API and may change. Groovy can use it because Groovy ignores Java's package-private visibility. Java code outside the `io.deephaven.engine.sql` package cannot use it.

```groovy order=t1,t2,result
import io.deephaven.engine.table.Table
import io.deephaven.qst.column.header.ColumnHeader
import io.deephaven.qst.table.TableHeader
import io.deephaven.qst.type.Type
import io.deephaven.qst.table.TicketTable
import io.deephaven.sql.ScopeStaticImpl
import io.deephaven.sql.TableInformation
import io.deephaven.sql.SqlAdapter
import io.deephaven.engine.sql.TableCreatorTicketInterceptor
import io.deephaven.engine.table.impl.TableCreatorImpl

// Create tables
t1 = emptyTable(10).update("X = i", "Y = i * 2")
t2 = emptyTable(10).update("X = i + 1", "Y = i * 3")

// Helper to convert TableDefinition to TableHeader
def headerFrom = { Table t ->
    def builder = TableHeader.builder()
    for (cd in t.getDefinition().getColumns()) {
        builder.addHeaders(ColumnHeader.of(cd.getName(), Type.find(cd.getDataType(), cd.getComponentType())))
    }
    builder.build()
}

// Create TicketTable specs for catalog entries
def tt1 = TicketTable.of(("sqlref/t1").getBytes("UTF-8"))
def tt2 = TicketTable.of(("sqlref/t2").getBytes("UTF-8"))

// Build explicit Scope with table metadata
def scope = ScopeStaticImpl.builder()
    .addTables(TableInformation.of(["t1"], headerFrom(t1), tt1))
    .addTables(TableInformation.of(["t2"], headerFrom(t2), tt2))
    .build()

// Parse SQL with explicit scope
def tableSpec = SqlAdapter.parseSql("SELECT t1.X, t2.Y FROM t1 JOIN t2 ON t1.X = t2.X", scope)

// Materialize the TableSpec with TicketTable mapping
def ticketMap = [(tt1): t1, (tt2): t2]
result = tableSpec.logic().create(new TableCreatorTicketInterceptor(TableCreatorImpl.INSTANCE, ticketMap))
```

## Read query results from an external database

To run a query against an external database and read the result into a Deephaven table, use [`JdbcToTableAdapter.readJdbc`](/core/javadoc/io/deephaven/jdbc/JdbcToTableAdapter.html). It converts a Java Database Connectivity (JDBC) [`ResultSet`](https://docs.oracle.com/en/java/javase/17/docs/api/java.sql/java/sql/ResultSet.html) into a static, in-memory table that doesn't update when the database changes. Put the database's JDBC driver on the server's classpath. To learn how, see [Install and use Java packages](../install-and-use-java-packages.md).

This example queries a PostgreSQL database. It reads the username, password, host, and port from environment variables so that it doesn't store credentials in plaintext.

```groovy skip-test
import io.deephaven.jdbc.JdbcToTableAdapter
import java.sql.DriverManager

username = System.getenv("POSTGRES_USERNAME")
password = System.getenv("POSTGRES_PASSWORD")
host = System.getenv("POSTGRES_HOST")
port = System.getenv("POSTGRES_PORT")

sqlQuery = "SELECT t_ts AS Timestamp, CAST(t_id AS text) AS Id, " +
    "CAST(t_instrument AS text) AS Instrument, " +
    "t_exchange AS Exchange, t_price AS Price, t_size AS Size " +
    "FROM crypto_trades"

jdbcUrl = "jdbc:postgresql://${host}:${port}/postgres"

DriverManager.getConnection(jdbcUrl, username, password).withCloseable { conn ->
    conn.createStatement().withCloseable { statement ->
        cryptoTrades = JdbcToTableAdapter.readJdbc(statement.executeQuery(sqlQuery))
    }
}
```

PostgreSQL converts unquoted identifiers such as `Timestamp` to lowercase, so the column names in `cryptoTrades` are lowercase (`timestamp`, `id`, `instrument`, and so on).

## Related documentation

- [`evaluate`](../../reference/data-import-export/SQL/evaluate.md)
- [`dryRun`](../../reference/data-import-export/SQL/dryRun.md)
- [`parseSql`](../../reference/data-import-export/SQL/parseSql.md)
- [Query scope](../query-scope.md)
- [Install and use Java packages](../install-and-use-java-packages.md)
- [`JdbcToTableAdapter` Javadoc](/core/javadoc/io/deephaven/jdbc/JdbcToTableAdapter.html)
