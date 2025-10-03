---
title: Execute SQL queries in Deephaven
sidebar_label: SQL queries in Deephaven
---

[Structured Query Language (SQL)](https://en.wikipedia.org/wiki/SQL) is the most popular programming language for database management and access. Its popularity can be attributed to a number of factors, including its simplicity, readability, and interoperability. Deephaven supports executing SQL queries against in-memory Deephaven tables using the [`Sql`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/sql/Sql.html) and [`SqlAdapter`](https://docs.deephaven.io/core/javadoc/io/deephaven/sql/SqlAdapter.html) Java classes.

This guide will show you how to execute SQL queries on Deephaven tables in your script session.

## The Sql class

The [`Sql`](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/sql/Sql.html) class provides high-level methods for executing SQL queries against tables in your script session. It offers two main methods:

- [`evaluate`](../../reference/data-import-export/SQL/evaluate.md) - Executes a SQL query and returns a Deephaven table.
- [`dryRun`](../../reference/data-import-export/SQL/dryRun.md) - Parses a SQL query into a [`TableSpec`](https://docs.deephaven.io/core/javadoc/io/deephaven/qst/table/TableSpec.html) without executing it. Useful for validation.

These methods automatically use tables from your script session's [query scope](../queryscope.md) as the catalog for SQL queries.

## Execute a query with `evaluate`

The simplest way to execute SQL against Deephaven tables is with `evaluate()`:

```groovy order=source,other,result
import io.deephaven.engine.sql.Sql

// Create some example tables in the script session
source = emptyTable(10).update("X = i", "Y = i * 2")
other = emptyTable(10).update("X = i + 5", "Y = i * 3")

// Execute SQL query against the tables
result = Sql.evaluate("SELECT source.X, other.Y FROM source JOIN other ON source.X = other.X")
```

The query references tables by their variable names in the script session (`source` and `other`). The result is a new Deephaven table.

### Supported SQL features

Deephaven's SQL implementation supports:

- **SELECT** statements with column selection and aliasing
- **WHERE** clauses for filtering
- **JOIN** operations (INNER, LEFT, RIGHT, FULL)
- **GROUP BY** and aggregate functions (SUM, COUNT, AVG, MIN, MAX)
- **ORDER BY** for sorting
- **LIMIT** and **OFFSET** for pagination
- Standard SQL expressions and operators

## Validate queries with `dryRun`

Use `dryRun` to validate SQL syntax and parse the query without executing it:

```groovy order=source,other,result
import io.deephaven.engine.sql.Sql

source = emptyTable(10).update("X = i", "Y = i * 2")
other = emptyTable(10).update("X = i + 5", "Y = i * 3")

// Parse the query without executing
tableSpec = Sql.dryRun("SELECT source.X, other.Y FROM source JOIN other ON source.X = other.X")

// Execute the same query to get a table
result = Sql.evaluate("SELECT source.X, other.Y FROM source JOIN other ON source.X = other.X")
```

This is useful for:

- Validating SQL syntax before execution
- Validating a query quickly without the overhead of execution
- Inspecting the query plan
- Building tools that work with SQL queries

## Advanced usage with SqlAdapter

For more control over SQL parsing, use [`SqlAdapter.parseSql()`](../../reference/data-import-export/SQL/parseSql.md). This method allows you to specify an explicit `Scope` (catalog) instead of using the script session's query scope.

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

This approach is useful when:

- Building custom SQL tooling
- Integrating SQL parsing into other systems
- You need full control over the catalog/scope

## Choose the right method

Each of the above methods serves different use cases.

- **`evaluate`:** This is the simplest and most direct way to run SQL queries against tables in your script session. Use it if you don't need control over the scope.
- **`dryRun`:** Use this when you want to validate SQL syntax without executing it. It's a lightweight way to check your queries.
- **`parseSql`:** Use this for advanced use cases requiring explicit scope control or custom SQL tooling, or if you want to use tables not in your script session.

## Related documentation

- [`evaluate`](../../reference/data-import-export/SQL/evaluate.md)
- [`dryRun`](../../reference/data-import-export/SQL/dryRun.md)
- [`parseSql`](../../reference/data-import-export/SQL/parseSql.md)
- [Query scope](../queryscope.md)
