---
title: parseSql
---

The `SqlAdapter.parseSql` method parses a SQL query string into a [`TableSpec`](https://docs.deephaven.io/core/javadoc/io/deephaven/qst/table/TableSpec.html) using an explicit Scope (catalog). This is a low-level API that provides full control over the catalog/scope, unlike the higher-level [`evaluate`](./evaluate.md) and [`dryRun`](./dryRun.md) methods, which automatically use the script session's query scope.

## Syntax

```
SqlAdapter.parseSql(sql, scope)
```

## Parameters

<ParamTable>
<Param name="sql" type="String">

The SQL query string to parse.

</Param>
<Param name="scope" type="Scope">

The Scope object that defines the catalog of available tables for the SQL query. This must be constructed explicitly using `ScopeStaticImpl.builder()` and populated with `TableInformation` objects.

</Param>
</ParamTable>

## Returns

A `TableSpec` object representing the parsed SQL query. It refers to the catalog's tables through the `TableSpec` placeholders in the `Scope`. Deephaven has no public API to turn this `TableSpec` into a table; to run a SQL query, use [`evaluate`](./evaluate.md).

## Examples

```groovy order=:log
import io.deephaven.engine.table.Table
import io.deephaven.qst.column.header.ColumnHeader
import io.deephaven.qst.table.TableHeader
import io.deephaven.qst.type.Type
import io.deephaven.qst.table.TicketTable
import io.deephaven.sql.ScopeStaticImpl
import io.deephaven.sql.TableInformation
import io.deephaven.sql.SqlAdapter
import io.deephaven.qst.table.Graphviz

// Create tables
def t1 = emptyTable(10).update("X = i", "Y = i * 2")
def t2 = emptyTable(10).update("X = i + 1", "Y = i * 3")

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

// Print the parsed query plan in Graphviz DOT format
println Graphviz.toDot(tableSpec)
```

The output is the parsed query plan in [Graphviz DOT](https://graphviz.org/doc/info/lang.html) format. Each `ticketTable` node is a placeholder from the `Scope`, and the `view` and `join` nodes are the operations that the query applies to those tables.

## Related documentation

- [Execute SQL queries in Deephaven](../../../how-to-guides/data-import-export/execute-sql-queries.md)
- [`dryRun`](./dryRun.md)
- [`evaluate`](./evaluate.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/sql/SqlAdapter.html#parseSql(java.lang.String,io.deephaven.sql.Scope))
