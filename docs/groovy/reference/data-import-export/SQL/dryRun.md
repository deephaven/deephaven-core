---
title: dryRun
---

The `dryRun` method will create a TableSpec for an SQL query without actually running it. This is useful for validating SQL queries before running them or for creating a TableSpec that can be used later to create a Deephaven table.

## Syntax

```
dryRun(sql)
```

## Parameters

<ParamTable>
<Param name="sql" type="String">

The SQL query to create a TableSpec for.

</Param>
</ParamTable>

## Returns

A TableSpec object that can be used to create a Deephaven table from the original SQL query.

## Examples-

```groovy order=table
import io.deephaven.engine.sql.Sql

t1 = emptyTable(10).update("X = i", "Y = i * 2")
t2 = emptyTable(10).update("X = i + 1", "Y = i * 3")


tableSpec = Sql.dryRun("SELECT t1.X, t2.Y FROM t1 JOIN t2 ON t1.X = t2.X")

println tableSpec.getClass()

// To materialize the SQL into a Deephaven Table, use evaluate with the same SQL
table = Sql.evaluate("SELECT t1.X, t2.Y FROM t1 JOIN t2 ON t1.X = t2.X")
println table.getClass()
```

## Related documentation

- [`dryRun`](./dryRun.md)
- [`evaluate`](./evaluate.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/sql/Sql.html#dryRun(java.lang.String))
