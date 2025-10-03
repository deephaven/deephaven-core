---
title: evaluate
---

The `evaluate` method will evaluate a SQL string and return the equivalent Deephaven table.

## Syntax

```
evaluate(sql)
```

## Parameters

<ParamTable>
<Param name="sql" type="String">

The SQL query, in string format, to evaluate.

</Param>
</ParamTable>

## Returns

A new in-memory table resulting from executing the SQL query on Deephavden data.

## Examples

```groovy order=t1,t2,result
import io.deephaven.engine.sql.Sql
import io.deephaven.engine.table.impl.TableCreatorImpl

t1 = emptyTable(10).update("X = i")
t2 = emptyTable(10).update("X = i + 5", "Y = i * 2")

result = Sql.evaluate("SELECT t1.X, t2.Y FROM t1 JOIN t2 ON t1.X = t2.X")
```

## Related documentation

- [Execute SQL queries in Deephaven](../../../how-to-guides/data-import-export/execute-sql-queries.md)
- [`dryRun`](./dryRun.md)
- [`parseSql`](./parseSql.md)
- [Javadoc](https://docs.deephaven.io/core/javadoc/io/deephaven/engine/sql/Sql.html#evaluate(java.lang.String))
