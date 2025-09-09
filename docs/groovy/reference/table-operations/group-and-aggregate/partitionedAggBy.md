---
title: partitionedAggBy
---

`partitionedAggBy` is a convenience method that performs an `aggBy` operation on the source table and wraps the result in a PartitionedTable.

> [!NOTE]
> If the argument `aggregations` does not include a partition, one will be added automatically with the default constituent column name `__CONSTITUENT__`.

## Syntax

```groovy syntax
partitionedAggBy(aggregations, preserveEmpty, initialGroups, keyColumnNames...)
```

## Parameters

<ParamTable>
<Param name="aggregations" type="Collection<? extends Aggregation>">

The aggregation(s) to apply to the source table.

</Param>
<Param name="preserveEmpty" type="boolean">

Whether to keep result rows for groups that are initially empty or become empty as a result of updates. Each aggregation operator defines its own value for empty groups.

</Param>
<Param name="initialGroups" type="Table">

A table whose distinct combinations of values for the `groupByColumns` should be used to create an initial set of aggregation groups. All other columns are ignored.
This is useful in combination with `preserveEmpty == true` to ensure that particular groups appear in the result table, or with `preserveEmpty == false` to control the encounter order for a collection of groups and thus their relative order in the result.
Changes to `initialGroups` are not expected or handled; if `initialGroups` is a refreshing table, only its contents at instantiation time will be used. If `initialGroups == null`, the result will be the same as if a table with no rows was supplied.

</Param>
<Param name="keyColumnNames" type="String...">

The names of the key columns to aggregate by.

</Param>
</ParamTable>

## Returns

A `PartitionedTable`.

## Examples

In this example, `partitionedAggBy` returns the `source` table, as partitioned by `StreetName`.

```groovy order=source
import static io.deephaven.api.agg.Aggregation.AggMed

source = newTable(
    stringCol(
        "HomeType",
        "Colonial",
        "Contemporary",
        "Contemporary",
        "Condo",
        "Colonial",
        "Apartment",
    ),
    intCol("HouseNumber", 1, 3, 4, 15, 4, 9),
    stringCol(
        "StreetName",
        "Test Drive",
        "Community Circle",
        "Test Drive",
        "Deephaven Road",
        "Community Circle",
        "Deephaven Road",
    ),
    intCol("SquareFeet", 2251, 1914, 4266, 1280, 3433, 981),
    intCol("Price", 450000, 400000, 1250000, 300000, 600000, 275000),
)

result = source.partitionedAggBy([AggMed("Size = SquareFeet")], false, source, "StreetName")
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [`aggBy`](./aggBy.md)
- [`partitionBy`](./partitionBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#partitionedAggBy(java.util.Collection,boolean,io.deephaven.engine.table.Table,java.lang.String...))
