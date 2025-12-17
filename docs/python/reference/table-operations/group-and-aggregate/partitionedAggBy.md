---
title: partitioned_agg_by
---

`partitioned_agg_by` is a convenience method that performs an `agg_by` operation on the source table and wraps the result in a PartitionedTable.

> [!NOTE]
> If the argument `aggs` does not include a partition aggregation created by calling `agg.partition()`, one will be added automatically with the default constituent column name `__CONSTITUENT__`.

## Syntax

```python syntax
partitioned_agg_by(
    aggs: Sequence[Aggregation],
    by: Sequence[str] = None,
    preserve_empty: bool = False,
    initial_groups: Table = None,
) -> PartitionedTable
```

## Parameters

<ParamTable>
<Param name="aggs" type="Union[Aggregation, Sequence[Aggregagation]]">

The aggregation(s) to apply to the source table.

</Param>
<Param name="by" type="Union[str, Sequence[str]]" optional>

The group by column name(s). The default is `None`.

</Param>
<Param name="preserve_empty" type="bool" optional>

Whether to keep result rows for groups that are initially empty, or become empty as a result of updates.
Each aggregation operator defines its own value for empty groups. The default is `False`.

</Param>
<Param name="initial_groups" type="Table" optional>

A table whose distinct combinations of values for the group by column(s) should be used to create an initial set of aggregation groups.
All other columns are ignored.

- In combination with `preserve_empty=True`, this ensures that particular groups appear in the result table.
- With `preserve_empty=False`, use this table to control the encounter order for a collection of groups, and thus their relative order in the result.
- Default is `None`, which will produce a result that is the same as if a table is provided but no rows were supplied.

When a table is provided, the `by` argument must also be provided to explicitly specify the grouping columns.

Note: Changes to this table are not expected or handled; if this table is a refreshing table, only its contents at instantiation time will be used.

</Param>
</ParamTable>

## Returns

A `PartitionedTable`.

## Examples

In this example, `partitioned_agg_by` returns the `source` table, as partitioned by `StreetName`.
The partitioned table is identical to the source table, so the `constituent_tables` method is invoked to demonstrate that the table has been partitioned.

```python order=source
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven import agg as agg

source = new_table(
    [
        string_col(
            "HomeType",
            [
                "Colonial",
                "Contemporary",
                "Contemporary",
                "Condo",
                "Colonial",
                "Apartment",
            ],
        ),
        int_col("HouseNumber", [1, 3, 4, 15, 4, 9]),
        string_col(
            "StreetName",
            [
                "Test Drive",
                "Community Circle",
                "Test Drive",
                "Deephaven Road",
                "Community Circle",
                "Deephaven Road",
            ],
        ),
        int_col("SquareFeet", [2251, 1914, 4266, 1280, 3433, 981]),
        int_col("Price", [450000, 400000, 1250000, 300000, 600000, 275000]),
    ]
)

result = source.partitioned_agg_by(
    aggs=[agg.median(cols=["Size = SquareFeet"])], by=["StreetName"]
)

print(result.constituent_tables)
```

## Related documentation

- [How to create static tables](../../../how-to-guides/new-and-empty-table.md)
- [How to perform dedicated aggregations](../../../how-to-guides/dedicated-aggregations.md)
- [`agg_by`](./aggBy.md)
- [`partition_by`](./partitionBy.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html#partitionedAggBy(java.util.Collection,boolean,io.deephaven.engine.table.Table,java.lang.String...))
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.partitioned_agg_by)
