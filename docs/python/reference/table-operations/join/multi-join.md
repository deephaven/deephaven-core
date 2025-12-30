---
title: multi_join
---

The `multi_join` method creates a [`MultiJoinTable`](#multijointable) object by performing a multi-table natural join on the source tables. The result consists of a set of distinct keys natural-joined to each source table. Source tables are not required to have a matching row for each key but may not have multiple matching rows for a given key.

## Syntax

```python syntax
multi_join(
  input: Union[Table, Sequence[Table], MultiJoinInput, Sequence[MultiJoinInput]],
  on: Union[str, Sequence[str]] = None,
) -> MultiJoinTable
```

## Parameters

<ParamTable>
<Param name="input" type="Union[Table, Sequence[Table], MultiJoinInput, Sequence[MultiJoinInput]]">

The tables to join, or [`MultiJoinInput`](#multijoininput) objects specifiying tables and columns to join.

</Param>
<Param name="on" type="Union[str, Sequence[str]]" optional>

The column(s) to match. This can be a common name or an equality expression that renames columns to match, e.g., `"col_A = col_B"`.

This parameter must be omitted when [`MultiJoinInput`](#multijoininput) objects are provided.

</Param>
</ParamTable>

## Returns

A [`MultiJoinTable`](#multijointable) object.

## Objects

### `MultiJoinTable`

A [`MultiJoinTable`](/core/pydoc/code/deephaven.table.html#deephaven.table.MultiJoinTable) object contains the result of a `multi_join` operation. The resulting table can be retrieved with the `table` attribute.

```python syntax
MultiJoinTable.table -> Table
```

### `MultiJoinInput`

A `MultiJoinInput` is an object that represents the input tables, key columns, and additional columns to be used in a `multi_join` operation.

```python syntax
MultiJoinInput(
  table: Table,
  on: Union[str, Sequence[str]],
  joins: Union[str, Sequence[str]] = None,
) -> MultiJoinInput
```

#### `MultiJoinInput` Parameters

<ParamTable>
<Param name="table" type="Table">

The table to join onto.

</Param>
<Param name="on" type="Union[str, Sequence[str]]" optional>

The column(s) to match. This can be a name common to both columns or an equality expression that renames columns to match, e.g., `"col_A = col_B"`.

</Param>
<Param name="joins" type="Union[str, Sequence[str]]" optional>

The column(s) to be added from this table to the result table. This can be a common name or an equality expression that renames columns to match, e.g., `"col_A = col_B"`.

</Param>
</ParamTable>

## Examples

This example shows how to use `multi_join` by using tables and column names as inputs.

```python test-set=1 order=t1,t2,t3,result
from deephaven.table import MultiJoinInput, MultiJoinTable, multi_join

from deephaven import new_table
from deephaven.column import string_col, int_col

## Create some tables
t1 = new_table([int_col("C1", [1, 3]), int_col("C2", [1, 1]), int_col("S1", [10, 22])])
t2 = new_table(
    [int_col("C1", [1342, 3323]), int_col("C3", [13, 51]), int_col("S2", [103, 222])]
)
t3 = new_table(
    [int_col("C1", [133, 31]), int_col("C4", [12342, 1969]), int_col("S3", [140, 2562])]
)

# join using tables and column names
multijoin_table_simple = multi_join(
    input=[t1, t2, t3], on="C1"
)  # all columns from t1,t2 included in output

result = multijoin_table_simple.table
```

The following example uses the source tables from the previous example, but joins them using `MultiJoinInput` objects instead of tables and column names. This is slightly more complex than using tables and column names, but gives the user a higher degree of control over `multi_join`'s behavior.

```python test-set=1 order=result
from deephaven.table import MultiJoinInput, MultiJoinTable, multi_join

# join using MultiJoinInput objects
mj_input = [
    MultiJoinInput(table=t1, on="key=C1"),  # all columns added
    MultiJoinInput(table=t2, on="key=C3", joins=["S2"]),  # specific column added
]

multijoin_table_complex = multi_join(input=mj_input)

result = multijoin_table_complex.table
```

## Related documentation

- [Joins: Exact and Relational](../../../how-to-guides/joins-exact-relational.md)
- [`natural_join`](./natural-join.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.multi_join)
