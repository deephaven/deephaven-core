File: docs/python/how-to-guides/rollup-table.md (How-to guide), "Supported aggregations" section.

I added this paragraph after the supported-aggregations table:

> `formula` is supported when the formula string names its output and input columns, for example `agg.formula("Total = sum(Value)")`. By default, `rollup` applies the formula to the source rows in each group at every level. To apply the formula to the results of the level below instead, pass `reaggregating=True` to `agg.formula`. Above the lowest level, the formula reads its input from the level below, so the output column must have the same name as the input column. For example, `agg.formula("Value = sum(Value)", reaggregating=True)` sums the source `Value` rows at the lowest level and sums the `Value` results from the level below at each higher level.

The sentence I'm least sure of is the fourth one, about the output column needing the same name as the input column.
