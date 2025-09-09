---
title: Input Table
---

Input Tables are in-memory Deephaven tables that allow users to manually edit the data in cells. They come in two flavors:

- [`AppendOnlyArrayBackedInputTable`](/core/javadoc/io/deephaven/engine/table/impl/util/AppendOnlyArrayBackedInputTable.html) - An append-only table allows users to add rows to the end of the table and does not support edits or deletions.

- [`KeyedArrayBackedInputTable`](/core/javadoc/io/deephaven/engine/table/impl/util/KeyedArrayBackedInputTable.html) - A keyed table has keys for each row, allowing modification of pre-existing values.

## Syntax

### Append-Only

```groovy syntax
AppendOnlyArrayBackedInputTable.make(definition)
AppendOnlyArrayBackedInputTable.make(definition, enumValues)
AppendOnlyArrayBackedInputTable.make(initialTable)
AppendOnlyArrayBackedInputTable.make(initialTable, enumValues)
```

### Keyed

```groovy syntax
KeyedArrayBackedInputTable.make(definition)
KeyedArrayBackedInputTable.make(definition, enumValues)
KeyedArrayBackedInputTable.make(initialTable)
KeyedArrayBackedInputTable.make(initialTable, enumValues)
```

## Parameters

<ParamTable>
<Param name="definition" type="TableDefinition">

The definition of the new table.

</Param>
<Param name="enumValues" type="Map<String, Object[]>">

A map of column names to enum values.

</Param>
<Param name="initialTable" type="Table">

The initial Table to copy into the append-only or keyed Input Table.

</Param>
</ParamTable>

## Returns

An append-only or keyed Input Table.

## Examples

In this example, we will create an append-only input table from a source table.

```groovy order=source,result
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable

source = newTable(
    doubleCol("Doubles", 3.1, 5.45, -1.0),
    stringCol("Strings", "Creating", "New", "Tables")
)

result = AppendOnlyArrayBackedInputTable.make(source)
```

In this example, we will create a keyed input table from a source table.

```groovy order=source,result
import io.deephaven.engine.table.impl.util.KeyedArrayBackedInputTable

source = newTable(
    doubleCol("Doubles", 3.1, 5.45, -1.0),
    stringCol("Strings", "Creating", "New", "Tables")
)

result = KeyedArrayBackedInputTable.make(source, "Strings")
```

## Add data to the table

To [manually add data](../../../how-to-guides/input-tables.md#add-data-to-the-table) to an input table, simply click on the cell in which you wish to enter data. Type the value into the cell, hit enter, and it will appear.

![A user manually adds data to an input table](../../../assets/how-to/groovy-input-table-ui.gif)

> [!IMPORTANT]
> Added rows are not final until you hit the **Commit** button.

## Related documentation

- [`emptyTable`](./emptyTable.md)
- [Table types](../../../conceptual/table-types.md)
- [AppendOnlyArrayBackedInputTable Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/AppendOnlyArrayBackedInputTable.html)
- [KeyedArrayBackedInputTable Javadoc](/core/javadoc/io/deephaven/engine/table/impl/util/KeyedArrayBackedInputTable.html)
