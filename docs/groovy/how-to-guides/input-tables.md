---
title: Create and use input tables
sidebar_label: Input tables
---

Input tables allow users to enter new data into tables in two ways: programmatically, and manually through the UI.

In the first case, data is added to a table with `add`, an input table-specific method similar to [`merge`](../reference/table-operations/merge/merge.md). In the second case, data is added to a table through the UI by clicking on cells and typing in the contents, similar to a spreadsheet program like [MS Excel](https://www.microsoft.com/en-us/microsoft-365/excel).

Input tables come in two flavors:

- [append-only](../conceptual/table-types.md#specialization-1-append-only)
  - An append-only input table puts any entered data at the bottom.
- [keyed](#create-a-keyed-input-table)
  - A keyed input table supports modification/deletion of contents, and allows access to rows by key.

We'll show you how to create and use both types in this guide.

## Create an input table

First, you need to import the `AppendOnlyArrayBackedInputTable` class:

```groovy
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable
```

An input table can be constructed from a pre-existing table _or_ a list of column definitions. In either case, one or more key columns can be specified, which turns the table from an append-only input table to a keyed input table.

### From a pre-existing table

Here, we will create an input table from a table that already exists in memory. In this case, we'll create one with [`emptyTable`](/core/docs/reference/table-operations/create/emptyTable/).

```groovy test-set-1 order=source,result
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable

source = emptyTable(10).update("X = i")

result = AppendOnlyArrayBackedInputTable.make(source)
```

### From scratch

Here, we will create an input table from a list of column definitions. Column definitions must be defined in a [TableDefinition](/core/javadoc/io/deephaven/engine/table/TableDefinition.html).

```groovy test-set=1 order=null
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.table.ColumnDefinition

definition = TableDefinition.of(ColumnDefinition.ofInt("X"))

result = AppendOnlyArrayBackedInputTable.make(definition)
```

The resulting table is initially empty, and ready to receive data.

### Create a keyed input table

To create a keyed input table, import [`KeyedArrayBackedInputTable`](/core/javadoc/io/deephaven/engine/table/impl/util/KeyedArrayBackedInputTable.html) and call [`make`](/core/javadoc/io/deephaven/engine/table/impl/util/KeyedArrayBackedInputTable.html), using a source table and at least one key column as arguments.

Let's first specify one key column.

```groovy test-set=1 order=source,result
import io.deephaven.engine.table.impl.util.KeyedArrayBackedInputTable

source = newTable(
    doubleCol("Doubles", 3.1, 5.45, -1.0),
    stringCol("Strings", "Creating", "New", "Tables")
)

result = KeyedArrayBackedInputTable.make(source, "Strings")
```

In the case of multiple key columns, specify them in a list.

```groovy test-set=1 order=null
result = KeyedArrayBackedInputTable.make(source, "Strings", "Doubles")
```

When creating a keyed input table from a pre-existing table, the key column(s) must satisfy uniqueness criteria. Each row or combination of rows in the initial table must not have repeating values. Take, for instance, the following table:

```groovy test-set=2 order=source
source = emptyTable(10).update(
        "Sym = (i % 2 == 0) ? `A` : `B`",
        "Marker = (i % 3 == 2) ? `J` : `K`",
        "X = i",
        "Y = sin(0.1 * X)",
)
```

A keyed input table _can_ be created from the `X` and `Y` columns, since they have no repeating values, and are thus unique:

```groovy test-set=2 order=inputSource
inputSource = KeyedArrayBackedInputTable.make(source, "X", "Y")
```

A keyed input table _cannot_ be created from the `Sym` _or_ `Marker` columns, since they have repeating values and combinations, and are thus _not_ unique:

```groovy test-set=2 should-fail
inputSource = KeyedArrayBackedInputTable.make(source, "Sym", "Marker")
```

## Add data to the table

### Programmatically

To add data to an input table programmatically, you will need to create a `InputTableUpdater` object using your input table's `INPUT_TABLE_ATTRIBUTE`. This object can be used to add or remove data from the associated table.

> [!NOTE]
> To programmatically add data to an input table, the table schemas (column definitions) must match. These column definitions comprise the names and data types of every column in the table.

```groovy order=source,result
// import the needed InputTableUpdater classes
import io.deephaven.engine.table.impl.util.KeyedArrayBackedInputTable
import io.deephaven.engine.util.input.InputTableUpdater

// create tables
source = newTable(
    doubleCol("Doubles", 1.0, 2.0, -3.0),
    stringCol("Strings", "Aaa", "Bbb", "Ccc")
)

table2 = newTable(
    doubleCol("Doubles", 6.9343, 1.45, -4.0),
    stringCol("Strings", "Ggg", "Hhh", "Iii")
)

// create a keyed input table
result = KeyedArrayBackedInputTable.make(source, "Strings")

// create a InputTableUpdater object with the result table's input table attribute
mit = (InputTableUpdater)result.getAttribute(Table.INPUT_TABLE_ATTRIBUTE)

// add the second table to the input table
mit.add(table2)
```

### Manually

To manually add data to an input table, simply click on the cell in which you wish to enter data. Type the value into the cell, hit enter, and it will appear.

![A user manually adds values to an input table](../assets/how-to/input-table-keyed-edit-existing.gif)

Note that a `KeyedArrayBackedInputTable` will allow you to edit existing rows, while an `AppendOnlyArrayBackedInputTable` will only allow you to add new rows.

> [!IMPORTANT]
> Added rows aren't final until you hit the **Commit** button. If you edit an existing row in a keyed input table, the result is immediate.

Here are some things to consider when manually entering data into an input table:

- Manually entered data in a table will not be final until the **Commit** button at the bottom right of the console is clicked.
- Data added manually to a table must be of the correct type for its column. For instance, attempting to add a string value to an int column will fail.
- Entering data in between populated cells and hitting **Enter** will add the data to the bottom of the column.

## Clickable links

Any string column in Deephaven can contain a clickable link — the string just has to be formatted correctly.

![An input table contains both valid and invalid links, with valid links underlined and highlighted in blue](../assets/how-to/ui/invalid_links.png)

Let's create an input table that we can add links to manually:

```groovy order=result
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.table.ColumnDefinition

definition = TableDefinition.of(ColumnDefinition.ofString("Title"), ColumnDefinition.ofString("Link"))

result = AppendOnlyArrayBackedInputTable.make(definition)
```

![Manually adding a clickable link to an input table](../assets/how-to/groovy-input-table-link.gif)

## Related documentation

- [InputTableUpdater](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [Table types](../conceptual/table-types.md)
- [Input Table](../reference/table-operations/create/InputTable.md)
