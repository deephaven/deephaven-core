---
title: Create and use input tables
---

> [!TIP]
> This guide covers input tables created and used directly on the Deephaven server. To stream data from an external Java application, see [Java client input tables](./java-client-input-tables.md).

Input tables allow users to enter new data into tables in two ways: programmatically and manually through the UI.

In the first case, you add data with the [`add`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html#add(io.deephaven.engine.table.Table)) method of the table's [`InputTableUpdater`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html), which works similarly to [`merge`](../reference/table-operations/merge/merge.md). In the second case, you click cells in the UI and type their contents, as in a spreadsheet program like [Microsoft Excel](https://www.microsoft.com/en-us/microsoft-365/excel).

Input tables come in two flavors:

- [append-only](#create-an-input-table)
  - An append-only input table puts any entered data at the bottom. It is an [append-only table](../conceptual/table-types.md#specialization-1-append-only).
- [keyed](#create-a-keyed-input-table)
  - A keyed input table has one or more key columns whose values identify each row. You can replace or delete existing rows by key.

This guide shows how to create and use both types.

## Create an input table

Append-only input tables use the [`AppendOnlyArrayBackedInputTable`](/core/javadoc/io/deephaven/engine/table/impl/util/AppendOnlyArrayBackedInputTable.html) class, and keyed input tables use the [`KeyedArrayBackedInputTable`](/core/javadoc/io/deephaven/engine/table/impl/util/KeyedArrayBackedInputTable.html) class. To create an append-only input table, first import `AppendOnlyArrayBackedInputTable`:

```groovy
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable
```

You can create an input table from a pre-existing table _or_ from a set of column definitions. Either source also works for a keyed input table. A key column holds values that identify each row, so no two rows share the same key value. With several key columns, no two rows share the same combination of values. The next two sections create append-only input tables, and [Create a keyed input table](#create-a-keyed-input-table) shows how to add key columns.

### From a pre-existing table

Here, we create an input table from a table that already exists in memory. The example creates that source table with [`emptyTable`](../reference/table-operations/create/emptyTable.md).

```groovy test-set=1 order=source,result
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable

source = emptyTable(10).update("X = i")

result = AppendOnlyArrayBackedInputTable.make(source)
```

### From column definitions

Here, we create an input table from a set of column definitions. Pass the columns as a [`TableDefinition`](/core/javadoc/io/deephaven/engine/table/TableDefinition.html) built from [`ColumnDefinition`](/core/javadoc/io/deephaven/engine/table/ColumnDefinition.html) objects.

```groovy test-set=1 order=result
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.table.ColumnDefinition

definition = TableDefinition.of(ColumnDefinition.ofInt("X"))

result = AppendOnlyArrayBackedInputTable.make(definition)
```

The resulting table is initially empty and ready to receive data.

### Create a keyed input table

To create a keyed input table, import [`KeyedArrayBackedInputTable`](/core/javadoc/io/deephaven/engine/table/impl/util/KeyedArrayBackedInputTable.html) and call [`make`](/core/javadoc/io/deephaven/engine/table/impl/util/KeyedArrayBackedInputTable.html#make(io.deephaven.engine.table.Table,java.lang.String...)) with a source table or a [`TableDefinition`](/core/javadoc/io/deephaven/engine/table/TableDefinition.html), followed by one or more key column names.

Let's first specify one key column.

```groovy test-set=1 order=source,result
import io.deephaven.engine.table.impl.util.KeyedArrayBackedInputTable

source = newTable(
    doubleCol("Doubles", 3.1, 5.45, -1.0),
    stringCol("Strings", "Creating", "New", "Tables")
)

result = KeyedArrayBackedInputTable.make(source, "Strings")
```

In the case of multiple key columns, pass each column name as a separate argument.

```groovy test-set=1 order=null
result = KeyedArrayBackedInputTable.make(source, "Strings", "Doubles")
```

When you create a keyed input table from a pre-existing table, the input table keeps one row per key. If several rows of the initial table share a key, the input table keeps the values from the last of those rows. With multiple key columns, a key is a combination of values. Take, for instance, the following table:

```groovy test-set=2 order=source
import io.deephaven.engine.table.impl.util.KeyedArrayBackedInputTable

source = emptyTable(10).update(
    "Sym = (i % 2 == 0) ? `A` : `B`",
    "Marker = (i % 3 == 2) ? `J` : `K`",
    "X = i",
    "Y = sin(0.1 * X)"
)
```

No two rows share the same combination of `X` and `Y` values, so a keyed input table with `X` and `Y` as key columns keeps all 10 rows:

```groovy test-set=2 order=inputSource
inputSource = KeyedArrayBackedInputTable.make(source, "X", "Y")
```

`Sym` and `Marker` together take only four distinct combinations of values, so a keyed input table with `Sym` and `Marker` as key columns has four rows. Each row holds the values from the last source row with that combination:

```groovy test-set=2 order=inputSource
inputSource = KeyedArrayBackedInputTable.make(source, "Sym", "Marker")
```

## Add data to the table

### Programmatically

To add data to an input table programmatically, get its [`InputTableUpdater`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html) with [`InputTableUpdater.from(table)`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html#from(io.deephaven.engine.table.Table)). This object adds data to and removes data from the input table.

An append-only input table adds new rows to the end of the table. In a keyed input table, an added row whose key already exists replaces the existing row with that key, and the other added rows become new rows.

> [!NOTE]
> To add data to an input table programmatically, the table you add must have the same column names and data types as the input table.

```groovy test-set=3 order=source,result
// import the needed classes
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

// get the InputTableUpdater for the result table
updater = InputTableUpdater.from(result)

// add the second table to the input table
updater.add(table2)
```

[`add`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html#add(io.deephaven.engine.table.Table)) blocks until Deephaven finishes adding the data. To add data without blocking, use [`addAsync`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html#addAsync(io.deephaven.engine.table.Table,io.deephaven.engine.util.input.InputTableStatusListener)).

[`addAsync`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html#addAsync(io.deephaven.engine.table.Table,io.deephaven.engine.util.input.InputTableStatusListener)) takes an [`InputTableStatusListener`](/core/javadoc/io/deephaven/engine/util/input/InputTableStatusListener.html), which Deephaven notifies when the queued addition succeeds or fails. [`InputTableStatusListener.DEFAULT`](/core/javadoc/io/deephaven/engine/util/input/InputTableStatusListener.html#DEFAULT) does nothing on success and logs the failure to the server log. Problems that Deephaven detects before it queues the addition, such as a table whose column names or types don't match the input table, throw an exception from `addAsync` immediately instead of reaching the listener.

Deephaven processes asynchronous calls from the same thread in the order you make them, but it doesn't guarantee an order across threads. The following code block asynchronously adds a row with a new key, `Jjj`, and replaces the row with the existing key `Aaa`:

```groovy test-set=3 order=null
import io.deephaven.engine.util.input.InputTableStatusListener

table3 = newTable(
    doubleCol("Doubles", 7.5, 0.25),
    stringCol("Strings", "Jjj", "Aaa")
)

updater.addAsync(table3, InputTableStatusListener.DEFAULT)
```

### Manually

To manually add data to an input table, click the cell in which you wish to enter data, type the value, and press **Enter**.

A [`KeyedArrayBackedInputTable`](/core/javadoc/io/deephaven/engine/table/impl/util/KeyedArrayBackedInputTable.html) allows you to edit existing rows, while an [`AppendOnlyArrayBackedInputTable`](/core/javadoc/io/deephaven/engine/table/impl/util/AppendOnlyArrayBackedInputTable.html) only allows you to add new rows. In a keyed input table, adding a row whose key already exists replaces the existing row with that key.

![A user edits an existing row in a keyed input table](../assets/how-to/input-table-keyed-edit-existing.gif)

> [!IMPORTANT]
> Added rows aren't final until you click the **Commit** button. If you edit an existing row in a keyed input table, the result is immediate.

Here are some things to consider when manually entering data into an input table:

- Data added manually to a table must be of the correct type for its column. For instance, attempting to add a string value to an `int` column fails.
- Entering data in between populated cells and pressing **Enter** adds the data to the bottom of the column.

## Delete data from a table

You can delete data only from a keyed input table. Calling [`delete`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html#delete(io.deephaven.engine.table.Table)) or [`deleteAsync`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html#deleteAsync(io.deephaven.engine.table.Table,io.deephaven.engine.util.input.InputTableStatusListener)) on an append-only input table throws an `UnsupportedOperationException`. To delete data from a keyed input table, use one of the following [`InputTableUpdater`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html) methods:

- [`delete`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html#delete(io.deephaven.engine.table.Table)): Synchronous deletion.
- [`deleteAsync`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html#deleteAsync(io.deephaven.engine.table.Table,io.deephaven.engine.util.input.InputTableStatusListener)): Asynchronous deletion.

To delete table data, supply a table that contains only the key columns, with the key values of the rows you wish to delete. For instance, the example in [Programmatically](#programmatically) creates a keyed input table whose key column is `Strings`, and gets its `updater`. The following code deletes the row with the key value `Bbb`:

```groovy test-set=3 order=null
updater.delete(newTable(stringCol("Strings", "Bbb")))
```

To delete data asynchronously, use [`deleteAsync`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html#deleteAsync(io.deephaven.engine.table.Table,io.deephaven.engine.util.input.InputTableStatusListener)), which takes an [`InputTableStatusListener`](/core/javadoc/io/deephaven/engine/util/input/InputTableStatusListener.html) and follows the same ordering rules as [`addAsync`](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html#addAsync(io.deephaven.engine.table.Table,io.deephaven.engine.util.input.InputTableStatusListener)). The following code block asynchronously deletes the row with the key value `Ccc`:

```groovy test-set=3 order=null
updater.deleteAsync(newTable(stringCol("Strings", "Ccc")), InputTableStatusListener.DEFAULT)
```

## Enter clickable links in an input table

Input tables are a convenient way to try out clickable links, because you can type links directly into their cells. Any string column in Deephaven can contain a clickable link if the string is formatted correctly. See [Add clickable links](./user-interface/add-clickable-links.md) for examples of strings that are and aren't displayed as links.

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

<!-- TODO DH-22694: Uncomment and update this section when the test-only validators are made production ready.

## Input table validators

Input table validators allow you to add validation rules to input tables, ensuring that data entered (either programmatically or manually through the UI) meets specific criteria. Validators wrap an existing input table and check data before it's added, throwing validation exceptions if the data doesn't meet the requirements.

Deephaven provides several built-in validators:

- **`RangeValidatingInputTable`** - Validates that integer values fall within a specified range (min/max inclusive).
- **`DoubleRangeValidatingInputTable`** - Validates that double values fall within a specified range (min/max inclusive).
- **`NotNullValidatingInputTable`** - Validates that values in a column are not null.
- **`NonEmptyValidatingInputTable`** - Validates that string values are not empty.
- **`StringListValidatingInputTable`** - Validates that string values belong to a predefined set of allowed values.

### Creating validated input tables

To create a validated input table, first create a base input table, then wrap it with one or more validators. Here's an example showing all available validators:

```groovy order=intRangeValidator,doubleRangeValidator,notNullValidator,notNullValidatorInt,nonEmptyValidator,stringListValidator
import io.deephaven.engine.table.impl.util.KeyedArrayBackedInputTable
import io.deephaven.server.table.inputtables.RangeValidatingInputTable
import io.deephaven.server.table.inputtables.DoubleRangeValidatingInputTable
import io.deephaven.server.table.inputtables.NotNullValidatingInputTable
import io.deephaven.server.table.inputtables.NonEmptyValidatingInputTable
import io.deephaven.server.table.inputtables.StringListValidatingInputTable

// Create source table with various column types
_source = newTable(
    stringCol("Key", "Apple", "Banana", "Carrot", "Date", "Eggplant"),
    intCol("IntValue", 1, 2, 3, 50, 75),
    doubleCol("DoubleValue", 1.5, 2.5, 3.5, 50.5, 75.5),
    stringCol("Category", "Fruit", "Fruit", "Vegetable", "Fruit", "Vegetable"),
    stringCol("Description", "Red", "Yellow", "Orange", "Sweet", "Purple")
)

// Example 1: Integer Range Validator (0-100)
intRangeValidator = RangeValidatingInputTable.make(
    KeyedArrayBackedInputTable.make(_source, "Key"),
    "IntValue",
    0,
    100
)

// Example 2: Double Range Validator (0.0-100.0)
doubleRangeValidator = DoubleRangeValidatingInputTable.make(
    KeyedArrayBackedInputTable.make(_source, "Key"),
    "DoubleValue",
    0.0,
    100.0
)

// Example 3: Not Null Validator on Category column
notNullValidator = NotNullValidatingInputTable.make(
    KeyedArrayBackedInputTable.make(_source, "Key"),
    "Category"
)

// Example 3.1: Not Null Validator on IntValue column
notNullValidatorInt = NotNullValidatingInputTable.make(
    KeyedArrayBackedInputTable.make(_source, "Key"),
    "IntValue"
)

// Example 4: Non-Empty Validator on Description column
nonEmptyValidator = NonEmptyValidatingInputTable.make(
    KeyedArrayBackedInputTable.make(_source, "Key"),
    "Description"
)

// Example 5: String List Validator - Category must be "Fruit", "Vegetable", or "Grain"
stringListValidator = StringListValidatingInputTable.make(
    KeyedArrayBackedInputTable.make(_source, "Key"),
    "Category",
    "Fruit", "Vegetable", "Grain"
)
```

-->

## Related documentation

- [Input table reference](../reference/table-operations/create/InputTable.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [Table types](../conceptual/table-types.md)
- [Add clickable links](./user-interface/add-clickable-links.md)
- [`InputTableUpdater` Javadoc](/core/javadoc/io/deephaven/engine/util/input/InputTableUpdater.html)
