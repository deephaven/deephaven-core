---
title: Create and use input tables
---

> [!TIP]
> This guide covers input tables created and used directly on the Deephaven server. To stream data from an external Python application using `pydeephaven`, see [Client input tables](./client-input-tables.md).

Input tables allow users to enter new data into tables in two ways: programmatically and manually through the UI.

In the first case, you add data with [`add`](../reference/table-operations/create/input-table.md#methods), an input table method similar to [`merge`](../reference/table-operations/merge/merge.md). In the second case, you click cells in the UI and type their contents, as in a spreadsheet program like [Microsoft Excel](https://www.microsoft.com/en-us/microsoft-365/excel).

Input tables come in two flavors:

- [append-only](#create-an-input-table)
  - An append-only input table puts any entered data at the bottom. It is an [append-only table](../conceptual/table-types.md#specialization-1-append-only).
- [keyed](#create-a-keyed-input-table)
  - A keyed input table has one or more key columns whose values identify each row. You can replace or delete existing rows by key.

This guide shows how to create and use both types.

## Create an input table

First, you need to import the [`input_table`](../reference/table-operations/create/input-table.md) function from the `deephaven` module:

```python
from deephaven import input_table
```

You can create an input table from a pre-existing table _or_ from a set of column definitions. In either case, specifying one or more key columns makes it a keyed input table instead of an append-only one. A key column holds values that identify each row, so no two rows share the same key value. With several key columns, no two rows share the same combination of values. The next two sections create append-only input tables, and [Create a keyed input table](#create-a-keyed-input-table) shows how to add key columns.

### From a pre-existing table

Here, we create an input table from a table that already exists in memory. The example creates that source table with [`empty_table`](../reference/table-operations/create/emptyTable.md).

```python order=result,source
from deephaven import empty_table, input_table

source = empty_table(10).update(["X = i"])

result = input_table(init_table=source)
```

### From column definitions

Here, we create an input table from a set of column definitions. You can pass the column definitions as a [dictionary](https://docs.python.org/3/tutorial/datastructures.html#dictionaries) that maps column names to data types, a [`TableDefinition`](/core/pydoc/code/deephaven.table.html#deephaven.table.TableDefinition), or a list of [`ColumnDefinition`](/core/pydoc/code/deephaven.column.html#deephaven.column.ColumnDefinition) objects. The following example uses a dictionary.

```python order=result
from deephaven import input_table
from deephaven import dtypes as dht

my_col_defs = {"Integers": dht.int32, "Doubles": dht.double, "Strings": dht.string}

result = input_table(col_defs=my_col_defs)
```

The resulting table is initially empty and ready to receive data.

### Create a keyed input table

By default, [`input_table`](../reference/table-operations/create/input-table.md) creates an append-only input table. To create a keyed input table instead, pass one or more key column names in the `key_cols` argument.

Let's first specify one key column.

```python test-set=1 order=null
from deephaven import input_table
from deephaven import dtypes as dht

my_col_defs = {"Integers": dht.int32, "Doubles": dht.double, "Strings": dht.string}

result = input_table(col_defs=my_col_defs, key_cols="Integers")
```

In the case of multiple key columns, specify them in a list.

```python test-set=1 order=null
result = input_table(col_defs=my_col_defs, key_cols=["Integers", "Doubles"])
```

When you create a keyed input table from a pre-existing table, the input table keeps one row per key. If several rows of the initial table share a key, the input table keeps the values from the last of those rows. With multiple key columns, a key is a combination of values. Take, for instance, the following table:

```python test-set=2 order=source
from deephaven import empty_table, input_table

source = empty_table(10).update(
    [
        "Sym = (i % 2 == 0) ? `A` : `B`",
        "Marker = (i % 3 == 2) ? `J` : `K`",
        "X = i",
        "Y = sin(0.1 * X)",
    ]
)
```

No two rows share the same combination of `X` and `Y` values, so a keyed input table with `X` and `Y` as key columns keeps all 10 rows:

```python test-set=2 order=input_source
input_source = input_table(init_table=source, key_cols=["X", "Y"])
```

`Sym` and `Marker` together take only four distinct combinations of values, so a keyed input table with `Sym` and `Marker` as key columns has four rows. Each row holds the values from the last source row with that combination:

```python test-set=2 order=input_source
input_source = input_table(init_table=source, key_cols=["Sym", "Marker"])
```

## Add data to the table

### Programmatically

Two methods add data to an input table programmatically:

- [`add`](../reference/table-operations/create/input-table.md#methods): Synchronous addition.
- [`add_async`](../reference/table-operations/create/input-table.md#methods): Asynchronous addition.

An append-only input table adds new rows to the end of the table. In a keyed input table, an added row whose key already exists replaces the existing row with that key, and the other added rows become new rows.

> [!NOTE]
> To add data to an input table programmatically, the table you add must have the same column names and data types as the input table.

```python test-set=1 order=my_table,my_input_table
from deephaven import empty_table, input_table
from deephaven import dtypes as dht

column_defs = {"Integers": dht.int32, "Doubles": dht.double, "Strings": dht.string}

my_table = empty_table(5).update(
    ["Integers = i", "Doubles = (double)i", "Strings = `a`"]
)

my_input_table = input_table(col_defs=column_defs)
my_input_table.add(my_table)
```

[`add`](../reference/table-operations/create/input-table.md#methods) blocks until Deephaven finishes adding the data. To add data without blocking, use [`add_async`](../reference/table-operations/create/input-table.md#methods). It returns immediately and accepts optional `on_success` and `on_error` callbacks, which Deephaven calls when the queued addition succeeds or fails. If you don't pass `on_error`, Deephaven prints that error instead of raising it. Problems that Deephaven detects before it queues the addition, such as a table whose column names or types don't match the input table, still raise a [`DHError`](/core/pydoc/code/deephaven.dherror.html#deephaven.dherror.DHError) immediately.

Deephaven processes asynchronous calls from the same thread in the order you make them, but it doesn't guarantee an order across threads. The following code block creates a keyed input table with the keys `A`, `B`, and `C`. It then asynchronously adds a row with a new key, `D`, and replaces the row with the existing key `A`:

```python test-set=3 order=my_input_table
from deephaven import new_table, input_table
from deephaven.column import string_col, int_col

my_input_table = input_table(
    init_table=new_table(
        [string_col("Key", ["A", "B", "C"]), int_col("Value", [1, 2, 3])]
    ),
    key_cols="Key",
)

my_input_table.add_async(
    new_table([string_col("Key", ["D", "A"]), int_col("Value", [4, 10])])
)
```

### Manually

To manually add data to an input table, click the cell in which you wish to enter data, type the value, and press **Enter**.

![A user manually adds values to an input table](../assets/how-to/input-tables/input-table-manual.gif)

In a keyed input table, you can edit existing rows. An append-only input table only lets you add new rows. In a keyed input table, adding a row whose key already exists replaces the existing row with that key.

![Adding a row whose key already exists replaces the existing row with that key](../assets/how-to/python-keyed-input-table.gif)

> [!IMPORTANT]
> Added rows aren't final until you click the **Commit** button. If you edit an existing row in a keyed input table, the result is immediate.

![A user clicks on the 'Commit' button](../assets/how-to/input-tables/input-table-commit.gif)

Here are some things to consider when manually entering data into an input table:

- Data added manually to a table must be of the correct type for its column. For instance, attempting to add a string value to an `int` column fails.
- Entering data in between populated cells and pressing **Enter** adds the data to the bottom of the column.

## Delete data from a table

You can delete data only from a keyed input table. Calling [`delete`](../reference/table-operations/create/input-table.md#methods) or [`delete_async`](../reference/table-operations/create/input-table.md#methods) on an append-only input table raises a [`DHError`](/core/pydoc/code/deephaven.dherror.html#deephaven.dherror.DHError). To delete data from a keyed input table, use one of the following methods:

- [`delete`](../reference/table-operations/create/input-table.md#methods): Synchronous deletion.
- [`delete_async`](../reference/table-operations/create/input-table.md#methods): Asynchronous deletion.

To delete table data, supply a table that contains only the key columns, with the key values of the rows you wish to delete. For instance, the asynchronous example in [Programmatically](#programmatically) creates a keyed input table whose key column is `Key`. The following code deletes the row with the key value `B`:

```python test-set=3 order=null
my_input_table.delete(new_table([string_col("Key", ["B"])]))
```

To delete data asynchronously, use [`delete_async`](../reference/table-operations/create/input-table.md#methods). It accepts the same optional `on_success` and `on_error` callbacks as [`add_async`](../reference/table-operations/create/input-table.md#methods) and follows the same ordering rules. The following code block asynchronously deletes the row with the key value `C`:

```python test-set=3 order=null
my_input_table.delete_async(new_table([string_col("Key", ["C"])]))
```

## Enter clickable links in an input table

Input tables are a convenient way to try out clickable links, because you can type links directly into their cells. Any string column in Deephaven can contain a clickable link if the string is formatted correctly. See [Add clickable links](./user-interface/add-clickable-links.md) for examples of strings that are and aren't displayed as links.

![An input table contains both valid and invalid links, with valid links underlined and highlighted in blue](../assets/how-to/ui/invalid_links.png)

Let's create an input table that we can add links to manually:

```python order=result
from deephaven import dtypes as dht, input_table

my_col_defs = {
    "Title": dht.string,
    "Link": dht.string,
}

result = input_table(col_defs=my_col_defs)
```

![Manually adding a clickable link to an input table](../assets/how-to/ui/clickable_link_gif.gif)

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

```python order=int_range_validator,double_range_validator,not_null_validator,not_null_validator_int,non_empty_validator,string_list_validator
from deephaven import new_table, input_table
from deephaven.column import string_col, int_col, double_col

# Create source table with various column types
source = new_table(
    [
        string_col("Key", ["Apple", "Banana", "Carrot", "Date", "Eggplant"]),
        int_col("IntValue", [1, 2, 3, 50, 75]),
        double_col("DoubleValue", [1.5, 2.5, 3.5, 50.5, 75.5]),
        string_col("Category", ["Fruit", "Fruit", "Vegetable", "Fruit", "Vegetable"]),
        string_col("Description", ["Red", "Yellow", "Orange", "Sweet", "Purple"]),
    ]
)

# Import Java classes for validators (not available in Python API yet)
import jpy

range_validating_input_table = jpy.get_type(
    "io.deephaven.server.table.inputtables.RangeValidatingInputTable"
)
double_range_validating_input_table = jpy.get_type(
    "io.deephaven.server.table.inputtables.DoubleRangeValidatingInputTable"
)
not_null_validating_input_table = jpy.get_type(
    "io.deephaven.server.table.inputtables.NotNullValidatingInputTable"
)
non_empty_validating_input_table = jpy.get_type(
    "io.deephaven.server.table.inputtables.NonEmptyValidatingInputTable"
)
string_list_validating_input_table = jpy.get_type(
    "io.deephaven.server.table.inputtables.StringListValidatingInputTable"
)

# Example 1: Integer Range Validator (0-100)
int_range_validator = range_validating_input_table.make(
    input_table(init_table=source, key_cols="Key").j_table, "IntValue", 0, 100
)

# Example 2: Double Range Validator (0.0-100.0)
double_range_validator = double_range_validating_input_table.make(
    input_table(init_table=source, key_cols="Key").j_table, "DoubleValue", 0.0, 100.0
)

# Example 3: Not Null Validator on Category column
not_null_validator = not_null_validating_input_table.make(
    input_table(init_table=source, key_cols="Key").j_table, "Category"
)

# Example 3.1: Not Null Validator on IntValue column
not_null_validator_int = not_null_validating_input_table.make(
    input_table(init_table=source, key_cols="Key").j_table, "IntValue"
)

# Example 4: Non-Empty Validator on Description column
non_empty_validator = non_empty_validating_input_table.make(
    input_table(init_table=source, key_cols="Key").j_table, "Description"
)

# Example 5: String List Validator - Category must be "Fruit" or "Vegetable"
string_list_validator = string_list_validating_input_table.make(
    input_table(init_table=source, key_cols="Key").j_table,
    "Category",
    "Fruit",
    "Vegetable",
    "Grain",
)
```

-->

## Related documentation

- [`input_table`](../reference/table-operations/create/input-table.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [Deephaven Python dtypes](../reference/python/deephaven-python-types.md)
- [Table types](../conceptual/table-types.md)
- [Add clickable links](./user-interface/add-clickable-links.md)
