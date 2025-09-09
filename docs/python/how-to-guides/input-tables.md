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

First, you need to import the `input_table` method from the `deephaven` module.

```python
from deephaven import input_table
```

An input table can be constructed from a pre-existing table _or_ a list of column definitions. In either case, one or more key columns can be specified, which turns the table from an append-only input table to a keyed input table.

### From a pre-existing table

Here, we will create an input table from a table that already exists in memory. In this case, we'll create one with [`empty_table`](../reference/table-operations/create/emptyTable.md).

```python order=result,source
from deephaven import empty_table, input_table

source = empty_table(10).update(["X = i"])

result = input_table(init_table=source)
```

### From scratch

Here, we will create an input table from a list of column definitions. Column definitions must be defined in a [dictionary](https://docs.python.org/3/tutorial/datastructures.html#dictionaries).

```python order=result
from deephaven import input_table
from deephaven import dtypes as dht

my_col_defs = {"Integers": dht.int32, "Doubles": dht.double, "Strings": dht.string}

result = input_table(col_defs=my_col_defs)
```

The resulting table is initially empty, and ready to receive data.

### Create a keyed input table

In the previous two examples, no key column was specified when creating the input tables. If one or more key columns is specified, the table becomes a keyed table.

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

When creating a keyed input table from a pre-existing table, the key column(s) must satisfy uniqueness criteria. Each row or combination of rows in the initial table must not have repeating values. Take, for instance, the following table:

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

A keyed input table _can_ be created from the `X` and `Y` columns, since they have no repeating values, and are thus unique:

```python test-set=2 order=input_source
input_source = input_table(init_table=source, key_cols=["X", "Y"])
```

A keyed input table _cannot_ be created from the `Sym` _or_ `Marker` columns, since they have repeating values and combinations, and are thus _not_ unique:

```python test-set=2 should-fail
input_source = input_table(init_table=source, key_cols=["Sym", "Marker"])
```

## Add data to the table

### Programmatically

You can add data to input tables in two ways:

- [`add`](../reference/table-operations/create/input-table.md#methods): Synchronous addition.
- [`add_async`](../reference/table-operations/create/input-table.md#methods): Asynchronous addition.

New data is added to the end of the input table. If the input table is keyed, the new data will overwrite any existing data with the same key.

> [!NOTE]
> To programmatically add data to an input table, the table schemas (column definitions) must match. These column definitions comprise the names and data types of every column in the table.

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

Data can also be added to an input table asynchronously. Asynchronous function calls in the same thread are queued and processed in order. However, ordering is not guaranteed across threads. The following code block asynchronously adds data to a keyed input table:

> [!IMPORTANT]
> Asynchronous adds can only be done on keyed input tables.

```python order=my_input_table
from deephaven import empty_table, input_table
from deephaven import dtypes as dht
from string import ascii_uppercase
from random import choice


def rand_key() -> str:
    return choice(ascii_uppercase)


def create_table(n_rows: int):
    return empty_table(n_rows).update(["Key = rand_key()", "Value = randomInt(0, 100)"])


column_defs = {"Key": dht.string, "Value": dht.int32}

my_input_table = input_table(column_defs, key_cols="Key")

my_input_table.add_async(create_table(5))
my_input_table.add_async(create_table(4))
my_input_table.add_async(create_table(3))
```

### Manually

To manually add data to an input table, simply click on the cell in which you wish to enter data. Type the value into the cell, hit enter, and it will appear.

![A user manually adds values to an input table](../assets/how-to/input-tables/input-table-manual.gif)

Note that with a keyed input table, you can edit existing rows; however, adding a new row will erase previous rows with the same key.

![Editing existing rows erases previous rows with the same key](../assets/how-to/python-keyed-input-table.gif)

> [!IMPORTANT]
> Added rows aren't final until you hit the **Commit** button. If you edit an existing row in a keyed input table, the result is immediate.

![A user clicks on the 'Commit' button](../assets/how-to/input-tables/input-table-commit.gif)

Here are some things to consider when manually entering data into an input table:

- Manually entered data in a table will not be final until the **Commit** button at the bottom right of the console is clicked.
- Data added manually to a table must be of the correct type for its column. For instance, attempting to add a string value to an int column will fail.
- Entering data in between populated cells and hitting **Enter** will add the data to the bottom of the column.

## Delete data from a table

Data can only be deleted from a keyed input table. To delete data from a keyed input table, use one of the following methods:

- [`delete`](../reference/table-operations/create/input-table.md#methods): Synchronous deletion.
- [`delete_async`](../reference/table-operations/create/input-table.md#methods): Asynchronous deletion.

To delete table data, supply only the key values of the rows you wish to delete. For instance, in the [previous section](#programmatically), we created a keyed input table where the key column is called `Key`. The following code deletes a row with the key value `A`:

```python skip-test
my_input_table.delete(empty_table(1).update("Key = `A`"))
```

The same applies for asynchronously deleting data from a keyed input table. The following code block asynchronously deletes a row with the key value `B`:

> [!NOTE]
> Asynchronous functions calls in the same thread are queued and processed in order. However, ordering is not guaranteed across threads.

```python skip-test
my_input_table.delete_async(empty_table(1).update("Key = `B`"))
```

## Clickable links

Any string column in Deephaven can contain a clickable link — the string just has to be formatted correctly.

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

## Related documentation

- [`input_table`](../reference/table-operations/create/input-table.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [Deephaven Python dtypes](../reference/python/deephaven-python-types.md)
- [Table types](../conceptual/table-types.md)
