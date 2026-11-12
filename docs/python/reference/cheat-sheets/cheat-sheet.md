---
title: Ultimate cheat sheet
---

This cheat sheet shows common Deephaven table operations in one place, with a short example of each. Most of them work the same way on static tables and on ticking tables that update in real time; the few exceptions are noted where they apply. Run the code in [Necessary data](#necessary-data) first, because most examples on this page use the tables it creates.

## Necessary data

This code creates the following tables, helper functions, and values:

- `static_source_1` and `static_source_2`: static tables.
- `ticking_source`: a ticking table, which updates while the query runs. [`time_table`](../table-operations/create/timeTable.md) adds a row to it every second. See [Deephaven's table types](../../conceptual/table-types.md).
- `rand_choice` and `get_random_string`: helper functions that generate random strings.
- `letters_upp`: a string of the uppercase letters A to Z, passed to `rand_choice`.

The code casts the helper functions' return values with `(String)`, which turns the Python return values into Java strings. See [Cast strings](#cast-strings).

> [!NOTE]
> In query strings, backticks (`` ` ``) delimit a string, and single quotes (`'`) delimit a literal that the engine parses, such as a date-time, duration, or time zone.

```python test-set=1 order=null reset
from deephaven import empty_table, time_table
import random
import string

letters_upp = string.ascii_uppercase


def get_random_string(length):
    letters = string.ascii_letters
    result_str = "".join(random.choice(letters) for i in range(length))
    return result_str


def rand_choice(array):
    return random.choice(array)


# Create static tables
static_source_1 = empty_table(26300).update(
    [
        "X = ii",
        "Double1 = randomDouble(1, 100)",
        "Int1 = randomInt(1, 100)",
        "Timestamp = parseInstant(`2016-01-01T01:00 ET`) + i * HOUR",
        "Date = formatDate(Timestamp, timeZone(`ET`))",
        "String1 = (String)rand_choice(letters_upp)",
        "String2 = (String)get_random_string(randomInt(1, 4))",
    ]
)

static_source_2 = empty_table(26300).update(
    [
        "X = ii",
        "Double2 = randomDouble(1, 80)",
        "Int2 = randomInt(1, 100)",
        "Timestamp = parseInstant(`2019-01-01T01:00 ET`) + i * HOUR",
        "String3 = (String)rand_choice(letters_upp)",
        "String4 = (String)get_random_string(randomInt(1, 4))",
    ]
)

# Create a ticking table
ticking_source = time_table("PT1S").update(
    [
        "X = ii",
        "Double3 = randomDouble(1, 100)",
        "Int3 = randomInt(1, 100)",
        "Date = formatDate(Timestamp, timeZone(`ET`))",
        "String5 = (String)rand_choice(letters_upp)",
        "String6 = (String)get_random_string(randomInt(1, 4))",
    ]
)
```

## Create tables

### Empty tables

- [`empty_table`](../table-operations/create/emptyTable.md)

```python order=result,result1
from deephaven import empty_table

result = empty_table(5)

# Empty tables are often followed with a formula
result1 = result.update(formulas=["X = 5"])
```

### New tables

- [`new_table`](../table-operations/create/newTable.md)

Create columns with the following methods:

- [`bool_col`](../table-operations/create/boolCol.md)
- [`byte_col`](../table-operations/create/byteCol.md)
- [`char_col`](../table-operations/create/charCol.md)
- [`datetime_col`](../table-operations/create/dateTimeCol.md)
- [`double_col`](../table-operations/create/doubleCol.md)
- [`float_col`](../table-operations/create/floatCol.md)
- [`int_col`](../table-operations/create/intCol.md)
- [`jobj_col`](../table-operations/create/jobj_col.md)
- [`long_col`](../table-operations/create/longCol.md)
- [`pyobj_col`](../table-operations/create/pyobj_col.md)
- [`short_col`](../table-operations/create/shortCol.md)
- [`string_col`](../table-operations/create/stringCol.md)

```python
from deephaven import new_table
from deephaven.column import string_col, int_col

result = new_table(
    [
        int_col("IntegerColumn", [1, 2, 3]),
        string_col("Strings", ["These", "are", "Strings"]),
    ]
)
```

### Time tables

- [`time_table`](../table-operations/create/timeTable.md)

The following code makes a [`time_table`](../table-operations/create/timeTable.md) that updates every second.

```python ticking-table order=null
from deephaven import time_table

result = time_table("PT1S")
```

### Input tables

Use an [`input_table`](../table-operations/create/input-table.md) to add or modify data in table cells directly.

```python order=my_input_table
from deephaven import input_table
from deephaven import dtypes as dht

my_column_defs = {"StringCol": dht.string, "DoubleCol": dht.double}

my_input_table = input_table(col_defs=my_column_defs, key_cols="StringCol")
```

### Ring tables

A [`ring_table`](../table-operations/create/ringTable.md) keeps the `n` most recent rows of a source table. When the source table ticks, rows older than the newest `n` drop out of the ring table.

```python ticking-table order=null
from deephaven import time_table, ring_table

tt = time_table("PT1S")
rt = ring_table(tt, 5)
```

### Tree tables

A [`tree`](../table-operations/create/tree.md) table has collapsible subsections that you can expand in the UI for more detail.

```python order=source,result
from deephaven import empty_table

source = empty_table(100).update_view(
    ["ID = i", "Parent = i == 0 ? NULL_INT : (int)(i / 4)"]
)

result = source.tree(id_col="ID", parent_col="Parent")
```

### Rollup tables

A [`rollup`](../table-operations/create/rollup.md) table aggregates the source table at each level of a list of grouping columns. You can expand each level in the UI to show the rows of the level below it.

```python order=insurance,insurance_rollup
from deephaven import read_csv, agg

insurance = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv"
)

agg_list = [agg.avg(cols=["bmi", "expenses"])]
by_list = ["region", "age"]

insurance_rollup = insurance.rollup(
    aggs=agg_list, by=by_list, include_constituents=True
)
```

### Tables written from code

A [`DynamicTableWriter`](../table-operations/create/DynamicTableWriter.md) creates an [append-only](../../conceptual/table-types.md#specialization-1-append-only) table and writes rows to it from your own code. It is the simpler choice when you add one row at a time. For most other cases, use [`table_publisher`](../table-operations/create/TablePublisher.md), which publishes rows to a [blink table](../../conceptual/table-types.md#specialization-3-blink). [Write data to an in-memory, real-time table](../../how-to-guides/table-publisher.md) compares the two.

```python order=null
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht

import threading
import time

table_writer = DynamicTableWriter(
    {
        "X": dht.double,
        "SawToothWave": dht.double,
        "SquareWave": dht.double,
        "TriangleWave": dht.double,
    }
)

waveforms = table_writer.table


def create_waveforms():
    for i in range(200):
        start = time.time()
        x = 0.1 * i
        y_sawtooth = (x % 1 - 0.5) * 2
        y_square = 1.0 if x % 2 < 1 else -1.0
        y_triangle = (x % 1 - 0.5) * 2 if x % 2 >= 1 else -(x % 1 - 0.5) * 2
        table_writer.write_row(x, y_sawtooth, y_square, y_triangle)
        end = time.time()
        time.sleep(max(0.0, 0.2 - (end - start)))


thread = threading.Thread(target=create_waveforms)
thread.start()
```

## View table metadata

[`meta_table`](../table-operations/metadata/meta_table.md) shows each column's name, data type, column type, and whether it is a partitioning column. Use it to confirm that schemas match before a [`merge`](../table-operations/merge/merge.md).

```python test-set=1 order=null
static_1_meta = static_source_1.meta_table
```

## Merge tables

Tables passed to [`merge`](../table-operations/merge/merge.md) must have identical schemas, with the same column names and the same column data types. This applies to both static and ticking tables.

- [`merge`](../table-operations/merge/merge.md)
- [`merge_sorted`](../table-operations/merge/merge-sorted.md)

```python test-set=1 ticking-table order=null
from deephaven import merge

copy1 = static_source_1
merge_same_static = merge([static_source_1, copy1])

copy_updating_1 = ticking_source
copy_updating_2 = ticking_source
merge_same_dynamic = merge([copy_updating_1, copy_updating_2])

# one can merge static and ticking tables
static_source_1v2 = static_source_1.view(["Date", "Timestamp", "SourceRowNum = X"])
ticking_source_v2 = ticking_source.view(["Date", "Timestamp", "SourceRowNum = X"])
merge_static_and_dynamic = merge([static_source_1v2, ticking_source_v2])
```

## Filter

Most queries benefit from [filtering](../../how-to-guides/use-filters.md) early. Less data generally means better performance.

> [!TIP]
> For SQL developers: in Deephaven, [joins](../../how-to-guides/joins-exact-relational.md) are not a primary operation for filtering. Filter your data with [`where`](../table-operations/filter/where.md), [`where_in`](../table-operations/filter/where-in.md), and [`where_not_in`](../table-operations/filter/where-not-in.md) before joining. Filtering first reduces the rows each join has to match.

### Date and time examples

```python test-set=1 ticking-table order=null
from deephaven.time import to_j_instant

todays_data_1 = ticking_source.where(
    ["Date = today(timeZone(`ET`))"]
)  # Date for this is a String
todays_data_3 = ticking_source.where(
    ["formatDate(Timestamp, 'Australia/Sydney') = today('Australia/Sydney')"]
)  # sydney timezone

single_string_date = static_source_1.where(["Date = `2017-01-03`"])
less_than_date = static_source_1.where(["Date < `2017-01-03`"])  # use >=, etc. as well
one_month_string = static_source_1.where(["Date.startsWith(`2017-02`)"])

single_db_date = static_source_2.where(
    ["formatDate(Timestamp, timeZone(`ET`)) = `2020-03-01`"]
)
after_db_datetime = static_source_1.where(["Timestamp > '2017-01-05T10:30:00 ET'"])

just_biz_time = static_source_1.where(
    ["calendar(`USNYSE_EXAMPLE`).isBusinessTime(Timestamp)"]
)  # the default calendar, UTC, treats every day as a business day and every moment as business time
just_august_datetime = static_source_2.where(
    ["monthOfYear(Timestamp, timeZone(`ET`)) = 8"]
)
just_10am_hour = static_source_1.where(
    ["hourOfDay(Timestamp, timeZone(`ET`), false) = 10"]
)
just_tues = static_source_2.where(
    ["dayOfWeek(Timestamp, timeZone(`ET`)).getValue() = 2"]
)  # Tuesdays

time1 = to_j_instant("2017-08-21T09:45:00 ET")
time2 = to_j_instant("2017-08-21T10:10:00 ET")
# You can use inRange to filter times by a range
trades = static_source_1.where(["inRange(Timestamp, time1, time2)"])
# However, conjunctive filters are faster, sometimes significantly so
trades = static_source_1.where(["Timestamp >= time1", "Timestamp <= time2"])

# You can also write date-time literals directly in the query string with single quotes (')
time1 = "2017-08-21T09:45:00 ET"
time2 = "2017-08-21T10:10:00 ET"
trades = static_source_1.where([f"inRange(Timestamp, '{time1}', '{time2}')"])
trades = static_source_1.where([f"Timestamp >= '{time1}'", f"Timestamp <= '{time2}'"])
```

### String examples

```python test-set=1 order=null
one_string_match = static_source_1.where(["String1 = `A`"])  # match filter

string_set_match = static_source_1.where(["String1 in `A`, `M`, `G`"])
case_insensitive = static_source_1.where(["String1 icase in `a`, `m`, `g`"])
not_in_example = static_source_1.where(["String1 not in `A`, `M`, `G`"])  # see "not"

contains_example = static_source_1.where(["String2.contains(`P`)"])
not_contains_example = static_source_1.where(["!String2.contains(`P`)"])
starts_with_example = static_source_1.where(["String2.startsWith(`A`)"])
ends_with_example = static_source_1.where(["String2.endsWith(`M`)"])
```

### Number examples

The `mod_example_1` filter condition uses the row-position variable [`i`](../query-language/variables/special-variables.md). See [Get the row number](#get-the-row-number) for when [`i` and `ii`](../query-language/variables/special-variables.md) are allowed in formulas. The engine doesn't check `where` filters, so a filter that uses [`i`](../query-language/variables/special-variables.md) on a ticking table runs, but its results can go stale as rows change.

```python test-set=1 order=null
equals_example = static_source_2.where(["Int2 = 44"])
less_than_example = static_source_2.where(["Double2 < 8.42"])
some_manipulation = static_source_2.where(["(Double2 - Int2) / Int2 > 0.05"])
mod_example_1 = static_source_2.where(["i % 10 = 0"])  # every 10th row
mod_example_2 = static_source_2.where(["String4.length() % 2 = 0"])
# String4 values with an even number of characters
```

### Multiple filters

```python test-set=1 order=null
conjunctive_comma = static_source_1.where(["Date = `2017-08-23`", "String1 = `A`"])
conjunctive_ampersand = static_source_1.where(["Date = `2017-08-23` && String1 = `A`"])

disjunctive_same_col = static_source_1.where(
    ["Date = `2017-08-23` || Date = `2017-08-25`"]
)
disjunctive_diff_col = static_source_1.where(["Date = `2017-08-23` || String1 = `A`"])

range_conjunctive = static_source_1.where(
    ["Date >= `2017-08-21`", "Date <= `2017-08-23`"]
)
range_in_range = static_source_1.where(["inRange(Date, `2017-08-21`, `2017-08-23`)"])

in_range_string = static_source_1.where("inRange(String2.substring(0,1), `A`, `D`)")
# starts with an uppercase letter from A to D
```

### OR filters

A list of filters passed to [`where`](../table-operations/filter/where.md) keeps rows that match all of them. To keep rows that match at least one condition, either combine the conditions with `||` in one filter string, as in [Multiple filters](#multiple-filters), or pass the filters to [`where_one_of`](../table-operations/filter/where-one-of.md).

```python order=source,result_or
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col
from deephaven.constants import NULL_INT

source = new_table(
    [
        string_col("Letter", ["A", "C", "F", "B", "E", "D", "A"]),
        int_col("Number", [NULL_INT, 2, 1, NULL_INT, 4, 5, 3]),
        string_col(
            "Color", ["red", "blue", "orange", "purple", "yellow", "pink", "blue"]
        ),
        int_col("Code", [12, 14, 11, NULL_INT, 16, 14, NULL_INT]),
    ]
)

result_or = source.where_one_of(
    filters=["Color = `blue`", "Number > 2"]
)  # OR operation - result will have _either_ criteria
```

### `where_in` and `where_not_in`

```python test-set=1 order=null
# run these two queries together
string_set_driver = static_source_1.rename_columns(cols=["String4 = String2"]).head(10)
where_in_example_1 = static_source_2.where_in(string_set_driver, ["String4"])

where_in_example_2 = static_source_2.where_in(static_source_1, ["String4 = String2"])
# String4 values in static_source_2 that appear in String2 of static_source_1

where_not_in_example = static_source_2.where_not_in(
    static_source_1, ["String4 = String2"]
)
# see the "not"
```

### Nulls and NaNs

```python test-set=1 order=null
# static_source_1 has no nulls or NaNs, so null_example and nan_example are empty
null_example = static_source_1.where(["isNull(Int1)"])  # all data types supported
not_null_example = static_source_1.where(["!isNull(Int1)"])

nan_example = static_source_1.where(["isNaN(Double1)"])
not_nan_example = static_source_1.where(["!isNaN(Double1)"])
```

### Head and tail

These methods reduce the number of rows:

- [`tail`](../table-operations/filter/tail.md)
- [`tail_pct`](../table-operations/filter/tail-pct.md)
- [`head`](../table-operations/filter/head.md)
- [`head_pct`](../table-operations/filter/head-pct.md)
- [`head_by`](../table-operations/group-and-aggregate/headBy.md)
- [`tail_by`](../table-operations/group-and-aggregate/tailBy.md)

```python test-set=1 ticking-table order=null
first_10k_rows = static_source_2.head(10_000)
ticking_last_rows = ticking_source.tail(100)

first_15_percent = ticking_source.reverse().head_pct(0.15)  # most recent 15% of rows
last_15_percent = static_source_2.tail_pct(0.15)  # the last 15% of rows

first_10_by_key = static_source_1.head_by(
    10, ["String1"]
)  # first 10 rows for each String1-key
last_10_by_key = static_source_1.tail_by(
    10, ["String1"]
)  # last 10 rows for each String1-key
```

## Sort

Sort on one or more columns in one direction:

- [`sort`](../table-operations/sort/sort.md)
- [`sort_descending`](../table-operations/sort/sort-descending.md)

Sort in mixed directions, with a direction for each column:

- [`sort`](../table-operations/sort/sort.md) with `order`

```python test-set=1 order=null
# works for numeric, string, date-time, and other Comparable column types

sort_time_v1 = static_source_1.sort(order_by=["Timestamp"])  # sort ascending
sort_double = static_source_1.sort("Double1")

sort_numbers = static_source_1.sort_descending(["Int1"])  # sort descending
sort_strings = static_source_1.sort_descending(["String2"])

from deephaven import SortDirection

sort_multi_desc = static_source_1.sort_descending(
    order_by=["String1", "Timestamp"]
)  # multiple
sort_multi = static_source_1.sort(["Int1", "String1"])

sort_multiple_col = [SortDirection.ASCENDING, SortDirection.DESCENDING]

multi_sort = static_source_1.sort(
    ["Int1", "Double1"], sort_multiple_col
)  # Int ascending then Double descending
```

Reverse the order of rows:

- [`reverse`](../table-operations/sort/reverse.md)

> [!TIP]
> [`reverse`](../table-operations/sort/reverse.md) is faster than [`sort`](../table-operations/sort/sort.md). UIs often use it to show the most recently appended rows at the top of a table.

```python test-set=1 ticking-table order=null
reverse_table = static_source_1.reverse()
reverse_updating = ticking_source.reverse()
```

## Select and create new columns

Deephaven has three ways to choose and add columns. They differ in when formulas are calculated and whether the results are stored in memory. For a full comparison, see [Select and create columns](../../how-to-guides/use-select-view-update.md). The last subsection, [Get the row number](#get-the-row-number), shows the built-in row-position variables that any of these methods can use.

### Option 1: Calculate and store new columns in memory

Use [`select`](../table-operations/select/select.md) and [`update`](../table-operations/select/update.md) when data is expensive to calculate or accessed frequently. These methods store results in memory, which speeds up access but uses more memory.

```python test-set=1 order=null
select_columns = static_source_2.select(["Timestamp", "Int2", "Double2", "String3"])
# constrain to only those 4 columns, write to memory
select_add_col = static_source_2.select(
    ["Timestamp", "Int2", "Double2", "String3", "Difference = Double2 - Int2"]
)
# constrain and add a new column calculating

select_and_update_col = static_source_2.select(
    ["Timestamp", "Int2", "Double2", "String3"]
).update("Difference = Double2 - Int2")
# add a new column calculating - logically equivalent to previous example
```

### Option 2: Calculate new columns on demand

Use [`view`](../table-operations/select/view.md) and [`update_view`](../table-operations/select/update-view.md) when the formula is quick to compute or when queries read only a portion of the data at a time. These methods minimize memory use.

```python test-set=1 order=null
view_columns = static_source_2.view(["Timestamp", "Int2", "Double2", "String3"])
# similar to select(), though uses on-demand formula
view_add_col = static_source_2.view(
    ["Timestamp", "Int2", "Double2", "String3", "Difference = Double2 - Int2"]
)
# view set and add a column, though with an on-demand formula

view_and_update_view_col = static_source_2.view(
    ["Timestamp", "Int2", "Double2", "String3"]
).update_view(["Difference = Double2 - Int2"])
# logically equivalent to previous example
```

### Option 3: Calculate on demand and cache results

Use [`lazy_update`](../table-operations/select/lazy-update.md) when the formula's input columns have a relatively small number of unique values. It computes each result on demand, caches it, and reuses it for repeated inputs.

```python test-set=1 order=null
# Int2 has at most 99 distinct values, so each square root is computed once and reused
lazy_update_example = static_source_2.lazy_update(["Int2Sqrt = sqrt(Int2)"])
```

### Get the row number

[`i` and `ii`](../query-language/variables/special-variables.md) are built-in variables that hold a row's position in the table. [`i`](../query-language/variables/special-variables.md) is an `int` and [`ii`](../query-language/variables/special-variables.md) is a `long`.

> [!CAUTION]
> A row's position can change as rows are added or removed. By default, the engine allows [`i` and `ii`](../query-language/variables/special-variables.md) in a formula on a ticking table only if the table is [append-only](../../conceptual/table-types.md#specialization-1-append-only) or a [blink table](../../conceptual/table-types.md#specialization-3-blink). An [add-only table](../../conceptual/table-types.md#specialization-2-add-only) whose row keys match its row positions also qualifies. The engine doesn't check `where` filters, so a filter that uses [`i` or `ii`](../query-language/variables/special-variables.md) on a ticking table runs, but its results can go stale as rows change.

```python test-set=1 order=null
get_row_num = static_source_2.update_view(["RowNumInt = i", "RowNumLong = ii"])
```

## Manipulate columns

These methods list distinct values and rename, drop, and reorder columns.

```python test-set=1 order=null
unique_values = static_source_2.select_distinct("String4")  # show unique set
# works on all data types; a high-cardinality column, such as Double2, returns nearly one row per source row
unique_values_combo = static_source_2.select_distinct(["String3", "String4"])
# unique combinations of String3 with String4

rename_stuff = static_source_2.rename_columns(["Symbol = String4", "Price = Double2"])
drop_column = static_source_2.drop_columns(["X"])  # same for drop one or many
drop_columns = static_source_2.drop_columns(["X", "Int2", "String3"])

put_cols_at_start = static_source_2.move_columns_up(
    ["String4", "Int2"]
)  # Makes 1st col(s)
put_cols_wherever = static_source_2.move_columns(1, ["String4", "Int2"])
# makes String4 the 2nd and Int2 the 3rd column
```

## Do math

Query strings can call Java `Math` methods and Deephaven's built-in numeric functions, such as `abs`, `exp`, `log`, `round`, and `floor`.

```python test-set=1 order=null
# should use .update() when working with random numbers
simple_math = static_source_2.update(
    [
        "RandomNumber = Math.random()",
        "RandomInt100 = new Random().nextInt(100)",
        "Arithmetic = Int2 * Double2",
        "SigNum = Math.signum(RandomNumber - 0.5)",
        "Signed = SigNum * Arithmetic",
        "AbsDlrVolume = abs(Signed)",
        "Power = Math.pow(i, 2)",
        "Exp = exp(Double2)",
        "Log = log(Double2)",
        "Round = round(Double2)",
        "Floor = floor(Double2)",
        "Mod = RandomInt100 % 5",
        "CastInt = (int)AbsDlrVolume",
        "CastLong = (long)AbsDlrVolume",
    ]
)
```

## Handle arrays

Append an underscore to a column name to use the whole column as an [array](../query-language/types/arrays.md). For example, `Double2_` is the `Double2` column as an array. Index the array with [`i`](../query-language/variables/special-variables.md) to reach values in other rows. [`subVector(start, end)`](../query-language/types/arrays.md) returns a slice of an array, from `start` up to but not including `end`.

> [!CAUTION]
> In a formula, a simple constant offset, such as `Double2_[i-1]`, works on any table. Other column-array expressions, such as `Double2_.subVector(i-2, i+1)`, work in formulas only on static tables and [blink tables](../../conceptual/table-types.md#specialization-3-blink). On other ticking tables, including append-only tables, the engine rejects them with an error. For rolling calculations on ticking tables, use [`update_by`](../table-operations/update-by-operations/updateBy.md). See [Special variables](../query-language/variables/special-variables.md).

```python test-set=1 order=null
array_examples = static_source_2.update_view(
    [
        "RowNumber = i",
        "PrevRowReference = Double2_[i-1]",
        "MultipleRowRef = Double2_[i-2] - Double2_[i-1]",
        "SubArray = Double2_.subVector(i-2, i+1)",
        "SumSubArray = sum(Double2_.subVector(i-2, i+1))",
        "ArraySize = SubArray.size()",
        "ArrayMin = min(SubArray)",
        "ArrayMax = max(SubArray)",
        "ArrayMedian = median(SubArray)",
        "ArrayAvg = avg(SubArray)",
        "ArrayStd = std(SubArray)",
        "ArrayVar = var(SubArray)",
        "ArrayLast = last(SubArray)",
        "ArrayIndex = SubArray[1]",
        "InArray = in(44, Int2_.subVector(i-2, i+1))",
    ]
)
```

## Calculate rolling sums and averages

[`update_by`](../table-operations/update-by-operations/updateBy.md) provides dedicated cumulative and rolling operations, such as [`cum_sum`](../table-operations/update-by-operations/cum-sum.md) and [`rolling_sum_tick`](../table-operations/update-by-operations/rolling-sum-tick.md).

### Create a rolling sum with `update_by`

[`update_by`](../table-operations/update-by-operations/updateBy.md) with [`rolling_sum_tick`](../table-operations/update-by-operations/rolling-sum-tick.md) computes a sum over a 20-row window that ends at the current row. It computes a separate sum for each `String1` group.

```python test-set=1 order=null
from deephaven.updateby import rolling_sum_tick

rolling_sum_op = rolling_sum_tick(
    cols=["RollingSumInt1 = Int1"], rev_ticks=20, fwd_ticks=0
)

sums = static_source_1.update_by(ops=[rolling_sum_op], by=["String1"])
```

### Set up an exponential moving average (EMA)

An exponential moving average (EMA) is an average that weights recent values more heavily than older ones. [`ema_time`](../table-operations/update-by-operations/ema-time.md) computes an EMA with a decay time of 30 seconds, across the whole table and for each `String5` group. [`last_by`](../table-operations/group-and-aggregate/lastBy.md) then keeps the latest row overall and the latest row for each group.

```python test-set=1 ticking-table order=null
from deephaven.updateby import ema_time

ema_30_sec = ema_time(ts_col="Timestamp", decay_time="PT30S", cols=["EmaData = Int3"])

ema_data = ticking_source.update_by(ops=ema_30_sec).last_by()
ema_data_grouped = (
    ticking_source.update_by(ops=ema_30_sec, by=["String5"])
    .last_by(by=["String5"])
    .sort(order_by=["String5"])
)
```

### Calculate a rolling window with column arrays

For a static table, you can also compute statistics over a window of rows with column arrays (see [Handle arrays](#handle-arrays)) instead of [`update_by`](../table-operations/update-by-operations/updateBy.md). This example [sorts](../table-operations/sort/sort.md) by `String3`. For each row, it then sums `Double2` and averages `Int2` over the 10 rows that end at that row. The result is null unless all 10 rows have the same `String3` value.

```python test-set=1 order=null
def has_others(array, target) -> bool:
    for x in array:
        if x != target:
            return True
    return False


make_arrays = static_source_2.sort("String3").update_view(
    [
        "String3Array10 = String3_.subVector(i-9, i+1)",
        "Int2Array10 = Int2_.subVector(i-9, i+1)",
        "Double2Array10 = Double2_.subVector(i-9, i+1)",
    ]
)

roll_sum_and_avg_10 = make_arrays.update(
    [
        "ArrayHasOtherString3s = has_others(String3Array10.toArray(), String3)",
        "RollSum10 = ArrayHasOtherString3s = false ? \
        sum(Double2Array10) : NULL_DOUBLE",
        "RollAvg10 = ArrayHasOtherString3s = false ? \
        avg(Int2Array10) : NULL_DOUBLE",
    ]
)
```

## Manipulate time and calendars

Query strings include built-in functions for date-times and business calendars. See [Work with time](../../conceptual/time-in-deephaven.md) and [Work with calendars](../../how-to-guides/business-calendar.md).

```python test-set=1 order=null
from deephaven.calendar import set_calendar

# The default calendar, UTC, treats every day as a business day and every moment as business time
set_calendar("USNYSE_EXAMPLE")

time_stuff = static_source_2.view(
    [
        "Timestamp",
        "CurrentTime = now()",
        "CurrentDateDefault = calendarDate()",
        "CurrentDateLon = today(timeZone(`Europe/London`))",
        "IsAfter = CurrentTime > Timestamp",
        "IsBizDay = isBusinessDay(CurrentDateLon)",
        "StringDT = formatDateTime(Timestamp, timeZone(`ET`))",
        "StringDate = formatDate(Timestamp, timeZone(`ET`))",
        "StringToTime = parseInstant(StringDate + `T12:00 ET`)",
        "AddTime = Timestamp + 'PT05:11:04.332'",
        "PlusHour = Timestamp + HOUR",
        "LessTenMins = Timestamp - 10 * MINUTE",
        "DiffYear = diffYears365(Timestamp, CurrentTime)",
        "DiffDay = diffDays(Timestamp, CurrentTime)",
        "DiffNanos = PlusHour - Timestamp",
        "DayWeek = dayOfWeek(Timestamp, timeZone(`ET`))",
        "HourDay = hourOfDay(Timestamp, timeZone(`ET`), false)",
        "DateAtMidnight = atMidnight(Timestamp, timeZone(`ET`))",
        "IsBizDayString = isBusinessDay(StringDate)",
        "IsBizDayDatetime = isBusinessDay(Timestamp)",
        "IsBizTime = isBusinessTime(Timestamp)",
        "FracBizDayDone = fractionBusinessDayComplete(CurrentTime)",
        "FracBizDayOpen = fractionBusinessDayRemaining(CurrentTime)",
        "NextBizDayString = plusBusinessDays(StringDate, 1).toString()",
        "NextBizDayDatetime = plusBusinessDays(Timestamp, 1)",
        "PlusFiveBizDayCurrent = futureBusinessDate(5)",
        "PlusFBizDayString = plusBusinessDays(StringDate, 5).toString()",
        "PlusFBizDayDatetime = plusBusinessDays(Timestamp, 5)",
        "PrevBizDayCurrent = pastBusinessDate(1)",
        "PrevBizDayString = minusBusinessDays(StringDate, 1).toString()",
        "PrevBizDayDatetime = minusBusinessDays(Timestamp, 1)",
        "MinusFiveBizDayCurrent = pastBusinessDate(5)",
        "MinusFiveBizDayString = minusBusinessDays(StringDate, 5).toString()",
        "MinusFiveBizDayDatetime = minusBusinessDays(Timestamp, 5)",
        "BizDayArray = businessDates(Timestamp, CurrentTime)",
        "NonBizDayArray = nonBusinessDates(Timestamp, CurrentTime)",
        "DiffBizDaysCount = numberBusinessDates(Timestamp, CurrentTime)",
        "DiffBizDaysExact = diffBusinessDays(Timestamp, CurrentTime)",
        "DiffBizDaysString = numberBusinessDates(MinusFiveBizDayString, StringDate)",
        "StandardBizDayNanos = standardBusinessNanos()",
        "DiffBizSecs = diffBusinessNanos(CurrentTime, CurrentTime + 5 * DAY) / 1E9",
        "LastBizOfMonth = isLastBusinessDayOfMonth(StringDate)",
        "LastBizOfWeek = isLastBusinessDayOfWeek(CurrentTime)",
    ]
)
```

## Manipulate strings

Query strings can call Java `String` methods, as the following examples show:

```python test-set=1 order=null
string_stuff = static_source_2.view(
    [
        "StringDate = formatDate(Timestamp, timeZone(`ET`))",
        "String4",
        "Double2",
        "NewString = `new_string_example_`",
        "ConcatV1 = NewString + String4",
        "ConcatV2 = NewString + `Note_backticks!!`",
        "ConcatV3 = NewString.concat(String4)",
        "ConcatV4 = NewString.concat(`Note_backticks!!`)",
        "StartBool = String4.startsWith(`M`)",
        "NoEndBool = !String4.endsWith(`G`)",
        "ContainedBool = String4.contains(`AA`)",
        "NoContainBool = !String4.contains(`AA`)",
        "FirstChar = String4.substring(0,1)",
        "LengthString = String4.length()",
        "CharIndexPos = ConcatV1.charAt(19)",
        "SubstringEx = ConcatV1.substring(11,20)",
        "FindIt = NewString.indexOf(`_`)",
        "FindItMiddle = NewString.indexOf(`_`, FindIt + 1)",
        "FindLastOf = NewString.lastIndexOf(`_`)",
        "SplitToArrays = NewString.split(`_`)",
        "SplitWithMax = NewString.split(`_`, 2)",
        "SplitIndexPos = NewString.split(`_`)[1]",
        "LowerCase = String4.toLowerCase()",
        "UpperCase = NewString.toUpperCase()",
        "DoubleToStringv1 = Double2 + ``",
        "DoubleToStringv2 = String.valueOf(Double2)",
        "DoubleToStringv3 = Double.toString(Double2)",
        "StringToDoublev1 = Double.valueOf(DoubleToStringv1)",
    ]
)
```

## Use ternaries (if-then)

For details, see [Use the ternary conditional operator in query strings](../../how-to-guides/ternary-if-how-to.md).

```python test-set=1 order=null
if_then_example_1 = static_source_2.update_view(
    ["SimpleTernary = Double2 < 50 ? `smaller` : `bigger`"]
)

if_then_example_2 = static_source_2.update_view(
    ["TwoLayers = Int2 <= 20 ? `small` : Int2 < 50 ? `medium` : `large`"]
)
```

## Create and use custom variables

A query string can use Python variables that are in scope when the query runs. For details, see [Python variables in query strings](../../how-to-guides/python-variables.md).

```python
from deephaven import empty_table

a = 7

result = empty_table(5).update(formulas=["Y = a"])
```

## Create and use a custom function

For details, see [Python functions in query strings](../../how-to-guides/python-functions.md).

```python test-set=1 order=null
def mult(a, b):
    return a * b


py_func_example = static_source_2.update(
    ["MObject = mult(Double2,Int2)", "MDouble = (double)mult(Double2,Int2)"]
)
```

## Cast types

A `(type)` cast converts a value from one type to another and follows Java casting rules, except that a Deephaven null converts to the null value of the target type. You can cast between the following numeric primitive types:

- `byte`
- `short`
- `int`
- `long`
- `float`
- `double`

You can also cast an object, such as the return value of a Python function, to `String` or to a numeric type. Casting is useful when an operation needs more or less precision than the original data type. For details, see [Casting](../query-language/control-flow/casting.md).

### Cast numeric types

This example casts a column of doubles near each type's largest and smallest values to each numeric type. The two metadata tables show the resulting column types.

```python order=numbers_max,numbers_min,numbers_min_meta,numbers_max_meta
from deephaven import new_table
from deephaven.column import double_col

numbers_max = new_table(
    [
        double_col(
            "MaxNumbers",
            [
                (2 - 1 / (2**52)) * (2**1023),
                (2 - 1 / (2**23)) * (2**127),
                (2**63) - 1,
                (2**31) - 1,
                (2**15) - 1,
                (2**7) - 1,
            ],
        )
    ]
).view(
    formulas=[
        "DoubleMax = (double)MaxNumbers",
        "FloatMax = (float)MaxNumbers",
        "LongMax = (long)MaxNumbers",
        "IntMax = (int)MaxNumbers",
        "ShortMax = (short)MaxNumbers",
        "ByteMax = (byte)MaxNumbers",
    ]
)

numbers_min = new_table(
    [
        double_col(
            "MinNumbers",
            [
                1 / (2**1074),
                1 / (2**149),
                -(2**63) + 513,
                -(2**31) + 2,
                -1 * (2**15) + 1,
                -(2**7) + 1,
            ],
        )
    ]
).view(
    formulas=[
        "DoubleMin = (double)MinNumbers",
        "FloatMin = (float)MinNumbers",
        "LongMin = (long)MinNumbers",
        "IntMin = (int)MinNumbers",
        "ShortMin = (short)MinNumbers",
        "ByteMin = (byte)MinNumbers",
    ]
)

numbers_min_meta = numbers_min.meta_table.view(formulas=["Name", "DataType"])
numbers_max_meta = numbers_max.meta_table.view(formulas=["Name", "DataType"])
```

Each row holds a value near one type's limit. When that value is outside a column's type, the cast wraps, saturates, or overflows to infinity, as in Java, and a result equal to the type's null value, such as `Integer.MIN_VALUE` for `int`, displays as null. See [Casting](../query-language/control-flow/casting.md).

### Cast strings

A Python function without type hints returns a Python object. Cast its return value with `(String)` to get a Java `String` column. Java methods such as `String.valueOf` and `Integer.parseInt` convert between numbers and strings. [Manipulate strings](#manipulate-strings) shows more conversions between numbers and strings.

```python test-set=1 order=null
string_casts = static_source_1.update(
    [
        "Letter = (String)rand_choice(letters_upp)",
        "IntAsString = String.valueOf(Int1)",
        "StringAsInt = Integer.parseInt(IntAsString)",
    ]
)
```

## Bin data

Binning groups values into buckets, usually as the first step before an [aggregation](../../how-to-guides/combined-aggregations.md) that downsamples or profiles the data.

```python test-set=1 ticking-table order=null
bins = ticking_source.update_view(
    [
        "PriceBin = upperBin(Double3, 5)",
        "SizeBin = lowerBin(Int3, 10)",
        "TimeBin = upperBin(Timestamp, 10 * SECOND)",
    ]
).reverse()
agg_bin = bins.view(["TimeBin", "Total = Int3 * Double3"]).sum_by(["TimeBin"])
```

## Group and aggregate

See [Group and ungroup data](../../how-to-guides/grouping-data.md) for more details.

### Simple grouping

```python test-set=1 order=group_to_arrays,group_multiple_keys
group_to_arrays = static_source_1.group_by(["String2"])
# one row per key (i.e. String2), all other columns are arrays
group_multiple_keys = static_source_1.group_by(["String1", "Date"])
# one row for each key-combination (i.e. String1-Date pairs)
```

### Ungrouping

[`ungroup`](../table-operations/group-and-aggregate/ungroup.md) expands array columns so that each array element becomes its own row.

```python test-set=1 order=null
# Run these lines together
agg_by_key = static_source_1.group_by(["Date"])
# one row per Date, other fields are arrays from static_source_1
ungroup_that_output = agg_by_key.ungroup()  # no arguments usually
# each array value becomes its own row
# in this case restores the rows of static_source_1, with Date moved to the first column
```

### Aggregations

- [Single aggregation](../../how-to-guides/dedicated-aggregations.md)
- [Multi-aggregation](../../how-to-guides/combined-aggregations.md)

```python test-set=1 order=null
count_of_entire_table = static_source_1.count_by(
    "Count"
)  # single arg returns tot count, you choose name of column output
count_of_group = static_source_1.count_by("N", ["String1"])  # N is naming choice

first_of_group = static_source_1.first_by(["String1"])
last_of_group = static_source_1.last_by(["String1"])

sum_of_group = static_source_1.view(["String1", "Double1"]).sum_by(["String1"])
# non-key field(s) must be numeric or Boolean
avg_of_group = static_source_1.view(["String1", "Double1", "Int1"]).avg_by(["String1"])
std_of_group = static_source_1.view(["String1", "Double1", "Int1"]).std_by(["String1"])
var_of_group = static_source_1.view(["String1", "Double1", "Int1"]).var_by(["String1"])
median_of_group = static_source_1.view(["String1", "Double1", "Int1"]).median_by(
    ["String1"]
)
min_of_group = static_source_1.view(["String1", "Double1", "Int1"]).min_by(["String1"])
max_of_group = static_source_1.view(["String1", "Double1", "Int1"]).max_by(["String1"])

# combine aggregations in a single method (using the same key-grouping)
from deephaven import agg

combined_aggs = static_source_1.agg_by(
    [agg.avg("AvgDouble1 = Double1"), agg.max_("MaxInt1 = Int1")], by=["String1"]
)
```

### Formula aggregations

[`agg.formula`](../table-operations/group-and-aggregate/AggFormula.md) applies a formula to each group. This example averages three columns for each `Letter`-`Color` pair.

```python order=source,result
from deephaven import empty_table
from deephaven import agg

colors = ["Red", "Blue", "Green"]

formulas = [
    "X = 0.1 * i",
    "Y1 = Math.pow(X, 2)",
    "Y2 = Math.sin(X)",
    "Y3 = Math.cos(X)",
]
grouping_cols = ["Letter = (i % 2 == 0) ? `A` : `B`", "Color = (String)colors[i % 3]"]

source = empty_table(40).update(formulas + grouping_cols)

my_agg = [agg.formula(f"AvgY{idx} = avg(Y{idx})") for idx in range(1, 4)]

result = source.agg_by(aggs=my_agg, by=["Letter", "Color"])
```

## Join data

See [Choose the right join method](../../how-to-guides/joins-exact-relational.md#which-method-should-you-use) for more details.

### Common joins

#### Natural join

[`natural_join`](../table-operations/join/natural-join.md) adds columns from the right table to every row of the left table, using at most one matching right row per left row.

- Returns all the rows of the left table, along with up to one matching row from the right table.
- If there is no match in the right table for a given row, nulls appear for that row in the columns from the right table.
- By default, the query fails if there are multiple matches in the right table for a given row. Set `type` to [`NaturalJoinType.FIRST_MATCH` or `NaturalJoinType.LAST_MATCH`](../table-operations/join/natural-join.md) (from `deephaven.table`) to keep the first or last match instead.

```python syntax
result = left_table.natural_join(
    table=right_table, on=columns_to_match, joins=columns_to_add
)
```

```python test-set=1 order=null
# run these together
last_by_key_table = (
    static_source_1.view(["String1", "Int1", "Timestamp"])
    .last_by(["String1"])
    .rename_columns(["Renamed = Int1", "RenamedTime = Timestamp"])
)

# creates 3-column table of String1 + last record of the other 2 cols
join_with_last_price = static_source_1.natural_join(last_by_key_table, ["String1"])
# arguments are (table, on)
# will have same number of rows as static_source_1 (left table)
# brings all non-key columns from last_by_key_table to static_source_1

specify_cols_from_r = static_source_1.natural_join(
    last_by_key_table, ["String1"], ["Renamed"]
)
# nearly identical to join_with_last_price example
# arguments are (table, on, joins)

rename_cols_on_join = static_source_1.natural_join(
    last_by_key_table, ["String1"], ["RenamedAgain = Renamed", "RenamedTime"]
)
# nearly identical to specify_cols_from_r example
# can rename column on the join
# sometimes this is necessary...
# if there would be 2 cols of same name

keys_of_diff_names = static_source_2.view(["String3", "Double2"]).natural_join(
    last_by_key_table, ["String3 = String1"]
)
# note that String3 in the L table is mapped as the key to String1 in R
# every String3 letter also appears in String1, so every row finds a match
```

#### Natural join on multiple keys

```python test-set=1 order=null
# run these together
last_price_two_keys = static_source_1.view(["Date", "String1", "Int1"]).last_by(
    ["Date", "String1"]
)
# creates 3-column table with the last Int1 for each Date-String1 pair
nat_join_two_keys = static_source_1.natural_join(
    last_price_two_keys, on=["Date", "String1"], joins=["LastInt1 = Int1"]
)
# arguments are (table, on=[key1, key2], joins=[cols])
# adds LastInt1, renamed because the left table already has an Int1 column
```

#### As-of joins (`aj` and `raj`)

[As-of joins](../table-operations/join/aj.md) are [time-series joins](../../how-to-guides/joins-timeseries-range.md). For each left-table row, [`aj`](../table-operations/join/aj.md) returns the right-table row whose key matches exactly or, if none does, the closest earlier row. With a timestamp key, that is the row at the same time or the most recent row before it. As-of joins also work on other ordered keys, such as numbers or strings.

```python syntax
result = left_table.aj(table=right_table, on=columns_to_match, joins=columns_to_add)
```

A reverse as-of join, [`raj`](../table-operations/join/raj.md), returns the exact match or, if there is none, the closest row after it. For a timestamp key, that is the row at the same time or the next later row.

```python syntax
result = left_table.raj(table=right_table, on=columns_to_match, joins=columns_to_add)
```

```python test-set=1 ticking-table order=null
from deephaven import time_table

ticking_source_2 = time_table("PT10.5S").update(
    [
        "X = ii",
        "Double4 = randomDouble(1, 100)",
        "Int4 = randomInt(1, 100)",
        "Date = formatDate(Timestamp, timeZone(`ET`))",
        "String7 = (String)rand_choice(letters_upp)",
        "String8 = (String)get_random_string(randomInt(1, 4))",
    ]
)

aj_join = ticking_source_2.aj(
    table=ticking_source,
    on=["Timestamp"],
    joins=["String5", "String6", "JoinTime = Timestamp"],
)

raj_join = ticking_source_2.raj(
    table=ticking_source,
    on=["Timestamp"],
    joins=["String5", "String6", "JoinTime = Timestamp"],
)

raj_join_two_keys = ticking_source_2.raj(
    table=ticking_source,
    on=["String7 = String5", "Timestamp"],
    joins=["String6", "JoinTime = Timestamp"],
)
```

### Less common joins

#### Exact join

[`exact_join`](../table-operations/join/exact-join.md) adds columns from the right table to every row of the left table, and each left row must match exactly one right row.

- Returns all rows of the left table.
- If a row has no matching key, the query fails.
- If a row has multiple matching keys in the right table, the query fails.

```python test-set=1 order=null
last_string_1aug23 = static_source_1.where(["Date = `2017-08-23`"]).last_by(["String1"])
last_string_1aug24 = (
    static_source_1.where(["Date = `2017-08-24`"])
    .last_by(["String1"])
    .where_in(last_string_1aug23, ["String1"])
)
# filters for String1 in last_string_1aug23
exact_join_ex = last_string_1aug24.exact_join(
    last_string_1aug23, ["String1"], ["RenamedDouble = Double1"]
)
```

#### Inner join

[`join`](../table-operations/join/join.md) returns every combination of matching rows from the left and right tables, similar to a SQL inner join.

- Returns only matching rows.
- Multiple matches produce duplicate rows, which can result in a long table.

```python test-set=1 order=null
last_ss_1 = static_source_1.last_by(["String1"]).view(["String1", "Double1"])
last_ss_2 = static_source_2.last_by(["String3"]).view(
    ["String1 = String3", "RenamedDouble = Double2"]
)
first_ss_2 = static_source_2.first_by(["String3"]).view(
    ["String1 = String3", "RenamedDouble = Double2"]
)

from deephaven import merge

merge_first_and_last_ss_2 = merge([first_ss_2, last_ss_2])
# the table has two rows per String1
join_example = last_ss_1.join(merge_first_and_last_ss_2, ["String1"], ["RenamedDouble"])
# note that the resultant set has two rows per String1
```

## Import and export data

Deephaven reads and writes files, such as CSV and Parquet files, and converts tables to and from NumPy arrays and pandas DataFrames.

### Read and write files

The following script reads CSV files from URLs and from the `/data` directory of a Deephaven Docker container, and writes and reads CSV and Parquet files there. For every parameter, see the [`read_csv`](../data-import-export/CSV/readCsv.md), [`write_csv`](../data-import-export/CSV/writeCsv.md), [Parquet `read`](../data-import-export/Parquet/readTable.md), and [Parquet `write`](../data-import-export/Parquet/writeTable.md) reference pages.

```python test-set=1 order=null
# CSV
from deephaven import read_csv

# import CSV from a URL
csv_imported = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv"
)

# import headerless CSV
csv_headerless = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv",
    headless=True,
)

# import headerless CSV then add headings
import deephaven.dtypes as dht

header = {"Year": dht.int64, "Rotten Tom Score": dht.int64, "Title": dht.string}
csv_manual_header = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv",
    header=header,
    headless=True,
)

# !!! WRITE CSV !!!
# export / write CSV to local /data/ directory
from deephaven import write_csv

write_csv(csv_manual_header, "/data/outputFile.csv")

# import the local CSV you just exported
csv_local_read = read_csv("/data/outputFile.csv")

# PARQUET

# write Parquet
from deephaven.parquet import write

table_sample = static_source_1.head(100)
write(table_sample, "/data/output_generic.parquet")

write(table_sample, "/data/output_gzip.parquet", compression_codec_name="GZIP")
# other compression codecs are available, such as "SNAPPY", "ZSTD", and "LZ4_RAW"

# read Parquet
from deephaven.parquet import read

parquet_read = read("/data/output_generic.parquet")
parquet_ok_if_zipped = read("/data/output_gzip.parquet")
```

You can also import files and export any table to a CSV file from the UI.

### Copy column values into a NumPy array

[`to_numpy`](../numpy/to-numpy.md) copies column values into a NumPy array, so you can read a cell by position.

```python test-set=1 order=:log
from deephaven.numpy import to_numpy

# Convert a column to a numpy array for positional index access
column_array = to_numpy(static_source_1, cols=["String1"]).flatten()
print(column_array)

# Access a cell by positional index
cell_value = column_array[0]
print(f"Value at index 0: {cell_value}")
```

### Use pandas

[`to_pandas`](../pandas/to-pandas.md) copies a table into a pandas DataFrame, and [`to_table`](../pandas/to-table.md) converts a DataFrame back into a table.

```python test-set=1 order=:log
import deephaven.pandas as dhpd

pandas_df = dhpd.to_pandas(static_source_1)

# Show that the result is a pandas DataFrame
print(type(pandas_df), pandas_df.dtypes, pandas_df.describe())

# Convert the pandas DataFrame back to a Deephaven table
dh_table_again = dhpd.to_table(pandas_df)
```

## Format tables

### Date-time formatting

`Date(...)` in [`format_columns`](../table-operations/format/format-columns.md) sets how the UI displays a date-time column, using Java date-time pattern letters. The column's data does not change.

```python test-set=1 order=null
time_formatting = (
    static_source_1.view(
        [
            "Time1 = Timestamp",
            "Time2 = Timestamp",
            "Time3 = Timestamp",
            "Time4 = Timestamp",
            "Time5 = Timestamp",
            "Time6 = Timestamp",
        ]
    )
    .format_columns(
        [
            "Time1 = Date(`yyyy_MM_dd'T'HH-mm-ss.S z`)",
            "Time2 = Date(`yyyy-MM-dd'T'HH:mm z`)",
            "Time3 = Date(`HH:mm:ss`)",
            "Time4 = Date(`HH:mm:ss.SSSSSSSSS`)",
            "Time5 = Date(`EEE dd MMM yy HH:mm:ss`)",
            "Time6 = Date(`yyyy-MM-dd`)",
        ]
    )
    .update_view(["Time7String = formatDate(Time6, timeZone(`ET`))"])
)
```

### Number formatting

```python test-set=1 order=null
number_formatting = static_source_1.view(
    [
        "Double1",
        "BigNum1 = Double1 * 1000",
        "BigNum2 = Double1 * 1000",
        "BigNum3 = Double1 * 1000",
        "Price1 = Double1",
        "Price2 = Double1",
        "Price3 = Double1",
        "SmallNum1 = Double1 / 1000",
        "SmallNum2 = Double1 / 1000",
        "SmallNum3 = Double1 / 1000",
        "TinyNum = Double1 / 1_000_000_000",
    ]
).format_columns(
    [
        "BigNum1 = Decimal(`###,#00`)",
        "BigNum2 = Decimal(`##0.00`)",
        "BigNum3 = Decimal(`0.####E0`)",
        "Price1 = Decimal(`###,###.##`)",
        "Price2 = Decimal(`$###,##0.00`)",
        "Price3 = Decimal(`#,##0`)",
        "SmallNum1 = Decimal(`##0.000000`)",
        "SmallNum2 = Decimal(`##0.00%`)",
        "SmallNum3 = Decimal(`##0%`)",
        "TinyNum = Decimal(`0.00E0`)",
    ]
)
```

### Color formatting

```python test-set=1 order=null
# set a background color for an entire table
background_format = static_source_1.format_columns("* = bgfga(colorRGB(0,0,128))")

# set a color column by column
just_colors = static_source_1.format_columns(
    [
        "X = `#90F060`",
        "Double1 = LIGHTBLUE",
        "Int1 = colorRGB(247,204,0)",
        "Timestamp = colorRGB(247,204,204)",
        "Date = bgfg(colorRGB(57,43,128),colorRGB(243,247,122))",
        "String1 = bgfga(MEDIUMVIOLETRED)",
        "String2 = colorHSL(0, 24, 36)",
    ]
)

# column formatting
column_colors = (
    static_source_1.update_view(["RowMod10 = i % 10"])
    .format_column_where("String1", "Int1 > 35", "DEEP_RED")
    .format_column_where("Double1", "Double1 > Double1_[i-1]", "LIMEGREEN")
    .format_columns(
        [
            "Int1 = (X % 5 !=0) ? ORANGE : BLACK",
            "Date = (String1 = `A`) ? colorRGB(253, 31, 203) : colorRGB(171, 254, 42)",
            "RowMod10 = heatmap(RowMod10, 0.0, 9.0, BRIGHT_GREEN, BRIGHT_RED)",
        ]
    )
)

# row formatting
row_colors = static_source_1.format_row_where("Int1 > 45", "PALE_BLUE")

# one can format_columns() for numbers and colors together
number_and_color = static_source_1.format_columns(
    [
        "Double1 = Decimal(`##0.00`)",
        "Double1 = (Double1 > Double1_[i-1]) ? FUCHSIA : GREY",
    ]
)
```

For the full list of stock colors, see [Named colors](../table-operations/format/named-colors.md).

## Use layout hints

[`layout_hints`](../table-operations/format/layout-hints.md) returns a new table that tells the UI how to arrange its columns. This example freezes `Even`, moves `Odd` to the front and `B` to the back, hides `C`, and groups `A` and `E` under `Vowels`.

```python order=source,result
from deephaven import new_table
from deephaven.column import string_col, int_col

source = new_table(
    [
        string_col("A", ["A", "a"]),
        string_col("B", ["B", "b"]),
        string_col("C", ["C", "c"]),
        string_col("D", ["D", "d"]),
        string_col("E", ["E", "e"]),
        string_col("Y", ["Y", "y"]),
        int_col("Even", [2, 4]),
        int_col("Odd", [1, 3]),
    ]
)

result = source.layout_hints(
    freeze=["Even"],
    front=["Odd"],
    back=["B"],
    hide=["C"],
    column_groups=[
        {"name": "Vowels", "children": ["A", "E"]},
    ],
)
```

## Plot programmatically

[Deephaven Express](/core/plotly/docs/) is the recommended choice for most plotting tasks, and the first example below uses it. The remaining examples use the built-in plotting API and show only the basics. For styling, labeling, and more, see the [plotting guide](../../how-to-guides/plotting/api-plotting.md).

### Deephaven Express

[Deephaven Express](/core/plotly/docs/) is a real-time version of Plotly Express.

```python test-set=1 order=null
import deephaven.plot.express as dx

dx_line = dx.line(static_source_1.head(100), x="Timestamp", y="Int1")
```

### Time series plots

```python test-set=1 order=null
from deephaven.plot import Figure

figure = Figure()
time_series = figure.plot_xy(
    series_name="Time_series", t=static_source_1.head(100), x="Timestamp", y="Int1"
).show()

time_series_2axis = (
    figure.plot_xy(
        series_name="Int1",
        t=static_source_1.where(["X < 100", "Int1 < 70"]),
        x="Timestamp",
        y="Int1",
    )
    .x_twin()
    .plot_xy(
        series_name="Double1",
        t=static_source_1.where(["X < 100", "Double1 > 30"]),
        x="Timestamp",
        y="Double1",
    )
    .show()
)

time_series_3axis = (
    figure.plot_xy(
        series_name="Int1", t=static_source_1.where(["X < 25"]), x="Timestamp", y="Int1"
    )
    .x_twin()
    .plot_xy(
        series_name="Double1",
        t=static_source_1.where(["X < 25"]),
        x="Timestamp",
        y="Double1",
    )
    .x_twin()
    .plot_xy(
        series_name="DoubleN",
        t=static_source_1.where(["X < 25"]).update(["DoubleN = pow(Double1,2)"]),
        x="Timestamp",
        y="DoubleN",
    )
    .show()
)
```

### Bar chart

```python test-set=1 order=null
from deephaven.plot import Figure, PlotStyle

figure = Figure()
bar_chart = (
    figure.axes(plot_style=PlotStyle.BAR)
    .plot_xy(
        series_name="Int1", t=static_source_1.where("X < 41"), x="Timestamp", y="Int1"
    )
    .show()
)
```

### Plot by key

Passing grouping columns plots one series for each key.

```python test-set=1 order=null
from deephaven.plot import Figure

table_3_keys = (
    static_source_1.head(2000)
    .where("String1 in `A`, `B`, `C`")
    .sort("Int1")
    .update(
        ["NewRow = i", "Y = Int1 * (String1 = `A` ? 2 : String1 = `B` ? 0.5 : 1.0)"]
    )
)
p = (
    Figure()
    .plot_xy(
        series_name="partitioned", t=table_3_keys, by=["String1"], x="NewRow", y="Y"
    )
    .show()
)
```

### Histogram

```python test-set=1 order=null
from deephaven.plot import Figure

figure = Figure()
histogram = (
    figure.plot_xy_hist(
        series_name="Histogram Values",
        t=static_source_1.where("X < 41"),
        x="Int1",
        nbins=10,
    )
    .chart_title(title="Histogram of Values")
    .show()
)
```

### Area graph

```python test-set=1 order=null
from deephaven.plot import PlotStyle, Figure

figure = Figure()
area_graph = (
    figure.axes(plot_style=PlotStyle.AREA)
    .plot_xy(
        series_name="Int_col",
        t=static_source_1.where("X < 41"),
        x="Timestamp",
        y="Int1",
    )
    .show()
)
```

### Stacked area

```python test-set=1 order=null
from deephaven.plot import PlotStyle, Figure

figure = Figure()
stacked_area_graph = (
    figure.axes(plot_style=PlotStyle.STACKED_AREA)
    .plot_xy(
        series_name="Series_1",
        t=static_source_1.where(["X < 1000", "String1 = `A`"]),
        x="Timestamp",
        y="Double1",
    )
    .plot_xy(
        series_name="Series_2",
        t=static_source_1.where(["X < 1000", "String1 = `B`"]),
        x="Timestamp",
        y="Double1",
    )
    .show()
)
```

### Scatter

```python test-set=1 order=null
from deephaven.plot import Figure, PlotStyle, Colors, Shape

figure = Figure()
plot_xy_scatter = (
    figure.plot_xy(
        series_name="Double1", t=static_source_1.head(50), x="Timestamp", y="Double1"
    )
    .axes(plot_style=PlotStyle.SCATTER)
    .point(shape=Shape.SQUARE, size=10, label="Big Point", color=Colors.RED)
    .x_twin()
    .axes(plot_style=PlotStyle.SCATTER)
    .plot_xy(series_name="ii", t=static_source_1.head(50), x="Timestamp", y="X")
    .point(shape=Shape.DIAMOND, size=16, label="Big Diamond", color=Colors.BLUE)
    .show()
)
```

## Work with ticking tables

### Wait for table updates

Use [`await_update`](../table-operations/table-listeners/await-update.md) to wait until a table updates before continuing.

```python ticking-table order=null
from deephaven import time_table

source = time_table("PT1S")

print(
    source.await_update(0)
)  # 0 ms timeout: returns False without waiting for an update
print(source.await_update(1000))  # waits up to 1000 ms; True if the table updated

result = source.update(formulas=["RenamedTimestamp = Timestamp"])
```

### Convert ticking tables to static tables

Use [`snapshot`](../table-operations/snapshot/snapshot.md) to create a static, in-memory copy of a source table.

```python ticking-table order=null
from deephaven import time_table

source = time_table("PT1S")

# Some time later...
result = source.snapshot()
```

### Reduce ticking frequency

[`snapshot_when`](../table-operations/snapshot/snapshot-when.md) takes a snapshot of the source table each time a trigger table ticks, so the result updates only as often as the trigger. The result stays empty until the trigger table first ticks, unless you pass `initial=True`.

```python ticking-table order=null
from deephaven import time_table
import random

source = time_table("PT0.5S").update(
    formulas=["X = (int) random.randint(0, 100)", "Y = sqrt(X)"]
)
trigger = time_table("PT5S").rename_columns(cols=["TriggerTimestamp = Timestamp"])

result = source.snapshot_when(trigger_table=trigger)
```

### Capture the history of ticking tables

Pass `history=True` to [`snapshot_when`](../table-operations/snapshot/snapshot-when.md) to append a full snapshot of the source table's rows each time the trigger table ticks, instead of replacing the previous snapshot. The result keeps the history of the ticking table.

```python ticking-table order=null
from deephaven import time_table
import random

source = (
    time_table("PT0.01S")
    .update(
        formulas=[
            "X = i%2 == 0 ? `A` : `B`",
            "Y = (int) random.randint(0, 100)",
            "Z = sqrt(Y)",
        ]
    )
    .last_by(by=["X"])
)

trigger = time_table("PT1S").rename_columns(cols=["TriggerTimestamp = Timestamp"])
result = source.snapshot_when(trigger_table=trigger, history=True)
```

## Related documentation

- [Work with time](../../conceptual/time-in-deephaven.md)
- [Deephaven's table types](../../conceptual/table-types.md)
- [Filter table data](../../how-to-guides/use-filters.md)
- [Select and create columns](../../how-to-guides/use-select-view-update.md)
- [Strings](../../how-to-guides/work-with-strings.md)
- [Use the ternary conditional operator in query strings](../../how-to-guides/ternary-if-how-to.md)
- [Group and ungroup data](../../how-to-guides/grouping-data.md)
- [Single aggregation](../../how-to-guides/dedicated-aggregations.md)
- [Multi-aggregation](../../how-to-guides/combined-aggregations.md)
- [Rolling aggregations](../../how-to-guides/rolling-aggregations.md)
- [Exact and relational joins](../../how-to-guides/joins-exact-relational.md)
- [Inexact, time-series, and range joins](../../how-to-guides/joins-timeseries-range.md)
- [Work with calendars](../../how-to-guides/business-calendar.md)
- [Python variables in query strings](../../how-to-guides/python-variables.md)
- [Python functions in query strings](../../how-to-guides/python-functions.md)
- [Convert between Deephaven tables and pandas DataFrames](../../how-to-guides/use-pandas.md)
- [Apply color formatting to columns](../../how-to-guides/format-columns.md)
- [Deephaven Express](/core/plotly/docs/)
- [Built-in plotting API](../../how-to-guides/plotting/api-plotting.md)
- [Write data to an in-memory, real-time table](../../how-to-guides/table-publisher.md)
- [Read CSV or other delimited files](../../how-to-guides/data-import-export/csv-import.md)
- [Read Parquet files into Deephaven tables](../../how-to-guides/data-import-export/parquet-import.md)
- [CSV cheat sheet](./csv.md)
- [Parquet cheat sheet](./parquet.md)
- [Simple table constructors](./simple-table-constructors.md)
- [Time operations cheat sheet](./time-cheat-sheet.md)
- [`agg_by`](../table-operations/group-and-aggregate/aggBy.md)
- [`agg.avg`](../table-operations/group-and-aggregate/AggAvg.md)
- [`agg.formula`](../table-operations/group-and-aggregate/AggFormula.md)
- [`agg.max_`](../table-operations/group-and-aggregate/AggMax.md)
- [`aj`](../table-operations/join/aj.md)
- [Arrays](../query-language/types/arrays.md)
- [`avg_by`](../table-operations/group-and-aggregate/avgBy.md)
- [`await_update`](../table-operations/table-listeners/await-update.md)
- [Casting](../query-language/control-flow/casting.md)
- [`count_by`](../table-operations/group-and-aggregate/countBy.md)
- [`cum_sum`](../table-operations/update-by-operations/cum-sum.md)
- [`drop_columns`](../table-operations/select/drop-columns.md)
- [`double_col`](../table-operations/create/doubleCol.md)
- [`DynamicTableWriter`](../table-operations/create/DynamicTableWriter.md)
- [`ema_time`](../table-operations/update-by-operations/ema-time.md)
- [`empty_table`](../table-operations/create/emptyTable.md)
- [`exact_join`](../table-operations/join/exact-join.md)
- [`first_by`](../table-operations/group-and-aggregate/firstBy.md)
- [`format_column_where`](../table-operations/format/format-column-where.md)
- [`format_columns`](../table-operations/format/format-columns.md)
- [`format_row_where`](../table-operations/format/format-row-where.md)
- [`group_by`](../table-operations/group-and-aggregate/groupBy.md)
- [`head`](../table-operations/filter/head.md)
- [`head_by`](../table-operations/group-and-aggregate/headBy.md)
- [`head_pct`](../table-operations/filter/head-pct.md)
- [`input_table`](../table-operations/create/input-table.md)
- [`int_col`](../table-operations/create/intCol.md)
- [`join`](../table-operations/join/join.md)
- [`last_by`](../table-operations/group-and-aggregate/lastBy.md)
- [`layout_hints`](../table-operations/format/layout-hints.md)
- [`lazy_update`](../table-operations/select/lazy-update.md)
- [`max_by`](../table-operations/group-and-aggregate/maxBy.md)
- [`median_by`](../table-operations/group-and-aggregate/medianBy.md)
- [`merge`](../table-operations/merge/merge.md)
- [`merge_sorted`](../table-operations/merge/merge-sorted.md)
- [`meta_table`](../table-operations/metadata/meta_table.md)
- [`min_by`](../table-operations/group-and-aggregate/minBy.md)
- [`move_columns`](../table-operations/select/move-columns.md)
- [`move_columns_up`](../table-operations/select/move-columns-up.md)
- [Named colors](../table-operations/format/named-colors.md)
- [`natural_join`](../table-operations/join/natural-join.md)
- [`new_table`](../table-operations/create/newTable.md)
- [Parquet `read`](../data-import-export/Parquet/readTable.md)
- [Parquet `write`](../data-import-export/Parquet/writeTable.md)
- [`plot_xy`](../plot/plot.md)
- [`plot_xy_hist`](../plot/histPlot.md)
- [`plot_style`](../plot/plotStyle.md)
- [`raj`](../table-operations/join/raj.md)
- [`read_csv`](../data-import-export/CSV/readCsv.md)
- [`rename_columns`](../table-operations/select/rename-columns.md)
- [`reverse`](../table-operations/sort/reverse.md)
- [`ring_table`](../table-operations/create/ringTable.md)
- [`rolling_sum_tick`](../table-operations/update-by-operations/rolling-sum-tick.md)
- [`rollup`](../table-operations/create/rollup.md)
- [`select`](../table-operations/select/select.md)
- [`select_distinct`](../table-operations/select/select-distinct.md)
- [`set_calendar`](../time/calendar/set-calendar.md)
- [`snapshot`](../table-operations/snapshot/snapshot.md)
- [`snapshot_when`](../table-operations/snapshot/snapshot-when.md)
- [`sort`](../table-operations/sort/sort.md)
- [`sort_descending`](../table-operations/sort/sort-descending.md)
- [Special variables](../query-language/variables/special-variables.md)
- [`std_by`](../table-operations/group-and-aggregate/stdBy.md)
- [`string_col`](../table-operations/create/stringCol.md)
- [`sum_by`](../table-operations/group-and-aggregate/sumBy.md)
- [`tail`](../table-operations/filter/tail.md)
- [`tail_by`](../table-operations/group-and-aggregate/tailBy.md)
- [`tail_pct`](../table-operations/filter/tail-pct.md)
- [`table_publisher`](../table-operations/create/TablePublisher.md)
- [`time_table`](../table-operations/create/timeTable.md)
- [`to_j_instant`](../time/datetime/to_j_instant.md)
- [`to_numpy`](../numpy/to-numpy.md)
- [`to_pandas`](../pandas/to-pandas.md)
- [`to_table`](../pandas/to-table.md)
- [`tree`](../table-operations/create/tree.md)
- [`ungroup`](../table-operations/group-and-aggregate/ungroup.md)
- [`update`](../table-operations/select/update.md)
- [`update_by`](../table-operations/update-by-operations/updateBy.md)
- [`update_view`](../table-operations/select/update-view.md)
- [`var_by`](../table-operations/group-and-aggregate/varBy.md)
- [`view`](../table-operations/select/view.md)
- [`where`](../table-operations/filter/where.md)
- [`where_in`](../table-operations/filter/where-in.md)
- [`where_not_in`](../table-operations/filter/where-not-in.md)
- [`where_one_of`](../table-operations/filter/where-one-of.md)
- [`write_csv`](../data-import-export/CSV/writeCsv.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table)
