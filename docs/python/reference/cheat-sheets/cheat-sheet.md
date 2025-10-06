---
title: Ultimate cheat sheet
sidebar_label: Ultimate cheat sheet
---

## Necessary data

The named tables in these lines are used throughout this page and should be run first.

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
        "Date = Timestamp.toString()",
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
        "Date = Timestamp.toString()",
        "String5 = (java.lang.String)rand_choice(letters_upp)",
        "String6 = (java.lang.String)get_random_string(randomInt(1, 4))",
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

Columns are created using the following methods:

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

The following code makes a `time_table` that updates every second.

```python ticking-table order=null
from deephaven import time_table

result = time_table("PT1S")
```

### Input tables

Use an [`input_table`](../table-operations/create/input-table.md) when you would like to add or modify data in table cells directly.

```python order=my_input_table
from deephaven import input_table
from deephaven import dtypes as dht

my_column_defs = {"StringCol": dht.string, "DoubleCol": dht.double}

my_input_table = input_table(col_defs=my_column_defs, key_cols="StringCol")
```

### Ring tables

A [`ring_table`](../table-operations/create/ringTable.md) is a table that contains the `n` most recent rows from a source table. If the table is non-static, rows outside of `n` will disappear from the ring table as the source table updates.

```python order=tt,rt
from deephaven import time_table, ring_table

tt = time_table("PT1S")
rt = ring_table(tt, 5)
```

### Tree tables

A [`tree_table`](../table-operations/create/tree.md) has collapsible subsections that can be be expanded in the UI for more detail.

```python order=source,result
from deephaven.constants import NULL_INT
from deephaven import empty_table

source = empty_table(100).update_view(
    ["ID = i", "Parent = i == 0 ? NULL_INT : (int)(i / 4)"]
)

result = source.tree(id_col="ID", parent_col="Parent")
```

### Rollup tables

A [`rollup`](../table-operations/create/rollup.md) table "rolls" several child specifications into one parent specification:

```python order=insurance,test_rollup,insurance_rollup
from deephaven import read_csv, agg

insurance = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv"
)

agg_list = [agg.avg(cols=["bmi", "expenses"])]
by_list = ["region", "age"]

test_rollup = insurance.rollup(aggs=None, by=by_list, include_constituents=True)
insurance_rollup = insurance.rollup(
    aggs=agg_list, by=by_list, include_constituents=True
)
```

## Merge tables

To [`merge`](../table-operations/merge/merge.md) tables, the schema must be identical: same column names, same column data types. This applies to both static and updating tables.

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

## View table metadata

[`meta_table`](../table-operations/metadata/meta_table.md) shows the column names, data types, partitions, and groups for the table. It is useful to make sure the schema matches before merging.

```python test-set=1 order=null
static_1_meta = static_source_1.meta_table
```

## Filter

Most queries benefit by starting with filters. Less data generally means better performance.

For SQL developers: in Deephaven, Joins are not a primary operation for filtering. Use [`where`](../table-operations/filter/where.md), [`where_in`](../table-operations/filter/where-in.md), and [`where_not_in`](../table-operations/filter/where-not-in.md).

> [!NOTE]
> Backticks `\` in query strings denote a string within it. Single quotes``'` denote a literal value that gets parsed by the engine.

### Date & Time examples

```python test-set=1 ticking-table order=null
from deephaven.time import to_j_instant

todays_data_1 = ticking_source.where(
    ["Date = calendarDate()"]
)  # Date for this is a String
todays_data_3 = ticking_source.where(
    ["Date = today('Australia/Sydney')"]  # note singe quotes used to denote a literal
)  # sydney timezone

single_string_date = static_source_1.where(["Date = `2017-01-03`"])  # HEAVILY USED!
less_than_date = static_source_1.where(["Date < `2017-01-03`"])  # use >=, etc. as well
one_month_string = static_source_1.where(
    ["Date.startsWith(`2017-02`)"]
)  # note backticks used to denote a string within a string

single_db_date = static_source_2.where(
    ["formatDate(Timestamp, timeZone(`ET`)) = `2020-03-01`"]
)
after_db_datetime = static_source_1.where(["Timestamp > '2017-01-05T10:30:00 ET'"])

just_biz_time = static_source_1.where(["isBusinessTime(Timestamp)"])  # HEAVILY USED!
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

# You can also parse string literals as date-time literals with the single quote (')
time1 = "2017-08-21T09:45:00 ET"
time2 = "2017-08-21T12:10:00 ET"
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

> [!CAUTION]
> Using `i` and `ii` is not a good idea in non-static use cases, as calculations based on these variables aren't stable.

```python test-set=1 order=null
equals_example = static_source_2.where(["round(Int2) = 44"])
less_than_example = static_source_2.where(["Double2 < 8.42"])
some_manipulation = static_source_2.where(["(Double2 - Int2) / Int2 > 0.05"])
mod_example_1 = static_source_2.where(["i % 10 = 0"])  # every 10th row
mod_example_2 = static_source_2.where(["String4.length() % 2 = 0"])
# even char-count Tickers
```

### Multiple filters

```python test-set=1 order=null
conjunctive_comma = static_source_1.where(["Date = `2017-08-23`", "String1 = `A`"])
# HEAVILY USED!
conjunctive_ampersand = static_source_1.where(["Date = `2017-08-23` && String1 = `A`"])

disjunctive_same_col = static_source_1.where(
    ["Date = `2017-08-23`|| Date = `2017-08-25`"]
)
disjunctive_diff_col = static_source_1.where(["Date = `2017-08-23` || String1 = `A`"])

range_lame_way = static_source_1.where(["Date >= `2017-08-21`", "Date <= `2017-08-23`"])
in_range_best = static_source_1.where(["inRange(Date, `2017-08-21`, `2017-08-23`)"])
# HEAVILY USED!

in_range_best_string = static_source_1.where(
    "inRange(String2.substring(0,1), `A`, `D`)"
)
# starts with letters A - D
```

### where

> [!TIP]
> For SQL developers: In Deephaven, filter your data before joining using [`where`](../table-operations/filter/where.md) operations. Deephaven is optimized for filtering rather than matching.

```python order=source,result_single_filter,result_or,result_and
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

result_single_filter = source.where(filters=["Color = `blue`"])
result_or = source.where_one_of(
    filters=["Color = `blue`", "Number > 2"]
)  # OR operation - result will have _either_ criteria
result_and = source.where(
    filters=["Color = `blue`", "Number > 2"]
)  # AND operation - result will have _both_ criteria
```

### where_in/where_not_in

```python test-set=1 order=null
# run these two queries together
string_set_driver = static_source_1.rename_columns(cols=["String4 = String2"]).head(10)
where_in_example_1 = static_source_2.where_in(
    string_set_driver, ["String4"]
)  # HEAVILY USED!

where_in_example_2 = static_source_2.where_in(static_source_1, ["String4 = String2"])
# Ticker in static_source_2 within list of USym of static_source_1

where_not_in_example = static_source_2.where_not_in(
    static_source_1, ["String4 = String2"]
)
# see the "not"
```

### Nulls and NaNs

```python test-set=1 order=null
null_example = static_source_1.where(["isNull(Int1)"])  # all data types supported
not_null_example = static_source_1.where(["!isNull(Int1)"])

nan_example = static_source_1.where(["isNaN(Int1)"])
not_nan_example = static_source_1.where(["!isNaN(Int1)"])
```

### Head and Tail

Used to reduce the number of rows:

- [`tail`](../table-operations/filter/tail.md)
- [`tail_pct`](../table-operations/filter/tail-pct.md)
- [`head`](../table-operations/filter/head.md)
- [`head_pct`](../table-operations/filter/head-pct.md)

```python test-set=1 order=null
first_10k_rows = static_source_2.head(10_000)
ticking_last_rows = ticking_source.tail(100)

first_15_percent = ticking_source.reverse().head_pct(0.15)  # the first 15% of rows
last_15_percent = static_source_2.tail_pct(0.15)  # the last 15% of rows

first_10_by_key = static_source_1.head_by(
    10, ["String1"]
)  # first 10 rows for each String1-key
last_10_by_key = static_source_1.tail_by(
    10, ["String1"]
)  # last 10 rows for each String1-key
```

## Sort

Single direction sorting:

- [`sort`](../table-operations/sort/sort.md)
- [`sortDescending`](../table-operations/sort/sort-descending.md)

Sort on multiple column or directions:

- [`sort(sortColumns)`](../table-operations/sort/sort.md)

Reverse the order of rows in a table:

- [`reverse`](../table-operations/sort/reverse.md)

```python test-set=1 order=null
# works for all data types

sort_time_v1 = static_source_1.sort(order_by=["Timestamp"])  # sort ascending
sort_time_v2 = static_source_1.sort("Double1")

sort_numbers = static_source_1.sort_descending(["Int1"])  # sort descending
sort_strings = static_source_1.sort_descending(["String2"])

from deephaven import SortDirection

sort_time_v1 = static_source_1.sort_descending(
    order_by=["String1", "Timestamp"]
)  # multiple
sort_multi = static_source_1.sort(["Int1", "String1"])

sort_multiple_col = [SortDirection.ASCENDING, SortDirection.DESCENDING]

multi_sort = static_source_1.sort(
    ["Int1", "Double1"], sort_multiple_col
)  # Int ascending then Double descending
```

> [!TIP]
> Reversing tables is faster than sorting, and often used in UIs for seeing appending rows at top of table.

```python syntax
reverse_table = static_source_1.reverse()
reverse_updating = ticking_source.reverse()
```

## Select and create new columns

### Option 1: Choose and add new columns - Calculate and write to memory

Use [`select`](../table-operations/select/select.md) and [`update`](../table-operations/select/update.md) when data is expensive to calculate or accessed frequently. Results are saved in RAM for faster access, but they take more memory.

```python test-set=1 order=null
select_columns = static_source_2.select(["Timestamp", "Int2", "Double2", "String3"])
# constrain to only those 4 columns, write to memory
select_add_col = static_source_2.select(
    ["Timestamp", "Int2", "Double2", "String3", "Difference = Double2 - Int2"]
)
# constrain and add a new column calculating

select_and_update_col = static_source_2.select(
    ["Timestamp", "Int2", "Double2", "String3"]
).update("difference = Double2 - Int2")
# add a new column calculating - logically equivalent to previous example
```

### Option 2: Choose and add new columns - Reference a formula and calculate on the fly

Use [`view`](../table-operations/select/view.md) and [`update_view`](../table-operations/select/update-view.md) when formula is quick or only a portion of the data is used at a time. Minimizes RAM used.

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

### Option 3: Add new columns - Reference a formula and calculate on the fly

Use [`lazy_update`](../table-operations/select/lazy-update.md) when there are a relatively small number of unique values. On-demand formula results are stored in cache and re-used.

```python test-set=1 order=null
lazy_update_example = static_source_2.lazy_update(
    ["Timestamp", "Int2", "Double2", "String3", "Difference = Double2 - Int2"]
)
```

### Get the row number

```python test-set=1 order=null
get_row_num = static_source_2.update_view(["RowNum_int = i", "RowNum_long = ii"])
```

## Do math

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
        "Exp = Double2 * 1E2",
        "Log = Double2 * log(2.0)",
        "Round = round(Double2)",
        "Floor = floor(Double2)",
        "Mod = RandomInt100 % 5",
        "CastInt = (int)AbsDlrVolume",
        "CastLong = (long)AbsDlrVolume",
    ]
)
```

## Handle arrays

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
        "InArray = in(45.71, SubArray)",
    ]
)
```

## Create a slice or sub-vector

```python test-set=1 order=null
slice_and_rolling = static_source_2.update(
    formulas=[
        "SubVector = Int2_.subVector(i-2,i+1)",
        "RollingMean = avg(SubVector)",
        "RollingMedian =median(SubVector)",
        "RollingSum = sum(SubVector)",
    ]
)
```

## Manipulate time and calendars

```python test-set=1 order=null
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
        "IsBizTime = isBusinessDay(Timestamp)",
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

## Bin data

Binning is a great pre-step for aggregating to support the down-sampling or other profiling of data.

```python test-set=1 order=null
bins = ticking_source.update_view(
    [
        "PriceBin = upperBin(Double3, 5)",
        "SizeBin = lowerBin(Int3, 10)",
        "TimeBin = upperBin(Timestamp, 200)",
    ]
).reverse()
agg_bin = bins.view(["TimeBin", "Total = Int3 * Double3"]).sum_by(["TimeBin"])
```

## Manipulate strings

You can use any of the standard Java String operators in your queries, as the following examples show:

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

```python order=source,one_str_match,str_set_match,case_insensitive,not_in_example,contains_example,not_contains_example,starts_with_example,ends_with_example
from deephaven import new_table
from deephaven.column import string_col

source = new_table(
    [
        string_col("Letter", ["A", "a", "B", "b", "C", "c", "AA", "Aa", "bB", "C"]),
    ]
)

one_str_match = source.where(filters=["Letter = `A`"])  # match filter
str_set_match = source.where(filters=["Letter in `A`, `B`, `C`"])
case_insensitive = source.where(filters=["Letter icase in `a`, `B`, `c`"])
not_in_example = source.where(filters=["Letter not in `A`, `B`, `C`"])  # see "not"
contains_example = source.where(filters=["Letter.contains(`A`)"])
not_contains_example = source.where(filters=["!Letter.contains(`AA`)"])
starts_with_example = source.where(filters=["Letter.startsWith(`A`)"])
ends_with_example = source.where(filters=["Letter.endsWith(`C`)"])
```

## Use Ternaries (If-Thens)

```python test-set=1 order=null
if_then_example_1 = static_source_2.update_view(
    ["SimpleTernary = Double2 < 50 ? `smaller` : `bigger`"]
)

if_then_example_2 = static_source_2.update_view(
    ["TwoLayers = Int2 <= 20 ? `small` : Int2 < 50 ? `medium` : `large`"]
)
```

## Create and use custom variables

```python
from deephaven import empty_table

a = 7

result = empty_table(5).update(formulas=["Y = a"])
```

## Create and use a custom function

See our guides:

- [Python functions in query strings](../../how-to-guides/python-functions.md)

```python test-set=1 order=null
## Create and Use a custom function
def mult(a, b):
    return a * b


py_func_example = static_source_2.update(
    ["M_object = mult(Double2,Int2)", "M_double = (double)mult(Double2,Int2)"]
)
```

## Type casting

`(type)` casting is used to cast from one numeric primitive type handled by Java to another.

- byte
- short
- int
- long
- float
- double

This is useful when working with operations that require more or less precision than the pre-cast data type.
See [casting](../query-language/control-flow/casting.md).

### Cast numeric types

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
        "ShortMin = (short)MinNumbers ",
        "ByteMin = (byte)MinNumbers ",
    ]
)

numbers_min_meta = numbers_min.meta_table.view(formulas=["Name", "DataType"])
numbers_max_meta = numbers_max.meta_table.view(formulas=["Name", "DataType"])
```

### Casting strings

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

myagg = [
    agg.formula(
        formula="avg(k)",
        formula_param="k",
        cols=[f"AvgY{idx} = Y{idx}" for idx in range(1, 4)],
    )
]

result = source.agg_by(aggs=myagg, by=["Letter", "Color"])
```

## Manipulate columns

```python test-set=1 order=null
unique_values = static_source_2.select_distinct("String4")  # show unique set
# works on all data types - careful with doubles, longs
unique_values_combo = static_source_2.select_distinct(["Timestamp", "String4"])
# unique combinations of Time with String4

rename_stuff = static_source_2.rename_columns(["Symbol = String4", "Price = Double2"])
drop_column = static_source_2.drop_columns(["X"])  # same for drop one or many
drop_columns = static_source_2.drop_columns(["X", "Int2", "String3"])

put_cols_at_start = static_source_2.move_columns_up(
    ["String4", "Int2"]
)  # Makes 1st col(s)
put_cols_wherever = static_source_2.move_columns(1, ["String4", "Int2"])
# makes String4 the 2nd and Int2 the 3rd column
```

## Group and aggregate

See [How to group and ungroup data](../../how-to-guides/grouping-data.md) for more details.

<!--TODO: add group -->

### Simple grouping

```python test-set=1 order=group_to_arrays,group_multiple_keys
group_to_arrays = static_source_1.group_by(["String2"])
# one row per key (i.e. String2), all other columns are arrays
group_multiple_keys = static_source_1.group_by(["String1", "Date"])
# one row for each key-combination (i.e. String1-Date pairs)
```

### Ungrouping

Expands each row so that each value in any array inside that row becomes itself a new row.

```python test-set=1 order=null
# Run these lines together
agg_by_key = static_source_1.group_by(["Date"])
# one row per Date, other fields are arrays from static_source_1
ungroup_that_output = agg_by_key.ungroup()  # no arguments usually
# each array value becomes its own row
# in this case turns aggByDatetimeKey back into static_source_1
```

### Aggregations

- [How to perform dedicated aggregations for groups](../../how-to-guides/dedicated-aggregations.md)
- [How to perform multiple aggregations for groups](../../how-to-guides/combined-aggregations.md)

```python test-set=1 order=null
count_of_entire_table = static_source_1.count_by(
    "Count"
)  # single arg returns tot count, you choose name of column output
count_of_group = static_source_1.count_by("N", ["String1"])  # N is naming choice

first_of_group = static_source_1.first_by(["String1"])
last_of_group = static_source_1.last_by(["String1"])

sum_of_group = static_source_1.view(["String1", "Double1"]).sum_by(["String1"])
# non-key field(s) must be numerical
avg_of_group = static_source_1.view(["String1", "Double1", "Int1"]).avg_by(["String1"])
std_of_group = static_source_1.view(["String1", "Double1", "Int1"]).std_by(["String1"])
var_of_group = static_source_1.view(["String1", "Double1", "Int1"]).var_by(["String1"])
median_of_group = static_source_1.view(["String1", "Double1", "Int1"]).median_by(
    ["String1"]
)
min_of_group = static_source_1.view(["String1", "Double1", "Int1"]).min_by(["String1"])
max_of_group = static_source_1.view(["String1", "Double1", "Int1"]).max_by(["String1"])
# combine aggregations in a single method (using the same key-grouping)
```

## Join data

See [Choose the right join method](../../how-to-guides/joins-exact-relational.md#which-method-should-you-use) for more details.

> [!TIP]
> For SQL developers: in Deephaven, joins are normally used to enrich a data set, not filter. Use [`where`](../table-operations/filter/where.md) to filter your data instead of using a join.

### Joins that get used a lot

#### Natural Join

[`natural_join`](../table-operations/join/natural-join.md)

- Returns all the rows of the left table, along with up to one matching row from the right table.
- If there is no match in the right table for a given row, nulls will appear for that row in the columns from the right table.
- If there are multiple matches in the right table for a given row, the query will fail.

`left_table.natural_join(rightTable, columnsToMatch, columnsToAdd)`

> [!IMPORTANT]
> The right table of the join needs to have only one match based on the key(s).

```python test-set=1 order=null
# run these together
last_by_key_table = (
    static_source_1.view(["String1", "Int1", "Timestamp"])
    .last_by(["String1"])
    .rename_columns(["Renamed = Int1", "RenamedTime = Timestamp"])
)

# creates 3-column table of String1 + last record of the other 2 cols
join_with_last_price = static_source_1.natural_join(last_by_key_table, ["String1"])
# arguments are (rightTableOfJoin, "JoinKey(s)")
# will have same number of rows as static_source_1 (left table)
# brings all non-key columns from last_price_table to static_source_1
# HEAVILY USED!

specify_cols_from_r = static_source_1.natural_join(
    last_by_key_table, ["String1"], ["Renamed"]
)
# nearly identical to joinWithLastPrice example
# args are (rightTableOfJoin, "JoinKey(s)", "Cols from R table")

rename_cols_on_join = static_source_1.natural_join(
    last_by_key_table, ["String1"], ["RenamedAgain = Renamed, RenamedTime"]
)
# nearly identical to specify_cols_from_r example
# can rename column on the join
# sometimes this is necessaryâ€¦
# if there would be 2 cols of same name
# Note the placement of quotes

keys_of_diff_names = static_source_2.view(["String3", "Double2"]).natural_join(
    last_by_key_table, ["String3 = String1"]
)
# note that String2 in the L table is mapped as the key to String1 in R
# this result has many null records, as there are many String2
# without matching String1
```

#### Multiple Keys

```python test-set=1 order=null
# run these together
last_price_two_keys = static_source_1.view(["Date", "String1", "Int1"]).last_by(
    ["Date", "String1"]
)
# creates 3-column table of LastDouble for each Date-String1 pair
nat_join_two_keys = static_source_1.natural_join(
    last_price_two_keys, ["Date, String1", "Int1"]
)
# arguments are (rightTableOfJoin, "JoinKey1, JoinKey2", "Col(s)")
# Note the placement of quotes
```

#### AJ (As-Of Join)

[As-of joins](../table-operations/join/aj.md) are time series joins. They can also be used with other ordered numerics as the join key(s). It is often wise to make sure the Right-table is sorted (based on the key). `aj` is designed to find "the exact match" of the key or "the record just before". For timestamp aj-keys, this means "that time or the record just before".

`left_table = right_table.aj(columnsToMatch, columnsToAdd)`

[Reverse As-of joins](../table-operations/join/raj.md) `raj` find "the exact match" of the key or "the record just after". For timestamp reverse aj-keys, this means "that time or the record just after".

`result = left_table.raj(right_table, columnsToMatch, columnsToAdd)`

```python test-set=1 order=null
from deephaven import time_table

ticking_source_2 = time_table("PT10.5S").update(
    [
        "X = ii",
        "Double4 = randomDouble(1, 100)",
        "Int4 = randomInt(1, 100)",
        "Date = Timestamp.toString()",
        "String7 = (java.lang.String)rand_choice(letters_upp)",
        "String8 = (java.lang.String)get_random_string(randomInt(1, 4))",
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

raj_join = ticking_source_2.raj(
    table=ticking_source,
    on=["String7 = String5", "Timestamp"],
    joins=["String6", "JoinTime = Timestamp"],
)
```

### Less common joins

[`join`](../table-operations/join/join.md) returns all rows that match between the left and right tables, potentially with duplicates. Similar to SQL inner join.

- Returns only matching rows.
- Multiple matches will have duplicate values, which can result in a long table.

[`exact_join`](../table-operations/join/exact-join.md)

- Returns all rows of `left_table`.
- If there are no matching keys, the result will fail.
- Multiple matches will fail.

```python test-set=1 order=null
# exact joins require precisely one match in the right table for each key
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


# join will find all matches from left in the right table
# if a match is not found, that row is omitted from the result
# if the Right-table has multiple matches, then multiple rows are included in result
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

## Use columns as arrays and cells as variables

```python test-set=1
get_a_column = static_source_1.j_object.getColumnSource("String1")
print(get_a_column)
get_a_cell = get_a_column.get(0)
print(get_a_cell)
```

## Read and write files

It is very easy to import files and to export any table to a CSV via the UI.

CSVs and Parquet files imported from a server-side directory should be done via the script below - these are NOT working examples. The code below provides syntax, but the filepaths are unknown.

```python skip-test
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

# full syntax
# csv_full_monty = read_csv(path: str,
#         header: Dict[str, DataType]=None,
#         headless: bool=False,
#         delimiter: str=",",
#         quote: str="\"",
#         ignore_surrounding_spaces: bool = True,
#         trim: bool = False,
#         charset: str = "utf-8")


# PARQUET

# write Parquet
from deephaven.parquet import write

table_sample = static_source_1.head(100)
write(table_sample, "/data/output_generic.parquet")

write(table_sample, "/data/output_gzip.parquet", compression_codec_name="GZIP")
# other compression codes are available

# read Parquet
from deephaven.parquet import read

parquet_read = read("/data/output_generic.parquet")
parquet_ok_if_zipped = read("/data/output_gzip.parquet")
```

## Do Cum-Sum and Rolling Average

```python test-set=1 order=null
def has_others(array, target):
    for x in array:
        if x != target:
            return True
    return False


###
make_arrays = static_source_2.sort("String3").update_view(
    [
        "String3Array10 = String3_.subVector(i-5, i-1)",
        "Int2Array10 = Int2_.subVector(i-5, i-1)",
        "Double2Array10 = Double2_.subVector(i-5, i-1)",
    ]
)
###
cum_and_roll_10 = make_arrays.update(
    [
        "ArrayHasOtherString3s = \
        has_others.call(String3Array10.toArray(), String3)",
        "CumSum10 = ArrayHasOtherString3s = false ? \
        (int)sum(Double2Array10) : NULL_INT",
        "RollAvg10 = ArrayHasOtherString3s = false ? \
        avg(Int2Array10) : NULL_DOUBLE",
    ]
)
```

### Another example of creating a rolling sum

```python test-set=1 order=null
from deephaven.updateby import rolling_sum_tick

rolling_sum_op = rolling_sum_tick(
    cols=["RollingSumInt1 = Int1"], rev_ticks=20, fwd_ticks=0
)

sums = trades.update_by(ops=[rolling_sum_op], by=["String1"])
```

### Set up an EMA

```python test-set=1 ticking-table order=null
from deephaven.experimental.ema import ByEmaSimple

ema30sec = ByEmaSimple("BD_SKIP", "BD_SKIP", "TIME", 30, "SECONDS", type="LEVEL")

ema_data = ticking_source.view(
    formulas=["EMA_data = ema30sec.update(Timestamp, Int3)"]
).last_by()
ema_data_grouped = (
    ticking_source.view(
        formulas=["String5", "EMA_data = ema30sec.update(Timestamp, Int3, String5)"]
    )
    .last_by(by=["String5"])
    .sort(order_by=["String5"])
)
```

## Use pandas

```python test-set=1 order=:log
import deephaven.pandas as dhpd

pandas_df = dhpd.to_pandas(static_source_1)

# do Pandas stuff to prove it is a Pandas Dataframe
print(type(pandas_df), pandas_df.dtypes, pandas_df.describe())

# Pandas to dh table
dh_table_again = dhpd.to_table(pandas_df)
```

## Format tables

### Date-time formatting

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
            "Time1=Date(`yyyy_MM_dd'T'HH-mm-ss.S z`)",
            "Time2 = Date(`yyyy-MM-dd'T'HH:mm t`)",
            "Time3 = Date(`HH:mm:ss`)",
            "Time4 = Date(`HH:mm:ss.SSSSSSSSS`)",
            "Time5 = Date(`EEE dd MMM yy HH:MM:ss`)",
            "Time6 = Date(`yyyy-MM-dd`)",
        ]
    )
    .update_view(["Time7_string = formatDate(Time6, timeZone(`ET`))"])
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
            "RowMod10 = heatmap(RowMod10, 1.0, 10.0, BRIGHT_GREEN, BRIGHT_RED)",
        ]
    )
)

# row formatting
row_colors = static_source_1.format_row_where("Int1 > 45", " PALE_BLUE")

# one can format_columns() for numbers and colors together
number_and_color = static_source_1.format_columns(
    [
        "Double1 = Decimal(`##0.00`)",
        "Double1 = (Double1 > Double1_[i-1]) ? FUCHSIA : GREY",
    ]
)
```

You will find an itemization of stock colors in [the docs](../../assets/deephaven-colors.pdf).

## Plot programmatically

Substantial documentation about [`plotting`](../../how-to-guides/plotting/api-plotting.md#xy-series) exists. The following example intends to show the basics, and everything about styling and labeling is omitted.

### Time series plots

```python test-set=1 order=null
from deephaven.plot.figure import Figure

figure = Figure()
time_series = figure.plot_xy(
    series_name="Time_series", t=static_source_1.head(100), x="Timestamp", y="Int1"
).show()

time_series_2axis = (
    figure.plot_xy(
        series_name="Int1",
        t=static_source_1.where(["X < 100", "Int1 < 70 "]),
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

### Plot-by-some key

```python test-set=1 order=null
from deephaven.plot import Figure

table_3_keys = (
    static_source_1.head(2000)
    .where("String1 in `A`, `B`, `C`")
    .sort("Int1")
    .update(
        ["New_Row = i", "Y = Int1 * (String1 = `A` ? 2 : String1 = `B` ? 0.5 : 1.0)"]
    )
)
p = (
    Figure()
    .plot_xy(
        series_name="partitioned", t=table_3_keys, by=["String1"], x="New_Row", y="Y"
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

### Stacked Area

```python test-set=1 order=null
from deephaven.plot import PlotStyle, Figure

figure = Figure()
stacked_area_graph = (
    figure.axes(plot_style=PlotStyle.AREA)
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
from deephaven.plot.figure import Figure
from deephaven.plot import PlotStyle
from deephaven.plot import Color, Colors
from deephaven.plot import font_family_names, Font, FontStyle, Shape

figure = Figure()
plotXYScatter = (
    figure.plot_xy(
        series_name="Double1", t=static_source_1.head(50), x="Timestamp", y="Double1"
    )
    .axes(plot_style=PlotStyle.SCATTER)
    .point(shape=Shape.SQUARE, size=10, label="Big Point", color=Colors.RED)
    .x_twin()
    .axes(plot_style=PlotStyle.SCATTER)
    .plot_xy(series_name="ii", t=static_source_1.head(50), x="Timestamp", y="X")
    .point(shape=Shape.DIAMOND, size=16, label="Big Triangle", color=Colors.BLUE)
    .show()
)
```

## Use layout hints

The `layout_hints` method creates a new table with the layout specified by the parameters.

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

## Other useful methods

### Wait for table updates

Use [`await_update`](../table-operations/table-listeners/await-update.md) to instruct Deephaven to wait for updates to a specified table before continuing.

```python order=source,result
from deephaven import time_table

source = time_table("PT1S")

print(source.await_update(0))
print(source.await_update(1000))

result = source.update(formulas=("renamedTimestamp = Timestamp"))
```

### Convert dynamic tables to static tables

Uses [`snapshot`](../table-operations/snapshot/snapshot.md) to create a static, in-memory copy of a source table.

```python order=source,result
from deephaven import time_table

source = time_table("PT1S")

# Some time later...
result = source.snapshot()
```

### Reduce ticking frequency

Uses [`snapshot_when`](../table-operations/snapshot/snapshot-when.md) to reduce the ticking frequency.

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

Uses [`snapshot_when`](../table-operations/snapshot/snapshot-when.md) to capture the history of ticking tables.

```python order=source,trigger,result
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

### Use DynamicTableWriter

See our guide [How to write data to an in-memory, real-time table](../../how-to-guides/table-publisher.md#dynamictablewriter).

```python order=null
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht
from deephaven.plot import Figure

import numpy as np, threading, time

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
        time.sleep(0.2 - (end - start))


thread = threading.Thread(target=create_waveforms)
thread.start()

figure = Figure()
new_fig = (
    figure.plot_xy(series_name="Sawtooth Wave", t=waveforms, x="X", y="SawToothWave")
    .plot_xy(series_name="Square Wave", t=waveforms, x="X", y="SquareWave")
    .plot_xy(series_name="Triangle Wave", t=waveforms, x="X", y="TriangleWave")
)
new_plot = new_fig.show()
```
