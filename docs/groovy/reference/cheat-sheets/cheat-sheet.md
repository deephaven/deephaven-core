---
title: Ultimate cheat sheet
---

This cheat sheet shows common Deephaven table operations in one place, with a short example of each. Most of them work the same way on static tables and on ticking tables that update in real time; the few exceptions are noted where they apply. Run the code in [Necessary data](#necessary-data) first, because most examples on this page use the tables it creates.

## Necessary data

This code creates the following tables, helper closures, and values:

- `staticSource1` and `staticSource2`: static tables.
- `tickingSource`: a ticking table, which updates while the query runs. [`timeTable`](../table-operations/create/timeTable.md) adds a row to it every second. See [Deephaven's table types](../../conceptual/table-types.md).
- `randChoice` and `getRandomString`: helper closures that generate random strings.
- `lettersUpp`: a list of the uppercase letters A to Z, passed to `randChoice`.

The code casts the closures' return values with `(String)`, which turns their `Object` return values into `String` columns. See [Cast strings](#cast-strings).

> [!NOTE]
> In query strings, backticks (`` ` ``) delimit a string, and single quotes (`'`) delimit a literal that the engine parses, such as a date-time, duration, or time zone.

```groovy test-set=1 order=null reset
lettersUpp = ["A".."Z"].flatten()

rand = new Random()

getRandomString = { length ->
    def letters = ["a".."z", "A".."Z"].flatten()
    StringBuilder resultStr = new StringBuilder()
    for (int i = 0; i < length; i++){
        resultStr.append(randChoice(letters))
    }
    return resultStr.toString()
}

randChoice = { array ->
    return array[rand.nextInt(array.size())]}

// Create static tables
staticSource1 = emptyTable(26300).update(
    "X = ii",
    "Double1 = randomDouble(1, 100)",
    "Int1 = randomInt(1, 100)",
    "Timestamp = '2016-01-01T01:00 ET' + i * HOUR",
    "Date = formatDate(Timestamp, 'ET')",
    "String1 = (String)randChoice(lettersUpp)",
    "String2 = (String)getRandomString(randomInt(1, 4))",
)

staticSource2 = emptyTable(26300).update(
    "X = ii",
    "Double2 = randomDouble(1, 80)",
    "Int2 = randomInt(1, 100)",
    "Timestamp = '2019-01-01T01:00 ET' + i * HOUR",
    "String3 = (String)randChoice(lettersUpp)",
    "String4 = (String)getRandomString(randomInt(1, 4))",
)

// Create a ticking table
tickingSource = timeTable("PT1S").update(
    "X = ii",
    "Double3 = randomDouble(1, 100)",
    "Int3 = randomInt(1, 100)",
    "Date = formatDate(Timestamp, 'ET')",
    "String5 = (String)randChoice(lettersUpp)",
    "String6 = (String)getRandomString(randomInt(1, 4))",
)
```

## Create tables

### Empty tables

- [`emptyTable`](../table-operations/create/emptyTable.md)

```groovy order=result,result1
result = emptyTable(5)

// Empty tables are often followed with a formula
result1 = result.update("X = 5")
```

### New tables

- [`newTable`](../table-operations/create/newTable.md)

Create columns with the following methods:

- [`booleanCol`](../table-operations/create/booleanCol.md)
- [`byteCol`](../table-operations/create/byteCol.md)
- [`charCol`](../table-operations/create/charCol.md)
- [`col`](../table-operations/create/col.md)
- [`doubleCol`](../table-operations/create/doubleCol.md)
- [`floatCol`](../table-operations/create/floatCol.md)
- [`instantCol`](../table-operations/create/instantCol.md)
- [`intCol`](../table-operations/create/intCol.md)
- [`longCol`](../table-operations/create/longCol.md)
- [`shortCol`](../table-operations/create/shortCol.md)
- [`stringCol`](../table-operations/create/stringCol.md)

```groovy
result = newTable(
    intCol("IntegerColumn", 1, 2, 3),
    stringCol("Strings", "These", "are", "Strings"),
)
```

### Time tables

- [`timeTable`](../table-operations/create/timeTable.md)

The following code makes a [`timeTable`](../table-operations/create/timeTable.md) that updates every second.

```groovy ticking-table order=null
result = timeTable("PT1S")
```

### Input tables

Use an [`InputTable`](../table-operations/create/InputTable.md) to add or modify data in table cells directly.

```groovy order=myInputTable
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.impl.util.KeyedArrayBackedInputTable
import io.deephaven.engine.table.TableDefinition

myInputTable = KeyedArrayBackedInputTable.make(
    TableDefinition.of(
        ColumnDefinition.ofString("StringCol"),
        ColumnDefinition.ofDouble("DoubleCol")
    ),
    "StringCol"
)
```

### Ring tables

A [ring table](../table-operations/create/ringTable.md) keeps the `n` most recent rows of a source table. When the source table ticks, rows older than the newest `n` drop out of the ring table.

```groovy ticking-table order=null
import io.deephaven.engine.table.impl.sources.ring.RingTableTools

tt = timeTable("PT1S")
rt = RingTableTools.of(tt, 5)
```

### Tree tables

A [`tree`](../table-operations/create/tree.md) table has collapsible subsections that you can expand in the UI for more detail.

```groovy order=source,result
source = emptyTable(100).updateView("ID = i", "Parent = i == 0 ? NULL_INT : (int)(i / 4)")

result = source.tree("ID", "Parent")
```

### Rollup tables

A [`rollup`](../table-operations/create/rollup.md) table aggregates the source table at each level of a list of grouping columns. You can expand each level in the UI to show the rows of the level below it.

```groovy order=insurance,insuranceRollup
import io.deephaven.csv.CsvTools

insurance = CsvTools.readCsv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv"
)

aggList = [AggAvg("bmi", "expenses")]

insuranceRollup = insurance.rollup(aggList, true, "region", "age")
```

### Tables written from code

A [`DynamicTableWriter`](../table-operations/create/DynamicTableWriter.md) creates an [append-only](../../conceptual/table-types.md#specialization-1-append-only) table and writes rows to it from your own code. It is the simpler choice when you add one row at a time. For most other cases, use [`TablePublisher`](../table-operations/create/TablePublisher.md), which publishes rows to a [blink table](../../conceptual/table-types.md#specialization-3-blink). [Write data to an in-memory, real-time table](../../how-to-guides/table-publisher.md) compares the two.

```groovy order=null
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.TableDefinition
import io.deephaven.engine.table.impl.util.DynamicTableWriter

def ySquareF = { double x ->
    if (x % 2 < 1) {
        return 1.0
    } else {
        return -1.0
    }
}

def yTriangleF = { double x ->
    if (x % 2 >= 1) {
        return (x % 1 - 0.5) * 2
    } else {
        return -((x % 1 - 0.5) * 2)
    }
}

def definition = TableDefinition.of(
    ColumnDefinition.ofDouble("X"),
    ColumnDefinition.ofDouble("SawToothWave"),
    ColumnDefinition.ofDouble("SquareWave"),
    ColumnDefinition.ofDouble("TriangleWave")
)
def tableWriter = new DynamicTableWriter(definition)

waveforms = tableWriter.getTable()

Thread.start {
    for (int i = 0; i < 200; i++) {
        long start = System.currentTimeMillis()
        double x = 0.1 * i
        double ySawtooth = (x % 1 - 0.5) * 2
        double ySquare = ySquareF(x)
        double yTriangle = yTriangleF(x)
        tableWriter.logRow(x, ySawtooth, ySquare, yTriangle)
        long elapsed = System.currentTimeMillis() - start
        sleep(Math.max(0L, 200 - elapsed))
    }
}
```

## View table metadata

[`meta`](../table-operations/metadata/meta.md) shows each column's name, data type, column type, and whether it is a partitioning column. Use it to confirm that schemas match before a [`merge`](../table-operations/merge/merge.md).

```groovy test-set=1 order=null
static1Meta = staticSource1.meta()
```

## Merge tables

Tables passed to [`merge`](../table-operations/merge/merge.md) must have identical schemas, with the same column names and the same column data types. This applies to both static and ticking tables.

- [`merge`](../table-operations/merge/merge.md)
- [`mergeSorted`](../table-operations/merge/merge-sorted.md)

```groovy test-set=1 ticking-table order=null
copy1 = staticSource1
mergeSameStatic = merge(staticSource1, copy1)

copyUpdating1 = tickingSource
copyUpdating2 = tickingSource
mergeSameDynamic = merge(copyUpdating1, copyUpdating2)

// one can merge static and ticking tables
staticSource1V2 = staticSource1.view("Date", "Timestamp", "SourceRowNum = X")
tickingSourceV2 = tickingSource.view("Date", "Timestamp", "SourceRowNum = X")
mergeStaticAndDynamic = merge(staticSource1V2, tickingSourceV2)
```

## Filter

Most queries benefit from [filtering](../../how-to-guides/use-filters.md) early. Less data generally means better performance.

> [!TIP]
> For SQL developers: in Deephaven, [joins](../../how-to-guides/joins-exact-relational.md) are not a primary operation for filtering. Filter your data with [`where`](../table-operations/filter/where.md), [`whereIn`](../table-operations/filter/where-in.md), and [`whereNotIn`](../table-operations/filter/where-not-in.md) before joining. Filtering first reduces the rows each join has to match.

### Date and time examples

```groovy test-set=1 ticking-table order=null
todaysData1 = tickingSource.where(
    "Date = today('ET')"
)  // Date for this is a String
todaysData3 = tickingSource.where(
    "formatDate(Timestamp, 'Australia/Sydney') = today('Australia/Sydney')"
)  // sydney timezone

singleStringDate = staticSource1.where("Date = `2017-01-03`")
lessThanDate = staticSource1.where("Date < `2017-01-03`")  // use >=, etc. as well
oneMonthString = staticSource1.where("Date.startsWith(`2017-02`)")

singleDbDate = staticSource2.where(
    "formatDate(Timestamp, 'ET') = `2020-03-01`"
)
afterDbDatetime = staticSource1.where("Timestamp > '2017-01-05T10:30:00 ET'")

justBizTime = staticSource1.where("calendar(`USNYSE_EXAMPLE`).isBusinessTime(Timestamp)")  // the default calendar, UTC, treats every day as a business day and every moment as business time
justAugustDatetime = staticSource2.where(
    "monthOfYear(Timestamp, 'ET') = 8"
)
just10AmHour = staticSource1.where(
    "hourOfDay(Timestamp, 'ET', false) = 10"
)
justTues = staticSource2.where(
    "dayOfWeek(Timestamp, 'ET').getValue() = 2"
)  // Tuesdays

time1 = parseInstant("2017-08-21T09:45:00 ET")
time2 = parseInstant("2017-08-21T10:10:00 ET")
// You can use inRange to filter times by a range
trades = staticSource1.where("inRange(Timestamp, time1, time2)")
// However, conjunctive filters are faster, sometimes significantly so
trades = staticSource1.where("Timestamp >= time1", "Timestamp <= time2")

// You can also write date-time literals directly in the query string with single quotes (')
trades = staticSource1.where("inRange(Timestamp, '2017-08-21T09:45:00 ET', '2017-08-21T10:10:00 ET')")
trades = staticSource1.where("Timestamp >= '2017-08-21T09:45:00 ET'", "Timestamp <= '2017-08-21T10:10:00 ET'")
```

### String examples

```groovy test-set=1 order=null
oneStringMatch = staticSource1.where("String1 = `A`")  // match filter

stringSetMatch = staticSource1.where("String1 in `A`, `M`, `G`")
caseInsensitive = staticSource1.where("String1 icase in `a`, `m`, `g`")
notInExample = staticSource1.where("String1 not in `A`, `M`, `G`")  // see "not"

containsExample = staticSource1.where("String2.contains(`P`)")
notContainsExample = staticSource1.where("!String2.contains(`P`)")
startsWithExample = staticSource1.where("String2.startsWith(`A`)")
endsWithExample = staticSource1.where("String2.endsWith(`M`)")
```

### Number examples

The `modExample1` filter condition uses the row-position variable [`i`](../query-language/variables/special-variables.md). See [Get the row number](#get-the-row-number) for when [`i` and `ii`](../query-language/variables/special-variables.md) are allowed in formulas. The engine doesn't check `where` filters, so a filter that uses [`i`](../query-language/variables/special-variables.md) on a ticking table runs, but its results can go stale as rows change.

```groovy test-set=1 order=null
equalsExample = staticSource2.where("Int2 = 44")
lessThanExample = staticSource2.where("Double2 < 8.42")
someManipulation = staticSource2.where("(Double2 - Int2) / Int2 > 0.05")
modExample1 = staticSource2.where("i % 10 = 0")  // every 10th row
modExample2 = staticSource2.where("String4.length() % 2 = 0")
// String4 values with an even number of characters
```

### Multiple filters

```groovy test-set=1 order=null
conjunctiveComma = staticSource1.where("Date = `2017-08-23`", "String1 = `A`")
conjunctiveAmpersand = staticSource1.where("Date = `2017-08-23` && String1 = `A`")

disjunctiveSameCol = staticSource1.where("Date = `2017-08-23` || Date = `2017-08-25`")

disjunctiveDiffCol = staticSource1.where("Date = `2017-08-23` || String1 = `A`")

rangeConjunctive = staticSource1.where("Date >= `2017-08-21`", "Date <= `2017-08-23`")
rangeInRange = staticSource1.where("inRange(Date, `2017-08-21`, `2017-08-23`)")

inRangeString = staticSource1.where(
    "inRange(String2.substring(0, 1), `A`, `D`)"
)
// starts with an uppercase letter from A to D
```

### OR filters

A list of filters passed to [`where`](../table-operations/filter/where.md) keeps rows that match all of them. To keep rows that match at least one condition, either combine the conditions with `||` in one filter string, as in [Multiple filters](#multiple-filters), or pass them to [`Filter.or`](/core/javadoc/io/deephaven/api/filter/Filter.html#or(java.util.Collection)). See [disjunctive filters](../../how-to-guides/use-filters.md#disjunctive).

```groovy order=source,resultOr
source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol(
        "Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"
    ),
    intCol("Code", 12, 14, 11, NULL_INT, 16, 14, NULL_INT),
)

resultOr = source.where(
    Filter.or(Filter.from("Color = `blue`", "Number > 2"))
)  // OR operation - result will have _either_ criteria
```

### `whereIn` and `whereNotIn`

```groovy test-set=1 order=null
// run these two queries together
stringSetDriver = staticSource1.renameColumns("String4 = String2").head(10)
whereInExample1 = staticSource2.whereIn(stringSetDriver, "String4")

whereInExample2 = staticSource2.whereIn(staticSource1, "String4 = String2")
// String4 values in staticSource2 that appear in String2 of staticSource1

whereNotInExample = staticSource2.whereNotIn(
    staticSource1, "String4 = String2"
)
// see the "not"
```

### Nulls and NaNs

```groovy test-set=1 order=null
// staticSource1 has no nulls or NaNs, so nullExample and nanExample are empty
nullExample = staticSource1.where("isNull(Int1)")  // all data types supported
notNullExample = staticSource1.where("!isNull(Int1)")

nanExample = staticSource1.where("isNaN(Double1)")
notNanExample = staticSource1.where("!isNaN(Double1)")
```

### Head and tail

These methods reduce the number of rows:

- [`tail`](../table-operations/filter/tail.md)
- [`tailPct`](../table-operations/filter/tail-pct.md)
- [`head`](../table-operations/filter/head.md)
- [`headPct`](../table-operations/filter/head-pct.md)
- [`headBy`](../table-operations/group-and-aggregate/headBy.md)
- [`tailBy`](../table-operations/group-and-aggregate/tailBy.md)

```groovy test-set=1 ticking-table order=null
first10kRows = staticSource2.head(10_000)
tickingLastRows = tickingSource.tail(100)

first15Percent = tickingSource.reverse().headPct(0.15)  // most recent 15% of rows
last15Percent = staticSource2.tailPct(0.15)  // the last 15% of rows

first10ByKey = staticSource1.headBy(
    10, "String1"
)  // first 10 rows for each String1-key
last10ByKey = staticSource1.tailBy(
    10, "String1"
)  // last 10 rows for each String1-key
```

## Sort

Sort on one or more columns in one direction:

- [`sort`](../table-operations/sort/sort.md)
- [`sortDescending`](../table-operations/sort/sort-descending.md)

Sort in mixed directions, with a direction for each column:

- [`sort`](../table-operations/sort/sort.md) with a list of [`SortColumn`](/core/javadoc/io/deephaven/api/SortColumn.html) objects

```groovy test-set=1 order=null
// works for numeric, string, date-time, and other Comparable column types

sortTimeV1 = staticSource1.sort("Timestamp")  // sort ascending
sortDouble = staticSource1.sort("Double1")

sortNumbers = staticSource1.sortDescending("Int1")  // sort descending
sortStrings = staticSource1.sortDescending("String2")

sortMultiDesc = staticSource1.sortDescending(
    "String1", "Timestamp"
)  // multiple
sortMulti = staticSource1.sort("Int1", "String1")

sortMultipleCol = [
    SortColumn.asc(ColumnName.of("Int1")),
    SortColumn.desc(ColumnName.of("Double1"))
]

multiSort = staticSource1.sort(
    sortMultipleCol
)  // Int ascending then Double descending
```

Reverse the order of rows:

- [`reverse`](../table-operations/sort/reverse.md)

> [!TIP]
> [`reverse`](../table-operations/sort/reverse.md) is faster than [`sort`](../table-operations/sort/sort.md). UIs often use it to show the most recently appended rows at the top of a table.

```groovy test-set=1 ticking-table order=null
reverseTable = staticSource1.reverse()
reverseUpdating = tickingSource.reverse()
```

## Select and create new columns

Deephaven has three ways to choose and add columns. They differ in when formulas are calculated and whether the results are stored in memory. For a full comparison, see [Select and create columns](../../how-to-guides/use-select-view-update.md). The last subsection, [Get the row number](#get-the-row-number), shows the built-in row-position variables that any of these methods can use.

### Option 1: Calculate and store new columns in memory

Use [`select`](../table-operations/select/select.md) and [`update`](../table-operations/select/update.md) when data is expensive to calculate or accessed frequently. These methods store results in memory, which speeds up access but uses more memory.

```groovy test-set=1 order=null
selectColumns = staticSource2.select("Timestamp", "Int2", "Double2", "String3")
// constrain to only those 4 columns, write to memory
selectAddCol = staticSource2.select(
    "Timestamp", "Int2", "Double2", "String3", "Difference = Double2 - Int2"
)
// constrain and add a new column calculating

selectAndUpdateCol = staticSource2.select(
    "Timestamp", "Int2", "Double2", "String3"
).update("Difference = Double2 - Int2")
// add a new column calculating - logically equivalent to previous example
```

### Option 2: Calculate new columns on demand

Use [`view`](../table-operations/select/view.md) and [`updateView`](../table-operations/select/update-view.md) when the formula is quick to compute or when queries read only a portion of the data at a time. These methods minimize memory use.

```groovy test-set=1 order=null
viewColumns = staticSource2.view("Timestamp", "Int2", "Double2", "String3")
// similar to select(), though uses on-demand formula
viewAddCol = staticSource2.view(
    "Timestamp", "Int2", "Double2", "String3", "Difference = Double2 - Int2"
)
// view set and add a column, though with an on-demand formula

viewAndUpdateViewCol = staticSource2.view(
    "Timestamp", "Int2", "Double2", "String3"
).updateView("Difference = Double2 - Int2")
// logically equivalent to previous example
```

### Option 3: Calculate on demand and cache results

Use [`lazyUpdate`](../table-operations/select/lazy-update.md) when the formula's input columns have a relatively small number of unique values. It computes each result on demand, caches it, and reuses it for repeated inputs.

```groovy test-set=1 order=null
// Int2 has at most 99 distinct values, so each square root is computed once and reused
lazyUpdateExample = staticSource2.lazyUpdate("Int2Sqrt = sqrt(Int2)")
```

### Get the row number

[`i` and `ii`](../query-language/variables/special-variables.md) are built-in variables that hold a row's position in the table. [`i`](../query-language/variables/special-variables.md) is an `int` and [`ii`](../query-language/variables/special-variables.md) is a `long`.

> [!CAUTION]
> A row's position can change as rows are added or removed. By default, the engine allows [`i` and `ii`](../query-language/variables/special-variables.md) in a formula on a ticking table only if the table is [append-only](../../conceptual/table-types.md#specialization-1-append-only) or a [blink table](../../conceptual/table-types.md#specialization-3-blink). An [add-only table](../../conceptual/table-types.md#specialization-2-add-only) whose row keys match its row positions also qualifies. The engine doesn't check `where` filters, so a filter that uses [`i` or `ii`](../query-language/variables/special-variables.md) on a ticking table runs, but its results can go stale as rows change.

```groovy test-set=1 order=null
getRowNum = staticSource2.updateView("RowNumInt = i", "RowNumLong = ii")
```

## Manipulate columns

These methods list distinct values and rename, drop, and reorder columns.

```groovy test-set=1 order=null
uniqueValues = staticSource2.selectDistinct("String4")  // show unique set
// works on all data types; a high-cardinality column, such as Double2, returns nearly one row per source row
uniqueValuesCombo = staticSource2.selectDistinct("String3", "String4")
// unique combinations of String3 with String4

renameStuff = staticSource2.renameColumns("Symbol = String4", "Price = Double2")
dropColumn = staticSource2.dropColumns("X")  // same for drop one or many
dropColumns = staticSource2.dropColumns("X", "Int2", "String3")

putColsAtStart = staticSource2.moveColumnsUp(
    "String4", "Int2"
)  // Makes 1st col(s)
putColsWherever = staticSource2.moveColumns(1, "String4", "Int2")
// makes String4 the 2nd and Int2 the 3rd column
```

## Do math

Query strings can call Java `Math` methods and Deephaven's built-in numeric functions, such as `abs`, `exp`, `log`, `round`, and `floor`.

```groovy test-set=1 order=null
// should use .update() when working with random numbers
simpleMath = staticSource2.update(
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
)
```

## Handle arrays

Append an underscore to a column name to use the whole column as an [array](../query-language/types/arrays.md). For example, `Double2_` is the `Double2` column as an array. Index the array with [`i`](../query-language/variables/special-variables.md) to reach values in other rows. [`subVector(start, end)`](../query-language/types/arrays.md) returns a slice of an array, from `start` up to but not including `end`.

> [!CAUTION]
> In a formula, a simple constant offset, such as `Double2_[i-1]`, works on any table. Other column-array expressions, such as `Double2_.subVector(i-2, i+1)`, work in formulas only on static tables and [blink tables](../../conceptual/table-types.md#specialization-3-blink). On other ticking tables, including append-only tables, the engine rejects them with an error. For rolling calculations on ticking tables, use [`updateBy`](../table-operations/update-by-operations/updateBy.md). See [Special variables](../query-language/variables/special-variables.md).

```groovy test-set=1 order=null
arrayExamples = staticSource2.updateView(
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
)
```

## Calculate rolling sums and averages

[`updateBy`](../table-operations/update-by-operations/updateBy.md) provides dedicated cumulative and rolling operations, such as [`CumSum`](../table-operations/update-by-operations/cum-sum.md) and [`RollingSum`](../table-operations/update-by-operations/rolling-sum.md).

### Create a rolling sum with `updateBy`

[`updateBy`](../table-operations/update-by-operations/updateBy.md) with [`RollingSum`](../table-operations/update-by-operations/rolling-sum.md) computes a sum over a 20-row window that ends at the current row. It computes a separate sum for each `String1` group.

```groovy test-set=1 order=null
sums = staticSource1.updateBy(RollingSum(20, 0, "RollingSumInt1 = Int1"), "String1")
```

### Set up an exponential moving average (EMA)

An exponential moving average (EMA) is an average that weights recent values more heavily than older ones. [`Ema`](../table-operations/update-by-operations/ema.md) computes an EMA with a decay time of 30 seconds, across the whole table and for each `String5` group. [`lastBy`](../table-operations/group-and-aggregate/lastBy.md) then keeps the latest row overall and the latest row for each group.

```groovy test-set=1 ticking-table order=null
ema30Sec = Ema("Timestamp", parseDuration("PT30S"), "EmaData = Int3")

emaData = tickingSource.updateBy(ema30Sec).lastBy()
emaDataGrouped = tickingSource.updateBy(ema30Sec, "String5")
    .lastBy("String5")
    .sort("String5")
```

### Calculate a rolling window with column arrays

For a static table, you can also compute statistics over a window of rows with column arrays (see [Handle arrays](#handle-arrays)) instead of [`updateBy`](../table-operations/update-by-operations/updateBy.md). This example [sorts](../table-operations/sort/sort.md) by `String3`. For each row, it then sums `Double2` and averages `Int2` over the 10 rows that end at that row. The result is null unless all 10 rows have the same `String3` value.

```groovy test-set=1 order=null
hasOthers = { array, target ->
    for (x in array) {
        if (x != target) {
            return true
        }
    }
    return false
}


makeArrays = staticSource2.sort("String3").updateView(
    "String3Array10 = String3_.subVector(i-9, i+1)",
    "Int2Array10 = Int2_.subVector(i-9, i+1)",
    "Double2Array10 = Double2_.subVector(i-9, i+1)",
)

rollSumAndAvg10 = makeArrays.update(
    "ArrayHasOtherString3s = hasOthers(String3Array10.toArray(), String3)",
    "RollSum10 = ArrayHasOtherString3s = false ? \
    sum(Double2Array10) : NULL_DOUBLE",
    "RollAvg10 = ArrayHasOtherString3s = false ? \
    avg(Int2Array10) : NULL_DOUBLE",
)
```

## Manipulate time and calendars

Query strings include built-in functions for date-times and business calendars. See [Work with time](../../conceptual/time-in-deephaven.md) and [Work with calendars](../../how-to-guides/business-calendar.md).

```groovy test-set=1 order=null
// The default calendar, UTC, treats every day as a business day and every moment as business time
setCalendar("USNYSE_EXAMPLE")

timeStuff = staticSource2.view(
    "Timestamp",
    "CurrentTime = now()",
    "CurrentDateDefault = calendarDate()",
    "CurrentDateLon = today('Europe/London')",
    "IsAfter = CurrentTime > Timestamp",
    "IsBizDay = isBusinessDay(CurrentDateLon)",
    "StringDT = formatDateTime(Timestamp, 'ET')",
    "StringDate = formatDate(Timestamp, 'ET')",
    "StringToTime = parseInstant(StringDate + `T12:00 ET`)",
    "AddTime = Timestamp + 'PT05:11:04.332'",
    "PlusHour = Timestamp + HOUR",
    "LessTenMins = Timestamp - 10 * MINUTE",
    "DiffYear = diffYears365(Timestamp, CurrentTime)",
    "DiffDay = diffDays(Timestamp, CurrentTime)",
    "DiffNanos = PlusHour - Timestamp",
    "DayWeek = dayOfWeek(Timestamp, 'ET')",
    "HourDay = hourOfDay(Timestamp, 'ET', false)",
    "DateAtMidnight = atMidnight(Timestamp, 'ET')",
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
)
```

## Manipulate strings

Query strings can call Java `String` methods, as the following examples show:

```groovy test-set=1 order=null
stringStuff = staticSource2.view(
    "StringDate = formatDate(Timestamp, 'ET')",
    "String4",
    "Double2",
    "NewString = `new_string_example_`",
    "ConcatV1 = NewString + String4",
    "ConcatV2 = NewString + `NoteBackticks!!`",
    "ConcatV3 = NewString.concat(String4)",
    "ConcatV4 = NewString.concat(`NoteBackticks!!`)",
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
)
```

## Use ternaries (if-then)

For details, see [Use the ternary conditional operator in query strings](../../how-to-guides/ternary-if-how-to.md).

```groovy test-set=1 order=null
ifThenExample1 = staticSource2.updateView(
    "SimpleTernary = Double2 < 50 ? `smaller` : `bigger`"
)

ifThenExample2 = staticSource2.updateView(
    "TwoLayers = Int2 <= 20 ? `small` : Int2 < 50 ? `medium` : `large`"
)
```

## Create and use custom variables

A query string can use Groovy variables in the script's binding, which are those assigned without `def` or a type. For details, see [Groovy variables in query strings](../../how-to-guides/groovy-variables.md).

```groovy
a = 7

result = emptyTable(5).update("Y = a")
```

## Create and use a custom function

For details, see [Groovy closures in queries](../../how-to-guides/groovy-closures.md).

```groovy test-set=1 order=null
mult = { a, b ->
    return a * b
}

grFuncExample = staticSource2.update(
    "MObject = mult(Double2,Int2)", "MDouble = (double)mult(Double2,Int2)"
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

You can also cast an object, such as the return value of a Groovy closure, to `String` or to a numeric type. Casting is useful when an operation needs more or less precision than the original data type. For details, see [Casting](../query-language/control-flow/casting.md).

### Cast numeric types

This example casts a column of doubles near each type's largest and smallest values to each numeric type. The two metadata tables show the resulting column types.

```groovy order=numbersMax,numbersMin,numbersMinMeta,numbersMaxMeta
numbersMax = newTable(
    doubleCol(
        "MaxNumbers",
        (2 - 1 / (2**52)) * (2**1023),
        (2 - 1 / (2**23)) * (2**127),
        (2**63) - 1,
        (2**31) - 1,
        (2**15) - 1,
        (2**7) - 1,
    )
).view(
    "DoubleMax = (double)MaxNumbers",
    "FloatMax = (float)MaxNumbers",
    "LongMax = (long)MaxNumbers",
    "IntMax = (int)MaxNumbers",
    "ShortMax = (short)MaxNumbers",
    "ByteMax = (byte)MaxNumbers",
)

numbersMin = newTable(
    doubleCol(
        "MinNumbers",
        1 / (2**1074),
        1 / (2**149),
        -(2**63) + 513,
        -(2**31) + 2,
        -1 * (2**15) + 1,
        -(2**7) + 1,
    )
).view(
    "DoubleMin = (double)MinNumbers",
    "FloatMin = (float)MinNumbers",
    "LongMin = (long)MinNumbers",
    "IntMin = (int)MinNumbers",
    "ShortMin = (short)MinNumbers",
    "ByteMin = (byte)MinNumbers",
)

numbersMinMeta = numbersMin.meta().view("Name", "DataType")
numbersMaxMeta = numbersMax.meta().view("Name", "DataType")
```

Each row holds a value near one type's limit. When that value is outside a column's type, the cast wraps, saturates, or overflows to infinity, as in Java, and a result equal to the type's null value, such as `Integer.MIN_VALUE` for `int`, displays as null. See [Casting](../query-language/control-flow/casting.md).

### Cast strings

A Groovy closure returns an `Object`. Cast its return value with `(String)` to get a `String` column. Java methods such as `String.valueOf` and `Integer.parseInt` convert between numbers and strings. [Manipulate strings](#manipulate-strings) shows more conversions between numbers and strings.

```groovy test-set=1 order=null
stringCasts = staticSource1.update(
    "Letter = (String)randChoice(lettersUpp)",
    "IntAsString = String.valueOf(Int1)",
    "StringAsInt = Integer.parseInt(IntAsString)",
)
```

## Bin data

Binning groups values into buckets, usually as the first step before an [aggregation](../../how-to-guides/combined-aggregations.md) that downsamples or profiles the data.

```groovy test-set=1 ticking-table order=null
bins = tickingSource.updateView(
    "PriceBin = upperBin(Double3, 5)",
    "SizeBin = lowerBin(Int3, 10)",
    "TimeBin = upperBin(Timestamp, 10 * SECOND)",
).reverse()
aggBin = bins.view("TimeBin", "Total = Int3 * Double3").sumBy("TimeBin")
```

## Group and aggregate

See [Group and ungroup data](../../how-to-guides/grouping-data.md) for more details.

### Simple grouping

```groovy test-set=1 order=groupToArrays,groupMultipleKeys
groupToArrays = staticSource1.groupBy("String2")
// one row per key (i.e. String2), all other columns are arrays
groupMultipleKeys = staticSource1.groupBy("String1", "Date")
// one row for each key-combination (i.e. String1-Date pairs)
```

### Ungrouping

[`ungroup`](../table-operations/group-and-aggregate/ungroup.md) expands array columns so that each array element becomes its own row.

```groovy test-set=1 order=null
// Run these lines together
aggByKey = staticSource1.groupBy("Date")
// one row per Date, other fields are arrays from staticSource1
ungroupThatOutput = aggByKey.ungroup()  // no arguments usually
// each array value becomes its own row
// in this case restores the rows of staticSource1, with Date moved to the first column
```

### Aggregations

- [Single aggregation](../../how-to-guides/dedicated-aggregations.md)
- [Multi-aggregation](../../how-to-guides/combined-aggregations.md)

```groovy test-set=1 order=null
countOfEntireTable = staticSource1.countBy(
    "Count"
)  // single arg returns tot count, you choose name of column output
countOfGroup = staticSource1.countBy("N", "String1")  // N is naming choice

firstOfGroup = staticSource1.firstBy("String1")
lastOfGroup = staticSource1.lastBy("String1")

sumOfGroup = staticSource1.view("String1", "Double1").sumBy("String1")
// non-key field(s) must be numeric or Boolean
avgOfGroup = staticSource1.view("String1", "Double1", "Int1").avgBy("String1")
stdOfGroup = staticSource1.view("String1", "Double1", "Int1").stdBy("String1")
varOfGroup = staticSource1.view("String1", "Double1", "Int1").varBy("String1")
medianOfGroup = staticSource1.view("String1", "Double1", "Int1").medianBy("String1")
minOfGroup = staticSource1.view("String1", "Double1", "Int1").minBy("String1")
maxOfGroup = staticSource1.view("String1", "Double1", "Int1").maxBy("String1")

// combine aggregations in a single method (using the same key-grouping)
combinedAggs = staticSource1.aggBy(
    [AggAvg("AvgDouble1 = Double1"), AggMax("MaxInt1 = Int1")], "String1"
)
```

### Formula aggregations

[`AggFormula`](../table-operations/group-and-aggregate/AggFormula.md) applies a formula to each group. This example averages three columns for each `Letter`-`Color` pair.

```groovy order=source,result
colors = ["Red", "Blue", "Green"]

formulas = [
    "X = 0.1 * i",
    "Y1 = Math.pow(X, 2)",
    "Y2 = Math.sin(X)",
    "Y3 = Math.cos(X)",
]
groupingCols = ["Letter = (i % 2 == 0) ? `A` : `B`", "Color = (String)colors[i % 3]"]

source = emptyTable(40).update(*(formulas + groupingCols))

myAgg = [
    AggFormula("AvgY1 = avg(Y1)"),
    AggFormula("AvgY2 = avg(Y2)"),
    AggFormula("AvgY3 = avg(Y3)"),
]

result = source.aggBy(myAgg, "Letter", "Color")
```

## Join data

See [Choose the right join method](../../how-to-guides/joins-exact-relational.md#which-method-should-you-use) for more details.

### Common joins

#### Natural join

[`naturalJoin`](../table-operations/join/natural-join.md) adds columns from the right table to every row of the left table, using at most one matching right row per left row.

- Returns all the rows of the left table, along with up to one matching row from the right table.
- If there is no match in the right table for a given row, nulls appear for that row in the columns from the right table.
- By default, the query fails if there are multiple matches in the right table for a given row. Pass [`NaturalJoinType.FIRST_MATCH` or `NaturalJoinType.LAST_MATCH`](/core/javadoc/io/deephaven/api/NaturalJoinType.html) as the last argument to keep the first or last match instead.

```groovy syntax
result = leftTable.naturalJoin(rightTable, columnsToMatch, columnsToAdd)
```

```groovy test-set=1 order=null
// run these together
lastByKeyTable = staticSource1.view("String1", "Int1", "Timestamp")
    .lastBy("String1")
    .renameColumns("Renamed = Int1", "RenamedTime = Timestamp")

// creates 3-column table of String1 + last record of the other 2 cols
joinWithLastPrice = staticSource1.naturalJoin(lastByKeyTable, "String1")
// arguments are (rightTableOfJoin, "JoinKey(s)")
// will have same number of rows as staticSource1 (left table)
// brings all non-key columns from lastByKeyTable to staticSource1

specifyColsFromR = staticSource1.naturalJoin(
    lastByKeyTable, "String1", "Renamed"
)
// nearly identical to joinWithLastPrice example
// args are (rightTableOfJoin, "JoinKey(s)", "Cols from R table")

renameColsOnJoin = staticSource1.naturalJoin(
    lastByKeyTable, "String1", "RenamedAgain = Renamed, RenamedTime"
)
// nearly identical to specifyColsFromR example
// can rename column on the join
// sometimes this is necessary...
// if there would be 2 cols of same name
// Note the placement of quotes

keysOfDiffNames = staticSource2.view("String3", "Double2").naturalJoin(
    lastByKeyTable, "String3 = String1"
)
// note that String3 in the L table is mapped as the key to String1 in R
// every String3 letter also appears in String1, so every row finds a match
```

#### Natural join on multiple keys

```groovy test-set=1 order=null
// run these together
lastPriceTwoKeys = staticSource1.view("Date", "String1", "Int1").lastBy(
    "Date", "String1"
)
// creates 3-column table with the last Int1 for each Date-String1 pair
natJoinTwoKeys = staticSource1.naturalJoin(
    lastPriceTwoKeys, "Date, String1", "LastInt1 = Int1"
)
// arguments are (rightTableOfJoin, "JoinKey1, JoinKey2", "Col(s)")
// adds LastInt1, renamed because the left table already has an Int1 column
```

#### As-of joins (`aj` and `raj`)

[As-of joins](../table-operations/join/aj.md) are [time-series joins](../../how-to-guides/joins-timeseries-range.md). For each left-table row, [`aj`](../table-operations/join/aj.md) returns the right-table row whose key matches exactly or, if none does, the closest earlier row. With a timestamp key, that is the row at the same time or the most recent row before it. As-of joins also work on other ordered keys, such as numbers or strings.

```groovy syntax
result = leftTable.aj(rightTable, columnsToMatch, columnsToAdd)
```

A reverse as-of join, [`raj`](../table-operations/join/raj.md), returns the exact match or, if there is none, the closest row after it. For a timestamp key, that is the row at the same time or the next later row.

```groovy syntax
result = leftTable.raj(rightTable, columnsToMatch, columnsToAdd)
```

```groovy test-set=1 ticking-table order=null
tickingSource2 = timeTable("PT10.5S").update(
    "X = ii",
    "Double4 = randomDouble(1, 100)",
    "Int4 = randomInt(1, 100)",
    "Date = formatDate(Timestamp, 'ET')",
    "String7 = (String)randChoice(lettersUpp)",
    "String8 = (String)getRandomString(randomInt(1, 4))",
)

ajJoin = tickingSource2.aj(
    tickingSource,
    "Timestamp",
    "String5, String6, JoinTime = Timestamp",
)

rajJoin = tickingSource2.raj(
    tickingSource,
    "Timestamp",
    "String5, String6, JoinTime = Timestamp",
)

rajJoinTwoKeys = tickingSource2.raj(
    tickingSource,
    "String7 = String5, Timestamp",
    "String6, JoinTime = Timestamp",
)
```

### Less common joins

#### Exact join

[`exactJoin`](../table-operations/join/exact-join.md) adds columns from the right table to every row of the left table, and each left row must match exactly one right row.

- Returns all rows of the left table.
- If a row has no matching key, the query fails.
- If a row has multiple matching keys in the right table, the query fails.

```groovy test-set=1 order=null
lastString1Aug23 = staticSource1.where("Date = `2017-08-23`").lastBy("String1")
lastString1Aug24 = staticSource1.where("Date = `2017-08-24`")
    .lastBy("String1")
    .whereIn(lastString1Aug23, "String1")

// filters for String1 in lastString1Aug23
exactJoinEx = lastString1Aug24.exactJoin(
    lastString1Aug23, "String1", "RenamedDouble = Double1"
)
```

#### Inner join

[`join`](../table-operations/join/join.md) returns every combination of matching rows from the left and right tables, similar to a SQL inner join.

- Returns only matching rows.
- Multiple matches produce duplicate rows, which can result in a long table.

```groovy test-set=1 order=null
lastSs1 = staticSource1.lastBy("String1").view("String1", "Double1")
lastSs2 = staticSource2.lastBy("String3").view(
    "String1 = String3", "RenamedDouble = Double2"
)
firstSs2 = staticSource2.firstBy("String3").view(
    "String1 = String3", "RenamedDouble = Double2"
)

mergeFirstAndLastSs2 = merge(firstSs2, lastSs2)
// the table has two rows per String1
joinExample = lastSs1.join(mergeFirstAndLastSs2, "String1", "RenamedDouble")
// note that the resultant set has two rows per String1
```

## Import and export data

Deephaven reads and writes files, such as CSV and Parquet files, and copies column values into Java arrays.

### Read and write files

The following script reads CSV files from URLs and from the `/data` directory of a Deephaven Docker container, and writes and reads CSV and Parquet files there. For every parameter, see the [`readCsv`](../data-import-export/CSV/readCsv.md), [`writeCsv`](../data-import-export/CSV/writeCsv.md), [`readTable`](../data-import-export/Parquet/readTable.md), and [`writeTable`](../data-import-export/Parquet/writeTable.md) reference pages.

```groovy test-set=1 order=null
// CSV
import io.deephaven.csv.CsvTools

// import CSV from a URL
csvImported = CsvTools.readCsv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv"
)

specs = CsvTools.builder().hasHeaderRow(false).build()

// import headerless CSV
csvHeaderless = CsvTools.readCsv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv",
    specs,
)

// import headerless CSV then add headings
headers = List.of("Year", "Score", "Title")
specs = CsvTools.builder().hasHeaderRow(false).headers(headers).build()
csvManualHeader = CsvTools.readCsv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv",
    specs
)

// !!! WRITE CSV !!!
// export / write CSV to local /data/ directory

CsvTools.writeCsv(csvManualHeader, "/data/outputFile.csv")

// import the local CSV you just exported
csvLocalRead = CsvTools.readCsv("/data/outputFile.csv")

// set more CSV options; see the readCsv reference for the rest
csvSpecsMoreOptions = CsvTools.builder().headers(headers).hasHeaderRow(false).ignoreSurroundingSpaces(true).trim(false).build()

csvMoreOptions = CsvTools.readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv", csvSpecsMoreOptions)

// PARQUET

// write Parquet
import io.deephaven.parquet.table.ParquetTools

tableSample = staticSource1.head(100)
ParquetTools.writeTable(tableSample, "/data/outputGeneric.parquet")

ParquetTools.writeTable(tableSample, "/data/outputGzip.parquet", ParquetTools.GZIP)
// other compression codecs are available, such as ParquetTools.SNAPPY, ParquetTools.ZSTD, and ParquetTools.LZ4_RAW

// read Parquet

parquetRead = ParquetTools.readTable("/data/outputGeneric.parquet")
parquetOkIfZipped = ParquetTools.readTable("/data/outputGzip.parquet")
```

You can also import files and export any table to a CSV file from the UI.

### Copy column values into a Java array

[`ColumnVectors`](/core/javadoc/io/deephaven/engine/table/vectors/ColumnVectors.html) wraps a column in a vector, and `copyToArray` copies the vector's values into a Java array, so you can read a cell by position.

```groovy test-set=1 order=:log
import io.deephaven.engine.table.vectors.ColumnVectors

// Copy a column into an array for positional index access
columnArray = ColumnVectors.ofObject(staticSource1, "String1", String).copyToArray()
println columnArray[0..9]

// Access a cell by positional index
cellValue = columnArray[0]
println "Value at index 0: ${cellValue}"
```

## Format tables

### Date-time formatting

`Date(...)` in [`formatColumns`](../table-operations/format/format-columns.md) sets how the UI displays a date-time column, using Java date-time pattern letters. The column's data does not change.

```groovy test-set=1 order=null
timeFormatting = staticSource1.view(
    "Time1 = Timestamp",
    "Time2 = Timestamp",
    "Time3 = Timestamp",
    "Time4 = Timestamp",
    "Time5 = Timestamp",
    "Time6 = Timestamp",
)
.formatColumns(
    "Time1 = Date(`yyyy_MM_dd'T'HH-mm-ss.S z`)",
    "Time2 = Date(`yyyy-MM-dd'T'HH:mm z`)",
    "Time3 = Date(`HH:mm:ss`)",
    "Time4 = Date(`HH:mm:ss.SSSSSSSSS`)",
    "Time5 = Date(`EEE dd MMM yy HH:mm:ss`)",
    "Time6 = Date(`yyyy-MM-dd`)",
)
.updateView("Time7String = formatDate(Time6, 'ET')")
```

### Number formatting

```groovy test-set=1 order=null
numberFormatting = staticSource1.view(
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
).formatColumns(
    "BigNum1 = Decimal(`###,#00`)",
    "BigNum2 = Decimal(`##0.00`)",
    "BigNum3 = Decimal(`0.####E0`)",
    "Price1 = Decimal(`###,###.##`)",
    "Price2 = Decimal(`\$###,##0.00`)",
    "Price3 = Decimal(`#,##0`)",
    "SmallNum1 = Decimal(`##0.000000`)",
    "SmallNum2 = Decimal(`##0.00%`)",
    "SmallNum3 = Decimal(`##0%`)",
    "TinyNum = Decimal(`0.00E0`)",
)
```

### Color formatting

```groovy test-set=1 order=null
// set a background color for an entire table
backgroundFormat = staticSource1.formatColumns("* = bgfga(colorRGB(0,0,128))")

// set a color column by column
justColors = staticSource1.formatColumns(
    "X = `#90F060`",
    "Double1 = LIGHTBLUE",
    "Int1 = colorRGB(247,204,0)",
    "Timestamp = colorRGB(247,204,204)",
    "Date = bgfg(colorRGB(57,43,128),colorRGB(243,247,122))",
    "String1 = bgfga(MEDIUMVIOLETRED)",
    "String2 = colorHSL(0, 24, 36)",
)

// column formatting
columnColors = staticSource1.updateView("RowMod10 = i % 10")
    .formatColumnWhere("String1", "Int1 > 35", "DEEP_RED")
    .formatColumnWhere("Double1", "Double1 > Double1_[i-1]", "LIMEGREEN")
    .formatColumns(
        "Int1 = (X % 5 !=0) ? ORANGE : BLACK",
        "Date = (String1 = `A`) ? colorRGB(253, 31, 203) : colorRGB(171, 254, 42)",
        "RowMod10 = heatmap(RowMod10, 0.0, 9.0, BRIGHT_GREEN, BRIGHT_RED)",
    )

// row formatting
rowColors = staticSource1.formatRowWhere("Int1 > 45", "PALE_BLUE")

// one can formatColumns() for numbers and colors together
numberAndColor = staticSource1.formatColumns(
    "Double1 = Decimal(`##0.00`)",
    "Double1 = (Double1 > Double1_[i-1]) ? FUCHSIA : GREY",
)
```

For the full list of stock colors, see [Named colors](../table-operations/format/named-colors.md).

## Use layout hints

[`setLayoutHints`](../table-operations/format/layout-hints.md) returns a new table that tells the UI how to arrange its columns. This example freezes `Even`, moves `Odd` to the front and `B` to the back, hides `C`, and groups `A` and `E` under `Vowels`.

```groovy order=source,result
import io.deephaven.engine.util.LayoutHintBuilder

source = newTable(
    stringCol("A", "A", "a"),
    stringCol("B", "B", "b"),
    stringCol("C", "C", "c"),
    stringCol("D", "D", "d"),
    stringCol("E", "E", "e"),
    stringCol("Y", "Y", "y"),
    intCol("Even", 2, 4),
    intCol("Odd", 1, 3),
)

result = source.setLayoutHints(
    LayoutHintBuilder.get().freeze("Even").atFront("Odd").atBack("B").hide("C").columnGroup("Vowels", ["A", "E"]).build()
)
```

## Plot programmatically

The following examples show only the basics of the built-in plotting API. For styling, labeling, and more, see the [plotting guide](../../how-to-guides/plotting/api-plotting.md).

### Time series plots

```groovy test-set=1 order=null
timeSeries = plot(
    "Time_series", staticSource1.head(100), "Timestamp", "Int1"
).show()

timeSeries2Axis = plot(
    "Int1",
    staticSource1.where("X < 100", "Int1 < 70"),
    "Timestamp",
    "Int1",
    )
    .twinX()
    .plot(
        "Double1",
        staticSource1.where("X < 100", "Double1 > 30"),
        "Timestamp",
        "Double1",
    )
    .show()

timeSeries3Axis = plot(
    "Int1", staticSource1.where("X < 25"), "Timestamp", "Int1"
    )
    .twinX()
    .plot(
        "Double1",
        staticSource1.where("X < 25"),
        "Timestamp",
        "Double1",
    )
    .twinX()
    .plot(
        "DoubleN",
        staticSource1.where("X < 25").update("DoubleN = pow(Double1,2)"),
        "Timestamp",
        "DoubleN",
    )
    .show()
```

### Bar chart

```groovy test-set=1 order=null
barChart = plot(
    "Int1",
    staticSource1.where("X < 41"),
    "Timestamp",
    "Int1"
).plotStyle("bar").show()
```

### Plot by key

Passing grouping columns plots one series for each key.

```groovy test-set=1 order=null
table3Keys = (
    staticSource1.head(2000)
    .where("String1 in `A`, `B`, `C`")
    .sort("Int1")
    .update(
        "NewRow = i", "Y = Int1 * (String1 = `A` ? 2 : String1 = `B` ? 0.5 : 1.0)"
    )
)

p = plotBy(
        "partitioned", table3Keys, "NewRow", "Y", "String1"
    )
    .show()
```

### Histogram

```groovy test-set=1 order=null
histogram = histPlot(
    "Histogram Values",
    staticSource1.where("X < 41"),
    "Int1",
    10,
    )
    .chartTitle("Histogram of Values")
    .show()
```

### Area graph

```groovy test-set=1 order=null
areaGraph = plot(
    "IntCol",
    staticSource1.where("X < 41"),
    "Timestamp",
    "Int1",
).plotStyle("area").show()
```

### Stacked area

```groovy test-set=1 order=null
stackedAreaGraph = plot(
    "Series1",
    staticSource1.where("X < 1000", "String1 = `A`"),
    "Timestamp",
    "Double1"
).plot(
    "Series2",
    staticSource1.where("X < 1000", "String1 = `B`"),
    "Timestamp",
    "Double1"
).plotStyle("stacked_area").show()
```

### Scatter

```groovy test-set=1 order=null
plotXYScatter = plot("Double1", staticSource1.head(50), "Timestamp", "Double1")
    .plotStyle("scatter")
    .pointShape("square")
    .pointSize(10)
    .pointColor("RED")
    .pointLabel("Big Point")
    .twinX()
    .plot("ii", staticSource1.head(50), "Timestamp", "X")
    .plotStyle("scatter")
    .pointShape("diamond")
    .pointSize(16)
    .pointLabel("Big Diamond")
    .pointColor("BLUE")
    .show()
```

## Work with ticking tables

### Wait for table updates

Use [`awaitUpdate`](../table-operations/table-listeners/await-update.md) to wait until a table updates before continuing.

```groovy ticking-table order=null
source = timeTable("PT1S")

println source.awaitUpdate(0)  // 0 ms timeout: returns false without waiting for an update
println source.awaitUpdate(1000)  // waits up to 1000 ms; true if the table updated

result = source.update("RenamedTimestamp = Timestamp")
```

### Convert ticking tables to static tables

Use [`snapshot`](../table-operations/snapshot/snapshot.md) to create a static, in-memory copy of a source table.

```groovy ticking-table order=null
source = timeTable("PT1S")

// Some time later...
result = source.snapshot()
```

### Reduce ticking frequency

[`snapshotWhen`](../table-operations/snapshot/snapshot-when.md) takes a snapshot of the source table each time a trigger table ticks, so the result updates only as often as the trigger. The result stays empty until the trigger table first ticks, unless you pass a `SnapshotWhenOptions` with the initial flag set.

```groovy ticking-table order=null
rand = new Random()

source = timeTable("PT0.5S").update(
    "X = rand.nextInt(0, 101)", "Y = sqrt(X)"
)
trigger = timeTable("PT5S").renameColumns("TriggerTimestamp = Timestamp")

result = source.snapshotWhen(trigger)
```

### Capture the history of ticking tables

To keep history, pass [`snapshotWhen`](../table-operations/snapshot/snapshot-when.md) a [`SnapshotWhenOptions`](/core/javadoc/io/deephaven/api/snapshot/SnapshotWhenOptions.html) whose history flag is set. [`SnapshotWhenOptions.of`](/core/javadoc/io/deephaven/api/snapshot/SnapshotWhenOptions.html) takes the flags in the order initial, incremental, history, so `of(false, false, true, ...)` sets only the history flag. Each time the trigger table ticks, the result appends a full snapshot of the source table's rows instead of replacing the previous snapshot.

```groovy ticking-table order=null
import io.deephaven.api.snapshot.SnapshotWhenOptions

rand = new Random()

myOpts = SnapshotWhenOptions.of(false, false, true, "TriggerTimestamp")

source = timeTable("PT0.01S")
    .update(
        "X = i%2 == 0 ? `A` : `B`",
        "Y = rand.nextInt(0, 101)",
        "Z = sqrt(Y)",
    )
    .lastBy("X")

trigger = timeTable("PT1S").renameColumns("TriggerTimestamp = Timestamp")
result = source.snapshotWhen(trigger, myOpts)
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
- [Groovy variables in query strings](../../how-to-guides/groovy-variables.md)
- [Groovy closures in queries](../../how-to-guides/groovy-closures.md)
- [Apply color formatting to columns](../../how-to-guides/format-columns.md)
- [Built-in plotting API](../../how-to-guides/plotting/api-plotting.md)
- [Write data to an in-memory, real-time table](../../how-to-guides/table-publisher.md)
- [Read CSV or other delimited files](../../how-to-guides/data-import-export/csv-import.md)
- [Read Parquet files into Deephaven tables](../../how-to-guides/data-import-export/parquet-import.md)
- [CSV cheat sheet](./csv.md)
- [Parquet cheat sheet](./parquet.md)
- [Simple table constructors](./simple-table-constructors.md)
- [Time operations cheat sheet](./datetime-cheat-sheet.md)
- [`aggBy`](../table-operations/group-and-aggregate/aggBy.md)
- [`AggAvg`](../table-operations/group-and-aggregate/AggAvg.md)
- [`AggFormula`](../table-operations/group-and-aggregate/AggFormula.md)
- [`AggMax`](../table-operations/group-and-aggregate/AggMax.md)
- [`aj`](../table-operations/join/aj.md)
- [Arrays](../query-language/types/arrays.md)
- [`avgBy`](../table-operations/group-and-aggregate/avgBy.md)
- [`awaitUpdate`](../table-operations/table-listeners/await-update.md)
- [Casting](../query-language/control-flow/casting.md)
- [`col`](../table-operations/create/col.md)
- [`countBy`](../table-operations/group-and-aggregate/countBy.md)
- [`CumSum`](../table-operations/update-by-operations/cum-sum.md)
- [`dropColumns`](../table-operations/select/drop-columns.md)
- [`doubleCol`](../table-operations/create/doubleCol.md)
- [`DynamicTableWriter`](../table-operations/create/DynamicTableWriter.md)
- [`Ema`](../table-operations/update-by-operations/ema.md)
- [`emptyTable`](../table-operations/create/emptyTable.md)
- [`exactJoin`](../table-operations/join/exact-join.md)
- [`firstBy`](../table-operations/group-and-aggregate/firstBy.md)
- [`formatColumns`](../table-operations/format/format-columns.md)
- [`formatColumnWhere`](../table-operations/format/format-column-where.md)
- [`formatRowWhere`](../table-operations/format/format-row-where.md)
- [`groupBy`](../table-operations/group-and-aggregate/groupBy.md)
- [`head`](../table-operations/filter/head.md)
- [`headBy`](../table-operations/group-and-aggregate/headBy.md)
- [`headPct`](../table-operations/filter/head-pct.md)
- [`histPlot`](../plot/histPlot.md)
- [`InputTable`](../table-operations/create/InputTable.md)
- [`intCol`](../table-operations/create/intCol.md)
- [`join`](../table-operations/join/join.md)
- [`lastBy`](../table-operations/group-and-aggregate/lastBy.md)
- [`lazyUpdate`](../table-operations/select/lazy-update.md)
- [`maxBy`](../table-operations/group-and-aggregate/maxBy.md)
- [`medianBy`](../table-operations/group-and-aggregate/medianBy.md)
- [`merge`](../table-operations/merge/merge.md)
- [`mergeSorted`](../table-operations/merge/merge-sorted.md)
- [`meta`](../table-operations/metadata/meta.md)
- [`minBy`](../table-operations/group-and-aggregate/minBy.md)
- [`moveColumns`](../table-operations/select/move-columns.md)
- [`moveColumnsUp`](../table-operations/select/move-columns-up.md)
- [Named colors](../table-operations/format/named-colors.md)
- [`naturalJoin`](../table-operations/join/natural-join.md)
- [`newTable`](../table-operations/create/newTable.md)
- [`parseDuration`](../time/datetime/parseDuration.md)
- [`parseInstant`](../time/datetime/parseInstant.md)
- [`plot`](../plot/plot.md)
- [`plotBy`](../plot/plotBy.md)
- [`plotStyle`](../plot/plotStyle.md)
- [`raj`](../table-operations/join/raj.md)
- [`readCsv`](../data-import-export/CSV/readCsv.md)
- [`readTable`](../data-import-export/Parquet/readTable.md)
- [`renameColumns`](../table-operations/select/rename-columns.md)
- [`reverse`](../table-operations/sort/reverse.md)
- [Ring tables](../table-operations/create/ringTable.md)
- [`RollingSum`](../table-operations/update-by-operations/rolling-sum.md)
- [`rollup`](../table-operations/create/rollup.md)
- [`select`](../table-operations/select/select.md)
- [`selectDistinct`](../table-operations/select/select-distinct.md)
- [`setLayoutHints`](../table-operations/format/layout-hints.md)
- [`snapshot`](../table-operations/snapshot/snapshot.md)
- [`snapshotWhen`](../table-operations/snapshot/snapshot-when.md)
- [`sort`](../table-operations/sort/sort.md)
- [`sortDescending`](../table-operations/sort/sort-descending.md)
- [Special variables](../query-language/variables/special-variables.md)
- [`stdBy`](../table-operations/group-and-aggregate/stdBy.md)
- [`stringCol`](../table-operations/create/stringCol.md)
- [`sumBy`](../table-operations/group-and-aggregate/sumBy.md)
- [`tail`](../table-operations/filter/tail.md)
- [`tailBy`](../table-operations/group-and-aggregate/tailBy.md)
- [`tailPct`](../table-operations/filter/tail-pct.md)
- [`TablePublisher`](../table-operations/create/TablePublisher.md)
- [`timeTable`](../table-operations/create/timeTable.md)
- [`tree`](../table-operations/create/tree.md)
- [`ungroup`](../table-operations/group-and-aggregate/ungroup.md)
- [`update`](../table-operations/select/update.md)
- [`updateBy`](../table-operations/update-by-operations/updateBy.md)
- [`updateView`](../table-operations/select/update-view.md)
- [`upperBin`](../time/datetime/upperBin.md)
- [`varBy`](../table-operations/group-and-aggregate/varBy.md)
- [`view`](../table-operations/select/view.md)
- [`where`](../table-operations/filter/where.md)
- [`whereIn`](../table-operations/filter/where-in.md)
- [`whereNotIn`](../table-operations/filter/where-not-in.md)
- [`writeCsv`](../data-import-export/CSV/writeCsv.md)
- [`writeTable`](../data-import-export/Parquet/writeTable.md)
- [`Filter` Javadoc](/core/javadoc/io/deephaven/api/filter/Filter.html)
- [`SnapshotWhenOptions` Javadoc](/core/javadoc/io/deephaven/api/snapshot/SnapshotWhenOptions.html)
- [`SortColumn` Javadoc](/core/javadoc/io/deephaven/api/SortColumn.html)
- [`Table` Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html)
