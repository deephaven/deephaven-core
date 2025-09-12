---
title: Ultimate cheat sheet
sidebar_label: Ultimate cheat sheet
---

## Necessary data

The named tables in these lines are used throughout this page and should be run first.

```groovy test-set=1 order=null reset
lettersUpp = ["A".."Z"].flatten()

rand = new Random()

getRandomString = { length ->
    letters = ["a".."z", "A".."Z"].flatten()
    StringBuilder resultStr = new StringBuilder()
    for (int i = 0; i < length; i++){
        resultStr.append(randChoice(letters))
    }
    r = resultStr.toString()
    return r
}

randChoice = { array ->
    return array[rand.nextInt(array.size())]}

// Create static tables
staticSource1 = emptyTable(26300).update(
    "X = ii",
    "Double1 = randomDouble(1, 100)",
    "Int1 = randomInt(1, 100)",
    "Timestamp = '2016-01-01T01:00 ET' + i * HOUR",
    "Date = Timestamp.toString()",
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
    "Date = Timestamp.toString()",
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

Columns are created using the following methods:

- [`booleanCol`](../table-operations/create/booleanCol.md)
- [`byteCol`](../table-operations/create/byteCol.md)
- [`charCol`](../table-operations/create/charCol.md)
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

The following code makes a `timeTable` that updates every second.

```groovy ticking-table order=null
result = timeTable("PT1S")
```

### Input tables

Use an [`InputTable`](../table-operations/create/InputTable.md) when you would like to add or modify data in table cells directly.

```groovy order=myInputTable
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable
import io.deephaven.engine.table.TableDefinition

myInputTable = AppendOnlyArrayBackedInputTable.make(
    TableDefinition.of(
        ColumnDefinition.ofString("StringCol"),
        ColumnDefinition.ofDouble("DoubleCol")
    )
)
```

### Ring tables

A [`Ring Table`](/core/javadoc/io/deephaven/engine/table/impl/sources/ring/RingTableTools.html)<!--TODO: link Groovy RingTableTools.of doc when available--> is a table that contains the `n` most recent rows from a source table. If the table is non-static, rows outside of `n` will disappear from the ring table as the source table updates.

```groovy order=tt,rt
import io.deephaven.engine.table.impl.sources.ring.RingTableTools

tt = timeTable("PT1s")
rt = RingTableTools.of(tt, 5)
```

### Tree tables

A [tree table](../table-operations/create/tree.md) has collapsible subsections that can be be expanded in the UI for more detail.

```groovy order=source,result
source = emptyTable(100).updateView("ID = i", "Parent = i == 0 ? NULL_INT : (int)(i / 4)")

result = source.tree("ID", "Parent")
```

### Rollup tables

A [`rollup`](../table-operations/create/rollup.md) table "rolls" several child specifications into one parent specification:

```groovy order=insurance,insuranceRollup
import static io.deephaven.api.agg.Aggregation.AggAvg
import io.deephaven.csv.CsvTools

insurance = CsvTools.readCsv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv"
)

aggList = [AggAvg("bmi", "expenses")]

insuranceRollup = insurance.rollup(aggList, true, "region", "age")
```

## Merge tables

To [`merge`](../table-operations/merge/merge.md) tables, the schema must be identical: same column names, same column data types. This applies to both static and updating tables.

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

## View table metadata

[`meta`](../table-operations/metadata/meta.md) shows the column names, data types, partitions, and groups for the table. It is useful to make sure the schema matches before merging.

```groovy test-set=1 order=null
static1Meta = staticSource1.meta()
```

## Filter

Most queries benefit by starting with filters. Less data generally means better performance.

For SQL developers: In Deephaven, joins are not the primary filtering operation. Use [`where`](../table-operations/filter/where.md), [`whereIn`](../table-operations/filter/where-in.md), and [`whereNotIn`](../table-operations/filter/where-not-in.md).

> [!NOTE]
> Backticks `\` in query strings denote a string within it. Single quotes``'` denote a literal value that gets parsed by the engine.

### Date & Time examples

```groovy test-set=1 ticking-table order=null
todaysData1 = tickingSource.where(
    "Date = calendarDate()"
)  // Date for this is a String
todaysData3 = tickingSource.where(
    "Date = today('Australia/Sydney')" //note singe quotes used to denote a literal
)  // sydney timezone

singleStringDate = staticSource1.where("Date = '2017-01-03'")  // HEAVILY USED!
lessThanDate = staticSource1.where("Date < `2017-01-03`")  // use >=, etc. as well
oneMonthString = staticSource1.where("Date.startsWith(`2017-02`)") // note backticks used to denote a string within a string

singleDbDate = staticSource2.where(
    "formatDate(Timestamp, 'ET') = `2020-03-01`"
)
afterDbDatetime = staticSource1.where("Timestamp > '2017-01-05T10:30:00 ET'")

justBizTime = staticSource1.where("isBusinessTime(Timestamp)")  // HEAVILY USED!
justAugustDatetime = staticSource2.where(
    "monthOfYear(Timestamp, timeZone(`ET`)) = 8"
)
just10AmHour = staticSource1.where(
    "hourOfDay(Timestamp, timeZone(`ET`), false) = 10"
)
justTues = staticSource2.where(
    "dayOfWeek(Timestamp, timeZone(`ET`)).getValue() = 2"
)  // Tuesdays

time1 = parseInstant("2017-08-21T09:45:00 ET")
time2 = parseInstant("2017-08-21T10:10:00 ET")
// You can use inRange to filter times by a range
trades = staticSource1.where("inRange(Timestamp, time1, time2)")
// However, conjunctive filters are faster, sometimes significantly so
trades = staticSource1.where("Timestamp >= time1", "Timestamp <= time2")
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

> [!CAUTION]
> Using `i` and `ii` is not a good idea in non-static use cases, as calculations based on these variables aren't stable.

```groovy test-set=1 order=null
equalsExample = staticSource2.where("round(Int2) = 44")
lessThanExample = staticSource2.where("Double2 < 8.42")
someManipulation = staticSource2.where("(Double2 - Int2) / Int2 > 0.05")
modExample1 = staticSource2.where("i % 10 = 0")  // every 10th row
modExample2 = staticSource2.where("String4.length() % 2 = 0")
// even char-count Tickers
```

### Multiple filters

```groovy test-set=1 order=null
conjunctiveComma = staticSource1.where("Date = `2017-08-23`", "String1 = `A`")
// HEAVILY USED!
conjunctiveAmpersand = staticSource1.where("Date = `2017-08-23` && String1 = `A`")

disjunctiveSameCol = staticSource1.where("Date = `2017-08-23`|| Date = `2017-08-25`")

disjunctiveDiffCol = staticSource1.where("Date = `2017-08-23` || String1 = `A`")

rangeLameWay = staticSource1.where("Date >= `2017-08-21`", "Date <= `2017-08-23`")
inRangeBest = staticSource1.where("inRange(Date, `2017-08-21`, `2017-08-23`)")
// HEAVILY USED!

inRangeBestString = staticSource1.where(
    "inRange((String)String2.substring(0, 1), `A`, `D`)"
)
// starts with letters A - D
```

### where

> [!TIP]
> For SQL developers: In Deephaven, filter your data before joining using [`where`](../table-operations/filter/where.md) operations. Deephaven is optimized for filtering rather than matching.

```groovy order=source,resultSingleFilter,resultOr,resultAnd
source = newTable(
    stringCol("Letter", "A", "C", "F", "B", "E", "D", "A"),
    intCol("Number", NULL_INT, 2, 1, NULL_INT, 4, 5, 3),
    stringCol(
        "Color", "red", "blue", "orange", "purple", "yellow", "pink", "blue"
    ),
    intCol("Code", 12, 14, 11, NULL_INT, 16, 14, NULL_INT),
)

resultSingleFilter = source.where("Color = `blue`")
resultOr = source.where(
    FilterOr.of(Filter.from("Color = `blue`", "Number > 2"))
)  // OR operation - result will have _either_ criteria
resultAnd = source.where(
    "Color = `blue`", "Number > 2"
)  // AND operation - result will have Both_ criteria
```

### whereIn/whereNotIn

```groovy test-set=1 order=null
// run these two queries together
stringSetDriver = staticSource1.renameColumns("String4 = String2").head(10)
whereInExample1 = staticSource2.whereIn(
    stringSetDriver, "String4"
)  // HEAVILY USED!

whereInExample2 = staticSource2.whereIn(staticSource1, "String4 = String2")
// Ticker in staticSource2 within list of USym of staticSource1

whereNotInExample = staticSource2.whereNotIn(
    staticSource1, "String4 = String2"
)
// see the "not"
```

### Nulls and NaNs

```groovy test-set=1 order=null
nullExample = staticSource1.where("isNull(Int1)")  // all data types supported
notNullExample = staticSource1.where("!isNull(Int1)")

nanExample = staticSource1.where("isNaN(Int1)")
notNanExample = staticSource1.where("!isNaN(Int1)")
```

### Head and Tail

Used to reduce the number of rows:

- [`tail`](../table-operations/filter/tail.md)
- [`tailPct`](../table-operations/filter/tail-pct.md)
- [`head`](../table-operations/filter/head.md)
- [`headPct`](../table-operations/filter/head-pct.md)

```groovy test-set=1 order=null
first10kRows = staticSource2.head(10_000)
tickingLastRows = tickingSource.tail(100)

first15Percent = tickingSource.reverse().headPct(0.15)  // the first 15% of rows
last15Percent = staticSource2.tailPct(0.15)  // the last 15% of rows

first10ByKey = staticSource1.headBy(
    10, "String1"
)  // first 10 rows for each String1-key
last10ByKey = staticSource1.tailBy(
    10, "String1"
)  // last 10 rows for each String1-key
```

## Sort

Single direction sorting:

- [`sort`](../table-operations/sort/sort.md)
- [`sortDescending`](../table-operations/sort/sort-descending.md)

Sort on multiple columns or directions:

- [`sort(sortColumns)`](../table-operations/sort/sort.md)

Reverse the order of rows in a table:

- [`reverse`](../table-operations/sort/reverse.md)

```groovy test-set=1 order=null
import io.deephaven.api.SortColumn
import io.deephaven.api.ColumnName

// works for all data types

sortTimeV1 = staticSource1.sort("Timestamp")  // sort ascending
sortTimeV2 = staticSource1.sort("Double1")

sortNumbers = staticSource1.sortDescending("Int1")  // sort descending
sortStrings = staticSource1.sortDescending("String2")

sortTimeV1 = staticSource1.sortDescending(
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

> [!TIP]
> Reversing tables is faster than sorting and is often used in UIs for seeing appending rows at the top of the table.

```groovy syntax
reverseTable = staticSource1.reverse()
reverseUpdating = tickingSource.reverse()
```

## Select and create new columns

### Option 1: Choose and add new columns - Calculate and write to memory

Use [`select`](../table-operations/select/select.md) and [`update`](../table-operations/select/update.md) when data is expensive to calculate or accessed frequently. Results are saved in RAM for faster access, but they take more memory.

```groovy test-set=1 order=null
selectColumns = staticSource2.select("Timestamp", "Int2", "Double2", "String3")
// constrain to only those 4 columns, write to memory
selectAddCol = staticSource2.select(
    "Timestamp", "Int2", "Double2", "String3", "Difference = Double2 - Int2"
)
// constrain and add a new column calculating

selectAndUpdateCol = staticSource2.select(
    "Timestamp", "Int2", "Double2", "String3"
).update("difference = Double2 - Int2")
// add a new column calculating - logically equivalent to previous example
```

### Option 2: Choose and add new columns - Reference a formula and calculate on the fly

Use [`view`](../table-operations/select/view.md) and [`updateView`](../table-operations/select/update-view.md) when formula is quick or only a portion of the data is used at a time. Minimizes RAM used.

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

### Option 3: Add new columns - Reference a formula and calculate on the fly

Use [`lazyUpdate`](../table-operations/select/lazy-update.md) when there are a relatively small number of unique values. On-demand formula results are stored in cache and re-used.

```groovy test-set=1 order=null
lazyUpdateExample = staticSource2.lazyUpdate(
    "Timestamp", "Int2", "Double2", "String3", "Difference = Double2 - Int2"
)
```

### Getting the row number

```groovy test-set=1 order=null
getRowNum = staticSource2.updateView("RowNumInt = i", "RowNumLong = ii")
```

## Do math

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
    "Exp = Double2 * 1E2",
    "Log = Double2 * log(2.0)",
    "Round = round(Double2)",
    "Floor = floor(Double2)",
    "Mod = RandomInt100 % 5",
    "CastInt = (int)AbsDlrVolume",
    "CastLong = (long)AbsDlrVolume",
)
```

## Handle arrays

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
    "InArray = in(45.71, SubArray)",
)
```

# Create a slice or sub-vector

```groovy test-set=1 order=null
sliceAndRolling = staticSource2.update(
    "SubVector = Int2_.subVector(i-2,i+1)",
    "RollingMean = avg(SubVector)",
    "RollingMedian =median(SubVector)",
    "RollingSum = sum(SubVector)",
)
```

## Manipulate time and calendars

```groovy test-set=1 order=null
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
    "AddTime = Timestamp + `PT05:11:04.332`",
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
)
```

<!--TODO: add these lines back in when Chip is done with calendar changes:
# "NextNonBizDay = nextNonBusinessDay()", \
            # "NextBizDayCurrent = nextBusinessDay()", \
 -->

## Bin data

Binning is a great pre-step for aggregating to support the down-sampling or other profiling of data.

```groovy test-set=1 order=null
bins = tickingSource.updateView(
    "PriceBin = upperBin(Double3, 5)",
    "SizeBin = lowerBin(Int3, 10)",
    "TimeBin = upperBin(Timestamp, 200)",
).reverse()
aggBin = bins.view("TimeBin", "Total = Int3 * Double3").sumBy("TimeBin")
```

## Manipulate strings

You can use any of the standard Java String operators in your queries, as the following examples show:

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

```groovy order=source,oneStrMatch,strSetMatch,caseInsensitive,notInExample,containsExample,notContainsExample,startsWithExample,endsWithExample
source = newTable(
    stringCol("Letter", "A", "a", "B", "b", "C", "c", "AA", "Aa", "bB", "C"),
)

oneStrMatch = source.where("Letter = `A`")  // match filter
strSetMatch = source.where("Letter in `A`, `B`, `C`")
caseInsensitive = source.where("Letter icase in `a`, `B`, `c`")
notInExample = source.where("Letter not in `A`, `B`, `C`")  // see "not"
containsExample = source.where("Letter.contains(`A`)")
notContainsExample = source.where("!Letter.contains(`AA`)")
startsWithExample = source.where("Letter.startsWith(`A`)")
endsWithExample = source.where("Letter.endsWith(`C`)")
```

## Use Ternaries (If-Thens)

```groovy test-set=1 order=null
ifThenExample1 = staticSource2.updateView(
    "SimpleTernary = Double2 < 50 ? `smaller` : `bigger`"
)

ifThenExample2 = staticSource2.updateView(
    "TwoLayers = Int2 <= 20 ? `small` : Int2 < 50 ? `medium` : `large`"
)
```

## Create and use custom variables

```groovy
a = 7

result = emptyTable(5).update("Y = a")
```

## Create and use a custom function

See our guides:

- [How to write a Groovy closure](../../how-to-guides/groovy-closures.md)

```groovy test-set=1 order=null
// Create and Use a custom function
mult = { a, b ->
    return a * b
}

grFuncExample = staticSource2.update(
    "M_object = mult(Double2,Int2)", "M_double = (double)mult(Double2,Int2)"
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

### cast numeric types

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
    "ShortMin = (short)MinNumbers ",
    "ByteMin = (byte)MinNumbers ",
)

numbersMinMeta = numbersMin.meta().view("Name", "DataType")
numbersMaxMeta = numbersMax.meta().view("Name", "DataType")
```

### Casting strings

```groovy order=source,result
import static io.deephaven.api.agg.Aggregation.AggFormula

colors = ["Red", "Blue", "Green"]

formulas = [
    "X = 0.1 * i",
    "Y1 = Math.pow(X, 2)",
    "Y2 = Math.sin(X)",
    "Y3 = Math.cos(X)",
]
groupingCols = ["Letter = (i % 2 == 0) ? `A` : `B`", "Color = (String)colors[i % 3]"]

source = emptyTable(40).update(*(formulas + groupingCols))

myAgg = AggFormula(
        "avg(k)",
        "k",
        "AvgY1 = Y1",
        "AvgY2 = Y2",
        "AvgY3 = Y3"
    )

result = source.aggBy(myAgg, "Letter", "Color")
```

## Manipulate columns

```groovy test-set=1 order=null
uniqueValues = staticSource2.selectDistinct("String4")  // show unique set
// works on all data types - careful with doubles, longs
uniqueValuesCombo = staticSource2.selectDistinct("Timestamp", "String4")
// unique combinations of Time with String4

renameStuff = staticSource2.renameColumns("Symbol = String4", "Price = Double2")
dropColumn = staticSource2.dropColumns("X")  // same for drop one or many
dropColumns = staticSource2.dropColumns("X", "Int2", "String3")

putColsAtStart = staticSource2.moveColumnsUp(
    "String4", "Int2"
)  // Makes 1st col(s)
putColsWherever = staticSource2.moveColumns(1, "String4", "Int2")
// makes String4 the 2nd and Int2 the 3rd column
```

## Group and aggregate

See [How to group and ungroup data](../../how-to-guides/grouping-data.md) for more details.

<!--TODO: add group -->

### Simple grouping

```groovy test-set=1 order=groupToArrays,groupMultipleKeys
groupToArrays = staticSource1.groupBy("String2")
// one row per key (i.e. String2), all other columns are arrays
groupMultipleKeys = staticSource1.groupBy("String1", "Date")
// one row for each key-combination (i.e. String1-Date pairs)
```

### Ungrouping

Expands each row so that each value in any array inside that row becomes itself a new row.

```groovy test-set=1 order=null
// Run these lines together
aggByKey = staticSource1.groupBy("Date")
// one row per Date, other fields are arrays from staticSource1
ungroupThatOutput = aggByKey.ungroup()  // no arguments usually
// each array value becomes its own row
// in this case turns aggByDatetimeKey back into staticSource1
```

### Aggregations

- [How to perform dedicated aggregations for groups](../../how-to-guides/dedicated-aggregations.md)
- [How to perform multiple aggregations for groups](../../how-to-guides/combined-aggregations.md)

```groovy test-set=1 order=null
countOfEntireTable = staticSource1.countBy(
    "Count"
)  // single arg returns tot count, you choose name of column output
countOfGroup = staticSource1.countBy("N", "String1")  // N is naming choice

firstOfGroup = staticSource1.firstBy("String1")
lastOfGroup = staticSource1.lastBy("String1")

sumOfGroup = staticSource1.view("String1", "Double1").sumBy("String1")
// non-key field(s) must be numerical
avgOfGroup = staticSource1.view("String1", "Double1", "Int1").avgBy("String1")
stdOfGroup = staticSource1.view("String1", "Double1", "Int1").stdBy("String1")
varOfGroup = staticSource1.view("String1", "Double1", "Int1").varBy("String1")
medianOfGroup = staticSource1.view("String1", "Double1", "Int1").medianBy("String1")
minOfGroup = staticSource1.view("String1", "Double1", "Int1").minBy("String1")
maxOfGroup = staticSource1.view("String1", "Double1", "Int1").maxBy("String1")
// combine aggregations in a single method (using the same key-grouping)
```

## Join data

See [Choose the right join method](../../how-to-guides/joins-exact-relational.md#which-method-should-you-use) for more details.

> [!TIP]
> For SQL developers: in Deephaven, joins are normally used to enrich a data set, not filter. Use [`where`](../table-operations/filter/where.md) to filter your data instead of using a join.

### Joins that get used a lot

#### Natural Join

[`naturalJoin`](../table-operations/join/natural-join.md)

- Returns all the rows of the left table, along with up to one matching row from the right table.
- If there is no match in the right table for a given row, nulls will appear for that row in the columns from the right table.
- If there are multiple matches in the right table for a given row, the query will fail.

`leftTable.naturalJoin(rightTable, columnsToMatch, columnsToAdd)`

> [!IMPORTANT]
> The right table of the join needs to have only one match based on the key(s).

```groovy test-set=1 order=null
// run these together
lastByKeyTable = staticSource1.view("String1", "Int1", "Timestamp")
    .lastBy("String1")
    .renameColumns("Renamed = Int1", "RenamedTime = Timestamp")

// creates 3-column table of String1 + last record of the other 2 cols
joinWithLastPrice = staticSource1.naturalJoin(lastByKeyTable, "String1")
// arguments are (rightTableOfJoin, "JoinKey(s)")
// will have same number of rows as staticSource1 (left table)
// brings all non-key columns from last_price_table to staticSource1
// HEAVILY USED!

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
// sometimes this is necessaryâ€¦
// if there would be 2 cols of same name
// Note the placement of quotes

keysOfDiffNames = staticSource2.view("String3", "Double2").naturalJoin(
    lastByKeyTable, "String3 = String1"
)
// note that String2 in the L table is mapped as the key to String1 in R
// this result has many null records, as there are many String2
// without matching String1
```

#### Multiple Keys

```groovy test-set=1 order=null
// run these together
lastPriceTwoKeys = staticSource1.view("Date", "String1", "Int1").lastBy(
    "Date", "String1"
)
// creates 3-column table of LastDouble for each Date-String1 pair
natJoinTwoKeys = staticSource1.naturalJoin(
    lastPriceTwoKeys, "Date, String1, Int1"
)
// arguments are (rightTableOfJoin, "JoinKey1, JoinKey2", "Col(s)")
// Note the placement of quotes
```

#### AJ (As-Of Join)

[As-of joins](../table-operations/join/aj.md) are time series joins. They can also be used with other ordered numerics as the join key(s). It is often wise to make sure the Right-table is sorted (based on the key). `aj` is designed to find "the exact match" of the key or "the record just before". For timestamp aj-keys, this means "that time or the record just before".

`leftTable = rightTable.aj(columnsToMatch, columnsToAdd)`

[Reverse As-of joins](../table-operations/join/raj.md) `raj` find "the exact match" of the key or "the record just after". For timestamp reverse aj-keys, this means "that time or the record just after".

`result = leftTable.raj(rightTable, columnsToMatch, columnsToAdd)`

```groovy test-set=1 order=null
tickingSource2 = timeTable("PT10.5S").update(
    "X = ii",
    "Double4 = randomDouble(1, 100)",
    "Int4 = randomInt(1, 100)",
    "Date = Timestamp.toString()",
    "String7 = randChoice(lettersUpp)",
    "String8 = getRandomString(randomInt(1, 4))",
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

rajJoin = tickingSource2.raj(
    tickingSource,
    "String7 = String5, Timestamp",
    "String6, JoinTime = Timestamp",
)
```

### Less common joins

[`join`](../table-operations/join/join.md) returns all rows that match between the left and right tables, potentially with duplicates. Similar to SQL inner join.

- Returns only matching rows.
- Multiple matches will have duplicate values, which can result in a long table.

[`exactJoin`](../table-operations/join/exact-join.md)

- Returns all rows of `leftTable`.
- If there are no matching keys, the result will fail.
- Multiple matches will fail.

```groovy test-set=1 order=null
// exact joins require precisely one match in the right table for each key
lastString1Aug23 = staticSource1.where("Date = `2017-08-23`").lastBy("String1")
lastString1Aug24 = staticSource1.where("Date = `2017-08-24`")
    .lastBy("String1")
    .whereIn(lastString1Aug23, "String1")

// filters for String1 in last_string_1aug23
exactJoinEx = lastString1Aug24.exactJoin(
    lastString1Aug23, "String1", "RenamedDouble = Double1"
)


// join will find all matches from left in the right table
// if a match is not found, that row is omitted from the result
// if the Right-table has multiple matches, then multiple rows are included in result
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

## Use columns as arrays and cells as variables

```groovy test-set=1
getAColumn = staticSource1.getColumnSource("String1")
println getAColumn
getACell = getAColumn.get(0)
println getACell
```

## Read and write files

It is very easy to import files and to export any table to a CSV via the UI.

CSVs and Parquet files imported from a server-side directory should be done via the script below - these are NOT working examples. The code below provides syntax, but the filepaths are unknown.

```groovy skip-test
// CSV
import io.deephaven.csv.CsvTools
import io.deephaven.csv.CsvSpecs

// import CSV from a URL
csvImported = CsvTools.readCsv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv"
)

specs = CsvSpecs.builder().hasHeaderRow(false).build()

// import headerless CSV
csvHeaderless = CsvTools.readCsv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv",
    specs,
)

// import headerless CSV then add headings
headers = List.of("Year", "Score", "Title")
specs = CsvSpecs.builder().hasHeaderRow(false).headers(headers).build()
csvManualHeader = CsvTools.readCsv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv",
    specs
)

// !!! WRITE CSV !!!
// export / write CSV to local /data/ directory

CsvTools.writeCsv(csvManualHeader, "/data/outputFile.csv")

// import the local CSV you just exported
csvLocalRead = CsvTools.readCsv("/data/outputFile.csv")

// full syntax
csvSpecsFullMonty = CsvSpecs.builder().headers(headers).hasHeaderRow(false).ignoreSurroundingSpaces(true).trim(false).build()

csvFullMonty = CsvTools.readCsv("https://media.githubusercontent.com/media/deephaven/examples/main/DeNiro/csv/deniro_headerless.csv", csvSpecsFullMonty)

// PARQUET

// write Parquet
import io.deephaven.parquet.table.ParquetTools

tableSample = staticSource1.head(100)
ParquetTools.writeTable(tableSample, "/data/outputGeneric.parquet")

ParquetTools.writeTable(tableSample, "/data/outputGzip.parquet", ParquetTools.GZIP)
// other compression codes are available

// read Parquet

parquetRead = ParquetTools.readTable("/data/outputGeneric.parquet")
parquetOkIfZipped = ParquetTools.readTable("/data/outputGzip.parquet")
```

## Do Cum-Sum and Rolling Average

```groovy test-set=1 order=null
hasOthers = { array, target ->
    for (x in array) {
        if (x != target) {
            return true
        }
    }
    return false
}


////
makeArrays = staticSource2.sort("String3").updateView(
    "String3Array10 = String3_.subVector(i-5, i-1)",
    "Int2Array10 = Int2_.subVector(i-5, i-1)",
    "Double2Array10 = Double2_.subVector(i-5, i-1)",
)
////
cumAndRoll10 = makeArrays.update(
    "ArrayHasOtherString3s = \
    hasOthers.call(String3Array10.toArray(), String3)",
    "CumSum10 = ArrayHasOtherString3s = false ? \
    (int)sum(Double2Array10) : NULL_INT",
    "RollAvg10 = ArrayHasOtherString3s = false ? \
    avg(Int2Array10) : NULL_DOUBLE",
)
```

### Another example of creating a rolling sum

```groovy test-set=1 order=null
sums = trades.updateBy(RollingSum(20, 0, "RollingSumInt1 = Int1"), "String1")
```

### Set up an EMA

```groovy test-set=1 ticking-table order=null
import io.deephaven.numerics.movingaverages.ByEmaSimple
import io.deephaven.numerics.movingaverages.ByEma.BadDataBehavior
import io.deephaven.numerics.movingaverages.AbstractMa.Mode
import java.util.concurrent.TimeUnit

ema30sec = new ByEmaSimple(BadDataBehavior.BD_SKIP, BadDataBehavior.BD_SKIP, Mode.TIME, (double)30, TimeUnit.valueOf("SECONDS"))

emaData = tickingSource.view(
    "EmaData = ema30sec.update(Timestamp, Int3)"
).lastBy()
emaDataGrouped = (
    tickingSource.view(
        "String5", "EmaData = ema30sec.update(Timestamp, Int3, String5)"
    )
    .lastBy("String5")
    .sort("String5")
)
```

```groovy
ctrl = OperationControl.builder().onNullValue(BadDataBehavior.SKIP).build()

ema30sec = Ema(
    ctrl, "TIME", 30, "LEVEL"
)
```

## Format tables

### Datetime formatting

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
    "Time1=Date(`yyyy_MM_dd'T'HH-mm-ss.S z`)",
    "Time2 = Date(`yyyy-MM-dd'T'HH:mm t`)",
    "Time3 = Date(`HH:mm:ss`)",
    "Time4 = Date(`HH:mm:ss.SSSSSSSSS`)",
    "Time5 = Date(`EEE dd MMM yy HH:MM:ss`)",
    "Time6 = Date(`yyyy-MM-dd`)",
)
.updateView("Time7_string = formatDate(Time6, 'ET')")
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
    "Price2 = Decimal(`###,##0.00`)",
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
        "RowMod10 = heatmap(RowMod10, 1.0, 10.0, BRIGHT_GREEN, BRIGHT_RED)",
    )

// row formatting
rowColors = staticSource1.formatRowWhere("Int1 > 45", " PALE_BLUE")

// one can formatColumns() for numbers and colors together
numberAndColor = staticSource1.formatColumns(
    "Double1 = Decimal(`##0.00`)",
    "Double1 = (Double1 > Double1_[i-1]) ? FUCHSIA : GREY",
)
```

You will find an itemization of stock colors in [the docs](../../assets/deephaven-colors.pdf).

## Plot programmatically

Substantial documentation about [`plotting`](../plot/plot.md) exists. The following example intends to show the basics, and everything about styling and labeling is omitted.

### Time series plots

```groovy test-set=1 order=null
timeSeries = plot(
    "Time_series", staticSource1.head(100), "Timestamp", "Int1"
).show()

timeSeries2axis = plot(
    "Int1",
    staticSource1.where("X < 100", "Int1 < 70 "),
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

timeSeries3axis = plot(
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

### Plot-by-some key

```groovy test-set=1 order=null
table3Keys = (
    staticSource1.head(2000)
    .where("String1 in `A`, `B`, `C`")
    .sort("Int1")
    .update(
        "New_Row = i", "Y = Int1 * (String1 = `A` ? 2 : String1 = `B` ? 0.5 : 1.0)"
    )
)

p = plotBy(
        "partitioned", table3Keys, "New_Row", "Y", "String1"
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
).show()
```

### Stacked Area

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
    .pointSize(5)
    .pointColor("RED")
    .pointLabel("Big Point")
    .twinX()
    .plot("ii", staticSource1.head(50), "Timestamp", "X")
    .plotStyle("scatter")
    .pointShape("diamond")
    .pointSize(10)
    .pointLabel("Big Triangle")
    .pointColor("BLUE")
    .show()
```

## Use layout hints

The `setLayoutHints` method creates a new table with the layout specified by the parameters.

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

## Other useful methods

### Wait for table updates

Use [`awaitUpdate`](../table-operations/table-listeners/await-update.md) to instruct Deephaven to wait for updates to a specified table before continuing.

```groovy order=source,result
source = timeTable("PT1S")

println source.awaitUpdate(0)
println source.awaitUpdate(1000)

result = source.update("renamedTimestamp = Timestamp")
```

### Convert dynamic tables to static tables

Uses [`snapshot`](../table-operations/snapshot/snapshot.md) to create a static, in-memory copy of a source table.

```groovy order=source,result
source = timeTable("PT1S")

// Some time later...
result = source.snapshot()
```

### Reduce ticking frequency

Uses [`snapshotWhen`](../table-operations/snapshot/snapshot-when.md) to reduce the ticking frequency.

```groovy ticking-table order=null
rand = new Random()

source = timeTable("PT0.5S").update(
    "X = (int) rand.nextInt(0, 100)", "Y = sqrt(X)"
)
trigger = timeTable("PT5S").renameColumns("TriggerTimestamp = Timestamp")

result = source.snapshotWhen(trigger)
```

### Capture the history of ticking tables

Uses [`snapshotWhen`](../table-operations/snapshot/snapshot-when.md) to capture the history of ticking tables.

```groovy order=source,trigger,result
import io.deephaven.api.snapshot.SnapshotWhenOptions

rand = new Random()

myOpts = SnapshotWhenOptions.of(false, false, true, "TriggerTimestamp")

source = timeTable("PT0.01S")
    .update(
        "X = i%2 == 0 ? `A` : `B`",
        "Y = (int) rand.nextInt(0, 100)",
        "Z = sqrt(Y)",
    )
    .lastBy("X")

trigger = timeTable("PT1S").renameColumns("TriggerTimestamp = Timestamp")
result = source.snapshotWhen(trigger, myOpts)
```

### Use DynamicTableWriter

See our guide [How to write data to an in-memory, real-time table](../../how-to-guides/dynamic-table-writer.md).

```groovy order=null
import io.deephaven.engine.table.impl.util.DynamicTableWriter

ySquareF = { double x ->
    if (x % 2 < 1) {
        return 1.0
    } else {
        return -1.0
    }
}

yTriangleF = { double x ->
    if (x % 2 >= 1) {
        return (x % 1 - 0.5) * 2
    } else {
        return -((x % 1 - 0.5) * 2)
    }
}

columnNames = ["X", "SawToothWave", "SquareWave", "TriangleWave"] as String[]
columnTypes = [double.class, double.class, double.class, double.class] as Class[]
tableWriter = new DynamicTableWriter(columnNames, columnTypes)

waveforms = tableWriter.getTable()


for (int i = 1; i < 201; i++) {
    x = (double)(0.1 * i)
    ySawtooth = (double)((x % 1 - 0.5) * 2)
    ySquare = (double)ySquareF(x)
    yTriangle = (double)yTriangleF(x)
    map = [ "X" : x, "SawToothWave": ySawtooth, "SquareWave": ySquare, "TriangleWave": yTriangle ]
    tableWriter.logRow(map)
    dur = parseDuration("PT0.2s")
    Thread.sleep(dur)
}

newPlot = plot("Sawtooth Wave", waveforms, "X", "SawToothWave")
    .plot("Square Wave", waveforms, "X", "SquareWave")
    .plot("Triangle Wave", waveforms, "X", "TriangleWave")
    .show()
```
