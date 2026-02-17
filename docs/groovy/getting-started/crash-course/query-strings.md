---
title: Deephaven's Query Strings
sidebar_label: Query Strings
---

Deephaven query strings are the primary way of expressing commands directly to the Deephaven engine. They translate the user's intention into compiled code that the engine can execute. These query strings can contain a mix of Java and Groovy code and are the entry point to a universe of powerful built-in tools.

## Syntax

Query strings are just Groovy strings that get passed into table operations. Deephaven **highly** recommends using double quotes to encapsulate query strings.

```groovy test-set=1
t = emptyTable(10).update("NewColumn = 1")
```

Here, the query string `NewColumn = 1` defines a formula for the engine to execute, and the [`update`](../../reference/table-operations/select/update.md) table operation understands that formula as a recipe for creating a new column called `NewColumn` that contains the value of `1` in each row.

## Literals

Query strings often use [literals](https://en.wikipedia.org/wiki/Literal_(computer_programming)). How the engine interprets a literal depends on how it's written in the query string.

- Literals not encapsulated by any special characters are interpreted as booleans, numeric values, column names, or variables.
- Literals encapsulated in backticks (`` ` ``) are interpreted as strings.
- Literals encapsulated in single quotes (`'`) are interpreted as date-time values.

```groovy test-set=1
literals = emptyTable(10).update(
        // Booleans are represented by 'true' and 'false'
        "BooleanCol = true",
        // 32b integers are represented by integers such as '1'
        "IntCol = 1",
        // 64b integers are represented by integers with an 'L' suffix such as '10L'
        "LongCol = 10L",
        // 32b floating point numbers are represented by numbers with an 'f' suffix such as '1.2345f'
        "FloatCol = 1.2345f",
        // 64b floating point numbers are represented by numbers with a decimal point such as '2.3456'
        "DoubleCol = 2.3456",
        // Strings must be enclosed with backticks
        "StringCol = `Hello!`",
        // Date-time values must be enclosed with single quotes
        "DateCol = '2020-01-01'",
        "TimeCol = '12:30:45.000'",
        "DateTimeCol = '2020-01-01T12:30:45.000Z'",
        // Durations must be enclosed with single quotes and have a leading PT
        "DurationCol = 'PT1m'",
        // For object types, 'null' is the literal for null values
        "NullCol = null",
)
```

The [`meta`](../../how-to-guides/metadata.md#get-a-metadata-table-with-meta) method is useful for assessing a table's schema. You can use it to confirm that the resulting columns are of the correct type.

```groovy test-set=1
literalsMeta = literals.meta()
```

## Special variables and constants

Deephaven provides several [special variables and constants](../../reference/query-language/variables/special-variables.md). The most commonly used of these are `i` and `ii`. They represent the row indices as 32-bit and 64-bit integers, `int` and `long`, respectively.

```groovy test-set=1 order=specialMeta,specialVars
specialVars = emptyTable(10).update("IdxInt = i", "IdxLong = ii")

specialMeta = specialVars.meta()
```

> **_NOTE:_** The special variables `i` and `ii` can only be used in [append-only](../../conceptual/table-types.md#specialization-1-append-only) tables.

Additionally, Deephaven provides a range of common constants that can be accessed from query strings. These constants are always written with snake case in capital letters. They include [minimum and maximum values for various data types](/core/javadoc/io/deephaven/util/QueryConstants.html), [conversion factors for time types](/core/javadoc/io/deephaven/time/DateTimeUtils.html), and more. Of particular interest are the null constants for primitive types.

```groovy test-set=1 order=nullValuesMeta,nullValues
nullValues = emptyTable(1).update(
        "StandardNull = null",
        "BoolNull = NULL_BOOLEAN",
        "CharNull = NULL_CHAR",
        "ByteNull = NULL_BYTE",
        "ShortNull = NULL_SHORT",
        "IntNull = NULL_INT",
        "LongNull = NULL_LONG",
        "FloatNull = NULL_FLOAT",
        "DoubleNull = NULL_DOUBLE",
)

nullValuesMeta = nullValues.meta()
```

These are useful for representing and handling null values of a specific type. Built-in query language functions handle null values. For example, `sqrt(NULL_DOUBLE)` returns `NULL_DOUBLE`. Custom functions need to handle null values appropriately.

## Common operations

Numeric types support mathematical operations such as `+`, `-`, `*`, `/`, and `%`.

```groovy test-set=1
mathOps = emptyTable(10).update(
        "Col1 = ii",
        // Math operations work between columns and literals
        "Col2 = 10 * (Col1 + 1.2345) / 4",
        // And they work between columns
        "Col3 = (Col1 - Col2) % 6",
)
```

String concatenation is also supported via the `+` operator.

```groovy test-set=1
addStrings = emptyTable(10).update("StringCol = `Hello ` + `there!`")
```

The `+` and `-` operators are defined for date-time types, making arithmetic on timestamp data easy.

```groovy test-set=1
timeOps = emptyTable(10).update(
        // Times in nanoseconds can be added or subtracted from date-times
        "Timestamp = '2021-07-11T12:00:00.000Z' + (ii * HOUR)",
        // Durations or Periods can be added or subtracted from date-times
        "TimestampPlusOneYear = Timestamp + 'P365d'",
        "TimestampMinusOneHour = Timestamp - 'PT1h'",
        // Timestamps can be subtracted to get their difference in nanoseconds
        // Use constants for unit conversion
        "DaysBetween = ('2023-01-02T20:00:00.000Z' - Timestamp) / DAY",
)
```

Logical operations, expressions, and comparison operators are supported.

```groovy test-set=1
logicalOps = emptyTable(10).update(
        "IdxCol = ii",
        "IsEven = IdxCol % 2 == 0",
        "IsDivisibleBy6 = IsEven && (IdxCol % 3 == 0)",
        "IsGreaterThan6 = IdxCol > 6",
)
```

Deephaven provides an [inline conditional operator (ternary-if)](../../how-to-guides/ternary-if-how-to.md) that makes writing conditional expressions compact and efficient.

```groovy test-set=1
conditional = emptyTable(10).update(
        // Read as '<condition> ? <if-true> : <if-false>'
        "Parity = ii % 2 == 0 ? `Even!` : `Odd...`",
        // Any logical expression is a valid condition
        "IsDivisibleBy6 = ((ii % 2 == 0) && (ii % 3 == 0)) ? true : false",
        // In-line conditionals can be chained together
        "RandomNumber = randomGaussian(0.0, 1.0)",
        "Score = RandomNumber < -1.282 ? `Bottom 10%` : RandomNumber > 1.282 ? `Top 10%` : `Middle of the pack`",
)
```

In Deephaven, typecasting is easy.

```groovy test-set=1
typecasted = emptyTable(10).update(
        "LongCol = ii",
        // Use (type) to cast a column to the desired type
        "IntCol = (int) LongCol",
        "FloatCol = (float) LongCol",
        // Or, use a cast as an intermediate step
        "IntSquare = i * (int) LongCol",
)
```

There are many more such operators supported in Deephaven. See the [guide on operators](../../how-to-guides/operators.md) to learn more.

## Built-in functions

Aside from the common operations, Deephaven hosts a large library of functions known as [built-in or auto-imported functions](../../reference/query-language/query-library/auto-imported-functions.md) that can be used in query strings.

The [numeric subset of this library](/core/javadoc/io/deephaven/function/package-summary.html) is full of functions that perform common mathematical operations on numeric types. These include exponentials, trigonometric functions, random number generators, and more.

```groovy test-set=1
mathFunctions = emptyTable(10).update(
        // Each function can be applied to many different types
        "Exponential = exp(ii)",
        "NatLog = log(i)",
        "Square = pow((float)ii, 2)",
        "Sqrt = sqrt((double)i)",
        "Trig = sin(ii)",
        // Functions can be composed
        "AbsVal = abs(cos(ii))",
        "RandInt = randomInt(0, 5)",
        "RandNormal = randomGaussian(0, 1)",
)
```

These functions can be combined with previously discussed literals and operators to generate complex expressions.

```groovy test-set=1
fakeData = emptyTable(100).update(
        "Group = randomInt(1, 4)",
        "GroupIntercept = Group == 1 ? 0 : Group == 2 ? 5 : 10",
        "GroupSlope = abs(GroupIntercept * randomGaussian(0, 1))",
        "GroupVariance = pow(sin(Group), 2)",
        "Data = GroupIntercept + GroupSlope * ii + randomGaussian(0.0, GroupVariance)",
)
```

The built-in library also contains many functions for [working with date-time values](/core/javadoc/io/deephaven/time/DateTimeUtils.html).

```groovy test-set=1
timeFunctions = emptyTable(10).update(
        // The now() function provides the current time according to the Deephaven clock
        "CurrentTime = now()",
        "Timestamp = CurrentTime + (ii * DAY)",
        // Many time functions require timezone information, typically provided as literals
        "DayOfWeek = dayOfWeek(Timestamp, 'ET')",
        "DayOfMonth = dayOfMonth(Timestamp, 'ET')",
        "Weekend = DayOfWeek == 6 || DayOfWeek == 7 ? true : false",
        "SecondsSinceY2K = nanosToSeconds(Timestamp - '2000-01-01T00:00:00 ET')",
)
```

Functions that begin with `parse` are useful for converting strings to date-time types.

```groovy test-set=1 order=stringMeta,stringDateTimes,convertedMeta,convertedDateTimes
stringDateTimes = emptyTable(10).update(
        "DateTime = `2022-04-03T19:34:22.000 UTC`",
        "Date = `2013-07-07`",
        "Time = `14:04:39.123`",
        "Duration = `PT6m`",
        "Period = `P10d`",
        "TimeZone = `America/Chicago`",
)

convertedDateTimes = stringDateTimes.update(
        "DateTime = parseInstant(DateTime)",
        "Date = parseLocalDate(Date)",
        "Time = parseLocalTime(Time)",
        "Duration = parseDuration(Duration)",
        "Period = parsePeriod(Period)",
        "TimeZone = parseTimeZone(TimeZone)",
)

stringMeta = stringDateTimes.meta()
convertedMeta = convertedDateTimes.meta()
```

[`upperBin`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#upperBin(java.time.Instant,long)) and [`lowerBin`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long)) bin timestamps into buckets. They are particularly useful in aggregation operations, as aggregated statistics are commonly computed over temporal buckets.

```groovy test-set=1 order=binnedTimestamps,lastByBin
binnedTimestamps = emptyTable(60).update(
        "Timestamp = now() + ii * MINUTE",
        // Rounds Timestamp to the last 5 minute mark
        "Last5Mins = lowerBin(Timestamp, 5 * MINUTE)",
        // Rounds Timestamp to the next 10 minute mark
        "Next10Mins = upperBin(Timestamp, 10 * MINUTE)",
        "Value = random()",
)

lastByBin = binnedTimestamps.lastBy("Last5Mins")
```

The [time user guide](../../conceptual/time-in-deephaven.md) provides a comprehensive overview of working with date-time data in Deephaven.

These functions provide only a glimpse of what the built-in library offers. There are modules for [sorting](/core/javadoc/io/deephaven/function/Sort.html), [searching](/core/javadoc/io/deephaven/function/BinSearch.html), [string parsing](/core/javadoc/io/deephaven/function/Parse.html), [null handling](https://deephaven.io/core/javadoc/io/deephaven/function/Basic.html#isNull(byte)), and much more. See the document on [auto-imported functions](../../reference/query-language/query-library/auto-imported-functions.md) for a comprehensive list of what's available or the [module summary page](/core/javadoc/io/deephaven/function/package-summary.html) for a high-level overview of what's offered.

## Java methods

The data structures that underlie Deephaven tables are Java data structures. So, many of the literals, operators, and functions we've spoken about are, at some level, Java. Java objects have methods attached to them that can be called from query strings, unlocking new levels of functionality and efficiency for Deephaven users.

To discover these, use [`meta`](../../reference/table-operations/metadata/meta.md) to inspect a column's underlying data type.

```groovy test-set=1 order=callMethodsMeta,callMethods
callMethods = emptyTable(1).update("Timestamp = '2024-03-03T15:00:00.000 UTC'")

callMethodsMeta = callMethods.meta()
```

This column is a Java [Instant](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/Instant.html). Java's [documentation](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/Instant.html) provides all of the available [methods](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/Instant.html#method.summary) that can be called. Here are just a few.

```groovy test-set=1
callMethods = callMethods.update(
        "SecondsSinceEpoch = Timestamp.getEpochSecond()",
        "TimestampInTokyo = Timestamp.atZone('Asia/Tokyo')",
        "StringRepresentation = Timestamp.toString()",
)
```

Additionally, there are several ways to create Java objects for use in query strings. The following example uses the `new` keyword to create new instances of Java's [URL](https://docs.oracle.com/javase/7/docs/api/java/net/URL.html) class.

```groovy test-set=1 order=t1,m1
// Use 'new' keyword to create a new URL object, then call methods
t1 = emptyTable(2).update(
        "URL = new java.net.URL(`https://deephaven.io:` + i)",
        "Protocol = URL.getProtocol()",
        "Host = URL.getHost()",
        "Port = URL.getPort()",
        "URI = URL.toURI()",
)
m1 = t1.meta()
```

## Arrays

Deephaven tables can have array columns. Array columns often come from a [`groupBy`](../../reference/table-operations/group-and-aggregate/groupBy.md) operation.

```groovy test-set=1 order=t,tGrouped
t = emptyTable(10).update("Group = ii % 2 == 0 ? `A` : `B`", "X = ii")

tGrouped = t.groupBy("Group")
```

Many built-in functions support array arguments.

```groovy test-set=1
tArrayFuncs = tGrouped.update(
        "IsEven = X % 2 == 0",
        "PlusOne = X + 1",
        "GroupSum = sum(X)",
        "GroupAvg = avg(X)",
        "NumUnique = countDistinct(X)",
)
```

These results can then be ungrouped with [`ungroup`](../../reference/table-operations/group-and-aggregate/ungroup.md), which is essentially the inverse of [`groupBy`](../../reference/table-operations/group-and-aggregate/groupBy.md).

```groovy test-set=1
tArrayFuncsUngrouped = tArrayFuncs.ungroup()
```

> **_NOTE:_** Aggregations done with Deephaven's [Aggregations](../../how-to-guides/combined-aggregations.md) are more performant than with array functions.

Deephaven provides array indexing and slicing operations.

```groovy test-set=1
tIndexed = tGrouped.update(
        "First = X[0]",
        "Third = X[2]",
        // Note that .subVector() includes the first arg and does not include the second
        "FirstThree = X.subVector(0, 3)",
        "MiddleThree = X.subVector(1, 4)",
        // Indexing outside the range returns null
        "OffTheFront = X[-1]",
        "OffTheEnd = X.subVector(3,6)",
)
```

Lastly, the columns of a Deephaven table can be interpreted as arrays.

```groovy test-set=1
columnAsArray = emptyTable(10).update(
        "X = ii",
        // To interpret X as an array, use a trailing underscore
        "ArrX = X_",
        // Functions that take array inputs can now be used
        "SumX = sum(X_)",
        "AvgX = avg(X_)",
        // Indexing and slicing work just the same
        "XMinus2 = X_[ii-2]",
        "RollingGroup = X_.subVector(i, i+3)",
        "RollingAvg = avg(RollingGroup)",
)
```

This functionality is only supported for static and append-only ticking tables. See [working with arrays](../../how-to-guides/work-with-arrays.md) for more information.

## Groovy in query strings

Groovy objects can be used in query strings. Variables are the simplest case.

```groovy test-set=1
a = 1
b = 2

addVars = emptyTable(1).update("Sum = a + b")
```

Groovy functions can also be used.

```groovy test-set=1
mySum = { x, y -> x + y }

a = 1
b = 2

addVarsFunc = emptyTable(1).update("Sum1 = mySum(1, 2)", "Sum2 = mySum(a, b)")
```

So can classes.

<!--TODO: next two examples, there is supposed to be a static and a non-static sum. I could not get the static method to work.-->

```groovy test-set=1
public class MyMathClass {

    public int x

    public MyMathClass(x) {
        this.x = x
    }

    public sum(y) {
        return this.x + y
    }

    public classSum(x, y) {
        return x + y
    }

    public static staticSum(x, y) {
        return x + y
    }
}

// import the class into the Query Library
// this enables the class methods to be called inside query strings
import io.deephaven.engine.context.ExecutionContext
ExecutionContext.getContext().getQueryLibrary().importClass(MyMathClass.class)

a = 1
b = 2
classInstance = new MyMathClass(a)

addVarsClass = emptyTable(1).update(
        "Sum1 = classInstance.sum(b)",
        "Sum2 = classInstance.classSum(a, b)",
        "Sum3 = MyMathClass.staticSum(a, b)",
)
```

Without any type casts, the Deephaven query engine cannot infer what datatype results from a Groovy function. It stores the result as a Java [Object](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html).

```groovy test-set=1 order=addVarsMeta,addVarsFuncMeta,addVarsClassMeta
addVarsMeta = addVars.meta()
addVarsFuncMeta = addVarsFunc.meta()
addVarsClassMeta = addVarsClass.meta()
```

This isn't ideal, as it doesn't support many of the DQL features we've covered. To rectify this, Groovy functions should utilize [casting](../../reference/query-language/control-flow/casting.md).

```groovy test-set=2 order=addVarsClass
public class MyMathClass {

    public int x

    public MyMathClass(x) {
        this.x = x
    }

    public sum(y) {
        return this.x + y
    }

    public classSum(x, y) {
        return x + y
    }

    public static staticSum(x, y) {
        return x + y
    }
}

// import the class into the Query Library
// this enables the class methods to be called inside query strings
import io.deephaven.engine.context.ExecutionContext
ExecutionContext.getContext().getQueryLibrary().importClass(MyMathClass.class)

a = 1
b = 2
classInstance = new MyMathClass(a)

addVarsClass = emptyTable(1).update(
        "Sum1 = (int)classInstance.sum(b)",
        "Sum2 = (int)classInstance.classSum(a, b)",
        "Sum3 = (int)MyMathClass.staticSum(a, b)",
)
```

> [!NOTE]
> In the two queries above, we used `ExecutionContext.getContext().getQueryLibrary().importClass(MyMathClass.class)` to import our class into the query library. This is a quick and easy way to make a user-defined class available in query strings. However, it is not best practice. It is recommended to define classes in their own Groovy files, and import those files via the `docker-compose.yml` file at startup. For an in-depth guide on how to do this, see [here](../../how-to-guides/install-and-use-java-packages.md).

To learn more about using Groovy in query strings, see the user guides on [functions](../../how-to-guides/groovy-closures.md) and [classes](../../how-to-guides/groovy-classes.md#classes-and-objects-in-groovy).

In Groovy, new variables are global by default, but Deephaven provides tools that allow users finer control over the scoping of variables and queries.

```groovy test-set=2 order=result1,result2
// Define the function f
f = { a, b ->
    return a * b
}

int1 = 10
int2 = 3

compute = {sourceTable, anInteger ->
    // add our integer to the query scope
    QueryScope.addParam("intInScope", anInteger)
    return sourceTable.update("X = f(intInScope, A)")
}

// Create an empty table and update it with values
table = emptyTable(5).update("A = i * 2")

// Perform the computations
result1 = compute(table, int1)
result2 = compute(table, int2)
```

For more information, see the [scoping rules](../../how-to-guides/query-scope.md).

Be mindful of whether or not Groovy functions are stateless or stateful. Generally, stateless functions have no side effects - they don't modify any objects outside of their scope. Also, they are invariant to execution order, so function calls can be evaluated in any order without affecting the result. This stateless function extracts elements from a list in a query string.

```groovy test-set=2
myList = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


getElementStateless = { idx ->
    return myList[idx]
}

tStateless = emptyTable(10).update("X = getElementStateless(ii)")
```

`getElementStateless` is stateless because it does not modify any objects outside its local scope. It could be evaluated in any order and give the same result.

Stateful functions modify objects outside their local scope - they do not leave the world as they found it. They also may depend on execution order. This stateful function achieves the same resulting table.

```groovy test-set=2
myList = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
idx = 0

getElementStateful = {
    idx += 1  // This modifies idx!
    return myList[idx - 1]
}

tStateful = emptyTable(10).update("X = getElementStateful()")
```

Print `idx` to verify it's been changed.

```groovy test-set=2
println idx
```

Since `getElementStateful` is stateful, it must be evaluated in the correct order to give the correct result.

Queries should use stateless functions whenever possible because:

- They minimize side effects when called.
- They are deterministic.
- They can be efficiently parallelized.
