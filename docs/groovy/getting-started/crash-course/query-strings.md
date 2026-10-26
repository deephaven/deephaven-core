---
title: Deephaven's Query Strings
---

Deephaven query strings are the primary way of expressing commands directly to the Deephaven engine. They tell the engine what to compute, and the engine turns them into filters and formulas, compiling code where needed. Query strings use Java-like syntax and can use Groovy variables, closures, and classes defined in your session. They are the entry point to a universe of powerful built-in tools, starting with Deephaven's built-in function library.

## Syntax

Query strings are Groovy strings that you pass to table operations. Deephaven **highly** recommends using double quotes for query strings, because query strings use single quotes for their own date-time and `char` literals.

```groovy test-set=1
t = emptyTable(10).update("NewColumn = 1")
```

Here, the query string `NewColumn = 1` defines a formula for the engine to execute, and the [`update`](../../reference/table-operations/select/update.md) table operation understands that formula as a recipe for creating a new column called `NewColumn` that contains the value of `1` in each row.

## Literals

Query strings often use [literals](https://en.wikipedia.org/wiki/Literal_(computer_programming)). How the engine interprets a literal depends on how it's written in the query string. The engine reads:

- Unquoted literals as booleans, numbers, or `null`. Other unquoted names refer to columns or variables.
- Literals in backticks (`` ` ``) as strings.
- Literals in single quotes (`'`) as date-time values, such as dates, times, durations, periods, and time zones. A single character in single quotes is a `char`.

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
        // Durations are enclosed with single quotes and use the ISO-8601 time-based format, such as 'PT1m'
        "DurationCol = 'PT1m'",
        // For object types, 'null' is the literal for null values
        "NullCol = null",
)
```

The [`meta`](../../how-to-guides/metadata.md#meta) method is useful for assessing a table's schema. You can use it to confirm that the resulting columns are of the correct type.

```groovy test-set=1
literalsMeta = literals.meta()
```

## Special variables and constants

Deephaven provides several [special variables and constants](../../reference/query-language/variables/special-variables.md). The most commonly used of these are `i` and `ii`. They represent the row position as 32-bit and 64-bit integers, `int` and `long`, respectively.

```groovy test-set=1 order=specialMeta,specialVars
specialVars = emptyTable(10).update("IdxInt = i", "IdxLong = ii")

specialMeta = specialVars.meta()
```

> [!NOTE]
> The special variables `i` and `ii` are safe to use on static tables, on [append-only](../../conceptual/table-types.md#specialization-1-append-only) tables (ticking tables that only add rows at the end), and on [blink](../../conceptual/table-types.md#specialization-3-blink) tables. On other ticking tables, the engine throws an error. See [refreshing table restrictions](../../reference/query-language/variables/special-variables.md#refreshing-table-restrictions).

Deephaven also provides a range of common constants that you can use in query strings. Their names are in uppercase snake case, such as `NULL_INT`. They include [minimum and maximum values for various data types](/core/javadoc/io/deephaven/util/QueryConstants.html), [conversion factors for time types](/core/javadoc/io/deephaven/time/DateTimeUtils.html), and more. Of particular interest are the null constants for primitive types.

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

These are useful for representing and handling null values of a specific type. Built-in numeric functions generally handle null values. For example, `sqrt(NULL_DOUBLE)` returns `NULL_DOUBLE`. Custom functions need to handle null values appropriately.

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

The `+` operator also concatenates strings.

```groovy test-set=1
addStrings = emptyTable(10).update("StringCol = `Hello ` + `there!`")
```

The `+` and `-` operators are defined for date-time types, making arithmetic on timestamp data easy.

```groovy test-set=1
timeOps = emptyTable(10).update(
        // Times in nanoseconds can be added to or subtracted from date-times
        "Timestamp = '2021-07-11T12:00:00.000Z' + (ii * HOUR)",
        // Durations or day-based Periods can be added to or subtracted from date-times
        "TimestampPlus365Days = Timestamp + 'P365d'",
        "TimestampMinusOneHour = Timestamp - 'PT1h'",
        // Timestamps can be subtracted to get their difference in nanoseconds
        // Use constants for unit conversion
        "DaysBetween = ('2023-01-02T20:00:00.000Z' - Timestamp) / DAY",
)
```

Query strings support logical operators such as `&&` and `||`, and comparison operators such as `==` and `>`.

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
        // randomGaussian is a built-in function; the next section covers these
        "RandomNumber = randomGaussian(0.0, 1.0)",
        // In-line conditionals can be chained together
        "Score = RandomNumber < -1.282 ? `Bottom 10%` : RandomNumber > 1.282 ? `Top 10%` : `Middle of the pack`",
)
```

In Deephaven, typecasting is easy: query strings support Java-style [typecasts](../../reference/query-language/control-flow/casting.md).

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

Deephaven supports many more operators. See the [guide on operators](../../how-to-guides/operators.md) to learn more.

## Built-in functions

Aside from the common operations, Deephaven hosts a large library of functions known as [built-in or auto-imported functions](../../reference/query-language/query-library/auto-imported/index.md) that you can use in query strings.

The [numeric subset of this library](/core/javadoc/io/deephaven/function/Numeric.html) is full of functions that perform common mathematical operations on numeric types. These include exponentials, trigonometric functions, and more. [Random number generators](/core/javadoc/io/deephaven/function/Random.html) are also available.

```groovy test-set=1
mathFunctions = emptyTable(10).update(
        // Each function can be applied to many different types
        "Exponential = exp(ii)",
        "NatLog = log(i)",
        "Square = pow((float) ii, 2)",
        "Sqrt = sqrt((double) i)",
        "Trig = sin(ii)",
        // Functions can be composed
        "AbsVal = abs(cos(ii))",
        "RandInt = randomInt(0, 5)",
        "RandNormal = randomGaussian(0, 1)",
)
```

Combine these functions with the literals and operators covered earlier to build complex expressions.

```groovy test-set=1
fakeData = emptyTable(100).update(
        "Group = randomInt(1, 4)",
        "GroupIntercept = Group == 1 ? 0 : Group == 2 ? 5 : 10",
        "GroupSlope = Group == 1 ? 0.5 : Group == 2 ? 1.0 : 2.0",
        "GroupStdDev = pow(sin(Group), 2)",
        "Data = GroupIntercept + GroupSlope * ii + randomGaussian(0.0, GroupStdDev)",
)
```

The previous chapter, [Time and Calendars](./time-calendars.md), used several of the built-in date-time functions, such as [`now`](../../reference/time/datetime/now.md), [`upperBin`](../../reference/time/datetime/upperBin.md), and [`lowerBin`](../../reference/time/datetime/lowerBin.md). The library contains many more [functions for working with date-time values](/core/javadoc/io/deephaven/time/DateTimeUtils.html).

```groovy test-set=1
timeFunctions = emptyTable(10).update(
        // The now() function provides the current time according to the Deephaven clock
        "CurrentTime = now()",
        "Timestamp = CurrentTime + (ii * DAY)",
        // Many time functions require timezone information, typically provided as literals
        "DayOfWeek = dayOfWeekValue(Timestamp, 'ET')",
        "DayOfMonth = dayOfMonth(Timestamp, 'ET')",
        "Weekend = DayOfWeek == 6 || DayOfWeek == 7 ? true : false",
        "SecondsSinceY2K = nanosToSeconds(Timestamp - '2000-01-01T00:00:00 ET')",
)
```

Date-time functions that begin with `parse`, such as [`parseInstant`](../../reference/time/datetime/parseInstant.md) and [`parseDuration`](../../reference/time/datetime/parseDuration.md), convert strings to date-time types.

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

The [time user guide](../../conceptual/time-in-deephaven.md) provides a comprehensive overview of working with date-time data in Deephaven.

These functions provide only a glimpse of what the built-in library offers. There are modules for [sorting](/core/javadoc/io/deephaven/function/Sort.html), [searching](/core/javadoc/io/deephaven/function/BinSearch.html), [string parsing](/core/javadoc/io/deephaven/function/Parse.html), [null handling](/core/javadoc/io/deephaven/function/Basic.html#isNull(byte)), and much more. See the document on [auto-imported functions](../../reference/query-language/query-library/auto-imported/index.md) for a comprehensive list of what's available or the [module summary page](/core/javadoc/io/deephaven/function/package-summary.html) for a high-level overview of what's offered.

## Java methods

The data structures that underlie Deephaven tables are Java data structures. Many of the literals, operators, and functions covered so far are Java under the hood. Query strings can call methods on Java objects, unlocking new levels of functionality and efficiency.

To find out which methods a column's values have, first check the column's Java type with [`meta`](../../reference/table-operations/metadata/meta.md), as in the [Literals](#literals) section.

```groovy test-set=1 order=callMethodsMeta,callMethods
callMethods = emptyTable(1).update("Timestamp = '2024-03-03T15:00:00.000 UTC'")

callMethodsMeta = callMethods.meta()
```

This column is a Java [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html). Its Javadoc lists every [method](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html#method-summary) you can call. Here are a few.

```groovy test-set=1
callMethods = callMethods.update(
        "SecondsSinceEpoch = Timestamp.getEpochSecond()",
        "TimestampInTokyo = Timestamp.atZone('Asia/Tokyo')",
        "StringRepresentation = Timestamp.toString()",
)
```

Additionally, you can create Java objects for use in query strings with the `new` keyword. The following example creates new instances of Java's [`URI`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/net/URI.html) class.

```groovy test-set=1 order=t1,m1
// Use 'new' keyword to create a new URI object, then call methods
t1 = emptyTable(2).update(
        "URI = new java.net.URI(`https://deephaven.io:` + i)",
        "Scheme = URI.getScheme()",
        "Host = URI.getHost()",
        "Port = URI.getPort()",
        "URL = URI.toURL()",
)
m1 = t1.meta()
```

## Arrays

Deephaven tables can have array columns. Array columns often come from a [`groupBy`](../../reference/table-operations/group-and-aggregate/groupBy.md) operation.

```groovy test-set=1 order=t,tGrouped
t = emptyTable(10).update("Group = ii % 2 == 0 ? `A` : `B`", "X = ii")

tGrouped = t.groupBy("Group")
```

Many operators and built-in functions support array arguments. Operators apply element by element, while functions such as `sum` and `avg` reduce the array to a single value.

```groovy test-set=1
tArrayFuncs = tGrouped.update(
        "IsEven = X % 2 == 0",
        "PlusOne = X + 1",
        "GroupSum = sum(X)",
        "GroupAvg = avg(X)",
        "NumUnique = countDistinct(X)",
)
```

> [!NOTE]
> On ticking tables, [aggregations](../../how-to-guides/combined-aggregations.md) are more performant than [`groupBy`](../../reference/table-operations/group-and-aggregate/groupBy.md) followed by array functions.

[`ungroup`](../../reference/table-operations/group-and-aggregate/ungroup.md) expands the array columns back into separate rows.

```groovy test-set=1
tArrayFuncsUngrouped = tArrayFuncs.ungroup()
```

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
        "OffTheEnd = X[5]",
)
```

Lastly, query strings can treat the columns of a Deephaven table as arrays.

```groovy test-set=1
columnAsArray = emptyTable(10).update(
        "X = ii",
        // To interpret X as an array, use a trailing underscore
        "ArrX = X_",
        // Functions that take array inputs can now be used
        "SumX = sum(X_)",
        "AvgX = avg(X_)",
        // Indexing and slicing work just the same
        "XMinus2 = X_[ii - 2]",
        "NextThree = X_.subVector(i, i + 3)",
        "NextThreeAvg = avg(NextThree)",
)
```

Simple constant-offset access such as `X_[ii - 2]` works on all tables, even though it uses `ii`. The engine recognizes this pattern as an exception to the `i` and `ii` restriction described in [Special variables and constants](#special-variables-and-constants). Other uses of column arrays, such as `sum(X_)` or `X_.subVector(i, i + 3)`, work only on static and blink tables. On any other ticking table, including an append-only table, the engine throws an error. See [working with arrays](../../how-to-guides/work-with-arrays.md) and the [special variables reference](../../reference/query-language/variables/special-variables.md#refreshing-table-restrictions) for more information.

## Groovy in query strings

Query strings can use Groovy objects. Variables are the simplest case.

```groovy test-set=1
a = 1
b = 2

addVars = emptyTable(1).update("Sum = a + b")
```

Query strings can also call Groovy closures.

```groovy test-set=1
mySum = { x, y -> x + y }

a = 1
b = 2

addVarsFunc = emptyTable(1).update("Sum1 = mySum(1, 2)", "Sum2 = mySum(a, b)")
```

So can classes.

```groovy test-set=1
public class MyMathClass {

    public int x

    public MyMathClass(x) {
        this.x = x
    }

    public sum(y) {
        return this.x + y
    }

    public instanceSum(x, y) {
        return x + y
    }

    public static staticSum(x, y) {
        return x + y
    }
}

// Import the class into the query library so query strings can call its methods
ExecutionContext.getContext().getQueryLibrary().importClass(MyMathClass.class)

a = 1
b = 2
classInstance = new MyMathClass(a)

addVarsClass = emptyTable(1).update(
        "Sum1 = classInstance.sum(b)",
        "Sum2 = classInstance.instanceSum(a, b)",
        "Sum3 = MyMathClass.staticSum(a, b)",
)
```

> [!NOTE]
> This example uses `ExecutionContext.getContext().getQueryLibrary().importClass(MyMathClass.class)` to import the class into the query library of the current [`ExecutionContext`](../../conceptual/execution-context.md). This is a quick way to make a user-defined class available in query strings. For production code, see [Import packages with user-defined functions](../../how-to-guides/groovy-closures.md#import-packages-with-user-defined-functions) to put classes in their own Groovy files on the classpath.

Closures don't expose a return type, and the class methods above don't declare one, so the engine can't infer what data type they return. It stores each result as a Java [`Object`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html). Plain Groovy variables are different: the engine knows their types, so the `Sum` column in `addVars` is an `int`.

```groovy test-set=1 order=addVarsMeta,addVarsFuncMeta,addVarsClassMeta
addVarsMeta = addVars.meta()
addVarsFuncMeta = addVarsFunc.meta()
addVarsClassMeta = addVarsClass.meta()
```

This isn't ideal, as [`Object`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html) columns don't support many of the query language features we've covered. To fix this, [cast](../../reference/query-language/control-flow/casting.md) the results of Groovy closures and methods in the query string. The following example casts the results of `mySum` and the `MyMathClass` methods defined above.

```groovy test-set=1 order=addVarsFuncTypedMeta,addVarsFuncTyped,addVarsClassTypedMeta,addVarsClassTyped
addVarsFuncTyped = emptyTable(1).update("Sum1 = (int) mySum(1, 2)", "Sum2 = (int) mySum(a, b)")

addVarsClassTyped = emptyTable(1).update(
        "Sum1 = (int) classInstance.sum(b)",
        "Sum2 = (int) classInstance.instanceSum(a, b)",
        "Sum3 = (int) MyMathClass.staticSum(a, b)",
)

addVarsFuncTypedMeta = addVarsFuncTyped.meta()
addVarsClassTypedMeta = addVarsClassTyped.meta()
```

To learn more about using Groovy in query strings, see the user guides on [functions](../../how-to-guides/groovy-closures.md) and [classes](../../how-to-guides/groovy-classes.md#classes-and-objects-in-groovy).

## Scoping

Query strings resolve names from the [query scope](../../how-to-guides/query-scope.md), the set of variables the engine can see when it compiles a query string. Variables you assign at the top level of a Groovy script without `def` or a type, such as `int1 = 10`, are in the query scope automatically. Variables declared with `def` or a type are not in the query scope, and neither are closure parameters. To use one of those in a query string, add it to the query scope with [`QueryScope.addParam`](../../how-to-guides/query-scope.md#manually).

```groovy test-set=2 order=result1,result2
// Define the function f
f = { a, b ->
    return a * b
}

int1 = 10
int2 = 3

compute = { sourceTable, anInteger ->
    // Add the integer to the query scope
    QueryScope.addParam("intInScope", anInteger)
    return sourceTable.update("X = (int) f(intInScope, A)")
}

// Create an empty table and update it with values
table = emptyTable(5).update("A = i * 2")

// Perform the computations
result1 = compute(table, int1)
result2 = compute(table, int2)
```

`compute` adds `anInteger` to the query scope as `intInScope`, so `result1` uses 10 and `result2` uses 3.

For more information, see the [scoping rules](../../how-to-guides/query-scope.md).

## Stateless and stateful functions

Know whether each Groovy closure you call in a query string is stateless or stateful. A stateless function has no side effects: it doesn't modify any object outside its own scope. Its result also doesn't depend on execution order, so the engine can evaluate the calls in any order. The following stateless function extracts elements from a list.

```groovy test-set=2
myList = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

getElementStateless = { idx ->
    return myList[idx]
}

tStateless = emptyTable(10).update("X = getElementStateless(ii)")
```

`getElementStateless` is stateless because it reads `myList` but never changes it. It could be evaluated in any order and give the same result.

A stateful function modifies objects outside its local scope: it doesn't leave the world as it found it. Its result can also depend on execution order. The engine assumes functions in query strings are stateless, so it may evaluate rows in any order, including in parallel.

To make the engine evaluate a query string in row order, wrap it with [`Selectable.parse`](/core/javadoc/io/deephaven/api/Selectable.html#parse(java.lang.String)) and call [`withSerial`](../../conceptual/query-engine/parallelization.md) on the result. In the following example, the `update` call that uses a stateful closure produces the same table as `tStateless` because the query string is marked serial.

```groovy test-set=2
myList = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
idx = 0

getElementStateful = {
    idx += 1  // This modifies idx!
    return myList[idx - 1]
}

tStateful = emptyTable(10).update(
    // update accepts a list of Selectable objects, so wrap the serial Selectable in List.of
    List.of(Selectable.parse("X = getElementStateful()").withSerial())
)
```

Print `idx` to verify that `getElementStateful` changed it.

```groovy test-set=2 order=:log
println idx
```

Queries should use stateless functions whenever possible because:

- They minimize side effects when called.
- Their result does not depend on the order of evaluation, so you don't need [`withSerial`](../../conceptual/query-engine/parallelization.md).
- The engine is free to choose the execution order, including evaluating them in parallel where it supports it.

This chapter covered the building blocks of query strings: literals, special variables and constants, operators, built-in functions, Java methods, arrays, and Groovy objects. It also showed how query strings resolve variables and why you should prefer stateless functions. Next, [Recipes, not loops!](./vectorization-vs-loops.md) explains why Deephaven queries describe calculations as column formulas rather than loops over rows.
