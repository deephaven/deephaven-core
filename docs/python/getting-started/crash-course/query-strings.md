---
title: Deephaven's Query Strings
sidebar_label: Query Strings
---

Deephaven query strings are the primary way of expressing commands directly to the Deephaven engine. They translate the user's intention into compiled code that the engine can execute. These query strings can contain a mix of Java and Python code and are the entry point to a universe of powerful built-in tools and Python-Java interoperability.

## Syntax

Query strings are just Python strings that get passed into table operations. Deephaven **highly** recommends using double quotes to encapsulate query strings.

```python test-set=1
from deephaven import empty_table

t = empty_table(10).update("NewColumn = 1")
```

Here, the query string `NewColumn = 1` defines a formula for the engine to execute, and the [`update`](../../reference/table-operations/select/update.md) table operation understands that formula as a recipe for creating a new column called `NewColumn` that contains the value of `1` in each row.

## Literals

Query strings often use [literals](https://en.wikipedia.org/wiki/Literal_(computer_programming)). How the engine interprets a literal depends on how it's written in the query string.

- Literals not encapsulated by any special characters are interpreted as booleans, numeric values, column names, or variables.
- Literals encapsulated in backticks (`` ` ``) are interpreted as strings.
- Literals encapsulated in single quotes (`'`) are interpreted as date-time values.

```python test-set=1
literals = empty_table(10).update(
    [
        # Booleans are represented by 'true' and 'false'
        "BooleanCol = true",
        # 32b integers are represented by integers such as '1'
        "IntCol = 1",
        # 64b integers are represented by integers with an 'L' suffix such as '10L'
        "LongCol = 10L",
        # 32b floating point numbers are represented by numbers with an 'f' suffix such as '1.2345f'
        "FloatCol = 1.2345f",
        # 64b floating point numbers are represented by numbers with a decimal point such as '2.3456'
        "DoubleCol = 2.3456",
        # Strings must be enclosed with backticks
        "StringCol = `Hello!`",
        # Date-time values must be enclosed with single quotes
        "DateCol = '2020-01-01'",
        "TimeCol = '12:30:45.000'",
        "DateTimeCol = '2020-01-01T12:30:45.000Z'",
        # Durations must be enclosed with single quotes and have a leading PT
        "DurationCol = 'PT1m'",
        # For object types, 'null' is the literal for null values
        "NullCol = null",
    ]
)
```

The [`meta_table`](../../how-to-guides/metadata.md#meta_table) attribute is useful for assessing a table's schema. You can use it to confirm that the resulting columns are of the correct type.

```python test-set=1
literals_meta = literals.meta_table
```

## Special variables and constants

Deephaven provides several [special variables and constants](../../reference/query-language/variables/special-variables.md). The most commonly used of these are `i` and `ii`. They represent the row indices as 32-bit and 64-bit integers, `int` and `long`, respectively.

```python test-set=1 order=special_meta,special_vars
special_vars = empty_table(10).update(["IdxInt = i", "IdxLong = ii"])

special_meta = special_vars.meta_table
```

> **_NOTE:_** The special variables `i` and `ii` can only be used in [append-only](../../conceptual/table-types.md#specialization-1-append-only) tables.

Additionally, Deephaven provides a range of common constants that can be accessed from query strings. These constants are always written with snake case in capital letters. They include [minimum and maximum values for various data types](/core/javadoc/io/deephaven/util/QueryConstants.html), [conversion factors for time types](/core/javadoc/io/deephaven/time/DateTimeUtils.html), and more. Of particular interest are the null constants for primitive types.

```python test-set=1 order=null_values_meta,null_values
null_values = empty_table(1).update(
    [
        "StandardNull = null",
        "BoolNull = NULL_BOOLEAN",
        "CharNull = NULL_CHAR",
        "ByteNull = NULL_BYTE",
        "ShortNull = NULL_SHORT",
        "IntNull = NULL_INT",
        "LongNull = NULL_LONG",
        "FloatNull = NULL_FLOAT",
        "DoubleNull = NULL_DOUBLE",
    ]
)

null_values_meta = null_values.meta_table
```

These are useful for representing and handling null values of a specific type. Built-in query language functions handle null values. For example, `sqrt(NULL_DOUBLE)` returns `NULL_DOUBLE`. Custom functions need to handle null values appropriately.

## Common operations

Numeric types support mathematical operations such as `+`, `-`, `*`, `/`, and `%`.

```python test-set=1
math_ops = empty_table(10).update(
    [
        "Col1 = ii",
        # Math operations work between columns and literals
        "Col2 = 10 * (Col1 + 1.2345) / 4",
        # And they work between columns
        "Col3 = (Col1 - Col2) % 6",
    ]
)
```

String concatenation is also supported via the `+` operator.

```python test-set=1
add_strings = empty_table(10).update("StringCol = `Hello ` + `there!`")
```

The `+` and `-` operators are defined for date-time types, making arithmetic on timestamp data easy.

```python test-set=1
time_ops = empty_table(10).update(
    [
        # Times in nanoseconds can be added or subtracted from date-times
        "Timestamp = '2021-07-11T12:00:00.000Z' + (ii * HOUR)",
        # Durations or Periods can be added or subtracted from date-times
        "TimestampPlusOneYear = Timestamp + 'P365d'",
        "TimestampMinusOneHour = Timestamp - 'PT1h'",
        # Timestamps can be subtracted to get their difference in nanoseconds
        # Use constants for unit conversion
        "DaysBetween = ('2023-01-02T20:00:00.000Z' - Timestamp) / DAY",
    ]
)
```

Logical operations, expressions, and comparison operators are supported.

```python test-set=1
logical_ops = empty_table(10).update(
    [
        "IdxCol = ii",
        "IsEven = IdxCol % 2 == 0",
        "IsDivisibleBy6 = IsEven && (IdxCol % 3 == 0)",
        "IsGreaterThan6 = IdxCol > 6",
    ]
)
```

Deephaven provides an [inline conditional operator (ternary-if)](../../how-to-guides/ternary-if-how-to.md) that makes writing conditional expressions compact and efficient.

```python test-set=1
conditional = empty_table(10).update(
    [
        # Read as '<condition> ? <if-true> : <if-false>'
        "Parity = ii % 2 == 0 ? `Even!` : `Odd...`",
        # Any logical expression is a valid condition
        "IsDivisibleBy6 = ((ii % 2 == 0) && (ii % 3 == 0)) ? true : false",
        # In-line conditionals can be chained together
        "RandomNumber = randomGaussian(0.0, 1.0)",
        "Score = RandomNumber < -1.282 ? `Bottom 10%` : RandomNumber > 1.282 ? `Top 10%` : `Middle of the pack`",
    ]
)
```

In Deephaven, typecasting is easy.

```python test-set=1
typecasted = empty_table(10).update(
    [
        "LongCol = ii",
        # Use (type) to cast a column to the desired type
        "IntCol = (int) LongCol",
        "FloatCol = (float) LongCol",
        # Or, use a cast as an intermediate step
        "IntSquare = i * (int) LongCol",
    ]
)
```

There are many more such operators supported in Deephaven. See the [guide on operators](../../how-to-guides/operators.md) to learn more.

## Built-in functions

Aside from the common operations, Deephaven hosts a large library of functions known as [built-in or auto-imported functions](../../reference/query-language/query-library/auto-imported/index.md) that can be used in query strings.

The [numeric subset of this library](/core/javadoc/io/deephaven/function/package-summary.html) is full of functions that perform common mathematical operations on numeric types. These include exponentials, trigonometric functions, random number generators, and more.

```python test-set=1
math_functions = empty_table(10).update(
    [
        # Each function can be applied to many different types
        "Exponential = exp(ii)",
        "NatLog = log(i)",
        "Square = pow((float)ii, 2)",
        "Sqrt = sqrt((double)i)",
        "Trig = sin(ii)",
        # Functions can be composed
        "AbsVal = abs(cos(ii))",
        "RandInt = randomInt(0, 5)",
        "RandNormal = randomGaussian(0, 1)",
    ]
)
```

These functions can be combined with previously discussed literals and operators to generate complex expressions.

```python test-set=1
fake_data = empty_table(100).update(
    [
        "Group = randomInt(1, 4)",
        "GroupIntercept = Group == 1 ? 0 : Group == 2 ? 5 : 10",
        "GroupSlope = abs(GroupIntercept * randomGaussian(0, 1))",
        "GroupVariance = pow(sin(Group), 2)",
        "Data = GroupIntercept + GroupSlope * ii + randomGaussian(0.0, GroupVariance)",
    ]
)
```

The built-in library also contains many functions for [working with date-time values](/core/javadoc/io/deephaven/time/DateTimeUtils.html).

```python test-set=1
time_functions = empty_table(10).update(
    [
        # The now() function provides the current time according to the Deephaven clock
        "CurrentTime = now()",
        "Timestamp = CurrentTime + (ii * DAY)",
        # Many time functions require timezone information, typically provided as literals
        "DayOfWeek = dayOfWeek(Timestamp, 'ET')",
        "DayOfMonth = dayOfMonth(Timestamp, 'ET')",
        "Weekend = DayOfWeek == 6 || DayOfWeek == 7 ? true : false",
        "SecondsSinceY2K = nanosToSeconds(Timestamp - '2000-01-01T00:00:00 ET')",
    ]
)
```

Functions that begin with `parse` are useful for converting strings to date-time types.

```python test-set=1 order=string_meta,string_date_times,converted_meta,converted_date_times
string_date_times = empty_table(10).update(
    [
        "DateTime = `2022-04-03T19:34:22.000 UTC`",
        "Date = `2013-07-07`",
        "Time = `14:04:39.123`",
        "Duration = `PT6m`",
        "Period = `P10d`",
        "TimeZone = `America/Chicago`",
    ]
)

converted_date_times = string_date_times.update(
    [
        "DateTime = parseInstant(DateTime)",
        "Date = parseLocalDate(Date)",
        "Time = parseLocalTime(Time)",
        "Duration = parseDuration(Duration)",
        "Period = parsePeriod(Period)",
        "TimeZone = parseTimeZone(TimeZone)",
    ]
)

string_meta = string_date_times.meta_table
converted_meta = converted_date_times.meta_table
```

[`upperBin`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#upperBin(java.time.Instant,long)) and [`lowerBin`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long)) bin timestamps into buckets. They are particularly useful in aggregation operations, as aggregated statistics are commonly computed over temporal buckets.

```python test-set=1 order=binned_timestamps,last_by_bin
binned_timestamps = empty_table(60).update(
    [
        "Timestamp = now() + ii * MINUTE",
        # Rounds Timestamp to the last 5 minute mark
        "Last5Mins = lowerBin(Timestamp, 5 * MINUTE)",
        # Rounds Timestamp to the next 10 minute mark
        "Next10Mins = upperBin(Timestamp, 10 * MINUTE)",
        "Value = random()",
    ]
)

last_by_bin = binned_timestamps.last_by("Last5Mins")
```

The [time user guide](../../conceptual/time-in-deephaven.md) provides a comprehensive overview of working with date-time data in Deephaven.

These functions provide only a glimpse of what the built-in library offers. There are modules for [sorting](/core/javadoc/io/deephaven/function/Sort.html), [searching](/core/javadoc/io/deephaven/function/BinSearch.html), [string parsing](/core/javadoc/io/deephaven/function/Parse.html), [null handling](https://deephaven.io/core/javadoc/io/deephaven/function/Basic.html#isNull(byte)), and much more. See the document on [auto-imported functions](../../reference/query-language/query-library/auto-imported/index.md) for a comprehensive list of what's available or the [module summary page](/core/javadoc/io/deephaven/function/package-summary.html) for a high-level overview of what's offered.

## Java methods

The data structures that underlie Deephaven tables are Java data structures. So, many of the literals, operators, and functions we've spoken about are, at some level, Java. Java objects have methods attached to them that can be called from query strings, unlocking new levels of functionality and efficiency for Deephaven users.

To discover these, use [`meta_table`](../../reference/table-operations/metadata/meta_table.md) to inspect a column's underlying data type.

```python test-set=1 order=call_methods_meta,call_methods
call_methods = empty_table(1).update("Timestamp = '2024-03-03T15:00:00.000 UTC'")

call_methods_meta = call_methods.meta_table
```

This column is a Java [Instant](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/Instant.html). Java's [documentation](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/Instant.html) provides all of the available [methods](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/Instant.html#method.summary) that can be called. Here are just a few.

```python test-set=1
call_methods = call_methods.update(
    [
        "SecondsSinceEpoch = Timestamp.getEpochSecond()",
        "TimestampInTokyo = Timestamp.atZone('Asia/Tokyo')",
        "StringRepresentation = Timestamp.toString()",
    ]
)
```

Some basic understanding of [how to read Javadocs](../../how-to-guides/read-javadocs.md) will help you make the most of these built-in methods.

Additionally, there are several ways to create Java objects for use in query strings. The following example uses (1) the `new` keyword and (2) the Python [jpy](../../how-to-guides/use-jpy.md) package to create new instances of Java's [URL](https://docs.oracle.com/javase/7/docs/api/java/net/URL.html) class.

```python test-set=1 order=t1,m1,t2,m2,t3,m3
import jpy

# Use 'new' keyword to create a new URL object, then call methods
t1 = empty_table(2).update(
    [
        "URL = new java.net.URL(`https://deephaven.io:` + i)",
        "Protocol = URL.getProtocol()",
        "Host = URL.getHost()",
        "Port = URL.getPort()",
        "URI = URL.toURI()",
    ]
)
m1 = t1.meta_table

# Use 'jpy' to create a new URL object, pass object to query string, then call methods
URL = jpy.get_type("java.net.URL")
url = URL("https://deephaven.io")

t2 = empty_table(1).update(
    [
        "URL = url",
        "Protocol = URL.getProtocol()",
        "Host = URL.getHost()",
        "Port = URL.getPort()",
        "URI = URL.toURI()",
    ]
)
m2 = t2.meta_table

# Use 'jpy' to create a new URL object, call methods, then pass results to query string
URL = jpy.get_type("java.net.URL")
url = URL("https://deephaven.io")
protocol = url.getProtocol()
host = url.getHost()
port = url.getPort()
uri = url.toURI()

t3 = empty_table(1).update(
    [
        "URL = url",
        "Protocol = protocol",
        "Host = host",
        "Port = port",
        "URI = uri",
    ]
)
m3 = t3.meta_table
```

For more information, see the [jpy guide](../../how-to-guides/use-jpy.md).

## Arrays

Deephaven tables can have array columns. Array columns often come from a [`group_by`](../../reference/table-operations/group-and-aggregate/groupBy.md) operation.

```python test-set=1 order=t,t_grouped
t = empty_table(10).update(["Group = ii % 2 == 0 ? `A` : `B`", "X = ii"])

t_grouped = t.group_by("Group")
```

Many built-in functions support array arguments.

```python test-set=1
t_array_funcs = t_grouped.update(
    [
        "IsEven = X % 2 == 0",
        "PlusOne = X + 1",
        "GroupSum = sum(X)",
        "GroupAvg = avg(X)",
        "NumUnique = countDistinct(X)",
    ]
)
```

These results can then be ungrouped with [`ungroup`](../../reference/table-operations/group-and-aggregate/ungroup.md), which is essentially the inverse of [`group_by`](../../reference/table-operations/group-and-aggregate/groupBy.md).

```python test-set=1
t_array_funcs_ungrouped = t_array_funcs.ungroup()
```

> **_NOTE:_** Aggregations done with [`deephaven.agg`](../../how-to-guides/combined-aggregations.md) are more performant than with array functions.

Deephaven provides array indexing and slicing operations.

```python test-set=1
t_indexed = t_grouped.update(
    [
        "First = X[0]",
        "Third = X[2]",
        # Note that .subVector() includes the first arg and does not include the second
        "FirstThree = X.subVector(0, 3)",
        "MiddleThree = X.subVector(1, 4)",
        # Indexing outside the range returns null
        "OffTheFront = X[-1]",
        "OffTheEnd = X.subVector(3,6)",
    ]
)
```

Lastly, the columns of a Deephaven table can be interpreted as arrays.

```python test-set=1
column_as_array = empty_table(10).update(
    [
        "X = ii",
        # To interpret X as an array, use a trailing underscore
        "ArrX = X_",
        # Functions that take array inputs can now be used
        "SumX = sum(X_)",
        "AvgX = avg(X_)",
        # Indexing and slicing work just the same
        "XMinus2 = X_[ii-2]",
        "RollingGroup = X_.subVector(i, i+3)",
        "RollingAvg = avg(RollingGroup)",
    ]
)
```

This functionality is only supported for static and append-only ticking tables. See [working with arrays](../../how-to-guides/work-with-arrays.md) for more information.

## Python in query strings

Python objects can be used in query strings. Variables are the simplest case.

```python test-set=1
a = 1
b = 2

add_vars = empty_table(1).update("Sum = a + b")
```

Python functions can also be used.

```python test-set=1
def my_sum(x, y):
    return x + y


a = 1
b = 2

add_vars_func = empty_table(1).update(["Sum1 = my_sum(1, 2)", "Sum2 = my_sum(a, b)"])
```

So can classes.

```python test-set=1
class MyMathClass:
    def __init__(self, x):
        self.x = x

    def sum(self, y):
        return self.x + y

    @classmethod
    def class_sum(self, x, y):
        return x + y

    @staticmethod
    def static_sum(x, y):
        return x + y


a = 1
b = 2
class_instance = MyMathClass(a)

add_vars_class = empty_table(1).update(
    [
        "Sum1 = class_instance.sum(b)",
        "Sum2 = MyMathClass.class_sum(a, b)",
        "Sum3 = MyMathClass.static_sum(a, b)",
    ]
)
```

Without any type casts or [type hints](https://docs.python.org/3/library/typing.html), the Deephaven query engine cannot infer what datatype results from a Python function. It stores the result as a Java [PyObject](../../how-to-guides/pyobjects.md#what-is-a-pyobject) or [Object](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html).

```python test-set=1 order=add_vars_meta,add_vars_func_meta,add_vars_class_meta
add_vars_meta = add_vars.meta_table
add_vars_func_meta = add_vars_func.meta_table
add_vars_class_meta = add_vars_class.meta_table
```

This isn't ideal, as neither of these data types support many of the DQL features we've covered. To rectify this, Python functions should utilize [type hints](https://docs.python.org/3/library/typing.html). The engine will infer the correct column types for functions that use type hints. Class methods don't support type hints yet, so a typecast in the query string is required.

```python test-set=2 order=add_vars_func_meta,add_vars_func,add_vars_class_meta,add_vars_class
from deephaven import empty_table


def my_sum(x, y) -> int:
    return x + y


class MyMathClass:
    def __init__(self, x):
        self.x = x

    def sum(self, y) -> int:
        return self.x + y

    @classmethod
    def class_sum(self, x, y) -> int:
        return x + y

    @staticmethod
    def static_sum(x, y) -> int:
        return x + y


a = 1
b = 2
class_instance = MyMathClass(a)

add_vars_func = empty_table(1).update(["Sum1 = my_sum(1, 2)", "Sum2 = my_sum(a, b)"])

# Note the (int) casts
add_vars_class = empty_table(1).update(
    [
        "Sum1 = (int) class_instance.sum(b)",
        "Sum2 = (int) MyMathClass.class_sum(a, b)",
        "Sum3 = (int) MyMathClass.static_sum(a, b)",
    ]
)

add_vars_func_meta = add_vars_func.meta_table
add_vars_class_meta = add_vars_class.meta_table
```

To learn more about using Python in query strings, see [Python in query strings](../../how-to-guides/query-string-overview.md#python).

Scoping in Deephaven follows Python's [LEGB](https://realpython.com/python-scope-legb-rule/) scoping rules. Functions that return tables or otherwise make use of query strings should pay careful attention to scoping details.

```python test-set=2 order=source,result1,result2
def f(a, b) -> int:
    return a * b


def compute(source, a):
    return source.update("X = f(a, A)")


source = empty_table(5).update("A = i")

result1 = compute(source, 10)
result2 = compute(source, 3)
```

For more information, see [scoping rules](../../how-to-guides/query-scope.md).

When Deephaven parallelizes a query, rows may be processed in any order across multiple CPU cores. Functions that work correctly regardless of execution order can run in parallel. This function works in parallel because each call is independent:

```python test-set=2
my_list = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


def get_element(idx) -> int:
    return my_list[idx]


t = empty_table(10).update("X = get_element(ii)")
```

This works because each row's result depends only on the input `idx` - it doesn't matter which row is processed first.

Some functions require rows to be processed one at a time, in order. This function increments a counter, so it needs [`.with_serial()`](../../conceptual/query-engine/parallelization.md#serialization) to force sequential execution:

```python test-set=2
from deephaven.table import Selectable

my_list = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
idx = 0


def get_next_element() -> int:
    global idx
    idx += 1  # Each call changes idx
    return my_list[idx - 1]


# Use .with_serial() to ensure sequential execution
col = Selectable.parse("X = get_next_element()").with_serial()
t = empty_table(10).update(col)
```

Print `idx` to verify it's been changed.

```python test-set=2
print(idx)
```

Without `.with_serial()`, multiple cores might increment `idx` simultaneously, causing incorrect results. Serial execution is slower (one core instead of many), so use it only when correctness requires it.

Queries run faster when they can be parallelized. To enable parallelization:

- Each row's result should depend only on that row's inputs.
- Avoid modifying external variables.
- Use Deephaven's built-in functions when possible.
