---
title: Deephaven's Query Strings
---

Deephaven query strings are the primary way of expressing commands directly to the Deephaven engine. They tell the engine what to compute, and the engine turns them into filters and formulas, compiling code where needed. Query strings use Java-like syntax and can use Python variables, functions, and objects. They are the entry point to a universe of powerful built-in tools and Python-Java interoperability: Deephaven's built-in function library, and the ability to mix Python and Java objects in one expression.

## Syntax

Query strings are Python strings that you pass to table operations. Deephaven **highly** recommends using double quotes for query strings, because query strings use single quotes for their own date-time and `char` literals.

```python test-set=1
from deephaven import empty_table

t = empty_table(10).update("NewColumn = 1")
```

Here, the query string `NewColumn = 1` defines a formula for the engine to execute, and the [`update`](../../reference/table-operations/select/update.md) table operation understands that formula as a recipe for creating a new column called `NewColumn` that contains the value of `1` in each row.

## Literals

Query strings often use [literals](https://en.wikipedia.org/wiki/Literal_(computer_programming)). How the engine interprets a literal depends on how it's written in the query string. The engine reads:

- Unquoted literals as booleans, numbers, or `null`. Other unquoted names refer to columns or variables.
- Literals in backticks (`` ` ``) as strings.
- Literals in single quotes (`'`) as date-time values, such as dates, times, durations, periods, and time zones. A single character in single quotes is a `char`.

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
        # Durations are enclosed with single quotes and use the ISO-8601 time-based format, such as 'PT1m'
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

Deephaven provides several [special variables and constants](../../reference/query-language/variables/special-variables.md). The most commonly used of these are `i` and `ii`. They represent the row position as 32-bit and 64-bit integers, `int` and `long`, respectively.

```python test-set=1 order=special_meta,special_vars
special_vars = empty_table(10).update(["IdxInt = i", "IdxLong = ii"])

special_meta = special_vars.meta_table
```

> [!NOTE]
> The special variables `i` and `ii` are safe to use on static tables, on [append-only](../../conceptual/table-types.md#specialization-1-append-only) tables (ticking tables that only add rows at the end), and on [blink](../../conceptual/table-types.md#specialization-3-blink) tables. On other ticking tables, the engine throws an error. See [refreshing table restrictions](../../reference/query-language/variables/special-variables.md#refreshing-table-restrictions).

Deephaven also provides a range of common constants that you can use in query strings. Their names are in uppercase snake case, such as `NULL_INT`. They include [minimum and maximum values for various data types](/core/javadoc/io/deephaven/util/QueryConstants.html), [conversion factors for time types](/core/javadoc/io/deephaven/time/DateTimeUtils.html), and more. Of particular interest are the null constants for primitive types.

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

These are useful for representing and handling null values of a specific type. Built-in numeric functions generally handle null values. For example, `sqrt(NULL_DOUBLE)` returns `NULL_DOUBLE`. Custom functions need to handle null values appropriately.

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

The `+` operator also concatenates strings.

```python test-set=1
add_strings = empty_table(10).update("StringCol = `Hello ` + `there!`")
```

The `+` and `-` operators are defined for date-time types, making arithmetic on timestamp data easy.

```python test-set=1
time_ops = empty_table(10).update(
    [
        # Times in nanoseconds can be added to or subtracted from date-times
        "Timestamp = '2021-07-11T12:00:00.000Z' + (ii * HOUR)",
        # Durations or day-based Periods can be added to or subtracted from date-times
        "TimestampPlus365Days = Timestamp + 'P365d'",
        "TimestampMinusOneHour = Timestamp - 'PT1h'",
        # Timestamps can be subtracted to get their difference in nanoseconds
        # Use constants for unit conversion
        "DaysBetween = ('2023-01-02T20:00:00.000Z' - Timestamp) / DAY",
    ]
)
```

Query strings support logical operators such as `&&` and `||`, and comparison operators such as `==` and `>`.

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
        # randomGaussian is a built-in function; the next section covers these
        "RandomNumber = randomGaussian(0.0, 1.0)",
        # In-line conditionals can be chained together
        "Score = RandomNumber < -1.282 ? `Bottom 10%` : RandomNumber > 1.282 ? `Top 10%` : `Middle of the pack`",
    ]
)
```

In Deephaven, typecasting is easy: query strings support Java-style [typecasts](../../reference/query-language/control-flow/casting.md).

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

Deephaven supports many more operators. See the [guide on operators](../../how-to-guides/operators.md) to learn more.

## Built-in functions

Aside from the common operations, Deephaven hosts a large library of functions known as [built-in or auto-imported functions](../../reference/query-language/query-library/auto-imported/index.md) that you can use in query strings.

The [numeric subset of this library](/core/javadoc/io/deephaven/function/Numeric.html) is full of functions that perform common mathematical operations on numeric types. These include exponentials, trigonometric functions, and more. [Random number generators](/core/javadoc/io/deephaven/function/Random.html) are also available.

```python test-set=1
math_functions = empty_table(10).update(
    [
        # Each function can be applied to many different types
        "Exponential = exp(ii)",
        "NatLog = log(i)",
        "Square = pow((float) ii, 2)",
        "Sqrt = sqrt((double) i)",
        "Trig = sin(ii)",
        # Functions can be composed
        "AbsVal = abs(cos(ii))",
        "RandInt = randomInt(0, 5)",
        "RandNormal = randomGaussian(0, 1)",
    ]
)
```

Combine these functions with the literals and operators covered earlier to build complex expressions.

```python test-set=1
fake_data = empty_table(100).update(
    [
        "Group = randomInt(1, 4)",
        "GroupIntercept = Group == 1 ? 0 : Group == 2 ? 5 : 10",
        "GroupSlope = Group == 1 ? 0.5 : Group == 2 ? 1.0 : 2.0",
        "GroupStdDev = pow(sin(Group), 2)",
        "Data = GroupIntercept + GroupSlope * ii + randomGaussian(0.0, GroupStdDev)",
    ]
)
```

The previous chapter, [Time and Calendars](./time-calendars.md), used several of the built-in date-time functions, such as [`now`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#now()), [`upperBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#upperBin(java.time.Instant,long)), and [`lowerBin`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,long)). The library contains many more [functions for working with date-time values](/core/javadoc/io/deephaven/time/DateTimeUtils.html).

```python test-set=1
time_functions = empty_table(10).update(
    [
        # The now() function provides the current time according to the Deephaven clock
        "CurrentTime = now()",
        "Timestamp = CurrentTime + (ii * DAY)",
        # Many time functions require timezone information, typically provided as literals
        "DayOfWeek = dayOfWeekValue(Timestamp, 'ET')",
        "DayOfMonth = dayOfMonth(Timestamp, 'ET')",
        "Weekend = DayOfWeek == 6 || DayOfWeek == 7 ? true : false",
        "SecondsSinceY2K = nanosToSeconds(Timestamp - '2000-01-01T00:00:00 ET')",
    ]
)
```

Date-time functions that begin with `parse`, such as `parseInstant` and `parseDuration`, convert strings to date-time types.

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

The [time user guide](../../conceptual/time-in-deephaven.md) provides a comprehensive overview of working with date-time data in Deephaven.

These functions provide only a glimpse of what the built-in library offers. There are modules for [sorting](/core/javadoc/io/deephaven/function/Sort.html), [searching](/core/javadoc/io/deephaven/function/BinSearch.html), [string parsing](/core/javadoc/io/deephaven/function/Parse.html), [null handling](/core/javadoc/io/deephaven/function/Basic.html#isNull(byte)), and much more. See the document on [auto-imported functions](../../reference/query-language/query-library/auto-imported/index.md) for a comprehensive list of what's available or the [module summary page](/core/javadoc/io/deephaven/function/package-summary.html) for a high-level overview of what's offered.

## Java methods

The data structures that underlie Deephaven tables are Java data structures. Many of the literals, operators, and functions covered so far are Java under the hood. Query strings can call methods on Java objects, unlocking new levels of functionality and efficiency.

To find out which methods a column's values have, first check the column's Java type with [`meta_table`](../../reference/table-operations/metadata/meta_table.md), as in the [Literals](#literals) section.

```python test-set=1 order=call_methods_meta,call_methods
call_methods = empty_table(1).update("Timestamp = '2024-03-03T15:00:00.000 UTC'")

call_methods_meta = call_methods.meta_table
```

This column is a Java [`Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html). Its Javadoc lists every [method](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html#method.summary) you can call. Here are a few.

```python test-set=1
call_methods = call_methods.update(
    [
        "SecondsSinceEpoch = Timestamp.getEpochSecond()",
        "TimestampInTokyo = Timestamp.atZone('Asia/Tokyo')",
        "StringRepresentation = Timestamp.toString()",
    ]
)
```

A basic understanding of [how to read Javadocs](../../how-to-guides/read-javadocs.md) helps you make the most of these Java methods.

You can also create Java objects for use in query strings. The following example uses Java's [`URI`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/net/URI.html) class in query strings in three ways:

- `t1` uses the `new` keyword inside the query string.
- `t2` uses the Python [jpy](../../how-to-guides/use-jpy.md) package to create the object, then passes it into the query string as a [Python variable](#python-in-query-strings).
- `t3` uses [jpy](../../how-to-guides/use-jpy.md) to create the object and call its methods in Python, then passes the results into the query string.

```python test-set=1 order=t1,m1,t2,m2,t3,m3
import jpy

# Use 'new' keyword to create a new URI object, then call methods
t1 = empty_table(2).update(
    [
        "URI = new java.net.URI(`https://deephaven.io:` + i)",
        "Scheme = URI.getScheme()",
        "Host = URI.getHost()",
        "Port = URI.getPort()",
        "URL = URI.toURL()",
    ]
)
m1 = t1.meta_table

# Use 'jpy' to create a new URI object, pass object to query string, then call methods
JavaURI = jpy.get_type("java.net.URI")
uri = JavaURI("https://deephaven.io")

t2 = empty_table(1).update(
    [
        "URI = uri",
        "Scheme = URI.getScheme()",
        "Host = URI.getHost()",
        "Port = URI.getPort()",
        "URL = URI.toURL()",
    ]
)
m2 = t2.meta_table

# Reuse the jpy URI object, call its methods in Python, then pass the results to the query string
scheme = uri.getScheme()
host = uri.getHost()
port = uri.getPort()
url = uri.toURL()

t3 = empty_table(1).update(
    [
        "URI = uri",
        "Scheme = scheme",
        "Host = host",
        "Port = port",
        "URL = url",
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

Many operators and built-in functions support array arguments. Operators apply element by element, while functions such as `sum` and `avg` reduce the array to a single value.

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

> [!NOTE]
> On ticking tables, aggregations with [`deephaven.agg`](../../how-to-guides/combined-aggregations.md) are more performant than [`group_by`](../../reference/table-operations/group-and-aggregate/groupBy.md) followed by array functions.

[`ungroup`](../../reference/table-operations/group-and-aggregate/ungroup.md) expands the array columns back into separate rows.

```python test-set=1
t_array_funcs_ungrouped = t_array_funcs.ungroup()
```

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
        "OffTheEnd = X[5]",
    ]
)
```

Lastly, query strings can treat the columns of a Deephaven table as arrays.

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
        "XMinus2 = X_[ii - 2]",
        "NextThree = X_.subVector(i, i + 3)",
        "NextThreeAvg = avg(NextThree)",
    ]
)
```

Simple constant-offset access such as `X_[ii - 2]` works on all tables, even though it uses `ii`. The engine recognizes this pattern as an exception to the `i` and `ii` restriction described in [Special variables and constants](#special-variables-and-constants). Other uses of column arrays, such as `sum(X_)` or `X_.subVector(i, i + 3)`, work only on static and blink tables. On any other ticking table, including an append-only table, the engine throws an error. See [working with arrays](../../how-to-guides/work-with-arrays.md) and the [special variables reference](../../reference/query-language/variables/special-variables.md#refreshing-table-restrictions) for more information.

## Python in query strings

Query strings can use Python objects. Variables are the simplest case.

```python test-set=1
a = 1
b = 2

add_vars = empty_table(1).update("Sum = a + b")
```

Query strings can also call Python functions.

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
    def class_sum(cls, x, y):
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

Without any type casts or [type hints](https://docs.python.org/3/library/typing.html), the Deephaven query engine cannot infer what data type results from a Python function. It stores the result as a Java [`PyObject`](../../how-to-guides/pyobjects.md#what-is-a-pyobject) or [`Object`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html). Plain Python variables are different: the engine converts them to Java types, so the `Sum` column in `add_vars` is an `int`.

```python test-set=1 order=add_vars_meta,add_vars_func_meta,add_vars_class_meta
add_vars_meta = add_vars.meta_table
add_vars_func_meta = add_vars_func.meta_table
add_vars_class_meta = add_vars_class.meta_table
```

This isn't ideal, as [`PyObject`](../../how-to-guides/pyobjects.md#what-is-a-pyobject) and [`Object`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html) columns don't support many of the query language features we've covered. To fix this, add [type hints](https://docs.python.org/3/library/typing.html) to Python functions so the engine can infer the correct column types. The engine ignores type hints on class methods, so cast class method results in the query string instead.

The following example redefines `my_sum` with a return type hint and casts the results of the `MyMathClass` methods defined above.

```python test-set=1 order=add_vars_func_meta,add_vars_func,add_vars_class_meta,add_vars_class
def my_sum(x, y) -> int:
    return x + y


add_vars_func = empty_table(1).update(["Sum1 = my_sum(1, 2)", "Sum2 = my_sum(a, b)"])

# The engine ignores type hints on class methods, so note the (int) casts
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

## Scoping

Functions that return tables or otherwise use query strings should pay careful attention to scoping details. Query strings resolve Python names from the calling function's local scope first, then from the global (module) scope.

```python test-set=2 order=source,result1,result2
from deephaven import empty_table


def f(a, b) -> int:
    return a * b


def compute(source, a):
    return source.update("X = f(a, A)")


source = empty_table(5).update("A = i")

result1 = compute(source, 10)
result2 = compute(source, 3)
```

Inside `compute`, the query string's `a` resolves to the function's local `a` argument (10, then 3), so `result1` and `result2` differ.

Python's [LEGB](https://realpython.com/python-scope-legb-rule/) rules resolve names from the local, enclosing, global, and built-in scopes. Query strings follow similar rules, with two differences:

- Python's built-in names aren't in scope.
- A query string inside a nested function can't see the enclosing function's variables unless the nested function's own code references them, for example by declaring them `nonlocal`.

For those cases and more examples, see [scoping rules](../../how-to-guides/query-scope.md).

## Stateless and stateful functions

Know whether each Python function you call in a query string is stateless or stateful. A stateless function has no side effects: it doesn't modify any object outside its own scope. Its result also doesn't depend on execution order, so the engine can evaluate the calls in any order. The following stateless function extracts elements from a list.

```python test-set=2
my_list = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


def get_element_stateless(idx) -> int:
    return my_list[idx]


t_stateless = empty_table(10).update("X = get_element_stateless(ii)")
```

`get_element_stateless` is stateless because it reads `my_list` but never changes it. It could be evaluated in any order and give the same result.

A stateful function modifies objects outside its local scope: it doesn't leave the world as it found it. Its result can also depend on execution order. The engine assumes functions in query strings are stateless, so it may evaluate rows in any order, including in parallel.

To make the engine evaluate a query string in row order, wrap it with [`Selectable.parse`](/core/pydoc/code/deephaven.table.html#deephaven.table.Selectable) and call [`with_serial`](../../conceptual/query-engine/parallelization.md) on the result. In the following example, the `update` call that uses a stateful function produces the same table as `t_stateless` because the query string is marked serial.

```python test-set=2
from deephaven.table import Selectable

my_list = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
idx = 0


def get_element_stateful() -> int:
    global idx
    idx += 1  # This modifies idx!
    return my_list[idx - 1]


t_stateful = empty_table(10).update(
    Selectable.parse("X = get_element_stateful()").with_serial()
)
```

Print `idx` to verify that `get_element_stateful` changed it.

```python test-set=2 order=:log
print(idx)
```

Queries should use stateless functions whenever possible because:

- They minimize side effects when called.
- Their result does not depend on the order of evaluation, so you don't need [`with_serial`](../../conceptual/query-engine/parallelization.md).
- The engine is free to choose the execution order, including evaluating them in parallel where it supports it.

This chapter covered the building blocks of query strings: literals, special variables and constants, operators, built-in functions, Java methods, arrays, and Python objects. It also showed how query strings resolve variables and why you should prefer stateless functions. Next, [Recipes, not loops!](./vectorization-vs-loops.md) explains why Deephaven queries describe calculations as column formulas rather than loops over rows.
