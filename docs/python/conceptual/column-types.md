---
title: Deephaven's column types
sidebar_label: Column types
---

Deephaven tables store data in strongly-typed columns, where each column has a specific data type that determines what values it can hold and how operations behave on that data. Understanding column types is essential for writing efficient queries, avoiding type errors, and optimizing memory usage. This guide covers Deephaven's type system, including primitive types, temporal types, strings, objects, arrays, and how to work with nulls and type conversions.

## Column type overview

Deephaven supports a rich type system built on Java's type system. The main type categories are:

<table className="text--center">
  <thead>
    <tr>
      <th>Type category</th>
      <th>Examples</th>
      <th>Nullable</th>
      <th>Memory efficient</th>
      <th>Common use cases</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td scope="row"><a href="#primitive-numeric-types">Primitive numeric</a></td>
      <td><code>byte</code>, <code>short</code>, <code>int</code>, <code>long</code>, <code>float</code>, <code>double</code></td>
      <td>✅ (special null values)</td>
      <td>✅</td>
      <td>Numeric calculations, counters, measurements</td>
    </tr>
    <tr>
      <td scope="row"><a href="#primitive-boolean-and-char">Primitive boolean/char</a></td>
      <td><code>boolean</code>, <code>char</code></td>
      <td>✅ (special null values)</td>
      <td>✅</td>
      <td>Flags, single characters</td>
    </tr>
    <tr>
      <td scope="row"><a href="#temporal-types">Temporal</a></td>
      <td><code>Instant</code>, <code>ZonedDateTime</code>, <code>LocalDate</code>, <code>LocalTime</code></td>
      <td>✅</td>
      <td>✅</td>
      <td>Timestamps, dates, times, durations</td>
    </tr>
    <tr>
      <td scope="row"><a href="#string-type">String</a></td>
      <td><code>String</code></td>
      <td>✅</td>
      <td>⚠️ (depends on cardinality)</td>
      <td>Text data, identifiers, categories</td>
    </tr>
    <tr>
      <td scope="row"><a href="#object-types">Object</a></td>
      <td><code>BigDecimal</code>, <code>BigInteger</code>, custom classes</td>
      <td>✅</td>
      <td>❌</td>
      <td>High-precision math, complex data structures</td>
    </tr>
    <tr>
      <td scope="row"><a href="#array-types">Array</a></td>
      <td><code>int[]</code>, <code>double[]</code>, <code>String[]</code></td>
      <td>✅ (array itself or elements)</td>
      <td>⚠️</td>
      <td>Lists of values, vectors, time series</td>
    </tr>
  </tbody>
</table>

## Primitive numeric types

Primitive numeric types are the most memory-efficient and performant types in Deephaven. They map directly to Java primitives and are stored unboxed in memory.

### Numeric type reference

| Type     | Size   | Range                             | Null value    | Example use case                           |
| -------- | ------ | --------------------------------- | ------------- | ------------------------------------------ |
| `byte`   | 8-bit  | -128 to 127                       | `NULL_BYTE`   | Small integers, status codes               |
| `short`  | 16-bit | -32,768 to 32,767                 | `NULL_SHORT`  | Medium integers, year values               |
| `int`    | 32-bit | -2³¹ to 2³¹-1                     | `NULL_INT`    | Standard integers, counters                |
| `long`   | 64-bit | -2⁶³ to 2⁶³-1                     | `NULL_LONG`   | Large integers, IDs, nanosecond timestamps |
| `float`  | 32-bit | ~±3.4×10³⁸ (7 digits precision)   | `NULL_FLOAT`  | Approximate decimals, measurements         |
| `double` | 64-bit | ~±1.7×10³⁰⁸ (15 digits precision) | `NULL_DOUBLE` | High-precision decimals, financial data    |

### Creating numeric columns

```python test-set=1 order=t1
from deephaven import empty_table

# Create columns of various numeric types
t1 = empty_table(5).update(
    [
        "ByteCol = (byte)(i % 100)",
        "ShortCol = (short)(i * 1000)",
        "IntCol = (int)(i * 1000000)",
        "LongCol = (long)(i * 1000000000)",
        "FloatCol = (float)(i * 1.5)",
        "DoubleCol = i * 3.14159",
    ]
)
```

### Null values for primitives

Unlike Java primitives, Deephaven's primitive columns can represent null values using special sentinel values:

```python test-set=2 order=t2
from deephaven import empty_table
from deephaven.constants import NULL_INT, NULL_DOUBLE

# Create columns with null values
t2 = empty_table(5).update(
    [
        "Value = i",
        "WithNulls = i % 2 == 0 ? i : NULL_INT",
        "Price = i % 2 == 0 ? i * 10.5 : NULL_DOUBLE",
    ]
)
```

You can check for nulls using `isNull()` or comparison with the null constant:

```python test-set=2 order=t3
# Filter for non-null values
t3 = t2.where("!isNull(WithNulls)")

# Alternatively, compare with null constant
t4 = t2.where("WithNulls != NULL_INT")
```

### Type promotion and arithmetic

When performing arithmetic with mixed numeric types, Deephaven follows Java's type promotion rules:

```python test-set=3 order=t5
from deephaven import empty_table

t5 = empty_table(3).update(
    [
        "IntValue = (int)i + 1",
        "LongValue = (long)i * 1000",
        # int + long promotes to long
        "MixedIntLong = IntValue + LongValue",
        # int * double promotes to double
        "MixedIntDouble = IntValue * 3.14",
        # Division with integers performs integer division
        "IntDiv = 7 / 2",  # Result: 3
        # Use double to get floating-point division
        "DoubleDiv = 7.0 / 2.0",  # Result: 3.5
    ]
)
```

## Primitive boolean and char

### Boolean type

The `boolean` type represents true/false values:

```python test-set=4 order=t6
from deephaven import empty_table

t6 = empty_table(5).update(["Value = i", "IsEven = i % 2 == 0", "IsPositive = i > 2"])
```

### Char type

The `char` type represents a single 16-bit Unicode character:

```python test-set=5 order=t7
from deephaven import empty_table

t7 = empty_table(5).update(["Index = i", "Letter = (char)('A' + i)"])
```

> [!NOTE]
> `char` is distinct from `String`. A `char` column holds single characters, while a `String` column holds character sequences.

## Temporal types

Deephaven provides rich support for date and time types, all based on Java 8's `java.time` package. These types are optimized for efficient storage and time-based operations.

### Temporal type reference

| Type            | Description               | Null support | Example value                                 |
| --------------- | ------------------------- | ------------ | --------------------------------------------- |
| `Instant`       | Point in time (UTC)       | ✅           | `2025-01-15T10:30:45.123456789Z`              |
| `ZonedDateTime` | Date-time with time zone  | ✅           | `2025-01-15T10:30:45-05:00[America/New_York]` |
| `LocalDate`     | Date without time or zone | ✅           | `2025-01-15`                                  |
| `LocalTime`     | Time without date or zone | ✅           | `10:30:45.123`                                |
| `LocalDateTime` | Date-time without zone    | ✅           | `2025-01-15T10:30:45`                         |
| `Duration`      | Time-based duration       | ✅           | `PT5H30M` (5 hours, 30 minutes)               |
| `Period`        | Date-based period         | ✅           | `P1Y2M3D` (1 year, 2 months, 3 days)          |

### Working with Instant

`Instant` is the most commonly used temporal type, representing an instantaneous point on the timeline in UTC:

```python test-set=6 order=t8
from deephaven import empty_table

t8 = empty_table(5).update(
    [
        "Timestamp = now() + i * SECOND",
        "HoursLater = Timestamp + 5 * HOUR",
        "DaysBefore = Timestamp - 2 * DAY",
    ]
)
```

### Working with ZonedDateTime

Use `ZonedDateTime` when time zone information is important:

```python test-set=7 order=t9
from deephaven import empty_table

t9 = empty_table(3).update(
    [
        "UTC = now()",
        "NewYork = toZonedDateTime(UTC, timeZone(`America/New_York`))",
        "Tokyo = toZonedDateTime(UTC, timeZone(`Asia/Tokyo`))",
        "HourOfDay = hourOfDay(NewYork, false)",
    ]
)
```

### Working with LocalDate and LocalTime

`LocalDate` and `LocalTime` are useful for date-only or time-only operations:

```python test-set=8 order=t10
from deephaven import empty_table

t10 = empty_table(5).update(
    [
        "Timestamp = now() + i * DAY",
        "DateOnly = toLocalDate(Timestamp, timeZone(`America/New_York`))",
        "TimeOnly = toLocalTime(Timestamp, timeZone(`America/New_York`))",
        "DayOfWeekValue = dayOfWeekValue(DateOnly)",
        "IsWeekend = DayOfWeekValue == 6 || DayOfWeekValue == 7",
    ]
)
```

### Temporal arithmetic

Deephaven provides constants and functions for temporal calculations:

```python test-set=9 order=t11
from deephaven import empty_table

t11 = empty_table(3).update(
    [
        "Now = now()",
        # Duration constants: SECOND, MINUTE, HOUR, DAY, WEEK
        "OneMinuteLater = Now + 1 * MINUTE",
        "TwoHoursEarlier = Now - 2 * HOUR",
        "ThreeDaysLater = Now + 3 * DAY",
        # Calculate differences
        "TimeDiff = Now - (Now - 5 * HOUR)",
    ]
)
```

### Time zone considerations

> [!IMPORTANT]
> Always be explicit about time zones when converting between `Instant` and zone-aware types. Implicit conversions can lead to subtle bugs, especially around daylight saving time transitions.

```python test-set=10 order=t12
from deephaven import empty_table

t12 = empty_table(2).update(
    [
        "UTC_Time = now()",
        # Explicit time zone conversion
        "NY_Time = toZonedDateTime(UTC_Time, timeZone(`America/New_York`))",
        "London_Time = toZonedDateTime(UTC_Time, timeZone(`Europe/London`))",
        # Extract components with time zone awareness
        "NY_Hour = hourOfDay(UTC_Time, timeZone(`America/New_York`), false)",
        "London_Hour = hourOfDay(UTC_Time, timeZone(`Europe/London`), false)",
    ]
)
```

## String type

The `String` type stores text data. Deephaven automatically interns strings to optimize memory usage for low-cardinality string columns.

### Creating string columns

```python test-set=11 order=t13
from deephaven import empty_table

t13 = empty_table(5).update(
    [
        "Index = i",
        "Letter = `A` + i",  # String concatenation
        "Formatted = `` + i + ` items`",
        "Conditional = i % 2 == 0 ? `Even` : `Odd`",
    ]
)
```

### String operations

String columns in Deephaven are `java.lang.String` objects, which means you can use standard Java String methods directly:

```python test-set=12 order=t14
from deephaven import empty_table

t14 = empty_table(3).update(
    [
        "Text = i == 0 ? `Hello World` : (i == 1 ? `DEEPHAVEN` : `  spaces  `)",
        "Length = Text.length()",
        "Upper = Text.toUpperCase()",
        "Lower = Text.toLowerCase()",
        "Trimmed = Text.trim()",
        "Substring = Text.substring(0, 5)",
        "Contains = Text.contains(`e`)",
    ]
)
```

### String interning and memory

Deephaven automatically interns strings, which means identical string values share the same memory location. This is very efficient for low-cardinality columns (like categories or symbols) but less beneficial for high-cardinality data (like unique IDs or free-form text).

```python test-set=13 order=t15
from deephaven import empty_table

# Low cardinality: memory efficient (only 3 unique strings stored)
t15 = empty_table(1000).update(
    ["Status = i % 3 == 0 ? `Active` : (i % 3 == 1 ? `Pending` : `Closed`)"]
)

# High cardinality: less efficient (many unique strings)
t16 = empty_table(1000).update(
    [
        "UniqueId = `ID-` + i"  # 1000 unique strings
    ]
)
```

### Null strings

Strings can be null, which is different from empty strings:

```python test-set=14 order=t17
from deephaven import empty_table

t17 = empty_table(5).update(
    [
        "Index = i",
        "WithNull = i % 2 == 0 ? `Valid` : (String)null",
        "EmptyString = i % 2 == 0 ? `Valid` : ``",
        "IsNull = isNull(WithNull)",
        "IsEmpty = WithNull == ``",  # This checks for empty, not null
    ]
)
```

## Object types

Object types can store any Java object. Common examples include `BigDecimal`, `BigInteger`, and custom classes.

### Object column considerations

- **Performance**: Object columns are slower than primitive columns due to boxing/unboxing overhead
- **Memory**: Objects require more memory (object header + data)
- **Null handling**: Objects can be `null` (Java null, not a sentinel value)
- **Use cases**: High-precision arithmetic (`BigDecimal`), custom data structures, complex types

> [!NOTE]
> For high-precision decimal arithmetic, use `java.math.BigDecimal` instead of `double` to avoid floating-point errors. However, `BigDecimal` operations are slower than primitive operations.

## Array types

Array columns store arrays as values, allowing each cell to contain a list of items.

### Creating array columns

```python test-set=17 order=t20
from deephaven import empty_table

t20 = empty_table(3).update(
    [
        "Index = i",
        # Create arrays of primitives
        "IntArray = new int[]{i, i+1, i+2}",
        "DoubleArray = new double[]{i * 1.5, i * 2.5, i * 3.5}",
        "StringArray = new String[]{`A` + i, `B` + i, `C` + i}",
    ]
)
```

### Working with array columns

Access array elements and properties:

```python test-set=17 order=t21
t21 = t20.update(
    [
        "ArrayLength = IntArray.length",
        "FirstElement = IntArray[0]",
        "LastElement = IntArray[IntArray.length - 1]",
        "SecondDoubleValue = DoubleArray[1]",
    ]
)
```

### Array null handling

Arrays themselves can be null, and array elements can also be null (for object arrays):

```python test-set=18 order=t22
from deephaven import empty_table

t22 = empty_table(5).update(
    [
        "Index = i",
        # Null array
        "NullArray = i % 2 == 0 ? new int[]{i, i+1} : (int[])null",
        "IsArrayNull = isNull(NullArray)",
        # Array with null elements (only for object arrays)
        "StringArrayWithNulls = new String[]{i == 0 ? (String)null : `Value` + i}",
    ]
)
```

### Array operations

Deephaven provides functions for array manipulation:

```python test-set=19 order=t23
from deephaven import empty_table

t23 = empty_table(3).update(
    [
        "Values = new int[]{i, i * 2, i * 3, i * 4, i * 5}",
        "Sum = sum(Values)",
        "Average = avg(Values)",
        "Min = min(Values)",
        "Max = max(Values)",
        "Count = Values.length",
    ]
)
```

## Type conversions and casting

### Explicit casting

Use explicit casts when you need to convert between numeric types:

```python test-set=20 order=t24
from deephaven import empty_table

t24 = empty_table(5).update(
    [
        "DoubleValue = i * 3.7",
        # Explicit casts to narrow types
        "AsInt = (int)DoubleValue",
        "AsLong = (long)DoubleValue",
        "AsByte = (byte)DoubleValue",
        # Widening (no cast needed, but allowed)
        "BackToDouble = (double)AsInt",
    ]
)
```

### Parsing strings to numbers

Convert strings to numeric types:

```python test-set=21 order=t25
from deephaven import empty_table

t25 = empty_table(3).update(
    [
        "StringNum = `` + (i * 100)",
        "AsInt = parseInt(StringNum)",
        "AsLong = parseLong(StringNum)",
        "AsDouble = parseDouble(StringNum + `.5`)",
    ]
)
```

### Formatting numbers as strings

Convert numbers to strings:

```python test-set=22 order=t26
from deephaven import empty_table

t26 = empty_table(3).update(
    [
        "Value = i * 1234.567",
        # Simple string conversion
        "AsString = `` + Value",
        # Formatted string (using Java's String.format)
        "Formatted = String.format(`%.2f`, Value)",
    ]
)
```

### Handling conversion errors

Always validate input before converting to avoid runtime errors. Use conditional logic to handle cases where conversion might fail:

```python test-set=23 order=t27
from deephaven import empty_table
from deephaven.constants import NULL_INT

t27 = empty_table(5).update(
    [
        "StringValue = i % 2 == 0 ? `` + i : `invalid`",
        # Use conditional logic to handle values that can't be parsed
        "SafeConversion = i % 2 == 0 ? parseInt(StringValue) : NULL_INT",
    ]
)
```

## Type considerations in table operations

### Aggregations and types

Different aggregation operations have type-specific behavior:

```python test-set=24 order=t28
from deephaven import empty_table

source = empty_table(100).update(
    [
        "Group = i % 3 == 0 ? `A` : (i % 3 == 1 ? `B` : `C`)",
        "IntValue = (int)i",
        "DoubleValue = i * 1.5",
    ]
)

# Aggregations preserve or promote types
from deephaven import agg

t28 = source.agg_by(
    [
        agg.sum_(["SumInt = IntValue"]),  # Returns long
        agg.sum_(["SumDouble = DoubleValue"]),  # Returns double
        agg.avg(["AvgInt = IntValue"]),  # Returns double
        agg.count_("CountRows"),  # Returns long
    ],
    by="Group",
)
```

### Joins and type matching

Joins require matching types in key columns:

```python test-set=25 order=t29
from deephaven import empty_table

left = empty_table(5).update(["IntKey = (int)i", "LeftValue = i * 10"])
right = empty_table(5).update(["IntKey = (int)i", "RightValue = i * 100"])

# This works - types match
t29 = left.natural_join(right, on="IntKey")
```

Type mismatches will cause errors:

```python syntax
from deephaven import empty_table

left = empty_table(5).update("Key = (int)i")
right = empty_table(5).update("Key = (long)i")

# This would fail - int vs long mismatch
# t_join = left.natural_join(right, on="Key")
```

Cast to matching types when necessary:

```python test-set=26 order=t30
from deephaven import empty_table

left = empty_table(5).update(["Key = (int)i", "LeftValue = i * 10"])
right = empty_table(5).update(["LongKey = (long)i", "RightValue = i * 100"])

# Cast to matching type
t30 = left.update("LongKey = (long)Key").natural_join(right, on="LongKey")
```

### Sorting and type behavior

Sorting behavior varies by type:

```python test-set=27 order=t31
from deephaven import empty_table
from deephaven.constants import NULL_INT

t31 = empty_table(10).update(
    [
        "Number = (int)(10 - i)",
        "Text = `` + (char)('Z' - i)",
        "Timestamp = now() - i * HOUR",
        "NullableInt = i % 3 == 0 ? NULL_INT : (int)i",
    ]
)

# Numeric: ascending by value
sorted_numeric = t31.sort("Number")

# String: lexicographic order
sorted_string = t31.sort("Text")

# Temporal: chronological order
sorted_time = t31.sort("Timestamp")

# Nulls: appear first by default in ascending sort
sorted_with_nulls = t31.sort("NullableInt")
```

## Best practices

### Choose the right type

- **Integers**: Use the smallest type that fits your range (`byte` < `short` < `int` < `long`)
- **Decimals**: Use `double` for most cases, `BigDecimal` only when exact precision is required
- **Timestamps**: Use `Instant` for UTC timestamps, `ZonedDateTime` when time zones matter
- **Text**: Use `String` for text data; consider enum patterns for low-cardinality categories
- **Arrays**: Use when each row needs multiple related values; consider separate columns if querying individual elements frequently

### Optimize for memory

- Prefer primitive types over objects (e.g., `int` over `Integer`, `double` over `BigDecimal`)
- Use appropriate numeric precision (don't use `long` when `int` suffices)
- Be cautious with high-cardinality strings and object columns
- Consider string interning benefits for categorical data

### Ensure type safety

- Always validate and handle nulls explicitly
- Use explicit casts when converting between types to make intent clear
- Match types in join keys and comparisons
- Test edge cases (nulls, extreme values, type boundaries)

### Performance considerations

| Operation            | Fast                            | Slow                           |
| -------------------- | ------------------------------- | ------------------------------ |
| Primitive arithmetic | ✅ `int`, `long`, `double`      | ❌ `BigDecimal`                |
| String operations    | ✅ Low-cardinality strings      | ❌ High-cardinality strings    |
| Null checks          | ✅ Primitive nulls (`NULL_INT`) | ❌ Complex null checking logic |
| Aggregations         | ✅ Numeric primitives           | ❌ Complex objects             |
| Memory usage         | ✅ Primitives, interned strings | ❌ Objects, large arrays       |

## Related documentation

- [Table types](./table-types.md)
- [Query language special variables](../reference/query-language/variables/special-variables.md)
- [Date and time functions](../reference/query-language/functions/date-time.md)
- [Math functions](../reference/query-language/functions/math.md)
- [String functions](../reference/query-language/functions/string.md)
