---
title: Deephaven's column types
sidebar_label: Column types
---

Deephaven tables store data in strongly-typed columns. Each column in a table must have a specific type that determines what values it can hold, how much memory it uses, and how operations behave on that column's data. Understanding column types is essential for designing efficient table schemas, writing correct queries, and optimizing performance. This guide covers the column types available in Deephaven tables, how to choose the right type for your data, and how types behave in table operations.

## Column type overview

Deephaven table columns support a rich type system built on Java's type system. The main column type categories are:

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
      <td scope="row"><a href="#time-and-dates">Temporal</a></td>
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

The rest of this guide explores each of these column types, their properties, and how to use them effectively in your tables.

## Type fundamentals

Understanding the fundamental types — numeric primitives, booleans, characters, and strings — is essential for working effectively with Deephaven tables.

### Primitive numeric types

Primitive numeric types are the most memory-efficient and performant types in Deephaven. They map directly to Java primitives and are stored unboxed in memory.

#### Numeric type reference

| Type     | Size   | Range                             | Null value    | Example use case                           |
| -------- | ------ | --------------------------------- | ------------- | ------------------------------------------ |
| `byte`   | 8-bit  | -128 to 127                       | `NULL_BYTE`   | Small integers, status codes               |
| `short`  | 16-bit | -32,768 to 32,767                 | `NULL_SHORT`  | Medium integers, year values               |
| `int`    | 32-bit | -2³¹ to 2³¹-1                     | `NULL_INT`    | Standard integers, counters                |
| `long`   | 64-bit | -2⁶³ to 2⁶³-1                     | `NULL_LONG`   | Large integers, IDs, nanosecond timestamps |
| `float`  | 32-bit | ~±3.4×10³⁸ (7 digits precision)   | `NULL_FLOAT`  | Approximate decimals, measurements         |
| `double` | 64-bit | ~±1.7×10³⁰⁸ (15 digits precision) | `NULL_DOUBLE` | High-precision decimals, financial data    |

#### Creating numeric columns

```groovy test-set=1 order=t1
// Create columns of various numeric types
t1 = emptyTable(5).update(
    "ByteCol = (byte)(ii % 100)",
    "ShortCol = (short)(ii * 1000)",
    "IntCol = (int)(ii * 1000000)",
    "LongCol = (long)(ii * 1000000000)",
    "FloatCol = (float)(ii * 1.5)",
    "DoubleCol = ii * 3.14159"
)
```

#### Null values for primitives

Unlike Java primitives, Deephaven's primitive columns can represent null values using special sentinel values:

```groovy test-set=1 order=t2
// Create columns with null values
t2 = emptyTable(5).update(
    "Value = ii",
    "WithNulls = ii % 2 == 0 ? (int)ii : NULL_INT",
    "Price = ii % 2 == 0 ? ii * 10.5 : NULL_DOUBLE"
)
```

You can check for nulls using `isNull` or comparison with the null constant:

```groovy test-set=1 order=t3
// Filter for non-null values
t3 = t2.where("!isNull(WithNulls)")

// Alternatively, compare with null constant
t4 = t2.where("WithNulls != NULL_INT")
```

#### Type promotion and arithmetic

When performing arithmetic with mixed numeric types, Deephaven follows Java's type promotion rules:

```groovy test-set=1 order=t5
t5 = emptyTable(3).update(
    "IntValue = (int)ii + 1",
    "LongValue = (long)ii * 1000",
    // int + long promotes to long
    "MixedIntLong = IntValue + LongValue",
    // int * double promotes to double
    "MixedIntDouble = IntValue * 3.14",
    // Division with integers performs integer division
    "IntDiv = 7 / 2", // Result: 3
    // Use double to get floating-point division
    "DoubleDiv = 7.0 / 2.0" // Result: 3.5
)
```

### Primitive boolean and char

#### Boolean type

The `boolean` type represents true/false values:

```groovy test-set=1 order=t6
t6 = emptyTable(5).update("Value = ii", "IsEven = ii % 2 == 0", "IsPositive = ii > 2")
```

#### Char type

The `char` type represents a single 16-bit Unicode character:

```groovy test-set=1 order=t7
t7 = emptyTable(5).update("Index = ii", "Letter = (char)('A' + ii)")
```

> [!NOTE]
> `char` is distinct from `String`. A `char` column holds single characters, while a `String` column holds character sequences.

## Time and dates

Deephaven provides robust support for date and time types, all based on Java 8's `java.time` package. These types are optimized for efficient storage and time-based operations, making them ideal for time-series data and temporal analysis.

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

```groovy test-set=1 order=t8
t8 = emptyTable(5).update(
    "Timestamp = now() + ii * SECOND",
    "HoursLater = Timestamp + 5 * HOUR",
    "DaysBefore = Timestamp - 2 * DAY"
)
```

### Working with ZonedDateTime

Use `ZonedDateTime` when time zone information is important:

```groovy test-set=1 order=t9
t9 = emptyTable(3).update(
    "UTC = now()",
    "NewYork = toZonedDateTime(UTC, timeZone(`America/New_York`))",
    "Tokyo = toZonedDateTime(UTC, timeZone(`Asia/Tokyo`))",
    "HourOfDay = hourOfDay(NewYork, false)"
)
```

### Working with LocalDate and LocalTime

`LocalDate` and `LocalTime` are useful for date-only or time-only operations:

```groovy test-set=1 order=t10
t10 = emptyTable(5).update(
    "Timestamp = now() + ii * DAY",
    "DateOnly = toLocalDate(Timestamp, timeZone(`America/New_York`))",
    "TimeOnly = toLocalTime(Timestamp, timeZone(`America/New_York`))",
    "DayOfWeekValue = dayOfWeekValue(DateOnly)",
    "IsWeekend = DayOfWeekValue == 6 || DayOfWeekValue == 7"
)
```

### Temporal arithmetic

Deephaven provides constants and functions for temporal calculations:

```groovy test-set=1 order=t11
t11 = emptyTable(3).update(
    "Now = now()",
    // Duration constants: SECOND, MINUTE, HOUR, DAY, WEEK
    "OneMinuteLater = Now + 1 * MINUTE",
    "TwoHoursEarlier = Now - 2 * HOUR",
    "ThreeDaysLater = Now + 3 * DAY",
    // Calculate differences
    "TimeDiff = Now - (Now - 5 * HOUR)"
)
```

### Time zone considerations

> [!IMPORTANT]
> Always be explicit about time zones when converting between `Instant` and zone-aware types. Implicit conversions can lead to subtle bugs, especially around daylight saving time transitions.

```groovy test-set=1 order=t12
t12 = emptyTable(2).update(
    "UTC_Time = now()",
    // Explicit time zone conversion
    "NY_Time = toZonedDateTime(UTC_Time, timeZone(`America/New_York`))",
    "London_Time = toZonedDateTime(UTC_Time, timeZone(`Europe/London`))",
    // Extract components with time zone awareness
    "NY_Hour = hourOfDay(UTC_Time, timeZone(`America/New_York`), false)",
    "London_Hour = hourOfDay(UTC_Time, timeZone(`Europe/London`), false)"
)
```

### String type

The `String` type stores text data. Deephaven automatically interns strings to optimize memory usage for low-cardinality string columns.

#### Creating string columns

```groovy test-set=1 order=t13
t13 = emptyTable(5).update(
    "Index = ii",
    "Letter = `A` + ii", // String concatenation
    "Formatted = `` + ii + ` items`",
    "Conditional = ii % 2 == 0 ? `Even` : `Odd`"
)
```

#### String operations

String columns in Deephaven are `java.lang.String` objects, which means you can use standard Java String methods directly:

```groovy test-set=1 order=t14
t14 = emptyTable(3).update(
    "Text = ii == 0 ? `Hello World` : (ii == 1 ? `DEEPHAVEN` : `  spaces  `)",
    "Length = Text.length()",
    "Upper = Text.toUpperCase()",
    "Lower = Text.toLowerCase()",
    "Trimmed = Text.trim()",
    "Substring = Text.substring(0, 5)",
    "Contains = Text.contains(`e`)"
)
```

#### String interning and memory

Deephaven automatically interns strings, which means identical string values share the same memory location. This is very efficient for low-cardinality columns (like categories or symbols) but less beneficial for high-cardinality data (like unique IDs or free-form text).

```groovy test-set=1 order=t15
// Low cardinality: memory efficient (only 3 unique strings stored)
t15 = emptyTable(1000).update(
    "Status = ii % 3 == 0 ? `Active` : (ii % 3 == 1 ? `Pending` : `Closed`)"
)

// High cardinality: less efficient (many unique strings)
t16 = emptyTable(1000).update(
    "UniqueId = `ID-` + ii" // 1000 unique strings
)
```

#### Null strings

Strings can be null, which is different from empty strings:

```groovy test-set=1 order=t17
t17 = emptyTable(5).update(
    "Index = ii",
    "WithNull = ii % 2 == 0 ? `Valid` : (String)null",
    "EmptyString = ii % 2 == 0 ? `Valid` : ``",
    "IsNull = isNull(WithNull)",
    "IsEmpty = WithNull == ``" // This checks for empty, not null
)
```

## Advanced types

For specialized use cases, Deephaven supports object types and arrays that provide flexibility beyond primitive types.

### Object types

Object types can store any Java object. Common examples include `BigDecimal`, `BigInteger`, and custom classes.

#### Object column considerations

- **Performance**: Object columns are slower than primitive columns due to boxing/unboxing overhead.
- **Memory**: Objects require more memory (object header + data).
- **Null handling**: Objects can be `null` (Java null, not a sentinel value).
- **Use cases**: High-precision arithmetic (`BigDecimal`), custom data structures, complex types.

> [!NOTE]
> For high-precision decimal arithmetic, use `java.math.BigDecimal` instead of `double` to avoid floating-point errors. However, `BigDecimal` operations are slower than primitive operations.

### Array types

Array columns store arrays as values, allowing each cell to contain a list of items.

#### Creating array columns

```groovy test-set=1 order=t20
t20 = emptyTable(3).update(
    "Index = ii",
    // Create arrays of primitives
    "IntArray = new int[]{(int)ii, (int)ii + 1, (int)ii + 2}",
    "DoubleArray = new double[]{ii * 1.5, ii * 2.5, ii * 3.5}",
    "StringArray = new String[]{`A` + ii, `B` + ii, `C` + ii}"
)
```

#### Working with array columns

Access array elements and properties:

```groovy test-set=1 order=t21
t21 = t20.update(
    "ArrayLength = IntArray.length",
    "FirstElement = IntArray[0]",
    "LastElement = IntArray[IntArray.length - 1]",
    "SecondDoubleValue = DoubleArray[1]"
)
```

#### Array null handling

Arrays themselves can be null, and array elements can also be null (for object arrays):

```groovy test-set=1 order=t22
t22 = emptyTable(5).update(
    "Index = ii",
    // Null array
    "NullArray = ii % 2 == 0 ? new int[]{(int)ii, (int)ii + 1} : (int[])null",
    "IsArrayNull = isNull(NullArray)",
    // Array with null elements (only for object arrays)
    "StringArrayWithNulls = new String[]{ii == 0 ? (String)null : `Value` + ii}"
)
```

#### Array operations

Deephaven provides functions for array manipulation:

```groovy test-set=1 order=t23
t23 = emptyTable(3).update(
    "Values = new int[]{(int)ii, (int)ii * 2, (int)ii * 3, (int)ii * 4, (int)ii * 5}",
    "Sum = sum(Values)",
    "Average = avg(Values)",
    "Min = min(Values)",
    "Max = max(Values)",
    "Count = Values.length"
)
```

## Type conversions and casting

### Explicit casting

Use explicit casts when you need to convert between numeric types:

```groovy test-set=1 order=t24
t24 = emptyTable(5).update(
    "DoubleValue = ii * 3.7",
    // Explicit casts to narrow types
    "AsInt = (int)DoubleValue",
    "AsLong = (long)DoubleValue",
    "AsByte = (byte)DoubleValue",
    // Widening (no cast needed, but allowed)
    "BackToDouble = (double)AsInt"
)
```

### Parsing strings to numbers

Convert strings to numeric types:

```groovy test-set=1 order=t25
t25 = emptyTable(3).update(
    "StringNum = `` + (ii * 100)",
    "AsInt = parseInt(StringNum)",
    "AsLong = parseLong(StringNum)",
    "AsDouble = parseDouble(StringNum + `.5`)"
)
```

### Formatting numbers as strings

Convert numbers to strings:

```groovy test-set=1 order=t26
t26 = emptyTable(3).update(
    "Value = ii * 1234.567",
    // Simple string conversion
    "AsString = `` + Value",
    // Formatted string (using Java's String.format)
    "Formatted = String.format(`%.2f`, Value)"
)
```

### Handling conversion errors

Always validate input before converting to avoid runtime errors. Use conditional logic to handle cases where conversion might fail:

```groovy test-set=1 order=t27
t27 = emptyTable(5).update(
    "StringValue = ii % 2 == 0 ? `` + ii : `invalid`",
    // Use conditional logic to handle values that can't be parsed
    "SafeConversion = ii % 2 == 0 ? parseInt(StringValue) : NULL_INT"
)
```

## Using types effectively

Understanding how types behave in table operations and following best practices ensures optimal performance and maintainability.

### Aggregations and types

Different aggregation operations have type-specific behavior:

```groovy test-set=1 order=t28
import static io.deephaven.api.agg.Aggregation.AggSum
import static io.deephaven.api.agg.Aggregation.AggAvg
import static io.deephaven.api.agg.Aggregation.AggCount

source = emptyTable(100).update(
    "Group = ii % 3 == 0 ? `A` : (ii % 3 == 1 ? `B` : `C`)",
    "IntValue = (int)ii",
    "DoubleValue = ii * 1.5"
)

// Aggregations preserve or promote types
t28 = source.aggBy(
    [
        AggSum("SumInt = IntValue"),    // Returns long
        AggSum("SumDouble = DoubleValue"), // Returns double
        AggAvg("AvgInt = IntValue"),    // Returns double
        AggCount("CountRows")           // Returns long
    ],
    "Group"
)
```

### Joins and type matching

Joins require matching types in key columns:

```groovy test-set=1 order=t29
left = emptyTable(5).update("IntKey = (int)ii", "LeftValue = ii * 10")
right = emptyTable(5).update("IntKey = (int)ii", "RightValue = ii * 100")

// This works - types match
t29 = left.naturalJoin(right, "IntKey")
```

Type mismatches will cause errors:

```groovy skip-test
left = emptyTable(5).update("Key = (int)ii")
right = emptyTable(5).update("Key = (long)ii")

// This would fail - int vs long mismatch
// t_join = left.naturalJoin(right, "Key")
```

Cast to matching types when necessary:

```groovy test-set=1 order=t30
left = emptyTable(5).update("Key = (int)ii", "LeftValue = ii * 10")
right = emptyTable(5).update("LongKey = (long)ii", "RightValue = ii * 100")

// Cast to matching type
t30 = left.update("LongKey = (long)Key").naturalJoin(right, "LongKey")
```

### Sorting and type behavior

Sorting behavior varies by type:

```groovy test-set=1 order=t31
t31 = emptyTable(10).update(
    "Number = (int)(10 - ii)",
    "Text = `` + (char)('Z' - ii)",
    "Timestamp = now() - ii * HOUR",
    "NullableInt = ii % 3 == 0 ? NULL_INT : (int)ii"
)

// Numeric: ascending by value
sorted_numeric = t31.sort("Number")

// String: lexicographic order
sorted_string = t31.sort("Text")

// Temporal: chronological order
sorted_time = t31.sort("Timestamp")

// Nulls: appear first by default in ascending sort
sorted_with_nulls = t31.sort("NullableInt")
```

### Choose the right type

- **Integers**: Use the smallest type that fits your range (`byte` < `short` < `int` < `long`).
- **Decimals**: Use `double` for most cases, `BigDecimal` only when exact precision is required.
- **Timestamps**: Use `Instant` for UTC timestamps, `ZonedDateTime` when time zones matter.
- **Text**: Use `String` for text data. Consider enum patterns for low-cardinality categories.
- **Arrays**: Use when each row needs multiple related values. Consider separate columns if querying individual elements frequently.

### Optimize for memory

- Prefer primitive types over objects (e.g., `int` over `Integer`, `double` over `BigDecimal`).
- Use appropriate numeric precision (don't use `long` when `int` suffices).
- Be cautious with high-cardinality strings and object columns.
- Consider string interning benefits for categorical data.

### Ensure type safety

- Always validate and handle nulls explicitly.
- Use explicit casts when converting between types to make intent clear.
- Match types in join keys and comparisons.
- Test edge cases (nulls, extreme values, type boundaries).

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
- [Data types in Deephaven and Groovy](../how-to-guides/data-types.md)
- [Work with strings](../how-to-guides/work-with-strings.md)
- [Query language special variables](../reference/query-language/variables/special-variables.md)
- [Built-in functions](../how-to-guides/built-in-functions.md)
- [Built-in constants](../how-to-guides/built-in-constants.md)
