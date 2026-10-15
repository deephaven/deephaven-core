---
title: Deephaven's column types
---

Deephaven tables store data in strongly typed columns. Each column in a table must have a specific type that determines what values it can hold, how much memory it uses, and how operations behave on that column's data. Understanding column types is essential for designing efficient table schemas, writing correct queries, and optimizing performance. This guide covers the column types available in Deephaven tables, how to convert between them, how types behave in table operations, and how to choose the right type for your data.

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
      <td>✅ (<a href="#null-values-for-primitives">sentinel null values</a>)</td>
      <td>✅</td>
      <td>Numeric calculations, counters, measurements</td>
    </tr>
    <tr>
      <td scope="row"><a href="#boolean-and-char">Boolean/char</a></td>
      <td><code>Boolean</code>, <code>char</code></td>
      <td>✅ <code>char</code> uses a sentinel null value. <code>Boolean</code> uses Java <code>null</code>.</td>
      <td>✅</td>
      <td>Flags, single characters</td>
    </tr>
    <tr>
      <td scope="row"><a href="#string-type">String</a></td>
      <td><code>String</code></td>
      <td>✅</td>
      <td>⚠️ (repeated values share memory only when they are the same object, such as string literals)</td>
      <td>Text data, identifiers, categories</td>
    </tr>
    <tr>
      <td scope="row"><a href="#temporal-types">Temporal</a></td>
      <td><code>Instant</code>, <code>ZonedDateTime</code>, <code>LocalDate</code>, <code>LocalTime</code>, <code>Duration</code></td>
      <td>✅</td>
      <td>⚠️ <code>Instant</code> is stored as a <code>long</code>. Other temporal types are stored as objects.</td>
      <td>Timestamps, dates, times, durations</td>
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
      <td>⚠️ Each array is a separate Java object. Primitive arrays store their elements as raw values.</td>
      <td>Lists of values, time series</td>
    </tr>
  </tbody>
</table>

The rest of this guide explores each of these column types, their properties, and how to use them effectively in your tables.

## Type fundamentals

Numeric primitives, booleans, characters, strings, and temporal (date and time) values are the types most columns use, so understanding them is essential for working effectively with Deephaven tables.

> [!NOTE]
> In query strings, string literals go inside backticks, like `` `Hello` ``, and `char` literals go inside single quotes, like `'A'`. Backticks work the same way as double quotes in Java. See [String and char literals](../how-to-guides/string-char-literals.md) for details.

### Primitive numeric types

Primitive numeric types are the most memory-efficient and performant types in Deephaven. They map directly to Java primitives, so each value is stored as a raw number rather than as a separate Java object.

#### Numeric type reference

| Type     | Size   | Range                                     | Null value    | Example use case                                   |
| -------- | ------ | ----------------------------------------- | ------------- | -------------------------------------------------- |
| `byte`   | 8-bit  | -127 to 127                               | `NULL_BYTE`   | Small integers, status codes                       |
| `short`  | 16-bit | -32,767 to 32,767                         | `NULL_SHORT`  | Medium integers, year values                       |
| `int`    | 32-bit | -2³¹+1 to 2³¹-1                           | `NULL_INT`    | Standard integers, counters                        |
| `long`   | 64-bit | -2⁶³+1 to 2⁶³-1                           | `NULL_LONG`   | Large integers, IDs, nanosecond timestamps         |
| `float`  | 32-bit | ~±3.4×10³⁸ (about 7 significant digits)   | `NULL_FLOAT`  | Approximate decimals, measurements                 |
| `double` | 64-bit | ~±1.7×10³⁰⁸ (about 15 significant digits) | `NULL_DOUBLE` | General-purpose decimals, measurements, statistics |

Deephaven reserves the minimum value of each integer type as that type's null value, so the integer ranges above exclude it. For `float` and `double`, the null value is the most negative finite value (`-Float.MAX_VALUE` and `-Double.MAX_VALUE`).

#### Creating numeric columns

The examples on this page build values from the [special variable](../reference/query-language/variables/special-variables.md) `ii`, which holds each row's index as a `long`.

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

Java primitives can't be null, so Deephaven reserves one value of each numeric primitive type, and of `char`, to represent null. The numeric types use the values described above, and `char` uses `Character.MAX_VALUE`. These reserved values are called sentinel null values. Each type's sentinel has a named [null constant](../how-to-guides/built-in-constants.md#null-constants), such as `NULL_INT` and `NULL_DOUBLE`:

```groovy test-set=1 order=t2
// Create columns with null values
t2 = emptyTable(5).update(
    "Value = ii",
    "WithNulls = ii % 2 == 0 ? (int)ii : NULL_INT",
    "Price = ii % 2 == 0 ? ii * 10.5 : NULL_DOUBLE"
)
```

You can check for nulls with [`isNull`](../reference/query-language/types/nulls.md) or by comparing with the null constant:

```groovy test-set=1 order=t3,t4
// Filter for non-null values
t3 = t2.where("!isNull(WithNulls)")

// Alternatively, compare with null constant
t4 = t2.where("WithNulls != NULL_INT")
```

#### Type promotion and arithmetic

When you do arithmetic with mixed numeric types, Deephaven follows Java's type promotion rules for most operators. Division is different. In query strings, dividing one integer value by another with `/` returns a `double`. In Java, `int / int` truncates instead. To get integer truncation, cast the result explicitly:

```groovy test-set=1 order=t5
t5 = emptyTable(3).update(
    "IntValue = (int)ii + 1",
    "LongValue = (long)ii * 1000",
    // int + long promotes to long
    "MixedIntLong = IntValue + LongValue",
    // int * double promotes to double
    "MixedIntDouble = IntValue * 3.14",
    // In query strings, / promotes to double: 7 / 2 = 3.5
    "DoubleDiv = 7 / 2", // Result: 3.5
    // Cast to int for truncating integer division
    "IntDiv = (int)(7 / 2)" // Result: 3
)
```

### Boolean and char

#### Boolean type

The `Boolean` type represents true/false values. Boolean columns have the data type `java.lang.Boolean`. Their null value is Java `null`, and the [null constant](../how-to-guides/built-in-constants.md#null-constants) `NULL_BOOLEAN` refers to the same value. Deephaven stores their values compactly as bytes, so they don't carry the per-value cost of the [object types](#object-types). Comparison expressions produce `Boolean` columns:

```groovy test-set=1 order=t6
t6 = emptyTable(5).update("Value = ii", "IsEven = ii % 2 == 0", "IsGreaterThanTwo = ii > 2")
```

#### Char type

The `char` type represents a single 16-bit Unicode character:

```groovy test-set=1 order=t7
t7 = emptyTable(5).update("Index = ii", "Letter = (char)('A' + ii)")
```

> [!NOTE]
> `char` is distinct from [`String`](../reference/query-language/types/strings.md). A `char` column holds single characters, while a `String` column holds character sequences.

### String type

The [`String`](../reference/query-language/types/strings.md) type stores text data.

#### Creating string columns

```groovy test-set=1 order=t8
t8 = emptyTable(5).update(
    "Index = ii",
    "Label = `A` + ii", // String concatenation
    "Formatted = `` + ii + ` items`",
    "Conditional = ii % 2 == 0 ? `Even` : `Odd`"
)
```

#### String operations

Values in a `String` column are `java.lang.String` objects, so you can call standard Java `String` methods on them in query strings:

```groovy test-set=1 order=t9
t9 = emptyTable(3).update(
    "Text = ii == 0 ? `Hello World` : (ii == 1 ? `DEEPHAVEN` : `  spaces  `)",
    "Length = Text.length()",
    "Upper = Text.toUpperCase()",
    "Lower = Text.toLowerCase()",
    "Trimmed = Text.trim()",
    "Substring = Text.substring(0, 5)",
    "Contains = Text.contains(`e`)"
)
```

#### Strings and memory

Each value in a `String` column refers to a `String` object, so a column's memory use depends on how many separate objects it holds. Deephaven doesn't deduplicate (intern) strings that formulas build at runtime. A formula that builds strings, such as by concatenation, creates a new `String` object for each row, even when values repeat. A string literal in a formula, such as `` `Active` `` in the example below, is one object shared across rows. A low-cardinality column has few distinct values. When those values come from a few string literals, the column stores only a few objects. A column of computed IDs stores one object per row.

```groovy test-set=1 order=t10,t11
// Low cardinality: only 3 distinct values
t10 = emptyTable(1000).update(
    "Status = ii % 3 == 0 ? `Active` : (ii % 3 == 1 ? `Pending` : `Closed`)"
)

// High cardinality: many distinct values
t11 = emptyTable(1000).update(
    "UniqueId = `ID-` + ii" // 1000 unique strings
)
```

#### Null strings

A null string is different from an empty string:

```groovy test-set=1 order=t12
t12 = emptyTable(5).update(
    "Index = ii",
    "WithNull = ii % 2 == 0 ? `Valid` : (String)null",
    "EmptyString = ii % 2 == 0 ? `Valid` : ``",
    "IsNull = isNull(WithNull)",
    "IsEmpty = EmptyString == ``" // This checks for empty, not null
)
```

### Temporal types

Deephaven provides robust support for the date and time types in the following table, all from Java's [`java.time`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/package-summary.html) package, and Deephaven's [built-in time functions](./time-in-deephaven.md) operate on them, making these types a natural fit for time-series data and temporal analysis. In memory, [`Instant`](../reference/query-language/types/date-time.md) columns are stored compactly as nanosecond `long` values. The other temporal types are stored as objects.

#### Temporal type reference

| Type                                                              | Description               | Null support | Example value                                 |
| ----------------------------------------------------------------- | ------------------------- | ------------ | --------------------------------------------- |
| [`Instant`](../reference/query-language/types/date-time.md)       | Point in time (UTC)       | ✅           | `2025-01-15T10:30:45.123456789Z`              |
| [`ZonedDateTime`](../reference/query-language/types/date-time.md) | Date-time with time zone  | ✅           | `2025-01-15T10:30:45-05:00[America/New_York]` |
| `LocalDate`                                                       | Date without time or zone | ✅           | `2025-01-15`                                  |
| `LocalTime`                                                       | Time without date or zone | ✅           | `10:30:45.123`                                |
| `LocalDateTime`                                                   | Date-time without zone    | ✅           | `2025-01-15T10:30:45`                         |
| [`Duration`](../reference/query-language/types/durations.md)      | Time-based duration       | ✅           | `PT5H30M` (5 hours, 30 minutes)               |
| [`Period`](../reference/query-language/types/periods.md)          | Date-based period         | ✅           | `P1Y2M3D` (1 year, 2 months, 3 days)          |

#### Working with Instant

[`Instant`](../reference/query-language/types/date-time.md) is the most commonly used temporal type, representing an instantaneous point on the timeline in UTC. Adding or subtracting a number of nanoseconds shifts an `Instant`, so you can use the [built-in nanosecond constants](../how-to-guides/built-in-constants.md#date-time-constants) `SECOND`, `MINUTE`, `HOUR`, `DAY`, and `WEEK`. Subtracting one `Instant` from another gives the difference in nanoseconds, and functions such as [`diffMinutes`](../reference/time/datetime/diffMinutes.md) return it in other units:

```groovy test-set=1 order=t13
t13 = emptyTable(3).update(
    "Now = now()",
    "OneMinuteLater = Now + 1 * MINUTE",
    "TwoHoursEarlier = Now - 2 * HOUR",
    "ThreeDaysLater = Now + 3 * DAY",
    // Difference between two Instants, in nanoseconds (long)
    "TimeDiff = Now - (Now - 5 * HOUR)",
    // Difference between two Instants, in minutes (double)
    "MinutesBetween = diffMinutes(TwoHoursEarlier, Now)"
)
```

#### Working with ZonedDateTime

Use [`ZonedDateTime`](../reference/query-language/types/date-time.md) when time zone information is important:

```groovy test-set=1 order=t14
t14 = emptyTable(3).update(
    "UTC = now()",
    "NewYork = toZonedDateTime(UTC, timeZone(`America/New_York`))",
    "Tokyo = toZonedDateTime(UTC, timeZone(`Asia/Tokyo`))",
    // true = local clock hour, correct across daylight saving time changes
    "HourOfDay = hourOfDay(NewYork, true)",
    "HourOfDayFromInstant = hourOfDay(UTC, timeZone(`America/New_York`), true)"
)
```

> [!IMPORTANT]
> Always be explicit about time zones when converting between `Instant` and zone-aware types. Implicit conversions can lead to subtle bugs, especially around daylight saving time transitions.

#### Working with LocalDate and LocalTime

`LocalDate` and `LocalTime` are useful for date-only or time-only operations:

```groovy test-set=1 order=t15
t15 = emptyTable(5).update(
    "Timestamp = now() + ii * DAY",
    "DateOnly = toLocalDate(Timestamp, timeZone(`America/New_York`))",
    "TimeOnly = toLocalTime(Timestamp, timeZone(`America/New_York`))",
    "DayOfWeekValue = dayOfWeekValue(DateOnly)",
    "IsWeekend = DayOfWeekValue == 6 || DayOfWeekValue == 7"
)
```

## Advanced types

For specialized use cases, object and array columns provide flexibility beyond primitive types. Object columns hold any Java object, such as a [`BigDecimal`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigDecimal.html), and [array columns](../reference/query-language/types/arrays.md) hold several values in each row.

### Object types

Common [object types](../reference/query-language/types/objects.md) include [`BigDecimal`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigDecimal.html), [`BigInteger`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigInteger.html), and custom classes. `String` and every temporal type except `Instant` are stored as Java objects too, so the considerations below apply to them as well. This section covers the remaining object types.

#### Object column considerations

- **Performance**: Object columns are slower than primitive columns because each value is a separate heap object accessed by reference, and operations on them can't use primitive-specialized code paths.
- **Memory**: Objects use more memory than primitives because each value carries an object header in addition to its data.
- **Null handling**: Objects can be Java `null` rather than a sentinel value.

#### Working with BigDecimal

Use [`BigDecimal`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigDecimal.html) when you need exact decimal arithmetic that `double` can't provide. The following example creates a `BigDecimal` column and multiplies it by another `BigDecimal`. Because `BigDecimal` isn't imported into query strings by default, the formulas use its full class name:

```groovy test-set=1 order=t16
t16 = emptyTable(3).update(
    "Quantity = ii + 1",
    "Price = new java.math.BigDecimal(`1.10`)",
    "Total = Price.multiply(new java.math.BigDecimal(Quantity))"
)
```

### Array types

[Array columns](../reference/query-language/types/arrays.md) store arrays as values, allowing each cell to contain a list of items. Operations such as [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) produce Deephaven vector columns (for example, `IntVector`) rather than Java arrays. See [Arrays vs. vectors](./arrays-vs-vectors.md) for the differences.

#### Creating array columns

```groovy test-set=1 order=t17
t17 = emptyTable(3).update(
    "Index = ii",
    // Create arrays of primitives and of objects
    "IntArray = new int[]{(int)ii, (int)ii + 1, (int)ii + 2}",
    "DoubleArray = new double[]{ii * 1.5, ii * 2.5, ii * 3.5}",
    "StringArray = new String[]{`A` + ii, `B` + ii, `C` + ii}"
)
```

#### Working with array columns

Access array elements and properties:

```groovy test-set=1 order=t18
t18 = t17.update(
    "ArrayLength = IntArray.length",
    "FirstElement = IntArray[0]",
    "LastElement = IntArray[IntArray.length - 1]",
    "SecondDoubleValue = DoubleArray[1]"
)
```

#### Array null handling

Arrays themselves can be null. Elements of object arrays can be `null`, and elements of primitive arrays can hold the type's null value (for example, `NULL_INT`):

```groovy test-set=1 order=t19
t19 = emptyTable(5).update(
    "Index = ii",
    // Null array
    "NullArray = ii % 2 == 0 ? new int[]{(int)ii, (int)ii + 1} : (int[])null",
    "IsArrayNull = isNull(NullArray)",
    // Arrays with null elements
    "StringArrayWithNulls = new String[]{ii == 0 ? (String)null : `Value` + ii}",
    "IntArrayWithNulls = new int[]{(int)ii, NULL_INT}"
)
```

#### Array operations

Deephaven's [built-in functions](../how-to-guides/built-in-functions.md), such as the [math functions](../reference/query-language/query-library/auto-imported/math.md) `sum`, `avg`, `min`, and `max`, operate on arrays:

```groovy test-set=1 order=t20
t20 = emptyTable(3).update(
    "Values = new int[]{(int)ii, (int)ii * 2, (int)ii * 3, (int)ii * 4, (int)ii * 5}",
    "Sum = sum(Values)",
    "Average = avg(Values)",
    "Min = min(Values)",
    "Max = max(Values)",
    "Count = Values.length"
)
```

## Type conversions and casting

The most common ways a numeric or string value changes type are:

- **Widening**: A numeric value widens to a larger numeric type automatically in arithmetic, as described in [Type promotion and arithmetic](#type-promotion-and-arithmetic).
- **Narrowing**: A numeric value narrows, for example from `double` to `int`, only with an explicit cast, and it can lose precision.
- **Parsing**: A string never becomes a number automatically. Convert it with a parsing function such as [`parseInt`](../reference/query-language/query-library/auto-imported/parse.md), or with a constructor such as ``new java.math.BigDecimal(`1.10`)``.
- **Formatting**: A number becomes a string when you concatenate it with a string or format it explicitly.

### Narrowing with explicit casts

Narrowing conversions need an explicit cast. Casting a `double` to an integer type truncates the fractional part:

```groovy test-set=1 order=t21
t21 = emptyTable(5).update(
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

```groovy test-set=1 order=t22
t22 = emptyTable(3).update(
    "StringNum = `` + (ii * 100)",
    "AsInt = parseInt(StringNum)",
    "AsLong = parseLong(StringNum)",
    "AsDouble = parseDouble(StringNum + `.5`)"
)
```

### Formatting numbers as strings

Convert numbers to strings:

```groovy test-set=1 order=t23
t23 = emptyTable(3).update(
    "Value = ii * 1234.567",
    // Simple string conversion
    "AsString = `` + Value",
    // Formatted string (using Java's String.format)
    "Formatted = String.format(`%.2f`, Value)"
)
```

### Handling conversion errors

Parsing functions such as [`parseInt`](../reference/query-language/query-library/auto-imported/parse.md) return a null value for a null string, but throw a `NumberFormatException` for a string that isn't a valid number. Check the string before you parse it:

```groovy test-set=1 order=t24
t24 = emptyTable(5).update(
    "StringValue = ii % 2 == 0 ? `` + ii : `invalid`",
    // Parse only strings of digits; use NULL_INT for the rest. Digit strings outside the int range still throw.
    "SafeConversion = StringValue.matches(`-?[0-9]+`) ? parseInt(StringValue) : NULL_INT"
)
```

## Using types effectively

Understanding how types behave in table operations helps you write queries that perform well and are easy to maintain. This section shows how column types affect [aggregations](../how-to-guides/combined-aggregations.md), [joins](../how-to-guides/joins-exact-relational.md), and [sorts](../reference/table-operations/sort/sort.md), then gives guidelines for choosing types, keeping queries type-safe, and keeping queries fast.

### Aggregations and types

An [aggregation's](../how-to-guides/combined-aggregations.md) output column doesn't always have its input column's type. The following [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) example uses three aggregation operators:

- [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md) returns a `long` for an `int` column and a `double` for a `double` column.
- [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md) returns a `double`.
- [`AggCount`](../reference/table-operations/group-and-aggregate/AggCount.md) returns a `long`.

```groovy test-set=1 order=t25
import static io.deephaven.api.agg.Aggregation.AggSum
import static io.deephaven.api.agg.Aggregation.AggAvg
import static io.deephaven.api.agg.Aggregation.AggCount

source = emptyTable(100).update(
    "Group = ii % 3 == 0 ? `A` : (ii % 3 == 1 ? `B` : `C`)",
    "IntValue = (int)ii",
    "DoubleValue = ii * 1.5"
)

// Aggregations preserve or promote types
t25 = source.aggBy(
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

[Joins](../how-to-guides/joins-exact-relational.md) such as [`naturalJoin`](../reference/table-operations/join/natural-join.md) require matching types in key columns:

```groovy test-set=1 order=t26
left = emptyTable(5).update("IntKey = (int)ii", "LeftValue = ii * 10")
right = emptyTable(5).update("IntKey = (int)ii", "RightValue = ii * 100")

// This works - types match
t26 = left.naturalJoin(right, "IntKey")
```

Type mismatches cause errors:

```groovy syntax
left = emptyTable(5).update("Key = (int)ii")
right = emptyTable(5).update("Key = (long)ii")

// This would fail - int vs long mismatch
// tJoin = left.naturalJoin(right, "Key")
```

Cast to matching types when necessary:

```groovy test-set=1 order=t27
left = emptyTable(5).update("Key = (int)ii", "LeftValue = ii * 10")
right = emptyTable(5).update("LongKey = (long)ii", "RightValue = ii * 100")

// Cast to matching type
t27 = left.update("LongKey = (long)Key").naturalJoin(right, "LongKey")
```

### Sorting and type behavior

[Sorting](../reference/table-operations/sort/sort.md) orders each column by its type: numbers by value, strings lexicographically, and temporal values chronologically. In an ascending sort, null values come first:

```groovy test-set=1 order=sortedNumeric,sortedString,sortedTime,sortedWithNulls,t28
t28 = emptyTable(10).update(
    "Number = (int)(10 - ii)",
    "Text = `` + (char)('Z' - ii)",
    "Timestamp = now() - ii * HOUR",
    "NullableInt = ii % 3 == 0 ? NULL_INT : (int)ii"
)

// Numeric: ascending by value
sortedNumeric = t28.sort("Number")

// String: lexicographic order
sortedString = t28.sort("Text")

// Temporal: chronological order
sortedTime = t28.sort("Timestamp")

// Nulls: appear first in an ascending sort
sortedWithNulls = t28.sort("NullableInt")
```

### Choosing the right type

- **Integers**: Use the smallest type that fits your range (`byte` < `short` < `int` < `long`).
- **Decimals**: Use `double` for most cases, [`BigDecimal`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigDecimal.html) only when exact precision is required.
- **Timestamps**: Use [`Instant`](../reference/query-language/types/date-time.md) for UTC timestamps and [`ZonedDateTime`](../reference/query-language/types/date-time.md) when time zones matter.
- **Text**: Use [`String`](../reference/query-language/types/strings.md) for text data. Store low-cardinality categories as `String` values or small integer codes.
- **Arrays**: Use arrays when each row needs multiple related values. Use separate columns instead if you query individual elements often.

### Type safety

- Handle nulls explicitly, for example with [`isNull`](../reference/query-language/types/nulls.md) or a comparison with the type's null constant.
- Use explicit casts when converting between types to make intent clear.
- Match types in [join](../how-to-guides/joins-exact-relational.md) keys. Comparisons between different numeric types work without a cast.
- Test edge cases (nulls, extreme values, type boundaries).

### Performance considerations

| Concern                                                   | Efficient                               | Less efficient                                                                                            |
| --------------------------------------------------------- | --------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| Arithmetic                                                | ✅ `int`, `long`, `double`              | ❌ [`BigDecimal`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigDecimal.html) |
| [Aggregations](../how-to-guides/combined-aggregations.md) | ✅ Numeric primitives                   | ❌ Objects                                                                                                |
| Memory usage                                              | ✅ Primitives, repeated string literals | ❌ Objects, large arrays                                                                                  |

## Related documentation

- [Table types](./table-types.md)
- [Data types in Deephaven](../how-to-guides/data-types.md)
- [Work with strings](../how-to-guides/work-with-strings.md)
- [Query language special variables](../reference/query-language/variables/special-variables.md)
- [Built-in functions](../how-to-guides/built-in-functions.md)
- [Built-in constants](../how-to-guides/built-in-constants.md)
