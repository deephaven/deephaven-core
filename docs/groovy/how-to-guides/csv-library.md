---
title: Use the Deephaven CSV library
---

The [Deephaven CSV library](https://github.com/deephaven/deephaven-csv) is a high-performance, column-oriented, type-inferring CSV parser. Unlike row-oriented CSV libraries, it organizes data into columns rather than rows, which allows for more efficient storage and retrieval. It can also infer the types of those columns from the input, so you don't have to specify the column types beforehand. Finally, it lets you specify the underlying data structures used for columnar storage. The library then writes the parsed values, chunk by chunk, directly into your preferred data structure, so you don't have to copy them out of an intermediate typed column afterward.

This guide is for Java developers who use the library directly. To read CSV files into Deephaven tables, use [`readCsv`](../reference/data-import-export/CSV/readCsv.md). For details, see [Read CSV or other delimited files](./data-import-export/csv-import.md).

The library writes each parsed column into a _sink_, an object that receives the column's values. A sink can wrap a Java array, your own custom column type, or any other destination that can accept writes out of order (see [The `Sink` interface](#the-sink-interface)).

You don't tell the library which column types to use. Instead:

- You provide a _sink factory_, an object that constructs a sink for any column type the library requests.
- The library decides which sinks it needs as it parses the input.

This guide first shows how to read a CSV file with the library's array-based reference sink factory, then explains what you need to implement to have the library write directly into your own column data structures. We hope it makes clear what you need to build, and gives you a reference implementation to use as a starting point.

## Use the reference implementation

To help you get started, the library provides a sink factory, [`SinkFactory.arrays`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/SinkFactory.html), that uses Java arrays for the underlying column representation. This version is best suited for simple examples and for learning how to use the library.

[`CsvSpecs`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.html) holds the parsing options, such as the delimiter, header handling, parsers, and null literals. Use [`CsvSpecs.csv`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.html) for the defaults, or build your own with [`CsvSpecs.builder`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.html) and its `build` method. The following example passes the specs and the array sink factory to [`CsvReader.read`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/reading/CsvReader.html):

```java skip-test
final CsvSpecs specs = ...;
final InputStream inputStream = ...;
final CsvReader.Result result = CsvReader.read(specs, inputStream,
        SinkFactory.arrays());
final long numRows = result.numRows();
for (CsvReader.ResultColumn col : result) {
    switch (col.dataType()) {
        case BOOLEAN_AS_BYTE: {
            byte[] data = (byte[]) col.data();
            // Process this boolean-as-byte column.
            // Be sure to use numRows rather than data.length, because
            // the underlying array might have more capacity than numRows.
            process(data, numRows);
            break;
        }
        case INT: {
            int[] data = (int[]) col.data();
            process(data, numRows); // process this int column
            break;
        }
        // etc...
    }
}
```

[`col.dataType`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/reading/CsvReader.ResultColumn.html) returns a [`DataType`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/parsers/DataType.html). Handle each type your [`CsvSpecs`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.html) can produce:

- The default parsers (`CsvSpecs.csv`) produce `BOOLEAN_AS_BYTE`, `INT`, `LONG`, `DOUBLE`, `DATETIME_AS_LONG`, `CHAR`, and `STRING`.
- `BYTE`, `SHORT`, `FLOAT`, and `TIMESTAMP_AS_LONG` appear only if you add their parsers.
- `CUSTOM` appears only for custom parsers.

With no arguments, [`SinkFactory.arrays`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/SinkFactory.html) has no null representation for the primitive types:

- It throws an exception when a non-string column contains a null value.
- By default, an empty cell is a null value. To treat other text, such as `"NULL"`, as null, see [Handle nulls](#handle-nulls).

If your input can contain nulls, call the [`SinkFactory.arrays`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/SinkFactory.html) overload that takes a _null sentinel_ for each type, such as `Integer.MIN_VALUE` for `int`. A sentinel is a value of the column's type that stands for null. The overload takes its arguments in this order:

```java skip-test
SinkFactory.arrays(
        Byte.MIN_VALUE,      // byte
        Short.MIN_VALUE,     // short
        Integer.MIN_VALUE,   // int
        Long.MIN_VALUE,      // long
        -Float.MAX_VALUE,    // float
        -Double.MAX_VALUE,   // double
        Byte.MIN_VALUE,      // boolean as byte
        Character.MIN_VALUE, // char
        null,                // String
        Long.MIN_VALUE,      // DateTime as long
        Long.MIN_VALUE)      // Timestamp as long
```

[Handle nulls](#handle-nulls) explains how sentinels affect type inference.

## Write your own data sinks

A production program could use the array-based sinks from [`SinkFactory.arrays`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/SinkFactory.html), as shown in [Use the reference implementation](#use-the-reference-implementation). However, if your system defines its own column data structures, it is more efficient for the library to write directly to them, rather than first writing to arrays and then copying the data to its final destination.

Type-specific column data structures are more efficient than `ArrayList` or another catch-all collection, and the difference grows with data size. High-performance applications therefore often have one column type per primitive type, such as `YourCharColumnType`, `YourIntColumnType`, and `YourDoubleColumnType`. This means some repetitive work: you write a sink for each of these types.

Deephaven's own [`readCsv`](../reference/data-import-export/CSV/readCsv.md) works this way: it builds its sinks with [`SinkFactory.of`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/SinkFactory.html) and Deephaven's null sentinels, so the library writes parsed values straight into Deephaven column sources.

To connect your own column data structures to the library, follow these four steps:

1. Wrap each of your data structures with an adapter class that implements the library's [`Sink<TARRAY>`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/Sink.html), where `TARRAY` is the array type of the blocks of values the library passes to the sink, such as `int[]` for an `int` column or `String[]` for a `String` column.
2. Create a [`SinkFactory`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/SinkFactory.html) that provides these wrapped data structures on demand, either by implementing the `SinkFactory` interface or by calling one of its factory methods. See [Create a `SinkFactory`](#create-a-sinkfactory).
3. If your data structures have a representation for a distinct null value, write code to support the translation to that value. See [Handle nulls](#handle-nulls).
4. Optionally, let the library read values back from your integral sinks, so it can widen a column's type (for example, from `int` to `long`) without parsing the input again. See [Support the fast path for numeric type inference](#support-the-fast-path-for-numeric-type-inference).

The following sections describe each step, then show how to [put it all together](#put-it-all-together).

### Wrap your data structures with adapter classes

The library can ask for the following adapter classes, one per type it can produce. This guide names them `MyXXXSink`, but you can give them any names you like.

1. `class MyByteSink implements Sink<byte[]>`
2. `class MyShortSink implements Sink<short[]>`
3. `class MyIntSink implements Sink<int[]>`
4. `class MyLongSink implements Sink<long[]>`
5. `class MyFloatSink implements Sink<float[]>`
6. `class MyDoubleSink implements Sink<double[]>`
7. `class MyBooleanAsByteSink implements Sink<byte[]>`
8. `class MyCharSink implements Sink<char[]>`
9. `class MyStringSink implements Sink<String[]>`
10. `class MyDateTimeAsLongSink implements Sink<long[]>`, for date-time text that the library converts to a `long` epoch value
11. `class MyTimestampAsLongSink implements Sink<long[]>`, for numeric columns that the library reads as epoch timestamps

You can skip any sink whose type your [`CsvSpecs`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.html) never parses. For example, the default parsers include a date-time parser but no timestamp parser, so with the defaults you need `MyDateTimeAsLongSink` but not `MyTimestampAsLongSink`. See the end of [Create a `SinkFactory`](#create-a-sinkfactory).

Each adapter class must:

1. Hold a reference to your actual underlying column data structure.
2. Implement the [`write`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/Sink.html) method from [`Sink<TARRAY>`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/Sink.html) to copy data to that data structure.
3. Implement the [`getUnderlying`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/Sink.html) method to give the underlying data structure back to you when done.

#### The `Sink` interface

The definition of the [`Sink<TARRAY>`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/Sink.html) interface is:

```java skip-test
public interface Sink<TARRAY> {
    void write(TARRAY src, boolean[] isNull, long destBegin, long destEnd,
        boolean appending);
    Object getUnderlying();
}
```

As it populates a column, the library repeatedly calls [`write`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/Sink.html) with chunks of data. Your `write` method must:

1. Ensure that the target column data structure has enough capacity.
2. Copy the data to the target column data structure.
3. If your data structure can represent null values, process them appropriately.

`write` takes five arguments:

1. `src`: the source data, a temporary array. Copy `destEnd - destBegin` elements from it, starting at index 0.
2. `isNull`: a parallel array of booleans. When `isNull[i]` is `true`, ignore `src[i]` and treat the element as null.
3. `destBegin`: the inclusive start index of the destination data range.
4. `destEnd`: the exclusive end index of the destination data range.
5. `appending`: `true` if the write extends the column past the last point written so far, and `false` if the whole range is below that point.

Each call to `write` is entirely one case or the other, never a mix of the two. How you use `appending` depends on your data structure:

- Sparse data structures, such as hash tables, can ignore it.
- Growable lists, such as `ArrayList`, can use it to decide whether to add elements or replace them.

The flag is a convenience. Comparing `destEnd` with the current size of your data structure gives the same answer.

Whichever you use, `write` must accept nonsequential calls, because the type inference algorithm sometimes writes the end of a column before its beginning. For example, the library might first append rows 50-99 to a `Sink<double[]>` and then fill in rows 0-49. A non-appending write can therefore overwrite values already written or fill in rows the library skipped. An appending write can start past the current end of the column, so a growable structure must pad the gap (for example, with nulls) before it adds the new values.

[Handle nulls](#handle-nulls) shows how to process null values in `write`.

#### Sample `MyIntSink`

Here is a sample implementation of `MyIntSink`, using a growable `int[]` as the underlying data structure.

Its constructor takes an `int` column index. The factory methods of [`SinkFactory`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/SinkFactory.html), described in [Create a `SinkFactory`](#create-a-sinkfactory), take one [`IntFunction`](https://docs.oracle.com/javase/8/docs/api/java/util/function/IntFunction.html) per sink type, which receives the column index and returns a new sink. The factory examples on this page pass constructor references such as `MyIntSink::new` as those functions:

```java skip-test
private static final class MyIntSink implements Sink<int[]> {
    private static final int INITIAL_SIZE = 1024;

    private int[] array;

    public MyIntSink(int columnIndex) {
        array = new int[INITIAL_SIZE];
    }

    @Override
    public void write(int[] src, boolean[] isNull, long destBegin,
                      long destEnd, boolean appending) {
        if (destBegin == destEnd) {
            return;
        }
        final int destBeginAsInt = Math.toIntExact(destBegin);
        final int destEndAsInt = Math.toIntExact(destEnd);
        final int destSize = destEndAsInt - destBeginAsInt;

        if (array.length < destEndAsInt) {
            final int highBit = Integer.highestOneBit(destEndAsInt);
            final int newCapacity =
                destEndAsInt == highBit ? highBit : highBit * 2;
            final int[] newArray = new int[newCapacity];
            System.arraycopy(array, 0, newArray, 0, array.length);
            array = newArray;
        }

        // Write chunk to storage.
        System.arraycopy(src, 0, array, destBeginAsInt, destSize);
    }

    @Override
    public Object getUnderlying() { return array; }
}
```

### Create a `SinkFactory`

Once you've written adapters for the sink types you need, you need to create a sink factory, which provides the adapters to the library on demand. To do this, you can either implement the [`SinkFactory`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/SinkFactory.html) interface or call one of its factory methods. The library can call the factory from several threads at once, so a class that implements `SinkFactory` directly must be thread safe. The factory methods synchronize their calls for you.

This example calls [`SinkFactory.ofSimple`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/SinkFactory.html), which takes one sink constructor for each column type, in the order shown:

```java skip-test
private static SinkFactory makeMySinkFactory() {
    return SinkFactory.ofSimple(
            MyByteSink::new,
            MyShortSink::new,
            MyIntSink::new,
            MyLongSink::new,
            MyFloatSink::new,
            MyDoubleSink::new,
            MyBooleanAsByteSink::new,
            MyCharSink::new,
            MyStringSink::new,
            MyDateTimeAsLongSink::new,
            MyTimestampAsLongSink::new);
}
```

If your [`CsvSpecs`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.html) doesn't include a parser for a type, the library never infers that type, so you can pass `null` for that type's sink. There is one exception. `CsvSpecs` also has a _null parser_, which is `STRING` by default. For a column that is empty or contains only nulls:

- If more than one parser is configured for the column (the `CsvSpecs` parsers list, unless you set parsers for that column), the library uses the null parser, even if your parsers don't include it. Always supply a sink for the null parser's type.
- If only one parser is configured for the column, the library uses that parser instead.

### Handle nulls

The CSV format has no notion of null. Many systems, including Deephaven, represent null values, so the library supports them.

- [`CsvSpecs`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.html) holds one or more null literals. By default, the only null literal is the empty string.
- To use others, such as `"NULL"`, pass the complete list to [`nullValueLiterals`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.Builder.html) on [`CsvSpecs.builder`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/CsvSpecs.html), for example `List.of("", "NULL")`. The list replaces the default, so include `""` if empty cells should stay null.
- When the library reads a null literal, it marks that element as null in the `isNull` array it passes to `write`.

Your sink decides how to store a null. The library supports both common approaches:

- A sentinel value reserved from the data type, such as `Integer.MIN_VALUE` for `int`.
- A separate boolean flag for each element.

If your data structure keeps its own null flags:

- In `write`, copy `isNull[i]` into your flag for each element.
- If you [support the fast path](#support-the-fast-path-for-numeric-type-inference), set `isNull[i]` from your flag in `read`.
- Use the [`SinkFactory`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/SinkFactory.html) overloads without sentinels.

The rest of this section shows the sentinel approach, which you add by modifying the `write` method from the [sample `MyIntSink`](#sample-myintsink).

Let's assume that your system uses `Integer.MIN_VALUE` as a null sentinel for the `int` type. Add this loop to `MyIntSink.write`, after the capacity check and before the `System.arraycopy` call that writes the chunk to storage. Add the same loop, with each type's sentinel, to every `MyXXXSink` class that stores nulls this way.

`MyBooleanAsByteSink` works the same way. Pick any null value other than 0 or 1, such as `Byte.MIN_VALUE`. The factory methods take no sentinel for this type, because parsed boolean values are always 0 or 1 and can't collide with it.

```java skip-test
// This is the new null-handling code, which conveniently
// modifies the source data in place before processing it
for (int i = 0; i < destSize; ++i) {
    if (isNull[i]) {
        src[i] = Integer.MIN_VALUE;
    }
}
```

The complete `MyIntSink` in [Support the fast path for numeric type inference](#support-the-fast-path-for-numeric-type-inference) shows this loop in place.

You also need to give your sentinel values to the library, so type inference doesn't treat them as ordinary values. If the input contains a numeric or `char` sentinel, inference moves to the next wider type. For example, if your null sentinel for `int` is `-2147483648` and the input contains the text `-2147483648`, the library infers the column as `long` rather than `int`. If the input contains a date-time or timestamp sentinel, that parser rejects the column and inference tries the remaining parsers. A `String` sentinel in the input makes the read fail, because the `String` parser is the last one tried.

You pass sentinel values to the library through the [`SinkFactory`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/SinkFactory.html). If you implement `SinkFactory` directly, return your sentinels from its `reservedByte`, `reservedShort`, `reservedInt`, and other `reserved*` methods, and, for the fast path, set the `source` argument of `forByte`, `forShort`, `forInt`, and `forLong` to your sink. If you use the factory methods, call this longer overload of [`SinkFactory.ofSimple`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/SinkFactory.html):

```java skip-test
private static SinkFactory makeMySinkFactory() {
    return SinkFactory.ofSimple(
            MyByteSink::new,
            Byte.MIN_VALUE, // assuming this is your null sentinel value
            MyShortSink::new,
            Short.MIN_VALUE, // likewise
            MyIntSink::new,
            Integer.MIN_VALUE,
            MyLongSink::new,
            Long.MIN_VALUE,
            MyFloatSink::new,
            -Float.MAX_VALUE,
            MyDoubleSink::new,
            -Double.MAX_VALUE,
            MyBooleanAsByteSink::new,
            // no reserved value for boolean as byte: parsed values are only 0 and 1
            MyCharSink::new,
            Character.MIN_VALUE,
            MyStringSink::new,
            null, // a String sentinel in the input makes the read fail
            MyDateTimeAsLongSink::new,
            Long.MIN_VALUE,
            MyTimestampAsLongSink::new,
            Long.MIN_VALUE);
}
```

### Support the fast path for numeric type inference

The four integral sinks (`byte`, `short`, `int`, and `long`) can optionally support faster type inference, at the cost of some additional implementation effort. When the library widens a column from one numeric type to another (for example, from `int` to `long` or `double`), it reads the data back from your column data structure instead of parsing the input again. Implement this optimization only if your data structure supports it easily.

To implement it:

- Make `MyByteSink`, `MyShortSink`, `MyIntSink`, and `MyLongSink` implement the [`Source<TARRAY>`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/Source.html) interface as well.
- Build your factory with [`SinkFactory.of`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/SinkFactory.html) instead of [`SinkFactory.ofSimple`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/SinkFactory.html), as shown at the end of this section.

The definition of [`Source<TARRAY>`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/Source.html) is:

```java skip-test
public interface Source<TARRAY> {
    void read(final TARRAY dest, final boolean[] isNull,
        final long srcBegin, final long srcEnd);
}
```

When it reads data back from a column, the library repeatedly calls [`read`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/Source.html) to get chunks of data. Your `read` method must:

1. Copy the data from the column data structure.
2. If your data structure can represent null values, process them appropriately.

`read` takes four arguments:

1. `dest`: the destination data, a temporary array. Copy `srcEnd - srcBegin` elements into it, starting at index 0.
2. `isNull`: a parallel array of booleans. If your data structure supports nulls, set `isNull[i]` to `true` for each null element and to `false` for every other element. The library ignores `dest[i]` when `isNull[i]` is `true`.
3. `srcBegin`: the inclusive start index of the source data range.
4. `srcEnd`: the exclusive end index of the source data range.

The library reads back only elements it has already written.

What follows is a complete implementation of `MyIntSink`, including a `Source<int[]>` implementation and null value handling:

```java skip-test
private static final class MyIntSink implements Sink<int[]>, Source<int[]> {
    private static final int INITIAL_SIZE = 1024;

    private int[] array;

    public MyIntSink(int columnIndex) {
        array = new int[INITIAL_SIZE];
    }

    @Override
    public void write(int[] src, boolean[] isNull, long destBegin,
                      long destEnd, boolean appending) {
        if (destBegin == destEnd) {
            return;
        }
        final int destBeginAsInt = Math.toIntExact(destBegin);
        final int destEndAsInt = Math.toIntExact(destEnd);
        final int destSize = destEndAsInt - destBeginAsInt;

        if (array.length < destEndAsInt) {
            final int highBit = Integer.highestOneBit(destEndAsInt);
            final int newCapacity =
                destEndAsInt == highBit ? highBit : highBit * 2;
            final int[] newArray = new int[newCapacity];
            System.arraycopy(array, 0, newArray, 0, array.length);
            array = newArray;
        }

        for (int i = 0; i < destSize; ++i) {
            if (isNull[i]) {
                src[i] = Integer.MIN_VALUE;
            }
        }

        // Write chunk to storage.
        System.arraycopy(src, 0, array, destBeginAsInt, destSize);
    }

    // new code here: implement Source<int[]>
    @Override
    public void read(int[] dest, boolean[] isNull, long srcBegin,
            long srcEnd) {
        if (srcBegin == srcEnd) {
            return;
        }
        final int srcBeginAsInt = Math.toIntExact(srcBegin);
        final int srcSize = Math.toIntExact(srcEnd - srcBegin);
        System.arraycopy(array, srcBeginAsInt, dest, 0, srcSize);
        for (int i = 0; i < srcSize; ++i) {
            isNull[i] = dest[i] == Integer.MIN_VALUE;
        }
    }

    @Override
    public Object getUnderlying() { return array; }
}
```

[`SinkFactory.of`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/SinkFactory.html) takes the same arguments as [`SinkFactory.ofSimple`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/SinkFactory.html), with or without sentinels, but requires the `byte`, `short`, `int`, and `long` sinks to implement both [`Sink`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/Sink.html) and [`Source`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/Source.html). `SinkFactory.ofSimple` never uses a `Source`, so a factory built with it does not use the fast path.

Because `MyIntSink` uses `Integer.MIN_VALUE` as its null sentinel, the example below calls the [`SinkFactory.of`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/SinkFactory.html) overload that also takes the sentinel values, with the same values as the longer `SinkFactory.ofSimple` call in [Handle nulls](#handle-nulls):

```java skip-test
private static SinkFactory makeMySinkFactory() {
    return SinkFactory.of(
            MyByteSink::new,
            Byte.MIN_VALUE,
            MyShortSink::new,
            Short.MIN_VALUE,
            MyIntSink::new,
            Integer.MIN_VALUE,
            MyLongSink::new,
            Long.MIN_VALUE,
            MyFloatSink::new,
            -Float.MAX_VALUE,
            MyDoubleSink::new,
            -Double.MAX_VALUE,
            MyBooleanAsByteSink::new,
            MyCharSink::new,
            Character.MIN_VALUE,
            MyStringSink::new,
            null,
            MyDateTimeAsLongSink::new,
            Long.MIN_VALUE,
            MyTimestampAsLongSink::new,
            Long.MIN_VALUE);
}
```

### Put it all together

You now have everything you need to use your own data structures with the library. Take the example code in [Use the reference implementation](#use-the-reference-implementation) and make two changes:

1. Replace the call to [`SinkFactory.arrays`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/SinkFactory.html) with a call to the version of `makeMySinkFactory` that matches your sinks:
   - Sinks without sentinel values: the [`SinkFactory.ofSimple`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/SinkFactory.html) version in [Create a `SinkFactory`](#create-a-sinkfactory).
   - Sinks that store sentinel values: the longer [`SinkFactory.ofSimple`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/SinkFactory.html) version in [Handle nulls](#handle-nulls).
   - Sinks that store sentinel values and implement [`Source`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/Source.html): the [`SinkFactory.of`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/sinks/SinkFactory.html) version in [Support the fast path for numeric type inference](#support-the-fast-path-for-numeric-type-inference), which also enables the fast path.
2. Change each cast of [`col.data`](https://javadoc.io/doc/io.deephaven/deephaven-csv/0.19.0/io/deephaven/csv/reading/CsvReader.ResultColumn.html) to the type that the matching sink's `getUnderlying` method returns. For example, cast to `int[]` for the sample `MyIntSink`, or to `YourIntColumnType` if your sink wraps one.

## Related documentation

- [deephaven-csv GitHub repository](https://github.com/deephaven/deephaven-csv)
- [A High-Performance CSV Reader with Type Inference](/blog/2022/02/23/csv-reader/)
- [Read CSV or other delimited files](./data-import-export/csv-import.md)
- [`readCsv`](../reference/data-import-export/CSV/readCsv.md)
