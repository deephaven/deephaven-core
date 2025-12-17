---
title: Use the Deephaven CSV library
sidebar_label: The Deephaven CSV library
---

The [Deephaven CSV Library](https://github.com/deephaven/deephaven-csv) is a high-performance, column-oriented, type inferencing CSV parser. It differs from other CSV libraries in that it organizes data into columns rather than rows, which allows for more efficient storage and retrieval. It also can dynamically infer the types of those columns based on the input, so the caller is not required to specify the column types beforehand. Finally it provides a way for the caller to specify the underlying data structures used for columnar storage, This allows the library to store its data directly in the caller's preferred data structure, without the inefficiency of going through intermediate temporary objects.

The Deephaven CSV Library is agnostic about what data sink you use, and it works equally well with Java arrays, your own custom column type, or perhaps even streaming to a file. But along with this flexibility comes extra programming effort on part of the implementor: instead of telling the library what column data structures to use, the caller provides a "factory" capable of constructing any requested column type, and the library then dynamically decides which ones it needs as it parses the input data. While it is tempting to just use ArrayList or some other catch-all collection, this is not as efficient as type-specific collectors, and makes a large impact on performance as data sizes increase. Instead, it is common practice in high-performance libraries to provide multiple, very similar but distinct implementations, one for each primitive type. For example, your high-performance application might have `YourCharColumnType`, `YourIntColumnType`, `YourDoubleColumnType`, and the like. Unfortunately this translates into a certain amount of tedium for the implementor, who needs to provide implementations for each type and code to move data from the CSV library to them.

With this guide we hope to make it clear what the caller needs to implement, and also to provide a reference implementation for people to use as a starting point.

## Use the reference implementation

To help you get started, the library provides a "sink factory" that uses Java arrays for the underlying column representation. This version is best suited for simple examples and for learning how to use the library. Developers of production applications will likely want to define their own column representations and create the sink factory that supplies them. The next section describes how to do this. For now, we show how to process data using the sink factory for arrays:

```
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
        case SHORT: {
            short[] data = (short[]) col.data();
            process(data, numRows); // process this short column
            break;
        }
        // etc...
    }
}
```

## Write your own data sinks

A production program could simply use Java arrays in the manner described above. However, if your system defines its own column data structures, it will be more efficient for the library to write directly to them, rather than first writing to arrays and then copying the data to its final destination.

To interface your own column data structures to the library, there are four steps:

1. Wrap each of your data structures with an adaptor class that implements our interface `Sink<TARRAY>`
2. Write a factory class, implementing our interface SinkFactory, that provides these wrapped data structures on demand
3. If your data structures have a representation for a distinct NULL value, write code to support the translation to that value
4. If you wish to support our fast path for numeric type inference optimization, write additional code to support that optimization.

These steps are outlined in more detail below.

### Wrap your data structures with adaptor classes

These are the adaptor classes you need to write (for convenience we will name them MyXXXSink, though of course you can name them whatever you like):

1. `class MyByteSink implements Sink<byte[]>`
2. `class MyShortSink implements Sink<short[]>`
3. `class MyIntSink implements Sink<int[]>`
4. `class MyLongSink implements Sink<long[]>`
5. `class MyFloatSink implements Sink<float[]>`
6. `class MyDoubleSink implements Sink<double[]>`
7. `class MyBooleanAsByteSink implements Sink<byte[]>`
8. `class MyCharSink implements Sink<char[]>`
9. `class MyStringSink implements Sink<String[]>`
10. `class MyDateTimeAsLongSink implements Sink<long[]>` (if your system has a notion of DateTime)
11. `class MyTimestampAsLongSink implements Sink<long[]>` (if your system has a notion of Timestamp)

The job of each wrapper class is to:

1. hold a reference to your actual underlying column data structure,
2. implement the `write()` method from `Sink<TARRAY>` in order to copy data to that data structure, and
3. implement the `getUnderlying()` method to give the underlying data structure back to the caller when done.

### Implement an adaptor class

The definition of the `Sink<TARRAY>` interface is:

```
public interface Sink<TARRAY> {
    void write(TARRAY src, boolean[] isNull, long destBegin, long destEnd,
        boolean appending);
    Object getUnderlying();
}
```

As it is populating a column, the Deephaven CSV Library will repeatedly call `write()` with chunks of data. It is the job of `write()` to:

1. Ensure that the target column data structure has enough capacity.
2. Copy the data to the target column data structure.
3. If your data structure is capable of representing NULL values, process them appropriately.

There are five arguments to `write()`:

1. `src` - the source data. This is a temporary array from which the data should be copied. The data should be copied from array index 0, and the number of elements to be copied is given by (`destEnd - destBegin`).
2. `isNull` - a parallel array of booleans. If `isNull[i]` is `true` for some index `i`, this means that the value at `src[i]` should be ignored; instead, the corresponding element should be considered to have a null value. We will discuss null handling in a later section.
3. `destBegin` the inclusive start index of the destination data range.
4. `destEnd` - the exclusive end index of the destination data range.
5. `appending` - this flag is set to true if the data being written will "grow" the column; i.e., if it is appending data beyond the last point where data was written before. If false, the data overwrites a previous range in the column. Note that some sparse data structures like hashtables don't care about this distinction. On the other hand, growable list types like `ArrayList` do care. Also note that every call will either be fully-appending or fully-overwriting: there is no partial case where it overwrites some data and then appends some more. The reason this flag exists is due to the type inference algorithm: in some cases the system will write the "suffix" of a column prior to writing its "prefix". For example the system might first append rows 50-99 to a `Sink<double[]>` and then come back and fill in rows 0-49.

#### Sample

Here is a sample implementation of `MyIntSink`, using `TIntArrayList` as the underlying data structure:

```
private static final class MyIntSink implements Sink<int[]> {
    private static final int INITIAL_SIZE = 1024;

    private int[] array;

    public MyIntSink() {
        array = new int[INITIAL_SIZE];
    }

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

    public Object getUnderlying() { return array; }
}
```

### Create a SinkFactory

Once you've written adaptors for all the sink types, you need to create a "sink factory" which provides the adaptors to the library on demand. To do this, you can either implement our `SinkFactory` interface or call one of its convenient factory methods. In our example, we would call `SinkFactory.ofSimple`, as described below. Also, if you know that a sink type is unused by your input, you can pass in null for that type.

```
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

### Put it all together

We now have everything we need to use our own data structures with the library. Simply take the example code in the [Use the Reference Implementation](#use-the-reference-implementation) section and change `SinkFactory.trove()` to `makeMySinkFactory()`.

## Handle nulls

Although the CSV specification itself doesn't contemplate the concept of "null value", many systems such as Deephaven do have such a concept, so it makes sense for the library to support it. Typically the caller will configure a null literal (perhaps the empty string or even the string "NULL"), and when the library encounters that string in the input text it will encode it as the appropriate null representation.

What constitutes the "appropriate null representation" of course depends on your implementation. Some implementations reserve a special sentinel value from the data type. For example, they may use Integer.MIN_VALUE to represent the null value for the integer types. Other implementations keep a boolean flag off to the side which indicates whether that element is null. Deephaven supports both approaches. To handle nulls, you will need to modify the `write()` method that we described [above](#sample).

Let's assume that your system uses `Integer.MIN_VALUE` as a null sentinel for the int type. These are the modifications needed for `MyIntSink`, and you would need to do something similar for all your `MyXXXSinks`.

```
private static final class MyIntSink implements Sink<int[]> {
    private static final int INITIAL_SIZE = 1024;

    private int[] array;

    public MyIntSink() {
        array = new int[INITIAL_SIZE];
    }

    public void write(int[] src, boolean[] isNull, long destBegin,
                      long destEnd, boolean appending) {
        if (destBegin == destEnd) {
            return;
        }
        final int destBeginAsInt = Math.toIntExact(destBegin);
        final int destEndAsInt = Math.toIntExact(destEnd);
        final int destSize = destEndAsInt - destBeginAsInt;

        // This is the new null-handling code, which conveniently
        // modifies the source data in place before processing it
        for (int i = 0; i < size; ++i) {
            if (isNull[i]) {
                src[i] = Integer.MIN_VALUE;
            }
        }

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

    public Object getUnderlying() { return array; }
}
```

If your implementation uses sentinel null values (rather than, say, representing them with a separate boolean flag), you also need to provide these values to the library. This is necessary so that type inference can correctly block those values from being considered as ordinary values. For most purposes the library doesn't care whether or not you're using sentinels or what their values are, but the type inference algorithm needs to know because it needs to make the right choice if it happens to encounter a sentinel in the input. For example if your sentinel null value for int is -2147483648, then the input text "-2147483648" should not be considered to be an int; rather it needs to be interpreted as a long. Sentinel values are conveyed to the library via the `SinkFactory` interface. For example, If you are using the factory methods, you can invoke this longer overload of `SinkFactory.ofSimple`:

```
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
            // no sentinel needed for boolean as byte
            MyCharSink::new,
            Character.MIN_VALUE,
            MyStringSink::new,
            null, // typically no special sentinel needed for String
            MyDateTimeAsLongSink::new,
            Long.MIN_VALUE,
            MyTimestampAsLongSink::new,
            Long.MIN_VALUE);
}
```

## Support the fast path for numeric type inference

There is an optional optimization available for the four integral sinks (namely byte, short, int, and long), which allows them to support faster type inference at the cost of some additional implementation effort. This optimization allows the library to read data back from your collection rather than reparsing the input when it needs to widen the type. To implement it, the corresponding four adaptor classes (`MyByteSink`, `MyShortSink`, `MyIntSink`, `MyLongSink`) should implement the `Source<TARRAY>` interface as well. Because this is an optional optimization, you should only implement it if your data structure can easily support it

The definition of `Source<TARRAY>` is:

```
public interface Source<TARRAY> {
    void read(final TARRAY dest, final boolean[] isNull,
        final long srcBegin, final long srcEnd);
}
```

When it is reading back data from a column, the library will repeatedly call `read()` to get chunks of data. It is the job of `read()` to:

1. Copy the data from the column data structure.
2. If your data structure is capable of representing null values, process them appropriately.

These are the four arguments to read:

1. `dest` - the destination data. This is a temporary array to which the data should be copied. The data should be copied starting at array index 0, and the number of elements to be copied is given by (`destEnd - destBegin`).
2. `isNull` - a parallel array of booleans. If nulls are supported, the implementor should set `isNull[i]` to `true` for each element that represents a null value, otherwise it should set it to `false`. If `isNull[i]` is `true`, then the corresponding value in `dest[i]` will be ignored.
3. `srctBegin` - the inclusive start index of the source data range.
4. `srcEnd` - the exclusive end index of the source data range. The library promises to only read from elements that it has previously written to.

What follows is a complete implementation of `MyIntSink`, including a `Source<int[]>` implementation and null value handling:

```
private static final class MyIntSink implements Sink<int[]>, Source<int[]> {
    private static final int INITIAL_SIZE = 1024;

    private int[] array;

    public MyIntSink() {
        array = new int[INITIAL_SIZE];
    }

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
        for (int ii = 0; ii < srcSize; ++ii) {
            isNull[ii] = dest[ii] == Integer.MIN_VALUE;
        }
    }

    public Object getUnderlying() { return array; }
}
```

## Related documentation

- [deephaven-csv GitHub repository](https://github.com/deephaven/deephaven-csv)
- [A High-Performance CSV Reader with Type Inference](/blog/2022/02/23/csv-reader/)
