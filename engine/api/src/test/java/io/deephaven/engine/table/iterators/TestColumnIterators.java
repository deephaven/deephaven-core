//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.iterators;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.ChunkColumnSource;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.util.mutable.MutableInt;
import org.junit.*;

import java.util.Objects;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;

import static io.deephaven.util.QueryConstants.NULL_BYTE;
import static io.deephaven.util.QueryConstants.NULL_CHAR;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.util.QueryConstants.NULL_SHORT;
import static io.deephaven.util.type.TypeUtils.box;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

/**
 * Unit tests for {@link ColumnIterator} implementations.
 */
public class TestColumnIterators {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private Table input;

    @Before
    public void setUp() {
        input = TstUtils.getTable(false, 100_000, new Random(0), new ColumnInfo[] {
                new ColumnInfo<>(new CharGenerator('A', 'z', 0.1), "CharCol"),
                new ColumnInfo<>(new ByteGenerator((byte) -100, (byte) 100, 0.1), "ByteCol"),
                new ColumnInfo<>(new ShortGenerator((short) -1000, (short) 1000, 0.1), "ShortCol"),
                new ColumnInfo<>(new IntGenerator(-100_000_000, 100_000_000, 0.1), "IntCol"),
                new ColumnInfo<>(new LongGenerator(-100_000_000_000L, 100_000_000_000L, 0.1), "LongCol"),
                new ColumnInfo<>(new FloatGenerator(-100_000F, 100_000F, 0.1), "FloatCol"),
                new ColumnInfo<>(new DoubleGenerator(-100_000_000_000.0, 100_000_000_000.0, 0.1), "DoubleCol"),
                new ColumnInfo<>(new BooleanGenerator(0.44, 0.1), "BooleanCol"),
                new ColumnInfo<>(new StringGenerator(1000), "StringCol")
        });
    }

    @After
    public void tearDown() {
        input = null;
    }

    @Test
    public void testCharacterColumnIterators() {
        final ColumnSource<Character> source = input.getColumnSource("CharCol", char.class);
        // @formatter:off
        try (final ChunkSource.GetContext sourceContext = source.makeGetContext(input.intSize());
             final CharacterColumnIterator serial1 = new SerialCharacterColumnIterator(source, input.getRowSet());
             final CharacterColumnIterator serial2 = new SerialCharacterColumnIterator(source, input.getRowSet());
             final CharacterColumnIterator serial3 = new SerialCharacterColumnIterator(source, input.getRowSet());
             final CharacterColumnIterator chunked1 = new ChunkedCharacterColumnIterator(source, input.getRowSet());
             final CharacterColumnIterator chunked2 = new ChunkedCharacterColumnIterator(source, input.getRowSet());
             final CharacterColumnIterator chunked3 = new ChunkedCharacterColumnIterator(source, input.getRowSet())) {
            // @formatter:on
            final CharChunk<?> data = source.getChunk(sourceContext, input.getRowSet()).asCharChunk();
            final int dataSize = data.size();
            for (int vi = 0; vi < dataSize; ++vi) {
                final char value = data.get(vi);
                assertEquals(serial1.remaining(), dataSize - vi);
                assertEquals(chunked1.remaining(), dataSize - vi);
                assertTrue(serial1.hasNext());
                assertTrue(chunked1.hasNext());
                assertEquals(value, serial1.nextChar());
                assertEquals(value, chunked1.nextChar());
                assertEquals(box(value), serial2.next());
                assertEquals(box(value), chunked2.next());
            }
            final MutableInt nextValueIndex = new MutableInt(0);
            assertEquals(0, serial3.stream()
                    .filter((final Character value) -> !(Objects.equals(value,
                            box(data.get(nextValueIndex.getAndIncrement())))))
                    .count());
            nextValueIndex.set(0);
            assertEquals(0, chunked3.stream()
                    .filter((final Character value) -> !(Objects.equals(value,
                            box(data.get(nextValueIndex.getAndIncrement())))))
                    .count());
        }
    }

    private static ChunkColumnSource<Character> createCharacterSource() {
        final WritableCharChunk<Values> data = WritableCharChunk.makeWritableChunk(10_000);
        for (int ei = 0; ei < data.size(); ++ei) {
            data.set(ei, ei % 4 == 0 ? NULL_CHAR : (char) ei);
        }

        // noinspection unchecked
        final ChunkColumnSource<Character> source =
                (ChunkColumnSource<Character>) ChunkColumnSource.make(ChunkType.Char, char.class);
        source.addChunk(data);
        return source;
    }

    @Test
    public void testCharacterColumnIteratorForBoxedNulls() {
        final ChunkColumnSource<Character> source = createCharacterSource();
        try (final RowSet rowset = RowSetFactory.flat(10_000);
                final CharacterColumnIterator serial = new SerialCharacterColumnIterator(source, rowset);
                final CharacterColumnIterator chunked = new ChunkedCharacterColumnIterator(source, rowset)) {

            final MutableInt numNulls = new MutableInt(0);
            final Consumer<Character> nullValidator = c -> {
                if (c == null) {
                    numNulls.add(1);
                } else if (c == NULL_CHAR) {
                    throw new IllegalStateException("Expected null, but got boxed NULL_CHAR");
                }
            };
            serial.stream().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());

            numNulls.set(0);
            chunked.stream().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());
        }
        source.clear();
    }

    @Test
    public void testCharacterColumnIteratorForStreamAsIntNulls() {
        final ChunkColumnSource<Character> source = createCharacterSource();

        try (final RowSet rowset = RowSetFactory.flat(10_000);
                final CharacterColumnIterator serial = new SerialCharacterColumnIterator(source, rowset);
                final CharacterColumnIterator chunked = new ChunkedCharacterColumnIterator(source, rowset)) {
            final MutableInt numNulls = new MutableInt(0);
            final IntConsumer nullValidator = c -> {
                if (c == NULL_INT) {
                    numNulls.add(1);
                } else if (c == NULL_CHAR) {
                    throw new IllegalStateException("Expected NULL_INT, but got NULL_CHAR");
                }
            };
            serial.streamAsInt().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());

            numNulls.set(0);
            chunked.streamAsInt().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());
        }
        source.clear();
    }

    @Test
    public void testByteColumnIterators() {
        final ColumnSource<Byte> source = input.getColumnSource("ByteCol", byte.class);
        // @formatter:off
        try (final ChunkSource.GetContext sourceContext = source.makeGetContext(input.intSize());
             final ByteColumnIterator serial1 = new SerialByteColumnIterator(source, input.getRowSet());
             final ByteColumnIterator serial2 = new SerialByteColumnIterator(source, input.getRowSet());
             final ByteColumnIterator serial3 = new SerialByteColumnIterator(source, input.getRowSet());
             final ByteColumnIterator chunked1 = new ChunkedByteColumnIterator(source, input.getRowSet());
             final ByteColumnIterator chunked2 = new ChunkedByteColumnIterator(source, input.getRowSet());
             final ByteColumnIterator chunked3 = new ChunkedByteColumnIterator(source, input.getRowSet())) {
            // @formatter:on
            final ByteChunk<?> data = source.getChunk(sourceContext, input.getRowSet()).asByteChunk();
            final int dataSize = data.size();
            for (int vi = 0; vi < dataSize; ++vi) {
                final byte value = data.get(vi);
                assertEquals(serial1.remaining(), dataSize - vi);
                assertEquals(chunked1.remaining(), dataSize - vi);
                assertTrue(serial1.hasNext());
                assertTrue(chunked1.hasNext());
                assertEquals(value, serial1.nextByte());
                assertEquals(value, chunked1.nextByte());
                assertEquals(box(value), serial2.next());
                assertEquals(box(value), chunked2.next());
            }
            final MutableInt nextValueIndex = new MutableInt(0);
            assertEquals(0, serial3.stream()
                    .filter((final Byte value) -> !(Objects.equals(value,
                            box(data.get(nextValueIndex.getAndIncrement())))))
                    .count());
            nextValueIndex.set(0);
            assertEquals(0, chunked3.stream()
                    .filter((final Byte value) -> !(Objects.equals(value,
                            box(data.get(nextValueIndex.getAndIncrement())))))
                    .count());
        }
    }

    private static ChunkColumnSource<Byte> createByteSource() {
        final WritableByteChunk<Values> data = WritableByteChunk.makeWritableChunk(10_000);
        for (int ei = 0; ei < data.size(); ++ei) {
            data.set(ei, ei % 4 == 0 ? NULL_BYTE : (byte) ei);
        }

        // noinspection unchecked
        final ChunkColumnSource<Byte> source =
                (ChunkColumnSource<Byte>) ChunkColumnSource.make(ChunkType.Byte, byte.class);
        source.addChunk(data);
        return source;
    }

    @Test
    public void testByteColumnIteratorForBoxedNulls() {
        final ChunkColumnSource<Byte> source = createByteSource();
        try (final RowSet rowset = RowSetFactory.flat(10_000);
                final ByteColumnIterator serial = new SerialByteColumnIterator(source, rowset);
                final ByteColumnIterator chunked = new ChunkedByteColumnIterator(source, rowset)) {

            final MutableInt numNulls = new MutableInt(0);
            final Consumer<Byte> nullValidator = c -> {
                if (c == null) {
                    numNulls.add(1);
                } else if (c == NULL_BYTE) {
                    throw new IllegalStateException("Expected null, but got boxed NULL_BYTE");
                }
            };
            serial.stream().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());

            numNulls.set(0);
            chunked.stream().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());
        }
        source.clear();
    }

    @Test
    public void testByteColumnIteratorForStreamAsIntNulls() {
        final ChunkColumnSource<Byte> source = createByteSource();

        try (final RowSet rowset = RowSetFactory.flat(10_000);
                final ByteColumnIterator serial = new SerialByteColumnIterator(source, rowset);
                final ByteColumnIterator chunked = new ChunkedByteColumnIterator(source, rowset)) {
            final MutableInt numNulls = new MutableInt(0);
            final IntConsumer nullValidator = c -> {
                if (c == NULL_INT) {
                    numNulls.add(1);
                } else if (c == NULL_BYTE) {
                    throw new IllegalStateException("Expected NULL_INT, but got NULL_BYTE");
                }
            };
            serial.streamAsInt().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());

            numNulls.set(0);
            chunked.streamAsInt().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());
        }
        source.clear();
    }

    @Test
    public void testShortColumnIterators() {
        final ColumnSource<Short> source = input.getColumnSource("ShortCol", short.class);
        // @formatter:off
        try (final ChunkSource.GetContext sourceContext = source.makeGetContext(input.intSize());
             final ShortColumnIterator serial1 = new SerialShortColumnIterator(source, input.getRowSet());
             final ShortColumnIterator serial2 = new SerialShortColumnIterator(source, input.getRowSet());
             final ShortColumnIterator serial3 = new SerialShortColumnIterator(source, input.getRowSet());
             final ShortColumnIterator chunked1 = new ChunkedShortColumnIterator(source, input.getRowSet());
             final ShortColumnIterator chunked2 = new ChunkedShortColumnIterator(source, input.getRowSet());
             final ShortColumnIterator chunked3 = new ChunkedShortColumnIterator(source, input.getRowSet())) {
            // @formatter:on
            final ShortChunk<?> data = source.getChunk(sourceContext, input.getRowSet()).asShortChunk();
            final int dataSize = data.size();
            for (int vi = 0; vi < dataSize; ++vi) {
                final short value = data.get(vi);
                assertEquals(serial1.remaining(), dataSize - vi);
                assertEquals(chunked1.remaining(), dataSize - vi);
                assertTrue(serial1.hasNext());
                assertTrue(chunked1.hasNext());
                assertEquals(value, serial1.nextShort());
                assertEquals(value, chunked1.nextShort());
                assertEquals(box(value), serial2.next());
                assertEquals(box(value), chunked2.next());
            }
            final MutableInt nextValueIndex = new MutableInt(0);
            assertEquals(0, serial3.stream()
                    .filter((final Short value) -> !(Objects.equals(value,
                            box(data.get(nextValueIndex.getAndIncrement())))))
                    .count());
            nextValueIndex.set(0);
            assertEquals(0, chunked3.stream()
                    .filter((final Short value) -> !(Objects.equals(value,
                            box(data.get(nextValueIndex.getAndIncrement())))))
                    .count());
        }
    }

    private static ChunkColumnSource<Short> createShortSource() {
        final WritableShortChunk<Values> data = WritableShortChunk.makeWritableChunk(10_000);
        for (int ei = 0; ei < data.size(); ++ei) {
            data.set(ei, ei % 4 == 0 ? NULL_SHORT : (short) ei);
        }

        // noinspection unchecked
        final ChunkColumnSource<Short> source =
                (ChunkColumnSource<Short>) ChunkColumnSource.make(ChunkType.Short, short.class);
        source.addChunk(data);
        return source;
    }

    @Test
    public void testShortColumnIteratorForBoxedNulls() {
        final ChunkColumnSource<Short> source = createShortSource();
        try (final RowSet rowset = RowSetFactory.flat(10_000);
                final ShortColumnIterator serial = new SerialShortColumnIterator(source, rowset);
                final ShortColumnIterator chunked = new ChunkedShortColumnIterator(source, rowset)) {

            final MutableInt numNulls = new MutableInt(0);
            final Consumer<Short> nullValidator = c -> {
                if (c == null) {
                    numNulls.add(1);
                } else if (c == NULL_SHORT) {
                    throw new IllegalStateException("Expected null, but got boxed NULL_SHORT");
                }
            };
            serial.stream().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());

            numNulls.set(0);
            chunked.stream().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());
        }
        source.clear();
    }

    @Test
    public void testShortColumnIteratorForStreamAsIntNulls() {
        final ChunkColumnSource<Short> source = createShortSource();

        try (final RowSet rowset = RowSetFactory.flat(10_000);
                final ShortColumnIterator serial = new SerialShortColumnIterator(source, rowset);
                final ShortColumnIterator chunked = new ChunkedShortColumnIterator(source, rowset)) {
            final MutableInt numNulls = new MutableInt(0);
            final IntConsumer nullValidator = c -> {
                if (c == NULL_INT) {
                    numNulls.add(1);
                } else if (c == NULL_SHORT) {
                    throw new IllegalStateException("Expected NULL_INT, but got NULL_SHORT");
                }
            };
            serial.streamAsInt().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());

            numNulls.set(0);
            chunked.streamAsInt().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());
        }
        source.clear();
    }

    @Test
    public void testIntegerColumnIterators() {
        final ColumnSource<Integer> source = input.getColumnSource("IntCol", int.class);
        // @formatter:off
        try (final ChunkSource.GetContext sourceContext = source.makeGetContext(input.intSize());
             final IntegerColumnIterator serial1 = new SerialIntegerColumnIterator(source, input.getRowSet());
             final IntegerColumnIterator serial2 = new SerialIntegerColumnIterator(source, input.getRowSet());
             final IntegerColumnIterator serial3 = new SerialIntegerColumnIterator(source, input.getRowSet());
             final IntegerColumnIterator chunked1 = new ChunkedIntegerColumnIterator(source, input.getRowSet());
             final IntegerColumnIterator chunked2 = new ChunkedIntegerColumnIterator(source, input.getRowSet());
             final IntegerColumnIterator chunked3 = new ChunkedIntegerColumnIterator(source, input.getRowSet())) {
            // @formatter:on
            final IntChunk<?> data = source.getChunk(sourceContext, input.getRowSet()).asIntChunk();
            final int dataSize = data.size();
            for (int vi = 0; vi < dataSize; ++vi) {
                final int value = data.get(vi);
                assertEquals(serial1.remaining(), dataSize - vi);
                assertEquals(chunked1.remaining(), dataSize - vi);
                assertTrue(serial1.hasNext());
                assertTrue(chunked1.hasNext());
                assertEquals(value, serial1.nextInt());
                assertEquals(value, chunked1.nextInt());
                assertEquals(box(value), serial2.next());
                assertEquals(box(value), chunked2.next());
            }
            final MutableInt nextValueIndex = new MutableInt(0);
            assertEquals(0, serial3.stream()
                    .filter((final Integer value) -> !(Objects.equals(value,
                            box(data.get(nextValueIndex.getAndIncrement())))))
                    .count());
            nextValueIndex.set(0);
            assertEquals(0, chunked3.stream()
                    .filter((final Integer value) -> !(Objects.equals(value,
                            box(data.get(nextValueIndex.getAndIncrement())))))
                    .count());
        }
    }

    private static ChunkColumnSource<Integer> createIntegerSource() {
        final WritableIntChunk<Values> data = WritableIntChunk.makeWritableChunk(10_000);
        for (int ei = 0; ei < data.size(); ++ei) {
            data.set(ei, ei % 4 == 0 ? NULL_INT : ei);
        }

        // noinspection unchecked
        final ChunkColumnSource<Integer> source =
                (ChunkColumnSource<Integer>) ChunkColumnSource.make(ChunkType.Int, int.class);
        source.addChunk(data);
        return source;
    }

    @Test
    public void testIntegerColumnIteratorForBoxedNulls() {
        final ChunkColumnSource<Integer> source = createIntegerSource();
        try (final RowSet rowset = RowSetFactory.flat(10_000);
                final IntegerColumnIterator serial = new SerialIntegerColumnIterator(source, rowset);
                final IntegerColumnIterator chunked = new ChunkedIntegerColumnIterator(source, rowset)) {

            final MutableInt numNulls = new MutableInt(0);
            final Consumer<Integer> nullValidator = c -> {
                if (c == null) {
                    numNulls.add(1);
                } else if (c == NULL_INT) {
                    throw new IllegalStateException("Expected null, but got boxed NULL_INT");
                }
            };
            serial.stream().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());

            numNulls.set(0);
            chunked.stream().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());
        }
        source.clear();
    }

    @Test
    public void testLongColumnIterators() {
        final ColumnSource<Long> source = input.getColumnSource("LongCol", long.class);
        // @formatter:off
        try (final ChunkSource.GetContext sourceContext = source.makeGetContext(input.intSize());
             final LongColumnIterator serial1 = new SerialLongColumnIterator(source, input.getRowSet());
             final LongColumnIterator serial2 = new SerialLongColumnIterator(source, input.getRowSet());
             final LongColumnIterator serial3 = new SerialLongColumnIterator(source, input.getRowSet());
             final LongColumnIterator chunked1 = new ChunkedLongColumnIterator(source, input.getRowSet());
             final LongColumnIterator chunked2 = new ChunkedLongColumnIterator(source, input.getRowSet());
             final LongColumnIterator chunked3 = new ChunkedLongColumnIterator(source, input.getRowSet())) {
            // @formatter:on
            final LongChunk<?> data = source.getChunk(sourceContext, input.getRowSet()).asLongChunk();
            final int dataSize = data.size();
            for (int vi = 0; vi < dataSize; ++vi) {
                final long value = data.get(vi);
                assertEquals(serial1.remaining(), dataSize - vi);
                assertEquals(chunked1.remaining(), dataSize - vi);
                assertTrue(serial1.hasNext());
                assertTrue(chunked1.hasNext());
                assertEquals(value, serial1.nextLong());
                assertEquals(value, chunked1.nextLong());
                assertEquals(box(value), serial2.next());
                assertEquals(box(value), chunked2.next());
            }
            final MutableInt nextValueIndex = new MutableInt(0);
            assertEquals(0, serial3.stream()
                    .filter((final Long value) -> !(Objects.equals(value,
                            box(data.get(nextValueIndex.getAndIncrement())))))
                    .count());
            nextValueIndex.set(0);
            assertEquals(0, chunked3.stream()
                    .filter((final Long value) -> !(Objects.equals(value,
                            box(data.get(nextValueIndex.getAndIncrement())))))
                    .count());
        }
    }

    private static ChunkColumnSource<Long> createLongSource() {
        final WritableLongChunk<Values> data = WritableLongChunk.makeWritableChunk(10_000);
        for (int ei = 0; ei < data.size(); ++ei) {
            data.set(ei, ei % 4 == 0 ? NULL_LONG : (long) ei);
        }

        // noinspection unchecked
        final ChunkColumnSource<Long> source =
                (ChunkColumnSource<Long>) ChunkColumnSource.make(ChunkType.Long, long.class);
        source.addChunk(data);
        return source;
    }

    @Test
    public void testLongColumnIteratorForBoxedNulls() {
        final ChunkColumnSource<Long> source = createLongSource();
        try (final RowSet rowset = RowSetFactory.flat(10_000);
                final LongColumnIterator serial = new SerialLongColumnIterator(source, rowset);
                final LongColumnIterator chunked = new ChunkedLongColumnIterator(source, rowset)) {

            final MutableInt numNulls = new MutableInt(0);
            final Consumer<Long> nullValidator = c -> {
                if (c == null) {
                    numNulls.add(1);
                } else if (c == NULL_LONG) {
                    throw new IllegalStateException("Expected null, but got boxed NULL_LONG");
                }
            };
            serial.stream().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());

            numNulls.set(0);
            chunked.stream().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());
        }
        source.clear();
    }

    @Test
    public void testFloatColumnIterators() {
        final ColumnSource<Float> source = input.getColumnSource("FloatCol", float.class);
        // @formatter:off
        try (final ChunkSource.GetContext sourceContext = source.makeGetContext(input.intSize());
             final FloatColumnIterator serial1 = new SerialFloatColumnIterator(source, input.getRowSet());
             final FloatColumnIterator serial2 = new SerialFloatColumnIterator(source, input.getRowSet());
             final FloatColumnIterator serial3 = new SerialFloatColumnIterator(source, input.getRowSet());
             final FloatColumnIterator chunked1 = new ChunkedFloatColumnIterator(source, input.getRowSet());
             final FloatColumnIterator chunked2 = new ChunkedFloatColumnIterator(source, input.getRowSet());
             final FloatColumnIterator chunked3 = new ChunkedFloatColumnIterator(source, input.getRowSet())) {
            // @formatter:on
            final FloatChunk<?> data = source.getChunk(sourceContext, input.getRowSet()).asFloatChunk();
            final int dataSize = data.size();
            for (int vi = 0; vi < dataSize; ++vi) {
                final float value = data.get(vi);
                assertEquals(serial1.remaining(), dataSize - vi);
                assertEquals(chunked1.remaining(), dataSize - vi);
                assertTrue(serial1.hasNext());
                assertTrue(chunked1.hasNext());
                assertEquals(value, serial1.nextFloat());
                assertEquals(value, chunked1.nextFloat());
                assertEquals(box(value), serial2.next());
                assertEquals(box(value), chunked2.next());
            }
            final MutableInt nextValueIndex = new MutableInt(0);
            assertEquals(0, serial3.stream()
                    .filter((final Float value) -> !(Objects.equals(value,
                            box(data.get(nextValueIndex.getAndIncrement())))))
                    .count());
            nextValueIndex.set(0);
            assertEquals(0, chunked3.stream()
                    .filter((final Float value) -> !(Objects.equals(value,
                            box(data.get(nextValueIndex.getAndIncrement())))))
                    .count());
        }
    }

    private static ChunkColumnSource<Float> createFloatSource() {
        final WritableFloatChunk<Values> data = WritableFloatChunk.makeWritableChunk(10_000);
        for (int ei = 0; ei < data.size(); ++ei) {
            data.set(ei, ei % 4 == 0 ? NULL_FLOAT : (float) ei);
        }

        // noinspection unchecked
        final ChunkColumnSource<Float> source =
                (ChunkColumnSource<Float>) ChunkColumnSource.make(ChunkType.Float, float.class);
        source.addChunk(data);
        return source;
    }

    @Test
    public void testFloatColumnIteratorForBoxedNulls() {
        final ChunkColumnSource<Float> source = createFloatSource();
        try (final RowSet rowset = RowSetFactory.flat(10_000);
                final FloatColumnIterator serial = new SerialFloatColumnIterator(source, rowset);
                final FloatColumnIterator chunked = new ChunkedFloatColumnIterator(source, rowset)) {

            final MutableInt numNulls = new MutableInt(0);
            final Consumer<Float> nullValidator = c -> {
                if (c == null) {
                    numNulls.add(1);
                } else if (c == NULL_FLOAT) {
                    throw new IllegalStateException("Expected null, but got boxed NULL_FLOAT");
                }
            };
            serial.stream().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());

            numNulls.set(0);
            chunked.stream().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());
        }
        source.clear();
    }

    @Test
    public void testFloatColumnIteratorForStreamAsIntNulls() {
        final ChunkColumnSource<Float> source = createFloatSource();

        try (final RowSet rowset = RowSetFactory.flat(10_000);
                final FloatColumnIterator serial = new SerialFloatColumnIterator(source, rowset);
                final FloatColumnIterator chunked = new ChunkedFloatColumnIterator(source, rowset)) {
            final MutableInt numNulls = new MutableInt(0);
            final DoubleConsumer nullValidator = c -> {
                if (c == NULL_DOUBLE) {
                    numNulls.add(1);
                } else if (c == NULL_FLOAT) {
                    throw new IllegalStateException("Expected NULL_DOUBLE, but got NULL_FLOAT");
                }
            };
            serial.streamAsDouble().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());

            numNulls.set(0);
            chunked.streamAsDouble().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());
        }
        source.clear();
    }

    @Test
    public void testDoubleColumnIterators() {
        final ColumnSource<Double> source = input.getColumnSource("DoubleCol", double.class);
        // @formatter:off
        try (final ChunkSource.GetContext sourceContext = source.makeGetContext(input.intSize());
             final DoubleColumnIterator serial1 = new SerialDoubleColumnIterator(source, input.getRowSet());
             final DoubleColumnIterator serial2 = new SerialDoubleColumnIterator(source, input.getRowSet());
             final DoubleColumnIterator serial3 = new SerialDoubleColumnIterator(source, input.getRowSet());
             final DoubleColumnIterator chunked1 = new ChunkedDoubleColumnIterator(source, input.getRowSet());
             final DoubleColumnIterator chunked2 = new ChunkedDoubleColumnIterator(source, input.getRowSet());
             final DoubleColumnIterator chunked3 = new ChunkedDoubleColumnIterator(source, input.getRowSet())) {
            // @formatter:on
            final DoubleChunk<?> data = source.getChunk(sourceContext, input.getRowSet()).asDoubleChunk();
            final int dataSize = data.size();
            for (int vi = 0; vi < dataSize; ++vi) {
                final double value = data.get(vi);
                assertEquals(serial1.remaining(), dataSize - vi);
                assertEquals(chunked1.remaining(), dataSize - vi);
                assertTrue(serial1.hasNext());
                assertTrue(chunked1.hasNext());
                assertEquals(value, serial1.nextDouble());
                assertEquals(value, chunked1.nextDouble());
                assertEquals(box(value), serial2.next());
                assertEquals(box(value), chunked2.next());
            }
            final MutableInt nextValueIndex = new MutableInt(0);
            assertEquals(0, serial3.stream()
                    .filter((final Double value) -> !(Objects.equals(value,
                            box(data.get(nextValueIndex.getAndIncrement())))))
                    .count());
            nextValueIndex.set(0);
            assertEquals(0, chunked3.stream()
                    .filter((final Double value) -> !(Objects.equals(value,
                            box(data.get(nextValueIndex.getAndIncrement())))))
                    .count());
        }
    }

    private static ChunkColumnSource<Double> createDoubleSource() {
        final WritableDoubleChunk<Values> data = WritableDoubleChunk.makeWritableChunk(10_000);
        for (int ei = 0; ei < data.size(); ++ei) {
            data.set(ei, ei % 4 == 0 ? NULL_DOUBLE : (double) ei);
        }

        // noinspection unchecked
        final ChunkColumnSource<Double> source =
                (ChunkColumnSource<Double>) ChunkColumnSource.make(ChunkType.Double, double.class);
        source.addChunk(data);
        return source;
    }

    @Test
    public void testDoubleColumnIteratorForBoxedNulls() {
        final ChunkColumnSource<Double> source = createDoubleSource();
        try (final RowSet rowset = RowSetFactory.flat(10_000);
                final DoubleColumnIterator serial = new SerialDoubleColumnIterator(source, rowset);
                final DoubleColumnIterator chunked = new ChunkedDoubleColumnIterator(source, rowset)) {

            final MutableInt numNulls = new MutableInt(0);
            final Consumer<Double> nullValidator = c -> {
                if (c == null) {
                    numNulls.add(1);
                } else if (c == NULL_DOUBLE) {
                    throw new IllegalStateException("Expected null, but got boxed NULL_DOUBLE");
                }
            };
            serial.stream().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());

            numNulls.set(0);
            chunked.stream().forEach(nullValidator);
            assertEquals("(rowset.size() + 3) / 4 == nulNulls.get()", (rowset.size() + 3) / 4, numNulls.get());
        }
        source.clear();
    }

    @Test
    public void testObjectColumnIteratorsOfBoolean() {
        final ColumnSource<Boolean> source = input.getColumnSource("BooleanCol", Boolean.class);
        // @formatter:off
        try (final ChunkSource.GetContext sourceContext = source.makeGetContext(input.intSize());
             final ObjectColumnIterator<Boolean> serial1 = new SerialObjectColumnIterator<>(source, input.getRowSet());
             final ObjectColumnIterator<Boolean> serial2 = new SerialObjectColumnIterator<>(source, input.getRowSet());
             final ObjectColumnIterator<Boolean> chunked1 = new ChunkedObjectColumnIterator<>(source, input.getRowSet());
             final ObjectColumnIterator<Boolean> chunked2 = new ChunkedObjectColumnIterator<>(source, input.getRowSet())) {
            // @formatter:on
            final ObjectChunk<Boolean, ?> data = source.getChunk(sourceContext, input.getRowSet()).asObjectChunk();
            final int dataSize = data.size();
            for (int vi = 0; vi < dataSize; ++vi) {
                final Boolean value = data.get(vi);
                assertEquals(serial1.remaining(), dataSize - vi);
                assertEquals(chunked1.remaining(), dataSize - vi);
                assertTrue(serial1.hasNext());
                assertTrue(chunked1.hasNext());
                assertEquals(value, serial1.next());
                assertEquals(value, chunked1.next());
            }
            final MutableInt nextValueIndex = new MutableInt(0);
            assertEquals(0, serial2.stream()
                    .filter((final Boolean value) -> !(Objects.equals(value,
                            data.get(nextValueIndex.getAndIncrement()))))
                    .count());
            nextValueIndex.set(0);
            assertEquals(0, chunked2.stream()
                    .filter((final Boolean value) -> !(Objects.equals(value,
                            data.get(nextValueIndex.getAndIncrement()))))
                    .count());
        }
    }

    @Test
    public void testObjectColumnIteratorsOfString() {
        final ColumnSource<String> source = input.getColumnSource("StringCol", String.class);
        // @formatter:off
        try (final ChunkSource.GetContext sourceContext = source.makeGetContext(input.intSize());
             final ObjectColumnIterator<String> serial1 = new SerialObjectColumnIterator<>(source, input.getRowSet());
             final ObjectColumnIterator<String> serial2 = new SerialObjectColumnIterator<>(source, input.getRowSet());
             final ObjectColumnIterator<String> chunked1 = new ChunkedObjectColumnIterator<>(source, input.getRowSet());
             final ObjectColumnIterator<String> chunked2 = new ChunkedObjectColumnIterator<>(source, input.getRowSet())) {
            // @formatter:on
            final ObjectChunk<String, ?> data = source.getChunk(sourceContext, input.getRowSet()).asObjectChunk();
            final int dataSize = data.size();
            for (int vi = 0; vi < dataSize; ++vi) {
                final String value = data.get(vi);
                assertEquals(serial1.remaining(), dataSize - vi);
                assertEquals(chunked1.remaining(), dataSize - vi);
                assertTrue(serial1.hasNext());
                assertTrue(chunked1.hasNext());
                assertEquals(value, serial1.next());
                assertEquals(value, chunked1.next());
            }
            final MutableInt nextValueIndex = new MutableInt(0);
            assertEquals(0, serial2.stream()
                    .filter((final String value) -> !(Objects.equals(value,
                            data.get(nextValueIndex.getAndIncrement()))))
                    .count());
            nextValueIndex.set(0);
            assertEquals(0, chunked2.stream()
                    .filter((final String value) -> !(Objects.equals(value,
                            data.get(nextValueIndex.getAndIncrement()))))
                    .count());
        }
    }
}
