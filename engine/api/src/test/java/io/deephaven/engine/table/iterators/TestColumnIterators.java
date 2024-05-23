//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.iterators;

import io.deephaven.chunk.*;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.util.mutable.MutableInt;
import org.junit.*;

import java.util.Objects;
import java.util.Random;

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
            nextValueIndex.setValue(0);
            assertEquals(0, chunked3.stream()
                    .filter((final Character value) -> !(Objects.equals(value,
                            box(data.get(nextValueIndex.getAndIncrement())))))
                    .count());
        }
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
            nextValueIndex.setValue(0);
            assertEquals(0, chunked3.stream()
                    .filter((final Byte value) -> !(Objects.equals(value,
                            box(data.get(nextValueIndex.getAndIncrement())))))
                    .count());
        }
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
            nextValueIndex.setValue(0);
            assertEquals(0, chunked3.stream()
                    .filter((final Short value) -> !(Objects.equals(value,
                            box(data.get(nextValueIndex.getAndIncrement())))))
                    .count());
        }
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
            nextValueIndex.setValue(0);
            assertEquals(0, chunked3.stream()
                    .filter((final Integer value) -> !(Objects.equals(value,
                            box(data.get(nextValueIndex.getAndIncrement())))))
                    .count());
        }
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
            nextValueIndex.setValue(0);
            assertEquals(0, chunked3.stream()
                    .filter((final Long value) -> !(Objects.equals(value,
                            box(data.get(nextValueIndex.getAndIncrement())))))
                    .count());
        }
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
            nextValueIndex.setValue(0);
            assertEquals(0, chunked3.stream()
                    .filter((final Float value) -> !(Objects.equals(value,
                            box(data.get(nextValueIndex.getAndIncrement())))))
                    .count());
        }
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
            nextValueIndex.setValue(0);
            assertEquals(0, chunked3.stream()
                    .filter((final Double value) -> !(Objects.equals(value,
                            box(data.get(nextValueIndex.getAndIncrement())))))
                    .count());
        }
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
            nextValueIndex.setValue(0);
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
            nextValueIndex.setValue(0);
            assertEquals(0, chunked2.stream()
                    .filter((final String value) -> !(Objects.equals(value,
                            data.get(nextValueIndex.getAndIncrement()))))
                    .count());
        }
    }
}
