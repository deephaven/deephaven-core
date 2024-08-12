//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.assertj;

import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSet.Iterator;
import io.deephaven.engine.table.ChunkSource.FillContext;
import io.deephaven.engine.table.ColumnSource;
import org.assertj.core.api.ByteArrayAssert;
import org.assertj.core.api.CharArrayAssert;
import org.assertj.core.api.DoubleArrayAssert;
import org.assertj.core.api.FloatArrayAssert;
import org.assertj.core.api.IntArrayAssert;
import org.assertj.core.api.IteratorAssert;
import org.assertj.core.api.LongArrayAssert;
import org.assertj.core.api.ShortArrayAssert;

import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

public class ColumnSourceValuesAssert<T> {

    private final ColumnSource<T> actual;
    private final RowSet rowSet;

    public ColumnSourceValuesAssert(ColumnSource<T> columnSource, RowSet rowSet) {
        this.actual = Objects.requireNonNull(columnSource);
        this.rowSet = Objects.requireNonNull(rowSet);
    }

    public CharArrayAssert chars() {
        return chars(false);
    }

    public ByteArrayAssert bytes() {
        return bytes(false);
    }

    public ShortArrayAssert shorts() {
        return shorts(false);
    }

    public IntArrayAssert ints() {
        return ints(false);
    }

    public LongArrayAssert longs() {
        return longs(false);
    }

    public FloatArrayAssert floats() {
        return floats(false);
    }

    public DoubleArrayAssert doubles() {
        return doubles(false);
    }

    public CharArrayAssert chars(boolean prev) {
        return assertThatChars(actual.cast(char.class), rowSet, prev);
    }

    public ByteArrayAssert bytes(boolean prev) {
        return assertThatBytes(actual.cast(byte.class), rowSet, prev);
    }

    public ShortArrayAssert shorts(boolean prev) {
        return assertThatShorts(actual.cast(short.class), rowSet, prev);
    }

    public IntArrayAssert ints(boolean prev) {
        return assertThatInts(actual.cast(int.class), rowSet, prev);
    }

    public LongArrayAssert longs(boolean prev) {
        return assertThatLongs(actual.cast(long.class), rowSet, prev);
    }

    public FloatArrayAssert floats(boolean prev) {
        return assertThatFloats(actual.cast(float.class), rowSet, prev);
    }

    public DoubleArrayAssert doubles(boolean prev) {
        return assertThatDoubles(actual.cast(double.class), rowSet, prev);
    }

    public static CharArrayAssert assertThatChars(ColumnSource<Character> source, RowSet rowSet, boolean prev) {
        final char[] out = new char[rowSet.intSize()];
        try (final Iterator it = rowSet.iterator()) {
            final IteratorAssert<Long> itAssert = assertThat(it);
            for (int i = 0; i < out.length; ++i) {
                itAssert.hasNext();
                out[i] = prev ? source.getPrevChar(it.nextLong()) : source.getChar(it.nextLong());
            }
            itAssert.isExhausted();
        }
        return new CharArrayAssert(out);
    }

    public static ByteArrayAssert assertThatBytes(ColumnSource<Byte> source, RowSet rowSet, boolean prev) {
        final byte[] out = new byte[rowSet.intSize()];
        try (
                final FillContext fillContext = source.makeFillContext(out.length);
                final WritableByteChunk<Values> chunk = WritableByteChunk.writableChunkWrap(out)) {
            if (prev) {
                source.fillChunk(fillContext, chunk, rowSet);
            } else {
                source.fillPrevChunk(fillContext, chunk, rowSet);
            }
        }
        return new ByteArrayAssert(out);
    }

    public static ShortArrayAssert assertThatShorts(ColumnSource<Short> source, RowSet rowSet, boolean prev) {
        final short[] out = new short[rowSet.intSize()];
        try (
                final FillContext fillContext = source.makeFillContext(out.length);
                final WritableShortChunk<Values> chunk = WritableShortChunk.writableChunkWrap(out)) {
            if (prev) {
                source.fillChunk(fillContext, chunk, rowSet);
            } else {
                source.fillPrevChunk(fillContext, chunk, rowSet);
            }
        }
        return new ShortArrayAssert(out);
    }

    public static IntArrayAssert assertThatInts(ColumnSource<Integer> source, RowSet rowSet, boolean prev) {
        final int[] out = new int[rowSet.intSize()];
        try (
                final FillContext fillContext = source.makeFillContext(out.length);
                final WritableIntChunk<Values> chunk = WritableIntChunk.writableChunkWrap(out)) {
            if (prev) {
                source.fillChunk(fillContext, chunk, rowSet);
            } else {
                source.fillPrevChunk(fillContext, chunk, rowSet);
            }
        }
        return new IntArrayAssert(out);
    }

    public static LongArrayAssert assertThatLongs(ColumnSource<Long> source, RowSet rowSet, boolean prev) {
        final long[] out = new long[rowSet.intSize()];
        try (
                final FillContext fillContext = source.makeFillContext(out.length);
                final WritableLongChunk<Values> chunk = WritableLongChunk.writableChunkWrap(out)) {
            if (prev) {
                source.fillChunk(fillContext, chunk, rowSet);
            } else {
                source.fillPrevChunk(fillContext, chunk, rowSet);
            }
        }
        return new LongArrayAssert(out);
    }

    public static FloatArrayAssert assertThatFloats(ColumnSource<Float> source, RowSet rowSet, boolean prev) {
        final float[] out = new float[rowSet.intSize()];
        try (
                final FillContext fillContext = source.makeFillContext(out.length);
                final WritableFloatChunk<Values> chunk = WritableFloatChunk.writableChunkWrap(out)) {
            if (prev) {
                source.fillChunk(fillContext, chunk, rowSet);
            } else {
                source.fillPrevChunk(fillContext, chunk, rowSet);
            }
        }
        return new FloatArrayAssert(out);
    }

    public static DoubleArrayAssert assertThatDoubles(ColumnSource<Double> source, RowSet rowSet, boolean prev) {
        final double[] out = new double[rowSet.intSize()];
        try (
                final FillContext fillContext = source.makeFillContext(out.length);
                final WritableDoubleChunk<Values> chunk = WritableDoubleChunk.writableChunkWrap(out)) {
            if (prev) {
                source.fillChunk(fillContext, chunk, rowSet);
            } else {
                source.fillPrevChunk(fillContext, chunk, rowSet);
            }
        }
        return new DoubleArrayAssert(out);
    }
}
