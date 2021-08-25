/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.Procedure;
import io.deephaven.db.tables.DataColumn;
import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.iterators.*;
import io.deephaven.db.v2.sources.*;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;
import java.util.stream.StreamSupport;

/**
 * DataColumn implementation backed by a ColumnSource and an Index.
 */
@SuppressWarnings("WeakerAccess")
public class IndexedDataColumn<TYPE> implements DataColumn<TYPE> {

    private final String name;
    @SuppressWarnings({"UnusedDeclaration", "FieldCanBeLocal"})
    private final Object parent; // DO NOT DELETE - This reference preserves strong-reachability of
                                 // the owning table and its listeners.
    private final Index index;
    private final ColumnSource<TYPE> columnSource;

    public IndexedDataColumn(@NotNull final String name, @NotNull final Table table) {
        // noinspection unchecked
        this(name, table, table.getIndex(), table.getColumnSource(name));
    }

    public IndexedDataColumn(@NotNull final String name, @NotNull final Index index,
        @NotNull final ColumnSource<TYPE> columnSource) {
        this(name, null, index, columnSource);
    }

    private IndexedDataColumn(@Nullable final String name, @Nullable final Object parent,
        @NotNull final Index index, @NotNull final ColumnSource<TYPE> columnSource) {
        this.name = name;
        this.parent = parent;
        this.index = index;
        this.columnSource = columnSource;
    }

    /**
     * This is intended as a unit test helper. It is not recommended for inexpert use.
     * 
     * @param index The index
     * @param columnSource The column source
     * @return A data column with previous values for the supplied column source, according to the
     *         previous version of the index
     */
    public static <TYPE> IndexedDataColumn<TYPE> makePreviousColumn(@NotNull final Index index,
        @NotNull final ColumnSource<TYPE> columnSource) {
        return new IndexedDataColumn<>(null, null, index.getPrevIndex(),
            new PrevColumnSource<>(columnSource));
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Class<TYPE> getType() {
        return columnSource.getType();
    }

    @Override
    public Class getComponentType() {
        return columnSource.getComponentType();
    }

    @Override
    public long size() {
        return index.size();
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------------------------------

    private Index getSubIndexByPos(final long startPosInclusive, final long endPosExclusive) {
        return startPosInclusive == 0 && endPosExclusive == index.size() ? index.clone()
            : index.subindexByPos(startPosInclusive, endPosExclusive);
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Get method implementations
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public TYPE get(final long pos) {
        long index = this.index.get(pos);
        if (index == -1) {
            return null;
        }
        return columnSource.get(index);
    }

    @Override
    public TYPE[] get(final long startPosInclusive, final long endPosExclusive) {
        final Iterable<TYPE> iterable =
            () -> new ColumnIterator<>(getSubIndexByPos(startPosInclusive, endPosExclusive),
                columnSource);
        // noinspection unchecked
        return StreamSupport.stream(iterable.spliterator(), false).toArray(s -> (TYPE[]) Array
            .newInstance(io.deephaven.util.type.TypeUtils.getBoxedType(columnSource.getType()), s));
    }

    @Override
    public TYPE[] get(final long... positions) {
        // noinspection unchecked
        return Arrays.stream(positions).map(index::get).mapToObj(columnSource::get)
            .toArray(s -> (TYPE[]) Array.newInstance(
                io.deephaven.util.type.TypeUtils.getBoxedType(columnSource.getType()), s));
    }

    @Override
    public TYPE[] get(final int... positions) {
        // noinspection unchecked
        return Arrays.stream(positions).mapToLong(i -> i).map(index::get)
            .mapToObj(columnSource::get).toArray(
                s -> (TYPE[]) Array.newInstance(TypeUtils.getBoxedType(columnSource.getType()), s));
    }

    @Override
    public Boolean getBoolean(final long pos) {
        long index = this.index.get(pos);
        if (index == -1) {
            return null;
        }
        return columnSource.getBoolean(index);
    }

    @Override
    public Boolean[] getBooleans(final long startPosInclusive, final long endPosExclusive) {
        return (Boolean[]) get(startPosInclusive, endPosExclusive);
    }

    @Override
    public Boolean[] getBooleans(final long... positions) {
        return (Boolean[]) get(positions);
    }

    public Boolean[] getBooleans(final int... positions) {
        return (Boolean[]) get(positions);
    }

    @Override
    public byte getByte(final long pos) {
        long index = this.index.get(pos);
        if (index == -1) {
            return QueryConstants.NULL_BYTE;
        }
        return columnSource.getByte(index);
    }

    @Override
    public byte[] getBytes(final long startPosInclusive, final long endPosExclusive) {
        try (final Index rangeIndex = getSubIndexByPos(startPosInclusive, endPosExclusive);
            final ChunkSource.FillContext context =
                columnSource.makeFillContext(rangeIndex.intSize("getBytes"), null)) {
            final byte[] result = new byte[rangeIndex.intSize("getBytes")];
            columnSource.fillChunk(context, WritableByteChunk.writableChunkWrap(result),
                rangeIndex);
            return result;
        }
    }

    @Override
    public byte[] getBytes(final long... positions) {
        final byte[] result = new byte[positions.length];
        for (int pi = 0; pi < positions.length; ++pi) {
            result[pi] = getByte(positions[pi]);
        }
        return result;
    }

    @Override
    public byte[] getBytes(final int... positions) {
        final byte[] result = new byte[positions.length];
        for (int pi = 0; pi < positions.length; ++pi) {
            result[pi] = getByte(positions[pi]);
        }
        return result;
    }

    @Override
    public char getChar(final long pos) {
        long index = this.index.get(pos);
        if (index == -1) {
            return QueryConstants.NULL_CHAR;
        }
        return columnSource.getChar(index);
    }

    @Override
    public char[] getChars(final long startPosInclusive, final long endPosExclusive) {
        try (final Index rangeIndex = getSubIndexByPos(startPosInclusive, endPosExclusive);
            final ChunkSource.FillContext context =
                columnSource.makeFillContext(rangeIndex.intSize("getChars"), null)) {
            final char[] result = new char[rangeIndex.intSize("getChars")];
            columnSource.fillChunk(context, WritableCharChunk.writableChunkWrap(result),
                rangeIndex);
            return result;
        }
    }

    @Override
    public char[] getChars(final long... positions) {
        final char[] result = new char[positions.length];
        for (int pi = 0; pi < positions.length; ++pi) {
            result[pi] = getChar(positions[pi]);
        }
        return result;
    }

    @Override
    public char[] getChars(final int... positions) {
        final char[] result = new char[positions.length];
        for (int pi = 0; pi < positions.length; ++pi) {
            result[pi] = getChar(positions[pi]);
        }
        return result;
    }

    @Override
    public double getDouble(final long pos) {
        long index = this.index.get(pos);
        if (index == -1) {
            return QueryConstants.NULL_DOUBLE;
        }
        return columnSource.getDouble(index);
    }

    @Override
    public double[] getDoubles(final long startPosInclusive, final long endPosExclusive) {
        try (final Index rangeIndex = getSubIndexByPos(startPosInclusive, endPosExclusive);
            final ChunkSource.FillContext context =
                columnSource.makeFillContext(rangeIndex.intSize("getDoubles"), null)) {
            final double[] result = new double[rangeIndex.intSize("getDoubles")];
            columnSource.fillChunk(context, WritableDoubleChunk.writableChunkWrap(result),
                rangeIndex);
            return result;
        }
    }

    @Override
    public double[] getDoubles(final long... positions) {
        final double[] result = new double[positions.length];
        for (int pi = 0; pi < positions.length; ++pi) {
            result[pi] = getDouble(positions[pi]);
        }
        return result;
    }

    @Override
    public double[] getDoubles(final int... positions) {
        final double[] result = new double[positions.length];
        for (int pi = 0; pi < positions.length; ++pi) {
            result[pi] = getDouble(positions[pi]);
        }
        return result;
    }

    @Override
    public float getFloat(final long pos) {
        long index = this.index.get(pos);
        if (index == -1) {
            return QueryConstants.NULL_FLOAT;
        }
        return columnSource.getFloat(index);
    }

    @Override
    public float[] getFloats(final long startPosInclusive, final long endPosExclusive) {
        try (final Index rangeIndex = getSubIndexByPos(startPosInclusive, endPosExclusive);
            final ChunkSource.FillContext context =
                columnSource.makeFillContext(rangeIndex.intSize("getFloats"), null)) {
            final float[] result = new float[rangeIndex.intSize("getFloats")];
            columnSource.fillChunk(context, WritableFloatChunk.writableChunkWrap(result),
                rangeIndex);
            return result;
        }
    }

    @Override
    public float[] getFloats(final long... positions) {
        final float[] result = new float[positions.length];
        for (int pi = 0; pi < positions.length; ++pi) {
            result[pi] = getFloat(positions[pi]);
        }
        return result;
    }

    @Override
    public float[] getFloats(final int... positions) {
        final float[] result = new float[positions.length];
        for (int pi = 0; pi < positions.length; ++pi) {
            result[pi] = getFloat(positions[pi]);
        }
        return result;
    }

    @Override
    public int getInt(final long pos) {
        long index = this.index.get(pos);
        if (index == -1) {
            return QueryConstants.NULL_INT;
        }
        return columnSource.getInt(index);
    }

    @Override
    public int[] getInts(final long startPosInclusive, final long endPosExclusive) {
        try (final Index rangeIndex = getSubIndexByPos(startPosInclusive, endPosExclusive);
            final ChunkSource.FillContext context =
                columnSource.makeFillContext(rangeIndex.intSize("getInts"), null)) {
            final int[] result = new int[rangeIndex.intSize("getInts")];
            columnSource.fillChunk(context, WritableIntChunk.writableChunkWrap(result), rangeIndex);
            return result;
        }
    }

    @Override
    public int[] getInts(final long... positions) {
        final int[] result = new int[positions.length];
        for (int pi = 0; pi < positions.length; ++pi) {
            result[pi] = getInt(positions[pi]);
        }
        return result;
    }

    @Override
    public int[] getInts(final int... positions) {
        final int[] result = new int[positions.length];
        for (int pi = 0; pi < positions.length; ++pi) {
            result[pi] = getInt(positions[pi]);
        }
        return result;
    }

    @Override
    public long getLong(final long pos) {
        long index = this.index.get(pos);
        if (index == -1) {
            return QueryConstants.NULL_LONG;
        }
        return columnSource.getLong(index);
    }

    @Override
    public long[] getLongs(final long startPosInclusive, final long endPosExclusive) {
        try (final Index rangeIndex = getSubIndexByPos(startPosInclusive, endPosExclusive);
            final ChunkSource.FillContext context =
                columnSource.makeFillContext(rangeIndex.intSize("getLongs"), null)) {
            final long[] result = new long[rangeIndex.intSize("getLongs")];
            columnSource.fillChunk(context, WritableLongChunk.writableChunkWrap(result),
                rangeIndex);
            return result;
        }
    }

    @Override
    public long[] getLongs(final long... positions) {
        final long[] result = new long[positions.length];
        for (int pi = 0; pi < positions.length; ++pi) {
            result[pi] = getLong(positions[pi]);
        }
        return result;
    }

    @Override
    public long[] getLongs(final int... positions) {
        final long[] result = new long[positions.length];
        for (int pi = 0; pi < positions.length; ++pi) {
            result[pi] = getLong(positions[pi]);
        }
        return result;
    }

    @Override
    public short getShort(final long pos) {
        long index = this.index.get(pos);
        if (index == -1) {
            return QueryConstants.NULL_SHORT;
        }
        return columnSource.getShort(index);
    }

    @Override
    public short[] getShorts(final long startPosInclusive, final long endPosExclusive) {
        try (final Index rangeIndex = getSubIndexByPos(startPosInclusive, endPosExclusive);
            final ChunkSource.FillContext context =
                columnSource.makeFillContext(rangeIndex.intSize("getShorts"), null)) {
            final short[] result = new short[rangeIndex.intSize("getShorts")];
            columnSource.fillChunk(context, WritableShortChunk.writableChunkWrap(result),
                rangeIndex);
            return result;
        }
    }

    @Override
    public short[] getShorts(final long... positions) {
        final short[] result = new short[positions.length];
        for (int pi = 0; pi < positions.length; ++pi) {
            result[pi] = getShort(positions[pi]);
        }
        return result;
    }

    @Override
    public short[] getShorts(final int... positions) {
        final short[] result = new short[positions.length];
        for (int pi = 0; pi < positions.length; ++pi) {
            result[pi] = getShort(positions[pi]);
        }
        return result;
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Set method implementations - will cast columnSource to a WritableSource internally
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public void set(final long pos, final TYPE value) {
        ((WritableSource<TYPE>) columnSource).set(index.get(pos), value);
    }

    @Override
    @SafeVarargs
    public final void setArray(final long startPos, final TYPE... values) {
        getSubIndexByPos(startPos, startPos + values.length).forAllLongs(new LongConsumer() {
            int vi;

            @Override
            public void accept(final long key) {
                ((WritableSource<TYPE>) columnSource).set(key, values[vi++]);
            }
        });
    }

    @Override
    public void setBoolean(final long pos, final Boolean value) {
        ((WritableSource<Boolean>) columnSource).set(index.get(pos), value);
    }

    @Override
    public void setBooleans(final long startPos, final Boolean... values) {
        // noinspection unchecked
        setArray(startPos, (TYPE[]) values);
    }

    @Override
    public void setByte(final long pos, final byte value) {
        ((WritableSource<Byte>) columnSource).set(index.get(pos), value);
    }

    @Override
    public void setBytes(final long startPos, final byte... values) {
        getSubIndexByPos(startPos, startPos + values.length).forAllLongs(new LongConsumer() {
            int vi;

            @Override
            public void accept(final long key) {
                ((WritableSource<TYPE>) columnSource).set(key, values[vi++]);
            }
        });
    }

    @Override
    public void setChar(final long pos, final char value) {
        ((WritableSource<Character>) columnSource).set(index.get(pos), value);
    }

    @Override
    public void setChars(final long startPos, final char... values) {
        getSubIndexByPos(startPos, startPos + values.length).forAllLongs(new LongConsumer() {
            int vi;

            @Override
            public void accept(final long key) {
                ((WritableSource<TYPE>) columnSource).set(key, values[vi++]);
            }
        });
    }

    @Override
    public void setDouble(final long pos, final double value) {
        ((WritableSource<Double>) columnSource).set(index.get(pos), value);
    }

    @Override
    public void setDoubles(final long startPos, final double... values) {
        getSubIndexByPos(startPos, startPos + values.length).forAllLongs(new LongConsumer() {
            int vi;

            @Override
            public void accept(final long key) {
                ((WritableSource<TYPE>) columnSource).set(key, values[vi++]);
            }
        });
    }

    @Override
    public void setFloat(final long pos, final float value) {
        ((WritableSource<Float>) columnSource).set(index.get(pos), value);
    }

    @Override
    public void setFloats(final long startPos, final float... values) {
        getSubIndexByPos(startPos, startPos + values.length).forAllLongs(new LongConsumer() {
            int vi;

            @Override
            public void accept(final long key) {
                ((WritableSource<TYPE>) columnSource).set(key, values[vi++]);
            }
        });
    }

    @Override
    public void setInt(final long pos, final int value) {
        ((WritableSource<Integer>) columnSource).set(index.get(pos), value);
    }

    @Override
    public void setInts(final long startPos, final int... values) {
        getSubIndexByPos(startPos, startPos + values.length).forAllLongs(new LongConsumer() {
            int vi;

            @Override
            public void accept(final long key) {
                ((WritableSource<TYPE>) columnSource).set(key, values[vi++]);
            }
        });
    }

    @Override
    public void setLong(final long pos, final long value) {
        ((WritableSource<Long>) columnSource).set(index.get(pos), value);
    }

    @Override
    public void setLongs(final long startPos, final long... values) {
        getSubIndexByPos(startPos, startPos + values.length).forAllLongs(new LongConsumer() {
            int vi;

            @Override
            public void accept(final long key) {
                ((WritableSource<TYPE>) columnSource).set(key, values[vi++]);
            }
        });
    }

    @Override
    public void setShort(final long pos, final short value) {
        ((WritableSource<Short>) columnSource).set(index.get(pos), value);
    }

    @Override
    public void setShorts(final long startPos, final short... values) {
        getSubIndexByPos(startPos, startPos + values.length).forAllLongs(new LongConsumer() {
            int vi;

            @Override
            public void accept(final long key) {
                ((WritableSource<TYPE>) columnSource).set(key, values[vi++]);
            }
        });
    }
}
