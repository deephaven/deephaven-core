/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.*;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.iterators.ColumnIterator;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.function.LongConsumer;
import java.util.stream.StreamSupport;

/**
 * DataColumn implementation backed by a ColumnSource and a RowSet.
 */
@SuppressWarnings("WeakerAccess")
public class IndexedDataColumn<TYPE> implements DataColumn<TYPE> {

    private final String name;
    @SuppressWarnings({"UnusedDeclaration", "FieldCanBeLocal"})
    @ReferentialIntegrity
    private final Object parent; // DO NOT DELETE - This reference preserves strong-reachability of the owning table and
                                 // its listeners.
    private final RowSet rowSet;
    private final ColumnSource<TYPE> columnSource;

    public IndexedDataColumn(@NotNull final String name, @NotNull final Table table) {
        this(name, table, table.getRowSet(), table.getColumnSource(name));
    }

    public IndexedDataColumn(@NotNull final String name, @NotNull final RowSet rowSet,
            @NotNull final ColumnSource<TYPE> columnSource) {
        this(name, null, rowSet, columnSource);
    }

    private IndexedDataColumn(@Nullable final String name, @Nullable final Object parent, @NotNull final RowSet rowSet,
            @NotNull final ColumnSource<TYPE> columnSource) {
        this.name = name;
        this.parent = parent;
        this.rowSet = rowSet;
        this.columnSource = columnSource;
    }

    /**
     * This is intended as a unit test helper. It is not recommended for inexpert use.
     * 
     * @param rowSet The RowSet
     * @param columnSource The column source
     * @return A data column with previous values for the supplied column source, according to the previous version of
     *         the RowSet
     */
    public static <TYPE> IndexedDataColumn<TYPE> makePreviousColumn(@NotNull final TrackingRowSet rowSet,
            @NotNull final ColumnSource<TYPE> columnSource) {
        return new IndexedDataColumn<>(null, null, rowSet.copyPrev(), new PrevColumnSource<>(columnSource));
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
        return rowSet.size();
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------------------------------

    private RowSet getSubIndexByPos(final long startPosInclusive, final long endPosExclusive) {
        return startPosInclusive == 0 && endPosExclusive == rowSet.size() ? rowSet.copy()
                : rowSet.subSetByPositionRange(startPosInclusive, endPosExclusive);
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Get method implementations
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public TYPE get(final long pos) {
        long index = this.rowSet.get(pos);
        if (index == -1) {
            return null;
        }
        return columnSource.get(index);
    }

    @Override
    public TYPE[] get(final long startPosInclusive, final long endPosExclusive) {
        final Iterable<TYPE> iterable =
                () -> ColumnIterator.make(columnSource, getSubIndexByPos(startPosInclusive, endPosExclusive));
        // noinspection unchecked
        return StreamSupport.stream(iterable.spliterator(), false).toArray(s -> (TYPE[]) Array
                .newInstance(io.deephaven.util.type.TypeUtils.getBoxedType(columnSource.getType()), s));
    }

    @Override
    public TYPE[] get(final long... positions) {
        // noinspection unchecked
        return Arrays.stream(positions).map(rowSet::get).mapToObj(columnSource::get).toArray(s -> (TYPE[]) Array
                .newInstance(io.deephaven.util.type.TypeUtils.getBoxedType(columnSource.getType()), s));
    }

    @Override
    public TYPE[] get(final int... positions) {
        // noinspection unchecked
        return Arrays.stream(positions).mapToLong(i -> i).map(rowSet::get).mapToObj(columnSource::get)
                .toArray(s -> (TYPE[]) Array.newInstance(TypeUtils.getBoxedType(columnSource.getType()), s));
    }

    @Override
    public Boolean getBoolean(final long pos) {
        long index = this.rowSet.get(pos);
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
        long index = this.rowSet.get(pos);
        if (index == -1) {
            return QueryConstants.NULL_BYTE;
        }
        return columnSource.getByte(index);
    }

    @Override
    public byte[] getBytes(final long startPosInclusive, final long endPosExclusive) {
        try (final RowSet rangeRowSet = getSubIndexByPos(startPosInclusive, endPosExclusive);
                final ChunkSource.FillContext context =
                        columnSource.makeFillContext(rangeRowSet.intSize("getBytes"), null)) {
            final byte[] result = new byte[rangeRowSet.intSize("getBytes")];
            columnSource.fillChunk(context, WritableByteChunk.writableChunkWrap(result), rangeRowSet);
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
        long index = this.rowSet.get(pos);
        if (index == -1) {
            return QueryConstants.NULL_CHAR;
        }
        return columnSource.getChar(index);
    }

    @Override
    public char[] getChars(final long startPosInclusive, final long endPosExclusive) {
        try (final RowSet rangeRowSet = getSubIndexByPos(startPosInclusive, endPosExclusive);
                final ChunkSource.FillContext context =
                        columnSource.makeFillContext(rangeRowSet.intSize("getChars"), null)) {
            final char[] result = new char[rangeRowSet.intSize("getChars")];
            columnSource.fillChunk(context, WritableCharChunk.writableChunkWrap(result), rangeRowSet);
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
        long index = this.rowSet.get(pos);
        if (index == -1) {
            return QueryConstants.NULL_DOUBLE;
        }
        return columnSource.getDouble(index);
    }

    @Override
    public double[] getDoubles(final long startPosInclusive, final long endPosExclusive) {
        try (final RowSet rangeRowSet = getSubIndexByPos(startPosInclusive, endPosExclusive);
                final ChunkSource.FillContext context =
                        columnSource.makeFillContext(rangeRowSet.intSize("getDoubles"), null)) {
            final double[] result = new double[rangeRowSet.intSize("getDoubles")];
            columnSource.fillChunk(context, WritableDoubleChunk.writableChunkWrap(result), rangeRowSet);
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
        long index = this.rowSet.get(pos);
        if (index == -1) {
            return QueryConstants.NULL_FLOAT;
        }
        return columnSource.getFloat(index);
    }

    @Override
    public float[] getFloats(final long startPosInclusive, final long endPosExclusive) {
        try (final RowSet rangeRowSet = getSubIndexByPos(startPosInclusive, endPosExclusive);
                final ChunkSource.FillContext context =
                        columnSource.makeFillContext(rangeRowSet.intSize("getFloats"), null)) {
            final float[] result = new float[rangeRowSet.intSize("getFloats")];
            columnSource.fillChunk(context, WritableFloatChunk.writableChunkWrap(result), rangeRowSet);
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
        long index = this.rowSet.get(pos);
        if (index == -1) {
            return QueryConstants.NULL_INT;
        }
        return columnSource.getInt(index);
    }

    @Override
    public int[] getInts(final long startPosInclusive, final long endPosExclusive) {
        try (final RowSet rangeRowSet = getSubIndexByPos(startPosInclusive, endPosExclusive);
                final ChunkSource.FillContext context =
                        columnSource.makeFillContext(rangeRowSet.intSize("getInts"), null)) {
            final int[] result = new int[rangeRowSet.intSize("getInts")];
            columnSource.fillChunk(context, WritableIntChunk.writableChunkWrap(result), rangeRowSet);
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
        long index = this.rowSet.get(pos);
        if (index == -1) {
            return QueryConstants.NULL_LONG;
        }
        return columnSource.getLong(index);
    }

    @Override
    public long[] getLongs(final long startPosInclusive, final long endPosExclusive) {
        try (final RowSet rangeRowSet = getSubIndexByPos(startPosInclusive, endPosExclusive);
                final ChunkSource.FillContext context =
                        columnSource.makeFillContext(rangeRowSet.intSize("getLongs"), null)) {
            final long[] result = new long[rangeRowSet.intSize("getLongs")];
            columnSource.fillChunk(context, WritableLongChunk.writableChunkWrap(result), rangeRowSet);
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
        long index = this.rowSet.get(pos);
        if (index == -1) {
            return QueryConstants.NULL_SHORT;
        }
        return columnSource.getShort(index);
    }

    @Override
    public short[] getShorts(final long startPosInclusive, final long endPosExclusive) {
        try (final RowSet rangeRowSet = getSubIndexByPos(startPosInclusive, endPosExclusive);
                final ChunkSource.FillContext context =
                        columnSource.makeFillContext(rangeRowSet.intSize("getShorts"), null)) {
            final short[] result = new short[rangeRowSet.intSize("getShorts")];
            columnSource.fillChunk(context, WritableShortChunk.writableChunkWrap(result), rangeRowSet);
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
}
