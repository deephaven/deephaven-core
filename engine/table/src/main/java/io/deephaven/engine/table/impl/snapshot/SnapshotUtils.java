//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.snapshot;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.select.VectorChunkAdapter;
import io.deephaven.engine.table.impl.sources.DelegatingColumnSource;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.engine.table.impl.util.fill.FillFromElementKernel;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.SafeCloseableArray;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class SnapshotUtils {
    /**
     * Creates a new columnSourceMap based on 'columns'.
     */
    public static <T> Map<String, T> createColumnSourceMap(Map<String, ? extends ColumnSource<?>> columns,
            BiFunction<Class<?>, Class<?>, T> factory) {
        final Map<String, T> result = new LinkedHashMap<>();
        for (final Map.Entry<String, ? extends ColumnSource<?>> entry : columns.entrySet()) {
            final String key = entry.getKey();
            final ColumnSource<?> cs = entry.getValue();
            final T newColumn = factory.apply(cs.getType(), cs.getComponentType());
            result.put(key, newColumn);
        }
        return result;
    }

    /**
     * For each name in stampColumns: - identify a stamp source (namely 'stampColumns.get(name)') - a row in that stamp
     * source (namely 'stampKey') - a stamp dest (namely the column identified by 'destColumns.get(name)') - a bunch of
     * destination rows (namely all the rows defined in 'destRowSet') Then "spray" that single source value over those
     * destination values.
     *
     * @param stampColumns The stamp columns that serve as the source data
     * @param stampKey The source key
     * @param destColumns The destination columns we are "spraying" to
     * @param destRowSet The keys in destColumns we want to write to
     */
    public static void copyStampColumns(@NotNull Map<String, ? extends ColumnSource<?>> stampColumns, long stampKey,
            @NotNull Map<String, ? extends WritableColumnSource<?>> destColumns, @NotNull RowSet destRowSet) {
        if (destRowSet.isEmpty()) {
            return;
        }
        final int chunkSize = (int) Math.min(destRowSet.size(), ChunkUtils.COPY_DATA_CHUNK_SIZE);
        for (Map.Entry<String, ? extends ColumnSource<?>> entry : stampColumns.entrySet()) {
            final String name = entry.getKey();
            final ColumnSource<?> src = entry.getValue();

            // Fill a chunk with the stamp value once, then write it to every destination row
            final WritableColumnSource<?> dest = destColumns.get(name);
            final FillFromElementKernel kernel = FillFromElementKernel.makeFillFromElementKernel(dest.getChunkType());
            try (final ChunkSource.GetContext stampContext = src.makeGetContext(1);
                    final WritableChunk<Values> stampChunk = dest.getChunkType().makeWritableChunk(chunkSize);
                    final ChunkSink.FillFromContext fillContext = dest.makeFillFromContext(chunkSize);
                    final RowSequence.Iterator destIter = destRowSet.getRowSequenceIterator()) {
                kernel.fillFromElement(stampChunk, 0, chunkSize, src.getChunk(stampContext, stampKey, stampKey), 0);
                while (destIter.hasMore()) {
                    final RowSequence destKeys = destIter.getNextRowSequenceWithLength(chunkSize);
                    stampChunk.setSize(destKeys.intSize());
                    dest.fillFromChunk(fillContext, stampChunk, destKeys);
                }
            }
        }
    }

    /**
     * For each name in stampColumns, write the value of each row in {@code stampKeys} to its own block of
     * {@code blockSize} consecutive rows in destColumns.get(name). The blocks are contiguous, follow the order of
     * {@code stampKeys}, and the first begins at row key {@code destOffset}.
     *
     * @param stampColumns The stamp columns that serve as the source data
     * @param stampKeys The source keys, one per block
     * @param destColumns The destination columns we are writing to, with capacity for every block
     * @param destOffset The first row key of the first block in destColumns
     * @param blockSize The number of rows in each block
     */
    public static void copyStampColumnsRepeated(@NotNull Map<String, ? extends ColumnSource<?>> stampColumns,
            @NotNull RowSet stampKeys, @NotNull Map<String, ? extends WritableColumnSource<?>> destColumns,
            long destOffset, long blockSize) {
        if (stampKeys.isEmpty() || blockSize == 0) {
            return;
        }
        final int chunkSize = (int) Math.min(blockSize, ChunkUtils.COPY_DATA_CHUNK_SIZE);
        final int stampChunkSize = (int) Math.min(stampKeys.size(), ChunkUtils.COPY_DATA_CHUNK_SIZE);
        for (Map.Entry<String, ? extends ColumnSource<?>> entry : stampColumns.entrySet()) {
            final ColumnSource<?> src = entry.getValue();
            final WritableColumnSource<?> dest = destColumns.get(entry.getKey());
            final FillFromElementKernel kernel = FillFromElementKernel.makeFillFromElementKernel(dest.getChunkType());
            try (final ChunkSource.GetContext stampContext = src.makeGetContext(stampChunkSize);
                    final WritableChunk<Values> stampChunk = dest.getChunkType().makeWritableChunk(chunkSize);
                    final ChunkSink.FillFromContext fillContext = dest.makeFillFromContext(chunkSize);
                    final RowSequence.Iterator stampIter = stampKeys.getRowSequenceIterator()) {
                long blockStart = destOffset;
                while (stampIter.hasMore()) {
                    final Chunk<? extends Values> stampValues =
                            src.getChunk(stampContext, stampIter.getNextRowSequenceWithLength(stampChunkSize));
                    final int numStamps = stampValues.size();
                    for (int si = 0; si < numStamps; ++si) {
                        stampChunk.setSize(chunkSize);
                        kernel.fillFromElement(stampChunk, 0, chunkSize, stampValues, si);
                        for (long blockPosition = 0; blockPosition < blockSize; blockPosition += chunkSize) {
                            final long lastBlockPosition = Math.min(blockPosition + chunkSize, blockSize) - 1;
                            stampChunk.setSize((int) (lastBlockPosition - blockPosition + 1));
                            try (final RowSequence destKeys = RowSequenceFactory.forRange(
                                    blockStart + blockPosition, blockStart + lastBlockPosition)) {
                                dest.fillFromChunk(fillContext, stampChunk, destKeys);
                            }
                        }
                        blockStart += blockSize;
                    }
                }
            }
        }
    }

    /**
     * Like the above, but with a singleton destination. For each name in stampColumns: - identify a stamp source
     * (namely 'stampColumns.get(name)') - a row in that stamp source (namely 'stampKey') - a stamp dest (namely the
     * column identified by 'destColumns.get(name)') - a row in the destination (namely 'destKey') Then copy the source
     * value to the destination value.
     *
     * @param stampColumns The stamp columns that serve as the source data
     * @param stampKey The source key
     * @param destColumns The destination columns we are writing to
     */
    public static void copyStampColumns(@NotNull Map<String, ? extends ColumnSource<?>> stampColumns, long stampKey,
            @NotNull Map<String, SingleValueColumnSource<?>> destColumns) {
        for (Map.Entry<String, ? extends ColumnSource<?>> entry : stampColumns.entrySet()) {
            final String name = entry.getKey();
            final ColumnSource<?> src = entry.getValue();

            // Fill the corresponding destination column
            // noinspection rawtypes
            final SingleValueColumnSource dest = destColumns.get(name);
            // noinspection unchecked
            dest.set(src.get(stampKey));
        }
    }

    /**
     * Like the above, but with a singleton destination that will be set to the appropriate {@code null} value.
     *
     * @param stampColumns The stamp columns that serve as the source data
     * @param destColumns The destination columns we are writing to
     */
    public static void setNullStampColumns(
            @NotNull Map<String, ? extends ColumnSource<?>> stampColumns,
            @NotNull Map<String, SingleValueColumnSource<?>> destColumns) {
        for (String name : stampColumns.keySet()) {
            destColumns.get(name).setNull();
        }
    }

    /**
     * Like the above, but sets each of the rows in {@code destRowSet} to the appropriate {@code null} value.
     *
     * @param stampColumns The stamp columns that serve as the source data
     * @param destColumns The destination columns we are writing to
     * @param destRowSet The keys in destColumns we want to write to
     */
    public static void setNullStampColumns(
            @NotNull Map<String, ? extends ColumnSource<?>> stampColumns,
            @NotNull Map<String, ? extends WritableColumnSource<?>> destColumns,
            @NotNull RowSet destRowSet) {
        for (String name : stampColumns.keySet()) {
            destColumns.get(name).setNull(destRowSet);
        }
    }

    /**
     * For each name in srcColumns, copy all the data at srcColumns.get(name) (with a range of rows defined by
     * srcRowSet) to a column indicated by destColumns.get(name) (with a range of rows defined by destRowSet).
     *
     * @param srcColumns The stamp columns that serve as the source data
     * @param srcRowSet The keys in the srcColumns we are reading from
     * @param destColumns The destination columns we are writing to
     * @param destRowSet The keys in destColumns we want to write to
     */
    public static void copyDataColumns(@NotNull Map<String, ChunkSource.WithPrev<? extends Values>> srcColumns,
            @NotNull RowSet srcRowSet, @NotNull Map<String, ? extends WritableColumnSource<?>> destColumns,
            @NotNull RowSet destRowSet,
            boolean usePrev) {
        assert srcRowSet.size() == destRowSet.size();
        if (srcRowSet.isEmpty()) {
            return;
        }
        for (Map.Entry<String, ? extends ChunkSource.WithPrev<? extends Values>> entry : srcColumns.entrySet()) {
            final String name = entry.getKey();
            final ChunkSource.WithPrev<? extends Values> srcCs = entry.getValue();

            final WritableColumnSource<?> destCs = destColumns.get(name);
            destCs.ensureCapacity(destRowSet.lastRowKey() + 1);
            ChunkUtils.copyData(srcCs, srcRowSet, destCs, destRowSet, usePrev);
        }
    }

    /**
     * For each name in srcColumns, copy the data at srcColumns.get(name) for the rows in srcRowSet into
     * {@code numCopies} consecutive, contiguous blocks of destColumns.get(name), the first of which begins at row key
     * {@code destOffset}. Each chunk of source data is read once and written to every block.
     *
     * @param srcColumns The columns that serve as the source data
     * @param srcRowSet The keys in the srcColumns we are reading from
     * @param destColumns The destination columns we are writing to, with capacity for every block
     * @param destOffset The first row key of the first block in destColumns
     * @param numCopies The number of blocks to write
     */
    public static void copyDataColumnsRepeated(
            @NotNull Map<String, ChunkSource.WithPrev<? extends Values>> srcColumns,
            @NotNull RowSet srcRowSet,
            @NotNull Map<String, ? extends WritableColumnSource<?>> destColumns,
            long destOffset,
            long numCopies) {
        final long blockSize = srcRowSet.size();
        if (blockSize == 0 || numCopies == 0) {
            return;
        }
        final int chunkSize = (int) Math.min(blockSize, ChunkUtils.COPY_DATA_CHUNK_SIZE);
        final long destEnd = destOffset + numCopies * blockSize;
        final int numColumns = srcColumns.size();

        // noinspection unchecked
        final ChunkSource.WithPrev<? extends Values>[] sources = new ChunkSource.WithPrev[numColumns];
        final WritableColumnSource<?>[] destinations = new WritableColumnSource[numColumns];
        final ChunkSource.GetContext[] getContexts = new ChunkSource.GetContext[numColumns];
        final ChunkSink.FillFromContext[] fillContexts = new ChunkSink.FillFromContext[numColumns];
        try (final SharedContext sharedContext = SharedContext.makeSharedContext();
                final SafeCloseableArray<ChunkSource.GetContext> ignored = new SafeCloseableArray<>(getContexts);
                final SafeCloseableArray<ChunkSink.FillFromContext> ignored2 = new SafeCloseableArray<>(fillContexts);
                final RowSequence.Iterator srcIter = srcRowSet.getRowSequenceIterator()) {
            int cc = 0;
            for (final Map.Entry<String, ChunkSource.WithPrev<? extends Values>> entry : srcColumns.entrySet()) {
                sources[cc] = entry.getValue();
                destinations[cc] = destColumns.get(entry.getKey());
                getContexts[cc] = sources[cc].makeGetContext(chunkSize, sharedContext);
                fillContexts[cc] = destinations[cc].makeFillFromContext(chunkSize);
                ++cc;
            }

            long blockPosition = 0;
            while (srcIter.hasMore()) {
                final RowSequence srcKeys = srcIter.getNextRowSequenceWithLength(chunkSize);
                final long lastBlockPosition = blockPosition + srcKeys.size() - 1;
                sharedContext.reset();
                for (cc = 0; cc < numColumns; ++cc) {
                    final Chunk<? extends Values> chunk = sources[cc].getChunk(getContexts[cc], srcKeys);
                    for (long blockStart = destOffset; blockStart < destEnd; blockStart += blockSize) {
                        try (final RowSequence destKeys = RowSequenceFactory.forRange(
                                blockStart + blockPosition, blockStart + lastBlockPosition)) {
                            destinations[cc].fillFromChunk(fillContexts[cc], chunk, destKeys);
                        }
                    }
                }
                blockPosition = lastBlockPosition + 1;
            }
        }
    }

    /**
     * Find the columns with an {@link ChunkType#Object Object} chunk type, whose values must be set to {@code null}
     * when their rows leave a result so that the columns hold no references to them.
     *
     * @param columns The columns to search
     * @return The columns of {@code columns} with an Object chunk type
     */
    public static WritableColumnSource<?>[] objectColumns(
            @NotNull Map<String, ? extends WritableColumnSource<?>> columns) {
        return columns.values().stream()
                .filter(column -> column.getChunkType() == ChunkType.Object)
                .toArray(WritableColumnSource<?>[]::new);
    }

    /**
     * Set the rows in {@code rowSequence} of each of {@code objectColumns} to {@code null}.
     *
     * @param objectColumns The columns to clear, as found by {@link #objectColumns(Map)}
     * @param rowSequence The keys in objectColumns to clear
     */
    public static void nullObjectColumns(
            @NotNull WritableColumnSource<?>[] objectColumns,
            @NotNull RowSequence rowSequence) {
        for (final WritableColumnSource<?> column : objectColumns) {
            ChunkUtils.fillWithNullValue(column, rowSequence);
        }
    }

    @NotNull
    public static Map<String, ChunkSource.WithPrev<? extends Values>> generateSnapshotDataColumns(final Table table) {
        final Map<String, ? extends ColumnSource<?>> sourceColumns = table.getColumnSourceMap();
        final Map<String, ChunkSource.WithPrev<? extends Values>> snapshotDataColumns =
                new LinkedHashMap<>(sourceColumns.size());

        for (final Map.Entry<String, ? extends ColumnSource<?>> entry : sourceColumns.entrySet()) {
            final ColumnSource<?> columnSource = entry.getValue();
            final ChunkSource.WithPrev<? extends Values> maybeTransformed = maybeWrapVector(columnSource);
            snapshotDataColumns.put(entry.getKey(), maybeTransformed);
        }
        return snapshotDataColumns;
    }

    /**
     * If columnSource's type is a vector, wrap it with a {@link VectorChunkAdapter} so that copying from it produces
     * direct arrays suitable for snapshots.
     *
     * @param columnSource the column source to wrap
     * @return the original column source or a VectorChunkAdapter as appropriate
     */
    @NotNull
    public static ChunkSource.WithPrev<? extends Values> maybeWrapVector(@NotNull final ColumnSource<?> columnSource) {
        final ChunkSource.WithPrev<? extends Values> maybeTransformed;
        if (Vector.class.isAssignableFrom(columnSource.getType())) {
            maybeTransformed = new VectorChunkAdapter<>(columnSource);
        } else {
            maybeTransformed = columnSource;
        }
        return maybeTransformed;
    }

    @NotNull
    public static Map<String, ? extends ColumnSource<?>> generateTriggerStampColumns(Table table) {
        final Map<String, ? extends ColumnSource<?>> stampColumns = table.getColumnSourceMap();
        if (stampColumns.values().stream().noneMatch(cs -> Vector.class.isAssignableFrom(cs.getType()))) {
            return stampColumns;
        }
        final Map<String, ColumnSource<?>> triggerStampColumns = new LinkedHashMap<>(stampColumns.size());
        for (Map.Entry<String, ? extends ColumnSource<?>> entry : stampColumns.entrySet()) {
            triggerStampColumns.put(entry.getKey(), maybeTransformToDirectVectorColumnSource(entry.getValue()));
        }
        return triggerStampColumns;
    }

    @NotNull
    public static ColumnSource<?> maybeTransformToDirectVectorColumnSource(ColumnSource<?> columnSource) {
        if (!Vector.class.isAssignableFrom(columnSource.getType())) {
            return columnSource;
        }
        // noinspection unchecked
        final Class<Vector<?>> vectorType = (Class<Vector<?>>) columnSource.getType();
        // noinspection unchecked
        final ColumnSource<Vector<?>> underlyingVectorSource = (ColumnSource<Vector<?>>) columnSource;
        return new VectorDirectDelegatingColumnSource(vectorType, columnSource.getComponentType(),
                underlyingVectorSource);
    }

    /**
     * Converts the Vectors of a snapshot trigger column into direct Vectors, both for single values and for chunks.
     */
    private static class VectorDirectDelegatingColumnSource extends DelegatingColumnSource<Vector<?>, Vector<?>> {
        private final VectorChunkAdapter<Values> directChunks;

        public VectorDirectDelegatingColumnSource(Class<Vector<?>> vectorType,
                Class<?> componentType,
                ColumnSource<Vector<?>> underlyingVectorSource) {
            super(vectorType, componentType, underlyingVectorSource);
            directChunks = new VectorChunkAdapter<>(underlyingVectorSource);
        }

        @Override
        public ChunkSource.FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
            return directChunks.makeFillContext(chunkCapacity, sharedContext);
        }

        @Override
        public ChunkSource.GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext) {
            return directChunks.makeGetContext(chunkCapacity, sharedContext);
        }

        @Override
        public Chunk<? extends Values> getChunk(@NotNull ChunkSource.GetContext context,
                @NotNull RowSequence rowSequence) {
            return directChunks.getChunk(context, rowSequence);
        }

        @Override
        public Chunk<? extends Values> getChunk(@NotNull ChunkSource.GetContext context, long firstKey,
                long lastKey) {
            return directChunks.getChunk(context, firstKey, lastKey);
        }

        @Override
        public void fillChunk(@NotNull ChunkSource.FillContext context,
                @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
            directChunks.fillChunk(context, destination, rowSequence);
        }

        @Override
        public Chunk<? extends Values> getPrevChunk(@NotNull ChunkSource.GetContext context,
                @NotNull RowSequence rowSequence) {
            return directChunks.getPrevChunk(context, rowSequence);
        }

        @Override
        public Chunk<? extends Values> getPrevChunk(@NotNull ChunkSource.GetContext context, long firstKey,
                long lastKey) {
            return directChunks.getPrevChunk(context, firstKey, lastKey);
        }

        @Override
        public void fillPrevChunk(@NotNull ChunkSource.FillContext context,
                @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
            directChunks.fillPrevChunk(context, destination, rowSequence);
        }

        @Override
        public Vector<?> get(long rowKey) {
            Vector<?> vector = super.get(rowKey);
            if (vector == null)
                return null;
            return vector.getDirect();
        }

        @Override
        public Vector<?> getPrev(long rowKey) {
            Vector<?> vector = super.getPrev(rowKey);
            if (vector == null)
                return null;
            return vector.getDirect();
        }
    }
}
