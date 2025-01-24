//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.stats.Stats;
import io.deephaven.base.stats.ThreadSafeCounter;
import io.deephaven.base.stats.Value;
import io.deephaven.base.string.cache.CharSequenceUtils;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.DataIndexOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.chunkfillers.ChunkFiller;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.chunkfilter.ChunkMatchFilterFactory;
import io.deephaven.engine.table.impl.sources.UnboxedLongBackedColumnSource;
import io.deephaven.engine.table.iterators.ChunkedColumnIterator;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.hash.KeyedObjectHashSet;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;

public abstract class AbstractColumnSource<T> implements
        ColumnSource<T>,
        DefaultChunkSource.WithPrev<Values> {

    /**
     * For a {@link #match(boolean, boolean, boolean, DataIndex, RowSet, Object...)} call that uses a DataIndex, by
     * default we do not force the entire DataIndex to be loaded into memory. This is because many
     * {@link io.deephaven.engine.table.impl.select.MatchFilter}s are highly selective and only need to instantiate a
     * single RowSet value rather than the complete DataIndex for the entire table. When the Configuration property
     * "AbstractColumnSource.usePartialDataIndex" is set to false, the query engine materializes the entire DataIndex
     * table for the match call.
     */
    public static boolean USE_PARTIAL_TABLE_DATA_INDEX = Configuration.getInstance()
            .getBooleanWithDefault("AbstractColumnSource.usePartialDataIndex", true);
    /**
     * After generating a DataIndex table and identifying which row keys are responsive to the filter, the result RowSet
     * can be built in serial or in parallel. By default, the index is built in parallel which may take advantage of
     * using more threads for I/O of the index data structure. Parallel builds do require more setup and thread
     * synchronization, so they can be disabled by setting the Configuration property
     * "AbstractColumnSource.useParallelIndexBuild" to false.
     */
    public static boolean USE_PARALLEL_ROWSET_BUILD = Configuration.getInstance()
            .getBooleanWithDefault("AbstractColumnSource.useParallelRowSetBuild", true);

    /**
     * Duration of match() calls using a DataIndex (also provides the count).
     */
    private static final Value INDEX_FILTER_MILLIS =
            Stats.makeItem("AbstractColumnSource", "indexFilterMillis", ThreadSafeCounter.FACTORY,
                    "Duration of match() with a DataIndex in millis")
                    .getValue();
    /**
     * Duration of match() calls using a chunk filter (i.e. no DataIndex).
     */
    private static final Value CHUNK_FILTER_MILLIS =
            Stats.makeItem("AbstractColumnSource", "chunkFilterMillis", ThreadSafeCounter.FACTORY,
                    "Duration of match() without a DataIndex in millis")
                    .getValue();

    /**
     * Minimum average run length in an {@link RowSequence} that should trigger {@link Chunk}-filling by key ranges
     * instead of individual keys.
     */
    public static final long USE_RANGES_AVERAGE_RUN_LENGTH = 5;

    private static final int CHUNK_SIZE = 1 << 11;

    protected final Class<T> type;
    protected final Class<?> componentType;

    protected final UpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph();

    protected AbstractColumnSource(@NotNull final Class<T> type) {
        this(type, null);
    }

    public AbstractColumnSource(@NotNull final Class<T> type, @Nullable final Class<?> elementType) {
        if (type == boolean.class) {
            // noinspection unchecked
            this.type = (Class<T>) Boolean.class;
        } else if (type == Boolean.class) {
            this.type = type;
        } else {
            // noinspection rawtypes
            final Class unboxedType = TypeUtils.getUnboxedType(type);
            // noinspection unchecked
            this.type = unboxedType != null ? unboxedType : type;
        }
        if (type.isArray()) {
            componentType = type.getComponentType();
        } else if (Vector.class.isAssignableFrom(type)) {
            if (ByteVector.class.isAssignableFrom(type)) {
                componentType = byte.class;
            } else if (CharVector.class.isAssignableFrom(type)) {
                componentType = char.class;
            } else if (DoubleVector.class.isAssignableFrom(type)) {
                componentType = double.class;
            } else if (FloatVector.class.isAssignableFrom(type)) {
                componentType = float.class;
            } else if (IntVector.class.isAssignableFrom(type)) {
                componentType = int.class;
            } else if (LongVector.class.isAssignableFrom(type)) {
                componentType = long.class;
            } else if (ShortVector.class.isAssignableFrom(type)) {
                componentType = short.class;
            } else {
                componentType = elementType == null ? Object.class : elementType;
            }
        } else {
            componentType = null;
        }
    }

    @Override
    public Class<T> getType() {
        return type;
    }

    @Override
    public Class<?> getComponentType() {
        return componentType;
    }

    @Override
    public ColumnSource<T> getPrevSource() {
        return new PrevColumnSource<>(this);
    }

    @Override
    public WritableRowSet match(
            final boolean invertMatch,
            final boolean usePrev,
            final boolean caseInsensitive,
            @Nullable final DataIndex dataIndex,
            @NotNull final RowSet rowsetToFilter,
            final Object... keys) {
        if (dataIndex == null) {
            return doChunkFilter(invertMatch, usePrev, caseInsensitive, rowsetToFilter, keys);
        }
        final long t0 = System.nanoTime();
        try {
            return doDataIndexFilter(invertMatch, usePrev, caseInsensitive, dataIndex, rowsetToFilter, keys);
        } finally {
            final long t1 = System.nanoTime();
            INDEX_FILTER_MILLIS.sample((t1 - t0) / 1_000_000);
        }
    }

    private WritableRowSet doDataIndexFilter(final boolean invertMatch,
            final boolean usePrev,
            final boolean caseInsensitive,
            @NotNull final DataIndex dataIndex,
            @NotNull final RowSet rowsetToFilter,
            final Object[] keys) {
        final DataIndexOptions partialOption =
                USE_PARTIAL_TABLE_DATA_INDEX ? DataIndexOptions.USING_PARTIAL_TABLE : DataIndexOptions.DEFAULT;

        final Table indexTable = dataIndex.table(partialOption);

        final RowSet matchingIndexRows;
        if (caseInsensitive && type == String.class) {
            // Linear scan through the index table, accumulating index row keys for case-insensitive matches
            final RowSetBuilderSequential matchingIndexRowsBuilder = RowSetFactory.builderSequential();

            // noinspection rawtypes
            final KeyedObjectHashSet keySet = new KeyedObjectHashSet<>(new CIStringKey());
            // noinspection unchecked
            Collections.addAll(keySet, keys);

            final RowSet indexRowSet = usePrev ? indexTable.getRowSet().prev() : indexTable.getRowSet();
            final ColumnSource<?> indexKeySource =
                    indexTable.getColumnSource(dataIndex.keyColumnNames().get(0), String.class);

            final int chunkSize = (int) Math.min(CHUNK_SIZE, indexRowSet.size());
            try (final RowSequence.Iterator indexRowSetIterator = indexRowSet.getRowSequenceIterator();
                    final GetContext indexKeyGetContext = indexKeySource.makeGetContext(chunkSize)) {
                while (indexRowSetIterator.hasMore()) {
                    final RowSequence chunkIndexRows = indexRowSetIterator.getNextRowSequenceWithLength(chunkSize);
                    final ObjectChunk<String, ? extends Values> chunkKeys = (usePrev
                            ? indexKeySource.getPrevChunk(indexKeyGetContext, chunkIndexRows)
                            : indexKeySource.getChunk(indexKeyGetContext, chunkIndexRows)).asObjectChunk();
                    final LongChunk<OrderedRowKeys> chunkRowKeys = chunkIndexRows.asRowKeyChunk();
                    final int thisChunkSize = chunkKeys.size();
                    for (int ii = 0; ii < thisChunkSize; ++ii) {
                        final String key = chunkKeys.get(ii);
                        if (keySet.containsKey(key)) {
                            matchingIndexRowsBuilder.appendKey(chunkRowKeys.get(ii));
                        }
                    }
                }
            }
            matchingIndexRows = matchingIndexRowsBuilder.build();
        } else {
            // Use the lookup function to get the index row keys for the matching keys
            final RowSetBuilderRandom matchingIndexRowsBuilder = RowSetFactory.builderRandom();

            final DataIndex.RowKeyLookup rowKeyLookup = dataIndex.rowKeyLookup(partialOption);
            for (final Object key : keys) {
                final long rowKey = rowKeyLookup.apply(key, usePrev);
                if (rowKey != RowSequence.NULL_ROW_KEY) {
                    matchingIndexRowsBuilder.addKey(rowKey);
                }
            }
            matchingIndexRows = matchingIndexRowsBuilder.build();
        }

        try (final SafeCloseable ignored = matchingIndexRows) {
            final WritableRowSet filtered = invertMatch ? rowsetToFilter.copy() : RowSetFactory.empty();
            if (matchingIndexRows.isNonempty()) {
                final ColumnSource<RowSet> indexRowSetSource = usePrev
                        ? dataIndex.rowSetColumn(partialOption).getPrevSource()
                        : dataIndex.rowSetColumn(partialOption);

                if (USE_PARALLEL_ROWSET_BUILD) {
                    final long[] rowKeyArray = new long[matchingIndexRows.intSize()];
                    matchingIndexRows.toRowKeyArray(rowKeyArray);
                    Arrays.stream(rowKeyArray).parallel().forEach((final long rowKey) -> {
                        final RowSet matchingRowSet = indexRowSetSource.get(rowKey);
                        assert matchingRowSet != null;
                        if (invertMatch) {
                            synchronized (filtered) {
                                filtered.remove(matchingRowSet);
                            }
                        } else {
                            try (final RowSet intersected = matchingRowSet.intersect(rowsetToFilter)) {
                                synchronized (filtered) {
                                    filtered.insert(intersected);
                                }
                            }
                        }
                    });
                } else {
                    try (final CloseableIterator<RowSet> matchingIndexRowSetIterator =
                            ChunkedColumnIterator.make(indexRowSetSource, matchingIndexRows)) {
                        matchingIndexRowSetIterator.forEachRemaining((final RowSet matchingRowSet) -> {
                            if (invertMatch) {
                                filtered.remove(matchingRowSet);
                            } else {
                                try (final RowSet intersected = matchingRowSet.intersect(rowsetToFilter)) {
                                    filtered.insert(intersected);
                                }
                            }
                        });
                    }
                }
            }
            return filtered;
        }
    }

    private WritableRowSet doChunkFilter(final boolean invertMatch,
            final boolean usePrev,
            final boolean caseInsensitive,
            @NotNull final RowSet rowsetToFilter,
            final Object[] keys) {
        final long t0 = System.nanoTime();
        try {
            return ChunkFilter.applyChunkFilter(rowsetToFilter, this, usePrev,
                    ChunkMatchFilterFactory.getChunkFilter(type, caseInsensitive, invertMatch, keys));
        } finally {
            final long t1 = System.nanoTime();
            CHUNK_FILTER_MILLIS.sample((t1 - t0) / 1_000_000);
        }
    }

    private static final class CIStringKey implements KeyedObjectKey<String, String> {
        @Override
        public String getKey(String s) {
            return s;
        }

        @Override
        public int hashKey(String s) {
            return (s == null) ? 0 : CharSequenceUtils.caseInsensitiveHashCode(s);
        }

        @Override
        public boolean equalKey(String s, String s2) {
            return (s == null) ? s2 == null : s.equalsIgnoreCase(s2);
        }
    }

    @Override
    public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        defaultFillChunk(context, destination, rowSequence);
    }

    @VisibleForTesting
    public final void defaultFillChunk(@SuppressWarnings("unused") @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        final ChunkFiller filler = ChunkFiller.forChunkType(destination.getChunkType());
        if (rowSequence.getAverageRunLengthEstimate() >= USE_RANGES_AVERAGE_RUN_LENGTH) {
            filler.fillByRanges(this, rowSequence, destination);
        } else {
            filler.fillByIndices(this, rowSequence, destination);
        }
    }

    @Override
    public void fillPrevChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        defaultFillPrevChunk(context, destination, rowSequence);
    }

    protected final void defaultFillPrevChunk(@SuppressWarnings("unused") @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        final ChunkFiller filler = ChunkFiller.forChunkType(destination.getChunkType());
        if (rowSequence.getAverageRunLengthEstimate() >= USE_RANGES_AVERAGE_RUN_LENGTH) {
            filler.fillPrevByRanges(this, rowSequence, destination);
        } else {
            filler.fillPrevByIndices(this, rowSequence, destination);
        }
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return false;
    }

    @Override
    public final <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> reinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) throws IllegalArgumentException {
        if (!allowsReinterpret(alternateDataType)) {
            throw new IllegalArgumentException("Unsupported reinterpret for " + getClass().getSimpleName()
                    + ": type=" + getType()
                    + ", alternateDataType=" + alternateDataType);
        }
        return doReinterpret(alternateDataType);
    }

    /**
     * Supply allowed reinterpret results. The default implementation handles the most common case to avoid code
     * duplication.
     *
     * @param alternateDataType The alternate data type
     * @return The resulting {@link ColumnSource}
     */
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        if (getType() == Instant.class || getType() == ZonedDateTime.class) {
            Assert.eq(alternateDataType, "alternateDataType", long.class);
            // noinspection unchecked
            return (ColumnSource<ALTERNATE_DATA_TYPE>) new UnboxedLongBackedColumnSource<>(this);
        }
        throw new IllegalArgumentException("Unsupported reinterpret for " + getClass().getSimpleName()
                + ": type=" + getType()
                + ", alternateDataType=" + alternateDataType);
    }

    public static abstract class DefaultedMutable<DATA_TYPE> extends AbstractColumnSource<DATA_TYPE>
            implements MutableColumnSourceGetDefaults.ForObject<DATA_TYPE> {

        protected DefaultedMutable(@NotNull final Class<DATA_TYPE> type) {
            super(type);
        }

        protected DefaultedMutable(@NotNull final Class<DATA_TYPE> type, @Nullable final Class<?> elementType) {
            super(type, elementType);
        }
    }

    public static abstract class DefaultedImmutable<DATA_TYPE> extends AbstractColumnSource<DATA_TYPE>
            implements ImmutableColumnSourceGetDefaults.ForObject<DATA_TYPE> {

        protected DefaultedImmutable(@NotNull final Class<DATA_TYPE> type) {
            super(type);
        }

        protected DefaultedImmutable(@NotNull final Class<DATA_TYPE> type, @Nullable final Class<?> elementType) {
            super(type, elementType);
        }
    }
}
