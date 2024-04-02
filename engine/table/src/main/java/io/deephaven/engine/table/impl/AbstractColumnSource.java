//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.string.cache.CharSequenceUtils;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataIndex;
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
import java.util.Collections;

public abstract class AbstractColumnSource<T> implements
        ColumnSource<T>,
        DefaultChunkSource.WithPrev<Values> {

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
            @NotNull final RowSet mapper,
            final Object... keys) {

        if (dataIndex != null) {
            final Table indexTable = dataIndex.table();
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

                final DataIndex.RowKeyLookup rowKeyLookup = dataIndex.rowKeyLookup();
                for (Object key : keys) {
                    final long rowKey = rowKeyLookup.apply(key, usePrev);
                    if (rowKey != RowSequence.NULL_ROW_KEY) {
                        matchingIndexRowsBuilder.addKey(rowKey);
                    }
                }
                matchingIndexRows = matchingIndexRowsBuilder.build();
            }

            try (final SafeCloseable ignored = matchingIndexRows) {
                final WritableRowSet filtered = invertMatch ? mapper.copy() : RowSetFactory.empty();
                if (matchingIndexRows.isNonempty()) {
                    final ColumnSource<RowSet> indexRowSetSource = usePrev
                            ? dataIndex.rowSetColumn().getPrevSource()
                            : dataIndex.rowSetColumn();
                    try (final CloseableIterator<RowSet> matchingIndexRowSetIterator =
                            ChunkedColumnIterator.make(indexRowSetSource, matchingIndexRows)) {
                        matchingIndexRowSetIterator.forEachRemaining((final RowSet matchingRowSet) -> {
                            if (invertMatch) {
                                filtered.remove(matchingRowSet);
                            } else {
                                try (final RowSet intersected = matchingRowSet.intersect(mapper)) {
                                    filtered.insert(intersected);
                                }
                            }
                        });
                    }
                }
                return filtered;
            }
        } else {
            return ChunkFilter.applyChunkFilter(mapper, this, usePrev,
                    ChunkMatchFilterFactory.getChunkFilter(type, caseInsensitive, invertMatch, keys));
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
