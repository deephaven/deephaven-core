/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.string.cache.CharSequenceUtils;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.GroupingProvider;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.chunkfillers.ChunkFiller;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.chunkfilter.ChunkMatchFilterFactory;
import io.deephaven.engine.table.GroupingBuilder;
import io.deephaven.engine.table.impl.sources.UnboxedLongBackedColumnSource;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.vector.*;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;

public abstract class AbstractColumnSource<T> implements
        ColumnSource<T>,
        DefaultChunkSource.WithPrev<Values> {

    /**
     * Minimum average run length in an {@link RowSequence} that should trigger {@link Chunk}-filling by key ranges
     * instead of individual keys.
     */
    public static final long USE_RANGES_AVERAGE_RUN_LENGTH = 5;

    protected final Class<T> type;
    protected final Class<?> componentType;

    protected final UpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph();

    private transient GroupingProvider groupingProvider;
    protected volatile List<ColumnSource<?>> rowSetIndexerKey;

    protected AbstractColumnSource(@NotNull final Class<T> type) {
        this(type, Object.class);
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
                componentType = elementType;
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
    public List<ColumnSource<?>> getColumnSources() {
        List<ColumnSource<?>> localRowSetWritableIndexerKey;
        if ((localRowSetWritableIndexerKey = rowSetIndexerKey) == null) {
            synchronized (this) {
                if ((localRowSetWritableIndexerKey = rowSetIndexerKey) == null) {
                    rowSetIndexerKey = localRowSetWritableIndexerKey = Collections.singletonList(this);
                }
            }
        }
        return localRowSetWritableIndexerKey;
    }

    @Override
    public GroupingBuilder getGroupingBuilder() {
        return groupingProvider == null ? null : groupingProvider.getGroupingBuilder();
    }

    @Override
    public boolean hasGrouping() {
        return groupingProvider != null && groupingProvider.hasGrouping();
    }

    public GroupingProvider getGroupingProvider() {
        return groupingProvider;
    }

    /**
     * Set a grouping provider for use in lazily-constructing groupings.
     *
     * @param groupingProvider The {@link GroupingProvider} to use
     */
    public void setGroupingProvider(@Nullable GroupingProvider groupingProvider) {
        this.groupingProvider = groupingProvider;
    }

    @Override
    public WritableRowSet match(boolean invertMatch,
            boolean usePrev,
            boolean caseInsensitive,
            @NotNull final RowSet startingWritableRowSet,
            final Object... keys) {
        if (canUseGrouping(usePrev)) {
            return matchWithGrouping(startingWritableRowSet, caseInsensitive, invertMatch, keys);
        }

        return ChunkFilter.applyChunkFilter(startingWritableRowSet, this, usePrev,
                ChunkMatchFilterFactory.getChunkFilter(type, caseInsensitive, invertMatch, keys));
    }

    protected final WritableRowSet matchWithGrouping(RowSet startingWritableRowSet, boolean caseInsensitive,
            boolean invertMatch, Object... keys) {
        final GroupingBuilder groupingBuilder = getGroupingBuilder();
        final Table filteredGroups = getGroupingBuilder()
                .clampToIndex(startingWritableRowSet)
                .matching(!caseInsensitive, invertMatch, keys)
                .buildTable();
        final RowSetBuilderRandom allInMatchingGroups = RowSetFactory.builderRandom();
        final ColumnSource<RowSet> indexSource =
                filteredGroups.getColumnSource(groupingBuilder.getIndexColumnName(), RowSet.class);
        filteredGroups.getRowSet().forAllRowKeys(key -> allInMatchingGroups.addRowSet(indexSource.get(key)));

        final WritableRowSet result = allInMatchingGroups.build();
        result.retain(startingWritableRowSet);
        return result;
    }

    protected final boolean canUseGrouping(final boolean usePrev) {
        return (isImmutable() || !usePrev) && groupingProvider != null && groupingProvider.hasGrouping();
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
