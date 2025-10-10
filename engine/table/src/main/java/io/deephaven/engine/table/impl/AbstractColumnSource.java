//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.chunkfillers.ChunkFiller;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.chunkfilter.ChunkMatchFilterFactory;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.UnboxedLongBackedColumnSource;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

public abstract class AbstractColumnSource<T> implements
        ColumnSource<T>,
        DefaultChunkSource.WithPrev<Values>,
        PushdownFilterMatcher {

    /**
     * Minimum average run length in an {@link RowSequence} that should trigger {@link Chunk}-filling by key ranges
     * instead of individual keys.
     */
    public static final long USE_RANGES_AVERAGE_RUN_LENGTH = 5;

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
            @NotNull final RowSet rowsetToFilter,
            final Object... keys) {
        return doChunkFilter(invertMatch, usePrev, caseInsensitive, rowsetToFilter, keys);
    }

    private WritableRowSet doChunkFilter(final boolean invertMatch,
            final boolean usePrev,
            final boolean caseInsensitive,
            @NotNull final RowSet rowsetToFilter,
            final Object[] keys) {
        return ChunkFilter.applyChunkFilter(rowsetToFilter, this, usePrev,
                ChunkMatchFilterFactory.getChunkFilter(type, caseInsensitive, invertMatch, keys));
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

    /**
     * Get the pushdown predicate manager for this column source; returns null if there is no pushdown manager.
     */
    public PushdownPredicateManager pushdownManager() {
        return null;
    }

    @Override
    public void estimatePushdownFilterCost(
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final PushdownFilterContext context,
            final JobScheduler jobScheduler,
            final LongConsumer onComplete,
            final Consumer<Exception> onError) {
        // Default to having no benefit by pushing down.
        onComplete.accept(Long.MAX_VALUE);
    }

    @Override
    public void pushdownFilter(
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final PushdownFilterContext context,
            final long costCeiling,
            final JobScheduler jobScheduler,
            final Consumer<PushdownResult> onComplete,
            final Consumer<Exception> onError) {
        // Default to returning all results as "maybe"
        onComplete.accept(PushdownResult.allMaybeMatch(selection));
    }

    @Override
    public PushdownFilterContext makePushdownFilterContext(
            final WhereFilter filter,
            final List<ColumnSource<?>> filterSources) {
        return PushdownFilterContext.NO_PUSHDOWN_CONTEXT;
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
