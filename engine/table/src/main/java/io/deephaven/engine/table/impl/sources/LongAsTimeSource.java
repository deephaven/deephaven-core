//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;/*
                                                * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
                                                */

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.BasePushdownFilterContext;
import io.deephaven.engine.table.impl.BasePushdownFilterContextImpl;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;
import org.jetbrains.annotations.NotNull;

import java.time.*;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

/**
 * Reinterpret result {@link ColumnSource} implementations that translates {@code long} values to various Time types.
 */
public abstract class LongAsTimeSource<TIME_TYPE> extends AbstractColumnSource<TIME_TYPE>
        implements MutableColumnSourceGetDefaults.ForObject<TIME_TYPE>, ConvertibleTimeSource {

    private final ColumnSource<Long> alternateColumnSource;

    private class BoxingFillContext implements FillContext {
        final FillContext alternateFillContext;
        final WritableLongChunk<Values> innerChunk;

        private BoxingFillContext(final int chunkCapacity, final SharedContext sharedContext) {
            alternateFillContext = alternateColumnSource.makeFillContext(chunkCapacity, sharedContext);
            innerChunk = WritableLongChunk.makeWritableChunk(chunkCapacity);
        }

        @Override
        public void close() {
            alternateFillContext.close();
            innerChunk.close();
        }
    }

    public LongAsTimeSource(final Class<TIME_TYPE> type, ColumnSource<Long> alternateColumnSource) {
        super(type);
        this.alternateColumnSource = alternateColumnSource;
    }

    protected abstract TIME_TYPE makeValue(long val);

    @Override
    public TIME_TYPE get(long index) {
        return makeValue(alternateColumnSource.getLong(index));
    }

    @Override
    public TIME_TYPE getPrev(long index) {
        return makeValue(alternateColumnSource.getPrevLong(index));
    }

    @Override
    public boolean isImmutable() {
        return alternateColumnSource.isImmutable();
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateColumnSource.allowsReinterpret(alternateDataType)
                || alternateDataType == alternateColumnSource.getType();
    }

    @Override
    public <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) throws IllegalArgumentException {
        // noinspection unchecked
        return alternateDataType == alternateColumnSource.getType()
                ? (ColumnSource<ALTERNATE_DATA_TYPE>) alternateColumnSource
                : alternateColumnSource.reinterpret(alternateDataType);
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new BoxingFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        final BoxingFillContext fillContext = (BoxingFillContext) context;
        final WritableLongChunk<Values> innerChunk = fillContext.innerChunk;
        alternateColumnSource.fillChunk(fillContext.alternateFillContext, innerChunk, rowSequence);
        convertToType(destination, innerChunk);
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        final BoxingFillContext fillContext = (BoxingFillContext) context;
        final WritableLongChunk<Values> innerChunk = fillContext.innerChunk;
        alternateColumnSource.fillPrevChunk(fillContext.alternateFillContext, innerChunk, rowSequence);
        convertToType(destination, innerChunk);
    }

    private void convertToType(@NotNull WritableChunk<? super Values> destination, LongChunk<Values> innerChunk) {
        final WritableObjectChunk<TIME_TYPE, ? super Values> dest = destination.asWritableObjectChunk();
        for (int ii = 0; ii < innerChunk.size(); ++ii) {
            dest.set(ii, makeValue(innerChunk.get(ii)));
        }
        dest.setSize(innerChunk.size());
    }

    @Override
    public ColumnSource<ZonedDateTime> toZonedDateTime(@NotNull final ZoneId zone) {
        return new LongAsZonedDateTimeColumnSource(alternateColumnSource, zone);
    }

    @Override
    public ColumnSource<LocalDate> toLocalDate(@NotNull final ZoneId zone) {
        return new LongAsLocalDateColumnSource(alternateColumnSource, zone);
    }

    @Override
    public ColumnSource<LocalTime> toLocalTime(@NotNull final ZoneId zone) {
        return new LongAsLocalTimeColumnSource(alternateColumnSource, zone);
    }

    @Override
    public ColumnSource<Instant> toInstant() {
        return new LongAsInstantColumnSource(alternateColumnSource);
    }

    @Override
    public ColumnSource<Long> toEpochNano() {
        return alternateColumnSource;
    }

    @Override
    public boolean supportsTimeConversion() {
        return true;
    }

    // region Pushdown
    /**
     * Whether the wrapped source holds one value for every row key. A filter over such a source can be evaluated once,
     * against that value, instead of once per row.
     */
    private boolean isSingleValued() {
        return alternateColumnSource instanceof RowKeyAgnosticChunkSource;
    }

    @Override
    public PushdownFilterContext makePushdownFilterContext(
            final WhereFilter filter,
            final List<ColumnSource<?>> filterSources) {
        if (!isSingleValued()) {
            return super.makePushdownFilterContext(filter, filterSources);
        }
        return new BasePushdownFilterContextImpl(filter, filterSources);
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
        if (!isSingleValued()) {
            super.estimatePushdownFilterCost(filter, selection, usePrev, context, jobScheduler, onComplete, onError);
            return;
        }
        onComplete.accept(PushdownResult.TABLE_SINGLE_VALUE_COLUMN_COST);
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
        if (!isSingleValued()) {
            super.pushdownFilter(filter, selection, usePrev, context, costCeiling, jobScheduler, onComplete, onError);
            return;
        }
        if (selection.isEmpty()) {
            // If the selection is empty, we can skip all pushdown filtering.
            onComplete.accept(PushdownResult.noneMatch(selection));
            return;
        }
        final BasePushdownFilterContext filterCtx = (BasePushdownFilterContext) context;
        final Supplier<Chunk<Values>> chunkSupplier =
                () -> SingleValuePushdownHelper.makeChunk((Object) (usePrev ? getPrev(0) : get(0)));
        final boolean matches =
                SingleValuePushdownHelper.filter(selection, usePrev, filterCtx, chunkSupplier, this);
        onComplete.accept(matches ? PushdownResult.allMatch(selection) : PushdownResult.noneMatch(selection));
    }
    // endregion Pushdown
}
