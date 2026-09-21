//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.BasePushdownFilterContext;
import io.deephaven.engine.table.impl.BasePushdownFilterContextImpl;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.util.BooleanUtils;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.*;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

/**
 * Reinterpret result {@link ColumnSource} implementations that translates {@code byte} to {@code Boolean} values.
 */
public class ByteAsBooleanColumnSource extends AbstractColumnSource<Boolean>
        implements MutableColumnSourceGetDefaults.ForBoolean, FillUnordered<Values> {

    private final ColumnSource<Byte> alternateColumnSource;

    public ByteAsBooleanColumnSource(@NotNull final ColumnSource<Byte> alternateColumnSource) {
        super(Boolean.class);
        this.alternateColumnSource = alternateColumnSource;
    }

    @Override
    public Boolean get(final long rowKey) {
        return BooleanUtils.byteAsBoolean(alternateColumnSource.getByte(rowKey));
    }

    @Override
    public Boolean getPrev(final long rowKey) {
        return BooleanUtils.byteAsBoolean(alternateColumnSource.getPrevByte(rowKey));
    }

    @Override
    public boolean isImmutable() {
        return alternateColumnSource.isImmutable();
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == byte.class || alternateDataType == Byte.class;
    }

    @Override
    public <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) throws IllegalArgumentException {
        // noinspection unchecked
        return (ColumnSource<ALTERNATE_DATA_TYPE>) alternateColumnSource;
    }

    private class ToBooleanFillContext implements FillContext {
        final GetContext alternateGetContext;
        final FillContext alternateFillContext;
        final WritableByteChunk<Values> byteChunk;

        private ToBooleanFillContext(final int chunkCapacity, final SharedContext sharedContext) {
            alternateGetContext = alternateColumnSource.makeGetContext(chunkCapacity, sharedContext);
            if (providesFillUnordered()) {
                alternateFillContext = alternateColumnSource.makeFillContext(chunkCapacity, sharedContext);
                byteChunk = WritableByteChunk.makeWritableChunk(chunkCapacity);
            } else {
                alternateFillContext = null;
                byteChunk = null;
            }
        }

        @Override
        public void close() {
            alternateGetContext.close();
            if (alternateFillContext != null) {
                alternateFillContext.close();
                byteChunk.close();
            }
        }
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new ToBooleanFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        final ToBooleanFillContext toBooleanFillContext = (ToBooleanFillContext) context;
        final ByteChunk<? extends Values> byteChunk =
                alternateColumnSource.getChunk(toBooleanFillContext.alternateGetContext, rowSequence).asByteChunk();
        convertToBoolean(destination, byteChunk);
    }

    @Override
    public void fillPrevChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final ToBooleanFillContext toBooleanFillContext = (ToBooleanFillContext) context;
        final ByteChunk<? extends Values> byteChunk =
                alternateColumnSource.getPrevChunk(toBooleanFillContext.alternateGetContext, rowSequence).asByteChunk();
        convertToBoolean(destination, byteChunk);
    }

    private static void convertToBoolean(@NotNull final WritableChunk<? super Values> destination,
            @NotNull final ByteChunk<? extends Values> byteChunk) {
        final WritableObjectChunk<Boolean, ? super Values> booleanObjectDestination =
                destination.asWritableObjectChunk();
        for (int ii = 0; ii < byteChunk.size(); ++ii) {
            booleanObjectDestination.set(ii, BooleanUtils.byteAsBoolean(byteChunk.get(ii)));
        }
        booleanObjectDestination.setSize(byteChunk.size());
    }

    @Override
    public boolean providesFillUnordered() {
        return FillUnordered.providesFillUnordered(alternateColumnSource);
    }

    @Override
    public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        final ToBooleanFillContext toBooleanFillContext = (ToBooleanFillContext) context;
        if (toBooleanFillContext.byteChunk == null) {
            throw new UnsupportedOperationException("Unordered fill is not supported by this column source!");
        }
        toBooleanFillContext.byteChunk.setSize(keys.size());
        // noinspection unchecked
        ((FillUnordered<Values>) alternateColumnSource).fillChunkUnordered(toBooleanFillContext.alternateFillContext,
                toBooleanFillContext.byteChunk, keys);
        convertToBoolean(dest, toBooleanFillContext.byteChunk);
    }

    @Override
    public void fillPrevChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        final ToBooleanFillContext toBooleanFillContext = (ToBooleanFillContext) context;
        if (toBooleanFillContext.byteChunk == null) {
            throw new UnsupportedOperationException("Unordered fill is not supported by this column source!");
        }
        toBooleanFillContext.byteChunk.setSize(keys.size());
        // noinspection unchecked
        ((FillUnordered<Values>) alternateColumnSource).fillPrevChunkUnordered(
                toBooleanFillContext.alternateFillContext, toBooleanFillContext.byteChunk, keys);
        convertToBoolean(dest, toBooleanFillContext.byteChunk);
    }

    @Override
    public boolean isStateless() {
        return alternateColumnSource.isStateless();
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
        // Boolean columns are filtered as object chunks.
        final Supplier<Chunk<Values>> chunkSupplier =
                () -> SingleValuePushdownHelper.makeChunk((Object) (usePrev ? getPrev(0) : get(0)));
        final boolean matches =
                SingleValuePushdownHelper.filter(selection, usePrev, filterCtx, chunkSupplier, this);
        onComplete.accept(matches ? PushdownResult.allMatch(selection) : PushdownResult.noneMatch(selection));
    }
    // endregion Pushdown
}
