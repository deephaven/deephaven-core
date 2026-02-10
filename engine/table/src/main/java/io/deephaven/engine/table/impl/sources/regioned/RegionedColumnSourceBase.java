//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.*;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocation;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.util.SafeCloseableArray;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * Partial implementation of {@link RegionedColumnSource} for array-backed and delegating implementations to extend.
 */
abstract class RegionedColumnSourceBase<DATA_TYPE, ATTR extends Values, REGION_TYPE extends ColumnRegion<ATTR>>
        extends AbstractColumnSource<DATA_TYPE>
        implements RegionedPageStore<Values, ATTR, REGION_TYPE>, RegionedColumnSource<DATA_TYPE> {

    protected final RegionedColumnSourceManager manager;

    static final Parameters PARAMETERS;
    static {
        PARAMETERS =
                new RegionedPageStore.Parameters(Long.MAX_VALUE, MAXIMUM_REGION_COUNT, REGION_CAPACITY_IN_ELEMENTS);
        Assert.eq(PARAMETERS.regionMask, "parameters.regionMask", ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK,
                "ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK");
        Assert.eq(PARAMETERS.regionMaskNumBits, "parameters.regionMaskNumBits", SUB_REGION_ROW_INDEX_ADDRESS_BITS,
                "SUB_REGION_ELEMENT_INDEX_ADDRESS_BITS");
    }

    RegionedColumnSourceBase(
            @NotNull final RegionedColumnSourceManager manager,
            @NotNull final Class<DATA_TYPE> type,
            @Nullable final Class<?> componentType) {
        super(type, componentType);
        this.manager = manager;
    }

    RegionedColumnSourceBase(
            @NotNull final RegionedColumnSourceManager manager,
            @NotNull final Class<DATA_TYPE> type) {
        this(manager, type, null);
    }

    @Override
    public void invalidateRegion(final int regionIndex) {
        getRegion(regionIndex).invalidate();
    }

    @Override
    public final Parameters parameters() {
        return PARAMETERS;
    }

    /**
     * Use the more efficient fill chunk implementation, rather than the default which uses get().
     */
    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        RegionedPageStore.super.fillChunk(context, destination, rowSequence);
    }

    /**
     * We are immutable, so stick with the efficient fill chunk even when getting prev.
     */
    @Override
    public void fillPrevChunk(@NotNull FillContext context,
            @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        fillChunk(context, destination, rowSequence);
    }

    /**
     * <p>
     * Add a pre-constructed region without going through the abstract factory method.
     * <p>
     * <em>This method is for unit testing purposes only!</em>
     *
     * @param region The region to add
     * @return The index assigned to the added region
     */
    @SuppressWarnings("UnusedReturnValue")
    @TestUseOnly
    abstract <OTHER_REGION_TYPE> int addRegionForUnitTests(final OTHER_REGION_TYPE region);

    /**
     * @return the region which represents null for this column source.
     */
    @NotNull
    abstract REGION_TYPE getNullRegion();

    @Override
    public PushdownPredicateManager pushdownManager() {
        return manager;
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
        final RegionedPushdownFilterContext filterContext = (RegionedPushdownFilterContext) context;
        final List<RegionedColumnSourceManager.IncludedTableLocationEntry> tleList =
                manager.includedLocationEntries();

        // Sample a few regions and return the lowest cost
        final int[] regionIndices = RegionedColumnSourceManager.regionIndices(selection,
                RegionedColumnSourceManager.PUSHDOWN_LOCATION_SAMPLES);

        final AtomicLong minCost = new AtomicLong(Long.MAX_VALUE);

        jobScheduler.iterateParallel(
                ExecutionContext.getContext(),
                (LogOutput output) -> output.append("RegionedColumnSourceBase#estimatePushdownFilterCost"),
                RegionedPushdownHelper.RegionThreadContext::new,
                0,
                regionIndices.length,
                (ctx, idx, nec, resume) -> {
                    ctx.reset();
                    final int regionIndex = regionIndices[idx];

                    // Create a local pushdown context that can provide the table location to the ColumnRegion
                    final RegionedColumnSourceManager.IncludedTableLocationEntry tle = tleList.get(regionIndex);
                    final RegionedPushdownFilterLocationContext newCtx =
                            filterContext.withTableLocation((AbstractTableLocation) tle.location);
                    ctx.shiftedRowSet = tle.subsetAndShiftIntoLocationSpace(selection);
                    getRegion(regionIndex).estimatePushdownFilterCost(
                            filter,
                            ctx.shiftedRowSet,
                            usePrev,
                            newCtx,
                            jobScheduler,
                            regionCost -> {
                                minCost.updateAndGet(old -> Math.min(old, regionCost));
                                resume.run();
                            },
                            nec);
                },
                () -> onComplete.accept(minCost.get()),
                () -> {
                },
                onError);
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
        final RegionedPushdownFilterContext filterContext = (RegionedPushdownFilterContext) context;

        final List<RegionedColumnSourceManager.IncludedTableLocationEntry> tleList =
                manager.includedLocationEntries();

        final int[] regionIndices = RegionedColumnSourceManager.regionIndices(selection, Integer.MAX_VALUE);

        final WritableRowSet[] matches = new WritableRowSet[regionIndices.length];
        final WritableRowSet[] maybeMatches = new WritableRowSet[regionIndices.length];

        jobScheduler.iterateParallel(
                ExecutionContext.getContext(),
                (LogOutput output) -> output.append("RegionedColumnSourceBase#pushdownFilter"),
                RegionedPushdownHelper.RegionThreadContext::new,
                0,
                regionIndices.length,
                (ctx, idx, nec, resume) -> {
                    ctx.reset();
                    final int regionIndex = regionIndices[idx];

                    // Create a local pushdown context that can provide the table location to the ColumnRegion
                    final RegionedColumnSourceManager.IncludedTableLocationEntry tle = tleList.get(regionIndex);
                    final RegionedPushdownFilterLocationContext newCtx =
                            filterContext.withTableLocation((AbstractTableLocation) tle.location);

                    ctx.shiftedRowSet = tle.subsetAndShiftIntoLocationSpace(selection);

                    getRegion(regionIndex).pushdownFilter(
                            filter,
                            ctx.shiftedRowSet,
                            usePrev,
                            newCtx,
                            costCeiling,
                            jobScheduler,
                            result -> {
                                tle.unshiftIntoRegionSpace(result);
                                matches[idx] = result.match();
                                maybeMatches[idx] = result.maybeMatch();
                                resume.run();
                            },
                            nec);
                },
                () -> onComplete.accept(RegionedPushdownHelper.buildResults(matches, maybeMatches, selection)),
                () -> {
                    SafeCloseableArray.close(matches);
                    SafeCloseableArray.close(maybeMatches);
                },
                onError);
    }

    @Override
    public PushdownFilterContext makePushdownFilterContext(
            final WhereFilter filter,
            final List<ColumnSource<?>> filterSources) {
        // Delegate to the manager.
        return manager.makePushdownFilterContext(filter, filterSources);
    }
}
