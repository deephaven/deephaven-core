//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.api.filter.Filter;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.FilterContext;
import io.deephaven.engine.table.impl.PushdownPredicateManager;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.function.Consumer;

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

    /**
     * Estimate the cost of pushing down a filter. This returns a unitless value that can be used to compare the cost of
     * executing different filters.
     *
     * @param filter The {@link Filter filter} to test
     * @param selection The set of rows to tests
     * @param fullSet The full set of rows
     * @param usePrev Whether to use the previous result
     * @param context The {@link FillContext} to use for the pushdown operation
     * @return The estimated cost of the push down operation
     */
    public long estimatePushdownFilterCost(
            final WhereFilter filter,
            final RowSet selection,
            final RowSet fullSet,
            final boolean usePrev,
            final FilterContext context) {
        return manager.estimatePushdownFilterCost(filter, selection, fullSet, usePrev, context);
    }

    /**
     * Push down the given filter to the underlying table and return the result.
     *
     * @param filter The {@link Filter filter} to apply
     * @param input The set of rows to test
     * @param fullSet The full set of rows
     * @param usePrev Whether to use the previous result
     * @param context The {@link FillContext} to use for the pushdown operation
     * @param costCeiling Execute all possible filters with a cost leq this value.
     * @param jobScheduler The job scheduler to use for scheduling child jobs
     * @param onComplete Consumer of the output rowsets for added and modified rows that pass the filter
     * @param onError Consumer of any exceptions that occur during the pushdown operation
     */
    public void pushdownFilter(
            final WhereFilter filter,
            final RowSet input,
            final RowSet fullSet,
            final boolean usePrev,
            final FilterContext context,
            final long costCeiling,
            final JobScheduler jobScheduler,
            final Consumer<PushdownResult> onComplete,
            final Consumer<Exception> onError) {
        // Get the column name for this column source from the manager.
        final String columnName = manager.getColumnSources().entrySet()
                .stream()
                .filter(entry -> entry.getValue() == this)
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("The column source cannot be found in the RegionedColumnSourceManager."));
        // Delegate the pushdown operation to the manager.
        Map<String, ColumnSource<?>> columnSourceMap = Map.of(columnName, this);
        manager.pushdownFilter(columnSourceMap, filter, input, fullSet, usePrev, context, costCeiling, jobScheduler, onComplete, onError);
    }

    /**
     * Get the pushdown predicate manager for this column source; returns null if there is no pushdown manager.
     */
    public PushdownPredicateManager pushdownManager() {
        return manager;
    }
}
