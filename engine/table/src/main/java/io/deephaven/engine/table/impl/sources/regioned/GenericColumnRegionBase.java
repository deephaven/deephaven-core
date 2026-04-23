//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.locations.InvalidatedRegionException;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;

import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * Base {@link ColumnRegion} implementation.
 */
public abstract class GenericColumnRegionBase<ATTR extends Any> implements ColumnRegion<ATTR> {

    private final long pageMask;
    private volatile boolean invalidated = false;

    public GenericColumnRegionBase(final long pageMask) {
        this.pageMask = pageMask;
    }

    @Override
    public final long mask() {
        return pageMask;
    }

    @Override
    public void invalidate() {
        this.invalidated = true;
    }

    protected final void throwIfInvalidated() {
        if (invalidated) {
            throw new InvalidatedRegionException("Column region has been invalidated due to data removal");
        }
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
        final RegionedPushdownFilterContext filterCtx = (RegionedPushdownFilterContext) context;
        onComplete.accept(
                ColumnRegionPushdownHelper.estimatePushdownFilterCost(this, filter, selection, usePrev, filterCtx));
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
        final RegionedPushdownFilterContext filterCtx = (RegionedPushdownFilterContext) context;
        onComplete.accept(
                ColumnRegionPushdownHelper.pushdownFilter(this, filter, selection, usePrev, filterCtx, costCeiling));
    }
}
