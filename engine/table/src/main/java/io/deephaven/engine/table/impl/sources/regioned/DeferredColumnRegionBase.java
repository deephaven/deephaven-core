//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;
import org.jetbrains.annotations.MustBeInvokedByOverriders;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

/**
 * Base deferred region implementation.
 */
public abstract class DeferredColumnRegionBase<ATTR extends Any, REGION_TYPE extends ColumnRegion<ATTR>>
        extends GenericColumnRegionBase<ATTR>
        implements DeferredColumnRegion<ATTR, REGION_TYPE> {

    private Supplier<REGION_TYPE> resultRegionFactory;

    private volatile REGION_TYPE resultRegion;

    DeferredColumnRegionBase(final long pageMask, @NotNull final Supplier<REGION_TYPE> resultRegionFactory) {
        super(pageMask);
        this.resultRegionFactory = Require.neqNull(resultRegionFactory, "resultRegionFactory");
    }

    @Override
    public void invalidate() {
        super.invalidate();
        synchronized (this) {
            REGION_TYPE localResultRegion;
            if ((localResultRegion = resultRegion) != null) {
                localResultRegion.invalidate();
            } else {
                // Never materialized: drop the factory and the removed location the factory holds.
                resultRegionFactory = null;
            }
        }
    }

    @Override
    public final REGION_TYPE getResultRegion() {
        REGION_TYPE localResultRegion;
        if ((localResultRegion = resultRegion) == null) {
            synchronized (this) {
                if ((localResultRegion = resultRegion) == null) {
                    // Fail rather than materialize a region for a removed location (and lose the invalidation).
                    throwIfInvalidated();
                    resultRegion =
                            localResultRegion = Require.neqNull(resultRegionFactory.get(), "resultRegionFactory.get()");
                    resultRegionFactory = null;
                }
            }
        }
        return localResultRegion;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void releaseCachedResources() {
        DeferredColumnRegion.super.releaseCachedResources();
        final REGION_TYPE localResultRegion = resultRegion;
        if (localResultRegion != null) {
            localResultRegion.releaseCachedResources();
        }
    }

    @Override
    public ChunkType getChunkType() {
        return getResultRegion().getChunkType();
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super ATTR> destination,
            @NotNull RowSequence rowSequence) {
        getResultRegion().fillChunk(context, destination, rowSequence);
    }

    @Override
    public void fillChunkAppend(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final RowSequence.Iterator rowSequenceIterator) {
        getResultRegion().fillChunkAppend(context, destination, rowSequenceIterator);
    }

    @Override
    public Chunk<? extends ATTR> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        return getResultRegion().getChunk(context, rowSequence);
    }

    @Override
    public Chunk<? extends ATTR> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        return getResultRegion().getChunk(context, firstKey, lastKey);
    }

    @Override
    public Optional<ColumnLocation> getColumnLocation() {
        return getResultRegion().getColumnLocation();
    }

    // The pushdown entry points forward so that ColumnRegion's defaults test action eligibility against the result
    // region rather than this deferred region.

    @Override
    public void estimatePushdownFilterCost(
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final PushdownFilterContext context,
            final JobScheduler jobScheduler,
            final LongConsumer onComplete,
            final Consumer<Exception> onError) {
        getResultRegion().estimatePushdownFilterCost(filter, selection, usePrev, context, jobScheduler, onComplete,
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
        getResultRegion().pushdownFilter(filter, selection, usePrev, context, costCeiling, jobScheduler, onComplete,
                onError);
    }

    @Override
    public List<RegionedPushdownAction> supportedActions() {
        return getResultRegion().supportedActions();
    }

    @Override
    public RegionedPushdownAction.EstimateContext makeEstimateContext(
            final WhereFilter filter,
            final PushdownFilterContext context) {
        return getResultRegion().makeEstimateContext(filter, context);
    }

    @Override
    public long estimatePushdownAction(
            final RegionedPushdownAction action,
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final PushdownFilterContext filterContext,
            final RegionedPushdownAction.EstimateContext estimateContext) {
        return getResultRegion().estimatePushdownAction(action, filter, selection, usePrev, filterContext,
                estimateContext);
    }

    @Override
    public RegionedPushdownAction.ActionContext makeActionContext(
            final WhereFilter filter,
            final PushdownFilterContext context) {
        return getResultRegion().makeActionContext(filter, context);
    }

    @Override
    @MustBeInvokedByOverriders
    public PushdownResult performPushdownAction(
            final RegionedPushdownAction action,
            final WhereFilter filter,
            final RowSet selection,
            final PushdownResult input,
            final boolean usePrev,
            final PushdownFilterContext filterContext,
            final RegionedPushdownAction.ActionContext actionContext) {
        return getResultRegion().performPushdownAction(action, filter, selection, input, usePrev, filterContext,
                actionContext);
    }
}
