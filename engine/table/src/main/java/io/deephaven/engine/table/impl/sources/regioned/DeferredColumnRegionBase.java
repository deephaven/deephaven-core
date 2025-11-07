//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;
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
            }
        }
    }

    @Override
    public final REGION_TYPE getResultRegion() {
        REGION_TYPE localResultRegion;
        if ((localResultRegion = resultRegion) == null) {
            synchronized (this) {
                if ((localResultRegion = resultRegion) == null) {
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
}
