package io.deephaven.db.v2.sources.regioned;

import io.deephaven.base.verify.Require;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.function.Supplier;

/**
 * Base deferred region implementation.
 */
public abstract class DeferredColumnRegionBase<ATTR extends Attributes.Any, REGION_TYPE extends ColumnRegion<ATTR>>
        extends GenericColumnRegionBase<ATTR>
        implements DeferredColumnRegion<ATTR, REGION_TYPE> {

    private Supplier<REGION_TYPE> resultRegionFactory;

    private volatile REGION_TYPE resultRegion;

    DeferredColumnRegionBase(final long pageMask, @NotNull final Supplier<REGION_TYPE> resultRegionFactory) {
        super(pageMask);
        this.resultRegionFactory = Require.neqNull(resultRegionFactory, "resultRegionFactory");
    }

    @Override
    public final REGION_TYPE getResultRegion() {
        if (resultRegion == null) {
            synchronized (this) {
                if (resultRegion == null) {
                    resultRegion = Require.neqNull(resultRegionFactory.get(), "resultRegionFactory.get()");
                    resultRegionFactory = null;
                }
            }
        }
        return resultRegion;
    }

    /**
     * Get the result region if it has already been supplied (because of a call to {@link #getResultRegion()}).
     *
     * @return The result region
     */
    private REGION_TYPE getResultRegionIfSupplied() {
        return resultRegion;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void releaseCachedResources() {
        DeferredColumnRegion.super.releaseCachedResources();
        final REGION_TYPE localResultRegion = getResultRegionIfSupplied();
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
            @NotNull OrderedKeys orderedKeys) {
        getResultRegion().fillChunk(context, destination, orderedKeys);
    }

    @Override
    public void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super ATTR> destination,
            @NotNull OrderedKeys.Iterator orderedKeysIterator) {
        getResultRegion().fillChunkAppend(context, destination, orderedKeysIterator);
    }

    @Override
    public Chunk<? extends ATTR> getChunk(@NotNull GetContext context, @NotNull OrderedKeys orderedKeys) {
        return getResultRegion().getChunk(context, orderedKeys);
    }

    @Override
    public Chunk<? extends ATTR> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        return getResultRegion().getChunk(context, firstKey, lastKey);
    }

    @Override
    public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
        return getResultRegion().makeFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext) {
        return getResultRegion().makeGetContext(chunkCapacity, sharedContext);
    }
}
