package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.sources.chunk.GetContextMaker;
import io.deephaven.db.v2.sources.chunk.SharedContext;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.page.Page;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;

public class ColumnRegionReferencingImpl<ATTR extends Any, REFERENCED_COLUMN_REGION extends ColumnRegion<ATTR>>
        implements ColumnRegionReferencing<ATTR, REFERENCED_COLUMN_REGION>, Page.WithDefaults<ATTR> {

    private final REFERENCED_COLUMN_REGION referencedColumnRegion;

    public ColumnRegionReferencingImpl(@NotNull final REFERENCED_COLUMN_REGION referencedColumnRegion) {
        this.referencedColumnRegion = referencedColumnRegion;
    }

    @NotNull
    @Override
    public REFERENCED_COLUMN_REGION getReferencedRegion() {
        return referencedColumnRegion;
    }

    @Override
    public long mask() {
        return getReferencedRegion().mask();
    }

    @Override
    public void fillChunkAppend(@NotNull ChunkSource.FillContext context,
            @NotNull WritableChunk<? super ATTR> destination, @NotNull OrderedKeys orderedKeys) {
        FillContext.<ATTR>converter(context).convertRegion(destination,
                referencedColumnRegion.getChunk(FillContext.nativeGetContext(context), orderedKeys), orderedKeys);
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void releaseCachedResources() {
        ColumnRegionReferencing.super.releaseCachedResources();
        referencedColumnRegion.releaseCachedResources();
    }

    static class FillContext<ATTR extends Any> implements ChunkSource.FillContext {
        private final ChunkSource.GetContext nativeGetContext;
        private final Converter<ATTR> converter;

        FillContext(GetContextMaker getContextMaker, Converter<ATTR> converter, int chunkCapacity,
                SharedContext sharedContext) {
            this.converter = converter;
            this.nativeGetContext = getContextMaker.makeGetContext(chunkCapacity, sharedContext);
        }

        static ChunkSource.GetContext nativeGetContext(ChunkSource.FillContext context) {
            return ((FillContext<?>) context).nativeGetContext;
        }

        static <ATTR extends Any> Converter<ATTR> converter(ChunkSource.FillContext context) {
            // noinspection unchecked
            return ((FillContext<ATTR>) context).converter;
        }

        @Override
        public void close() {
            nativeGetContext.close();
        }
    }
}
