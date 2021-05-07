package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.page.Page;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;

public class ColumnRegionReferencingImpl<ATTR extends Attributes.Any, REFERENCED_COLUMN_REGION extends ColumnRegion<ATTR>>
        implements ColumnRegionReferencing<ATTR, REFERENCED_COLUMN_REGION>, Page.WithDefaults<ATTR> {

    private final Class<?> nativeType;
    private final REFERENCED_COLUMN_REGION referencedColumnRegion;


    public ColumnRegionReferencingImpl(@NotNull Class<?> nativeType,
                                       @NotNull REFERENCED_COLUMN_REGION referencedColumnRegion) {
        this.nativeType = nativeType;
        this.referencedColumnRegion = referencedColumnRegion;
    }

    @NotNull
    @Override
    public REFERENCED_COLUMN_REGION getReferencedRegion() {
        return referencedColumnRegion;
    }

    @Override
    public Class<?> getNativeType() {
        return nativeType;
    }

    @Override
    public long length() {
        return referencedColumnRegion.length();
    }

    @Override
    public void fillChunkAppend(@NotNull ChunkSource.FillContext context, @NotNull WritableChunk<? super ATTR> destination, @NotNull OrderedKeys orderedKeys) {
        FillContext.<ATTR>converter(context).convertRegion(destination, referencedColumnRegion.getChunk(FillContext.nativeGetContext(context), orderedKeys), orderedKeys);
    }

    @Override @OverridingMethodsMustInvokeSuper
    public void releaseCachedResources() {
        ColumnRegionReferencing.super.releaseCachedResources();
        referencedColumnRegion.releaseCachedResources();
    }

    static class FillContext<ATTR extends Attributes.Any> implements ChunkSource.FillContext {
        private final ChunkSource.GetContext nativeGetContext;
        private final Converter<ATTR> converter;

        FillContext(GetContextMaker getContextMaker, Converter<ATTR> converter, int chunkCapacity, SharedContext sharedContext) {
            this.converter = converter;
            this.nativeGetContext = getContextMaker.makeGetContext(chunkCapacity, sharedContext);
        }

        static ChunkSource.GetContext nativeGetContext(ChunkSource.FillContext context) {
            return ((FillContext<?>) context).nativeGetContext;
        }

        static <ATTR extends Attributes.Any> Converter<ATTR> converter(ChunkSource.FillContext context) {
            //noinspection unchecked
            return ((FillContext<ATTR>)context).converter;
        }

        @Override
        public void close() {
            nativeGetContext.close();
        }
    }
}
