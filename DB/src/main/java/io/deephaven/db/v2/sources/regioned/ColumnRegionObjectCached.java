package io.deephaven.db.v2.sources.regioned;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.page.Page;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.datastructures.cache.OffsetLookup;
import io.deephaven.util.datastructures.cache.OffsetLookupCache;
import org.jetbrains.annotations.NotNull;

/**
 * Column region interface for regions that support fetching objects.
 */
public final class ColumnRegionObjectCached<T, ATTR extends Attributes.Any, OFFSET_LOOKUP_CACHE extends OffsetLookupCache<T, ChunkSource.FillContext>>
        implements ColumnRegionObject<T, ATTR>, OffsetLookup<T, ChunkSource.FillContext>, Page.WithDefaults<ATTR> {

    @FunctionalInterface
    interface CreateCache<T, OFFSET_LOOKUP_CACHE extends OffsetLookupCache<T, FillContext>> {
        OFFSET_LOOKUP_CACHE create(OffsetLookup<T, ChunkSource.FillContext> offsetLookup, FillContextMaker fillContextMaker);
    }

    private final OFFSET_LOOKUP_CACHE cache;
    private final ColumnRegionObject<T, ATTR> columnRegionObject;

    ColumnRegionObjectCached(CreateCache<T, OFFSET_LOOKUP_CACHE> createCache, FillContextMaker fillContextMaker, ColumnRegionObject<T, ATTR> columnRegionObject) {
        this.cache = createCache.create(this, fillContextMaker);
        this.columnRegionObject = columnRegionObject;
    }

    public OFFSET_LOOKUP_CACHE getCache() {
        return cache;
    }

    @Override
    public T getObject(@NotNull FillContext context, long elementIndex) {
        return cache.get((int)getRowOffset(elementIndex), context);
    }

    @Override
    public T getObject(long elementIndex) {
        return cache.get((int)getRowOffset(elementIndex), null);
    }

    @Override
    public long length() {
        return columnRegionObject.length();
    }

    public ColumnRegionObject<T, ATTR> skipCache() {
        return columnRegionObject;
    }

    @Override
    public void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super ATTR> destination, @NotNull OrderedKeys orderedKeys) {
        WritableObjectChunk<T, ? super ATTR> objectDestination = destination.asWritableObjectChunk();
        orderedKeys.forAllLongs((final long key) -> objectDestination.add(getObject(context, key)));
    }

    @Override @OverridingMethodsMustInvokeSuper
    public void releaseCachedResources() {
        ColumnRegionObject.super.releaseCachedResources();
        cache.clear();
        columnRegionObject.releaseCachedResources();
    }

    @Override
    public T lookup(int offset, FillContext extra) {
        return extra == null ? columnRegionObject.getObject(offset) : columnRegionObject.getObject(extra, offset);
    }
}
