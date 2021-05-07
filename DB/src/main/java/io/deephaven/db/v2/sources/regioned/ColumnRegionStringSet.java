package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.tables.libs.StringSet;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.sources.StringSetImpl;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.page.Page;
import io.deephaven.db.v2.sources.regioned.RegionedColumnSourceStringSet.ReversibleCache;
import io.deephaven.db.v2.utils.OrderedKeys;

import org.jetbrains.annotations.NotNull;
import javax.annotation.OverridingMethodsMustInvokeSuper;

public final class ColumnRegionStringSet<ATTR extends Attributes.Any>
        implements ColumnRegionObject<StringSet, ATTR>, Page.WithDefaults<ATTR> {

    private final ColumnRegionLong<Attributes.StringSetBitmasks> bitmaskColumnRegion;
    private final ColumnRegionObjectCached<String, ATTR, ReversibleCache> dictionaryColumnRegion;

    ColumnRegionStringSet(ColumnRegionLong<Attributes.StringSetBitmasks> bitmaskColumnRegion,
                          ColumnRegionObjectCached<String, ATTR, ReversibleCache> dictionaryColumnRegion) {
        this.bitmaskColumnRegion = bitmaskColumnRegion;
        this.dictionaryColumnRegion = dictionaryColumnRegion;
    }

    @Override
    public StringSet getObject(long elementIndex) {
        return new StringSetImpl(dictionaryColumnRegion.getCache(), bitmaskColumnRegion.getLong(elementIndex));
    }

    @Override
    public StringSet getObject(@NotNull ChunkSource.FillContext fillContext, long elementIndex) {
        return new StringSetImpl(dictionaryColumnRegion.getCache(),
                bitmaskColumnRegion.getLong(FillContext.getBitmaskGetContext(fillContext).getFillContext(),
                        elementIndex));
    }

    @Override
    public void fillChunkAppend(@NotNull ChunkSource.FillContext fillContext, @NotNull WritableChunk<? super ATTR> destination, @NotNull OrderedKeys orderedKeys) {
        WritableObjectChunk<StringSet, ? super ATTR> objectDestination = destination.asWritableObjectChunk();

        LongChunk<? extends Attributes.StringSetBitmasks> bitmaskChunk =
                bitmaskColumnRegion.getChunk(FillContext.getBitmaskGetContext(fillContext), orderedKeys).asLongChunk();

        for (int i = 0; i < bitmaskChunk.size(); ++i) {
            objectDestination.add(new StringSetImpl(dictionaryColumnRegion.getCache(), bitmaskChunk.get(i)));
        }
    }

    @Override @NotNull
    public Class<StringSet> getNativeType() {
        return StringSet.class;
    }

    @Override
    public long length() {
        return bitmaskColumnRegion.length();
    }

    @Override @OverridingMethodsMustInvokeSuper
    public void releaseCachedResources() {
        ColumnRegionObject.super.releaseCachedResources();
        bitmaskColumnRegion.releaseCachedResources();
        dictionaryColumnRegion.releaseCachedResources();
    }

    static class FillContext implements ChunkSource.FillContext {

        DefaultGetContext<Attributes.StringSetBitmasks> bitmaskGetContext;

        public FillContext(int keysChunkCapacity) {
            bitmaskGetContext = new DefaultGetContext<>(new ColumnRegionFillContext(), ChunkType.Long, keysChunkCapacity);
        }

        static DefaultGetContext<Attributes.StringSetBitmasks> getBitmaskGetContext(ChunkSource.FillContext context) {
            return ((FillContext) context).bitmaskGetContext;
        }

        @Override @OverridingMethodsMustInvokeSuper
        public void close() {
            bitmaskGetContext.close();
        }
    }
}
