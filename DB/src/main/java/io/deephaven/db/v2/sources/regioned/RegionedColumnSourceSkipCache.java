package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.ColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.SharedContext;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.db.v2.utils.ReadableIndex.NULL_KEY;

class RegionedColumnSourceSkipCache<T> extends RegionedColumnSourceInner<T, Attributes.Values, ColumnRegionObject<T, Attributes.Values>, T, ColumnRegionObject<T, Attributes.Values>>
    implements ColumnSourceGetDefaults.ForObject<T> {

    RegionedColumnSourceSkipCache(@NotNull Class<T> type,
                                  RegionedColumnSourceBase<T, Attributes.Values, ColumnRegionObject<T, Attributes.Values>> outerColumnSource) {
        super(type, outerColumnSource);
    }

    @Override
    public T get(long elementIndex) {
        return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getObject(elementIndex);
    }

    @NotNull
    @Override
    ColumnRegionObject<T, Attributes.Values> getNullRegion() {
        return getOuterColumnSource().getNullRegion().skipCache();
    }

    @Override
    public ColumnRegionObject<T, Attributes.Values> getRegion(int regionIndex) {
        return getOuterColumnSource().getRegion(regionIndex).skipCache();
    }

    @Override
    public GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext) {
        return getOuterColumnSource().makeGetContext(chunkCapacity, sharedContext);
    }

    @Override
    public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
        return getOuterColumnSource().makeFillContext(chunkCapacity, sharedContext);
    }
}
