package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.libs.StringSet;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.StringSetImpl;
import io.deephaven.util.datastructures.cache.ArrayBackedOffsetLookupCache;
import io.deephaven.util.datastructures.cache.OffsetLookup;
import io.deephaven.util.datastructures.cache.ReverseOffsetLookupCache;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.db.v2.utils.ReadOnlyIndex.NULL_KEY;

class RegionedColumnSourceStringSet extends RegionedColumnSourceObject<StringSet, Attributes.Values> {

    RegionedColumnSourceStringSet() {
        super(StringSet.class);
    }

    @Override
    public StringSet get(long elementIndex) {
        return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getObject(elementIndex);
    }

    @Override
    public ColumnRegionObject<StringSet, Attributes.Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                                                                       @NotNull final ColumnLocation columnLocation,
                                                                       final int regionIndex) {
        if (columnLocation.exists()) {
            //noinspection unchecked
            return (ColumnRegionObject<StringSet, Attributes.Values>) columnLocation.makeColumnRegionObject(columnDefinition);
        }

        return null;
    }

    @Override
    public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
        return new ColumnRegionStringSet.FillContext(chunkCapacity);
    }

    static class ReversibleCache extends ArrayBackedOffsetLookupCache<String, ChunkSource.FillContext>
            implements StringSetImpl.ReversibleLookup<String> {

        ReverseOffsetLookupCache<String, FillContext> reverseOffsetLookupCache;
        FillContextMaker fillContextMaker;

        ReversibleCache(@NotNull OffsetLookup<String, FillContext> lookupFunction,
                        FillContextMaker fillContextMaker) {
            super(String.class, lookupFunction);
            reverseOffsetLookupCache = new ReverseOffsetLookupCache<>(this::get);
            this.fillContextMaker = fillContextMaker;
        }

        @Override
        public String get(long index) {
            return get(Math.toIntExact(index), null);
        }

        @Override
        public int rget(int highestIndex, String value) {
            reverseOffsetLookupCache.ensurePopulated(highestIndex, () -> fillContextMaker.makeFillContext(1), Context::close);
            return reverseOffsetLookupCache.applyAsInt(value);
        }
    }
}
