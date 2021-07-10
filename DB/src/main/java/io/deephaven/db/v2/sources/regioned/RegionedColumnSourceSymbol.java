package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.locations.TableLocation;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.sources.chunk.FillContextMaker;
import io.deephaven.db.v2.sources.regioned.ColumnRegionObjectCached.CreateCache;
import io.deephaven.util.codec.ObjectDecoder;
import io.deephaven.util.datastructures.cache.ArrayBackedOffsetLookupCache;
import io.deephaven.util.datastructures.cache.OffsetLookup;
import io.deephaven.util.datastructures.cache.OffsetLookupCache;
import io.deephaven.util.datastructures.cache.SoftArrayBackedOffsetLookupCache;
import org.jetbrains.annotations.NotNull;

class RegionedColumnSourceSymbol<T, OFFSET_CACHE extends OffsetLookupCache<T, ChunkSource.FillContext>>
        extends RegionedColumnSourceObject.AsValues<T>  {

    private final CreateCache<T, OFFSET_CACHE> createCache;

    RegionedColumnSourceSymbol(ObjectDecoder<T> decoder, Class<T> dataType, CreateCache<T, OFFSET_CACHE> createCache) {
        super(dataType, decoder);
        this.createCache = createCache;
    }

    public static <T> RegionedColumnSourceSymbol<T, OffsetLookupCache<T, FillContext>>
    createWithLookupCache(ObjectDecoder<T> decoder, Class<T> dataType, boolean useSoft) {
        return new RegionedColumnSourceSymbol<>(decoder, dataType, useSoft ?
                (OffsetLookup<T, FillContext> offsetLookup, FillContextMaker unused) -> new SoftArrayBackedOffsetLookupCache<>(dataType, offsetLookup) :
                (OffsetLookup<T, FillContext> offsetLookup, FillContextMaker unused) -> new ArrayBackedOffsetLookupCache<>(dataType, offsetLookup));
    }

    public static <T> RegionedColumnSourceSymbol<T, ?>
    createWithoutCache(ObjectDecoder<T> decoder, Class<T> dataType) {
        return new RegionedColumnSourceSymbol<>(decoder, dataType, null);
    }

    @Override
    public ColumnRegionObject<T, Attributes.Values>
    makeRegion(@NotNull ColumnDefinition<?> columnDefinition,
               @NotNull ColumnLocation<?> columnLocation,
               int regionIndex) {
        if (columnLocation.exists()) {
            // TODO-RWC: I have no idea what to do here...
            if (columnLocation.getFormat() == TableLocation.Format.PARQUET) {
                Chunk<Attributes.Values> chunk = columnLocation.asParquetFormat().getDictionary(columnDefinition);
                return chunk == null ? null : ParquetColumnRegionSymbolTable.create(type, chunk);
            }
            throw new IllegalArgumentException("Unsupported column location format " + columnLocation.getFormat() + " in " + columnLocation);
        }

        return null;
    }

    @Override
    synchronized <OTHER_REGION_TYPE> int addRegionForUnitTests(OTHER_REGION_TYPE region) {
        //noinspection unchecked
        final ColumnRegionObject<T, Attributes.Values> columnRegionObject = (ColumnRegionObject<T, Attributes.Values>) region;

        return super.addRegionForUnitTests(createCache == null ? columnRegionObject :
                new ColumnRegionObjectCached<>(createCache, this, columnRegionObject));
    }
}
