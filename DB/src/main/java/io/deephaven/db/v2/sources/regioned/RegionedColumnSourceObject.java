package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.locations.TableLocation;
import io.deephaven.db.v2.sources.ColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.SharedContext;
import io.deephaven.util.codec.ObjectDecoder;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.db.v2.utils.ReadOnlyIndex.NULL_KEY;

abstract class RegionedColumnSourceObject<T, ATTR extends Attributes.Values> extends RegionedColumnSourceArray<T, ATTR, ColumnRegionObject<T, ATTR>>
        implements ColumnSourceGetDefaults.ForObject<T> {

    private RegionedColumnSourceObject(@NotNull ColumnRegionObject<T, ATTR> nullRegion, @NotNull Class<T> type) {
        super(nullRegion, type, DeferredColumnRegionObject::new);
    }

    RegionedColumnSourceObject(@NotNull Class<T> type) {
        this(ColumnRegionObject.createNull(), type);
    }

    public static class AsValues<T> extends RegionedColumnSourceObject<T, Attributes.Values> {
        private final ObjectDecoder<T> decoder;

        public AsValues(Class<T> type, ObjectDecoder<T> decoder) {
            super(type);
            this.decoder = decoder;
        }

        @Override
        public T get(long elementIndex) {
            return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getObject(elementIndex);
        }

        ObjectDecoder<T> getDecoder() {
            return decoder;
        }

        public ColumnRegionObject<T, Attributes.Values> makeRegion(@NotNull ColumnDefinition<?> columnDefinition,
                                                                   @NotNull ColumnLocation<?> columnLocation, int regionIndex) {
            if (columnLocation.exists()) {
                if (columnLocation.getFormat() == TableLocation.Format.PARQUET) {
                    return new ParquetColumnRegionObject<>(columnLocation.asParquetFormat().getPageStore(columnDefinition));
                }
                throw new IllegalArgumentException("Unsupported column location format " + columnLocation.getFormat() + " in " + columnLocation);
            }

            return null;
        }

        @Override
        public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
            int width = decoder.expectedObjectWidth();

            if (width == ObjectDecoder.VARIABLE_WIDTH_SENTINEL) {
                return new ColumnRegionObjectCodecVariable.FillContext(RegionUtilities.INITIAL_DECODER_BUFFER_SIZE, chunkCapacity);
            } else {
                return new ColumnRegionObjectCodecFixed.FillContext(chunkCapacity * width);
            }
        }
    }
}
