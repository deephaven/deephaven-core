package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.sources.ColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.SharedContext;
import io.deephaven.util.codec.ObjectDecoder;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;

import static io.deephaven.db.v2.utils.ReadOnlyIndex.NULL_KEY;

abstract class RegionedColumnSourceObject<DATA_TYPE, ATTR extends Values> extends RegionedColumnSourceArray<DATA_TYPE, ATTR, ColumnRegionObject<DATA_TYPE, ATTR>>
        implements ColumnSourceGetDefaults.ForObject<DATA_TYPE> {

    private RegionedColumnSourceObject(@NotNull final ColumnRegionObject<DATA_TYPE, ATTR> nullRegion,
                                       @NotNull final Class<DATA_TYPE> type,
                                       @Nullable final Class<?> componentType) {
        super(nullRegion, type, componentType, DeferredColumnRegionObject::new);
    }

    RegionedColumnSourceObject(@NotNull final Class<DATA_TYPE> type) {
        this(ColumnRegionObject.createNull(), type, null);
    }

    public static class AsValues<DATA_TYPE> extends RegionedColumnSourceObject<DATA_TYPE, Values> {

        private final ObjectDecoder<DATA_TYPE> decoder;

        public AsValues(@NotNull final Class<DATA_TYPE> type, @NotNull final ObjectDecoder<DATA_TYPE> decoder) {
            this(type, null, decoder);
        }

        public AsValues(@NotNull final Class<DATA_TYPE> type, @Nullable final Class<?> componentType, @NotNull final ObjectDecoder<DATA_TYPE> decoder) {
            super(ColumnRegionObject.createNull(), type, componentType);
            this.decoder = decoder;
        }

        @Override
        public DATA_TYPE get(final long elementIndex) {
            return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getObject(elementIndex);
        }

        public ColumnRegionObject<DATA_TYPE, Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                                                                @NotNull final ColumnLocation<?> columnLocation,
                                                                final int regionIndex) {
            if (columnLocation.exists()) {
                //noinspection unchecked
                return (ColumnRegionObject<DATA_TYPE, Values>) columnLocation.makeColumnRegionObject(columnDefinition);
            }

            return null;
        }

        @Override
        public FillContext makeFillContext(final int chunkCapacity, @Nullable final SharedContext sharedContext) {
            // TODO-RWC: Maybe we should do this per-region?

            final int width = decoder.expectedObjectWidth();

            if (width == ObjectDecoder.VARIABLE_WIDTH_SENTINEL) {
                return new ColumnRegionObjectCodecVariable.FillContext(RegionUtilities.INITIAL_DECODER_BUFFER_SIZE, chunkCapacity);
            } else {
                return new ColumnRegionObjectCodecFixed.FillContext(chunkCapacity * width);
            }
        }
    }
}
