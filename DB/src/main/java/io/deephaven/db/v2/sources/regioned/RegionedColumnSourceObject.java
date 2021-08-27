package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.sources.ColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.db.v2.utils.ReadOnlyIndex.NULL_KEY;

abstract class RegionedColumnSourceObject<DATA_TYPE, ATTR extends Values>
        extends RegionedColumnSourceArray<DATA_TYPE, ATTR, ColumnRegionObject<DATA_TYPE, ATTR>>
        implements ColumnSourceGetDefaults.ForObject<DATA_TYPE> {

    private RegionedColumnSourceObject(@NotNull final ColumnRegionObject<DATA_TYPE, ATTR> nullRegion,
                                       @NotNull final Class<DATA_TYPE> dataType,
                                       @Nullable final Class<?> componentType,
                                       @NotNull final MakeDeferred<ATTR, ColumnRegionObject<DATA_TYPE, ATTR>> makeDeferred) {
        super(nullRegion, dataType, componentType, makeDeferred);
    }

    @Override
    public final DATA_TYPE get(final long elementIndex) {
        return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getObject(elementIndex);
    }

    public static class AsValues<DATA_TYPE> extends RegionedColumnSourceObject<DATA_TYPE, Values> {

        public AsValues(@NotNull final Class<DATA_TYPE> dataType) {
            this(dataType, null);
        }

        public AsValues(@NotNull final Class<DATA_TYPE> dataType, @Nullable final Class<?> componentType) {
            super(ColumnRegionObject.createNull(PARAMETERS.regionMask), dataType, componentType, DeferredColumnRegionObject::new);
        }

        public ColumnRegionObject<DATA_TYPE, Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                                                                @NotNull final ColumnLocation columnLocation,
                                                                final int regionIndex) {
            if (columnLocation.exists()) {
                //noinspection unchecked
                return (ColumnRegionObject<DATA_TYPE, Values>) columnLocation.makeColumnRegionObject(columnDefinition);
            }
            return null;
        }
    }

    static final class Partitioning<DATA_TYPE> extends RegionedColumnSourceObject<DATA_TYPE, Values> {

        Partitioning(@NotNull final Class<DATA_TYPE> dataType) {
            super(ColumnRegionObject.createNull(PARAMETERS.regionMask), dataType, null,
                    (pm, rs) -> rs.get() // No need to interpose a deferred region in this case
            );
        }

        @Override
        public ColumnRegionObject<DATA_TYPE, Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                                                                @NotNull final ColumnLocation columnLocation,
                                                                final int regionIndex) {
            final TableLocationKey locationKey = columnLocation.getTableLocation().getKey();
            final Object partitioningColumnValue = locationKey.getPartitionValue(columnDefinition.getName());
            if (partitioningColumnValue != null && !getType().isAssignableFrom(partitioningColumnValue.getClass())) {
                throw new TableDataException("Unexpected partitioning column value type for " + columnDefinition.getName()
                        + ": " + partitioningColumnValue + " is not a " + getType() + " at location " + locationKey);
            }
            //noinspection unchecked
            return new ColumnRegionObject.Constant<>(PARAMETERS.regionMask, (DATA_TYPE) partitioningColumnValue);
        }
    }
}
