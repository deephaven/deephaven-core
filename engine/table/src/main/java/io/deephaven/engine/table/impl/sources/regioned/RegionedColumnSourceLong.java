/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit RegionedColumnSourceChar and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.table.impl.ColumnSourceGetDefaults;
import io.deephaven.chunk.attributes.Values;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.type.TypeUtils.unbox;

/**
 * Regioned column source implementation for columns of longs.
 */
abstract class RegionedColumnSourceLong<ATTR extends Values>
        extends RegionedColumnSourceArray<Long, ATTR, ColumnRegionLong<ATTR>>
        implements ColumnSourceGetDefaults.ForLong {

    RegionedColumnSourceLong(@NotNull final ColumnRegionLong<ATTR> nullRegion,
                             @NotNull final MakeDeferred<ATTR, ColumnRegionLong<ATTR>> makeDeferred) {
        super(nullRegion, long.class, makeDeferred);
    }

    @Override
    public long getLong(final long rowKey) {
        return (rowKey == RowSequence.NULL_ROW_KEY ? getNullRegion() : lookupRegion(rowKey)).getLong(rowKey);
    }

    interface MakeRegionDefault extends MakeRegion<Values, ColumnRegionLong<Values>> {
        @Override
        default ColumnRegionLong<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                                                    @NotNull final ColumnLocation columnLocation,
                                                    final int regionIndex) {
            if (columnLocation.exists()) {
                return columnLocation.makeColumnRegionLong(columnDefinition);
            }
            return null;
        }
    }

    static final class AsValues extends RegionedColumnSourceLong<Values> implements MakeRegionDefault {
        AsValues() {
            super(ColumnRegionLong.createNull(PARAMETERS.regionMask), DeferredColumnRegionLong::new);
        }
    }

    /**
     * These are used by {@link RegionedColumnSourceReferencing} subclass who want a native long type.  This class does
     * <em>not</em> hold an array of regions, but rather derives from {@link RegionedColumnSourceBase}, accessing its
     * regions by looking into the delegate instance's region array.
     */
    @SuppressWarnings("unused")
    static abstract class NativeType<DATA_TYPE, ATTR extends Values>
            extends RegionedColumnSourceReferencing.NativeColumnSource<DATA_TYPE, ATTR, Long, ColumnRegionLong<ATTR>>
            implements ColumnSourceGetDefaults.ForLong {

        NativeType(@NotNull final RegionedColumnSourceBase<DATA_TYPE, ATTR, ColumnRegionReferencing<ATTR, ColumnRegionLong<ATTR>>> outerColumnSource) {
            super(Long.class, outerColumnSource);
        }

        @Override
        public long getLong(final long rowKey) {
            return (rowKey == RowSequence.NULL_ROW_KEY ? getNullRegion() : lookupRegion(rowKey)).getLong(rowKey);
        }

        static final class AsValues<DATA_TYPE> extends NativeType<DATA_TYPE, Values> implements MakeRegionDefault {
            AsValues(@NotNull final RegionedColumnSourceBase<DATA_TYPE, Values, ColumnRegionReferencing<Values, ColumnRegionLong<Values>>> outerColumnSource) {
                super(outerColumnSource);
            }
        }
    }

    static final class Partitioning extends RegionedColumnSourceLong<Values> {

        Partitioning() {
            super(ColumnRegionLong.createNull(PARAMETERS.regionMask),
                    (pm, rs) -> rs.get() // No need to interpose a deferred region in this case
            );
        }

        @Override
        public ColumnRegionLong<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                                                   @NotNull final ColumnLocation columnLocation,
                                                   final int regionIndex) {
            final TableLocationKey locationKey = columnLocation.getTableLocation().getKey();
            final Object partitioningColumnValue = locationKey.getPartitionValue(columnDefinition.getName());
            if (partitioningColumnValue != null && !Long.class.isAssignableFrom(partitioningColumnValue.getClass())) {
                throw new TableDataException("Unexpected partitioning column value type for " + columnDefinition.getName()
                        + ": " + partitioningColumnValue + " is not a Long at location " + locationKey);
            }
            return new ColumnRegionLong.Constant<>(regionMask(), unbox((Long) partitioningColumnValue));
        }
    }
}
