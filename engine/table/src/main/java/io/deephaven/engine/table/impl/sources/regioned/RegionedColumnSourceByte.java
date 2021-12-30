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
 * Regioned column source implementation for columns of bytes.
 */
abstract class RegionedColumnSourceByte<ATTR extends Values>
        extends RegionedColumnSourceArray<Byte, ATTR, ColumnRegionByte<ATTR>>
        implements ColumnSourceGetDefaults.ForByte {

    RegionedColumnSourceByte(@NotNull final ColumnRegionByte<ATTR> nullRegion,
                             @NotNull final MakeDeferred<ATTR, ColumnRegionByte<ATTR>> makeDeferred) {
        super(nullRegion, byte.class, makeDeferred);
    }

    @Override
    public byte getByte(final long elementIndex) {
        return (elementIndex == RowSequence.NULL_ROW_KEY ? getNullRegion() : lookupRegion(elementIndex)).getByte(elementIndex);
    }

    interface MakeRegionDefault extends MakeRegion<Values, ColumnRegionByte<Values>> {
        @Override
        default ColumnRegionByte<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                                                    @NotNull final ColumnLocation columnLocation,
                                                    final int regionIndex) {
            if (columnLocation.exists()) {
                return columnLocation.makeColumnRegionByte(columnDefinition);
            }
            return null;
        }
    }

    static final class AsValues extends RegionedColumnSourceByte<Values> implements MakeRegionDefault {
        AsValues() {
            super(ColumnRegionByte.createNull(PARAMETERS.regionMask), DeferredColumnRegionByte::new);
        }
    }

    /**
     * These are used by {@link RegionedColumnSourceReferencing} subclass who want a native byte type.  This class does
     * <em>not</em> hold an array of regions, but rather derives from {@link RegionedColumnSourceBase}, accessing its
     * regions by looking into the delegate instance's region array.
     */
    @SuppressWarnings("unused")
    static abstract class NativeType<DATA_TYPE, ATTR extends Values>
            extends RegionedColumnSourceReferencing.NativeColumnSource<DATA_TYPE, ATTR, Byte, ColumnRegionByte<ATTR>>
            implements ColumnSourceGetDefaults.ForByte {

        NativeType(@NotNull final RegionedColumnSourceBase<DATA_TYPE, ATTR, ColumnRegionReferencing<ATTR, ColumnRegionByte<ATTR>>> outerColumnSource) {
            super(Byte.class, outerColumnSource);
        }

        @Override
        public byte getByte(final long elementIndex) {
            return (elementIndex == RowSequence.NULL_ROW_KEY ? getNullRegion() : lookupRegion(elementIndex)).getByte(elementIndex);
        }

        static final class AsValues<DATA_TYPE> extends NativeType<DATA_TYPE, Values> implements MakeRegionDefault {
            AsValues(@NotNull final RegionedColumnSourceBase<DATA_TYPE, Values, ColumnRegionReferencing<Values, ColumnRegionByte<Values>>> outerColumnSource) {
                super(outerColumnSource);
            }
        }
    }

    static final class Partitioning extends RegionedColumnSourceByte<Values> {

        Partitioning() {
            super(ColumnRegionByte.createNull(PARAMETERS.regionMask),
                    (pm, rs) -> rs.get() // No need to interpose a deferred region in this case
            );
        }

        @Override
        public ColumnRegionByte<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                                                   @NotNull final ColumnLocation columnLocation,
                                                   final int regionIndex) {
            final TableLocationKey locationKey = columnLocation.getTableLocation().getKey();
            final Object partitioningColumnValue = locationKey.getPartitionValue(columnDefinition.getName());
            if (partitioningColumnValue != null && !Byte.class.isAssignableFrom(partitioningColumnValue.getClass())) {
                throw new TableDataException("Unexpected partitioning column value type for " + columnDefinition.getName()
                        + ": " + partitioningColumnValue + " is not a Byte at location " + locationKey);
            }
            return new ColumnRegionByte.Constant<>(regionMask(), unbox((Byte) partitioningColumnValue));
        }
    }
}
