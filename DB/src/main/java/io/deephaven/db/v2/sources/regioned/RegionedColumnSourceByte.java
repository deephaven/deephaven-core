/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit RegionedColumnSourceChar and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.sources.ColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

import static io.deephaven.db.v2.utils.ReadOnlyIndex.NULL_KEY;
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
        return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getByte(elementIndex);
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
            super(ColumnRegionByte.createNull(), DeferredColumnRegionByte::new);
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
            return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getByte(elementIndex);
        }

        static final class AsValues<DATA_TYPE> extends NativeType<DATA_TYPE, Values> implements MakeRegionDefault {
            AsValues(@NotNull final RegionedColumnSourceBase<DATA_TYPE, Values, ColumnRegionReferencing<Values, ColumnRegionByte<Values>>> outerColumnSource) {
                super(outerColumnSource);
            }
        }
    }

    static final class Partitioning extends RegionedColumnSourceByte<Values> {

        Partitioning() {
            super(ColumnRegionByte.createNull(), Supplier::get);
        }

        @Override
        public ColumnRegionByte<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                                                   @NotNull final ColumnLocation columnLocation,
                                                   final int regionIndex) {
            return new ColumnRegionByte.Constant<>(unbox((Byte) columnLocation.getTableLocation().getKey().getPartitionValue(columnDefinition.getName())));
        }
    }
}
