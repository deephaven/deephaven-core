/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit RegionedColumnSourceChar and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.sources.ColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.chunk.Attributes;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.db.v2.utils.ReadOnlyIndex.NULL_KEY;

/**
 * Regioned column source implementation for columns of bytes.
 */
abstract class RegionedColumnSourceByte<ATTR extends Attributes.Values>
        extends RegionedColumnSourceArray<Byte, ATTR, ColumnRegionByte<ATTR>>
        implements ColumnSourceGetDefaults.ForByte {

    RegionedColumnSourceByte(ColumnRegionByte<ATTR> nullRegion) {
        super(nullRegion, byte.class, DeferredColumnRegionByte::new);
    }

    @Override
    public byte getByte(long elementIndex) {
        return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getByte(elementIndex);
    }

    interface MakeRegionDefault extends MakeRegion<Attributes.Values, ColumnRegionByte<Attributes.Values>> {
        @Override
        default ColumnRegionByte<Attributes.Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                                                               @NotNull final ColumnLocation columnLocation,
                                                               final int regionIndex) {
            if (columnLocation.exists()) {
                return columnLocation.makeColumnRegionByte(columnDefinition);
            }

            return null;
        }
    }

    public static final class AsValues extends RegionedColumnSourceByte<Attributes.Values> implements MakeRegionDefault {
        public AsValues() {
            super(ColumnRegionByte.createNull());
        }
    }

    /**
     * These are used by {@link RegionedColumnSourceReferencing} subclass who want a native byte type.  This class does
     * <em>not</em> hold an array of regions, but rather derives from {@link RegionedColumnSourceBase}, accessing its
     * regions by looking into the delegate instance's region array.
     */

    @SuppressWarnings("unused")
    static abstract class NativeType<DATA_TYPE, ATTR extends Attributes.Values>
            extends RegionedColumnSourceReferencing.NativeColumnSource<DATA_TYPE, ATTR, Byte, ColumnRegionByte<ATTR>>
            implements ColumnSourceGetDefaults.ForByte {

        NativeType(RegionedColumnSourceBase<DATA_TYPE, ATTR, ColumnRegionReferencing<ATTR, ColumnRegionByte<ATTR>>> outerColumnSource) {
            super(Byte.class, outerColumnSource);
        }

        @Override
        public byte getByte(long elementIndex) {
            return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getByte(elementIndex);
        }

        static final class AsValues<DATA_TYPE> extends NativeType<DATA_TYPE, Attributes.Values> implements MakeRegionDefault {
            AsValues(RegionedColumnSourceBase<DATA_TYPE, Attributes.Values, ColumnRegionReferencing<Attributes.Values, ColumnRegionByte<Attributes.Values>>> outerColumnSource) {
                super(outerColumnSource);
            }
        }
    }

}
