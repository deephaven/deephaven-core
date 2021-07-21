/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit RegionedColumnSourceChar and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.sources.ColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.db.v2.utils.ReadOnlyIndex.NULL_KEY;

/**
 * Regioned column source implementation for columns of floats.
 */
abstract class RegionedColumnSourceFloat<ATTR extends Values>
        extends RegionedColumnSourceArray<Float, ATTR, ColumnRegionFloat<ATTR>>
        implements ColumnSourceGetDefaults.ForFloat {

    RegionedColumnSourceFloat(ColumnRegionFloat<ATTR> nullRegion) {
        super(nullRegion, float.class, DeferredColumnRegionFloat::new);
    }

    @Override
    public float getFloat(final long elementIndex) {
        return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getFloat(elementIndex);
    }

    interface MakeRegionDefault extends MakeRegion<Values, ColumnRegionFloat<Values>> {
        @Override
        default ColumnRegionFloat<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                                                    @NotNull final ColumnLocation columnLocation,
                                                    final int regionIndex) {
            if (columnLocation.exists()) {
                return columnLocation.makeColumnRegionFloat(columnDefinition);
            }
            return null;
        }
    }

    public static final class AsValues extends RegionedColumnSourceFloat<Values> implements MakeRegionDefault {
        public AsValues() {
            super(ColumnRegionFloat.createNull());
        }
    }

    /**
     * These are used by {@link RegionedColumnSourceReferencing} subclass who want a native float type.  This class does
     * <em>not</em> hold an array of regions, but rather derives from {@link RegionedColumnSourceBase}, accessing its
     * regions by looking into the delegate instance's region array.
     */
    @SuppressWarnings("unused")
    static abstract class NativeType<DATA_TYPE, ATTR extends Values>
            extends RegionedColumnSourceReferencing.NativeColumnSource<DATA_TYPE, ATTR, Float, ColumnRegionFloat<ATTR>>
            implements ColumnSourceGetDefaults.ForFloat {

        NativeType(@NotNull final RegionedColumnSourceBase<DATA_TYPE, ATTR, ColumnRegionReferencing<ATTR, ColumnRegionFloat<ATTR>>> outerColumnSource) {
            super(Float.class, outerColumnSource);
        }

        @Override
        public float getFloat(final long elementIndex) {
            return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getFloat(elementIndex);
        }

        static final class AsValues<DATA_TYPE> extends NativeType<DATA_TYPE, Values> implements MakeRegionDefault {
            AsValues(@NotNull final RegionedColumnSourceBase<DATA_TYPE, Values, ColumnRegionReferencing<Values, ColumnRegionFloat<Values>>> outerColumnSource) {
                super(outerColumnSource);
            }
        }
    }
}
