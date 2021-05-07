/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit RegionedColumnSourceChar and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.locations.TableLocation;
import io.deephaven.db.v2.sources.ColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.chunk.Attributes;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.db.v2.utils.ReadOnlyIndex.NULL_KEY;

/**
 * Regioned column source implementation for columns of shorts.
 */
abstract class RegionedColumnSourceShort<ATTR extends Attributes.Values>
        extends RegionedColumnSourceArray<Short, ATTR, ColumnRegionShort<ATTR>>
        implements ColumnSourceGetDefaults.ForShort {

    RegionedColumnSourceShort(ColumnRegionShort<ATTR> nullRegion) {
        super(nullRegion, short.class, DeferredColumnRegionShort::new);
    }

    @Override
    public short getShort(long elementIndex) {
        return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getShort(elementIndex);
    }

    interface MakeRegionDefault extends MakeRegion<Attributes.Values, ColumnRegionShort<Attributes.Values>> {
        @Override
        default ColumnRegionShort<Attributes.Values> makeRegion(@NotNull ColumnDefinition<?> columnDefinition,
                                                               @NotNull ColumnLocation<?> columnLocation,
                                                               int regionIndex) {
            if (columnLocation.exists()) {
                if (columnLocation.getFormat() == TableLocation.Format.PARQUET) {
                    return new ParquetColumnRegionShort<>(columnLocation.asParquetFormat().getPageStore(columnDefinition));
                }
                throw new IllegalArgumentException("Unsupported column location format " + columnLocation.getFormat() + " in " + columnLocation);
            }

            return null;
        }
    }

    public static final class AsValues extends RegionedColumnSourceShort<Attributes.Values> implements MakeRegionDefault {
        public AsValues() {
            super(ColumnRegionShort.createNull());
        }
    }

    /**
     * These are used by {@link RegionedColumnSourceReferencing} subclass who want a native short type.  This class does
     * <em>not</em> hold an array of regions, but rather derives from {@link RegionedColumnSourceBase}, accessing its
     * regions by looking into the delegate instance's region array.
     */

    @SuppressWarnings("unused")
    static abstract class NativeType<DATA_TYPE, ATTR extends Attributes.Values>
            extends RegionedColumnSourceReferencing.NativeColumnSource<DATA_TYPE, ATTR, Short, ColumnRegionShort<ATTR>>
            implements ColumnSourceGetDefaults.ForShort {

        NativeType(RegionedColumnSourceBase<DATA_TYPE, ATTR, ColumnRegionReferencing<ATTR, ColumnRegionShort<ATTR>>> outerColumnSource) {
            super(Short.class, outerColumnSource);
        }

        @Override
        public short getShort(long elementIndex) {
            return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getShort(elementIndex);
        }

        static final class AsValues<DATA_TYPE> extends NativeType<DATA_TYPE, Attributes.Values> implements MakeRegionDefault {
            AsValues(RegionedColumnSourceBase<DATA_TYPE, Attributes.Values, ColumnRegionReferencing<Attributes.Values, ColumnRegionShort<Attributes.Values>>> outerColumnSource) {
                super(outerColumnSource);
            }
        }
    }

}
