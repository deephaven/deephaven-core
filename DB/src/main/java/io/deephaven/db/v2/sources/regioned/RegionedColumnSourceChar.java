package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.locations.TableLocation;
import io.deephaven.db.v2.sources.ColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.chunk.Attributes;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.db.v2.utils.ReadOnlyIndex.NULL_KEY;

/**
 * Regioned column source implementation for columns of chars.
 */
abstract class RegionedColumnSourceChar<ATTR extends Attributes.Values>
        extends RegionedColumnSourceArray<Character, ATTR, ColumnRegionChar<ATTR>>
        implements ColumnSourceGetDefaults.ForChar {

    RegionedColumnSourceChar(ColumnRegionChar<ATTR> nullRegion) {
        super(nullRegion, char.class, DeferredColumnRegionChar::new);
    }

    @Override
    public char getChar(long elementIndex) {
        return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getChar(elementIndex);
    }

    interface MakeRegionDefault extends MakeRegion<Attributes.Values, ColumnRegionChar<Attributes.Values>> {
        @Override
        default ColumnRegionChar<Attributes.Values> makeRegion(@NotNull ColumnDefinition<?> columnDefinition,
                                                               @NotNull ColumnLocation<?> columnLocation,
                                                               int regionIndex) {
            if (columnLocation.exists()) {
                if (columnLocation.getFormat() == TableLocation.Format.PARQUET) {
                    return new ParquetColumnRegionChar<>(columnLocation.asParquetFormat().getPageStore(columnDefinition));
                }
                throw new IllegalArgumentException("Unsupported column location format " + columnLocation.getFormat() + " in " + columnLocation);
            }

            return null;
        }
    }

    public static final class AsValues extends RegionedColumnSourceChar<Attributes.Values> implements MakeRegionDefault {
        public AsValues() {
            super(ColumnRegionChar.createNull());
        }
    }

    /**
     * These are used by {@link RegionedColumnSourceReferencing} subclass who want a native char type.  This class does
     * <em>not</em> hold an array of regions, but rather derives from {@link RegionedColumnSourceBase}, accessing its
     * regions by looking into the delegate instance's region array.
     */

    @SuppressWarnings("unused")
    static abstract class NativeType<DATA_TYPE, ATTR extends Attributes.Values>
            extends RegionedColumnSourceReferencing.NativeColumnSource<DATA_TYPE, ATTR, Character, ColumnRegionChar<ATTR>>
            implements ColumnSourceGetDefaults.ForChar {

        NativeType(RegionedColumnSourceBase<DATA_TYPE, ATTR, ColumnRegionReferencing<ATTR, ColumnRegionChar<ATTR>>> outerColumnSource) {
            super(Character.class, outerColumnSource);
        }

        @Override
        public char getChar(long elementIndex) {
            return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getChar(elementIndex);
        }

        static final class AsValues<DATA_TYPE> extends NativeType<DATA_TYPE, Attributes.Values> implements MakeRegionDefault {
            AsValues(RegionedColumnSourceBase<DATA_TYPE, Attributes.Values, ColumnRegionReferencing<Attributes.Values, ColumnRegionChar<Attributes.Values>>> outerColumnSource) {
                super(outerColumnSource);
            }
        }
    }

}
