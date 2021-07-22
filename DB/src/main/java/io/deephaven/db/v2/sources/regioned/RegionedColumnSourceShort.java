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
 * Regioned column source implementation for columns of shorts.
 */
abstract class RegionedColumnSourceShort<ATTR extends Values>
        extends RegionedColumnSourceArray<Short, ATTR, ColumnRegionShort<ATTR>>
        implements ColumnSourceGetDefaults.ForShort {

    RegionedColumnSourceShort(@NotNull final ColumnRegionShort<ATTR> nullRegion,
                             @NotNull final MakeDeferred<ATTR, ColumnRegionShort<ATTR>> makeDeferred) {
        super(nullRegion, short.class, makeDeferred);
    }

    @Override
    public short getShort(final long elementIndex) {
        return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getShort(elementIndex);
    }

    interface MakeRegionDefault extends MakeRegion<Values, ColumnRegionShort<Values>> {
        @Override
        default ColumnRegionShort<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                                                    @NotNull final ColumnLocation columnLocation,
                                                    final int regionIndex) {
            if (columnLocation.exists()) {
                return columnLocation.makeColumnRegionShort(columnDefinition);
            }
            return null;
        }
    }

    static final class AsValues extends RegionedColumnSourceShort<Values> implements MakeRegionDefault {
        AsValues() {
            super(ColumnRegionShort.createNull(), DeferredColumnRegionShort::new);
        }
    }

    /**
     * These are used by {@link RegionedColumnSourceReferencing} subclass who want a native short type.  This class does
     * <em>not</em> hold an array of regions, but rather derives from {@link RegionedColumnSourceBase}, accessing its
     * regions by looking into the delegate instance's region array.
     */
    @SuppressWarnings("unused")
    static abstract class NativeType<DATA_TYPE, ATTR extends Values>
            extends RegionedColumnSourceReferencing.NativeColumnSource<DATA_TYPE, ATTR, Short, ColumnRegionShort<ATTR>>
            implements ColumnSourceGetDefaults.ForShort {

        NativeType(@NotNull final RegionedColumnSourceBase<DATA_TYPE, ATTR, ColumnRegionReferencing<ATTR, ColumnRegionShort<ATTR>>> outerColumnSource) {
            super(Short.class, outerColumnSource);
        }

        @Override
        public short getShort(final long elementIndex) {
            return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getShort(elementIndex);
        }

        static final class AsValues<DATA_TYPE> extends NativeType<DATA_TYPE, Values> implements MakeRegionDefault {
            AsValues(@NotNull final RegionedColumnSourceBase<DATA_TYPE, Values, ColumnRegionReferencing<Values, ColumnRegionShort<Values>>> outerColumnSource) {
                super(outerColumnSource);
            }
        }
    }

    static final class Partitioning extends RegionedColumnSourceShort<Values> {

        Partitioning() {
            super(ColumnRegionShort.createNull(), Supplier::get);
        }

        @Override
        public ColumnRegionShort<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                                                   @NotNull final ColumnLocation columnLocation,
                                                   final int regionIndex) {
            return new ColumnRegionShort.Constant<>(unbox((Short) columnLocation.getTableLocation().getKey().getPartitionValue(columnDefinition.getName())));
        }
    }
}
