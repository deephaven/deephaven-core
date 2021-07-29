/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit RegionedColumnSourceChar and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.sources.ColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

import static io.deephaven.db.v2.utils.ReadOnlyIndex.NULL_KEY;
import static io.deephaven.util.type.TypeUtils.unbox;

/**
 * Regioned column source implementation for columns of ints.
 */
abstract class RegionedColumnSourceInt<ATTR extends Values>
        extends RegionedColumnSourceArray<Integer, ATTR, ColumnRegionInt<ATTR>>
        implements ColumnSourceGetDefaults.ForInt {

    RegionedColumnSourceInt(@NotNull final ColumnRegionInt<ATTR> nullRegion,
                             @NotNull final MakeDeferred<ATTR, ColumnRegionInt<ATTR>> makeDeferred) {
        super(nullRegion, int.class, makeDeferred);
    }

    @Override
    public int getInt(final long elementIndex) {
        return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getInt(elementIndex);
    }

    interface MakeRegionDefault extends MakeRegion<Values, ColumnRegionInt<Values>> {
        @Override
        default ColumnRegionInt<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                                                    @NotNull final ColumnLocation columnLocation,
                                                    final int regionIndex) {
            if (columnLocation.exists()) {
                return columnLocation.makeColumnRegionInt(columnDefinition);
            }
            return null;
        }
    }

    static final class AsValues extends RegionedColumnSourceInt<Values> implements MakeRegionDefault {
        AsValues() {
            super(ColumnRegionInt.createNull(), DeferredColumnRegionInt::new);
        }
    }

    /**
     * These are used by {@link RegionedColumnSourceReferencing} subclass who want a native int type.  This class does
     * <em>not</em> hold an array of regions, but rather derives from {@link RegionedColumnSourceBase}, accessing its
     * regions by looking into the delegate instance's region array.
     */
    @SuppressWarnings("unused")
    static abstract class NativeType<DATA_TYPE, ATTR extends Values>
            extends RegionedColumnSourceReferencing.NativeColumnSource<DATA_TYPE, ATTR, Integer, ColumnRegionInt<ATTR>>
            implements ColumnSourceGetDefaults.ForInt {

        NativeType(@NotNull final RegionedColumnSourceBase<DATA_TYPE, ATTR, ColumnRegionReferencing<ATTR, ColumnRegionInt<ATTR>>> outerColumnSource) {
            super(Integer.class, outerColumnSource);
        }

        @Override
        public int getInt(final long elementIndex) {
            return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getInt(elementIndex);
        }

        static final class AsValues<DATA_TYPE> extends NativeType<DATA_TYPE, Values> implements MakeRegionDefault {
            AsValues(@NotNull final RegionedColumnSourceBase<DATA_TYPE, Values, ColumnRegionReferencing<Values, ColumnRegionInt<Values>>> outerColumnSource) {
                super(outerColumnSource);
            }
        }
    }

    static final class Partitioning extends RegionedColumnSourceInt<Values> {

        Partitioning() {
            super(ColumnRegionInt.createNull(),
                    Supplier::get // No need to interpose a deferred region in this case
            );
        }

        @Override
        public ColumnRegionInt<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                                                   @NotNull final ColumnLocation columnLocation,
                                                   final int regionIndex) {
            final TableLocationKey locationKey = columnLocation.getTableLocation().getKey();
            final Object partitioningColumnValue = locationKey.getPartitionValue(columnDefinition.getName());
            if (partitioningColumnValue != null && !Integer.class.isAssignableFrom(partitioningColumnValue.getClass())) {
                throw new TableDataException("Unexpected partitioning column value type for " + columnDefinition.getName()
                        + ": " + partitioningColumnValue + " is not a Integer at location " + locationKey);
            }
            return new ColumnRegionInt.Constant<>(unbox((Integer) partitioningColumnValue));
        }
    }
}
