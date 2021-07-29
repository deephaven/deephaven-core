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
 * Regioned column source implementation for columns of doubles.
 */
abstract class RegionedColumnSourceDouble<ATTR extends Values>
        extends RegionedColumnSourceArray<Double, ATTR, ColumnRegionDouble<ATTR>>
        implements ColumnSourceGetDefaults.ForDouble {

    RegionedColumnSourceDouble(@NotNull final ColumnRegionDouble<ATTR> nullRegion,
                             @NotNull final MakeDeferred<ATTR, ColumnRegionDouble<ATTR>> makeDeferred) {
        super(nullRegion, double.class, makeDeferred);
    }

    @Override
    public double getDouble(final long elementIndex) {
        return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getDouble(elementIndex);
    }

    interface MakeRegionDefault extends MakeRegion<Values, ColumnRegionDouble<Values>> {
        @Override
        default ColumnRegionDouble<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                                                    @NotNull final ColumnLocation columnLocation,
                                                    final int regionIndex) {
            if (columnLocation.exists()) {
                return columnLocation.makeColumnRegionDouble(columnDefinition);
            }
            return null;
        }
    }

    static final class AsValues extends RegionedColumnSourceDouble<Values> implements MakeRegionDefault {
        AsValues() {
            super(ColumnRegionDouble.createNull(), DeferredColumnRegionDouble::new);
        }
    }

    /**
     * These are used by {@link RegionedColumnSourceReferencing} subclass who want a native double type.  This class does
     * <em>not</em> hold an array of regions, but rather derives from {@link RegionedColumnSourceBase}, accessing its
     * regions by looking into the delegate instance's region array.
     */
    @SuppressWarnings("unused")
    static abstract class NativeType<DATA_TYPE, ATTR extends Values>
            extends RegionedColumnSourceReferencing.NativeColumnSource<DATA_TYPE, ATTR, Double, ColumnRegionDouble<ATTR>>
            implements ColumnSourceGetDefaults.ForDouble {

        NativeType(@NotNull final RegionedColumnSourceBase<DATA_TYPE, ATTR, ColumnRegionReferencing<ATTR, ColumnRegionDouble<ATTR>>> outerColumnSource) {
            super(Double.class, outerColumnSource);
        }

        @Override
        public double getDouble(final long elementIndex) {
            return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getDouble(elementIndex);
        }

        static final class AsValues<DATA_TYPE> extends NativeType<DATA_TYPE, Values> implements MakeRegionDefault {
            AsValues(@NotNull final RegionedColumnSourceBase<DATA_TYPE, Values, ColumnRegionReferencing<Values, ColumnRegionDouble<Values>>> outerColumnSource) {
                super(outerColumnSource);
            }
        }
    }

    static final class Partitioning extends RegionedColumnSourceDouble<Values> {

        Partitioning() {
            super(ColumnRegionDouble.createNull(),
                    Supplier::get // No need to interpose a deferred region in this case
            );
        }

        @Override
        public ColumnRegionDouble<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                                                   @NotNull final ColumnLocation columnLocation,
                                                   final int regionIndex) {
            final TableLocationKey locationKey = columnLocation.getTableLocation().getKey();
            final Object partitioningColumnValue = locationKey.getPartitionValue(columnDefinition.getName());
            if (partitioningColumnValue != null && !Double.class.isAssignableFrom(partitioningColumnValue.getClass())) {
                throw new TableDataException("Unexpected partitioning column value type for " + columnDefinition.getName()
                        + ": " + partitioningColumnValue + " is not a Double at location " + locationKey);
            }
            return new ColumnRegionDouble.Constant<>(unbox((Double) partitioningColumnValue));
        }
    }
}
