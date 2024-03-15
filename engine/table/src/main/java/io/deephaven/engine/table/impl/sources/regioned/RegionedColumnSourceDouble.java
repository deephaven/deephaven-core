//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit RegionedColumnSourceChar and run "./gradlew replicateRegionsAndRegionedSources" to regenerate
//
// @formatter:off
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
 * Regioned column source implementation for columns of doubles.
 */
abstract class RegionedColumnSourceDouble<ATTR extends Values>
        extends RegionedColumnSourceArray<Double, ATTR, ColumnRegionDouble<ATTR>>
        implements ColumnSourceGetDefaults.ForDouble /* MIXIN_INTERFACES */ {

    RegionedColumnSourceDouble(@NotNull final ColumnRegionDouble<ATTR> nullRegion,
            @NotNull final MakeDeferred<ATTR, ColumnRegionDouble<ATTR>> makeDeferred) {
        super(nullRegion, double.class, makeDeferred);
    }

    @Override
    public double getDouble(final long rowKey) {
        return (rowKey == RowSequence.NULL_ROW_KEY ? getNullRegion() : lookupRegion(rowKey)).getDouble(rowKey);
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

    // region reinterpretation
    // endregion reinterpretation

    static final class AsValues extends RegionedColumnSourceDouble<Values> implements MakeRegionDefault {
        AsValues() {
            super(ColumnRegionDouble.createNull(PARAMETERS.regionMask), DeferredColumnRegionDouble::new);
        }
    }

    static final class Partitioning extends RegionedColumnSourceDouble<Values> {

        Partitioning() {
            super(ColumnRegionDouble.createNull(PARAMETERS.regionMask),
                    (pm, rs) -> rs.get() // No need to interpose a deferred region in this case
            );
        }

        @Override
        public ColumnRegionDouble<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                @NotNull final ColumnLocation columnLocation,
                final int regionIndex) {
            final TableLocationKey locationKey = columnLocation.getTableLocation().getKey();
            final Object partitioningColumnValue = locationKey.getPartitionValue(columnDefinition.getName());
            if (partitioningColumnValue != null
                    && !Double.class.isAssignableFrom(partitioningColumnValue.getClass())) {
                throw new TableDataException(
                        "Unexpected partitioning column value type for " + columnDefinition.getName()
                                + ": " + partitioningColumnValue + " is not a Double at location " + locationKey);
            }
            return new ColumnRegionDouble.Constant<>(regionMask(), unbox((Double) partitioningColumnValue));
        }
    }
}
