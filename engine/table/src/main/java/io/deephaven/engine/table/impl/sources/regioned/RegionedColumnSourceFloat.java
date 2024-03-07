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
 * Regioned column source implementation for columns of floats.
 */
abstract class RegionedColumnSourceFloat<ATTR extends Values>
        extends RegionedColumnSourceArray<Float, ATTR, ColumnRegionFloat<ATTR>>
        implements ColumnSourceGetDefaults.ForFloat /* MIXIN_INTERFACES */ {

    RegionedColumnSourceFloat(@NotNull final ColumnRegionFloat<ATTR> nullRegion,
            @NotNull final MakeDeferred<ATTR, ColumnRegionFloat<ATTR>> makeDeferred) {
        super(nullRegion, float.class, makeDeferred);
    }

    @Override
    public float getFloat(final long rowKey) {
        return (rowKey == RowSequence.NULL_ROW_KEY ? getNullRegion() : lookupRegion(rowKey)).getFloat(rowKey);
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

    // region reinterpretation
    // endregion reinterpretation

    static final class AsValues extends RegionedColumnSourceFloat<Values> implements MakeRegionDefault {
        AsValues() {
            super(ColumnRegionFloat.createNull(PARAMETERS.regionMask), DeferredColumnRegionFloat::new);
        }
    }

    static final class Partitioning extends RegionedColumnSourceFloat<Values> {

        Partitioning() {
            super(ColumnRegionFloat.createNull(PARAMETERS.regionMask),
                    (pm, rs) -> rs.get() // No need to interpose a deferred region in this case
            );
        }

        @Override
        public ColumnRegionFloat<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                @NotNull final ColumnLocation columnLocation,
                final int regionIndex) {
            final TableLocationKey locationKey = columnLocation.getTableLocation().getKey();
            final Object partitioningColumnValue = locationKey.getPartitionValue(columnDefinition.getName());
            if (partitioningColumnValue != null
                    && !Float.class.isAssignableFrom(partitioningColumnValue.getClass())) {
                throw new TableDataException(
                        "Unexpected partitioning column value type for " + columnDefinition.getName()
                                + ": " + partitioningColumnValue + " is not a Float at location " + locationKey);
            }
            return new ColumnRegionFloat.Constant<>(regionMask(), unbox((Float) partitioningColumnValue));
        }
    }
}
