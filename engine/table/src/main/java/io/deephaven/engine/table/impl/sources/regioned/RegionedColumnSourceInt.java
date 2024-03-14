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
 * Regioned column source implementation for columns of ints.
 */
abstract class RegionedColumnSourceInt<ATTR extends Values>
        extends RegionedColumnSourceArray<Integer, ATTR, ColumnRegionInt<ATTR>>
        implements ColumnSourceGetDefaults.ForInt /* MIXIN_INTERFACES */ {

    RegionedColumnSourceInt(@NotNull final ColumnRegionInt<ATTR> nullRegion,
            @NotNull final MakeDeferred<ATTR, ColumnRegionInt<ATTR>> makeDeferred) {
        super(nullRegion, int.class, makeDeferred);
    }

    @Override
    public int getInt(final long rowKey) {
        return (rowKey == RowSequence.NULL_ROW_KEY ? getNullRegion() : lookupRegion(rowKey)).getInt(rowKey);
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

    // region reinterpretation
    // endregion reinterpretation

    static final class AsValues extends RegionedColumnSourceInt<Values> implements MakeRegionDefault {
        AsValues() {
            super(ColumnRegionInt.createNull(PARAMETERS.regionMask), DeferredColumnRegionInt::new);
        }
    }

    static final class Partitioning extends RegionedColumnSourceInt<Values> {

        Partitioning() {
            super(ColumnRegionInt.createNull(PARAMETERS.regionMask),
                    (pm, rs) -> rs.get() // No need to interpose a deferred region in this case
            );
        }

        @Override
        public ColumnRegionInt<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                @NotNull final ColumnLocation columnLocation,
                final int regionIndex) {
            final TableLocationKey locationKey = columnLocation.getTableLocation().getKey();
            final Object partitioningColumnValue = locationKey.getPartitionValue(columnDefinition.getName());
            if (partitioningColumnValue != null
                    && !Integer.class.isAssignableFrom(partitioningColumnValue.getClass())) {
                throw new TableDataException(
                        "Unexpected partitioning column value type for " + columnDefinition.getName()
                                + ": " + partitioningColumnValue + " is not a Integer at location " + locationKey);
            }
            return new ColumnRegionInt.Constant<>(regionMask(), unbox((Integer) partitioningColumnValue));
        }
    }
}
