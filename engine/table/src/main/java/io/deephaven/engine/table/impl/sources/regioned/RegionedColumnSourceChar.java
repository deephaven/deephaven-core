//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
 * Regioned column source implementation for columns of chars.
 */
abstract class RegionedColumnSourceChar<ATTR extends Values>
        extends RegionedColumnSourceArray<Character, ATTR, ColumnRegionChar<ATTR>>
        implements ColumnSourceGetDefaults.ForChar /* MIXIN_INTERFACES */ {

    RegionedColumnSourceChar(@NotNull final ColumnRegionChar<ATTR> nullRegion,
            @NotNull final MakeDeferred<ATTR, ColumnRegionChar<ATTR>> makeDeferred) {
        super(nullRegion, char.class, makeDeferred);
    }

    @Override
    public char getChar(final long rowKey) {
        return (rowKey == RowSequence.NULL_ROW_KEY ? getNullRegion() : lookupRegion(rowKey)).getChar(rowKey);
    }

    interface MakeRegionDefault extends MakeRegion<Values, ColumnRegionChar<Values>> {
        @Override
        default ColumnRegionChar<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                @NotNull final ColumnLocation columnLocation,
                final int regionIndex) {
            if (columnLocation.exists()) {
                return columnLocation.makeColumnRegionChar(columnDefinition);
            }
            return null;
        }
    }

    // region reinterpretation
    // endregion reinterpretation

    static final class AsValues extends RegionedColumnSourceChar<Values> implements MakeRegionDefault {
        AsValues() {
            super(ColumnRegionChar.createNull(PARAMETERS.regionMask), DeferredColumnRegionChar::new);
        }
    }

    static final class Partitioning extends RegionedColumnSourceChar<Values> {

        Partitioning() {
            super(ColumnRegionChar.createNull(PARAMETERS.regionMask),
                    (pm, rs) -> rs.get() // No need to interpose a deferred region in this case
            );
        }

        @Override
        public ColumnRegionChar<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                @NotNull final ColumnLocation columnLocation,
                final int regionIndex) {
            final TableLocationKey locationKey = columnLocation.getTableLocation().getKey();
            final Object partitioningColumnValue = locationKey.getPartitionValue(columnDefinition.getName());
            if (partitioningColumnValue != null
                    && !Character.class.isAssignableFrom(partitioningColumnValue.getClass())) {
                throw new TableDataException(
                        "Unexpected partitioning column value type for " + columnDefinition.getName()
                                + ": " + partitioningColumnValue + " is not a Character at location " + locationKey);
            }
            return new ColumnRegionChar.Constant<>(regionMask(), unbox((Character) partitioningColumnValue));
        }
    }
}
