//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit RegionedColumnSourceChar and run "./gradlew replicateRegionsAndRegionedSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.table.ColumnSource;

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
 * Regioned column source implementation for columns of bytes.
 */
abstract class RegionedColumnSourceByte<ATTR extends Values>
        extends RegionedColumnSourceArray<Byte, ATTR, ColumnRegionByte<ATTR>>
        implements ColumnSourceGetDefaults.ForByte /* MIXIN_INTERFACES */ {

    RegionedColumnSourceByte(@NotNull final ColumnRegionByte<ATTR> nullRegion,
            @NotNull final MakeDeferred<ATTR, ColumnRegionByte<ATTR>> makeDeferred) {
        super(nullRegion, byte.class, makeDeferred);
    }

    @Override
    public byte getByte(final long rowKey) {
        return (rowKey == RowSequence.NULL_ROW_KEY ? getNullRegion() : lookupRegion(rowKey)).getByte(rowKey);
    }

    interface MakeRegionDefault extends MakeRegion<Values, ColumnRegionByte<Values>> {
        @Override
        default ColumnRegionByte<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                @NotNull final ColumnLocation columnLocation,
                final int regionIndex) {
            if (columnLocation.exists()) {
                return columnLocation.makeColumnRegionByte(columnDefinition);
            }
            return null;
        }
    }

    // region reinterpretation
    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == boolean.class || alternateDataType == Boolean.class || super.allowsReinterpret(alternateDataType);
    }

    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        //noinspection unchecked
        return (ColumnSource<ALTERNATE_DATA_TYPE>) new RegionedColumnSourceBoolean((RegionedColumnSourceByte<Values>)this);
    }
    // endregion reinterpretation

    static final class AsValues extends RegionedColumnSourceByte<Values> implements MakeRegionDefault {
        AsValues() {
            super(ColumnRegionByte.createNull(PARAMETERS.regionMask), DeferredColumnRegionByte::new);
        }
    }

    static final class Partitioning extends RegionedColumnSourceByte<Values> {

        Partitioning() {
            super(ColumnRegionByte.createNull(PARAMETERS.regionMask),
                    (pm, rs) -> rs.get() // No need to interpose a deferred region in this case
            );
        }

        @Override
        public ColumnRegionByte<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                @NotNull final ColumnLocation columnLocation,
                final int regionIndex) {
            final TableLocationKey locationKey = columnLocation.getTableLocation().getKey();
            final Object partitioningColumnValue = locationKey.getPartitionValue(columnDefinition.getName());
            if (partitioningColumnValue != null
                    && !Byte.class.isAssignableFrom(partitioningColumnValue.getClass())) {
                throw new TableDataException(
                        "Unexpected partitioning column value type for " + columnDefinition.getName()
                                + ": " + partitioningColumnValue + " is not a Byte at location " + locationKey);
            }
            return new ColumnRegionByte.Constant<>(regionMask(), unbox((Byte) partitioningColumnValue));
        }
    }
}
