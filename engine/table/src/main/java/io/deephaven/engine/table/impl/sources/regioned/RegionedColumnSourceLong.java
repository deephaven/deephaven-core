//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit RegionedColumnSourceChar and run "./gradlew replicateRegionsAndRegionedSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.regioned;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.LongAsLocalDateColumnSource;
import io.deephaven.engine.table.impl.sources.LongAsLocalTimeColumnSource;
import io.deephaven.engine.table.impl.sources.ConvertibleTimeSource;

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
 * Regioned column source implementation for columns of longs.
 */
abstract class RegionedColumnSourceLong<ATTR extends Values>
        extends RegionedColumnSourceArray<Long, ATTR, ColumnRegionLong<ATTR>>
        implements ColumnSourceGetDefaults.ForLong , ConvertibleTimeSource {

    RegionedColumnSourceLong(@NotNull final ColumnRegionLong<ATTR> nullRegion,
            @NotNull final MakeDeferred<ATTR, ColumnRegionLong<ATTR>> makeDeferred) {
        super(nullRegion, long.class, makeDeferred);
    }

    @Override
    public long getLong(final long rowKey) {
        return (rowKey == RowSequence.NULL_ROW_KEY ? getNullRegion() : lookupRegion(rowKey)).getLong(rowKey);
    }

    interface MakeRegionDefault extends MakeRegion<Values, ColumnRegionLong<Values>> {
        @Override
        default ColumnRegionLong<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                @NotNull final ColumnLocation columnLocation,
                final int regionIndex) {
            if (columnLocation.exists()) {
                return columnLocation.makeColumnRegionLong(columnDefinition);
            }
            return null;
        }
    }

    // region reinterpretation
    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        if(super.allowsReinterpret(alternateDataType)) {
            return true;
        }

        return alternateDataType == Instant.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        if(alternateDataType == Instant.class) {
            return (ColumnSource<ALTERNATE_DATA_TYPE>) toInstant();
        }

        return super.doReinterpret(alternateDataType);
    }

    @Override
    public boolean supportsTimeConversion() {
        return true;
    }

    public ColumnSource<Instant> toInstant() {
        //noinspection unchecked
        return new RegionedColumnSourceInstant((RegionedColumnSourceLong<Values>) this);
    }

    @Override
    public ColumnSource<ZonedDateTime> toZonedDateTime(ZoneId zone) {
        //noinspection unchecked
        return new RegionedColumnSourceZonedDateTime(zone, (RegionedColumnSourceLong<Values>) this);
    }

    @Override
    public ColumnSource<LocalTime> toLocalTime(ZoneId zone) {
        return new LongAsLocalTimeColumnSource(this, zone);
    }

    @Override
    public ColumnSource<LocalDate> toLocalDate(ZoneId zone) {
        return new LongAsLocalDateColumnSource(this, zone);
    }

    @Override
    public ColumnSource<Long> toEpochNano() {
        return this;
    }
    // endregion reinterpretation

    static final class AsValues extends RegionedColumnSourceLong<Values> implements MakeRegionDefault {
        AsValues() {
            super(ColumnRegionLong.createNull(PARAMETERS.regionMask), DeferredColumnRegionLong::new);
        }
    }

    static final class Partitioning extends RegionedColumnSourceLong<Values> {

        Partitioning() {
            super(ColumnRegionLong.createNull(PARAMETERS.regionMask),
                    (pm, rs) -> rs.get() // No need to interpose a deferred region in this case
            );
        }

        @Override
        public ColumnRegionLong<Values> makeRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                @NotNull final ColumnLocation columnLocation,
                final int regionIndex) {
            final TableLocationKey locationKey = columnLocation.getTableLocation().getKey();
            final Object partitioningColumnValue = locationKey.getPartitionValue(columnDefinition.getName());
            if (partitioningColumnValue != null
                    && !Long.class.isAssignableFrom(partitioningColumnValue.getClass())) {
                throw new TableDataException(
                        "Unexpected partitioning column value type for " + columnDefinition.getName()
                                + ": " + partitioningColumnValue + " is not a Long at location " + locationKey);
            }
            return new ColumnRegionLong.Constant<>(regionMask(), unbox((Long) partitioningColumnValue));
        }
    }
}
