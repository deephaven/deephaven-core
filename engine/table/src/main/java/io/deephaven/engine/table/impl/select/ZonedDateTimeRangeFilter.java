//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.ZonedDateTime;

public class ZonedDateTimeRangeFilter extends LongRangeFilter {

    public ZonedDateTimeRangeFilter(String columnName, ZonedDateTime val1, ZonedDateTime val2) {
        super(columnName, DateTimeUtils.epochNanos(val1), DateTimeUtils.epochNanos(val2), true, true);
    }

    public ZonedDateTimeRangeFilter(
            String columnName, ZonedDateTime val1, ZonedDateTime val2, boolean lowerInclusive, boolean upperInclusive) {
        super(columnName, DateTimeUtils.epochNanos(val1), DateTimeUtils.epochNanos(val2),
                lowerInclusive, upperInclusive);
    }

    public ZonedDateTimeRangeFilter(
            String columnName, long val1, long val2, boolean lowerInclusive, boolean upperInclusive) {
        super(columnName, val1, val2, lowerInclusive, upperInclusive);
    }

    @Override
    public void init(@NotNull final TableDefinition tableDefinition) {
        if (chunkFilter != null) {
            return;
        }

        final ColumnDefinition<?> def = tableDefinition.getColumn(columnName);
        if (def == null) {
            throw new RuntimeException("Column \"" + columnName + "\" doesn't exist in this table, available columns: "
                    + tableDefinition.getColumnNames());
        }

        final Class<?> colClass = def.getDataType();
        Assert.eq(colClass, "colClass", ZonedDateTime.class);

        longFilter = super.initChunkFilter();
        chunkFilter = new ZonedDateTimeLongChunkFilterAdapter();
    }

    @Override
    public ZonedDateTimeRangeFilter copy() {
        final ZonedDateTimeRangeFilter copy =
                new ZonedDateTimeRangeFilter(columnName, lower, upper, lowerInclusive, upperInclusive);
        copy.chunkFilter = chunkFilter;
        copy.longFilter = longFilter;
        return copy;
    }

    @Override
    public String toString() {
        return "ZonedDateTimeRangeFilter(" + columnName + " in "
                + (lowerInclusive ? "[" : "(")
                + DateTimeUtils.epochNanosToInstant(lower) + "," + DateTimeUtils.epochNanosToInstant(upper)
                + (upperInclusive ? "]" : ")") + ")";
    }

    @NotNull
    @Override
    WritableRowSet binarySearch(
            @NotNull final RowSet selection,
            @NotNull final ColumnSource<?> columnSource,
            final boolean usePrev,
            final boolean reverse) {
        if (selection.isEmpty()) {
            return selection.copy();
        }

        // noinspection unchecked
        final ColumnSource<Long> zdtColumnSource =
                ReinterpretUtils.zonedDateTimeToLongSource((ColumnSource<ZonedDateTime>) columnSource);
        return super.binarySearch(selection, zdtColumnSource, usePrev, reverse);
    }

    private class ZonedDateTimeLongChunkFilterAdapter implements ChunkFilter {
        @Override
        public void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            try (final WritableLongChunk<Values> writableLongChunk =
                    WritableLongChunk.makeWritableChunk(values.size())) {

                final ObjectChunk<ZonedDateTime, ? extends Values> objectValues = values.asObjectChunk();
                for (int ii = 0; ii < values.size(); ++ii) {
                    final ZonedDateTime zdt = objectValues.get(ii);
                    writableLongChunk.set(ii, DateTimeUtils.epochNanos(zdt));
                }
                writableLongChunk.setSize(values.size());
                longFilter.filter(writableLongChunk, keys, results);
            }
        }
    }
}
