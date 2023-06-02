/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
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

import java.time.Instant;

public class InstantRangeFilter extends LongRangeFilter {

    public InstantRangeFilter(String columnName, Instant val1, Instant val2) {
        super(columnName, DateTimeUtils.epochNanos(val1), DateTimeUtils.epochNanos(val2), true, true);
    }

    public InstantRangeFilter(
            String columnName, Instant val1, Instant val2, boolean lowerInclusive, boolean upperInclusive) {
        super(columnName, DateTimeUtils.epochNanos(val1), DateTimeUtils.epochNanos(val2),
                lowerInclusive, upperInclusive);
    }

    public InstantRangeFilter(
            String columnName, long val1, long val2, boolean lowerInclusive, boolean upperInclusive) {
        super(columnName, val1, val2, lowerInclusive, upperInclusive);
    }

    @Override
    public void init(TableDefinition tableDefinition) {
        if (chunkFilter != null) {
            return;
        }

        final ColumnDefinition<?> def = tableDefinition.getColumn(columnName);
        if (def == null) {
            throw new RuntimeException("Column \"" + columnName + "\" doesn't exist in this table, available columns: "
                    + tableDefinition.getColumnNames());
        }

        final Class<?> colClass = def.getDataType();
        Assert.eq(colClass, "colClass", Instant.class);

        longFilter = super.initChunkFilter();
        chunkFilter = new InstantLongChunkFilterAdapter();
    }

    @Override
    public InstantRangeFilter copy() {
        final InstantRangeFilter copy =
                new InstantRangeFilter(columnName, lower, upper, lowerInclusive, upperInclusive);
        copy.chunkFilter = chunkFilter;
        copy.longFilter = longFilter;
        return copy;
    }

    @Override
    public String toString() {
        return "InstantRangeFilter(" + columnName + " in "
                + (lowerInclusive ? "[" : "(")
                + DateTimeUtils.epochNanosToInstant(lower) + "," + DateTimeUtils.epochNanosToInstant(upper)
                + (upperInclusive ? "]" : ")") + ")";
    }

    @Override
    WritableRowSet binarySearch(RowSet selection, ColumnSource columnSource, boolean usePrev, boolean reverse) {
        if (selection.isEmpty()) {
            return selection.copy();
        }

        // noinspection unchecked
        final ColumnSource<Long> instantColumnSource =
                ReinterpretUtils.instantToLongSource((ColumnSource<Instant>) columnSource);
        return super.binarySearch(selection, instantColumnSource, usePrev, reverse);
    }

    private class InstantLongChunkFilterAdapter implements ChunkFilter {
        @Override
        public void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            try (final WritableLongChunk<Values> writableLongChunk =
                    WritableLongChunk.makeWritableChunk(values.size())) {

                final ObjectChunk<Instant, ? extends Values> objectValues = values.asObjectChunk();
                for (int ii = 0; ii < values.size(); ++ii) {
                    final Instant instant = objectValues.get(ii);
                    writableLongChunk.set(ii, DateTimeUtils.epochNanos(instant));
                }
                writableLongChunk.setSize(values.size());
                longFilter.filter(writableLongChunk, keys, results);
            }
        }
    }
}
