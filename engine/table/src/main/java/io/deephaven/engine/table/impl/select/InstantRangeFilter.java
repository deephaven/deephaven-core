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
        final ColumnSource<Long> instantColumnSource =
                ReinterpretUtils.instantToLongSource((ColumnSource<Instant>) columnSource);
        return super.binarySearch(selection, instantColumnSource, usePrev, reverse);
    }

    private class InstantLongChunkFilterAdapter implements ChunkFilter {
        /**
         * Convert an Instant chunk to a Long chunk.
         */
        private WritableLongChunk<Values> convert(final Chunk<? extends Values> values) {
            final ObjectChunk<Instant, ? extends Values> objectValues = values.asObjectChunk();
            final WritableLongChunk<Values> outputChunk = WritableLongChunk.makeWritableChunk(objectValues.size());
            for (int ii = 0; ii < values.size(); ++ii) {
                final Instant instant = objectValues.get(ii);
                outputChunk.set(ii, DateTimeUtils.epochNanos(instant));
            }
            outputChunk.setSize(values.size());
            return outputChunk;
        }

        @Override
        public void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            try (final WritableLongChunk<Values> convertedChunk = convert(values)) {
                longFilter.filter(convertedChunk, keys, results);
            }
        }

        @Override
        public int filter(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            try (final WritableLongChunk<Values> convertedChunk = convert(values)) {
                return longFilter.filter(convertedChunk, results);
            }
        }

        @Override
        public int filterAnd(
                final Chunk<? extends Values> values,
                final WritableBooleanChunk<Values> results) {
            try (final WritableLongChunk<Values> convertedChunk = convert(values)) {
                return longFilter.filterAnd(convertedChunk, results);
            }
        }
    }
}
