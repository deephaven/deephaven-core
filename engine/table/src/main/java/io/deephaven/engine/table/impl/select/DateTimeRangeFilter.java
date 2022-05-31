package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.time.DateTime;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.QueryConstants;

public class DateTimeRangeFilter extends LongRangeFilter {
    public DateTimeRangeFilter(String columnName, DateTime val1, DateTime val2) {
        super(columnName, val1.getNanos(), val2.getNanos(), true, true);
    }

    public DateTimeRangeFilter(String columnName, DateTime val1, DateTime val2, boolean lowerInclusive,
            boolean upperInclusive) {
        super(columnName, val1.getNanos(), val2.getNanos(), lowerInclusive, upperInclusive);
    }

    public DateTimeRangeFilter(String columnName, long val1, long val2, boolean lowerInclusive,
            boolean upperInclusive) {
        super(columnName, val1, val2, lowerInclusive, upperInclusive);
    }

    @Override
    public void init(TableDefinition tableDefinition) {
        if (chunkFilter != null) {
            return;
        }

        final ColumnDefinition def = tableDefinition.getColumn(columnName);
        if (def == null) {
            throw new RuntimeException("Column \"" + columnName + "\" doesn't exist in this table, available columns: "
                    + tableDefinition.getColumnNames());
        }

        final Class colClass = def.getDataType();
        Assert.eq(colClass, "colClass", DateTime.class);

        longFilter = super.initChunkFilter();
        chunkFilter = new DateTimeLongChunkFilterAdapter();
    }

    @Override
    public DateTimeRangeFilter copy() {
        return new DateTimeRangeFilter(columnName, lower, upper, lowerInclusive, upperInclusive);
    }

    @Override
    public String toString() {
        return "DateTimeRangeFilter(" + columnName + " in " +
                (lowerInclusive ? "[" : "(") + new DateTime(lower) + "," + new DateTime(upper) +
                (upperInclusive ? "]" : ")") + ")";
    }

    @Override
    WritableRowSet binarySearch(RowSet selection, ColumnSource columnSource, boolean usePrev, boolean reverse) {
        if (selection.isEmpty()) {
            return selection.copy();
        }

        // noinspection unchecked
        final ColumnSource<Long> dateTimeColumnSource =
                ReinterpretUtils.dateTimeToLongSource((ColumnSource<DateTime>) columnSource);
        return super.binarySearch(selection, dateTimeColumnSource, usePrev, reverse);
    }

    private class DateTimeLongChunkFilterAdapter implements ChunkFilter {
        @Override
        public void filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            try (final WritableLongChunk<Values> writableLongChunk =
                    WritableLongChunk.makeWritableChunk(values.size())) {

                final ObjectChunk<DateTime, ? extends Values> objectValues = values.asObjectChunk();
                for (int ii = 0; ii < values.size(); ++ii) {
                    final DateTime dateTime = objectValues.get(ii);
                    writableLongChunk.set(ii, dateTime == null ? QueryConstants.NULL_LONG : dateTime.getNanos());
                }
                writableLongChunk.setSize(values.size());
                longFilter.filter(writableLongChunk, keys, results);
            }
        }
    }
}
