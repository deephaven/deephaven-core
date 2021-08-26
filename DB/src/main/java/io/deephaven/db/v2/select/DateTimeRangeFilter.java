package io.deephaven.db.v2.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ReinterpretUtilities;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.util.QueryConstants;

public class DateTimeRangeFilter extends LongRangeFilter {
    public DateTimeRangeFilter(String columnName, DBDateTime val1, DBDateTime val2) {
        super(columnName, val1.getNanos(), val2.getNanos(), true, true);
    }

    public DateTimeRangeFilter(String columnName, DBDateTime val1, DBDateTime val2,
        boolean lowerInclusive, boolean upperInclusive) {
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
            throw new RuntimeException(
                "Column \"" + columnName + "\" doesn't exist in this table, available columns: "
                    + tableDefinition.getColumnNames());
        }

        final Class colClass = def.getDataType();
        Assert.eq(colClass, "colClass", DBDateTime.class);

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
            (lowerInclusive ? "[" : "(") + new DBDateTime(lower) + "," + new DBDateTime(upper) +
            (upperInclusive ? "]" : ")") + ")";
    }

    @Override
    Index binarySearch(Index selection, ColumnSource columnSource, boolean usePrev,
        boolean reverse) {
        if (selection.isEmpty()) {
            return selection;
        }

        // noinspection unchecked
        final ColumnSource<Long> dateTimeColumnSource =
            ReinterpretUtilities.dateTimeToLongSource((ColumnSource<DBDateTime>) columnSource);
        return super.binarySearch(selection, dateTimeColumnSource, usePrev, reverse);
    }

    private class DateTimeLongChunkFilterAdapter implements ChunkFilter {
        @Override
        public void filter(Chunk<? extends Values> values, LongChunk<OrderedKeyIndices> keys,
            WritableLongChunk<OrderedKeyIndices> results) {
            try (final WritableLongChunk<Values> writableLongChunk =
                WritableLongChunk.makeWritableChunk(values.size())) {

                final ObjectChunk<DBDateTime, ? extends Values> objectValues =
                    values.asObjectChunk();
                for (int ii = 0; ii < values.size(); ++ii) {
                    final DBDateTime dbDateTime = objectValues.get(ii);
                    writableLongChunk.set(ii,
                        dbDateTime == null ? QueryConstants.NULL_LONG : dbDateTime.getNanos());
                }
                writableLongChunk.setSize(values.size());
                longFilter.filter(writableLongChunk, keys, results);
            }
        }
    }
}
