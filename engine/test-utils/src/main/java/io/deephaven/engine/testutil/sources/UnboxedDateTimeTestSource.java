package io.deephaven.engine.testutil.sources;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.UnboxedLongBackedColumnSource;
import io.deephaven.time.DateTime;

/**
 * Wrap a regular {@link TestColumnSource<Long>} to make it reinterpretable as a DateTime column source.
 */
public class UnboxedDateTimeTestSource extends UnboxedLongBackedColumnSource<DateTime>
        implements TestColumnSource<Long> {

    // the actual data storage
    private final TestColumnSource<Long> longTestSource;

    public UnboxedDateTimeTestSource(ColumnSource<DateTime> alternateColumnSource,
            TestColumnSource<Long> testColumnSource) {
        super(alternateColumnSource);
        this.longTestSource = testColumnSource;
    }

    @Override
    public void add(RowSet rowSet, Chunk<Values> data) {
        longTestSource.add(rowSet, data);
    }

    @Override
    public void remove(RowSet rowSet) {
        longTestSource.remove(rowSet);
    }

    @Override
    public void shift(long startKeyInclusive, long endKeyInclusive, long shiftDelta) {
        longTestSource.shift(startKeyInclusive, endKeyInclusive, shiftDelta);
    }
}
