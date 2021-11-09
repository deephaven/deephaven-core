package io.deephaven.engine.v2.sources;

import io.deephaven.engine.tables.utils.DateTime;
import io.deephaven.engine.v2.utils.RowSet;

/**
 * Wrap a regular {@code TreeMapSource<Long>} to make it reinterpretable as a DateTime column source.
 */
public class UnboxedDateTimeTreeMapSource extends UnboxedDateTimeColumnSource implements ColumnSource<Long> {

    // the actual data storage
    private final TreeMapSource<Long> treeMapSource;

    public UnboxedDateTimeTreeMapSource(ColumnSource<DateTime> alternateColumnSource,
            TreeMapSource<Long> treeMapSource) {
        super(alternateColumnSource);
        this.treeMapSource = treeMapSource;
    }

    public void add(RowSet rowSet, Long[] data) {
        treeMapSource.add(rowSet, data);
    }

    public void remove(RowSet rowSet) {
        treeMapSource.remove(rowSet);
    }
}
