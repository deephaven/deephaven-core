package io.deephaven.engine.v2.sources;

import io.deephaven.engine.tables.utils.DBDateTime;
import io.deephaven.engine.v2.utils.Index;

/**
 * Wrap a regular {@code TreeMapSource<Long>} to make it reinterpretable as a DBDateTime column source.
 */
public class UnboxedDateTimeTreeMapSource extends UnboxedDateTimeColumnSource implements ColumnSource<Long> {

    // the actual data storage
    private final TreeMapSource<Long> treeMapSource;

    public UnboxedDateTimeTreeMapSource(ColumnSource<DBDateTime> alternateColumnSource,
            TreeMapSource<Long> treeMapSource) {
        super(alternateColumnSource);
        this.treeMapSource = treeMapSource;
    }

    public void add(Index index, Long[] data) {
        treeMapSource.add(index, data);
    }

    public void remove(Index index) {
        treeMapSource.remove(index);
    }
}
