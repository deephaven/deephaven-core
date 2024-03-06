//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

/**
 * This is a column source that uses no additional memory and maps from row key to row position in the supplied row set.
 */
public class RowPositionColumnSource extends AbstractColumnSource<Long>
        implements MutableColumnSourceGetDefaults.ForLong {

    private final TrackingRowSet rowSet;

    public RowPositionColumnSource(final TrackingRowSet rowSet) {
        super(Long.class);
        this.rowSet = rowSet;
    }

    @Override
    public long getLong(long rowKey) {
        return rowKey < 0 ? QueryConstants.NULL_LONG : rowSet.find(rowKey);
    }

    @Override
    public long getPrevLong(long rowKey) {
        return rowKey < 0 ? QueryConstants.NULL_LONG : rowSet.findPrev(rowKey);
    }

    @Override
    public void fillChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        try (final RowSet result = rowSet.invert(rowSequence.asRowSet())) {
            RowKeyColumnSource.doFillChunk(destination, result);
        }
    }

    @Override
    public void fillPrevChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        try (final RowSet result = rowSet.prev().invert(rowSequence.asRowSet())) {
            RowKeyColumnSource.doFillChunk(destination, result);
        }
    }
}
