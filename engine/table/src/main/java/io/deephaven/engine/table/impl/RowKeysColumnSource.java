package io.deephaven.engine.table.impl;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.TrackingRowSet;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

public class RowKeysColumnSource extends AbstractColumnSource<Long> implements MutableColumnSourceGetDefaults.ForLong {

    private final TrackingRowSet rowSet;

    public RowKeysColumnSource(final TrackingRowSet rowSet) {
        super(Long.class);
        this.rowSet = rowSet;
    }

    @Override
    public long getLong(long rowKey) {
        return rowSet.get(rowKey);
    }

    @Override
    public long getPrevLong(long rowKey) {
        return rowSet.getPrev(rowKey);
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        final WritableLongChunk<? super Values> longChunk = destination.asWritableLongChunk();
        longChunk.setSize(0);

        final MutableLong position = new MutableLong();
        final RowSequence.Iterator rsit = rowSet.getRowSequenceIterator();
        rowSequence.forAllRowKeys(rowKey -> {
            long numToAdvance = rowKey - position.longValue();
            if (numToAdvance > 0) {
                rsit.getNextRowSequenceWithLength(numToAdvance);
                position.add(numToAdvance);
            }
            longChunk.add(rsit.peekNextKey());
        });
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        final WritableLongChunk<? super Values> longChunk = destination.asWritableLongChunk();
        longChunk.setSize(0);

        final MutableLong position = new MutableLong();
        final RowSequence.Iterator rsit = rowSet.prev().getRowSequenceIterator();
        rowSequence.forAllRowKeys(rowKey -> {
            long numToAdvance = rowKey - position.longValue();
            if (numToAdvance > 0) {
                rsit.getNextRowSequenceWithLength(numToAdvance);
                position.add(numToAdvance);
            }
            longChunk.add(rsit.peekNextKey());
        });
    }
}
