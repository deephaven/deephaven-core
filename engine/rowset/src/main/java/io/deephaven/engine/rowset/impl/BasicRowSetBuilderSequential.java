package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetBuilderSequential;

/**
 * {@link RowSetBuilderRandom} implementation that uses an {@link OrderedLongSetBuilderSequential} internally.
 */
public class BasicRowSetBuilderSequential extends OrderedLongSetBuilderSequential implements RowSetBuilderSequential {

    @Override
    public WritableRowSet build() {
        return new WritableRowSetImpl(getTreeIndexImpl());
    }

    @Override
    public void appendRowSequence(final RowSequence rowSequence) {
        appendRowSequenceWithOffset(rowSequence, 0);
    }

    @Override
    public void appendRowSequenceWithOffset(final RowSequence rowSequence, final long shiftAmount) {
        if (rowSequence instanceof WritableRowSetImpl) {
            appendTreeIndexImpl(shiftAmount, ((WritableRowSetImpl) rowSequence).getInnerSet(), false);
            return;
        }
        rowSequence.forAllRowKeyRanges((start, end) -> {
            appendRange(start + shiftAmount, end + shiftAmount);
        });
    }
}
