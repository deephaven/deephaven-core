package io.deephaven.engine.v2.utils;

import io.deephaven.engine.structures.RowSequence;

/**
 * {@link RowSetBuilderRandom} implementation that uses an {@link TreeIndexImplBuilderSequential} internally.
 */
class BasicRowSetBuilderSequential extends TreeIndexImplBuilderSequential implements RowSetBuilderSequential {

    @Override
    public MutableRowSet build() {
        return new MutableRowSetImpl(getTreeIndexImpl());
    }

    @Override
    public void appendRowSequence(final RowSequence rowSequence) {
        appendRowSequenceWithOffset(rowSequence, 0);
    }

    @Override
    public void appendRowSequenceWithOffset(final RowSequence rowSequence, final long shiftAmount) {
        if (rowSequence instanceof MutableRowSetImpl) {
            appendTreeIndexImpl(shiftAmount, ((MutableRowSetImpl) rowSequence).getImpl(), false);
            return;
        }
        rowSequence.forAllLongRanges((start, end) -> {
            appendRange(start + shiftAmount, end + shiftAmount);
        });
    }
}
