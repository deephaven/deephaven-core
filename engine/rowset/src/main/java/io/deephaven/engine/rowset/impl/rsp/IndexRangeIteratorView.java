//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.impl.rsp.container.SearchRangeIterator;
import io.deephaven.engine.rowset.impl.rsp.container.ContainerUtil;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.uGreaterOrEqual;

public class IndexRangeIteratorView implements SearchRangeIterator {
    private RowSet.RangeIterator it;
    private final long offset;
    /**
     * Exclusive end of the range of keys this view exposes. One past the last key of the block, which for the last
     * block of the key space is one past {@link Long#MAX_VALUE} and so wraps negative; every comparison against it is
     * therefore unsigned.
     */
    private final long rangesEnd;
    private long itStart;
    private long itEnd;
    private int start;
    private int end; // Note RangeIterator uses exclusive ends.
    private int nextStart;
    private int nextEnd; // Note RangeIterator uses exclusive ends.
    private boolean nextValid;
    private boolean noMore;
    private boolean itFinished;

    public IndexRangeIteratorView(final RowSet.RangeIterator it, final long offset, final long rangesEnd) {
        this.it = it;
        this.offset = offset;
        this.rangesEnd = rangesEnd;
        itStart = it.currentRangeStart();
        itEnd = it.currentRangeEnd();
        if (itStart < offset) {
            nextValid = false;
            return;
        }
        noMore = false;
        itFinished = false;
        computeNext();
    }

    private void setTerminated() {
        it = null;
        nextValid = false;
    }

    private void computeNext() {
        if (noMore || uGreaterOrEqual(itStart, rangesEnd)) {
            setTerminated();
            return;
        }
        nextValid = true;
        nextStart = (int) (itStart - offset);
        if (uGreaterOrEqual(itEnd, rangesEnd)) {
            nextEnd = (int) (rangesEnd - offset);
            itStart = rangesEnd;
            it.postpone(itStart);
            noMore = true;
            return;
        }
        nextEnd = (int) (itEnd - offset) + 1;
        if (it.hasNext()) {
            it.next();
            itStart = it.currentRangeStart();
            itEnd = it.currentRangeEnd();
            noMore = false;
            return;
        }
        itFinished = true;
        noMore = true;
    }

    @Override
    public boolean hasNext() {
        return nextValid;
    }

    @Override
    public int start() {
        return start;
    }

    @Override
    public int end() {
        return end;
    }

    @Override
    public void next() {
        start = nextStart;
        end = nextEnd;
        computeNext();
    }

    @Override
    public boolean advance(int v) {
        throw new UnsupportedOperationException("advance is not supported on RangeIteratorView");
    }

    @Override
    public boolean search(final ContainerUtil.TargetComparator comp) {
        throw new UnsupportedOperationException("search is not supported on RangeIteratorView");
    }

    public boolean underlyingIterFinished() {
        return itFinished;
    }
}
