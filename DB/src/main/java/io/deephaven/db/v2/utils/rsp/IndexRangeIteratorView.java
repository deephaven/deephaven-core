package io.deephaven.db.v2.utils.rsp;

import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.rsp.container.SearchRangeIterator;
import io.deephaven.db.v2.utils.rsp.container.ContainerUtil;

public class IndexRangeIteratorView implements SearchRangeIterator {
    private Index.RangeIterator it;
    private final long offset;
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

    public IndexRangeIteratorView(final Index.RangeIterator it, final long offset,
        final long rangesEnd) {
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
        if (noMore || itStart >= rangesEnd) {
            setTerminated();
            return;
        }
        nextValid = true;
        nextStart = (int) (itStart - offset);
        if (itEnd >= rangesEnd) {
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
