package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSet;

/**
 * An iterator for the complement set over an universe of [0, Long.MAX_VALUE].
 */
public class ComplementRangeIterator implements RowSet.RangeIterator {
    private final RowSet.RangeIterator it;
    private long currStart;
    private long currEnd;
    private long nextStart;
    private long nextEnd;

    public ComplementRangeIterator(RowSet.RangeIterator it) {
        this.it = it;
        currStart = -1;
        currEnd = -1;
        if (!it.hasNext()) {
            nextStart = 0;
            nextEnd = Long.MAX_VALUE;
            return;
        }
        it.next();
        nextEnd = it.currentRangeStart() - 1;
        if (nextEnd == -1) {
            if (it.currentRangeEnd() == Long.MAX_VALUE) {
                return;
            }
            nextStart = it.currentRangeEnd() + 1;
            if (it.hasNext()) {
                it.next();
                nextEnd = it.currentRangeStart() - 1;
            } else {
                nextEnd = Long.MAX_VALUE;
            }
            return;
        }
        nextStart = 0;
    }

    @Override
    public void close() {
        it.close();
    }

    @Override
    public boolean hasNext() {
        return nextEnd != -1 && currEnd != Long.MAX_VALUE;
    }

    @Override
    public boolean advance(final long v) {
        if (currStart == -1) {
            if (!hasNext()) {
                return false;
            }
            next();
        }
        if (v <= currStart) {
            return true;
        }
        if (v <= currEnd) {
            currStart = v;
            return true;
        }
        if (!hasNext()) {
            return false;
        }
        if (v <= nextEnd) {
            next();
            if (currStart < v) {
                currStart = v;
            }
            return true;
        }
        final boolean valid = it.advance(v);
        if (!valid) {
            currStart = v;
            currEnd = Long.MAX_VALUE;
            nextEnd = -1;
            return true;
        }
        if (v < it.currentRangeStart()) {
            currStart = v;
            currEnd = it.currentRangeStart() - 1;
            if (!it.hasNext()) {
                if (it.currentRangeEnd() == Long.MAX_VALUE) {
                    nextEnd = -1;
                    return true;
                }
                nextStart = it.currentRangeEnd() + 1;
                nextEnd = Long.MAX_VALUE;
                return true;
            }
            nextStart = it.currentRangeEnd() + 1;
            it.next();
            nextEnd = it.currentRangeStart() - 1;
            return true;
        }
        if (!it.hasNext()) {
            if (it.currentRangeEnd() == Long.MAX_VALUE) {
                nextEnd = -1;
                return false;
            }
            currStart = it.currentRangeEnd() + 1;
            currEnd = Long.MAX_VALUE;
            nextEnd = -1;
            return true;
        }
        currStart = it.currentRangeEnd() + 1;
        it.next();
        currEnd = it.currentRangeStart() - 1;
        if (!it.hasNext()) {
            if (it.currentRangeEnd() == Long.MAX_VALUE) {
                nextEnd = -1;
                return true;
            }
            nextStart = it.currentRangeEnd() + 1;
            nextEnd = Long.MAX_VALUE;
            return true;
        }
        nextStart = it.currentRangeEnd() + 1;
        it.next();
        nextEnd = it.currentRangeStart() - 1;
        return true;
    }

    @Override
    public void postpone(final long v) {
        currStart = v;
    }

    @Override
    public long currentRangeStart() {
        return currStart;
    }

    @Override
    public long currentRangeEnd() {
        return currEnd;
    }

    @Override
    public long next() {
        currStart = nextStart;
        currEnd = nextEnd;
        if (it.currentRangeEnd() == Long.MAX_VALUE) {
            nextEnd = -1;
            return currStart;
        }
        nextStart = it.currentRangeEnd() + 1;
        if (it.hasNext()) {
            it.next();
            nextEnd = it.currentRangeStart() - 1;
        } else {
            nextEnd = Long.MAX_VALUE;
        }
        return currStart;
    }
}
