package io.deephaven.db.v2.utils.rsp;

import io.deephaven.db.v2.utils.LongRangeAbortableConsumer;
import io.deephaven.db.v2.utils.LongRangeIterator;
import io.deephaven.db.v2.utils.IndexUtilities;
import io.deephaven.db.v2.utils.rsp.container.SingletonContainer;
import io.deephaven.util.SafeCloseable;
import io.deephaven.db.v2.utils.rsp.container.Container;
import io.deephaven.db.v2.utils.rsp.container.SearchRangeIterator;
import io.deephaven.db.v2.utils.rsp.container.ContainerUtil;

import static io.deephaven.db.v2.utils.rsp.RspArray.*;
import static io.deephaven.db.v2.utils.IndexUtilities.Comparator;

public class RspRangeIterator implements LongRangeIterator, SafeCloseable {
    private RspArray.SpanCursorForward p;
    // Iterator pointing to the next value to deliver in the current RB Container if there is one, null otherwise.
    private SearchRangeIterator ri;
    // Current start and end values.
    private long start;
    private long end;  // inclusive.
    private boolean nextValid;  // True if there is an additional range after the current one.

    public RspRangeIterator(final RspArray.SpanCursorForward p) {
        start = 0;
        end = -1;
        if (!p.hasNext()) {
            p.release();
            this.p = null;
            nextValid = false;
            return;
        }
        this.p = p;
        nextValid = true;
        p.next();
    }

    private void setFinished() {
        if (p != null) {
            p.release();
            p = null;
        }
        nextValid = false;
    }

    /**
     * There is a lot of complexity here because we may need to merge adjacent ranges belonging to different,
     * consecutive blocks.
     */
    private void nextInterval() {
        // if hasPrev is true, we have accumulated in [start, end] a range that we can't deliver yet,
        // as end corresponds exactly with the last element in a block interval, which may need to be merged
        // with the next range.
        boolean hasPrev = false;
        long sk = p.spanKey();
        long sbk = highBits(sk);
        while (true) {
            if (ri != null) {
                final long kris = sbk | (long) ri.start();
                final int rie = ri.end() - 1;
                final long krie = sbk | (long) rie;
                if (hasPrev) {
                    if (ri.start() != 0) {
                        return;
                    }
                    end = krie;
                } else {
                    start = kris;
                    end = krie;
                }
                if (ri.hasNext()) {
                    ri.next();
                    return;
                }
                ri = null;
                if (!p.hasNext()) {
                    setFinished();
                    return;
                }
                p.next();
                if (rie != BLOCK_LAST) {
                    return;
                }
                // we need to check for a potential merge with the next range.
                hasPrev = true;
                sk = p.spanKey();
                sbk = highBits(sk);
            }
            if (hasPrev && sbk - end != 1) {
                return;
            }
            Object s = p.span();
            final long slen = getFullBlockSpanLen(s);
            if (slen > 0) {
                if (!hasPrev) {
                    start = sbk;
                    end = sbk + slen * BLOCK_SIZE - 1;
                } else {
                    end += slen * BLOCK_SIZE;
                }
                if (!p.hasNext()) {
                    setFinished();
                    return;
                }
                p.next();
                sk = p.spanKey();
                final long sbkNew = highBits(sk);
                if (sbk + slen * BLOCK_SIZE < sbkNew) {
                    nextValid = true;
                    return;
                }
                sbk = sbkNew;
                // This span can't be a full block span: it would have been merged with the previous one.
                // Therefore at this point we know p.span() is an RB Container.
                hasPrev = true;
                s = p.span();
            }
            final Container c;
            if (s == null) {
                ri = new SingletonContainer.SearchRangeIter(lowBits(sk));
            } else {
                c = (Container) s;
                ri = c.getShortRangeIterator(0);
            }
            // ri.hasNext() has to be true by construction; this container can't be empty or it wouldn't be present.
            ri.hasNext();  // we call it for its potential side effects.
            ri.next();
        }
    }

    private long peekNextStart() {
        if (ri != null) {
            return p.spanKey() | (long) ri.start();
        }
        final Object s = p.span();
        if (s == null || RspArray.getFullBlockSpanLen(s) > 0) {
            return p.spanKey();
        }
        final Container c = (Container) s;
        return p.spanKey() | (long) c.first();
    }

    /**
     * @return start point of the current interval.
     */
    @Override
    public long start() {
        return start;
    }

    /**
     * @return end point of the current interval, inclusive.
     */
    @Override
    public long end() {
        return end;
    }

    /**
     * At call, start() <= v <= end()
     * @param v Next call to start will return this value.
     */
    public void postpone(final long v) {
        start = v;
    }

    /**
     * This method should be called:
     * * After the iterator is created and before calling any other methods;
     *   it returns false, calling any other methods results in undefined behavior.
     * * Right after a call to next, similar to above.
     *
     * @return true if a call to next leads to a valid range to be read from start() and end().
     */
    @Override
    public boolean hasNext() {
        return nextValid;
    }

    @Override
    public void next() {
        if (!nextValid) {
            return;
        }
        nextInterval();
    }

    @Override
    public boolean forEachLongRange(final LongRangeAbortableConsumer lrc) {
        while (nextValid) {
            nextInterval();
            if (!lrc.accept(start, end)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Advance the current iterator position while the current range end is less than key.
     * This results in either (a) true, leaving a current range whose start value is greater or equal than key,
     * or (b) false, leaving an exhausted, invalid iterator.
     *
     * Note if the iterator is not exhausted, true is returned and the satisfying range is left as
     * the iterator's current range: no need to call next to get to it.
     * Also note the iterator may not move if at call entry the current range already satisfies (a).
     *
     * If this method returns false, it implies the iterator has been exhausted, the current range is invalid,
     * and subsequent calls to hasNext will return false;  there is no guarantee as to where the start
     * and end positions are left in this case.
     *
     * @param key a key to search forward from the current iterator position
     * @return true if case (a), false if case (b).
     *
     */
    public boolean advance(final long key) {
        if (end < start) {
            // next() has never been called.
            if (nextValid) {
                next();
            } else {
                return false;
            }
        }
        if (end >= key) {
            start = Math.max(key, start);
            return true;
        }
        if (!hasNext()) {
            return false;
        }
        final long k = p.spanKey();
        if (!p.advance(key)) {
            setFinished();
            return false;
        }
        if (k != p.spanKey()) {
            // we are in a different container now.
            ri = null;
        }
        nextInterval();
        if (end >= key) {
            start = Math.max(key, start);
            return true;
        }
        if (ri == null) {
            if (!hasNext()) {
                setFinished();
                return false;
            }
            next();
            return true;
        }
        final int rk = (int) (key - highBits(p.spanKey()));
        if (ri.advance(rk)) {
            nextInterval();
            start = Math.max(key, start);
            return true;
        }
        if (!p.hasNext()) {
            setFinished();
            return false;
        }
        p.next();
        ri = null;
        nextInterval();
        start = Math.max(key, start);
        return true;
    }

    /**
     * Advance the current iterator (start) position to the rightmost (last) value v that maintains
     * comp.directionToTargetFrom(v) >= 0.  I.e, either hasNext() returns false after this call, or the next value
     * in the iterator nv would be such that comp.directionToTargetFrom(nv) < 0.
     *
     * Note this method should be called only after calling hasNext() and next() at least once, eg,
     * from a valid current position in a non-empty and also non-exhausted iterator.
     *
     * @param comp a comparator used to search forward from the current iterator position
     *
     */
    public void search(final Comparator comp) {
        if (!hasNext()) {
            start = IndexUtilities.rangeSearch(start, end, comp);
            return;
        }
        int c = comp.directionToTargetFrom(end);
        if (c <= 0) {
            if (c == 0 || start >= end) {
                start = end;
                return;
            }
            start = IndexUtilities.rangeSearch(start, end - 1, comp);
            return;
        }
        final long oldSpanKey = p.spanKey();
        p.search(comp);
        final long targetSpanKey = p.spanKey();
        if (oldSpanKey != targetSpanKey) {
            // we are in a different container now.
            ri = null;
        } else {
            final long s = peekNextStart();
            c = comp.directionToTargetFrom(s);
            if (c < 0) {
                start = end;
                return;
            }
        }
        while (ri == null) {
            nextInterval();
            c = comp.directionToTargetFrom(end);
            if (c <= 0) {
                if (c == 0 || start >= end) {
                    start = end;
                    return;
                }
                start = IndexUtilities.rangeSearch(start, end - 1, comp);
                return;
            }
            if (!hasNext() || p.spanKey() != targetSpanKey) {
                start = end;
                return;
            }
        }
        final ContainerUtil.TargetComparator rcomp = (int v) -> comp.directionToTargetFrom(highBits(p.spanKey()) | v);
        final boolean found = ri.search(rcomp);
        if (found) {
            nextInterval();
            return;
        }
        start = IndexUtilities.rangeSearch(start, end, comp);
    }

    public void close() {
        if (p == null) {
            return;
        }
        p.release();
        p = null;
    }

    /**
     * Create a RangeIterator that is a view into this iterator;
     * the returned rangeIterator has current start() - startOffset as it initial start value
     * (note the iterator needs to have a valid current position at the time of the call).
     * The returned RangeIterator includes all the ranges until the end parameter (exclusive),
     * and as it advances it will make the underlying iterator advance.
     * Once the RangeIterator is exhausted, the underlying iterator will have a current value
     * that is one after the last range returned by the range iterator (not this may have
     * been truncated to a partial, still valid, range).
     * @param startOffset The resulting range iterator returns ranges offset with this value.
     * @param rangesEnd boundary (exclusive) on the underlying iterator ranges for the ranges returned.
     * @return
     */
    public RangeIteratorView rangeIteratorView(final long startOffset, final long rangesEnd) {
        return new RangeIteratorView(this, startOffset, rangesEnd);
    }

    public static class RangeIteratorView implements SearchRangeIterator {
        private RspRangeIterator it;
        private final long offset;
        private final long rangesEnd;
        private int start;
        private int end;        // Note RangeIterator uses exclusive ends.
        private int nextStart;
        private int nextEnd;    // Note RangeIterator uses exclusive ends.
        private boolean nextValid;
        private boolean noMore;
        private boolean itFinished;
        public RangeIteratorView(final RspRangeIterator it, final long offset, final long rangesEnd) {
            this.it = it;
            this.offset = offset;
            this.rangesEnd = rangesEnd;
            if (it.start() < offset) {
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
            if (noMore || it.start() >= rangesEnd) {
                setTerminated();
                return;
            }
            nextValid = true;
            nextStart = (int) (it.start() - offset);
            if (it.end() >= rangesEnd) {
                nextEnd = (int) (rangesEnd - offset);
                it.postpone(rangesEnd);
                noMore = true;
                return;
            }
            nextEnd = (int) (it.end() - offset) + 1;
            if (it.hasNext()) {
                it.next();
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
}
