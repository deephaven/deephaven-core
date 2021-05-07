package io.deephaven.db.v2.utils.rsp;

import io.deephaven.db.v2.utils.rsp.container.Container;
import io.deephaven.db.v2.utils.rsp.container.ShortAdvanceIterator;
import io.deephaven.db.v2.utils.rsp.container.SingletonContainer;

import static io.deephaven.db.v2.utils.rsp.RspArray.*;

public class RspReverseIterator {
    private RspArray.SpanCursor rp;
    // Iterator pointing to the next value to deliver in the current RB Container if there is one, null otherwise.
    private ShortAdvanceIterator ri;
    // Current start and end values.
    long current = -1;
    long next;
    boolean nextValid;
    long fullBlockSpanKey = -1;

    public RspReverseIterator(final RspArray.SpanCursor rp) {
        this.rp = rp;
        if (!rp.hasNext()) {
            setEnded();
            return;
        }
        rp.next();
        computeNext();
    }

    private void setEnded() {
        nextValid = false;
        release();
    }

    private void computeNext() {
        if (nextValid && fullBlockSpanKey != -1) {
            computeNextFromFullSpan();
            return;
        }
        if (ri != null) {
            if (ri.hasNext()) {
                final long k = rp.spanKey();
                next = unsignedShortToLong(ri.next()) | k;
                nextValid = true;
                return;
            }
            ri = null;
            if (!rp.hasNext()) {
                setEnded();
                return;
            }
            rp.next();
        }
        updateNextFromSpanCursor();
    }

    private void updateNextFromSpanCursor() {
        final long k = rp.spanKey();
        final Object s = rp.span();
        long flen = getFullBlockSpanLen(s);
        if (flen > 0) {
            next = k + BLOCK_SIZE * flen - 1;
            fullBlockSpanKey = k;
            nextValid = true;
            return;
        }
        final Container c;
        if (s == null) {
            ri = new SingletonContainer.ReverseIter(lowBits(k));
        } else {
            c = (Container) s;
            ri = c.getReverseShortIterator();
        }
        nextValid = true;
        next = unsignedShortToLong(ri.next()) | k;
        fullBlockSpanKey = -1;
    }

    private void computeNextFromFullSpan() {
        --next;
        if (Long.compareUnsigned(next, fullBlockSpanKey) >= 0) {
            nextValid = true;
            return;
        }
        fullBlockSpanKey = -1;
        if (!rp.hasNext()) {
            setEnded();
            return;
        }
        rp.next();
        updateNextFromSpanCursor();
    }

    /**
     * @return current iterator value, without advancing it. A valid call to next() should have happened before calling
     * this method.
     */
    public long current() {
        return current;
    }

    /**
     * This method should be called:
     * * After the iterator is created and before calling any other methods;
     *   if it returns false, calling any other methods results in undefined behavior.
     * * Right after a call to next, similar to above.
     *
     * @return true if a call to next leads to a valid next iterator value.
     */
    public boolean hasNext() {
        return nextValid;
    }

    public void next() {
        if (!nextValid) {
            return;
        }
        current = next;
        computeNext();
    }

    private void setAdvanceOverranState() {
        final Object span = rp.span();
        if (span == null) {
            current = rp.spanKey();
            return;
        }
        final long flen = getFullBlockSpanLen(span);
        if (flen > 0) {
            current = rp.spanKey();
        } else {
            final Container c = (Container) span;
            current = rp.spanKey() | c.first();
        }
    }

    private boolean tryCurrentSpanForAdvance(final long v) {
        final long kb = highBits(rp.spanKey());
        if (v < kb) {
            return false;
        }
        if (ri == null) {
            current = next = v;
            next();
            return true;
        }
        if (ri.advance((int) (BLOCK_LAST & v))) {
            current = kb | ri.currAsInt();
            nextValid = false;
            computeNext();
            return true;
        }
        ri = null;
        if (!rp.hasNext()) {
            setAdvanceOverranState();
            setEnded();
            return false;
        }
        rp.next();
        nextValid = false;
        computeNext();
        current = next;
        computeNext();
        return true;
    }

    public boolean advance(final long v) {
        if (!nextValid) {
            return current != -1 && current <= v;
        }
        if (current < 0) {
            // next has never been called yet.
            next();
            if (!nextValid) {
                return current <= v;
            }
        }
        if (current <= v) {
            return true;
        }
        // At this point nextValid has to be true; otherwise we would have returned false on entry.
        if (next <= v) {
            next();
            return true;
        }
        if (tryCurrentSpanForAdvance(v)) {
            return true;
        }
        if (rp == null) {
            return false;
        }
        ri = null;
        nextValid = false;
        final boolean valid = rp.advance(v);
        if (!valid) {
            setAdvanceOverranState();
            setEnded();
            return false;
        }
        computeNext();
        if (next <= v) {
            next();
            return true;
        }
        return tryCurrentSpanForAdvance(v);
    }

    public void release() {
        ri = null;
        if (rp != null) {
            rp.release();
        }
        rp = null;
    }
}
