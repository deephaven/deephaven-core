package io.deephaven.db.v2.utils.rsp;

import io.deephaven.configuration.Configuration;
import static io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyRanges;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.db.v2.utils.rsp.container.Container;
import io.deephaven.db.v2.utils.rsp.container.SearchRangeIterator;
import io.deephaven.db.v2.utils.rsp.container.SingletonContainer;
import io.deephaven.util.SafeCloseable;

import static io.deephaven.db.v2.utils.rsp.RspArray.*;
import static io.deephaven.db.v2.utils.rsp.RspArray.BLOCK_SIZE;

public class RspRangeBatchIterator implements SafeCloseable {
    private RspArray.SpanCursorForward p;
    // Iterator pointing to the next value to deliver in the current RB Container if there is one,
    // null otherwise.
    private SearchRangeIterator ri;
    // To hold the container to which ri refers.
    private SpanView riView;
    private boolean moreSpans; // if true, p has not been exhausted yet.

    private static final int BUF_SZ = Configuration.getInstance().getIntegerForClassWithDefault(
        RspRangeBatchIterator.class, "shortBufSize", 2 * 32); // 32 shorts is an IA64 cache line
                                                              // size.
    private short[] buf = new short[BUF_SZ];
    private int bufOffset = 0;
    private int bufCount = 0;
    private long bufKey = -1L;
    private long pendingStartOffset = 0;
    private long remaining;

    public RspRangeBatchIterator(final RspArray.SpanCursorForward p, final long startOffset,
        final long maxCount) {
        if (!p.hasNext() || maxCount <= 0) {
            p.release();
            this.p = null;
            moreSpans = false;
            return;
        }
        riView = new SpanView(null);
        this.p = p;
        remaining = maxCount;
        moreSpans = true;
        p.next();
        Object s = p.span();
        final long spanInfo = p.spanInfo();
        final long slen = getFullBlockSpanLen(spanInfo, s);
        if (slen > 0) {
            pendingStartOffset = startOffset;
            return;
        }
        if (isSingletonSpan(s)) {
            if (startOffset != 0) {
                throw new IllegalStateException("null span and startOffset=" + startOffset);
            }
            final long singletonValue = spanInfoToSingletonSpanValue(spanInfo);
            ri = Container.singleton(lowBits(singletonValue)).getShortRangeIterator(0);
            bufKey = spanInfoToKey(spanInfo);
            return;
        }
        riView.init(p.arr(), p.arrIdx(), spanInfo, s);
        ri = riView.getContainer()
            .getShortRangeIterator((int) ((long) (Integer.MAX_VALUE) & startOffset));
        bufKey = spanInfoToKey(spanInfo);
        if (!ri.hasNext()) {
            throw new IllegalStateException("Illegal offset");
        }
    }

    @Override
    public void close() {
        if (riView != null) {
            riView.close();
        }
    }

    public boolean hasNext() {
        return moreSpans || bufCount > 0;
    }

    private int flushBufToChunk(
        final WritableLongChunk<OrderedKeyRanges> chunk, final int chunkOffset,
        final int chunkDelta, final int chunkMaxCount) {
        final int bufDelta = Math.min(chunkMaxCount - chunkDelta, bufCount);
        int i = 0;
        while (remaining > 0 && i < bufDelta) {
            final long v1 = unsignedShortToLong(buf[bufOffset + i]);
            chunk.set(chunkOffset + chunkDelta + i, bufKey | v1);
            ++i;
            long v2 = unsignedShortToLong(buf[bufOffset + i]);
            long delta = v2 - v1 + 1;
            if (delta > remaining) {
                delta = remaining;
                v2 = v1 + remaining - 1;
            }
            chunk.set(chunkOffset + chunkDelta + i, bufKey | v2);
            remaining -= delta;
            ++i;
        }
        bufOffset += i;
        bufCount -= i;
        return i;
    }

    private void loadBuffer() {
        bufOffset = 0;
        final int rangesWritten = ri.next(buf, 0, BUF_SZ / 2);
        bufCount = 2 * rangesWritten;
        if (!ri.hasNext()) {
            riView.reset();
            ri = null;
            if (!p.hasNext()) {
                moreSpans = false;
                return;
            }
            p.next();
        }
    }

    private static final short BLOCK_LAST_AS_SHORT = (short) -1;

    /**
     * Fill a writable long chunk with pairs of range boundaries (start, endInclusive) starting from
     * the next iterator position forward.
     *
     * @param chunk A writable chunk to fill
     * @param chunkOffset An offset inside the chunk to the position to start writing
     * @return The count of ranges written (which matches 2 times the number of elements written).
     */
    public int fillRangeChunk(final WritableLongChunk<OrderedKeyRanges> chunk,
        final int chunkOffset) {
        final int chunkMaxCount = chunk.capacity();
        int chunkDelta = 0;
        // first, flush any leftovers in buf from previous calls.
        long keyForPrevRangeEndAtSpanBoundary = -1;
        if (bufCount > 0) {
            chunkDelta += flushBufToChunk(chunk, chunkOffset, chunkDelta, chunkMaxCount);
            if (remaining <= 0) {
                setFinished();
                return chunkDelta / 2;
            }
            if (bufCount > 0) {
                return chunkDelta / 2;
            }
            keyForPrevRangeEndAtSpanBoundary =
                (buf[bufOffset - 1] == BLOCK_LAST_AS_SHORT) ? bufKey + BLOCK_SIZE : -1;
        }
        while (true) {
            while (ri != null) {
                if (bufCount == 0) {
                    loadBuffer();
                    if (keyForPrevRangeEndAtSpanBoundary != -1 &&
                        keyForPrevRangeEndAtSpanBoundary == bufKey &&
                        buf[0] == (short) 0) {
                        long v = unsignedShortToLong(buf[1]);
                        long delta = v + 1;
                        if (delta >= remaining) {
                            v = remaining - 1;
                            delta = remaining;
                        }
                        chunk.set(chunkOffset + chunkDelta - 1, bufKey | v);
                        remaining -= delta;
                        bufOffset = 2;
                        bufCount -= 2;
                        if (remaining <= 0) {
                            setFinished();
                            return chunkDelta / 2;
                        }
                    }
                    keyForPrevRangeEndAtSpanBoundary = -1;
                }
                if (chunkMaxCount - chunkDelta < 2) {
                    // moreSpans is still true.
                    return chunkDelta / 2;
                }
                chunkDelta += flushBufToChunk(chunk, chunkOffset, chunkDelta, chunkMaxCount);
                if (remaining <= 0) {
                    setFinished();
                    return chunkDelta / 2;
                }
                if (ri == null) {
                    // we can't return even if max had been reached: we may need to merge a later
                    // range.
                    if (bufCount > 0) {
                        // the only way we have leftover buf is if not all of it fit into chunk.
                        return chunkDelta / 2;
                    }
                    keyForPrevRangeEndAtSpanBoundary =
                        (bufOffset > 0 && buf[bufOffset - 1] == BLOCK_LAST_AS_SHORT)
                            ? bufKey + BLOCK_SIZE
                            : -1;
                    break;
                }
                if (bufCount > 0) {
                    // the only way we have leftover buf is if not all of it fit into chunk.
                    return chunkDelta / 2;
                }
            }
            Object s = p.span();
            long spanInfo = p.spanInfo();
            final long slen = getFullBlockSpanLen(spanInfo, s);
            if (slen > 0) {
                final long sk = spanInfoToKey(spanInfo);
                final long d;
                // do we need to merge a previously stored span last range?
                if (keyForPrevRangeEndAtSpanBoundary != -1
                    && keyForPrevRangeEndAtSpanBoundary == sk) {
                    d = Math.min(remaining, slen * BLOCK_SIZE);
                    chunk.set(chunkOffset + chunkDelta - 1, sk + d - 1);
                } else {
                    if (chunkMaxCount - chunkDelta < 2) {
                        // moreSpans is still true.
                        return chunkDelta / 2;
                    }
                    final long start = sk + pendingStartOffset;
                    chunk.set(chunkOffset + chunkDelta, start);
                    d = Math.min(remaining, slen * BLOCK_SIZE - pendingStartOffset);
                    pendingStartOffset = 0;
                    chunk.set(chunkOffset + chunkDelta + 1, start + d - 1);
                    chunkDelta += 2;
                }
                remaining -= d;
                if (remaining <= 0) {
                    setFinished();
                    return chunkDelta / 2;
                }
                keyForPrevRangeEndAtSpanBoundary = sk + slen * BLOCK_SIZE;
                if (!p.hasNext()) {
                    setFinished();
                    return chunkDelta / 2;
                }
                p.next();
                // This span can't be a full block span: it would have been merged with the previous
                // one.
                // Therefore at this point we know p.span() is an RB Container.
                s = p.span();
            }
            spanInfo = p.spanInfo();
            bufKey = spanInfoToKey(spanInfo);
            if (isSingletonSpan(s)) {
                final long singletonValue = spanInfoToSingletonSpanValue(spanInfo);
                final short lowBitsValue = lowBits(singletonValue);
                riView.reset();
                ri = new SingletonContainer.SearchRangeIter(lowBitsValue);
            } else {
                riView.init(p.arr(), p.arrIdx(), spanInfo, s);
                ri = riView.getContainer().getShortRangeIterator(0);
            }
        }
    }

    private void setFinished() {
        moreSpans = false;
        bufCount = 0;
        release();
    }

    public void release() {
        if (p == null) {
            return;
        }
        p.release();
        p = null;
    }
}
