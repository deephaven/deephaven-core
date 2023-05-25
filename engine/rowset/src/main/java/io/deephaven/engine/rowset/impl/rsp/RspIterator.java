/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.rowset.impl.rsp;

import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.util.datastructures.LongAbortableConsumer;
import io.deephaven.engine.rowset.impl.rsp.container.Container;
import io.deephaven.engine.rowset.impl.rsp.container.ContainerShortBatchIterator;

import java.util.PrimitiveIterator;

import io.deephaven.configuration.Configuration;
import io.deephaven.util.SafeCloseable;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.SpanView;

public class RspIterator implements PrimitiveIterator.OfLong, SafeCloseable {
    private interface SingleSpanIterator {
        boolean forEachLong(LongAbortableConsumer lc);

        int copyTo(long[] vs, int offset, int maxCount);

        int copyTo(WritableLongChunk<? super OrderedRowKeys> chunk, int offset, int maxCount);

        long nextLong();

        boolean hasNext();
    }

    private RspArray.SpanCursorForward p;
    private SingleSpanIterator sit;
    // Resource to hold the container sit may be pointing to.
    private SpanView sitView;
    private static final int BUFSZ =
            Configuration.getInstance().getIntegerForClassWithDefault(RspIterator.class, "bufferSize", 122);
    private boolean hasNext;

    RspIterator(final RspArray.SpanCursorForward p, final long firstSpanSkipCount) {
        if (!p.hasNext()) {
            p.release();
            this.p = null;
            hasNext = false;
            return;
        }
        sitView = new SpanView(null);
        this.p = p;
        nextSingleSpanIterator(firstSpanSkipCount);
        hasNext = true;
    }

    public RspIterator(final RspArray.SpanCursorForward p) {
        this(p, 0);
    }

    public void release() {
        if (sitView != null) {
            sitView.close();
        }
        if (p == null) {
            return;
        }
        p.release();
        p = null;
    }

    @Override
    public void close() {
        release();
    }

    public boolean forEachLong(final LongAbortableConsumer lc) {
        while (hasNext) {
            if (!sit.hasNext()) {
                nextSingleSpanIterator(0);
            }
            final boolean wantMore = sit.forEachLong(lc);
            hasNext = sit.hasNext() || p.hasNext();
            if (!wantMore) {
                return false;
            }
        }
        return true;
    }

    public int copyTo(final long[] vs, final int offset, final int max) {
        int c = 0;
        while (hasNext) {
            if (!sit.hasNext()) {
                nextSingleSpanIterator(0);
            }
            c += sit.copyTo(vs, offset + c, max - c);
            hasNext = sit.hasNext() || p.hasNext();
            if (c >= max) {
                break;
            }
        }
        return c;
    }

    public int copyTo(final WritableLongChunk<? super OrderedRowKeys> chunk, final int offset, final int max) {
        int c = 0;
        while (hasNext) {
            if (!sit.hasNext()) {
                nextSingleSpanIterator(0);
            }
            c += sit.copyTo(chunk, offset + c, max - c);
            hasNext = sit.hasNext() || p.hasNext();
            if (c >= max) {
                break;
            }
        }
        return c;
    }

    private void nextSingleSpanIterator(final long skipCount) {
        io.deephaven.base.verify.Assert.neqNull(p, "p"); // IDS-6989
        p.next();
        final long spanInfo = p.spanInfo();
        final Object s = p.span();
        if (RspArray.isSingletonSpan(s)) {
            if (skipCount != 0) {
                throw new IllegalArgumentException("skipCount=" + skipCount + " and next span is single element");
            }
            final long singletonValue = RspArray.spanInfoToSingletonSpanValue(spanInfo);
            sitView.reset();
            sit = new SingleSpanIterator() {
                long v = singletonValue;

                @Override
                public long nextLong() {
                    final long ret = v;
                    v = -1;
                    return ret;
                }

                @Override
                public boolean hasNext() {
                    return v != -1;
                }

                @Override
                public boolean forEachLong(final LongAbortableConsumer lc) {
                    if (v == -1) {
                        return true;
                    }
                    final long ret = v;
                    v = -1;
                    return lc.accept(ret);
                }

                @Override
                public int copyTo(final long[] vs, final int offset, final int max) {
                    if (max <= 0 || v == -1) {
                        return 0;
                    }
                    vs[offset] = v;
                    v = -1;
                    return 1;
                }

                @Override
                public int copyTo(final WritableLongChunk<? super OrderedRowKeys> chunk, final int offset,
                        final int max) {
                    if (max <= 0 || v == -1) {
                        return 0;
                    }
                    chunk.set(offset, v);
                    v = -1;
                    return 1;
                }
            };
            return;
        }
        final long flen = RspArray.getFullBlockSpanLen(spanInfo, s);
        final long k = RspArray.spanInfoToKey(spanInfo);
        if (flen > 0) {
            sitView.reset();
            sit = new SingleSpanIterator() {
                long curr = k + skipCount;
                final long end = k + flen * RspArray.BLOCK_SIZE - 1;

                @Override
                public boolean hasNext() {
                    return curr <= end;
                }

                @Override
                public long nextLong() {
                    return curr++;
                }

                @Override
                public boolean forEachLong(final LongAbortableConsumer lc) {
                    while (curr <= end) {
                        final boolean wantMore = lc.accept(curr++);
                        if (!wantMore) {
                            return false;
                        }
                    }
                    return true;
                }

                @Override
                public int copyTo(final long vs[], final int offset, final int max) {
                    int c = 0;
                    final long last = Math.min(curr + max - 1, end);
                    while (curr <= last) {
                        vs[offset + c++] = curr++;
                    }
                    return c;
                }

                @Override
                public int copyTo(final WritableLongChunk<? super OrderedRowKeys> chunk, final int offset,
                        final int max) {
                    int c = 0;
                    final long last = Math.min(curr + max - 1, end);
                    while (curr <= last) {
                        chunk.set(offset + c++, curr++);
                    }
                    return c;
                }
            };
            return;
        }
        sitView.init(p.arr(), p.arrIdx(), spanInfo, s);
        final Container c = sitView.getContainer();
        final int intSkipCount = (int) (((long) Integer.MAX_VALUE) & skipCount);
        sit = new SingleSpanIterator() {
            final ContainerShortBatchIterator cit = c.getShortBatchIterator(intSkipCount);
            short[] buf;
            int count = 0;
            int bi = 0;

            private long longValue(final short v) {
                return k | RspArray.unsignedShortToLong(v);
            }

            @Override
            public long nextLong() {
                if (bi >= count) {
                    if (buf == null) {
                        // Lazy initialize to avoid the allocation in the cases it might never be used
                        // (eg, pure forEachRowKey consumption).
                        buf = new short[BUFSZ];
                    }
                    count = cit.next(buf, 0, buf.length);
                    bi = 0;
                }
                return longValue(buf[bi++]);
            }

            @Override
            public boolean hasNext() {
                return (bi < count) || cit.hasNext();
            }

            @Override
            public boolean forEachLong(final LongAbortableConsumer lc) {
                while (bi < count) {
                    final boolean wantMore = lc.accept(longValue(buf[bi++]));
                    if (!wantMore) {
                        return false;
                    }
                }
                return cit.forEach((final short v) -> lc.accept(longValue(v)));
            }

            @Override
            public int copyTo(final long[] vs, final int offset, final int max) {
                int c = 0;
                while (c < max && bi < count) {
                    vs[offset + c++] = longValue(buf[bi++]);
                }
                if (c < max && cit.hasNext()) {
                    final int[] ac = {c};
                    cit.forEach((short v) -> {
                        vs[offset + ac[0]++] = longValue(v);
                        return ac[0] < max;
                    });
                    return ac[0];
                }
                return c;
            }

            @Override
            public int copyTo(final WritableLongChunk<? super OrderedRowKeys> chunk, final int offset, final int max) {
                int c = 0;
                if (buf == null) {
                    // Lazy initialize to avoid the allocation in the cases it might never be used
                    // (eg, pure forEachRowKey consumption).
                    buf = new short[BUFSZ];
                }
                while (true) {
                    final int remaining = max - c;
                    final int limit = Math.min(bi + remaining, count);
                    while (bi < limit) {
                        chunk.set(offset + c++, longValue(buf[bi++]));
                    }
                    if (c >= max || !cit.hasNext()) {
                        return c;
                    }
                    count = cit.next(buf, 0, buf.length);
                    bi = 0;
                }
            }
        };
    }

    public boolean hasNext() {
        return hasNext;
    }

    public long nextLong() {
        if (!sit.hasNext()) {
            nextSingleSpanIterator(0);
        }
        final long v = sit.nextLong();
        hasNext = sit.hasNext() || p.hasNext();
        if (!hasNext) {
            p.release();
            p = null;
        }
        return v;
    }
}
