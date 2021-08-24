package io.deephaven.db.v2.utils.singlerange;

import static io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyRanges;

import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.db.v2.utils.*;

public class SingleRangeOrderedKeys extends OrderedKeysAsChunkImpl implements SingleRangeMixin {
    private long rangeStart;
    private long rangeEnd;

    @Override
    public long rangeStart() {
        return rangeStart;
    }

    @Override
    public long rangeEnd() {
        return rangeEnd;
    }

    SingleRangeOrderedKeys(final long rangeStart, final long rangeEnd) {
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
    }

    protected void reset(final long rangeStart, final long rangeEnd) {
        invalidateOrderedKeysAsChunkImpl();
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
    }

    public SingleRangeOrderedKeys copy() {
        return new SingleRangeOrderedKeys(rangeStart, rangeEnd);
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public long lastKey() {
        return rangeEnd;
    }

    @Override
    public long firstKey() {
        return rangeStart;
    }

    @Override
    public long size() {
        return rangeEnd - rangeStart + 1;
    }

    @Override
    public long rangesCountUpperBound() {
        return 1;
    }

    @Override
    public Index asIndex() {
        return new TreeIndex(SingleRange.make(rangeStart(), rangeEnd()));
    }

    @Override
    public void fillKeyIndicesChunk(final WritableLongChunk<? extends KeyIndices> chunkToFill) {
        final int n = intSize();
        for (int i = 0; i < n; ++i) {
            chunkToFill.set(i, rangeStart() + i);
        }
        chunkToFill.setSize(n);
    }

    @Override
    public void fillKeyRangesChunk(final WritableLongChunk<OrderedKeyRanges> chunkToFill) {
        final int maxSz = chunkToFill.size();
        if (maxSz < 2) {
            chunkToFill.setSize(0);
            return;
        }
        chunkToFill.set(0, rangeStart());
        chunkToFill.set(1, rangeEnd());
        chunkToFill.setSize(2);
    }

    static final class OKIterator implements OrderedKeys.Iterator {
        private long currStart;
        private long currEnd;
        private long sizeLeft;
        private final SingleRangeOrderedKeys currBuf;

        public OKIterator(final long rangeStart, final long rangeEnd) {
            currStart = rangeStart;
            currEnd = -1;
            sizeLeft = rangeEnd - rangeStart + 1;
            currBuf = new SingleRangeOrderedKeys(0, 0);
        }

        @Override
        public boolean hasMore() {
            return sizeLeft > 0;
        }

        @Override
        public long peekNextKey() {
            if (sizeLeft <= 0) {
                return Index.NULL_KEY;
            }
            if (currEnd == -1) {
                return currStart;
            }
            return currEnd + 1;
        }

        @Override
        public OrderedKeys getNextOrderedKeysThrough(final long maxKeyInclusive) {
            if (maxKeyInclusive < 0 || sizeLeft <= 0) {
                return OrderedKeys.EMPTY;
            }
            if (currEnd != -1) {
                if (maxKeyInclusive <= currEnd) {
                    return OrderedKeys.EMPTY;
                }
                currStart = currEnd + 1;
            } else if (maxKeyInclusive < currStart) {
                return OrderedKeys.EMPTY;
            }
            currEnd = Math.min(currStart + sizeLeft - 1, maxKeyInclusive);
            sizeLeft -= currEnd - currStart + 1;
            currBuf.reset(currStart, currEnd);
            return currBuf;
        }

        @Override
        public OrderedKeys getNextOrderedKeysWithLength(final long numberOfKeys) {
            if (numberOfKeys <= 0 || sizeLeft <= 0) {
                return OrderedKeys.EMPTY;
            }
            if (currEnd != -1) {
                currStart = currEnd + 1;
            }
            currEnd = currStart + Math.min(sizeLeft, numberOfKeys) - 1;
            sizeLeft -= currEnd - currStart + 1;
            currBuf.reset(currStart, currEnd);
            return currBuf;
        }

        @Override
        public boolean advance(final long toKey) {
            if (sizeLeft <= 0) {
                return false;
            }
            final long last;
            if (currEnd == -1) {
                last = currStart + sizeLeft - 1;
            } else {
                last = currEnd + 1 + sizeLeft - 1;
            }
            if (last < toKey) {
                sizeLeft = 0;
                return false;
            }
            if (toKey > currStart) {
                currStart = toKey;
                currEnd = -1;
                sizeLeft = last - currStart + 1;
            }
            return true;
        }

        @Override
        public long getRelativePosition() {
            return -sizeLeft;
        }

        @Override
        public void close() {
            currBuf.close();
        }
    }
}
