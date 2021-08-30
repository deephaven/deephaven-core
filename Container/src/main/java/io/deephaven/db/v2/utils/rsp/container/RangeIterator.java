package io.deephaven.db.v2.utils.rsp.container;

import java.util.Arrays;

public interface RangeIterator {
    /**
     * Checks if the iterator has more ranges. If hasNext() returns false, calling next thereafter results in undefined
     * behavior.
     *
     * @return whether there is another range.
     */
    boolean hasNext();

    /**
     * Start of the current range.
     * <p>
     * Calling start() without calling next() at least once results in undefined behavior.
     *
     * @return the start of the current range.
     */
    int start();

    /**
     * End of the current range (exclusive).
     * <p>
     * Calling end() without calling next() at least once results in undefined behavior.
     *
     * @return the end of the current range (exclusive).
     */
    int end();

    /**
     * Advance the iterator to the next range. Only call after hasNext() has returned true.
     */
    void next();

    /**
     * Call accept on the provided AbortableRangeConsumer until it returns false or we run out of values.
     *
     * @param rc An AbortableRangeConsumer to feed ranges to.
     * @return false if AbortableRangeConsumer returned false at any point, true otherwise.
     */
    default boolean forEachRange(AbortableRangeConsumer rc) {
        while (hasNext()) {
            next();
            final boolean wantMore = rc.accept(start(), end());
            if (!wantMore) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get the next batch of ranges.
     *
     * @param buffer a short array where consecutive pairs of (start, end-1) values will be stored.
     * @param offset where in buffer to start storing range boundary values.
     * @param maxRanges maximum number of ranges that can be written to buffer; {@code buffer} should have at least
     *        space for {@code 2*maxRanges} shorts starting at {@code offset}.
     * @return how many ranges were written in {@code buffer}; this is two times the individual elements written.
     */
    default int next(final short[] buffer, final int offset, final int maxRanges) {
        int count = 0;
        while (hasNext() && count < maxRanges) {
            next();
            buffer[offset + 2 * count] = (short) start();
            buffer[offset + 2 * count + 1] = (short) (end() - 1);
            ++count;
        }
        return count;
    }

    class Single implements RangeIterator {
        int start;
        int end;
        boolean hasNext;

        public Single(int start, int end) {
            if (end < start || start < 0) {
                throw new IllegalArgumentException("Invalid range start=" + start + ", endI=" + end);
            }
            this.start = start;
            this.end = end;
            this.hasNext = true;
        }

        private Single(Single other) {
            start = other.start;
            end = other.end;
            hasNext = other.hasNext;
        }

        public Single copy() {
            return new Single(this);
        }

        @Override
        public boolean hasNext() {
            return hasNext;
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
            hasNext = false;
        }

        @Override
        public int next(short[] buffer, int offset, int maxShortCount) {
            throw new UnsupportedOperationException();
        }
    }

    class ArrayBacked implements RangeIterator {
        private final int[] ranges;
        private int pos;

        public ArrayBacked(int[] ranges) {
            if ((ranges.length & 1) != 0) {
                throw new IllegalArgumentException("Invalid array for range, odd size=" + ranges.length);
            }
            this.ranges = Arrays.copyOf(ranges, ranges.length);
            pos = -2;
        }

        private ArrayBacked(ArrayBacked other) {
            ranges = other.ranges;
            pos = other.pos;
        }

        public ArrayBacked copy() {
            return new ArrayBacked(this);
        }

        @Override
        public boolean hasNext() {
            return pos + 2 < ranges.length;
        }

        @Override
        public int start() {
            return ranges[pos];
        }

        @Override
        public int end() {
            return ranges[pos + 1];
        }

        @Override
        public void next() {
            pos += 2;
        }

        @Override
        public int next(short[] buffer, int offset, int maxShortCount) {
            throw new UnsupportedOperationException();
        }
    }
}
