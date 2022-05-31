package io.deephaven.engine.rowset.impl.rsp.container;

import static io.deephaven.engine.rowset.impl.rsp.container.MutableInteger.setIfNotNull;
import static io.deephaven.engine.rowset.impl.rsp.container.PositionHint.resetIfNotNull;

public final class SingleRangeContainer extends ImmutableContainer {
    private final short rangeFirstValue;
    private final short rangeLastValue;

    public SingleRangeContainer(final int begin, final int end) {
        if (DEBUG) {
            if (end <= begin || begin < 0 || end > MAX_RANGE) {
                throw new IllegalArgumentException("begin=" + begin + ", end=" + end);
            }
        }
        rangeFirstValue = ContainerUtil.lowbits(begin);
        rangeLastValue = ContainerUtil.lowbits(end - 1);
    }

    private int begin() {
        return first();
    }

    private int end() {
        return last() + 1;
    }

    @Override
    public Container add(final int rangeBegin, final int rangeEnd) {
        if (rangeEnd <= rangeBegin) {
            return this;
        }
        final int begin = begin();
        final int end = end();
        final boolean minBeginIsUs = begin <= rangeBegin;
        final boolean maxEndIsUs = rangeEnd <= end;
        if (minBeginIsUs && maxEndIsUs) {
            return this;
        }
        if (rangeEnd < begin) {
            return Container.twoRanges(rangeBegin, rangeEnd, begin, end);
        }
        // begin <= rangeEnd.
        if (end < rangeBegin) {
            return Container.twoRanges(begin, end, rangeBegin, rangeEnd);
        }
        // rangeBegin <= end
        return new SingleRangeContainer(minBeginIsUs ? begin : rangeBegin, maxEndIsUs ? end : rangeEnd);
    }

    @Override
    Container set(final short x, final PositionHint positionHint) {
        return setImpl(x, positionHint);
    }

    @Override
    public Container set(final short x) {
        return setImpl(x, null);
    }

    private Container setImpl(final short x, final PositionHint positionHint) {
        final int v = ContainerUtil.toIntUnsigned(x);
        final int begin = begin();
        final int end = end();
        if (v < begin) {
            if (v + 1 == begin) {
                resetIfNotNull(positionHint);
                return new SingleRangeContainer(v, end);
            }
            // v + 1 < begin
            setIfNotNull(positionHint, 0);
            return new RunContainer(v, v + 1, begin, end);
        }
        // begin <= v.
        if (v < end) {
            return this;
        }
        // end <= v.
        if (end == v) {
            resetIfNotNull(positionHint);
            return new SingleRangeContainer(begin, v + 1);
        }
        // end < v.
        setIfNotNull(positionHint, 1);
        return new RunContainer(begin, end, v, v + 1);
    }

    private Container andImpl(final Container x) {
        return x.andRange(begin(), end());
    }

    @Override
    public Container and(final ArrayContainer x) {
        return andImpl(x);
    }

    @Override
    public Container and(final BitmapContainer x) {
        return andImpl(x);
    }

    @Override
    public Container and(final RunContainer x) {
        return andImpl(x);
    }

    @Override
    public Container andRange(final int rangeBegin, final int rangeEnd) {
        final int begin = begin();
        final int end = end();
        if (rangeEnd <= begin || end <= rangeBegin) {
            return Container.empty();
        }
        final boolean minBeginIsThem = rangeBegin <= begin;
        final boolean maxEndIsThem = end <= rangeEnd;
        if (minBeginIsThem && maxEndIsThem) { // avoid creating a new object if we can.
            return this;
        }
        return new SingleRangeContainer(minBeginIsThem ? begin : rangeBegin, maxEndIsThem ? end : rangeEnd);
    }

    private Container andNotImpl(final Container x) {
        final int begin = begin();
        final int end = end();
        return x.andRange(begin, end).inot(begin, end);
    }

    @Override
    public Container andNot(final ArrayContainer x) {
        return andNotImpl(x);
    }

    @Override
    public Container andNot(final BitmapContainer x) {
        return andNotImpl(x);
    }

    @Override
    public Container andNot(final RunContainer x) {
        return andNotImpl(x);
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean isAllOnes() {
        return rangeFirstValue == (short) 0 && rangeLastValue == (short) MAX_VALUE;
    }

    @Override
    public boolean contains(final short x) {
        final int v = ContainerUtil.toIntUnsigned(x);
        return begin() <= v && v < end();
    }

    @Override
    public boolean contains(int rangeStart, int rangeEnd) {
        return begin() <= rangeStart && rangeEnd <= end();
    }

    private boolean containsImpl(final Container c) {
        return c.isEmpty() ||
                (begin() <= c.first() && c.last() < end());
    }

    @Override
    protected boolean contains(final RunContainer runContainer) {
        return containsImpl(runContainer);
    }

    @Override
    protected boolean contains(final ArrayContainer arrayContainer) {
        return containsImpl(arrayContainer);
    }

    @Override
    protected boolean contains(final BitmapContainer bitmapContainer) {
        return containsImpl(bitmapContainer);
    }


    @Override
    public Container iflip(final short x) {
        final int v = ContainerUtil.toIntUnsigned(x);
        final int begin = begin();
        final int end = end();
        if (v <= begin) {
            if (v == begin) {
                final int card = end - begin;
                if (card == 2) {
                    return new SingletonContainer(ContainerUtil.lowbits((begin + 1)));
                }
                if (card == 1) {
                    return Container.empty();
                }
                return new SingleRangeContainer(begin + 1, end);
            }
            if (v == begin - 1) {
                return new SingleRangeContainer(begin - 1, end);
            }
            return new RunContainer(v, v + 1, begin, end);
        }
        if (v >= end - 1) {
            if (v == end - 1) {
                final int card = end - begin;
                if (card == 2) {
                    return new SingletonContainer(ContainerUtil.lowbits(begin));
                }
                if (card == 1) {
                    return Container.empty();
                }
                return new SingleRangeContainer(begin, end - 1);
            }
            if (v == end) {
                return new SingleRangeContainer(begin, end + 1);
            }
            return new RunContainer(begin, end(), v, v + 1);
        }
        return Container.twoRanges(begin, v, v + 1, end);
    }

    @Override
    public int getCardinality() {
        return end() - begin();
    }

    @Override
    public boolean forEach(final ShortConsumer sc) {
        return forEach(0, sc);
    }

    @Override
    public boolean forEach(final int rankOffset, final ShortConsumer sc) {
        final int end = end();
        for (int i = begin() + rankOffset; i < end; ++i) {
            if (!sc.accept(ContainerUtil.lowbits(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean forEachRange(final int rankOffset, final ShortRangeConsumer sc) {
        final int rangeStart = begin() + rankOffset;
        if (rangeStart >= end()) {
            throw new IllegalStateException("rankOffset=" + rankOffset + ", cardinality=" + getCardinality());
        }
        return sc.accept(ContainerUtil.lowbits(rangeStart), rangeLastValue);
    }

    private static abstract class IterBase {
        protected int curr;
        protected final int last;

        public IterBase(final int curr, final int last) {
            this.curr = curr;
            this.last = last;
        }

        public boolean hasNext() {
            return curr != last;
        }

        protected abstract void nextPos();

        public short curr() {
            return ContainerUtil.lowbits(curr);
        }

        public int currAsInt() {
            return curr;
        }

        public short next() {
            nextPos();
            return curr();
        }

        public int nextAsInt() {
            nextPos();
            return currAsInt();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static final class ForwardIter extends IterBase implements ShortAdvanceIterator {
        public ForwardIter(final SingleRangeContainer s) {
            super(s.begin() - 1, s.end() - 1);
        }

        @Override
        protected void nextPos() {
            ++curr;
        }

        @Override
        public boolean advance(final int v) {
            if (v <= curr) {
                return true;
            }
            if (v > last) {
                curr = last;
                return false;
            }
            // curr < v <= last.
            curr = v;
            return true;
        }
    }

    private static final class ReverseIter extends IterBase implements ShortAdvanceIterator {
        public ReverseIter(final SingleRangeContainer s) {
            super(s.end(), s.begin());
        }

        @Override
        protected void nextPos() {
            --curr;
        }

        @Override
        public boolean advance(final int v) {
            if (v >= curr) {
                return true;
            }
            if (v < last) {
                curr = last;
                return false;
            }
            // last <= v < curr.
            curr = v;
            return true;
        }
    }

    @Override
    public ShortAdvanceIterator getReverseShortIterator() {
        return new ReverseIter(this);
    }

    @Override
    public ShortIterator getShortIterator() {
        return new ForwardIter(this);
    }

    private static class ContainerShortBatchIter implements ContainerShortBatchIterator {
        private int curr;
        private final int last;

        public ContainerShortBatchIter(final SingleRangeContainer sr, final int skip) {
            curr = sr.first() + skip - 1;
            last = sr.last();
        }

        @Override
        public int next(final short[] buffer, final int offset, final int maxCount) {
            int p = 0;
            while (p < maxCount && curr < last) {
                buffer[offset + p++] = ContainerUtil.lowbits(++curr);
            }
            return p;
        }

        @Override
        public boolean hasNext() {
            return curr < last;
        }

        @Override
        public boolean forEach(final ShortConsumer sc) {
            while (curr < last) {
                if (!sc.accept(ContainerUtil.lowbits(++curr))) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public ContainerShortBatchIterator getShortBatchIterator(final int skipFromStartCount) {
        if (DEBUG && skipFromStartCount >= getCardinality()) {
            throw new IllegalArgumentException(
                    "skipFromStartCount=" + skipFromStartCount + ", cardinality=" + getCardinality());
        }
        return new ContainerShortBatchIter(this, skipFromStartCount);
    }

    private static class SearchRangeIter implements SearchRangeIterator {
        private int start;
        private final int end;
        private boolean hasNext;

        public SearchRangeIter(final SingleRangeContainer sr, final int skip) {
            start = sr.first() + skip;
            end = sr.last() + 1;
            hasNext = true;
        }

        @Override
        public boolean advance(final int v) {
            hasNext = false;
            if (end <= v) {
                return false;
            }
            // v < end.
            start = Math.max(v, start);
            return true;
        }

        @Override
        public boolean search(final ContainerUtil.TargetComparator comp) {
            hasNext = false;
            int c = comp.directionFrom(end - 1);
            if (c >= 0) {
                start = end - 1;
                return true;
            }
            if (end - start == 1) {
                return false;
            }
            c = comp.directionFrom(start);
            if (c <= 0) {
                return c == 0;
            }
            if (end - start == 2) {
                // start's it.
                return true;
            }
            int v = ContainerUtil.rangeSearch(start + 1, end - 1, comp);
            if (v >= 0) {
                start = v;
            }
            return true;
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
    }

    @Override
    public SearchRangeIterator getShortRangeIterator(final int skipFromStartCount) {
        if (DEBUG && skipFromStartCount >= getCardinality()) {
            throw new IllegalArgumentException(
                    "skipFromStartCount=" + skipFromStartCount + ", cardinality=" + getCardinality());
        }
        return new SearchRangeIter(this, skipFromStartCount);
    }

    private Container orImpl(final Container c) {
        final int begin = begin();
        final int end = end();
        if (c.isEmpty() || (begin <= c.first() && c.last() < end)) {
            return this;
        }
        return c.add(begin, end);
    }

    private Container xorImpl(final Container x) {
        return x.not(begin(), end());
    }

    @Override
    public Container not(final int negBegin, final int negEnd) {
        final int begin = begin();
        final int end = end();
        // Completely to the left?
        if (end <= negBegin) {
            if (end == negBegin) {
                return new SingleRangeContainer(begin, negEnd);
            }
            return new RunContainer(begin, end, negBegin, negEnd);
        }
        // Completely to the right?
        if (negEnd <= begin) {
            if (negEnd == begin) {
                return new SingleRangeContainer(negBegin, end);
            }
            return new RunContainer(negBegin, negEnd, begin, end);
        }
        // We have some intersection.
        final int minBegin, maxBegin;
        if (begin < negBegin) {
            minBegin = begin;
            maxBegin = negBegin;
        } else {
            minBegin = negBegin;
            maxBegin = begin;
        }
        final int minEnd, maxEnd;
        if (end < negEnd) {
            minEnd = end;
            maxEnd = negEnd;
        } else {
            minEnd = negEnd;
            maxEnd = end;
        }
        if (minBegin == maxBegin) {
            if (minEnd == maxEnd) {
                return Container.empty();
            }
            return Container.singleRange(minEnd, maxEnd);
        }
        if (minEnd == maxEnd) {
            return Container.singleRange(minBegin, maxBegin);
        }
        return Container.twoRanges(minBegin, maxBegin, minEnd, maxEnd);
    }

    @Override
    int numberOfRuns() {
        return 1;
    }

    @Override
    public Container or(final ArrayContainer x) {
        return orImpl(x);
    }

    @Override
    public Container or(final BitmapContainer x) {
        return orImpl(x);
    }

    @Override
    public Container or(final RunContainer x) {
        return orImpl(x);
    }

    @Override
    public int rank(final short lowbits) {
        final int v = ContainerUtil.toIntUnsigned(lowbits);
        final int first = first();
        final int last = last();
        if (v > last) {
            return last - first + 1;
        }
        if (v < first) {
            return 0;
        }
        return 1 + v - first;
    }

    @Override
    public Container remove(final int rangeFirst, final int rangeEnd) {
        final int rangeLast = rangeEnd - 1;
        final int first = first();
        final int last = last();
        if (rangeLast < first || last < rangeFirst) {
            return this;
        }
        // Non-null intersection.
        if (rangeFirst <= first) {
            if (last <= rangeLast) {
                return Container.empty();
            }
            // last > rangeLast.
            if (last - 1 == rangeLast) {
                return Container.singleton(ContainerUtil.lowbits(last));
            }
            return new SingleRangeContainer(rangeLast + 1, last + 1);
        }
        // first < rangeFirst.
        if (last <= rangeLast) {
            if (first + 1 == rangeFirst) {
                return Container.singleton(ContainerUtil.lowbits(first));
            }
            return new SingleRangeContainer(first, rangeFirst);
        }
        // rangeLast < last.
        if (first + 1 == rangeFirst && rangeLast + 1 == last) {
            return new TwoValuesContainer(ContainerUtil.lowbits(first), ContainerUtil.lowbits(last));
        }
        return new RunContainer(first, rangeFirst, rangeLast + 1, last + 1);
    }

    @Override
    public Container unset(final short x) {
        final int v = ContainerUtil.toIntUnsigned(x);
        final int begin = begin();
        final int end = end();
        if (v < begin || v >= end) {
            return this;
        }
        if (v == begin) {
            if (end - begin == 1) {
                return Container.empty();
            }
            if (end - begin == 2) {
                return new SingletonContainer(ContainerUtil.lowbits(begin + 1));
            }
            return new SingleRangeContainer(begin + 1, end);
        }
        if (v + 1 == end) {
            if (end - begin == 2) {
                return new SingletonContainer(ContainerUtil.lowbits(begin));
            }
            return new SingleRangeContainer(begin, end - 1);
        }
        return Container.twoRanges(begin, v, v + 1, end);
    }

    @Override
    Container unset(final short x, final PositionHint positionHint) {
        positionHint.reset();
        return unset(x);
    }

    @Override
    public Container runOptimize() {
        return this;
    }

    @Override
    public short select(final int j) {
        if (j < 0 || j >= getCardinality()) {
            throw new IllegalArgumentException("j=" + j);
        }
        return ContainerUtil.lowbits(begin() + j);
    }

    @Override
    public Container select(final int startRank, final int endRank) {
        if (startRank == 0 && endRank >= getCardinality()) {
            return this;
        }
        final int begin = begin();
        return Container.singleRange(begin + startRank, begin + endRank);
    }

    @Override
    public int find(final short x) {
        final int v = ContainerUtil.toIntUnsigned(x);
        final int begin = begin();
        if (v < begin) {
            return ~0;
        }
        final int end = end();
        if (v >= end) {
            return ~getCardinality();
        }
        return v - begin;
    }

    @Override
    public void selectRanges(final RangeConsumer outValues, final RangeIterator inPositions) {
        final int begin = begin();
        while (inPositions.hasNext()) {
            inPositions.next();
            final int pBegin = inPositions.start();
            final int pEnd = inPositions.end();
            outValues.accept(begin + pBegin, begin + pEnd);
        }
    }

    @Override
    public boolean findRanges(final RangeConsumer outPositions, final RangeIterator inValues, final int maxPos) {
        final int begin = begin();
        final int end = end();
        while (inValues.hasNext()) {
            inValues.next();
            final int vBegin = inValues.start();
            if (vBegin < begin) {
                throw new IllegalStateException("begin=" + begin + ", vBegin=" + vBegin);
            }
            final int vEnd = inValues.end();
            if (vEnd > end) {
                throw new IllegalStateException("end=" + end + ", vEnd=" + vEnd);
            }
            final int rBegin = vBegin - begin;
            if (rBegin > maxPos) {
                return true;
            }
            final int rEnd = vEnd - begin;
            final int rLast = rEnd - 1;
            if (rLast >= maxPos) {
                outPositions.accept(rBegin, maxPos + 1);
                return true;
            }
            outPositions.accept(rBegin, rEnd);
        }
        return false;
    }

    @Override
    public void trim() {}

    @Override
    public Container xor(final ArrayContainer x) {
        return xorImpl(x);
    }

    @Override
    public Container xor(final BitmapContainer x) {
        return xorImpl(x);
    }

    @Override
    public Container xor(final RunContainer x) {
        return xorImpl(x);
    }

    @Override
    public BitmapContainer toBitmapContainer() {
        return BitmapContainer.singleRange(begin(), end());
    }

    @Override
    public int nextValue(final short fromValue) {
        final int v = ContainerUtil.toIntUnsigned(fromValue);
        final int begin = begin();
        if (v < begin) {
            return begin;
        }
        final int end = end();
        if (v >= end) {
            return -1;
        }
        return v;
    }

    @Override
    public int first() {
        return ContainerUtil.toIntUnsigned(rangeFirstValue);
    }

    @Override
    public int last() {
        return ContainerUtil.toIntUnsigned(rangeLastValue);
    }

    private boolean subsetOfImpl(final Container c) {
        return c.contains(begin(), end());
    }

    @Override
    public boolean subsetOf(final ArrayContainer x) {
        return subsetOfImpl(x);
    }

    @Override
    public boolean subsetOf(final BitmapContainer x) {
        return subsetOfImpl(x);
    }

    @Override
    public boolean subsetOf(final RunContainer x) {
        return subsetOfImpl(x);
    }

    private boolean overlapsImpl(final Container c) {
        return c.overlapsRange(begin(), end());
    }

    @Override
    public boolean overlaps(final ArrayContainer x) {
        return overlapsImpl(x);
    }

    @Override
    public boolean overlaps(final BitmapContainer x) {
        return overlapsImpl(x);
    }

    @Override
    public boolean overlaps(final RunContainer x) {
        return overlapsImpl(x);
    }

    @Override
    public boolean overlapsRange(final int rangeStart, final int rangeEnd) {
        return !(end() <= rangeStart || rangeEnd <= begin());
    }

    @Override
    public void setCopyOnWrite() {}

    @Override
    public int bytesAllocated() {
        return 4;
    }

    @Override
    public int bytesUsed() {
        return 4;
    }

    @Override
    public String toString() {
        return "{ " + first() + "-" + last() + " }";
    }

    @Override
    public Container toLargeContainer() {
        return new RunContainer(begin(), end());
    }

    @Override
    public void validate() {
        final int first = first();
        final int last = last();
        if (last < first || first < 0 || last > MAX_VALUE) {
            throw new IllegalStateException("first=" + first + ", last=" + last);
        }
    }
}
