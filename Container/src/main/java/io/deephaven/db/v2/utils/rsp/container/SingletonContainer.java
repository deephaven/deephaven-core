package io.deephaven.db.v2.utils.rsp.container;

public final class SingletonContainer extends ImmutableContainer {
    final short value;

    public int intValue() {
        return ContainerUtil.toIntUnsigned(value);
    }

    public SingletonContainer(final short value) {
        this.value = value;
    }

    @Override
    public Container add(final int begin, final int end) {
        final int card = end - begin;
        if (card <= 0) {
            return this;
        }
        if (card == 1) {
            return set(ContainerUtil.lowbits(begin));
        }
        final int intValue = intValue();
        if (intValue < begin) {
            if (intValue + 1 == begin) {
                return new SingleRangeContainer(intValue, end);
            }
            // intValue + 1 < begin.
            return new RunContainer(intValue, intValue + 1, begin, end);
        }
        // begin <= intValue.
        if (begin == intValue) {
            return new SingleRangeContainer(begin, end);
        }
        // begin < intValue.
        if (intValue < end) {
            return new SingleRangeContainer(begin, end);
        }
        // end <= intValue.
        if (end == intValue) {
            return new SingleRangeContainer(begin, end + 1);
        }
        // end < intValue.
        return new RunContainer(begin, end, intValue, intValue + 1);
    }

    @Override
    Container set(final short x, final PositionHint positionHint) {
        positionHint.reset();
        return set(x);
    }

    @Override
    public Container set(final short x) {
        final int intValue = intValue();
        final int v = ContainerUtil.toIntUnsigned(x);
        if (intValue < v) {
            if (intValue + 1 == v) {
                return new SingleRangeContainer(intValue, v + 1);
            }
            // intValue + 1 < v.
            return new TwoValuesContainer(value, x);
        }
        // v <= intValue.
        if (v == intValue) {
            return this;
        }
        // v < intValue.
        if (v + 1 == intValue) {
            return new SingleRangeContainer(v, intValue + 1);
        }
        // v + 1 < intRange.
        return new TwoValuesContainer(x, value);
    }

    private Container andImpl(final Container c) {
        return c.contains(value) ? this : Container.empty();
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
    public Container andRange(final int start, final int end) {
        final int v = intValue();
        if (v < start || end <= v) {
            return Container.empty();
        }
        return this;
    }

    private Container andNotImpl(final Container c) {
        return c.contains(value) ? Container.empty() : this;
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
        return false;
    }

    @Override
    public boolean contains(final short x) {
        return value == x;
    }

    @Override
    public boolean contains(final int rangeStart, final int rangeEnd) {
        return rangeEnd <= rangeStart ||
            (rangeStart == intValue() && rangeEnd - rangeStart == 1);
    }

    @Override
    protected boolean contains(final RunContainer runContainer) {
        return runContainer.nbrruns == 0 ||
            (runContainer.nbrruns == 1 &&
                runContainer.getValue(0) == value &&
                runContainer.getLength(0) == 0);
    }

    @Override
    protected boolean contains(final ArrayContainer arrayContainer) {
        return arrayContainer.cardinality == 0 ||
            (arrayContainer.cardinality == 1 && arrayContainer.content[0] == value);
    }

    @Override
    protected boolean contains(final BitmapContainer bitmapContainer) {
        return bitmapContainer.cardinality == 0 ||
            (bitmapContainer.cardinality == 1 && bitmapContainer.contains(value));
    }

    @Override
    public Container iflip(short x) {
        if (x == value) {
            return Container.empty();
        }
        final int v = ContainerUtil.toIntUnsigned(x);
        final int intValue = intValue();
        if (v < intValue) {
            if (v + 1 == intValue) {
                return new SingleRangeContainer(v, intValue + 1);
            }
            return new TwoValuesContainer(x, value);
        }
        // intValue < v.
        if (intValue + 1 == v) {
            return new SingleRangeContainer(intValue, v + 1);
        }
        return new TwoValuesContainer(value, x);
    }

    @Override
    public int getCardinality() {
        return 1;
    }

    @Override
    public boolean forEach(final ShortConsumer sc) {
        return forEach(0, sc);
    }

    @Override
    public boolean forEach(final int rankOffset, final ShortConsumer sc) {
        if (rankOffset > 0) {
            return true;
        }
        return sc.accept(value);
    }

    @Override
    public boolean forEachRange(final int rankOffset, final ShortRangeConsumer sc) {
        if (rankOffset > 0) {
            return true;
        }
        return sc.accept(value, value);
    }

    private static class Iter {
        private final short value;
        protected boolean hasNext;

        public Iter(final short value) {
            this.value = value;
            hasNext = true;
        }

        public boolean hasNext() {
            return hasNext;
        }

        public short next() {
            hasNext = false;
            return curr();
        }

        public int nextAsInt() {
            hasNext = false;
            return currAsInt();
        }

        public short curr() {
            return value;
        }

        public int currAsInt() {
            return intValue();
        }

        protected int intValue() {
            return ContainerUtil.toIntUnsigned(value);
        }
    }

    private static final class ForwardIter extends Iter implements ShortAdvanceIterator {
        public ForwardIter(final short value) {
            super(value);
        }

        @Override
        public boolean advance(final int v) {
            hasNext = false;
            return v <= intValue();
        }
    }

    public static final class ReverseIter extends Iter implements ShortAdvanceIterator {
        public ReverseIter(final short value) {
            super(value);
        }

        @Override
        public boolean advance(final int v) {
            hasNext = false;
            return intValue() <= v;
        }
    }

    @Override
    public ShortAdvanceIterator getReverseShortIterator() {
        return new ReverseIter(value);
    }

    @Override
    public ShortIterator getShortIterator() {
        return new ForwardIter(value);
    }

    private static class ContainerShortBatchIter implements ContainerShortBatchIterator {
        private final short value;
        private boolean hasNext;

        public ContainerShortBatchIter(final SingletonContainer sc) {
            value = sc.value;
            hasNext = true;
        }

        @Override
        public int next(final short[] buffer, final int offset, final int maxCount) {
            if (!hasNext || maxCount < 1) {
                return 0;
            }
            buffer[offset] = value;
            hasNext = false;
            return 1;
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public boolean forEach(final ShortConsumer sc) {
            if (!hasNext) {
                return true;
            }
            hasNext = false;
            return sc.accept(value);
        }
    }

    @Override
    public ContainerShortBatchIterator getShortBatchIterator(final int skipFromStartCount) {
        if (skipFromStartCount > 0) {
            throw new IllegalArgumentException("skipFromStartCount=" + skipFromStartCount);
        }
        return new ContainerShortBatchIter(this);
    }

    public static class SearchRangeIter implements SearchRangeIterator {
        private final int intValue;
        private boolean hasNext;

        public SearchRangeIter(final short v) {
            intValue = ContainerUtil.toIntUnsigned(v);
            hasNext = true;
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public int start() {
            return intValue;
        }

        @Override
        public int end() {
            return intValue + 1;
        }

        @Override
        public void next() {
            hasNext = false;
        }

        @Override
        public boolean advance(final int v) {
            hasNext = false;
            return v <= intValue;
        }

        @Override
        public boolean search(final ContainerUtil.TargetComparator comp) {
            hasNext = false;
            return comp.directionFrom(intValue) >= 0;
        }
    }

    @Override
    public SearchRangeIterator getShortRangeIterator(final int skipFromStartCount) {
        if (skipFromStartCount > 0) {
            throw new IllegalArgumentException("skipFromStartCount=" + skipFromStartCount);
        }
        return new SearchRangeIter(value);
    }

    private Container orImpl(final Container x) {
        return x.cowRef().iset(value);
    }

    private Container xorImpl(final Container x) {
        return x.cowRef().iflip(value);
    }

    @Override
    public Container not(final int rangeStart, final int rangeEnd) {
        if (rangeEnd <= rangeStart) {
            return this;
        }
        final int intValue = intValue();
        if (rangeEnd <= intValue) {
            if (intValue == rangeEnd) {
                return Container.singleRange(rangeStart, intValue + 1);
            }
            return Container.twoRanges(rangeStart, rangeEnd, intValue, intValue + 1);
        }
        // intValue < rangeEnd.
        if (intValue < rangeStart) {
            if (intValue == rangeStart - 1) {
                return new SingleRangeContainer(intValue, rangeEnd);
            }
            return Container.twoRanges(intValue, intValue + 1, rangeStart, rangeEnd);
        }
        // rangeStart <= intValue.
        if (rangeStart == intValue) {
            if (intValue + 1 == rangeEnd) {
                return Container.empty();
            }
            return Container.singleRange(intValue + 1, rangeEnd);
        }
        if (rangeEnd - 1 == intValue) {
            // rangeStart != rangeEnd - 1, otherwise we would have hit the earlier case.
            return Container.singleRange(rangeStart, rangeEnd - 1);
        }
        // 0 < intValue < MAX_RANGE
        //
        return Container.twoRanges(rangeStart, intValue, intValue + 1, rangeEnd);
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
        final int intLowbits = ContainerUtil.toIntUnsigned(lowbits);
        final int intValue = intValue();
        if (intLowbits < intValue) {
            return 0;
        }
        return 1;
    }

    @Override
    public Container remove(final int begin, final int end) {
        final int intValue = intValue();
        return (begin <= intValue && intValue < end) ? Container.empty() : this;
    }

    @Override
    public Container unset(final short x) {
        return (x == value) ? Container.empty() : this;
    }

    @Override
    public Container unset(final short x, final PositionHint positionHint) {
        positionHint.reset();
        return unset(x);
    }

    @Override
    public Container runOptimize() {
        return this;
    }

    @Override
    public short select(final int j) {
        if (j != 0) {
            throw new IllegalArgumentException("j=" + j);
        }
        return value;
    }

    @Override
    public Container select(final int startRank, final int endRank) {
        if (startRank != 0 || endRank != 1) {
            throw new IllegalStateException("startRank=" + startRank + ", endRank=" + endRank);
        }
        return this;
    }

    @Override
    public int find(final short x) {
        final int intValue = intValue();
        final int v = ContainerUtil.toIntUnsigned(x);
        if (v < intValue) {
            return ~0;
        }
        if (v == intValue) {
            return 0;
        }
        return ~1;
    }

    @Override
    public void selectRanges(final RangeConsumer outValues, final RangeIterator inPositions) {
        if (!inPositions.hasNext()) {
            return;
        }
        inPositions.next();
        final int start = inPositions.start();
        final int end = inPositions.end();
        if (start != 0 || end != 1) {
            throw new IllegalArgumentException("start=" + start + ", end=" + end);
        }
        final int intValue = intValue();
        outValues.accept(intValue, intValue + 1);
        if (inPositions.hasNext()) {
            throw new IllegalArgumentException("hasNext=true");
        }
    }

    @Override
    public boolean findRanges(final RangeConsumer outPositions, final RangeIterator inValues,
        final int maxPos) {
        if (maxPos < 0 || !inValues.hasNext()) {
            return false;
        }
        inValues.next();
        final int start = inValues.start();
        final int end = inValues.end();
        final int intValue = intValue();
        if (start == intValue && end == intValue + 1) {
            outPositions.accept(0, 1);
            return maxPos == 0;
        }
        throw new IllegalArgumentException("start=" + start + ", end=" + end);
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
    public Container xor(RunContainer x) {
        return xorImpl(x);
    }

    @Override
    public BitmapContainer toBitmapContainer() {
        final BitmapContainer bc = new BitmapContainer();
        bc.bitmapSet(value);
        bc.cardinality = 1;
        return bc;
    }

    @Override
    public int nextValue(final short fromValue) {
        final int intValue = intValue();
        final int intFromValue = ContainerUtil.toIntUnsigned(fromValue);
        if (intFromValue <= intValue) {
            return intValue;
        }
        return -1;
    }

    @Override
    public int first() {
        return intValue();
    }

    @Override
    public int last() {
        return intValue();
    }

    @Override
    public boolean subsetOf(final ArrayContainer x) {
        return x.contains(value);
    }

    @Override
    public boolean subsetOf(final BitmapContainer x) {
        return x.contains(value);
    }

    @Override
    public boolean subsetOf(final RunContainer x) {
        return x.contains(value);
    }

    @Override
    public boolean overlaps(final ArrayContainer x) {
        return x.contains(value);
    }

    @Override
    public boolean overlaps(final BitmapContainer x) {
        return x.contains(value);
    }

    @Override
    public boolean overlaps(final RunContainer x) {
        return x.contains(value);
    }

    @Override
    public boolean overlapsRange(final int start, final int end) {
        final int intValue = intValue();
        return start <= intValue && intValue < end;
    }

    @Override
    public void setCopyOnWrite() {}

    @Override
    public int bytesAllocated() {
        return 2;
    }

    @Override
    public int bytesUsed() {
        return 2;
    }

    @Override
    public String toString() {
        return "{ " + intValue() + " }";
    }

    @Override
    public Container toLargeContainer() {
        return new ArrayContainer(new short[] {value});
    }

    @Override
    public void validate() {}
}
