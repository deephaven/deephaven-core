package io.deephaven.engine.rowset.impl.rsp.container;

public final class EmptyContainer extends ImmutableContainer {
    private EmptyContainer() {}

    protected static final EmptyContainer instance = new EmptyContainer();

    @Override
    public Container add(final int begin, final int end) {
        final int card = end - begin;
        if (card <= 0) {
            return this;
        }
        if (card == 1) {
            return Container.singleton(ContainerUtil.lowbits(begin));
        }
        return Container.singleRange(begin, end);
    }

    @Override
    public Container set(final short x) {
        return Container.singleton(x);
    }

    @Override
    Container set(final short x, final PositionHint positionHint) {
        positionHint.reset();
        return set(x);
    }

    @Override
    public EmptyContainer and(final ArrayContainer x) {
        return this;
    }

    @Override
    public EmptyContainer and(final BitmapContainer x) {
        return this;
    }

    @Override
    public EmptyContainer and(final RunContainer x) {
        return this;
    }

    @Override
    public EmptyContainer andRange(final int start, final int end) {
        return this;
    }

    @Override
    public EmptyContainer andNot(final ArrayContainer x) {
        return this;
    }

    @Override
    public EmptyContainer andNot(final BitmapContainer x) {
        return this;
    }

    @Override
    public EmptyContainer andNot(final RunContainer x) {
        return this;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public boolean isAllOnes() {
        return false;
    }

    @Override
    public boolean contains(short x) {
        return false;
    }

    @Override
    public boolean contains(int rangeStart, int rangeEnd) {
        return rangeEnd <= rangeStart;
    }

    @Override
    protected boolean contains(final RunContainer runContainer) {
        return runContainer.isEmpty();
    }

    @Override
    protected boolean contains(final ArrayContainer arrayContainer) {
        return arrayContainer.isEmpty();
    }

    @Override
    protected boolean contains(final BitmapContainer bitmapContainer) {
        return bitmapContainer.isEmpty();
    }

    @Override
    public Container iflip(final short x) {
        return new SingletonContainer(x);
    }

    @Override
    public int getCardinality() {
        return 0;
    }

    @Override
    public boolean forEach(final ShortConsumer sc) {
        return true;
    }

    @Override
    public boolean forEach(final int rankOffset, final ShortConsumer sc) {
        return true;
    }

    @Override
    public boolean forEachRange(final int rankOffset, final ShortRangeConsumer sc) {
        return true;
    }

    private static final class Iter implements ShortAdvanceIterator, ContainerShortBatchIterator {
        @Override
        public int next(final short[] buffer, final int offset, final int maxCount) {
            return 0;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public boolean forEach(final ShortConsumer sc) {
            return true;
        }

        @Override
        public short next() {
            throw new IllegalStateException();
        }

        @Override
        public int nextAsInt() {
            throw new IllegalStateException();
        }

        @Override
        public short curr() {
            throw new IllegalStateException();
        }

        @Override
        public int currAsInt() {
            throw new IllegalStateException();
        }

        @Override
        public boolean advance(final int v) {
            return false;
        }
    }

    private static final Iter iter = new Iter();

    @Override
    public ShortAdvanceIterator getReverseShortIterator() {
        return iter;
    }

    @Override
    public ShortIterator getShortIterator() {
        return iter;
    }

    @Override
    public ContainerShortBatchIterator getShortBatchIterator(int skipFromStartCount) {
        return iter;
    }

    private static final class SearchRangeIter implements SearchRangeIterator {
        @Override
        public boolean advance(final int v) {
            return false;
        }

        @Override
        public boolean search(final ContainerUtil.TargetComparator comp) {
            return false;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public int start() {
            throw new IllegalStateException();
        }

        @Override
        public int end() {
            throw new IllegalStateException();
        }

        @Override
        public void next() {
            throw new IllegalStateException();
        }
    }

    private static final SearchRangeIter searchRangeIter = new SearchRangeIter();

    @Override
    public SearchRangeIterator getShortRangeIterator(int skipFromStartCount) {
        return searchRangeIter;
    }

    @Override
    public Container not(final int rangeStart, final int rangeEnd) {
        return Container.singleRange(rangeStart, rangeEnd);
    }

    @Override
    int numberOfRuns() {
        return 0;
    }

    @Override
    public Container or(final ArrayContainer x) {
        return x.cowRef();
    }

    @Override
    public Container or(final BitmapContainer x) {
        return x.cowRef();
    }

    @Override
    public Container or(final RunContainer x) {
        return x.cowRef();
    }

    @Override
    public int rank(final short lowbits) {
        return 0;
    }

    @Override
    public EmptyContainer remove(final int begin, final int end) {
        return this;
    }

    @Override
    public EmptyContainer unset(final short x) {
        return this;
    }

    @Override
    Container unset(final short x, final PositionHint positionHint) {
        positionHint.reset();
        return unset(x);
    }

    @Override
    public EmptyContainer runOptimize() {
        return this;
    }

    @Override
    public short select(int j) {
        throw new IllegalStateException("j=" + j);
    }

    @Override
    public Container select(final int startRank, final int endRank) {
        if (endRank <= startRank) {
            return this;
        }
        throw new IllegalArgumentException("startRank=" + startRank + ", endRank=" + endRank);
    }

    @Override
    public int find(final short x) {
        return -1;
    }

    @Override
    public void selectRanges(final RangeConsumer outValues, final RangeIterator inPositions) {
        if (inPositions.hasNext()) {
            throw new IllegalStateException();
        }
    }

    @Override
    public boolean findRanges(final RangeConsumer outPositions, final RangeIterator inValues, final int maxPos) {
        if (inValues.hasNext()) {
            throw new IllegalStateException();
        }
        return false;
    }

    @Override
    public void trim() {}

    @Override
    public Container xor(final ArrayContainer x) {
        return x.cowRef();
    }

    @Override
    public Container xor(final BitmapContainer x) {
        return x.cowRef();
    }

    @Override
    public Container xor(final RunContainer x) {
        return x.cowRef();
    }

    @Override
    public BitmapContainer toBitmapContainer() {
        return new BitmapContainer();
    }

    @Override
    public int nextValue(short fromValue) {
        throw new IllegalStateException("fromValue=" + fromValue);
    }

    @Override
    public int first() {
        throw new IllegalStateException();
    }

    @Override
    public int last() {
        throw new IllegalStateException();
    }

    @Override
    public boolean subsetOf(final ArrayContainer x) {
        return true;
    }

    @Override
    public boolean subsetOf(final BitmapContainer x) {
        return true;
    }

    @Override
    public boolean subsetOf(final RunContainer x) {
        return true;
    }

    @Override
    public boolean overlaps(final ArrayContainer x) {
        return false;
    }

    @Override
    public boolean overlaps(final BitmapContainer x) {
        return false;
    }

    @Override
    public boolean overlaps(final RunContainer x) {
        return false;
    }

    @Override
    public boolean overlapsRange(final int start, final int end) {
        return false;
    }

    @Override
    public void setCopyOnWrite() {}

    @Override
    public int bytesAllocated() {
        return 0;
    }

    @Override
    public int bytesUsed() {
        return 0;
    }

    @Override
    public String toString() {
        return "{ }";
    }

    @Override
    public Container toLargeContainer() {
        return new ArrayContainer();
    }

    @Override
    public void validate() {}
}
