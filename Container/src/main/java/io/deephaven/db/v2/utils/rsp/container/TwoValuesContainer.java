package io.deephaven.db.v2.utils.rsp.container;

import static io.deephaven.db.v2.utils.rsp.container.PositionHint.resetIfNotNull;
import static io.deephaven.db.v2.utils.rsp.container.ContainerUtil.toIntUnsigned;
import static io.deephaven.db.v2.utils.rsp.container.MutableInteger.setIfNotNull;

public final class TwoValuesContainer extends ImmutableContainer {
    final short v1;
    final short v2;

    int v1AsInt() {
        return toIntUnsigned(v1);
    }

    int v2AsInt() {
        return toIntUnsigned(v2);
    }

    public TwoValuesContainer(final short v1, final short v2) {
        final int iv1 = toIntUnsigned(v1);
        final int iv2 = toIntUnsigned(v2);
        if (iv2 - iv1 < 2) {
            throw new IllegalStateException("iv1=" + iv1 + ", iv2=" + iv2);
        }
        this.v1 = v1;
        this.v2 = v2;
    }

    @Override
    public Container add(final int rangeStart, final int rangeEnd) {
        if (rangeEnd <= rangeStart) {
            return this;
        }
        if (rangeEnd - rangeStart == 1) {
            return set(ContainerUtil.lowbits(rangeStart));
        }
        final int iv1 = v1AsInt();
        final int iv2 = v2AsInt();
        if (iv2 < rangeStart) {
            if (iv2 + 1 == rangeStart) {
                return new RunContainer(iv1, iv1 + 1, iv2, rangeEnd);
            }
            final RunContainer c = new RunContainer(iv1, iv1 + 1, iv2, iv2 + 1);
            return c.iaddUnsafe(rangeStart, rangeEnd, 2);
        }
        // rangeStart <= iv2.
        if (rangeEnd <= iv1) {
            if (rangeEnd == iv1) {
                return new RunContainer(rangeStart, iv1 + 1, iv2, iv2 + 1);
            }
            final RunContainer c = new RunContainer(rangeStart, rangeEnd, iv1, iv1 + 1);
            return c.iaddUnsafe(iv2, iv2 + 1, 2);
        }
        // iv1 < rangeEnd.
        final boolean v1Contained = rangeStart <= iv1;
        final boolean v2Contained = iv2 < rangeEnd;
        if (v1Contained && v2Contained) {
            return new SingleRangeContainer(rangeStart, rangeEnd);
        }
        if (v1Contained) {
            // v2 not contained
            if (iv2 == rangeEnd) {
                return new SingleRangeContainer(rangeStart, iv2 + 1);
            }
            return new RunContainer(rangeStart, rangeEnd, iv2, iv2 + 1);
        }
        if (v2Contained) {
            // v1 not contained.
            if (iv1 + 1 == rangeStart) {
                return new SingleRangeContainer(iv1, rangeEnd);
            }
            return new RunContainer(iv1, iv1 + 1, rangeStart, rangeEnd);
        }
        // neither v1 nor v2 contained.
        if (iv1 + 1 == rangeStart) {
            if (iv2 == rangeEnd) {
                return new SingleRangeContainer(iv1, iv2 + 1);
            }
            return new RunContainer(iv1, rangeEnd, iv2, iv2 + 1);
        }
        final RunContainer c = new RunContainer(iv1, iv1 + 1, rangeStart, rangeEnd);
        return c.iaddUnsafe(iv2, iv2 + 1, 2);
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
        final int iv1 = v1AsInt();
        final int iv2 = v2AsInt();
        final int v = toIntUnsigned(x);
        if (v < iv1) {
            if (v + 1 == iv1) {
                setIfNotNull(positionHint, 0);
                return new RunContainer(v, iv1 + 1, iv2, iv2 + 1);
            }
            // v + 1 < iv1.
            setIfNotNull(positionHint, 1);
            return new ArrayContainer(x, v1, v2);
        }
        // iv1 <= v.
        if (iv1 == v) {
            return this;
        }
        // iv1 < v.
        if (iv1 + 1 == v) {
            if (v + 1 == iv2) {
                resetIfNotNull(positionHint);
                return new SingleRangeContainer(iv1, iv2 + 1);
            }
            setIfNotNull(positionHint, 0);
            return new RunContainer(iv1, iv1 + 2, iv2, iv2 + 1);
        }
        // iv1 + 1 < v.
        if (v < iv2) {
            if (v + 1 == iv2) {
                setIfNotNull(positionHint, 1);
                return new RunContainer(iv1, iv1 + 1, v, iv2 + 1);
            }
            // v + 1 < iv2.
            setIfNotNull(positionHint, 2);
            return new ArrayContainer(v1, x, v2);
        }
        // iv2 <= v.
        if (iv2 == v) {
            return this;
        }
        // iv2 < v.
        if (iv2 + 1 == v) {
            setIfNotNull(positionHint, 1);
            return new RunContainer(iv1, iv1 + 1, iv2, v + 1);
        }
        // iv2 + 1 < v.
        setIfNotNull(positionHint, 3);
        return new ArrayContainer(v1, v2, x);
    }

    private Container andImpl(final Container c) {
        final boolean containsv1 = c.contains(v1);
        final boolean containsv2 = c.contains(v2);
        if (containsv1 && containsv2) {
            return this;
        }
        if (containsv1) {
            return new SingletonContainer(v1);
        }
        if (containsv2) {
            return new SingletonContainer(v2);
        }
        return Container.empty();
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

    private int andCardImpl(final Container c) {
        int ac = 0;
        if (c.contains(v1)) {
            ++ac;
        }
        if (c.contains(v2)) {
            ++ac;
        }
        return ac;
    }

    @Override
    public Container andRange(final int start, final int end) {
        final int iv1 = v1AsInt();
        final int iv2 = v2AsInt();
        if (iv1 < start) {
            if (iv2 < start || end <= iv2) {
                return Container.empty();
            }
            return new SingletonContainer(v2);
        }
        // start <= iv1.
        if (end <= iv2) {
            if (iv1 < end) {
                return new SingletonContainer(v1);
            }
            return Container.empty();
        }
        return this;
    }

    private Container andNotImpl(final Container c) {
        final boolean containsv1 = c.contains(v1);
        final boolean containsv2 = c.contains(v2);
        if (containsv1 && containsv2) {
            return Container.empty();
        }
        if (containsv1) {
            return new SingletonContainer(v2);
        }
        if (containsv2) {
            return new SingletonContainer(v1);
        }
        return this;
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
        return x == v1 || x == v2;
    }

    @Override
    public boolean contains(final int rangeStart, final int rangeEnd) {
        if (rangeEnd <= rangeStart) {
            return true;
        }
        if (rangeEnd - rangeStart == 1) {
            return contains(ContainerUtil.lowbits(rangeStart));
        }
        return false;
    }

    private boolean containsImpl(final Container c) {
        if (c.isEmpty()) {
            return true;
        }
        final int card = c.getCardinality();
        if (card > 2) {
            return false;
        }
        if (card == 2) {
            return c.first() == v1AsInt() && c.last() == v2AsInt();
        }
        // card == 1.
        return contains(ContainerUtil.lowbits(c.first()));
    }

    @Override
    protected boolean contains(final RunContainer x) {
        return containsImpl(x);
    }

    @Override
    protected boolean contains(final ArrayContainer x) {
        return containsImpl(x);
    }

    @Override
    protected boolean contains(final BitmapContainer c) {
        if (c.isEmpty()) {
            return true;
        }
        final int card = c.getCardinality();
        if (card > 2) {
            return false;
        }
        if (card == 2) {
            return c.contains(v1) && c.contains(v2);
        }
        // card == 1.
        return c.contains(v1) || c.contains(v2);
    }

    @Override
    public Container iflip(final short x) {
        if (x == v1) {
            return new SingletonContainer(v2);
        }
        if (x == v2) {
            return new SingletonContainer(v1);
        }
        return set(x);
    }

    @Override
    public int getCardinality() {
        return 2;
    }

    @Override
    public boolean forEach(final ShortConsumer sc) {
        return sc.accept(v1) && sc.accept(v2);
    }

    @Override
    public boolean forEach(final int rankOffset, final ShortConsumer sc) {
        if (rankOffset > 1) {
            return true;
        }
        if (rankOffset <= 0) {
            return sc.accept(v1) && sc.accept(v2);
        }
        // randOffset == 1.
        return sc.accept(v2);
    }

    @Override
    public boolean forEachRange(final int rankOffset, final ShortRangeConsumer sc) {
        if (rankOffset > 1) {
            return true;
        }
        if (rankOffset <= 0) {
            return sc.accept(v1, v1) && sc.accept(v2, v2);
        }
        // randOffset == 1.
        return sc.accept(v2, v2);
    }

    private static abstract class IterBase {
        protected final short v1;
        protected final short v2;
        protected int pos;

        public IterBase(final TwoValuesContainer c, final int initialPos) {
            v1 = c.v1;
            v2 = c.v2;
            pos = initialPos;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        public short curr() {
            return (pos == 0) ? v1 : v2;
        }

        public int currAsInt() {
            return toIntUnsigned(curr());
        }

        protected short nextAsShort() {
            nextPos();
            return curr();
        }

        public int nextAsInt() {
            nextPos();
            return currAsInt();
        }

        protected abstract void nextPos();

        protected int v1AsInt() {
            return toIntUnsigned(v1);
        }

        protected int v2AsInt() {
            return toIntUnsigned(v2);
        }
    }

    private static class ForwardIterBase extends IterBase {
        public ForwardIterBase(final TwoValuesContainer c, final int initialPos) {
            super(c, initialPos);
        }

        public boolean hasNext() {
            return pos < 1;
        }

        @Override
        protected final void nextPos() {
            ++pos;
        }

        public boolean advance(final int v) {
            if (pos >= 1) {
                return v <= v2AsInt();
            }
            // pos == -1 || pos == 0.
            if (v <= v1AsInt()) {
                pos = 0;
                return true;
            }
            pos = 1;
            return v <= v2AsInt();
        }
    }

    private static final class ForwardIter extends ForwardIterBase implements ShortAdvanceIterator {
        public ForwardIter(final TwoValuesContainer c) {
            super(c, -1);
        }

        @Override
        public short next() {
            return nextAsShort();
        }
    }

    private static final class ReverseIter extends IterBase implements ShortAdvanceIterator {
        public ReverseIter(final TwoValuesContainer c) {
            super(c, 2);
        }

        @Override
        public short next() {
            return nextAsShort();
        }

        protected void nextPos() {
            --pos;
        }

        @Override
        public boolean hasNext() {
            return pos > 0;
        }

        @Override
        public boolean advance(final int v) {
            if (pos <= 0) {
                return v1AsInt() <= v;
            }
            // pos == 1 || pos == 2.
            if (v2AsInt() <= v) {
                pos = 1;
                return true;
            }
            pos = 0;
            return v1AsInt() <= v;
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

    public static final class ContainerShortBatchIter extends ForwardIterBase
        implements ContainerShortBatchIterator {
        public ContainerShortBatchIter(final TwoValuesContainer c, final int skip) {
            super(c, -1 + skip);
        }

        @Override
        public int next(final short[] buffer, final int offset, final int maxCount) {
            int n = 0;
            while (n < maxCount && pos < 1) {
                ++pos;
                buffer[offset + n++] = curr();
            }
            return n;
        }

        @Override
        public boolean forEach(final ShortConsumer sc) {
            while (pos < 1) {
                ++pos;
                if (!sc.accept(curr())) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public ContainerShortBatchIterator getShortBatchIterator(final int skipFromStartCount) {
        if (DEBUG && skipFromStartCount > 1) {
            throw new IllegalArgumentException("skipFromStartCount=" + skipFromStartCount);
        }
        return new ContainerShortBatchIter(this, skipFromStartCount);
    }

    private static final class SearchRangeIter extends ForwardIterBase
        implements SearchRangeIterator {
        public SearchRangeIter(final TwoValuesContainer c, final int skip) {
            super(c, -1 + skip);
        }

        @Override
        public void next() {
            nextPos();
        }

        @Override
        public boolean search(final ContainerUtil.TargetComparator comp) {
            if (pos >= 1) {
                final int c = comp.directionFrom(v2AsInt());
                return c >= 0;
            }
            if (pos == -1) {
                pos = 0;
            }
            int c = comp.directionFrom(v2AsInt());
            if (c >= 0) {
                pos = 1;
                return true;
            }
            // the position we are looking for is to the left of v2.
            c = comp.directionFrom(v1AsInt());
            return c >= 0;
        }

        @Override
        public int start() {
            return (pos == 0) ? v1AsInt() : v2AsInt();
        }

        @Override
        public int end() {
            return start() + 1;
        }
    }

    @Override
    public SearchRangeIterator getShortRangeIterator(final int skipFromStartCount) {
        if (DEBUG && skipFromStartCount > 1) {
            throw new IllegalArgumentException("skipFromStartCount=" + skipFromStartCount);
        }
        return new SearchRangeIter(this, skipFromStartCount);
    }

    private static final ThreadLocal<int[]> intBuf = ThreadLocal.withInitial(() -> new int[6]);

    @Override
    public Container not(final int rangeStart, final int rangeEnd) {
        if (rangeEnd <= rangeStart) {
            return this;
        }
        if (rangeEnd - rangeStart == 1) {
            return iflip(ContainerUtil.lowbits(rangeStart));
        }
        final int iv1 = v1AsInt();
        final int iv2 = v2AsInt();
        if (iv2 < rangeStart) {
            if (iv2 + 1 == rangeStart) {
                return new RunContainer(iv1, iv1 + 1, iv2, rangeEnd);
            }
            final RunContainer ans = new RunContainer(iv1, iv1 + 1, iv2, iv2 + 1);
            return ans.iaddUnsafe(rangeStart, rangeEnd, 2);
        }
        // rangeStart <= iv2.
        if (rangeEnd <= iv1) {
            if (rangeEnd == iv1) {
                return new RunContainer(rangeStart, iv1 + 1, iv2, iv2 + 1);
            }
            final RunContainer c = new RunContainer(rangeStart, rangeEnd, iv1, iv1 + 1);
            return c.iaddUnsafe(iv2, iv2 + 1, 2);
        }
        // iv1 < rangeEnd.
        final boolean v1Contained = rangeStart <= iv1;
        final boolean v2Contained = iv2 < rangeEnd;
        final int[] buf = intBuf.get(); // buf will contain consecutive pairs of [begin,end]
                                        // segments.
        int n = 0;
        if (v1Contained) {
            if (iv1 == rangeStart) {
                buf[n++] = iv1 + 1;
            } else {
                if (iv1 + 1 == rangeEnd) {
                    return Container.twoRanges(rangeStart, iv1, iv2, iv2 + 1);
                }
                buf[n++] = rangeStart;
                buf[n++] = iv1;
                buf[n++] = iv1 + 1;
            }
        } else {
            if (iv1 + 1 == rangeStart) {
                buf[n++] = iv1;
            } else {
                if (iv2 == rangeStart) {
                    return Container.twoRanges(iv1, iv1 + 1, iv2 + 1, rangeEnd);
                }
                buf[n++] = iv1;
                buf[n++] = iv1 + 1;
                buf[n++] = rangeStart;
            }
        }
        if (v2Contained) {
            buf[n++] = iv2;
            if (iv2 != rangeEnd - 1) {
                buf[n++] = iv2 + 1;
                buf[n++] = rangeEnd;
            }
        } else {
            if (iv2 == rangeEnd) {
                buf[n++] = iv2 + 1;
            } else {
                buf[n++] = rangeEnd;
                buf[n++] = iv2;
                buf[n++] = iv2 + 1;
            }
        }
        if (n == 2) {
            return Container.singleRange(buf[0], buf[1]);
        }
        Container c = Container.twoRanges(buf[0], buf[1], buf[2], buf[3]);
        if (n == 6) {
            c = c.iadd(buf[4], buf[5]);
        }
        return c;
    }

    @Override
    int numberOfRuns() {
        return 2;
    }

    private Container orImpl(final Container c) {
        final PositionHint hint = new PositionHint();
        final Container ans = c.set(v1, hint);
        return ans.iset(v2, hint);
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
        final int v = toIntUnsigned(lowbits);
        final int iv1 = v1AsInt();
        if (v < iv1) {
            return 0;
        }
        if (v == iv1) {
            return 1;
        }
        final int iv2 = v2AsInt();
        if (v < iv2) {
            return 1;
        }
        return 2;
    }

    @Override
    public Container remove(final int begin, final int end) {
        if (end <= begin) {
            return this;
        }
        final int iv1 = v1AsInt();
        final int iv2 = v2AsInt();
        final boolean hasv1 = begin <= iv1 && iv1 < end;
        final boolean hasv2 = begin <= iv2 && iv2 < end;
        if (hasv1) {
            if (hasv2) {
                return Container.empty();
            }
            return new SingletonContainer(v2);
        }
        if (hasv2) {
            return new SingletonContainer(v1);
        }
        return this;
    }

    @Override
    public Container unset(final short x) {
        if (x == v1) {
            return new SingletonContainer(v2);
        }
        if (x == v2) {
            return new SingletonContainer(v1);
        }
        return this;
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
        if (j == 0) {
            return v1;
        }
        if (j == 1) {
            return v2;
        }
        throw new IllegalArgumentException("j=" + j);
    }

    @Override
    public Container select(final int startRank, final int endRank) {
        if (endRank <= startRank) {
            return Container.empty();
        }
        if (startRank < 0 || endRank > 2) {
            throw new IllegalArgumentException();
        }
        final int n = endRank - startRank;
        if (startRank == 0) {
            if (n == 1) {
                return new SingletonContainer(v1);
            }
            return this;
        }
        // startRank == 1
        return new SingletonContainer(v2);
    }

    @Override
    public int find(final short x) {
        final int v = toIntUnsigned(x);
        final int iv1 = v1AsInt();
        if (v < iv1) {
            return ~0;
        }
        if (v == iv1) {
            return 0;
        }
        final int iv2 = v2AsInt();
        if (v < iv2) {
            return ~1;
        }
        if (v == iv2) {
            return 1;
        }
        // v > iv2;
        return ~2;
    }

    @Override
    public void selectRanges(final RangeConsumer outValues, final RangeIterator inPositions) {
        if (!inPositions.hasNext()) {
            return;
        }
        final int iv1 = v1AsInt();
        final int iv2 = v2AsInt();
        inPositions.next();
        int pStart = inPositions.start();
        int pEnd = inPositions.end();
        if (pStart == 0) {
            if (pEnd == 1) {
                outValues.accept(iv1, iv1 + 1);
                return;
            }
            if (pEnd == 2) {
                outValues.accept(iv1, iv1 + 1);
                outValues.accept(iv2, iv2 + 1);
                return;
            }
        } else if (pStart == 1) {
            if (pEnd == 2) {
                outValues.accept(iv2, iv2 + 1);
                return;
            }
        }
        throw new IllegalArgumentException("pStart=" + pStart + ", pEnd=" + pEnd +
            ", iv1=" + iv1 + ", iv2=" + iv2);
    }

    @Override
    public boolean findRanges(final RangeConsumer outPositions, final RangeIterator inValues,
        final int maxPos) {
        if (maxPos < 0) {
            throw new IllegalArgumentException("maxPos=" + maxPos);
        }
        final int iv1 = v1AsInt();
        final int iv2 = v2AsInt();
        int start = -1;
        int end = -1;
        boolean doThrow = false;
        boolean accept1 = false;
        boolean accept2 = false; // won't become true if maxPos == 0.
        while (inValues.hasNext()) {
            inValues.next();
            start = inValues.start();
            end = inValues.end();
            if (start <= iv1 && iv1 < end) {
                if (start < iv1 || end != iv1 + 1) {
                    doThrow = true;
                    break;
                }
                accept1 = true;
                if (maxPos == 0) {
                    break;
                }
                continue;
            }
            if (start <= iv2 && iv2 < end) {
                if (start < iv2 || end != iv2 + 1) {
                    doThrow = true;
                    break;
                }
                if (maxPos == 0) {
                    break;
                }
                accept2 = true;
                if (inValues.hasNext()) {
                    inValues.next();
                    start = inValues.start();
                    end = inValues.end();
                    doThrow = true;
                }
                break;
            }
            doThrow = true;
            break;
        }
        if (doThrow) {
            throw new IllegalArgumentException(
                "start=" + start + ", end=" + end + ", iv1=" + iv1 + ", iv2=" + iv2);
        }
        if (accept1) {
            if (accept2) {
                outPositions.accept(0, 2);
                return maxPos == 1;
            }
            outPositions.accept(0, 1);
            return maxPos == 0;
        }
        if (accept2) {
            outPositions.accept(1, 2);
            return maxPos == 1;
        }
        return false;
    }

    @Override
    public void trim() {}

    private Container xorImpl(final Container c) {
        return c.deepCopy().iflip(v1).iflip(v2);
    }

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
        final BitmapContainer b = new BitmapContainer();
        b.bitmapSet(v1);
        b.bitmapSet(v2);
        b.cardinality = 2;
        return b;
    }

    @Override
    public int nextValue(final short fromValue) {
        final int v = toIntUnsigned(fromValue);
        final int iv1 = v1AsInt();
        if (v <= iv1) {
            return iv1;
        }
        final int iv2 = v2AsInt();
        if (v <= iv2) {
            return iv2;
        }
        return -1;
    }

    @Override
    public int first() {
        return v1AsInt();
    }

    @Override
    public int last() {
        return v2AsInt();
    }

    private boolean subsetOfImpl(final Container c) {
        return c.contains(v1) && c.contains(v2);
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
        return c.contains(v1) || c.contains(v2);
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
    public boolean overlapsRange(final int start, final int end) {
        final int iv1 = v1AsInt();
        final int iv2 = v2AsInt();
        return (start <= iv1 && iv1 < end) ||
            (start <= iv2 && iv2 < end);
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
        return "{ " + v1AsInt() + "," + v2AsInt() + " }";
    }

    @Override
    public Container toLargeContainer() {
        return new ArrayContainer(new short[] {v1, v2});
    }

    @Override
    public void validate() {
        final int iv1 = v1AsInt();
        final int iv2 = v2AsInt();
        if (iv2 - iv1 < 2) {
            throw new IllegalStateException("iv1=" + iv1 + ", iv2=" + iv2);
        }
    }
}
