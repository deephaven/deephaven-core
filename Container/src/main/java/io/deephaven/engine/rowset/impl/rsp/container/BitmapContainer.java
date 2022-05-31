/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 *
 * The code in this file is a heavily modified version of the original in the RoaringBitmap library; please see
 * https://roaringbitmap.org/
 *
 */

package io.deephaven.engine.rowset.impl.rsp.container;

import java.util.NoSuchElementException;
import java.util.function.Supplier;

import static java.lang.Long.numberOfLeadingZeros;
import static java.lang.Long.numberOfTrailingZeros;
import static io.deephaven.engine.rowset.impl.rsp.container.MutableInteger.setIfNotNull;
import static io.deephaven.engine.rowset.impl.rsp.container.PositionHint.resetIfNotNull;
import static io.deephaven.engine.rowset.impl.rsp.container.ContainerUtil.*;

/**
 * Simple bitset-like container.
 */
public final class BitmapContainer extends Container implements Cloneable {
    protected static final int BITMAP_CAPACITY = MAX_RANGE / (8 * Long.BYTES);
    protected static final int BITMAP_SIZE_IN_BYTES = BITMAP_CAPACITY * Long.BYTES;

    /**
     * optimization flag: whether the cardinality of the bitmaps is maintained through branchless operations
     */
    public static final boolean USE_BRANCHLESS = true;

    /**
     * Return a bitmap iterator over this array
     *
     * @param bitmap array to be iterated over
     * @return an iterator
     */
    public static ShortAdvanceIterator getReverseShortIterator(final long[] bitmap) {
        return new ReverseShortIterator(bitmap);
    }

    /**
     * Return a bitmap iterator over this array
     *
     * @param bitmap array to be iterated over
     * @return an iterator
     */
    public static ShortIterator getShortIterator(final long[] bitmap) {
        return new ForwardShortIterator(bitmap);
    }

    final long[] bitmap;
    int cardinality;
    private boolean shared = false;

    /**
     * Create a bitmap container with all bits set to false
     */
    public BitmapContainer() {
        cardinality = 0;
        bitmap = new long[BITMAP_CAPACITY];
    }

    private BitmapContainer(final int start, final int end) {
        cardinality = end - start;
        bitmap = new long[BITMAP_CAPACITY];
        setBitmapRange(bitmap, start, end);
    }

    /**
     * Create a bitmap container with a run of ones from start to end. Caller must ensure that the range isn't so small
     * that an ArrayContainer should have been created instead
     *
     * @param start first index
     * @param end end index (exclusive)
     * @return the new container.
     */
    public static BitmapContainer singleRange(final int start, final int end) {
        return new BitmapContainer(start, end);
    }

    private BitmapContainer(final BitmapContainer other) {
        cardinality = other.cardinality;
        bitmap = new long[BITMAP_CAPACITY];
        System.arraycopy(other.bitmap, 0, bitmap, 0, bitmap.length);
    }

    /**
     * Create a new container, no copy is made.
     *
     * @param newBitmap content
     * @param newCardinality desired cardinality.
     */
    // For tests.
    BitmapContainer(final long[] newBitmap, final int newCardinality) {
        if (newBitmap.length != BITMAP_CAPACITY ||
                newCardinality < 0 ||
                newCardinality > MAX_RANGE) {
            throw new IllegalArgumentException(
                    "newBitmap.length=" + newBitmap.length + ", newCardinality=" + newCardinality);
        }
        cardinality = newCardinality;
        bitmap = newBitmap;
    }

    private int findFirstZeroBit() {
        return findFirstZeroBit(0);
    }

    private int findFirstZeroBit(final int iStart) {
        for (int i = iStart; i < bitmap.length; ++i) {
            long v = bitmap[i];
            if (v == ~(0L)) {
                continue;
            }
            return 64 * i + numberOfTrailingZeros(~v);
        }
        return -1;
    }

    Container maybeSwitchContainer() {
        final Container c = maybeSwitchContainerAfterShrinking();
        if (c != this) {
            return c;
        }
        return maybeSwitchContainerAfterGrowing();
    }

    private Container maybeSwitchContainerToArrayOrRun() {
        if (cardinality == 0) {
            return Container.empty();
        }
        if (cardinality <= ArrayContainer.SWITCH_CONTAINER_CARDINALITY_THRESHOLD) {
            return toArrayContainer();
        }
        int frontZeroes = 0;
        int i = 0;
        while (bitmap[i++] == 0) {
            ++frontZeroes;
        }
        int backZeroes = 0;
        i = BITMAP_CAPACITY - 1;
        while (bitmap[i--] == 0) {
            ++backZeroes;
        }
        final int bitsAvailableInNonZeroWords = (BITMAP_CAPACITY - frontZeroes - backZeroes) * Long.BYTES * 8;
        // One every other bit set maximizes number of runs.
        final int runsUpperBound1 = bitsAvailableInNonZeroWords / 2;
        final int zeroBitsInNonZeroWords = bitsAvailableInNonZeroWords - cardinality;
        final int runsUpperBound2 = 1 + zeroBitsInNonZeroWords; // Spread the fingers in one hand: 4 holes => 5 fingers.
        if (Math.min(runsUpperBound1, runsUpperBound2) < ArrayContainer.SWITCH_CONTAINER_CARDINALITY_THRESHOLD / 2) {
            return toRunContainer();
        }
        return this;
    }

    private Container maybeSwitchContainerAfterGrowing() {
        if (cardinality < MAX_VALUE) {
            return this;
        }
        if (cardinality == MAX_VALUE) {
            final int v = findFirstZeroBit();
            if (v == 0) {
                return Container.singleRange(1, MAX_RANGE);
            }
            if (v == MAX_VALUE) {
                return Container.singleRange(0, MAX_VALUE);
            }
            return Container.twoRanges(0, v, v + 1, MAX_RANGE);
        }
        // cardinality > MAX_VALUE => all ones.
        return Container.full();
    }

    private Container maybeSwitchContainerAfterShrinking() {
        if (cardinality == 0) {
            return Container.empty();
        }
        if (cardinality == 1) {
            return Container.singleton(lowbits(first()));
        }
        if (cardinality == 2) {
            return Container.twoValues(lowbits(first()), lowbits(last()));
        }
        if (cardinality <= ArrayContainer.SWITCH_CONTAINER_CARDINALITY_THRESHOLD) {
            return toArrayContainer();
        }
        return this;
    }

    @Override
    public Container add(final int begin, final int end) {
        // TODO: may need to convert to a RunContainer
        if (end == begin) {
            return cowRef();
        }
        if (begin > end || end > MAX_RANGE) {
            throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
        }
        final BitmapContainer ans = deepCopy();
        return ans.iaddImpl(begin, end);
    }

    @Override
    public Container iset(final short x) {
        return setImpl(x, () -> this, this::deepCopyIfShared);
    }

    @Override
    public Container set(final short x) {
        return setImpl(x, this::cowRef, this::deepCopy);
    }

    @Override
    Container iset(final short x, final PositionHint positionHint) {
        positionHint.reset();
        return iset(x);
    }

    @Override
    Container set(final short x, final PositionHint positionHint) {
        positionHint.reset();
        return set(x);
    }

    private Container setImpl(
            final short x, final Supplier<BitmapContainer> self, final Supplier<BitmapContainer> copy) {
        if (contains(x)) {
            return self.get();
        }
        final BitmapContainer ans = copy.get();
        ans.setUnsafe(x);
        return ans.maybeSwitchContainerAfterGrowing();
    }

    void setUnsafe(final short x) {
        final int xAsInt = toIntUnsigned(x);
        final long previous = bitmap[xAsInt / 64];
        long newval = previous | (1L << xAsInt);
        bitmap[xAsInt / 64] = newval;
        if (USE_BRANCHLESS) {
            cardinality += (previous ^ newval) >>> xAsInt;
        } else if (previous != newval) {
            ++cardinality;
        }
    }

    @Override
    public ArrayContainer and(final ArrayContainer value2) {
        final ArrayContainer answer = new ArrayContainer(value2.content.length);
        int c = value2.cardinality;
        for (int k = 0; k < c; ++k) {
            short v = value2.content[k];
            answer.content[answer.cardinality] = v;
            answer.cardinality += bitValue(v);
        }
        return answer;
    }

    @Override
    public Container and(final BitmapContainer value2) {
        return iandImpl(value2, false);
    }

    @Override
    public Container and(RunContainer x) {
        return x.and(this);
    }

    @Override
    public Container andRange(final int rangeStart, final int rangeEnd) {
        return andRangeImpl(false, rangeStart, rangeEnd);
    }

    @Override
    public Container iandRange(final int rangeStart, final int rangeEnd) {
        return andRangeImpl(!shared, rangeStart, rangeEnd);
    }

    private Container andRangeImpl(final boolean inPlace, final int rangeStart, final int rangeEnd) {
        if (rangeEnd <= rangeStart || isEmpty()) {
            return Container.empty();
        }
        final int first = first();
        if (first >= rangeEnd) {
            return Container.empty();
        }
        // first < end.
        if (cardinality == 1) {
            if (first < rangeStart) {
                return Container.empty();
            }
            return Container.singleton(lowbits(first));
        }
        // cardinality >= 2.
        final int last = last();
        if (last < rangeStart) {
            return Container.empty();
        }
        if (rangeStart <= first && last <= rangeEnd - 1) {
            if (inPlace) {
                return this;
            }
            return cowRef();
        }
        final int rFirst = Math.max(first, rangeStart);
        final int rLast = Math.min(last, rangeEnd - 1);
        final ValuesInRangeContext ctx = new ValuesInRangeContext(rFirst, rLast + 1);
        final int newCard = ctx.cardinalityInRange(bitmap);
        if (newCard == 0) {
            return Container.empty();
        }
        if (newCard <= 2) {
            final ValuesInRangeIter it = new ValuesInRangeIter(bitmap, ctx);
            if (!it.hasNext()) {
                throw new IllegalStateException();
            }
            final short v0 = it.next();
            if (!it.hasNext()) {
                return Container.singleton(v0);
            }
            final short v1 = it.next();
            if (it.hasNext()) {
                throw new IllegalStateException("v=" + it.next());
            }
            return Container.twoValues(v0, v1);
        }
        if (newCard <= ArrayContainer.SWITCH_CONTAINER_CARDINALITY_THRESHOLD) {
            ArrayContainer ans = new ArrayContainer(newCard);
            final ValuesInRangeIter it = new ValuesInRangeIter(bitmap, ctx);
            while (it.hasNext()) {
                ans.content[ans.cardinality++] = it.next();
            }
            return ans;
        }

        final BitmapContainer ans;
        if (inPlace) {
            ans = this;
        } else {
            ans = new BitmapContainer();
        }
        long v = bitmap[ctx.iFirst] & ctx.maskFirst;
        if (ctx.iFirst == ctx.iLast) {
            v &= ctx.maskLast;
            ans.bitmap[ctx.iFirst] = v;
            ans.cardinality = Long.bitCount(v);
            return ans;
        }
        ans.bitmap[ctx.iFirst] = v;
        ans.cardinality = Long.bitCount(v);
        for (int i = ctx.iFirst + 1; i < ctx.iLast; ++i) {
            v = bitmap[i];
            ans.bitmap[i] = v;
            ans.cardinality += Long.bitCount(v);
        }
        v = bitmap[ctx.iLast] & ctx.maskLast;
        ans.bitmap[ctx.iLast] = v;
        ans.cardinality += Long.bitCount(v);
        return ans;
    }

    @Override
    public Container andNot(final ArrayContainer value2) {
        if (value2.isEmpty()) {
            return cowRef();
        }
        final BitmapContainer answer = deepCopy();
        int c = value2.cardinality;
        for (int k = 0; k < c; ++k) {
            short v = value2.content[k];
            final int i = toIntUnsigned(v) >>> 6;
            long w = answer.bitmap[i];
            long aft = w & (~(1L << v));
            answer.bitmap[i] = aft;
            answer.cardinality -= (w ^ aft) >>> v;
        }
        return answer.maybeSwitchContainerAfterShrinking();
    }

    @Override
    public Container andNot(final BitmapContainer value2) {
        if (value2.isEmpty()) {
            return cowRef();
        }
        return iandNotImpl(value2, false);
    }

    @Override
    public Container andNot(final RunContainer x) {
        if (x.isEmpty()) {
            return cowRef();
        }
        // could be rewritten as return andNot(x.toBitmapOrArrayContainer());
        final BitmapContainer answer = deepCopy();
        for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
            int start = toIntUnsigned(x.getValue(rlepos));
            int end = start + toIntUnsigned(x.getLength(rlepos)) + 1;
            int prevOnesInRange = answer.cardinalityInRange(start, end);
            resetBitmapRange(answer.bitmap, start, end);
            answer.updateCardinality(prevOnesInRange, 0);
        }
        return answer.maybeSwitchContainerAfterShrinking();
    }

    @Override
    public BitmapContainer cowRef() {
        setCopyOnWrite();
        return this;
    }

    @Override
    public BitmapContainer deepCopy() {
        return new BitmapContainer(this);
    }

    @Override
    public boolean isEmpty() {
        return cardinality == 0;
    }

    @Override
    public boolean isAllOnes() {
        return cardinality == MAX_RANGE;
    }

    private static int computeCardinality(final BitmapContainer bc) {
        int cardinality = 0;
        for (int k = 0; k < bc.bitmap.length; k++) {
            cardinality += Long.bitCount(bc.bitmap[k]);
        }
        return cardinality;
    }

    protected int cardinalityInRange(int start, int end) {
        if (end - start > MAX_RANGE / 2) {
            int before = cardinalityInBitmapRange(bitmap, 0, start);
            int after = cardinalityInBitmapRange(bitmap, end, MAX_RANGE);
            return cardinality - before - after;
        }
        return cardinalityInBitmapRange(bitmap, start, end);
    }

    protected void updateCardinality(int prevOnes, int newOnes) {
        int oldCardinality = cardinality;
        cardinality = oldCardinality - prevOnes + newOnes;
    }

    @Override
    public boolean contains(final short i) {
        final int x = toIntUnsigned(i);
        return (bitmap[x / 64] & (1L << x)) != 0;
    }

    @Override
    public boolean contains(final int rangeStart, final int rangeEnd) {
        final ValuesInRangeContext ctx = new ValuesInRangeContext(rangeStart, rangeEnd);
        if (ctx.iFirst == ctx.iLast) {
            return ((bitmap[ctx.iLast] & ctx.maskFirst & ctx.maskLast) == (ctx.maskFirst & ctx.maskLast));
        }
        if ((bitmap[ctx.iFirst] & ctx.maskFirst) != ctx.maskFirst) {
            return false;
        }
        if ((bitmap[ctx.iLast] & ctx.maskLast) != ctx.maskLast) {
            return false;
        }
        for (int i = ctx.iFirst + 1; i < bitmap.length && i < ctx.iLast; ++i) {
            if (bitmap[i] != -1L) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected boolean contains(final BitmapContainer bitmapContainer) {
        if (cardinality < bitmapContainer.cardinality) {
            return false;
        }
        for (int i = 0; i < bitmapContainer.bitmap.length; ++i) {
            final long v = bitmapContainer.bitmap[i];
            if ((bitmap[i] & v) != v) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected boolean contains(final RunContainer runContainer) {
        final int numRuns = runContainer.numberOfRuns();
        if (cardinality < numRuns) {
            return false;
        }
        for (int i = 0; i < numRuns; ++i) {
            int start = toIntUnsigned(runContainer.getValue(i));
            int length = toIntUnsigned(runContainer.getLength(i));
            if (!contains(start, start + length + 1)) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected boolean contains(final ArrayContainer arrayContainer) {
        if (cardinality < arrayContainer.cardinality) {
            return false;
        }
        for (int i = 0; i < arrayContainer.cardinality; ++i) {
            if (!contains(arrayContainer.content[i])) {
                return false;
            }
        }
        return true;
    }

    protected long bitValue(final short i) {
        final int x = toIntUnsigned(i);
        return (bitmap[x / 64] >>> x) & 1;
    }

    /**
     * Fill the array with set bits
     *
     * @param array container (should be sufficiently large)
     */
    protected void fillArray(final short[] array) {
        int pos = 0;
        int base = 0;
        for (long bits : bitmap) {
            while (bits != 0) {
                array[pos++] = (short) (base + numberOfTrailingZeros(bits));
                bits &= bits - 1;
            }
            base += 64;
        }
    }

    int fillArrayWithSkipValue(final short[] array, final short valueToSkip, final PositionHint positionHintOut) {
        int pos = 0;
        int base = 0;
        for (long bits : bitmap) {
            while (bits != 0) {
                final short v = (short) (base + numberOfTrailingZeros(bits));
                if (v != valueToSkip) {
                    array[pos++] = v;
                } else {
                    setIfNotNull(positionHintOut, pos);
                }
                bits &= bits - 1;
            }
            base += 64;
        }
        return pos;
    }

    @Override
    public Container iflip(final short x) {
        final int xAsInt = toIntUnsigned(x);
        int index = xAsInt / 64;
        long bef = bitmap[index];
        long mask = 1L << xAsInt;
        final boolean isOnAndWillBeTurnedOff = (bef & mask) != 0;
        if (isOnAndWillBeTurnedOff && cardinality <= ArrayContainer.SWITCH_CONTAINER_CARDINALITY_THRESHOLD) {
            if (cardinality > 3) {
                final ArrayContainer ac = new ArrayContainer(cardinality - 1);
                ac.loadDataWithSkipValue(this, x, null);
                return ac;
            }
            // cardinality in { 1, 2, 3 }.
            if (cardinality == 1) {
                return Container.empty();
            }
            // cardinality is either 2 or 3, and we are going to be removing an element.
            int first = -1;
            int second = -1;
            final ShortIterator it = getShortIterator();
            while (it.hasNext()) {
                final int v = it.nextAsInt();
                if (v == xAsInt) {
                    continue;
                }
                if (first == -1) {
                    first = v;
                    continue;
                }
                second = v;
                break;
            }
            if (first != -1) {
                if (second != -1) {
                    if (first + 1 == second) {
                        return makeSingleRangeContainer(first, second + 1);
                    }
                    return makeTwoValuesContainer(lowbits(first), lowbits(second));
                }
                return makeSingletonContainer(lowbits(first));
            }
            throw new IllegalStateException("first=-1");
        }

        final BitmapContainer ans = deepCopyIfShared();
        ans.cardinality += 1 - 2 * ((bef & mask) >>> xAsInt);
        ans.bitmap[index] ^= mask;
        if (isOnAndWillBeTurnedOff) {
            return ans.maybeSwitchContainerAfterShrinking();
        }
        return ans.maybeSwitchContainerAfterGrowing();
    }

    @Override
    public int getCardinality() {
        return cardinality;
    }

    static final class ReverseShortIterator implements ShortAdvanceIterator {
        private int curr;
        private long nextWord;
        private int nextPos;
        private long[] bitmap;

        ReverseShortIterator(final long[] bitmap) {
            wrap(bitmap);
        }

        @Override
        public boolean hasNext() {
            return nextPos >= 0;
        }

        private void eatZeroes() {
            while (nextWord == 0) {
                --nextPos;
                if (nextPos < 0) {
                    break;
                }
                nextWord = bitmap[nextPos];
            }
        }

        @Override
        public short next() {
            final int shift = Long.numberOfLeadingZeros(nextWord) + 1;
            curr = (nextPos + 1) * 64 - shift;
            nextWord &= ~(1L << (64 - shift));
            eatZeroes();
            return (short) curr;
        }

        @Override
        public int currAsInt() {
            return curr;
        }

        @Override
        public short curr() {
            return (short) curr;
        }

        @Override
        public int nextAsInt() {
            return toIntUnsigned(next());
        }

        @Override
        public boolean advance(final int v) {
            if (curr == -1) {
                if (!hasNext()) {
                    return false;
                }
                next();
            }
            if (v >= curr) {
                return true;
            }
            nextPos = v >> 6;
            final long mod64 = v & 63;
            final long mask;
            if (mod64 != 63) {
                mask = (1L << (mod64 + 1)) - 1L;
            } else {
                mask = -1L;
            }
            nextWord = bitmap[nextPos] & mask;
            int savedPos = nextPos;
            eatZeroes();
            if (nextPos < 0) {
                while (bitmap[savedPos] == 0) {
                    ++savedPos; // there is some nonzero element otherwise we would have returned earlier.
                }
                curr = (savedPos + 1) * 64 - Long.numberOfLeadingZeros(bitmap[savedPos]) - 1;
                return false;
            }
            next();
            return true;
        }

        void wrap(final long[] b) {
            curr = -1;
            bitmap = b;
            for (nextPos = bitmap.length - 1; nextPos >= 0; --nextPos) {
                if ((nextWord = bitmap[nextPos]) != 0) {
                    break;
                }
            }
        }
    }

    @Override
    public ShortAdvanceIterator getReverseShortIterator() {
        return new ReverseShortIterator(bitmap);
    }

    static class ForwardShortIterator implements ShortIterator {
        protected final long[] bitmap;
        protected long w;
        protected int x;

        ForwardShortIterator(final long[] p) {
            bitmap = p;
            for (x = 0; x < bitmap.length; ++x) {
                if ((w = bitmap[x]) != 0) {
                    break;
                }
            }
        }

        @Override
        public boolean hasNext() {
            return x < bitmap.length;
        }

        @Override
        public short next() {
            final short answer = (short) (x * 64 + numberOfTrailingZeros(w));
            w &= (w - 1);
            while (w == 0) {
                ++x;
                if (x == bitmap.length) {
                    break;
                }
                w = bitmap[x];
            }
            return answer;
        }

        @Override
        public int nextAsInt() {
            return toIntUnsigned(next());
        }
    }

    static class ValuesInRangeContext {
        public final int iFirst;
        public final int iLast;
        public final long maskFirst;
        public final long maskLast;

        public ValuesInRangeContext(final int rangeStart, final int rangeEnd) {
            iFirst = rangeStart >>> 6;
            final int max = rangeEnd - 1;
            iLast = max >>> 6;
            maskFirst = ~((1L << rangeStart) - 1);
            final long lastShift = (1L << rangeEnd) - 1;
            maskLast = (lastShift == 0) ? -1 : lastShift;
        }

        public int cardinalityInRange(final long[] bitmap) {
            long w = bitmap[iFirst] & maskFirst;
            if (iFirst == iLast) {
                w &= maskLast;
                return Long.bitCount(w);
            }
            int card = Long.bitCount(w);
            for (int i = iFirst + 1; i < iLast; ++i) {
                card += Long.bitCount(bitmap[i]);
            }
            card += Long.bitCount(bitmap[iLast] & maskLast);
            return card;
        }
    }

    static class ValuesInRangeIter implements ShortIterator {
        private final long[] bitmap;
        private int x;
        private final int xlast;
        private final long lastMask;
        private long w;

        private void eatZeroesForward() {
            while (w == 0 && x < xlast) {
                w = bitmap[++x];
            }
            if (x == xlast) {
                w &= lastMask;
            }
            if (w == 0) {
                ++x;
            }
        }

        public ValuesInRangeIter(final long[] bitmap, final ValuesInRangeContext ctx) {
            this.bitmap = bitmap;
            lastMask = ctx.maskLast;
            xlast = ctx.iLast;
            x = ctx.iFirst;
            w = bitmap[x] & ctx.maskFirst;
            if (x == xlast) {
                w &= lastMask;
                if (w == 0) {
                    ++x;
                }
                return;
            }
            eatZeroesForward();
        }

        public ValuesInRangeIter(final long[] bitmap, final int rangeStart, final int rangeEnd) {
            this(bitmap, new ValuesInRangeContext(rangeStart, rangeEnd));
        }

        @Override
        public boolean hasNext() {
            return x <= xlast;
        }

        @Override
        public short next() {
            final short answer = (short) (x * 64 + numberOfTrailingZeros(w));
            w &= (w - 1);
            eatZeroesForward();
            return answer;
        }

        @Override
        public int nextAsInt() {
            return toIntUnsigned(next());
        }
    }

    @Override
    public ShortIterator getShortIterator() {
        return new ForwardShortIterator(bitmap);
    }

    @Override
    public ContainerShortBatchIterator getShortBatchIterator(final int skipCount) {
        if (DEBUG && skipCount != 0 && skipCount >= cardinality) {
            throw new IllegalArgumentException("initialSeek=" + skipCount);
        }
        return new BitmapShortBatchIterator(this, skipCount);
    }

    @Override
    public SearchRangeIterator getShortRangeIterator(final int initialSeek) {
        if (DEBUG && initialSeek != 0 && initialSeek >= cardinality) {
            throw new IllegalArgumentException("initialSeek=" + initialSeek);
        }
        return new BitmapContainerRangeIterator(this, initialSeek);
    }

    @Override
    public Container iadd(final int begin, final int end) {
        // TODO: may need to convert to a RunContainer
        if (end == begin) {
            return this;
        }
        if (begin > end || end > MAX_RANGE) {
            throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
        }
        final BitmapContainer ans = deepCopyIfShared();
        return ans.iaddImpl(begin, end);
    }

    private Container iaddImpl(final int begin, final int end) {
        int prevOnesInRange = cardinalityInRange(begin, end);
        setBitmapRange(bitmap, begin, end);
        updateCardinality(prevOnesInRange, end - begin);
        return maybeSwitchContainerAfterGrowing();
    }

    @Override
    public Container iappend(final int begin, final int end) {
        final BitmapContainer ans = deepCopyIfShared();
        setBitmapRange(ans.bitmap, begin, end);
        ans.cardinality += end - begin;
        return ans.maybeSwitchContainerAfterGrowing();
    }

    @Override
    public Container iand(final ArrayContainer b2) {
        // We always produce an ArrayContainer result for and(ArrayContainer), so this will never be in-place.
        return and(b2);
    }

    @Override
    public Container iand(final BitmapContainer b2) {
        return iandImpl(b2, true);
    }

    private Container iandImpl(final BitmapContainer b2, final boolean inPlace) {
        if (b2.isEmpty()) {
            return Container.empty();
        }
        int newCardinality = 0;
        int ixFirstNonZero = -1;
        int ixSecondNonZero = -1;
        for (int k = 0; k < bitmap.length; ++k) {
            final long r = bitmap[k] & b2.bitmap[k];
            if (r == 0) {
                continue;
            }
            if (ixFirstNonZero == -1) {
                ixFirstNonZero = k;
            } else if (ixSecondNonZero == -1) {
                ixSecondNonZero = k;
            }
            newCardinality += Long.bitCount(r);
        }
        if (newCardinality == 0) {
            return Container.empty();
        }
        if (newCardinality == 1) {
            final long v = bitmap[ixFirstNonZero] & b2.bitmap[ixFirstNonZero];
            return Container.singleton(lowbits(64 * ixFirstNonZero + numberOfTrailingZeros(v)));
        }
        if (newCardinality == 2) {
            final long v0word = bitmap[ixFirstNonZero] & b2.bitmap[ixFirstNonZero];
            final int v0 = 64 * ixFirstNonZero + numberOfTrailingZeros(v0word);
            final int v1;
            if (ixSecondNonZero == -1) {
                // We want the last (=second) bit set in the and result.
                v1 = (ixFirstNonZero + 1) * 64 - Long.numberOfLeadingZeros(v0word) - 1;
            } else {
                final long v1word = bitmap[ixSecondNonZero] & b2.bitmap[ixSecondNonZero];
                v1 = 64 * ixSecondNonZero + numberOfTrailingZeros(v1word);
            }
            return Container.twoValues(lowbits(v0), lowbits(v1));
        }
        if (newCardinality > ArrayContainer.SWITCH_CONTAINER_CARDINALITY_THRESHOLD) {
            final BitmapContainer ans = inPlace ? deepCopyIfShared() : new BitmapContainer();
            for (int k = 0; k < ans.bitmap.length; ++k) {
                ans.bitmap[k] = bitmap[k] & b2.bitmap[k];
            }
            ans.cardinality = newCardinality;
            return ans;
        }
        final ArrayContainer ac = new ArrayContainer(newCardinality);
        fillArrayAND(ac.content, bitmap, b2.bitmap);
        ac.cardinality = newCardinality;
        return ac;
    }

    @Override
    public Container iand(final RunContainer x) {
        if (x.isEmpty()) {
            return Container.empty();
        }
        // could probably be replaced with return iand(x.toBitmapOrArrayContainer());
        final int card = x.getCardinality();
        if (card <= ArrayContainer.SWITCH_CONTAINER_CARDINALITY_THRESHOLD) {
            // no point in doing it in-place
            final ArrayContainer answer = new ArrayContainer(card);
            answer.cardinality = 0;
            for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
                int runStart = toIntUnsigned(x.getValue(rlepos));
                int runEnd = runStart + toIntUnsigned(x.getLength(rlepos));
                for (int runValue = runStart; runValue <= runEnd; ++runValue) {
                    answer.content[answer.cardinality] = (short) runValue;
                    answer.cardinality += bitValue((short) runValue);
                }
            }
            return answer.maybeSwitchContainer();
        }

        final BitmapContainer ans = deepCopyIfShared();
        return ans.iandImpl(x);
    }

    private Container iandImpl(final RunContainer x) {
        int start = 0;
        for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
            int end = toIntUnsigned(x.getValue(rlepos));
            int prevOnes = cardinalityInRange(start, end);
            resetBitmapRange(bitmap, start, end);
            updateCardinality(prevOnes, 0);
            start = end + toIntUnsigned(x.getLength(rlepos)) + 1;
        }
        int ones = cardinalityInRange(start, MAX_RANGE);
        resetBitmapRange(bitmap, start, MAX_RANGE);
        updateCardinality(ones, 0);
        return maybeSwitchContainerAfterShrinking();
    }

    @Override
    public Container iandNot(final ArrayContainer b2) {
        if (b2.isEmpty()) {
            return this;
        }
        final BitmapContainer ans = deepCopyIfShared();
        return ans.iandNotImpl(b2);
    }

    private Container iandNotImpl(final ArrayContainer b2) {
        for (int k = 0; k < b2.cardinality; ++k) {
            final int x = toIntUnsigned(b2.content[k]);
            int index = x / 64;
            long bef = bitmap[index];
            long mask = 1L << x;
            long aft = bef & (~mask);
            cardinality -= (aft - bef) >>> 63;
            bitmap[index] = aft;
        }
        return maybeSwitchContainerAfterShrinking();
    }

    @Override
    public Container iandNot(final BitmapContainer b2) {
        if (b2.isEmpty()) {
            return this;
        }
        return iandNotImpl(b2, true);
    }

    private Container iandNotImpl(final BitmapContainer b2, final boolean inPlace) {
        int newCardinality = 0;
        int ixFirstNonZero = -1;
        int ixSecondNonZero = -1;
        for (int k = 0; k < bitmap.length; ++k) {
            final long v = bitmap[k] & ~b2.bitmap[k];
            if (v == 0) {
                continue;
            }
            if (ixFirstNonZero == -1) {
                ixFirstNonZero = k;
            } else if (ixSecondNonZero == -1) {
                ixSecondNonZero = k;
            }
            newCardinality += Long.bitCount(v);
        }
        if (newCardinality == 0) {
            return Container.empty();
        }
        if (newCardinality == 1) {
            final long v = bitmap[ixFirstNonZero] & ~b2.bitmap[ixFirstNonZero];
            return Container.singleton(lowbits(64 * ixFirstNonZero + numberOfTrailingZeros(v)));
        }
        if (newCardinality == 2) {
            final long v0word = bitmap[ixFirstNonZero] & ~b2.bitmap[ixFirstNonZero];
            final int v0 = 64 * ixFirstNonZero + numberOfTrailingZeros(v0word);
            final int v1;
            if (ixSecondNonZero == -1) {
                v1 = (ixFirstNonZero + 1) * 64 - Long.numberOfLeadingZeros(v0word) - 1;
            } else {
                final long v2word = bitmap[ixSecondNonZero] & ~b2.bitmap[ixSecondNonZero];
                v1 = 64 * ixSecondNonZero + (63 - numberOfLeadingZeros(v2word));
            }
            return Container.twoValues(lowbits(v0), lowbits(v1));
        }
        if (newCardinality > ArrayContainer.SWITCH_CONTAINER_CARDINALITY_THRESHOLD) {
            final BitmapContainer ans = inPlace ? deepCopyIfShared() : new BitmapContainer();
            for (int k = 0; k < bitmap.length; ++k) {
                ans.bitmap[k] = bitmap[k] & ~b2.bitmap[k];
            }
            ans.cardinality = newCardinality;
            return ans;
        }
        final ArrayContainer ac = new ArrayContainer(newCardinality);
        fillArrayANDNOT(ac.content, bitmap, b2.bitmap);
        ac.cardinality = newCardinality;
        return ac.maybeSwitchContainer();
    }

    @Override
    public Container iandNot(RunContainer x) {
        if (x.isEmpty()) {
            return this;
        }
        final BitmapContainer ans = deepCopyIfShared();
        return ans.iandNotImpl(x);
    }

    private Container iandNotImpl(final RunContainer x) {
        // could probably be replaced with return iandNot(x.toBitmapOrArrayContainer());
        for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
            int start = toIntUnsigned(x.getValue(rlepos));
            int end = start + toIntUnsigned(x.getLength(rlepos)) + 1;
            int prevOnesInRange = cardinalityInRange(start, end);
            resetBitmapRange(bitmap, start, end);
            updateCardinality(prevOnesInRange, 0);
        }
        return maybeSwitchContainerAfterShrinking();
    }

    @Override
    public Container inot(final int firstOfRange, final int lastOfRange) {
        final BitmapContainer ans = deepCopyIfShared();
        return ans.inotImpl(firstOfRange, lastOfRange);
    }

    private Container inotImpl(final int firstOfRange, final int lastOfRange) {
        int prevOnes = cardinalityInRange(firstOfRange, lastOfRange);
        flipBitmapRange(bitmap, firstOfRange, lastOfRange);
        updateCardinality(prevOnes, lastOfRange - firstOfRange - prevOnes);
        return maybeSwitchContainer();
    }

    @Override
    public Container ior(final ArrayContainer value2) {
        if (shared) {
            return or(value2);
        }
        if (isAllOnes()) {
            return Container.full();
        }
        if (value2.isEmpty()) {
            return this;
        }
        if (isEmpty()) {
            return value2.cowRef();
        }
        final BitmapContainer ans = deepCopyIfShared();
        return ans.iorImpl(value2);
    }

    private BitmapContainer iorImpl(final ArrayContainer value2) {
        int c = value2.cardinality;
        for (int k = 0; k < c; ++k) {
            final int i = toIntUnsigned(value2.content[k]) >>> 6;

            long bef = bitmap[i];
            long aft = bef | (1L << value2.content[k]);
            bitmap[i] = aft;
            if (USE_BRANCHLESS) {
                cardinality += (bef - aft) >>> 63;
            } else {
                if (bef != aft) {
                    cardinality++;
                }
            }
        }
        return this;
    }

    @Override
    public Container ior(final BitmapContainer b2) {
        if (shared) {
            return or(b2);
        }
        if (isAllOnes() || b2.isAllOnes()) {
            return Container.full();
        }
        if (b2.isEmpty()) {
            return this;
        }
        if (isEmpty()) {
            return b2.cowRef();
        }
        final BitmapContainer ans = deepCopyIfShared();
        return ans.iorImpl(b2);
    }

    private Container iorImpl(final BitmapContainer b2) {
        cardinality = 0;
        for (int k = 0; k < bitmap.length; k++) {
            long w = bitmap[k] | b2.bitmap[k];
            bitmap[k] = w;
            cardinality += Long.bitCount(w);
        }
        if (isAllOnes()) {
            return Container.full();
        }
        return this;
    }

    @Override
    public Container ior(final RunContainer x) {
        if (shared) {
            return or(x);
        }
        if (isAllOnes()) {
            return Container.full();
        }
        if (x.isEmpty()) {
            return this;
        }
        if (isEmpty() || x.isAllOnes()) {
            return x.cowRef();
        }
        return iorImpl(x);
    }

    private Container iorImpl(final RunContainer x) {
        // could probably be replaced with return ior(x.toBitmapOrArrayContainer());
        for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
            int start = toIntUnsigned(x.getValue(rlepos));
            int end = start + toIntUnsigned(x.getLength(rlepos)) + 1;
            int prevOnesInRange = cardinalityInRange(start, end);
            setBitmapRange(bitmap, start, end);
            updateCardinality(prevOnesInRange, end - start);
        }
        if (isAllOnes()) {
            return Container.full();
        }
        return this;
    }

    @Override
    public Container iremove(final int begin, final int end) {
        if (end == begin) {
            return this;
        }
        if (begin > end || end > MAX_RANGE) {
            throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
        }
        final BitmapContainer ans = deepCopyIfShared();
        return ans.iremoveImpl(begin, end);
    }

    private Container iremoveImpl(final int begin, final int end) {
        int prevOnesInRange = cardinalityInRange(begin, end);
        resetBitmapRange(bitmap, begin, end);
        updateCardinality(prevOnesInRange, 0);
        return maybeSwitchContainerAfterShrinking();
    }

    @Override
    public Container ixor(final ArrayContainer value2) {
        if (shared) {
            return xor(value2);
        }
        if (value2.isEmpty()) {
            return this;
        }
        if (isEmpty()) {
            return value2.cowRef();
        }
        return ixorImpl(value2);
    }

    private Container ixorImpl(final ArrayContainer value2) {
        int c = value2.cardinality;
        for (int k = 0; k < c; ++k) {
            short vc = value2.content[k];
            long mask = 1L << vc;
            final int index = toIntUnsigned(vc) >>> 6;
            long ba = bitmap[index];
            // TODO: check whether a branchy version could be faster
            cardinality += 1 - 2 * ((ba & mask) >>> vc);
            bitmap[index] = ba ^ mask;
        }
        return maybeSwitchContainer();
    }


    @Override
    public Container ixor(final BitmapContainer b2) {
        if (shared) {
            return xor(b2);
        }
        if (b2.isEmpty()) {
            return this;
        }
        if (isEmpty()) {
            return b2.cowRef();
        }
        int newCardinality = 0;
        for (int k = 0; k < bitmap.length; ++k) {
            newCardinality += Long.bitCount(bitmap[k] ^ b2.bitmap[k]);
        }
        if (newCardinality > ArrayContainer.SWITCH_CONTAINER_CARDINALITY_THRESHOLD) {
            for (int k = 0; k < bitmap.length; ++k) {
                bitmap[k] = bitmap[k] ^ b2.bitmap[k];
            }
            cardinality = newCardinality;
            return this;
        }
        final ArrayContainer ac = new ArrayContainer(newCardinality);
        fillArrayXOR(ac.content, bitmap, b2.bitmap);
        ac.cardinality = newCardinality;
        return ac.maybeSwitchContainer();
    }

    @Override
    public Container ixor(final RunContainer x) {
        if (shared) {
            return xor(x);
        }
        if (x.isEmpty()) {
            return this;
        }
        if (isEmpty()) {
            return x.cowRef();
        }
        // could probably be replaced with return ixor(x.toBitmapOrArrayContainer());
        for (int rlepos = 0; rlepos < x.nbrruns; ++rlepos) {
            int start = toIntUnsigned(x.getValue(rlepos));
            int end = start + toIntUnsigned(x.getLength(rlepos)) + 1;
            int prevOnes = cardinalityInRange(start, end);
            flipBitmapRange(bitmap, start, end);
            updateCardinality(prevOnes, end - start - prevOnes);
        }
        return maybeSwitchContainer();
    }

    protected void loadData(final ArrayContainer arrayContainer) {
        cardinality = arrayContainer.cardinality;
        for (int k = 0; k < arrayContainer.cardinality; ++k) {
            final short x = arrayContainer.content[k];
            bitmapSet(x);
        }
    }

    void bitmapSet(final short x) {
        bitmap[toIntUnsigned(x) / 64] |= (1L << x);
    }

    /**
     * Find the index of the next set bit greater or equal to i, returns -1 if none found.
     *
     * @param i starting index
     * @return index of the next set bit
     */
    public int nextSetBit(final int i) {
        int x = i >> 6; // i / 64 with sign extension
        long w = bitmap[x];
        w >>>= i;
        if (w != 0) {
            return i + numberOfTrailingZeros(w);
        }
        for (++x; x < bitmap.length; ++x) {
            if (bitmap[x] != 0) {
                return x * 64 + numberOfTrailingZeros(bitmap[x]);
            }
        }
        return -1;
    }


    @Override
    public Container not(final int firstOfRange, final int lastOfRange) {
        final BitmapContainer answer = deepCopy();
        return answer.inot(firstOfRange, lastOfRange);
    }

    /**
     * Computes the number of runs
     *
     * @return the number of runs
     */
    @Override
    int numberOfRuns() {
        int numRuns = 0;
        long nextWord = bitmap[0];

        for (int i = 0; i < bitmap.length - 1; i++) {
            long word = nextWord;
            nextWord = bitmap[i + 1];
            numRuns += Long.bitCount((~word) & (word << 1)) + ((word >>> 63) & ~nextWord);
        }

        final long word = nextWord;
        numRuns += Long.bitCount((~word) & (word << 1));
        if ((word & 0x8000000000000000L) != 0) {
            numRuns++;
        }

        return numRuns;
    }

    @Override
    public Container or(final ArrayContainer value2) {
        if (isAllOnes()) {
            return Container.full();
        }
        if (value2.isEmpty()) {
            return cowRef();
        }
        if (isEmpty()) {
            return value2.cowRef();
        }
        final BitmapContainer answer = deepCopy();
        int c = value2.cardinality;
        for (int k = 0; k < c; ++k) {
            short v = value2.content[k];
            final int i = toIntUnsigned(v) >>> 6;
            long w = answer.bitmap[i];
            long aft = w | (1L << v);
            answer.bitmap[i] = aft;
            if (USE_BRANCHLESS) {
                answer.cardinality += (w - aft) >>> 63;
            } else {
                if (w != aft) {
                    answer.cardinality++;
                }
            }
        }
        if (answer.isAllOnes()) {
            return Container.full();
        }
        return answer;
    }

    @Override
    public Container or(final BitmapContainer value2) {
        if (isAllOnes() || value2.isAllOnes()) {
            return Container.full();
        }
        if (value2.isEmpty()) {
            return cowRef();
        }
        if (isEmpty()) {
            return value2.cowRef();
        }
        final BitmapContainer value1 = deepCopy();
        return value1.iorImpl(value2);
    }

    @Override
    public Container or(final RunContainer x) {
        return x.or(this);
    }

    /**
     * Find the index of the previous set bit less than or equal to i, returns -1 if none found.
     *
     * @param i starting index
     * @return index of the previous set bit
     */
    public int prevSetBit(final int i) {
        int x = i >> 6; // i / 64 with sign extension
        long w = bitmap[x];
        w <<= 64 - i - 1;
        if (w != 0) {
            return i - Long.numberOfLeadingZeros(w);
        }
        for (--x; x >= 0; --x) {
            if (bitmap[x] != 0) {
                return x * 64 + 63 - Long.numberOfLeadingZeros(bitmap[x]);
            }
        }
        return -1;
    }

    @Override
    public int rank(final short lowbits) {
        int x = toIntUnsigned(lowbits);
        int leftover = (x + 1) & 63;
        int answer = 0;
        for (int k = 0; k < (x + 1) / 64; ++k) {
            answer += Long.bitCount(bitmap[k]);
        }
        if (leftover != 0) {
            answer += Long.bitCount(bitmap[(x + 1) / 64] << (64 - leftover));
        }
        return answer;
    }

    @Override
    public Container remove(final int begin, final int end) {
        if (end == begin) {
            return cowRef();
        }
        if ((begin > end) || (end > MAX_RANGE)) {
            throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
        }
        final BitmapContainer answer = deepCopy();
        return answer.iremoveImpl(begin, end);
    }

    @Override
    public Container iunset(final short v) {
        return unsetImpl(v, true, null);
    }

    @Override
    public Container unset(final short v) {
        return unsetImpl(v, false, null);
    }

    @Override
    Container iunset(final short x, final PositionHint positionHint) {
        return unsetImpl(x, true, positionHint);
    }

    @Override
    Container unset(final short x, final PositionHint positionHint) {
        return unsetImpl(x, false, positionHint);
    }

    private Container unsetImpl(
            final short v,
            final boolean inPlace,
            final PositionHint positionHintOut) {
        final int x = toIntUnsigned(v);
        final int index = x / 64;
        final long bef = bitmap[index];
        final long mask = 1L << x;
        if ((bef & mask) == 0) {
            resetIfNotNull(positionHintOut);
            return inPlace ? this : cowRef();
        }
        final int newCardinality = cardinality - 1;
        if (newCardinality > ArrayContainer.SWITCH_CONTAINER_CARDINALITY_THRESHOLD) {
            final long aft = bef & (~mask);
            final BitmapContainer ans = inPlace ? deepCopyIfShared() : deepCopy();
            ans.cardinality = newCardinality;
            ans.bitmap[index] = aft;
            resetIfNotNull(positionHintOut);
            return ans;
        }
        if (newCardinality == 0) {
            return Container.empty();
        }
        if (newCardinality == 1) {
            final MutableInteger mi = new MutableInteger(-1);
            searchForwardForNonzeroWord(0, (final int k, long word) -> {
                if (k == index) {
                    word = word & ~mask;
                    if (word == 0) {
                        return false;
                    }
                }
                mi.value = 64 * k + numberOfTrailingZeros(word);
                return true;
            });
            resetIfNotNull(positionHintOut);
            return Container.singleton(lowbits(mi.value));
        }
        if (newCardinality == 2) {
            final MutableInteger mi0 = new MutableInteger(-1);
            final MutableInteger mi1 = new MutableInteger(-1);
            searchForwardForNonzeroWord(0, (final int k, long word) -> {
                if (k == index) {
                    word = word & ~mask;
                    if (word == 0) {
                        return false;
                    }
                }
                final int trail0s = numberOfTrailingZeros(word);
                final int kValue = 64 * k + trail0s;
                if (mi0.value == -1) {
                    mi0.value = kValue;
                    if (Long.bitCount(word) > 1) {
                        final int lead0s = Long.numberOfLeadingZeros(word);
                        mi1.value = (k + 1) * 64 - lead0s - 1;
                        return true;
                    }
                    return false;
                }
                mi1.value = kValue;
                return true;
            });
            resetIfNotNull(positionHintOut);
            return Container.twoValues(lowbits(mi0.value), lowbits(mi1.value));
        }
        final ArrayContainer ac = new ArrayContainer(newCardinality);
        ac.loadDataWithSkipValue(this, v, positionHintOut);
        return ac;
    }

    @Override
    public Container runOptimize() {
        final Container c = maybeSwitchContainer();
        if (c != this) {
            return c;
        }
        return maybeSwitchContainerToArrayOrRun();
    }

    @Override
    public short select(final int j) {
        int leftover = j;
        for (int k = 0; k < bitmap.length; ++k) {
            int w = Long.bitCount(bitmap[k]);
            if (w > leftover) {
                return (short) (k * 64 + ContainerUtil.select(bitmap[k], leftover));
            }
            leftover -= w;
        }
        throw new IllegalArgumentException("Insufficient cardinality.");
    }

    private class SeekToRankContext {
        public final int cardinalityBeforeIndex;
        public final int index;
        public final long wordAtIndexAfterDiscards;

        public SeekToRankContext(final int startRank) {
            int cardBefore = 0;
            int i = 0;
            long word = 0;
            int bitCount;
            while (i < bitmap.length) {
                word = bitmap[i];
                if (word == 0) {
                    ++i;
                    continue;
                }
                bitCount = Long.bitCount(word);
                final int posCard = cardBefore + bitCount;
                if (posCard >= startRank) {
                    break;
                }
                cardBefore = posCard;
                ++i;
            }
            cardinalityBeforeIndex = cardBefore;
            index = i;
            final int discard = startRank - cardBefore;
            for (int j = 0; j < discard; ++j) {
                word &= word - 1;
            }
            wordAtIndexAfterDiscards = word;
        }
    }

    private ArrayContainer selectToArrayContainer(final int startRank, final int count) {
        final ArrayContainer c = new ArrayContainer(count);
        final SeekToRankContext seeker = new SeekToRankContext(startRank);
        int i = seeker.index;
        long word = seeker.wordAtIndexAfterDiscards;
        int base = 64 * i;
        int pos = 0;
        FILLING: while (true) {
            while (word != 0) {
                final short v = (short) (base + numberOfTrailingZeros(word));
                c.content[pos++] = v;
                if (pos >= count) {
                    break FILLING;
                }
                word &= word - 1;
            }
            base += 64;
            ++i;
            if (i >= bitmap.length) {
                break;
            }
            word = bitmap[i];
        }
        c.cardinality = count;
        return c;
    }

    private BitmapContainer selectToBitmapContainer(final int startRank, final int count) {
        final BitmapContainer c = new BitmapContainer();
        final SeekToRankContext seeker = new SeekToRankContext(startRank);
        int i = seeker.index;
        long word = seeker.wordAtIndexAfterDiscards;
        int bitCount = Long.bitCount(word);
        int preCard = 0;
        while (true) {
            final int preCardPlusBitCount = preCard + bitCount;
            if (preCardPlusBitCount < count) {
                c.bitmap[i] = word;
                ++i;
                if (i >= bitmap.length) {
                    break;
                }
                preCard = preCardPlusBitCount;
                word = bitmap[i];
                bitCount = Long.bitCount(word);
                continue;
            } else if (bitCount + preCard == count) {
                c.bitmap[i] = word;
                break;
            }
            // bitCount + preCard > count
            int remaining = count - preCard;
            do {
                final long v = Long.lowestOneBit(word);
                c.bitmap[i] |= v;
                word &= word - 1;
                --remaining;
            } while (remaining > 0);
            break;
        }
        c.cardinality = count;
        return c;

    }

    @Override
    public Container select(final int startRank, final int endRank) {
        if (endRank <= startRank || endRank > cardinality) {
            throw new IllegalArgumentException(
                    "startRank=" + startRank + ", endRank=" + endRank + ", cardinality=" + cardinality);
        }
        final int card = endRank - startRank;
        if (card < ArrayContainer.DEFAULT_MAX_SIZE) {
            return selectToArrayContainer(startRank, card);
        }
        return selectToBitmapContainer(startRank, card);
    }

    private int findSecondHalf(final int index, final int rest, final int bitCountPreIndex) {
        long preMask = (1L << rest) - 1;
        long bitsAtIndex = bitmap[index];
        long preMasked = bitsAtIndex & preMask;
        int pos = bitCountPreIndex + Long.bitCount(preMasked);
        long mask = 1L << rest;
        long masked = bitsAtIndex & mask;
        if (masked != 0) {
            return pos;
        } else {
            return -pos - 1;
        }
    }

    @Override
    public int find(final short x) {
        final int value = toIntUnsigned(x);
        int index = value >>> 6; // value / 64.
        int r = value & 63; // value % 64.
        int bitCountPreIndex = 0;
        for (int k = 0; k < index; ++k) {
            bitCountPreIndex += Long.bitCount(bitmap[k]);
        }
        return findSecondHalf(index, r, bitCountPreIndex);
    }

    @Override
    public void selectRanges(final RangeConsumer outValues, final RangeIterator inPositions) {
        if (!inPositions.hasNext()) {
            return;
        }
        int ostart = -1;
        int oend = -1; // inclusive
        int wordIndex = 0;
        if (isEmpty()) {
            throw new IllegalArgumentException("select Ranges for invalid pos=" + inPositions.start());
        }
        int wordAccumBitCount = Long.bitCount(bitmap[0]);
        int prevWordAccumBitCount = 0;
        do {
            inPositions.next();
            int istart = inPositions.start();
            while (istart >= wordAccumBitCount) {
                ++wordIndex;
                if (wordIndex >= bitmap.length) {
                    throw new IllegalArgumentException("selectRanges for invalid pos=" + istart);
                }
                prevWordAccumBitCount = wordAccumBitCount;
                wordAccumBitCount += Long.bitCount(bitmap[wordIndex]);
            }
            int key = wordIndex * 64 + ContainerUtil.select(bitmap[wordIndex], istart - prevWordAccumBitCount);
            if (ostart == -1) {
                ostart = oend = key;
            } else {
                if (oend + 1 == key) {
                    oend = key;
                } else {
                    outValues.accept(ostart, oend + 1);
                    ostart = oend = key;
                }
            }
            int iend = inPositions.end();
            for (int j = istart + 1; j < iend; ++j) {
                while (j >= wordAccumBitCount) {
                    ++wordIndex;
                    if (wordIndex >= bitmap.length) {
                        throw new IllegalArgumentException("selectRanges for invalid pos=" + j);
                    }
                    prevWordAccumBitCount = wordAccumBitCount;
                    wordAccumBitCount += Long.bitCount(bitmap[wordIndex]);
                }
                key = wordIndex * 64 + ContainerUtil.select(bitmap[wordIndex], j - prevWordAccumBitCount);
                if (oend + 1 == key) {
                    oend = key;
                } else {
                    outValues.accept(ostart, oend + 1);
                    ostart = oend = key;
                }
            }
        } while (inPositions.hasNext());
        outValues.accept(ostart, oend + 1);
    }

    @Override
    public boolean findRanges(final RangeConsumer outPositions, final RangeIterator inValues, final int maxPos) {
        if (!inValues.hasNext()) {
            return false;
        }
        if (isEmpty()) {
            throw new IllegalArgumentException("find Ranges for invalid key=" + inValues.start());
        }
        int ostart = -1;
        int oend = -1; // inclusive
        int k = 0;
        int kAccumBitCount = 0;
        do {
            inValues.next();
            int istart = inValues.start();
            int kistart = istart >>> 6; // istart / 64.
            int ristart = istart & 63; // istart % 64.
            while (k < kistart) {
                if (k >= bitmap.length) {
                    throw new IllegalArgumentException("findRanges for invalid key=" + istart);
                }
                kAccumBitCount += Long.bitCount(bitmap[k]);
                ++k;
            }
            int pos = findSecondHalf(k, ristart, kAccumBitCount);
            if (pos < 0) {
                throw new IllegalArgumentException("findRanges for invalid key=" + istart);
            }
            if (ostart == -1) {
                if (pos > maxPos) {
                    return true;
                }
                ostart = oend = pos;
            } else {
                if (pos > maxPos) {
                    outPositions.accept(ostart, oend + 1);
                    return true;
                }
                if (oend + 1 == pos) {
                    oend = pos;
                } else {
                    outPositions.accept(ostart, oend + 1);
                    ostart = oend = pos;
                }
            }
            int iend = inValues.end();
            for (int j = istart + 1; j < iend; ++j) {
                int kj = j >>> 6; // j / 64.
                int rj = j & 63; // j % 64.
                while (k < kj) {
                    if (k >= bitmap.length) {
                        throw new IllegalArgumentException("findRanges for invalid key=" + j);
                    }
                    kAccumBitCount += Long.bitCount(bitmap[k]);
                    ++k;
                }
                // k == j
                pos = findSecondHalf(k, rj, kAccumBitCount);
                if (pos < 0) {
                    throw new IllegalArgumentException("findRanges for invalid key=" + j);
                    // Note we do not validate potential values between istart and iend, just the endpoints of the
                    // range.
                }
                if (pos > maxPos) {
                    outPositions.accept(ostart, oend + 1);
                    return true;
                }
                if (oend + 1 == pos) {
                    oend = pos;
                } else {
                    outPositions.accept(ostart, oend + 1);
                    ostart = oend = pos;
                }
            }
        } while (inValues.hasNext());
        outPositions.accept(ostart, oend + 1);
        return false;
    }

    /**
     * Copies the data to an array container
     *
     * @return the array container
     */
    public ArrayContainer toArrayContainer() {
        final ArrayContainer ac = new ArrayContainer(cardinality);
        ac.loadData(this);
        if (ac.getCardinality() != cardinality) {
            throw new RuntimeException("Internal error.");
        }
        return ac;
    }

    public RunContainer toRunContainer() {
        return new RunContainer(this);
    }

    @Override
    public void trim() {}

    @Override
    public Container xor(final ArrayContainer value2) {
        if (value2.isEmpty()) {
            return cowRef();
        }
        if (isEmpty()) {
            return value2.cowRef();
        }
        final BitmapContainer answer = deepCopy();
        int c = value2.cardinality;
        for (int k = 0; k < c; ++k) {
            short vc = value2.content[k];
            final int index = toIntUnsigned(vc) >>> 6;
            final long mask = 1L << vc;
            final long val = answer.bitmap[index];
            // TODO: check whether a branchy version could be faster
            answer.cardinality += 1 - 2 * ((val & mask) >>> vc);
            answer.bitmap[index] = val ^ mask;
        }
        return answer.maybeSwitchContainer();
    }

    @Override
    public Container xor(BitmapContainer value2) {
        if (value2.isEmpty()) {
            return cowRef();
        }
        if (isEmpty()) {
            return value2.cowRef();
        }
        int newCardinality = 0;
        for (int k = 0; k < bitmap.length; ++k) {
            newCardinality += Long.bitCount(bitmap[k] ^ value2.bitmap[k]);
        }
        if (newCardinality > ArrayContainer.SWITCH_CONTAINER_CARDINALITY_THRESHOLD) {
            final BitmapContainer answer = new BitmapContainer();
            for (int k = 0; k < answer.bitmap.length; ++k) {
                answer.bitmap[k] = bitmap[k] ^ value2.bitmap[k];
            }
            answer.cardinality = newCardinality;
            return answer;
        }
        final ArrayContainer ac = new ArrayContainer(newCardinality);
        fillArrayXOR(ac.content, bitmap, value2.bitmap);
        ac.cardinality = newCardinality;
        return ac.maybeSwitchContainer();
    }

    @Override
    public Container xor(RunContainer x) {
        return x.xor(this);
    }

    private boolean acceptBitsFrom(final int x, final long win, final ShortConsumer sc) {
        long w = win;
        while (w != 0) {
            final int v = x * 64 + numberOfTrailingZeros(w);
            final boolean wantMore = sc.accept((short) v);
            if (!wantMore) {
                return false;
            }
            w &= w - 1;
        }
        return true;
    }

    @Override
    public boolean forEach(final ShortConsumer sc) {
        for (int x = 0; x < bitmap.length; ++x) {
            long w = bitmap[x];
            if (!acceptBitsFrom(x, w, sc)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean forEach(final int rankOffset, final ShortConsumer sc) {
        int rank = 0;
        int x = 0;
        while (rank < rankOffset && x < bitmap.length) {
            long w = bitmap[x];
            if (w != 0) {
                final int bitCount = Long.bitCount(w);
                final int nextRank = rank + bitCount;
                if (nextRank > rankOffset) {
                    while (rank < rankOffset) {
                        w &= w - 1;
                        ++rank;
                    }
                    if (!acceptBitsFrom(x, w, sc)) {
                        return false;
                    }
                    ++x;
                    break;
                }
                rank = nextRank;
            }
            ++x;
        }
        for (; x < bitmap.length; ++x) {
            long w = bitmap[x];
            if (!acceptBitsFrom(x, w, sc)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean forEachRange(final int rankOffset, final ShortRangeConsumer sc) {
        int rank = 0;
        int x = 0;
        long w = 0;
        while (rank <= rankOffset && x < bitmap.length) {
            w = bitmap[x];
            if (w != 0) {
                final int bitCount = Long.bitCount(w);
                final int nextRank = rank + bitCount;
                if (nextRank >= rankOffset) {
                    while (rank < rankOffset) {
                        w &= w - 1;
                        ++rank;
                    }
                    break;
                }
                rank = nextRank;
            }
            ++x;
        }
        if (w == 0) {
            do {
                if (x >= bitmap.length - 1) {
                    return true;
                }
                ++x;
                w = bitmap[x];
            } while (w == 0);
        }
        int xTimes64 = x * 64;
        int pendingStart = xTimes64 + numberOfTrailingZeros(w);
        int pendingEnd = pendingStart;
        w &= w - 1;
        while (w != 0) {
            final int v = xTimes64 + numberOfTrailingZeros(w);
            if (pendingEnd + 1 == v) {
                pendingEnd = v;
            } else {
                if (!sc.accept((short) pendingStart, (short) pendingEnd)) {
                    return false;
                }
                pendingStart = pendingEnd = v;
            }
            w &= w - 1;
        }
        ++x;
        for (; x < bitmap.length; ++x) {
            w = bitmap[x];
            if (w != 0) {
                xTimes64 = x * 64;
                do {
                    final int v = xTimes64 + numberOfTrailingZeros(w);
                    if (pendingEnd + 1 == v) {
                        pendingEnd = v;
                    } else {
                        if (!sc.accept((short) pendingStart, (short) pendingEnd)) {
                            return false;
                        }
                        pendingStart = pendingEnd = v;
                    }
                    w &= w - 1;
                } while (w != 0);
            }
        }
        return sc.accept((short) pendingStart, (short) pendingEnd);
    }

    @Override
    public BitmapContainer toBitmapContainer() {
        return this;
    }

    @Override
    public int nextValue(short fromValue) {
        return nextSetBit(toIntUnsigned(fromValue));
    }

    private void assertNonEmpty() {
        if (cardinality == 0) {
            throw new NoSuchElementException("Empty " + getContainerName());
        }
    }

    @Override
    public int first() {
        assertNonEmpty();
        return first(0, null);
    }

    private int first(final int startPos, final MutableInteger positionOutput) {
        int i = startPos;
        while (i < bitmap.length - 1 && bitmap[i] == 0) {
            ++i;
        }
        if (positionOutput != null) {
            positionOutput.value = i;
        }
        return i * 64 + numberOfTrailingZeros(bitmap[i]);
    }

    @FunctionalInterface
    private interface WordMatcher {
        boolean accept(int pos, long word);
    }

    private void searchForwardForNonzeroWord(final int startPos, final WordMatcher m) {
        for (int i = startPos; i < bitmap.length; ++i) {
            final long word = bitmap[i];
            if (word == 0) {
                continue;
            }
            if (m.accept(i, bitmap[i])) {
                return;
            }
        }
    }

    @Override
    public int last() {
        assertNonEmpty();
        return last(bitmap.length - 1, null);
    }

    private int last(final int startPos, final MutableInteger positionOutput) {
        int i = startPos;
        while (i > 0 && bitmap[i] == 0) {
            --i;
        }
        if (positionOutput != null) {
            positionOutput.value = i;
        }
        return (i + 1) * 64 - Long.numberOfLeadingZeros(bitmap[i]) - 1;
    }

    @Override
    public boolean subsetOf(final ArrayContainer c) {
        if (isEmpty()) {
            return true;
        }
        if (c.isEmpty()) {
            return false;
        }
        // don't do a first/last based check since those are not cached in bitmap.
        if (getCardinality() > c.getCardinality()) {
            return false;
        }
        // This is not commonly expected as a bitmap container should have more elements
        // than an array container in normal circumstances.
        int count = 0;
        for (int i = 0; i < c.getCardinality(); ++i) {
            short k = c.content[i];
            if (contains(k)) {
                ++count;
                if (count == getCardinality()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean subsetOf(BitmapContainer c) {
        if (isEmpty()) {
            return true;
        }
        if (c.isEmpty()) {
            return false;
        }
        // don't do a first/last based check since those are not cached in bitmap.
        if (getCardinality() > c.getCardinality()) {
            return false;
        }
        for (int i = 0; i < bitmap.length; ++i) {
            long w1 = bitmap[i];
            long w2 = c.bitmap[i];
            if ((w1 | w2) != w2) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean subsetOf(final RunContainer c) {
        if (isEmpty()) {
            return true;
        }
        if (c.isEmpty()) {
            return false;
        }
        // don't do a first/last based check since those are not cached in bitmap;
        // don't do a cardinality based check since that is not cached in run.

        // This is not commonly expected as a bitmap container should have more elements
        // than a run container in normal circumstances.
        int count = 0;
        for (int i = 0; i < c.nbrruns; ++i) {
            int ival = toIntUnsigned(c.getValue(i));
            int ilen = toIntUnsigned(c.getLength(i));
            for (int j = ival; j <= ival + ilen; ++j) {
                if (contains(lowbits(j))) {
                    ++count;
                    if (count == getCardinality()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    final long endMask(final int endBit) {
        if (endBit == 63) {
            return ~0L;
        }
        return (1L << (endBit + 1)) - 1;
    }

    // rangeEnd is exclusive.
    @Override
    public boolean overlapsRange(final int rangeStart, final int rangeEnd) {
        final int ifirst = rangeStart >>> 6;
        final int last = rangeEnd - 1;
        final int ilast = last >>> 6;
        final int startBit = rangeStart & 63;
        final int endBit = last & 63;
        final long startMask = ~((1L << startBit) - 1);
        if (ifirst == ilast) {
            final long endMask = endMask(endBit);
            final long v = bitmap[ifirst] & startMask & endMask;
            return v != 0;
        }
        long v = bitmap[ifirst] & startMask;
        if (v != 0) {
            return true;
        }
        int j = ifirst + 1;
        while (j < ilast) {
            v = bitmap[j];
            if (v != 0) {
                return true;
            }
            ++j;
        }
        final long endMask = endMask(endBit);
        v = bitmap[ilast] & endMask;
        return v != 0;
    }

    @Override
    public boolean overlaps(final ArrayContainer c) {
        // A bitmap container should have more elements
        // than an array container in normal circumstances,
        // and a bitmap container has O(1) contains checks.
        for (int i = 0; i < c.getCardinality(); ++i) {
            final short k = c.content[i];
            if (contains(k)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean overlaps(final BitmapContainer c) {
        for (int i = 0; i < bitmap.length; ++i) {
            long w1 = bitmap[i];
            long w2 = c.bitmap[i];
            if ((w1 & w2) != 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean overlaps(final RunContainer c) {
        // A bitmap container should have more elements
        // than a run container in normal circumstances,
        // and a bitmap container has O(1) contains checks.
        for (int i = 0; i < c.nbrruns; ++i) {
            int ival = toIntUnsigned(c.getValue(i));
            int ilen = toIntUnsigned(c.getLength(i));
            for (int j = ival; j <= ival + ilen; ++j) {
                if (contains(lowbits(j))) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void setCopyOnWrite() {
        shared = true;
    }

    private BitmapContainer deepCopyIfShared() {
        return shared ? deepCopy() : this;
    }

    @Override
    public int bytesAllocated() {
        return BITMAP_SIZE_IN_BYTES;
    }

    @Override
    public int bytesUsed() {
        return bytesAllocated();
    }

    @Override
    public Container toLargeContainer() {
        return this;
    }

    @Override
    public void validate() {
        final int computedCard = computeCardinality(this);
        if (computedCard != cardinality) {
            throw new IllegalStateException("computedCard=" + computedCard + ", cardinality=" + cardinality);
        }
    }

    @Override
    public boolean isShared() {
        return shared;
    }
}
