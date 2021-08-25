/*
 * (c) the authors Licensed under the Apache License, Version 2.0.
 *
 * The code in this file is a heavily modified version of the original in the RoaringBitmap library;
 * please see https://roaringbitmap.org/
 *
 */

package io.deephaven.db.v2.utils.rsp.container;

import static io.deephaven.db.v2.utils.rsp.container.MutableInteger.getIfNotNullAndNonNegative;
import static io.deephaven.db.v2.utils.rsp.container.MutableInteger.setIfNotNull;
import static io.deephaven.db.v2.utils.rsp.container.PositionHint.resetIfNotNull;

import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * Simple container made of an array of 16-bit integers
 */
public class ArrayContainer extends Container {

    // Sizing of a short array object in a 64 bit JVM (Hotspot) uses
    // 12 bytes of object overhead (including array.length), plus
    // the space for the short elements payload, plus padding to round
    // objects to 8 bytes boundaries.
    // So an array of 4 elements uses: 12 + 2*4 + 4 padding = 24 bytes = 3*8 bytes.
    // The 4 bytes of padding are wasted and can be used for another 2 short elements.
    private static final int DEFAULT_INIT_SIZE = 6;

    // containers with DEFAULT_MAX_SZE or less integers should be ArrayContainers
    static final int DEFAULT_MAX_SIZE = 4096 - 6; // 12 bytes of object overhead is 6 shorts.

    public static final int SWITCH_CONTAINER_CARDINALITY_THRESHOLD =
        DEFAULT_MAX_SIZE - DEFAULT_MAX_SIZE / 16;

    static int sizeInBytes(final int cardinality) {
        return cardinality * Short.BYTES;
    }

    protected int cardinality = 0;

    protected short[] content;

    protected boolean shared = false;

    public short[] getContent() {
        return content;
    }

    protected ArrayContainer(final short[] content, final int cardinality, final boolean shared) {
        this.content = content;
        this.cardinality = cardinality;
        this.shared = shared;
    }

    /**
     * Create an array container with default capacity
     */
    public ArrayContainer() {
        this(DEFAULT_INIT_SIZE);
    }

    /**
     * Create an array container with specified capacity
     *
     * @param capacity The capacity of the container
     */
    public ArrayContainer(final int capacity) {
        content = new short[shortArraySizeRounding(capacity)];
    }

    /**
     * Create an array container with a run of ones from firstOfRun to lastOfRun, inclusive. Caller
     * is responsible for making sure the range is small enough that ArrayContainer is appropriate.
     *
     * @param firstOfRun first index
     * @param lastOfRun last index (range is exclusive)
     */
    ArrayContainer(final int firstOfRun, final int lastOfRun) {
        final int valuesInRange = lastOfRun - firstOfRun;
        content = new short[shortArraySizeRounding(valuesInRange)];
        for (int i = 0; i < valuesInRange; ++i) {
            content[i] = (short) (firstOfRun + i);
        }
        cardinality = valuesInRange;
    }

    private ArrayContainer(final ArrayContainer src, final int startRank, final int endRank) {
        cardinality = endRank - startRank;
        content = new short[shortArraySizeRounding(cardinality)];
        System.arraycopy(src.content, startRank, content, 0, cardinality);
    }

    /**
     * Create a new container from existing values array. This copies the data.
     *
     * @param newCapacity The new container's capacity.
     * @param arr Array containing values to copy from, in increasing unsigned short order.
     * @param offset index position for the first value to copy.
     * @param sz number of values to copy.
     */
    private ArrayContainer(final int newCapacity, final short[] arr, final int offset,
        final int sz) {
        cardinality = sz;
        final short[] cs = new short[shortArraySizeRounding(newCapacity)];
        System.arraycopy(arr, offset, cs, 0, sz);
        content = cs;
    }

    public ArrayContainer(final short[] arr, final int sz) {
        cardinality = sz;
        content = arr;
    }

    // Caller should ensure arguments provided in increasing unsigned order.
    ArrayContainer(final short v0, final short v1, final short v2) {
        content = new short[DEFAULT_INIT_SIZE];
        content[0] = v0;
        content[1] = v1;
        content[2] = v2;
        cardinality = 3;
    }

    // Caller should ensure arguments provided in increasing unsigned order.
    ArrayContainer(final short v0, final short v1) {
        content = new short[DEFAULT_INIT_SIZE];
        content[0] = v0;
        content[1] = v1;
        cardinality = 2;
    }

    ArrayContainer(final short v) {
        content = new short[DEFAULT_INIT_SIZE];
        content[0] = v;
        cardinality = 1;
    }

    /**
     * Construct a new ArrayContainer from values copied from the provided array.
     *
     * @param arr array with values in increasing unsigned short order.
     * @param offset index position for the first element to take.
     * @param sz number of elements to read from the array.
     */
    @SuppressWarnings("unused")
    public static ArrayContainer makeByCopying(final short[] arr, final int offset, final int sz) {
        return makeByCopying(sz, arr, offset, sz);
    }

    public static ArrayContainer makeByCopying(final int newCapacity, final short[] arr,
        final int offset, final int sz) {
        return new ArrayContainer(newCapacity, arr, offset, sz);
    }

    /**
     * Construct a new ArrayContainer using the provided array. The container takes ownership of the
     * array.
     *
     * @param arr array with values in increasing unsigned short order. The container takes
     *        ownership of this array.
     * @param sz number of elements in arr.
     */
    @SuppressWarnings("unused")
    public static ArrayContainer makeByWrapping(final short[] arr, final int sz) {
        return new ArrayContainer(arr, sz);
    }

    public ArrayContainer(final short[] newContent) {
        cardinality = newContent.length;
        content = newContent;
    }

    @Override
    public Container add(final int begin, final int end) {
        if (end == begin) {
            return cowRef();
        }
        if (begin > end || end > MAX_RANGE) {
            throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
        }
        // TODO: may need to convert to a RunContainer
        int indexstart = ContainerUtil.unsignedBinarySearch(content, 0, cardinality, (short) begin);
        if (indexstart < 0) {
            indexstart = -indexstart - 1;
        }
        int indexend =
            ContainerUtil.unsignedBinarySearch(content, 0, cardinality, (short) (end - 1));
        if (indexend < 0) {
            indexend = -indexend - 1;
        } else {
            indexend++;
        }
        int rangelength = end - begin;
        int newcardinality = indexstart + (cardinality - indexend) + rangelength;
        if (newcardinality > DEFAULT_MAX_SIZE) {
            Container a = toBiggerCardinalityContainer(newcardinality);
            return a.iadd(begin, end);
        }
        final ArrayContainer answer = makeByCopying(newcardinality, content, 0, indexstart);
        System.arraycopy(content, indexend, answer.content, indexstart + rangelength,
            cardinality - indexend);
        for (int k = 0; k < rangelength; ++k) {
            answer.content[k + indexstart] = (short) (begin + k);
        }
        answer.cardinality = newcardinality;
        return answer;
    }

    /**
     * requirement: newcardinality >= cardinality.
     */
    private Container toBiggerCardinalityContainer(final int newcardinality) {
        if (cardinality > 0) {
            final int range = last() - first() + 1;
            final int holesUpperBound = range - cardinality;
            final int runsUpperBound = holesUpperBound + 1 + newcardinality - cardinality;
            if (runsUpperBound < DEFAULT_MAX_SIZE / 16) { // heuristic.
                return new RunContainer(this, runsUpperBound);
            }
        }
        return toBitmapContainer();
    }

    @Override
    public Container iset(final short x) {
        return isetImpl(x, null, () -> this, this::deepcopyIfShared);
    }

    @Override
    Container iset(final short x, final PositionHint positionHint) {
        return isetImpl(x, positionHint, () -> this, this::deepcopyIfShared);
    }

    @Override
    Container set(final short x, final PositionHint positionHint) {
        return isetImpl(x, positionHint, this::cowRef, this::deepCopy);
    }

    @Override
    public Container set(final short x) {
        return isetImpl(x, null, this::cowRef, this::deepCopy);
    }

    private Container isetImpl(final short x,
        final PositionHint positionHint,
        final Supplier<ArrayContainer> self,
        final Supplier<ArrayContainer> copy) {
        final int begin = getIfNotNullAndNonNegative(positionHint, 0);
        int loc = ContainerUtil.unsignedBinarySearch(content, begin, cardinality, x);
        if (loc >= 0) {
            setIfNotNull(positionHint, loc + 1);
            return self.get();
        }
        // Transform the ArrayContainer to a BitmapContainer
        // when cardinality = DEFAULT_MAX_SIZE
        if (cardinality >= DEFAULT_MAX_SIZE) {
            final Container a = toBiggerCardinalityContainer(cardinality + 1);
            if (positionHint != null) {
                positionHint.reset();
                return a.iset(x, positionHint);
            }
            return a.iset(x);
        }
        final ArrayContainer ans = copy.get();
        return ans.isetImplSecondHalf(x, loc, positionHint);
    }

    private Container isetImplSecondHalf(final short x, final int loc,
        final PositionHint positionHintOut) {
        if (cardinality >= content.length) {
            increaseCapacity();
        }
        // insertion : shift the elements > x by one position to
        // the right
        // and put x in it's appropriate place
        System.arraycopy(content, -loc - 1, content, -loc, cardinality + loc + 1);
        content[-loc - 1] = x;
        setIfNotNull(positionHintOut, -loc);
        ++cardinality;
        return this;
    }

    private int advance(ShortIterator it) {
        if (it.hasNext()) {
            return ContainerUtil.toIntUnsigned(it.next());
        } else {
            return -1;
        }
    }

    Container maybeSwitchContainer() {
        if (!ImmutableContainer.ENABLED) {
            return this;
        }
        if (cardinality == 0) {
            return Container.empty();
        }
        if (cardinality == 1) {
            return Container.singleton(content[0]);
        }
        if (cardinality == 2) {
            return Container.twoValues(content[0], content[1]);
        }
        return this;
    }

    @Override
    public Container and(final ArrayContainer value2) {
        if (value2.isEmpty() || isEmpty()) {
            return Container.empty();
        }
        ArrayContainer value1 = this;
        final int desiredCapacity = Math.min(value1.getCardinality(), value2.getCardinality());
        ArrayContainer answer = new ArrayContainer(desiredCapacity);
        answer.cardinality =
            ContainerUtil.unsignedIntersect2by2(value1.content, value1.getCardinality(),
                value2.content, value2.getCardinality(), answer.content);
        return answer.maybeSwitchContainer();
    }

    @Override
    public Container and(final BitmapContainer x) {
        return x.and(this);
    }

    @Override
    // see andNot for an approach that might be better.
    public Container and(final RunContainer x) {
        return x.and(this);
    }

    @Override
    public Container andNot(final ArrayContainer value2) {
        if (value2.isEmpty()) {
            return cowRef();
        }
        ArrayContainer value1 = this;
        final int desiredCapacity = value1.getCardinality();
        ArrayContainer answer = new ArrayContainer(desiredCapacity);
        answer.cardinality =
            ContainerUtil.unsignedDifference(value1.content, value1.getCardinality(),
                value2.content, value2.getCardinality(), answer.content);
        return answer.maybeSwitchContainer();
    }

    @Override
    public Container andNot(final BitmapContainer value2) {
        if (value2.isEmpty()) {
            return cowRef();
        }
        final ArrayContainer answer = new ArrayContainer(content.length);
        int pos = 0;
        for (int k = 0; k < cardinality; ++k) {
            short val = content[k];
            answer.content[pos] = val;
            pos += 1 - value2.bitValue(val);
        }
        answer.cardinality = pos;
        return answer.maybeSwitchContainer();
    }

    @Override
    public Container andNot(final RunContainer x) {
        if (x.isEmpty()) {
            return cowRef();
        } else if (x.isAllOnes()) {
            return Container.empty();
        }
        int write = 0;
        int read = 0;
        final ArrayContainer answer = new ArrayContainer(cardinality);
        for (int i = 0; i < x.numberOfRuns() && read < cardinality; ++i) {
            int runStart = ContainerUtil.toIntUnsigned(x.getValue(i));
            int runEnd = runStart + ContainerUtil.toIntUnsigned(x.getLength(i));
            if (ContainerUtil.toIntUnsigned(content[read]) > runEnd) {
                continue;
            }
            int firstInRun = ContainerUtil.iterateUntil(content, read, cardinality, runStart);
            int toWrite = firstInRun - read;
            System.arraycopy(content, read, answer.content, write, toWrite);
            write += toWrite;

            read = ContainerUtil.iterateUntil(content, firstInRun, cardinality, runEnd + 1);
        }
        System.arraycopy(content, read, answer.content, write, cardinality - read);
        write += cardinality - read;
        answer.cardinality = write;
        return answer.maybeSwitchContainer();
    }

    @Override
    public ArrayContainer deepCopy() {
        return makeByCopying(content, 0, cardinality);
    }

    @Override
    public ArrayContainer cowRef() {
        setCopyOnWrite();
        return this;
    }

    @Override
    public boolean isEmpty() {
        return cardinality == 0;
    }

    @Override
    public boolean isAllOnes() {
        return false;
    }

    @Override
    public boolean contains(final short x) {
        return ContainerUtil.unsignedBinarySearch(content, 0, cardinality, x) >= 0;
    }

    @Override
    public boolean contains(final int rangeStart, final int rangeEnd) {
        final int maximum = rangeEnd - 1;
        final int start = ContainerUtil.advanceUntil(content, -1, cardinality, (short) rangeStart);
        if (start >= cardinality) {
            return false;
        }
        final int end =
            ContainerUtil.advanceUntil(content, start - 1, cardinality, (short) maximum);
        return end < cardinality
            && end - start == maximum - rangeStart
            && content[start] == (short) rangeStart
            && content[end] == (short) maximum;
    }

    @Override
    protected boolean contains(final RunContainer runContainer) {
        if (runContainer.getCardinality() > cardinality) {
            return false;
        }

        int prev = -1;
        for (int i = 0; i < runContainer.numberOfRuns(); ++i) {
            final int first = runContainer.getValueAsInt(i);
            final int last = first + runContainer.getLengthAsInt(i);
            int start = ContainerUtil.advanceUntil(content, prev, cardinality, (short) first);
            if (start >= cardinality) {
                return false;
            }
            int end = ContainerUtil.advanceUntil(content, start - 1, cardinality, (short) last);
            if (end >= cardinality ||
                end - start != last - first ||
                content[start] != (short) first ||
                content[end] != (short) last) {
                return false;
            }
            prev = end - 1;
        }
        return true;
    }

    @Override
    protected boolean contains(final ArrayContainer arrayContainer) {
        if (cardinality < arrayContainer.cardinality) {
            return false;
        }
        int i1 = 0, i2 = 0;
        while (i1 < cardinality && i2 < arrayContainer.cardinality) {
            if (content[i1] == arrayContainer.content[i2]) {
                ++i1;
                ++i2;
            } else if (ContainerUtil.compareUnsigned(content[i1], arrayContainer.content[i2]) < 0) {
                ++i1;
            } else {
                return false;
            }
        }
        return i2 == arrayContainer.cardinality;
    }

    @Override
    protected boolean contains(final BitmapContainer bitmapContainer) {
        if (bitmapContainer.getCardinality() > getCardinality()) {
            return false;
        }
        final short[] buf = threadLocalBuf.get();
        int pos = 0;
        final ContainerShortBatchIterator bit = bitmapContainer.getShortBatchIterator(0);
        while (bit.hasNext()) {
            final int n = bit.next(buf, 0, buf.length);
            for (int i = 0; i < n; ++i) {
                final int r = ContainerUtil.unsignedBinarySearch(content, pos, cardinality, buf[i]);
                if (r < 0) {
                    return false;
                }
                pos = r + 1;
            }
        }
        return true;
    }

    @Override
    public Container iflip(final short x) {
        int loc = ContainerUtil.unsignedBinarySearch(content, 0, cardinality, x);
        if (loc < 0) {
            // Transform the ArrayContainer to a BitmapContainer
            // when cardinality = DEFAULT_MAX_SIZE
            if (cardinality >= DEFAULT_MAX_SIZE) {
                final BitmapContainer a = toBitmapContainer();
                return a.iset(x);
            }
            final ArrayContainer ans = deepcopyIfShared();
            if (ans.cardinality >= ans.content.length) {
                ans.increaseCapacity();
            }
            // insertion : shift the elements > x by one position to
            // the right
            // and put x in it's appropriate place
            System.arraycopy(ans.content, -loc - 1, ans.content, -loc, ans.cardinality + loc + 1);
            ans.content[-loc - 1] = x;
            ++ans.cardinality;
            return ans;
        }
        final ArrayContainer ans = deepcopyIfShared();
        System.arraycopy(ans.content, loc + 1, ans.content, loc, ans.cardinality - loc - 1);
        --ans.cardinality;
        return ans.maybeSwitchContainer();
    }

    @Override
    public int getCardinality() {
        return cardinality;
    }

    static final class ReverseShortIterator implements ShortAdvanceIterator {
        private int pos;
        private ArrayContainer parent;

        ReverseShortIterator(final ArrayContainer p) {
            wrap(p);
        }

        @Override
        public boolean hasNext() {
            return pos > 0;
        }

        @Override
        public short next() {
            return parent.content[--pos];
        }

        @Override
        public int nextAsInt() {
            return ContainerUtil.toIntUnsigned(next());
        }

        @Override
        public short curr() {
            return parent.content[pos];
        }

        @Override
        public int currAsInt() {
            return ContainerUtil.toIntUnsigned(curr());
        }

        @Override
        public boolean advance(final int v) {
            if (pos == parent.cardinality) {
                --pos;
            }
            if (pos < 0) {
                return false;
            }
            if (ContainerUtil.toIntUnsigned(parent.content[pos]) <= v) {
                return true;
            }
            int i = ContainerUtil.unsignedBinarySearch(parent.content, 0, pos + 1,
                ContainerUtil.lowbits(v));
            if (i < 0) {
                i = -i - 1;
                if (i == 0) {
                    pos = 0;
                    return false;
                }
                pos = i - 1;
                return true;
            }
            pos = i;
            return true;
        }

        void wrap(final ArrayContainer p) {
            parent = p;
            pos = parent.cardinality;
        }
    }

    @Override
    public ShortAdvanceIterator getReverseShortIterator() {
        return new ReverseShortIterator(this);
    }

    final static class ShortForwardIterator implements ShortIterator {
        private int pos;
        private ArrayContainer parent;

        ShortForwardIterator(final ArrayContainer p) {
            wrap(p);
        }

        @Override
        public boolean hasNext() {
            return pos < parent.cardinality - 1;
        }

        @Override
        public short next() {
            ++pos;
            return curr();
        }

        public short peekNext() {
            return parent.content[pos + 1];
        }

        public short curr() {
            return parent.content[pos];
        }

        @Override
        public int nextAsInt() {
            return ContainerUtil.toIntUnsigned(next());
        }

        public int currAsInt() {
            return ContainerUtil.toIntUnsigned(curr());
        }

        private void wrap(final ArrayContainer p) {
            parent = p;
            pos = -1;
        }
    }

    @Override
    public ShortForwardIterator getShortIterator() {
        return new ShortForwardIterator(this);
    }

    @Override
    public ContainerShortBatchIterator getShortBatchIterator(final int skipCount) {
        if (DEBUG && skipCount != 0 && skipCount >= cardinality) {
            throw new IllegalArgumentException("initialSeek=" + skipCount);
        }
        return new ArrayShortBatchIterator(this, skipCount);
    }

    @Override
    public SearchRangeIterator getShortRangeIterator(final int initialSeek) {
        if (DEBUG && initialSeek != 0 && initialSeek >= cardinality) {
            throw new IllegalArgumentException("initialSeek=" + initialSeek);
        }
        return new ArrayContainerRangeIterator(this, initialSeek);
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
        int indexstart = ContainerUtil.unsignedBinarySearch(content, 0, cardinality, (short) begin);
        if (indexstart < 0) {
            indexstart = -indexstart - 1;
        }
        int indexend =
            ContainerUtil.unsignedBinarySearch(content, 0, cardinality, (short) (end - 1));
        if (indexend < 0) {
            indexend = -indexend - 1;
        } else {
            indexend++;
        }
        int rangelength = end - begin;
        int newcardinality = indexstart + (cardinality - indexend) + rangelength;
        if (newcardinality > DEFAULT_MAX_SIZE) {
            final Container a = toBiggerCardinalityContainer(newcardinality);
            return a.iadd(begin, end);
        }
        /*
         * b - index of begin(indexstart), e - index of end(indexend), |--| is current sequential
         * indexes in content. Total 6 cases are possible, listed as below:
         *
         * case-1) |--------|b-e case-2) |----b---|e case-3) |---b---e---| case-4) b|----e---|
         * case-5) b-e|------| case-6) b|-----|e
         *
         * In case of old approach, we did (1a) Array.copyOf in increaseCapacity ( # of elements
         * copied -> cardinality), (1b) then we moved elements using System.arrayCopy ( # of
         * elements copied -> cardinality -indexend), (1c) then we set all elements from begin to
         * end ( # of elements set -> end - begin)
         *
         * With new approach, (2a) we set all elements from begin to end ( # of elements set -> end-
         * begin), (2b) we only copy elements in current set which are not in range begin-end ( # of
         * elements copied -> cardinality - (end-begin) )
         *
         * why is it faster? Logically we are doing less # of copies. Mathematically proof as below:
         * -> 2a is same as 1c, so we can avoid. Assume, 2b < (1a+1b), lets prove this assumption.
         * Substitute the values. (cardinality - (end-begin)) < ( 2*cardinality - indexend) , lowest
         * possible value of indexend is 0 and equation holds true , hightest possible value of
         * indexend is cardinality and equation holds true , hence "<" equation holds true always
         */
        final ArrayContainer ans;
        if (newcardinality > content.length) {
            short[] destination = new short[calculateCapacity(newcardinality)];
            // if b > 0, we copy from 0 to b. Do nothing otherwise.
            System.arraycopy(content, 0, destination, 0, indexstart);
            // set values from b to e
            for (int k = 0; k < rangelength; ++k) {
                destination[k + indexstart] = (short) (begin + k);
            }
            /*
             * so far cases - 1,2 and 6 are done Now, if e < cardinality, we copy from e to
             * cardinality.Otherwise do noting this covers remaining 3,4 and 5 cases
             */
            System.arraycopy(content, indexend,
                destination, indexstart + rangelength,
                cardinality - indexend);
            if (shared) {
                ans = new ArrayContainer(destination, newcardinality);
            } else {
                content = destination;
                cardinality = newcardinality;
                ans = this;
            }
        } else {
            if (shared) {
                ans = new ArrayContainer(calculateCapacity(newcardinality));
                System.arraycopy(content, 0, ans.content, 0, indexstart);
            } else {
                ans = this;
            }
            System.arraycopy(content, indexend,
                ans.content, indexstart + rangelength,
                cardinality - indexend);
            for (int k = 0; k < rangelength; ++k) {
                ans.content[k + indexstart] = (short) (begin + k);
            }
            ans.cardinality = newcardinality;
        }
        return ans;
    }

    @Override
    public Container iappend(final int begin, final int end) {
        // Note we don't validate inputs in the same way
        // as other methods; this method is part of the fast path for builders.
        final int rangeLength = end - begin;
        if (rangeLength <= 0) {
            return this;
        }
        final int newCardinality = cardinality + rangeLength;
        if (newCardinality > DEFAULT_MAX_SIZE) {
            final Container a = toBiggerCardinalityContainer(newCardinality);
            return a.iappend(begin, end);
        }
        final ArrayContainer ans;
        final int firstIndexNewRange = cardinality;
        if (shared || newCardinality > content.length) {
            short[] destination = new short[calculateCapacity(newCardinality)];
            System.arraycopy(content, 0, destination, 0, cardinality);
            if (shared) {
                ans = new ArrayContainer(destination, newCardinality);
            } else {
                content = destination;
                cardinality = newCardinality;
                ans = this;
            }
        } else {
            cardinality = newCardinality;
            ans = this;
        }
        for (int k = 0; k < rangeLength; ++k) {
            ans.content[firstIndexNewRange + k] = (short) (begin + k);
        }
        return ans;
    }

    @Override
    public Container iand(final ArrayContainer value2) {
        final ArrayContainer ans = deepcopyIfShared();
        ans.cardinality = ContainerUtil.unsignedIntersect2by2(ans.content, ans.getCardinality(),
            value2.content, value2.getCardinality(), ans.content);
        return ans.maybeSwitchContainer();
    }

    @Override
    public Container iand(BitmapContainer value2) {
        final ArrayContainer ans = deepcopyIfShared();
        int pos = 0;
        for (int k = 0; k < ans.cardinality; ++k) {
            short v = ans.content[k];
            ans.content[pos] = v;
            pos += value2.bitValue(v);
        }
        ans.cardinality = pos;
        return ans.maybeSwitchContainer();
    }

    @Override
    public Container iand(final RunContainer x) {
        // possible performance issue, not taking advantage of possible inplace
        return x.and(this);
    }


    @Override
    public Container iandNot(final ArrayContainer value2) {
        final ArrayContainer ans = deepcopyIfShared();
        ans.cardinality =
            ContainerUtil.unsignedDifference(ans.content, ans.getCardinality(), value2.content,
                value2.getCardinality(), ans.content);
        return ans.maybeSwitchContainer();
    }

    @Override
    public Container iandNot(final BitmapContainer value2) {
        final ArrayContainer ans = deepcopyIfShared();
        int pos = 0;
        for (int k = 0; k < ans.cardinality; ++k) {
            short v = ans.content[k];
            ans.content[pos] = v;
            pos += 1 - value2.bitValue(v);
        }
        ans.cardinality = pos;
        return ans.maybeSwitchContainer();
    }

    @Override
    public Container iandNot(final RunContainer x) {
        // possible performance issue, not taking advantage of possible inplace
        // could adapt algo above
        return andNot(x);
    }

    private void increaseCapacity() {
        increaseCapacity(false);
    }

    private static int nextCapacity(final int oldCapacity) {
        return (oldCapacity == 0) ? DEFAULT_INIT_SIZE
            : oldCapacity < 64 ? shortArraySizeRounding(oldCapacity * 2)
                : oldCapacity < 1067 ? shortArraySizeRounding(oldCapacity * 3 / 2)
                    : shortArraySizeRounding(oldCapacity * 5 / 4);
    }

    // temporarily allow an illegally large size, as long as the operation creating
    // the illegal container does not return it.
    private void increaseCapacity(final boolean allowIllegalSize) {
        int newCapacity = nextCapacity(content.length);
        if (newCapacity > ArrayContainer.DEFAULT_MAX_SIZE && !allowIllegalSize) {
            newCapacity = ArrayContainer.DEFAULT_MAX_SIZE;
        }
        // if we are within ~1/16th of the max, go to max
        if (newCapacity > ArrayContainer.DEFAULT_MAX_SIZE - 256 && !allowIllegalSize) {
            newCapacity = ArrayContainer.DEFAULT_MAX_SIZE;
        }
        final short[] vs = new short[newCapacity];
        System.arraycopy(content, 0, vs, 0, cardinality);
        content = vs;
    }

    private int calculateCapacity(final int min) {
        int newCapacity = shortArraySizeRounding(min);
        // if we are within ~1/16th of the max, or over, go to max
        if (newCapacity > ArrayContainer.DEFAULT_MAX_SIZE - 256) {
            newCapacity = ArrayContainer.DEFAULT_MAX_SIZE;
        }
        return newCapacity;
    }

    @Override
    public Container inot(final int firstOfRange, final int exclusiveEndOfRange) {
        // TODO: may need to convert to a RunContainer
        // determine the span of array indices to be affected
        int startIndex =
            ContainerUtil.unsignedBinarySearch(content, 0, cardinality, (short) firstOfRange);
        if (startIndex < 0) {
            startIndex = -startIndex - 1;
        }
        int lastIndex = ContainerUtil.unsignedBinarySearch(content, 0, cardinality,
            (short) (exclusiveEndOfRange - 1));
        if (lastIndex < 0) {
            lastIndex = -lastIndex - 1 - 1;
        }
        final int currentValuesInRange = lastIndex - startIndex + 1;
        final int spanToBeFlipped = exclusiveEndOfRange - firstOfRange;
        final int newValuesInRange = spanToBeFlipped - currentValuesInRange;
        final int cardinalityChange = newValuesInRange - currentValuesInRange;
        final int newCardinality = cardinality + cardinalityChange;

        final ArrayContainer ans;
        if (cardinalityChange > 0) { // expansion, right shifting needed
            final short[] src = content;
            // so big we need a bitmap?
            if (newCardinality > DEFAULT_MAX_SIZE) {
                return toBiggerCardinalityContainer(newCardinality).inot(firstOfRange,
                    exclusiveEndOfRange);
            }
            if (shared) {
                ans = new ArrayContainer(calculateCapacity(newCardinality));
                System.arraycopy(content, 0, ans.content, 0, lastIndex + 1);
            } else {
                ans = this;
                if (newCardinality > content.length) {
                    content = new short[calculateCapacity(newCardinality)];
                    System.arraycopy(src, 0, content, 0, lastIndex + 1);
                }
            }
            // slide right the contents after the range
            System.arraycopy(src, startIndex + currentValuesInRange, ans.content,
                startIndex + newValuesInRange, cardinality - 1 - lastIndex);
            ans.negateRange(newValuesInRange, startIndex, lastIndex, firstOfRange,
                exclusiveEndOfRange);
        } else { // no alloc expansion needed
            if (shared) {
                if (cardinalityChange == 0) {
                    ans = deepCopy();
                } else {
                    ans = new ArrayContainer(content.length);
                    System.arraycopy(content, 0, ans.content, 0, lastIndex + 1);
                }
            } else {
                ans = this;
            }
            ans.negateRange(newValuesInRange, startIndex, lastIndex, firstOfRange,
                exclusiveEndOfRange);
            if (cardinalityChange < 0) {
                // contraction, left sliding.
                // Leave array oversize
                System.arraycopy(content, startIndex + currentValuesInRange, ans.content,
                    startIndex + newValuesInRange, cardinality - 1 - lastIndex);
            }
        }
        ans.cardinality = newCardinality;
        return ans.maybeSwitchContainer();
    }

    @Override
    public Container ior(final ArrayContainer value2) {
        if (shared) {
            return or(value2);
        }
        if (value2.isEmpty()) {
            return this;
        }
        if (isEmpty()) {
            return value2.cowRef();
        }
        final int sumOfCardinalities = getCardinality() + value2.getCardinality();
        if (sumOfCardinalities > DEFAULT_MAX_SIZE) {// it could be a bitmap!
            BitmapContainer bc = new BitmapContainer();
            for (int k = 0; k < value2.cardinality; ++k) {
                short v = value2.content[k];
                final int i = ContainerUtil.toIntUnsigned(v) >>> 6;
                bc.bitmap[i] |= (1L << v);
            }
            for (int k = 0; k < cardinality; ++k) {
                short v = content[k];
                final int i = ContainerUtil.toIntUnsigned(v) >>> 6;
                bc.bitmap[i] |= (1L << v);
            }
            bc.cardinality = 0;
            for (long k : bc.bitmap) {
                bc.cardinality += Long.bitCount(k);
            }
            if (bc.cardinality <= DEFAULT_MAX_SIZE) {
                return bc.toArrayContainer();
            } else if (bc.isAllOnes()) {
                return Container.full();
            }
            return bc;
        }
        if (sumOfCardinalities >= content.length) {
            int newCapacity = calculateCapacity(sumOfCardinalities);
            final ArrayContainer ans = new ArrayContainer(newCapacity);
            ans.cardinality =
                ContainerUtil.unsignedUnion2by2(content, 0, cardinality, value2.content, 0,
                    value2.cardinality,
                    ans.content);
            return ans;
        }
        System.arraycopy(content, 0, content, value2.cardinality, cardinality);
        cardinality =
            ContainerUtil.unsignedUnion2by2(content, value2.cardinality, cardinality,
                value2.content, 0,
                value2.cardinality, content);
        return this;
    }

    @Override
    public Container ior(final BitmapContainer x) {
        if (x.isEmpty()) {
            return this;
        }
        return x.or(this);
    }

    @Override
    public Container ior(final RunContainer x) {
        if (x.isEmpty()) {
            return this;
        }
        // possible performance issue, not taking advantage of possible inplace
        return x.or(this);
    }

    @Override
    public Container remove(final int begin, final int end) {
        return removeImpl(begin, end, false);
    }

    @Override
    public Container iremove(final int begin, final int end) {
        return removeImpl(begin, end, true);
    }

    private Container removeImpl(final int begin, final int end, final boolean inPlace) {
        if (end <= begin) {
            return inPlace ? this : cowRef();
        }
        if (begin > end || end > MAX_RANGE) {
            throw new IllegalArgumentException("Invalid range [" + begin + "," + end + ")");
        }
        int indexstart = ContainerUtil.unsignedBinarySearch(content, 0, cardinality, (short) begin);
        if (indexstart < 0) {
            indexstart = -indexstart - 1;
        }
        int indexend =
            ContainerUtil.unsignedBinarySearch(content, 0, cardinality, (short) (end - 1));
        if (indexend < 0) {
            indexend = -indexend - 1;
        } else {
            indexend++;
        }
        final int rangelength = indexend - indexstart;
        if (rangelength == 0) {
            return inPlace ? this : cowRef();
        }
        final int newCardinality = cardinality - rangelength;
        if (newCardinality == 0) {
            return Container.empty();
        }
        if (newCardinality == 1) {
            return makeSingletonContainer(content[indexstart > 0 ? 0 : indexend]);
        }
        if (newCardinality == 2) {
            // Since elements in the range need to be contiguous, you can think about the range
            // inside our contents
            // array as one block. The two values remaining can only be either both at the
            // beginning, before the block,
            // or one before and one after, or both after the block at the end.
            final int i0;
            final int i1;
            if (indexstart == 0) {
                i0 = indexend;
                i1 = indexend + 1;
            } else if (indexstart == 1) {
                i0 = 0;
                i1 = indexend;
            } else {
                // indexstart == 2.
                i0 = 0;
                i1 = 1;
            }
            return Container.twoValues(content[i0], content[i1]);
        }
        final ArrayContainer ans;
        if (inPlace && !shared) {
            ans = this;
        } else {
            ans = new ArrayContainer(calculateCapacity(newCardinality));
            System.arraycopy(content, 0, ans.content, 0, indexstart);
        }
        System.arraycopy(content, indexstart + rangelength, ans.content, indexstart,
            cardinality - indexstart - rangelength);
        ans.cardinality = newCardinality;
        return ans;
    }

    @Override
    public Container ixor(final ArrayContainer value2) {
        return xor(value2);
    }

    @Override
    public Container ixor(BitmapContainer x) {
        return x.xor(this);
    }

    @Override
    public Container ixor(RunContainer x) {
        // possible performance issue, not taking advantage of possible inplace
        return x.xor(this);
    }

    protected void loadData(final BitmapContainer bitmapContainer) {
        cardinality = bitmapContainer.cardinality;
        bitmapContainer.fillArray(content);
    }

    protected void loadDataWithSkipValue(
        final BitmapContainer bitmapContainer, final short valueToSkip,
        final PositionHint positionHintOut) {
        cardinality = bitmapContainer.fillArrayWithSkipValue(content, valueToSkip, positionHintOut);
    }

    // for use in inot range known to be nonempty
    private void negateRange(final int valuesInRange,
        final int startIndex, final int lastIndex,
        final int startRange, final int lastRange) {
        // compute the negation into buffer
        final short[] buf = threadLocalBuf.get();
        final short[] buffer = buf.length >= valuesInRange ? buf : new short[valuesInRange];

        int outPos = 0;
        int inPos = startIndex; // value here always >= valInRange,
        // until it is exhausted
        // n.b., we can start initially exhausted.

        int valInRange = startRange;
        for (; valInRange < lastRange && inPos <= lastIndex; ++valInRange) {
            if ((short) valInRange != content[inPos]) {
                buffer[outPos++] = (short) valInRange;
            } else {
                ++inPos;
            }
        }

        // if there are extra items (greater than the biggest
        // pre-existing one in range), buffer them
        for (; valInRange < lastRange; ++valInRange) {
            buffer[outPos++] = (short) valInRange;
        }

        // copy back from buffer...caller must ensure there is room
        int i = startIndex;
        for (int j = 0; j < outPos; ++j) {
            content[i++] = buffer[j];
        }
    }

    // shares lots of code with inot; candidate for refactoring
    @Override
    public Container not(final int firstOfRange, final int lastOfRange) {
        // TODO: may need to convert to a RunContainer
        if (firstOfRange >= lastOfRange) {
            return cowRef(); // empty range
        }

        // determine the span of array indices to be affected
        int startIndex =
            ContainerUtil.unsignedBinarySearch(content, 0, cardinality, (short) firstOfRange);
        if (startIndex < 0) {
            startIndex = -startIndex - 1;
        }
        int lastIndex =
            ContainerUtil.unsignedBinarySearch(content, 0, cardinality, (short) (lastOfRange - 1));
        if (lastIndex < 0) {
            lastIndex = -lastIndex - 2;
        }
        final int currentValuesInRange = lastIndex - startIndex + 1;
        final int spanToBeFlipped = lastOfRange - firstOfRange;
        final int newValuesInRange = spanToBeFlipped - currentValuesInRange;
        final int cardinalityChange = newValuesInRange - currentValuesInRange;
        final int newCardinality = cardinality + cardinalityChange;

        if (newCardinality > DEFAULT_MAX_SIZE) {
            return toBiggerCardinalityContainer(newCardinality).not(firstOfRange, lastOfRange);
        }

        ArrayContainer answer = new ArrayContainer(newCardinality);

        // copy stuff before the active area
        System.arraycopy(content, 0, answer.content, 0, startIndex);

        int outPos = startIndex;
        int inPos = startIndex; // item at inPos always >= valInRange

        int valInRange = firstOfRange;
        for (; valInRange < lastOfRange && inPos <= lastIndex; ++valInRange) {
            if ((short) valInRange != content[inPos]) {
                answer.content[outPos++] = (short) valInRange;
            } else {
                ++inPos;
            }
        }

        for (; valInRange < lastOfRange; ++valInRange) {
            answer.content[outPos++] = (short) valInRange;
        }

        // content after the active range
        for (int i = lastIndex + 1; i < cardinality; ++i) {
            answer.content[outPos++] = content[i];
        }
        answer.cardinality = newCardinality;
        return answer.maybeSwitchContainer();
    }

    @Override
    int numberOfRuns() {
        if (cardinality == 0) {
            return 0; // should never happen
        }
        int numRuns = 1;
        int oldv = ContainerUtil.toIntUnsigned(content[0]);
        for (int i = 1; i < cardinality; i++) {
            int newv = ContainerUtil.toIntUnsigned(content[i]);
            if (oldv + 1 != newv) {
                ++numRuns;
            }
            oldv = newv;
        }
        return numRuns;
    }

    @Override
    public Container or(final ArrayContainer value2) {
        if (value2.isEmpty()) {
            return cowRef();
        }
        if (isEmpty()) {
            return value2.cowRef();
        }
        final ArrayContainer value1 = this;
        int totalCardinality = value1.getCardinality() + value2.getCardinality();
        if (totalCardinality > DEFAULT_MAX_SIZE) {// it could be a bitmap!
            BitmapContainer bc = new BitmapContainer();
            for (int k = 0; k < value2.cardinality; ++k) {
                short v = value2.content[k];
                final int i = ContainerUtil.toIntUnsigned(v) >>> 6;
                bc.bitmap[i] |= (1L << v);
            }
            for (int k = 0; k < cardinality; ++k) {
                short v = content[k];
                final int i = ContainerUtil.toIntUnsigned(v) >>> 6;
                bc.bitmap[i] |= (1L << v);
            }
            bc.cardinality = 0;
            for (long k : bc.bitmap) {
                bc.cardinality += Long.bitCount(k);
            }
            if (bc.cardinality <= DEFAULT_MAX_SIZE) {
                return bc.toArrayContainer();
            } else if (bc.isAllOnes()) {
                return Container.full();
            }
            return bc;
        }
        ArrayContainer answer = new ArrayContainer(totalCardinality);
        answer.cardinality =
            ContainerUtil.unsignedUnion2by2(
                value1.content, 0, value1.getCardinality(),
                value2.content, 0, value2.getCardinality(),
                answer.content);
        return answer;
    }

    @Override
    public Container or(final BitmapContainer x) {
        return x.or(this);
    }

    @Override
    public Container or(final RunContainer x) {
        return x.or(this);
    }

    protected Container or(final ShortIterator it) {
        return or(it, false);
    }

    private void forceAppend(final short val) {
        if (cardinality == content.length) {
            increaseCapacity(true);
        }
        content[cardinality++] = val;
    }

    /**
     * it must return items in (unsigned) sorted order. Possible candidate for Container interface?
     **/
    private Container or(final ShortIterator it, final boolean exclusive) {
        final ArrayContainer ac = new ArrayContainer();
        int myItPos = 0;
        // do a merge. int -1 denotes end of input.
        int myHead =
            (myItPos == cardinality) ? -1 : ContainerUtil.toIntUnsigned(content[myItPos++]);
        int hisHead = advance(it);

        while (myHead != -1 && hisHead != -1) {
            if (myHead < hisHead) {
                ac.forceAppend((short) myHead);
                myHead =
                    (myItPos == cardinality) ? -1 : ContainerUtil.toIntUnsigned(content[myItPos++]);
            } else if (myHead > hisHead) {
                ac.forceAppend((short) hisHead);
                hisHead = advance(it);
            } else {
                if (!exclusive) {
                    ac.forceAppend((short) hisHead);
                }
                hisHead = advance(it);
                myHead =
                    (myItPos == cardinality) ? -1 : ContainerUtil.toIntUnsigned(content[myItPos++]);
            }
        }

        while (myHead != -1) {
            ac.forceAppend((short) myHead);
            myHead =
                (myItPos == cardinality) ? -1 : ContainerUtil.toIntUnsigned(content[myItPos++]);
        }

        while (hisHead != -1) {
            ac.forceAppend((short) hisHead);
            hisHead = advance(it);
        }

        if (ac.cardinality > DEFAULT_MAX_SIZE) {
            return ac.toBiggerCardinalityContainer(ac.cardinality);
        } else {
            return ac;
        }
    }

    @Override
    public int rank(final short lowbits) {
        int answer = ContainerUtil.unsignedBinarySearch(content, 0, cardinality, lowbits);
        if (answer >= 0) {
            return answer + 1;
        } else {
            return -answer - 1;
        }
    }

    private void removeAtIndex(final int loc) {
        System.arraycopy(content, loc + 1, content, loc, cardinality - loc - 1);
        --cardinality;
    }

    @Override
    public Container iunset(final short x) {
        return unsetImpl(x, true, null);
    }

    @Override
    public Container unset(final short x) {
        return unsetImpl(x, false, null);
    }

    @Override
    Container iunset(final short x, final PositionHint positionHint) {
        return unsetImpl(x, true, positionHint);
    }

    @Override
    Container unset(final short x, final PositionHint positionHint) {
        return unsetImpl(x, false, positionHint);
    }

    private Container unsetImpl(final short x, final boolean inPlace,
        final PositionHint positionHint) {
        final int searchStartPos = getIfNotNullAndNonNegative(positionHint, 0);
        final int loc = ContainerUtil.unsignedBinarySearch(content, searchStartPos, cardinality, x);
        if (loc < 0) {
            setIfNotNull(positionHint, ~loc);
            return inPlace ? this : cowRef();
        }
        final int newCardinality = cardinality - 1;
        if (newCardinality == 0) {
            resetIfNotNull(positionHint);
            return Container.empty();
        }
        if (newCardinality == 1) {
            resetIfNotNull(positionHint);
            return Container.singleton(content[loc == 0 ? 1 : 0]);
        }
        if (newCardinality == 2) {
            final int i0;
            final int i1;
            if (loc == 0) {
                i0 = 1;
                i1 = 2;
            } else if (loc == 1) {
                i0 = 0;
                i1 = 2;
            } else {
                i0 = 0;
                i1 = 1;
            }
            resetIfNotNull(positionHint);
            return Container.twoValues(content[i0], content[i1]);
        }
        final ArrayContainer ans = inPlace ? deepcopyIfShared() : deepCopy();
        ans.removeAtIndex(loc);
        setIfNotNull(positionHint, loc);
        return ans;
    }

    @Override
    public Container runOptimize() {
        final Container c = maybeSwitchContainer();
        if (c != this) {
            return c;
        }
        // TODO: consider borrowing the BitmapContainer idea of early
        // abandonment
        // with ArrayContainers, when the number of runs in the arrayContainer
        // passes some threshold based on the cardinality.
        int numRuns = numberOfRuns();
        if (numRuns == 1) {
            return Container.singleRange(first(), last() + 1);
        }
        int sizeAsRunContainer = RunContainer.sizeInBytes(numRuns);
        if (bytesUsed() > sizeAsRunContainer) {
            return new RunContainer(this, numRuns); // this could be maybe
            // faster if initial
            // container is a bitmap
        } else {
            compact();
            return this;
        }
    }

    private void compact() {
        if (shared || content.length == cardinality
            || (cardinality == 0 && content.length == DEFAULT_INIT_SIZE)) {
            return;
        }
        final short[] newContent =
            new short[cardinality == 0 ? DEFAULT_INIT_SIZE : shortArraySizeRounding(cardinality)];
        System.arraycopy(content, 0, newContent, 0, cardinality);
        content = newContent;
    }

    @Override
    public short select(int j) {
        return content[j];
    }

    @Override
    public Container select(final int startRank, final int endRank) {
        return new ArrayContainer(this, startRank, endRank);
    }

    @Override
    public int find(short x) {
        return ContainerUtil.unsignedBinarySearch(content, 0, cardinality, x);
    }

    @Override
    public void selectRanges(RangeConsumer outValues, RangeIterator inPositions) {
        if (!inPositions.hasNext()) {
            return;
        }
        int ostart = -1;
        int oend = -1; // inclusive
        do {
            inPositions.next();
            int istart = inPositions.start();
            int iend = inPositions.end();
            if (iend > cardinality) {
                throw new IllegalArgumentException("selectRanges for invalid position=" + iend);
            }
            for (int pos = istart; pos < iend; ++pos) {
                int key = ContainerUtil.toIntUnsigned(select(pos));
                if (ostart == -1) {
                    ostart = oend = key;
                } else {
                    if (key == oend + 1) {
                        oend = key;
                    } else {
                        outValues.accept(ostart, oend + 1);
                        ostart = oend = key;
                    }
                }
            }
        } while (inPositions.hasNext());
        outValues.accept(ostart, oend + 1);
    }

    @Override
    public Container iandRange(final int start, final int end) {
        return andRangeImpl(!shared, start, end);
    }

    @Override
    public Container andRange(final int start, final int end) {
        return andRangeImpl(false, start, end);
    }

    private Container andRangeImpl(final boolean inPlace, final int start, final int end) {
        if (end <= start || isEmpty()) {
            return Container.empty();
        }
        int firstPos = ContainerUtil.unsignedBinarySearch(content, 0, cardinality,
            ContainerUtil.lowbits(start));
        if (firstPos < 0) {
            firstPos = ~firstPos;
            if (firstPos >= cardinality || ContainerUtil.toIntUnsigned(content[firstPos]) >= end) {
                return Container.empty();
            }
        }
        // inclusive.
        int lastPos = ContainerUtil.unsignedBinarySearch(content, firstPos, cardinality,
            ContainerUtil.lowbits(end - 1));
        if (lastPos < 0) {
            lastPos = ~lastPos - 1;
        }
        if (firstPos == 0 && lastPos == cardinality - 1) {
            if (inPlace) {
                return this;
            }
            return cowRef();
        }
        final int newCard = lastPos - firstPos + 1;
        if (ImmutableContainer.ENABLED) {
            if (newCard == 1) {
                return Container.singleton(content[firstPos]);
            }
            if (newCard == 2) {
                return Container.twoValues(content[firstPos], content[lastPos]);
            }
        }
        if (inPlace) {
            System.arraycopy(content, firstPos, content, 0, newCard);
            final int dcard = cardinality - newCard;
            cardinality = newCard;
            if (dcard > newCard) {
                compact();
            }
            return this;
        }
        return new ArrayContainer(this, firstPos, lastPos + 1);
    }

    @Override
    public boolean findRanges(RangeConsumer outPositions, RangeIterator inValues, int maxPos) {
        if (!inValues.hasNext()) {
            return false;
        }
        int ostart = -1;
        int oend = -1; // inclusive
        int startSearch = 0;
        do {
            inValues.next();
            int istart = inValues.start();
            int iend = inValues.end();
            for (int key = istart; key < iend; ++key) {
                if (startSearch > maxPos) {
                    return true;
                }
                int pos = ContainerUtil.unsignedBinarySearch(content, startSearch, cardinality,
                    ContainerUtil.lowbits(key));
                if (pos < 0) {
                    throw new IllegalArgumentException("findRanges for invalid key=" + key);
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
                    if (pos == oend + 1) {
                        oend = pos;
                    } else {
                        outPositions.accept(ostart, oend + 1);
                        ostart = oend = pos;
                    }
                }
                startSearch = pos + 1;
            }
        } while (inValues.hasNext());
        outPositions.accept(ostart, oend + 1);
        return false;
    }

    /**
     * Copies the data in a bitmap container.
     *
     * @return the bitmap container
     */
    @Override
    public BitmapContainer toBitmapContainer() {
        BitmapContainer bc = new BitmapContainer();
        bc.loadData(this);
        return bc;
    }

    @Override
    public int nextValue(short fromValue) {
        int index = ContainerUtil.advanceUntil(content, -1, cardinality, fromValue);
        int effectiveIndex = index >= 0 ? index : -index - 1;
        return effectiveIndex >= cardinality ? -1
            : ContainerUtil.toIntUnsigned(content[effectiveIndex]);
    }

    private void assertNonEmpty() {
        if (cardinality == 0) {
            throw new NoSuchElementException("Empty " + getContainerName());
        }
    }

    @Override
    public int first() {
        assertNonEmpty();
        return ContainerUtil.toIntUnsigned(content[0]);
    }

    @Override
    public int last() {
        assertNonEmpty();
        return ContainerUtil.toIntUnsigned(content[cardinality - 1]);
    }

    @Override
    public void trim() {
        compact();
    }

    @Override
    public Container xor(final ArrayContainer value2) {
        final ArrayContainer value1 = this;
        final int totalCardinality = value1.getCardinality() + value2.getCardinality();
        if (totalCardinality > DEFAULT_MAX_SIZE) {// it could be a bitmap!
            BitmapContainer bc = new BitmapContainer();
            for (int k = 0; k < value2.cardinality; ++k) {
                short v = value2.content[k];
                final int i = ContainerUtil.toIntUnsigned(v) >>> 6;
                bc.bitmap[i] ^= (1L << v);
            }
            for (int k = 0; k < cardinality; ++k) {
                short v = content[k];
                final int i = ContainerUtil.toIntUnsigned(v) >>> 6;
                bc.bitmap[i] ^= (1L << v);
            }
            bc.cardinality = 0;
            for (long k : bc.bitmap) {
                bc.cardinality += Long.bitCount(k);
            }
            if (bc.cardinality <= DEFAULT_MAX_SIZE) {
                return bc.toArrayContainer();
            }
            return bc;
        }
        ArrayContainer answer = new ArrayContainer(totalCardinality);
        answer.cardinality =
            ContainerUtil.unsignedExclusiveUnion2by2(value1.content, value1.getCardinality(),
                value2.content, value2.getCardinality(), answer.content);
        return answer.maybeSwitchContainer();
    }

    @Override
    public Container xor(BitmapContainer x) {
        return x.xor(this);
    }

    @Override
    public Container xor(RunContainer x) {
        return x.xor(this);
    }


    protected Container xor(ShortIterator it) {
        return or(it, true);
    }

    @Override
    public boolean forEach(ShortConsumer sc) {
        return forEach(0, sc);
    }

    @Override
    public boolean forEach(final int rankOffset, final ShortConsumer sc) {
        for (int k = rankOffset; k < cardinality; ++k) {
            final boolean wantMore = sc.accept(content[k]);
            if (!wantMore) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean forEachRange(final int rankOffset, final ShortRangeConsumer sc) {
        if (rankOffset >= cardinality) {
            return true;
        }
        int k = rankOffset;
        short pendingStart = content[k];
        short pendingEnd = pendingStart;

        for (k = rankOffset + 1; k < cardinality; ++k) {
            final short v = content[k];
            if (pendingEnd + 1 == v) {
                pendingEnd = v;
            } else {
                if (!sc.accept(pendingStart, pendingEnd)) {
                    return false;
                }
                pendingStart = pendingEnd = v;
            }
        }
        return sc.accept(pendingStart, pendingEnd);
    }

    @Override
    public boolean subsetOf(ArrayContainer c) {
        if (isEmpty()) {
            return true;
        }
        if (c.isEmpty()) {
            return false;
        }
        if (cardinality > c.cardinality ||
            first() < c.first() ||
            last() > c.last()) {
            return false;
        }
        int ci = 0;
        for (int i = 0; i < cardinality; ++i) {
            if (ci >= c.cardinality) {
                return false;
            }
            short k = content[i];
            int s = ContainerUtil.unsignedBinarySearch(c.content, ci, c.cardinality, k);
            if (s < 0) {
                return false;
            }
            ci = s + 1;
        }
        return true;
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
        if (cardinality > c.getCardinality()) {
            return false;
        }
        for (int i = 0; i < cardinality; ++i) {
            short k = content[i];
            if (!c.contains(k)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean subsetOf(RunContainer c) {
        if (isEmpty()) {
            return true;
        }
        if (c.isEmpty()) {
            return false;
        }
        // Don't do a cardinality based check since that is not cached in run.
        if (first() < c.first() || last() > c.last()) {
            return false;
        }
        int ci = 0;
        for (int i = 0; i < cardinality; ++i) {
            short k = content[i];
            int s = c.searchFrom(k, ci);
            if (s < 0) {
                return false;
            }
            ci = s;
        }
        return true;
    }

    private static boolean overlaps(final ArrayContainer c1, final ArrayContainer c2) {
        int c2i = 0;
        for (int c1i = 0; c1i < c1.cardinality; ++c1i) {
            if (c2i >= c2.cardinality) {
                return false;
            }
            short k = c1.content[c1i];
            int s = ContainerUtil.unsignedBinarySearch(c2.content, c2i, c2.cardinality, k);
            if (s >= 0) {
                return true;
            }
            c2i = -(s + 1);
        }
        return false;
    }

    // rangeEnd is exclusive.
    @Override
    public boolean overlapsRange(final int rangeStart, final int rangeEnd) {
        int s = ContainerUtil.unsignedBinarySearch(content, 0, cardinality, (short) rangeStart);
        if (s >= 0) {
            return true;
        }
        s = ~s;
        if (s >= cardinality) {
            return false;
        }
        final int last = rangeEnd - 1;
        int e = ContainerUtil.unsignedBinarySearch(content, s, cardinality, (short) last);
        if (e >= 0) {
            return true;
        }
        e = ~e;
        // If s and e "sandwich" some element, we have an overlap.
        return s != e;
    }

    @Override
    public boolean overlaps(final ArrayContainer c) {
        return (cardinality < c.cardinality) ? overlaps(this, c) : overlaps(c, this);
    }

    @Override
    public boolean overlaps(final BitmapContainer c) {
        if (c.isEmpty()) {
            return false;
        }
        // Don't do a first/last based check since those are not cached in bitmap.
        // BitmapContainers should not normally have more elements than an array container.
        for (int i = 0; i < cardinality; ++i) {
            short k = content[i];
            if (c.contains(k)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean overlaps(final RunContainer c) {
        return (getCardinality() < c.getCardinality()) ? ContainerUtil.overlaps(this, c)
            : ContainerUtil.overlaps(c, this);
    }

    @Override
    public final void setCopyOnWrite() {
        if (shared) {
            return;
        }
        shared = true;
        onCopyOnWrite();
    }

    protected void onCopyOnWrite() {}

    private ArrayContainer deepcopyIfShared() {
        return shared ? deepCopy() : this;
    }

    @Override
    public int bytesAllocated() {
        return Short.BYTES * content.length;
    }

    @Override
    public int bytesUsed() {
        return Short.BYTES * cardinality;
    }

    @Override
    public Container toLargeContainer() {
        return this;
    }

    @Override
    public void validate() {
        int prev = -1;
        for (int i = 0; i < cardinality; ++i) {
            final int val = ContainerUtil.toIntUnsigned(content[i]);
            if (val <= prev || val > MAX_VALUE) {
                throw new IllegalStateException("i=" + i + ", prev=" + prev + ", val=" + val);
            }
            prev = val;
        }
    }

    @Override
    public boolean isShared() {
        return shared;
    }
}
