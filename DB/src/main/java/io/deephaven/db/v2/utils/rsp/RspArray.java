package io.deephaven.db.v2.utils.rsp;

import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.v2.utils.*;
import io.deephaven.db.v2.utils.singlerange.SingleRange;
import io.deephaven.db.v2.utils.sortedranges.SortedRanges;
import io.deephaven.db.v2.utils.sortedranges.SortedRangesInt;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;
import io.deephaven.db.v2.utils.rsp.container.Container;
import io.deephaven.db.v2.utils.rsp.container.ArrayContainer;
import io.deephaven.db.v2.utils.rsp.container.BitmapContainer;
import io.deephaven.db.v2.utils.rsp.container.TwoValuesContainer;
import io.deephaven.db.v2.utils.rsp.container.SingleRangeContainer;
import io.deephaven.db.v2.utils.rsp.container.RunContainer;
import io.deephaven.db.v2.utils.rsp.container.SearchRangeIterator;
import io.deephaven.util.annotations.VisibleForTesting;

import java.util.PrimitiveIterator;
import java.util.function.IntToLongFunction;
import java.util.function.LongConsumer;

import static io.deephaven.db.v2.utils.IndexUtilities.Comparator;

/**
 *
 * <p>
 * A set representation for long values using Regular Space Partitioning (RSP) of the long space in "blocks" of (2^16)
 * elements.
 * </p>
 *
 * <p>
 * Modeled heavily after roaringbitmap.RoaringArray (keeping API method names and semantics as much as possible), with
 * modifications for:
 * </p>
 *
 * <ol>
 * <li>Full "unsigned long" 64 bit range (as opposed to 32 bit in RoaringArray).</li>
 * <li>Spans of all bits set ("AllSet") that can be arbitrarily big (ie, not constrained to 2^16 = RB Container
 * size).</li>
 * </ol>
 *
 * <p>
 * The handling of unsigned values follows RB; ie, key values are compared/sorted as unsigned longs.
 * </p>
 *
 * <p>
 * Definitions:
 * </p>
 *
 * <ul>
 * <li>A "block" is a particular interval [n*2^16, (n+1)*2^16 - 1] of the long domain.</li>
 * <li>A "span" is a partition of the domain consisting of one or more consecutive blocks;</li> a span is a subset of
 * the domain represented by an interval [n*2^16, (n+m)*2^16 - 1], m >= 1.
 * <li>Full blocks are blocks whose domain are fully contained in the set, ie, the set contains every possible value in
 * the block's interval (as a bitmap, it would be "all ones").</li>
 * <li>Spans of full blocks are represented by a single "full blocks span" object (just a Long) which knows how many
 * 2^16 ranges it has (it's "full blocks span len" ("flen") is the number of full blocks in the span).</li>
 * <li>Individual blocks that are not completely full are stored in an RB Container; their "full blocks span len" is
 * zero.</li>
 * </ul>
 *
 * <p>
 * Our internal representation uses two parallel arrays:
 * </p>
 *
 * <ul>
 * <li>a <code>long[] spanInfos</code> array that contains the information for the offset to the values in the span,
 * which we call the span's "key". For instance, a full block span that represents all the long values in [65536,
 * 196607] has as its key the value 65536.</li>
 * <li>an <code>Object[] spans</code> array that contains the actual spans. At the most basic level, a span can be
 * either a full block span or a container of values (but there is nuance in exactly how to represent them, see
 * below).</li>
 * </ul>
 *
 * <p>
 * We use several optimizations to reduce memory utilization for sparse sets. Details follow.
 * </p>
 *
 * <p>
 * The <code>long[] spanInfos</code> and <code>Object[] spans</code> data members of this class are used, combined, to
 * represent the offset (key) and span values in the set, against that offset. The two arrays are used, together, as
 * parallel arrays and the information for a given conceptual span is contained in both of them for the same
 * corresponding index i.
 * </p>
 *
 * <p>
 * There are two basic cases for a span: it is either a full blocks span, containing a >=1 number of full blocks, or it
 * is a container, containing individual values in the particular 2^16 block corresponding to the span's key.
 * </p>
 *
 * <p>
 * There are four ways total that these two cases can be represented between the long in the `spanInfos` array and the
 * Object in the `spans` array. Given a span at position `i`:
 * </p>
 *
 * <ol>
 * <li>If the corresponding <code>Object spans[i]</code> is of type Long, then the <code>long spanInfos[i]</code> value
 * is the key for the span (with its lower 16 bits as zero), and the Long value represents how many full blocks are
 * present. Example, the set [ 0, 2^50 - 1 ] is represented as spanInfo==0 and span==Long(2^34).</li>
 *
 * <li>As an optimization to conserve memory, if the Object spans[i] is the Object reference with value
 * <code>FULL_BLOCK_SPAN_MARKER</code> (a singleton and final marker Object defined statically in this file). then the
 * upper 48 bits of the <code>long spanInfo[i]</code> value represent the key for the span, and the lower 16 bits of the
 * <code>long spanInfo[i]</code> value represent the full block span length. Example, the set [ 65536, 196607 ] is
 * represented by <code>spanInfo==65538</code> and <code>span==FULL_BLOCK_SPAN_MARKER</code> (note
 * <code>196607 == 65536*3 - 1</code>, so the set is 2 full blocks, and <code>65538 == 65536 | 2</code>.</li>
 *
 * <li>If the corresponding <code>Object spans[i]</code> is null, then the <code>long spanInfos[i]</code> represents the
 * single value present in the span (note in this case, its upper 16 bits still corresponds to its key). Example, the
 * set { 65537 } is represented by spanInfo==65537 and span==null.</li>
 *
 * <li>If the corresponding <code>Object spans[i]</code> is of type <code>short[]</code> or of type
 * <code>Container</code>, then it represents a container of multiple values in a single block (but not all of the
 * possible values in the block, since in that case it would be a full block span as above). In this case the higher 48
 * bits of its corresponding spanInfo represent the key for the span. Depending on the actual type of span there are two
 * subcases:
 *
 * <ol type="a">
 * <li>If <code>spans[i]</code> is of type <code>Container</code>, then the values in the roaringbitmaps container
 * object are part of the set, considered against its key offset. The key is represented in the higher 48 bits of its
 * corresponding spaninfo. The lower 16 bits of spanInfo are zero in this case. Example, the set [ 100,000-100,010,
 * 100,020-100,030 ] is represented by <code>spaInfo==65536</code>,
 * <code>span==RunContainer({34464-34474, 34484-34494})</code></li>
 *
 * <li>If <code>spans[i]</code> is of type <code>short[]</code>, then an <code>ArrayContainer</code> with the
 * <code>short[]</code> contents needs to be reconstructed. The lower 16 bits of the spanInfo value are used to
 * represent the other data members of ArrayContainer. This case exists as an optimization to reduce memory utilization
 * for sparse blocks. For details of this reconstruction please see the code for the definition of the SpanView class
 * below.</li>
 * </ol>
 * </li>
 * </ol>
 *
 * <p>
 * Notes:
 * </p>
 *
 * <ul>
 * <li>Our version of RB Container supports a "shared" boolean flag that is used to implement copy-on-write (COW)
 * semantics and allow operation results to share containers in COW fashion.</li>
 * <li>We extended the Container class hierarchy to include specializations for empty, single value, single range, and
 * two values containers. These are immutable; empty is used only as a way to return empty results, and are never actual
 * stored in the spans array. For details, please see the Container class definition and derived class hierarchy.</li>
 * </ul>
 */
public abstract class RspArray<T extends RspArray> extends RefCountedCow<T> {

    public static final boolean debug =
            Configuration.getInstance().getBooleanForClassWithDefault(RspArray.class, "debug", false);

    private static final int doublingAllocThreshold = Configuration.getInstance().getIntegerForClassWithDefault(
            RspArray.class, "doublingAllocThreshold", 1024);
    // minimum growth size after passing doubling alloc threshold
    private static final int linearAllocStep = Configuration.getInstance().getIntegerForClassWithDefault(
            RspArray.class, "linearAllocStep", 1024);
    // after doublingAllocThreshold, growth rate is (1 + 2^-n) with minimum step size of linearAllocStep (all rounded to
    // nearest multiple of 1024)
    private static final int logarithmicAllocGrowthRate = Configuration.getInstance().getIntegerForClassWithDefault(
            RspArray.class, "logarithmicAllocGrowthRate", 4);
    // when size > accNullThreshold, the cardinality cache array is populated, otherwise is kept null.
    static final int accNullThreshold = Configuration.getInstance().getIntegerForClassWithDefault(
            RspArray.class, "accNullThreshold", 8);

    static {
        Assert.assertion(0 <= logarithmicAllocGrowthRate && logarithmicAllocGrowthRate < 32,
                "RspArray.logarithmicAllocGrowthRate must be >= 0 and < 32");
    }

    // BLOCK_SIZE should be a power of 2.
    public static final int BLOCK_SIZE = Container.MAX_RANGE;
    // BLOCK_LAST is used both as the last valid position in a block, and as a bitmask to get
    // the block offset for a long value.
    // Note that since BLOCK_SIZE is a power of 2, BLOCK_SIZE - 1 is an all-bits-one mask for the bits
    // below the single most significant bit in BLOCK_SIZE.
    public static final int BLOCK_LAST = (BLOCK_SIZE - 1);
    public static final int BITS_PER_BLOCK = Integer.numberOfTrailingZeros(BLOCK_SIZE);
    static {
        Assert.assertion(Integer.bitCount(BLOCK_SIZE) == 1,
                "RspArray.BITS_PER_BLOCK should be a power of 2.");
        Assert.assertion((BLOCK_LAST & (BLOCK_LAST + 1)) == 0,
                "BLOCK_LAST is not a bitmask.");
    }

    protected boolean shareContainers() {
        return true;
    }

    protected abstract T make(final RspArray src,
            final int startIdx, final long startOffset,
            final int endIdx, final long endOffset);

    protected abstract T make();

    public static long highBits(final long val) {
        return val & ~((long) BLOCK_LAST);
    }

    public static short lowBits(final long val) {
        return (short) (val & BLOCK_LAST);
    }

    public static int lowBitsAsInt(final long val) {
        return (int) (val & BLOCK_LAST);
    }

    public static long divBlockSize(final long v) {
        return v / BLOCK_SIZE; // division by a constant power of two is optimized fine.
    }

    public static int modBlockSize(final long v) {
        // modulo by a constant power of two can't be optimized if the compiler can't tell if v is negative;
        // we know it isn't.
        return (int) (v & (long) BLOCK_LAST);
    }

    /**
     * Array of keys (in the long's higher 48 bits) and other span data (in the long's lower 16 bits) parallel to the
     * spans array, mapping the long value in a given array position to the corresponding span in the same position.
     * Please see the documentation for this class for details of the different cases for the lower 16 bits, depending
     * on the type of span.
     *
     * Values are kept in unsigned sorted order according to higher 16 bits to enable binary search of keys.
     */
    protected long[] spanInfos;

    /**
     * Array of Spans parallel to the spanInfos array, mapping the same index to the corresponding span for the
     * spanInfo. Please see the documentation for this class for the different possible types allowed and their
     * meanings.
     */
    protected Object[] spans;

    /**
     * How many spans we have. Also how many spanInfos, since they are parallel arrays.
     */
    int size;

    @VisibleForTesting
    long[] getKeys() {
        final long[] out = new long[size];
        for (int i = 0; i < size; ++i) {
            out[i] = spanInfoToKey(getSpanInfo(i));
        }
        return out;
    }

    int getSize() {
        return size;
    }

    @VisibleForTesting
    Object[] getSpans() {
        return spans;
    }

    protected final long getSpanInfo(final int i) {
        return spanInfos[i];
    }

    protected static long spanInfoToKey(final long spanInfo) {
        return highBits(spanInfo);
    }

    protected final long getKey(final int i) {
        return spanInfoToKey(spanInfos[i]);
    }

    protected final long getSingletonSpanValue(final int i) {
        return spanInfos[i];
    }

    protected static long spanInfoToSingletonSpanValue(final long spanInfo) {
        return spanInfo;
    }

    // set singleton span without calling modified(i).
    protected final void setSingletonSpanRaw(final int i, final long value) {
        if (value < 0) {
            throw new IllegalArgumentException("value=" + value);
        }
        spans[i] = null;
        spanInfos[i] = value;
    }

    protected final void setSingletonSpan(final int i, final long value) {
        setSingletonSpanRaw(i, value);
        modifiedSpan(i);
    }

    protected final void applyKeyOffset(final int i, final long offset) {
        spanInfos[i] += offset;
    }

    public static final Object FULL_BLOCK_SPAN_MARKER = new Object() {
        public String toString() {
            return "full block span"; // to facilitate debugger inspection.
        }
    };

    // set full block span without calling modified(i).
    protected void setFullBlockSpanRaw(final int i, final long key, final long flen) {
        setFullBlockSpanRaw(i, spanInfos, spans, key, flen);
    }

    protected static void setFullBlockSpanRaw(
            final int i, final long[] spanInfos, final Object[] spans,
            final long key, final long flen) {
        if (key < 0 || flen <= 0) {
            throw new IllegalArgumentException("i=" + i + ", key=" + key + ", flen=" + flen);
        }
        final long lowflen = lowBitsAsInt(flen);
        if (lowflen == flen) {
            spanInfos[i] = key | lowflen;
            spans[i] = FULL_BLOCK_SPAN_MARKER;
            return;
        }
        spanInfos[i] = key;
        spans[i] = flen;
    }

    protected void setFullBlockSpan(final int i, final long key, final long flen) {
        setFullBlockSpanRaw(i, key, flen);
        modifiedSpan(i);
    }

    public static long getPackedInfoLowBits(final ArrayContainer ac) {
        final long sharedBit = ac.isShared() ? SPANINFO_ARRAYCONTAINER_SHARED_BITMASK : 0L;
        final long cardinalityBits = SPANINFO_ARRAYCONTAINER_CARDINALITY_BITMASK & (long) ac.getCardinality();
        return sharedBit | cardinalityBits;
    }

    protected static void setContainerSpanRaw(
            final long[] spanInfos, final Object[] spans, final int i, final long key, final Container container) {
        if (container instanceof ArrayContainer) {
            final ArrayContainer ac = (ArrayContainer) container;
            spanInfos[i] = key | getPackedInfoLowBits(ac);
            spans[i] = ac.getContent();
            return;
        }
        spanInfos[i] = key;
        spans[i] = container;
    }

    // set container without calling modified(i).
    protected void setContainerSpanRaw(final int i, final long key, final Container container) {
        setContainerSpanRaw(spanInfos, spans, i, key, container);
    }

    protected void appendSharedContainer(
            final RspArray other, final long otherSpanInfo, final Container container) {
        if (size > 0) {
            tryOptimizeContainer(size - 1);
        }
        ensureSizeCanGrowBy(1);
        setSharedContainerRaw(size, other, otherSpanInfo, container);
        ++size;
    }

    protected void appendSharedContainerMaybePacked(
            final RspArray other, final int otherIdx, final long otherSpanInfo, final Object otherContainer) {
        if (size > 0) {
            tryOptimizeContainer(size - 1);
        }
        ensureSizeCanGrowBy(1);
        setSharedContainerMaybePackedRaw(size, other, otherIdx, otherSpanInfo, otherContainer);
        ++size;
    }

    protected void setSharedContainerMaybePackedRaw(
            final int i, final RspArray src, final int srcIdx, final long srcSpanInfo, final Object srcContainer) {
        if (srcContainer instanceof short[]) {
            spanInfos[i] = (src.spanInfos[srcIdx] |= SPANINFO_ARRAYCONTAINER_SHARED_BITMASK);
            spans[i] = srcContainer;
            return;
        }
        setContainerSpanRaw(i, srcSpanInfo, src.shareContainer((Container) srcContainer));
    }

    protected void insertSharedContainer(
            final int i, final RspArray other, final long otherSpanInfo, final Container otherContainer) {
        open(i);
        setSharedContainerRaw(i, other, otherSpanInfo, otherContainer);
        modifiedSpan(i);
    }

    protected void setSharedContainerRaw(
            final int i, final RspArray other, final long key, final Container container) {
        setContainerSpanRaw(i, key, other.shareContainer(container));
    }

    protected void copyKeyAndSpanStealingContainers(
            final int srcIdx, final long[] srcSpanInfos, final Object[] srcSpans,
            final int dstIdx, final long[] dstSpanInfos, final Object[] dstSpans) {
        dstSpanInfos[dstIdx] = srcSpanInfos[srcIdx];
        dstSpans[dstIdx] = srcSpans[srcIdx];
    }

    private static final long SPANINFO_ARRAYCONTAINER_SHARED_BITMASK = (1L << 15);
    private static final long SPANINFO_ARRAYCONTAINER_CARDINALITY_BITMASK =
            ~SPANINFO_ARRAYCONTAINER_SHARED_BITMASK & (long) BLOCK_LAST;

    // shiftAmount is a multiple of BLOCK_SIZE.
    protected void copyKeyAndSpanMaybeSharing(
            final long shiftAmount,
            final RspArray src, final int srcIdx,
            final long[] dstSpanInfos, final Object[] dstSpans, final int dstIdx,
            final boolean tryShare) {
        Object span = src.spans[srcIdx];
        if (tryShare && src.shareContainers()) {
            if (span instanceof short[]) {
                src.spanInfos[srcIdx] |= SPANINFO_ARRAYCONTAINER_SHARED_BITMASK;
            } else if (span instanceof Container) {
                final Container c = (Container) span;
                c.setCopyOnWrite();
            }
        }
        dstSpanInfos[dstIdx] = src.spanInfos[srcIdx] + shiftAmount;
        dstSpans[dstIdx] = span;
    }

    protected void copyKeyAndSpanMaybeSharing(
            final RspArray src, final int srcIdx,
            final long[] dstSpanInfos, final Object[] dstSpans, final int dstIdx) {
        copyKeyAndSpanMaybeSharing(0, src, srcIdx, dstSpanInfos, dstSpans, dstIdx, true);
    }

    protected void setContainerSpan(final int i, final long key, final Container c) {
        setContainerSpanRaw(i, key, c);
        modifiedSpan(i);
    }

    protected void setContainerSpan(final Container oldContainer, final int i, final long key,
            final Container newContainer) {
        if (oldContainer != newContainer || oldContainer instanceof ArrayContainer) {
            setContainerSpanRaw(i, key, newContainer);
        }
        modifiedSpan(i);
    }

    // Helper class to unpack span data from the parallel arrays.
    // The loaded span may be a full block span, a singleton, or a container.
    // In the container case, we may need to unpack an ArrayContainer from the stored short[].
    // We inherit from ArrayContainer to support that case while avoiding to create more objects.
    // This object is only intended to exist during a stack activation for a method
    // that needs to do some computations on spans; to avoid keeping any live references
    // to objects it is an AutoCloseable.
    protected static final class SpanView extends ArrayContainer
            implements AutoCloseable {
        private final SpanViewRecycler recycler;
        // The original array and index for which we loaded; we need to keep the reference
        // for the cases where we need to update it (eg, setting a copy on write shared flag for an ArrayContainer
        // stored as short[]).
        private RspArray<?> arr;
        private int arrIdx;
        private long spanInfo;
        private Object span;

        public SpanView(final SpanViewRecycler recycler) {
            super(null, 0, false);
            this.recycler = recycler;
        }

        // The returned container is guaranteed to be valid until the next init() or close().
        public Container getContainer() {
            if (span instanceof short[]) {
                shared = (spanInfo & SPANINFO_ARRAYCONTAINER_SHARED_BITMASK) != 0;
                cardinality = (int) (spanInfo & SPANINFO_ARRAYCONTAINER_CARDINALITY_BITMASK);
                content = (short[]) span;
                return this;
            }
            shared = false;
            cardinality = 0;
            content = null;
            return (Container) span;
        }

        public boolean isFullBlockSpan() {
            return RspArray.isFullBlockSpan(span);
        }

        public long getFullBlockSpanLen() {
            return RspArray.getFullBlockSpanLen(spanInfo, span);
        }

        public boolean isSingletonSpan() {
            return RspArray.isSingletonSpan(span);
        }

        public long getSingletonSpanValue() {
            return spanInfoToSingletonSpanValue(spanInfo);
        }

        public long getKey() {
            return spanInfoToKey(spanInfo);
        }

        public long getSpanInfo() {
            return spanInfo;
        }

        public void init(final RspArray<?> arr, final int arrIdx) {
            init(arr, arrIdx, arr.spanInfos[arrIdx], arr.spans[arrIdx]);
        }

        public void init(final RspArray<?> arr, final int arrIdx, final long spanInfo, final Object span) {
            this.arr = arr;
            this.arrIdx = arrIdx;
            this.spanInfo = spanInfo;
            this.span = span;
        }

        @Override
        protected void onCopyOnWrite() {
            arr.spanInfos[arrIdx] |= SPANINFO_ARRAYCONTAINER_SHARED_BITMASK;
        }

        public void reset() {
            content = null;
            arr = null;
            span = null;
        }

        @Override
        public void close() {
            reset();
            if (recycler != null) {
                recycler.returnSpanView(this);
            }
        }
    }

    protected static boolean isSingletonSpan(final Object o) {
        return o == null;
    }

    /**
     * Cache of accumulated cardinalities. Parallel array to keys and spans. acc[i] == total cardinality for { span[0],
     * span[1], ..., span[i] }. Should be updated by clients after mutating operations by calling
     * ensureCardinalityCache, so that public methods on entry can assume it is up to date, ie maxAccIdx == size - 1 is
     * a class invariant. Note this class own mutators do not update the cache themselves as clients can perform a
     * series of update operations and only call ensureCardinalityCache at the end.
     *
     * For a small number of keys, this is not created an kept null.
     */
    long[] acc;

    /**
     * If acc != null, highest index in acc that is valid, -1 if none. if acc == null: * if cardinality fits in an int,
     * the actual cardinality. * if cardinality does not fit in an int, -1.
     */
    int cardData;

    static final int INITIAL_CAPACITY = 1;

    RspArray() {
        spanInfos = new long[INITIAL_CAPACITY];
        spans = new Object[INITIAL_CAPACITY];
        acc = null;
        cardData = 0;
        size = 0;
    }

    void setCardDataFor(final long cardinality) {
        final int intCard = (int) cardinality;
        cardData = (cardinality != intCard) ? -1 : intCard;
    }

    RspArray(final long start, final long end) {
        final long sHigh = highBits(start);
        final long eHigh = highBits(end);
        final int sLow = lowBitsAsInt(start);
        final int eLow = lowBitsAsInt(end);
        final long cardinality = end - start + 1;
        if (sHigh == eHigh) {
            acc = null;
            setCardDataFor(cardinality);
            size = 1;
            spanInfos = new long[1];
            spans = new Object[1];
            if (cardinality == 1) {
                setSingletonSpan(0, start);
            } else {
                if (cardinality == BLOCK_SIZE) {
                    setFullBlockSpanRaw(0, sHigh, 1L);
                } else {
                    setContainerSpanRaw(0, sHigh, Container.rangeOfOnes(sLow, eLow + 1));
                }
            }
            return;
        }
        if (sLow == 0) {
            if (eLow == BLOCK_LAST) {
                spanInfos = new long[1];
                spans = new Object[1];
                setFullBlockSpanRaw(0, sHigh, distanceInBlocks(sHigh, eHigh) + 1);
                acc = null;
                setCardDataFor(cardinality);
                size = 1;
                return;
            }
            spanInfos = new long[2];
            spans = new Object[2];
            setFullBlockSpanRaw(0, sHigh, distanceInBlocks(sHigh, eHigh));
            if (eLow == 0) {
                setSingletonSpan(1, end);
            } else {
                setContainerSpanRaw(1, eHigh, Container.rangeOfOnes(0, eLow + 1));
            }
            acc = null;
            setCardDataFor(cardinality);
            size = 2;
            return;
        }
        if (eLow == BLOCK_LAST) {
            spanInfos = new long[2];
            spans = new Object[2];
            if (sLow == BLOCK_LAST) {
                setSingletonSpan(0, start);
            } else {
                setContainerSpanRaw(0, sHigh, Container.rangeOfOnes(sLow, BLOCK_SIZE));
            }
            setFullBlockSpanRaw(1, nextKey(sHigh), distanceInBlocks(sHigh, eHigh));
            acc = null;
            setCardDataFor(cardinality);
            size = 2;
            return;
        }
        final long nextKey = nextKey(sHigh);
        final long midflen = distanceInBlocks(nextKey, eHigh);
        if (midflen == 0) {
            spanInfos = new long[2];
            spans = new Object[2];
            if (sLow == BLOCK_LAST) {
                setSingletonSpan(0, start);
            } else {
                setContainerSpanRaw(0, sHigh, Container.rangeOfOnes(sLow, BLOCK_SIZE));
            }
            if (eLow == 0) {
                setSingletonSpan(1, end);
            } else {
                setContainerSpanRaw(1, eHigh, Container.rangeOfOnes(0, eLow + 1));
            }
            acc = null;
            setCardDataFor(cardinality);
            size = 2;
            return;
        }
        spanInfos = new long[3];
        spans = new Object[3];
        if (sLow == BLOCK_LAST) {
            setSingletonSpan(0, start);
        } else {
            setContainerSpanRaw(0, sHigh, Container.rangeOfOnes(sLow, BLOCK_SIZE));
        }
        setFullBlockSpanRaw(1, nextKey, midflen);
        if (eLow == 0) {
            setSingletonSpan(2, end);
        } else {
            setContainerSpanRaw(2, eHigh, Container.rangeOfOnes(0, eLow + 1));
        }
        acc = null;
        setCardDataFor(cardinality);
        size = 3;
    }

    /**
     * Used by one RspArray instance to ask another instance to share one of its containers.
     *
     * @param c The container requested
     * @return The container, possibly adjusted to make it shareable
     */
    Container shareContainer(final Container c) {
        if (c != null && shareContainers()) {
            c.setCopyOnWrite();
        }
        return c;
    }

    protected RspArray(final RspArray other) {
        copySharingSpansFrom(other, 0);
    }

    // shiftAmount is a multiple of BLOCK_SIZE.
    private void copySharingSpansFrom(final RspArray other, final long shiftAmount) {
        final int newSize =
                (other.size >= 2 * INITIAL_CAPACITY && other.size < other.spanInfos.length / 2)
                        ? other.spanInfos.length / 2
                        : other.spanInfos.length;
        spanInfos = new long[newSize];
        spans = new Object[newSize];
        for (int i = 0; i < other.size; ++i) {
            copyKeyAndSpanMaybeSharing(shiftAmount, other, i, spanInfos, spans, i, true);
        }
        if (other.acc != null) {
            acc = new long[newSize];
            System.arraycopy(other.acc, 0, acc, 0, other.size);
        } else {
            acc = null;
        }
        cardData = other.cardData;
        size = other.size;
    }

    private void maybeSetAcc(final int i, final long accumCard) {
        if (acc == null) {
            return;
        }
        acc[i] = accumCard;
    }

    public RspArray(
            final RspArray src,
            final int startIdx, final long startOffset,
            final int endIdx, final long endOffset) {
        // an initial full block span that needs to be split may result in a sequence of spans as follows,
        // any of which may or may not be present:
        // (a) an initial container.
        // (b) an intermediate full block span.
        // (c) an ending container (this will only exist if endIdx == startIdx).
        // the following variables represent the computed cardinality (or full span len) of each, zero if not present.
        int startSplitInitialContainerCard = 0;
        long startSplitIntermediateFullBlockSpanLen = 0;
        long startSplitIntermediateFullBlockSpanCard = 0;
        int startSplitEndingContainerCard = 0;
        final Object firstSpan = src.spans[startIdx];
        // If either of the spans at startIndex or endIndex are a full block span of more than one block
        // that needs to be broken into an RB container and the remaining full block span,
        // that would affect our resulting size.
        int sz = endIdx - startIdx + 1;
        final long firstSpanInfo = src.spanInfos[startIdx];
        final long startIdxKey = spanInfoToKey(firstSpanInfo);
        final long flenFirstSpan = getFullBlockSpanLen(firstSpanInfo, firstSpan);
        final boolean firstSpanIsFull;
        final long keyForFirstBlock;
        if (flenFirstSpan == 0) {
            firstSpanIsFull = false;
            keyForFirstBlock = startIdxKey;
        } else {
            firstSpanIsFull = true;
            final long startKey = startIdxKey + startOffset;
            keyForFirstBlock = highBits(startKey);
            final long resultingCardFromFirstSpan;
            if (endIdx <= startIdx) {
                resultingCardFromFirstSpan = endOffset - startOffset + 1;
                final long singleIdxEndKey = highBits(startIdxKey + endOffset);
                if (singleIdxEndKey == keyForFirstBlock) {
                    size = 1;
                    spanInfos = new long[1];
                    spans = new Object[1];
                    if (resultingCardFromFirstSpan == BLOCK_SIZE) {
                        setFullBlockSpanRaw(0, keyForFirstBlock, 1L);
                    } else {
                        final int containerStart = (int) (startOffset & BLOCK_LAST);
                        final int containerEndInclusive = (int) (endOffset & BLOCK_LAST);
                        if (containerStart == containerEndInclusive) {
                            setSingletonSpanRaw(0, startKey);
                        } else {
                            final Container c = Container.rangeOfOnes(containerStart, containerEndInclusive + 1);
                            setContainerSpanRaw(0, keyForFirstBlock, c);
                        }
                    }
                    acc = null;
                    setCardDataFor(resultingCardFromFirstSpan);
                    ifDebugValidate();
                    return;
                }
            } else {
                resultingCardFromFirstSpan = flenFirstSpan * BLOCK_SIZE - startOffset;
            }
            int n = 0; // how many containers we end up with after (potentially) splitting the first.
            final long startOffsetModBlockSize = RspArray.modBlockSize(startOffset);
            if (startOffsetModBlockSize == 0) {
                startSplitInitialContainerCard = 0;
            } else {
                startSplitInitialContainerCard = (int) (BLOCK_SIZE - startOffsetModBlockSize);
                ++n;
            }
            final long remainingCard = resultingCardFromFirstSpan - startSplitInitialContainerCard;
            startSplitIntermediateFullBlockSpanLen = RspArray.divBlockSize(remainingCard);
            if (startSplitIntermediateFullBlockSpanLen > 0) {
                ++n;
            }
            startSplitEndingContainerCard = RspArray.modBlockSize(remainingCard);
            startSplitIntermediateFullBlockSpanCard = remainingCard - startSplitEndingContainerCard;
            if (startSplitEndingContainerCard > 0) {
                ++n;
            }
            sz += n - 1;
        }
        boolean lastSpanIsFull = false; // will set below to true if we find out otherwise.
        long deltaLast = 0; // cardinality of the span(s) resulting from the split of a last full block span.
        int copyLastIdx = endIdx;
        if (endIdx > startIdx && endOffset < src.getSpanCardinalityAtIndexMaybeAcc(endIdx) - 1) {
            copyLastIdx = endIdx - 1;
            final Object lastSpan = src.spans[endIdx];
            final long lastSpanInfo = src.spanInfos[endIdx];
            final long flenLastSpan = getFullBlockSpanLen(lastSpanInfo, lastSpan);
            if (flenLastSpan > 0) {
                lastSpanIsFull = true;
                deltaLast = endOffset + 1;
                if (deltaLast > BLOCK_SIZE) {
                    ++sz;
                }
            }
        }
        size = sz;
        spanInfos = new long[size];
        spans = new Object[size];
        if (size > accNullThreshold) {
            acc = new long[size];
            cardData = size - 1;
        } else {
            acc = null;
        }
        int i = 0;
        long accSum = 0;
        int isrc; // index in src from where to start copying spans.
        final WorkDataHolder wd = new WorkDataHolder();
        if (firstSpanIsFull) {
            long nextKey = keyForFirstBlock;
            if (startSplitInitialContainerCard > 0) {
                if (startSplitInitialContainerCard == 1) {
                    setSingletonSpanRaw(0, nextKey | BLOCK_LAST);
                } else {
                    final Container c = Container.rangeOfOnes(BLOCK_SIZE - startSplitInitialContainerCard, BLOCK_SIZE);
                    setContainerSpanRaw(0, nextKey, c);
                }
                nextKey = nextKey(nextKey);
                accSum = startSplitInitialContainerCard;
                maybeSetAcc(0, accSum);
                i = 1;
            }
            if (startSplitIntermediateFullBlockSpanLen > 0) {
                setFullBlockSpanRaw(i, nextKey, startSplitIntermediateFullBlockSpanLen);
                nextKey = highBits(nextKey + startSplitIntermediateFullBlockSpanCard);
                accSum += startSplitIntermediateFullBlockSpanCard;
                maybeSetAcc(i, accSum);
                ++i;
            }
            if (startSplitEndingContainerCard > 0) {
                if (startSplitEndingContainerCard == 1) {
                    setSingletonSpanRaw(i, nextKey);
                } else {
                    final Container c = Container.rangeOfOnes(0, startSplitEndingContainerCard);
                    setContainerSpanRaw(i, nextKey, c);
                }
                accSum += startSplitEndingContainerCard;
                if (acc != null) {
                    acc[i] = accSum;
                } else {
                    setCardDataFor(accSum);
                }
                ifDebugValidate();
                return;
            }
            isrc = startIdx + 1;
        } else {
            if (startOffset != 0 || (endIdx <= startIdx && endOffset < BLOCK_LAST)) {
                final Object spanSrc = src.spans[startIdx];
                if (isSingletonSpan(spanSrc)) {
                    if (startOffset != 0) {
                        throw new IllegalArgumentException(
                                "startOffset=" + startOffset + " and span at startIdx has a single element.");
                    }
                    setSingletonSpanRaw(0, src.getSingletonSpanValue(startIdx));
                    accSum = 1;
                    maybeSetAcc(0, 1);
                } else {
                    try (SpanView res = wd.get().borrowSpanView(src, startIdx)) {
                        final Container csrc = res.getContainer();
                        if (endIdx <= startIdx) { // single span.
                            final int card = (int) src.getSpanCardinalityAtIndexMaybeAcc(startIdx);
                            if (endOffset + 1 < card) {
                                if (startOffset == endOffset) {
                                    setSingletonSpanRaw(0,
                                            startIdxKey | unsignedShortToInt(csrc.select((int) startOffset)));
                                } else {
                                    final Container c = csrc.select((int) startOffset, (int) (endOffset + 1));
                                    setContainerSpanRaw(0, startIdxKey, c);
                                }
                                accSum = endOffset - startOffset + 1;
                                if (acc != null) {
                                    acc[0] = accSum;
                                } else {
                                    setCardDataFor(accSum);
                                }
                                ifDebugValidate();
                                return;
                            }
                        }
                        final int card = (int) src.getSpanCardinalityAtIndexMaybeAcc(startIdx);
                        final int startOffsetInt = (int) startOffset;
                        if (startOffsetInt + 1 == card) {
                            setSingletonSpanRaw(0, startIdxKey | unsignedShortToInt(csrc.select(startOffsetInt)));
                        } else {
                            final Container c = csrc.select(startOffsetInt, card);
                            setContainerSpanRaw(0, startIdxKey, c);
                        }
                        accSum = card - startOffset;
                        maybeSetAcc(0, accSum);
                    }
                }
                i = 1;
                isrc = startIdx + 1;
            } else {
                i = 0;
                isrc = startIdx;
                accSum = 0;
            }
        }
        if (endIdx <= startIdx && i > 0) {
            if (acc == null) {
                setCardDataFor(accSum);
            }
            ifDebugValidate();
            return;
        }
        while (isrc <= copyLastIdx) {
            copyKeyAndSpanMaybeSharing(src, isrc, spanInfos, spans, i);
            accSum += src.getSpanCardinalityAtIndexMaybeAcc(isrc);
            maybeSetAcc(i, accSum);
            ++i;
            ++isrc;
        }
        if (isrc > endIdx) {
            if (acc == null) {
                setCardDataFor(accSum);
            }
            ifDebugValidate();
            return;
        }
        final long srcSpanInfo = src.spanInfos[isrc];
        final long srcKey = spanInfoToKey(srcSpanInfo);
        if (lastSpanIsFull) {
            if (deltaLast >= BLOCK_SIZE) {
                final long flen = RspArray.divBlockSize(deltaLast);
                final int delta = RspArray.modBlockSize(deltaLast);
                setFullBlockSpanRaw(i, srcKey, flen);
                accSum += flen * BLOCK_SIZE;
                maybeSetAcc(i, accSum);
                ++i;
                if (delta > 0) {
                    final long nextKey = srcKey + flen * BLOCK_SIZE;
                    if (delta == 1) {
                        setSingletonSpanRaw(i, nextKey);
                    } else {
                        final Container c = Container.rangeOfOnes(0, delta);
                        setContainerSpanRaw(i, nextKey, c);
                    }
                    accSum += delta;
                    maybeSetAcc(i, accSum);
                }
            } else {
                final int ce = (int) deltaLast;
                if (ce == 1) {
                    setSingletonSpanRaw(i, srcKey);
                } else {
                    setContainerSpanRaw(i, srcKey, Container.rangeOfOnes(0, ce));
                }
                accSum += deltaLast;
                maybeSetAcc(i, accSum);
            }
        } else {
            final long card = endOffset + 1;
            final Object srcSpan = src.spans[isrc];
            if (!isSingletonSpan(srcSpan)) {
                try (SpanView res = wd.get().borrowSpanView(src, isrc, srcSpanInfo, srcSpan)) {
                    final Container csrc = res.getContainer();
                    // This can't be the full container or we would have copied it earlier.
                    if (endOffset == 0) {
                        setSingletonSpanRaw(i, srcKey | csrc.first());
                    } else {
                        final Container c = csrc.select(0, (int) card);
                        setContainerSpanRaw(i, srcKey, c);
                    }
                    accSum += card;
                    maybeSetAcc(i, accSum);
                }
            } else {
                // Can't happen; a single element span should have been copied over in its entirety earlier.
                throw new IllegalStateException("endIdx=" + endIdx + ", endOffset=" + endOffset + ", key=" + srcKey);
            }
        }
        if (acc == null) {
            setCardDataFor(accSum);
        }
        ifDebugValidate();
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public long firstValue() {
        return firstValueAtIndex(0);
    }

    public long firstValueAtIndex(final int i) {
        try (SpanView view = workDataPerThread.get().borrowSpanView(this, i)) {
            if (view.isSingletonSpan()) {
                return view.getSingletonSpanValue();
            }
            final long k = view.getKey();
            if (view.isFullBlockSpan()) {
                return k;
            }
            return k | (long) view.getContainer().first();
        }
    }

    public long keyForFirstBlock() {
        return getKey(0);
    }

    public long keyForLastBlock() {
        final Object span = spans[size - 1];
        final long spanInfo = spanInfos[size - 1];
        final long key = spanInfoToKey(spanInfo);
        final long flen = getFullBlockSpanLen(spanInfo, span);
        if (flen > 0) {
            return key + (flen - 1) * BLOCK_SIZE;
        }
        return key;
    }

    public long lastValue() {
        try (SpanView view = workDataPerThread.get().borrowSpanView(this, size - 1)) {
            if (view.isSingletonSpan()) {
                return view.getSingletonSpanValue();
            }
            final long flen = view.getFullBlockSpanLen();
            final long key = view.getKey();
            if (flen > 0) {
                return key + BLOCK_SIZE * flen - 1;
            }
            return key | (long) view.getContainer().last();
        }
    }

    public interface SpanCursor {
        /**
         * @return the current span's first key if the current span is valid, undefined otherwise.
         */
        default long spanKey() {
            return spanInfoToKey(spanInfo());
        }

        /**
         * @return the current span's span info if the current span is valid, undefined otherwise.
         */
        long spanInfo();

        /**
         * @return the current span if valid, undefined otherwise.
         */
        Object span();

        /**
         * Advances the pointer to the next span in the linear sequence. If the span before the call was the last one, a
         * subsequent call to hasNext will return false.
         */
        void next();

        /**
         * This method should be called: * After the pointer is created and before calling any other methods; if it
         * returns false, calling any other methods results in undefined behavior. * Right after a call to any advance
         * method, similar to above.
         * 
         * @return true if the pointer currently points to a valid span.
         */
        boolean hasNext();

        /**
         * Advances the pointer forward to the next span in the sequence whose interval could have it include the key
         * argument.
         *
         * More specifically, the current span position is effectively advanced forward as long as the provided key is
         * bigger than the right endpoint for the current span.
         *
         * This operation is O(log(cardinality)).
         *
         * Note this may not move the pointer if the current span already satisfies the constraint, or it may invalidate
         * the pointer if the key is to the right of the last valid span. Note also advance should only be called on a
         * non-empty cursor, after having called hasNext() and next() at least once.
         *
         * @param key key to search forward from the current span position.
         * @return false if the cursor is exhausted and there was no span satisfying the restriction found, true
         *         otherwise.
         */
        boolean advance(long key);

        /**
         * Releases the RspArray reference.
         */
        void release();

        RspArray arr();

        int arrIdx();
    }

    public interface SpanCursorForward extends SpanCursor {
        /**
         * Move the current position one step backwards.
         */
        void prev();

        /**
         * Advances the pointer forward to the last span in the sequence whose interval range has a value v such that
         * comp.directionToTargetFrom(v) >= 0.
         *
         * This operation is O(log(cardinality)).
         *
         * This operation never invalidates a valid cursor, it may only move it forward from its current position but
         * never exhaust it.
         *
         * Note also search should only be called on a non-empty cursor, after having called hasNext() and next() at
         * least once.
         *
         * @param comp a Comparator used to search forward from the current span position.
         */
        void search(Comparator comp);

        /**
         * Saner clone.
         *
         * @returns a SpanCursorForward that is a copy of this one.
         */
        SpanCursorForward copy();
    }

    static class SpanCursorForwardImpl implements SpanCursorForward {
        private RspArray ra;
        private int si;

        public SpanCursorForwardImpl(final RspArray ra, final int si) {
            ra.acquire();
            this.ra = ra;
            this.si = si - 1;
        }

        public SpanCursorForwardImpl(final RspArray ra) {
            this(ra, 0);
        }

        @Override
        public SpanCursorForwardImpl copy() {
            return new SpanCursorForwardImpl(ra, si);
        }

        @Override
        public long spanInfo() {
            return ra.spanInfos[si];
        }

        @Override
        public Object span() {
            return ra.spans[si];
        }

        @Override
        public void prev() {
            --si;
        }

        @Override
        public boolean hasNext() {
            return si < ra.size - 1;
        }

        @Override
        public void next() {
            ++si;
        }

        private boolean advanceSecondHalf(int i) {
            if (i >= 0) {
                si = i;
                return true;
            }
            si = -i - 1;
            return si < ra.size;
        }

        @Override
        public boolean advance(final long key) {
            final int i = ra.getSpanIndex(si, highBits(key));
            return advanceSecondHalf(i);
        }

        @Override
        public void search(final Comparator comp) {
            si = ra.searchSpanIndex(si, comp);
        }

        @Override
        public void release() {
            if (ra == null) {
                return;
            }
            ra.release();
            ra = null;
        }

        @Override
        public RspArray arr() {
            return ra;
        }

        @Override
        public int arrIdx() {
            return si;
        }
    }

    public RspRangeIterator getRangeIterator() {
        return new RspRangeIterator(new SpanCursorForwardImpl(this));
    }

    public RspRangeBatchIterator getRangeBatchIterator(final long initialSeek, final long maxCount) {
        return new RspRangeBatchIterator(new SpanCursorForwardImpl(this), initialSeek, maxCount);
    }

    public RspIterator getIterator() {
        return new RspIterator(new SpanCursorForwardImpl(this));
    }

    private static class SpanCursorBackwardImpl implements SpanCursor {
        private RspArray ra;
        private int si;

        public SpanCursorBackwardImpl(final RspArray ra, int si) {
            ra.acquire();
            this.ra = ra;
            this.si = si;
        }

        public SpanCursorBackwardImpl(final RspArray ra) {
            this(ra, ra.size);
        }

        @Override
        public long spanInfo() {
            return ra.spanInfos[si];
        }

        @Override
        public Object span() {
            return ra.spans[si];
        }

        @Override
        public boolean hasNext() {
            return si > 0;
        }

        @Override
        public void next() {
            --si;
        }

        private boolean advanceSecondHalf(final int i) {
            if (i >= 0) {
                si = i;
                return true;
            }
            final int j = ~i - 1;
            if (j >= 0) {
                si = j;
                return true;
            }
            si = 0;
            return false;
        }

        @Override
        public boolean advance(final long key) {
            final int i = ra.getSpanIndex(0, si, highBits(key));
            return advanceSecondHalf(i);
        }

        @Override
        public void release() {
            if (ra == null) {
                return;
            }
            ra.release();
            ra = null;
        }

        @Override
        public RspArray arr() {
            return ra;
        }

        @Override
        public int arrIdx() {
            return si;
        }
    }

    public RspReverseIterator getReverseIterator() {
        return new RspReverseIterator(new SpanCursorBackwardImpl(this));
    }

    private int targetCapacityForSize(final int sz) {
        if (sz < 1024) {
            // smallest power of two greater or equal to sz.
            return 1 << (31 - Integer.numberOfLeadingZeros(sz - 1));
        }
        if ((sz & 3) == 0) {
            return sz;
        }
        return 5 * (sz / 4);
    }

    /**
     * Make sure there is capacity for at least n more spans
     */
    protected void ensureSizeCanGrowBy(final int n) {
        final int minCapacity = size + n;
        if (minCapacity <= spanInfos.length) {
            return;
        }
        int newCapacity = size == 0 ? 1 : size;
        while (newCapacity < minCapacity && newCapacity < doublingAllocThreshold) {
            newCapacity = 2 * newCapacity;
        }
        while (newCapacity < minCapacity) {
            final int rawStep = Math.max(linearAllocStep, newCapacity >> logarithmicAllocGrowthRate);
            newCapacity += (rawStep + 1023) & (~1023);
        }
        realloc(newCapacity);
    }

    // size <= newCapacity
    private void realloc(final int newCapacity) {
        final long[] newSpanInfos = new long[newCapacity];
        System.arraycopy(spanInfos, 0, newSpanInfos, 0, size);
        spanInfos = newSpanInfos;
        final Object[] newSpans = new Object[newCapacity];
        System.arraycopy(spans, 0, newSpans, 0, size);
        spans = newSpans;
        if (acc != null) {
            final long[] newAcc = new long[newCapacity];
            System.arraycopy(acc, 0, newAcc, 0, size);
            acc = newAcc;
        }
    }

    /**
     * @param compactFactor if k == 0, compact if count < capacity. k > 0, compact if (capacity - count > (capacity >>
     *        k).
     */
    public void tryCompactUnsafe(final int compactFactor) {
        if (compactFactor == 0) {
            if (size == spanInfos.length) {
                return;
            }
        } else if (spanInfos.length - size <= (spanInfos.length >> compactFactor)) {
            return;
        }
        realloc(size);
    }

    public void tryCompact(final int compactFactor) {
        if (!canWrite()) {
            return;
        }
        tryCompactUnsafe(compactFactor);
    }

    public int keySearch(final int startPos, final long key) {
        return keySearch(startPos, size, key);
    }

    public int keySearch(final int startPos, final int endPosExclusive, final long key) {
        final long blockKey = highBits(key);
        if (endPosExclusive == 0 || getKey(endPosExclusive - 1) == blockKey) {
            return endPosExclusive - 1;
        }
        return unsignedBinarySearch(this::getKey, startPos, endPosExclusive, blockKey);
    }

    public static boolean isFullBlockSpan(final Object s) {
        return (s == FULL_BLOCK_SPAN_MARKER) || (s instanceof Long);
    }

    public static boolean isContainer(final Object s) {
        return (s instanceof short[]) || (s instanceof Container);
    }

    @VisibleForTesting
    long getFullBlockSpanLenAt(final int i) {
        final long spanInfo = spanInfos[i];
        final Object span = spans[i];
        return getFullBlockSpanLen(spanInfo, span);
    }

    public static long getFullBlockSpanLen(final long spanInfo, final Object span) {
        if (span == FULL_BLOCK_SPAN_MARKER) {
            return spanInfo & BLOCK_LAST;
        }
        if (span instanceof Long) {
            return (Long) span;
        }
        return 0;
    }

    /**
     * @return if the key is included in some existing span, returns the index of that span. if the key is not included
     *         in any existing span, returns -(p - 1) where p is the position a span for the key would be inserted.
     *
     *         Note that, since a span's covered interval may include multiple blocks, a key contained by a span may be
     *         different than its first key (if the span includes more than one block).
     */
    public int getSpanIndex(final long key) {
        return getSpanIndex(0, key);
    }

    public int getSpanIndex(final int fromIndex, final long key) {
        return getSpanIndex(fromIndex, size, key);
    }

    public int getSpanIndex(final int fromIndex, final int endIndexExclusive, final long key) {
        final int i = keySearch(fromIndex, endIndexExclusive, key);
        if (i >= 0) {
            return i;
        }
        int preIdx = -i - 2;
        if (preIdx < fromIndex) {
            return i;
        }
        final Object preSpan = spans[preIdx];
        final long preSpanInfo = spanInfos[preIdx];
        final long flen = getFullBlockSpanLen(preSpanInfo, preSpan);
        if (flen == 0) {
            return i;
        }
        final long preKey = spanInfoToKey(preSpanInfo);
        if (distanceInBlocks(preKey, key) < flen) {
            return preIdx;
        }
        return i;
    }

    public int searchSpanIndex(final int startPos, final Comparator comp) {
        int i = binarySearchKeys(startPos, size, comp);
        if (i < 0) {
            i = -i - 2;
            if (i < startPos) {
                return startPos;
            }
        }
        return i;
    }

    public long getSpanCardinalityAtIndexMaybeAcc(final int i) {
        if (acc != null) {
            return getSpanCardinalityFromAccAtIndex(i);
        }
        return getSpanCardinalityAtIndex(i);
    }

    public long getSpanCardinalityAtIndex(final int i) {
        return getSpanCardinalityAtIndex(i, false);
    }

    public long getSpanCardinalityAtIndex(final int i, final boolean optimizeContainers) {
        final Object span = spans[i];
        final long spanInfo = spanInfos[i];
        final long flen = getFullBlockSpanLen(spanInfo, span);
        if (flen > 0) {
            return BLOCK_SIZE * flen;
        }
        if (isSingletonSpan(span)) {
            return 1;
        }
        if (span instanceof short[]) {
            return spanInfo & SPANINFO_ARRAYCONTAINER_CARDINALITY_BITMASK;
        }
        Container c = (Container) span;
        if (optimizeContainers) {
            spans[i] = c = c.runOptimize();
        }
        return c.getCardinality();
    }

    boolean isCardinalityCached() {
        if (acc != null) {
            return cardData == size - 1;
        }
        return cardData >= 0;
    }

    /**
     * Ensure cardinality cache is valid.
     *
     * @return total cardinality
     */
    void ensureCardinalityCache() {
        ensureCardinalityCache(false);
    }

    T forceAcc() {
        if (acc != null) {
            return self();
        }
        T ref = getWriteRef();
        ref.acc = new long[ref.spanInfos.length];
        ref.cardData = ref.size - 1;
        long card = 0;
        for (int i = 0; i < ref.size; ++i) {
            card += ref.getSpanCardinalityAtIndex(i);
            ref.acc[i] = card;
        }
        return ref;
    }

    void ensureCardinalityCache(final boolean optimizeContainers) {
        if (size == 0) {
            acc = null;
            cardData = 0;
            ifDebugValidate();
            return;
        }
        if (size <= accNullThreshold) {
            acc = null;
            long c = 0;
            for (int i = 0; i < size; ++i) {
                c += getSpanCardinalityAtIndex(i, optimizeContainers);
                if (c > Integer.MAX_VALUE) {
                    cardData = -1;
                    return;
                }
            }
            cardData = (int) c;
            return;
        }
        if (acc == null) {
            acc = new long[spanInfos.length];
            cardData = -1;
        }
        long cardinality = (cardData >= 0) ? acc[cardData] : 0;
        while (cardData < size - 1) {
            ++cardData;
            final long c = getSpanCardinalityAtIndex(cardData, optimizeContainers);
            cardinality += c;
            acc[cardData] = cardinality;
        }
        ifDebugValidate();
    }

    private static Container maybeOptimize(final Container c) {
        if (shouldOptimize(c)) {
            return c.runOptimize();
        }
        return c;
    }

    private static boolean shouldOptimize(final Container c) {
        final int bytesUsed = c.bytesUsed();
        final int bytesOverhead = c.bytesAllocated() - bytesUsed;
        if (bytesOverhead > bytesUsed) {
            return true;
        }
        if (!(c instanceof ArrayContainer) &&
                !(c instanceof BitmapContainer)) {
            return false;
        }
        final int card = c.getCardinality();
        if (card == 0) {
            throw new IllegalStateException("Zero cardinality container:" + c.toString());
        }
        final int range = c.last() - c.first() + 1;
        final int runsMinusOneUpperBound = Math.min(card - 1, range - card);
        // heuristic: an array of 16 runs (2 shorts each run)
        // fits in a 64 byte cache line.
        return runsMinusOneUpperBound < 16;
    }

    void ensureAccAndOptimize() {
        ensureCardinalityCache(true);
    }

    private long calculateCardinality() {
        long c = 0;
        for (int i = 0; i < size; ++i) {
            c += getSpanCardinalityAtIndex(i);
        }
        return c;
    }

    public long getCardinality() {
        if (acc != null) {
            return (size == 0) ? 0 : acc[size - 1];
        }
        if (cardData >= 0) {
            return cardData;
        }
        return calculateCardinality();
    }

    void modifiedSpan(final int i) {
        if (acc != null) {
            cardData = Math.min(i - 1, cardData);
            return;
        }
        cardData = -1;
    }

    void modifiedLastSpan() {
        if (acc != null) {
            cardData = Math.min(size - 2, cardData);
            return;
        }
        cardData = -1;
    }

    // For tests
    long getFullBlocksCount() {
        long tflen = 0;
        for (int i = 0; i < size; ++i) {
            final Object span = spans[i];
            final long spanInfo = spanInfos[i];
            tflen += getFullBlockSpanLen(spanInfo, span);
        }
        return tflen;
    }

    public static long nextKey(final long key) {
        return key + BLOCK_SIZE;
    }

    /**
     *
     * blockKeyEnd is exclusive. Assumption on entry: blockKeyStart <= blockKeyEnd
     *
     * @param blockKeyStart inclusive start block key (only high 48 bits set).
     * @param blockKeyEnd exclusive end block key (only high 48 bits set).
     * @return distance in blocks between blockKeyStart and blockKeyEnd
     */
    public static long distanceInBlocks(final long blockKeyStart, final long blockKeyEnd) {
        return (blockKeyEnd - blockKeyStart) >> 16;
    }

    private void arrayCopies(final int src, final int dst, final int n) {
        System.arraycopy(spanInfos, src, spanInfos, dst, n);
        System.arraycopy(spans, src, spans, dst, n);
    }

    private void compact() {
        if (size >= spans.length) {
            return;
        }
        realloc(size);
    }

    private void checkCompact() {
        final int thresholdSize;
        if (size < 2 * INITIAL_CAPACITY || size > (thresholdSize = spans.length / 2)) {
            return;
        }
        realloc(thresholdSize);
    }

    /**
     * Collapse an inner range of spans, by overwriting it with a range of spans from a certain later position till the
     * end, and reducing size accordingly to the number of spans removed ({@code size -= isrc - idst}). The resulting
     * array will remove all spans between the original values at {@code idst .. (isrc - 1)} inclusive.
     *
     * @param idst specifies the beginning position where the source range will move.
     * @param isrc specifies the source range to copy over as [isrc, size) (eg, from isrc inclusive till the end). If
     *        isrc == size no actual spans are copied, resulting in a size reduction only.
     */
    private void collapseRange(final int idst, final int isrc) {
        int newSize = size - (isrc - idst);
        int thresholdSize = 0;
        if (newSize > 2 * INITIAL_CAPACITY && newSize < (thresholdSize = spans.length / 2)) {
            final Object[] newSpans = new Object[thresholdSize];
            System.arraycopy(spans, 0, newSpans, 0, idst);
            System.arraycopy(spans, isrc, newSpans, idst, size - isrc);
            spans = newSpans;
            final long[] newSpanInfos = new long[thresholdSize];
            System.arraycopy(spanInfos, 0, newSpanInfos, 0, idst);
            System.arraycopy(spanInfos, isrc, newSpanInfos, idst, size - isrc);
            spanInfos = newSpanInfos;
            if (acc != null && thresholdSize > accNullThreshold) {
                final long[] newAcc = new long[thresholdSize];
                System.arraycopy(acc, 0, newAcc, 0, idst);
                // no point copying the rest, it was invalidated by this operation.
                acc = newAcc;
            } else {
                acc = null;
                cardData = -1;
            }
            size = newSize;
            return;
        }
        arrayCopies(isrc, idst, size - isrc);
        // ensure we don't leak elements.
        for (int j = newSize; j <= size - 1; ++j) {
            spans[j] = null;
        }
        size = newSize;
        checkCompact();
    }

    /**
     *
     * @param newSpanIdx an index, as returned by getSpanAtIndex(k). Note this can be negative, in which case this is an
     *        insertion (existing elements pushed to the right as necessary).
     * @param newSpanKey the key.
     * @param newSpanFlen the number of 2^16 intervals.
     *
     * @return the (positive) index where the span was actually inserted.
     */
    public int setOrInsertFullBlockSpanAtIndex(final int newSpanIdx, final long newSpanKey, final long newSpanFlen,
            final MutableObject<SortedRanges> madeNullSpansMu) {
        final int ii; // set or insert position.
        long newflen = newSpanFlen; // may grow if we merge to our right.
        final int idxForFirstKeyBigger; // first index for a key bigger than newSpanKey.
        if (newSpanIdx < 0) {
            ii = -(newSpanIdx + 1);
            if (ii == size) {
                appendFullBlockSpan(newSpanKey, newSpanFlen);
                return size;
            }
            idxForFirstKeyBigger = ii;
        } else {
            ii = newSpanIdx;
            idxForFirstKeyBigger = ii + 1;
        }
        int lastIdx = 0; // Last position that will be "overridden" by this new span, inclusive.
        if (idxForFirstKeyBigger >= size) {
            lastIdx = ii;
        } else {
            final long newSpanLastKey = newSpanKey + (newSpanFlen - 1) * BLOCK_SIZE; // New span's last key.
            final int j = getSpanIndex(idxForFirstKeyBigger, newSpanLastKey);
            final int idxForLastKeyInsideNewSpan;
            if (j >= 0) {
                idxForLastKeyInsideNewSpan = j;
            } else {
                // One before (-j-1), which is the first position whose key is > newSpanLastKey. Note this may be -1.
                idxForLastKeyInsideNewSpan = -j - 2;
            }
            // We may need to merge with a full block span extending to the right.
            boolean rightDone = false;
            final int idxForFirstKeyOutsideNewSpan = idxForLastKeyInsideNewSpan + 1;
            if (idxForFirstKeyOutsideNewSpan < size) {
                final long rightSpanInfo = spanInfos[idxForFirstKeyOutsideNewSpan];
                final long rightKey = spanInfoToKey(rightSpanInfo);
                if (rightKey - newSpanLastKey <= BLOCK_SIZE) {
                    final long rightLen = getFullBlockSpanLen(rightSpanInfo, spans[idxForFirstKeyOutsideNewSpan]);
                    if (rightLen > 0) {
                        final long rightSpanLastKey = getKeyForLastBlockInFullSpan(rightKey, rightLen);
                        if (rightSpanLastKey > newSpanLastKey) {
                            newflen += distanceInBlocks(newSpanLastKey, rightSpanLastKey);
                            rightDone = true;
                            lastIdx = idxForFirstKeyOutsideNewSpan;
                        }
                    }
                }
            }
            if (!rightDone) {
                if (idxForLastKeyInsideNewSpan >= 0) {
                    // we did not merge with a full block span to the right; we may need to absorb some len.
                    final long spanInfo = spanInfos[idxForLastKeyInsideNewSpan];
                    final long len = getFullBlockSpanLen(spanInfo, spans[idxForLastKeyInsideNewSpan]);
                    if (len > 0) {
                        final long spanKey = spanInfoToKey(spanInfo);
                        final long spanLastKey = getKeyForLastBlockInFullSpan(spanKey, len);
                        if (spanLastKey > newSpanLastKey) {
                            newflen += distanceInBlocks(newSpanLastKey, spanLastKey);
                        }
                    }
                }
                lastIdx = idxForLastKeyInsideNewSpan;
            }
        }
        long firstKey = newSpanKey;
        int firstIdx = ii;
        if (newSpanIdx >= 0) {
            // We may need to merge with a full block span at our position.
            final Object span = spans[ii];
            final long spanInfo = spanInfos[ii];
            final long spanLen = getFullBlockSpanLen(spanInfo, span);
            if (spanLen > 0) {
                final long key = spanInfoToKey(spanInfo);
                if (uLessOrEqual(key, firstKey)) {
                    final long d1 = distanceInBlocks(key, firstKey);
                    newflen = uMax(spanLen, newflen + d1);
                    firstKey = key;
                }
            }
        }
        if (ii > 0) {
            // We may need to merge with a full block span to our left.
            final int leftIdx = ii - 1;
            // Note getFullBlockSpanLen(null) == 0.
            final long leftSpanInfo = spanInfos[leftIdx];
            if (leftSpanInfo != -1) { // it may have been marked for deletion.
                final long leftSpanLen = getFullBlockSpanLen(leftSpanInfo, spans[leftIdx]);
                if (leftSpanLen > 0) {
                    final long leftKey = spanInfoToKey(leftSpanInfo);
                    final long keyDistance = distanceInBlocks(leftKey, newSpanKey);
                    if (leftSpanLen >= keyDistance) {
                        firstKey = leftKey;
                        firstIdx = leftIdx;
                        newflen += keyDistance;
                    }
                }
            }
        }
        if (firstIdx > lastIdx) {
            // We are inserting and did not merge as to avoid making space for the new span:
            // move keys over.
            ensureSizeCanGrowBy(1);
            arrayCopies(firstIdx, firstIdx + 1, size - firstIdx);
            ++size;
            lastIdx = firstIdx;
        }
        modifiedSpan(firstIdx);
        if (lastIdx == firstIdx) {
            setFullBlockSpanRaw(firstIdx, firstKey, newflen);
            return firstIdx;
        }
        if (madeNullSpansMu == null) {
            setFullBlockSpanRaw(firstIdx, firstKey, newflen);
            collapseRange(firstIdx + 1, lastIdx + 1);
            return firstIdx;
        }
        markIndexRangeAsRemoved(madeNullSpansMu, firstIdx, lastIdx - 1);
        setFullBlockSpanRaw(lastIdx, firstKey, newflen);
        return lastIdx;
    }

    private boolean tryMergeLeftFullBlockSpan(final int idx, final long k, final long slen) {
        if (idx < 1) {
            return false;
        }
        final int leftIdx = idx - 1;
        final long leftSpanInfo = spanInfos[leftIdx];
        final long leftSpanLen = getFullBlockSpanLen(leftSpanInfo, spans[leftIdx]);
        if (leftSpanLen > 0) {
            final long leftKey = spanInfoToKey(leftSpanInfo);
            final long keyDistance = distanceInBlocks(leftKey, k);
            if (leftSpanLen == keyDistance) { // by construction leftSpanLen <= keyDistance.
                setFullBlockSpan(leftIdx, leftKey, leftSpanLen + slen);
                return true;
            }
        }
        return false;
    }

    public void setLastFullBlockSpan(final long k, final long slen) {
        // we may need to merge with a full block span to our left.
        if (tryMergeLeftFullBlockSpan(size - 1, k, slen)) {
            spans[size - 1] = null;
            --size;
            checkCompact();
            return;
        }
        setFullBlockSpan(size - 1, k, slen);
    }

    private void tryOptimizeContainer(final int i) {
        final Object o = spans[i];
        if (o instanceof short[]) {
            final short[] contents = (short[]) o;
            if (contents.length < 3 || contents.length > 12) {
                final long spanInfo = spanInfos[i];
                try (SpanView res = workDataPerThread.get().borrowSpanView(this, i, spanInfo, contents)) {
                    final Container c = res.getContainer();
                    final Container prevContainer = c.runOptimize();
                    if (prevContainer != c) {
                        setContainerSpanRaw(i, spanInfoToKey(spanInfo), prevContainer);
                    }
                    return;
                }
            }
        }
        if (!(o instanceof Container)) {
            return;
        }
        final Container prevContainer = (Container) o;
        spans[i] = prevContainer.runOptimize();
    }

    public void appendSingletonSpan(final long v) {
        if (size > 0) {
            tryOptimizeContainer(size - 1);
        }
        ensureSizeCanGrowBy(1);
        setSingletonSpanRaw(size, v);
        ++size;
    }

    public void appendContainer(final long k, final Container c) {
        if (size > 0) {
            tryOptimizeContainer(size - 1);
        }
        ensureSizeCanGrowBy(1);
        setContainerSpanRaw(size, k, c);
        ++size;
    }

    public void appendFullBlockSpan(final long k, final long slen) {
        // we may need to merge with a full block span to our left.
        if (tryMergeLeftFullBlockSpan(size, k, slen)) {
            return;
        }
        if (size > 0) {
            tryOptimizeContainer(size - 1);
        }
        ensureSizeCanGrowBy(1);
        setFullBlockSpanRaw(size, k, slen);
        if (isCardinalityCached()) {
            // since is easy enough...
            long deltaCard = slen * BLOCK_SIZE;
            if (acc != null) {
                final long c = (size == 0) ? 0 : acc[size - 1];
                acc[size] = c + deltaCard;
            } else {
                setCardDataFor(cardData + deltaCard);
            }
        }
        ++size;
    }

    private void open(final int i) {
        ensureSizeCanGrowBy(1);
        final int dstPos = i + 1;
        final int n = size - i;
        arrayCopies(i, dstPos, n);
        ++size;
    }

    /**
     * Insert a full block span at position i with key k, pushing the existing elements to the right. The caller should
     * ensure that the key order is preserved by this operation.
     *
     * @param i position in which to insert
     * @param key key for the span to be inserted
     * @param flen the full block len for the span inserted.
     */
    public void insertFullBlockSpanAtIndex(final int i, final long key, final long flen) {
        open(i);
        setFullBlockSpan(i, key, flen);
    }

    /**
     * Insert a new singleton span at position i with key k, pushing the existing elements to the right. The caller
     * should ensure that the key order is preserved by this operation.
     *
     * @param i position in which to insert
     * @param value the singleton value for the span to be inserted
     */
    public void insertSingletonAtIndex(final int i, final long value) {
        open(i);
        setSingletonSpan(i, value);
    }

    /**
     * Insert a container at position i with key k, pushing the existing elements to the right. The caller should ensure
     * that the key order is preserved by this operation.
     *
     * @param i position in which to insert
     * @param key key for the span to be inserted
     * @param c the container to be inserted
     */
    public void insertContainerAtIndex(final int i, final long key, final Container c) {
        open(i);
        setContainerSpan(i, key, c);
    }

    public void removeSpanAtIndex(final int i) {
        collapseRange(i, i + 1);
        modifiedSpan(i);
    }

    /**
     * Replace the span at index i with the keys and spans from buf,
     */
    public void replaceSpanAtIndex(final int i, final ArraysBuf buf) {
        ensureSizeCanGrowBy(buf.size - 1);
        final int dstPos = i + buf.size;
        final int srcPos = i + 1;
        final int count = size - srcPos;
        arrayCopies(srcPos, dstPos, count);
        for (int j = 0; j < buf.size; ++j) {
            spanInfos[i + j] = buf.spanInfos[j];
            spans[i + j] = buf.spans[j];
        }
        size += buf.size - 1;
        modifiedSpan(i);
    }

    // Modeled after the version in RB Util class, which in turn is modeled on the Arrays.binarySearch API.
    // Like in them, toIndex is exclusive.
    static int unsignedBinarySearch(final IntToLongFunction fun, final int fromIndex, final int toIndex, final long k) {
        // next line accelerates the possibly common case where the value would
        // be inserted at the end
        if (toIndex > 0 && Long.compareUnsigned(fun.applyAsLong(toIndex - 1), k) < 0) {
            return -toIndex - 1;
        }
        int low = fromIndex;
        int high = toIndex - 1;
        // 8 in the next line matches the size of a cache line.
        while (low + 8 <= high) {
            final int middleIndex = (low + high) >>> 1;
            final long middleValue = fun.applyAsLong(middleIndex);

            int comp = Long.compareUnsigned(middleValue, k);
            if (comp < 0) {
                low = middleIndex + 1;
            } else if (comp > 0) {
                high = middleIndex - 1;
            } else {
                return middleIndex;
            }
        }
        // we finish the job with a sequential search
        int x = low;
        for (; x <= high; ++x) {
            final long val = fun.applyAsLong(x);
            if (Long.compareUnsigned(val, k) >= 0) {
                if (val == k) {
                    return x;
                }
                break;
            }
        }
        return -(x + 1);
    }

    public int binarySearchKeys(final int fromIndex, final int toIndex, final Comparator comp) {
        int low = fromIndex;
        int high = toIndex - 1;
        while (low <= high) {
            final int middleIndex = (low + high) / 2;
            final long middleValue = firstValueAtIndex(middleIndex);
            final int c = comp.directionToTargetFrom(middleValue);
            if (c < 0) {
                high = middleIndex - 1;
            } else if (c > 0) {
                low = middleIndex + 1;
            } else {
                return middleIndex;
            }
        }
        return -(low + 1);
    }

    public static long unsignedShortToLong(final short x) {
        return x & (long) BLOCK_LAST;
    }

    public static int unsignedShortToInt(final short x) {
        return x & BLOCK_LAST;
    }

    public static long paste(final long highBits, final short lowBits) {
        return highBits | unsignedShortToLong(lowBits);
    }

    // assumed on entry: cardinalityBefore(fromIndex) <= pos < getCardinality().
    // acc != null
    private int getIndexForRankWithAcc(final int fromIndex, final long pos) {
        final long posp1 = pos + 1;
        final int i = unsignedBinarySearch(j -> acc[j], fromIndex, size, posp1);
        return (i < 0) ? -i - 1 : i;
    }

    private int getIndexForRankNoAcc(final int fromIndex, final long pos, final MutableLong prevCardMu) {
        int i = fromIndex;
        final long posp1 = pos + 1;
        long card = (prevCardMu == null) ? 0 : prevCardMu.longValue();
        long prevCard;
        while (true) {
            prevCard = card;
            card += getSpanCardinalityAtIndex(i);
            if (posp1 <= card) {
                break;
            }
            ++i;
            if (i == size) {
                if (prevCardMu != null) {
                    prevCardMu.setValue(prevCard);
                }
                return size;
            }
        }
        if (prevCardMu != null) {
            prevCardMu.setValue(prevCard);
        }
        return i;
    }

    public long get(final long pos) {
        if (pos < 0) {
            return -1;
        }
        if (isCardinalityCached()) {
            final long cardinality = getCardinality();
            if (pos >= cardinality) {
                return -1;
            }
        }

        final int rankIndex;
        final long prevCard;
        // The only case where `acc != null && cardData != size - 1` is between client
        // calls for unsafe mutations, before a call to `finishMutations*()`.
        // In this case, as with `acc == null`, we use the else path, which allows
        // `get()` to be called when the cardinality cache is dirty.
        if (acc != null && cardData == size - 1) {
            rankIndex = getIndexForRankWithAcc(0, pos);
            prevCard = cardinalityBeforeWithAcc(rankIndex);
        } else {
            final MutableLong prevCardMu = new MutableLong(0);
            rankIndex = getIndexForRankNoAcc(0, pos, prevCardMu);
            if (rankIndex == size) {
                return -1;
            }
            prevCard = prevCardMu.longValue();
        }
        return get(rankIndex, pos - prevCard);
    }

    public void getKeysForPositions(final PrimitiveIterator.OfLong inputPositions, final LongConsumer outputKeys) {
        int fromIndex = 0;
        final long cardinality = isCardinalityCached() ? getCardinality() : -1;
        final MutableLong prevCardMu = (acc == null) ? new MutableLong(0) : null;
        while (inputPositions.hasNext()) {
            final long pos = inputPositions.nextLong();
            if (pos < 0 || (cardinality != -1 && pos >= cardinality)) {
                outputKeys.accept(-1);
                while (inputPositions.hasNext()) {
                    inputPositions.nextLong();
                    outputKeys.accept(-1);
                }
                return;
            }
            final long prevCardinality;
            if (acc != null) {
                fromIndex = getIndexForRankWithAcc(fromIndex, pos);
                prevCardinality = cardinalityBeforeWithAcc(fromIndex);
            } else {
                fromIndex = getIndexForRankNoAcc(fromIndex, pos, prevCardMu);
                if (fromIndex == size) {
                    outputKeys.accept(-1);
                    while (inputPositions.hasNext()) {
                        inputPositions.nextLong();
                        outputKeys.accept(-1);
                    }
                    return;
                }
                prevCardinality = prevCardMu.longValue();
            }
            final long key = get(fromIndex, pos - prevCardinality);
            outputKeys.accept(key);
        }
    }

    long get(final int idx, final long offset) {
        try (SpanView view = workDataPerThread.get().borrowSpanView(this, idx)) {
            if (view.isSingletonSpan()) {
                if (offset != 0) {
                    throw new IllegalArgumentException("Invalid offset=" + offset + " for index=" + idx);
                }
                return view.getSingletonSpanValue();
            }
            final long flen = view.getFullBlockSpanLen();
            final long highBits = view.getKey();
            if (flen > 0) {
                return highBits + offset;
            }
            // flen == 0
            final int sv = (int) offset;
            if (sv != offset) {
                throw new IllegalArgumentException("Invalid offset=" + offset + " for index=" + idx);
            }
            final short lowBits = view.getContainer().select(sv);
            return paste(highBits, lowBits);
        }
    }

    final long cardinalityBeforeWithAcc(final int i) {
        return (i > 0) ? acc[i - 1] : 0;
    }

    final long cardinalityBeforeMaybeAcc(final int idx) {
        return cardinalityBeforeMaybeAcc(idx, 0, 0);
    }

    final long cardinalityBeforeNoAcc(final int idx, final int knownIdx, final long knownBeforeCard) {
        int i = knownIdx;
        long card = knownBeforeCard;
        while (i < idx) {
            card += getSpanCardinalityAtIndex(i);
            ++i;
        }
        return card;
    }

    final long cardinalityBeforeMaybeAcc(final int idx, final int knownIdx, final long knownBeforeCard) {
        if (acc != null) {
            return cardinalityBeforeWithAcc(idx);
        }
        return cardinalityBeforeNoAcc(idx, knownIdx, knownBeforeCard);
    }

    final long cardinalityBeforeMaybeAcc(final int idx, final BeforeCardContext ctx) {
        if (acc != null) {
            return cardinalityBeforeWithAcc(idx);
        }
        final long card = cardinalityBeforeNoAcc(idx, ctx.knownIdx, ctx.knownBeforeCard);
        ctx.knownIdx = idx;
        ctx.knownBeforeCard = card;
        return card;
    }

    final long getSpanCardinalityFromAccAtIndex(final int i) {
        return acc[i] - cardinalityBeforeWithAcc(i);
    }

    @FunctionalInterface
    interface FindOutput {
        void setResult(int index, long offset);
    }

    // returns false if val is to the left of the first value in the span at startIdx;
    // otherwise calls setResult on out with the appropriate position for val or the first position after it.
    boolean findOrNext(final int startIdx, final int endIdxExclusive, final long val, final FindOutput out) {
        final int ki = getSpanIndex(startIdx, endIdxExclusive, highBits(val));
        if (ki < 0) {
            final int i = ~ki;
            if (i <= startIdx) {
                return false;
            }
            out.setResult(i, 0);
            return true;
        }
        try (SpanView view = workDataPerThread.get().borrowSpanView(this, ki)) {
            if (view.isSingletonSpan()) {
                final long singletonValue = view.getSingletonSpanValue();
                if (val < singletonValue) {
                    if (ki == startIdx) {
                        return false;
                    }
                    out.setResult(ki, 0);
                } else if (val == singletonValue) {
                    out.setResult(ki, 0);
                } else {
                    out.setResult(ki + 1, 0);
                }
                return true;
            }
            final long flen = view.getFullBlockSpanLen();
            if (flen > 0) {
                final long key = view.getKey();
                out.setResult(ki, val - key);
                return true;
            }
            final Container c = view.getContainer();
            final int cf = c.find(lowBits(val));
            if (cf >= 0) {
                out.setResult(ki, cf);
                return true;
            }
            int j = ~cf;
            final long spanCard = c.getCardinality();
            if (j == spanCard) {
                out.setResult(ki + 1, 0);
                return true;
            }
            if (j == 0) {
                if (ki == startIdx) {
                    return false;
                }
            }
            out.setResult(ki, j);
            return true;
        }
    }

    // returns false if val is to the left of the first value in the span at startIdx;
    // otherwise calls setResult on out with the appropriate position for val or the last position before it.
    boolean findOrPrev(final int startIdx, final int endIdxExclusive, final long val, final FindOutput out) {
        final int ki = getSpanIndex(startIdx, endIdxExclusive, highBits(val));
        if (ki < 0) {
            final int i = ~ki;
            if (i <= startIdx) {
                return false;
            }
            out.setResult(i - 1, getSpanCardinalityAtIndexMaybeAcc(i - 1) - 1);
            return true;
        }
        try (SpanView view = workDataPerThread.get().borrowSpanView(this, ki)) {
            if (view.isSingletonSpan()) {
                final long singletonValue = view.getSingletonSpanValue();
                if (val < singletonValue) {
                    if (ki == startIdx) {
                        return false;
                    }
                    out.setResult(ki - 1, getSpanCardinalityAtIndexMaybeAcc(ki - 1) - 1);
                } else {
                    out.setResult(ki, 0);
                }
                return true;
            }
            final long flen = view.getFullBlockSpanLen();
            if (flen > 0) {
                final long k = view.getKey();
                out.setResult(ki, val - k);
                return true;
            }
            final int cf = view.getContainer().find(lowBits(val));
            if (cf >= 0) {
                out.setResult(ki, cf);
                return true;
            }
            int j = ~cf;
            if (j == 0) {
                if (ki == startIdx) {
                    return false;
                }
                out.setResult(ki - 1, getSpanCardinalityAtIndexMaybeAcc(ki - 1) - 1);
                return true;
            }
            out.setResult(ki, j - 1);
            return true;
        }
    }

    static class BeforeCardContext {
        public int knownIdx;
        public long knownBeforeCard;

        public BeforeCardContext(final int knownIdx, final long knownBeforeCard) {
            this.knownIdx = knownIdx;
            this.knownBeforeCard = knownBeforeCard;
        }
    }

    long find(final int startIdx, final long val) {
        final int ki = getSpanIndex(startIdx, highBits(val));
        if (ki < 0) {
            final int i = -ki - 1;
            if (i == 0) {
                return -1;
            }
            final long prevAcc = cardinalityBeforeMaybeAcc(i);
            return -prevAcc - 1;
        }
        final long prevAcc = cardinalityBeforeMaybeAcc(ki);
        return findInSpan(ki, val, prevAcc);
    }

    long findInSpan(final int idx, final long val, final long prevAcc) {
        try (SpanView view = workDataPerThread.get().borrowSpanView(this, idx)) {
            if (view.isSingletonSpan()) {
                final long singletonValue = view.getSingletonSpanValue();
                if (val == singletonValue) {
                    return prevAcc;
                }
                if (val < singletonValue) {
                    return ~prevAcc;
                }
                // val > k
                return ~(prevAcc + 1);
            }
            final long flen = view.getFullBlockSpanLen();
            if (flen > 0) {
                final long k = view.getKey();
                return prevAcc + val - k;
            }
            final int cf = view.getContainer().find(lowBits(val));
            if (cf >= 0) {
                return prevAcc + cf;
            }
            return -prevAcc + cf;
        }
    }

    public long find(final long val) {
        ifDebugValidateNoAssert();
        return find(0, val);
    }

    public boolean subsetOf(final RspArray other) {
        if (size == 0) {
            return true;
        }
        // we are not empty.
        if (other.size == 0) {
            return false;
        }
        // other is not empty either.
        if (isCardinalityCached() && other.isCardinalityCached() &&
                getCardinality() > other.getCardinality()) {
            return false;
        }
        return subsetOf(this, other);
    }

    private static boolean subsetOf(final RspArray r1, final RspArray r2) {
        int p2 = 0;
        final WorkData wd = workDataPerThread.get();
        for (int i1 = 0; i1 < r1.size; ++i1) {
            try (SpanView view1 = wd.borrowSpanView(r1, i1)) {
                final long k1 = view1.getKey();
                final long flen1 = view1.getFullBlockSpanLen();
                final int i2 = r2.getSpanIndex(p2, k1);
                if (i2 < 0) {
                    return false;
                }
                try (SpanView view2 = wd.borrowSpanView(r2, i2)) {
                    final long flen2 = view2.getFullBlockSpanLen();
                    if (flen1 > 0) {
                        if (flen2 == 0) {
                            return false;
                        }
                        final long kend1 = getKeyForLastBlockInSpan(k1, flen1);
                        final long k2 = view2.getKey();
                        // Note getKeyForLastBlockInSpan works both for full block spans and rb containers
                        // (in that later case it just returns the single block key).
                        final long kend2 = getKeyForLastBlockInSpan(k2, flen2);
                        if (uLess(kend2, kend1)) {
                            return false;
                        }
                        if (kend2 == kend1) {
                            ++p2;
                            if (p2 >= r2.size) {
                                return i1 == r1.size - 1;
                            }
                        }
                        continue;
                    }
                    if (flen2 > 0) {
                        continue;
                    }
                    if (view1.isSingletonSpan()) {
                        final long v1 = view1.getSingletonSpanValue();
                        if (view2.isSingletonSpan()) {
                            final long v2 = view2.getSingletonSpanValue();
                            if (v1 != v2) {
                                return false;
                            }
                        } else {
                            if (!view2.getContainer().contains(lowBits(v1))) {
                                return false;
                            }
                        }
                    } else if (view2.isSingletonSpan()) {
                        return false;
                    } else {
                        final Container c1 = view1.getContainer();
                        final Container c2 = view2.getContainer();
                        if (!c1.subsetOf(c2)) {
                            return false;
                        }
                    }
                    ++p2;
                    if (p2 >= r2.size) {
                        return i1 == r1.size - 1;
                    }
                }
            }
        }
        return true;
    }

    public boolean containsRange(final long start, final long end) {
        final long sHigh = highBits(start);
        final int si = getSpanIndex(sHigh);
        if (si < 0) {
            return false;
        }
        final long eHigh = highBits(end);
        final int ei = getSpanIndex(si, eHigh);
        if (ei < 0) {
            return false;
        }
        long pendingStart = start;
        final long pendingEnd = end;
        final WorkData wd = workDataPerThread.get();
        for (int i = si; i <= ei; ++i) {
            try (SpanView view = wd.borrowSpanView(this, i)) {
                final long ki = view.getKey();
                if (ki > pendingStart) {
                    return false;
                }
                if (view.isSingletonSpan()) {
                    final long v1 = view.getSingletonSpanValue();
                    if (pendingStart != v1) {
                        return false;
                    }
                    if (pendingEnd == pendingStart) {
                        return true;
                    }
                    if (lowBitsAsInt(pendingStart) != BLOCK_LAST) {
                        return false;
                    }
                    ++pendingStart;
                    continue;
                }
                final long slen = view.getFullBlockSpanLen();
                if (slen > 0) {
                    final long spanLast = ki + slen * BLOCK_SIZE - 1;
                    if (spanLast >= pendingEnd) {
                        return true;
                    }
                    pendingStart = spanLast + 1;
                    continue;
                }
                final Container c = view.getContainer();
                final int cstart = (int) (pendingStart - ki);
                final long blockLast = ki + BLOCK_LAST;
                final int cend = (int) (Math.min(pendingEnd, blockLast) - ki);
                if (!c.contains(cstart, cend + 1)) {
                    return false;
                }
                if (blockLast >= pendingEnd) {
                    return true;
                }
                pendingStart = blockLast + 1;
            }
        }
        return true;
    }

    public boolean overlaps(final RspArray other) {
        if (size == 0 || other.size == 0) {
            return false;
        }
        return (size < other.size) ? overlaps(this, other) : overlaps(other, this);
    }

    // Works for both full block spans and containers.
    private static long getKeyForLastBlockInSpan(final long spanStartKey, final long flen) {
        final long additionalBlocksAfterFirst = (flen > 1) ? flen - 1 : 0;
        return spanStartKey + additionalBlocksAfterFirst * BLOCK_SIZE;
    }

    private static long getKeyForLastBlockInFullSpan(final long spanKey, final long flen) {
        return spanKey + (flen - 1) * BLOCK_SIZE;
    }

    /**
     * Returns true if any value in this RspArray is contained inside the range [first, last], false otherwise.
     *
     * @param first First value in the range to check
     * @param last Last value in the range to check
     * @return
     */
    public boolean overlapsRange(final long first, final long last) {
        return overlapsRange(0, first, last) >= 0;
    }

    public int overlapsRange(final int iStart, final long start, final long end) {
        final long startHighBits = highBits(start);
        int i = getSpanIndex(iStart, startHighBits);
        if (i < 0) {
            i = ~i;
            if (i >= size) {
                return ~i;
            }
        }
        final long endHighBits = highBits(end);
        long keyBlock = getKey(i);
        if (endHighBits < keyBlock) {
            return ~i;
        }
        final WorkData wd = workDataPerThread.get();
        while (true) {
            final long sk = Math.max(start, keyBlock);
            final long ek = Math.min(end, keyBlock + BLOCK_LAST);
            if (sk == keyBlock && ek == keyBlock + BLOCK_LAST) {
                return i;
            }
            try (SpanView view = wd.borrowSpanView(this, i)) {
                if (view.isSingletonSpan()) {
                    final long v = view.getSingletonSpanValue();
                    if (start <= v && v <= end) {
                        return i;
                    }
                } else {
                    final long slen = view.getFullBlockSpanLen();
                    if (slen > 0) {
                        return i;
                    }
                    final Container c = view.getContainer();
                    final int cstart = lowBitsAsInt(sk);
                    final int cend = lowBitsAsInt(ek) + 1;
                    if (c.overlapsRange(cstart, cend)) {
                        return i;
                    }
                }
                ++i;
                if (i >= size) {
                    return ~i;
                }
                keyBlock = getKey(i);
                if (endHighBits < keyBlock) {
                    return ~(i - 1);
                }
            }
        }
    }

    private static boolean overlaps(final RspArray r1, final RspArray r2) {
        if (r1.keyForLastBlock() < r2.keyForFirstBlock()
                || r2.keyForLastBlock() < r1.keyForFirstBlock()) {
            return false;
        }
        int p2 = 0;
        final WorkData wd = workDataPerThread.get();
        for (int i1 = 0; i1 < r1.size; ++i1) {
            try (SpanView view1 = wd.borrowSpanView(r1, i1)) {
                final long k1 = view1.getKey();
                final long flen1 = view1.getFullBlockSpanLen();
                final int i2 = r2.getSpanIndex(p2, k1);
                if (i2 >= 0) {
                    if (flen1 > 0) {
                        return true;
                    }
                    try (SpanView view2 = wd.borrowSpanView(r2, i2)) {
                        final long flen2 = view2.getFullBlockSpanLen();
                        if (flen2 > 0) {
                            return true;
                        }
                        if (view1.isSingletonSpan()) {
                            final long v1 = view1.getSingletonSpanValue();
                            if (view2.isSingletonSpan()) {
                                final long v2 = view2.getSingletonSpanValue();
                                if (v1 == v2) {
                                    return true;
                                }
                            } else {
                                final Container c2 = view2.getContainer();
                                if (c2.contains(lowBits(v1))) {
                                    return true;
                                }
                            }
                        } else if (view2.isSingletonSpan()) {
                            final long v2 = view2.getSingletonSpanValue();
                            final Container c1 = view1.getContainer();
                            if (c1.contains(lowBits(v2))) {
                                return true;
                            }
                        } else {
                            final Container c1 = view1.getContainer();
                            final Container c2 = view2.getContainer();
                            if (c1.overlaps(c2)) {
                                return true;
                            }
                        }
                        ++p2;
                        if (p2 >= r2.size) {
                            return false;
                        }
                        continue;
                    }
                }
                // i2 < 0
                if (flen1 > 0) {
                    final long k1Blocklast = k1 + (flen1 - 1) * BLOCK_SIZE;
                    int j2 = r2.getSpanIndex(p2, k1Blocklast);
                    if (j2 >= 0) {
                        return true;
                    }
                    // Both i2 and j2, the indices to the first and last key in s1, are negative,
                    // so those exact keys are not in r2. However, if they are different,
                    // it means there is something in between them.
                    if (i2 != j2) {
                        return true;
                    }
                    p2 = -i2 - 1;
                    if (p2 >= r2.size) {
                        return false;
                    }
                    continue;
                }
                // s1 is a Container, and its block key is not an exact match in r2.
                // Only possibility would be for s2 to be a full block span
                // containing s1, but this can't be since i2 < 0.
                p2 = -i2 - 1;
                if (p2 >= r2.size) {
                    return false;
                }
            }
        }
        return false;
    }

    /**
     * OrEquals a single span into this container.
     *
     * @param shiftAmount an amount to shift the keys in the other container; shiftAmount should be a multiple of
     *        BLOCK_SIZE.
     * @param other the other RspArray to ask for container sharing
     * @param otherIdx the index into other for the span to apply or to.
     * @param startPos the first position to start looking for orKey in this container.
     * @return the index in this container to continue searches for keys after (orKey, orSpan).
     */
    private int orEqualsSpan(final long shiftAmount, final RspArray other, final int otherIdx,
            final int startPos, final MutableObject<SortedRanges> sortedRangesMu,
            final WorkData wd) {
        final Object otherSpan = other.spans[otherIdx];
        final long otherSpanInfo = other.getSpanInfo(otherIdx) + shiftAmount;
        try (SpanView otherView = wd.borrowSpanView(other, otherIdx, otherSpanInfo, otherSpan)) {
            final long otherKey = otherView.getKey();
            final long otherflen = otherView.getFullBlockSpanLen();
            final int orIdx = getSpanIndex(startPos, otherKey);
            if (otherflen > 0) {
                final int j = setOrInsertFullBlockSpanAtIndex(orIdx, otherKey, otherflen, sortedRangesMu);
                // can't increment for return since it may have been absorbed by a longer full block span.
                return j;
            }
            if (orIdx < 0) {
                final int i = -orIdx - 1;
                if (i >= size) {
                    if (otherView.isSingletonSpan()) {
                        appendSingletonSpan(otherView.getSingletonSpanValue());
                    } else {
                        final Container otherContainer = otherView.getContainer();
                        appendSharedContainer(other, otherKey, otherContainer);
                    }
                    return size;
                }
                if (otherView.isSingletonSpan()) {
                    insertSingletonAtIndex(i, otherView.getSingletonSpanValue());
                } else {
                    final Container otherContainer = otherView.getContainer();
                    insertSharedContainer(i, other, otherKey, otherContainer);
                }
                return i + 1;
            }
            try (SpanView ourView = wd.borrowSpanView(this, orIdx)) {
                final long flen = ourView.getFullBlockSpanLen();
                if (flen > 0) {
                    final long ki = ourView.getKey();
                    final long lastKey = getKeyForLastBlockInSpan(ki, flen);
                    if (uGreater(lastKey, otherKey)) {
                        return orIdx;
                    }
                    return orIdx + 1;
                }
                final Container orResultContainer;
                if (ourView.isSingletonSpan()) {
                    final long orIdxValue = ourView.getSingletonSpanValue();
                    if (otherView.isSingletonSpan()) {
                        final long otherValue = otherView.getSingletonSpanValue();
                        if (otherValue == orIdxValue) {
                            // no change.
                            return orIdx + 1;
                        }
                        final short v1, v2;
                        if (otherValue < orIdxValue) {
                            v1 = lowBits(otherValue);
                            v2 = lowBits(orIdxValue);
                        } else {
                            v1 = lowBits(orIdxValue);
                            v2 = lowBits(otherValue);
                        }
                        orResultContainer = Container.twoValues(v1, v2);
                    } else {
                        final Container orContainer = otherView.getContainer();
                        orResultContainer = orContainer.set(lowBits(orIdxValue));
                    }
                } else {
                    final Container c = ourView.getContainer();
                    if (otherView.isSingletonSpan()) {
                        final long otherValue = otherView.getSingletonSpanValue();
                        orResultContainer = c.iset(lowBits(otherValue));
                    } else {
                        final Container orContainer = otherView.getContainer();
                        orResultContainer = c.ior(orContainer);
                    }
                }
                if (orResultContainer.isAllOnes()) {
                    final int j = setOrInsertFullBlockSpanAtIndex(orIdx, otherKey, 1, sortedRangesMu);
                    // can't increment since it may have been merged with another span.
                    return j;
                }
                final Container copt = maybeOptimize(orResultContainer);
                setContainerSpan(orIdx, otherKey, copt);
                return orIdx + 1;
            }
        }
    }

    interface SpanViewRecycler {
        void returnSpanView(SpanView spanView);
    }

    static final class WorkData implements SpanViewRecycler {
        private static final SpanView[] ZERO_LENGTH_SPAN_VIEW_ARRAY = new SpanView[0];
        private int[] intArray;
        private SortedRangesInt sortedRangesInt;
        private SortedRangesInt madeNullSortedRanges;
        private ArraysBuf rspArraysBuf;
        private SpanView[] spanViewStack = ZERO_LENGTH_SPAN_VIEW_ARRAY;
        private int stackEnd = 0;
        private int stackGrowAmount = 0;

        int[] getIntArray() {
            if (intArray == null) {
                // In a 64bit hotspot JVM, the array object header is 12 bytes = 3 ints.
                intArray = new int[64 - 3];
            }
            return intArray;
        }

        void setIntArray(final int[] arr) {
            intArray = arr;
        }

        SortedRangesInt getSortedRanges() {
            if (sortedRangesInt == null) {
                sortedRangesInt = new SortedRangesInt(
                        Math.max(16 * 4 * 1024 / Integer.BYTES, SortedRanges.INT_DENSE_MAX_CAPACITY), 0);
            }
            sortedRangesInt.clear();
            return sortedRangesInt;
        }

        SortedRangesInt getMadeNullSortedRanges() {
            if (madeNullSortedRanges == null) {
                madeNullSortedRanges = new SortedRangesInt(
                        Math.max(16 * 4 * 1024 / Integer.BYTES, SortedRanges.INT_DENSE_MAX_CAPACITY), 0);
            }
            madeNullSortedRanges.clear();
            return madeNullSortedRanges;
        }

        ArraysBuf getArraysBuf(final int minCapacity) {
            if (rspArraysBuf == null) {
                rspArraysBuf = new ArraysBuf(minCapacity);
            } else {
                rspArraysBuf.reset(minCapacity);
            }
            return rspArraysBuf;
        }

        public SpanView borrowSpanView(final RspArray arr, final int arrIdx, final long spanInfo, final Object span) {
            final SpanView sv = borrowSpanView();
            sv.init(arr, arrIdx, spanInfo, span);
            return sv;
        }

        public SpanView borrowSpanView(final RspArray arr, final int arrIdx) {
            final SpanView sv = borrowSpanView();
            sv.init(arr, arrIdx);
            return sv;
        }

        public SpanView borrowSpanView() {
            if (stackEnd == 0) {
                ++stackGrowAmount;
                return new SpanView(this);
            }
            return spanViewStack[--stackEnd];
        }

        @Override
        public void returnSpanView(final SpanView sv) {
            if (stackGrowAmount != 0) {
                spanViewStack = new SpanView[spanViewStack.length + stackGrowAmount];
                stackGrowAmount = 0;
            }
            spanViewStack[stackEnd++] = sv;
        }
    }

    protected static final ThreadLocal<WorkData> workDataPerThread = ThreadLocal.withInitial(WorkData::new);

    private static final class WorkDataHolder {
        private WorkData wd = null;

        public WorkData get() {
            if (wd == null) {
                wd = workDataPerThread.get();
            }
            return wd;
        }
    }

    /**
     * For every element in other, add element to this RspArray. The argument won't be modified (with the possible
     * exclusion of sharing some of its containers Copy On Write).
     *
     * @param other the RspArray to add to this.
     */
    public void orEqualsUnsafeNoWriteCheck(final RspArray other) {
        orEqualsShiftedUnsafeNoWriteCheck(0, other);
    }

    /**
     * For every element in other, add (element + shiftAmount) to this RspArray. Note shiftAmount is assumed to be a
     * multiple of BLOCK_SIZE. The argument won't be modified (with the possible exclusion of sharing some of its
     * containers Copy On Write).
     *
     * @param shiftAmount the amount to add to each key in other before insertion
     * @param other the base keys to add in the (key + shiftAmount) formula for insertion.
     */
    public void orEqualsShiftedUnsafeNoWriteCheck(final long shiftAmount, final RspArray other) {
        if (other.size == 0) {
            return;
        }
        if (size == 0) {
            copySharingSpansFrom(other, shiftAmount);
            return;
        }
        // First check if this is effectively an append.
        if (tryAppendShiftedUnsafeNoWriteCheck(shiftAmount, other, false)) {
            return;
        }

        // Do a first pass finding all the containers on other for a key not present in this;
        // this way we try our best to avoid an O(n^2) scenario where we have to move later
        // spans to make space for earlier new keys over and over.

        // The first pass will accumulate pairs of (a, b) = (other's idx, this' idx) in this array,
        // for indices a of other that correspond to containers that will be inserted in b
        // in this.
        final WorkData wd = workDataPerThread.get();
        int[] idxPairs = wd.getIntArray();
        // Total number of elements stored in idxPairs array; should always be even.
        int idxPairsCount = 0;

        // As we check containers in others, the indices of the ones that were taken care of by the first
        // pass, and therefore can be skipped by the second pass, are stored here.
        SortedRanges secondPassSkips = wd.getSortedRanges();
        boolean tryAddToSecondPassSkips = true;
        int startPos = 0;
        for (int otherIdx = 0; otherIdx < other.size; ++otherIdx) {
            final Object otherSpan = other.spans[otherIdx];
            if (isFullBlockSpan(otherSpan)) {
                continue;
            }
            final long otherSpanKey = shiftAmount + other.getKey(otherIdx);
            int i = unsignedBinarySearch(this::getKey, startPos, size, otherSpanKey);
            if (i >= 0) {
                startPos = i + 1;
                continue;
            }
            i = ~i;
            if (i == size) {
                break;
            }
            // At this point we know this container from other has a key that is not
            // in our spans array; either it is part of a full block span,
            // and can be skipped altogether, or it will be added in this
            // first pass. Either way, it needs to be skipped in the second pass.
            if (tryAddToSecondPassSkips) {
                final SortedRanges sr = secondPassSkips.appendUnsafe(otherIdx);
                if (sr == null) {
                    tryAddToSecondPassSkips = false;
                } else {
                    secondPassSkips = sr;
                }
            }

            startPos = i;
            if (i > 0) {
                final Object span = spans[i - 1];
                final long spanInfo = spanInfos[i - 1];
                final long flen = getFullBlockSpanLen(spanInfo, span);
                if (flen > 0) {
                    final long kSpan = spanInfoToKey(spanInfo);
                    final long kSpanLast = getKeyForLastBlockInSpan(kSpan, flen);
                    if (kSpanLast >= otherSpanKey) {
                        continue;
                    }
                }
            }
            if (idxPairsCount + 2 > idxPairs.length) {
                final int[] newArr;
                if (idxPairs.length + 3 < 1024) {
                    newArr = new int[2 * idxPairs.length + 3];
                } else {
                    newArr = new int[idxPairs.length + 1024];
                }
                wd.setIntArray(newArr);
                System.arraycopy(idxPairs, 0, newArr, 0, idxPairsCount);
                idxPairs = newArr;
            }
            idxPairs[idxPairsCount++] = otherIdx;
            idxPairs[idxPairsCount++] = i;
        }
        if (idxPairsCount > 0) {
            boolean inPlace = true;
            Object[] newSpans = spans;
            long[] newSpanInfos = spanInfos;
            long[] newAcc = acc;
            final int deltaSpans = idxPairsCount / 2;
            final int newSize = size + deltaSpans;
            boolean accWasNull = acc == null;
            if (newSize > spanInfos.length) {
                inPlace = false;
                newSpans = new Object[newSize];
                newSpanInfos = new long[newSize];
                if (accWasNull) {
                    cardData = -1;
                }
                newAcc = (newSize > accNullThreshold) ? new long[newSize] : null;
            }
            int lastMoveRangeIdx = size - 1;
            int dstIdx = newSize - 1;
            while (idxPairsCount > 0) {
                final int thisIdx = idxPairs[--idxPairsCount];
                final int otherIdx = idxPairs[--idxPairsCount];
                for (int i = lastMoveRangeIdx; i >= thisIdx; --i) {
                    copyKeyAndSpanStealingContainers(i, spanInfos, spans, dstIdx, newSpanInfos, newSpans);
                    --dstIdx;
                }
                copyKeyAndSpanMaybeSharing(shiftAmount, other, otherIdx, newSpanInfos, newSpans, dstIdx, true);
                --dstIdx;
                lastMoveRangeIdx = thisIdx - 1;
            }
            if (!inPlace) {
                for (int i = lastMoveRangeIdx; i >= 0; --i) {
                    copyKeyAndSpanStealingContainers(i, spanInfos, spans, dstIdx, newSpanInfos, newSpans);
                    --dstIdx;
                }
                spanInfos = newSpanInfos;
                spans = newSpans;
                if (lastMoveRangeIdx >= 0 && !accWasNull && newAcc != null) {
                    System.arraycopy(acc, 0, newAcc, 0, lastMoveRangeIdx + 1);
                }
                acc = newAcc;
            }
            modifiedSpan(idxPairs[1]);
            size = newSize;
        }

        // Do the actual merging of container to container.
        startPos = 0;
        final Index.Iterator skipsIter = secondPassSkips.getIterator();
        int nextSkip;
        if (!skipsIter.hasNext()) {
            nextSkip = -1;
        } else {
            nextSkip = (int) skipsIter.nextLong();
        }
        final MutableObject<SortedRanges> sortedRangesMu = getWorkSortedRangesMutableObject(wd);
        for (int otherIdx = 0; otherIdx < other.size; ++otherIdx) {
            if (nextSkip == otherIdx) {
                if (!skipsIter.hasNext()) {
                    nextSkip = -1;
                } else {
                    nextSkip = (int) skipsIter.nextLong();
                }
                continue;
            }
            startPos = orEqualsSpan(shiftAmount, other, otherIdx, startPos, sortedRangesMu, wd);
        }
        collectRemovedIndicesIfAny(sortedRangesMu);
    }

    protected void markIndexAsRemoved(final MutableObject<SortedRanges> madeNullSpansMu, final int index) {
        spanInfos[index] = -1;
        modifiedSpan(index);
        SortedRanges madeNullSpans = madeNullSpansMu.getValue();
        if (madeNullSpans != null) {
            madeNullSpans = madeNullSpans.appendUnsafe(index);
            madeNullSpansMu.setValue(madeNullSpans);
        }
    }

    protected void markIndexRangeAsRemoved(
            final MutableObject<SortedRanges> madeNullSpansMu, final int iFirst, final int iLast) {
        for (int i = iFirst; i <= iLast; ++i) {
            spanInfos[i] = -1;
        }
        modifiedSpan(iFirst);
        SortedRanges madeNullSpans = madeNullSpansMu.getValue();
        if (madeNullSpans != null) {
            madeNullSpans = madeNullSpans.appendRangeUnsafe(iFirst, iLast);
            madeNullSpansMu.setValue(madeNullSpans);
        }
    }

    protected void collectRemovedIndicesIfAny(final MutableObject<SortedRanges> madeNullSpansMu) {
        final SortedRanges madeNullSpans = madeNullSpansMu.getValue();
        if (madeNullSpans == null || madeNullSpans.getCardinality() > 0) {
            collectRemovedIndicesUnsafeNoWriteCheck(madeNullSpans);
        }
    }

    protected MutableObject<SortedRanges> getWorkSortedRangesMutableObject(final WorkData wd) {
        final SortedRanges madeNullSpans = wd.getMadeNullSortedRanges();
        return new MutableObject<>(madeNullSpans);
    }

    /**
     * AndNotEquals a single span into this container.
     *
     * @param startPos the first position to start looking for orKey in this container.
     * @param other RspArray for the span to remove.
     * @param otherIdx the index of the span to remove in the other RspArray.
     * @return the index in our parallel arrays to continue searches for keys after (removeFirstKey, removeSpan).
     */
    private int andNotEqualsSpan(final int startPos, final RspArray other, final int otherIdx,
            final MutableObject<SortedRanges> madeNullSpansMu,
            final WorkData wd) {
        try (SpanView otherView = wd.borrowSpanView(other, otherIdx)) {
            final long removeKey = otherView.getKey();
            final long removeflen = otherView.getFullBlockSpanLen();
            if (removeflen == 0) {
                final int i = getSpanIndex(startPos, removeKey);
                if (i < 0) {
                    return ~i;
                }
                try (SpanView ourView = wd.borrowSpanView(this, i)) {
                    final long flen = ourView.getFullBlockSpanLen();
                    if (flen > 0) {
                        final Container notContainerPre;
                        if (otherView.isSingletonSpan()) {
                            final long singletonValue = otherView.getSingletonSpanValue();
                            int v = lowBitsAsInt(singletonValue);
                            if (v == 0) {
                                notContainerPre = Container.singleRange(1, BLOCK_SIZE);
                            } else if (v == BLOCK_LAST) {
                                notContainerPre = Container.singleRange(0, BLOCK_LAST);
                            } else {
                                notContainerPre = new RunContainer(0, v, v + 1, BLOCK_SIZE);
                            }
                        } else {
                            final Container rc = otherView.getContainer();
                            notContainerPre = rc.not(0, BLOCK_SIZE);
                        }
                        final Container notContainer;
                        final long keyNotContainer;
                        if (notContainerPre.isSingleElement()) {
                            notContainer = null;
                            keyNotContainer = removeKey | notContainerPre.first();
                        } else {
                            notContainer = maybeOptimize(notContainerPre);
                            keyNotContainer = removeKey;
                        }
                        final long firstKey = getKey(i);
                        final long endKey = firstKey + BLOCK_SIZE * (flen - 1); // inclusive
                        if (uLess(firstKey, removeKey)) {
                            if (uLess(removeKey, endKey)) {
                                final ArraysBuf buf = wd.getArraysBuf(3);
                                buf.pushFullBlockSpan(firstKey, distanceInBlocks(firstKey, removeKey));
                                buf.pushContainer(keyNotContainer, notContainer);
                                buf.pushFullBlockSpan(removeKey + BLOCK_SIZE, distanceInBlocks(removeKey, endKey));
                                replaceSpanAtIndex(i, buf);
                            } else {
                                final ArraysBuf buf = wd.getArraysBuf(2);
                                buf.pushFullBlockSpan(firstKey, distanceInBlocks(firstKey, removeKey));
                                buf.pushContainer(keyNotContainer, notContainer);
                                replaceSpanAtIndex(i, buf);
                            }
                            return i + 2;
                        }
                        if (uLess(removeKey, endKey)) {
                            final ArraysBuf buf = wd.getArraysBuf(2);
                            buf.pushContainer(keyNotContainer, notContainer);
                            buf.pushFullBlockSpan(removeKey + BLOCK_SIZE, distanceInBlocks(removeKey, endKey));
                            replaceSpanAtIndex(i, buf);
                        } else if (notContainer == null) {
                            setSingletonSpan(i, keyNotContainer);
                        } else {
                            setContainerSpan(i, keyNotContainer, notContainer);
                        }
                        return i + 1;
                    }
                    if (ourView.isSingletonSpan()) {
                        final long firstValue = ourView.getSingletonSpanValue();
                        if (otherView.isSingletonSpan()) {
                            if (otherView.getSingletonSpanValue() == firstValue) {
                                markIndexAsRemoved(madeNullSpansMu, i);
                            }
                            return i + 1;
                        }
                        final Container removeContainer = otherView.getContainer();
                        if (removeContainer.contains(lowBits(firstValue))) {
                            markIndexAsRemoved(madeNullSpansMu, i);
                        }
                        return i + 1;
                    }
                    final Container container = ourView.getContainer();
                    final Container result;
                    if (otherView.isSingletonSpan()) {
                        final long v = otherView.getSingletonSpanValue();
                        result = container.iunset(lowBits(v));
                    } else {
                        final Container removed = otherView.getContainer();
                        result = container.iandNot(removed);
                    }
                    if (result.isEmpty()) {
                        markIndexAsRemoved(madeNullSpansMu, i);
                        return i + 1;
                    }
                    final long firstKey = ourView.getKey();
                    if (result.isSingleElement()) {
                        setSingletonSpan(i, firstKey | result.first());
                    } else {
                        final Container c3 = maybeOptimize(result);
                        setContainerSpan(container, i, firstKey, c3);
                    }
                    return i + 1;
                }
            }
            // removeflen > 0.
            int idxBegin = getSpanIndex(startPos, removeKey);
            if (idxBegin < 0) {
                idxBegin = -idxBegin - 1;
                if (idxBegin >= size) {
                    return size;
                }
            }
            final long removeLastKey = removeKey + BLOCK_SIZE * (removeflen - 1); // inclusive.
            final int idxEnd;
            if (removeLastKey == removeKey) {
                idxEnd = idxBegin;
            } else {
                int j = getSpanIndex(idxBegin + 1, removeLastKey);
                idxEnd = (j >= 0) ? j : -j - 2;
            }
            final long spanInfoAtIdxBegin = spanInfos[idxBegin];
            final long keyAtIdxBegin = spanInfoToKey(spanInfoAtIdxBegin);
            if (keyAtIdxBegin > removeLastKey) {
                return idxBegin;
            }
            final Object spanAtIdxBegin = spans[idxBegin];
            final long flenAtIdxBegin = getFullBlockSpanLen(spanInfoAtIdxBegin, spanAtIdxBegin);
            final long keyAtIdxEnd;
            final long flenAtIdxEnd;
            if (idxBegin == idxEnd) {
                keyAtIdxEnd = keyAtIdxBegin;
                flenAtIdxEnd = flenAtIdxBegin;
            } else {
                final Object spanAtIdxEnd = spans[idxEnd];
                final long spanInfoAtIdxEnd = spanInfos[idxEnd];
                keyAtIdxEnd = spanInfoToKey(spanInfoAtIdxEnd);
                flenAtIdxEnd = getFullBlockSpanLen(spanInfoAtIdxEnd, spanAtIdxEnd);
            }
            int dst = idxBegin;
            if (uLess(keyAtIdxBegin, removeKey)) {
                dst = idxBegin + 1;
                if (flenAtIdxBegin > 0) {
                    final long newflen = distanceInBlocks(keyAtIdxBegin, removeKey);
                    setFullBlockSpan(idxBegin, keyAtIdxBegin, newflen);
                }
            }
            int src = idxEnd + 1;
            final long blockKeyAtIdxEnd = highBits(keyAtIdxEnd);
            final long lastKeyAtIdxEnd = getKeyForLastBlockInSpan(blockKeyAtIdxEnd, flenAtIdxEnd);
            if (uLess(removeLastKey, lastKeyAtIdxEnd)) {
                final long nextKey = nextKey(removeLastKey);
                final long newflen = distanceInBlocks(removeLastKey, lastKeyAtIdxEnd);
                if (idxEnd >= dst) {
                    setFullBlockSpan(idxEnd, nextKey, newflen);
                    src = idxEnd;
                } else {
                    insertFullBlockSpanAtIndex(dst, nextKey, newflen);
                    src = dst;
                }
            }
            if (dst < src) {
                markIndexRangeAsRemoved(madeNullSpansMu, dst, src - 1);
            }
            return src;
        }
    }

    /**
     * Remove every element from argument from this RspArray. Argument won't be modified.
     *
     * @param other the elements to remove.
     */
    public void andNotEqualsUnsafeNoWriteCheck(final RspArray other) {
        if (isEmpty() || other.isEmpty()) {
            return;
        }
        final long keyForFirstBlock = keyForFirstBlock();
        if (other.keyForLastBlock() < keyForFirstBlock || keyForLastBlock() < other.keyForFirstBlock()) {
            return;
        }
        int startPos = 0;
        int firstKey = other.getSpanIndex(0, keyForFirstBlock);
        if (firstKey < 0) {
            firstKey = -firstKey - 1;
        }
        final WorkData wd = workDataPerThread.get();
        final MutableObject<SortedRanges> madeNullSpansMu = getWorkSortedRangesMutableObject(wd);
        for (int andNotIdx = firstKey; andNotIdx < other.size; ++andNotIdx) {
            startPos = andNotEqualsSpan(startPos, other, andNotIdx, madeNullSpansMu, wd);
            if (startPos >= size) {
                break;
            }
        }
        collectRemovedIndicesIfAny(madeNullSpansMu);
    }

    private void collectRemovedIndicesUnsafeNoWriteCheck(final SortedRanges madeNullSpans) {
        if (madeNullSpans == null) {
            compactRemovedUnsafeNoWriteCheck();
            return;
        }
        final Index.RangeIterator it = madeNullSpans.getRangeIterator();
        if (!it.hasNext()) {
            return;
        }
        it.next();
        long dst = it.currentRangeStart();
        while (true) {
            long j = it.currentRangeEnd() + 1;
            long k; // exclusive end of range of non-nulls.
            if (!it.hasNext()) {
                k = size;
            } else {
                it.next();
                k = it.currentRangeStart();
            }
            long n = k - j;
            arrayCopies((int) j, (int) dst, (int) n);
            dst += n;
            if (k == size) {
                break;
            }
        }
        size = (int) dst;
        tryCompactUnsafe(4);
    }

    private void compactRemovedUnsafeNoWriteCheck() {
        int i = cardData + 1;
        while (i < size && getSpanInfo(i) != -1) {
            ++i;
        }
        if (i == size) {
            return;
        }
        // getKey(i) == -1.
        int dst = i;
        while (true) {
            int j = i + 1;
            while (j < size && getSpanInfo(j) == -1) {
                ++j;
            }
            if (j == size) {
                break;
            }
            // getKey(j) != -1
            int k = j + 1;
            while (k < size && getSpanInfo(k) != -1) {
                ++k;
            }
            // keys[k - 1] != -1.
            // move the block [j, k - 1] to i.
            final int n = k - j;
            arrayCopies(j, dst, n);
            dst += n;
            i = k;
            while (i < size && getSpanInfo(i) != -1) {
                ++i;
            }
            if (i == size) {
                break;
            }
        }
        size = dst;
        tryCompactUnsafe(4);
    }

    private int andEqualsSpan(final RspArray other, final int otherIdx,
            final int startPos, final MutableObject<SortedRanges> madeNullSpansMu,
            final WorkData wd) {
        try (SpanView otherView = wd.borrowSpanView(other, otherIdx)) {
            final long andflen = otherView.getFullBlockSpanLen();
            final long andKey = otherView.getKey();
            if (andflen > 0) {
                final long andLastKey = getKeyForLastBlockInSpan(andKey, andflen);
                int andLastKeyIdx = keySearch(startPos, andLastKey);
                if (andLastKeyIdx >= 0) {
                    return andLastKeyIdx + 1;
                }
                return -andLastKeyIdx - 1;
            }
            int andIdx = getSpanIndex(startPos, andKey);
            if (andIdx < 0) {
                return -andIdx - 1;
            }
            try (SpanView ourView = wd.borrowSpanView(this, andIdx)) {
                final long flen = ourView.getFullBlockSpanLen();
                if (flen > 0) {
                    final long ourKey = ourView.getKey();
                    final long lastKey = getKeyForLastBlockInSpan(ourKey, flen);
                    // andIdx > 0, therefore andKey is contained in this span.
                    if (uLess(ourKey, andKey)) {
                        // when this method is called from andEquals, given the previous pruning this
                        // case can't be hit.
                        if (uLess(andKey, lastKey)) {
                            final ArraysBuf buf = wd.getArraysBuf(3);
                            buf.pushFullBlockSpan(ourKey, distanceInBlocks(ourKey, andKey));
                            if (otherView.isSingletonSpan()) {
                                buf.pushSingletonSpan(otherView.getSingletonSpanValue());
                            } else {
                                buf.pushSharedContainer(other, andKey, otherView.getContainer());
                            }
                            buf.pushFullBlockSpan(nextKey(andKey), distanceInBlocks(andKey, lastKey));
                            replaceSpanAtIndex(andIdx, buf);
                        } else {
                            final ArraysBuf buf = wd.getArraysBuf(2);
                            buf.pushFullBlockSpan(ourKey, distanceInBlocks(ourKey, andKey));
                            if (otherView.isSingletonSpan()) {
                                buf.pushSingletonSpan(ourView.getSingletonSpanValue());
                            } else {
                                buf.pushSharedContainer(other, andKey, otherView.getContainer());
                            }
                            replaceSpanAtIndex(andIdx, buf);
                        }
                    } else if (uLess(andKey, lastKey)) {
                        final ArraysBuf buf = wd.getArraysBuf(2);
                        // when this method is called from andEquals, given the previous pruning this
                        // case can't be hit.
                        if (otherView.isSingletonSpan()) {
                            buf.pushSingletonSpan(otherView.getSingletonSpanValue());
                        } else {
                            buf.pushSharedContainer(other, andKey, otherView.getContainer());
                        }
                        buf.pushFullBlockSpan(nextKey(andKey), distanceInBlocks(andKey, lastKey));
                        replaceSpanAtIndex(andIdx, buf);
                    } else if (otherView.isSingletonSpan()) {
                        setSingletonSpan(andIdx, otherView.getSingletonSpanValue());
                    } else {
                        setSharedContainerRaw(andIdx, other, andKey, otherView.getContainer());
                    }
                    return andIdx + 1;
                }
                Container result = null; // if result stays null, the result is empty and we should remove this span.
                Container ourContainer = null;
                // Container operations may return copy on write copies in either direction;
                // when a ContainerResource is used it can't be freed until we are certain there is no
                // outstanding reference to a container it may be holding.
                if (ourView.isSingletonSpan()) {
                    final long ourValue = ourView.getSingletonSpanValue();
                    if (otherView.isSingletonSpan()) {
                        final long andSpanValue = otherView.getSingletonSpanValue();
                        if (andSpanValue == ourValue) {
                            return andIdx + 1;
                        }
                    } else {
                        final Container ac = otherView.getContainer();
                        if (ac.contains(lowBits(ourValue))) {
                            return andIdx + 1;
                        }
                    }
                } else {
                    ourContainer = ourView.getContainer();
                    if (otherView.isSingletonSpan()) {
                        final long andSpanValue = otherView.getSingletonSpanValue();
                        if (ourContainer.contains(lowBits(andSpanValue))) {
                            setSingletonSpan(andIdx, andSpanValue);
                            return andIdx + 1;
                        }
                    } else {
                        final Container ac = otherView.getContainer();
                        result = ourContainer.iand(ac);
                        if (result.isEmpty()) {
                            result = null;
                        }
                    }
                }
                if (result == null) {
                    markIndexAsRemoved(madeNullSpansMu, andIdx);
                    return andIdx + 1;
                }
                if (result.isSingleElement()) {
                    setSingletonSpan(andIdx, andKey | result.first());
                } else {
                    final Container c3 = maybeOptimize(result);
                    setContainerSpan(ourContainer, andIdx, andKey, c3);
                }
                return andIdx + 1;
            }
        }
    }

    protected static class ArraysBuf {
        private static final int CAPACITY_FLOOR = 4;
        private long[] spanInfos;
        private Object[] spans;
        private int size;

        ArraysBuf(final int minCapacity) {
            final int capacity = Math.max(minCapacity, CAPACITY_FLOOR);
            spanInfos = new long[capacity];
            spans = new Object[capacity];
            size = 0;
        }

        void reset(final int minCapacity) {
            if (minCapacity > capacity()) {
                final int capacity = Math.max(minCapacity, CAPACITY_FLOOR);
                spanInfos = new long[capacity];
                spans = new Object[capacity];
            }
            size = 0;
        }

        int capacity() {
            if (spanInfos == null) {
                return 0;
            }
            return spanInfos.length;
        }

        void swap(final long[] spanInfos, final Object[] spans) {
            if (spanInfos.length >= CAPACITY_FLOOR) {
                this.spanInfos = spanInfos;
                this.spans = spans;
                return;
            }
            this.spanInfos = null;
            this.spans = null;
        }

        void pushSharedContainer(final RspArray other, final long key, final Container c) {
            setContainerSpanRaw(spanInfos, spans, size, key, other.shareContainer(c));
        }

        void pushContainer(final long key, final Container container) {
            spanInfos[size] = key;
            spans[size] = container;
            ++size;
        }

        void pushSingletonSpan(final long value) {
            spanInfos[size] = value;
            spans[size] = null;
            ++size;
        }

        void pushFullBlockSpan(final long key, final long flen) {
            setFullBlockSpanRaw(size, spanInfos, spans, key, flen);
            ++size;
        }
    }

    /**
     * Intersects this RspArray with the argument, leaving the result on this RspArray. The argument won't be modified.
     *
     * @param other an RspArray.
     */
    public void andEqualsUnsafeNoWriteCheck(final RspArray other) {
        // First do one pass of pruning spans that do not have a chance to intersect.
        // The max number of resulting spans is potentially greater than max(size, other.size):
        // think about a full block span in one array that ends up being split due to multiple
        // single block containers in the other array; eg:
        // this: {[key / BLOCK_SIZE, flen]} = { [0, 4], [10, 0], [11, 0] }
        // other: = { [1, 0], [2, 0], [9, 7] }
        // result: = { [1, 0], [2, 0], [10, 0], [11, 0] }
        final WorkData wd = workDataPerThread.get();
        final int maxNewCapacity = size + other.size;
        final ArraysBuf buf = wd.getArraysBuf(maxNewCapacity);
        int startPos = 0;
        for (int andIdx = 0; andIdx < other.size; ++andIdx) {
            if (startPos >= size) {
                break;
            }
            final Object andSpan = other.spans[andIdx];
            final long andSpanInfo = other.spanInfos[andIdx];
            final long andSpanKey = spanInfoToKey(andSpanInfo);
            final long andflen = getFullBlockSpanLen(andSpanInfo, andSpan);
            final long andLastKey = getKeyForLastBlockInSpan(andSpanKey, andflen);
            int firstIdx = getSpanIndex(startPos, andSpanKey);
            if (firstIdx < 0) {
                firstIdx = -firstIdx - 1;
                if (firstIdx == size) {
                    break;
                }
            }
            int lastIdx;
            if (andSpanKey == andLastKey || firstIdx == size - 1) {
                lastIdx = firstIdx;
            } else {
                lastIdx = getSpanIndex(firstIdx + 1, andLastKey);
                if (lastIdx < 0) {
                    lastIdx = -lastIdx - 2;
                }
            }
            int i = firstIdx;
            while (true) {
                final long spanInfo = spanInfos[i];
                final long spanKey = spanInfoToKey(spanInfo);
                if (uGreater(spanKey, andLastKey)) {
                    startPos = i;
                    break;
                }
                final Object span = spans[i];
                final long flen = getFullBlockSpanLen(spanInfo, span);
                if (flen > 0) {
                    final long lastKey = getKeyForLastBlockInSpan(spanKey, flen);
                    if (uGreaterOrEqual(lastKey, andSpanKey)) {
                        final long newKey = uMax(andSpanKey, spanKey);
                        long newLen = flen - distanceInBlocks(spanKey, newKey);
                        boolean bail = false;
                        if (uGreater(lastKey, andLastKey)) {
                            newLen -= distanceInBlocks(andLastKey, lastKey);
                            bail = true;
                        }
                        setFullBlockSpanRaw(buf.size, buf.spanInfos, buf.spans, newKey, newLen);
                        ++buf.size;
                        if (bail) {
                            startPos = i;
                            break;
                        }
                    }
                } else if (uGreaterOrEqual(spanKey, andSpanKey)) {
                    buf.spanInfos[buf.size] = spanInfo;
                    buf.spans[buf.size] = span;
                    ++buf.size;
                }
                ++i;
                if (i > lastIdx) {
                    startPos = i;
                    break;
                }
            }
        }
        final int maxWaste = 7;
        size = buf.size;
        if (buf.capacity() - buf.size > maxWaste) {
            spanInfos = new long[buf.size];
            spans = new Object[buf.size];
            if (buf.size > accNullThreshold) {
                acc = new long[buf.size];
            }
            System.arraycopy(buf.spanInfos, 0, spanInfos, 0, buf.size);
            System.arraycopy(buf.spans, 0, spans, 0, buf.size);
        } else {
            final long[] oldSpanInfos = spanInfos;
            final Object[] oldSpans = spans;
            if (buf.capacity() != spanInfos.length && buf.capacity() > accNullThreshold) {
                acc = new long[buf.capacity()];
            }
            spanInfos = buf.spanInfos;
            spans = buf.spans;
            buf.swap(oldSpanInfos, oldSpans);
        }
        cardData = -1;
        startPos = 0;
        final MutableObject<SortedRanges> madeNullSpansMu = getWorkSortedRangesMutableObject(wd);
        for (int otherIdx = 0; otherIdx < other.size; ++otherIdx) {
            startPos = andEqualsSpan(other, otherIdx, startPos, madeNullSpansMu, wd);
            if (startPos >= size) {
                break;
            }
        }
        collectRemovedIndicesIfAny(madeNullSpansMu);
    }

    public void applyKeyOffset(final long offset) {
        for (int i = 0; i < size; ++i) {
            applyKeyOffset(i, offset);
        }
    }

    // end is inclusive
    private void appendSpanIntersectionByKeyRange(final RspArray r, final int i, final long start, final long end) {
        final Object span = spans[i];
        final long spanInfo = spanInfos[i];
        final long flen = getFullBlockSpanLen(spanInfo, span);
        if (flen > 0) {
            final long key = spanInfoToKey(spanInfo);
            final long sLastPlusOne = key + BLOCK_SIZE * flen;
            final long resultStart = uMax(key, start);
            if (uGreaterOrEqual(resultStart, sLastPlusOne)) {
                return;
            }
            final long resultEnd = uMin(sLastPlusOne - 1, end);
            final long nextHiBits;
            if (highBits(resultStart) == resultStart) {
                nextHiBits = resultStart;
            } else { // k < resultStart (unsigned comparison)
                final long resultStartHiBits = highBits(resultStart);
                if (uLess(resultEnd, resultStartHiBits + BLOCK_LAST)) {
                    final int cs = lowBitsAsInt(resultStart);
                    final int ce = lowBitsAsInt(resultEnd);
                    if (cs == ce) {
                        r.appendSingletonSpan(resultStart);
                    } else {
                        r.appendContainer(resultStartHiBits, Container.rangeOfOnes(cs, ce + 1 /* exclusive */));
                    }
                    return;
                }
                final int cs = lowBitsAsInt(resultStart);
                if (cs == BLOCK_LAST) {
                    r.appendSingletonSpan(resultStart);
                } else {
                    r.appendContainer(resultStartHiBits, Container.rangeOfOnes(cs, BLOCK_SIZE));
                }
                nextHiBits = nextKey(resultStartHiBits);
            }
            final long resultEndHiBits = highBits(resultEnd);
            final long midflen = distanceInBlocks(nextHiBits, resultEndHiBits);
            final long keyAfterMid;
            if (midflen > 0) {
                r.appendFullBlockSpan(nextHiBits, midflen);
                keyAfterMid = nextHiBits + midflen * BLOCK_SIZE;
            } else {
                keyAfterMid = nextHiBits;
            }
            if (uGreaterOrEqual(resultEnd, keyAfterMid)) {
                final int e = lowBitsAsInt(resultEnd);
                if (e == BLOCK_LAST) {
                    r.appendFullBlockSpan(keyAfterMid, 1);
                } else {
                    if (e == 0) {
                        r.appendSingletonSpan(keyAfterMid);
                    } else {
                        r.appendContainer(keyAfterMid, Container.rangeOfOnes(0, e + 1 /* exclusive */));
                    }
                }
            }
            return;
        }
        // we have a single container to intersect against [start, end).
        if (isSingletonSpan(span)) {
            final long v = spanInfoToSingletonSpanValue(spanInfo);
            if (start <= v && v <= end) {
                r.appendSingletonSpan(v);
            }
            return;
        }
        final long key = spanInfoToKey(spanInfo);
        final long blockLast = key + BLOCK_LAST;
        if (uLess(end, key) || uGreater(start, blockLast)) {
            return;
        }
        final long resultStart = uMax(key, start);
        final long resultEnd;
        if (uLess(end, blockLast)) {
            resultEnd = end;
        } else {
            if (resultStart == key) {
                r.appendSharedContainerMaybePacked(this, i, key, span);
                return;
            }
            resultEnd = blockLast;
        }
        final int rStart = (int) (resultStart - key);
        final int rEndExclusive = (int) (resultEnd - key) + 1;
        final Container result;
        try (SpanView view = workDataPerThread.get().borrowSpanView(this, i, spanInfo, span)) {
            final Container c = view.getContainer();
            result = c.andRange(rStart, rEndExclusive);
            if (result.isEmpty()) {
                return;
            }
            if (result.isSingleElement()) {
                r.appendSingletonSpan(key | result.first());
                return;
            }
            r.appendContainer(key, maybeOptimize(result));
        }
    }

    boolean forEachLongInSpanWithOffsetAndMaxCount(final int i, final long offset, LongAbortableConsumer lc,
            final long maxCount) {
        final MutableLong n = new MutableLong(0);
        forEachLongInSpanWithOffset(i, offset, (final long v) -> {
            if (!lc.accept(v)) {
                return false;
            }
            n.increment();
            return n.longValue() < maxCount;
        });
        return n.longValue() >= maxCount; // The only way we get to maxCount is if lc never returns false above.
    }

    boolean forEachLongInSpanWithOffset(final int i, final long offset, LongAbortableConsumer lc) {
        try (SpanView view = workDataPerThread.get().borrowSpanView(this, i)) {
            if (view.isSingletonSpan()) {
                final long v = view.getSingletonSpanValue();
                return lc.accept(v);
            }
            final long flen = view.getFullBlockSpanLen();
            final long key = view.getKey();
            if (flen > 0) {
                final long oneAfterLast = key + flen * BLOCK_SIZE;
                for (long v = key + offset; v < oneAfterLast; ++v) {
                    final boolean wantMore = lc.accept(v);
                    if (!wantMore) {
                        return false;
                    }
                }
                return true;
            }
            final Container c = view.getContainer();
            final boolean wantMore = c.forEach(
                    (int) offset, (short v) -> lc.accept(key | unsignedShortToLong(v)));
            return wantMore;
        }
    }

    boolean forEachLongInSpanWithMaxCount(final int i, LongAbortableConsumer lc, final long maxCount) {
        final MutableLong n = new MutableLong(0);
        forEachLongInSpan(i, (final long v) -> {
            if (!lc.accept(v)) {
                return false;
            }
            n.increment();
            return n.longValue() < maxCount;
        });
        return n.longValue() >= maxCount; // The only way we get to maxCount is if lc never returns false above.
    }

    boolean forEachLongInSpan(final int i, LongAbortableConsumer lc) {
        try (SpanView view = workDataPerThread.get().borrowSpanView(this, i)) {
            if (view.isSingletonSpan()) {
                final long v = view.getSingletonSpanValue();
                return lc.accept(v);
            }
            final long flen = view.getFullBlockSpanLen();
            final long key = view.getKey();
            if (flen > 0) {
                final long oneAfterLast = key + flen * BLOCK_SIZE;
                for (long v = key; v < oneAfterLast; ++v) {
                    final boolean wantMore = lc.accept(v);
                    if (!wantMore) {
                        return false;
                    }
                }
                return true;
            }
            final Container c = view.getContainer();
            final boolean wantMore = c.forEach(
                    (short v) -> lc.accept(key | unsignedShortToLong(v)));
            return wantMore;
        }
    }

    public boolean forEachLong(final LongAbortableConsumer lc) {
        for (int i = 0; i < size; ++i) {
            if (!forEachLongInSpan(i, lc)) {
                return false;
            }
        }
        return true;
    }

    public boolean forEachLongRangeInSpanWithOffsetAndMaxCardinality(
            final int i, final long offset, final long maxCardinality,
            final LongRangeAbortableConsumer larc) {
        if (maxCardinality <= 0) {
            return true;
        }
        try (SpanView view = workDataPerThread.get().borrowSpanView(this, i)) {
            if (view.isSingletonSpan()) {
                if (offset != 0) {
                    throw new IllegalArgumentException("offset=" + offset + " and single key span.");
                }
                final long v = view.getSingletonSpanValue();
                return larc.accept(v, v);
            }
            final long flen = view.getFullBlockSpanLen();
            final long key = view.getKey();
            if (flen > 0) {
                final long start = key + offset;
                long end = key + flen * BLOCK_SIZE - 1;
                final long d = end - start + 1;
                if (d > maxCardinality) {
                    end = start + maxCardinality - 1;
                }
                return larc.accept(start, end);
            }
            long remaining = maxCardinality;
            final int bufSz = 10;
            final short[] buf = new short[bufSz];
            final Container c = view.getContainer();
            final SearchRangeIterator ri = c.getShortRangeIterator((int) offset);
            while (ri.hasNext()) {
                final int nRanges = ri.next(buf, 0, bufSz / 2);
                for (int j = 0; j < 2 * nRanges; j += 2) {
                    final long start = key | unsignedShortToLong(buf[j]);
                    long end = key | unsignedShortToLong(buf[j + 1]);
                    long delta = end - start + 1;
                    if (delta > remaining) {
                        delta = remaining;
                        end = start + remaining - 1;
                    }
                    if (!larc.accept(start, end)) {
                        return false;
                    }
                    remaining -= delta;
                    if (remaining <= 0) {
                        return true;
                    }
                }
            }
        }
        return true;
    }

    boolean forEachLongRangeInSpanWithOffset(final int i, final long offset,
            final LongRangeAbortableConsumer larc) {
        try (SpanView view = workDataPerThread.get().borrowSpanView(this, i)) {
            if (view.isSingletonSpan()) {
                if (offset != 0) {
                    throw new IllegalArgumentException("offset=" + offset + " and single key span.");
                }
                final long v = view.getSingletonSpanValue();
                return larc.accept(v, v);
            }
            final long flen = view.getFullBlockSpanLen();
            final long key = view.getKey();
            if (flen > 0) {
                final long oneAfterLast = key + flen * BLOCK_SIZE;
                return larc.accept(key + offset, oneAfterLast - 1);
            }
            final int bufSz = 10;
            final short[] buf = new short[bufSz];
            final Container c = view.getContainer();
            final SearchRangeIterator ri = c.getShortRangeIterator((int) offset);
            while (ri.hasNext()) {
                final int nRanges = ri.next(buf, 0, bufSz / 2);
                for (int j = 0; j < 2 * nRanges; j += 2) {
                    final long start = key | unsignedShortToLong(buf[j]);
                    final long end = key | unsignedShortToLong(buf[j + 1]);
                    if (!larc.accept(start, end)) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    static LongRangeAbortableConsumer makeAdjacentRangesCollapsingWrapper(final long[] pendingRange,
            final LongRangeAbortableConsumer lrac) {
        pendingRange[0] = -2;
        pendingRange[1] = -2;
        final LongRangeAbortableConsumer wrapper = (final long start, final long end) -> {
            if (pendingRange[0] == -2) {
                pendingRange[0] = start;
                pendingRange[1] = end;
                return true;
            }
            if (pendingRange[1] + 1 == start) {
                pendingRange[1] = end;
                return true;
            }
            final boolean wantsMore = lrac.accept(pendingRange[0], pendingRange[1]);
            pendingRange[0] = start;
            pendingRange[1] = end;
            return wantsMore;
        };
        return wrapper;
    }

    public boolean forEachLongRange(final LongRangeAbortableConsumer lrac) {
        if (size == 0) {
            return true;
        }
        if (size == 1) {
            return forEachLongRangeInSpanWithOffset(0, 0, lrac);
        }
        final long[] pendingRange = new long[2];
        final LongRangeAbortableConsumer wrapper = makeAdjacentRangesCollapsingWrapper(pendingRange, lrac);
        for (int i = 0; i < size; ++i) {
            if (!forEachLongRangeInSpanWithOffset(i, 0, wrapper)) {
                return false;
            }
        }
        if (pendingRange[0] != -2) {
            return lrac.accept(pendingRange[0], pendingRange[1]);
        }
        return true;
    }

    // end is inclusive
    protected T subrangeByKeyInternal(final long start, final long end) {
        T r = make();
        final long startHiBits = highBits(start);
        int ikstart = getSpanIndex(startHiBits);
        if (ikstart < 0) {
            ikstart = -ikstart - 1;
            if (ikstart >= size) {
                return r;
            }
        }
        final long endHighBits = highBits(end);
        int ikend = getSpanIndex(endHighBits);
        if (ikend < 0) {
            // If end is not an exact match, the range cannot span beyond the previous position returned.
            ikend = -ikend - 2;
            if (ikend < 0) {
                return r;
            }
        }
        for (int i = ikstart; i <= ikend; ++i) {
            appendSpanIntersectionByKeyRange(r, i, start, end);
        }
        return r;
    }

    // If the result is empty we return null.
    protected T subrangeByPosInternal(final long firstPos, final long lastPos) {
        final long effectiveLastPos;
        if (isCardinalityCached()) {
            final long cardinality = getCardinality();
            if (firstPos >= cardinality) {
                return null;
            }
            final long arrLast = cardinality - 1;
            if (lastPos >= arrLast) {
                if (firstPos == 0) {
                    return cowRef();
                }
                effectiveLastPos = arrLast;
            } else {
                effectiveLastPos = lastPos;
            }
        } else {
            effectiveLastPos = lastPos;
        }
        final int startIdx;
        final long cardBeforeStart;
        final MutableLong prevCardMu;
        if (acc != null) {
            prevCardMu = null;
            startIdx = getIndexForRankWithAcc(0, firstPos);
            cardBeforeStart = cardinalityBeforeWithAcc(startIdx);
        } else {
            prevCardMu = new MutableLong(0);
            startIdx = getIndexForRankNoAcc(0, firstPos, prevCardMu);
            if (startIdx == size) {
                return null;
            }
            cardBeforeStart = prevCardMu.longValue();
        }
        final int endIdx;
        final long endOffset;
        if (acc != null) {
            endIdx = getIndexForRankWithAcc(startIdx, effectiveLastPos);
            final long cardBeforeEnd = cardinalityBeforeWithAcc(endIdx);
            endOffset = effectiveLastPos - cardBeforeEnd;
        } else {
            final int ansIdx = getIndexForRankNoAcc(startIdx, effectiveLastPos, prevCardMu);
            if (ansIdx == size) {
                endIdx = size - 1;
                endOffset = getSpanCardinalityAtIndex(endIdx) - 1;
            } else {
                endIdx = ansIdx;
                final long cardBeforeEnd = prevCardMu.longValue();
                endOffset = effectiveLastPos - cardBeforeEnd;
            }
        }
        final long startOffset = firstPos - cardBeforeStart;
        return make(this, startIdx, startOffset, endIdx, endOffset);
    }

    // rsEnd is inclusive.
    static private void setToRangeOfOnesMinusRangeForKey(
            ArraysBuf buf,
            final long kHigh,
            final long rsStart, final long rsEnd) {
        final int crsStart = (int) (rsStart - kHigh);
        final int crsEnd = (int) (rsEnd - kHigh);
        if (crsStart > 0) {
            if (crsEnd < BLOCK_LAST) {
                buf.pushContainer(kHigh, Container.twoRanges(0, crsStart, crsEnd + 1, BLOCK_SIZE));
                return;
            }
            if (crsStart == 1) {
                buf.pushSingletonSpan(kHigh);
                return;
            }
            buf.pushContainer(kHigh, Container.singleRange(0, crsStart));
            return;
        }
        if (debug && crsEnd >= BLOCK_LAST) {
            throw new IllegalStateException("crsEnd=" + crsEnd);
        }
        if (crsEnd + 1 == BLOCK_LAST) {
            buf.pushSingletonSpan(kHigh | BLOCK_LAST);
            return;
        }
        buf.pushContainer(kHigh, Container.rangeOfOnes(crsEnd + 1, BLOCK_SIZE));
    }

    /**
     * requirement: (start, end) should intersect span at index i.
     * 
     * @param i span index.
     * @param spanInfo spanInfo for span i
     * @param key block key for span i
     * @param start start of range to remove (may be outside of span)
     * @param end end of range to remove (may be outside of span)
     * @param madeNullSpansMu where to store the indices of spans that were made null because they ended up empty; these
     *        should be collected later by the caller.
     * @return if >= 0, the index of the last span where the removal effectively happened. if < 0, ~index for the span
     *         where to continue the removals, after a span was effectively eliminated.
     */
    private int removeRangeInSpan(final int i, final long spanInfo, final long key, final long start, final long end,
            final MutableObject<SortedRanges> madeNullSpansMu,
            final WorkData wd) {
        final Object span = spans[i];
        try (SpanView view = wd.borrowSpanView(this, i, spanInfo, span)) {
            final long flen = view.getFullBlockSpanLen();
            if (flen > 0) {
                final long sLastPlusOne = key + BLOCK_SIZE * flen;
                final long rsStart = uMax(key, start);
                if (uGreaterOrEqual(rsStart, sLastPlusOne)) {
                    return i;
                }
                final long rsEnd = uMin(sLastPlusOne - 1, end);
                final long kStart = highBits(rsStart);
                final long kEnd = highBits(rsEnd);
                final ArraysBuf buf = wd.getArraysBuf(4);
                int returnValue = ~i;
                final long preflen = distanceInBlocks(key, kStart);
                if (preflen > 0) {
                    buf.pushFullBlockSpan(key, preflen);
                }
                if (kStart == kEnd) {
                    if (rsEnd - rsStart < BLOCK_LAST) {
                        setToRangeOfOnesMinusRangeForKey(buf, kStart, rsStart, rsEnd);
                        returnValue = i + buf.size - 1;
                    }
                } else {
                    final long c1End = Math.min(kEnd, nextKey(kStart)) - 1;
                    if (rsStart != kStart || c1End - rsStart < BLOCK_LAST) {
                        setToRangeOfOnesMinusRangeForKey(buf, kStart, rsStart, c1End);
                        returnValue = i + buf.size - 1;
                    }
                    if (rsEnd - kEnd < BLOCK_LAST) {
                        setToRangeOfOnesMinusRangeForKey(buf, kEnd, kEnd, rsEnd);
                        returnValue = i + buf.size - 1;
                    }
                }
                final long posSpanFirstKey = nextKey(kEnd);
                final long posflen = distanceInBlocks(posSpanFirstKey, sLastPlusOne);
                if (posflen > 0) {
                    buf.pushFullBlockSpan(posSpanFirstKey, posflen);
                }
                if (buf.size > 0) {
                    replaceSpanAtIndex(i, buf);
                    return returnValue;
                }
                // the full span is being removed.
                markIndexAsRemoved(madeNullSpansMu, i);
                return ~(i + 1);
            }
            // we have a single container from where to remove [start, end).
            Container result = null; // if result stays null this span should be eliminated.
            Container ourContainer = null;
            if (view.isSingletonSpan()) {
                final long v = view.getSingletonSpanValue();
                if (v < start || end < v) {
                    return i;
                }
            } else {
                final long resultStart = uMax(key, start);
                final long resultEnd = uMin(key + BLOCK_LAST, end);
                ourContainer = view.getContainer();
                final int rStart = (int) (resultStart - key);
                final int rEnd = (int) (resultEnd - key) + 1; // exclusive.
                result = ourContainer.iremove(rStart, rEnd);
                if (result.isEmpty()) {
                    result = null;
                }
            }
            if (result == null) {
                markIndexAsRemoved(madeNullSpansMu, i);
                return ~(i + 1);
            }
            if (result.isSingleElement()) {
                setSingletonSpan(i, key | result.first());
            } else {
                final Container c3 = maybeOptimize(result);
                setContainerSpan(ourContainer, i, key, c3);
            }
            return i;
        }
    }

    public void removeRangeUnsafeNoWriteCheck(final long start, final long end) {
        final WorkData wd = workDataPerThread.get();
        final MutableObject<SortedRanges> madeNullSpansMu = getWorkSortedRangesMutableObject(wd);
        removeRange(0, start, end, madeNullSpansMu, wd);
        collectRemovedIndicesIfAny(madeNullSpansMu);
    }

    private int removeRange(final int fromIdx, final long start, final long end,
            final MutableObject<SortedRanges> madeNullSpansMu,
            final WorkData wd) {
        final long startHiBits = highBits(start);
        int i = getSpanIndex(fromIdx, startHiBits);
        if (i < 0) {
            i = -i - 1;
            if (i >= size) {
                return i;
            }
        }
        final long kEnd = highBits(end);
        int last = i;
        while (i < size) {
            final long spanInfo = getSpanInfo(i);
            final long blockKey = spanInfoToKey(spanInfo);
            if (blockKey > kEnd) {
                break;
            }
            i = removeRangeInSpan(i, spanInfo, blockKey, start, end, madeNullSpansMu, wd);
            if (i >= 0) {
                last = i;
                ++i;
            } else {
                last = i = ~i;
            }
        }
        return last;
    }

    public void removeRangesUnsafeNoWriteCheck(final Index.RangeIterator rit) {
        try {
            final WorkData wd = workDataPerThread.get();
            final MutableObject<SortedRanges> madeNullSpansMu = getWorkSortedRangesMutableObject(wd);
            int i = 0;
            while (rit.hasNext()) {
                rit.next();
                final long start = rit.currentRangeStart();
                final long end = rit.currentRangeEnd();
                i = removeRange(i, start, end, madeNullSpansMu, wd);
                if (i >= size) {
                    break;
                }
            }
            collectRemovedIndicesIfAny(madeNullSpansMu);
        } finally {
            rit.close();
        }
    }

    // Neither this nor other can be empty.
    // shiftAmount should be a multiple of BLOCK_SIZE.
    boolean tryAppendShiftedUnsafeNoWriteCheck(final long shiftAmount, final RspArray other, final boolean acquire) {
        if (RspArray.debug) {
            if (size == 0 || other.size == 0) {
                throw new IllegalArgumentException(
                        "Append called for empty argument: size=" + size + ", other.size=" + other.size);
            }
        }
        final long otherFirstSpanInfo = other.spanInfos[0];
        final long firstOtherBlockKey = spanInfoToKey(otherFirstSpanInfo) + shiftAmount;
        final long ourLastSpanInfo = spanInfos[size - 1];
        final Object ourLastSpan = spans[size - 1];
        final long ourflen = getFullBlockSpanLen(ourLastSpanInfo, ourLastSpan);
        final long ourLastKey = spanInfoToKey(ourLastSpanInfo);
        final long ourLastBlockKey = ourLastKey + ((ourflen == 0) ? 0 : (ourflen - 1) * BLOCK_SIZE);
        if (firstOtherBlockKey <= ourLastBlockKey) {
            return false;
        }
        int firstOtherSpan = 0;
        if (ourLastBlockKey + BLOCK_SIZE == firstOtherBlockKey) {
            if (ourflen > 0) {
                final Object otherFirstSpan = other.spans[0];
                final long otherflen = getFullBlockSpanLen(otherFirstSpanInfo, otherFirstSpan);
                if (otherflen > 0) {
                    // we need to merge these spans.
                    setFullBlockSpan(size - 1, ourLastKey, ourflen + otherflen);
                    firstOtherSpan = 1;
                }
            }

        }
        ensureSizeCanGrowBy(other.size - firstOtherSpan);
        if (!acquire) {
            for (int i = firstOtherSpan; i < other.size; ++i) {
                final int pos = size + i - firstOtherSpan;
                copyKeyAndSpanMaybeSharing(shiftAmount, other, i, spanInfos, spans, pos, !acquire);
            }
        }
        size += other.size - firstOtherSpan;
        // leave acc alone.
        return true;
    }

    public static long uMax(final long k1, final long k2) {
        return uGreater(k1, k2) ? k1 : k2;
    }

    public static long uMin(final long k1, final long k2) {
        return uLess(k1, k2) ? k1 : k2;
    }

    public static boolean uLess(final long k1, final long k2) {
        return Long.compareUnsigned(k1, k2) < 0;
    }

    public static boolean uLessOrEqual(final long k1, final long k2) {
        return Long.compareUnsigned(k1, k2) <= 0;
    }

    public static boolean uGreater(final long k1, final long k2) {
        return Long.compareUnsigned(k1, k2) > 0;
    }

    public static boolean uGreaterOrEqual(final long k1, final long k2) {
        return Long.compareUnsigned(k1, k2) >= 0;
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder(valuesToString());
        sb.append(" -- RspArray{");
        boolean first = true;
        for (int i = 0; i < size; ++i) {
            if (!first) {
                sb.append(" ,");
            }
            sb.append("[ ");
            final long sInfo = spanInfos[i];
            final long k = spanInfoToKey(sInfo);
            sb.append(String.format("0x%X", k >> 16));
            sb.append(", ");
            final Object s = spans[i];
            if (isSingletonSpan(s)) {
                sb.append('n');
            } else {
                final long flen = getFullBlockSpanLen(sInfo, s);
                sb.append(flen);
            }
            sb.append(" ]");
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    private static boolean debugValidateEnabled = false;

    void ifDebugValidateNoAssert() {
        if (debugValidateEnabled && debug) {
            validate("", false, false);
        }
    }

    void ifDebugValidate() {
        if (debugValidateEnabled && debug) {
            validate("", true, false);
        }
    }

    boolean validate() {
        return validate("", false, false);
    }

    void validate(final String s) {
        validate(s, true, false);
    }

    boolean validate(final String strArg, final boolean doAssert, final boolean isUnsafe) {
        final String str = strArg == null ? "" : strArg;
        boolean firstTime = true;
        long lastSpanLastBlockKey = 0;
        boolean lastSpanWasFullBlock = false;
        final int refCount = refCount();
        if (refCount <= 0) {
            final String m = str + ": invalid refCount=" + refCount;
            if (doAssert) {
                Assert.assertion(false, m);
            }
            return false;
        }
        if (cardData < -1 || (acc != null && cardData > size - 1)) {
            final String m = str + ": invalid cardData=" + cardData + " (size=" + size + ")";
            if (doAssert) {
                Assert.assertion(false, m);
            }
            return false;
        }
        final WorkData wd = workDataPerThread.get();
        for (int i = 0; i < size; ++i) {
            try (SpanView view = wd.borrowSpanView(this, i)) {
                final long sInfo = view.getSpanInfo();
                final long k = view.getKey();
                if (k < 0) {
                    if (doAssert) {
                        final String m = str + ": i=" + i + ", k=" + k;
                        Assert.assertion(false, m);
                    }
                    return false;
                }
                final Object s = spans[i];
                if (s != null && s != FULL_BLOCK_SPAN_MARKER && !(s instanceof short[]) && (sInfo & BLOCK_LAST) != 0) {
                    if (doAssert) {
                        final String m = str + ": lower 16 bits of spanInfo non-zero i=" + i + ", sInfo=" + sInfo;
                        Assert.assertion(false, m);
                    }
                    return false;
                }
                if (!firstTime) {
                    if (!uLess(lastSpanLastBlockKey, k)) {
                        if (doAssert) {
                            final String m = str + ": non-increasing key found i=" + i + ", k=" + k +
                                    ", lastSpanLastBlockKey=" + lastSpanLastBlockKey + ", size=" + size;
                            Assert.assertion(false, m);
                        }
                        return false;
                    }
                }
                final long flen = view.getFullBlockSpanLen();
                if (flen > 0) {
                    if (lastSpanWasFullBlock && k - lastSpanLastBlockKey <= BLOCK_SIZE) {
                        if (doAssert) {
                            final String m = str + ": consecutive full block spans found i=" + i + ", size=" + size;
                            Assert.assertion(false, m);
                        }
                        return false;
                    }
                    lastSpanWasFullBlock = true;
                } else {
                    if (s != null) {
                        if (!(s instanceof Container || s instanceof short[])) {
                            if (doAssert) {
                                final String m =
                                        str + ": can't cast s=" + s + " of class " + s.getClass().getSimpleName() +
                                                " to Container or short[] when !(flen > 0).";
                                Assert.assertion(false, m);
                            }
                            return false;
                        }
                        final Container c = view.getContainer();
                        if (c.isEmpty()) {
                            if (doAssert) {
                                final String m = str + ": empty RB container found i=" + i + ", size=" + size;
                                Assert.assertion(false, m);
                            }
                            return false;
                        }
                        if (c.isAllOnes()) {
                            if (doAssert) {
                                final String m = str + ": full RB container found i=" + i + ", size=" + size;
                                Assert.assertion(false, m);
                            }
                            return false;
                        }
                        if (c.isSingleElement()) {
                            if (doAssert) {
                                final String m = str + ": singleton container found i=" + i + ", type="
                                        + c.getClass().getSimpleName();
                                Assert.assertion(false, m);
                            }
                            return false;
                        }
                    }
                    lastSpanWasFullBlock = false;
                }
                if (acc != null) {
                    final int lastIndexForAccValidation = (isUnsafe) ? cardData : size - 1;
                    if (i <= lastIndexForAccValidation) {
                        final long prevCard = (i == 0) ? 0 : acc[i - 1];
                        final long dCard = acc[i] - prevCard;
                        final long c = getSpanCardinalityAtIndex(i);
                        if (dCard != c) {
                            final String m = str + ": acc cardinality mismatch, isUnsafe=" + isUnsafe + " at i=" + i
                                    + ", prevCard=" + prevCard + ", dCard=" + dCard + ", c=" + c + ", size=" + size;
                            Assert.assertion(false, m);
                        }
                    }
                }
                lastSpanLastBlockKey = getKeyForLastBlockInSpan(k, flen);
                firstTime = false;
            }
        }
        if (acc == null && cardData >= 0) {
            final long cardinality = calculateCardinality();
            if (cardinality != (long) cardData) {
                final String m = str + ": acc == null && cardData (=" + cardData +
                        ") != cardinality (=" + cardinality + ")";
                Assert.assertion(false, m);
            }

        }
        return true;
    }

    public OrderedKeys getOrderedKeysByPosition(final long startPositionInclusive, final long length) {
        if (startPositionInclusive < 0) {
            throw new IllegalArgumentException(
                    ("startPositionInclusive=" + startPositionInclusive + " should be >=0."));
        }
        final long endPositionInclusive;
        if (isCardinalityCached()) {
            final long cardinality = getCardinality();
            if (startPositionInclusive >= cardinality) {
                return OrderedKeys.EMPTY;
            }
            endPositionInclusive = Math.min(startPositionInclusive + length, cardinality) - 1;
        } else {
            endPositionInclusive = startPositionInclusive + length;
        }
        final MutableLong prevCardMu;
        final int startIdx;
        final long cardBeforeStart;
        if (acc != null) {
            prevCardMu = null;
            startIdx = getIndexForRankWithAcc(0, startPositionInclusive);
            cardBeforeStart = cardinalityBeforeWithAcc(startIdx);
        } else {
            prevCardMu = new MutableLong(0);
            startIdx = getIndexForRankNoAcc(0, startPositionInclusive, prevCardMu);
            if (startIdx == size) {
                return OrderedKeys.EMPTY;
            }
            cardBeforeStart = prevCardMu.longValue();
        }
        final int endIdx;
        final long endOffset;
        final long cardBeforeEnd;
        if (acc != null) {
            endIdx = getIndexForRankWithAcc(startIdx, endPositionInclusive);
            cardBeforeEnd = cardinalityBeforeWithAcc(endIdx);
            endOffset = endPositionInclusive - cardBeforeEnd;
        } else {
            final int ansIdx = getIndexForRankNoAcc(startIdx, endPositionInclusive, prevCardMu);
            cardBeforeEnd = prevCardMu.longValue();
            if (ansIdx == size) {
                endIdx = size - 1;
                endOffset = getSpanCardinalityAtIndex(endIdx) - 1;
            } else {
                endIdx = ansIdx;
                endOffset = endPositionInclusive - cardBeforeEnd;
            }
        }
        final long startOffset = startPositionInclusive - cardBeforeStart;
        return new RspOrderedKeys(this, startIdx, startOffset, cardBeforeStart, endIdx, endOffset, cardBeforeEnd);
    }

    public OrderedKeys getOrderedKeysByKeyRange(final long startValueInclusive, final long endValueInclusive) {
        if (isEmpty() || endValueInclusive < startValueInclusive) {
            return OrderedKeys.EMPTY;
        }
        final long lastSpanCardinality = getSpanCardinalityAtIndexMaybeAcc(size - 1);
        return getOrderedKeysByKeyRangeConstrainedToIndexAndOffsetRange(startValueInclusive, endValueInclusive,
                0, 0, 0, size - 1, lastSpanCardinality - 1);
    }

    public RspOrderedKeys asOrderedKeys() {
        if (isEmpty()) {
            throw new IllegalStateException("Cannot convert to ordered keys an empty array");
        }
        final long lastSpanCard = getSpanCardinalityAtIndexMaybeAcc(size - 1);
        return new RspOrderedKeys(this,
                0, 0, 0,
                size - 1, lastSpanCard - 1, getCardinality() - lastSpanCard);
    }

    // endIdx and endOffsetIn are inclusive.
    OrderedKeys getOrderedKeysByKeyRangeConstrainedToIndexAndOffsetRange(
            final long startValue, final long endValue,
            final int startIdx, final long startOffsetIn, final long cardBeforeStartIdx,
            final int endIdx, final long endOffsetIn) {
        final long startKey = highBits(startValue);
        int startKeyIdx = getSpanIndex(startIdx, startKey);
        if (startKeyIdx < 0) {
            startKeyIdx = -startKeyIdx - 1;
            if (startKeyIdx >= size) {
                return OrderedKeys.EMPTY;
            }
        }
        final long endKey = highBits(endValue);
        int endKeyIdx = getSpanIndex(startKeyIdx, endKey);
        final boolean endKeyIdxWasNegative = endKeyIdx < 0;
        if (endKeyIdxWasNegative) {
            // endIdx can't be -1, otherwise we would have returned above.
            endKeyIdx = -endKeyIdx - 2;
        }
        final BeforeCardContext beforeCardCtx = (acc == null)
                ? new BeforeCardContext(startIdx, cardBeforeStartIdx)
                : null;
        long cardBeforeStartKeyIdx = cardinalityBeforeMaybeAcc(startKeyIdx, beforeCardCtx);
        long absoluteStartPos = findInSpan(startKeyIdx, startValue, cardBeforeStartKeyIdx);
        if (absoluteStartPos < 0) {
            // the following result can't be outside of valid pos space or we would have returned above.
            absoluteStartPos = -absoluteStartPos - 1;
            if (absoluteStartPos == getCardinality()) {
                return OrderedKeys.EMPTY;
            }
        }
        final long cardBeforeEndKeyIdx = cardinalityBeforeMaybeAcc(endKeyIdx, beforeCardCtx);
        long absoluteEndPos;
        if (endKeyIdxWasNegative) {
            absoluteEndPos = cardBeforeEndKeyIdx + getSpanCardinalityAtIndexMaybeAcc(endKeyIdx) - 1;
        } else {
            absoluteEndPos = findInSpan(endKeyIdx, endValue, cardBeforeEndKeyIdx);
            if (absoluteEndPos < 0) {
                absoluteEndPos = -absoluteEndPos - 2;
                if (absoluteEndPos < 0) {
                    return OrderedKeys.EMPTY;
                }
                final long totalCardAtEndKeyIdx = cardBeforeEndKeyIdx + getSpanCardinalityAtIndexMaybeAcc(endKeyIdx);
                final long lastValidPos = totalCardAtEndKeyIdx - 1;
                if (absoluteEndPos > lastValidPos) {
                    absoluteEndPos = lastValidPos;
                }
            }
        }
        long relativeStartOffset = absoluteStartPos - cardBeforeStartKeyIdx;
        final long spanCardAtStartKeyIdx = getSpanCardinalityAtIndexMaybeAcc(startKeyIdx);
        if (relativeStartOffset >= spanCardAtStartKeyIdx) {
            ++startKeyIdx;
            cardBeforeStartKeyIdx += spanCardAtStartKeyIdx;
            relativeStartOffset = 0;
        }
        final long startOffsetOut;
        if (startKeyIdx > startIdx || relativeStartOffset >= startOffsetIn) {
            startOffsetOut = relativeStartOffset;
        } else {
            startKeyIdx = startIdx;
            cardBeforeStartKeyIdx = cardBeforeStartIdx;
            startOffsetOut = startOffsetIn;
        }
        final long relativeEndOffset = absoluteEndPos - cardBeforeEndKeyIdx;
        final long endOffsetOut;
        if (endKeyIdx < endIdx || relativeEndOffset <= endOffsetIn) {
            endOffsetOut = relativeEndOffset;
        } else {
            endOffsetOut = endOffsetIn;
        }
        return new RspOrderedKeys(this,
                startKeyIdx, startOffsetOut, cardBeforeStartKeyIdx,
                endKeyIdx, endOffsetOut, cardBeforeEndKeyIdx);
    }

    public OrderedKeys.Iterator getOrderedKeysIterator() {
        if (isEmpty()) {
            return OrderedKeys.Iterator.EMPTY;
        }
        return new RspOrderedKeys.Iterator(asOrderedKeys());
    }

    public long getAverageRunLengthEstimate() {
        if (isEmpty()) {
            return 1;
        }
        return getAverageRunLengthEstimate(0, size - 1);
    }

    public long getAverageRunLengthEstimate(final int startIdx, final int endIdx) {
        if (acc != null) {
            final long sz = acc[endIdx] - cardinalityBeforeWithAcc(startIdx);
            final long nRanges = rangesCountUpperBound(startIdx, endIdx);
            if (sz < nRanges) {
                return 1;
            }
            return sz / nRanges;
        }
        if (cardData >= 0 && cardData <= 32) {
            // don't bother.
            return 1;
        }
        long card = 0;
        long nRanges = 0;
        for (int i = startIdx; i <= endIdx; ++i) {
            final Object span = spans[i];
            if (isSingletonSpan(span)) {
                ++card;
                ++nRanges;
                continue;
            }
            final long spanInfo = spanInfos[i];
            final long flen = getFullBlockSpanLen(spanInfo, span);
            if (flen > 0) {
                card += flen * BLOCK_SIZE;
                ++nRanges;
            } else if (span instanceof short[]) {
                // expensive numberOfRanges implementation; estimate.
                final long containerCard = spanInfo & SPANINFO_ARRAYCONTAINER_CARDINALITY_BITMASK;
                card += containerCard;
                nRanges += containerCard;
            } else if (span instanceof ArrayContainer) {
                // expensive numberOfRanges implementation; estimate.
                final ArrayContainer container = (ArrayContainer) span;
                final int containerCard = container.getCardinality();
                card += containerCard;
                nRanges += containerCard;
            } else if (span instanceof BitmapContainer) {
                // expensive numberOfRanges implementation; estimate.
                final BitmapContainer bc = (BitmapContainer) span;
                final int containerCard = bc.getCardinality();
                card += containerCard;
                nRanges += Math.max(1, containerCard / 3); // estimate.
            } else {
                // use numberOfRanges implementation.
                final Container c = (Container) span;
                card += c.getCardinality();
                nRanges += c.numberOfRanges();
            }
        }
        final double estimate = card / (double) nRanges;
        if (estimate < 1.0) {
            return 1;
        }
        return Math.round(estimate);
    }

    public long rangesCountUpperBound() {
        if (isEmpty()) {
            return 0;
        }
        return rangesCountUpperBound(0, size - 1);
    }

    public long rangesCountUpperBound(final int startIdx, final int endIdx) {
        long nRanges = 0;
        for (int idx = startIdx; idx <= endIdx; ++idx) {
            final Object s = spans[idx];
            final long sInfo = spanInfos[idx];
            final long slen = RspArray.getFullBlockSpanLen(sInfo, s);
            if (slen > 0) {
                ++nRanges;
                continue;
            }
            if (isSingletonSpan(s)) {
                ++nRanges;
            } else if (s instanceof io.deephaven.db.v2.utils.rsp.container.RunContainer) {
                nRanges += ((io.deephaven.db.v2.utils.rsp.container.RunContainer) s).numberOfRanges();
            } else if (s instanceof io.deephaven.db.v2.utils.rsp.container.SingleRangeContainer) {
                nRanges += 1;
            } else if (s instanceof io.deephaven.db.v2.utils.rsp.container.TwoValuesContainer) {
                nRanges += 2;
            } else if (s instanceof short[]) {
                final long card = sInfo & SPANINFO_ARRAYCONTAINER_CARDINALITY_BITMASK;
                nRanges += card;
            } else { // Bitmap or Array Container.
                final long card = ((Container) s).getCardinality();
                nRanges += card;
            }
        }
        return nRanges;
    }

    public String valuesToString() {
        final StringBuilder sb = new StringBuilder(Integer.toString(refCount()));
        sb.append(" { ");
        if (isEmpty()) {
            sb.append("}");
            return sb.toString();
        }
        boolean first = true;
        int range = 0;
        final int maxRanges = 500;
        try (final RspRangeIterator rit = getRangeIterator()) {
            while (rit.hasNext() && range < maxRanges) {
                rit.next();
                if (!first) {
                    sb.append(",");
                }
                final long s = rit.start();
                final long e = rit.end();
                if (s == e) {
                    sb.append(s);
                } else {
                    sb.append(rit.start()).append("-").append(rit.end());
                }
                first = false;
                ++range;
            }
            if (rit.hasNext()) {
                sb.append(" ...");
            }
        }
        sb.append(" }");
        return sb.toString();
    }

    public double containerOverhead() {
        if (size <= 0) {
            return 0.0;
        }
        long used = 0;
        long allocated = 0;
        for (int i = 0; i < size; ++i) {
            final Object o = spans[i];
            if (o instanceof short[]) {
                used += spanInfos[i] & SPANINFO_ARRAYCONTAINER_CARDINALITY_BITMASK;
                allocated += ((short[]) o).length;
                continue;
            }
            if (!(o instanceof Container)) {
                continue;
            }
            final Container c = (Container) o;
            used += c.bytesUsed();
            allocated += c.bytesAllocated();
        }
        return (allocated - used) / (double) allocated;
    }

    public void sampleMetrics(
            final LongConsumer rspParallelArraysSizeUsed,
            final LongConsumer rspParallelArraysSizeUnused,
            final LongConsumer arrayContainersBytesAllocated,
            final LongConsumer arrayContainersBytesUnused,
            final LongConsumer arrayContainersCardinality,
            final LongConsumer arrayContainersCount,
            final LongConsumer bitmapContainersBytesAllocated,
            final LongConsumer bitmapContainersBytesUnused,
            final LongConsumer bitmapContainersCardinality,
            final LongConsumer bitmapContainersCount,
            final LongConsumer runContainersBytesAllocated,
            final LongConsumer runContainersBytesUnused,
            final LongConsumer runContainersCardinality,
            final LongConsumer runContainersCount,
            final LongConsumer runContainersRunsCount,
            final LongConsumer singleRangeContainersCount,
            final LongConsumer singleRangeContainerCardinality,
            final LongConsumer singletonContainersCount,
            final LongConsumer twoValuesContainerCount) {
        rspParallelArraysSizeUsed.accept(size);
        rspParallelArraysSizeUnused.accept(spanInfos.length - size);
        // TODO: It would be much more efficient to accumulate multiple samples (perhaps one array of them per Metric),
        // and then provide them to the metric in one call, to prevent multiple volatile assignments.
        for (int i = 0; i < size; ++i) {
            final Object o = spans[i];
            if (isSingletonSpan(o)) {
                singletonContainersCount.accept(1);
                continue;
            }
            if (o instanceof short[]) {
                arrayContainersCount.accept(1);
                final long card = spanInfos[i] & SPANINFO_ARRAYCONTAINER_CARDINALITY_BITMASK;
                arrayContainersCardinality.accept(card);
                final long allocated = ((short[]) o).length;
                arrayContainersBytesAllocated.accept(allocated);
                arrayContainersBytesUnused.accept(allocated - card);
                continue;
            }
            if (!(o instanceof Container)) {
                continue;
            }
            final Container c = (Container) o;
            if (c instanceof ArrayContainer) {
                arrayContainersCount.accept(1);
                arrayContainersCardinality.accept(c.getCardinality());
                final long allocated = c.bytesAllocated();
                arrayContainersBytesAllocated.accept(allocated);
                arrayContainersBytesUnused.accept(allocated - c.bytesUsed());
            } else if (c instanceof RunContainer) {
                runContainersCount.accept(1);
                runContainersCardinality.accept(c.getCardinality());
                final long allocated = c.bytesAllocated();
                runContainersBytesAllocated.accept(allocated);
                runContainersBytesUnused.accept(allocated - c.bytesUsed());
                runContainersRunsCount.accept(c.numberOfRanges());
            } else if (c instanceof BitmapContainer) {
                bitmapContainersCount.accept(1);
                bitmapContainersCardinality.accept(c.getCardinality());
                final long allocated = c.bytesAllocated();
                bitmapContainersBytesAllocated.accept(allocated);
                bitmapContainersBytesUnused.accept(allocated - c.bytesUsed());
            } else if (c instanceof SingleRangeContainer) {
                singleRangeContainersCount.accept(1);
                singleRangeContainerCardinality.accept(c.getCardinality());
            } else if (c instanceof TwoValuesContainer) {
                twoValuesContainerCount.accept(1);
            } else {
                throw new IllegalStateException("unknown Container subtype");
            }
        }
    }

    // Returns null if we can't compact.
    protected final TreeIndexImpl tryCompact() {
        if (size == 0) {
            return TreeIndexImpl.EMPTY;
        }
        final long card = getCardinality();
        final long last = lastValue();
        final long first = firstValue();
        if (size == 1) {
            final long range = last - first;
            if (range == card - 1) {
                return SingleRange.make(firstValue(), lastValue());
            }
            // if we have a single container, is likely
            // that we would fit, but not guaranteed.
        } else if (card > SortedRanges.LONG_DENSE_MAX_CAPACITY) {
            return null;
        }
        SortedRanges sr = SortedRanges.tryMakeForKnownRangeUnknownMaxCapacity(SortedRanges.LONG_DENSE_MAX_CAPACITY,
                first, last, true);
        try (RspRangeIterator it = getRangeIterator()) {
            while (it.hasNext()) {
                it.next();
                sr = sr.appendRange(it.start(), it.end());
                if (sr == null) {
                    return null;
                }
            }
        }
        if (sr != null) {
            sr = sr.tryCompactUnsafe(0);
        }
        return sr;
    }
}
