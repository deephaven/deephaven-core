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

import java.util.PrimitiveIterator;
import java.util.function.IntToLongFunction;
import java.util.function.LongConsumer;

import static io.deephaven.db.v2.utils.IndexUtilities.Comparator;

/**
 * A set representation for long values using Regular Space Partitioning (RSP) of the long space
 * in "blocks" of (2^16) elements.
 *
 * Modeled heavily after roaringbitmap.RoaringArray (keeping API method names and semantics as much as possible),
 * with modifications for:
 *
 * (1) Full "unsigned long" 64 bit range (as opposed to 32 bit in RoaringArray)
 * (2) Spans of all bits set ("AllSet") that can be arbitrarily big (ie, not constrained to 2^16 = RB Container size).
 *
 * The handling of unsigned values follows RB; ie, key values are compared/sorted as unsigned longs.
 *
 * Definitions:
 *
 *   * A "block" is a particular interval [n*2^16, (n+1)*2^16 - 1] of the long domain.
 *   * A "span" is a partition of the domain consisting of one or more consecutive blocks;
 *     a span is a subset of the domain represented by an interval [n*2^16, (n+m)*2^16 - 1], m >= 1.
 *   * Full blocks are blocks whose domain are fully contained in the set, ie, the set contains every
 *     possible value in the block's interval (as a bitmap, it would be "all ones").
 *   * Spans of full blocks are represented by a single "full blocks span" object (just a Long)
 *     which knows how many 2^16 ranges it has (it's "full blocks span len" ("flen")
 *     is the number of full blocks in the span).
 *   * Individual blocks that are not completely full are stored in an RB Container; their "full blocks span len"
 *     is zero.
 *
 * Notes about the implementation:
 *
 *  * Our containers support a "shared" boolean flag that is used to implement copy-on-write (COW) semantics
 *    and allow operation results to share containers in COW fashion.
 *  * As an optimization, when the corresponding element in the spans array is null, the lower
 *    16 bits of the key correspond to the single value in the block; this allows considerable memory savings
 *    for sparse sets.
 *
 */
public abstract class RspArray<T extends RspArray> extends RefCountedCow<T> {

    public static final boolean debug =
            Configuration.getInstance().getBooleanForClassWithDefault(RspArray.class, "debug", false);

    private static final int doublingAllocThreshold = Configuration.getInstance().getIntegerForClassWithDefault(
            RspArray.class,"doublingAllocThreshold", 1024);
    // minimum growth size after passing doubling alloc threshold
    private static final int linearAllocStep = Configuration.getInstance().getIntegerForClassWithDefault(
            RspArray.class,"linearAllocStep", 1024);
    // after doublingAllocThreshold, growth rate is (1 + 2^-n) with minimum step size of linearAllocStep (all rounded to nearest multiple of 1024)
    private static final int logarithmicAllocGrowthRate = Configuration.getInstance().getIntegerForClassWithDefault(
            RspArray.class,"logarithmicAllocGrowthRate", 4);
    // when size > accNullThreshold, the cardinality cache array is populated, otherwise is kept null.
    static final int accNullThreshold = Configuration.getInstance().getIntegerForClassWithDefault(
            RspArray.class,"accNullThreshold", 8);

    static {
        Assert.assertion(0 <= logarithmicAllocGrowthRate && logarithmicAllocGrowthRate < 32,
            "RspArray.logarithmicAllocGrowthRate must be >= 0 and < 32");
    }

    public static final int BLOCK_SIZE = Container.MAX_RANGE;
    public static final int BLOCK_LAST = (BLOCK_SIZE - 1);
    public static final int BITS_PER_BLOCK = Integer.numberOfTrailingZeros(BLOCK_SIZE);
    static {
        Assert.assertion(Integer.bitCount(BLOCK_SIZE) == 1,
                "RspArray.BITS_PER_BLOCK should be a power of 2.");
    }

    protected abstract T make(final RspArray src,
                                   final int startIdx, final long startOffset,
                                   final int endIdx, final long endOffset);
    protected abstract T make();

    public static long highBits(final long val) {
        return val & ~((long) BLOCK_LAST);
    }
    public static short lowBits(final long val) { return (short) (val & BLOCK_LAST); }
    public static int lowBitsAsInt(final long val) { return (int) (val & BLOCK_LAST); }

    public static long divBlockSize(final long v) {
        return v / BLOCK_SIZE;  // division by a constant power of two is optimized fine.
    }

    public static int modBlockSize(final long v) {
        // modulo by a constant power of two can't be optimized if the compiler can't tell if v is negative;
        // we know it isn't.
        return (int) (v & (long) BLOCK_LAST);
    }

    /**
     * Array of keys parallel to the spans array, mapping the same index to the corresponding key for a span.
     * If the associated span is a Container, the corresponding key is the higher 48 bits for the keys in the Container.
     * The lower 16 bits of the key in this case are zero.
     * If the associated span is a full block span (represented by a Long), the corresponding key is the first
     * key for the full blcok span.  The lower 16 bits of the key in this case are also zero.
     * If the associated span is null, the corresponding key represents the single value present in the block
     * implied by the higher 48 bits for the key.  The lower 16 bits may or may not be zero in this case.
     *
     * Kept in unsigned sorted order to enable binary search of keys.
     */
    protected long[] keys;
    // For tests.
    long[] getKeys() {
        return keys;
    }
    /**
     * Array of Spans parallel to the keys array, mapping the same index to the corresponding span for the key.
     * Each span is either (a) an RB Container (with a flen of 0), or (b) a Long M representing
     * M consecutive full blocks, or (c) null.  The null case implies there is a single element in the block implied by
     * the key.
     * The high 48 bits of a key corresponding to a span always corresponds to the key to its first
     * (potentially, single) block.  If the span is null, the lower 16 bits of the key are also meaningful
     * and indicate the single value present in the block.
     */
    protected Object[] spans;
    // For tests.
    Object[] getSpans() {
        return spans;
    }

    /**
     * How many keys we have.  Also how many spans, since they are parallel arrays.
     */
    int size;
    // For tests.
    int getSize() {
        return size;
    }

    /**
     *  Cache of accumulated cardinalities.  Parallel array to keys and spans.
     *  acc[i] == total cardinality for { span[0], span[1], ..., span[i] }.
     *  Should be updated by clients after mutating operations by calling ensureCardinalityCache,
     *  so that public methods on entry can assume
     *  it is up to date, ie maxAccIdx == size - 1 is a class invariant.
     *  Note this class own mutators do not update the cache themselves
     *  as clients can perform a series of update operations and
     *  only call ensureCardinalityCache at the end.
     *
     *  For a small number of keys, this is not created an kept null.
     */
    long[] acc;

    /**
     * If acc != null, highest index in acc that is valid, -1 if none.
     * if acc == null:
     *     * if cardinality fits in an int, the actual cardinality.
     *     * if cardinality does not fit in an int, -1.
     */
    int cardData;

    static final int InitialCapacity = 1;

    RspArray() {
        keys = new long[InitialCapacity];
        spans = new Object[InitialCapacity];
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
            if (cardinality == 1) {
                keys = new long[] { start };
                spans = new Object[] { null };
            } else {
                keys = new long[]{sHigh};
                if (cardinality == BLOCK_SIZE) {
                    spans = new Object[] { 1L };
                } else {
                    spans = new Object[] { Container.rangeOfOnes(sLow, eLow + 1) };
                }
            }
            return;
        }
        if (sLow == 0) {
            if (eLow == BLOCK_LAST) {
                keys = new long[] { sHigh };
                spans = new Object[] { distanceInBlocks(sHigh, eHigh) + 1 };
                acc = null;
                setCardDataFor(cardinality);
                size = 1;
                return;
            }
            keys = new long[2];
            keys[0] = sHigh;
            spans = new Object[2];
            setSpanAtIndex(spans, 0, distanceInBlocks(sHigh, eHigh));
            if (eLow == 0) {
                keys[1] = end;
                spans[1] = null;
            } else {
                keys[1] = eHigh;
                spans[1] = Container.rangeOfOnes(0, eLow + 1);
            }
            acc = null;
            setCardDataFor(cardinality);
            size = 2;
            return;
        }
        if (eLow == BLOCK_LAST) {
            keys = new long[2];
            keys[1] = nextKey(sHigh);
            spans = new Object[2];
            setSpanAtIndex(spans, 1, distanceInBlocks(sHigh, eHigh));
            if (sLow == BLOCK_LAST) {
                keys[0] = start;
                spans[0] = null;
            } else {
                keys[0] = sHigh;
                spans[0] = Container.rangeOfOnes(sLow, BLOCK_SIZE);
            }
            acc = null;
            setCardDataFor(cardinality);
            size = 2;
            return;
        }
        final long nextKey = nextKey(sHigh);
        final long midflen = distanceInBlocks(nextKey, eHigh);
        if (midflen == 0) {
            keys = new long[2];
            spans = new Object[2];
            if (sLow == BLOCK_LAST) {
                keys[0] = start;
                spans[0] = null;
            } else {
                keys[0] = sHigh;
                spans[0] = Container.rangeOfOnes(sLow, BLOCK_SIZE);
            }
            if (eLow == 0) {
                keys[1] = end;
                spans[1] = null;
            } else {
                keys[1] = eHigh;
                spans[1] = Container.rangeOfOnes(0, eLow + 1);
            }
            acc = null;
            setCardDataFor(cardinality);
            size = 2;
            return;
        }
        keys = new long[3];
        spans = new Object[3];
        if (sLow == BLOCK_LAST) {
            keys[0] = start;
            spans[0] = null;
        } else {
            keys[0] = sHigh;
            spans[0] = Container.rangeOfOnes(sLow, BLOCK_SIZE);
        }
        keys[1] = nextKey;
        spans[1] = midflen;
        if (eLow == 0) {
            keys[2] = end;
            spans[2] = null;
        } else {
            keys[2] = eHigh;
            spans[2] = Container.rangeOfOnes(0, eLow + 1);
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
        if (c != null) {
            c.setCopyOnWrite();
        }
        return c;
    }

    protected RspArray(final RspArray other) {
        copyFrom(other, 0);
    }

    private void copyFrom(final RspArray other, final long shiftAmount) {
        final int newSize =
                (other.size >= 2*InitialCapacity && other.size < other.keys.length / 2)
                        ? other.keys.length / 2
                        : other.keys.length;
        keys = new long[newSize];
        if (shiftAmount == 0) {
            System.arraycopy(other.keys, 0, keys, 0, other.size);
        } else {
            for (int i = 0; i < other.size; ++i) {
                keys[i] = other.keys[i] + shiftAmount;
            }
        }
        spans = copySpans(newSize, other.spans, other.size);
        if (other.acc != null) {
            acc = new long[newSize];
            System.arraycopy(other.acc, 0, acc, 0, other.size);
        } else {
            acc = null;
        }
        cardData = other.cardData;
        size = other.size;
    }

    static Object copySpan(final Object span) {
        if (span instanceof Container) {
            final Container c = (Container) span;
            c.setCopyOnWrite();
        }
        return span;
    }

    static Object[] copySpans(final int newSz, final Object[] other, final int otherSz) {
        final Object[] spansCopy = new Object[newSz];
        for (int i = 0; i < otherSz; ++i) {
            spansCopy[i] = copySpan(other[i]);
        }
        return spansCopy;
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
        final long startIdxKey = highBits(src.keys[startIdx]);
        final long flenFirstSpan = getFullBlockSpanLen(firstSpan);
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
                    keys = new long[1];
                    if (resultingCardFromFirstSpan == BLOCK_SIZE) {
                        keys[0] = keyForFirstBlock;
                        spans = new Object[]{1L};
                    } else {
                        spans = new Object[1];
                        final int containerStart = (int) (startOffset & BLOCK_LAST);
                        final int containerEndInclusive = (int) (endOffset & BLOCK_LAST);
                        if (containerStart == containerEndInclusive) {
                            keys[0] = startKey;
                            spans[0] = null;
                        } else {
                            keys[0] = keyForFirstBlock;
                            spans[0] = Container.rangeOfOnes(containerStart, containerEndInclusive + 1);
                        }
                    }
                    acc = null;
                    setCardDataFor(resultingCardFromFirstSpan);
                    ifDebugValidate();
                    return;
                }
            } else {
                resultingCardFromFirstSpan = flenFirstSpan*BLOCK_SIZE - startOffset;
            }
            int n = 0;  // how many containers we end up with after (potentially) splitting the first.
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
        boolean lastSpanIsFull = false;  // will set below to true if we find out otherwise.
        long deltaLast = 0;  // cardinality of the span(s) resulting from the split of a last full block span.
        int copyLastIdx = endIdx;
        if (endIdx > startIdx && endOffset < src.getSpanCardinalityAtIndexMaybeAcc(endIdx) - 1) {
            copyLastIdx = endIdx - 1;
            final Object lastSpan = src.spans[endIdx];
            final long flenLastSpan = getFullBlockSpanLen(lastSpan);
            if (flenLastSpan > 0) {
                lastSpanIsFull = true;
                deltaLast = endOffset + 1;
                if (deltaLast > BLOCK_SIZE) {
                    ++sz;
                }
            }
        }
        size = sz;
        keys = new long[size];
        spans = new Object[size];
        if (size > accNullThreshold) {
            acc = new long[size];
            cardData = size - 1;
        } else {
            acc = null;
        }
        int i = 0;
        long accSum = 0;
        int isrc;  // index in src from where to start copying spans.
        if (firstSpanIsFull) {
            long nextKey = keyForFirstBlock;
            if (startSplitInitialContainerCard > 0) {
                if (startSplitInitialContainerCard == 1) {
                    keys[0] = nextKey | BLOCK_LAST;
                    spans[0] = null;
                } else {
                    keys[0] = nextKey;
                    spans[0] = Container.rangeOfOnes(BLOCK_SIZE - startSplitInitialContainerCard, BLOCK_SIZE);
                }
                nextKey = nextKey(nextKey);
                accSum = startSplitInitialContainerCard;
                maybeSetAcc(0, accSum);
                i = 1;
            }
            if (startSplitIntermediateFullBlockSpanLen > 0) {
                keys[i] = nextKey;
                nextKey = highBits(nextKey + startSplitIntermediateFullBlockSpanCard);
                setSpanAtIndex(spans, i, startSplitIntermediateFullBlockSpanLen);
                accSum += startSplitIntermediateFullBlockSpanCard;
                maybeSetAcc(i, accSum);
                ++i;
            }
            if (startSplitEndingContainerCard > 0) {
                keys[i] = nextKey;
                if (startSplitEndingContainerCard == 1) {
                    spans[i] = null;
                } else {
                    spans[i] = Container.rangeOfOnes(0, startSplitEndingContainerCard);
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
                if (spanSrc == null) {
                    if (startOffset != 0) {
                        throw new IllegalArgumentException(
                                "startOffset=" + startOffset + " and span at startIdx has a single element.");
                    }
                    keys[0] = src.keys[startIdx];
                    spans[0] = null;
                    accSum = 1;
                    maybeSetAcc(0, 1);
                } else {
                    final Container csrc = (Container) src.spans[startIdx];
                    if (endIdx <= startIdx) {  // single span.
                        final int card = (int) src.getSpanCardinalityAtIndexMaybeAcc(startIdx);
                        if (endOffset + 1 < card) {
                            if (startOffset == endOffset) {
                                keys[0] = startIdxKey | unsignedShortToInt(csrc.select((int) startOffset));
                                spans[0] = null;
                            } else {
                                keys[0] = startIdxKey;
                                spans[0] = csrc.select((int) startOffset, (int) (endOffset + 1));
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
                        keys[0] = startIdxKey | unsignedShortToInt(csrc.select(startOffsetInt));
                        spans[0] = null;
                    } else {
                        keys[0] = startIdxKey;
                        spans[0] = csrc.select(startOffsetInt, card);;
                    }
                    accSum = card - startOffset;
                    maybeSetAcc(0, accSum);
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
            keys[i] = src.keys[isrc];
            spans[i] = copySpan(src.spans[isrc]);
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
        final long key = keys[i] = src.keys[isrc];
        if (lastSpanIsFull) {
            if (deltaLast >= BLOCK_SIZE) {
                final long flen = RspArray.divBlockSize(deltaLast);
                final int delta = RspArray.modBlockSize(deltaLast);
                setSpanAtIndex(spans, i, flen);
                accSum += flen*BLOCK_SIZE;
                maybeSetAcc(i, accSum);
                ++i;
                if (delta > 0) {
                    keys[i] = key + flen*BLOCK_SIZE;
                    if (delta == 1) {
                        spans[i] = null;
                    } else {
                        spans[i] = Container.rangeOfOnes(0, delta);
                    }
                    accSum += delta;
                    maybeSetAcc(i, accSum);
                }
            } else {
                final int ce = (int) deltaLast;
                if (ce == 1) {
                    spans[i] = null;
                } else {
                    spans[i] = Container.rangeOfOnes(0, ce);
                }
                accSum += deltaLast;
                maybeSetAcc(i, accSum);
            }
        } else {
            final long card = endOffset + 1;
            final Object spanSrc = src.spans[isrc];
            if (spanSrc != null) {
                final Container csrc = (Container) spanSrc;
                // This can't be the full container or we would have copied it earlier.
                if (endOffset == 0) {
                    keys[i] = key | csrc.first();
                    spans[i] = null;
                } else {
                    spans[i] = csrc.select(0, (int) card);;
                }
                accSum += card;
                maybeSetAcc(i, accSum);
            } else {
                // Can't happen; a single element span should have been copied over in its entirety earlier.
                throw new IllegalStateException("endIdx=" + endIdx + ", endOffset=" + endOffset + ", key=" + key);
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
        final long k = keys[i];
        final Object s = spans[i];
        if (s == null || getFullBlockSpanLen(s) > 0) {
            return k;
        }
        final Container c = (Container) s;
        return k | (long) c.first();
    }

    public long keyForFirstBlock() {
        return highBits(keys[0]);
    }

    public long keyForLastBlock() {
        final Object s = spans[size - 1];
        final long k = keys[size - 1];
        if (s == null) {
            return highBits(k);
        }
        if (s instanceof Long) {
            final long flen = (Long) s;
            return k + (flen - 1) * BLOCK_SIZE;
        }
        return k;
    }

    public long lastValue() {
        final long k = keys[size - 1];
        final Object s = spans[size - 1];
        if (s == null) {
            return k;
        }
        final long flen = getFullBlockSpanLen(s);
        if (flen == 0) {
            final Container c = (Container) s;
            return k | (long) c.last();
        }
        return k + BLOCK_SIZE * flen - 1;
    }

    public interface SpanCursor {
        /**
         * @return the current span's first key if the current span is valid, undefined otherwise.
         */
        long spanKey();
        /**
         * @return the current span if valid, undefined otherwise.
         */
        Object span();
        /**
         * Advances the pointer to the next span in the linear sequence.
         * If the span before the call was the last one, a subsequent call to hasNext will return false.
         */
        void next();
        /**
         *  This method should be called:
         *   * After the pointer is created and before calling any other methods;
         *     if it returns false, calling any other methods results in undefined behavior.
         *  * Right after a call to any advance method, similar to above.
         * @return true if the pointer currently points to a valid span.
         */
        boolean hasNext();

        /**
         * Advances the pointer forward to the next span in the sequence whose interval could have it include
         * the key argument.
         *
         * More specifically, the current span position is effectively advanced forward as long as the provided key
         * is bigger than the right endpoint for the current span.
         *
         * This operation is O(log(cardinality)).
         *
         * Note this may not move the pointer if the current span already satisfies the constraint, or it
         * may invalidate the pointer if the key is to the right of the last valid span.
         * Note also advance should only be called on a non-empty cursor, after having called hasNext() and next()
         * at least once.
         *
         * @param key key to search forward from the current span position.
         * @return false if the cursor is exhausted and there was no span satisfying the restriction found,
         *         true otherwise.
         */
        boolean advance(long key);

        /**
         * Releases the RspArray reference.
         */
        void release();
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
         * This operation never invalidates a valid cursor, it may only move it forward from its current position
         * but never exhaust it.
         *
         * Note also search should only be called on a non-empty cursor, after having called hasNext() and next()
         * at least once.
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
        @Override public SpanCursorForwardImpl copy() {
            return new SpanCursorForwardImpl(ra, si);
        }
        @Override public long spanKey() {
            return ra.keys[si];
        }
        @Override public Object span() {
            return ra.spans[si];
        }
        @Override public void prev() {
            --si;
        }
        @Override public boolean hasNext() {
            return si < ra.size - 1;
        }
        @Override public void next() {
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
        @Override public boolean advance(final long key) {
            final int i = ra.getSpanIndex(si, highBits(key));
            return advanceSecondHalf(i);
        }
        @Override public void search(final Comparator comp) {
            si = ra.searchSpanIndex(si, comp);
        }
        @Override public void release() {
            if (ra == null) {
                return;
            }
            ra.release();
            ra = null;
        }
    }

    public RspRangeIterator getRangeIterator()  {
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
        @Override public long spanKey() {
            return ra.keys[si];
        }
        @Override public Object span() {
            return ra.spans[si];
        }
        @Override public boolean hasNext() {
            return si > 0;
        }
        @Override public void next() {
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
        @Override public boolean advance(final long key) {
            final int i = ra.getSpanIndex(0, si, highBits(key));
            return advanceSecondHalf(i);
        }
        @Override public void release() {
            if (ra == null) {
                return;
            }
            ra.release();
            ra = null;
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
        if (minCapacity <= keys.length) {
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
        final long[] newKeys = new long[newCapacity];
        System.arraycopy(keys, 0, newKeys, 0, size);
        keys = newKeys;
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
     * @param compactFactor if k == 0, compact if count < capacity.
     *             k > 0, compact if (capacity - count > (capacity >> k).
     */
    public void tryCompactUnsafe(final int compactFactor) {
        if (compactFactor == 0) {
            if (size == keys.length) {
                return;
            }
        } else if (keys.length - size <= (keys.length >> compactFactor)) {
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

    public int keySearch(final long key) {
        return keySearch(0, key);
    }

    public int keySearch(final int startPos, final long key) {
        return keySearch(startPos, size, key);
    }

    public int keySearch(final int startPos, final int endPosExclusive, final long key) {
        final long blockKey = highBits(key);
        if (endPosExclusive == 0 || keys[endPosExclusive - 1] == blockKey) {
            return endPosExclusive - 1;
        }
        return unsignedBinarySearch(this::highBitsForKeyAtIndex, startPos, endPosExclusive, blockKey);
    }

    public static boolean isFullBlockSpan(final Object s) {
        return (s instanceof Long);
    }

    public static long getFullBlockSpanLen(final Object s) {
        if (s instanceof Long) {
            return (Long) s;
        }
        return 0;
    }
    /**
     * @return if the key is included in some existing span, returns the index of that span.
     *         if the key is not included in any existing span, returns -(p - 1) where
     *         p is the position a span for the key would be inserted.
     *
     * Note that, since a span's covered interval may include multiple blocks, a key contained by a span may be different
     * than its first key (if the span includes more than one block).
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
        final long flen = getFullBlockSpanLen(preSpan);
        if (flen == 0) {
            return i;
        }
        final long preKey = keys[preIdx];
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

    public long highBitsForKeyAtIndex(final int i) {
        return highBits(keys[i]);
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
        final Object s = spans[i];
        if (s instanceof Long) {
            return BLOCK_SIZE * (Long) s;
        }
        if (s == null) {
            return 1;
        }
        Container c = (Container) s;
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
        ref.acc = new long[ref.keys.length];
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
            acc = new long[keys.length];
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
            final Object s = spans[i];
            tflen += getFullBlockSpanLen(s);
        }
        return tflen;
    }
    public static long nextKey(final long key) {
        return key + BLOCK_SIZE;
    }

    /**
     *
     * blockKeyEnd is exclusive.  Assumption on entry: blockKeyStart <= blockKeyEnd
     *
     * @param blockKeyStart inclusive start block key (only high 48 bits set).
     * @param blockKeyEnd exclusive end block key (only high 48 bits set).
     * @return distance in blocks between blockKeyStart and blockKeyEnd
     */
    public static long distanceInBlocks(final long blockKeyStart, final long blockKeyEnd) {
        return (blockKeyEnd - blockKeyStart) >> 16;
    }

    public static void setSpanAtIndex(final Object[] spans, final int i, final long flen) {
        if (flen <= 0) {
            throw new IllegalArgumentException("i=" + i + ", flen=" + flen);
        }
        spans[i] = flen;
    }

    public void setSpanAtIndex(final int i, final long flen) {
        setSpanAtIndex(spans, i, flen);
        modifiedSpan(i);
    }

    public void setSpanAtIndex(final int i, final Object s) {
        spans[i] = s;
        modifiedSpan(i);
    }

    public void setContainerAtIndexMaybeSingle(final int i, final Container c) {
        if (c.isSingleElement()) {
            keys[i] |= c.first();
            setSpanAtIndex(i, null);
            return;
        }
        setSpanAtIndex(i, c);
    }

    public void setNotNullContainerAtIndex(final int i, final Container c) {
        if (c == null) {
            throw new IllegalStateException("Null container, i=" + i);
        }
        setSpanAtIndex(i, c);
    }

    public void setNullContainerAtIndex(final int i) {
        setSpanAtIndex(i, null);
    }

    private void arrayCopies(final int src, final int dst, final int n) {
        System.arraycopy(keys, src, keys, dst, n);
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
        if (size < 2*InitialCapacity || size > (thresholdSize = spans.length / 2)) {
            return;
        }
        realloc(thresholdSize);
    }

    /**
     * Collapse an inner range of spans, by overwriting it with a range
     * of spans from a certain later position till the end,
     * and reducing size accordingly to the number of spans removed
     * ({@code size -= isrc - idst}).
     * The resulting array will remove all spans between the original values at
     * {@code idst .. (isrc - 1)} inclusive.
     *
     * @param idst specifies the beginning position where the source range will move.
     * @param isrc specifies the source range to copy over as [isrc, size) (eg, from isrc inclusive till the end).
     *             If isrc == size no actual spans are copied, resulting in a size reduction only.
     */
    private void collapseRange(final int idst, final int isrc) {
        int newSize = size - (isrc - idst);
        int thresholdSize = 0;
        if (newSize > 2*InitialCapacity && newSize < (thresholdSize = spans.length / 2)) {
            final Object[] newSpans = new Object[thresholdSize];
            System.arraycopy(spans, 0, newSpans, 0, idst);
            System.arraycopy(spans, isrc, newSpans, idst, size - isrc);
            spans = newSpans;
            final long[] newKeys = new long[thresholdSize];
            System.arraycopy(keys, 0, newKeys, 0, idst);
            System.arraycopy(keys, isrc, newKeys, idst, size - isrc);
            keys = newKeys;
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
     * @param newSpanIdx an index, as returned by getSpanAtIndex(k).  Note this can be negative, in which case this is an
     *              insertion (existing elements pushed to the right as necessary).
     * @param newSpanKey the key.
     * @param newSpanFlen the number of 2^16 intervals.
     *
     * @return the (positive) index where the span was actually inserted.
     */
    public int setOrInsertFullBlockSpanAtIndex(final int newSpanIdx, final long newSpanKey, final long newSpanFlen,
                                               final MutableObject<SortedRanges> madeNullSpansMu) {
        final int ii;  // set or insert position.
        long newflen = newSpanFlen;  // may grow if we merge to our right.
        final int idxForFirstKeyBigger;  // first index for a key bigger than newSpanKey.
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
        int lastIdx = 0;  // Last position that will be "overridden" by this new span, inclusive.
        if (idxForFirstKeyBigger >= size) {
            lastIdx = ii;
        } else {
            final long newSpanLastKey = newSpanKey + (newSpanFlen - 1) * BLOCK_SIZE;  // New span's last key.
            final int j = getSpanIndex(idxForFirstKeyBigger, newSpanLastKey);
            final int idxForLastKeyInsideNewSpan;
            if (j >= 0) {
                idxForLastKeyInsideNewSpan = j;
            } else {
                // One before (-j-1), which is the first position whose key is > newSpanLastKey.  Note this may be -1.
                idxForLastKeyInsideNewSpan = -j - 2;
            }
            // We may need to merge with a full block span extending to the right.
            boolean rightDone = false;
            final int idxForFirstKeyOutsideNewSpan = idxForLastKeyInsideNewSpan + 1;
            if (idxForFirstKeyOutsideNewSpan < size) {
                final long rightKey = highBitsForKeyAtIndex(idxForFirstKeyOutsideNewSpan);
                if (rightKey - newSpanLastKey <= BLOCK_SIZE) {
                    final long rightLen = getFullBlockSpanLen(spans[idxForFirstKeyOutsideNewSpan]);
                    if (rightLen > 0) {
                        final long rightSpanLastKey = rightKey + (rightLen - 1) * BLOCK_SIZE;
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
                    final long len = getFullBlockSpanLen(spans[idxForLastKeyInsideNewSpan]);
                    if (len > 0) {
                        final long spanLastKey = keys[idxForLastKeyInsideNewSpan] + (len - 1) * BLOCK_SIZE;
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
            final long spanLen = getFullBlockSpanLen(span);
            if (spanLen > 0) {
                final long key = keys[ii];
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
            final long leftSpanLen = getFullBlockSpanLen(spans[leftIdx]);
            if (leftSpanLen > 0) {
                final long leftKey = keys[leftIdx];
                final long keyDistance = distanceInBlocks(leftKey, newSpanKey);
                if (leftSpanLen >= keyDistance) {
                    firstKey = leftKey;
                    firstIdx = leftIdx;
                    newflen += keyDistance;
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
            keys[firstIdx] = firstKey;
            spans[firstIdx] = newflen;
            return firstIdx;
        }
        if (madeNullSpansMu == null) {
            keys[firstIdx] = firstKey;
            spans[firstIdx] = newflen;
            collapseRange(firstIdx + 1, lastIdx + 1);
            return firstIdx;
        }
        markIndexRangeAsRemoved(madeNullSpansMu, firstIdx, lastIdx - 1);
        keys[lastIdx] = firstKey;
        spans[lastIdx] = newflen;
        return lastIdx;
    }
    /*
     * Caller needs to ensure the previous span at position (size - 1)
     * being replaced is not a full block span that would need to be broken;
     */
    public void replaceLastContainer(final Container c) {
        setNotNullContainerAtIndex(size - 1, c);
    }
    private boolean tryMergeLeftFullBlockSpan(final int idx, final long k, final long slen) {
        if (idx < 1) {
            return false;
        }
        final int leftIdx = idx - 1;
        final long leftSpanLen = getFullBlockSpanLen(spans[leftIdx]);
        if (leftSpanLen > 0) {
            final long leftKey = keys[leftIdx];
            final long keyDistance = distanceInBlocks(leftKey, k);
            if (leftSpanLen == keyDistance) {  // by construction leftSpanLen <= keyDistance.
                spans[leftIdx] = leftSpanLen + slen;
                modifiedSpan(leftIdx);
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
        keys[size - 1] = k;
        spans[size - 1] = slen;
        modifiedSpan(size - 1);
    }
    private void tryOptimizeContainer(final int i) {
        final Object o = spans[i];
        if (!(o instanceof Container)) {
            return;
        }
        final Container prevContainer = (Container) o;
        spans[i] = prevContainer.runOptimize();
    }
    public void appendContainer(final long k, final Container c) {
        if (size > 0) {
            tryOptimizeContainer(size - 1);
        }
        ensureSizeCanGrowBy(1);
        keys[size] = k;
        spans[size] = c;
        ++size;
    }
    public void appendSingle(final long k) {
        ensureSizeCanGrowBy(1);
        keys[size] = k;
        spans[size] = null;
        ++size;
    }
    public Container mergeContainer(final int i, Container c, final MutableBoolean isSpan) {
        final Container prev = (Container) spans[i];
        final Container prevOred;
        if (prev == null) {
            prevOred = c.iset(lowBits(keys[i]));
        } else {
            prevOred = prev.ior(c);
        }
        if (prevOred.isAllOnes()) {
            setOrInsertFullBlockSpanAtIndex(i, keys[i], 1, null);
            isSpan.setTrue();
            return null;
        }
        if (prevOred != prev) {
            setNotNullContainerAtIndex(i, prevOred);
            if (prev == null) {
                keys[i] = highBits(keys[i]);
            }
        } else {
            modifiedSpan(i);
        }
        return prevOred;
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
        keys[size] = k;
        setSpanAtIndex(spans, size, slen);
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

    public void insertSpanAtIndex(final int i, final long key, final Container c) {
        insertSpanAtIndexRaw(i, key, c);
    }

    public void insertSpanAtIndex(final int i, final long key, final long flen) {
        if (flen <= 0) {
            throw new IllegalArgumentException("i=" + i + ", flen=" + flen);
        }
        insertSpanAtIndexRaw(i, key, flen);
    }

    /**
     * Insert a new span at position i with key k, pushing the existing elements to the right.
     * The caller should ensure that the key order is preserved by this operation.
     *
     * @param i position in which to insert
     * @param key key for the span to be inserted
     * @param s span to be inserted
     */
    public void insertSpanAtIndexRaw(final int i, final long key, final Object s) {
        ensureSizeCanGrowBy(1);
        final int dstPos = i + 1;
        final int n = size - i;
        arrayCopies(i, dstPos, n);
        keys[i] = key;
        spans[i] = s;
        modifiedSpan(i);
        ++size;
    }

    public void removeSpanAtIndex(final int i) {
        collapseRange(i, i + 1);
        modifiedSpan(i);
    }

    /**
     * Replace the span at index i with the sequence of (key, span) provided as arguments.
     */
    public void replaceSpanAtIndex(final int i,
                                   final long k1, final Object s1,
                                   final long k2, final Object s2) {
        ensureSizeCanGrowBy(1);
        final int dstPos = i + 2;
        final int srcPos = i + 1;
        final int n = size - srcPos;
        arrayCopies(srcPos, dstPos, n);
        keys[i] = k1;
        spans[i] = s1;
        keys[i + 1] = k2;
        spans[i + 1] = s2;
        size += 1;
        modifiedSpan(i);
    }
    /**
     * Replace the span at index i with the sequence of (key, span) provided as arguments.
     */
    public void replaceSpanAtIndex(final int i,
                                   final long k1, final Object s1,
                                   final long k2, final Object s2,
                                   final long k3, final Object s3) {
        ensureSizeCanGrowBy(2);
        final int dstPos = i + 3;
        final int srcPos = i + 1;
        final int n = size - srcPos;
        arrayCopies(srcPos, dstPos, n);
        keys[i] = k1;
        spans[i] = s1;
        keys[i + 1] = k2;
        spans[i + 1] = s2;
        keys[i + 2] = k3;
        spans[i + 2] = s3;
        size += 2;
        modifiedSpan(i);
    }
    /**
     * Replace the span at index i with the sequence of n spans (key=k[i], span=ss[i])
     */
    public void replaceSpanAtIndex(final int i,
                                   final int n,
                                   final long[] ks,
                                   final Object[] ss) {
        ensureSizeCanGrowBy(n - 1);
        final int dstPos = i + n;
        final int srcPos = i + 1;
        final int count = size - srcPos;
        arrayCopies(srcPos, dstPos, count);
        for (int j = 0; j < n; ++j) {
            keys[i + j] = ks[j];
            spans[i + j] = ss[j];
        }
        size += n - 1;
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
        final Object s = spans[idx];
        if (s == null) {
            if (offset != 0) {
                throw new IllegalArgumentException("Invalid offset=" + offset + " for index=" + idx);
            }
            return keys[idx];
        }
        final long highBits = keys[idx];
        final long flen = getFullBlockSpanLen(s);
        if (flen == 0) {
            final Container c = (Container) s;
            final int sv = (int) offset;
            if (sv != offset) {
                throw new IllegalArgumentException("Invalid offset=" + offset + " for index=" + idx);
            }
            final short lowBits = c.select(sv);
            return paste(highBits, lowBits);
        }
        return highBits + offset;
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
        final Object s = spans[ki];
        if (s == null) {
            final long k = keys[ki];
            if (val < k) {
                if (ki == startIdx) {
                    return false;
                }
                out.setResult(ki, 0);
            } else if (val == k) {
                out.setResult(ki, 0);
            } else {
                out.setResult(ki + 1, 0);
            }
            return true;
        }
        final long flen = getFullBlockSpanLen(s);
        if (flen > 0) {
            final long k = keys[ki];
            out.setResult(ki, val - k);
            return true;
        }
        final Container c = (Container) s;
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
        final Object s = spans[ki];
        if (s == null) {
            final long k = keys[ki];
            if (val < k) {
                if (ki == startIdx) {
                    return false;
                }
                out.setResult(ki - 1, getSpanCardinalityAtIndexMaybeAcc(ki - 1) - 1);
            } else {
                out.setResult(ki, 0);
            }
            return true;
        }
        final long flen = getFullBlockSpanLen(s);
        if (flen > 0) {
            final long k = keys[ki];
            out.setResult(ki, val - k);
            return true;
        }
        final Container c = (Container) s;
        final int cf = c.find(lowBits(val));
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
        final Object s = spans[idx];
        if (s == null) {
            final long k = keys[idx];
            if (val == k) {
                return prevAcc;
            }
            if (val < k) {
                return ~prevAcc;
            }
            // val > k
            return ~(prevAcc + 1);
        }
        final long flen = getFullBlockSpanLen(s);
        if (flen > 0) {
            final long k = keys[idx];
            return prevAcc + val - k;

        }
        final Container c = (Container) s;
        final int cf = c.find(lowBits(val));
        if (cf >= 0) {
            return prevAcc + cf;
        }
        return -prevAcc + cf;
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
        for (int i1 = 0; i1 < r1.size; ++i1) {
            final long k1 = r1.keys[i1];
            final Object s1 = r1.spans[i1];
            final long bk1 = highBits(k1);
            final long flen1 = getFullBlockSpanLen(s1);
            final int i2 = r2.getSpanIndex(p2, bk1);
            if (i2 < 0) {
                return false;
            }
            final Object s2 = r2.spans[i2];
            final long flen2 = getFullBlockSpanLen(s2);
            if (flen1 > 0) {
                if (flen2 == 0) {
                    return false;
                }
                final long kend1 = getKeyForLastBlockInSpan(bk1, flen1);
                final long k2 = r2.highBitsForKeyAtIndex(i2);
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
            if (s1 == null) {
                if (s2 == null) {
                    final long k2 = r2.keys[i2];
                    if (k1 != k2) {
                        return false;
                    }
                } else {
                    final Container c2 = (Container) s2;
                    if (!c2.contains(lowBits(k1))) {
                        return false;
                    }
                }
            } else if (s2 == null) {
                if (debug) {
                    final Container c1 = (Container) s1;
                    if (c1.isSingleElement()) {
                        throw new IllegalStateException("Single element container at i1=" + i1);
                    }
                }
                return false;
            } else {
                final Container c1 = (Container) s1;
                final Container c2 = (Container) s2;
                if (!c1.subsetOf(c2)) {
                    return false;
                }
            }
            ++p2;
            if (p2 >= r2.size) {
                return i1 == r1.size - 1;
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
        for (int i = si; i <= ei; ++i) {
            final long ki = keys[i];
            if (ki > pendingStart) {
                return false;
            }
            final Object span = spans[i];
            if (span == null) {
                if (pendingStart != ki) {
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
            final long slen = getFullBlockSpanLen(span);
            if (slen > 0) {
                final long spanLast = ki + slen * BLOCK_SIZE - 1;
                if (spanLast >= pendingEnd) {
                    return true;
                }
                pendingStart = spanLast + 1;
                continue;
            }
            final Container c = (Container) span;
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

    /**
     * Returns true if any value in this RspArray is contained inside the range [first, last], false
     * otherwise.
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
        long key = keys[i];
        final long endHighBits = highBits(end);
        long keyBlock = highBits(key);
        if (endHighBits < keyBlock) {
            return ~i;
        }
        while (true) {
            final long sk = Math.max(start, keyBlock);
            final long ek = Math.min(end, keyBlock + BLOCK_LAST);
            if (sk == keyBlock && ek == keyBlock + BLOCK_LAST) {
                return i;
            }
            final Object span = spans[i];
            if (span == null) {
                if (start <= key && key <= end) {
                    return i;
                }
            } else {
                final long slen = getFullBlockSpanLen(span);
                if (slen > 0) {
                    return i;
                }
                final Container c = (Container) span;
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
            key = keys[i];
            keyBlock = highBits(key);
            if (endHighBits < keyBlock) {
                return ~(i - 1);
            }
        }
    }

    private static boolean overlaps(final RspArray r1, final RspArray r2) {
        if (r1.keyForLastBlock() < r2.keyForFirstBlock()
            || r2.keyForLastBlock() < r1.keyForFirstBlock()) {
            return false;
        }
        int p2 = 0;
        for (int i1 = 0; i1 < r1.size; ++i1) {
            final long k1 = r1.keys[i1];
            final Object s1 = r1.spans[i1];
            final long flen1 = getFullBlockSpanLen(s1);
            final int i2 = r2.getSpanIndex(p2, k1);
            if (i2 >= 0) {
                if (flen1 > 0) {
                    return true;
                }
                final Object s2 = r2.spans[i2];
                final long flen2 = getFullBlockSpanLen(s2);
                if (flen2 > 0) {
                    return true;
                }
                if (s1 == null) {
                    if (s2 == null) {
                        if (k1 == r2.keys[i2]) {
                            return true;
                        }
                    } else {
                        final Container c2 = (Container) s2;
                        if (c2.contains(lowBits(k1))) {
                            return true;
                        }
                    }
                } else if (s2 == null) {
                    final Container c1 = (Container) s1;
                    if (c1.contains(lowBits(r2.keys[i2]))) {
                        return true;
                    }
                } else {
                    final Container c1 = (Container) s1;
                    final Container c2 = (Container) s2;
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
            // i2 < 0
            if (flen1 > 0) {
                final long k1Blocklast = highBits(k1) + (flen1 - 1) * BLOCK_SIZE;
                int j2 = r2.getSpanIndex(p2, k1Blocklast);
                if (j2 >= 0) {
                    return true;
                }
                // Both i2 and j2, the indices to the first and last key in s1, are negative,
                // so those exact keys are not in r2.  However, if they are different,
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
        return false;
    }

    /**
     * OrEquals a single span into this container.
     *
     * @param other the other RspArray to ask for container sharing
     * @param startPos the first position to start looking for orKey in this container.
     * @param orKey the key for the span to add.
     * @param orSpan the span to add.
     * @return the index in this container to continue searches for keys after (orKey, orSpan).
     */
    private int orEqualsSpan(final RspArray other, final int startPos, final long orKey, final Object orSpan,
                             final MutableObject<SortedRanges> sortedRangesMu) {
        final long orflen = getFullBlockSpanLen(orSpan);
        final int orIdx = getSpanIndex(startPos, orKey);
        if (orflen > 0) {
            final int j = setOrInsertFullBlockSpanAtIndex(orIdx, orKey, orflen, sortedRangesMu);
            // can't increment for return since it may have been absorbed by a longer full block span.
            return j;
        }
        if (orIdx < 0) {
            final Container c = other.shareContainer((Container) orSpan);
            final int i = -orIdx - 1;
            if (i >= size) {
                appendContainer(orKey, c);
                return size;
            }
            insertSpanAtIndex(i, orKey, c);
            return i + 1;
        }
        final Object s = spans[orIdx];
        final long flen = getFullBlockSpanLen(s);
        if (flen > 0) {
            final long ki = highBitsForKeyAtIndex(orIdx);
            final long lastKey = getKeyForLastBlockInSpan(ki, flen);
            if (uGreater(lastKey, highBits(orKey))) {
                return orIdx;
            }
            return orIdx + 1;
        }
        final Container orResultContainer;
        if (s == null) {
            final long kOrIdx = keys[orIdx];
            if (orSpan == null) {
                if (orKey == kOrIdx) {
                    // no change.
                    return orIdx + 1;
                }
                final short v1, v2;
                if (orKey < kOrIdx) {
                    v1 = lowBits(orKey);
                    v2 = lowBits(kOrIdx);
                } else {
                    v1 = lowBits(kOrIdx);
                    v2 = lowBits(orKey);
                }
                orResultContainer = Container.twoValues(v1, v2);
            } else {
                final Container orContainer = (Container) orSpan;
                orResultContainer = orContainer.set(lowBits(kOrIdx));
            }
            keys[orIdx] = highBits(kOrIdx);
        } else {
            final Container c = (Container) s;
            if (orSpan == null) {
                orResultContainer = c.iset(lowBits(orKey));
            } else {
                final Container orContainer = (Container) orSpan;
                orResultContainer = c.ior(orContainer);
            }
        }
        if (orResultContainer.isAllOnes()) {
            final int j = setOrInsertFullBlockSpanAtIndex(orIdx, orKey, 1, sortedRangesMu);
            // can't increment since it may have been merged with another span.
            return j;
        }
        final Container copt = maybeOptimize(orResultContainer);
        setNotNullContainerAtIndex(orIdx, copt);
        return orIdx + 1;
    }

    // In a 64bit hotspot JVM, the array object header is 12 bytes = 3 ints.
    private static final ThreadLocal<int[]> workIntArrayPerThread = ThreadLocal.withInitial(() -> new int[64 - 3]);
    private static final ThreadLocal<SortedRangesInt> workSortedRangesPerThread =
            ThreadLocal.withInitial(
                    () -> new SortedRangesInt(Math.max(16 * 4 * 1024 / Integer.BYTES, SortedRanges.INT_DENSE_MAX_CAPACITY), 0));
    /**
     * Add all elements from argument to this RspArray.
     * The argument won't be modified.
     *
     * @param other the elements to add
     */
    public void orEqualsUnsafeNoWriteCheck(final RspArray other) {
        if (other.size == 0) {
            return;
        }
        if (size == 0) {
            copyFrom(other, 0);
            return;
        }
        // First check if this is effectively an append.
        if (tryAppendShiftedUnsafeNoWriteCheck(0, other, false)) {
            return;
        }

        // Do a first pass finding all the containers on other for a key not present in this;
        // this way we try our best to avoid an O(n^2) scenario where we have to move later
        // spans to make space for earlier new keys over and over.

        // The first pass will accumulate pairs of (a, b) = (other's idx, this' idx) in this array,
        // for indices a of other that correspond to containers that will be inserted in b
        // in this.
        int[] idxPairs = workIntArrayPerThread.get();
        // Total number of elements stored in idxPairs array; should always be even.
        int idxPairsCount = 0;

        // As we check containers in others, the indices of the ones that were taken care of by the first
        // pass, and therefore can be skipped by the second pass, are stored here.
        SortedRanges secondPassSkips = workSortedRangesPerThread.get();
        secondPassSkips.clear();
        boolean tryAddToSecondPassSkips = true;
        int startPos = 0;
        for (int orIdx = 0; orIdx < other.size; ++orIdx) {
            final long orKey = other.keys[orIdx];
            final Object orSpan = other.spans[orIdx];
            if (orSpan instanceof Long) {
                continue;
            }
            int i = unsignedBinarySearch(j -> highBits(keys[j]), startPos, size, highBits(orKey));
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
            // first pass.  Either way, it needs to be skipped in the second pass.
            if (tryAddToSecondPassSkips) {
                final SortedRanges sr = secondPassSkips.appendUnsafe(orIdx);
                if (sr == null) {
                    tryAddToSecondPassSkips = false;
                } else {
                    secondPassSkips = sr;
                }
            }

            startPos = i;
            if (i > 0) {
                final Object span = spans[i - 1];
                final long flen = getFullBlockSpanLen(span);
                if (flen > 0) {
                    final long kSpan = keys[i - 1];
                    final long kSpanLast = getKeyForLastBlockInSpan(kSpan, flen);
                    if (kSpanLast >= orKey) {
                        continue;
                    }
                }
            }
            if (idxPairsCount + 2 > idxPairs.length) {
                final int[] newArr;
                if (idxPairs.length + 3 < 1024) {
                    newArr = new int[2*idxPairs.length + 3];
                } else {
                    newArr = new int[idxPairs.length + 1024];
                }
                workIntArrayPerThread.set(newArr);
                System.arraycopy(idxPairs, 0, newArr, 0, idxPairsCount);
                idxPairs = newArr;
            }
            idxPairs[idxPairsCount++] = orIdx;
            idxPairs[idxPairsCount++] = i;
        }
        if (idxPairsCount > 0) {
            boolean inPlace = true;
            Object[] newSpans = spans;
            long[] newKeys = keys;
            long[] newAcc = acc;
            final int deltaSpans = idxPairsCount / 2;
            final int newSize = size + deltaSpans;
            boolean accWasNull = acc == null;
            if (newSize > keys.length) {
                inPlace = false;
                newSpans = new Object[newSize];
                newKeys = new long[newSize];
                if (accWasNull) {
                    cardData = -1;
                }
                newAcc = (newSize > accNullThreshold) ? new long[newSize] : null;
            }
            int lastMoveRangeIdx = size - 1;
            int dst = newSize - 1;
            while (idxPairsCount > 0) {
                final int thisIdx = idxPairs[--idxPairsCount];
                final int otherIdx = idxPairs[--idxPairsCount];
                for (int i = lastMoveRangeIdx; i >= thisIdx; --i) {
                    newSpans[dst] = spans[i];
                    newKeys[dst] = keys[i];
                    --dst;
                }
                final Container otherContainer = (Container) other.spans[otherIdx];
                newSpans[dst] = other.shareContainer(otherContainer);
                newKeys[dst] = other.keys[otherIdx];
                --dst;
                lastMoveRangeIdx = thisIdx - 1;
            }
            if (!inPlace) {
                for (int i = lastMoveRangeIdx; i >= 0; --i) {
                    newSpans[dst] = spans[i];
                    newKeys[dst] = keys[i];
                    --dst;
                }
                keys = newKeys;
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
        final MutableObject<SortedRanges> sortedRangesMu = getWorkSortedRangesMutableObject();
        for (int orIdx = 0; orIdx < other.size; ++orIdx) {
            if (nextSkip == orIdx) {
                if (!skipsIter.hasNext()) {
                    nextSkip = -1;
                } else {
                    nextSkip = (int) skipsIter.nextLong();
                }
                continue;
            }
            final long orKey = other.keys[orIdx];
            final Object orSpan = other.spans[orIdx];
            startPos = orEqualsSpan(other, startPos, orKey, orSpan, sortedRangesMu);
        }
        collectRemovedIndicesIfAny(sortedRangesMu);
    }

    protected void markIndexAsRemoved(final MutableObject<SortedRanges> madeNullSpansMu, final int index) {
        keys[index] = -1;
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
            keys[i] = -1;
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

    private static final ThreadLocal<SortedRangesInt> madeNullSortedRangesPerThread =
            ThreadLocal.withInitial(
                    () -> new SortedRangesInt(Math.max(16 * 4 * 1024 / Integer.BYTES, SortedRanges.INT_DENSE_MAX_CAPACITY), 0));

    /**
     * For every element in other, add (element + shiftAmount) to this RspArray.
     * Note shiftAmount is assumed to be a multiple of BLOCK_SIZE.
     * The argument won't be modified.
     *
     * @param shiftAmount the amount to add to each key in other before insertion
     * @param other the base keys to add in the (key + shiftAmount) formula for insertion.
     */
    public void orEqualsShiftedUnsafeNoWriteCheck(final long shiftAmount, final RspArray other) {
        if (other.size == 0) {
            return;
        }
        if (size == 0) {
            copyFrom(other, shiftAmount);
            return;
        }
        // First check if this is effectively an append.
        if ((shiftAmount & BLOCK_LAST) == 0) {
            if (tryAppendShiftedUnsafeNoWriteCheck(shiftAmount, other, false)) {
                return;
            }
        }
        int startPos = 0;
        final MutableObject<SortedRanges> sortedRangesMu = getWorkSortedRangesMutableObject();
        for (int orIdx = 0; orIdx < other.size; ++orIdx) {
            final long orKey = other.keys[orIdx];
            final Object orSpan = other.spans[orIdx];
            startPos = orEqualsSpan(other, startPos, orKey + shiftAmount, orSpan, sortedRangesMu);
        }
        collectRemovedIndicesIfAny(sortedRangesMu);
    }

    protected MutableObject<SortedRanges> getWorkSortedRangesMutableObject() {
        SortedRanges madeNullSpans = madeNullSortedRangesPerThread.get();
        madeNullSpans.clear();
        return new MutableObject(madeNullSpans);
    }

    /**
     * AndNotEquals a single span into this container.
     *
     * @param startPos the first position to start looking for orKey in this container.
     * @param removeFirstKey the key for the span to remove.
     * @param removeSpan the span to remove.
     * @return the index in our parallel arrays to continue searches for keys after (removeFirstKey, removeSpan).
     */
    private int andNotEqualsSpan(final int startPos, final long removeFirstKey, final Object removeSpan,
                                 final MutableObject<SortedRanges> madeNullSpansMu) {
        final long removeflen = getFullBlockSpanLen(removeSpan);
        if (removeflen == 0) {
            final int i = getSpanIndex(startPos, removeFirstKey);
            if (i < 0) {
                return ~i;
            }
            final long firstKey = keys[i];
            final Object span = spans[i];
            final long flen = getFullBlockSpanLen(span);
            if (flen > 0) {
                final Container notContainerPre;
                if (removeSpan == null) {
                    int v = lowBitsAsInt(removeFirstKey);
                    if (v == 0) {
                        notContainerPre = Container.singleRange(1, BLOCK_SIZE);
                    } else if (v == BLOCK_LAST) {
                        notContainerPre = Container.singleRange(0, BLOCK_LAST);
                    } else {
                        notContainerPre = new RunContainer(0, v, v + 1, BLOCK_SIZE);
                    }
                } else {
                    final Container rc = (Container) removeSpan;
                    notContainerPre = rc.not(0, BLOCK_SIZE);
                }
                final Container notContainer;
                final long keyNotContainer;
                final long removeFirstBlockKey = highBits(removeFirstKey);
                if (notContainerPre.isSingleElement()) {
                    notContainer = null;
                    keyNotContainer = removeFirstBlockKey | notContainerPre.first();
                } else {
                    notContainer = maybeOptimize(notContainerPre);
                    keyNotContainer = removeFirstBlockKey;
                }
                final long endKey = firstKey + BLOCK_SIZE * (flen - 1);  // inclusive
                if (uLess(firstKey, removeFirstBlockKey)) {
                    if (uLess(removeFirstBlockKey, endKey)) {
                        replaceSpanAtIndex(i,
                                firstKey, distanceInBlocks(firstKey, removeFirstBlockKey),
                                keyNotContainer, notContainer,
                                removeFirstBlockKey + BLOCK_SIZE, distanceInBlocks(removeFirstBlockKey, endKey));
                    } else {
                        replaceSpanAtIndex(i,
                                firstKey, distanceInBlocks(firstKey, removeFirstBlockKey),
                                keyNotContainer, notContainer);
                    }
                    return i + 2;
                }
                if (uLess(removeFirstBlockKey, endKey)) {
                    replaceSpanAtIndex(i,
                            keyNotContainer, notContainer,
                            removeFirstBlockKey + BLOCK_SIZE, distanceInBlocks(removeFirstBlockKey, endKey));
                } else if (notContainer == null) {
                        keys[i] = keyNotContainer;
                        setNullContainerAtIndex(i);
                } else {
                    setNotNullContainerAtIndex(i, notContainer);
                }
                return i + 1;
            }
            if (span == null) {
                if (removeSpan == null) {
                    if (removeFirstKey == firstKey) {
                        markIndexAsRemoved(madeNullSpansMu, i);
                    }
                    return i + 1;
                }
                final Container removeContainer = (Container) removeSpan;
                if (removeContainer.contains(lowBits(firstKey))) {
                    markIndexAsRemoved(madeNullSpansMu, i);
                }
                return i + 1;
            }
            final Container container = (Container) span;
            final Container result;
            if (removeSpan == null) {
                result = container.iunset(lowBits(removeFirstKey));
            }
            else {
                final Container rc = (Container) removeSpan;
                result = container.iandNot(rc);
            }
            if (result.isEmpty()) {
               markIndexAsRemoved(madeNullSpansMu, i);
               return i + 1;
            }
            if (result.isSingleElement()) {
                keys[i] = firstKey | result.first();
                setNullContainerAtIndex(i);
            } else {
                final Container c3 = maybeOptimize(result);
                if (container != c3) {
                    setNotNullContainerAtIndex(i, c3);
                } else {
                    modifiedSpan(i);
                }
            }
            return i + 1;
        }
        int idxBegin = getSpanIndex(startPos, removeFirstKey);
        if (idxBegin < 0) {
            idxBegin = -idxBegin - 1;
            if (idxBegin >= size) {
                return size;
            }
        }
        final long removeLastKey = removeFirstKey + BLOCK_SIZE*(removeflen - 1);// inclusive.
        final int idxEnd;
        if (removeLastKey == removeFirstKey) {
            idxEnd = idxBegin;
        } else {
            int j = getSpanIndex(idxBegin + 1, removeLastKey);
            idxEnd = (j >= 0) ? j : -j - 2;
        }
        final long keyAtIdxBegin = highBits(keys[idxBegin]);
        if (keyAtIdxBegin > removeLastKey) {
            return idxBegin;
        }
        final Object spanAtIdxBegin = spans[idxBegin];
        final long flenAtIdxBegin = getFullBlockSpanLen(spanAtIdxBegin);
        final long keyAtIdxEnd;
        final long flenAtIdxEnd;
        if (idxBegin == idxEnd) {
            keyAtIdxEnd = keyAtIdxBegin;
            flenAtIdxEnd = flenAtIdxBegin;
        } else {
            keyAtIdxEnd = highBits(keys[idxEnd]);
            final Object spanAtIdxEnd = spans[idxEnd];
            flenAtIdxEnd = getFullBlockSpanLen(spanAtIdxEnd);
        }
        int dst = idxBegin;
        if (uLess(keyAtIdxBegin, removeFirstKey)) {
            dst = idxBegin + 1;
            if (flenAtIdxBegin > 0) {
                final long newflen = distanceInBlocks(keyAtIdxBegin, removeFirstKey);
                setSpanAtIndex(idxBegin, newflen);
            }
        }
        int src = idxEnd + 1;
        final long blockKeyAtIdxEnd = highBits(keyAtIdxEnd);
        final long lastKeyAtIdxEnd = getKeyForLastBlockInSpan(blockKeyAtIdxEnd, flenAtIdxEnd);
        if (uLess(removeLastKey, lastKeyAtIdxEnd)) {
            final long nextKey = nextKey(removeLastKey);
            final long newSpan = distanceInBlocks(removeLastKey, lastKeyAtIdxEnd);
            if (idxEnd >= dst) {
                keys[idxEnd] = nextKey;
                setSpanAtIndex(idxEnd, newSpan);
                src = idxEnd;
            } else {
                insertSpanAtIndex(dst, nextKey, newSpan);
                src = dst;
            }
        }
        if (dst < src) {
            markIndexRangeAsRemoved(madeNullSpansMu, dst, src - 1);
        }
        return src;
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
        final MutableObject<SortedRanges> madeNullSpansMu = getWorkSortedRangesMutableObject();
        for (int andNotIdx = firstKey; andNotIdx < other.size; ++andNotIdx) {
            final long andNotKey = other.keys[andNotIdx];
            final Object andNotSpan = other.spans[andNotIdx];
            startPos = andNotEqualsSpan(startPos, andNotKey, andNotSpan, madeNullSpansMu);
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
        while (i < size && keys[i] != -1) {
            ++i;
        }
        if (i == size) {
            return;
        }
        // keys[i] == -1.
        int dst = i;
        while (true) {
            int j = i + 1;
            while (j < size && keys[j] == -1) {
                ++j;
            }
            if (j == size) {
                break;
            }
            // keys[j] != -1
            int k = j + 1;
            while (k < size && keys[k] != -1) {
                ++k;
            }
            // keys[k - 1] != -1.
            // move the block [j, k - 1] to i.
            final int n = k - j;
            arrayCopies(j, dst, n);
            dst += n;
            i = k;
            while (i < size && keys[i] != -1) {
                ++i;
            }
            if (i == size) {
                break;
            }
        }
        size = dst;
        tryCompactUnsafe(4);
    }

    private int andEqualsSpan(final RspArray other, final int startPos, final long andKey, final Object andSpan,
                              final MutableObject<SortedRanges> madeNullSpansMu) {
        final long andflen = getFullBlockSpanLen(andSpan);
        if (andflen > 0) {
            final long andLastKey = getKeyForLastBlockInSpan(andKey, andflen);
            int andLastKeyIdx = keySearch(startPos, andLastKey);
            if (andLastKeyIdx >= 0) {
                return andLastKeyIdx + 1;
            }
            return -andLastKeyIdx - 1;
        }
        final long andBlockKey = highBits(andKey);
        int andIdx = getSpanIndex(startPos, andKey);
        if (andIdx < 0) {
            return -andIdx - 1;
        }
        final Object ourSpan = spans[andIdx];
        final long flen = getFullBlockSpanLen(ourSpan);
        if (flen > 0) {
            final long ourKey = keys[andIdx];
            final long lastKey = getKeyForLastBlockInSpan(ourKey, flen);
            final Container toAddContainer;
            if (andSpan == null) {
                toAddContainer = null;
            } else {
                final Container andContainer = (Container) andSpan;
                toAddContainer = other.shareContainer(andContainer);
            }
            // andIdx > 0, therefore andKey is contained in this span.
            if (uLess(ourKey, andBlockKey)) {
                // when this method is called from andEquals, given the previous pruning this
                // case can't be hit.
                if (uLess(andBlockKey, lastKey)) {
                    replaceSpanAtIndex(andIdx,
                            ourKey, distanceInBlocks(ourKey, andBlockKey),
                            andKey, toAddContainer,
                            nextKey(andKey), distanceInBlocks(andBlockKey, lastKey));
                } else {
                    replaceSpanAtIndex(andIdx,
                            ourKey, distanceInBlocks(ourKey, andBlockKey),
                            andKey, toAddContainer);
                }
            } else if (uLess(andBlockKey, lastKey)) {
                // when this method is called from andEquals, given the previous pruning this
                // case can't be hit.
                replaceSpanAtIndex(andIdx,
                        andKey, toAddContainer,
                        nextKey(andBlockKey), distanceInBlocks(andBlockKey, lastKey));
            } else if (toAddContainer == null) {
                keys[andIdx] = andKey;
                setNullContainerAtIndex(andIdx);
            } else {
                setNotNullContainerAtIndex(andIdx, toAddContainer);
            }
            return andIdx + 1;
        }
        Container result = null;  // if result stays null, the result is empty and we should remove this span.
        Container ourContainer = null;
        final long ourKey = keys[andIdx];
        if (ourSpan == null) {
            if (andSpan == null) {
                if (andKey == ourKey) {
                    return andIdx + 1;
                }
            } else {
                final Container ac = (Container) andSpan;
                if (ac.contains(lowBits(ourKey))) {
                    return andIdx + 1;
                }
            }
        } else {
            ourContainer = (Container) ourSpan;
            if (andSpan == null) {
                if (ourContainer.contains(lowBits(andKey))) {
                    keys[andIdx] = andKey;
                    setNullContainerAtIndex(andIdx);
                    return andIdx + 1;
                }
            } else {
                final Container ac = (Container) andSpan;
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
            keys[andIdx] = andKey | result.first();
            setNullContainerAtIndex(andIdx);
        } else {
            final Container c3 = maybeOptimize(result);
            if (c3 != ourContainer) {
                setNotNullContainerAtIndex(andIdx, c3);
            } else {
                modifiedSpan(andIdx);
            }
        }
        return andIdx + 1;
    }

    private static class ArraysBuf {
        public long[] keys;
        public Object[] spans;
        public int len() {
            if (keys == null) {
                return 0;
            }
            return keys.length;
        }
        public void ensure(final int size) {
            if (len() >= size) {
                return;
            }
            setToSize(size);
        }
        public void swap(final long[] keys, final Object[] spans) {
            this.keys = keys;
            this.spans = spans;
        }
        private void setToSize(final int size) {
            keys = new long[size];
            spans = new Object[size];
        }
    }

    private static ThreadLocal<ArraysBuf> rspArrayBuf = ThreadLocal.withInitial(ArraysBuf::new);
    /**
     * Intersects this RspArray with the argument, leaving the result on this RspArray.
     * The argument won't be modified.
     *
     * @param other an RspArray.
     */
    public void andEqualsUnsafeNoWriteCheck(final RspArray other) {
        // First do one pass of pruning spans that do not have a chance to intersect.
        // The max number of resulting spans is potentially greater than max(size, other.size):
        // think about a full block span in one array that ends up being split due to multiple
        // single block containers in the other array; eg:
        // this:  {[key / BLOCK_SIZE, flen]} = { [0, 4], [10, 0], [11, 0] }
        // other:                            = { [1, 0], [2, 0], [9, 7] }
        // result:                           = { [1, 0], [2, 0], [10, 0], [11, 0] }
        final ArraysBuf buf = rspArrayBuf.get();
        final int maxNewCapacity = size + other.size;
        buf.ensure(maxNewCapacity);
        int newSize = 0;
        int startPos = 0;
        for (int andIdx = 0; andIdx < other.size; ++andIdx) {
            if (startPos >= size) {
                break;
            }
            final long andFirstBlockKey = highBits(other.keys[andIdx]);
            final Object andSpan = other.spans[andIdx];
            final long andflen = getFullBlockSpanLen(andSpan);
            final long andLastKey = getKeyForLastBlockInSpan(andFirstBlockKey, andflen);
            int firstIdx = getSpanIndex(startPos, andFirstBlockKey);
            if (firstIdx < 0) {
                firstIdx = -firstIdx - 1;
                if (firstIdx == size) {
                    break;
                }
            }
            int lastIdx;
            if (andFirstBlockKey == andLastKey || firstIdx == size - 1) {
                lastIdx = firstIdx;
            } else {
                lastIdx = getSpanIndex(firstIdx + 1, andLastKey);
                if (lastIdx < 0) {
                    lastIdx = -lastIdx - 2;
                }
            }
            int i = firstIdx;
            while (true) {
                final long k = keys[i];
                final long bk = highBits(k);
                if (uGreater(bk, andLastKey)) {
                    startPos = i;
                    break;
                }
                final Object s = spans[i];
                final long flen = getFullBlockSpanLen(s);
                if (flen > 0) {
                    final long lastKey = getKeyForLastBlockInSpan(bk, flen);
                    if (uGreaterOrEqual(lastKey, andFirstBlockKey)) {
                        final long newKey = uMax(andFirstBlockKey, bk);
                        buf.keys[newSize] = newKey;
                        long newLen = flen - distanceInBlocks(bk, newKey);
                        boolean bail = false;
                        if (uGreater(lastKey, andLastKey)) {
                            newLen -= distanceInBlocks(andLastKey, lastKey);
                            bail = true;
                        }
                        setSpanAtIndex(buf.spans, newSize, newLen);
                        ++newSize;
                        if (bail) {
                            startPos = i;
                            break;
                        }
                    }
                } else if (uGreaterOrEqual(bk, andFirstBlockKey)) {
                    buf.keys[newSize] = k;
                    buf.spans[newSize] = s;
                    ++newSize;
                }
                ++i;
                if (i > lastIdx) {
                    startPos = i;
                    break;
                }
            }
        }
        final int maxWaste = 7;
        size = newSize;
        if (buf.len() - newSize > maxWaste) {
            keys = new long[newSize];
            spans = new Object[newSize];
            if (newSize > accNullThreshold) {
                acc = new long[newSize];
            }
            System.arraycopy(buf.keys, 0, keys, 0, newSize);
            System.arraycopy(buf.spans, 0, spans, 0, newSize);
        } else {
            final long[] oldKeys = keys;
            final Object[] oldSpans = spans;
            if (buf.len() != keys.length && buf.len() > accNullThreshold) {
                acc = new long[buf.len()];
            }
            keys = buf.keys;
            spans = buf.spans;
            buf.swap(oldKeys, oldSpans);
        }
        cardData = -1;
        startPos = 0;
        final MutableObject<SortedRanges> madeNullSpansMu = getWorkSortedRangesMutableObject();
        for (int andIdx = 0; andIdx < other.size; ++andIdx) {
            final long andKey = other.keys[andIdx];
            final Object andSpan = other.spans[andIdx];
            startPos = andEqualsSpan(other, startPos, andKey, andSpan, madeNullSpansMu);
            if (startPos >= size) {
                break;
            }
        }
        collectRemovedIndicesIfAny(madeNullSpansMu);
    }

    public void applyKeyOffset(final long offset) {
        for (int i = 0; i < size; ++i) {
            keys[i] += offset;
        }
    }

    // end is inclusive
    private void appendSpanIntersectionByKeyRange(final RspArray r, final int i, final long start, final long end) {
        final long k = keys[i];
        final Object s = spans[i];
        final long flen = getFullBlockSpanLen(s);
        if (flen > 0) {
            final long sLastPlusOne = k + BLOCK_SIZE * flen;
            final long resultStart = uMax(k, start);
            if (uGreaterOrEqual(resultStart, sLastPlusOne)) {
                return;
            }
            final long resultEnd = uMin(sLastPlusOne - 1, end);
            final long nextHiBits;
            if (highBits(resultStart) == resultStart) {
                nextHiBits = resultStart;
            } else {  // k < resultStart (unsigned comparison)
                final long resultStartHiBits = highBits(resultStart);
                if (uLess(resultEnd, resultStartHiBits + BLOCK_LAST)) {
                    final int cs = lowBitsAsInt(resultStart);
                    final int ce = lowBitsAsInt(resultEnd);
                    if (cs == ce) {
                        r.appendSingle(resultStart);
                    } else {
                        r.appendContainer(resultStartHiBits, Container.rangeOfOnes(cs, ce + 1 /* exclusive */));
                    }
                    return;
                }
                final int cs = lowBitsAsInt(resultStart);
                if (cs == BLOCK_LAST) {
                    r.appendSingle(resultStart);
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
                        r.appendSingle(keyAfterMid);
                    } else {
                        r.appendContainer(keyAfterMid, Container.rangeOfOnes(0, e + 1 /* exclusive */));
                    }
                }
            }
            return;
        }
        // we have a single container to intersect against [start, end).
        if (s == null) {
            if (start <= k && k <= end) {
                r.appendContainer(k, null);
            }
            return;
        }
        final long blockLast = k + BLOCK_LAST;
        if (uLess(end, k) || uGreater(start, blockLast)) {
            return;
        }
        final Container c = (Container) s;
        final long resultStart = uMax(k, start);
        final long resultEnd;
        if (uLess(end, blockLast)) {
            resultEnd = end;
        } else {
            if (resultStart == k) {
                r.appendContainer(k, shareContainer(c));
                return;
            }
            resultEnd = blockLast;
        }
        final int rStart = (int) (resultStart - k);
        final int rEnd = (int) (resultEnd - k) + 1;  // exclusive.
        final Container res = c.andRange(rStart, rEnd);
        if (res.isEmpty()) {
            return;
        }
        if (res.isSingleElement()) {
            r.appendSingle(k | res.first());
            return;
        }
        r.appendContainer(k, maybeOptimize(res));
    }

    boolean forEachLongInSpanWithOffsetAndMaxCount(final int i, final long offset, LongAbortableConsumer lc, final long maxCount) {
        final MutableLong n = new MutableLong(0);
        forEachLongInSpanWithOffset(i, offset, (final long v) -> {
            if (!lc.accept(v)) {
                return false;
            }
            n.increment();
            return n.longValue() < maxCount;
        });
        return n.longValue() >= maxCount;  // The only way we get to maxCount is if lc never returns false above.
    }

    boolean forEachLongInSpanWithOffset(final int i, final long offset, LongAbortableConsumer lc) {
        final long k = keys[i];
        final Object s = spans[i];
        if (s == null) {
            return lc.accept(k);
        }
        final long flen = getFullBlockSpanLen(s);
        if (flen > 0) {
            final long oneAfterLast = k + flen * BLOCK_SIZE;
            for (long v = k + offset; v < oneAfterLast; ++v) {
                final boolean wantMore = lc.accept(v);
                if (!wantMore) {
                    return false;
                }
            }
            return true;
        }
        final Container c = (Container) s;
        final boolean wantMore = c.forEach((int) offset, (short v) -> lc.accept(k | unsignedShortToLong(v)));
        return wantMore;
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
        return n.longValue() >= maxCount;  // The only way we get to maxCount is if lc never returns false above.
    }

    boolean forEachLongInSpan(final int i, LongAbortableConsumer lc) {
        final long k = keys[i];
        final Object s = spans[i];
        if (s == null) {
            return lc.accept(k);
        }
        final long flen = getFullBlockSpanLen(s);
        if (flen > 0) {
            final long oneAfterLast = k + flen * BLOCK_SIZE;
            for (long v = k; v < oneAfterLast; ++v) {
                final boolean wantMore = lc.accept(v);
                if (!wantMore) {
                    return false;
                }
            }
            return true;
        }
        final Container c = (Container) s;
        final boolean wantMore = c.forEach((short v) -> lc.accept(k | unsignedShortToLong(v)));
        return wantMore;
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
        final long k = keys[i];
        final Object s = spans[i];
        if (s == null) {
            if (offset != 0) {
                throw new IllegalArgumentException("offset=" + offset + " and single key span.");
            }
            return larc.accept(k, k);
        }
        final long flen = getFullBlockSpanLen(s);
        if (flen > 0) {
            final long start = k + offset;
            long end = k + flen * BLOCK_SIZE - 1;
            final long d = end - start + 1;
            if (d > maxCardinality) {
                end = start + maxCardinality - 1;
            }
            return larc.accept(start, end);
        }
        long remaining = maxCardinality;
        final int bufSz = 10;
        final short[] buf = new short[bufSz];
        final Container c = (Container) s;
        final SearchRangeIterator ri = c.getShortRangeIterator((int) offset);
        while (ri.hasNext()) {
            final int nRanges = ri.next(buf, 0, bufSz/2);
            for (int j = 0; j < 2*nRanges; j += 2) {
                final long start = k|unsignedShortToLong(buf[j]);
                long end = k|unsignedShortToLong(buf[j + 1]);
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
        return true;
    }

    boolean forEachLongRangeInSpanWithOffset(final int i, final long offset,
                                             final LongRangeAbortableConsumer larc) {
        final long k = keys[i];
        final Object s = spans[i];
        if (s == null) {
            if (offset != 0) {
                throw new IllegalArgumentException("offset=" + offset + " and single key span.");
            }
            return larc.accept(k, k);
        }
        final long flen = getFullBlockSpanLen(s);
        if (flen > 0) {
            final long oneAfterLast = k + flen * BLOCK_SIZE;
            return larc.accept(k + offset, oneAfterLast - 1);
        }
        final int bufSz = 10;
        final short[] buf = new short[bufSz];
        final Container c = (Container) s;
        final SearchRangeIterator ri = c.getShortRangeIterator((int) offset);
        while (ri.hasNext()) {
            final int nRanges = ri.next(buf, 0, bufSz/2);
            for (int j = 0; j < 2*nRanges; j += 2) {
                final long start = k|unsignedShortToLong(buf[j]);
                final long end = k|unsignedShortToLong(buf[j + 1]);
                if (!larc.accept(start, end)) {
                    return false;
                }
            }
        }
        return true;
    }

    static LongRangeAbortableConsumer makeAdjacentRangesCollapsingWrapper(final long[] pendingRange, final LongRangeAbortableConsumer lrac) {
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
            final int i,
            final long[] keys, final Object[] spans,
            final long kHigh,
            final long rsStart, final long rsEnd) {
        final int crsStart = (int) (rsStart - kHigh);
        final int crsEnd = (int) (rsEnd - kHigh);
        if (crsStart > 0) {
            if (crsEnd < BLOCK_LAST) {
                keys[i] = kHigh;
                spans[i] = Container.twoRanges(0, crsStart, crsEnd + 1, BLOCK_SIZE);
                return;
            }
            if (crsStart == 1) {
                keys[i] = kHigh;
                spans[i] = null;
                return;
            }
            keys[i] = kHigh;
            spans[i] = Container.singleRange(0, crsStart);
            return;
        }
        if (debug && crsEnd >= BLOCK_LAST) {
            throw new IllegalStateException("crsEnd=" + crsEnd);
        }
        if (crsEnd + 1 == BLOCK_LAST) {
            keys[i] = kHigh | BLOCK_LAST;
            spans[i] = null;
            return;
        }
        keys[i] = kHigh;
        spans[i] = Container.rangeOfOnes(crsEnd + 1, BLOCK_SIZE);
    }

    /**
     * requirement: (start, end) should intersect span at index i.
     * @param i span index.
     * @param k key for span i.
     * @param start start of range to remove (may be outside of span)
     * @param end end of range to remove (may be outside of span)
     * @param madeNullSpansMu where to store the indices of spans that were made null
     *                       because they ended up empty; these should be collected later by the caller.
     * @return if >= 0, the index of the last span where the removal effectively happened.
     *         if < 0, ~index for the span where to continue the removals, after a span was effectively eliminated.
     */
    private int removeRangeInSpan(final int i, final long k, final long start, final long end,
                                  final MutableObject<SortedRanges> madeNullSpansMu) {
        final Object s = spans[i];
        final long flen = getFullBlockSpanLen(s);
        if (flen > 0) {
            final long sLastPlusOne = k + BLOCK_SIZE * flen;
            final long rsStart = uMax(k, start);
            if (uGreaterOrEqual(rsStart, sLastPlusOne)) {
                return i;
            }
            final long rsEnd = uMin(sLastPlusOne - 1, end);
            final long kStart = highBits(rsStart);
            final long kEnd = highBits(rsEnd);
            final long[] ks = new long[4];
            final Object[] ss = new Object[4];
            int count = 0;
            int returnValue = ~i;
            final long preflen = distanceInBlocks(k, kStart);
            if (preflen > 0) {
                ks[count] = k;
                ss[count] = preflen;
                ++count;
            }
            if (kStart == kEnd) {
                if (rsEnd - rsStart < BLOCK_LAST) {
                    setToRangeOfOnesMinusRangeForKey(count, ks, ss, kStart, rsStart, rsEnd);
                    returnValue = i + count;
                    ++count;
                }
            } else {
                final long c1End = Math.min(kEnd, nextKey(kStart)) - 1;
                if (rsStart != kStart || c1End - rsStart < BLOCK_LAST) {
                    setToRangeOfOnesMinusRangeForKey(count, ks, ss, kStart, rsStart, c1End);
                    returnValue = i + count;
                    ++count;
                }
                if (rsEnd - kEnd < BLOCK_LAST) {
                    setToRangeOfOnesMinusRangeForKey(count, ks, ss, kEnd, kEnd, rsEnd);
                    returnValue = i + count;
                    ++count;
                }
            }
            final long posSpanFirstKey = nextKey(kEnd);
            final long posflen = distanceInBlocks(posSpanFirstKey, sLastPlusOne);
            if (posflen > 0) {
                ks[count] = posSpanFirstKey;
                ss[count] = posflen;
                ++count;
            }
            if (count > 1) {
                replaceSpanAtIndex(i, count, ks, ss);
                return returnValue;
            }
            if (count > 0) {
                keys[i] = ks[0];
                spans[i] = ss[0];
                modifiedSpan(i);
                return returnValue;
            }
            // the full span is being removed.
            markIndexAsRemoved(madeNullSpansMu, i);
            return ~(i + 1);
        }
        // we have a single container from where to remove [start, end).
        Container result = null;  // if result stays null this span should be eliminated.
        Container ourContainer = null;
        if (s == null) {
            if (k < start || end < k) {
                return i;
            }
        } else {
            final long resultStart = uMax(k, start);
            final long resultEnd = uMin(k + BLOCK_LAST, end);
            ourContainer = (Container) s;
            final int rStart = (int) (resultStart - k);
            final int rEnd = (int) (resultEnd - k) + 1;  // exclusive.
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
            keys[i] = k | result.first();
            setNullContainerAtIndex(i);
        } else {
            final Container c3 = maybeOptimize(result);
            if (c3 != ourContainer) {
                setContainerAtIndexMaybeSingle(i, c3);
            } else {
                modifiedSpan(i);
            }
        }
        return i;
    }

    public void removeRangeUnsafeNoWriteCheck(final long start, final long end) {
        final MutableObject<SortedRanges> madeNullSpansMu = getWorkSortedRangesMutableObject();
        removeRange(0, start, end, madeNullSpansMu);
        collectRemovedIndicesIfAny(madeNullSpansMu);
    }

    private int removeRange(final int fromIdx, final long start, final long end,
                            final MutableObject<SortedRanges> madeNullSpansMu) {
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
            final long k = keys[i];
            if (highBits(k) > kEnd) {
                break;
            }
            i = removeRangeInSpan(i, k, start, end, madeNullSpansMu);
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
            final MutableObject<SortedRanges> madeNullSpansMu = getWorkSortedRangesMutableObject();
            int i = 0;
            while (rit.hasNext()) {
                rit.next();
                final long start = rit.currentRangeStart();
                final long end = rit.currentRangeEnd();
                i = removeRange(i, start, end, madeNullSpansMu);
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
    // shiftAmount should be such that (shiftAmount & BLOCK_LAST) == 0
    boolean tryAppendShiftedUnsafeNoWriteCheck(final long shiftAmount, final RspArray other, final boolean acquire) {
        if (RspArray.debug) {
            if (size == 0 || other.size == 0) {
                throw new IllegalArgumentException(
                        "Append called for empty argument: size=" + size + ", other.size=" + other.size);
            }
        }
        final long firstOtherBlockKey = highBits(other.keys[0]) + shiftAmount;
        final long ourLastKey = keyForLastBlock();
        if (firstOtherBlockKey <= ourLastKey) {
            return false;
        }
        int firstOtherSpan = 0;
        if (ourLastKey + BLOCK_SIZE == firstOtherBlockKey) {
            final Object ourLastSpan = spans[size - 1];
            if (ourLastSpan instanceof Long) {
                final Object otherFirstSpan = other.spans[0];
                if (otherFirstSpan instanceof Long) {
                    // we need to merge these spans.
                    spans[size - 1] = ((Long) ourLastSpan) + ((Long) otherFirstSpan);
                    modifiedSpan(size - 1);
                    firstOtherSpan = 1;
                }
            }

        }
        ensureSizeCanGrowBy(other.size - firstOtherSpan);
        for (int i = firstOtherSpan; i < other.size; ++i) {
            final int pos = size + i - firstOtherSpan;
            keys[pos] = other.keys[i] + shiftAmount;
            final Object span = other.spans[i];
            if (!acquire && span instanceof Container) {
                final Container c = (Container) span;
                c.setCopyOnWrite();
            }
            spans[pos] = span;
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
            sb.append(String.format("0x%X", keys[i] >> 16));
            sb.append(", ");
            final Object s = spans[i];
            if (s == null) {
                sb.append('n');
            } else {
                final long flen = getFullBlockSpanLen(s);
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
        return validate("",false, false);
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
        for (int i = 0; i < size; ++i) {
            final long k = keys[i];
            final Object s = spans[i];
            if (s != null && (k & BLOCK_LAST) != 0) {
                final String m = str + ": lower 16 bits of key non-zero i=" + i + ", k=" + k;
                if (doAssert) {
                    Assert.assertion(false, m);
                }
                return false;
            }
            if (!firstTime) {
                if (!uLess(lastSpanLastBlockKey, k)) {
                    final String m = str + ": non-increasing key found i=" + i + ", k=" + k +
                            ", lastSpanLastBlockKey=" + lastSpanLastBlockKey + ", size=" + size;
                    if (doAssert) {
                        Assert.assertion(false, m);
                    }
                    return false;
                }
            }
            final long flen = getFullBlockSpanLen(s);
            if (flen > 0) {
                if (lastSpanWasFullBlock && k - lastSpanLastBlockKey <= BLOCK_SIZE) {
                    final String m = str + ": consecutive full block spans found i=" + i + ", size=" + size;
                    if (doAssert) {
                        Assert.assertion(false, m);
                    }
                    return false;
                }
                lastSpanWasFullBlock = true;
            } else {
                if (s != null) {
                    if (!(s instanceof Container)) {
                        final String m = str + ": can't cast s=" + s + " of class " + s.getClass().getSimpleName() +
                                " to Container when !(flen > 0).";
                        if (doAssert) {
                            Assert.assertion(false, m);
                        }
                        return false;
                    }
                    final Container c = (Container) s;
                    if (c.isEmpty()) {
                        final String m = str + ": empty RB container found i=" + i + ", size=" + size;
                        if (doAssert) {
                            Assert.assertion(false, m);
                        }
                        return false;
                    }
                    if (c.isAllOnes()) {
                        final String m = str + ": full RB container found i=" + i + ", size=" + size;
                        if (doAssert) {
                            Assert.assertion(false, m);
                        }
                        return false;
                    }
                    if (c.isSingleElement()) {
                        final String m = str + ": singleton container found i=" + i + ", type=" + c.getClass().getSimpleName();
                        if (doAssert) {
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
            throw new IllegalArgumentException(("startPositionInclusive=" + startPositionInclusive + " should be >=0."));
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
            if (span == null) {
                ++card;
                ++nRanges;
                continue;
            }
            final long flen = getFullBlockSpanLen(span);
            if (flen > 0) {
                card += flen * BLOCK_SIZE;
                ++nRanges;
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
                nRanges += Math.max(1, containerCard / 3);   // estimate.
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
        return rangesCountUpperBound(0, size -1);
    }

    public long rangesCountUpperBound(final int startIdx, final int endIdx) {
        long nRanges = 0;
        for (int idx = startIdx; idx <= endIdx; ++idx) {
            Object s = spans[idx];
            final long slen = RspArray.getFullBlockSpanLen(s);
            if (slen > 0) {
                ++nRanges;
                continue;
            }
            if (s == null) {
                ++nRanges;
            }
            else if (s instanceof io.deephaven.db.v2.utils.rsp.container.RunContainer) {
                nRanges += ((io.deephaven.db.v2.utils.rsp.container.RunContainer) s).numberOfRanges();
            } else if (s instanceof io.deephaven.db.v2.utils.rsp.container.SingleRangeContainer) {
                nRanges += 1;
            } else if (s instanceof io.deephaven.db.v2.utils.rsp.container.TwoValuesContainer) {
                nRanges += 2;
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
            final LongConsumer twoValuesContainerCount
    ) {
        rspParallelArraysSizeUsed.accept(size);
        rspParallelArraysSizeUnused.accept(keys.length - size);
        // TODO: It would be much more efficient to accumulate multiple samples (perhaps one array of them per Metric),
        // and then provide them to the metric in one call, to prevent multiple volatile assignments.
        for (int i = 0; i < size; ++i) {
            final Object o = spans[i];
            if (o == null) {
                singletonContainersCount.accept(1);
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
        SortedRanges sr = SortedRanges.tryMakeForKnownRangeUnknownMaxCapacity(SortedRanges.LONG_DENSE_MAX_CAPACITY, first, last, true);
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
