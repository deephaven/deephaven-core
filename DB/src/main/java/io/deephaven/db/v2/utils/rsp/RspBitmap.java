package io.deephaven.db.v2.utils.rsp;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.utils.*;
import io.deephaven.db.v2.utils.singlerange.SingleRange;
import io.deephaven.db.v2.utils.sortedranges.SortedRanges;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import io.deephaven.db.v2.utils.rsp.container.*;

import java.util.PrimitiveIterator;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

/**
 * See header comment on RspArray for explanation on space partitioning.
 */
public class RspBitmap extends RspArray<RspBitmap> implements TreeIndexImpl {
    public RspBitmap() {
        super();
    }

    // Create a bitmap with a single initial range.
    public RspBitmap(final long start, final long end) {
        super(start, end);
    }

    private RspBitmap(final RspBitmap other) {
        super(other);
    }

    public RspBitmap(
            final RspArray src,
            final int startIdx, final long startOffset,
            final int endIdx, final long endOffset) {
        super(src, startIdx, startOffset, endIdx, endOffset);
    }

    public static RspBitmap makeEmpty() {
        return new RspBitmap();
    }

    public static RspBitmap makeSingleRange(final long start, final long end) {
        return new RspBitmap(start, end);
    }

    public static RspBitmap makeSingle(final long v) {
        return makeSingleRange(v, v);
    }

    @Override
    protected final RspBitmap make(final RspArray src,
                                   final int startIdx, final long startOffset,
                                   final int endIdx, final long endOffset) {
        return new RspBitmap(src, startIdx, startOffset, endIdx, endOffset);
    }

    @Override
    protected final RspBitmap make() {
        return new RspBitmap();
    }

    // RefCounted
    @Override
    protected RspBitmap self() {
        return this;
    }

    @Override
    public RspBitmap deepCopy() {
        return new RspBitmap(this);
    }

    public RspBitmap writeCheck() {
        return getWriteRef();
    }

    @VisibleForTesting
    RspArray getKvs() {
        return this;
    }

    private static short lowBitsAsShort(final long val) {
        return (short) (val & BLOCK_LAST);
    }

    public RspBitmap addValues(final long... dat) {
        final RspBitmap rb = addN(dat, 0, dat.length);
        return rb;
    }

    public long first() {
        return firstValue();
    }

    public long last() {
        return lastValue();
    }

    private final static class AddCtx {
        long key;
        int index;
        Container c; // The RB Container, or null if key corresponds to a full block span or single key.
    }

    private void addAndPopulateCtx(final long val, final AddCtx ctx, final MutableObject<SortedRanges> sortedRangesMu) {
        ctx.index = getSpanIndex(val);
        if (ctx.index < 0) {
            ctx.key = val;
            ctx.index = -ctx.index - 1;
            ctx.c = null;
            insertSpanAtIndex(ctx.index, val, null);
            return;
        }
        final Object span = spans[ctx.index];
        final long flen = RspArray.getFullBlockSpanLen(span);
        if (flen > 0) {
            // if flen > 0 nothing to do, val is already there.
            ctx.key = highBits(val);
            ctx.c = null;
            return;
        }
        final Container result;
        Container container = null;
        if (span == null) {
            final long key = keys[ctx.index];
            result = containerForTwoValues(key, val);
            if (result == null) {
                ctx.key = val;
                return;
            }
            keys[ctx.index] = highBits(key);
        } else {
            container = (Container) span;
            result = container.iset(lowBitsAsShort(val));
        }
        ctx.key = highBits(val);
        if (result.isAllOnes()) {
            setOrInsertFullBlockSpanAtIndex(ctx.index, ctx.key, 1, sortedRangesMu);
            ctx.c = null;
        } else {
            if (result != container) {
                setSpanAtIndex(ctx.index, result);
            } else {
                modifiedSpan(ctx.index);
            }
            ctx.c = result;
        }
    }

    static Container containerForTwoValues(final long v1, final long v2) {
        if (v1 == v2) {
            return null;
        }
        if (v1 < v2) {
            return Container.twoValues(lowBitsAsShort(v1), lowBitsAsShort(v2));
        }
        return Container.twoValues(lowBitsAsShort(v2), lowBitsAsShort(v1));
    }

    public RspBitmap addN(final long[] dat, final int offset, final int n) {
        final RspBitmap rb = addValuesUnsafe(dat, offset, n);
        rb.finishMutations();
        return rb;
    }

    public RspBitmap addValuesUnsafe(final LongChunk<OrderedKeyIndices> values, final int offset, final int length) {
        final RspBitmap rb = writeCheck();
        rb.addValuesUnsafeNoWriteCheck(values, offset, length);
        return rb;
    }

    public void addValuesUnsafeNoWriteCheck(final LongChunk<OrderedKeyIndices> values, final int offset, final int length) {
        int lengthFromThisSpan;
        final MutableObject<SortedRanges> sortedRangesMu = getWorkSortedRangesMutableObject();
        int spanIndex = 0;
        for (int vi = 0; vi < length; vi += lengthFromThisSpan) {
            final long value = values.get(vi + offset);
            final long highBits = highBits(value);
            lengthFromThisSpan = countContiguousHighBitsMatches(values, vi + offset + 1, length - vi - 1, highBits) + 1;
            final int spanIndexRaw = getSpanIndex(spanIndex, highBits);
            Container container = null;
            boolean existing = false;
            if (spanIndexRaw < 0) {
                spanIndex = ~spanIndexRaw;
            } else {
                spanIndex = spanIndexRaw;
                final Object existingSpan = spans[spanIndex];
                if (getFullBlockSpanLen(existingSpan) >= 1) {
                    continue;
                }
                container = (Container) existingSpan;
                existing = true;
            }
            final Container result = createOrUpdateContainerForValues(values, vi + offset, lengthFromThisSpan, existing, spanIndex, container);
            if (result != null && result.isAllOnes()) {
                spanIndex = setOrInsertFullBlockSpanAtIndex(spanIndexRaw, highBits, 1, sortedRangesMu);
            } else if (!existing) {
                if (result == null) {
                    insertSpanAtIndex(spanIndex, value, null);
                } else {
                    insertSpanAtIndex(spanIndex, highBits, result);
                }
            } else if (container != result) {
                setNotNullContainerAtIndex(spanIndex, result);
                if (container == null) {
                    keys[spanIndex] = highBits;
                }
            } else {
                modifiedSpan(spanIndex);
            }
        }
        collectRemovedIndicesIfAny(sortedRangesMu);
    }

    private static int countContiguousHighBitsMatches(final LongChunk<OrderedKeyIndices> values,
                                                      final int offset, final int length,
                                                      final long highBits) {
        for (int vi = 0; vi < length; ++vi) {
            if (highBits(values.get(vi + offset)) != highBits) {
                return vi ;
            }
        }
        return length;
    }

    private Container createOrUpdateContainerForValues(@NotNull final LongChunk<OrderedKeyIndices> values,
                                                              final int offset, final int length,
                                                              final boolean existing,
                                                              final int keyIdx,
                                                              Container container) {
        final long firstValue = values.get(offset);
        if (length == 1) {
            // We're adding only one value
            if (!existing) {
                return null;
            }
            if (container == null) {
                final long key = keys[keyIdx];
                if (firstValue == key) {
                    return null;
                }
                final long left, right;
                if (firstValue < key) {
                    left = firstValue;
                    right = key;
                } else {
                    left = key;
                    right = firstValue;
                }
                if (left + 1 == right) {
                    final int start = lowBitsAsInt(left);
                    final int end = lowBitsAsInt(right);
                    return new SingleRangeContainer(start, end + 1);
                }
                final short leftLow = lowBitsAsShort(left);
                final short rightLow = lowBitsAsShort(right);
                return new TwoValuesContainer(leftLow, rightLow);
            }
            final short firstValueLowBits = lowBitsAsShort(firstValue);
            return container.iset(firstValueLowBits);
        }
        final long lastValue = values.get(offset + length - 1);
        if (lastValue - firstValue + 1 == length) {
            // We know we're adding a contiguous range of values
            if (!existing) {
                return Container.singleRange(lowBitsAsInt(firstValue), lowBitsAsInt(lastValue) + 1);
            }
            if (container == null) {
                return new RunContainer(lowBitsAsInt(firstValue), lowBitsAsInt(lastValue) + 1)
                        .iset(lowBitsAsShort(keys[keyIdx]));
            }
            return container.iadd(lowBitsAsInt(firstValue), lowBitsAsInt(lastValue) + 1);
        }
        if (length == 2) {
            // We know we're adding exactly two items, with no contiguous range
            if (!existing) {
                return Container.twoValues(lowBitsAsShort(firstValue), lowBitsAsShort(lastValue));
            }
            if (container == null) {
                return new ArrayContainer(3)
                        .iset(lowBitsAsShort(firstValue))
                        .iset(lowBitsAsShort(lastValue))
                        .iset(lowBitsAsShort(keys[keyIdx]));
            }
            return container.iset(lowBitsAsShort(firstValue)).iset(lowBitsAsShort(lastValue));
        }
        // We're adding more than two non-contiguous values
        if (!existing) {
            return makeValuesContainer(values, offset, length).runOptimize();
        }
        if (container == null) {
            container = Container.singleton(lowBitsAsShort(keys[keyIdx]));
        }
        return addValuesToContainer(values, offset, length, container);
    }

    private static Container makeValuesContainer(final LongChunk<OrderedKeyIndices> values,
                                                 final int offset, final int length) {
        if (length <= ArrayContainer.SWITCH_CONTAINER_CARDINALITY_THRESHOLD) {
            final short[] valuesArray = new short[length];
            for (int vi = 0; vi < length; ++vi) {
                valuesArray[vi] = lowBitsAsShort(values.get(vi + offset));
            }
            return new ArrayContainer(valuesArray);
        }
        final BitmapContainer bitmapContainer = new BitmapContainer();
        for (int vi = 0; vi < length; ++vi) {
            bitmapContainer.iset(lowBitsAsShort(values.get(vi + offset)));
        }
        return bitmapContainer;
    }

    private static Container addValuesToContainer(final LongChunk<OrderedKeyIndices> values,
                                                  final int offset, final int length,
                                                  Container container) {
        if (container.getCardinality() <= length / 2) {
            return makeValuesContainer(values, offset, length).ior(container);
        }
        for (int vi = 0; vi < length; ++vi) {
            container = container.iset(lowBitsAsShort(values.get(vi + offset)));
        }
        return container;
    }

    public RspBitmap addValuesUnsafe(final PrimitiveIterator.OfLong it) {
        final RspBitmap rb = writeCheck();
        rb.addValuesUnsafeNoWriteCheck(it);
        return rb;
    }

    public void addValuesUnsafeNoWriteCheck(final PrimitiveIterator.OfLong it) {
        final AddCtx ctx = new AddCtx();
        ctx.index = -1;
        final MutableObject<SortedRanges> sortedRangesMu = getWorkSortedRangesMutableObject();
        while (it.hasNext()) {
            final long val = it.nextLong();
            final long valHighBits = highBits(val);
            if (ctx.index != -1 && highBits(ctx.key) == valHighBits) {
                if (ctx.c == null) {
                    final Container valCont = containerForTwoValues(ctx.key, val);
                    ctx.key = keys[ctx.index] = valHighBits;
                    ctx.c = valCont;
                    setSpanAtIndex(ctx.index, valCont);
                } else {
                    final Container valCont = ctx.c.iset(lowBitsAsShort(val));
                    if (valCont.isAllOnes()) {
                        setOrInsertFullBlockSpanAtIndex(ctx.index, valHighBits, 1, sortedRangesMu);
                        ctx.c = null;
                    } else if (valCont != ctx.c) {
                        setSpanAtIndex(ctx.index, valCont);
                        ctx.c = valCont;
                    } else {
                        modifiedSpan(ctx.index);
                    }
                }
            } else {
                addAndPopulateCtx(val, ctx, sortedRangesMu);
            }
        }
        collectRemovedIndicesIfAny(sortedRangesMu);
    }

    public RspBitmap addValuesUnsafe(final long[] dat, final int offset, final int n) {
        final PrimitiveIterator.OfLong it = new PrimitiveIterator.OfLong() {
            int j = offset;
            final int end = offset + n;
            @Override public boolean hasNext() {
                return j < end;
            }
            @Override public long nextLong() {
                return dat[j++];
            }
        };
        return addValuesUnsafe(it);
    }

    public RspBitmap add(final long val) {
        final RspBitmap rb = addUnsafe(val);
        rb.finishMutations();
        return rb;
    }

    // Does not update cardinality cache.  Caller must ensure finishMutations() is called before calling
    // any operation depending on the cardinality cache being up to date.
    public RspBitmap addUnsafe(final long val) {
        final RspBitmap rb = writeCheck();
        rb.addUnsafeNoWriteCheck(val);
        return rb;
    }

    public void addUnsafeNoWriteCheck(final long val) {
        addAndPopulateCtx(val, new AddCtx(), null);
    }

    // Prerequisite: keyForLastBlock <= sHigh
    // end is inclusive
    public void appendRangeUnsafeNoWriteCheck(final long sHigh, final long start, final long end) {
        appendRangeUnsafeNoWriteCheck(sHigh, start, highBits(end), end);
    }

    private void appendRangeUnsafeNoWriteCheck(final long sHigh, final long start, final long eHigh, final long end) {
        final int sLow = lowBitsAsInt(start);
        final int eLow = lowBitsAsInt(end);
        if (sHigh == eHigh) {
            singleBlockAppendRange(sHigh, start, sLow, eLow);
            return;
        }
        singleBlockAppendRange(sHigh, start, sLow, BLOCK_LAST);
        final long sHighNext = RspArray.nextKey(sHigh);
        if (sHighNext == eHigh) {
            if (eLow == BLOCK_LAST) {
                appendFullBlockSpan(sHighNext, 1);
            } else {
                if (eLow == 0) {
                    appendContainer(sHighNext, null);
                } else {
                    appendContainer(sHighNext, Container.rangeOfOnes(0, eLow + 1));
                }
            }
            return;
        }
        if (eLow < BLOCK_LAST) {
            appendFullBlockSpan(sHighNext, RspArray.distanceInBlocks(sHighNext, eHigh));
            if (eLow == 0) {
                appendContainer(eHigh, null);
            } else {
                appendContainer(eHigh, Container.rangeOfOnes(0, eLow + 1));
            }
            return;
        }
        appendFullBlockSpan(sHighNext, RspArray.distanceInBlocks(sHighNext, eHigh) + 1);
    }

    // end is inclusive.
    public RspBitmap appendRange(final long start, final long end) {
        final RspBitmap rb = appendRangeUnsafe(start, end);
        rb.finishMutations();
        return rb;
    }

    // end is inclusive.
    // Does not update cardinality cache.  Caller must ensure finishMutations() is called before calling
    // any operation depending on the cardinality cache being up to date.
    public RspBitmap appendRangeUnsafe(final long start, final long end) {
        if (start > end) {
            if (Index.BAD_RANGES_AS_ERROR) {
                throw new IllegalArgumentException("bad range start=" + start + " > end=" + end + ".");
            }
            return this;
        }
        final long sHigh = highBits(start);
        final RspBitmap rb = writeCheck();
        rb.appendRangeUnsafeNoWriteCheck(sHigh, start, end);
        return rb;
    }

    public void appendRangeUnsafeNoWriteCheck(final long start, final long end) {
        appendRangeUnsafeNoWriteCheck(highBits(start), start, end);
    }

    public void appendContainerUnsafeNoWriteCheck(final long k, final Container c) {
        if (c != null) {
            if (c.isAllOnes()) {
                appendFullBlockSpan(k, 1);
                return;
            }
            if (c.isSingleElement()) {
                final long fullKey = k | c.first();
                appendContainer(fullKey, null);
                return;
            }
        }
        appendContainer(k, c);
    }

    public void appendFullBlockSpanUnsafeNoWriteCheck(final long k, final long slen) {
        appendFullBlockSpan(k, slen);
    }

    private abstract static class AppendCtx {
        final RspArray kvs;
        long blockKey;
        // the data below is a cache that may be kept up to date between flushes; lastSpanIndex = -1 signals a
        // non-populated cache.
        int lastSpanIndex = -1;
        long lastSpanKey = -1;
        Object lastSpan = null;
        long lastSpanLen = -1;
        long lastBlockKey = -1;
        AppendCtx(final RspBitmap rb) {
            kvs = rb;
            blockKey = -1;
        }
        void loadLastSpan() {
            if (kvs.isEmpty()) {
                lastSpanIndex = -1;
                lastBlockKey = -1;
                return;
            }
            lastSpanIndex = kvs.getSize() - 1;
            lastSpanKey = highBits(kvs.keys[lastSpanIndex]);
            lastSpan = kvs.spans[lastSpanIndex];
            lastSpanLen = getFullBlockSpanLen(lastSpan);
            lastBlockKey = (lastSpanLen > 0) ?
                    lastSpanKey + (lastSpanLen - 1)*BLOCK_SIZE :
                    lastSpanKey;
        }
        void flushRuns() {
            if (blockKey == -1) {
                return;
            }
            if (lastSpanIndex == -1) {
                loadLastSpan();
            }
            if (debug) {
                Assert.geq(blockKey, "k", lastBlockKey, "lastKey");
            }
            // !(lastSpanLen > 0) {
            if (blockKey > lastBlockKey) {
                lastSpan = appendContainer();
                ++lastSpanIndex;
                lastSpanKey = lastBlockKey = blockKey;
                lastSpanLen = 0;
                blockKey = -1;
                return;
            }
            final MutableBoolean isFullBlock = new MutableBoolean(false);
            lastSpan = mergeContainer(isFullBlock);
            if (isFullBlock.booleanValue()) {
                lastSpanIndex = -1;
            }
            blockKey = -1;
        }
        void invalidateLastCache() {
            lastSpanIndex = -1;
        }
        abstract Container appendContainer();
        abstract Container mergeContainer(MutableBoolean isFullBlock);
    }

    private final static class AppendRangesCtx extends AppendCtx {
        static final int runSz = 126;
        short[] runs = null;
        int nrunsTimesTwo = 0;
        int cardinality = 0;
        AppendRangesCtx(final RspBitmap rb) {
            super(rb);
        }
        void appendRange(final long kHigh, final long start, final long end) {
            if (this.blockKey != kHigh && nrunsTimesTwo != 0) {
                flushRuns();
            }
            this.blockKey = kHigh;
            if (runs == null) {
                runs = new short[runSz];
            }
            final int s = lowBitsAsInt(start);
            final int e = lowBitsAsInt(end);
            if (nrunsTimesTwo > 0) {
                final int lastStart = unsignedShortToInt(runs[nrunsTimesTwo - 2]);
                final int lastEnd = lastStart + unsignedShortToInt(runs[nrunsTimesTwo - 1]);
                if (lastEnd + 1 == s) {
                    runs[nrunsTimesTwo - 1] = (short) (e - lastStart);
                    return;
                }
            }
            runs[nrunsTimesTwo++] = (short) s;
            final int len = e - s;
            runs[nrunsTimesTwo++] = (short) len;
            cardinality += len + 1;
            if (nrunsTimesTwo == runs.length) {
                flushRuns();
            }
        }
        Container tryImmutableContainer() {
            if (!ImmutableContainer.ENABLED) {
                return null;
            }
            final int nruns = nrunsTimesTwo/2;
            if (nruns == 1) {
                if (runs[1] == 0) {
                    throw new IllegalStateException("singleton");
                }
                final int start = unsignedShortToInt(runs[0]);
                final int endExclusive = start + unsignedShortToInt(runs[1]) + 1;
                return Container.singleRange(start, endExclusive);
            }
            if (nruns == 2 && runs[1] == 0 && runs[3] == 0) {
                return Container.twoValues(runs[0], runs[2]);
            }
            return null;
        }
        Container appendContainer() {
            if (nrunsTimesTwo == 2 && runs[1] == 0) {
                kvs.appendSingle(blockKey | unsignedShortToInt(runs[0]));
                nrunsTimesTwo = 0;
                return null;
            }
            Container c = tryImmutableContainer();
            if (c == null) {
                c = RunContainer.makeByWrapping(runs,nrunsTimesTwo /2, cardinality);
                runs = null;
            }
            kvs.appendContainer(blockKey, c);
            nrunsTimesTwo = 0;
            cardinality = 0;
            return c;
        }
        Container mergeContainer(final MutableBoolean isFullBlock) {
            if (nrunsTimesTwo == 2) {
                Container c = null;
                if (lastSpan == null) {
                    final int v = lowBitsAsInt(lastSpanKey);
                    final int rs = unsignedShortToInt(runs[0]);
                    final int re = rs + unsignedShortToInt(runs[1]);
                    c = containerForLowValueAndRange(v, rs, re);
                    if (c == null) {
                        kvs.setNullContainerAtIndex(lastSpanIndex);
                    } else {
                        kvs.keys[lastSpanIndex] = highBits(lastSpanKey);
                        kvs.setNotNullContainerAtIndex(lastSpanIndex, c);
                    }
                } else {
                    c = (Container) lastSpan;
                    final int rs = unsignedShortToInt(runs[0]);
                    final int re = rs + unsignedShortToInt(runs[1]);
                    c = c.iadd(rs, re + 1);
                    if (c != lastSpan) {
                        kvs.setNotNullContainerAtIndex(lastSpanIndex, c);
                    } else {
                        kvs.modifiedSpan(lastSpanIndex);
                    }
                }
                nrunsTimesTwo = 0;
                return c;
            }
            Container c = tryImmutableContainer();
            if (c == null) {
                c = RunContainer.makeByWrapping(runs, nrunsTimesTwo /2, cardinality);
                runs = null;
            }
            nrunsTimesTwo = 0;
            cardinality = 0;
            return kvs.mergeContainer(lastSpanIndex, c, isFullBlock);
        }
    }

    private final static class AppendValuesCtx extends AppendCtx {
        static final int valuesSz = 254;
        short[] values = null;
        int iv = 0;
        AppendValuesCtx(final RspBitmap rb) {
            super(rb);
        }
        void appendValue(final long k, final long v) {
            if (this.blockKey != k && iv != 0) {
                flushRuns();
            }
            this.blockKey = k;
            if (values == null) {
                values = new short[valuesSz];
            }
            values[iv++] = lowBitsAsShort(v);
            if (iv == values.length) {
                flushRuns();
            }
        }
        Container tryImmutableContainer() {
            if (!ImmutableContainer.ENABLED) {
                return null;
            }
            if (iv == 1) {
                throw new IllegalStateException("singleton");
            }
            if (iv == 2) {
                final int first = unsignedShortToInt(values[0]);
                final int second = unsignedShortToInt(values[1]);
                if (first + 1 == second) {
                    return Container.singleRange(first, second + 1);
                }
                return Container.twoValues(values[0], values[1]);
            }
            return null;
        }
        @Override
        Container appendContainer() {
            if (iv == 1) {
                kvs.appendSingle(blockKey | unsignedShortToInt(values[0]));
                iv = 0;
                return null;
            }
            Container c = tryImmutableContainer();
            if (c == null) {
                c = ArrayContainer.makeByCopying(values, 0, iv);
            }
            kvs.appendContainer(blockKey, c);
            iv = 0;
            return c;
        }
        @Override
        Container mergeContainer(final MutableBoolean isFullBlock) {
            if (iv == 1) {
                Container c = null;
                if (lastSpan == null) {
                    final int kv = lowBitsAsInt(lastSpanKey);
                    final int v = unsignedShortToInt(values[0]);
                    c = containerForLowValueAndRange(kv, v, v);
                    if (c == null) {
                        kvs.setNullContainerAtIndex(lastSpanIndex);
                    } else  {
                        kvs.keys[lastSpanIndex] = highBits(lastSpanKey);
                        kvs.setNotNullContainerAtIndex(lastSpanIndex, c);
                    }
                } else {
                    c = (Container) lastSpan;
                    c = c.iset(values[0]);
                    if (c != lastSpan) {
                        kvs.setNotNullContainerAtIndex(lastSpanIndex, c);
                    } else {
                        kvs.modifiedSpan(lastSpanIndex);
                    }
                }
                iv = 0;
                return c;
            }
            Container c = tryImmutableContainer();
            if (c == null) {
                c = ArrayContainer.makeByWrapping(values, iv);
                values = null;
            }
            iv = 0;
            return kvs.mergeContainer(lastSpanIndex, c, isFullBlock);
        }
    }

    public RspBitmap appendRangesUnsafe(final LongRangeIterator rit) {
        final RspBitmap rb = writeCheck();
        rb.appendRangesUnsafeNoWriteCheck(rit);
        return rb;
    }

    public void appendRangesUnsafeNoWriteCheck(final LongRangeIterator rit) {
        final AppendRangesCtx ctx = new AppendRangesCtx(this);
        while (rit.hasNext()) {
            rit.next();
            final long start = rit.start();
            final long sHigh = highBits(start);
            final long end = rit.end();
            final long eHigh = highBits(end);
            if (sHigh != eHigh) {
                ctx.flushRuns();
                appendRangeUnsafeNoWriteCheck(sHigh, start, eHigh, end);
                ctx.invalidateLastCache();
                continue;
            }
            ctx.appendRange(sHigh, start, end);
        }
        ctx.flushRuns();
    }

    public RspBitmap appendValuesUnsafe(final PrimitiveIterator.OfLong it) {
        final RspBitmap rb = writeCheck();
        rb.appendValuesUnsafeNoWriteCheck(it);
        return rb;
    }

    public void appendValuesUnsafeNoWriteCheck(final PrimitiveIterator.OfLong it) {
        final AppendValuesCtx ctx = new AppendValuesCtx(this);
        while (it.hasNext()) {
            final long v = it.nextLong();
            final long k = highBits(v);
            ctx.appendValue(k, v);
        }
        ctx.flushRuns();;
    }

    public RspBitmap append(final long v) {
        final RspBitmap rb = appendUnsafe(v);
        rb.finishMutations();
        return rb;
    }

    // Does not update cardinality cache.  Caller must ensure finishMutations() is called before
    // any operation depending on the cardinality cache being up to date are called.
    public RspBitmap appendUnsafe(final long v) {
        final RspBitmap rb = writeCheck();
        rb.appendUnsafeNoWriteCheck(v);
        return rb;
    }

    public void appendUnsafeNoWriteCheck(final long v) {
        final long sHigh = highBits(v);
        final short low = lowBits(v);
        long keyForLastBlock = 0;
        if (isEmpty() || (keyForLastBlock = keyForLastBlock()) < sHigh) {
            appendSingle(v);
            return;
        }
        if (keyForLastBlock != sHigh) {
            throw new IllegalArgumentException("Can't append v=" + v + " when keyForLastBlock=" + keyForLastBlock);
        }

        final Object span = spans[size - 1];
        if (getFullBlockSpanLen(span) > 0) {
            // if it is a full block span we already have the value.
            return;
        }
        final Container result;
        Container container = null;
        if (span == null) {
            final long key = keys[size - 1];
            if (key == v) {
                return;
            }
            if (key < v) {
                result = Container.twoValues(lowBitsAsShort(key), lowBitsAsShort(v));
            } else {
                result = Container.twoValues(lowBitsAsShort(v), lowBitsAsShort(key));
            }
            keys[size - 1] = highBits(key);
        } else {
            container = (Container) span;
            result = container.iset(low);
        }
        if (result.isAllOnes()) {
            setLastFullBlockSpan(sHigh, 1);
            return;
        }
        if (result != container) {
            replaceLastContainer(result);
        } else {
            modifiedLastSpan();
        }
    }

    /**
     * Adds the provided (start, end) range, relative to the given key, to this array.
     *
     * @param startPos the initial index from which to start the search for k
     * @param k the key to use for the range provided.
     * @param start the start of the range to add.  0 <= start < BLOCK_SIZE
     * @param end the end (inclusive) of the range to add.  0 <= end < BLOCK_SIZE
     * @return the index of the span where the interval was added.
     */
    private int
    singleBlockAddRange(final int startPos, final long kHigh, final long k, final int start, final int end) {
        final int endExclusive = end + 1;
        final int i = getSpanIndex(startPos, k);
        if (endExclusive - start == BLOCK_SIZE) {
            return setOrInsertFullBlockSpanAtIndex(i, kHigh, 1, null);
        }
        if (i < 0) {
            final int j = -i - 1;
            if (start == end) {
                insertSpanAtIndex(j, k, null);
            } else {
                insertSpanAtIndex(j, kHigh, Container.rangeOfOnes(start, endExclusive));
            }
            return j;
        }
        final Object span = spans[i];
        if (RspArray.isFullBlockSpan(span)) {
            return i;
        }
        Container container = null;
        final Container result;
        if (span == null) {
            final long key = keys[i];
            final int keyLowAsInt = lowBitsAsInt(key);
            if (start == end && start == keyLowAsInt) {
                return i;
            }
            keys[i] = highBits(key);
            if (keyLowAsInt + 1 < start) {
                result = new RunContainer(keyLowAsInt, keyLowAsInt + 1, start, endExclusive);
            } else if (keyLowAsInt + 1 == start) {
                if (endExclusive - keyLowAsInt == BLOCK_SIZE) {
                    return setOrInsertFullBlockSpanAtIndex(i, kHigh, 1, null);
                }
                result = Container.singleRange(keyLowAsInt, endExclusive);
            } else if (end + 1 < keyLowAsInt) {
                result = new RunContainer(start, endExclusive, keyLowAsInt, keyLowAsInt + 1);
            } else if (end + 1 == keyLowAsInt) {
                if (keyLowAsInt + 1 - start == BLOCK_SIZE) {
                    return setOrInsertFullBlockSpanAtIndex(i, kHigh, 1, null);
                }
                result = Container.singleRange(start, keyLowAsInt + 1);
            } else { // start <= key <= end
                result = Container.singleRange(start, endExclusive);
            }
        } else {
            container = (Container) span;
            result = container.iadd(start, endExclusive);
            if (result.isAllOnes()) {
                return setOrInsertFullBlockSpanAtIndex(i, kHigh, 1, null);
            }
        }
        if (result != container) {
            if (container == null) {
                keys[i] = kHigh;
            }
            setNotNullContainerAtIndex(i, result);
        } else {
            modifiedSpan(i);
        }
        return i;
    }


    /**
     * Appends the provided (start, end) range, relative to the given key, to this array.
     * Prerequisite: keyForLastBlock() <= k
     *
     * @param k the key to use for the range provided.
     * @param start the start of the range to add.  0 <= start < BLOCK_SIZE
     * @param end the end (inclusive) of the range to add.  0 <= end < BLOCK_SIZE
     * @return the index of the span where the interval was added.
     */
    private int
    singleBlockAppendRange(final long kHigh, final long k, final int start, final int end) {
        final int endExclusive = end + 1;
        long keyForLastBlock = 0;
        if (isEmpty() || (keyForLastBlock = keyForLastBlock()) < kHigh) {
            final int pos = size();
            if (start == end) {
                appendSingle(k);
            } else {
                if (endExclusive - start == BLOCK_SIZE) {
                    final int insertIdx = -pos - 1;
                    return setOrInsertFullBlockSpanAtIndex(insertIdx, kHigh, 1, null);
                }
                appendContainer(kHigh, Container.rangeOfOnes(start, endExclusive));
            }
            return pos;
        }
        if (keyForLastBlock == kHigh) {
            final int pos = size() - 1;
            final Object span = spans[pos];
            if (!RspArray.isFullBlockSpan(span)) {  // if it is a full block span, we already have the range.
                final Container result;
                Container container = null;
                if (span == null) {
                    final long key = keys[pos];
                    result = containerForLowValueAndRange(lowBitsAsInt(key), start, end);
                    if (result != null) {
                        keys[pos] = highBits(key);
                    }
                } else {
                    container = (Container) span;
                    result = container.iadd(start, endExclusive);
                }
                if (result != null && result.isAllOnes()) {
                    return setOrInsertFullBlockSpanAtIndex(pos, kHigh, 1, null);
                }
                if (result != container) {
                    setSpanAtIndex(pos, result);
                } else {
                    modifiedSpan(pos);
                }
            }
            return pos;
        }
        throw new IllegalArgumentException("Can't append range (k=" + k + ", start=" + start + ", end=" + end +
                ") when keyForLastBlock=" + keyForLastBlock);
    }

    public static Container containerForLowValueAndRange(final int val, final int start, final int end) {
        if (end == start) {
            return containerForTwoValues(val, start);
        }
        if (val + 1 < start) {
            return new RunContainer(val, val + 1, start, end + 1);
        }
        if (val + 1 == start) {
            return Container.singleRange(val, end + 1);
        }
        if (end + 1 < val) {
            return new RunContainer(start, end + 1, val, val + 1);
        }
        if (end + 1 == val) {
            return Container.singleRange(start, val + 1);
        }
        // start <= val <= end.
        return Container.singleRange(start, end + 1);
    }

    // Note end is exclusive; the range is open on the right.
    public RspBitmap addRangeExclusiveEnd(final long start, final long end) {
        return addRange(start, end - 1);
    }

    // end is inclusive
    public RspBitmap addRange(final long start, final long end) {
        final RspBitmap rb = addRangeUnsafe(start, end);
        rb.finishMutations();
        return rb;
    }

    // Figure out where to insert for k, starting from index i
    private int getSetOrInsertIdx(final int startIdx, final long keyToInsert) {
        final Object startIdxSpan = spans[startIdx];
        if (getFullBlockSpanLen(startIdxSpan) > 1) {
            return startIdx;
        }
        final int i = startIdx + 1;
        if (i >= size() || highBits(keys[i]) > keyToInsert) {
            return -i - 1;
        }
        return i;
    }

    // end is inclusive
    // Does not update cardinality cache.  Caller must ensure finishMutations() is called before
    // any operation depending on the cardinality cache being up to date are called.
    public RspBitmap addRangeUnsafe(final long start, final long end) {
        if (start > end) {
            if (Index.BAD_RANGES_AS_ERROR) {
                throw new IllegalArgumentException("bad range start=" + start + " > end=" + end + ".");
            }
            return this;
        }
        final RspBitmap rb = writeCheck();
        rb.addRangeUnsafeNoWriteCheck(0, start, end);
        return rb;
    }

    public void addRangeUnsafeNoWriteCheck(final long first, final long last) {
        addRangeUnsafeNoWriteCheck(0, first, last);
    }

    public int addRangeUnsafeNoWriteCheck(final int fromIdx, final long start, final long end) {
        if (start > end) {
            if (Index.BAD_RANGES_AS_ERROR) {
                throw new IllegalArgumentException("bad range start=" + start + " > end=" + end + ".");
            }
            return -1;
        }
        final long sHigh = highBits(start);
        final boolean kvsIsEmpty = isEmpty();
        if (kvsIsEmpty || sHigh >= keyForLastBlock()) {  // append case.
            appendRangeUnsafeNoWriteCheck(sHigh, start, end);
            return size - 1;
        }
        // not an append; need to lookup.
        final long eHigh = highBits(end);
        final int sLow = lowBitsAsInt(start);
        final int eLow = lowBitsAsInt(end);
        if (sHigh == eHigh) {
            return singleBlockAddRange(fromIdx, sHigh, start, sLow, eLow);
        }
        int i = singleBlockAddRange(fromIdx, sHigh, start, sLow, BLOCK_LAST);
        final long sHighNext = RspArray.nextKey(sHigh);
        final int idxForFull = getSetOrInsertIdx(i, sHighNext);
        if (sHighNext == eHigh) {
            if (eLow == BLOCK_LAST) {
                i = setOrInsertFullBlockSpanAtIndex(idxForFull, sHighNext, 1, null);
            } else {
                i = singleBlockAddRange(i, sHighNext, sHighNext, 0, eLow);
            }
            return i;
        }
        if (eLow < BLOCK_LAST) {
            final int j = setOrInsertFullBlockSpanAtIndex(
                    idxForFull, sHighNext, RspArray.distanceInBlocks(sHighNext, eHigh), null);
            return singleBlockAddRange(j, eHigh, eHigh, 0, eLow);
        }
        return setOrInsertFullBlockSpanAtIndex(
                idxForFull, sHighNext, RspArray.distanceInBlocks(sHighNext, eHigh) + 1, null);

    }

    public RspBitmap addRangesUnsafe(final Index.RangeIterator it) {
        final RspBitmap rb = writeCheck();
        rb.addRangesUnsafeNoWriteCheck(it);
        return rb;
    }

    public void addRangesUnsafeNoWriteCheck(final Index.RangeIterator rit) {
        try {
            int i = 0;
            while (rit.hasNext()) {
                rit.next();
                i = addRangeUnsafeNoWriteCheck(i, rit.currentRangeStart(), rit.currentRangeEnd());
                if (i == -1) {
                    return;
                }
            }
        } finally {
            rit.close();
        }
    }

    public boolean contains(final long val) {
        final long key = highBits(val);
        final int i = getSpanIndex(key);
        if (i < 0) {
            return false;
        }
        final Object s = spans[i];
        if (RspArray.isFullBlockSpan(s)) {
            return true;
        }
        if (s == null) {
            return keys[i] == val;
        }
        final Container c = (Container) s;
        return c.contains(lowBitsAsShort(val));
    }

    public RspBitmap remove(final long val) {
        final RspBitmap rb = removeUnsafe(val);
        rb.finishMutations();
        return rb;
    }

    public RspBitmap removeUnsafe(final long val) {
        final long key = highBits(val);
        final int i = getSpanIndex(key);
        if (i < 0) {
            return this;
        }
        final RspBitmap rb = writeCheck();
        rb.removeUnsafeNoWriteCheck(val, key, i);
        return rb;
    }

    public RspBitmap removeUnsafeNoWriteCheck(final long val) {
        final long key = highBits(val);
        final int i = getSpanIndex(key);
        if (i >= 0) {
            removeUnsafeNoWriteCheck(val, key, i);
        }
        return this;
    }

    public void removeUnsafeNoWriteCheck(final long val, final long blockKey, final int i) {
        final Object s = spans[i];
        final long flen = RspArray.getFullBlockSpanLen(s);
        if (flen == 0) {
            if (s == null) {
                final long key = keys[i];
                if (val == key) {
                    removeSpanAtIndex(i);
                }
            } else {
                final Container orig = (Container) s;
                final Container result = orig.iunset(lowBitsAsShort(val));
                if (result.isSingleElement()) {
                    keys[i] = blockKey | result.first();
                    setNullContainerAtIndex(i);
                } else if (result.isEmpty()) {
                    removeSpanAtIndex(i);
                } else if (result != orig) {
                    setNotNullContainerAtIndex(i, result);
                } else {
                    modifiedSpan(i);
                }
            }
            return;
        }
        final long spanStartKey = keys[i];
        final long spanEndKey = spanStartKey + BLOCK_SIZE *flen;  // exclusive
        final int low = lowBitsAsInt(val);
        final Container c;
        long cKey = blockKey;
        if (low == 0) {
            c = Container.rangeOfOnes(1, BLOCK_SIZE);
        } else if (low == BLOCK_LAST) {
            c = Container.rangeOfOnes(0, BLOCK_LAST);
        } else {
            final int preStart = 0;
            final int preEnd = low;  // exclusive
            final int posStart = low + 1;
            final int posEnd = BLOCK_SIZE;  // exclusive
            // Do the bigger subrange first, to avoid changing the container type unnecessarily.
            Container c2;
            if (posEnd - posStart > preEnd - preStart) {
                c2 = Container.rangeOfOnes(posStart, posEnd);
                c2 = c2.iadd(preStart, preEnd);
            } else {
                c2 = Container.rangeOfOnes(preStart, preEnd);
                c2 = c2.iadd(posStart, posEnd);
            }
            if (c2.isSingleElement()) {
                cKey = blockKey | c2.first();
                c = null;
            } else {
                c = c2;
            }
        }
        final long preflen = RspArray.distanceInBlocks(spanStartKey, blockKey);
        final long posSpanFirstKey = RspArray.nextKey(blockKey);
        final long posflen = RspArray.distanceInBlocks(posSpanFirstKey, spanEndKey);
        if (preflen > 0) {
            if (posflen > 0) {
                replaceSpanAtIndex(i, spanStartKey, preflen, cKey, c, posSpanFirstKey, posflen);
                return;
            }
            replaceSpanAtIndex(i, spanStartKey, preflen, cKey, c);
            return;
        }
        if (posflen > 0) {
            replaceSpanAtIndex(i, cKey, c, posSpanFirstKey, posflen);
            return;
        }
        if (c == null) {
            keys[i] = cKey;
            setNullContainerAtIndex(i);
        } else {
            setNotNullContainerAtIndex(i, c);
        }
    }

    // end is inclusive.
    public RspBitmap removeRange(final long start, final long end) {
        if (isEmpty() || last() < start || end < first()) {
            return this;
        }
        final RspBitmap rb = removeRangeUnsafe(start, end);
        rb.finishMutations();
        return rb;
    }

    public RspBitmap removeRangeUnsafe(final long start, final long end) {
        final RspBitmap rb = writeCheck();
        rb.removeRangeUnsafeNoWriteCheck(start, end);
        return rb;
    }

    /**
     * Return the logical or of two RspArrays as a new RspArray.
     * The arguments won't be modified.
     *
     * @param r1 an RspArray
     * @param r2 an RspArray
     * @return
     */
    private static RspBitmap orImpl(final RspBitmap r1, final RspBitmap r2) {
        final RspBitmap r;
        if (r1.size > r2.size) {
            r = r1.deepCopy();
            r.orEquals(r2);
        } else {
            r = r2.deepCopy();
            r.orEquals(r1);
        }
        return r;
    }

    /**
     * Return the logical or of two bitmaps as a new bitmap.
     * This is equivalent to the union of the two bitmaps as sets.
     * The arguments won't be modified.
     *
     * @param b1 a bitmap
     * @param b2 a bitmap
     * @return b1 or b2 as a new bitmap.
     */
    public static RspBitmap or(final RspBitmap b1, final RspBitmap b2) {
        final RspBitmap rb = orImpl(b1, b2);
        rb.finishMutations();
        return rb;
    }

    /**
     * Add every element on other to this bitmap.
     */
    public RspBitmap orEquals(final RspBitmap other) {
       final RspBitmap rb = orEqualsUnsafe(other);
       rb.finishMutations();
       return rb;
    }

    /**
     * For every key on other, add (key + shiftAmount) to this bitmap.
     */
    public RspBitmap orEqualsShifted(final long shiftAmount, final RspBitmap other) {
        final RspBitmap rb = orEqualsShiftedUnsafe(shiftAmount, other);
        rb.finishMutations();
        return rb;
    }

    /**
     * Add every element on other to this bitmap.
     * Does not update cardinality cache.  Caller must ensure finishMutations() is called before
     * any operation depending on the cardinality cache being up to date are called.
     */
    public RspBitmap orEqualsUnsafe(final RspBitmap other) {
        if (other.isEmpty()) {
            return this;
        }
        final RspBitmap rb = writeCheck();
        rb.orEqualsUnsafeNoWriteCheck(other);
        return rb;
    }

    /**
     * For every key on other, add (key + shiftAmount) to this bitmap.
     * Note shiftAmount is assumed to be a multiple of BLOCK_SIZE.
     * Does not update cardinality cache.  Caller must ensure finishMutations() is called before
     * any operation depending on the cardinality cache being up to date are called.
     */
    public RspBitmap orEqualsShiftedUnsafe(final long shiftAmount, final RspBitmap other) {
        if (other.isEmpty()) {
            return this;
        }
        final RspBitmap rb = writeCheck();
        rb.orEqualsShiftedUnsafeNoWriteCheck(shiftAmount, other);
        return rb;
    }

    public void appendShiftedUnsafeNoWriteCheck(final long shiftAmount, final RspArray other, final boolean acquire) {
        if ((shiftAmount & BLOCK_LAST) == 0) {
            if (tryAppendShiftedUnsafeNoWriteCheck(shiftAmount, other, acquire)) {
                return;
            }
        } else if (lastValue() < other.firstValue() + shiftAmount) {
            other.forEachLongRange((final long start, final long end) -> {
                appendRangeUnsafeNoWriteCheck(start + shiftAmount, end + shiftAmount);
                return true;
            });
            return;
        }
        throw new IllegalArgumentException(
                "Cannot append index with shiftAmount=" + shiftAmount + ", firstKey=" + other.firstValue() +
                        " when our lastValue=" + lastValue());

    }

    /**
     * Return the logical and of r1 and r2 as a new RspArray.
     *
     * @param r1 an RspArray.
     * @param r2 an RspArray.
     * @return r1 and r2 as a new RspArray.
     */
    private static RspBitmap andImpl(final RspBitmap r1, final RspBitmap r2) {
        if (r1.isEmpty() || r2.isEmpty()) {
            return new RspBitmap();
        }
        if (r1.size < r2.size) {
            final RspBitmap r = r1.deepCopy();
            r.andEquals(r2);
            return r;
        }
        final RspBitmap r = r2.deepCopy();
        r.andEquals(r1);
        return r;
    }

    /**
     * Return the logical and of two bitmaps as a new bitmap.
     * This is equivalent to the intersection of the two bitmaps as sets.
     *
     * @param b1 a bitmap
     * @param b2 a bitmap
     * @return b1 and b2 as a new bitmap.
     */
    public static RspBitmap and(final RspBitmap b1, final RspBitmap b2) {
        final RspBitmap rb = andImpl(b1, b2);
        rb.finishMutations();
        return rb;
    }

    /**
     * Removes every element from this bitmap that is not in the other bitmap.
     */
    public RspBitmap andEquals(final RspBitmap other) {
        final RspBitmap rb = andEqualsUnsafe(other);
        rb.finishMutations();
        return rb;
    }

    public RspBitmap andEqualsUnsafe(final RspBitmap other) {
        final RspBitmap rb = writeCheck();
        rb.andEqualsUnsafeNoWriteCheck(other);
        return rb;
    }

    /**
     * Return the logical result of r1 and not r2 as a new RspArray.
     * The arguments won't be modified.
     *
     * @param r1 an RspArray
     * @param r2 an RspArray
     * @return r1 and not r2 as a new RspArray.
     */
    public static RspBitmap andNotImpl(final RspBitmap r1, final RspBitmap r2) {
        final RspBitmap r = r1.deepCopy();
        r.andNotEqualsUnsafeNoWriteCheck(r2);
        return r;
    }

    /**
     * Return the logical result of r1 and not r2 as a new bitmap.
     * This is equivalent to removing every element in b2 from b1.
     * The arguments won't be modified.
     *
     * @param b1 a bitmap
     * @param b2 a bitmap
     * @return b1 and not b2 as a new bitmap.
     */
    public static RspBitmap andNot(final RspBitmap b1, final RspBitmap b2) {
        final RspBitmap rb = andNotImpl(b1, b2);
        rb.finishMutations();
        return rb;
    }

    /**
     * Updates the bitmap by adding and removing the bitmaps given as parameter.
     *
     * @param added    Elements to add. Assumed disjoint with removed.
     * @param removed  Elements to remove. Assumed disjoint with added.
     */
    public RspBitmap update(final RspBitmap added, final RspBitmap removed) {
        final RspBitmap rb = updateUnsafe(added, removed);
        rb.finishMutations();
        return rb;
    }

    public RspBitmap updateUnsafe(final RspBitmap added, final RspBitmap removed) {
        if (debug) {
            if (added.overlaps((removed))) {
                throw new IllegalArgumentException(("index update: added overlaps with removed."));
            }
        }
        final RspBitmap rb = writeCheck();
        rb.updateUnsafeNoWriteCheck(added, removed);
        return rb;
    }

    public void updateUnsafeNoWriteCheck(final RspBitmap added, final RspBitmap removed) {
        andNotEqualsUnsafeNoWriteCheck(removed);
        orEqualsUnsafeNoWriteCheck(added);
    }

    public RspBitmap andNotEquals(final RspBitmap other) {
        final RspBitmap rb = andNotEqualsUnsafe(other);
        rb.finishMutations();
        return rb;
    }

    /**
     * Remove every element in other from this bitmap.
     *
     */
    public RspBitmap andNotEqualsUnsafe(final RspBitmap other) {
        if (other.isEmpty()) {
            return this;
        }
        final RspBitmap rb = writeCheck();
        rb.andNotEqualsUnsafeNoWriteCheck(other);
        return rb;
    }

    /**
     * Apply an offset to every value in this bitmap, mutating it.
     *
     * @param offset The offset to apply.
     */
    public RspBitmap applyOffset(final long offset) {
        return applyOffsetImpl(offset, this::self, this::writeCheck);
    }

    public RspBitmap applyOffsetNoWriteCheck(final long offset) {
        return applyOffsetImpl(offset, this::self, this::self);
    }

    /**
     * Apply an offset to every value in this bitmap, returning a new bitmap (original is not changed).
     *
     * @param offset The offset to apply.
     */
    public RspBitmap applyOffsetOnNew(final long offset) {
        return applyOffsetImpl(offset, this::cowRef, this::deepCopy);
    }

    public RspBitmap applyOffsetImpl(
            final long offset, final Supplier<RspBitmap> onZeroOffset, final Supplier<RspBitmap> onAlignedOffset) {
        if (offset == 0) {
            return onZeroOffset.get();
        }
        if ((offset & BLOCK_LAST) == 0) {
            final RspBitmap ans = onAlignedOffset.get();
            ans.applyKeyOffset(offset);
            ans.ifDebugValidate();
            return ans;
        }
        final RspBitmap rb = new RspBitmap();
        final RspRangeIterator it = getRangeIterator();
        int i = 0;
        while (it.hasNext()) {
            it.next();
            final long s = it.start();
            final long e = it.end();
            i = rb.addRangeUnsafeNoWriteCheck(i,s + offset, e + offset);
        }
        rb.finishMutations();
        return rb;
    }

    // lastPos is inclusive
    public RspBitmap subrangeByPos(final long firstPos, final long lastPos) {
        final RspBitmap rb = subrangeByPosInternal(firstPos, lastPos);
        if (rb == null) {
            return new RspBitmap();
        }
        return rb;
    }

    // end is inclusive.
    public RspBitmap subrangeByValue(final long start, final long end) {
        if (isEmpty() || (start <= first() && last() <= end)) {
            return cowRef();
        }
        final RspBitmap rb = subrangeByKeyInternal(start, end);
        rb.finishMutationsAndOptimize();
        return rb;
    }

    public void invert(final LongRangeConsumer builder, final Index.RangeIterator it, final long maxPos) {
        if (!it.hasNext()) {
            return;
        }
        int startIndex = 0;
        it.next();
        int knownIdx = 0;
        long knownBeforeCard = 0;
        SPANS_LOOP:
        while (true) {
            final long startHiBits = highBits(it.currentRangeStart());
            final int i = getSpanIndex(startIndex, startHiBits);
            if (i < 0) {
                throw new IllegalArgumentException("invert for non-existing key:" + it.currentRangeStart());
            }
            final long prevCap;
            if (acc == null) {
                prevCap = cardinalityBeforeNoAcc(i, knownIdx, knownBeforeCard);
                knownIdx = i;
                knownBeforeCard = prevCap;
            } else {
                prevCap = cardinalityBeforeWithAcc(i);
            }
            if (prevCap - 1 >= maxPos) {
                return;
            }
            final Object s = spans[i];
            final long k = keys[i];
            final long flen = getFullBlockSpanLen(s);
            if (flen > 0) {
                final long spanCard = flen * BLOCK_SIZE;
                final long sLastPlusOne = k + spanCard;
                while (true) {
                    final long startPos = prevCap + it.currentRangeStart() - k;
                    if (startPos > maxPos) {
                        return;
                    }
                    final long end = uMin(sLastPlusOne - 1, it.currentRangeEnd());
                    final long endPos = prevCap + end - k;
                    if (endPos > maxPos) {
                        builder.accept(startPos, maxPos);
                        return;
                    }
                    builder.accept(startPos, endPos);
                    if (it.currentRangeEnd() >= sLastPlusOne) {
                        it.postpone(sLastPlusOne);
                        startIndex = i + 1;
                        if (acc == null) {
                            knownIdx = startIndex;
                            knownBeforeCard += spanCard;
                        }
                        continue SPANS_LOOP;
                    }
                    if (!it.hasNext()) {
                        return;
                    }
                    it.next();
                    if (it.currentRangeStart() >= sLastPlusOne) {
                        startIndex = i + 1;
                        if (acc == null) {
                            knownIdx = startIndex;
                            knownBeforeCard += spanCard;
                        }
                        continue SPANS_LOOP;
                    }
                }
            }
            final Container c;
            if (s == null) {
                c = Container.singleton(lowBitsAsShort(k));
            } else {
                c = (Container) s;
            }
            final RangeConsumer rc = (final int rs, final int re) -> {
                final long start = prevCap + rs;
                final long end = prevCap + re;
                builder.accept(start, end - 1);
            };
            final int rMaxPos = (int) uMin(maxPos - prevCap, BLOCK_SIZE);
            final IndexRangeIteratorView rv = new IndexRangeIteratorView(it, startHiBits, startHiBits + BLOCK_SIZE);
            final boolean maxReached = c.findRanges(rc, rv, rMaxPos);
            if (maxReached || rv.underlyingIterFinished()) {
                return;
            }
            startIndex = i + 1;
            if (acc == null) {
                knownIdx = startIndex;
                knownBeforeCard += c.getCardinality();
            }
        }
    }

    private static int long2hash(final long v) {
        return (int)(v ^ (v >>> 32));
    }

    // Simple minded hashCode and equals implementations, intended for testing.
    @Override public int hashCode() {
        int r = 17;
        if (!isEmpty()) {
            r = 31*r + long2hash(getCardinality());
            r = 31*r + long2hash(last());
        }
        return r;
    }

    @Override public boolean equals(final Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof RspBitmap)) {
            return false;
        }
        final RspBitmap other = (RspBitmap) o;
        if (getCardinality() != other.getCardinality()) {
            return false;
        }
        final RspRangeIterator it = getRangeIterator();
        final RspRangeIterator oit = other.getRangeIterator();
        while (it.hasNext()) {
            if (!oit.hasNext()) {
                return false;
            }
            it.next();
            oit.next();
            if (it.start() != oit.start()) {
                return false;
            }
            if (it.end() != oit.end()) {
                return false;
            }
        }
        // no need to check for oit.hasNext() since we checked for cardinality already.
        return true;
    }

    public void finishMutations() {
        ensureCardinalityCache();
    }

    public void finishMutationsAndOptimize() {
        ensureAccAndOptimize();
    }

    /*
     *
     * =============
     * TreeIndexImpl
     * =============
     *
     */

    @Override
    public RspBitmap ixCowRef() {
        return cowRef();
    }

    @Override
    public RspBitmap ixInsert(final long key) {
        return add(key);
    }

    @Override
    public void ixRelease() {
        release();
    }

    @VisibleForTesting @Override
    public int ixRefCount() {
        return refCount();
    }

    @Override
    public RspBitmap ixInsertRange(final long startKey, final long endKey) {
        return addRange(startKey, endKey);
    }

    @Override
    public final TreeIndexImpl ixInsertSecondHalf(final LongChunk<OrderedKeyIndices> values,
                                                  final int offset,  final int length) {
        final RspBitmap ans = addValuesUnsafe(values, offset, length);
        ans.finishMutations();
        return ans;
    }

    @Override
    public final TreeIndexImpl ixRemoveSecondHalf(final LongChunk<OrderedKeyIndices> values,
                                                  final int offset, final int length) {
        return ixRemove(TreeIndexImpl.fromChunk(values, offset, length, true));
    }

    @Override
    public RspBitmap ixAppendRange(final long startKey, final long endKey) {
        return appendRange(startKey, endKey);
    }

    @Override
    public RspBitmap ixRemove(final long key) {
        return remove(key);
    }

    @Override
    public long ixLastKey() {
        return isEmpty() ? Index.NULL_KEY : last();
    }

    @Override
    public long ixFirstKey() {
        return isEmpty() ? Index.NULL_KEY : first();
    }

    @Override
    public long ixGet(final long pos) {
        if (pos < 0) {
            return Index.NULL_KEY;
        }
        return get(pos);
    }

    @Override
    public void ixGetKeysForPositions(final PrimitiveIterator.OfLong inputPositions, final LongConsumer outputKeys) {
        getKeysForPositions(inputPositions, outputKeys);
    }

    @Override
    public long ixFind(final long key) {
        return find(key);
    }

    @Override
    public long ixCardinality() {
        return getCardinality();
    }

    @Override
    public boolean ixIsEmpty() {
        return isEmpty();
    }

    @Override
    public TreeIndexImpl ixInvertOnNew(final TreeIndexImpl keys, final long maximumPosition) {
        if (keys.ixIsEmpty()) {
            return TreeIndexImpl.EMPTY;
        }
        if (keys instanceof SingleRange) {
            final long pos = ixFind(keys.ixFirstKey());
            if (pos < 0) {
                throw new IllegalArgumentException("invert for non-existing key:" + keys.ixFirstKey());
            }
            if (pos > maximumPosition) {
                return TreeIndexImpl.EMPTY;
            }
            return SingleRange.make(pos, Math.min(pos + keys.ixCardinality() - 1, maximumPosition));
        }
        try (final Index.RangeIterator rit = keys.ixRangeIterator()) {
            final TreeIndexImpl.SequentialBuilder builder = new TreeIndexImplSequentialBuilder();
            invert(builder, rit, maximumPosition);
            return builder.getTreeIndexImpl();
        }
    }

    @Override
    public boolean ixForEachLong(final LongAbortableConsumer lc) {
        return forEachLong(lc);
    }

    @Override
    public boolean ixForEachLongRange(final LongRangeAbortableConsumer lc) {
        return forEachLongRange(lc);
    }

    // the range [startPos, endPosExclusive) is closed on the left and open on the right.
    @Override
    public TreeIndexImpl ixSubindexByPosOnNew(final long startPos, final long endPosExclusive) {
        final long endPos = endPosExclusive - 1;  // make inclusive.
        if (endPos < startPos || endPos < 0) {
            return TreeIndexImpl.EMPTY;
        }
        long effectiveStartPos = Math.max(0, startPos);
        return subrangeByPos(effectiveStartPos, endPos);
    }

    @Override
    public TreeIndexImpl ixSubindexByKeyOnNew(long startKey, final long endKey) {
        if (endKey < startKey || endKey < 0) {
            return TreeIndexImpl.EMPTY;
        }
        startKey = Math.max(0, startKey);
        return subrangeByValue(startKey, endKey);
    }

    // API assumption: added and removed are disjoint.
    @Override
    public TreeIndexImpl ixUpdate(final TreeIndexImpl added, final TreeIndexImpl removed) {
        if (added.ixIsEmpty()) {
            if (removed.ixIsEmpty()) {
                return this;
            }
            return ixRemove(removed);
        }
        if (removed.ixIsEmpty()) {
            return ixInsert(added);
        }
        return getWriteRef().ixUpdateNoWriteCheck(added, removed);
    }

    public TreeIndexImpl ixUpdateNoWriteCheck(final TreeIndexImpl added, final TreeIndexImpl removed) {
        if (added instanceof SingleRange) {
            addRangeUnsafeNoWriteCheck(added.ixFirstKey(), added.ixLastKey());
            if (removed instanceof SingleRange) {
                removeRangeUnsafeNoWriteCheck(removed.ixFirstKey(),removed.ixLastKey());
            } else if (removed instanceof SortedRanges) {
                removeRangesUnsafeNoWriteCheck(removed.ixRangeIterator());
            } else {
                andNotEqualsUnsafeNoWriteCheck((RspBitmap) removed);
            }
        } else if (removed instanceof SingleRange) {
            removeRangeUnsafeNoWriteCheck(removed.ixFirstKey(), removed.ixLastKey());
            if (added instanceof SortedRanges) {
                addRangesUnsafeNoWriteCheck(added.ixRangeIterator());
            } else {
                orEqualsUnsafeNoWriteCheck((RspBitmap) added);
            }
        } else if (added instanceof  RspBitmap && removed instanceof RspBitmap) {
            updateUnsafeNoWriteCheck((RspBitmap) added, (RspBitmap) removed);
        } else {
            final TreeIndexImpl ans = ixRemoveNoWriteCheck(removed);
            return ans.ixInsert(added);
        }
        if (isEmpty()) {
            return TreeIndexImpl.EMPTY;
        }
        finishMutations();
        return this;
    }


    @Override
    public RspBitmap ixInsert(final TreeIndexImpl other) {
        if (other.ixIsEmpty()) {
            return this;
        }
        return getWriteRef().ixInsertNoWriteCheck(other);
    }

    public RspBitmap ixInsertNoWriteCheck(final TreeIndexImpl other) {
        if (other instanceof SingleRange) {
            insertTreeIndexUnsafeNoWriteCheck((SingleRange) other);
        } else if (other instanceof SortedRanges) {
            insertTreeIndexUnsafeNoWriteCheck((SortedRanges) other);
        } else {
            insertTreeIndexUnsafeNoWriteCheck((RspBitmap) other);
        }
        finishMutations();
        return this;
    }

    public void insertTreeIndexUnsafeNoWriteCheck(final SingleRange ix) {
        addRangeUnsafeNoWriteCheck(0, ix.ixFirstKey(), ix.ixLastKey());
    }

    public void insertTreeIndexUnsafeNoWriteCheck(final SortedRanges sr) {
        addRangesUnsafeNoWriteCheck(sr.getRangeIterator());
    }

    public void insertTreeIndexUnsafeNoWriteCheck(final RspBitmap rb) {
        orEqualsUnsafeNoWriteCheck(rb);
    }

    @Override
    public TreeIndexImpl ixRemove(final TreeIndexImpl other) {
        if (isEmpty()) {
            return TreeIndexImpl.EMPTY;
        }
        if (other.ixIsEmpty()) {
            return this;
        }
        return getWriteRef().ixRemoveNoWriteCheck(other);
    }

    public TreeIndexImpl ixRemoveNoWriteCheck(final TreeIndexImpl other) {
        if (other instanceof SingleRange) {
            removeRangeUnsafeNoWriteCheck(other.ixFirstKey(), other.ixLastKey());
        } else if (other instanceof SortedRanges) {
            removeRangesUnsafeNoWriteCheck(other.ixRangeIterator());
        } else {
            andNotEqualsUnsafeNoWriteCheck((RspBitmap) other);
        }
        if (isEmpty()) {
            return TreeIndexImpl.EMPTY;
        }
        finishMutations();
        return this;
    }

    @Override
    public TreeIndexImpl ixRetain(final TreeIndexImpl other) {
        return retainImpl(other, this::getWriteRef);
    }

    public TreeIndexImpl ixRetainNoWriteCheck(final TreeIndexImpl other) {
        return retainImpl(other, () -> this);
    }

    private TreeIndexImpl retainImpl(final TreeIndexImpl other, Supplier<RspBitmap> refSupplier) {
        if (isEmpty() || other.ixIsEmpty() || last() < other.ixFirstKey() || other.ixLastKey() < first()) {
            return TreeIndexImpl.EMPTY;
        }
        if (other instanceof SingleRange) {
            return refSupplier.get().ixRetainRange(other.ixFirstKey(), other.ixLastKey());
        }
        if (other instanceof SortedRanges) {
            final SortedRanges sr = (SortedRanges) other;
            final TreeIndexImpl ans = sr.intersectOnNew(this);
            return (ans != null) ? ans : retainImpl(sr.toRsp(), refSupplier);
        }
        final RspBitmap o = (RspBitmap) other;
        return retainImpl(o, refSupplier);
    }

    private static TreeIndexImpl retainImpl(final RspBitmap other, Supplier<RspBitmap> refSupplier) {
        final RspBitmap ans = refSupplier.get();
        ans.andEqualsUnsafeNoWriteCheck(other);
        if (ans.isEmpty()) {
            return TreeIndexImpl.EMPTY;
        }
        ans.finishMutations();
        return ans;
    }

    @Override
    public TreeIndexImpl ixRetainRange(final long start, final long end) {
        if (ixIsEmpty()) {
            return TreeIndexImpl.EMPTY;
        }
        final long myFirstKey = ixFirstKey();
        final long myLastKey = ixLastKey();
        if (myLastKey < start || end < myFirstKey) {
            return TreeIndexImpl.EMPTY;
        }
        boolean mayHaveChanged = false;
        RspBitmap ans = this;
        if (end < myLastKey) {
            mayHaveChanged = true;
            ans = ans.removeRangeUnsafe(end + 1, myLastKey);
        }
        if (myFirstKey < start) {
            if (!mayHaveChanged) {
                mayHaveChanged = true;
                // start can't be 0 given the if condition above.
                ans = ans.removeRangeUnsafe(myFirstKey, start - 1);
            } else {
                ans.removeRangeUnsafeNoWriteCheck(myFirstKey, start - 1);
            }
        }
        if (mayHaveChanged) {
            if (ans.isEmpty()) {
                return TreeIndexImpl.EMPTY;
            }
            ans.finishMutations();
            return ans;
        }
        return this;
    }

    public TreeIndexImpl ixRetainRangeNoWriteCheck(final long start, final long end) {
        boolean mayHaveChanged = false;
        if (end < ixLastKey()) {
            mayHaveChanged = true;
            removeRangeUnsafeNoWriteCheck(end + 1, ixLastKey());
        }
        if (ixFirstKey() < start) {
            mayHaveChanged = true;
            // start can't be 0 given the if condition above.
            removeRangeUnsafeNoWriteCheck(ixFirstKey(), start - 1);
        }
        if (mayHaveChanged) {
            if (isEmpty()) {
                return TreeIndexImpl.EMPTY;
            }
            finishMutations();
        }
        return this;
    }

    @Override
    public TreeIndexImpl ixRemoveRange(final long startKey, final long endKey) {
        if (isEmpty()) {
            return TreeIndexImpl.EMPTY;
        }
        final RspBitmap rb = removeRangeUnsafe(startKey, endKey);
        if (rb.isEmpty()) {
            return TreeIndexImpl.EMPTY;
        }
        rb.finishMutations();
        return rb;
    }

    @Override
    public TreeIndexImpl ixIntersectOnNew(final TreeIndexImpl other) {
        if (other.ixIsEmpty()) {
            return TreeIndexImpl.EMPTY;
        }
        if (other instanceof SingleRange) {
            return ixSubindexByKeyOnNew(other.ixFirstKey(), other.ixLastKey());
        }
        if (other instanceof SortedRanges) {
            final SortedRanges sr = (SortedRanges) other;
            return sr.intersectOnNew(this);
        }
        return RspBitmap.and(this, (RspBitmap) other);
    }

    @Override
    public boolean ixContainsRange(final long start, final long end) {
        return containsRange(start, end);
    }

    @Override
    public boolean ixOverlaps(final TreeIndexImpl other) {
        if (other.ixIsEmpty()) {
            return false;
        }
        if (other instanceof SingleRange) {
            return overlapsRange(other.ixFirstKey(), other.ixLastKey());
        }
        if (other instanceof SortedRanges) {
            final SortedRanges sr = (SortedRanges) other;
            return sr.overlaps(ixRangeIterator());
        }
        final RspBitmap o = (RspBitmap) other;
        return overlaps(o);
    }

    @Override
    public boolean ixOverlapsRange(final long start, final long end) {
        return overlapsRange(start, end);
    }

    public boolean subsetOf(final SortedRanges sr) {
        if (isEmpty()) {
            return true;
        }
        if (sr.isEmpty()) {
            return false;
        }
        // Take the complement sr, and see if we have any elements in it, which would make the return false.
        // If no element of us is in the complement of sr, return true.
        if (first() < sr.first() || sr.last() < last()) {
            return false;
        }
        long pendingLast = -1;
        final Index.RangeIterator it = sr.getRangeIterator();
        int i = 0;
        while (it.hasNext()) {
            it.next();
            final long start = it.currentRangeStart();
            if (pendingLast != -1) {
                i = overlapsRange(i, pendingLast + 1, start - 1);
                if (i >= 0) {
                    return false;
                }
                i = ~i;
            }
            pendingLast = it.currentRangeEnd();
        }
        return true;
    }

    @Override
    public boolean ixSubsetOf(final TreeIndexImpl other) {
        if (ixIsEmpty()) {
            return true;
        }
        if (other.ixIsEmpty()) {
            return false;
        }
        if (other instanceof SingleRange) {
            return other.ixFirstKey() <= ixFirstKey() && ixLastKey() <= other.ixLastKey();
        }
        if (other instanceof SortedRanges) {
            return subsetOf((SortedRanges) other);
        }
        return subsetOf((RspBitmap) other);
    }

    @Override
    public TreeIndexImpl ixMinusOnNew(final TreeIndexImpl other) {
        if (other.ixIsEmpty()) {
            return cowRef();
        }
        if (other instanceof SingleRange) {
            if (other.ixFirstKey() <= ixFirstKey() && ixLastKey() <= other.ixLastKey()) {
                return TreeIndexImpl.EMPTY;
            }
            final RspBitmap ans = deepCopy();
            ans.removeRangeUnsafeNoWriteCheck(other.ixFirstKey(), other.ixLastKey());
            ans.finishMutations();
            return ans;
        }
        if (other instanceof SortedRanges) {
            final RspBitmap ans = deepCopy();
            final SortedRanges sr = (SortedRanges) other;
            ans.removeRangesUnsafeNoWriteCheck(sr.getRangeIterator());
            ans.finishMutations();
            return ans;
        }
        return RspBitmap.andNot(this, (RspBitmap) other);
    }

    @Override
    public TreeIndexImpl ixUnionOnNew(final TreeIndexImpl other) {
        if (isEmpty()) {
            return other.ixCowRef();
        }
        if (other.ixIsEmpty()) {
            return this.cowRef();
        }
        if (other instanceof SingleRange) {
            if (other.ixFirstKey() <= ixFirstKey() && ixLastKey() <= other.ixLastKey()) {
                return other.ixCowRef();
            }
            final RspBitmap b = deepCopy();
            b.addRangeUnsafeNoWriteCheck(0, other.ixFirstKey(), other.ixLastKey());
            b.finishMutations();
            return b;
        }
        if (other instanceof SortedRanges) {
            return other.ixUnionOnNew(this);
        }
        return RspBitmap.or(this, (RspBitmap) other);
    }

    @Override
    public RspBitmap ixShiftOnNew(final long shiftAmount) {
        return applyOffsetOnNew(shiftAmount);
    }

    @Override
    public RspBitmap ixShiftInPlace(final long shiftAmount) {
        return applyOffset(shiftAmount);
    }

    public TreeIndexImpl ixInsertWithShift(final long shiftAmount, final SortedRanges sr) {
        final RspBitmap ans = getWriteRef();
        int i = 0;
        try (final ReadOnlyIndex.RangeIterator rit = sr.getRangeIterator()) {
            while (rit.hasNext()) {
                rit.next();
                final long start = rit.currentRangeStart() + shiftAmount;
                final long end = rit.currentRangeEnd() + shiftAmount;
                i = ans.addRangeUnsafeNoWriteCheck(i, start, end);
            }
        }
        ans.finishMutations();
        return ans;
    }

    @Override
    public TreeIndexImpl ixInsertWithShift(final long shiftAmount, final TreeIndexImpl other) {
        if (other.ixIsEmpty()) {
            return this;
        }
        if (isEmpty()) {
            return other.ixShiftOnNew(shiftAmount);
        }
        if (other instanceof  SingleRange) {
            return addRange(other.ixFirstKey() + shiftAmount, other.ixLastKey() + shiftAmount);
        }
        if (other instanceof SortedRanges) {
            return ixInsertWithShift(shiftAmount, (SortedRanges) other);
        }
        if ((shiftAmount & BLOCK_LAST) != 0) {
            RspBitmap rspOther = (RspBitmap) other;
            rspOther = rspOther.applyOffsetOnNew(shiftAmount);
            final RspBitmap ans = getWriteRef();
            ans.insertTreeIndexUnsafeNoWriteCheck(rspOther);
            ans.finishMutations();
            return ans;
        }
        return orEqualsShifted(shiftAmount, (RspBitmap) other);
    }

    private static class SearchIteratorImpl implements Index.SearchIterator {
        private final RspRangeIterator it;
        private long curr = 0;
        private long next = 0;
        private long currRangeEnd = -1;

        public SearchIteratorImpl(final RspBitmap rb) {
            it = rb.getRangeIterator();
        }

        @Override
        public void close() {
            it.close();
        }
        @Override
        public boolean hasNext() {
            if (next <= currRangeEnd) {
                return true;
            }
            return it.hasNext();
        }
        @Override
        public long currentValue() {
            return curr;
        }
        @Override
        public long nextLong() {
            if (next <= currRangeEnd) {
                curr = next++;
            } else {
                it.next();
                curr = it.start();
                next = curr + 1;
                currRangeEnd = it.end();
            }
            return curr;
        }
        @Override
        public boolean advance(final long v) {
            if (currRangeEnd == -1) {  // not-started-yet iterator
                if (!it.hasNext()) {
                    return false;
                }
                it.next();
                curr = it.start();
                next = curr + 1;
                currRangeEnd = it.end();
            }
            if (v <= currRangeEnd) {
                if (v > curr) {
                    curr = v;
                    next = curr + 1;
                }
                return true;
            }
            if (it.advance(v)) {
                if (v < it.start()) {
                    curr = it.start();
                } else {
                    curr = v;
                }
                currRangeEnd = it.end();
                next = curr + 1;
                return true;
            }
            // it.hasNext() == false since it.advance(v) returned false.
            next = 0;
            currRangeEnd = -1;
            return false;
        }
        @Override
        public long binarySearchValue(final Index.TargetComparator tc, final int dir){
            if (currRangeEnd == -1) {  // not-started-yet iterator
                if (!it.hasNext()) {
                    return -1;
                }
                it.next();
                curr = next = it.start();
                currRangeEnd = it.end();
            }
            final IndexUtilities.Comparator comp = (long k) -> tc.compareTargetTo(k, dir);
            int c = comp.directionToTargetFrom(curr);
            if (c < 0) {
                return -1;
            }
            it.search(comp);
            curr = it.start();
            next = curr + 1;
            currRangeEnd = it.end();
            return curr;
        }
    }

    @Override
    public Index.SearchIterator ixSearchIterator() {
        return new SearchIteratorImpl(this);
    }

    private static class IteratorImpl implements Index.Iterator {
        private final RspIterator it;
        public IteratorImpl(final RspBitmap rb) {
            it = rb.getIterator();
        }
        @Override public void close() {
            it.release();
        }
        @Override public boolean forEachLong(final LongAbortableConsumer lc) {
            return it.forEachLong(lc);
        }
        @Override public boolean hasNext() {
            return it.hasNext();
        }
        @Override public long nextLong() {
            return it.nextLong();
        }
    }

    @Override
    public Index.Iterator ixIterator() {
        return new IteratorImpl(this);
    }

    @Override
    public Index.SearchIterator ixReverseIterator() {
        return new Index.SearchIterator() {
            final RspReverseIterator it = getReverseIterator();
            @Override
            public void close() {
                it.release();
            }
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }
            @Override
            public long currentValue() {
                return it.current();
            }
            @Override
            public long nextLong() {
                it.next();
                return it.current();
            }
            @Override
            public boolean advance(long v) {
                return it.advance(v);
            }
            @Override
            public long binarySearchValue(Index.TargetComparator targetComparator, int direction) {
                throw new UnsupportedOperationException("Reverse iterator does not support binary search.");
            }
        };
    }

    @Override
    public Index.RangeIterator ixRangeIterator() {
        return new Index.RangeIterator() {
            final RspRangeIterator it = getRangeIterator();
            @Override
            public void close() {
                it.close();
            }
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }
            @Override
            public boolean advance(final long v) {
                return it.advance(v);
            }
            @Override
            public void postpone(final long v) {
                it.postpone(v);
            }
            @Override
            public long currentRangeStart() {
                return it.start();
            }
            @Override
            public long currentRangeEnd() {
                return it.end();
            }
            @Override
            public long next() {
                it.next();
                return it.start();
            }
        };
    }

    @Override
    public TreeIndexImpl ixCompact() {
        final TreeIndexImpl timpl = tryCompact();
        if (timpl != null) {
            return timpl;
        }
        return this;
    }

    @Override
    public void ixValidate(final String failMsg) {
        validate(failMsg);
    }

    @Override
    public OrderedKeys ixGetOrderedKeysByPosition(final long startPositionInclusive, final long length) {
        return getOrderedKeysByPosition(startPositionInclusive, length);
    }

    @Override
    public OrderedKeys ixGetOrderedKeysByKeyRange(final long startKeyInclusive, final long endKeyInclusive) {
        return getOrderedKeysByKeyRange(startKeyInclusive, endKeyInclusive);
    }

    @Override
    public OrderedKeys.Iterator ixGetOrderedKeysIterator() {
        return getOrderedKeysIterator();
    }

    @Override
    public long ixRangesCountUpperBound() {
        return rangesCountUpperBound();
    }

    @Override
    public long ixGetAverageRunLengthEstimate() {
        return getAverageRunLengthEstimate();
    }

    @Override
    public RspBitmap ixToRspOnNew() {
        return cowRef();
    }

    @Override
    public String toString() {
        return valuesToString();
    }

    public static class RandomBuilder implements TreeIndexImpl.RandomBuilder {
        public RspBitmap rb;
        public IndexCounts indexCounts;

        public RandomBuilder(final IndexCounts indexCounts, final long start, final long end) {
            rb = new RspBitmap(start, end);
            this.indexCounts = indexCounts;
        }

        public RandomBuilder(final IndexCounts indexCounts) {
            rb = new RspBitmap();
            this.indexCounts = indexCounts;
        }

        @Override
        public RspBitmap getTreeIndexImpl() {
            final RspBitmap ans = rb;
            rb = null;
            ans.tryCompactUnsafe(4);
            ans.finishMutationsAndOptimize();
            indexCounts.sampleRsp(ans);
            return ans;
        }

        @Override
        public void addKey(final long key) {
            rb.addUnsafeNoWriteCheck(key);
        }

        @Override
        public void addRange(final long start, final long endInclusive) {
            rb.addRangeUnsafeNoWriteCheck(start, endInclusive);
        }

        @Override
        public void appendKey(final long key) {
            rb.appendUnsafeNoWriteCheck(key);
        }

        @Override
        public void appendRange(final long start, final long endInclusive) {
            rb.appendRangeUnsafeNoWriteCheck(start, endInclusive);
        }

        @Override
        public void add(final SortedRanges ix, final boolean acquire) {
            rb.insertTreeIndexUnsafeNoWriteCheck(ix);
        }

        @Override
        public void add(final RspBitmap ix, final boolean acquire) {
            rb.insertTreeIndexUnsafeNoWriteCheck(ix);
        }
    }
}
