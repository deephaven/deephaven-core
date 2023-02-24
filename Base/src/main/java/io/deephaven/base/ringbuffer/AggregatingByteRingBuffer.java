/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit AggregatingCharRingBuffer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.base.ringbuffer;

import io.deephaven.base.verify.Assert;

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * A ring buffer which aggregates its contents according to a user-defined aggregation function. This aggregation
 * calculation is performed lazily, when the user calls evaluate(). Internally the class manages a tree of intermediate
 * aggregation values. This allows the class to efficiently update the final aggregated value when entries enter and
 * leave the buffer, without necessarily running the calculation over the whole buffer.
 */

public class AggregatingByteRingBuffer extends ByteRingBuffer {
    private final ByteFunction aggFunction;
    private final byte identityVal;
    private byte[] treeStorage;
    private long calcHead = 0; // inclusive
    private long calcTail = 0; // exclusive

    @FunctionalInterface
    public interface ByteFunction {
        /**
         * Applies this function to the given arguments.
         *
         * @param a the first function argument
         * @param b the second function argument
         * @return the function result
         */
        byte apply(byte a, byte b);
    }

    /**
     * Create a ring buffer for byte values which aggregates its contents according to a user-defined aggregation
     * function. This aggregation calculation is performed lazily, when the user calls evaluate(). Internally the class
     * manages a tree of intermediate aggregation values. This allows the class to efficiently update the final
     * aggregated value when entries enter and leave the buffer, without necessarily running the calculation over the
     * whole buffer.
     *
     * The buffer expands its capacity as needed, employing a capacity-doubling strategy. However, note that the data
     * structure never gives back storage: i.e. its capacity never shrinks.
     *
     * @param capacity the minimum size for the structure to hold
     * @param identityVal The identity value associated with the aggregation function. This is a value e that satisfies
     *        f(x,e) == x and f(e,x) == x for all x. For example, for addition, multiplication, Math.min, and Math.max,
     *        the identity values are 0.0f, 1.0f, Float.MAX_VALUE, and -Float.MAX_VALUE respectively.
     * @param aggFunction A function used to aggregate the data in the ring buffer. The function must be associative:
     *        that is it must satisfy f(f(a, b), c) == f(a, f(b, c)). For example, addition is associative, because (a +
     *        b) + c == a + (b + c). Some examples of associative functions are addition, multiplication, Math.min(),
     *        and Math.max(). This data structure maintains a tree of partially-evaluated subranges of the data,
     *        combining them efficiently whenever the data changes.
     */
    public AggregatingByteRingBuffer(int capacity, byte identityVal, ByteFunction aggFunction) {
        this(capacity, identityVal, aggFunction, true);
    }

    /**
     * Create a ring buffer for byte values which aggregates its contents according to a user-defined aggregation
     * function. This aggregation calculation is performed lazily, when the user calls evaluate(). Internally the class
     * manages a tree of intermediate aggregation values. This allows the class to efficiently update the final
     * aggregated value when entries enter and leave the buffer, without necessarily running the calculation over the
     * whole buffer.
     *
     * If {@code growable = true}, the buffer expands its capacity as needed, employing a capacity-doubling strategy.
     * However, note that the data structure never gives back storage: i.e. its capacity never shrinks.
     *
     * @param capacity the minimum size for the structure to hold
     * @param identityVal The identity value associated with the aggregation function. This is a value e that satisfies
     *        f(x,e) == x and f(e,x) == x for all x. For example, for addition, multiplication, Math.min, and Math.max,
     *        the identity values are 0.0f, 1.0f, Float.MAX_VALUE, and -Float.MAX_VALUE respectively.
     * @param aggFunction A function used to aggregate the data in the ring buffer. The function must be associative:
     *        that is it must satisfy f(f(a, b), c) == f(a, f(b, c)). For example, addition is associative, because (a +
     *        b) + c == a + (b + c). Some examples of associative functions are addition, multiplication, Math.min(),
     *        and Math.max(). This data structure maintains a tree of partially-evaluated subranges of the data,
     *        combining them efficiently whenever the data changes.
     * @param growable whether to allow growth when the buffer is full.
     */
    public AggregatingByteRingBuffer(int capacity, byte identityVal, ByteFunction aggFunction, boolean growable) {
        super(capacity, growable);

        this.aggFunction = aggFunction;
        this.identityVal = identityVal;

        treeStorage = new byte[storage.length];

        clear();
    }

    /**
     * Increase the capacity of the aggregating ring buffer.
     *
     * @param increase Increase amount. The ring buffer's capacity will be increased by at least this amount.
     */
    @Override
    protected void grow(int increase) {
        super.grow(increase);

        treeStorage = new byte[storage.length];

        // Fill the tree buffer with the identity value
        Arrays.fill(treeStorage, identityVal);
        // Fill the unpopulated section of the storage array with the identity value
        Arrays.fill(storage, size(), storage.length, identityVal);

        calcHead = calcTail = 0;
    }

    /**
     * Add values without overflow detection. The caller *must* ensure that there is at least one element of free space
     * in the ring buffer before calling this method. The caller may use {@link #ensureRemaining(int)} or
     * {@link #remaining()} for this purpose.
     *
     * @param e the value to add to the buffer
     */
    public void addUnsafe(byte e) {
        storage[(int) (tail++ & mask)] = e;
        // This is an extremely paranoid wrap check that in all likelihood will never run. With FIXUP_THRESHOLD at
        // 1 << 62, and the user pushing 2^32 values per second(!), it will take 68 years to wrap this counter .
        if (tail >= FIXUP_THRESHOLD) {
            // Reset calc[head, tail]
            long length = calcTail - calcHead;
            calcHead = (calcHead & mask);
            calcTail = calcHead + length;

            // Reset [head, tail] but force it not to overlap.
            length = tail - head;
            head = (head & mask) + storage.length;
            tail = head + length;
        }
    }

    public void addIdentityValue() {
        add(identityVal);
    }

    /**
     * Remove an element without empty buffer detection. The caller *must* ensure that there is at least one element in
     * the ring buffer. The {@link #size()} method may be used for this purpose.
     *
     * @return the value removed from the buffer
     */
    @Override
    public byte removeUnsafe() {
        final int idx = (int) (head++ & mask);
        byte val = storage[idx];
        // Reset the storage entry to the identity value
        storage[idx] = identityVal;
        return val;
    }

    /**
     * Remove multiple elements from the front of the ring buffer
     *
     * @param count The number of elements to remove.
     * @throws NoSuchElementException if the buffer is empty
     */
    @Override
    public byte[] remove(int count) {
        final int size = size();
        if (size < count) {
            throw new NoSuchElementException();
        }
        final byte[] result = new byte[count];

        final int storageHead = (int) (head & mask);

        // firstCopyLen is either the size of the ring buffer, the distance from head to the end of the storage array,
        // or the size of the destination buffer, whichever is smallest.
        final int firstCopyLen = Math.min(Math.min(storage.length - storageHead, size), result.length);

        // secondCopyLen is either the number of uncopied elements remaining from the first copy,
        // or the amount of space remaining in the dest array, whichever is smaller.
        final int secondCopyLen = Math.min(size - firstCopyLen, result.length - firstCopyLen);

        System.arraycopy(storage, storageHead, result, 0, firstCopyLen);
        Arrays.fill(storage, storageHead, storageHead + firstCopyLen, identityVal);
        System.arraycopy(storage, 0, result, firstCopyLen, secondCopyLen);
        Arrays.fill(storage, 0, secondCopyLen, identityVal);

        head += count;
        return result;
    }

    @Override
    public void clear() {
        super.clear();
        calcHead = calcTail = 0;
        // Prefill the storage buffers with the identity value
        Arrays.fill(storage, identityVal);
        Arrays.fill(treeStorage, identityVal);
    }

    // region evaluation
    public byte evaluate() {
        // [calcedHead, calcedTail) is the interval that was calculated the last time
        // that evaluate() was called.
        // [head, tail) is the interval holding the current data.
        // Their intersection (if any) is the interval of values that are still live
        // and don't need to be recalculated.
        final long intersectionSize = calcTail > head ? calcTail - head : 0;

        // Now r1 and r2 are the two ranges that need to be recalculated.
        // r1 needs to be recalculated because the values have been reset to identityVal.
        // r2 needs to be recalculated because the values are new.
        long r1Head = calcHead;
        long r1Tail = calcTail - intersectionSize;
        long r2Head = head + intersectionSize;
        long r2Tail = tail;

        final long newBase = r1Head; // aka calcHead

        r1Head -= newBase; // aka 0
        r1Tail -= newBase;
        r2Head -= newBase; // >= 0 because head is always ahead of calcHead
        r2Tail -= newBase;

        final long r2Size = r2Tail - r2Head;
        r2Head = r2Head & mask; // aka r2Head % capacity
        r2Tail = r2Head + r2Size;

        if (r2Tail <= storage.length) {
            // R2 is a single segment in the "normal" direction
            // with no wrapping. You're in one of these cases
            // [----------) R1
            // [--) case 1 (subsume)
            // [----) case 2 (extend)
            // [--) case 3 (two ranges)

            if (r2Tail <= r1Tail) { // case 1: subsume
                r2Head = r2Tail = 0; // empty
            } else if (r2Head <= r1Tail) { // case 2: extend
                r1Tail = r2Tail;
                r2Head = r2Tail = 0; // empty
            }
            // else : R1 and R2 are correct as is.
        } else {
            // R2 crosses the modulus. We can think of it in two parts as
            // part 1: [r2ModHead, capacity)
            // part 2: [capacity, r2ModTail)
            // If we shift it left by capacity (note: values become negative which is ok)
            // then we have
            // part 1: [r2ModHead - capacity, 0)
            // part 2: [0, r2ModTail - capacity)

            // Because it's now centered at 0, it is suitable for extending
            // R1 on both sides. We do all this (adjust R2, extend R1, reverse the adjustment)
            // as a oneliner.
            r1Head = Math.min(r1Head, r2Head - storage.length) + storage.length;
            r1Tail = Math.max(r2Tail, r2Tail - storage.length) + storage.length;
            r2Head = r2Tail = 0; // empty
        }

        // Reverse base adjustment
        r1Head += newBase;
        r1Tail += newBase;
        r2Head += newBase;
        r2Tail += newBase;

        if (r1Tail - r1Head >= storage.length || r2Tail - r2Head >= storage.length) {
            // Evaluate everything
            normalizeAndEvaluate(0, storage.length, 0, 0);
        } else {
            normalizeAndEvaluate(r1Head, r1Tail, r2Head, r2Tail);
        }

        // Store our computed range
        calcHead = head;
        calcTail = tail;

        return treeStorage[1];
    }

    void normalizeAndEvaluate(final long head1, final long tail1, final long head2, final long tail2) {
        final long size1 = tail1 - head1;
        final long size2 = tail2 - head2;

        // Compute the offset to store the results from the storage array to the tree array.
        final int offset = storage.length / 2;

        if (size1 == 0 && size2 == 0) {
            // No ranges to compute.
            return;
        } else if (size2 == 0) {
            // Only one range to compute (although it may be wrapped).
            int head1Normal = (int) (head1 & mask);
            int tail1Normal = (int) (head1Normal + size1);

            if (tail1Normal <= storage.length) {
                // Single non-wrapping range.
                final int h1 = head1Normal;
                final int t1 = tail1Normal - 1; // change to inclusive

                evaluateRangeFast(h1, t1, storage, offset);
                evaluateTree(offset + (h1 / 2), offset + (t1 / 2));
            } else {
                // Two ranges because of the wrap-around.
                final int h1 = 0;
                final int t1 = tail1Normal - storage.length - 1; // change to inclusive
                final int h2 = head1Normal;
                final int t2 = storage.length - 1; // change to inclusive

                evaluateRangeFast(h1, t1, storage, offset);
                evaluateRangeFast(h2, t2, storage, offset);

                evaluateTree(offset + (h1 / 2), offset + (t1 / 2),
                        offset + (h2 / 2), offset + (t2 / 2));
            }
        } else {
            // Two ranges to compute, only one can wrap.
            int head1Normal = (int) (head1 & mask);
            int tail1Normal = (int) (head1Normal + size1);
            int head2Normal = (int) (head2 & mask);
            int tail2Normal = (int) (head2Normal + size2);

            if (tail1Normal <= storage.length && tail2Normal <= storage.length) {
                // Neither range wraps around.
                final int h1 = head1Normal;
                final int t1 = tail1Normal - 1; // change to inclusive
                final int h2 = head2Normal;
                final int t2 = tail2Normal - 1; // change to inclusive

                evaluateRangeFast(h1, t1, storage, offset);
                evaluateRangeFast(h2, t2, storage, offset);

                evaluateTree(offset + (h1 / 2), offset + (t1 / 2),
                        offset + (h2 / 2), offset + (t2 / 2));
            } else if (tail1Normal <= storage.length) {
                // r2 wraps, r1 does not.
                final int h1 = 0;
                final int t1 = tail2Normal - storage.length - 1; // change to inclusive
                final int h2 = head1Normal;
                final int t2 = tail1Normal - 1; // change to inclusive
                final int h3 = head2Normal;
                final int t3 = storage.length - 1; // change to inclusive

                evaluateRangeFast(h1, t1, storage, offset);
                evaluateRangeFast(h2, t2, storage, offset);
                evaluateRangeFast(h3, t3, storage, offset);

                evaluateTree(offset + (h1 / 2), offset + (t1 / 2),
                        offset + (h2 / 2), offset + (t2 / 2),
                        offset + (h3 / 2), offset + (t3 / 2));
            } else {
                // r1 wraps, r2 does not.
                final int h1 = 0;
                final int t1 = tail1Normal - storage.length - 1; // change to inclusive
                final int h2 = head2Normal;
                final int t2 = tail2Normal - 1; // change to inclusive
                final int h3 = head1Normal;
                final int t3 = storage.length - 1; // change to inclusive

                evaluateRangeFast(h1, t1, storage, offset);
                evaluateRangeFast(h2, t2, storage, offset);
                evaluateRangeFast(h3, t3, storage, offset);

                evaluateTree(offset + (h1 / 2), offset + (t1 / 2),
                        offset + (h2 / 2), offset + (t2 / 2),
                        offset + (h3 / 2), offset + (t3 / 2));
            }
        }
    }

    public static boolean rangesCollapse(final int x1, final int y1, final int x2, final int y2) {
        // Ranges overlap when the start of one range is <= the end of the other. In this case, we want to extend
        // detection to when the ranges are consecutive as well. Do this by adding one to the range ends before
        // comparison.
        return x1 <= (y2 + 1) && x2 <= (y1 + 1);
    }

    private void evaluateRangeFast(int start, int end, byte[] src, int dstOffset) {
        // Everything from start to end (inclusive) should be evaluated
        for (int left = start & 0xFFFFFFFE; left <= end; left += 2) {
            final int right = left + 1;
            final int parent = left / 2;

            // load the data values
            final byte leftVal = src[left];
            final byte rightVal = src[right];

            // compute & store (always in the tree area)
            final byte computeVal = aggFunction.apply(leftVal, rightVal);
            treeStorage[parent + dstOffset] = computeVal;
        }
    }

    private byte evaluateTree(int startA, int endA) {
        while (endA > 1) {
            // compute this level
            evaluateRangeFast(startA, endA, treeStorage, 0);

            // compute the new parents
            startA /= 2;
            endA /= 2;
        }
        return treeStorage[endA];
    }

    private byte evaluateTree(int startA, int endA, int startB, int endB) {
        while (endB > 1) {
            if (rangesCollapse(startA, endA, startB, endB)) {
                // all collapse together into a single range
                return evaluateTree(Math.min(startA, startB), Math.max(endA, endB));
            } else {
                // compute this level
                evaluateRangeFast(startA, endA, treeStorage, 0);
                evaluateRangeFast(startB, endB, treeStorage, 0);

                // compute the new parents
                startA /= 2;
                endA /= 2;
                startB /= 2;
                endB /= 2;
            }
        }
        throw Assert.statementNeverExecuted();
    }

    private byte evaluateTree(int startA, int endA, int startB, int endB, int startC, int endC) {
        while (endC > 1) {
            if (rangesCollapse(startA, endA, startB, endB)) {
                // A and B overlap
                return evaluateTree(Math.min(startA, startB), Math.max(endA, endB), startC, endC);
            } else if (rangesCollapse(startB, endB, startC, endC)) {
                // B and C overlap
                return evaluateTree(startA, endA, Math.min(startB, startC), Math.max(endB, endC));
            } else {
                // no collapse
                evaluateRangeFast(startA, endA, treeStorage, 0);
                evaluateRangeFast(startB, endB, treeStorage, 0);
                evaluateRangeFast(startC, endC, treeStorage, 0);

                // compute the new parents
                startA /= 2;
                endA /= 2;
                startB /= 2;
                endB /= 2;
                startC /= 2;
                endC /= 2;
            }
        }
        throw Assert.statementNeverExecuted();
    }
    // endregion evaluation
}
