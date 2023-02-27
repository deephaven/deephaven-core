/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit AggregatingCharRingBuffer and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.base.ringbuffer;

import java.util.*;

/**
 * A ring buffer which aggregates its contents according to a user-defined aggregation function. This aggregation
 * calculation is performed lazily, when the user calls evaluate(). Internally the class manages a tree of intermediate
 * aggregation values. This allows the class to efficiently update the final aggregated value when entries enter and
 * leave the buffer, without necessarily running the calculation over the whole buffer.
 */

public class AggregatingLongRingBuffer {
    private final LongRingBuffer internalBuffer;
    private final LongFunction aggFunction;
    private final long identityVal;

    private static long defaultValueForThisType;
    private long[] treeStorage;
    private long calcHead = 0; // inclusive
    private long calcTail = 0; // exclusive

    @FunctionalInterface
    public interface LongFunction {
        /**
         * Applies this function to the given arguments.
         *
         * @param a the first function argument
         * @param b the second function argument
         * @return the function result
         */
        long apply(long a, long b);
    }

    /**
     * Create a ring buffer for long values which aggregates its contents according to a user-defined aggregation
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
    public AggregatingLongRingBuffer(int capacity, long identityVal, LongFunction aggFunction) {
        this(capacity, identityVal, aggFunction, true);
    }

    /**
     * Create a ring buffer for long values which aggregates its contents according to a user-defined aggregation
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
    public AggregatingLongRingBuffer(int capacity, long identityVal, LongFunction aggFunction,
                                     boolean growable) {
        internalBuffer = new LongRingBuffer(capacity, growable);
        this.aggFunction = aggFunction;
        this.identityVal = identityVal;

        treeStorage = new long[internalBuffer.storage.length];

        if (identityVal != defaultValueForThisType) {
            // Fill the tree buffer with the identity value
            Arrays.fill(treeStorage, identityVal);
            // Fill the unpopulated section of the storage array with the identity value
            Arrays.fill(internalBuffer.storage, identityVal);
        }
    }

    /**
     * Increase the capacity of the aggregating ring buffer.
     *
     * @param increase Increase amount. The ring buffer's capacity will be increased by at least this amount.
     */
    protected void grow(int increase) {
        internalBuffer.grow(increase);

        treeStorage = new long[internalBuffer.storage.length];

        if (identityVal != defaultValueForThisType) {
            // Fill the tree buffer with the identity value
            Arrays.fill(treeStorage, identityVal);
            // Fill the unpopulated section of the storage array with the identity value
            Arrays.fill(internalBuffer.storage, internalBuffer.size(), internalBuffer.storage.length, identityVal);
        }
        calcHead = calcTail = 0;
    }

    public boolean isFull() {
        return internalBuffer.isFull();
    }

    public boolean isEmpty() {
        return internalBuffer.isEmpty();
    }

    public int size() {
        return internalBuffer.size();
    }

    public int capacity() {
        return internalBuffer.capacity();
    }

    public int remaining() {
        return internalBuffer.remaining();
    }

    /**
     * Adds an entry to the ring buffer, will throw an exception if buffer is full. For a graceful failure, use
     * {@link #offer(long)}
     *
     * @param e the long to be added to the buffer
     * @throws UnsupportedOperationException when {@code growable} is {@code false} and buffer is full
     * @return {@code true} if the long was added successfully
     */
    public boolean add(long e) {
        if (isFull()) {
            if (!internalBuffer.growable) {
                throw new UnsupportedOperationException("Ring buffer is full and growth is disabled");
            } else {
                grow(1);
            }
        }
        addUnsafe(e);
        return true;
    }

    /**
     * Ensure that there is sufficient empty space to store {@code count} items in the buffer. If the buffer is
     * {@code growable}, this may result in an internal growth operation. This call should be used in conjunction with
     * {@link #addUnsafe(long)}.
     *
     * @param count the minimum number of empty entries in the buffer after this call
     * @throws UnsupportedOperationException when {@code growable} is {@code false} and buffer is full
     */
    public void ensureRemaining(int count) {
        if (remaining() < count) {
            if (!internalBuffer.growable) {
                throw new UnsupportedOperationException("Ring buffer is full and growth is disabled");
            } else {
                grow(count);
            }
        }
    }

    /**
     * Add an entry to the ring buffer. If the buffer is full, will overwrite the oldest entry with the new one.
     *
     * @param e the long to be added to the buffer
     * @param notFullResult value to return is the buffer is not full
     * @return the overwritten entry if the buffer is full, the provided value otherwise
     */
    public long addOverwrite(long e, long notFullResult) {
        long val = notFullResult;
        if (isFull()) {
            val = remove();
        }
        addUnsafe(e);
        return val;
    }

    /**
     * Attempt to add an entry to the ring buffer. If the buffer is full, the add will fail and the buffer will not grow
     * even if growable.
     *
     * @param e the long to be added to the buffer
     * @return true if the value was added successfully, false otherwise
     */
    public boolean offer(long e) {
        if (isFull()) {
            return false;
        }
        addUnsafe(e);
        return true;
    }

    /**
     * Remove one element from the front of the ring buffer.
     *
     * @throws NoSuchElementException if the buffer is empty
     */
    public long remove() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return removeUnsafe();
    }

    /**
     * If the ring buffer is non-empty, removes the element at the head of the ring buffer. Otherwise does nothing.
     *
     * @param onEmpty the value to return if the ring buffer is empty
     * @return The removed element if the ring buffer was non-empty, otherwise the value of 'onEmpty'
     */
    public long poll(long onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        return removeUnsafe();
    }

    /**
     * If the ring buffer is non-empty, returns the element at the head of the ring buffer.
     *
     * @throws NoSuchElementException if the buffer is empty
     * @return The head element if the ring buffer is non-empty, otherwise the value of 'onEmpty'
     */
    public long element() {
        return internalBuffer.element();
    }

    /**
     * If the ring buffer is non-empty, returns the element at the head of the ring buffer. Otherwise returns the
     * specified element.
     *
     * @param onEmpty the value to return if the ring buffer is empty
     * @return The head element if the ring buffer is non-empty, otherwise the value of 'onEmpty'
     */
    public long peek(long onEmpty) {
        return internalBuffer.peek(onEmpty);
    }

    /**
     * Returns the element at the head of the ring buffer
     *
     * @return The element at the head of the ring buffer
     */
    public long front() {
        return front(0);
    }

    /**
     * Returns the element at the specified offset in the ring buffer.
     *
     * @param offset The specified offset.
     * @throws NoSuchElementException if the buffer is empty
     * @return The element at the specified offset
     */
    public long front(int offset) {
        return internalBuffer.front(offset);
    }

    /**
     * Returns the element at the tail of the ring buffer
     *
     * @throws NoSuchElementException if the buffer is empty
     * @return The element at the tail of the ring buffer
     */
    public long back() {
        return internalBuffer.back();
    }

    /**
     * If the ring buffer is non-empty, returns the element at the tail of the ring buffer. Otherwise returns the
     * specified element.
     *
     * @param onEmpty the value to return if the ring buffer is empty
     * @return The tail element if the ring buffer is non-empty, otherwise the value of 'onEmpty'
     */
    public long peekBack(long onEmpty) {
        return internalBuffer.peekBack(onEmpty);
    }

    /**
     * Make a copy of the elements in the ring buffer.
     *
     * @return An array containing a copy of the elements in the ring buffer.
     */
    public long[] getAll() {
        return internalBuffer.getAll();
    }

















































    /**
     * Add values without overflow detection. The caller *must* ensure that there is at least one element of free space
     * in the ring buffer before calling this method. The caller may use {@link #ensureRemaining(int)} or
     * {@link #remaining()} for this purpose.
     *
     * @param e the value to add to the buffer
     */
    public void addUnsafe(long e) {
        // Perform a specialized version of the fix-up test.
        if (internalBuffer.tail >= internalBuffer.FIXUP_THRESHOLD) {
            // Reset calc[head, tail]
            long length = calcTail - calcHead;
            calcHead = (calcHead & internalBuffer.mask);
            calcTail = calcHead + length;

            // Reset [head, tail] but force it not to overlap with calc[head, tail]
            length = internalBuffer.tail - internalBuffer.head;
            internalBuffer.head = (internalBuffer.head & internalBuffer.mask) + internalBuffer.storage.length;
            internalBuffer.tail = internalBuffer.head + length;
        }
        internalBuffer.addUnsafe(e);
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
    public long removeUnsafe() {
        // NOTE: remove() for this data structure must replace the removed value with identityVal.
        final long prevHead = internalBuffer.head;
        long val = internalBuffer.removeUnsafe();

        // Reset the storage entry to the identity value
        final int idx = (int) (prevHead & internalBuffer.mask);
        internalBuffer.storage[idx] = identityVal;

        return val;
    }

    /**
     * Remove multiple elements from the front of the ring buffer
     *
     * @param count The number of elements to remove.
     * @throws NoSuchElementException if the buffer is empty
     */
    public long[] remove(int count) {
        // NOTE: remove() for this data structure must replace the removed value with identityVal.
        final long prevHead = internalBuffer.head;
        final long[] result = internalBuffer.remove(count);

        // Reset the cleared storage entries to the identity value
        fillWithIdentityVal(prevHead, count, size());

        return result;
    }

    /**
     * Remove all elements from the ring buffer and reset the data structure.  This may require resetting all entries
     * in the storage buffer and evaluation tree and should be considered to be of complexity O(capacity) instead of
     * O(size).
     */
    public void clear() {
        final int size = size();
        final long prevHead = internalBuffer.head;

        internalBuffer.clear();

        calcHead = calcTail = 0;
        // Fill the tree buffer with the identity value
        Arrays.fill(treeStorage, identityVal);

        // Reset the cleared storage entries to the identity value
        fillWithIdentityVal(prevHead, size, size);
    }

    private void fillWithIdentityVal(long head, int count, int size) {
        final int storageHead = (int) (head & internalBuffer.mask);

        // firstCopyLen is either the size of the ring buffer, the distance from head to the end of the storage array,
        // or the count, whichever is smallest.
        final int firstCopyLen = Math.min(Math.min(internalBuffer.storage.length - storageHead, size), count);

        // secondCopyLen is either the number of uncopied elements remaining from the first copy,
        // or the remaining to copy from count, whichever is smaller.
        final int secondCopyLen = Math.min(size - firstCopyLen, count - firstCopyLen);

        Arrays.fill(internalBuffer.storage, storageHead, storageHead + firstCopyLen, identityVal);
        Arrays.fill(internalBuffer.storage, 0, secondCopyLen, identityVal);
    }

    // region evaluation
    public long evaluate() {
        // [calcedHead, calcedTail) is the interval that was calculated the last time
        // that evaluate() was called.
        // [head, tail) is the interval holding the current data.
        // Their intersection (if any) is the interval of values that are still live
        // and don't need to be recalculated.
        final long intersectionSize = calcTail > internalBuffer.head ? calcTail - internalBuffer.head : 0;

        // Now r1 and r2 are the two ranges that need to be recalculated.
        // r1 needs to be recalculated because the values have been reset to identityVal.
        // r2 needs to be recalculated because the values are new.
        long r1Head = calcHead;
        long r1Tail = calcTail - intersectionSize;
        long r2Head = internalBuffer.head + intersectionSize;
        long r2Tail = internalBuffer.tail;

        final long newBase = r1Head; // aka calcHead

        r1Head -= newBase; // aka 0
        r1Tail -= newBase;
        r2Head -= newBase; // >= 0 because head is always ahead of calcHead
        r2Tail -= newBase;

        final long r2Size = r2Tail - r2Head;
        r2Head = r2Head & internalBuffer.mask; // aka r2Head % capacity
        r2Tail = r2Head + r2Size;

        if (r2Tail <= internalBuffer.storage.length) {
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
            r1Head = Math.min(r1Head, r2Head - internalBuffer.storage.length) + internalBuffer.storage.length;
            r1Tail = Math.max(r2Tail, r2Tail - internalBuffer.storage.length) + internalBuffer.storage.length;
            r2Head = r2Tail = 0; // empty
        }

        // Reverse base adjustment
        r1Head += newBase;
        r1Tail += newBase;
        r2Head += newBase;
        r2Tail += newBase;
        // KOSAK: I wouldn't bother with this 'if' statement because the range code merges right away anyway
        if (r1Tail - r1Head >= internalBuffer.storage.length || r2Tail - r2Head >= internalBuffer.storage.length) {
            // Evaluate everything
            fixTree(0, internalBuffer.storage.length, 0, 0, internalBuffer.storage.length);
            // normalizeAndEvaluate(0, internalBuffer.storage.length, 0, 0);
        } else {
            fixTree(r1Head, r1Tail, r2Head, r2Tail, internalBuffer.storage.length);
            // normalizeAndEvaluate(r1Head, r1Tail, r2Head, r2Tail);
        }

        // Store our computed range
        calcHead = internalBuffer.head;
        calcTail = internalBuffer.tail;

        return treeStorage[1];
    }

    private static class Range {
        public static final Range zero = new Range(0, 0);

        final int begin;
        final int end;

        public Range(int begin, int end) {
            this.begin = begin;
            this.end = end;
        }
    }

    private void fixTree(long r1Head, long r1Tail, long r2Head, long r2Tail, int capacityForThisLevel) {
        final Range[] r1s = maybeSplit(r1Head, r1Tail, capacityForThisLevel);
        final Range[] r2s = maybeSplit(r2Head, r2Tail, capacityForThisLevel);

        final Range r1, r2, r3;
        if (r1s.length == 1) {
            if (r2s.length == 1) {
                if (r1s[0].begin < r2s[0].begin) {
                    r1 = r1s[0];
                    r2 = r2s[0];
                } else {
                    r2 = r1s[0];
                    r1 = r2s[0];
                }
                r3 = Range.zero;
            } else {
                assert(r2s.length == 2);
                // r2 is split but r1 is not.
                r1 = r2s[0];
                r2 = r1s[0];
                r3 = r2s[1];
            }
        } else {
            // r1 is split but r2 is not
            assert(r1s.length == 2);
            assert(r2s.length == 1);
            r1 = r1s[0];
            r2 = r2s[0];
            r3 = r1s[1];
        }

        int r1h = r1.begin, r1t = r1.end;
        int r2h = r2.begin, r2t = r2.end;
        int r3h = r3.begin, r3t = r3.end;

//        System.out.println("Calling fixTree()");

        // We need to perform the first round of evaluation from values in the storage array and adjust the ranges
        // properly so subsequent evaluations are over the correct subset of the tree
        int offset = internalBuffer.storage.length / 2;
        evaluateRangeFast(r1h, r1t - 1, internalBuffer.storage, offset);
        evaluateRangeFast(r2h, r2t - 1, internalBuffer.storage, offset);
        evaluateRangeFast(r3h, r3t - 1, internalBuffer.storage, offset);
        r1h = offset + (r1h / 2);
        r1t = offset + ((r1t + 1) / 2);
        r2h = offset + (r2h / 2);
        r2t = offset + ((r2t + 1) / 2);
        r3h = offset + (r3h / 2);
        r3t = offset + ((r3t + 1) / 2);

        while (capacityForThisLevel > 2) {
            if (r2h == r2t) {
                // r2 empty: take over and clear r3
                r2h = r3h;
                r2t = r3t;
                r3h = 0;
                r3t = 0;
            } else if (r2t == r3h) {
                // r2 abuts r3: extend r2 to r3 and clear r3
                r2t = r3t;
                r3h = 0;
                r3t = 0;
            }

            if (r1h == r1t) {
                // r2 empty: take over and clear r2
                r1h = r2h;
                r1t = r2t;
                r2h = 0;
                r2t = 0;
            } else if (r1t == r2h) {
                // r1 abuts r2: extend r1 to r2 and clear r2
                r1t = r2t;
                r2h = 0;
                r2t = 0;
            }
            evaluateRangeFast(r1h, r1t - 1, treeStorage, 0);
            evaluateRangeFast(r2h, r2t - 1, treeStorage, 0);
            evaluateRangeFast(r3h, r3t - 1, treeStorage, 0);

            r1h /= 2;  //round down
            r1t = (r1t + 1) / 2;  // round up

            r2h /= 2;  //round down
            r2t = (r2t + 1) / 2;  // round up

            r3h /= 2;  //round down
            r3t = (r3t + 1) / 2;  // round up

            capacityForThisLevel /= 2;
        }
    }

    Range[] maybeSplit(final long head, final long tail, final int capacity) {
        final long size = tail - head;
        if (size == 0) {
            return new Range[]{Range.zero};
        }
        int mask = capacity - 1;
        int normalizedHead = (int) (head & mask);
        int normalizedTail = (int) (normalizedHead + size);
        if (normalizedTail <= capacity) {
            return new Range[]{new Range(normalizedHead, normalizedTail)};
        }
        return new Range[] {
                new Range(0, normalizedTail - capacity),
                new Range(normalizedHead, capacity)
        };
    }

    void normalizeAndEvaluate(final long head1, final long tail1, final long head2, final long tail2) {
//        System.out.println("Calling normalizeAndEvaluate()");

        final long size1 = tail1 - head1;
        final long size2 = tail2 - head2;

        // Compute the offset to store the results from the storage array to the tree array.
        final int offset = internalBuffer.storage.length / 2;

        if (size1 == 0 && size2 == 0) {
            // No ranges to compute.
            return;
        } else if (size2 == 0) {
            // Only one range to compute (although it may be wrapped).
            int head1Normal = (int) (head1 & internalBuffer.mask);
            int tail1Normal = (int) (head1Normal + size1);

            if (tail1Normal <= internalBuffer.storage.length) {
                // Single non-wrapping range.
                final int h1 = head1Normal;
                final int t1 = tail1Normal - 1; // change to inclusive

                evaluateRangeFast(h1, t1, internalBuffer.storage, offset);
                evaluateTree(offset + (h1 / 2), offset + (t1 / 2));
            } else {
                // Two ranges because of the wrap-around.
                final int h1 = 0;
                final int t1 = tail1Normal - internalBuffer.storage.length - 1; // change to inclusive
                final int h2 = head1Normal;
                final int t2 = internalBuffer.storage.length - 1; // change to inclusive

                evaluateRangeFast(h1, t1, internalBuffer.storage, offset);
                evaluateRangeFast(h2, t2, internalBuffer.storage, offset);

                evaluateTree(offset + (h1 / 2), offset + (t1 / 2),
                        offset + (h2 / 2), offset + (t2 / 2));
            }
        } else {
            // Two ranges to compute, only one can wrap.
            int head1Normal = (int) (head1 & internalBuffer.mask);
            int tail1Normal = (int) (head1Normal + size1);
            int head2Normal = (int) (head2 & internalBuffer.mask);
            int tail2Normal = (int) (head2Normal + size2);

            if (tail1Normal <= internalBuffer.storage.length && tail2Normal <= internalBuffer.storage.length) {
                // Neither range wraps around.
                final int h1 = head1Normal;
                final int t1 = tail1Normal - 1; // change to inclusive
                final int h2 = head2Normal;
                final int t2 = tail2Normal - 1; // change to inclusive

                evaluateRangeFast(h1, t1, internalBuffer.storage, offset);
                evaluateRangeFast(h2, t2, internalBuffer.storage, offset);

                evaluateTree(offset + (h1 / 2), offset + (t1 / 2),
                        offset + (h2 / 2), offset + (t2 / 2));
            } else if (tail1Normal <= internalBuffer.storage.length) {
                // r2 wraps, r1 does not.
                final int h1 = 0;
                final int t1 = tail2Normal - internalBuffer.storage.length - 1; // change to inclusive
                final int h2 = head1Normal;
                final int t2 = tail1Normal - 1; // change to inclusive
                final int h3 = head2Normal;
                final int t3 = internalBuffer.storage.length - 1; // change to inclusive

                evaluateRangeFast(h1, t1, internalBuffer.storage, offset);
                evaluateRangeFast(h2, t2, internalBuffer.storage, offset);
                evaluateRangeFast(h3, t3, internalBuffer.storage, offset);

                evaluateTree(offset + (h1 / 2), offset + (t1 / 2),
                        offset + (h2 / 2), offset + (t2 / 2),
                        offset + (h3 / 2), offset + (t3 / 2));
            } else {
                // r1 wraps, r2 does not.
                final int h1 = 0;
                final int t1 = tail1Normal - internalBuffer.storage.length - 1; // change to inclusive
                final int h2 = head2Normal;
                final int t2 = tail2Normal - 1; // change to inclusive
                final int h3 = head1Normal;
                final int t3 = internalBuffer.storage.length - 1; // change to inclusive

                evaluateRangeFast(h1, t1, internalBuffer.storage, offset);
                evaluateRangeFast(h2, t2, internalBuffer.storage, offset);
                evaluateRangeFast(h3, t3, internalBuffer.storage, offset);

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

    private void evaluateRangeFast(int start, int end, long[] src, int dstOffset) {
        if (end < start) {
            return;
        }
        // System.out.printf("Offset %d, computing [%d,%d)\n", dstOffset, start, end);
        // Everything from start to end (inclusive) should be evaluated
        for (int left = start & 0xFFFFFFFE; left <= end; left += 2) {
            final int right = left + 1;
            final int parent = left / 2;

            // load the data values
            final long leftVal = src[left];
            final long rightVal = src[right];

            // compute & store (always in the tree area)
            final long computeVal = aggFunction.apply(leftVal, rightVal);
            treeStorage[parent + dstOffset] = computeVal;
        }
    }

    private void evaluateTree(int startA, int endA) {
        while (endA > 1) {
            // compute this level
            evaluateRangeFast(startA, endA, treeStorage, 0);

            // compute the new parents
            startA /= 2;
            endA /= 2;
        }
    }

    private void evaluateTree(int startA, int endA, int startB, int endB) {
        while (endB > 1) {
            if (rangesCollapse(startA, endA, startB, endB)) {
                // all collapse together into a single range
                evaluateTree(Math.min(startA, startB), Math.max(endA, endB));
                return;
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
    }

    private void evaluateTree(int startA, int endA, int startB, int endB, int startC, int endC) {
        while (endC > 1) {
            if (rangesCollapse(startA, endA, startB, endB)) {
                // A and B overlap
                evaluateTree(Math.min(startA, startB), Math.max(endA, endB), startC, endC);
                return;
            } else if (rangesCollapse(startB, endB, startC, endC)) {
                // B and C overlap
                evaluateTree(startA, endA, Math.min(startB, startC), Math.max(endB, endC));
                return;
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
    }
    // endregion evaluation
}
