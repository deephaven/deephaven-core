//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.ringbuffer;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * A ring buffer which aggregates its contents according to a user-defined aggregation function. This aggregation
 * calculation is performed lazily, when the user calls evaluate(). Internally the class manages a tree of intermediate
 * aggregation values. This allows the class to efficiently update the final aggregated value when entries enter and
 * leave the buffer, without necessarily running the calculation over the whole buffer.
 */
public class AggregatingCharRingBuffer {
    private final CharRingBuffer internalBuffer;
    private final CharFunction aggInitialFunction;
    private final CharFunction aggTreeFunction;
    private final char identityVal;
    private static char defaultValueForThisType;
    private char[] treeStorage;
    private long calcHead = 0; // inclusive
    private long calcTail = 0; // exclusive

    @FunctionalInterface
    public interface CharFunction {
        /**
         * Applies this function to the given arguments.
         *
         * @param a the first function argument
         * @param b the second function argument
         * @return the function result
         */
        char apply(char a, char b);
    }

    /**
     * Creates a ring buffer for char values which aggregates its contents according to a user-defined aggregation
     * function. This aggregation calculation is performed lazily, when the user calls evaluate(). Internally the class
     * manages a tree of intermediate aggregation values. This allows the class to efficiently update the final
     * aggregated value when entries enter and leave the buffer, without necessarily running the calculation over the
     * whole buffer.
     * <p>
     * The buffer expands its capacity as needed, employing a capacity-doubling strategy. However, note that the data
     * structure never gives back storage: i.e. its capacity never shrinks.
     *
     * @param capacity the minimum size for the structure to hold
     * @param identityVal The identity value associated with the aggregation function. This is a value e that satisfies
     *        f(x,e) == x and f(e,x) == x for all x. For example, for the AggregatingFloatRingBuffer, if the aggFunction
     *        is addition, multiplication, Math.min, or Math.max, the corresponding identity values would be 0.0f, 1.0f,
     *        Float.MAX_VALUE, and -Float.MAX_VALUE respectively.
     * @param aggFunction A function used to aggregate the data in the ring buffer. The function must be associative:
     *        that is it must satisfy f(f(a, b), c) == f(a, f(b, c)). For example, addition is associative, because (a +
     *        b) + c == a + (b + c). Some examples of associative functions are addition, multiplication, Math.min(),
     *        and Math.max(). This data structure maintains a tree of partially-evaluated subranges of the data,
     *        combining them efficiently whenever the data changes.
     */
    public AggregatingCharRingBuffer(final int capacity, final char identityVal,
            @NotNull final CharFunction aggFunction) {
        this(capacity, identityVal, aggFunction, aggFunction, true);
    }

    /**
     * Creates a ring buffer for char values which aggregates its contents according to a user-defined aggregation
     * function. This aggregation calculation is performed lazily, when the user calls evaluate(). Internally the class
     * manages a tree of intermediate aggregation values. This allows the class to efficiently update the final
     * aggregated value when entries enter and leave the buffer, without necessarily running the calculation over the
     * whole buffer.
     * <p>
     * The buffer expands its capacity as needed, employing a capacity-doubling strategy. However, note that the data
     * structure never gives back storage: i.e. its capacity never shrinks.
     *
     * @param capacity the minimum size for the structure to hold
     * @param identityVal The identity value associated with the aggregation function. This is a value e that satisfies
     *        f(x,e) == x and f(e,x) == x for all x. For example, for the AggregatingFloatRingBuffer, if the aggFunction
     *        is addition, multiplication, Math.min, or Math.max, the corresponding identity values would be 0.0f, 1.0f,
     *        Float.MAX_VALUE, and -Float.MAX_VALUE respectively.
     * @param aggTreeFunction A function used to aggregate the data in the ring buffer. The function must be
     *        associative: that is it must satisfy f(f(a, b), c) == f(a, f(b, c)). For example, addition is associative,
     *        because (a + b) + c == a + (b + c). Some examples of associative functions are addition, multiplication,
     *        Math.min(), and Math.max(). This data structure maintains a tree of partially-evaluated subranges of the
     *        data, combining them efficiently whenever the data changes.
     * @param aggInitialFunction An associative function separate from {@code aggTreeFunction} to be applied only to the
     *        user-supplied values in the ring buffer. The results of this function will be populated into the tree for
     *        later evaluation by {@code aggTreeFunction}. This function could be used to filter or translate data
     *        values at the leaf of the tree without affecting later computation.
     */
    public AggregatingCharRingBuffer(final int capacity,
            final char identityVal,
            @NotNull final CharFunction aggTreeFunction,
            @NotNull final CharFunction aggInitialFunction) {
        this(capacity, identityVal, aggTreeFunction, aggInitialFunction, true);
    }

    /**
     * Creates a ring buffer for char values which aggregates its contents according to a user-defined aggregation
     * function. This aggregation calculation is performed lazily, when the user calls evaluate(). Internally the class
     * manages a tree of intermediate aggregation values. This allows the class to efficiently update the final
     * aggregated value when entries enter and leave the buffer, without necessarily running the calculation over the
     * whole buffer.
     * <p>
     * If {@code growable = true}, the buffer expands its capacity as needed, employing a capacity-doubling strategy.
     * However, note that the data structure never gives back storage: i.e. its capacity never shrinks.
     *
     * @param capacity the minimum size for the structure to hold
     * @param identityVal The identity value associated with the aggregation function. This is a value e that satisfies
     *        f(x,e) == x and f(e,x) == x for all x. For example, for the AggregatingFloatRingBuffer, if the aggFunction
     *        is addition, multiplication, Math.min, or Math.max, the corresponding identity values would be 0.0f, 1.0f,
     *        Float.MAX_VALUE, and -Float.MAX_VALUE respectively.
     * @param aggTreeFunction A function used to aggregate the data in the ring buffer. The function must be
     *        associative: that is it must satisfy f(f(a, b), c) == f(a, f(b, c)). For example, addition is associative,
     *        because (a + b) + c == a + (b + c). Some examples of associative functions are addition, multiplication,
     *        Math.min(), and Math.max(). This data structure maintains a tree of partially-evaluated subranges of the
     *        data, combining them efficiently whenever the data changes.
     * @param aggInitialFunction An associative function separate from {@code aggTreeFunction} to be applied only to the
     *        user-supplied values in the ring buffer. The results of this function will be populated into the tree for
     *        later evaluation by {@code aggTreeFunction}. This function could be used to filter or translate data
     *        values at the leaf of the tree without affecting later computation.
     * @param growable whether to allow growth when the buffer is full.
     */
    public AggregatingCharRingBuffer(final int capacity,
            final char identityVal,
            @NotNull final CharFunction aggTreeFunction,
            @NotNull final CharFunction aggInitialFunction,
            final boolean growable) {
        internalBuffer = new CharRingBuffer(capacity, growable);
        this.aggTreeFunction = aggTreeFunction;
        this.aggInitialFunction = aggInitialFunction;
        this.identityVal = identityVal;

        treeStorage = new char[internalBuffer.storage.length];

        if (identityVal != defaultValueForThisType) {
            // Fill the tree buffer with the identity value
            Arrays.fill(treeStorage, identityVal);
            // Fill the unpopulated section of the storage array with the identity value
            Arrays.fill(internalBuffer.storage, identityVal);
        }
    }

    /**
     * Increases the capacity of the aggregating ring buffer.
     *
     * @param increase Increase amount. The ring buffer's capacity will be increased by at least this amount.
     */
    protected void grow(int increase) {
        internalBuffer.grow(increase);

        treeStorage = new char[internalBuffer.storage.length];

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
     * Adds an entry to the ring buffer. Throws an exception if buffer is full and growth is disabled. For a graceful
     * failure, use {@link #offer(char)}
     *
     * @param e the char to be added to the buffer
     * @throws UnsupportedOperationException when {@code growable} is {@code false} and buffer is full
     */
    public void add(char e) {
        if (isFull()) {
            if (!internalBuffer.growable) {
                throw new UnsupportedOperationException("Ring buffer is full and growth is disabled");
            } else {
                grow(1);
            }
        }
        addUnsafe(e);
    }

    /**
     * Ensures that there is sufficient empty space to store {@code count} additional items in the buffer. If the buffer
     * is {@code growable}, this may result in an internal growth operation. This call should be used in conjunction
     * with {@link #addUnsafe(char)}.
     *
     * @param count the minimum number of empty entries in the buffer after this call
     * @throws UnsupportedOperationException when {@code growable} is {@code false} and the buffer's available space is
     *         less than {@code count}
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
     * Adds an entry to the ring buffer. If the buffer is full, overwrites the oldest entry with the new one.
     *
     * @param e the char to be added to the buffer
     * @param notFullResult value to return if the buffer is not full
     * @return the overwritten entry if the buffer is full, the provided value otherwise
     */
    public char addOverwrite(char e, char notFullResult) {
        char val = notFullResult;
        if (isFull()) {
            val = remove();
        }
        addUnsafe(e);
        return val;
    }

    /**
     * Attempts to add an entry to the ring buffer. If the buffer is full, the add fails and the buffer will not grow
     * even if growable.
     *
     * @param e the char to be added to the buffer
     * @return true if the value was added successfully, false otherwise
     */
    public boolean offer(char e) {
        if (isFull()) {
            return false;
        }
        addUnsafe(e);
        return true;
    }

    /**
     * Removes one element from the head of the ring buffer.
     *
     * @return The removed element if the ring buffer was non-empty
     * @throws NoSuchElementException if the buffer is empty
     */
    public char remove() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return removeUnsafe();
    }

    /**
     * Removes the element at the head of the ring buffer if the ring buffer is non-empty. Otherwise returns
     * {@code onEmpty}.
     *
     * @param onEmpty the value to return if the ring buffer is empty
     * @return The removed element if the ring buffer was non-empty, otherwise the value of {@code onEmpty}.
     */
    public char poll(char onEmpty) {
        if (isEmpty()) {
            return onEmpty;
        }
        return removeUnsafe();
    }

    /**
     * Returns the element at the head of the ring buffer if the ring buffer is non-empty.
     *
     * @return The head element if the ring buffer is non-empty
     * @throws NoSuchElementException if the buffer is empty
     */
    public char element() {
        return internalBuffer.element();
    }

    /**
     * Returns the element at the head of the ring buffer if the ring buffer is non-empty. Otherwise returns
     * {@code onEmpty}.
     *
     * @param onEmpty the value to return if the ring buffer is empty
     * @return The head element if the ring buffer is non-empty
     */
    public char peek(char onEmpty) {
        return internalBuffer.peek(onEmpty);
    }

    /**
     * Returns the element at the head of the ring buffer
     *
     * @return The element at the head of the ring buffer
     * @throws NoSuchElementException if the buffer is empty
     */
    public char front() {
        return front(0);
    }

    /**
     * Returns the element at the specified offset from the head in the ring buffer.
     *
     * @param offset The specified offset.
     * @return The element at the specified offset
     * @throws NoSuchElementException if the buffer is empty
     */
    public char front(int offset) {
        return internalBuffer.front(offset);
    }

    /**
     * Returns the element at the tail of the ring buffer
     *
     * @return The element at the tail of the ring buffer
     * @throws NoSuchElementException if the buffer is empty
     */
    public char back() {
        return internalBuffer.back();
    }

    /**
     * Returns the element at the tail of the ring buffer if the ring buffer is non-empty. Otherwise returns
     * {@code onEmpty}.
     *
     * @param onEmpty the value to return if the ring buffer is empty
     * @return The element at the tail of the ring buffer, otherwise the value of {@code onEmpty}.
     */
    public char peekBack(char onEmpty) {
        return internalBuffer.peekBack(onEmpty);
    }

    /**
     * Returns an array containing all elements in the ring buffer.
     *
     * @return An array containing a copy of the elements in the ring buffer.
     */
    public char[] getAll() {
        return internalBuffer.getAll();
    }

    /**
     * Adds a value without overflow detection. The caller must ensure that there is at least one element of free space
     * in the ring buffer before calling this method. The caller may use {@link #ensureRemaining(int)} or
     * {@link #remaining()} for this purpose.
     *
     * @param e the value to add to the buffer
     */
    public void addUnsafe(char e) {
        // Perform a specialized version of the fix-up test.
        if (internalBuffer.tail >= CharRingBuffer.FIXUP_THRESHOLD) {
            // Reset calc[Head, Tail] so that they have a smaller absolute value but still represent the
            // same position that they did before (modulo internalBuffer.storage.length) . Furthermore
            // arrange things so that calc[Head, Tail] is ahead of (and therefore does not overlap)
            // [head, tail]. The rationale for this is that if the two ranges did not intersect before the
            // adjustment, we do not want them to intersect after the adjustment. However if they *did*
            // intersect before the adjustment, we will lose that relationship. The downside in that case
            // is that the next evalute() may do more work than is necessary. This case is so unlikely
            // and infrequent that it is not worth the programming effort of trying to do better.
            long length = calcTail - calcHead;
            calcHead = (calcHead & internalBuffer.mask);
            calcTail = calcHead + length;

            length = internalBuffer.tail - internalBuffer.head;
            internalBuffer.head = (internalBuffer.head & internalBuffer.mask) + internalBuffer.storage.length;
            internalBuffer.tail = internalBuffer.head + length;
        }
        internalBuffer.addUnsafe(e);
    }

    /**
     * Adds the identity element to the ring buffer. Throws an exception if the buffer is full and not growable.
     *
     * @throws UnsupportedOperationException when {@code growable} is {@code false} and buffer is full
     */
    public void addIdentityValue() {
        add(identityVal);
    }

    /**
     * Removes an element without empty buffer detection. The caller must ensure that there is at least one element in
     * the ring buffer. The {@link #size()} method may be used for this purpose.
     *
     * @return the value removed from the buffer
     */
    public char removeUnsafe() {
        // NOTE: remove() calls for this data structure must replace the removed value with identityVal.
        final long prevHead = internalBuffer.head;

        final char val = internalBuffer.removeUnsafe();

        // Reset the storage entry to the identity value
        final int idx = (int) (prevHead & internalBuffer.mask);
        internalBuffer.storage[idx] = identityVal;

        return val;
    }

    /**
     * Removes multiple elements from the front of the ring buffer and returns the items as an array.
     *
     * @param count The number of elements to remove.
     * @throws NoSuchElementException if the buffer is empty
     */
    public char[] remove(int count) {
        // NOTE: remove() calls for this data structure must replace the removed value with identityVal.
        final long prevHead = internalBuffer.head;

        final char[] result = internalBuffer.remove(count);

        // Reset the cleared storage entries to the identity value
        fillWithIdentityVal(prevHead, count);

        return result;
    }

    /**
     * Removes all elements from the ring buffer and resets the data structure. This may require resetting all entries
     * in the storage buffer and evaluation tree and should be considered to be of complexity O(capacity) instead of
     * O(size).
     */
    public void clear() {
        final long prevHead = internalBuffer.head;
        final int prevSize = size();

        // Reset the pointers in the ring buffer without clearing the storage array. This leaves existing `identityVal`
        // entries in place for the next `evaluate()` call.
        internalBuffer.head = internalBuffer.tail = 0;

        calcHead = calcTail = 0;

        // Reset the previously populated storage entries to the identity value. After this call, all entries in the
        // storage buffer are `identityVal`
        fillWithIdentityVal(prevHead, prevSize);

        // Reset the tree buffer with the identity value
        Arrays.fill(treeStorage, identityVal);
    }

    /**
     * Fills a range of elements with the identity value.
     *
     * @param head The un-normalized starting point for the fill operation
     * @param count The number of elements to fill
     */
    private void fillWithIdentityVal(long head, int count) {
        final int storageHead = (int) (head & internalBuffer.mask);

        // firstCopyLen is either the size of the ring buffer, the distance from head to the end of the storage array,
        // or the count, whichever is smallest.
        final int firstCopyLen = Math.min(internalBuffer.storage.length - storageHead, count);

        // secondCopyLen is either the number of uncopied elements remaining from the first copy,
        // or the remaining to copy from count, whichever is smaller.
        final int secondCopyLen = count - firstCopyLen;

        Arrays.fill(internalBuffer.storage, storageHead, storageHead + firstCopyLen, identityVal);
        Arrays.fill(internalBuffer.storage, 0, secondCopyLen, identityVal);
    }

    // region evaluation

    /**
     * Return the result of aggregation function applied to the contents of the aggregating ring buffer.
     *
     * @return The result of (@code aggFunction()} applied to each value in the buffer
     */
    public char evaluate() {
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

        if (r1Tail - r1Head >= internalBuffer.storage.length || r2Tail - r2Head >= internalBuffer.storage.length) {
            // Evaluate everything
            fixupTree(0, internalBuffer.storage.length, 0, 0);
        } else {
            fixupTree(r1Head, r1Tail, r2Head, r2Tail);
        }

        // Store the range of items that have been computed.
        calcHead = internalBuffer.head;
        calcTail = internalBuffer.tail;

        // Return the root of the tree.
        return treeStorage[1];
    }

    /**
     * This function will accept the two un-normalized dirty ranges and apply the aggregation function to those ranges
     * in the storage ring buffer and successively to the tree of evaluation results until a single result is available
     * at the root of the tree.
     * <p>
     * The provided ranges are half-open intervals. The head is included but the tail is excluded from consideration.
     */
    void fixupTree(final long r1Head, final long r1Tail, final long r2Head, final long r2Tail) {
        final long r1Size = r1Tail - r1Head;
        final long r2Size = r2Tail - r2Head;

        // Compute the offset to store the results from the storage array to the tree array.
        final int offset = internalBuffer.storage.length / 2;

        if (r1Size == 0 && r2Size == 0) {
            // No ranges to compute.
            return;
        }

        if (r2Size == 0) {
            // Only r1 to compute.
            final int r1HeadNormal = (int) (r1Head & internalBuffer.mask);
            final int r1TailNormal = (int) (r1HeadNormal + r1Size);

            if (r1TailNormal <= internalBuffer.storage.length) {
                // r1 did not wrap. Single range to compute
                final int r1h = r1HeadNormal;
                final int r1t = r1TailNormal - 1; // change to inclusive

                // Evaluate the values in the storage buffer (results stored in the tree)
                evaluateAndStoreResults(r1h, r1t, internalBuffer.storage, offset, aggInitialFunction);
                // Evaluate the single range in the tree.
                evaluateRange(offset + (r1h / 2), offset + (r1t / 2),
                        aggTreeFunction);
                return;
            }

            // r1 wraps the ring buffer boundary. Process as two ranges.
            final int r1h = 0;
            final int r1t = r1TailNormal - internalBuffer.storage.length - 1; // change to inclusive
            final int r2h = r1HeadNormal;
            final int r2t = internalBuffer.storage.length - 1; // change to inclusive

            // Evaluate the values in the storage buffer (results stored in the tree)
            evaluateAndStoreResults(r1h, r1t, internalBuffer.storage, offset, aggInitialFunction);
            evaluateAndStoreResults(r2h, r2t, internalBuffer.storage, offset, aggInitialFunction);

            // Evaluate the two ranges in the tree.
            evaluateTwoRanges(offset + (r1h / 2), offset + (r1t / 2),
                    offset + (r2h / 2), offset + (r2t / 2),
                    aggTreeFunction);
        }

        // r1 and r2 both need to be evaluated. Take advantage of the fact that at most one will wrap. Also sort the
        // ranges now to be more efficient through the rest of the algorithm
        final int r1HeadNormal;
        final int r1TailNormal;
        final int r2HeadNormal;
        final int r2TailNormal;
        {
            final int r1HeadTmp = (int) (r1Head & internalBuffer.mask);
            final int r1TailTmp = (int) (r1HeadTmp + r1Size);
            final int r2HeadTmp = (int) (r2Head & internalBuffer.mask);
            final int r2TailTmp = (int) (r2HeadTmp + r2Size);

            if (r1HeadTmp <= r2HeadTmp) {
                r1HeadNormal = r1HeadTmp;
                r1TailNormal = r1TailTmp;
                r2HeadNormal = r2HeadTmp;
                r2TailNormal = r2TailTmp;
            } else {
                r1HeadNormal = r2HeadTmp;
                r1TailNormal = r2TailTmp;
                r2HeadNormal = r1HeadTmp;
                r2TailNormal = r1TailTmp;
            }
        }

        if (r1TailNormal <= internalBuffer.storage.length && r2TailNormal <= internalBuffer.storage.length) {
            // Neither range wraps and r1h <= r2h so we can evaluate directly.
            final int r1h = r1HeadNormal;
            final int r1t = r1TailNormal - 1; // change to inclusive
            final int r2h = r2HeadNormal;
            final int r2t = r2TailNormal - 1; // change to inclusive

            // Evaluate the values from the storage buffer and store into the evaluation tree.
            evaluateAndStoreResults(r1h, r1t, internalBuffer.storage, offset, aggInitialFunction);
            evaluateAndStoreResults(r2h, r2t, internalBuffer.storage, offset, aggInitialFunction);

            // Evaluate the two disjoint ranges in the tree.
            evaluateTwoRanges(offset + (r1h / 2), offset + (r1t / 2),
                    offset + (r2h / 2), offset + (r2t / 2),
                    aggTreeFunction);
            return;
        }
        if (r1TailNormal <= internalBuffer.storage.length) {
            // r2 wraps, but r1 does not.
            final int r1h = 0;
            final int r1t = r2TailNormal - internalBuffer.storage.length - 1; // change to inclusive
            final int r2h = r1HeadNormal;
            final int r2t = r1TailNormal - 1; // change to inclusive
            final int r3h = r2HeadNormal;
            final int r3t = internalBuffer.storage.length - 1; // change to inclusive

            // Evaluate the values from the storage buffer and store into the evaluation tree.
            evaluateAndStoreResults(r1h, r1t, internalBuffer.storage, offset, aggInitialFunction);
            evaluateAndStoreResults(r2h, r2t, internalBuffer.storage, offset, aggInitialFunction);
            evaluateAndStoreResults(r3h, r3t, internalBuffer.storage, offset, aggInitialFunction);

            // Evaluate the three disjoint ranges in the tree.
            evaluateThreeRanges(offset + (r1h / 2), offset + (r1t / 2),
                    offset + (r2h / 2), offset + (r2t / 2),
                    offset + (r3h / 2), offset + (r3t / 2),
                    aggTreeFunction);
            return;
        }
        // r1 wraps, r2 does not.
        final int r1h = 0;
        final int r1t = r1TailNormal - internalBuffer.storage.length - 1; // change to inclusive
        final int r2h = r2HeadNormal;
        final int r2t = r2TailNormal - 1; // change to inclusive
        final int r3h = r1HeadNormal;
        final int r3t = internalBuffer.storage.length - 1; // change to inclusive

        // Evaluate the values from the storage buffer and store into the evaluation tree.
        evaluateAndStoreResults(r1h, r1t, internalBuffer.storage, offset, aggInitialFunction);
        evaluateAndStoreResults(r2h, r2t, internalBuffer.storage, offset, aggInitialFunction);
        evaluateAndStoreResults(r3h, r3t, internalBuffer.storage, offset, aggInitialFunction);

        // Evaluate the three disjoint ranges in the tree.
        evaluateThreeRanges(offset + (r1h / 2), offset + (r1t / 2),
                offset + (r2h / 2), offset + (r2t / 2),
                offset + (r3h / 2), offset + (r3t / 2),
                aggTreeFunction);
    }

    /**
     * This function accepts three disjoint ranges in the evaluation tree and iteratively computes aggregation results
     * of value pairs in the range which are stored in parent nodes. Each iteration will shrink the ranges by a factor
     * of 2. As the ranges shrink, they will eventually overlap. When this happens, this function will transfer to
     * {@link #evaluateTwoRanges(int, int, int, int, CharFunction)} for better performance.
     * <p>
     * The provided ranges ore closed-interval. The head and tail are both included in the range.
     */
    private void evaluateThreeRanges(int r1h, int r1t, int r2h, int r2t, int r3h, int r3t, CharFunction evalFunction) {
        while (true) {
            if (r1t >= r2h) {
                // r1 and r2 overlap. Collapse them together and call the two range version.
                evaluateTwoRanges(r1h, r2t, r3h, r3t, evalFunction);
                return;
            }
            if (r2t >= r3h) {
                // r2 and r3 overlap. Collapse them together and call the two range version.
                evaluateTwoRanges(r1h, r1t, r2h, r3t, evalFunction);
                return;
            }

            // No collapse is possible. Evaluate the disjoint ranges.
            evaluateAndStoreResults(r1h, r1t, treeStorage, 0, evalFunction);
            evaluateAndStoreResults(r2h, r2t, treeStorage, 0, evalFunction);
            evaluateAndStoreResults(r3h, r3t, treeStorage, 0, evalFunction);

            // Determine the new parent ranges and loop until two ranges collapse
            r1h /= 2;
            r1t /= 2;
            r2h /= 2;
            r2t /= 2;
            r3h /= 2;
            r3t /= 2;
        }
    }

    /**
     * This function accepts two disjoint ranges in the evaluation tree and iteratively computes aggregation results of
     * value pairs in the range which are stored in parent nodes. Each iteration will shrink the ranges by a factor of
     * 2. As the ranges shrink, they will eventually overlap. When this happens, this function will transfer to
     * {@link #evaluateRange(int, int, CharFunction)} for better performance.
     * <p>
     * The provided ranges ore closed-interval. The head and tail are both included in the range.
     */
    private void evaluateTwoRanges(int r1h, int r1t, int r2h, int r2t, CharFunction evalFunction) {
        while (true) {
            if (r1t >= r2h) {
                // r1 and r2 overlap. Collapse them together and call the single range version.
                evaluateRange(r1h, r2t, evalFunction);
                return;
            }

            // No collapse is possible. Evaluate the disjoint ranges.
            evaluateAndStoreResults(r1h, r1t, treeStorage, 0, evalFunction);
            evaluateAndStoreResults(r2h, r2t, treeStorage, 0, evalFunction);

            // Determine the new parent ranges and loop until two ranges collapse
            r1h /= 2;
            r1t /= 2;
            r2h /= 2;
            r2t /= 2;
        }
    }

    /**
     * This function accepts a range in the evaluation tree and iteratively computes aggregation results of value pairs
     * in the range which are stored in parent nodes. Each iteration will shrink the range by a factor of 2. When the
     * tail of the range reaches 1, we have computed the root of the tree (stored in position 1) and can stop
     * evaluating. Position 0 is not used.
     * <p>
     * The provided range is closed-interval. The head and tail are both included in the range.
     */
    private void evaluateRange(int r1h, int r1t, CharFunction evalFunction) {
        while (r1t > 1) {
            // Evaluate the single range
            evaluateAndStoreResults(r1h, r1t, treeStorage, 0, evalFunction);

            // Determine the new parent range and loop until the range reaches the root (tail = 1).
            r1h /= 2;
            r1t /= 2;
        }
    }

    /**
     * This function aggregates pairs of values from the provided buffer and stores the results in parent nodes of the
     * evaluation tree. To populate of the leaf nodes of the tree (which are the results of aggregation from the storage
     * buffer) the source array and an offset can be specified.
     * <p>
     * The source of the data is either the storage buffer (when we are evaluating the bottommost row of the tree) or
     * the tree area (at all other times). The destination is always the tree area.
     */
    private void evaluateAndStoreResults(int start, int end, char[] src, int dstOffset, CharFunction evalFunction) {
        // Everything from start to end (inclusive) should be evaluated
        for (int left = start & 0xFFFFFFFE; left <= end; left += 2) {
            final int right = left + 1;
            final int parent = left / 2;

            // Load the data values from either the storage buffer or the tree area
            final char leftVal = src[left];
            final char rightVal = src[right];

            // Compute and store. Unlike src, which may be point to the storage buffer or tree area,
            // the destination is always in the tree area.
            final char computeVal = evalFunction.apply(leftVal, rightVal);
            treeStorage[parent + dstOffset] = computeVal;
        }
    }
    // endregion evaluation
}
