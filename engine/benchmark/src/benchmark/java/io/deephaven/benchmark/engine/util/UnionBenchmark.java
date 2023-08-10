/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.benchmark.engine.util;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.*;
import org.openjdk.jmh.annotations.*;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(1)
public class UnionBenchmark {
    @Param({"10", "100", "1000"})
    private int nToUnion;

    @Param({"100000000"})
    private int totalRows;

    @Param({"50"})
    private int percentRanges;

    private RowSet[] toUnion;
    private WritableRowSet actual;
    private RowSet expected;

    @Setup(Level.Trial)
    public void setupTrial() {
        final int targetIndexSize = totalRows / nToUnion;
        final Random randy = new Random(nToUnion ^ targetIndexSize);
        final RowSetBuilderRandom rb = RowSetFactory.builderRandom();
        toUnion = new RowSet[nToUnion];
        for (int indexNo = 0; indexNo < nToUnion; indexNo++) {
            final RowSetBuilderSequential sb = RowSetFactory.builderSequential();
            long lastKey = 0;
            for (int rowCount = 0; rowCount < targetIndexSize;) {
                boolean insertRange = randy.nextInt(100) < percentRanges;
                if (insertRange) {
                    final long rs = randy.nextInt(100) + lastKey;
                    final long re = randy.nextInt(100) + rs;
                    sb.appendRange(rs, re);
                    rb.addRange(rs, re);
                    lastKey = re + 1;
                    rowCount += re - rs + 1;
                } else {
                    final long key = randy.nextInt(100) + lastKey;
                    sb.appendKey(key);
                    rb.addKey(key);
                    lastKey = key + 1;
                    rowCount++;
                }
            }

            toUnion[indexNo] = sb.build();
        }

        expected = rb.build();
    }

    @TearDown
    public void validateResult() {
        if (!actual.equals(expected)) {
            throw new IllegalStateException();
        }
    }

    @Benchmark
    public void unionPriortyQueue() {
        actual = unionPriorityQueue(toUnion);
    }

    @Benchmark
    public void unionRandomBuilder() {
        final RowSetBuilderRandom rb = RowSetFactory.builderRandom();
        for (final RowSet i : toUnion) {
            rb.addRowSet(i);
        }

        actual = rb.build();
    }

    @Benchmark
    public void unionInsertOnly() {
        actual = toUnion[0].copy();
        for (int ii = 1; ii < toUnion.length; ++ii) {
            actual.insert(toUnion[ii]);
        }
    }

    private static WritableRowSet unionPriorityQueue(final RowSet... indices) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        final IndexRangeFirstKeyPriorityQueue pq = new IndexRangeFirstKeyPriorityQueue(indices.length);
        for (final RowSet index : indices) {
            final RowSet.RangeIterator itToAdd = index.rangeIterator();
            if (itToAdd.hasNext()) {
                itToAdd.next();
                pq.add(itToAdd);
            }
        }

        long lastAppendedRangeEnd = -1;
        while (!pq.isEmpty()) {
            final RowSet.RangeIterator it = pq.pop();
            long rangeToAppendStart = it.currentRangeStart();
            long rangeToAppendEnd = it.currentRangeEnd();

            // Make sure that we actually have something to append, and if the ranges intersect
            // make sure we only jam in the part of the range not already included.
            if (rangeToAppendEnd > lastAppendedRangeEnd) {
                if (rangeToAppendStart > lastAppendedRangeEnd) {
                    builder.appendRange(rangeToAppendStart, rangeToAppendEnd);
                } else {
                    rangeToAppendStart = lastAppendedRangeEnd + 1;
                    builder.appendRange(rangeToAppendStart, rangeToAppendEnd);
                }

                lastAppendedRangeEnd = rangeToAppendEnd;
            }

            if (it.hasNext()) {
                it.next();
                pq.add(it);
            }
        }

        return builder.build();
    }

    // Since this implementation fared worse than using insert, I keep it here so the benchmark
    // can be re-run, and there is context
    static class IndexRangeFirstKeyPriorityQueue {
        private static final int doublingAllocThreshold = Configuration.getInstance().getIntegerWithDefault(
                "IndexFirstKeyPriorityQueue.doublingAllocThreshold", 4 * 1024 * 1024);

        // Things are nicer (integer division will be bit shift) if this is a power of 2, but it is not mandatory.
        private static final int linearAllocStep = Configuration.getInstance().getIntegerWithDefault(
                "IndexFirstKeyPriorityQueue.linearAllocStep", 1024 * 1024);

        /** The iterators, slot 0 is unused. */
        private RowSet.RangeIterator[] iterators;

        /**
         * The size of the queue (invariant: size < start.length - 1). Note since we don't use element 0 in start and
         * end arrays, this size does not match the normal invariant in array access where the last element used is an
         * array a[] is a[size - 1]; in our case the last element used is a[size].
         */
        private int size = 0;

        /**
         * Create a TrackerPriorityQueue with the given initial capacity.
         *
         * @param initialCapacity how many ranges should we allocate room for
         */
        public IndexRangeFirstKeyPriorityQueue(final int initialCapacity) {
            iterators = new RowSet.RangeIterator[initialCapacity + 1];
        }

        /**
         * Adds an element to the queue.
         */
        public void add(final RowSet.RangeIterator iter) {
            final int newSize = size + 1;
            ensureCapacityFor(newSize);

            iterators[newSize] = iter;
            size = newSize;
            fixUp(size);
        }

        /**
         * Pop the top element from the queue.
         *
         * @return The item popped
         */
        public RowSet.RangeIterator pop() {
            if (size == 0) {
                return null;
            }

            final RowSet.RangeIterator atTop = iterators[1];
            if (--size > 0) {
                iterators[1] = iterators[size + 1];
                fixDown(1);
            }

            return atTop;
        }

        private void ensureCapacityFor(final int lastIndex) {
            final int minCapacity = lastIndex + 1;
            if (minCapacity < iterators.length) {
                return;
            }

            int newCapacity = iterators.length;
            while (newCapacity < minCapacity && newCapacity < doublingAllocThreshold) {
                newCapacity = 2 * newCapacity;
            }

            if (newCapacity < minCapacity) {
                final int delta = minCapacity - doublingAllocThreshold;
                final int steps = (delta + linearAllocStep - 1) / linearAllocStep;
                newCapacity = doublingAllocThreshold + steps * linearAllocStep;
            }

            final RowSet.RangeIterator[] newiterators = new RowSet.RangeIterator[newCapacity];
            System.arraycopy(iterators, 1, newiterators, 1, size);
            iterators = newiterators;
        }

        /**
         * move queue[itemIndex] up the heap until its start is >= that of its parent.
         */
        private void fixUp(int itemIndex) {
            if (itemIndex <= 1) {
                return;
            }

            final RowSet.RangeIterator item = iterators[itemIndex];
            int parentIndex = itemIndex >> 1;
            RowSet.RangeIterator parent;
            while (itemIndex > 1 && valueOf(item) < valueOf(parent = iterators[parentIndex])) {
                iterators[itemIndex] = parent;
                itemIndex = parentIndex;
                parentIndex = itemIndex >> 1;
            }
            iterators[itemIndex] = item;
        }

        /**
         * move queue[itemIndex] down the heap until its start is <= those of its children.
         */
        private void fixDown(@SuppressWarnings("SameParameterValue") int itemIndex) {
            // Start the smallest child at the left and then adjust
            int smallestChildIdx = itemIndex << 1;
            if (smallestChildIdx > size) {
                return;
            }

            final RowSet.RangeIterator item = iterators[itemIndex];
            RowSet.RangeIterator smallestChild = iterators[smallestChildIdx];
            RowSet.RangeIterator nextChild;
            // Just pick the smallest of the two values.
            if (smallestChildIdx < size
                    && valueOf(nextChild = iterators[smallestChildIdx + 1]) < valueOf(smallestChild)) {
                smallestChild = nextChild;
                smallestChildIdx++;
            }

            if (valueOf(smallestChild) < valueOf(item)) {
                iterators[itemIndex] = smallestChild;
                itemIndex = smallestChildIdx;
                smallestChildIdx = itemIndex << 1;
                while (smallestChildIdx <= size) {
                    smallestChild = iterators[smallestChildIdx];
                    if (smallestChildIdx < size
                            && valueOf(nextChild = iterators[smallestChildIdx + 1]) < valueOf(smallestChild)) {
                        smallestChild = nextChild;
                        smallestChildIdx++;
                    }

                    if (valueOf(smallestChild) >= valueOf(item)) {
                        break;
                    }
                    iterators[itemIndex] = smallestChild;

                    itemIndex = smallestChildIdx;
                    smallestChildIdx = itemIndex << 1;
                }

                iterators[itemIndex] = item;
            }
        }

        private long valueOf(final RowSet.RangeIterator iter) {
            return iter.currentRangeStart();
        }

        public boolean isEmpty() {
            return size == 0;
        }
    }
}
