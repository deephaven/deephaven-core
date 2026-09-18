//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
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

    /**
     * How the {@code nToUnion} row sets relate to one another. Each shape exists because it separates merge strategies
     * that agree everywhere else; see {@code engine/rowset/docs/rowset-union-performance.md}.
     */
    public enum Shape {
        /** Every set walks the same span from key 0, so all of them overlap all of the others. */
        REDUNDANT,
        /** Successive sets overlap halfway into the previous one. */
        PARTIAL,
        /** Disjoint sets laid end to end, presented in random order. */
        BLOCKS_SHUFFLED,
        /** Abutting ranges dealt round robin, so ranges touch across sets but not within one. */
        ADJACENT,
        /** Ranges with gaps dealt round robin: disjoint sets that all span the key space. */
        INTERLEAVED,
        /**
         * Set {@code i} of {@code n} holds the two keys {@code i << 16} and {@code (1 << 30) + ((n + i) << 16)}: two
         * single keys, each in a block no other set touches, the low keys in blocks {@code [0, n)} and the high keys in
         * blocks {@code [16384 + n, 16384 + 2n)}. Inserted one set at a time in first-key order, every low key splices
         * a span into the middle of an array whose tail is every earlier set's high key, so sequential insertion shifts
         * {@code i} spans for set {@code i} and is quadratic in the set count. Two keys per set, so {@code totalRows}
         * is ignored; the sets are {@link io.deephaven.engine.rowset.impl.sortedranges.SortedRanges}, and at
         * {@code nToUnion} above 4096 their entries overflow one.
         */
        NEW_BLOCKS,
        /**
         * Sets of 50 single keys each, scattered over 64 regions of 10M rows, addressed as regioned column sources
         * address them: region index in the high bits, 2^43 keys apart. The blocks touched are dense within each region
         * and the block range between regions is empty, so this is what per-region or per-key row sets of a partitioned
         * table look like to the union. Ignores {@code totalRows}.
         */
        REGIONED
    }

    @Param({"REDUNDANT"})
    private Shape shape;

    /** {@link RowSetFactory#unionStrategy} for the {@link #union} and {@link #unionBatcher} cells. */
    @Param({"MERGE_IN_PASSES", "RADIX"})
    private RowSetFactory.UnionStrategy strategy;

    /** {@link RowSetUnionBatcher#maxBatchSize} for the {@link #unionBatcher} cells. */
    @Param({"8192"})
    private int batchCap;

    private RowSet[] toUnion;
    private WritableRowSet actual;
    private RowSet expected;

    @Setup(Level.Trial)
    public void setupTrial() {
        RowSetFactory.unionStrategy = strategy;
        RowSetUnionBatcher.maxBatchSize = batchCap;
        final int targetIndexSize = totalRows / nToUnion;
        final Random randy = new Random(nToUnion ^ targetIndexSize);
        final RowSetBuilderRandom rb = RowSetFactory.builderRandom();
        toUnion = new RowSet[nToUnion];
        switch (shape) {
            case REDUNDANT:
            case PARTIAL:
            case BLOCKS_SHUFFLED: {
                // Each set is a random walk of ranges and gaps; the shape is where each walk starts relative to the
                // one before it, taken from that walk's actual extent so that BLOCKS_SHUFFLED sets never touch and
                // PARTIAL sets overlap the previous one by half whatever length its walk came to.
                long previousStart = 0;
                long previousLast = -1;
                for (int indexNo = 0; indexNo < nToUnion; indexNo++) {
                    final long start = shape == Shape.REDUNDANT ? 0
                            : shape == Shape.PARTIAL ? previousStart + (previousLast - previousStart + 1) / 2
                                    : previousLast + 1;
                    final RowSetBuilderSequential sb = RowSetFactory.builderSequential();
                    long lastKey = start;
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
                    previousStart = start;
                    previousLast = lastKey - 1;
                }
                if (shape == Shape.BLOCKS_SHUFFLED) {
                    for (int i = nToUnion - 1; i > 0; --i) {
                        final int j = randy.nextInt(i + 1);
                        final RowSet swap = toUnion[i];
                        toUnion[i] = toUnion[j];
                        toUnion[j] = swap;
                    }
                }
                break;
            }
            case ADJACENT:
            case INTERLEAVED: {
                // One walk over the key space, dealing each range to the next set in turn.
                final RowSetBuilderSequential[] builders = new RowSetBuilderSequential[nToUnion];
                for (int indexNo = 0; indexNo < nToUnion; indexNo++) {
                    builders[indexNo] = RowSetFactory.builderSequential();
                }
                long key = 0;
                int dealTo = 0;
                for (long rowCount = 0; rowCount < totalRows;) {
                    final long rs = key;
                    final long re = rs + randy.nextInt(100);
                    builders[dealTo].appendRange(rs, re);
                    rb.addRange(rs, re);
                    rowCount += re - rs + 1;
                    key = re + 1 + (shape == Shape.INTERLEAVED ? randy.nextInt(100) : 0);
                    dealTo = dealTo + 1 == nToUnion ? 0 : dealTo + 1;
                }
                for (int indexNo = 0; indexNo < nToUnion; indexNo++) {
                    toUnion[indexNo] = builders[indexNo].build();
                }
                break;
            }
            case REGIONED: {
                final int regions = 64;
                final long regionRows = 10_000_000L;
                final int regionBits = 43;
                for (int indexNo = 0; indexNo < nToUnion; indexNo++) {
                    final RowSetBuilderRandom sb = RowSetFactory.builderRandom();
                    for (int k = 0; k < 50; ++k) {
                        final long key = ((long) randy.nextInt(regions) << regionBits)
                                + (long) (randy.nextDouble() * regionRows);
                        sb.addKey(key);
                        rb.addKey(key);
                    }
                    toUnion[indexNo] = sb.build();
                }
                break;
            }
            case NEW_BLOCKS: {
                for (int indexNo = 0; indexNo < nToUnion; indexNo++) {
                    final long low = (long) indexNo << 16;
                    final long high = (1L << 30) + ((long) (nToUnion + indexNo) << 16);
                    final RowSetBuilderSequential sb = RowSetFactory.builderSequential();
                    sb.appendKey(low);
                    sb.appendKey(high);
                    rb.addKey(low);
                    rb.addKey(high);
                    toUnion[indexNo] = sb.build();
                }
                break;
            }
            default:
                throw new IllegalStateException(shape.toString());
        }

        expected = rb.build();
    }

    /** Every invocation's result is checked and closed, so no result outlives the invocation that built it. */
    @TearDown(Level.Invocation)
    public void validateResult() {
        if (actual == null) {
            return;
        }
        try {
            if (!actual.equals(expected)) {
                throw new IllegalStateException();
            }
        } finally {
            actual.close();
            actual = null;
        }
    }

    @TearDown(Level.Trial)
    public void closeInputs() {
        for (final RowSet rowSet : toUnion) {
            rowSet.close();
        }
        expected.close();
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

    /** {@link RowSetFactory#union} under the {@link #strategy} in force. */
    @Benchmark
    public void union() {
        actual = RowSetFactory.union(toUnion);
    }

    /**
     * The same union through {@link RowSetUnionBatcher}, as the converted call sites reach it: every input handed over
     * as a copy, at most {@link RowSetUnionBatcher#maxBatchSize} to a batch, each batch merged under the
     * {@link #strategy} in force and the batches folded together at the end.
     */
    @Benchmark
    public void unionBatcher() {
        try (final RowSetUnionBatcher batcher = new RowSetUnionBatcher(toUnion.length)) {
            for (final RowSet rowSet : toUnion) {
                batcher.add(rowSet.copy());
            }
            actual = batcher.build();
        }
    }

    @Benchmark
    public void unionIteratorRuns() {
        actual = unionIteratorRuns(toUnion);
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

    /**
     * Union through the same priority queue of iterators, but drain each popped iterator for as long as the rest of the
     * queue allows instead of re-enqueueing it after every range. While the popped iterator's current range starts at
     * or before the queue head's, no other iterator can produce an earlier range, so its ranges are globally next and
     * can be appended without consulting the queue; when it passes the head it goes back in. That is one heap operation
     * per run of ranges rather than one per range. Ranges are still clipped against the last one appended, since a run
     * may end past where another iterator starts.
     */
    private static WritableRowSet unionIteratorRuns(final RowSet... indices) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        final IndexRangeFirstKeyPriorityQueue pq = new IndexRangeFirstKeyPriorityQueue(indices.length);
        for (final RowSet index : indices) {
            final RowSet.RangeIterator itToAdd = index.rangeIterator();
            if (itToAdd.hasNext()) {
                itToAdd.next();
                pq.add(itToAdd);
            } else {
                itToAdd.close();
            }
        }

        long lastAppendedRangeEnd = -1;
        while (!pq.isEmpty()) {
            final RowSet.RangeIterator it = pq.pop();
            final RowSet.RangeIterator nextIt = pq.peek();
            final long nextStart = nextIt == null ? Long.MAX_VALUE : nextIt.currentRangeStart();

            boolean exhausted = false;
            while (true) {
                final long rangeToAppendEnd = it.currentRangeEnd();
                if (rangeToAppendEnd > lastAppendedRangeEnd) {
                    final long rangeToAppendStart = Math.max(it.currentRangeStart(), lastAppendedRangeEnd + 1);
                    builder.appendRange(rangeToAppendStart, rangeToAppendEnd);
                    lastAppendedRangeEnd = rangeToAppendEnd;
                }

                if (!it.hasNext()) {
                    exhausted = true;
                    break;
                }
                it.next();
                if (it.currentRangeStart() > nextStart) {
                    break;
                }
            }

            if (exhausted) {
                it.close();
            } else {
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

        /**
         * The top element, left in the queue.
         *
         * @return The item at the top, or null when the queue is empty
         */
        public RowSet.RangeIterator peek() {
            return size == 0 ? null : iterators[1];
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
