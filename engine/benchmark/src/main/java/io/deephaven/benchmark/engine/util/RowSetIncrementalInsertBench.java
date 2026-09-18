//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.util;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * One update cycle of a bucketed, ticking {@code updateBy}, reduced to the row set work it does per bucket.
 *
 * <p>
 * Each cycle a contiguous run of {@code addedPerCycle} rows arrives at the top of a {@code rows}-key table. Rows are
 * dealt across {@code buckets} buckets according to {@code layout}, and each dirty bucket reports the added rows it
 * received plus the {@code windowRows} rows before them as a small row set: a {@link SortedRanges} when there are many
 * buckets, an {@link RspBitmap} when there are few and each bucket's rows are dense. {@code updateBy} accumulates those
 * per-bucket sets into one row set (the modified rows, and the rows each input source must supply), so the accumulator
 * sees one small insert per dirty bucket: an {@link RspBitmap} once it outgrows {@link SortedRanges}, and every insert
 * lands well below its last block.
 *
 * <p>
 * The layout decides whether merging two buckets' sets shrinks anything. Under {@link Layout#ROUND_ROBIN} a bucket's
 * rows are single keys exactly {@code buckets} apart, so buckets adjacent in key order have adjacent keys and every
 * pairwise merge coalesces ranges; the merge tree's later passes are over ever fewer ranges. Under
 * {@link Layout#RANDOM} each row lands in a uniformly random bucket, which is how the nightly {@code updateBy}
 * benchmarks generate their keys and what keyed data generally looks like: a bucket's rows are still single keys, but
 * no two buckets' keys are ever adjacent, so nothing coalesces and every pass over the merge tree walks every range
 * again.
 *
 * <p>
 * {@link #rspIxInsert} is the current {@link RspBitmap#ixInsert} path, which for a {@link SortedRanges} runs a pre-pass
 * over the incoming ranges before adding them; {@link #rspAddRangesDirect} is the body that
 * {@code insertOrderedLongSetUnsafeNoWriteCheck} had before DH-23407, adding the ranges directly. The two do the same
 * thing for an {@link RspBitmap} bucket. {@link #rowSetApiInsert} is the same work through the {@link WritableRowSet}
 * API, including the accumulator's growth from a single range through {@link SortedRanges} into an {@link RspBitmap},
 * so it can be run unchanged against builds on either side of the change.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(value = 1)
public class RowSetIncrementalInsertBench {

    private static final long RANDOM_SEED = 1;

    /** Distinct bucket-key combinations; the 1, 2 and 3 group scales of the nightly updateBy benchmarks. */
    @Param({"100", "10000", "100000"})
    private int buckets;

    /** Key space of the source table. */
    @Param({"10000000"})
    private long rows;

    /** Rows the incremental release filter lets through per cycle. */
    @Param({"100000"})
    private int addedPerCycle;

    /** Rows before the added ones that each dirty bucket needs, e.g. the reverse window of a rolling operation. */
    @Param({"50"})
    private int windowRows;

    /** How rows are dealt to buckets; see the class comment for what each does to the merge. */
    @Param({"ROUND_ROBIN", "RANDOM"})
    private Layout layout;

    public enum Layout {
        /** Row {@code k} belongs to bucket {@code k % buckets}. */
        ROUND_ROBIN,
        /** Each row belongs to a uniformly random bucket. */
        RANDOM
    }

    /** {@link RowSetFactory#unionStrategy} for the {@link #rowSetApiUnion} cells. */
    @Param({"SHIPPED", "RADIX"})
    private RowSetFactory.UnionStrategy unionStrategy;

    private long frontier;
    private OrderedLongSet[] bucketAffectedSets;
    private WritableRowSet[] bucketAffectedRowSets;

    @Setup
    public void setup() {
        RowSetFactory.unionStrategy = unionStrategy;
        frontier = rows - addedPerCycle;
        final long windowStart = Math.max(0, frontier - (long) windowRows * buckets);
        bucketAffectedSets = new OrderedLongSet[buckets];
        bucketAffectedRowSets = new WritableRowSet[buckets];
        final Random random = new Random(RANDOM_SEED);
        // Every key from the start of the window region through the end of the added rows belongs to exactly one
        // bucket, so each bucket's affected set averages windowRows plus addedPerCycle / buckets keys under either
        // layout; only where those keys fall differs.
        final RowSetBuilderSequential[] builders = new RowSetBuilderSequential[buckets];
        for (int bucket = 0; bucket < buckets; ++bucket) {
            builders[bucket] = RowSetFactory.builderSequential();
        }
        final boolean[] dirty = new boolean[buckets];
        for (long key = windowStart; key < rows; ++key) {
            final int bucket = layout == Layout.ROUND_ROBIN
                    ? (int) Math.floorMod(key, (long) buckets)
                    : random.nextInt(buckets);
            builders[bucket].appendKey(key);
            if (key >= frontier) {
                dirty[bucket] = true;
            }
        }
        // Only buckets that received an added row are dirty, and updateBy unions only those; under a random layout
        // some buckets receive none this cycle and are left out, lookback rows and all.
        int dirtyCount = 0;
        long totalKeys = 0;
        for (int bucket = 0; bucket < buckets; ++bucket) {
            final WritableRowSet affected = builders[bucket].build();
            if (!dirty[bucket]) {
                affected.close();
                continue;
            }
            totalKeys += affected.size();
            bucketAffectedRowSets[dirtyCount] = affected;
            bucketAffectedSets[dirtyCount++] = ((WritableRowSetImpl) affected).getInnerSet();
        }
        bucketAffectedRowSets = Arrays.copyOf(bucketAffectedRowSets, dirtyCount);
        bucketAffectedSets = Arrays.copyOf(bucketAffectedSets, dirtyCount);
        // Buckets go dirty in an order unrelated to their keys.
        for (int i = dirtyCount - 1; i > 0; --i) {
            final int j = random.nextInt(i + 1);
            final OrderedLongSet set = bucketAffectedSets[i];
            bucketAffectedSets[i] = bucketAffectedSets[j];
            bucketAffectedSets[j] = set;
            final WritableRowSet rs = bucketAffectedRowSets[i];
            bucketAffectedRowSets[i] = bucketAffectedRowSets[j];
            bucketAffectedRowSets[j] = rs;
        }
        System.out.println("buckets=" + buckets + " dirtyBuckets=" + dirtyCount
                + " keysPerDirtyBucket=" + (totalKeys / dirtyCount) + " keysInsertedPerCycle=" + totalKeys
                + " bucketSetType=" + bucketAffectedSets[0].getClass().getSimpleName());
        describeWorkload();
        checkUnionAgreesWithInsert();
    }

    /**
     * The merge under test is configurable, so make sure the configuration in force still produces the same row set as
     * the insert loop before timing it.
     */
    private void checkUnionAgreesWithInsert() {
        final List<RowSet> toUnion = new ArrayList<>(bucketAffectedRowSets.length + 1);
        try (final WritableRowSet expected = RowSetFactory.fromRange(frontier, rows - 1);
                final WritableRowSet seed = RowSetFactory.fromRange(frontier, rows - 1)) {
            for (final WritableRowSet affected : bucketAffectedRowSets) {
                expected.insert(affected);
            }
            toUnion.add(seed);
            toUnion.addAll(Arrays.asList(bucketAffectedRowSets));
            try (final WritableRowSet actual = RowSetFactory.union(toUnion)) {
                if (!actual.equals(expected)) {
                    throw new IllegalStateException("union disagrees with insert under " + unionStrategy + ": "
                            + actual.size() + " vs " + expected.size());
                }
            }
        }
    }

    /**
     * Report the shape the searches see over one cycle: how long the accumulator's spans array gets, how many ranges
     * each per-bucket set brings, and how often a range lands in the same block as the range before it.
     */
    private void describeWorkload() {
        long ranges = 0;
        long rangesInSameBlockAsPrevious = 0;
        int maxSpans = 0;
        RspBitmap accumulator = new RspBitmap(frontier, rows - 1);
        for (final OrderedLongSet affected : bucketAffectedSets) {
            long previousBlock = -1;
            try (final io.deephaven.engine.rowset.RowSet.RangeIterator it = affected.ixRangeIterator()) {
                while (it.hasNext()) {
                    it.next();
                    final long block = it.currentRangeStart() >> 16;
                    ++ranges;
                    if (block == previousBlock) {
                        ++rangesInSameBlockAsPrevious;
                    }
                    previousBlock = block;
                }
            }
            accumulator = accumulator.ixInsert(affected);
            maxSpans = Math.max(maxSpans, accumulator.size());
        }
        System.out.println("rangesPerBucketSet=" + (ranges / buckets)
                + " rangesInSameBlockAsPrevious=" + (100 * rangesInSameBlockAsPrevious / ranges) + "%"
                + " accumulatorSpansMax=" + maxSpans + " accumulatorSpansFinal=" + accumulator.size()
                + " accumulatorBlocks=" + ((rows - 1) / RspBitmap.BLOCK_SIZE
                        - (frontier - (long) windowRows * buckets) / RspBitmap.BLOCK_SIZE + 1));
    }

    @Benchmark
    public long rowSetApiInsert() {
        try (final WritableRowSet accumulator = RowSetFactory.fromRange(frontier, rows - 1)) {
            for (final WritableRowSet affected : bucketAffectedRowSets) {
                accumulator.insert(affected);
            }
            return accumulator.size();
        }
    }

    /**
     * The same accumulation through {@link RowSetFactory#union}, which orders the per-bucket sets and merges them in
     * passes instead of inserting each one into a single growing accumulator. {@code updateBy} builds its per-window
     * and downstream modified row sets with the insert loop above, so this is what moving those to the factory costs or
     * saves. The list is built per invocation because a caller would have to build one too.
     */
    @Benchmark
    public long rowSetApiUnion() {
        final List<RowSet> toUnion = new ArrayList<>(bucketAffectedRowSets.length + 1);
        try (final WritableRowSet seed = RowSetFactory.fromRange(frontier, rows - 1)) {
            toUnion.add(seed);
            toUnion.addAll(Arrays.asList(bucketAffectedRowSets));
            try (final WritableRowSet accumulator = RowSetFactory.union(toUnion)) {
                return accumulator.size();
            }
        }
    }

    /**
     * The same accumulation through a random builder, walking each per-bucket set range by range. This is what
     * {@link io.deephaven.engine.rowset.RowSetBuilderRandom#addRowSet} did before the builder took the row set's
     * implementation whole, and is kept alongside {@link #rowSetBuilderRandom} to show what that is worth.
     */
    @Benchmark
    public long rowSetBuilderRandomPerRange() {
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        builder.addRange(frontier, rows - 1);
        for (final WritableRowSet affected : bucketAffectedRowSets) {
            try (final RowSet.RangeIterator it = affected.rangeIterator()) {
                while (it.hasNext()) {
                    it.next();
                    builder.addRange(it.currentRangeStart(), it.currentRangeEnd());
                }
            }
        }
        try (final WritableRowSet accumulator = builder.build()) {
            return accumulator.size();
        }
    }

    /**
     * The same accumulation through a random builder, which takes each per-bucket set whole rather than walking it.
     */
    @Benchmark
    public long rowSetBuilderRandom() {
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        builder.addRange(frontier, rows - 1);
        for (final WritableRowSet affected : bucketAffectedRowSets) {
            builder.addRowSet(affected);
        }
        try (final WritableRowSet accumulator = builder.build()) {
            return accumulator.size();
        }
    }

    @Benchmark
    public long rspIxInsert() {
        RspBitmap accumulator = new RspBitmap(frontier, rows - 1);
        for (final OrderedLongSet affected : bucketAffectedSets) {
            accumulator = accumulator.ixInsert(affected);
        }
        return accumulator.getCardinality();
    }

    @Benchmark
    public long rspAddRangesDirect() {
        RspBitmap accumulator = new RspBitmap(frontier, rows - 1);
        for (final OrderedLongSet affected : bucketAffectedSets) {
            if (!(affected instanceof SortedRanges)) {
                accumulator = accumulator.ixInsert(affected);
                continue;
            }
            accumulator = accumulator.getWriteRef();
            accumulator.addRangesUnsafeNoWriteCheck(((SortedRanges) affected).getRangeIterator());
            accumulator.finishMutations();
        }
        return accumulator.getCardinality();
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(RowSetIncrementalInsertBench.class);
    }
}
