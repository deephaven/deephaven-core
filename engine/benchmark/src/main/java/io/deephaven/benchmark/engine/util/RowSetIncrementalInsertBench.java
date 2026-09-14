//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.util;

import io.deephaven.benchmarking.BenchUtil;
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

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * One update cycle of a bucketed, ticking {@code updateBy}, reduced to the row set work it does per bucket.
 *
 * <p>
 * Each cycle a contiguous run of {@code addedPerCycle} rows arrives at the top of a {@code rows}-key table. Rows are
 * dealt round robin across {@code buckets} buckets, so a bucket's rows are single keys spaced {@code buckets} apart,
 * and each dirty bucket reports the added rows it received plus the {@code windowRows} rows before them as a small row
 * set: a {@link SortedRanges} when there are many buckets, an {@link RspBitmap} when there are few and each bucket's
 * rows are dense. {@code updateBy} accumulates those per-bucket sets into one row set (the modified rows, and the rows
 * each input source must supply), so the accumulator sees one small insert per dirty bucket: an {@link RspBitmap} once
 * it outgrows {@link SortedRanges}, and every insert lands well below its last block.
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

    private long frontier;
    private OrderedLongSet[] bucketAffectedSets;
    private WritableRowSet[] bucketAffectedRowSets;

    @Setup
    public void setup() {
        frontier = rows - addedPerCycle;
        final long windowStart = Math.max(0, frontier - (long) windowRows * buckets);
        bucketAffectedSets = new OrderedLongSet[buckets];
        bucketAffectedRowSets = new WritableRowSet[buckets];
        long totalKeys = 0;
        for (int bucket = 0; bucket < buckets; ++bucket) {
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            long key = windowStart + Math.floorMod(bucket - windowStart, (long) buckets);
            for (; key < rows; key += buckets) {
                builder.appendKey(key);
                ++totalKeys;
            }
            final WritableRowSet affected = builder.build();
            bucketAffectedRowSets[bucket] = affected;
            bucketAffectedSets[bucket] = ((WritableRowSetImpl) affected).getInnerSet();
        }
        // Buckets go dirty in an order unrelated to their keys.
        final Random random = new Random(RANDOM_SEED);
        for (int i = buckets - 1; i > 0; --i) {
            final int j = random.nextInt(i + 1);
            final OrderedLongSet set = bucketAffectedSets[i];
            bucketAffectedSets[i] = bucketAffectedSets[j];
            bucketAffectedSets[j] = set;
            final WritableRowSet rs = bucketAffectedRowSets[i];
            bucketAffectedRowSets[i] = bucketAffectedRowSets[j];
            bucketAffectedRowSets[j] = rs;
        }
        System.out.println("buckets=" + buckets + " keysPerBucket=" + (totalKeys / buckets)
                + " keysInsertedPerCycle=" + totalKeys
                + " bucketSetType=" + bucketAffectedSets[0].getClass().getSimpleName());
        describeWorkload();
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
