package io.deephaven.benchmark.engine.util;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.benchmarking.runner.EnvUtils;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.util.metrics.MetricsManager;
import org.apache.commons.lang3.mutable.MutableInt;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 10)
@Measurement(iterations = 5, time = 10)
@Fork(value = 1)

public class RowSetGetFindBench {

    private static final long randomSeed = 1;
    private static final int containers = 10_000;
    private static final double probRunContainer = 0.5;
    private static final int runsPerRunContainer = 1_000;
    private static final int runLen = 20;
    // RowSet size = containers * runsPerContainer*runLen.
    private static RowSet rowSet;
    private static long[] opsValues;

    private static final Random rand = new Random(randomSeed);

    // No bigger than ix.size, see above.
    @Param({"40"})
    private static long millionOps;

    @Setup(Level.Trial)
    public void setup() {
        final String rowSetFilename = System.getProperty("rowset.filename");
        if (rowSetFilename == null || rowSetFilename.equals("")) {
            rowSet = generateRandomRowSet();
        } else {
            rowSet = loadRowSet(rowSetFilename);
        }
        // To get this output we need JVM flag `-DMetricsManager.enabled=true`
        System.out.println(MetricsManager.getCounters());
        final long ops = millionOps * 1000 * 1000;
        final RowSet opRowSet = rowSet.subSetByPositionRange(0, ops);
        opsValues = new long[(int) opRowSet.size()];
        opRowSet.fillRowKeyChunk(WritableLongChunk.writableChunkWrap(opsValues));
    }

    EnvUtils.GcTimeCollector gcTimeCollector = new EnvUtils.GcTimeCollector();

    @Setup(Level.Iteration)
    public void setupIteration() {
        gcTimeCollector.resetAndStart();
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
        gcTimeCollector.stopAndSample();
        System.out.println(
                "Gc during iteration: count=" + gcTimeCollector.getCollectionCount() +
                        ", timeMs=" + gcTimeCollector.getCollectionTimeMs());
    }

    private static RowSet loadRowSet(final String rowSetFilename) {
        final RowSet exported;
        long st = System.currentTimeMillis();
        System.out.print("Loading rowset " + rowSetFilename + "... ");
        try (final ObjectInputStream ois = new ObjectInputStream(new FileInputStream(rowSetFilename))) {
            exported = (RowSet) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
            // keep the compiler happy
            return null;
        }
        System.out.println("Load done in " + (System.currentTimeMillis() - st) + " ms");

        st = System.currentTimeMillis();
        System.out.print("Reconstructing it by sequential builder... ");
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        exported.forAllRowKeys(builder::appendKey);
        final RowSet rowSet = builder.build();
        System.out.println("Builder done in " + (System.currentTimeMillis() - st) + " ms");

        return rowSet;
    }

    private static RowSet generateRandomRowSet() {
        long st = System.currentTimeMillis();
        System.out.print("Generating pseudorandom RowSet... ");
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (int i = 0; i < containers; ++i) {
            final long base = RspBitmap.BLOCK_SIZE * i;
            if (rand.nextDouble() < probRunContainer) {
                for (int run = 0; run < runsPerRunContainer; ++run) {
                    final long start = base + (runLen + 2) * run;
                    builder.appendRange(start, start + runLen - 1);
                }
            } else { // full block span
                builder.appendRange(base, base + RspBitmap.BLOCK_LAST);
            }
        }
        final RowSet rowSet = builder.build();
        System.out.println("RowSet generation done in " + (System.currentTimeMillis() - st) + " ms");
        return rowSet;
    }

    @Benchmark
    public void b00_get(final Blackhole bh) {
        final long ops = millionOps * 1000 * 1000;
        long acc = 0;
        for (int op = 0; op < ops; ++op) {
            acc += rowSet.get(op);
        }
        bh.consume(acc);
    }

    @Benchmark
    public void b01_find(final Blackhole bh) {
        long accum = 0;
        for (long v : opsValues) {
            accum += rowSet.find(v);
        }
        bh.consume(accum);
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(RowSetGetFindBench.class);
    }
}
