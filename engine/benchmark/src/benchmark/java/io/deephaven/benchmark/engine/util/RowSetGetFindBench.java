package io.deephaven.benchmark.engine.util;

import io.deephaven.benchmarking.BenchUtil;
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
    // Index size = containers * runsPerContainer*runLen.
    private static RowSet rowSet;
    private static RowSet opRowSet;

    private static final Random rand = new Random(randomSeed);

    // No bigger than ix.size, see above.
    @Param({"40"})
    private static long millionOps;

    @Setup(Level.Trial)
    public void setup() {
        final String indexFilename = System.getProperty("index.filename");
        if (indexFilename == null || indexFilename.equals("")) {
            rowSet = generateRandomIndex();
        } else {
            rowSet = loadRowSet(indexFilename);
        }
        // To get this output we need JVM flag `-DMetricsManager.enabled=true`
        System.out.println(MetricsManager.getCounters());
        final long ops = millionOps * 1000 * 1000;
        opRowSet = rowSet.subSetByPositionRange(0, ops);
    }

    private static RowSet loadRowSet(final String indexFilename) {
        final RowSet exported;
        long st = System.currentTimeMillis();
        System.out.print("Loading index " + indexFilename + "... ");
        try (final ObjectInputStream ois = new ObjectInputStream(new FileInputStream(indexFilename))) {
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
        final RowSet index = builder.build();
        System.out.println("Builder done in " + (System.currentTimeMillis() - st) + " ms");

        return index;
    }

    private static RowSet generateRandomIndex() {
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
        System.out.println("Index generation done in " + (System.currentTimeMillis() - st) + " ms");
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
        final MutableInt accum = new MutableInt(0);
        opRowSet.forAllRowKeys(l -> accum.add(rowSet.find(l)));
        bh.consume(accum.getValue());
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(RowSetGetFindBench.class);
    }
}
