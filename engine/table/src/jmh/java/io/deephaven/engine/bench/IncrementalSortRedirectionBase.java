package io.deephaven.engine.bench;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.IncrementalReleaseFilter;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.junit4.EngineCleanup;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This is a benchmark base that was constructed based on the implementation / interaction details between
 * {@link io.deephaven.util.datastructures.hash.HashMapBase},
 * {@link io.deephaven.engine.table.impl.util.RowRedirection},
 * {@link io.deephaven.engine.table.impl.sources.UnionRedirection}, and sort.
 *
 * <p>
 * Classes that extend this may want to experiment with the system property "UnionRedirection.allocationUnit" and
 * implementation of {@link io.deephaven.util.datastructures.hash.HashMapBase}.
 *
 * <p>
 * See <a href="https://github.com/deephaven/deephaven-core/issues/2784">#2784</a>
 */
@Fork(value = 2, jvmArgs = {"-Xms16G", "-Xmx16G"})
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
public abstract class IncrementalSortRedirectionBase {
    static {
        System.setProperty("Configuration.rootFile", "dh-tests.prop");
        System.setProperty("workspace", "build/workspace");
    }

    private static final int REMAINING_ROWS = 1000000;

    private EngineCleanup engine;
    private UpdateGraphProcessor ugp;
    private IncrementalReleaseFilter filter;
    private Table ms;
    private int numCycles;
    private BlackholeListener listener;

    @Setup(Level.Invocation)
    public void setup(Blackhole blackhole) throws Exception {
        engine = new EngineCleanup();
        engine.setUp();
        ugp = UpdateGraphProcessor.DEFAULT;

        final int componentSize = 2000000;
        final int numBuckets = 2000;
        final int numParts = 10;
        final int remainingRows = REMAINING_ROWS;
        final int tableSize = numParts * componentSize;
        final int initialSize = tableSize - remainingRows;
        final int cycleIncrement = 20000;
        numCycles = remainingRows / cycleIncrement;

        // create the initial table
        ugp.startCycleForUnitTests();
        ms = create(componentSize, numBuckets, numParts, initialSize, cycleIncrement);
        listener = new BlackholeListener(blackhole);
        ms.listenForUpdates(listener);
        ugp.completeCycleForUnitTests();

    }

    private Table create(int componentSize, int numBuckets, int numParts, int initialSize, int cycleIncrement) {
        final Table base = TableTools.emptyTable(componentSize).update("Bucket = ii % " + numBuckets, "Time = ii");
        final List<Table> parts = new ArrayList<>();
        for (int i = 0; i < numParts; ++i) {
            parts.add(base.update("Tab=" + i));
        }
        final Table m = TableTools.merge(parts);
        filter = new IncrementalReleaseFilter(initialSize, cycleIncrement);
        final Table mf = m.where(filter);
        filter.start();
        return mf.sort("Tab", "Bucket", "Time");
    }

    @TearDown(Level.Invocation)
    public void teardown() throws Exception {
        ms.removeUpdateListener(listener);
        listener = null;
        ms.close();
        ms = null;
        ugp = null;
        engine.tearDown();
        engine = null;
    }

    @Benchmark
    @OperationsPerInvocation(REMAINING_ROWS)
    public void numRows() throws Throwable {
        for (int i = 0; i < numCycles; ++i) {
            ugp.startCycleForUnitTests();
            try {
                filter.run();
            } finally {
                ugp.completeCycleForUnitTests();
            }
            if (listener.e != null) {
                throw listener.e;
            }
        }
    }
}
