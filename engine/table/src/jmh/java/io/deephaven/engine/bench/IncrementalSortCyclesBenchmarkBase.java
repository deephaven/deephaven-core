package io.deephaven.engine.bench;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.IncrementalReleaseFilter;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.junit4.EngineCleanup;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@Fork(value = 2, jvmArgs = {"-Xms16G", "-Xmx16G"})
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
public abstract class IncrementalSortCyclesBenchmarkBase {
    static {
        System.setProperty("Configuration.rootFile", "dh-tests.prop");
        System.setProperty("workspace", "build/workspace");
    }

    private EngineCleanup engine;
    private UpdateGraphProcessor ugp;
    private IncrementalReleaseFilter filter;
    private Table out;
    private BlackholeListener listener;
    private int numCycles;

    public void init(long initialSize, long cycleSize, int numCycles, String indexToValueFunction, boolean reverse,
            Blackhole blackhole)
            throws Exception {
        engine = new EngineCleanup();
        engine.setUp();
        ugp = UpdateGraphProcessor.DEFAULT;
        ugp.startCycleForUnitTests();
        try {
            this.numCycles = numCycles;
            filter = new IncrementalReleaseFilter(initialSize, cycleSize);
            out = TableTools.emptyTable(initialSize + cycleSize * numCycles)
                    .select(String.format("Value=%s(ii)", indexToValueFunction));
            if (reverse) {
                out = out.reverse();
            }
            out = out.where(filter).sort("Value");
            filter.start();
            listener = new BlackholeListener(blackhole);
            out.listenForUpdates(listener);
        } finally {
            ugp.completeCycleForUnitTests();
        }
    }

    @TearDown(Level.Invocation)
    public void teardown() throws Exception {
        filter = null;
        out.removeUpdateListener(listener);
        listener = null;
        out.close();
        out = null;
        ugp = null;
        engine.tearDown();
        engine = null;
    }

    public void runCycles() throws Throwable {
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
