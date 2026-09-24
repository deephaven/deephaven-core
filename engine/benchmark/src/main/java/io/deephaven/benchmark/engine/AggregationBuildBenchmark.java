//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.util.SafeCloseable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures the initial build of a keyed {@code sumBy} over a refreshing table, with and without reclaiming the states
 * of removed keys ({@code reclaimStates}). Only refreshing inputs can use the reclaiming state manager, so the input is
 * refreshing even though no update is ever applied. The keys are uniformly random over {@code keyCount} distinct values
 * in row order.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
@Fork(1)
public class AggregationBuildBenchmark {

    @Param({"false", "true"})
    private boolean reclaimStates;

    @Param({"10000000"})
    private int tableSize;

    @Param({"100", "100000", "5000000"})
    private int keyCount;

    @Param({"long", "String"})
    private String keyType;

    private ControlledUpdateGraph updateGraph;
    private SafeCloseable executionContext;
    private QueryTable source;
    private LivenessScope resultScope;

    @Setup(Level.Trial)
    public void setupEnv() {
        executionContext = TestExecutionContext.createForUnitTests().open();
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);
        AggregationStateBenchSupport.setReclaimStates(reclaimStates);

        final Random random = new Random(0);
        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
        if (keyType.equals("long")) {
            final long[] keys = new long[tableSize];
            for (int ii = 0; ii < tableSize; ++ii) {
                keys[ii] = random.nextInt(keyCount);
            }
            columns.put("Key", ArrayBackedColumnSource.getMemoryColumnSource(keys));
        } else if (keyType.equals("String")) {
            final String[] pool = new String[keyCount];
            for (int ii = 0; ii < keyCount; ++ii) {
                pool[ii] = "Key" + ii;
            }
            final String[] keys = new String[tableSize];
            for (int ii = 0; ii < tableSize; ++ii) {
                keys[ii] = pool[random.nextInt(keyCount)];
            }
            columns.put("Key", ArrayBackedColumnSource.getMemoryColumnSource(keys, String.class, null));
        } else {
            throw new IllegalArgumentException("Unknown keyType " + keyType);
        }
        final long[] values = new long[tableSize];
        for (int ii = 0; ii < tableSize; ++ii) {
            values[ii] = random.nextInt(1000);
        }
        columns.put("Value", ArrayBackedColumnSource.getMemoryColumnSource(values));

        source = new QueryTable(RowSetFactory.flat(tableSize).toTracking(), columns);
        source.setRefreshing(true);
    }

    @Setup(Level.Invocation)
    public void setupInvocation() {
        resultScope = new LivenessScope();
        LivenessScopeStack.push(resultScope);
    }

    @TearDown(Level.Invocation)
    public void tearDownInvocation() {
        LivenessScopeStack.pop(resultScope);
        resultScope.release();
    }

    @TearDown(Level.Trial)
    public void tearDownEnv() {
        executionContext.close();
    }

    @Benchmark
    public Table build() {
        return updateGraph.sharedLock().computeLocked(() -> source.sumBy("Key"));
    }

    public static void main(String[] args) {
        BenchUtil.run(AggregationBuildBenchmark.class);
    }
}
