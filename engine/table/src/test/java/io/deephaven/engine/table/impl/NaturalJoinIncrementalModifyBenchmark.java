//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.experimental.categories.Category;

import java.util.LinkedHashMap;
import java.util.Random;

/**
 * A throughput/latency micro-benchmark (written as a unit test so it can be driven by the controlled update graph) that
 * measures the wall-clock cost of a single {@code naturalJoin} update cycle when a large left table churns its join
 * keys.
 *
 * <p>
 * The left table is built so that the natural join's per-key left-row-set state is forced into the {@code RspBitmap}
 * ("RSP") representation: keys are assigned round-robin ({@code Key = rowPosition % numKeys}), so each of the
 * {@code numKeys} keys owns a strided set of row keys (every {@code numKeys}-th row). With a moderately large table
 * that is thousands of singleton ranges per key, which the row set implementation stores as an {@code RspBitmap} rather
 * than a {@code SingleRange} or {@code SortedRanges}. The join therefore exercises RSP insert/remove as keys move
 * between buckets.
 * </p>
 *
 * <p>
 * Each update cycle emits a modify-only {@link TableUpdate} (no adds/removes/shifts) in which:
 * <ul>
 * <li>{@code keyChangeFraction} (default 10%) of the rows are given a <em>new</em> join key — these rows must migrate
 * between the join's per-key row sets. The other 90% of keys are unchanged.</li>
 * <li>{@code valueModifyFraction} (default 40%) of the rows have a non-key {@code Value} column modified (the
 * key-changing rows are a subset of these, so at least 40% of the total rows are modified).</li>
 * </ul>
 * The modified column set is {@code {Key, Value}} every cycle, so the join must consider key migration for the modified
 * rows.
 * </p>
 *
 * <p>
 * All parameters are read from system properties so they can be overridden from the Gradle command line without editing
 * the source, e.g.:
 *
 * <pre>
 * ./gradlew :engine-table:testOutOfBand \
 *     --tests '*NaturalJoinIncrementalModifyBenchmark*' \
 *     -PforceTest=true -PshowStandardStreams=true \
 *     -Dnjbench.size=5000000 -Dnjbench.cycles=30
 * </pre>
 *
 * <p>
 * {@code -PshowStandardStreams=true} is required to see the per-cycle timing printed to stdout;
 * {@code -PforceTest=true} re-runs the benchmark even when nothing changed. The {@code -Dnjbench.*} properties are
 * forwarded to the test JVM by this module's {@code build.gradle}. Larger sizes may require more heap, e.g.
 * {@code -PmaxHeapSize=12g}.
 * </p>
 */
@Category(OutOfBandTest.class)
public class NaturalJoinIncrementalModifyBenchmark extends RefreshingTableTestCase {

    /** Total number of left rows. Moderately large by default so the per-key state row sets are RSP-backed. */
    private static final int SIZE = Integer.getInteger("njbench.size", 1_000_000);
    /** Number of distinct join keys (kept small so each key owns many, widely-strided rows). */
    private static final int NUM_KEYS = Integer.getInteger("njbench.numKeys", 10);
    /** Fraction of rows whose join key changes each cycle. */
    private static final double KEY_CHANGE_FRACTION = fractionProperty("njbench.keyChangeFraction", 0.10);
    /** Fraction of rows whose non-key {@code Value} column is modified each cycle. */
    private static final double VALUE_MODIFY_FRACTION = fractionProperty("njbench.valueModifyFraction", 0.40);
    /** Number of measured update cycles. */
    private static final int CYCLES = Integer.getInteger("njbench.cycles", 20);
    /** Number of warmup cycles run before measurement begins (excluded from the reported statistics). */
    private static final int WARMUP_CYCLES = Integer.getInteger("njbench.warmupCycles", 5);
    /** Seed for the RNG that selects which rows are modified each cycle. */
    private static final long SEED = Long.getLong("njbench.seed", 0L);
    /** If set, a heap dump (.hprof) is written to this path after {@link #HEAP_DUMP_AFTER_CYCLES} cycles. */
    private static final String HEAP_DUMP_PATH = System.getProperty("njbench.heapDumpPath");
    /** Number of update cycles (warmup + measured) to run before writing the heap dump. */
    private static final int HEAP_DUMP_AFTER_CYCLES = Integer.getInteger("njbench.heapDumpAfterCycles", 2);

    private static double fractionProperty(final String name, final double defaultValue) {
        final String value = System.getProperty(name);
        return value == null ? defaultValue : Double.parseDouble(value);
    }

    private WritableColumnSource<Integer> keySource;
    private WritableColumnSource<Long> valueSource;
    private QueryTable leftTable;
    private ModifiedColumnSet modifiedColumnSet;

    private final Random random = new Random(SEED);
    /** A persistent permutation of [0, size); a partial Fisher-Yates shuffle picks a fresh random subset each cycle. */
    private int[] permutation;

    public void testNaturalJoinIncrementalModify() {
        final int numKeys = NUM_KEYS;
        final int size = SIZE;

        // Left table: Key = position % numKeys (strided per key -> RSP state), Value = an arbitrary mutable long.
        keySource = ArrayBackedColumnSource.getMemoryColumnSource(size, Integer.class);
        valueSource = ArrayBackedColumnSource.getMemoryColumnSource(size, Long.class);
        permutation = new int[size];
        for (int row = 0; row < size; ++row) {
            keySource.set(row, row % numKeys);
            valueSource.set(row, (long) row);
            permutation[row] = row;
        }

        final LinkedHashMap<String, ColumnSource<?>> leftColumns = new LinkedHashMap<>();
        leftColumns.put("Key", keySource);
        leftColumns.put("Value", valueSource);
        final TrackingWritableRowSet leftRowSet = RowSetFactory.flat(size).toTracking();
        leftTable = new QueryTable(leftRowSet, leftColumns);
        leftTable.setRefreshing(true);
        keySource.startTrackingPrevValues();
        valueSource.startTrackingPrevValues();
        modifiedColumnSet = leftTable.newModifiedColumnSet("Key", "Value");

        // Right table: one unique row per key so the natural join is valid. It is marked refreshing (even though it
        // never ticks) so that naturalJoin selects the both-incremental state manager
        // (IncrementalNaturalJoinStateManagerTypedBase), which maintains a per-key left WritableRowSet -- the
        // RSP-backed
        // "left hand side state" this benchmark exercises. With a static right table, naturalJoin would instead keep
        // only
        // a left->right row redirection and no per-key left row sets.
        final WritableColumnSource<Integer> rightKeySource =
                ArrayBackedColumnSource.getMemoryColumnSource(numKeys, Integer.class);
        final WritableColumnSource<Long> rightSentinelSource =
                ArrayBackedColumnSource.getMemoryColumnSource(numKeys, Long.class);
        for (int key = 0; key < numKeys; ++key) {
            rightKeySource.set(key, key);
            rightSentinelSource.set(key, key * 1_000_000L);
        }
        final LinkedHashMap<String, ColumnSource<?>> rightColumns = new LinkedHashMap<>();
        rightColumns.put("Key", rightKeySource);
        rightColumns.put("RightSentinel", rightSentinelSource);
        final QueryTable rightTable = new QueryTable(RowSetFactory.flat(numKeys).toTracking(), rightColumns);
        rightTable.setRefreshing(true);
        rightKeySource.startTrackingPrevValues();
        rightSentinelSource.startTrackingPrevValues();

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final Table result = updateGraph.sharedLock().computeLocked(
                () -> leftTable.naturalJoin(rightTable, "Key", "RightSentinel"));

        // A listener keeps the result live, verifies propagation, and blocks dead-code elimination of the join output.
        final long[] lastModified = new long[1];
        final InstrumentedTableUpdateListenerAdapter listener =
                new InstrumentedTableUpdateListenerAdapter("njbench", result, true) {
                    @Override
                    public void onUpdate(final TableUpdate upstream) {
                        lastModified[0] = upstream.modified().size();
                    }
                };
        result.addUpdateListener(listener);

        final long valCount = Math.max(1L, (long) (size * VALUE_MODIFY_FRACTION));
        final long keyCount = Math.min(valCount, Math.max(1L, (long) (size * KEY_CHANGE_FRACTION)));

        System.out.printf(
                "naturalJoin incremental modify benchmark: size=%,d numKeys=%d valueModified=%,d (%.0f%%) keyChanged=%,d (%.0f%%) warmup=%d cycles=%d%n",
                size, numKeys, valCount, 100.0 * valCount / size, keyCount, 100.0 * keyCount / size, WARMUP_CYCLES,
                CYCLES);

        final long[] measured = new long[CYCLES];
        final int totalCycles = WARMUP_CYCLES + CYCLES;
        for (int cycle = 0; cycle < totalCycles; ++cycle) {
            // Decompose the update cycle so we can time only the join propagation, not the data mutation. The mutation
            // and notifyListeners() merely enqueue a source notification; the join's listener actually runs during
            // flushAllNormalNotificationsForUnitTests(), which is the region we measure.
            final long elapsedNanos;
            updateGraph.startCycleForUnitTests();
            try {
                applyModifications(size, numKeys, valCount, keyCount);
                final long startNanos = System.nanoTime();
                updateGraph.flushAllNormalNotificationsForUnitTests();
                elapsedNanos = System.nanoTime() - startNanos;
            } finally {
                updateGraph.completeCycleForUnitTests();
            }

            if (cycle >= WARMUP_CYCLES) {
                measured[cycle - WARMUP_CYCLES] = elapsedNanos;
                System.out.printf("cycle %3d: %,15d ns (%8.3f ms)%n",
                        cycle - WARMUP_CYCLES, elapsedNanos, elapsedNanos / 1_000_000.0);
            } else {
                System.out.printf("warmup %3d: %,15d ns (%8.3f ms)%n",
                        cycle, elapsedNanos, elapsedNanos / 1_000_000.0);
            }

            // While the join result -- and hence its internal per-key left-state row sets -- are live, print a live
            // class histogram of the row set implementation types and (optionally) dump the heap, so the RowSet types
            // in use (e.g. RspBitmap) can be validated.
            if (HEAP_DUMP_PATH != null && cycle + 1 == HEAP_DUMP_AFTER_CYCLES) {
                printRowSetHistogram();
                dumpHeap(HEAP_DUMP_PATH);
            }
        }

        result.removeUpdateListener(listener);
        printStatistics(measured);
        // Touch the listener output so the JIT cannot elide the join propagation.
        assertTrue(lastModified[0] > 0);
    }

    /**
     * Emit one modify-only update. A partial Fisher-Yates shuffle draws {@code valCount} distinct rows uniformly at
     * random (no pattern, fresh each cycle); every drawn row has its {@code Value} modified and the first
     * {@code keyCount} of them are additionally moved to a different, randomly chosen join key.
     */
    private void applyModifications(final int size, final int numKeys, final long valCount, final long keyCount) {
        final int valCountInt = (int) valCount;
        final RowSetBuilderRandom modifiedBuilder = RowSetFactory.builderRandom();
        for (int i = 0; i < valCountInt; ++i) {
            // Partial Fisher-Yates: permutation[i] becomes a uniformly random draw from the not-yet-drawn suffix.
            final int j = i + random.nextInt(size - i);
            final int tmp = permutation[i];
            permutation[i] = permutation[j];
            permutation[j] = tmp;

            final int row = permutation[i];
            if (i < keyCount) {
                keySource.set(row, differentKey(keySource.getInt(row), numKeys));
            }
            valueSource.set(row, valueSource.getLong(row) + 1);
            modifiedBuilder.addKey(row);
        }

        leftTable.notifyListeners(new TableUpdateImpl(
                RowSetFactory.empty(),
                RowSetFactory.empty(),
                modifiedBuilder.build(),
                RowSetShiftData.EMPTY,
                modifiedColumnSet));
    }

    /**
     * Print a live-object class histogram (via the {@code DiagnosticCommand} MBean's {@code gcClassHistogram}, which
     * forces a full GC first) filtered to the {@code io.deephaven.engine.rowset} package, so the concrete RowSet and
     * RSP container implementation types in use can be validated without a separate heap analyzer.
     */
    private static void printRowSetHistogram() {
        try {
            final javax.management.MBeanServer server = java.lang.management.ManagementFactory.getPlatformMBeanServer();
            final javax.management.ObjectName name =
                    new javax.management.ObjectName("com.sun.management:type=DiagnosticCommand");
            final String histogram = (String) server.invoke(name, "gcClassHistogram",
                    new Object[] {new String[0]}, new String[] {String[].class.getName()});
            System.out.println("--- live class histogram (io.deephaven.engine.rowset) ---");
            histogram.lines()
                    .filter(line -> line.contains("io.deephaven.engine.rowset"))
                    .forEach(System.out::println);
            System.out.println("--- end histogram ---");
        } catch (final Exception e) {
            throw new RuntimeException("Failed to collect class histogram", e);
        }
    }

    /** Write a live-object heap dump (forces a full GC first) to {@code path}; {@code path} must not already exist. */
    private static void dumpHeap(final String path) {
        try {
            final com.sun.management.HotSpotDiagnosticMXBean bean = java.lang.management.ManagementFactory
                    .getPlatformMXBean(com.sun.management.HotSpotDiagnosticMXBean.class);
            bean.dumpHeap(path, true);
            System.out.println("Wrote heap dump to " + path);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to write heap dump to " + path, e);
        }
    }

    /** Return a uniformly random key in [0, numKeys) that is not {@code currentKey}. */
    private int differentKey(final int currentKey, final int numKeys) {
        final int candidate = random.nextInt(numKeys - 1);
        return candidate < currentKey ? candidate : candidate + 1;
    }

    private static void printStatistics(final long[] measured) {
        final long[] sorted = measured.clone();
        java.util.Arrays.sort(sorted);
        long sum = 0;
        for (final long value : sorted) {
            sum += value;
        }
        final long min = sorted[0];
        final long max = sorted[sorted.length - 1];
        final long median = sorted[sorted.length / 2];
        final double mean = (double) sum / sorted.length;
        System.out.printf(
                "summary over %d cycles: min=%.3f ms  median=%.3f ms  mean=%.3f ms  max=%.3f ms%n",
                sorted.length, min / 1_000_000.0, median / 1_000_000.0, mean / 1_000_000.0, max / 1_000_000.0);
    }
}
