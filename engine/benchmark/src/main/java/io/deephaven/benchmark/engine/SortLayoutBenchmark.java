//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.impl.RowSetUtils;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.table.impl.SortListener;
import io.deephaven.engine.table.impl.SortOperation;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.util.SafeCloseable;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Incremental sort cost for different initial layouts of a refreshing sort's result keys.
 * <p>
 * Each iteration sorts a fresh refreshing table of {@code initialSize} rows with the layout under test, then the
 * measured invocation adds {@code rowsPerCycle} rows in each of {@code cycles} update cycles. Layouts are {@code dense}
 * (one run) or {@code runSize/stride}: runs of runSize rows starting every stride keys.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 8)
@Fork(2)
public class SortLayoutBenchmark {
    private static final int BLOCK_SIZE = 1024;

    @Param({"1000000", "10000000"})
    private int initialSize;

    @Param({"1", "1000"})
    private int rowsPerCycle;

    @Param({"100"})
    private int cycles;

    /**
     * uniform and hotspot sort one long column V. feed and feedZipf sort by Sym then Ts: rows arrive in nearly
     * increasing Ts order (each Ts jittered by up to five rows' worth) with one of 10,000 symbols, chosen uniformly
     * (feed) or with Zipf weights 1/rank (feedZipf).
     */
    @Param({"uniform", "hotspot", "feed", "feedZipf"})
    private String workload;

    @Param({"dense", "64/128", "1024/16384"})
    private String layout;

    /**
     * unbounded (spread mode inserts gaps until its pass ends), sqrt (bounded, sqrt(size) window, eight lookahead
     * windows), or local (bounded, 128-key window, one lookahead window).
     */
    @Param({"unbounded", "local"})
    private String spread;

    /** binary, gallop (row set search), nextRow (next-row runs over the keys), or nextRowGallop (both). */
    @Param({"binary", "nextRowGallop"})
    private String search;

    private SafeCloseable executionContext;
    private ControlledUpdateGraph updateGraph;
    private static final int NUM_SYMBOLS = 10_000;
    private static final long TS_STEP = 1000;

    private final Map<String, ColumnSource<?>> sourceColumns = new LinkedHashMap<>();
    private String[] sortColumns;

    private LivenessScope scope;
    private TrackingWritableRowSet sourceRowSet;
    private QueryTable source;
    private Table sorted;
    private long nextRowKey;
    private long initialBlocks;

    @Setup(Level.Trial)
    public void setupTrial() {
        executionContext = TestExecutionContext.createForUnitTests().open();
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);

        final Random random = new Random(0xC0FFEE);
        sourceColumns.clear();
        if (workload.startsWith("feed")) {
            setupFeed(random);
        } else {
            setupValues(random);
        }

        RowSetUtils.GALLOP_SEARCH = search.equals("gallop") || search.equals("nextRowGallop");
        SortListener.PLACE_RUNS_BEFORE_NEXT_ROW = search.startsWith("nextRow");
        SortListener.REBALANCE_BOUNDED_SPREAD = !spread.equals("unbounded");
        SortListener.REBALANCE_SPREAD_WINDOW = spread.equals("local") ? 128 : 0;
        SortListener.REBALANCE_SPREAD_LOOKAHEAD_WINDOWS = spread.equals("local") ? 1 : 8;
        if (layout.equals("dense")) {
            SortOperation.INITIAL_LAYOUT_RUN_SIZE = 0;
            SortOperation.INITIAL_LAYOUT_STRIDE = 0;
        } else {
            final String[] parts = layout.split("/");
            SortOperation.INITIAL_LAYOUT_RUN_SIZE = Integer.parseInt(parts[0]);
            SortOperation.INITIAL_LAYOUT_STRIDE = Integer.parseInt(parts[1]);
        }
    }

    private void setupFeed(final Random random) {
        final int totalRows = initialSize + cycles * rowsPerCycle;
        final String[] symbolNames = new String[NUM_SYMBOLS];
        final double[] cumulativeWeight = new double[NUM_SYMBOLS];
        double total = 0;
        for (int ii = 0; ii < NUM_SYMBOLS; ++ii) {
            symbolNames[ii] = String.format("SYM%05d", ii);
            total += workload.equals("feedZipf") ? 1.0 / (ii + 1) : 1.0;
            cumulativeWeight[ii] = total;
        }
        // Zipf ranks are assigned to symbols in a scrambled order so the busiest symbols are not adjacent.
        final int[] symbolForRank = new int[NUM_SYMBOLS];
        for (int ii = 0; ii < NUM_SYMBOLS; ++ii) {
            symbolForRank[ii] = ii;
        }
        for (int ii = NUM_SYMBOLS - 1; ii > 0; --ii) {
            final int swap = random.nextInt(ii + 1);
            final int held = symbolForRank[ii];
            symbolForRank[ii] = symbolForRank[swap];
            symbolForRank[swap] = held;
        }
        final String[] symbols = new String[totalRows];
        final long[] timestamps = new long[totalRows];
        for (int ii = 0; ii < totalRows; ++ii) {
            int rank = Arrays.binarySearch(cumulativeWeight, random.nextDouble() * total);
            rank = rank >= 0 ? rank : Math.min(NUM_SYMBOLS - 1, -rank - 1);
            symbols[ii] = symbolNames[symbolForRank[rank]];
            timestamps[ii] = ii * TS_STEP + random.nextInt((int) (5 * TS_STEP));
        }
        sourceColumns.put("Sym", ArrayBackedColumnSource.getMemoryColumnSource(symbols, String.class, null));
        sourceColumns.put("Ts", ArrayBackedColumnSource.getMemoryColumnSource(timestamps));
        sortColumns = new String[] {"Sym", "Ts"};
    }

    private void setupValues(final Random random) {
        final long[] values = new long[initialSize + cycles * rowsPerCycle];
        final long range = 1L << 62;
        for (int ii = 0; ii < initialSize; ++ii) {
            values[ii] = (random.nextLong() >>> 2) % range;
        }
        final long hotspotLow = range / 2;
        final long hotspotWidth = range >> 30;
        for (int ii = initialSize; ii < values.length; ++ii) {
            switch (workload) {
                case "uniform":
                    values[ii] = (random.nextLong() >>> 2) % range;
                    break;
                case "hotspot":
                    values[ii] = hotspotLow + (random.nextLong() >>> 2) % hotspotWidth;
                    break;
                case "append":
                    values[ii] = range + ii;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown workload " + workload);
            }
        }
        sourceColumns.put("V", ArrayBackedColumnSource.getMemoryColumnSource(values));
        sortColumns = new String[] {"V"};
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
        scope = new LivenessScope();
        LivenessScopeStack.push(scope);
        try {
            sourceRowSet = RowSetFactory.flat(initialSize).toTracking();
            source = new QueryTable(sourceRowSet, new LinkedHashMap<>(sourceColumns));
            source.setRefreshing(true);
            sorted = source.sort(sortColumns);
        } finally {
            LivenessScopeStack.pop(scope);
        }
        nextRowKey = initialSize;
        initialBlocks = countBlocks(sorted.getRowSet());
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
        final RowSet result = sorted.getRowSet();
        if (sorted.isFailed() || result.size() != sourceRowSet.size()) {
            throw new IllegalStateException("sort failed or lost rows: failed=" + sorted.isFailed() + " size="
                    + result.size() + " expected=" + sourceRowSet.size());
        }
        // The last sort column is a long; a feed sorts by Sym first.
        final String lastColumn = sortColumns[sortColumns.length - 1];
        final long[] sortedValues = ColumnVectors.ofLong(sorted, lastColumn).toArray();
        final String[] sortedSymbols = sortColumns.length > 1
                ? ColumnVectors.ofObject(sorted, "Sym", String.class).toArray()
                : null;
        long sortedSum = 0;
        for (int ii = 0; ii < sortedValues.length; ++ii) {
            if (ii > 0) {
                final int symbolOrder = sortedSymbols == null ? 0 : sortedSymbols[ii - 1].compareTo(sortedSymbols[ii]);
                if (symbolOrder > 0 || (symbolOrder == 0 && sortedValues[ii - 1] > sortedValues[ii])) {
                    throw new IllegalStateException("sort out of order at position " + ii);
                }
            }
            sortedSum += sortedValues[ii] + (sortedSymbols == null ? 0 : sortedSymbols[ii].hashCode());
        }
        long sourceSum = 0;
        final long[] sourceValues = ColumnVectors.ofLong(source, lastColumn).toArray();
        final String[] sourceSymbols = sortColumns.length > 1
                ? ColumnVectors.ofObject(source, "Sym", String.class).toArray()
                : null;
        for (int ii = 0; ii < sourceValues.length; ++ii) {
            sourceSum += sourceValues[ii] + (sourceSymbols == null ? 0 : sourceSymbols[ii].hashCode());
        }
        if (sortedSum != sourceSum) {
            throw new IllegalStateException("sort values differ from the source's");
        }
        System.out.printf("%nLAYOUT %s spread=%s search=%s size=%d workload=%s rowsPerCycle=%d initialBlocks=%d"
                + " finalBlocks=%d finalSpan=%d%n", layout, spread, search, initialSize, workload, rowsPerCycle,
                initialBlocks, countBlocks(result), result.lastRowKey() - result.firstRowKey() + 1);
        scope.release();
        sorted = null;
        source = null;
        sourceRowSet = null;
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        executionContext.close();
    }

    @Benchmark
    public void addRows(final Blackhole blackhole) {
        for (int cycle = 0; cycle < cycles; ++cycle) {
            updateGraph.runWithinUnitTestCycle(() -> {
                final RowSet added = RowSetFactory.fromRange(nextRowKey, nextRowKey + rowsPerCycle - 1);
                nextRowKey += rowsPerCycle;
                sourceRowSet.insert(added);
                source.notifyListeners(added, RowSetFactory.empty(), RowSetFactory.empty());
            });
        }
        blackhole.consume(sorted.size());
    }

    private static long countBlocks(final RowSet rowSet) {
        final long[] blocks = new long[1];
        final long[] lastBlock = {-1};
        rowSet.forAllRowKeyRanges((first, last) -> {
            final long firstBlock = first / BLOCK_SIZE;
            final long endBlock = last / BLOCK_SIZE;
            blocks[0] += endBlock - firstBlock + 1;
            if (firstBlock == lastBlock[0]) {
                --blocks[0];
            }
            lastBlock[0] = endBlock;
        });
        return blocks[0];
    }
}
