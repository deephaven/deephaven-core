//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.generic.region;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.impl.ShiftedRowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.SimpleSourceTable;
import io.deephaven.engine.table.impl.SourcePartitionedTable;
import io.deephaven.engine.table.impl.TableUpdateMode;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableIntArraySource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.engine.table.iterators.ChunkedColumnIterator;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.testutil.locations.TableBackedTableLocationProvider;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.datastructures.LongAbortableConsumer;
import io.deephaven.util.datastructures.LongRangeAbortableConsumer;
import jdk.jfr.Recording;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Micro-benchmark comparing column-scan cost of a coalesced {@link SimpleSourceTable} (the
 * {@link io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource regioned} path) against a merged
 * {@link SourcePartitionedTable} (the union-source path). Both tables present the same logical data:
 * {@value #DEFAULT_NUM_LOCATIONS} locations of {@value #DEFAULT_ROWS_PER_LOCATION} rows of a single random {@code int}
 * column, held in {@link ImmutableIntArraySource}s and served through a {@link TableBackedTableLocationProvider} (no
 * real I/O, so we measure engine/computation cost rather than disk).
 *
 * <p>
 * Each (table, row-set shape) combination is scanned in three <em>modes</em> to attribute where scan time goes:
 * <ul>
 * <li>{@code iterate} — only iterate the {@link RowSequence} in {@value ChunkedColumnIterator#DEFAULT_CHUNK_SIZE}-row
 * windows, never touching a column (isolates row-set iteration cost).</li>
 * <li>{@code getChunk} — what {@link io.deephaven.engine.table.impl.ForceReadUtility} does:
 * {@link ChunkSource#getChunk}, which returns a zero-copy view for contiguous windows and falls back to a fill for
 * fragmented ones (iteration + descent-to-location + view/fill).</li>
 * <li>{@code fillChunk} — {@link ChunkSource#fillChunk} into a reused destination, which always copies (iteration +
 * descent-to-location + copy).</li>
 * </ul>
 * The deltas ({@code getChunk - iterate}, {@code fillChunk - getChunk}) approximate the descent and fill contributions.
 *
 * <p>
 * Row-set shapes, all defined by row <em>position</em> so both tables scan the identical logical rows:
 * <ul>
 * <li>{@code full} — the entire row set (contiguous windows → getChunk returns views).</li>
 * <li>{@code everyOther} — every 2nd row (single keys → {@code fillChunkByKeys} per-element gather).</li>
 * <li>{@code sparse} — every {@value #DEFAULT_SPARSE_STRIDE}-th row (single keys, mostly fixed overhead).</li>
 * <li>{@code strideRanges} — runs of {@value #DEFAULT_RANGE_RUN} consecutive rows every {@value #DEFAULT_RANGE_PERIOD}
 * positions; the period is not a multiple of {@value ChunkedColumnIterator#DEFAULT_CHUNK_SIZE}, so read windows
 * straddle gaps and exercise {@code fillChunkByRanges} bulk copies rather than per-element gathers.</li>
 * </ul>
 *
 * <p>
 * This is an {@link OutOfBandTest} and is not wired into {@code check}; run it explicitly. Note the heap footprint:
 * {@code locations * rowsPerLocation * 4} bytes of {@code int} data stays resident (~4 GiB at the defaults), so run
 * with a large {@code -Xmx} (e.g. gradle {@code -PbenchHeap=12g}). All sizing is overridable via {@code -Dbench.*}
 * system properties (forwarded to the test JVM by this module's build). Pass {@code -Dbench.jfr=<path.jfr>} to capture
 * a Java Flight Recorder execution-sample profile scoped to just the scan section.
 */
@Category(OutOfBandTest.class)
public class SourceTableScanBenchmarkTest {

    private static final int DEFAULT_NUM_LOCATIONS = 1000;
    private static final int DEFAULT_ROWS_PER_LOCATION = 1_000_000;
    private static final int DEFAULT_SPARSE_STRIDE = 1000;
    private static final int DEFAULT_RANGE_RUN = 1000;
    private static final int DEFAULT_RANGE_PERIOD = 2000;

    private static final int NUM_LOCATIONS = Integer.getInteger("bench.locations", DEFAULT_NUM_LOCATIONS);
    private static final int ROWS_PER_LOCATION = Integer.getInteger("bench.rowsPerLocation", DEFAULT_ROWS_PER_LOCATION);
    private static final long SEED = Long.getLong("bench.seed", 20260701L);
    private static final int EVERY_OTHER_STRIDE = 2;
    private static final int SPARSE_STRIDE = Integer.getInteger("bench.sparseStride", DEFAULT_SPARSE_STRIDE);
    private static final int RANGE_RUN = Integer.getInteger("bench.rangeRun", DEFAULT_RANGE_RUN);
    private static final int RANGE_PERIOD = Integer.getInteger("bench.rangePeriod", DEFAULT_RANGE_PERIOD);
    private static final int WARMUP_ITERS = Integer.getInteger("bench.warmup", 2);
    private static final int TIMED_ITERS = Integer.getInteger("bench.iters", 3);
    private static final int READ_SIZE = ChunkedColumnIterator.DEFAULT_CHUNK_SIZE;
    private static final String COLUMN_NAME = "IntCol";

    private static final long TOTAL_ROWS = (long) NUM_LOCATIONS * ROWS_PER_LOCATION;

    // Single int column, no partitioning column (SimpleSourceTable forbids partitioning columns).
    private static final TableDefinition CONSTITUENT_DEFINITION =
            TableDefinition.of(ColumnDefinition.ofInt(COLUMN_NAME));

    /** Consumes chunk/row-sequence results so the JIT cannot dead-code-eliminate the scan work. */
    @SuppressWarnings("unused")
    private static volatile long blackhole;

    private enum Mode {
        ITERATE, GET_CHUNK, FILL_CHUNK
    }

    @Rule
    public final EngineCleanup cleanup = new EngineCleanup();

    @Test
    public void benchmarkColumnScans() throws IOException {
        System.out.println("=== Scan benchmark config ===");
        System.out.printf("  locations=%d, rows/location=%d, total rows=%d%n",
                NUM_LOCATIONS, ROWS_PER_LOCATION, TOTAL_ROWS);
        System.out.printf("  everyOther stride=%d, sparse stride=%d, ranges=%d-in-%d, readSize=%d%n",
                EVERY_OTHER_STRIDE, SPARSE_STRIDE, RANGE_RUN, RANGE_PERIOD, READ_SIZE);
        System.out.printf("  warmup iters=%d, timed iters=%d%n%n", WARMUP_ITERS, TIMED_ITERS);

        final UpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph();

        // Backing in-memory tables, shared by both providers so the int data is allocated only once.
        long t0 = System.nanoTime();
        final Table[] backing = buildBacking();
        System.out.printf("Built %d backing tables (%.3f s)%n", NUM_LOCATIONS, seconds(t0));

        // SourceTable side (regioned).
        t0 = System.nanoTime();
        final Table sourceTable = new SimpleSourceTable(
                CONSTITUENT_DEFINITION,
                "BenchSourceTable",
                RegionedTableComponentFactoryImpl.INSTANCE,
                makeProvider(updateGraph, backing),
                null /* static */).coalesce();
        System.out.printf("Coalesced SimpleSourceTable: size=%d (%.3f s)%n", sourceTable.size(), seconds(t0));

        // Merged SourcePartitionedTable side (union sources).
        t0 = System.nanoTime();
        final SourcePartitionedTable spt = new SourcePartitionedTable(
                CONSTITUENT_DEFINITION,
                null /* constituentTransformer */,
                makeProvider(updateGraph, backing),
                false /* subscribeToTableLocationProvider */,
                false /* subscribeToTableLocations */,
                null /* locationKeyMatcher */);
        final Table merged = spt.merge().coalesce();
        System.out.printf("Merged SourcePartitionedTable: size=%d (%.3f s)%n%n", merged.size(), seconds(t0));

        assertThat(sourceTable.size()).isEqualTo(TOTAL_ROWS);
        assertThat(merged.size()).isEqualTo(TOTAL_ROWS);

        // Position-based row-set templates, mapped to each table's own keys via subSetForPositions.
        t0 = System.nanoTime();
        final WritableRowSet everyOtherPositions = stridedKeys(EVERY_OTHER_STRIDE);
        final WritableRowSet sparsePositions = stridedKeys(SPARSE_STRIDE);
        final WritableRowSet rangePositions = stridedRanges(RANGE_RUN, RANGE_PERIOD);
        System.out.printf("Built position templates: everyOther=%d, sparse=%d, strideRanges=%d (%.3f s)%n%n",
                everyOtherPositions.size(), sparsePositions.size(), rangePositions.size(), seconds(t0));

        final Map<String, Function<Table, RowSet>> shapes = new LinkedHashMap<>();
        shapes.put("full", Table::getRowSet);
        shapes.put("everyOther", t -> t.getRowSet().subSetForPositions(everyOtherPositions));
        shapes.put("sparse", t -> t.getRowSet().subSetForPositions(sparsePositions));
        shapes.put("strideRanges", t -> t.getRowSet().subSetForPositions(rangePositions));

        final Map<String, Table> tables = new LinkedHashMap<>();
        tables.put("SourceTable", sourceTable);
        tables.put("MergedSourcePartTable", merged);

        // Optionally restrict to a subset of shapes/tables (CSV), e.g. to capture an isolated profile.
        retainByProperty(shapes, "bench.shapes");
        retainByProperty(tables, "bench.tables");

        // Precompute every row set BEFORE profiling so that row-set construction (RSP/bitmap building) does not
        // pollute the scan profile; the JFR window then covers only iteration/descent/fill work.
        final List<Case> cases = new ArrayList<>();
        for (final Map.Entry<String, Function<Table, RowSet>> shape : shapes.entrySet()) {
            for (final Map.Entry<String, Table> table : tables.entrySet()) {
                final Table value = table.getValue();
                cases.add(new Case(table.getKey(), shape.getKey(), value.getColumnSource(COLUMN_NAME),
                        value, shape.getValue().apply(value)));
            }
        }

        final Recording recording = startRecording();
        final List<Result> results = new ArrayList<>();
        System.out.println("=== Scans (ns/row) ===");
        try {
            for (final Case c : cases) {
                for (final Mode mode : Mode.values()) {
                    results.add(timeScan(c.tableLabel, c.shapeLabel, mode, c.columnSource, c.rowSet));
                }
            }
        } finally {
            stopRecording(recording);
            for (final Case c : cases) {
                // The "full" shape returns the table's own (shared) row set; only close derived subsets.
                if (c.rowSet != c.table.getRowSet()) {
                    c.rowSet.close();
                }
            }
        }

        printModeSummary(results);
        printAttribution(results);
    }

    private static final class Case {
        private final String tableLabel;
        private final String shapeLabel;
        private final ColumnSource<?> columnSource;
        private final Table table;
        private final RowSet rowSet;

        private Case(final String tableLabel, final String shapeLabel, final ColumnSource<?> columnSource,
                final Table table, final RowSet rowSet) {
            this.tableLabel = tableLabel;
            this.shapeLabel = shapeLabel;
            this.columnSource = columnSource;
            this.table = table;
            this.rowSet = rowSet;
        }
    }

    /**
     * Isolated micro-benchmark of {@link ShiftedRowSequence} enumeration overhead. Uses only row sets over a synthetic
     * key space (no column data), so it runs quickly under the default heap. For each shape it compares, at equal
     * consumer shape, raw enumeration against the {@link ShiftedRowSequence}-wrapped enumeration, plus a
     * chunk-materialize-then-vector-shift alternative for the by-key path. Tune with {@code -Dbench.enumRows} and
     * {@code -Dbench.shift}.
     */
    @Test
    public void benchmarkShiftedRowSequenceEnumeration() {
        final long enumRows = Long.getLong("bench.enumRows", 200_000_000L);
        final long shift = Long.getLong("bench.shift", 1L << 40);
        System.out.println("=== ShiftedRowSequence enumeration micro-benchmark ===");
        System.out.printf("  enumRows=%d, shift=%d, readSize=%d, warmup=%d, iters=%d%n%n",
                enumRows, shift, READ_SIZE, WARMUP_ITERS, TIMED_ITERS);

        try (final WritableRowSet full = RowSetFactory.flat(enumRows);
                final WritableRowSet everyOther = stridedKeysOver(enumRows, EVERY_OTHER_STRIDE);
                final WritableRowSet strideRanges = stridedRangesOver(enumRows, RANGE_RUN, RANGE_PERIOD)) {
            final Map<String, RowSet> shapes = new LinkedHashMap<>();
            shapes.put("full", full);
            shapes.put("everyOther", everyOther);
            shapes.put("strideRanges", strideRanges);

            final LongSink sink = new LongSink();
            System.out.printf("%-14s %-24s %14s %10s%n", "shape", "variant", "rows", "ns/row");
            for (final Map.Entry<String, RowSet> shape : shapes.entrySet()) {
                final RowSet rs = shape.getValue();
                final RowSequence shifted = ShiftedRowSequence.wrap(rs, shift);
                final long rows = rs.size();
                timeEnum(shape.getKey(), "forEachRowKey/raw", rows, () -> {
                    sink.sum = 0;
                    rs.forEachRowKey(sink);
                    return sink.sum;
                });
                timeEnum(shape.getKey(), "forEachRowKey/shifted", rows, () -> {
                    sink.sum = 0;
                    shifted.forEachRowKey(sink);
                    return sink.sum;
                });
                timeEnum(shape.getKey(), "forEachRowKey/chunked", rows, () -> chunkedShiftKeys(rs, shift));
                timeEnum(shape.getKey(), "forEachRowKeyRange/raw", rows, () -> {
                    sink.sum = 0;
                    rs.forEachRowKeyRange(sink);
                    return sink.sum;
                });
                timeEnum(shape.getKey(), "forEachRowKeyRange/shifted", rows, () -> {
                    sink.sum = 0;
                    shifted.forEachRowKeyRange(sink);
                    return sink.sum;
                });
            }
        }
    }

    /** Enumerate keys by materializing each window into a chunk and applying the shift as a tight vector loop. */
    private static long chunkedShiftKeys(@NotNull final RowSet rs, final long shift) {
        long sum = 0;
        try (final RowSequence.Iterator it = rs.getRowSequenceIterator();
                final WritableLongChunk<OrderedRowKeys> keys = WritableLongChunk.makeWritableChunk(READ_SIZE)) {
            while (it.hasMore()) {
                it.getNextRowSequenceWithLength(READ_SIZE).fillRowKeyChunk(keys);
                final int n = keys.size();
                for (int i = 0; i < n; ++i) {
                    sum += keys.get(i) + shift;
                }
            }
        }
        return sum;
    }

    private static void timeEnum(
            @NotNull final String shapeLabel,
            @NotNull final String variant,
            final long rows,
            @NotNull final LongSupplier op) {
        for (int i = 0; i < WARMUP_ITERS; ++i) {
            blackhole += op.getAsLong();
        }
        long best = Long.MAX_VALUE;
        for (int i = 0; i < TIMED_ITERS; ++i) {
            final long start = System.nanoTime();
            blackhole += op.getAsLong();
            best = Math.min(best, System.nanoTime() - start);
        }
        System.out.printf("%-14s %-24s %,14d %10.3f%n", shapeLabel, variant, rows, (double) best / rows);
    }

    /** Reusable consumer used for all enumeration variants so the consumer's call-site shape is held constant. */
    private static final class LongSink implements LongAbortableConsumer, LongRangeAbortableConsumer {
        private long sum;

        @Override
        public boolean accept(final long value) {
            sum += value;
            return true;
        }

        @Override
        public boolean accept(final long start, final long end) {
            sum += start + end;
            return true;
        }
    }

    private static final long[] POLLUTE = new long[8];

    /**
     * Isolates the dispatch cost of {@link ShiftedRowSequence}: push ({@code forEachRowKey} + consumer) vs pull
     * ({@link RowSet.Iterator} with the shift applied inline by the caller), each measured with a monomorphic consumer
     * call site and again after the shared {@code forEachRowKey} call sites are polluted with many distinct consumer
     * classes (megamorphic), which is what a running server actually presents.
     */
    @Test
    public void benchmarkShiftedRowSequenceDispatch() {
        final long enumRows = Long.getLong("bench.enumRows", 200_000_000L);
        final long shift = Long.getLong("bench.shift", 1L << 40);
        System.out.println("=== ShiftedRowSequence dispatch micro-benchmark (push vs pull; mono vs megamorphic) ===");
        System.out.printf("  enumRows=%d, shift=%d, warmup=%d, iters=%d (ns/row)%n%n",
                enumRows, shift, WARMUP_ITERS, TIMED_ITERS);

        try (final WritableRowSet everyOther = stridedKeysOver(enumRows, EVERY_OTHER_STRIDE);
                final WritableRowSet strideRanges = stridedRangesOver(enumRows, RANGE_RUN, RANGE_PERIOD)) {
            final Map<String, WritableRowSet> shapes = new LinkedHashMap<>();
            shapes.put("everyOther", everyOther);
            shapes.put("strideRanges", strideRanges);

            System.out.printf("%-14s %-28s %10s %10s%n", "shape", "variant", "mono", "mega");
            for (final Map.Entry<String, WritableRowSet> shape : shapes.entrySet()) {
                final WritableRowSet rs = shape.getValue();
                final RowSequence shifted = ShiftedRowSequence.wrap(rs, shift);
                final long rows = rs.size();
                final LongSink sink = new LongSink();

                final double pushRawMono = nsPerRow(rows, () -> {
                    sink.sum = 0;
                    rs.forEachRowKey(sink);
                    return sink.sum;
                });
                final double pushShiftMono = nsPerRow(rows, () -> {
                    sink.sum = 0;
                    shifted.forEachRowKey(sink);
                    return sink.sum;
                });
                final double pullRawMono = nsPerRow(rows, () -> pullKeys(rs, 0));
                final double pullShiftMono = nsPerRow(rows, () -> pullKeys(rs, shift));

                polluteForEachRowKey(shift);

                final double pushRawMega = nsPerRow(rows, () -> {
                    sink.sum = 0;
                    rs.forEachRowKey(sink);
                    return sink.sum;
                });
                final double pushShiftMega = nsPerRow(rows, () -> {
                    sink.sum = 0;
                    shifted.forEachRowKey(sink);
                    return sink.sum;
                });
                final double pullRawMega = nsPerRow(rows, () -> pullKeys(rs, 0));
                final double pullShiftMega = nsPerRow(rows, () -> pullKeys(rs, shift));

                dispatchRow(shape.getKey(), "push forEachRowKey raw", pushRawMono, pushRawMega);
                dispatchRow(shape.getKey(), "push forEachRowKey shifted", pushShiftMono, pushShiftMega);
                dispatchRow(shape.getKey(), "pull iterator raw", pullRawMono, pullRawMega);
                dispatchRow(shape.getKey(), "pull iterator shifted", pullShiftMono, pullShiftMega);
            }
        }
    }

    /** Pull-style per-key enumeration; the caller applies the shift inline, with no consumer object at all. */
    private static long pullKeys(@NotNull final RowSet rs, final long shift) {
        long sum = 0;
        try (final RowSet.Iterator it = rs.iterator()) {
            while (it.hasNext()) {
                sum += it.nextLong() + shift;
            }
        }
        return sum;
    }

    /**
     * Drive the shared {@code forEachRowKey} consumer call sites with many distinct consumer classes so the JIT
     * recompiles them megamorphic, mirroring a running server. Uses a tiny row set so this is cheap.
     */
    private static void polluteForEachRowKey(final long shift) {
        final LongAbortableConsumer[] polluters = new LongAbortableConsumer[] {
                v -> {
                    POLLUTE[0] += v;
                    return true;
                },
                v -> {
                    POLLUTE[1] += v;
                    return true;
                },
                v -> {
                    POLLUTE[2] += v;
                    return true;
                },
                v -> {
                    POLLUTE[3] += v;
                    return true;
                },
                v -> {
                    POLLUTE[4] += v;
                    return true;
                },
                v -> {
                    POLLUTE[5] += v;
                    return true;
                },
                v -> {
                    POLLUTE[6] += v;
                    return true;
                },
                v -> {
                    POLLUTE[7] += v;
                    return true;
                },
        };
        try (final WritableRowSet small = RowSetFactory.fromRange(0, 100_000)) {
            final RowSequence smallShifted = ShiftedRowSequence.wrap(small, shift);
            for (int round = 0; round < 200; ++round) {
                for (final LongAbortableConsumer polluter : polluters) {
                    small.forEachRowKey(polluter);
                    smallShifted.forEachRowKey(polluter);
                }
            }
        }
    }

    private static double nsPerRow(final long rows, @NotNull final LongSupplier op) {
        for (int i = 0; i < WARMUP_ITERS; ++i) {
            blackhole += op.getAsLong();
        }
        long best = Long.MAX_VALUE;
        for (int i = 0; i < TIMED_ITERS; ++i) {
            final long start = System.nanoTime();
            blackhole += op.getAsLong();
            best = Math.min(best, System.nanoTime() - start);
        }
        return (double) best / rows;
    }

    private static void dispatchRow(
            @NotNull final String shapeLabel, @NotNull final String variant, final double mono, final double mega) {
        System.out.printf("%-14s %-28s %10.3f %10.3f%n", shapeLabel, variant, mono, mega);
    }

    /**
     * Measures how the {@link ShiftedRowSequence} wrapper penalty depends on consumer weight. The hypothesis is that
     * the wrapper's cost is an inlining barrier: it prevents the consumer from folding into the container's tight
     * enumeration loop. A heavier (gather) consumer that reads and writes memory per key should show a larger absolute
     * shifted-minus-raw penalty than a trivial accumulate consumer, especially where the raw path was a tight loop.
     */
    @Test
    public void benchmarkShiftedRowSequenceInliningBarrier() {
        final long enumRows = Long.getLong("bench.enumRows", 200_000_000L);
        final long shift = Long.getLong("bench.shift", 1L << 40);
        final int srcSize = 1 << 20;
        System.out.println("=== ShiftedRowSequence inlining-barrier micro-benchmark (trivial vs gather consumer) ===");
        System.out.printf("  enumRows=%d, shift=%d, srcSize=%d, warmup=%d, iters=%d (ns/row)%n%n",
                enumRows, shift, srcSize, WARMUP_ITERS, TIMED_ITERS);

        final int[] src = new int[srcSize];
        final Random random = new Random(SEED);
        for (int i = 0; i < srcSize; ++i) {
            src[i] = random.nextInt();
        }
        final GatherSink gather = new GatherSink(src, new int[READ_SIZE]);
        final LongSink sum = new LongSink();

        try (final WritableRowSet everyOther = stridedKeysOver(enumRows, EVERY_OTHER_STRIDE);
                final WritableRowSet strideRanges = stridedRangesOver(enumRows, RANGE_RUN, RANGE_PERIOD)) {
            final Map<String, WritableRowSet> shapes = new LinkedHashMap<>();
            shapes.put("everyOther", everyOther);
            shapes.put("strideRanges", strideRanges);

            System.out.printf("%-14s %-20s %10s %10s %10s%n", "shape", "consumer", "raw", "shifted", "penalty");
            for (final Map.Entry<String, WritableRowSet> shape : shapes.entrySet()) {
                final WritableRowSet rs = shape.getValue();
                final RowSequence shifted = ShiftedRowSequence.wrap(rs, shift);
                final long rows = rs.size();

                final double trivialRaw = nsPerRow(rows, () -> {
                    sum.sum = 0;
                    rs.forEachRowKey(sum);
                    return sum.sum;
                });
                final double trivialShift = nsPerRow(rows, () -> {
                    sum.sum = 0;
                    shifted.forEachRowKey(sum);
                    return sum.sum;
                });
                final double gatherRaw = nsPerRow(rows, () -> {
                    gather.pos = 0;
                    rs.forEachRowKey(gather);
                    return gather.pos;
                });
                final double gatherShift = nsPerRow(rows, () -> {
                    gather.pos = 0;
                    shifted.forEachRowKey(gather);
                    return gather.pos;
                });

                barrierRow(shape.getKey(), "trivial sum", trivialRaw, trivialShift);
                barrierRow(shape.getKey(), "gather load+store", gatherRaw, gatherShift);
            }
        }
    }

    private static void barrierRow(
            @NotNull final String shapeLabel, @NotNull final String consumer, final double raw, final double shifted) {
        System.out.printf("%-14s %-20s %10.3f %10.3f %10.3f%n", shapeLabel, consumer, raw, shifted, shifted - raw);
    }

    /** Realistic gather consumer: one array load (masked key) and one array store per key. */
    private static final class GatherSink implements LongAbortableConsumer {
        private final int[] src;
        private final int[] dst;
        private final int srcMask;
        private final int dstMask;
        private int pos;

        private GatherSink(final int[] src, final int[] dst) {
            this.src = src;
            this.dst = dst;
            this.srcMask = src.length - 1;
            this.dstMask = dst.length - 1;
        }

        @Override
        public boolean accept(final long key) {
            dst[pos++ & dstMask] = src[(int) (key & srcMask)];
            return true;
        }
    }

    /**
     * Decomposes the union column-scan tax into (a) union machinery and (b) the {@link ShiftedRowSequence} barrier.
     * The union's first constituent (slot 0) has row-key shift 0, so with the {@code shiftAmount == 0} short-circuit in
     * {@link ShiftedRowSequence#forEachRowKey} its by-key fill runs the full union machinery (getChunk dispatch, slot
     * lookup, context churn) but with no per-key barrier. A later constituent has a nonzero shift, so it additionally
     * pays the barrier. Comparing each merged scan to the SourceTable scan of the same logical rows isolates both.
     */
    @Test
    public void benchmarkUnionTaxDecomposition() {
        final UpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph();
        final Table[] backing = buildBacking();
        final Table sourceTable = new SimpleSourceTable(CONSTITUENT_DEFINITION, "BenchSourceTable",
                RegionedTableComponentFactoryImpl.INSTANCE, makeProvider(updateGraph, backing), null).coalesce();
        final Table merged = new SourcePartitionedTable(CONSTITUENT_DEFINITION, null,
                makeProvider(updateGraph, backing), false, false, null).merge().coalesce();
        assertThat(sourceTable.size()).isEqualTo(TOTAL_ROWS);
        assertThat(merged.size()).isEqualTo(TOTAL_ROWS);

        final ColumnSource<?> sourceCol = sourceTable.getColumnSource(COLUMN_NAME);
        final ColumnSource<?> mergedCol = merged.getColumnSource(COLUMN_NAME);
        final long rpl = ROWS_PER_LOCATION;
        final long lastStart = (long) (NUM_LOCATIONS - 1) * rpl;

        System.out.println("=== Union tax decomposition (everyOther, getChunk, ns/row) ===");
        System.out.printf("  locations=%d, rows/location=%d%n%n", NUM_LOCATIONS, ROWS_PER_LOCATION);

        try (final WritableRowSet firstPos = everyOtherInRange(0, rpl);
                final WritableRowSet lastPos = everyOtherInRange(lastStart, lastStart + rpl);
                final RowSet srcFirst = sourceTable.getRowSet().subSetForPositions(firstPos);
                final RowSet mrgFirst = merged.getRowSet().subSetForPositions(firstPos);
                final RowSet srcLast = sourceTable.getRowSet().subSetForPositions(lastPos);
                final RowSet mrgLast = merged.getRowSet().subSetForPositions(lastPos)) {
            final double sourceFirst = nsPerRow(srcFirst.size(), () -> {
                scan(Mode.GET_CHUNK, sourceCol, srcFirst);
                return 0L;
            });
            final double mergedFirst = nsPerRow(mrgFirst.size(), () -> {
                scan(Mode.GET_CHUNK, mergedCol, mrgFirst);
                return 0L;
            });
            final double sourceLast = nsPerRow(srcLast.size(), () -> {
                scan(Mode.GET_CHUNK, sourceCol, srcLast);
                return 0L;
            });
            final double mergedLast = nsPerRow(mrgLast.size(), () -> {
                scan(Mode.GET_CHUNK, mergedCol, mrgLast);
                return 0L;
            });

            System.out.printf("%-26s %10s %10s%n", "", "first(shift0)", "last(shifted)");
            System.out.printf("%-26s %10.3f %10.3f%n", "SourceTable", sourceFirst, sourceLast);
            System.out.printf("%-26s %10.3f %10.3f%n", "Merged", mergedFirst, mergedLast);
            final double machinery = mergedFirst - sourceFirst;
            final double machineryPlusBarrier = mergedLast - sourceLast;
            System.out.printf("%n%-26s %10.3f  (merged - source, no barrier)%n", "union machinery:", machinery);
            System.out.printf("%-26s %10.3f  (merged - source, with barrier)%n",
                    "machinery + barrier:", machineryPlusBarrier);
            System.out.printf("%-26s %10.3f  (difference)%n", "ShiftedRowSequence barrier:",
                    machineryPlusBarrier - machinery);
        }
    }

    private static WritableRowSet everyOtherInRange(final long startPosition, final long endPosition) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (long position = startPosition; position < endPosition; position += EVERY_OTHER_STRIDE) {
            builder.appendKey(position);
        }
        return builder.build();
    }

    private static Table[] buildBacking() {
        final Random random = new Random(SEED);
        final Table[] backing = new Table[NUM_LOCATIONS];
        for (int location = 0; location < NUM_LOCATIONS; ++location) {
            final int[] values = new int[ROWS_PER_LOCATION];
            for (int ri = 0; ri < ROWS_PER_LOCATION; ++ri) {
                values[ri] = random.nextInt();
            }
            final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
            columns.put(COLUMN_NAME, new ImmutableIntArraySource(values));
            final TrackingRowSet rowSet = RowSetFactory.flat(ROWS_PER_LOCATION).toTracking();
            // TableBackedTableLocationProvider requires each backing location to be append-only.
            backing[location] = new QueryTable(CONSTITUENT_DEFINITION, rowSet, columns)
                    .withAttributes(Map.of(Table.APPEND_ONLY_TABLE_ATTRIBUTE, true));
        }
        return backing;
    }

    private static TableBackedTableLocationProvider makeProvider(
            @NotNull final UpdateGraph registrar,
            @NotNull final Table[] backing) {
        return new TableBackedTableLocationProvider(
                registrar, false, TableUpdateMode.STATIC, TableUpdateMode.STATIC, backing);
    }

    /** Positions {@code {0, stride, 2*stride, ...}} as single keys, exercising by-key fills when fragmented. */
    private static WritableRowSet stridedKeys(final long stride) {
        return stridedKeysOver(TOTAL_ROWS, stride);
    }

    private static WritableRowSet stridedKeysOver(final long total, final long stride) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (long position = 0; position < total; position += stride) {
            builder.appendKey(position);
        }
        return builder.build();
    }

    /** Positions in runs of {@code run} consecutive rows every {@code period}, exercising by-range bulk fills. */
    private static WritableRowSet stridedRanges(final long run, final long period) {
        return stridedRangesOver(TOTAL_ROWS, run, period);
    }

    private static WritableRowSet stridedRangesOver(final long total, final long run, final long period) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (long start = 0; start < total; start += period) {
            builder.appendRange(start, Math.min(start + run - 1, total - 1));
        }
        return builder.build();
    }

    private static Result timeScan(
            @NotNull final String tableLabel,
            @NotNull final String shapeLabel,
            @NotNull final Mode mode,
            @NotNull final ColumnSource<?> columnSource,
            @NotNull final RowSet rowSet) {
        for (int i = 0; i < WARMUP_ITERS; ++i) {
            scan(mode, columnSource, rowSet);
        }
        long best = Long.MAX_VALUE;
        long sum = 0;
        for (int i = 0; i < TIMED_ITERS; ++i) {
            final long start = System.nanoTime();
            scan(mode, columnSource, rowSet);
            final long elapsed = System.nanoTime() - start;
            best = Math.min(best, elapsed);
            sum += elapsed;
        }
        final long rows = rowSet.size();
        final Result result = new Result(tableLabel, shapeLabel, mode, rows,
                best / 1e6, (sum / (double) TIMED_ITERS) / 1e6, (double) best / rows);
        System.out.printf("  %-22s %-12s %-10s rows=%,12d  best=%9.2f ms  %7.3f ns/row%n",
                result.tableLabel, result.shapeLabel, mode, result.rows, result.bestMs, result.nsPerRow);
        return result;
    }

    /** Runs a single scan pass over {@code rowSet} in {@link #READ_SIZE}-row windows using {@code mode}. */
    private static void scan(
            @NotNull final Mode mode,
            @NotNull final ColumnSource<?> columnSource,
            @NotNull final RowSet rowSet) {
        long sink = 0;
        switch (mode) {
            case ITERATE:
                try (final RowSequence.Iterator it = rowSet.getRowSequenceIterator()) {
                    while (it.hasMore()) {
                        sink += it.getNextRowSequenceWithLength(READ_SIZE).size();
                    }
                }
                break;
            case GET_CHUNK:
                try (final ChunkSource.GetContext context = columnSource.makeGetContext(READ_SIZE);
                        final RowSequence.Iterator it = rowSet.getRowSequenceIterator()) {
                    while (it.hasMore()) {
                        final RowSequence rs = it.getNextRowSequenceWithLength(READ_SIZE);
                        final Chunk<? extends Values> chunk = columnSource.getChunk(context, rs);
                        sink += chunk.size();
                    }
                }
                break;
            case FILL_CHUNK:
                try (final ChunkSource.FillContext context = columnSource.makeFillContext(READ_SIZE);
                        final WritableIntChunk<Values> destination = WritableIntChunk.makeWritableChunk(READ_SIZE);
                        final RowSequence.Iterator it = rowSet.getRowSequenceIterator()) {
                    while (it.hasMore()) {
                        final RowSequence rs = it.getNextRowSequenceWithLength(READ_SIZE);
                        columnSource.fillChunk(context, destination, rs);
                        sink += destination.size();
                    }
                }
                break;
        }
        blackhole += sink;
    }

    private static Recording startRecording() {
        final String jfrPath = System.getProperty("bench.jfr");
        if (jfrPath == null) {
            return null;
        }
        final Recording recording = new Recording();
        recording.enable("jdk.ExecutionSample").withPeriod(Duration.ofMillis(1));
        recording.setToDisk(true);
        recording.start();
        System.out.printf("JFR recording started; will write %s%n%n", jfrPath);
        return recording;
    }

    private static void stopRecording(final Recording recording) throws IOException {
        if (recording == null) {
            return;
        }
        recording.stop();
        final Path path = Path.of(System.getProperty("bench.jfr"));
        recording.dump(path);
        recording.close();
        System.out.printf("%nJFR recording written to %s%n", path.toAbsolutePath());
    }

    private static void printModeSummary(@NotNull final List<Result> results) {
        System.out.printf("%n=== Summary (best-of-%d, ns/row) ===%n", TIMED_ITERS);
        System.out.printf("%-22s %-12s %-10s %14s %12s %10s%n",
                "table", "shape", "mode", "rows", "best(ms)", "ns/row");
        for (final Result r : results) {
            System.out.printf("%-22s %-12s %-10s %,14d %12.2f %10.3f%n",
                    r.tableLabel, r.shapeLabel, r.mode, r.rows, r.bestMs, r.nsPerRow);
        }
    }

    /** Derives per-row iteration / descent / fill contributions from the three modes. */
    private static void printAttribution(@NotNull final List<Result> results) {
        final Map<String, Map<Mode, Double>> byKey = new LinkedHashMap<>();
        for (final Result r : results) {
            byKey.computeIfAbsent(r.tableLabel + "|" + r.shapeLabel, k -> new LinkedHashMap<>()).put(r.mode,
                    r.nsPerRow);
        }
        System.out.printf("%n=== Attribution (ns/row) ===%n");
        System.out.printf("%-22s %-12s %10s %10s %10s %10s%n",
                "table", "shape", "iterate", "descend", "fill", "total");
        for (final Map.Entry<String, Map<Mode, Double>> entry : byKey.entrySet()) {
            final String[] parts = entry.getKey().split("\\|", 2);
            final Map<Mode, Double> modes = entry.getValue();
            final double iterate = modes.getOrDefault(Mode.ITERATE, 0.0);
            final double getChunk = modes.getOrDefault(Mode.GET_CHUNK, 0.0);
            final double fillChunk = modes.getOrDefault(Mode.FILL_CHUNK, 0.0);
            // descend ~= getChunk - iterate (region/constituent resolution + any view/fill getChunk already did);
            // fill ~= fillChunk - getChunk (extra cost of forcing a copy beyond what getChunk did).
            final double descend = getChunk - iterate;
            final double fill = fillChunk - getChunk;
            System.out.printf("%-22s %-12s %10.3f %10.3f %10.3f %10.3f%n",
                    parts[0], parts[1], iterate, descend, fill, fillChunk);
        }
    }

    private static void retainByProperty(@NotNull final Map<String, ?> map, @NotNull final String property) {
        final String csv = System.getProperty(property);
        if (csv != null) {
            map.keySet().retainAll(new LinkedHashSet<>(Arrays.asList(csv.split(","))));
        }
    }

    private static double seconds(final long startNanos) {
        return (System.nanoTime() - startNanos) / 1e9;
    }

    private static final class Result {
        private final String tableLabel;
        private final String shapeLabel;
        private final Mode mode;
        private final long rows;
        private final double bestMs;
        private final double meanMs;
        private final double nsPerRow;

        private Result(final String tableLabel, final String shapeLabel, final Mode mode, final long rows,
                final double bestMs, final double meanMs, final double nsPerRow) {
            this.tableLabel = tableLabel;
            this.shapeLabel = shapeLabel;
            this.mode = mode;
            this.rows = rows;
            this.bestMs = bestMs;
            this.meanMs = meanMs;
            this.nsPerRow = nsPerRow;
        }
    }
}
