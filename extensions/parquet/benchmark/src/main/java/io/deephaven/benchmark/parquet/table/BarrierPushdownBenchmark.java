//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.parquet.table;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterDelegating;
import io.deephaven.engine.table.impl.select.WhereFilterDelegatingBase;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.parquet.table.metadata.RowGroupInfo;
import io.deephaven.util.SafeCloseable;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The cost of keeping a barrier respecter's pushdown to the keys of the rows its declarer kept, at each pushdown tier
 * that evaluates a filter over a structure wider than its selection.
 *
 * <p>
 * One {@code where} with two filters over 10M rows: a declarer that models a row ACL (a simple range filter keeping
 * {@code aclPct} percent of the rows) and a respecter that models a user formula ({@link #pass}, keeping half of the
 * keys), which respects a barrier the ACL declares. {@code CORRELATED} keeps whole keys, so the respecter's rows hold
 * only {@code aclPct} percent of the keys; {@code INDEPENDENT} keeps rows of every key, the worst case for a pushdown
 * kept to those rows, which then saves nothing. {@code PUSHDOWN} lets the formula be pushed down: on a build that keeps
 * pushdown behind a barrier to the rows in question it measures that safe pushdown, and on a build that does not, the
 * unsafe pushdown it replaces. {@code NO_PUSHDOWN} never pushes the formula down, so it runs on every row the ACL kept.
 * </p>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(1)
public class BarrierPushdownBenchmark {
    private static final long ROWS = 10_000_000;
    private static final int ROWS_PER_RUN = 100;
    private static final Path DATA_DIR = Paths.get(System.getProperty("bench.dataDir", "bench-dh24083-data"));

    private static final AtomicLong CALLS = new AtomicLong();

    /** The user formula: counts its calls, and keeps half of the keys. Null is the pushdown null-behavior probe. */
    public static boolean pass(final String key) {
        if (key == null) {
            return false;
        }
        CALLS.incrementAndGet();
        return (key.hashCode() & 1) == 0;
    }

    public enum Tier {
        INDEX_MEMORY, PARQUET_DICT, PARQUET_INDEX
    }

    public enum Layout {
        CORRELATED, INDEPENDENT
    }

    public enum Pushdown {
        NO_PUSHDOWN, PUSHDOWN
    }

    /** The respecter is never pushed down, so it runs on every row its declarer kept. */
    private static final class NoPushdownFilter extends WhereFilterDelegatingBase {
        private NoPushdownFilter(final WhereFilter filter) {
            super(filter);
        }

        @Override
        public boolean canPushdown() {
            return false;
        }

        @Override
        public WhereFilter maybeUnwrapFilter() {
            return WhereFilterDelegating.maybeUnwrapFilter(filter);
        }

        @Override
        public WhereFilter copy() {
            return new NoPushdownFilter(filter.copy());
        }
    }

    @Param({"INDEX_MEMORY", "PARQUET_DICT", "PARQUET_INDEX"})
    private Tier tier;
    @Param({"1", "10", "50", "90"})
    private int aclPct;
    @Param({"CORRELATED", "INDEPENDENT"})
    private Layout layout;
    @Param({"NO_PUSHDOWN", "PUSHDOWN"})
    private Pushdown pushdown;
    @Param({"1000", "20000"})
    private int keys;

    private SafeCloseable contextCloseable;
    private Table source;
    /** Held strongly: DataIndexer keeps the index weakly, so without this GC could drop it mid-trial. */
    private DataIndex dataIndex;
    private Filter filters;

    private long callsTotal;
    private long invocations;
    private long resultSize = -1;

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        contextCloseable = TestExecutionContext.createForUnitTests().open();
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().enableUnitTestMode();
        ExecutionContext.getContext().getQueryLibrary().importClass(BarrierPushdownBenchmark.class);

        source = makeSource();

        final Object barrier = new Object();
        final WhereFilter acl = WhereFilterFactory.getExpression(layout == Layout.CORRELATED
                ? "KeyId < " + (int) ((long) keys * aclPct / 100)
                : "Bucket < " + aclPct);
        WhereFilter user = WhereFilterFactory.getExpression("BarrierPushdownBenchmark.pass(Key)");
        if (pushdown == Pushdown.NO_PUSHDOWN) {
            user = new NoPushdownFilter(user);
        }
        filters = Filter.and(acl.withDeclaredBarriers(barrier), user.withRespectedBarriers(barrier));

        // Settle lazy state (index materialization, parquet metadata) outside the measurement, and check the result.
        resultSize = source.where(filters).size();
        CALLS.set(0);
        callsTotal = 0;
        invocations = 0;
    }

    private Table makeSource() throws Exception {
        final Table generated = TableTools.emptyTable(ROWS).select(
                "KeyId = (int) ((ii / " + ROWS_PER_RUN + ") % " + keys + ")",
                "Bucket = (int) (ii % 100)",
                "Key = `K` + KeyId");
        if (tier == Tier.INDEX_MEMORY) {
            // where() uses only an index whose table is already computed.
            dataIndex = DataIndexer.getOrCreateDataIndex(generated, "Key");
            dataIndex.table();
            return generated;
        }
        Files.createDirectories(DATA_DIR);
        final Path file = DATA_DIR.resolve(tier.name().toLowerCase() + "-" + keys + ".parquet");
        if (!Files.exists(file)) {
            final ParquetInstructions.Builder instructions = ParquetInstructions.builder()
                    .setRowGroupInfo(RowGroupInfo.maxRows(1_000_000));
            if (tier == Tier.PARQUET_INDEX) {
                instructions.addIndexColumns("Key").setMaximumDictionaryKeys(0);
            }
            ParquetTools.writeTable(generated, file.toString(), instructions.build());
        }
        return ParquetTools.readTable(file.toString()).coalesce();
    }


    @TearDown(Level.Trial)
    public void tearDown() {
        System.out.printf(
                "CALLS tier=%s aclPct=%d layout=%s pushdown=%s keys=%d callsPerInvocation=%.1f resultSize=%d%n",
                tier, aclPct, layout, pushdown, keys, invocations == 0 ? 0.0 : (double) callsTotal / invocations,
                resultSize);
        contextCloseable.close();
    }

    @Benchmark
    public void where(final Blackhole bh) {
        final long before = CALLS.get();
        final Table result = source.where(filters);
        bh.consume(result.size());
        callsTotal += CALLS.get() - before;
        ++invocations;
    }
}
