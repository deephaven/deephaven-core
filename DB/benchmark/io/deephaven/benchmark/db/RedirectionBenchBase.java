package io.deephaven.benchmark.db;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.select.IncrementalReleaseFilter;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.SharedContext;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.benchmarking.BenchmarkTools;
import io.deephaven.benchmarking.runner.TableBenchmarkState;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;

public abstract class RedirectionBenchBase {

    private Table inputTable;
    private WritableChunk<Attributes.Values> chunk;

    protected TableBenchmarkState state;

    private int chunkCapacity;
    private boolean skipResultsProcessing;
    private int nFillCols;
    private ColumnSource[] fillSources;
    private ChunkSource.FillContext[] fillContexts;
    private SharedContext sharedContext;

    public class QueryData {
        public final Table live;
        public final IncrementalReleaseFilter incrementalReleaseFilter;
        public final int steps;
        public final String[] fillCols;
        public final WritableChunk<Attributes.Values> chunk;

        public QueryData(
                final Table live,
                final IncrementalReleaseFilter incrementalReleaseFilter,
                final int steps,
                final String[] fillCol,
                final WritableChunk<Attributes.Values> chunk) {
            this.live = live;
            this.incrementalReleaseFilter = incrementalReleaseFilter;
            this.steps = steps;
            this.fillCols = fillCol;
            this.chunk = chunk;
        }
    }

    protected abstract QueryData getQuery();


    @Setup(Level.Trial)
    public void setupEnv(final BenchmarkParams params) {
        chunkCapacity = Integer.parseInt(params.getParam("chunkCapacity"));
        skipResultsProcessing = Boolean.parseBoolean(params.getParam("skipResultsProcessing"));

        LiveTableMonitor.DEFAULT.enableUnitTestMode();

        state = new TableBenchmarkState(BenchmarkTools.stripName(params.getBenchmark()), params.getWarmup().getCount());

        final QueryData queryData = getQuery();
        for (int step = 0; step < queryData.steps; ++step) {
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(queryData.incrementalReleaseFilter::refresh);
        }
        inputTable = queryData.live;
        nFillCols = queryData.fillCols.length;
        fillSources = new ColumnSource[nFillCols];
        fillContexts = new ChunkSource.FillContext[nFillCols];
        sharedContext = nFillCols > 1 ? SharedContext.makeSharedContext() : null;
        for (int i = 0; i < nFillCols; ++i) {
            fillSources[i] = inputTable.getColumnSource(queryData.fillCols[i]);
            fillContexts[i] = fillSources[i].makeFillContext(chunkCapacity, sharedContext);
        }
        chunk = queryData.chunk;
    }

    @TearDown(Level.Trial)
    public void finishTrial() {
        try {
            state.logOutput();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
        state.init();
    }

    @TearDown(Level.Iteration)
    public void finishIteration(BenchmarkParams params) throws IOException {
        if (skipResultsProcessing) {
            return;
        }
        state.processResult(params);
    }

    private Table doFill(final Table t, final Blackhole bh) {
        final Index ix = t.getIndex();
        try (final OrderedKeys.Iterator it = ix.getOrderedKeysIterator()) {
            while (it.hasMore()) {
                if (sharedContext != null) {
                    sharedContext.reset();
                }
                for (int i = 0; i < nFillCols; ++i) {
                    final ColumnSource cs = fillSources[i];
                    final ChunkSource.FillContext fc = fillContexts[i];
                    try (final OrderedKeys oks = it.getNextOrderedKeysWithLength(chunkCapacity)) {
                        cs.fillChunk(fc, chunk, oks);
                        bh.consume(chunk);
                        chunk.setSize(chunkCapacity);
                    }
                }
            }
        }
        return t;
    }

    @Benchmark
    public Table redirectedFillChunk(final Blackhole bh) {
        final Table t1 = doFill(inputTable, bh);
        final Table result = state.setResult(t1);
        bh.consume(result);
        return result;
    }
}
