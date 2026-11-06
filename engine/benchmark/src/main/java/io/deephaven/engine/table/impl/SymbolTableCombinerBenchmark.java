//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.sources.IntegerSparseArraySource;
import io.deephaven.engine.table.impl.sources.regioned.SymbolTableSource;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
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

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures combining two symbol tables the way a static join on a symbol table column does: give each symbol of the
 * smaller table a unique identifier, then look up the larger table's symbols, mapping every symbol table id to its
 * symbol's identifier.
 * <ul>
 * <li>{@link #combiner()} uses {@link SymbolTableCombiner}.</li>
 * <li>{@link #fastutilMap()} does the same with a fastutil {@link Object2IntOpenHashMap}, one symbol at a time.</li>
 * </ul>
 * Both write their results into {@link IntegerSparseArraySource IntegerSparseArraySources}, as the join does. The two
 * tables have {@code symbols} distinct symbols each, of which {@code overlapPercent} percent are shared. The benchmark
 * lives in the combiner's package so that it can use the package-private combiner.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
@Fork(1)
public class SymbolTableCombinerBenchmark {
    private static final int CHUNK_SIZE = 4096;

    @Param({"10000", "1000000"})
    private int symbols;

    @Param({"50"})
    private int overlapPercent;

    private SafeCloseable executionContext;
    private Table buildTable;
    private Table probeTable;

    @Setup(Level.Trial)
    public void setupEnv() {
        executionContext = TestExecutionContext.createForUnitTests().open();
        final Random random = new Random(0);
        final int shared = (int) ((long) symbols * overlapPercent / 100);
        // distinct symbols: the build table has [0, symbols) and the probe table [symbols - shared, 2 * symbols -
        // shared), each in a random order and with a random prefix so that the strings are not all the same length
        buildTable = symbolTable(random, 0);
        probeTable = symbolTable(random, symbols - shared);
    }

    private Table symbolTable(final Random random, final int firstSymbol) {
        final long[] ids = new long[symbols];
        final String[] values = new String[symbols];
        for (int ii = 0; ii < symbols; ++ii) {
            // symbol table ids are sparse
            ids[ii] = 3L * ii + 7;
            final int symbol = firstSymbol + ii;
            values[ii] = "SYM" + (symbol % 7 == 0 ? "_" : "") + symbol;
        }
        for (int ii = symbols - 1; ii > 0; --ii) {
            final int jj = random.nextInt(ii + 1);
            final String swap = values[ii];
            values[ii] = values[jj];
            values[jj] = swap;
        }
        return TableTools.newTable(TableTools.longCol(SymbolTableSource.ID_COLUMN_NAME, ids),
                TableTools.stringCol(SymbolTableSource.SYMBOL_COLUMN_NAME, values));
    }

    @TearDown(Level.Trial)
    public void tearDownEnv() {
        executionContext.close();
    }

    @Benchmark
    public int combiner() {
        final IntegerSparseArraySource buildMapper = new IntegerSparseArraySource();
        final IntegerSparseArraySource probeMapper = new IntegerSparseArraySource();
        final SymbolTableCombiner combiner = new SymbolTableCombiner(
                new ColumnSource[] {buildTable.getColumnSource(SymbolTableSource.SYMBOL_COLUMN_NAME)},
                SymbolTableCombiner.hashTableSize(symbols));
        combiner.addSymbols(buildTable, buildMapper);
        combiner.lookupSymbols(probeTable, probeMapper, Integer.MAX_VALUE);
        return combiner.getMaximumIdentifier();
    }

    @Benchmark
    public int fastutilMap() {
        final IntegerSparseArraySource buildMapper = new IntegerSparseArraySource();
        final IntegerSparseArraySource probeMapper = new IntegerSparseArraySource();
        final Object2IntOpenHashMap<String> uniqueIds = new Object2IntOpenHashMap<>(symbols);
        uniqueIds.defaultReturnValue(-1);
        forEachSymbol(buildTable, (id, symbol) -> {
            int uniqueId = uniqueIds.getInt(symbol);
            if (uniqueId == -1) {
                uniqueId = uniqueIds.size();
                uniqueIds.put(symbol, uniqueId);
            }
            buildMapper.set(id, uniqueId);
        });
        forEachSymbol(probeTable, (id, symbol) -> {
            final int uniqueId = uniqueIds.getInt(symbol);
            probeMapper.set(id, uniqueId == -1 ? Integer.MAX_VALUE : uniqueId);
        });
        return uniqueIds.size();
    }

    @FunctionalInterface
    private interface SymbolConsumer {
        void accept(long id, String symbol);
    }

    /**
     * Read the ids and symbols of {@code symbolTable} a chunk at a time, as the combiner does.
     */
    private static void forEachSymbol(final Table symbolTable, final SymbolConsumer consumer) {
        final ColumnSource<Long> idSource = symbolTable.getColumnSource(SymbolTableSource.ID_COLUMN_NAME);
        final ColumnSource<String> symbolSource = symbolTable.getColumnSource(SymbolTableSource.SYMBOL_COLUMN_NAME);
        final int chunkSize = (int) Math.min(CHUNK_SIZE, symbolTable.size());
        try (final ColumnSource.GetContext idContext = idSource.makeGetContext(chunkSize);
                final ColumnSource.GetContext symbolContext = symbolSource.makeGetContext(chunkSize);
                final RowSequence.Iterator rsIt = symbolTable.getRowSet().getRowSequenceIterator()) {
            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(chunkSize);
                final LongChunk<? extends Values> ids = idSource.getChunk(idContext, chunkOk).asLongChunk();
                final ObjectChunk<String, ? extends Values> values =
                        symbolSource.getChunk(symbolContext, chunkOk).asObjectChunk();
                for (int ii = 0; ii < ids.size(); ++ii) {
                    consumer.accept(ids.get(ii), values.get(ii));
                }
            }
        }
    }

    public static void main(String[] args) {
        BenchUtil.run(SymbolTableCombinerBenchmark.class);
    }
}
