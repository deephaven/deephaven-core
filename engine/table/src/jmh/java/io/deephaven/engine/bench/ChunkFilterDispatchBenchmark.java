//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.bench;

import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.chunkfilter.IntChunkMatchFilterFactory;
import io.deephaven.engine.table.impl.chunkfilter.IntRangeComparator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * The cost per cell of an int {@link ChunkFilter}, when the filter kernels have or have not already run other filter
 * classes.
 *
 * <p>
 * Every filter is called through the same helper methods, standing in for the engine's callers (for example
 * {@code ChunkFilterApplier} and {@code CountWhereOperator}), which see every filter type at one call site. With
 * {@code pollute}, the setup first runs a mix of other int filters through those helpers, as a long-running server
 * would. Each filter matches a uniformly random half of the values.
 * </p>
 *
 * <pre>
 * ./gradlew engine-table:jmhJar
 * java -jar engine/table/build/libs/deephaven-engine-table-&lt;version&gt;-jmh.jar ChunkFilterDispatchBenchmark
 * </pre>
 */
@Fork(value = 2, jvmArgs = {"-Xms2G", "-Xmx2G"})
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
public class ChunkFilterDispatchBenchmark {
    private static final int CHUNK_SIZE = 2048;
    private static final int CHUNKS = 256;
    private static final int CELLS = CHUNK_SIZE * CHUNKS;

    public enum Kind {
        /** An inclusive range covering half of [0, 1024). */
        RANGE,
        /** A one value match against values drawn from {0, 1}. */
        MATCH,
        /** A four value (set-based) match against values drawn from [0, 8). */
        SET
    }

    @Param
    public Kind kind;

    @Param({"false", "true"})
    public boolean pollute;

    private WritableIntChunk<Values>[] values;
    private WritableLongChunk<OrderedRowKeys> keys;
    private WritableLongChunk<OrderedRowKeys> keyResults;
    private WritableBooleanChunk<Values> results;
    private ChunkFilter filter;

    @Setup(Level.Trial)
    public void setup() {
        final Random random = new Random(0);
        final int bound = kind == Kind.RANGE ? 1024 : kind == Kind.MATCH ? 2 : 8;
        // noinspection unchecked
        values = new WritableIntChunk[CHUNKS];
        for (int cc = 0; cc < CHUNKS; ++cc) {
            values[cc] = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
            for (int ii = 0; ii < CHUNK_SIZE; ++ii) {
                values[cc].set(ii, random.nextInt(bound));
            }
        }
        keys = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
        for (int ii = 0; ii < CHUNK_SIZE; ++ii) {
            keys.set(ii, ii);
        }
        keyResults = WritableLongChunk.makeWritableChunk(CHUNK_SIZE);
        results = WritableBooleanChunk.makeWritableChunk(CHUNK_SIZE);

        switch (kind) {
            case RANGE:
                filter = IntRangeComparator.makeIntFilter(0, 511, true, true);
                break;
            case MATCH:
                filter = IntChunkMatchFilterFactory.makeFilter(MatchOptions.REGULAR, 1);
                break;
            case SET:
                filter = IntChunkMatchFilterFactory.makeFilter(MatchOptions.REGULAR, 0, 2, 4, 6);
                break;
            default:
                throw new IllegalStateException("Unexpected kind " + kind);
        }

        if (pollute) {
            final List<ChunkFilter> others = List.of(
                    IntRangeComparator.makeIntFilter(0, 511, false, true),
                    IntRangeComparator.makeIntFilter(0, 511, true, false),
                    IntRangeComparator.makeIntFilter(0, 511, false, false),
                    IntChunkMatchFilterFactory.makeFilter(MatchOptions.INVERTED, 1),
                    IntChunkMatchFilterFactory.makeFilter(MatchOptions.REGULAR, 1, 2),
                    IntChunkMatchFilterFactory.makeFilter(MatchOptions.REGULAR, 1, 2, 3),
                    IntChunkMatchFilterFactory.makeFilter(MatchOptions.INVERTED, 1, 2, 3, 4),
                    IntChunkMatchFilterFactory.makeFilter(MatchOptions.REGULAR,
                            IntStream.range(0, 512).toArray()));
            for (int rep = 0; rep < 200; ++rep) {
                for (final ChunkFilter other : others) {
                    for (int cc = 0; cc < CHUNKS; cc += 16) {
                        filterKeys(other, values[cc], keys, keyResults);
                        filterAndAllTrue(other, values[cc], results);
                    }
                }
            }
        }
    }

    @TearDown(Level.Trial)
    public void teardown() {
        for (final WritableIntChunk<Values> chunk : values) {
            chunk.close();
        }
        keys.close();
        keyResults.close();
        results.close();
    }

    /** The {@code where} kernel: the row keys of the matching values. */
    @Benchmark
    @OperationsPerInvocation(CELLS)
    public void filterKeys(final Blackhole bh) {
        for (int cc = 0; cc < CHUNKS; ++cc) {
            filterKeys(filter, values[cc], keys, keyResults);
            bh.consume(keyResults.size());
        }
    }

    /** {@code filterAnd} over results that start all true, so that every value is tested. */
    @Benchmark
    @OperationsPerInvocation(CELLS)
    public void filterAndAllTrue(final Blackhole bh) {
        for (int cc = 0; cc < CHUNKS; ++cc) {
            bh.consume(filterAndAllTrue(filter, values[cc], results));
        }
    }

    private static void filterKeys(
            final ChunkFilter filter,
            final WritableIntChunk<Values> values,
            final WritableLongChunk<OrderedRowKeys> keys,
            final WritableLongChunk<OrderedRowKeys> keyResults) {
        filter.filter(values, keys, keyResults);
    }

    private static int filterAndAllTrue(
            final ChunkFilter filter,
            final WritableIntChunk<Values> values,
            final WritableBooleanChunk<Values> results) {
        results.fillWithValue(0, CHUNK_SIZE, true);
        return filter.filterAnd(values, results);
    }
}
