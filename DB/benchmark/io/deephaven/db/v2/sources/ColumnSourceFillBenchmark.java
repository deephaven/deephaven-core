package io.deephaven.db.v2.sources;


import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.db.v2.utils.OrderedKeys;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.deephaven.benchmarking.CsvResultWriter;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(timeUnit = TimeUnit.NANOSECONDS, iterations = 1, time = 500000000)
@Measurement(timeUnit = TimeUnit.NANOSECONDS, iterations = 5, time = 500000000)
@Fork(1)
public class ColumnSourceFillBenchmark {

    private static final int FULL_SIZE = 1024 * 1024 * 256;

    private FillBenchmarkHelper helper;

    private WritableLongChunk<Attributes.OrderedKeyIndices> keys;
    private OrderedKeys orderedKeys;

    @Param({"long"})
    String typeName;

    @Param({"4096"})
    int fetchSize;

    @Setup(Level.Trial)
    public void setupEnv() {
        final Random random = new Random(0);

        switch (typeName) {
            case "char": {
                helper = new CharHelper(random, FULL_SIZE, fetchSize);
                break;
            }
            case "byte": {
                helper = new ByteHelper(random, FULL_SIZE, fetchSize);
                break;
            }
            case "short": {
                helper = new ShortHelper(random, FULL_SIZE, fetchSize);
                break;
            }
            case "int": {
                helper = new IntHelper(random, FULL_SIZE, fetchSize);
                break;
            }
            case "long": {
                helper = new LongHelper(random, FULL_SIZE, fetchSize);
                break;
            }
            case "float": {
                helper = new FloatHelper(random, FULL_SIZE, fetchSize);
                break;
            }
            case "double": {
                helper = new DoubleHelper(random, FULL_SIZE, fetchSize);
                break;
            }
        }

        final TIntSet seen = new TIntHashSet(fetchSize);
        int accepted = 0;
        final long[] keyArray = new long[fetchSize];
        while (accepted < fetchSize) {
            final int value = random.nextInt(FULL_SIZE);
            if (seen.add(value)) {
                keyArray[accepted++] = value;
            }
        }
        Arrays.sort(keyArray);

        keys = WritableLongChunk.writableChunkWrap(keyArray, 0, keyArray.length);
        orderedKeys = OrderedKeys.wrapKeyIndicesChunkAsOrderedKeys(keys);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        helper.release();
    }

    @Benchmark
    public void arrayBaseline(Blackhole bh) {
        helper.getFromArray(bh, fetchSize, keys);
    }

    @Benchmark
    public void fillChunkArrayBacked(Blackhole bh) {
        helper.fillFromArrayBacked(bh, fetchSize, orderedKeys);
    }

    @Benchmark
    public void fillChunkSparseArray(Blackhole bh) {
        helper.fillFromSparse(bh, fetchSize, orderedKeys);
    }

    public static void main(String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include(ColumnSourceFillBenchmark.class.getSimpleName())
                .param("typeName", "char", "byte", "short", "int", "long", "float", "double")
                .param("fetchSize",
                        IntStream.range(6, 25).filter(exp -> exp % 2 == 0).map(exp -> 1 << exp)
                                .mapToObj(Integer::toString).toArray(String[]::new))
                .jvmArgs("-Xmx8g", "-Xms8g")
                .forks(1)
                .build();

        final Collection<RunResult> results = new Runner(opt).run();
        CsvResultWriter.recordResults(results, ColumnSourceFillBenchmark.class);
    }
}
