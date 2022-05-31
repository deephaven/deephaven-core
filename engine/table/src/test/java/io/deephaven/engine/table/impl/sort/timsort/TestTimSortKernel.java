package io.deephaven.engine.table.impl.sort.timsort;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;

import io.deephaven.engine.rowset.impl.PerfStats;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public abstract class TestTimSortKernel {
    static final int MAX_CHUNK_SIZE = 1 << 20;
    static final int MAX_PARTTITION_CHUNK_SIZE = 1 << 16;
    private static final int INITIAL_PERFORMANCE_CHUNK_SIZE = 1 << 8;
    static final int INITIAL_CORRECTNESS_SIZE = 23;
    private static final int PERFORMANCE_SEEDS = 10;
    private static final int CORRECTNESS_SEEDS = 10;

    @FunctionalInterface
    public interface GenerateTupleList<T> {
        List<T> generate(Random random, int size);
    }

    abstract static class MergeStuff<T> {
        final long[] posarray;
        final long[] posarray2;

        MergeStuff(List<T> javaTuples) {
            posarray = LongStream.range(0, javaTuples.size()).toArray();
            posarray2 = LongStream.range(0, javaTuples.size()).toArray();
        }

        abstract void run();
    }

    abstract static class SortKernelStuff<T> {
        final WritableLongChunk<RowKeys> rowKeys;

        SortKernelStuff(int size) {
            rowKeys = WritableLongChunk.makeWritableChunk(size);
        }

        abstract void run();

        abstract void check(List<T> expected);
    }

    abstract static class PartitionKernelStuff<T> {
        final WritableLongChunk<RowKeys> rowKeys;

        PartitionKernelStuff(int size) {
            rowKeys = WritableLongChunk.makeWritableChunk(size);
        }

        abstract void run();

        abstract void check(List<T> expected);
    }

    abstract static class SortMultiKernelStuff<T> extends SortKernelStuff<T> {
        final WritableIntChunk<ChunkPositions> offsets;
        final WritableIntChunk<ChunkLengths> lengths;
        final WritableIntChunk<ChunkPositions> offsetsOut;
        final WritableIntChunk<ChunkLengths> lengthsOut;

        SortMultiKernelStuff(int size) {
            super(size);
            offsets = WritableIntChunk.makeWritableChunk(1);
            offsets.set(0, 0);
            lengths = WritableIntChunk.makeWritableChunk(1);
            lengths.set(0, size);

            offsetsOut = WritableIntChunk.makeWritableChunk(size);
            lengthsOut = WritableIntChunk.makeWritableChunk(size);

            offsetsOut.setSize(0);
            lengthsOut.setSize(0);
        }

        abstract void run();

        abstract void check(List<T> expected);
    }

    <T, K, M> void performanceTest(GenerateTupleList<T> generateValues,
            Function<List<T>, K> createKernelStuff,
            Consumer<K> runKernel,
            Comparator<T> comparator,
            Function<List<T>, M> createMergeStuff,
            Consumer<M> runMerge) {
        for (int chunkSize = INITIAL_PERFORMANCE_CHUNK_SIZE; chunkSize <= MAX_CHUNK_SIZE; chunkSize *= 2) {
            System.out.println("Size = " + chunkSize);

            final PerfStats timStats = new PerfStats(100);
            final PerfStats javaStats = new PerfStats(100);
            final PerfStats mergeStats = new PerfStats(100);

            performanceTest(chunkSize, generateValues, createKernelStuff, runKernel, timStats);
            performanceTest(chunkSize, generateValues, x -> x, x -> x.sort(comparator), javaStats);
            performanceTest(chunkSize, generateValues, createMergeStuff, runMerge, mergeStats);

            javaStats.compute();
            timStats.compute();
            mergeStats.compute();

            javaStats.print("Java", 1);
            timStats.print("TimKernel", 1);
            mergeStats.print("Merge", 1);

            PerfStats.comparePrint(timStats, "TimKernel", javaStats, "Java", "");
            PerfStats.comparePrint(mergeStats, "Merge", javaStats, "Java", "");
            PerfStats.comparePrint(timStats, "TimKernel", mergeStats, "Merge", "");

            System.out.println();
        }
    }

    private <T, R> void performanceTest(int chunkSize, GenerateTupleList<T> tupleListGenerator,
            Function<List<T>, R> prepareFunction, Consumer<R> timedFunction, @Nullable PerfStats stats) {
        for (int seed = 0; seed < PERFORMANCE_SEEDS; ++seed) {
            final Random random = new Random(seed);

            final List<T> javaTuples = tupleListGenerator.generate(random, chunkSize);

            final R sortStuff = prepareFunction.apply(javaTuples);

            final long start = System.nanoTime();
            timedFunction.accept(sortStuff);
            final long end = System.nanoTime();

            if (stats != null) {
                stats.sample(end - start);
            }
        }
    }

    <T> void correctnessTest(int size, GenerateTupleList<T> tupleListGenerator, Comparator<T> comparator,
            Function<List<T>, SortKernelStuff<T>> prepareFunction) {
        for (int seed = 0; seed < CORRECTNESS_SEEDS; ++seed) {
            System.out.println("Size = " + size + ", seed=" + seed);
            final Random random = new Random(seed);

            final List<T> javaTuples = tupleListGenerator.generate(random, size);

            final SortKernelStuff<T> sortStuff = prepareFunction.apply(javaTuples);

            javaTuples.sort(comparator);
            sortStuff.run();

            sortStuff.check(javaTuples);
        }
    }

    @FunctionalInterface
    interface PartitionKernelStuffFactory<T> {
        PartitionKernelStuff<T> apply(List<T> javaTuples, RowSet rowSet, int chunkSize, int nPartitions,
                boolean preserveEquality);
    }

    <T> void partitionCorrectnessTest(int dataSize, int chunkSize, int nPartitions,
            GenerateTupleList<T> tupleListGenerator, Comparator<T> comparator,
            PartitionKernelStuffFactory<T> prepareFunction) {
        for (int seed = 0; seed < CORRECTNESS_SEEDS; ++seed) {
            System.out.println("Size = " + dataSize + ", seed=" + seed + ", nPartitions=" + nPartitions);
            final Random random = new Random(seed);

            final List<T> javaTuples = tupleListGenerator.generate(random, dataSize);

            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            for (int ii = 0; ii < javaTuples.size(); ++ii) {
                builder.appendKey(ii * 10);
            }
            final RowSet rowSet = builder.build();

            final PartitionKernelStuff<T> partitionStuff =
                    prepareFunction.apply(javaTuples, rowSet, chunkSize, nPartitions, false);

            partitionStuff.run();

            partitionStuff.check(javaTuples);
        }
    }

    <T> void multiCorrectnessTest(int size, GenerateTupleList<T> tupleListGenerator, Comparator<T> comparator,
            Function<List<T>, SortKernelStuff<T>> prepareFunction) {
        for (int seed = 0; seed < CORRECTNESS_SEEDS; ++seed) {
            System.out.println("Size = " + size + ", seed=" + seed);
            final Random random = new Random(seed);

            final List<T> javaTuples = tupleListGenerator.generate(random, size);

            final SortKernelStuff<T> sortStuff = prepareFunction.apply(javaTuples);

            // System.out.println("Prior to sort: " + javaTuples);
            sortStuff.run();

            javaTuples.sort(comparator);
            // System.out.println("After sort: " + javaTuples);

            sortStuff.check(javaTuples);
        }
    }

    static String generateObjectValue(Random random) {
        return Integer.toHexString(random.nextInt(Integer.MAX_VALUE / 2));
    }

    static char generateCharValue(Random random) {
        return (char) ('A' + random.nextInt(26));
    }

    static byte generateByteValue(Random random) {
        return (byte) random.nextInt(Byte.MAX_VALUE);
    }

    static short generateShortValue(Random random) {
        return (short) random.nextInt(Short.MAX_VALUE);
    }

    static int generateIntValue(Random random) {
        return random.nextInt();
    }

    static long generateLongValue(Random random) {
        return random.nextLong();
    }

    static float generateFloatValue(Random random) {
        return random.nextFloat() * 10000.0f;
    }

    static double generateDoubleValue(Random random) {
        return random.nextDouble() * 100000.0;
    }


    static String incrementObjectValue(@SuppressWarnings("unused") Random unused, Object value) {
        final int intValue = Integer.parseInt((String) value, 16);
        return Integer.toHexString(intValue + 1);
    }

    static String decrementObjectValue(@SuppressWarnings("unused") Random unused, Object value) {
        final int intValue = Integer.parseInt((String) value, 16);
        return Integer.toHexString(intValue - 1);
    }

    static char incrementCharValue(@SuppressWarnings("unused") Random unused, char value) {
        return (char) (value + 1);
    }

    static char decrementCharValue(@SuppressWarnings("unused") Random unused, char value) {
        return (char) (value - 1);
    }

    static byte incrementByteValue(@SuppressWarnings("unused") Random unused, byte value) {
        return (byte) (value + 1);
    }

    static byte decrementByteValue(@SuppressWarnings("unused") Random unused, byte value) {
        return (byte) (value - 1);
    }

    static short incrementShortValue(Random random, short value) {
        return (short) (value + random.nextInt(100));
    }

    static short decrementShortValue(Random random, short value) {
        return (short) (value - random.nextInt(100));
    }

    static int incrementIntValue(Random random, int value) {
        return (value + random.nextInt(100));
    }

    static int decrementIntValue(Random random, int value) {
        return (value - random.nextInt(100));
    }

    static long incrementLongValue(Random random, long value) {
        return value + random.nextInt(100);
    }

    static long decrementLongValue(Random random, long value) {
        return value - random.nextInt(100);
    }

    static double incrementDoubleValue(Random random, double value) {
        return (value + random.nextDouble() * 100.0);
    }

    static double decrementDoubleValue(Random random, double value) {
        return (value - random.nextDouble() * 100.0);
    }

    static float incrementFloatValue(Random random, float value) {
        return (value + random.nextFloat() * 100.0f);
    }

    static float decrementFloatValue(Random random, float value) {
        return (value - random.nextFloat() * 100.0f);
    }

    void dumpKeys(LongChunk chunk) {
        System.out.println("[" + IntStream.range(0, chunk.size()).mapToObj(chunk::get).map(Object::toString)
                .collect(Collectors.joining(",")) + "]");
    }
}
