package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyRanges;
import io.deephaven.benchmarking.BenchUtil;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 1)
@Fork(1)
public class IndexIteration {

    // @Param({/*100", "10000",*/ "1000000"/*, "10000000"*/})
    private int indexCount = 1000000;

    // @Param({"1","2", "4", "10", "100", "10000", "100000000"})
    @Param({"1", "4", "16", "64", "256"})
    private int avgElementsPerRange;

    // @Param({"1", "2", "4", "10", "100", "10000", "100000000"})
    // private int sparsity;

    @Param({"1", "2", "4", "16"})
    private int setsCount;

    // @Param({"256", /*"512", */"1024", /*"2048", */"4096"})
    private int chunkSize = 1024;

    long indexPoints[];
    long indexRanges[];
    Index index;

    double sets[][];
    WritableDoubleChunk chunks[];
    private WritableLongChunk indexChunk;
    private WritableLongChunk rangeChunk;
    private WritableDoubleChunk result;
    private double expectedSum;

    @Setup(Level.Trial)
    public void setupEnv() {

        Random random = new Random(0);
        indexPoints = new long[indexCount];
        int rangeCount = Math.max(1, (indexCount + avgElementsPerRange / 2) / avgElementsPerRange);
        indexRanges = new long[rangeCount * 2];
        long lastPos = 0;
        int remainingCount = indexCount;
        int j = 0;
        for (int i = 0; i < rangeCount - 1; i++) {
            indexRanges[2 * i] = lastPos + 1 + random.nextInt(2 * avgElementsPerRange - 1);
            int step =
                    1 + Math.max(0, Math.min(random.nextInt(2 * avgElementsPerRange - 1), remainingCount - rangeCount));
            lastPos = indexRanges[2 * i + 1] = indexRanges[2 * i] + step;
            remainingCount -= step;
            indexPoints[j++] = indexRanges[2 * i];
            for (int k = 1; k < step; k++) {
                indexPoints[j] = indexPoints[j - 1] + 1;
                j++;
            }
        }
        indexRanges[2 * rangeCount - 2] = lastPos + random.nextInt(2 * avgElementsPerRange);
        indexRanges[2 * rangeCount - 1] = indexRanges[2 * rangeCount - 2] + remainingCount;
        indexPoints[j++] = indexRanges[2 * rangeCount - 2];
        for (int k = 1; k < remainingCount; k++) {
            indexPoints[j] = indexPoints[j - 1] + 1;
            j++;
        }
        index = Index.FACTORY.getIndexByValues(indexPoints);

        sets = new double[setsCount][];

        for (int i = 0; i < sets.length; i++) {
            sets[i] = new double[(int) indexRanges[indexRanges.length - 1]];
            for (int k = 0; k < sets[i].length; k++) {
                sets[i][k] = random.nextDouble();
            }
        }

        chunks = new WritableDoubleChunk[setsCount];
        for (int i = 0; i < chunks.length; i++) {
            chunks[i] = WritableDoubleChunk.makeWritableChunk(chunkSize);
        }
        indexChunk = WritableLongChunk.makeWritableChunk(chunkSize);
        rangeChunk = WritableLongChunk.makeWritableChunk(2 * chunkSize);
        result = WritableDoubleChunk.makeWritableChunk(chunkSize);
        expectedSum = 0.0;
        for (long indexPoint : indexPoints) {
            for (double[] set : sets) {
                expectedSum += set[(int) indexPoint];
            }
        }
        System.out.println("Expected expectedSum = " + expectedSum);
    }

    private int fillChunkOfIndicesFromRange(WritableLongChunk indices, int posInRange, long startValue, int count) {
        indices.setSize(0);
        long pos = startValue;
        do {
            long lastPos;
            if (indexRanges[posInRange + 1] - pos <= count - indices.size()) {
                lastPos = indexRanges[posInRange + 1];
                while (pos < lastPos) {
                    indices.add(pos++);
                }
                posInRange += 2;
                pos = indexRanges[posInRange];
            } else {
                lastPos = count - indices.size() + pos;
                while (pos < lastPos) {
                    indices.add(pos++);
                }
                break;
            }
        } while (true);
        return posInRange;
    }

    private void fillChunkOfRangesFromIndices(WritableLongChunk ranges, final int posInIndex, final int count) {
        ranges.setSize(0);
        ranges.add(indexPoints[posInIndex]);
        long prevValue = indexPoints[posInIndex];
        for (int i = posInIndex + 1; i < posInIndex + count; i++) {
            if (prevValue != indexPoints[i] - 1) {
                ranges.add(prevValue + 1);
                if (i < posInIndex + count) {
                    prevValue = indexPoints[i];
                    ranges.add(prevValue);
                    i++;
                } else {
                    ranges.add(indexPoints[i]);
                    ranges.add(indexPoints[i] + 1);
                    return;
                }
            } else {
                prevValue++;
            }
        }
        ranges.add(prevValue + 1);
    }

    private void fillChunkByOrderedKeysRange(OrderedKeys orderedKeys, WritableDoubleChunk doubleChunk, int sourceId) {
        fillChunkDirectByRange(orderedKeys.asKeyRangesChunk(), doubleChunk, sourceId);
    }

    private void fillChunkByOrderedKeyItems(OrderedKeys orderedKeys, WritableDoubleChunk doubleChunk, int sourceId) {
        fillChunkDirectByItems(orderedKeys.asKeyIndicesChunk(), doubleChunk, sourceId);
    }

    private void fillChunkDirectByRange(LongChunk<OrderedKeyRanges> ranges, WritableDoubleChunk doubleChunk,
            int sourceId) {
        int pos = 0;
        final int size = ranges.size();
        for (int i = 0; i < size; i += 2) {
            final long start = ranges.get(i);
            final int length = (int) (ranges.get(i + 1) - start) + 1;
            doubleChunk.copyFromArray(sets[sourceId], (int) start, pos, length);
            pos += length;
        }
        doubleChunk.setSize(pos);
    }

    private void fillChunkDirectByItems(LongChunk<OrderedKeyIndices> indices, WritableDoubleChunk doubleChunk,
            int sourceId) {
        final int size = indices.size();
        doubleChunk.setSize(0);
        for (int i = 0; i < size; i++) {
            doubleChunk.add(sets[sourceId][(int) indices.get(i)]);
        }
    }

    private void fillChunkByIndexIterator(Index.Iterator it, int size, WritableDoubleChunk doubleChunk, int sourceId) {
        doubleChunk.setSize(0);
        for (int i = 0; i < size; i++) {
            doubleChunk.add(sets[sourceId][(int) it.nextLong()]);
        }
    }

    private int fillChunkByIndexRangeIterator(Index.RangeIterator it, int rangeStart, int size,
            WritableDoubleChunk doubleChunk, int sourceId) {
        int pos = 0;
        int rangeEnd = (int) it.currentRangeEnd() + 1;
        int length = rangeEnd - rangeStart;
        while (length + pos < size) {
            doubleChunk.copyFromArray(sets[sourceId], rangeStart, pos, length);
            pos += length;
            it.next();
            rangeStart = (int) it.currentRangeStart();
            rangeEnd = (int) it.currentRangeEnd() + 1;
            length = rangeEnd - rangeStart;
        }
        length = size - pos;
        doubleChunk.copyFromArray(sets[sourceId], rangeStart, pos, length);
        doubleChunk.setSize(pos + length);
        return rangeStart + length;
    }

    private int[] fillChunkDirectByRangeIndexIteration(int posInRange, int rangeStart, int size,
            WritableDoubleChunk doubleChunk, int sourceId) {
        int pos = 0;
        int rangeEnd = (int) indexRanges[posInRange + 1];
        int length = rangeEnd - rangeStart;
        while (length + pos < size) {
            doubleChunk.copyFromArray(sets[sourceId], rangeStart, pos, length);
            posInRange += 2;
            pos += length;
            rangeStart = (int) (int) indexRanges[posInRange];
            rangeEnd = (int) (int) indexRanges[posInRange + 1];
            length = rangeEnd - rangeStart;
        }
        length = size - pos;
        doubleChunk.copyFromArray(sets[sourceId], rangeStart, pos, length);
        doubleChunk.setSize(pos + length);
        return new int[] {posInRange, rangeStart + length};
    }


    private void evaluate(WritableDoubleChunk result, WritableDoubleChunk... inputChunks) {
        int size = inputChunks[0].size();
        result.setSize(0);
        for (int i = 0; i < size; i++) {
            double sum = 0;
            for (int j = 0; j < inputChunks.length; j++) {
                sum += inputChunks[j].get(i);
            }
            result.add(sum);
        }
        result.setSize(size);
    }

    @Benchmark
    public void orderedKeysByRange(Blackhole bh) {
        double sum = 0;
        final int stepCount = indexCount / chunkSize;
        final OrderedKeys.Iterator okit = index.getOrderedKeysIterator();

        for (int step = 0; step < stepCount; step++) {
            final OrderedKeys ok = okit.getNextOrderedKeysWithLength(chunkSize);
            for (int i = 0; i < chunks.length; i++) {
                fillChunkByOrderedKeysRange(ok, chunks[i], i);
            }
            evaluate(result, chunks);
            bh.consume(result);
            sum = sum(sum);
        }
        final OrderedKeys ok = okit.getNextOrderedKeysWithLength(chunkSize);
        for (int i = 0; i < chunks.length; i++) {
            fillChunkByOrderedKeysRange(ok, chunks[i], i);
        }
        evaluate(result, chunks);
        sum = sum(sum);
        bh.consume(result);
        print(sum);
    }

    boolean printed = false;

    private void print(double sum) {
        /*
         * Assert.assertEquals(sum, expectedSum, 0.0001 * expectedSum); if (!printed) { System.out.println("Sum = " +
         * sum); printed = true; }
         */
    }

    @Benchmark
    public void orderedKeysByItems(Blackhole bh) {
        double sum = 0;
        final int stepCount = indexCount / chunkSize;
        final OrderedKeys.Iterator okit = index.getOrderedKeysIterator();
        for (int step = 0; step < stepCount; step++) {
            final OrderedKeys ok = okit.getNextOrderedKeysWithLength(chunkSize);
            for (int i = 0; i < chunks.length; i++) {
                fillChunkByOrderedKeyItems(ok, chunks[i], i);
            }
            evaluate(result, chunks);
            sum = sum(sum);
            bh.consume(result);
        }
        final OrderedKeys ok = okit.getNextOrderedKeysWithLength(chunkSize);
        for (int i = 0; i < chunks.length; i++) {
            fillChunkByOrderedKeyItems(ok, chunks[i], i);
        }
        evaluate(result, chunks);
        sum = sum(sum);
        bh.consume(result);
        print(sum);
    }

    @Benchmark
    public void directByRangeIteration(Blackhole bh) {
        int stepCount = indexCount / chunkSize;
        double sum = 0;
        int lastPosInRange = 0;
        int rangeStart = (int) indexRanges[0];
        for (int step = 0; step < stepCount; step++) {
            int[] posInRangeAndRangeStart = null;
            for (int i = 0; i < chunks.length; i++) {
                posInRangeAndRangeStart =
                        fillChunkDirectByRangeIndexIteration(lastPosInRange, rangeStart, chunkSize, chunks[i], i);
            }
            lastPosInRange = posInRangeAndRangeStart[0];
            rangeStart = posInRangeAndRangeStart[1];
            evaluate(result, chunks);
            sum = sum(sum);
            bh.consume(result);
        }

        for (int i = 0; i < chunks.length; i++) {
            fillChunkDirectByRangeIndexIteration(lastPosInRange, rangeStart, indexCount % chunkSize, chunks[i], i);
        }
        evaluate(result, chunks);
        sum = sum(sum);
        bh.consume(result);
        print(sum);
    }

    @Benchmark
    public void directByIndexIteration(Blackhole bh) {
        int stepCount = indexCount / chunkSize;
        double sum = 0;
        for (int step = 0; step < stepCount; step++) {
            indexChunk = WritableLongChunk.writableChunkWrap(indexPoints, step * chunkSize, chunkSize);
            for (int i = 0; i < chunks.length; i++) {
                fillChunkDirectByItems(indexChunk, chunks[i], i);
            }
            evaluate(result, chunks);
            sum = sum(sum);
            bh.consume(result);
        }
        indexChunk = WritableLongChunk.writableChunkWrap(indexPoints, (indexCount / chunkSize) * chunkSize,
                indexCount % chunkSize);
        for (int i = 0; i < chunks.length; i++) {
            fillChunkDirectByItems(indexChunk, chunks[i], i);
        }
        evaluate(result, chunks);
        sum = sum(sum);
        bh.consume(result);
        print(sum);
    }

    @Benchmark
    public void indexByIndexIterator(Blackhole bh) {
        double sum = 0;
        int stepCount = indexCount / chunkSize;
        Index.Iterator its[] = new Index.Iterator[sets.length];
        for (int i = 0; i < its.length; i++) {
            its[i] = index.iterator();

        }
        for (int step = 0; step < stepCount; step++) {
            for (int i = 0; i < chunks.length; i++) {
                fillChunkByIndexIterator(its[i], chunkSize, chunks[i], i);
            }
            evaluate(result, chunks);
            sum = sum(sum);
            bh.consume(result);
        }
        for (int i = 0; i < chunks.length; i++) {
            fillChunkByIndexIterator(its[i], indexCount % chunkSize, chunks[i], i);
        }
        evaluate(result, chunks);
        sum = sum(sum);
        bh.consume(result);
        print(sum);
    }

    @Benchmark
    public void indexByIndexRangeIterator(Blackhole bh) {
        double sum = 0;
        int stepCount = indexCount / chunkSize;
        Index.RangeIterator its[] = new Index.RangeIterator[sets.length];

        for (int i = 0; i < its.length; i++) {
            its[i] = index.rangeIterator();
            its[i].next();
        }
        int rangeStart = (int) its[0].currentRangeStart();
        for (int step = 0; step < stepCount; step++) {
            int nextRangeStart = rangeStart;
            for (int i = 0; i < chunks.length; i++) {
                nextRangeStart = fillChunkByIndexRangeIterator(its[i], rangeStart, chunkSize, chunks[i], i);
            }
            rangeStart = nextRangeStart;
            evaluate(result, chunks);
            sum = sum(sum);
            bh.consume(result);
        }
        int nextRangeStart = rangeStart;
        for (int i = 0; i < chunks.length; i++) {
            nextRangeStart = fillChunkByIndexRangeIterator(its[i], rangeStart, indexCount % chunkSize, chunks[i], i);
        }
        evaluate(result, chunks);
        sum = sum(sum);
        bh.consume(result);
        print(sum);
    }
    // private void fillChunkByIndexIterator(Index.Iterator it, int size, DoubleChunk doubleChunk, int sourceId) {
    // private int fillChunkByIndexRangeIterator(Index.RangeIterator it, int rangeStart, int size, DoubleChunk
    // doubleChunk, int sourceId) {

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(IndexIteration.class);
    }

    double sum(double prevSum) {
        /*
         * for (int i = 0; i < result.size(); i++) { prevSum += result.get(i); } return prevSum;
         */
        return 0;
    }
}
