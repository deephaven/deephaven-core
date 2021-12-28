package io.deephaven.benchmark.engine.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
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
public class RowSetIterationRaw {

    // Generate RowSet as ranges and as individual position
    // Build a RowSet for it
    // Generate 4 double arrays
    // Have chunk fetching:
    // - using RowSequence, range, direct,
    // - using fetched long chunk range/direct,
    // - iteration over array of keys/ranges with chunk get,
    // - direct iteration over array of keys/ranges
    // - iteration over RowSet by keys or by ranges
    // - Params: spars

    @Param({/* 100", "10000", */ "1000000", "10000000"})
    private int indexCount;

    // @Param({"1","2", "4", "10", "100", "10000", "100000000"})
    @Param({"1", "4", "16", "64"})
    private int avgElementsPerRange;

    // @Param({"1", "2", "4", "10", "100", "10000", "100000000"})
    // private int sparsity;

    // @Param({"256", /*"512", */"1024", /*"2048", */"4096"})
    private int chunkSize = 1024;

    long indexPoints[];
    long indexRanges[];
    RowSet rspRowSet;

    private WritableLongChunk<OrderedRowKeys> indexChunk;
    private WritableLongChunk<OrderedRowKeyRanges> rangeChunk;
    private long expectedSum;

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
        rspRowSet = RowSetFactory.fromKeys(indexPoints);

        indexChunk = WritableLongChunk.makeWritableChunk(chunkSize);
        rangeChunk = WritableLongChunk.makeWritableChunk(2 * chunkSize);
        expectedSum = 0;
        for (long indexPoint : indexPoints) {

            expectedSum += indexPoint;
        }
        System.out.println("Expected expectedSum = " + expectedSum);
    }

    private long fillChunkByRowSequenceRange(RowSequence rowSequence) {
        return fillChunkDirectByRange(rowSequence.asRowKeyRangesChunk());
    }

    private long fillChunkByRowSequenceItems(RowSequence rowSequence) {
        return fillChunkDirectByItems(rowSequence.asRowKeyChunk());
    }

    private long fillChunkDirectByRange(LongChunk<OrderedRowKeyRanges> ranges) {
        long sum = 0;
        int size = ranges.size();
        for (int i = 0; i < size; i += 2) {
            long start = ranges.get(i);
            int length = (int) (ranges.get(i + 1) - start);
            for (long j = start; j < ranges.get(i + 1); j++) {
                sum += j;
            }
        }
        return sum;
    }

    private long fillChunkDirectByItems(LongChunk<OrderedRowKeys> indices) {
        long sum = 0;
        int size = indices.size();
        for (int i = 0; i < size; i++) {
            sum += indices.get(i);
        }
        return sum;
    }

    private long fillChunkByIndexIterator(RowSet.Iterator it, int size) {
        long sum = 0;
        for (int i = 0; i < size; i++) {
            sum += it.nextLong();
        }
        return sum;
    }

    private long[] fillChunkByIndexRangeIterator(RowSet.RangeIterator it, int rangeStart, int size) {
        long sum = 0;
        int pos = 0;
        int rangeEnd = (int) it.currentRangeEnd() + 1;
        int length = rangeEnd - rangeStart;
        while (length + pos < size) {
            pos += length;
            for (long i = rangeStart; i < rangeEnd; i++) {
                sum += i;
            }
            it.next();
            rangeStart = (int) it.currentRangeStart();
            rangeEnd = (int) it.currentRangeEnd() + 1;
            length = rangeEnd - rangeStart;
        }
        length = size - pos;
        for (long i = rangeStart; i < rangeStart + length; i++) {
            sum += i;
        }
        return new long[] {rangeStart + length, sum};
    }

    private long[] fillChunkDirectByRangeIndexIteration(int posInRange, int rangeStart, int size) {
        int pos = 0;
        int rangeEnd = (int) indexRanges[posInRange + 1];
        int length = rangeEnd - rangeStart;
        long sum = 0;
        while (length + pos < size) {
            for (long i = rangeStart; i < rangeEnd; i++) {
                sum += i;
            }
            posInRange += 2;
            pos += length;
            rangeStart = (int) (int) indexRanges[posInRange];
            rangeEnd = (int) (int) indexRanges[posInRange + 1];
            length = rangeEnd - rangeStart;
        }
        length = size - pos;
        for (long i = rangeStart; i < rangeStart + length; i++) {
            sum += i;
        }
        return new long[] {posInRange, rangeStart + length, sum};
    }



    @Benchmark
    public void rspRowSequenceByRange(Blackhole bh) {
        RowSequenceByRange(bh, rspRowSet);
    }

    private void RowSequenceByRange(Blackhole bh, RowSet rowSet) {
        long sum = 0;
        final int stepCount = indexCount / chunkSize;
        final RowSequence.Iterator rsIt = rowSet.getRowSequenceIterator();
        for (int step = 0; step < stepCount; step++) {
            final RowSequence rs = rsIt.getNextRowSequenceWithLength(chunkSize);
            sum += fillChunkByRowSequenceRange(rs);
            bh.consume(sum);
        }
        final RowSequence rs = rsIt.getNextRowSequenceWithLength(chunkSize);
        sum += fillChunkByRowSequenceRange(rs);
        bh.consume(sum);
        print(sum);
    }

    boolean printed = false;

    private void print(double sum) {
        Assert.assertion(Math.abs(sum - expectedSum) < Math.abs(0.0001 * expectedSum), "sum == expectedSum");
        if (!printed) {
            System.out.println("Sum = " + sum);
            printed = true;
        }
    }

    @Benchmark
    public void rspRowSequenceByItems(Blackhole bh) {
        RowSequenceByItems(bh, rspRowSet);
    }

    public void RowSequenceByItems(Blackhole bh, RowSet rowSet) {
        long sum = 0;
        final int stepCount = indexCount / chunkSize;
        final RowSequence.Iterator rsIt = rowSet.getRowSequenceIterator();
        for (int step = 0; step < stepCount; step++) {
            final RowSequence rs = rsIt.getNextRowSequenceWithLength(chunkSize);
            sum += fillChunkByRowSequenceItems(rs);
            bh.consume(sum);
        }
        final RowSequence rs = rsIt.getNextRowSequenceWithLength(chunkSize);
        sum += fillChunkByRowSequenceItems(rs);
        bh.consume(sum);
        print(sum);
    }

    @Benchmark
    public void directByRangeIteration(Blackhole bh) {
        int stepCount = indexCount / chunkSize;
        long sum = 0;
        int lastPosInRange = 0;
        int rangeStart = (int) indexRanges[0];
        for (int step = 0; step < stepCount; step++) {
            int[] posInRangeAndRangeStart = null;
            long[] res = fillChunkDirectByRangeIndexIteration(lastPosInRange, rangeStart, chunkSize);
            lastPosInRange = (int) res[0];
            rangeStart = (int) res[1];
            sum += res[2];
            bh.consume(sum);
        }


        sum += fillChunkDirectByRangeIndexIteration(lastPosInRange, rangeStart, indexCount % chunkSize)[2];
        bh.consume(sum);
        print(sum);
    }

    @Benchmark
    public void directByIndexIteration(Blackhole bh) {
        int stepCount = indexCount / chunkSize;
        long sum = 0;
        for (int step = 0; step < stepCount; step++) {
            indexChunk = WritableLongChunk.writableChunkWrap(indexPoints, step * chunkSize, chunkSize);

            sum += fillChunkDirectByItems(indexChunk);
            bh.consume(sum);
        }
        indexChunk = WritableLongChunk.writableChunkWrap(indexPoints, (indexCount / chunkSize) * chunkSize,
                indexCount % chunkSize);
        sum += fillChunkDirectByItems(indexChunk);
        bh.consume(sum);
        print(sum);
    }

    private int fillChunkOfIndicesFromRange(WritableLongChunk<OrderedRowKeys> indices, int posInRange,
            long startValue, int count) {
        indices.setSize(0);
        long pos = startValue;
        do {
            long lastPos;
            if (indexRanges[posInRange + 1] - pos < count - indices.size()) {
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

    @Benchmark
    public void fromRangesByIndexIteration(Blackhole bh) {
        int stepCount = indexCount / chunkSize;
        long sum = 0;
        int posInRange = 0;
        long startValue = indexRanges[0];
        for (int step = 0; step < stepCount; step++) {
            posInRange = fillChunkOfIndicesFromRange(indexChunk, posInRange, startValue, chunkSize);
            startValue = indexChunk.get(chunkSize - 1) + 1;
            sum += fillChunkDirectByItems(indexChunk);
            bh.consume(sum);
        }
        fillChunkOfIndicesFromRange(indexChunk, posInRange, startValue, indexCount % chunkSize);
        sum += fillChunkDirectByItems(indexChunk);
        bh.consume(sum);
        print(sum);
    }

    private void fillChunkOfRangesFromIndices(WritableLongChunk<OrderedRowKeyRanges> ranges,
            final int posInIndex,
            final int count) {
        ranges.setSize(0);
        ranges.add(indexPoints[posInIndex]);
        long prevValue = indexPoints[posInIndex];
        for (int i = posInIndex + 1; i < posInIndex + count; i++) {
            if (prevValue != indexPoints[i] - 1) {
                ranges.add(prevValue + 1);
                if (i < posInIndex + count) {
                    prevValue = indexPoints[i];
                    ranges.add(prevValue);
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

    @Benchmark
    public void fromIndicesByRangeIteration(Blackhole bh) {
        int stepCount = indexCount / chunkSize;
        long sum = 0;
        for (int step = 0; step < stepCount; step++) {
            fillChunkOfRangesFromIndices(rangeChunk, step * chunkSize, chunkSize);
            sum += fillChunkDirectByRange(rangeChunk);
            bh.consume(sum);
        }

        fillChunkOfRangesFromIndices(rangeChunk, (indexCount / chunkSize) * chunkSize, indexCount % chunkSize);
        sum += fillChunkDirectByRange(rangeChunk);

        bh.consume(sum);
        print(sum);
    }

    @Benchmark
    public void rspIndexByIndexIterator(Blackhole bh) {
        indexByIndexIterator(bh, rspRowSet);
    }

    public void indexByIndexIterator(Blackhole bh, RowSet rowSet) {
        long sum = 0;
        int stepCount = indexCount / chunkSize;
        RowSet.Iterator it = rowSet.iterator();


        for (int step = 0; step < stepCount; step++) {
            sum += fillChunkByIndexIterator(it, chunkSize);
            bh.consume(sum);
        }
        sum += fillChunkByIndexIterator(it, indexCount % chunkSize);
        bh.consume(sum);
        print(sum);
    }

    @Benchmark
    public void rspIndexByIndexRangeIterator(Blackhole bh) {
        indexByIndexRangeIterator(bh, rspRowSet);
    }

    public void indexByIndexRangeIterator(Blackhole bh, RowSet rowSet) {
        long sum = 0;
        int stepCount = indexCount / chunkSize;
        RowSet.RangeIterator it = rowSet.rangeIterator();
        int rangeStart = (int) it.currentRangeStart();
        for (int step = 0; step < stepCount; step++) {

            long[] res = fillChunkByIndexRangeIterator(it, rangeStart, chunkSize);
            sum += res[1];
            rangeStart = (int) res[0];
            bh.consume(sum);
        }
        long[] res = fillChunkByIndexRangeIterator(it, rangeStart, indexCount % chunkSize);
        sum += res[1];
        bh.consume(sum);
        print(sum);
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(RowSetIterationRaw.class);
    }
}
