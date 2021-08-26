package io.deephaven.db.v2.utils;

import io.deephaven.benchmarking.BenchUtil;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.RunnerException;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(timeUnit = TimeUnit.MILLISECONDS, iterations = 1, time = 500)
@Measurement(timeUnit = TimeUnit.MILLISECONDS, iterations = 1, time = 500)
@Fork(1)
public class IndexCreation {

    @Param({"10000000"})
    private int indexCount;

    // @Param({"1", "4", "16", "64","256","1024","1000000"})
    @Param({"64", "10000"})
    private int avgElementsPerRange;

    // @Param({"1", "2", "4", "10", "100", "1000"})
    @Param({"1", "1000"})
    private int sparsityFactor;


    long indexPoints[];
    long indexRanges[];


    long indexPointsPooled[];
    long indexRangesPooled[];
    private long[] bmpsPool;


    @Setup(Level.Trial)
    public void setupEnv() {

        Random random = new Random(0);
        indexPointsPooled = new long[indexCount];
        indexPoints = new long[indexCount];
        int rangeCount = Math.max(1, (indexCount + avgElementsPerRange / 2) / avgElementsPerRange);
        indexRanges = new long[rangeCount * 2];
        indexRangesPooled = new long[rangeCount * 2];
        long lastPos = 0;
        int remainingCount = indexCount;
        int j = 0;
        for (int i = 0; i < rangeCount - 1; i++) {
            indexRanges[2 * i] = lastPos + 1 + random.nextInt(2 * avgElementsPerRange - 1) * sparsityFactor;
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
        bmpsPool = new long[(int) (indexPoints[indexPoints.length - 1] >> 4) + 1];

    }

    @Benchmark
    public void createRspIndexByValues(Blackhole bh) {
        Index.SequentialBuilder builder = Index.FACTORY.getSequentialBuilder();
        for (long indexPoint : indexPoints) {
            builder.appendKey(indexPoint);
        }
        bh.consume(builder.getIndex());
    }

    @Benchmark
    public void valuesToValues(Blackhole bh) {
        long values[] = new long[indexPoints.length];
        valuesToValuesInternal(bh, values, indexPoints);
    }

    @Benchmark
    public void valuesToValuesPooled(Blackhole bh) {
        valuesToValuesInternal(bh, indexPointsPooled, indexPoints);
    }

    private void valuesToValuesInternal(Blackhole bh, long[] values, long[] indexPoints) {
        int pos = 0;
        for (long indexPoint : indexPoints) {
            values[pos++] = indexPoint;
        }
        bh.consume(values);
    }


    @Benchmark
    public void valuesToRanges(Blackhole bh) {
        long ranges[] = new long[indexRanges.length];
        valuesToRangesInternal(bh, ranges);
    }

    @Benchmark
    public void valuesToRangesPooled(Blackhole bh) {
        valuesToRangesInternal(bh, indexRangesPooled);
    }

    private void valuesToRangesInternal(Blackhole bh, long[] ranges) {
        int pos = 1;
        ranges[0] = indexPoints[0];

        for (int i = 1; i < indexPoints.length; i++) {
            if (indexPoints[i] == indexPoints[i - 1] + 1) {
                ranges[pos] = indexPoints[i];
            } else {
                ranges[pos]++;
                pos++;
                ranges[pos] = indexPoints[i];
            }
        }
        bh.consume(ranges);
    }

    @Benchmark
    public void rangesToRanges(Blackhole bh) {
        long ranges[] = new long[indexRanges.length];
        valuesToValuesInternal(bh, ranges, indexRanges);
    }

    @Benchmark
    public void rangesToValues(Blackhole bh) {
        long index[] = new long[indexPoints.length];
        rangesToValueInternal(bh, index);
    }

    @Benchmark
    public void rangesToRangesPooled(Blackhole bh) {
        valuesToValuesInternal(bh, indexRangesPooled, indexRanges);
    }

    @Benchmark
    public void rangesToValuesPooled(Blackhole bh) {
        rangesToValueInternal(bh, indexPointsPooled);
    }

    private void rangesToValueInternal(Blackhole bh, long[] index) {
        int pos = 0;
        for (int i = 0; i < indexRanges.length; i += 2) {
            for (long j = indexRanges[i]; j < indexRanges[i + 1]; j++) {
                index[pos++] = j;
            }
        }
        bh.consume(index);
    }

    @Benchmark
    public void createRspIndexByRanges(Blackhole bh) {
        Index.SequentialBuilder builder = Index.FACTORY.getSequentialBuilder();
        for (int i = 0; i < indexRanges.length; i += 2) {
            builder.appendRange(indexRanges[i], indexRanges[i + 1] - 1);
        }
        bh.consume(builder.getIndex());
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(IndexCreation.class);
    }
}
