/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.benchmark.engine.util;

import gnu.trove.list.array.TLongArrayList;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import org.openjdk.jmh.annotations.*;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(1)
public class RowSetBuilderBenchmark {
    @Param({"1000000"})
    private int keysToInsert;

    @Param({"5", "50", "95"})
    private int percentRanges;

    private final TLongArrayList rStart = new TLongArrayList();
    private final TLongArrayList rEnd = new TLongArrayList();

    int[] insertOrder;

    @Setup(Level.Trial)
    public void setupTrial() {
        final Random randy = new Random(keysToInsert);
        long lastKey = 0;
        for (int rowCount = 0; rowCount < keysToInsert;) {
            boolean insertRange = randy.nextInt(100) < percentRanges;
            if (insertRange) {
                final long rs = randy.nextInt(100) + lastKey;
                final long re = randy.nextInt(100) + rs;
                rStart.add(rs);
                rEnd.add(re);
                lastKey = re + 1;
                rowCount += re - rs + 1;
            } else {
                final long key = randy.nextInt(100) + lastKey;
                rStart.add(key);
                rEnd.add(key);
                lastKey = key + 1;
                rowCount++;
            }
        }

        insertOrder = IntStream.range(0, rStart.size()).toArray();
        shuffle(insertOrder, randy);
    }

    private void shuffle(int[] array, Random random) {
        int index;
        for (int i = array.length - 1; i > 0; i--) {
            index = random.nextInt(i + 1);
            if (index != i) {
                array[index] ^= array[i];
                array[i] ^= array[index];
                array[index] ^= array[i];
            }
        }
    }

    @Benchmark
    public void insertSequentialStandard() {
        final RowSetBuilderRandom rb = RowSetFactory.builderRandom();
        for (int ii = 0; ii < rStart.size(); ii++) {
            rb.addRange(rStart.get(ii), rEnd.get(ii));
        }
    }

    @Benchmark
    public void insertSequentialInsertOnly() {
        try (final WritableRowSet acc = RowSetFactory.empty()) {
            for (int ii = 0; ii < rStart.size(); ii++) {
                acc.insertRange(rStart.get(ii), rEnd.get(ii));
            }
        }
    }

    @Benchmark
    public void insertRandomStandard() {
        final RowSetBuilderRandom rb = RowSetFactory.builderRandom();
        for (int ii = 0; ii < rStart.size(); ii++) {
            rb.addRange(rStart.get(insertOrder[ii]), rEnd.get(insertOrder[ii]));
        }
    }

    @Benchmark
    public void insertRandomInsertOnly() {
        try (final WritableRowSet acc = RowSetFactory.empty()) {
            for (int ii = 0; ii < rStart.size(); ii++) {
                acc.insertRange(rStart.get(insertOrder[ii]), rEnd.get(insertOrder[ii]));
            }
        }
    }
}
