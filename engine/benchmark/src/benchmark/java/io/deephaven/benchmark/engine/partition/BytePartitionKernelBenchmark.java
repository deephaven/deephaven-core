//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharPartitionKernelBenchmark and run "./gradlew replicateSortKernelTests" to regenerate
//
// @formatter:off
package io.deephaven.benchmark.engine.partition;

import io.deephaven.tuple.generated.ByteLongTuple;
import io.deephaven.engine.table.impl.sort.timsort.BaseTestByteTimSortKernel;
import io.deephaven.engine.table.impl.sort.timsort.TestTimSortKernel;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import org.openjdk.jmh.annotations.*;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class BytePartitionKernelBenchmark {
    @Param({"32"})
    private int chunkSize;

    @Param({"128"})
    private int dataSize;

    @Param({"random", "runs", "ascending", "descending"})
    private String runType;

    @Param({"true", "false"})
    private boolean preserveEquality;

    @Param({"sqrt", "2", "8", "1024"})
    private String numPartitions;

    private Runnable doPartition;

    @TearDown(Level.Trial)
    public void finishTrial() {}

    @Setup(Level.Iteration)
    public void setupIteration() {
        System.out.println("Size = " + chunkSize);

        final TestTimSortKernel.GenerateTupleList<ByteLongTuple> generate;
        switch (runType) {
            case "random":
                generate = BaseTestByteTimSortKernel::generateByteRandom;
                break;
            case "runs":
                generate = BaseTestByteTimSortKernel::generateByteRuns;
                break;
            case "ascending":
                generate = BaseTestByteTimSortKernel::generateAscendingByteRuns;
                break;
            case "descending":
                generate = BaseTestByteTimSortKernel::generateDescendingByteRuns;
                break;
            default:
                throw new IllegalArgumentException("Bad runType: " + runType);
        }

        // i would prefer to update the seed here
        final Random random = new Random(0);
        final List<ByteLongTuple> stuffToSort = generate.generate(random, dataSize);

        final RowSetBuilderSequential sequentialBuilder = RowSetFactory.builderSequential();
        stuffToSort.stream().mapToLong(ByteLongTuple::getSecondElement).forEach(sequentialBuilder::appendKey);
        final RowSet rowSet = sequentialBuilder.build();
        final int numPartitionsValue;
        if ("sqrt".equals(numPartitions)) {
            numPartitionsValue = (int) Math.sqrt(stuffToSort.size());
        } else {
            numPartitionsValue = Integer.parseInt(numPartitions);
        }
        final BaseTestByteTimSortKernel.BytePartitionKernelStuff partitionStuff =
                new BaseTestByteTimSortKernel.BytePartitionKernelStuff(stuffToSort, rowSet, chunkSize,
                        numPartitionsValue, preserveEquality);
        doPartition = partitionStuff::run;
    }

    @Benchmark
    public void partition() {
        doPartition.run();
    }
}
