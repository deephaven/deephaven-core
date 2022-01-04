/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharPartitionKernelBenchmark and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.benchmark.engine.partition;

import io.deephaven.tuple.generated.LongLongTuple;
import io.deephaven.engine.table.impl.sort.timsort.BaseTestLongTimSortKernel;
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
public class LongPartitionKernelBenchmark {
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
    public void finishTrial() {
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
        System.out.println("Size = " + chunkSize);

        final TestTimSortKernel.GenerateTupleList<LongLongTuple> generate;
        switch (runType) {
            case "random":
                generate = BaseTestLongTimSortKernel::generateLongRandom;
                break;
            case "runs":
                generate = BaseTestLongTimSortKernel::generateLongRuns;
                break;
            case "ascending":
                generate = BaseTestLongTimSortKernel::generateAscendingLongRuns;
                break;
            case "descending":
                generate = BaseTestLongTimSortKernel::generateDescendingLongRuns;
                break;
            default:
                throw new IllegalArgumentException("Bad runType: " + runType);
        }

        // i would prefer to update the seed here
        final Random random = new Random(0);
        final List<LongLongTuple> stuffToSort = generate.generate(random, dataSize);

        final RowSetBuilderSequential sequentialBuilder = RowSetFactory.builderSequential();
        stuffToSort.stream().mapToLong(LongLongTuple::getSecondElement).forEach(sequentialBuilder::appendKey);
        final RowSet rowSet = sequentialBuilder.build();
        final int numPartitionsValue;
        if ("sqrt".equals(numPartitions)) {
            numPartitionsValue = (int) Math.sqrt(stuffToSort.size());
        } else {
            numPartitionsValue = Integer.parseInt(numPartitions);
        }
        final BaseTestLongTimSortKernel.LongPartitionKernelStuff partitionStuff = new BaseTestLongTimSortKernel.LongPartitionKernelStuff(stuffToSort, rowSet, chunkSize, numPartitionsValue, preserveEquality);
        doPartition = partitionStuff::run;
    }

    @Benchmark
    public void partition() {
        doPartition.run();
    }
}