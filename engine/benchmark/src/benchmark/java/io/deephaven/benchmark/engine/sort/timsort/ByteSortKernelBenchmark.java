/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSortKernelBenchmark and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.benchmark.engine.sort.timsort;

import io.deephaven.engine.table.impl.sort.timsort.BaseTestByteTimSortKernel;
import io.deephaven.engine.table.impl.sort.timsort.TestTimSortKernel;
import io.deephaven.tuple.generated.ByteLongTuple;
import org.openjdk.jmh.annotations.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class ByteSortKernelBenchmark {
    @Param({"32"})
    private int chunkSize;

    @Param({"random", "runs", "ascending", "descending"})
    private String runType;

    @Param({"java", "javaarray", "timsort", "mergesort"})
    private String algorithm;

    private Runnable doSort;

    @TearDown(Level.Trial)
    public void finishTrial() {
    }

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
        final List<ByteLongTuple> stuffToSort = generate.generate(random, chunkSize);

        switch (algorithm) {
            case "java":
                final Comparator<ByteLongTuple> javaComparator = BaseTestByteTimSortKernel.getJavaComparator();
                doSort = () -> stuffToSort.sort(javaComparator);
                break;
            case "javaarray":
                final byte [] javaArray = new byte[stuffToSort.size()];
                for (int ii = 0; ii < javaArray.length; ++ii) {
                    javaArray[ii] = stuffToSort.get(ii).getFirstElement();
                }
                doSort = () -> Arrays.sort(javaArray);
                break;
            case "timsort":
                final BaseTestByteTimSortKernel.ByteSortKernelStuff sortStuff = new BaseTestByteTimSortKernel.ByteSortKernelStuff(stuffToSort);
                doSort = sortStuff::run;
                break;
            case "mergesort":
                final BaseTestByteTimSortKernel.ByteMergeStuff mergeStuff = new BaseTestByteTimSortKernel.ByteMergeStuff(stuffToSort);
                doSort = mergeStuff::run;
                break;
        }

    }

    @Benchmark
    public void sort() {
        doSort.run();
    }
}