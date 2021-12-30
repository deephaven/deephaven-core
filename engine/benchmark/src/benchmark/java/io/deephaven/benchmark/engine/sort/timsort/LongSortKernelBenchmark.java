/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSortKernelBenchmark and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.benchmark.engine.sort.timsort;

import io.deephaven.engine.table.impl.sort.timsort.BaseTestLongTimSortKernel;
import io.deephaven.engine.table.impl.sort.timsort.TestTimSortKernel;
import io.deephaven.tuple.generated.LongLongTuple;
import org.openjdk.jmh.annotations.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class LongSortKernelBenchmark {
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
        final List<LongLongTuple> stuffToSort = generate.generate(random, chunkSize);

        switch (algorithm) {
            case "java":
                final Comparator<LongLongTuple> javaComparator = BaseTestLongTimSortKernel.getJavaComparator();
                doSort = () -> stuffToSort.sort(javaComparator);
                break;
            case "javaarray":
                final long [] javaArray = new long[stuffToSort.size()];
                for (int ii = 0; ii < javaArray.length; ++ii) {
                    javaArray[ii] = stuffToSort.get(ii).getFirstElement();
                }
                doSort = () -> Arrays.sort(javaArray);
                break;
            case "timsort":
                final BaseTestLongTimSortKernel.LongSortKernelStuff sortStuff = new BaseTestLongTimSortKernel.LongSortKernelStuff(stuffToSort);
                doSort = sortStuff::run;
                break;
            case "mergesort":
                final BaseTestLongTimSortKernel.LongMergeStuff mergeStuff = new BaseTestLongTimSortKernel.LongMergeStuff(stuffToSort);
                doSort = mergeStuff::run;
                break;
        }

    }

    @Benchmark
    public void sort() {
        doSort.run();
    }
}