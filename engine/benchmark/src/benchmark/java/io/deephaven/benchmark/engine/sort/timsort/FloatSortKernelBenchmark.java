/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSortKernelBenchmark and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.benchmark.engine.sort.timsort;

import io.deephaven.engine.table.impl.sort.timsort.BaseTestFloatTimSortKernel;
import io.deephaven.engine.table.impl.sort.timsort.TestTimSortKernel;
import io.deephaven.tuple.generated.FloatLongTuple;
import org.openjdk.jmh.annotations.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class FloatSortKernelBenchmark {
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

        final TestTimSortKernel.GenerateTupleList<FloatLongTuple> generate;
        switch (runType) {
            case "random":
                generate = BaseTestFloatTimSortKernel::generateFloatRandom;
                break;
            case "runs":
                generate = BaseTestFloatTimSortKernel::generateFloatRuns;
                break;
            case "ascending":
                generate = BaseTestFloatTimSortKernel::generateAscendingFloatRuns;
                break;
            case "descending":
                generate = BaseTestFloatTimSortKernel::generateDescendingFloatRuns;
                break;
            default:
                throw new IllegalArgumentException("Bad runType: " + runType);
        }

        // i would prefer to update the seed here
        final Random random = new Random(0);
        final List<FloatLongTuple> stuffToSort = generate.generate(random, chunkSize);

        switch (algorithm) {
            case "java":
                final Comparator<FloatLongTuple> javaComparator = BaseTestFloatTimSortKernel.getJavaComparator();
                doSort = () -> stuffToSort.sort(javaComparator);
                break;
            case "javaarray":
                final float [] javaArray = new float[stuffToSort.size()];
                for (int ii = 0; ii < javaArray.length; ++ii) {
                    javaArray[ii] = stuffToSort.get(ii).getFirstElement();
                }
                doSort = () -> Arrays.sort(javaArray);
                break;
            case "timsort":
                final BaseTestFloatTimSortKernel.FloatSortKernelStuff sortStuff = new BaseTestFloatTimSortKernel.FloatSortKernelStuff(stuffToSort);
                doSort = sortStuff::run;
                break;
            case "mergesort":
                final BaseTestFloatTimSortKernel.FloatMergeStuff mergeStuff = new BaseTestFloatTimSortKernel.FloatMergeStuff(stuffToSort);
                doSort = mergeStuff::run;
                break;
        }

    }

    @Benchmark
    public void sort() {
        doSort.run();
    }
}