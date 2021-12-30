/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharTimSortKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sort.timsort;

import io.deephaven.test.types.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelTest.class)
public class TestShortTimSortKernel extends BaseTestShortTimSortKernel {
    // I like this output, but for now am leaving these tests off, so we can focus on getting right answers and we can try
    // out JMH for running morally equivalent things.

//    @Test
//    public void shortRandomPerformanceTest() {
//        performanceTest(TestShortTimSortKernel::generateShortRandom, ShortSortKernelStuff::new, ShortSortKernelStuff::run, getJavaComparator(), ShortMergeStuff::new, ShortMergeStuff::run);
//    }
//
//    @Test
//    public void shortRunPerformanceTest() {
//        performanceTest(TestShortTimSortKernel::generateShortRuns, ShortSortKernelStuff::new, ShortSortKernelStuff::run, getJavaComparator(), ShortMergeStuff::new, ShortMergeStuff::run);
//    }
//
//    @Test
//    public void shortRunDescendingPerformanceTest() {
//        performanceTest(TestShortTimSortKernel::generateDescendingShortRuns, ShortSortKernelStuff::new, ShortSortKernelStuff::run, getJavaComparator(), ShortMergeStuff::new, ShortMergeStuff::run);
//    }
//
//    @Test
//    public void shortRunAscendingPerformanceTest() {
//        performanceTest(TestShortTimSortKernel::generateAscendingShortRuns, ShortSortKernelStuff::new, ShortSortKernelStuff::run, getJavaComparator(), ShortMergeStuff::new, ShortMergeStuff::run);
//    }
//
    @Test
    public void shortRandomCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestShortTimSortKernel::generateShortRandom, getJavaComparator(), ShortSortKernelStuff::new);
        }
    }

    @Test
    public void shortRandomPartitionCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_PARTTITION_CHUNK_SIZE; size *= 2) {
            int partitions = 2;
            while (partitions < (int)Math.sqrt(size)) {
                partitionCorrectnessTest(size, size, partitions, TestShortTimSortKernel::generateShortRandom, getJavaComparator(), ShortPartitionKernelStuff::new);
                if (size < 1000) {
                    break;
                }
                partitions *= 3;
            }
        }
    }

    @Test
    public void shortAscendingRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestShortTimSortKernel::generateAscendingShortRuns, getJavaComparator(), ShortSortKernelStuff::new);
        }
    }

    @Test
    public void shortDescendingRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestShortTimSortKernel::generateDescendingShortRuns, getJavaComparator(), ShortSortKernelStuff::new);
        }
    }

    @Test
    public void shortRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestShortTimSortKernel::generateShortRuns, getJavaComparator(), ShortSortKernelStuff::new);
        }
    }

    @Test
    public void shortMultiRandomCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            multiCorrectnessTest(size, TestShortTimSortKernel::generateMultiShortRandom, getJavaMultiComparator(), ShortMultiSortKernelStuff::new);
        }
    }
}
