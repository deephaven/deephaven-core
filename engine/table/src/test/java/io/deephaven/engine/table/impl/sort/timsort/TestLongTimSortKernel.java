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
public class TestLongTimSortKernel extends BaseTestLongTimSortKernel {
    // I like this output, but for now am leaving these tests off, so we can focus on getting right answers and we can try
    // out JMH for running morally equivalent things.

//    @Test
//    public void longRandomPerformanceTest() {
//        performanceTest(TestLongTimSortKernel::generateLongRandom, LongSortKernelStuff::new, LongSortKernelStuff::run, getJavaComparator(), LongMergeStuff::new, LongMergeStuff::run);
//    }
//
//    @Test
//    public void longRunPerformanceTest() {
//        performanceTest(TestLongTimSortKernel::generateLongRuns, LongSortKernelStuff::new, LongSortKernelStuff::run, getJavaComparator(), LongMergeStuff::new, LongMergeStuff::run);
//    }
//
//    @Test
//    public void longRunDescendingPerformanceTest() {
//        performanceTest(TestLongTimSortKernel::generateDescendingLongRuns, LongSortKernelStuff::new, LongSortKernelStuff::run, getJavaComparator(), LongMergeStuff::new, LongMergeStuff::run);
//    }
//
//    @Test
//    public void longRunAscendingPerformanceTest() {
//        performanceTest(TestLongTimSortKernel::generateAscendingLongRuns, LongSortKernelStuff::new, LongSortKernelStuff::run, getJavaComparator(), LongMergeStuff::new, LongMergeStuff::run);
//    }
//
    @Test
    public void longRandomCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestLongTimSortKernel::generateLongRandom, getJavaComparator(), LongSortKernelStuff::new);
        }
    }

    @Test
    public void longRandomPartitionCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_PARTTITION_CHUNK_SIZE; size *= 2) {
            int partitions = 2;
            while (partitions < (int)Math.sqrt(size)) {
                partitionCorrectnessTest(size, size, partitions, TestLongTimSortKernel::generateLongRandom, getJavaComparator(), LongPartitionKernelStuff::new);
                if (size < 1000) {
                    break;
                }
                partitions *= 3;
            }
        }
    }

    @Test
    public void longAscendingRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestLongTimSortKernel::generateAscendingLongRuns, getJavaComparator(), LongSortKernelStuff::new);
        }
    }

    @Test
    public void longDescendingRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestLongTimSortKernel::generateDescendingLongRuns, getJavaComparator(), LongSortKernelStuff::new);
        }
    }

    @Test
    public void longRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestLongTimSortKernel::generateLongRuns, getJavaComparator(), LongSortKernelStuff::new);
        }
    }

    @Test
    public void longMultiRandomCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            multiCorrectnessTest(size, TestLongTimSortKernel::generateMultiLongRandom, getJavaMultiComparator(), LongMultiSortKernelStuff::new);
        }
    }
}
