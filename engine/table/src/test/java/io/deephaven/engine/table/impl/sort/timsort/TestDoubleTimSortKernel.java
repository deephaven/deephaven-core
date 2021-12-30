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
public class TestDoubleTimSortKernel extends BaseTestDoubleTimSortKernel {
    // I like this output, but for now am leaving these tests off, so we can focus on getting right answers and we can try
    // out JMH for running morally equivalent things.

//    @Test
//    public void doubleRandomPerformanceTest() {
//        performanceTest(TestDoubleTimSortKernel::generateDoubleRandom, DoubleSortKernelStuff::new, DoubleSortKernelStuff::run, getJavaComparator(), DoubleMergeStuff::new, DoubleMergeStuff::run);
//    }
//
//    @Test
//    public void doubleRunPerformanceTest() {
//        performanceTest(TestDoubleTimSortKernel::generateDoubleRuns, DoubleSortKernelStuff::new, DoubleSortKernelStuff::run, getJavaComparator(), DoubleMergeStuff::new, DoubleMergeStuff::run);
//    }
//
//    @Test
//    public void doubleRunDescendingPerformanceTest() {
//        performanceTest(TestDoubleTimSortKernel::generateDescendingDoubleRuns, DoubleSortKernelStuff::new, DoubleSortKernelStuff::run, getJavaComparator(), DoubleMergeStuff::new, DoubleMergeStuff::run);
//    }
//
//    @Test
//    public void doubleRunAscendingPerformanceTest() {
//        performanceTest(TestDoubleTimSortKernel::generateAscendingDoubleRuns, DoubleSortKernelStuff::new, DoubleSortKernelStuff::run, getJavaComparator(), DoubleMergeStuff::new, DoubleMergeStuff::run);
//    }
//
    @Test
    public void doubleRandomCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestDoubleTimSortKernel::generateDoubleRandom, getJavaComparator(), DoubleSortKernelStuff::new);
        }
    }

    @Test
    public void doubleRandomPartitionCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_PARTTITION_CHUNK_SIZE; size *= 2) {
            int partitions = 2;
            while (partitions < (int)Math.sqrt(size)) {
                partitionCorrectnessTest(size, size, partitions, TestDoubleTimSortKernel::generateDoubleRandom, getJavaComparator(), DoublePartitionKernelStuff::new);
                if (size < 1000) {
                    break;
                }
                partitions *= 3;
            }
        }
    }

    @Test
    public void doubleAscendingRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestDoubleTimSortKernel::generateAscendingDoubleRuns, getJavaComparator(), DoubleSortKernelStuff::new);
        }
    }

    @Test
    public void doubleDescendingRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestDoubleTimSortKernel::generateDescendingDoubleRuns, getJavaComparator(), DoubleSortKernelStuff::new);
        }
    }

    @Test
    public void doubleRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestDoubleTimSortKernel::generateDoubleRuns, getJavaComparator(), DoubleSortKernelStuff::new);
        }
    }

    @Test
    public void doubleMultiRandomCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            multiCorrectnessTest(size, TestDoubleTimSortKernel::generateMultiDoubleRandom, getJavaMultiComparator(), DoubleMultiSortKernelStuff::new);
        }
    }
}
