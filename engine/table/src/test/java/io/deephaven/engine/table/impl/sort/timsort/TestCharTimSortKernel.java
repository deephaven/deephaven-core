package io.deephaven.engine.table.impl.sort.timsort;

import io.deephaven.test.types.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelTest.class)
public class TestCharTimSortKernel extends BaseTestCharTimSortKernel {
    // I like this output, but for now am leaving these tests off, so we can focus on getting right answers and we can try
    // out JMH for running morally equivalent things.

//    @Test
//    public void charRandomPerformanceTest() {
//        performanceTest(TestCharTimSortKernel::generateCharRandom, CharSortKernelStuff::new, CharSortKernelStuff::run, getJavaComparator(), CharMergeStuff::new, CharMergeStuff::run);
//    }
//
//    @Test
//    public void charRunPerformanceTest() {
//        performanceTest(TestCharTimSortKernel::generateCharRuns, CharSortKernelStuff::new, CharSortKernelStuff::run, getJavaComparator(), CharMergeStuff::new, CharMergeStuff::run);
//    }
//
//    @Test
//    public void charRunDescendingPerformanceTest() {
//        performanceTest(TestCharTimSortKernel::generateDescendingCharRuns, CharSortKernelStuff::new, CharSortKernelStuff::run, getJavaComparator(), CharMergeStuff::new, CharMergeStuff::run);
//    }
//
//    @Test
//    public void charRunAscendingPerformanceTest() {
//        performanceTest(TestCharTimSortKernel::generateAscendingCharRuns, CharSortKernelStuff::new, CharSortKernelStuff::run, getJavaComparator(), CharMergeStuff::new, CharMergeStuff::run);
//    }
//
    @Test
    public void charRandomCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestCharTimSortKernel::generateCharRandom, getJavaComparator(), CharSortKernelStuff::new);
        }
    }

    @Test
    public void charRandomPartitionCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_PARTTITION_CHUNK_SIZE; size *= 2) {
            int partitions = 2;
            while (partitions < (int)Math.sqrt(size)) {
                partitionCorrectnessTest(size, size, partitions, TestCharTimSortKernel::generateCharRandom, getJavaComparator(), CharPartitionKernelStuff::new);
                if (size < 1000) {
                    break;
                }
                partitions *= 3;
            }
        }
    }

    @Test
    public void charAscendingRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestCharTimSortKernel::generateAscendingCharRuns, getJavaComparator(), CharSortKernelStuff::new);
        }
    }

    @Test
    public void charDescendingRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestCharTimSortKernel::generateDescendingCharRuns, getJavaComparator(), CharSortKernelStuff::new);
        }
    }

    @Test
    public void charRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestCharTimSortKernel::generateCharRuns, getJavaComparator(), CharSortKernelStuff::new);
        }
    }

    @Test
    public void charMultiRandomCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            multiCorrectnessTest(size, TestCharTimSortKernel::generateMultiCharRandom, getJavaMultiComparator(), CharMultiSortKernelStuff::new);
        }
    }
}
