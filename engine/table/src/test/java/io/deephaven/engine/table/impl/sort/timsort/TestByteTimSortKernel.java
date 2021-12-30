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
public class TestByteTimSortKernel extends BaseTestByteTimSortKernel {
    // I like this output, but for now am leaving these tests off, so we can focus on getting right answers and we can try
    // out JMH for running morally equivalent things.

//    @Test
//    public void byteRandomPerformanceTest() {
//        performanceTest(TestByteTimSortKernel::generateByteRandom, ByteSortKernelStuff::new, ByteSortKernelStuff::run, getJavaComparator(), ByteMergeStuff::new, ByteMergeStuff::run);
//    }
//
//    @Test
//    public void byteRunPerformanceTest() {
//        performanceTest(TestByteTimSortKernel::generateByteRuns, ByteSortKernelStuff::new, ByteSortKernelStuff::run, getJavaComparator(), ByteMergeStuff::new, ByteMergeStuff::run);
//    }
//
//    @Test
//    public void byteRunDescendingPerformanceTest() {
//        performanceTest(TestByteTimSortKernel::generateDescendingByteRuns, ByteSortKernelStuff::new, ByteSortKernelStuff::run, getJavaComparator(), ByteMergeStuff::new, ByteMergeStuff::run);
//    }
//
//    @Test
//    public void byteRunAscendingPerformanceTest() {
//        performanceTest(TestByteTimSortKernel::generateAscendingByteRuns, ByteSortKernelStuff::new, ByteSortKernelStuff::run, getJavaComparator(), ByteMergeStuff::new, ByteMergeStuff::run);
//    }
//
    @Test
    public void byteRandomCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestByteTimSortKernel::generateByteRandom, getJavaComparator(), ByteSortKernelStuff::new);
        }
    }

    @Test
    public void byteRandomPartitionCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_PARTTITION_CHUNK_SIZE; size *= 2) {
            int partitions = 2;
            while (partitions < (int)Math.sqrt(size)) {
                partitionCorrectnessTest(size, size, partitions, TestByteTimSortKernel::generateByteRandom, getJavaComparator(), BytePartitionKernelStuff::new);
                if (size < 1000) {
                    break;
                }
                partitions *= 3;
            }
        }
    }

    @Test
    public void byteAscendingRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestByteTimSortKernel::generateAscendingByteRuns, getJavaComparator(), ByteSortKernelStuff::new);
        }
    }

    @Test
    public void byteDescendingRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestByteTimSortKernel::generateDescendingByteRuns, getJavaComparator(), ByteSortKernelStuff::new);
        }
    }

    @Test
    public void byteRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestByteTimSortKernel::generateByteRuns, getJavaComparator(), ByteSortKernelStuff::new);
        }
    }

    @Test
    public void byteMultiRandomCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            multiCorrectnessTest(size, TestByteTimSortKernel::generateMultiByteRandom, getJavaMultiComparator(), ByteMultiSortKernelStuff::new);
        }
    }
}
