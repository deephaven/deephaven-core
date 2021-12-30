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
public class TestObjectTimSortKernel extends BaseTestObjectTimSortKernel {
    // I like this output, but for now am leaving these tests off, so we can focus on getting right answers and we can try
    // out JMH for running morally equivalent things.

//    @Test
//    public void ObjectRandomPerformanceTest() {
//        performanceTest(TestObjectTimSortKernel::generateObjectRandom, ObjectSortKernelStuff::new, ObjectSortKernelStuff::run, getJavaComparator(), ObjectMergeStuff::new, ObjectMergeStuff::run);
//    }
//
//    @Test
//    public void ObjectRunPerformanceTest() {
//        performanceTest(TestObjectTimSortKernel::generateObjectRuns, ObjectSortKernelStuff::new, ObjectSortKernelStuff::run, getJavaComparator(), ObjectMergeStuff::new, ObjectMergeStuff::run);
//    }
//
//    @Test
//    public void ObjectRunDescendingPerformanceTest() {
//        performanceTest(TestObjectTimSortKernel::generateDescendingObjectRuns, ObjectSortKernelStuff::new, ObjectSortKernelStuff::run, getJavaComparator(), ObjectMergeStuff::new, ObjectMergeStuff::run);
//    }
//
//    @Test
//    public void ObjectRunAscendingPerformanceTest() {
//        performanceTest(TestObjectTimSortKernel::generateAscendingObjectRuns, ObjectSortKernelStuff::new, ObjectSortKernelStuff::run, getJavaComparator(), ObjectMergeStuff::new, ObjectMergeStuff::run);
//    }
//
    @Test
    public void ObjectRandomCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestObjectTimSortKernel::generateObjectRandom, getJavaComparator(), ObjectSortKernelStuff::new);
        }
    }

    @Test
    public void ObjectRandomPartitionCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_PARTTITION_CHUNK_SIZE; size *= 2) {
            int partitions = 2;
            while (partitions < (int)Math.sqrt(size)) {
                partitionCorrectnessTest(size, size, partitions, TestObjectTimSortKernel::generateObjectRandom, getJavaComparator(), ObjectPartitionKernelStuff::new);
                if (size < 1000) {
                    break;
                }
                partitions *= 3;
            }
        }
    }

    @Test
    public void ObjectAscendingRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestObjectTimSortKernel::generateAscendingObjectRuns, getJavaComparator(), ObjectSortKernelStuff::new);
        }
    }

    @Test
    public void ObjectDescendingRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestObjectTimSortKernel::generateDescendingObjectRuns, getJavaComparator(), ObjectSortKernelStuff::new);
        }
    }

    @Test
    public void ObjectRunCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            correctnessTest(size, TestObjectTimSortKernel::generateObjectRuns, getJavaComparator(), ObjectSortKernelStuff::new);
        }
    }

    @Test
    public void ObjectMultiRandomCorrectness() {
        for (int size = INITIAL_CORRECTNESS_SIZE; size <= MAX_CHUNK_SIZE; size *= 2) {
            multiCorrectnessTest(size, TestObjectTimSortKernel::generateMultiObjectRandom, getJavaMultiComparator(), ObjectMultiSortKernelStuff::new);
        }
    }
}
