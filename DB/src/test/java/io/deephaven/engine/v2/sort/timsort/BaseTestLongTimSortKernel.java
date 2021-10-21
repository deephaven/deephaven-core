/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit BaseTestCharTimSortKernel and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sort.timsort;

import io.deephaven.engine.structures.rowsequence.RowSequenceUtil;
import io.deephaven.util.QueryConstants;
import io.deephaven.engine.util.tuples.generated.LongLongLongTuple;
import io.deephaven.engine.util.tuples.generated.LongLongTuple;
import io.deephaven.engine.v2.sort.findruns.LongFindRunsKernel;
import io.deephaven.engine.v2.sort.partition.LongPartitionKernel;
import io.deephaven.engine.v2.sources.AbstractColumnSource;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.chunk.*;
import io.deephaven.engine.v2.sources.chunk.Attributes.*;
import io.deephaven.engine.v2.utils.Index;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public abstract class BaseTestLongTimSortKernel extends TestTimSortKernel {
    // region getJavaComparator
    static Comparator<LongLongTuple> getJavaComparator() {
        return Comparator.comparing(LongLongTuple::getFirstElement);
    }
    // endregion getJavaComparator

    // region getJavaMultiComparator
    static Comparator<LongLongLongTuple> getJavaMultiComparator() {
        return Comparator.comparing(LongLongLongTuple::getFirstElement).thenComparing(LongLongLongTuple::getSecondElement);
    }
    // endregion getJavaMultiComparator


    static class LongSortKernelStuff extends SortKernelStuff<LongLongTuple> {
        final WritableLongChunk<Any> longChunk;
        private final LongLongTimsortKernel.LongLongSortKernelContext context;

        LongSortKernelStuff(List<LongLongTuple> javaTuples) {
            super(javaTuples.size());
            final int size = javaTuples.size();
            longChunk = WritableLongChunk.makeWritableChunk(size);
            context = LongLongTimsortKernel.createContext(size);

            prepareLongChunks(javaTuples, longChunk, indexKeys);
        }

        @Override
        void run() {
            LongLongTimsortKernel.sort(context, indexKeys, longChunk);
        }

        @Override
        void check(List<LongLongTuple> expected) {
            verify(expected.size(), expected, longChunk, indexKeys);
        }
    }

    public static class LongPartitionKernelStuff extends PartitionKernelStuff<LongLongTuple> {
        final WritableLongChunk valuesChunk;
        private final LongPartitionKernel.PartitionKernelContext context;
        private final Index index;
        private final ColumnSource<Long> columnSource;

        public LongPartitionKernelStuff(List<LongLongTuple> javaTuples, Index index, int chunkSize, int nPartitions, boolean preserveEquality) {
            super(javaTuples.size());
            this.index = index;
            final int size = javaTuples.size();
            valuesChunk = WritableLongChunk.makeWritableChunk(size);

            for (int ii = 0; ii < javaTuples.size(); ++ii) {
                final long indexKey = javaTuples.get(ii).getSecondElement();
                if (indexKey != ii * 10) {
                    throw new IllegalStateException();
                }
            }

            columnSource = new AbstractColumnSource.DefaultedImmutable<Long>(long.class) {
                // region tuple column source
                @Override
                public Long get(long index) {
                    return getLong(index);
                }

                @Override
                public long getLong(long index) {
                    return javaTuples.get(((int)index) / 10).getFirstElement();
                }
                // endregion tuple column source
            };

            context = LongPartitionKernel.createContext(index, columnSource, chunkSize, nPartitions, preserveEquality);

            prepareLongChunks(javaTuples, valuesChunk, indexKeys);
        }

        @Override
        public void run() {
            LongPartitionKernel.partition(context, indexKeys, valuesChunk);
        }

        @Override
        void check(List<LongLongTuple> expected) {
            verifyPartition(context, index, expected.size(), expected, valuesChunk, indexKeys, columnSource);
        }
    }

    static class LongMergeStuff extends MergeStuff<LongLongTuple> {
        final long arrayValues[];
        LongMergeStuff(List<LongLongTuple> javaTuples) {
            super(javaTuples);
            arrayValues = new long[javaTuples.size()];
            for (int ii = 0; ii < javaTuples.size(); ++ii) {
                arrayValues[ii] = javaTuples.get(ii).getFirstElement();
            }
        }

        void run() {
            // region mergesort
            MergeSort.mergeSort(posarray, posarray2, 0, arrayValues.length, 0, (pos1, pos2) -> Long.compare(arrayValues[(int)pos1], arrayValues[(int)pos2]));
            // endregion mergesort
        }
    }

    static class LongMultiSortKernelStuff extends SortMultiKernelStuff<LongLongLongTuple> {
        final WritableLongChunk<Any> primaryChunk;
        final WritableLongChunk<Any> secondaryChunk;
        final WritableLongChunk<Any> secondaryChunkPermuted;

        final LongIntTimsortKernel.LongIntSortKernelContext sortIndexContext;
        final WritableLongChunk<RowKeys> indicesToFetch;
        final WritableIntChunk<ChunkPositions> originalPositions;

        final ColumnSource<Long> secondaryColumnSource;

        private final LongLongTimsortKernel.LongLongSortKernelContext context;
        private final LongLongTimsortKernel.LongLongSortKernelContext secondarySortContext;
        private final ColumnSource.FillContext secondaryColumnSourceContext;

        LongMultiSortKernelStuff(List<LongLongLongTuple> javaTuples) {
            super(javaTuples.size());
            final int size = javaTuples.size();
            primaryChunk = WritableLongChunk.makeWritableChunk(size);
            secondaryChunk = WritableLongChunk.makeWritableChunk(size);
            secondaryChunkPermuted = WritableLongChunk.makeWritableChunk(size);
            context = LongLongTimsortKernel.createContext(size);

            sortIndexContext = LongIntTimsortKernel.createContext(size);

            indicesToFetch = WritableLongChunk.makeWritableChunk(size);
            originalPositions = WritableIntChunk.makeWritableChunk(size);

            secondarySortContext = io.deephaven.engine.v2.sort.timsort.LongLongTimsortKernel.createContext(size);

            prepareMultiLongChunks(javaTuples, primaryChunk, secondaryChunk, indexKeys);

            secondaryColumnSource = new AbstractColumnSource.DefaultedImmutable<Long>(long.class) {
                @Override
                public Long get(long index) {
                    final long result = getLong(index);
                    return result == QueryConstants.NULL_LONG ? null : result;
                }

                @Override
                public long getLong(long index) {
                    final LongLongLongTuple longLongLongTuple = javaTuples.get((int) (index / 10));
                    return longLongLongTuple.getSecondElement();
                }
            };

            secondaryColumnSourceContext = secondaryColumnSource.makeFillContext(size);
        }

        @Override
        void run() {
            LongLongTimsortKernel.sort(context, indexKeys, primaryChunk, offsets, lengths);
            LongFindRunsKernel.findRuns(primaryChunk, offsets, lengths, offsetsOut, lengthsOut);
//            dumpChunk(primaryChunk);
//            dumpOffsets(offsetsOut, lengthsOut);
            if (offsetsOut.size() > 0) {
                // the secondary context is actually just bogus at this point, it is no longer parallel,
                // what we need to do is fetch the things from that columnsource, but only the things needed to break
                // ties, and then put them in chunks that would be parallel to the index chunk based on offsetsOut and lengthsOut
                //
                // after some consideration, I think the next stage of the sort is:
                // (1) using the chunk of index keys that are relevant, build a second chunk that indicates their position
                // (2) use the LongTimsortKernel to sort by the index key; using the position keys as our as our "indexKeys"
                //     argument.  The sorted index keys can be used as input to an index builder for filling a chunk.
                // (3) After the chunk of secondary keys is filled, the second sorted indexKeys (really positions that
                //     we care about), will then be used to permute the resulting chunk into a parallel chunk
                //     to our actual indexKeys.
                // (4) We can call this kernel; and do the sub region sorts

                indicesToFetch.setSize(0);
                originalPositions.setSize(0);

                for (int ii = 0; ii < offsetsOut.size(); ++ii) {
                    final int runStart = offsetsOut.get(ii);
                    final int runLength = lengthsOut.get(ii);

                    for (int jj = 0; jj < runLength; ++jj) {
                        indicesToFetch.add(indexKeys.get(runStart + jj));
                        originalPositions.add(runStart +jj);
                    }
                }

                sortIndexContext.sort(originalPositions, indicesToFetch);

                // now we have the indices that we need to fetch from the secondary column source, in sorted order
                secondaryColumnSource.fillChunk(secondaryColumnSourceContext, WritableLongChunk.downcast(secondaryChunk), RowSequenceUtil.wrapRowKeysChunkAsRowSequence(WritableLongChunk.downcast(indicesToFetch)));

                // permute the results back to the order that we would like them in the subsequent sort
                secondaryChunkPermuted.setSize(secondaryChunk.size());
                for (int ii = 0; ii < originalPositions.size(); ++ii) {
                    secondaryChunkPermuted.set(originalPositions.get(ii), secondaryChunk.get(ii));
                }

                // and we can sort the stuff within the run now
                LongLongTimsortKernel.sort(secondarySortContext, indexKeys, secondaryChunkPermuted, offsetsOut, lengthsOut);
            }
        }

        @Override
        void check(List<LongLongLongTuple> expected) {
            verify(expected.size(), expected, primaryChunk, secondaryChunk, indexKeys);
        }
    }

    static private void prepareLongChunks(List<LongLongTuple> javaTuples, WritableLongChunk valueChunk, WritableLongChunk<RowKeys> indexKeys) {
        for (int ii = 0; ii < javaTuples.size(); ++ii) {
            valueChunk.set(ii, javaTuples.get(ii).getFirstElement());
            indexKeys.set(ii, javaTuples.get(ii).getSecondElement());
        }
    }

    static private void prepareMultiLongChunks(List<LongLongLongTuple> javaTuples, WritableLongChunk valueChunk, WritableLongChunk secondaryChunk, WritableLongChunk<RowKeys> indexKeys) {
        for (int ii = 0; ii < javaTuples.size(); ++ii) {
            valueChunk.set(ii, javaTuples.get(ii).getFirstElement());
            secondaryChunk.set(ii, javaTuples.get(ii).getSecondElement());
            indexKeys.set(ii, javaTuples.get(ii).getThirdElement());
        }
    }

    @NotNull
    public static List<LongLongTuple> generateLongRandom(Random random, int size) {
        final List<LongLongTuple> javaTuples = new ArrayList<>();
        for (int ii = 0; ii < size; ++ii) {
            final long value = generateLongValue(random);
            final long longValue = ii * 10;

            final LongLongTuple tuple = new LongLongTuple(value, longValue);

            javaTuples.add(tuple);
        }
        return javaTuples;
    }

    @NotNull
    static List<LongLongLongTuple> generateMultiLongRandom(Random random, int size) {
        final List<LongLongTuple> primaryTuples = generateLongRandom(random, size);

        return primaryTuples.stream().map(clt -> new LongLongLongTuple(clt.getFirstElement(), random.nextLong(), clt.getSecondElement())).collect(Collectors.toList());
    }

    @NotNull
    public static List<LongLongTuple> generateLongRuns(Random random, int size) {
        return generateLongRuns(random, size, true, true);
    }

    @NotNull
    public static List<LongLongTuple> generateAscendingLongRuns(Random random, int size) {
        return generateLongRuns(random, size, true, false);
    }

    @NotNull
    public static  List<LongLongTuple> generateDescendingLongRuns(Random random, int size) {
        return generateLongRuns(random, size, false, true);
    }

    @NotNull
    static private List<LongLongTuple> generateLongRuns(Random random, int size, boolean allowAscending, boolean allowDescending) {
        final List<LongLongTuple> javaTuples = new ArrayList<>();
        int runStart = 0;
        while (runStart < size) {
            final int maxrun = size - runStart;
            final int runSize = Math.max(random.nextInt(200), maxrun);

            long value = generateLongValue(random);
            final boolean descending = !allowAscending || (allowDescending && random.nextBoolean());

            for (int ii = 0; ii < runSize; ++ii) {
                final long indexValue = (runStart + ii) * 10;
                final LongLongTuple tuple = new LongLongTuple(value, indexValue);

                javaTuples.add(tuple);

                if (descending) {
                    value = decrementLongValue(random, value);
                } else {
                    value = incrementLongValue(random, value);
                }
            }

            runStart += runSize;
        }
        return javaTuples;
    }

    static private void verify(int size, List<LongLongTuple> javaTuples, LongChunk longChunk, LongChunk indexKeys) {
//        System.out.println("Verify: " + javaTuples);
//        dumpChunk(valuesChunk);

        for (int ii = 0; ii < size; ++ii) {
            final long timSorted = longChunk.get(ii);
            final long javaSorted = javaTuples.get(ii).getFirstElement();

            final long timIndex = indexKeys.get(ii);
            final long javaIndex = javaTuples.get(ii).getSecondElement();

            TestCase.assertEquals("values[" + ii + "]", javaSorted, timSorted);
            TestCase.assertEquals("index[" + ii + "]", javaIndex, timIndex);
        }
    }

    static private void verifyPartition(LongPartitionKernel.PartitionKernelContext context, Index source, int size, List<LongLongTuple> javaTuples, LongChunk longChunk, LongChunk indexKeys, ColumnSource<Long> columnSource) {

        final LongLongTuple [] pivots = context.getPivots();

        final Index [] results = context.getPartitions(true);

        final Index reconstructed = Index.FACTORY.getEmptyIndex();

        // make sure that each partition is a subset of the index and is disjoint
        for (int ii = 0; ii < results.length; ii++) {
            final Index partition = results[ii];
            TestCase.assertTrue("partition[" + ii + "].subsetOf(source)", partition.subsetOf(source));
            TestCase.assertFalse("reconstructed[\" + ii + \"]..overlaps(partition)", reconstructed.overlaps(partition));
            reconstructed.insert(partition);
        }

        TestCase.assertEquals(source, reconstructed);

        // now verify that each partition has keys less than the next larger partition

//        System.out.println(javaTuples);


//        System.out.println(javaTuples);

        for (int ii = 0; ii < results.length - 1; ii++) {
            final Index partition = results[ii];

            final long expectedPivotValue = pivots[ii].getFirstElement();
            final long expectedPivotKey = pivots[ii].getSecondElement();

            for (int jj = 0; jj < partition.size(); ++jj) {
                final long index = partition.get(jj);
                final long value = columnSource.get(index);
                if (gt(value, expectedPivotValue)) {
                    TestCase.fail("pivot[" + ii + "] = " + expectedPivotValue + ", " + expectedPivotKey + ": is exceeded by" + value);
                } else if (value == expectedPivotValue && index > expectedPivotKey) {
                    TestCase.fail("pivot[" + ii + "] = " + expectedPivotValue + ", " + expectedPivotKey + ": is exceeded by" + value + ", "  + index);
                }
            }
        }

        final List<LongLongTuple> sortedTuples = new ArrayList<>(javaTuples);
        sortedTuples.sort(getJavaComparator());

        int lastSize = 0;

        for (int ii = 0; ii < results.length; ii++) {
            final Index partition = results[ii];

//            System.out.println("Partition[" + ii + "] " + partition.size());

//            System.out.println("(" + lastSize + ", " + (lastSize + partition.intSize()) + ")");
            final List<LongLongTuple> expectedPartition = sortedTuples.subList(lastSize, lastSize + partition.intSize());
            lastSize += partition.intSize();
//            System.out.println("Expected Partition Max: " + expectedPartition.get(expectedPartition.size() - 1));

            final Index.RandomBuilder builder = Index.FACTORY.getRandomBuilder();
            expectedPartition.stream().mapToLong(LongLongTuple::getSecondElement).forEach(builder::addKey);
            final Index expectedIndex = builder.getIndex();

            if (!expectedIndex.equals(partition)) {
                System.out.println("partition.minus(expected): " + partition.minus(expectedIndex));
                System.out.println("expectedIndex.minus(partition): " + expectedIndex.minus(partition));
            }

            TestCase.assertEquals(expectedIndex, partition);
        }

//
//        for (int ii = 0; ii < size; ++ii) {
//            final long timSorted = valuesChunk.get(ii);
//            final long javaSorted = javaTuples.get(ii).getFirstElement();
//
//            final long timIndex = indexKeys.get(ii);
//            final long javaIndex = javaTuples.get(ii).getSecondElement();
//
//            TestCase.assertEquals("values[" + ii + "]", javaSorted, timSorted);
//            TestCase.assertEquals("index[" + ii + "]", javaIndex, timIndex);
//        }
    }

    static private void verify(int size, List<LongLongLongTuple> javaTuples, LongChunk primaryChunk, LongChunk secondaryChunk, LongChunk<RowKeys> indexKeys) {
//        System.out.println("Verify: " + javaTuples);
//        dumpChunks(primaryChunk, indexKeys);

        for (int ii = 0; ii < size; ++ii) {
            final long timSortedPrimary = primaryChunk.get(ii);
            final long javaSorted = javaTuples.get(ii).getFirstElement();

            final long timIndex = indexKeys.get(ii);
            final long javaIndex = javaTuples.get(ii).getThirdElement();

            TestCase.assertEquals("values[" + ii + "]", javaSorted, timSortedPrimary);
            TestCase.assertEquals("index[" + ii + "]", javaIndex, timIndex);
        }
    }

//    private static void dumpChunk(LongChunk chunk) {
//        System.out.println("[" + IntStream.range(0, chunk.size()).mapToObj(chunk::get).map(c -> (short)(long)c).map(Object::toString).collect(Collectors.joining(",")) + "]");
//    }
//
//    private static void dumpOffsets(IntChunk starts, IntChunk lengths) {
//        System.out.println("[" + IntStream.range(0, starts.size()).mapToObj(idx -> "(" + starts.get(idx) + " -> " + lengths.get(idx) + ")").collect(Collectors.joining(",")) + "]");
//    }
//
//    private static void dumpChunks(LongChunk primary, LongChunk secondary) {
//        System.out.println("[" + IntStream.range(0, primary.size()).mapToObj(ii -> new LongLongTuple(primary.get(ii), secondary.get(ii)).toString()).collect(Collectors.joining(",")) + "]");
//    }

    // region comparison functions
    private static int doComparison(long lhs, long rhs) {
        return Long.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean gt(long lhs, long rhs) {
        return doComparison(lhs, rhs) > 0;
    }
}
