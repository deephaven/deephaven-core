/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit BaseTestCharTimSortKernel and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sort.timsort;

import io.deephaven.engine.structures.rowsequence.RowSequenceUtil;
import io.deephaven.engine.v2.utils.*;
import io.deephaven.util.QueryConstants;
import io.deephaven.engine.tuple.generated.FloatLongLongTuple;
import io.deephaven.engine.tuple.generated.FloatLongTuple;
import io.deephaven.engine.v2.sort.findruns.FloatFindRunsKernel;
import io.deephaven.engine.v2.sort.partition.FloatPartitionKernel;
import io.deephaven.engine.v2.sources.AbstractColumnSource;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.chunk.*;
import io.deephaven.engine.chunk.Attributes.*;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public abstract class BaseTestFloatTimSortKernel extends TestTimSortKernel {
    // region getJavaComparator
    static Comparator<FloatLongTuple> getJavaComparator() {
        return Comparator.comparing(FloatLongTuple::getFirstElement);
    }
    // endregion getJavaComparator

    // region getJavaMultiComparator
    static Comparator<FloatLongLongTuple> getJavaMultiComparator() {
        return Comparator.comparing(FloatLongLongTuple::getFirstElement).thenComparing(FloatLongLongTuple::getSecondElement);
    }
    // endregion getJavaMultiComparator


    static class FloatSortKernelStuff extends SortKernelStuff<FloatLongTuple> {
        final WritableFloatChunk<Any> floatChunk;
        private final FloatLongTimsortKernel.FloatLongSortKernelContext context;

        FloatSortKernelStuff(List<FloatLongTuple> javaTuples) {
            super(javaTuples.size());
            final int size = javaTuples.size();
            floatChunk = WritableFloatChunk.makeWritableChunk(size);
            context = FloatLongTimsortKernel.createContext(size);

            prepareFloatChunks(javaTuples, floatChunk, indexKeys);
        }

        @Override
        void run() {
            FloatLongTimsortKernel.sort(context, indexKeys, floatChunk);
        }

        @Override
        void check(List<FloatLongTuple> expected) {
            verify(expected.size(), expected, floatChunk, indexKeys);
        }
    }

    public static class FloatPartitionKernelStuff extends PartitionKernelStuff<FloatLongTuple> {
        final WritableFloatChunk valuesChunk;
        private final FloatPartitionKernel.PartitionKernelContext context;
        private final RowSet rowSet;
        private final ColumnSource<Float> columnSource;

        public FloatPartitionKernelStuff(List<FloatLongTuple> javaTuples, RowSet rowSet, int chunkSize, int nPartitions, boolean preserveEquality) {
            super(javaTuples.size());
            this.rowSet = rowSet;
            final int size = javaTuples.size();
            valuesChunk = WritableFloatChunk.makeWritableChunk(size);

            for (int ii = 0; ii < javaTuples.size(); ++ii) {
                final long indexKey = javaTuples.get(ii).getSecondElement();
                if (indexKey != ii * 10) {
                    throw new IllegalStateException();
                }
            }

            columnSource = new AbstractColumnSource.DefaultedImmutable<Float>(float.class) {
                // region tuple column source
                @Override
                public Float get(long index) {
                    return getFloat(index);
                }

                @Override
                public float getFloat(long index) {
                    return javaTuples.get(((int)index) / 10).getFirstElement();
                }
                // endregion tuple column source
            };

            context = FloatPartitionKernel.createContext(rowSet, columnSource, chunkSize, nPartitions, preserveEquality);

            prepareFloatChunks(javaTuples, valuesChunk, indexKeys);
        }

        @Override
        public void run() {
            FloatPartitionKernel.partition(context, indexKeys, valuesChunk);
        }

        @Override
        void check(List<FloatLongTuple> expected) {
            verifyPartition(context, rowSet, expected.size(), expected, valuesChunk, indexKeys, columnSource);
        }
    }

    static class FloatMergeStuff extends MergeStuff<FloatLongTuple> {
        final float arrayValues[];
        FloatMergeStuff(List<FloatLongTuple> javaTuples) {
            super(javaTuples);
            arrayValues = new float[javaTuples.size()];
            for (int ii = 0; ii < javaTuples.size(); ++ii) {
                arrayValues[ii] = javaTuples.get(ii).getFirstElement();
            }
        }

        void run() {
            // region mergesort
            MergeSort.mergeSort(posarray, posarray2, 0, arrayValues.length, 0, (pos1, pos2) -> Float.compare(arrayValues[(int)pos1], arrayValues[(int)pos2]));
            // endregion mergesort
        }
    }

    static class FloatMultiSortKernelStuff extends SortMultiKernelStuff<FloatLongLongTuple> {
        final WritableFloatChunk<Any> primaryChunk;
        final WritableLongChunk<Any> secondaryChunk;
        final WritableLongChunk<Any> secondaryChunkPermuted;

        final LongIntTimsortKernel.LongIntSortKernelContext sortIndexContext;
        final WritableLongChunk<RowKeys> indicesToFetch;
        final WritableIntChunk<ChunkPositions> originalPositions;

        final ColumnSource<Long> secondaryColumnSource;

        private final FloatLongTimsortKernel.FloatLongSortKernelContext context;
        private final LongLongTimsortKernel.LongLongSortKernelContext secondarySortContext;
        private final ColumnSource.FillContext secondaryColumnSourceContext;

        FloatMultiSortKernelStuff(List<FloatLongLongTuple> javaTuples) {
            super(javaTuples.size());
            final int size = javaTuples.size();
            primaryChunk = WritableFloatChunk.makeWritableChunk(size);
            secondaryChunk = WritableLongChunk.makeWritableChunk(size);
            secondaryChunkPermuted = WritableLongChunk.makeWritableChunk(size);
            context = FloatLongTimsortKernel.createContext(size);

            sortIndexContext = LongIntTimsortKernel.createContext(size);

            indicesToFetch = WritableLongChunk.makeWritableChunk(size);
            originalPositions = WritableIntChunk.makeWritableChunk(size);

            secondarySortContext = io.deephaven.engine.v2.sort.timsort.LongLongTimsortKernel.createContext(size);

            prepareMultiFloatChunks(javaTuples, primaryChunk, secondaryChunk, indexKeys);

            secondaryColumnSource = new AbstractColumnSource.DefaultedImmutable<Long>(long.class) {
                @Override
                public Long get(long index) {
                    final long result = getLong(index);
                    return result == QueryConstants.NULL_LONG ? null : result;
                }

                @Override
                public long getLong(long index) {
                    final FloatLongLongTuple floatLongLongTuple = javaTuples.get((int) (index / 10));
                    return floatLongLongTuple.getSecondElement();
                }
            };

            secondaryColumnSourceContext = secondaryColumnSource.makeFillContext(size);
        }

        @Override
        void run() {
            FloatLongTimsortKernel.sort(context, indexKeys, primaryChunk, offsets, lengths);
            FloatFindRunsKernel.findRuns(primaryChunk, offsets, lengths, offsetsOut, lengthsOut);
//            dumpChunk(primaryChunk);
//            dumpOffsets(offsetsOut, lengthsOut);
            if (offsetsOut.size() > 0) {
                // the secondary context is actually just bogus at this point, it is no longer parallel,
                // what we need to do is fetch the things from that columnsource, but only the things needed to break
                // ties, and then put them in chunks that would be parallel to the rowSet chunk based on offsetsOut and lengthsOut
                //
                // after some consideration, I think the next stage of the sort is:
                // (1) using the chunk of rowSet keys that are relevant, build a second chunk that indicates their position
                // (2) use the LongTimsortKernel to sort by the rowSet key; using the position keys as our as our "indexKeys"
                //     argument.  The sorted rowSet keys can be used as input to an rowSet builder for filling a chunk.
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
        void check(List<FloatLongLongTuple> expected) {
            verify(expected.size(), expected, primaryChunk, secondaryChunk, indexKeys);
        }
    }

    static private void prepareFloatChunks(List<FloatLongTuple> javaTuples, WritableFloatChunk valueChunk, WritableLongChunk<RowKeys> indexKeys) {
        for (int ii = 0; ii < javaTuples.size(); ++ii) {
            valueChunk.set(ii, javaTuples.get(ii).getFirstElement());
            indexKeys.set(ii, javaTuples.get(ii).getSecondElement());
        }
    }

    static private void prepareMultiFloatChunks(List<FloatLongLongTuple> javaTuples, WritableFloatChunk valueChunk, WritableLongChunk secondaryChunk, WritableLongChunk<RowKeys> indexKeys) {
        for (int ii = 0; ii < javaTuples.size(); ++ii) {
            valueChunk.set(ii, javaTuples.get(ii).getFirstElement());
            secondaryChunk.set(ii, javaTuples.get(ii).getSecondElement());
            indexKeys.set(ii, javaTuples.get(ii).getThirdElement());
        }
    }

    @NotNull
    public static List<FloatLongTuple> generateFloatRandom(Random random, int size) {
        final List<FloatLongTuple> javaTuples = new ArrayList<>();
        for (int ii = 0; ii < size; ++ii) {
            final float value = generateFloatValue(random);
            final long longValue = ii * 10;

            final FloatLongTuple tuple = new FloatLongTuple(value, longValue);

            javaTuples.add(tuple);
        }
        return javaTuples;
    }

    @NotNull
    static List<FloatLongLongTuple> generateMultiFloatRandom(Random random, int size) {
        final List<FloatLongTuple> primaryTuples = generateFloatRandom(random, size);

        return primaryTuples.stream().map(clt -> new FloatLongLongTuple(clt.getFirstElement(), random.nextLong(), clt.getSecondElement())).collect(Collectors.toList());
    }

    @NotNull
    public static List<FloatLongTuple> generateFloatRuns(Random random, int size) {
        return generateFloatRuns(random, size, true, true);
    }

    @NotNull
    public static List<FloatLongTuple> generateAscendingFloatRuns(Random random, int size) {
        return generateFloatRuns(random, size, true, false);
    }

    @NotNull
    public static  List<FloatLongTuple> generateDescendingFloatRuns(Random random, int size) {
        return generateFloatRuns(random, size, false, true);
    }

    @NotNull
    static private List<FloatLongTuple> generateFloatRuns(Random random, int size, boolean allowAscending, boolean allowDescending) {
        final List<FloatLongTuple> javaTuples = new ArrayList<>();
        int runStart = 0;
        while (runStart < size) {
            final int maxrun = size - runStart;
            final int runSize = Math.max(random.nextInt(200), maxrun);

            float value = generateFloatValue(random);
            final boolean descending = !allowAscending || (allowDescending && random.nextBoolean());

            for (int ii = 0; ii < runSize; ++ii) {
                final long indexValue = (runStart + ii) * 10;
                final FloatLongTuple tuple = new FloatLongTuple(value, indexValue);

                javaTuples.add(tuple);

                if (descending) {
                    value = decrementFloatValue(random, value);
                } else {
                    value = incrementFloatValue(random, value);
                }
            }

            runStart += runSize;
        }
        return javaTuples;
    }

    static private void verify(int size, List<FloatLongTuple> javaTuples, FloatChunk floatChunk, LongChunk indexKeys) {
//        System.out.println("Verify: " + javaTuples);
//        dumpChunk(valuesChunk);

        for (int ii = 0; ii < size; ++ii) {
            final float timSorted = floatChunk.get(ii);
            final float javaSorted = javaTuples.get(ii).getFirstElement();

            final long timIndex = indexKeys.get(ii);
            final long javaIndex = javaTuples.get(ii).getSecondElement();

            TestCase.assertEquals("values[" + ii + "]", javaSorted, timSorted);
            TestCase.assertEquals("rowSet[" + ii + "]", javaIndex, timIndex);
        }
    }

    static private void verifyPartition(FloatPartitionKernel.PartitionKernelContext context, RowSet source, int size, List<FloatLongTuple> javaTuples, FloatChunk floatChunk, LongChunk indexKeys, ColumnSource<Float> columnSource) {

        final FloatLongTuple [] pivots = context.getPivots();

        final RowSet[] results = context.getPartitions(true);

        final MutableRowSet reconstructed = RowSetFactory.empty();

        // make sure that each partition is a subset of the rowSet and is disjoint
        for (int ii = 0; ii < results.length; ii++) {
            final RowSet partition = results[ii];
            TestCase.assertTrue("partition[" + ii + "].subsetOf(source)", partition.subsetOf(source));
            TestCase.assertFalse("reconstructed[\" + ii + \"]..overlaps(partition)", reconstructed.overlaps(partition));
            reconstructed.insert(partition);
        }

        TestCase.assertEquals(source, reconstructed);

        // now verify that each partition has keys less than the next larger partition

//        System.out.println(javaTuples);


//        System.out.println(javaTuples);

        for (int ii = 0; ii < results.length - 1; ii++) {
            final RowSet partition = results[ii];

            final float expectedPivotValue = pivots[ii].getFirstElement();
            final long expectedPivotKey = pivots[ii].getSecondElement();

            for (int jj = 0; jj < partition.size(); ++jj) {
                final long index = partition.get(jj);
                final float value = columnSource.get(index);
                if (gt(value, expectedPivotValue)) {
                    TestCase.fail("pivot[" + ii + "] = " + expectedPivotValue + ", " + expectedPivotKey + ": is exceeded by" + value);
                } else if (value == expectedPivotValue && index > expectedPivotKey) {
                    TestCase.fail("pivot[" + ii + "] = " + expectedPivotValue + ", " + expectedPivotKey + ": is exceeded by" + value + ", "  + index);
                }
            }
        }

        final List<FloatLongTuple> sortedTuples = new ArrayList<>(javaTuples);
        sortedTuples.sort(getJavaComparator());

        int lastSize = 0;

        for (int ii = 0; ii < results.length; ii++) {
            final RowSet partition = results[ii];

//            System.out.println("Partition[" + ii + "] " + partition.size());

//            System.out.println("(" + lastSize + ", " + (lastSize + partition.intSize()) + ")");
            final List<FloatLongTuple> expectedPartition = sortedTuples.subList(lastSize, lastSize + partition.intSize());
            lastSize += partition.intSize();
//            System.out.println("Expected Partition Max: " + expectedPartition.get(expectedPartition.size() - 1));

            final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
            expectedPartition.stream().mapToLong(FloatLongTuple::getSecondElement).forEach(builder::addKey);
            final RowSet expectedRowSet = builder.build();

            if (!expectedRowSet.equals(partition)) {
                System.out.println("partition.minus(expected): " + partition.minus(expectedRowSet));
                System.out.println("expectedRowSet.minus(partition): " + expectedRowSet.minus(partition));
            }

            TestCase.assertEquals(expectedRowSet, partition);
        }

//
//        for (int ii = 0; ii < size; ++ii) {
//            final float timSorted = valuesChunk.get(ii);
//            final float javaSorted = javaTuples.get(ii).getFirstElement();
//
//            final long timIndex = indexKeys.get(ii);
//            final long javaIndex = javaTuples.get(ii).getSecondElement();
//
//            TestCase.assertEquals("values[" + ii + "]", javaSorted, timSorted);
//            TestCase.assertEquals("rowSet[" + ii + "]", javaIndex, timIndex);
//        }
    }

    static private void verify(int size, List<FloatLongLongTuple> javaTuples, FloatChunk primaryChunk, LongChunk secondaryChunk, LongChunk<RowKeys> indexKeys) {
//        System.out.println("Verify: " + javaTuples);
//        dumpChunks(primaryChunk, indexKeys);

        for (int ii = 0; ii < size; ++ii) {
            final float timSortedPrimary = primaryChunk.get(ii);
            final float javaSorted = javaTuples.get(ii).getFirstElement();

            final long timIndex = indexKeys.get(ii);
            final long javaIndex = javaTuples.get(ii).getThirdElement();

            TestCase.assertEquals("values[" + ii + "]", javaSorted, timSortedPrimary);
            TestCase.assertEquals("rowSet[" + ii + "]", javaIndex, timIndex);
        }
    }

//    private static void dumpChunk(FloatChunk chunk) {
//        System.out.println("[" + IntStream.range(0, chunk.size()).mapToObj(chunk::get).map(c -> (short)(float)c).map(Object::toString).collect(Collectors.joining(",")) + "]");
//    }
//
//    private static void dumpOffsets(IntChunk starts, IntChunk lengths) {
//        System.out.println("[" + IntStream.range(0, starts.size()).mapToObj(idx -> "(" + starts.get(idx) + " -> " + lengths.get(idx) + ")").collect(Collectors.joining(",")) + "]");
//    }
//
//    private static void dumpChunks(FloatChunk primary, LongChunk secondary) {
//        System.out.println("[" + IntStream.range(0, primary.size()).mapToObj(ii -> new FloatLongTuple(primary.get(ii), secondary.get(ii)).toString()).collect(Collectors.joining(",")) + "]");
//    }

    // region comparison functions
    private static int doComparison(float lhs, float rhs) {
        return Float.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean gt(float lhs, float rhs) {
        return doComparison(lhs, rhs) > 0;
    }
}
