/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit BaseTestCharTimSortKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sort.timsort;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.util.QueryConstants;
import io.deephaven.tuple.generated.FloatLongLongTuple;
import io.deephaven.tuple.generated.FloatLongTuple;
import io.deephaven.engine.table.impl.sort.findruns.FloatFindRunsKernel;
import io.deephaven.engine.table.impl.sort.partition.FloatPartitionKernel;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public abstract class BaseTestFloatTimSortKernel extends TestTimSortKernel {
    // region getJavaComparator
    public static Comparator<FloatLongTuple> getJavaComparator() {
        return Comparator.comparing(FloatLongTuple::getFirstElement);
    }
    // endregion getJavaComparator

    // region getJavaMultiComparator
    public static Comparator<FloatLongLongTuple> getJavaMultiComparator() {
        return Comparator.comparing(FloatLongLongTuple::getFirstElement).thenComparing(FloatLongLongTuple::getSecondElement);
    }
    // endregion getJavaMultiComparator

    public static class FloatSortKernelStuff extends SortKernelStuff<FloatLongTuple> {

        private final WritableFloatChunk<Any> floatChunk;
        private final FloatLongTimsortKernel.FloatLongSortKernelContext context;

        public FloatSortKernelStuff(List<FloatLongTuple> javaTuples) {
            super(javaTuples.size());
            final int size = javaTuples.size();
            floatChunk = WritableFloatChunk.makeWritableChunk(size);
            context = FloatLongTimsortKernel.createContext(size);

            prepareFloatChunks(javaTuples, floatChunk, rowKeys);
        }

        @Override
        public void run() {
            FloatLongTimsortKernel.sort(context, rowKeys, floatChunk);
        }

        @Override
        void check(List<FloatLongTuple> expected) {
            verify(expected.size(), expected, floatChunk, rowKeys);
        }

        @Override
        public void close() {
            super.close();
            floatChunk.close();
            context.close();
        }
    }

    public static class FloatPartitionKernelStuff extends PartitionKernelStuff<FloatLongTuple> {

        private final WritableFloatChunk valuesChunk;
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
                public Float get(long rowKey) {
                    return getFloat(rowKey);
                }

                @Override
                public float getFloat(long rowKey) {
                    return javaTuples.get(((int) rowKey) / 10).getFirstElement();
                }
                // endregion tuple column source
            };

            context = FloatPartitionKernel.createContext(rowSet, columnSource, chunkSize, nPartitions, preserveEquality);

            prepareFloatChunks(javaTuples, valuesChunk, rowKeys);
        }

        @Override
        public void run() {
            FloatPartitionKernel.partition(context, rowKeys, valuesChunk);
        }

        @Override
        void check(List<FloatLongTuple> expected) {
            verifyPartition(context, rowSet, expected.size(), expected, valuesChunk, rowKeys, columnSource);
        }

        @Override
        public void close() {
            super.close();
            valuesChunk.close();
            context.close();
            rowSet.close();
        }
    }

    public static class FloatMergeStuff extends MergeStuff<FloatLongTuple> {

        private final float arrayValues[];

        public FloatMergeStuff(List<FloatLongTuple> javaTuples) {
            super(javaTuples);
            arrayValues = new float[javaTuples.size()];
            for (int ii = 0; ii < javaTuples.size(); ++ii) {
                arrayValues[ii] = javaTuples.get(ii).getFirstElement();
            }
        }

        public void run() {
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

            secondarySortContext = io.deephaven.engine.table.impl.sort.timsort.LongLongTimsortKernel.createContext(size);

            prepareMultiFloatChunks(javaTuples, primaryChunk, secondaryChunk, rowKeys);

            secondaryColumnSource = new AbstractColumnSource.DefaultedImmutable<Long>(long.class) {
                @Override
                public Long get(long rowKey) {
                    final long result = getLong(rowKey);
                    return result == QueryConstants.NULL_LONG ? null : result;
                }

                @Override
                public long getLong(long rowKey) {
                    final FloatLongLongTuple floatLongLongTuple = javaTuples.get((int) (rowKey / 10));
                    return floatLongLongTuple.getSecondElement();
                }
            };

            secondaryColumnSourceContext = secondaryColumnSource.makeFillContext(size);
        }

        @Override
        public void run() {
            FloatLongTimsortKernel.sort(context, rowKeys, primaryChunk, offsets, lengths);
            FloatFindRunsKernel.findRuns(primaryChunk, offsets, lengths, offsetsOut, lengthsOut);
//            dumpChunk(primaryChunk);
//            dumpOffsets(offsetsOut, lengthsOut);
            if (offsetsOut.size() > 0) {
                // the secondary context is actually just bogus at this point, it is no longer parallel,
                // what we need to do is fetch the things from that columnsource, but only the things needed to break
                // ties, and then put them in chunks that would be parallel to the rowSet chunk based on offsetsOut and lengthsOut
                //
                // after some consideration, I think the next stage of the sort is:
                // (1) using the chunk of row keys that are relevant, build a second chunk that indicates their position
                // (2) use the LongTimsortKernel to sort by the row key; using the position keys as our as our "rowKeys"
                //     argument.  The sorted row keys can be used as input to a RowSet builder for filling a chunk.
                // (3) After the chunk of secondary keys is filled, the second sorted rowKeys (really positions that
                //     we care about), will then be used to permute the resulting chunk into a parallel chunk
                //     to our actual rowKeys.
                // (4) We can call this kernel; and do the sub region sorts

                indicesToFetch.setSize(0);
                originalPositions.setSize(0);

                for (int ii = 0; ii < offsetsOut.size(); ++ii) {
                    final int runStart = offsetsOut.get(ii);
                    final int runLength = lengthsOut.get(ii);

                    for (int jj = 0; jj < runLength; ++jj) {
                        indicesToFetch.add(rowKeys.get(runStart + jj));
                        originalPositions.add(runStart +jj);
                    }
                }

                sortIndexContext.sort(originalPositions, indicesToFetch);

                // now we have the indices that we need to fetch from the secondary column source, in sorted order
                secondaryColumnSource.fillChunk(secondaryColumnSourceContext, secondaryChunk, RowSequenceFactory.wrapRowKeysChunkAsRowSequence(WritableLongChunk.downcast(indicesToFetch)));

                // permute the results back to the order that we would like them in the subsequent sort
                secondaryChunkPermuted.setSize(secondaryChunk.size());
                for (int ii = 0; ii < originalPositions.size(); ++ii) {
                    secondaryChunkPermuted.set(originalPositions.get(ii), secondaryChunk.get(ii));
                }

                // and we can sort the stuff within the run now
                LongLongTimsortKernel.sort(secondarySortContext, rowKeys, secondaryChunkPermuted, offsetsOut, lengthsOut);
            }
        }

        @Override
        void check(List<FloatLongLongTuple> expected) {
            verify(expected.size(), expected, primaryChunk, secondaryChunk, rowKeys);
        }

        @Override
        public void close() {
            super.close();
            primaryChunk.close();
            secondaryChunk.close();
            secondaryChunkPermuted.close();
            sortIndexContext.close();
            indicesToFetch.close();
            originalPositions.close();
            context.close();
            secondarySortContext.close();
            secondaryColumnSourceContext.close();
        }
    }

    static private void prepareFloatChunks(List<FloatLongTuple> javaTuples, WritableFloatChunk valueChunk, WritableLongChunk<RowKeys> rowKeys) {
        for (int ii = 0; ii < javaTuples.size(); ++ii) {
            valueChunk.set(ii, javaTuples.get(ii).getFirstElement());
            rowKeys.set(ii, javaTuples.get(ii).getSecondElement());
        }
    }

    static private void prepareMultiFloatChunks(List<FloatLongLongTuple> javaTuples, WritableFloatChunk valueChunk, WritableLongChunk secondaryChunk, WritableLongChunk<RowKeys> rowKeys) {
        for (int ii = 0; ii < javaTuples.size(); ++ii) {
            valueChunk.set(ii, javaTuples.get(ii).getFirstElement());
            secondaryChunk.set(ii, javaTuples.get(ii).getSecondElement());
            rowKeys.set(ii, javaTuples.get(ii).getThirdElement());
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

    static private void verify(int size, List<FloatLongTuple> javaTuples, FloatChunk floatChunk, LongChunk rowKeys) {
//        System.out.println("Verify: " + javaTuples);
//        dumpChunk(valuesChunk);

        for (int ii = 0; ii < size; ++ii) {
            final float timSorted = floatChunk.get(ii);
            final float javaSorted = javaTuples.get(ii).getFirstElement();

            final long timIndex = rowKeys.get(ii);
            final long javaIndex = javaTuples.get(ii).getSecondElement();

            TestCase.assertEquals("values[" + ii + "]", javaSorted, timSorted);
            TestCase.assertEquals("rowKeys[" + ii + "]", javaIndex, timIndex);
        }
    }

    static private void verifyPartition(FloatPartitionKernel.PartitionKernelContext context, RowSet source, int size, List<FloatLongTuple> javaTuples, FloatChunk floatChunk, LongChunk rowKeys, ColumnSource<Float> columnSource) {

        final FloatLongTuple [] pivots = context.getPivots();

        final RowSet[] results = context.getPartitions(true);

        final WritableRowSet reconstructed = RowSetFactory.empty();

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
//            final long timIndex = rowKeys.get(ii);
//            final long javaIndex = javaTuples.get(ii).getSecondElement();
//
//            TestCase.assertEquals("values[" + ii + "]", javaSorted, timSorted);
//            TestCase.assertEquals("rowKeys[" + ii + "]", javaIndex, timIndex);
//        }
    }

    static private void verify(int size, List<FloatLongLongTuple> javaTuples, FloatChunk primaryChunk, LongChunk secondaryChunk, LongChunk<RowKeys> rowKeys) {
//        System.out.println("Verify: " + javaTuples);
//        dumpChunks(primaryChunk, rowKeys);

        for (int ii = 0; ii < size; ++ii) {
            final float timSortedPrimary = primaryChunk.get(ii);
            final float javaSorted = javaTuples.get(ii).getFirstElement();

            final long timIndex = rowKeys.get(ii);
            final long javaIndex = javaTuples.get(ii).getThirdElement();

            TestCase.assertEquals("values[" + ii + "]", javaSorted, timSortedPrimary);
            TestCase.assertEquals("rowKeys[" + ii + "]", javaIndex, timIndex);
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
