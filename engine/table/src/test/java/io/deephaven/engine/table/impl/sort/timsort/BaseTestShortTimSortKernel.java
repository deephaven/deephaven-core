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
import io.deephaven.tuple.generated.ShortLongLongTuple;
import io.deephaven.tuple.generated.ShortLongTuple;
import io.deephaven.engine.table.impl.sort.findruns.ShortFindRunsKernel;
import io.deephaven.engine.table.impl.sort.partition.ShortPartitionKernel;
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

public abstract class BaseTestShortTimSortKernel extends TestTimSortKernel {
    // region getJavaComparator
    public static Comparator<ShortLongTuple> getJavaComparator() {
        return Comparator.comparing(ShortLongTuple::getFirstElement);
    }
    // endregion getJavaComparator

    // region getJavaMultiComparator
    public static Comparator<ShortLongLongTuple> getJavaMultiComparator() {
        return Comparator.comparing(ShortLongLongTuple::getFirstElement).thenComparing(ShortLongLongTuple::getSecondElement);
    }
    // endregion getJavaMultiComparator

    public static class ShortSortKernelStuff extends SortKernelStuff<ShortLongTuple> {

        private final WritableShortChunk<Any> shortChunk;
        private final ShortLongTimsortKernel.ShortLongSortKernelContext context;

        public ShortSortKernelStuff(List<ShortLongTuple> javaTuples) {
            super(javaTuples.size());
            final int size = javaTuples.size();
            shortChunk = WritableShortChunk.makeWritableChunk(size);
            context = ShortLongTimsortKernel.createContext(size);

            prepareShortChunks(javaTuples, shortChunk, rowKeys);
        }

        @Override
        public void run() {
            ShortLongTimsortKernel.sort(context, rowKeys, shortChunk);
        }

        @Override
        void check(List<ShortLongTuple> expected) {
            verify(expected.size(), expected, shortChunk, rowKeys);
        }
    }

    public static class ShortPartitionKernelStuff extends PartitionKernelStuff<ShortLongTuple> {

        private final WritableShortChunk valuesChunk;
        private final ShortPartitionKernel.PartitionKernelContext context;
        private final RowSet rowSet;
        private final ColumnSource<Short> columnSource;

        public ShortPartitionKernelStuff(List<ShortLongTuple> javaTuples, RowSet rowSet, int chunkSize, int nPartitions, boolean preserveEquality) {
            super(javaTuples.size());
            this.rowSet = rowSet;
            final int size = javaTuples.size();
            valuesChunk = WritableShortChunk.makeWritableChunk(size);

            for (int ii = 0; ii < javaTuples.size(); ++ii) {
                final long indexKey = javaTuples.get(ii).getSecondElement();
                if (indexKey != ii * 10) {
                    throw new IllegalStateException();
                }
            }

            columnSource = new AbstractColumnSource.DefaultedImmutable<Short>(short.class) {
                // region tuple column source
                @Override
                public Short get(long index) {
                    return getShort(index);
                }

                @Override
                public short getShort(long index) {
                    return javaTuples.get(((int)index) / 10).getFirstElement();
                }
                // endregion tuple column source
            };

            context = ShortPartitionKernel.createContext(rowSet, columnSource, chunkSize, nPartitions, preserveEquality);

            prepareShortChunks(javaTuples, valuesChunk, rowKeys);
        }

        @Override
        public void run() {
            ShortPartitionKernel.partition(context, rowKeys, valuesChunk);
        }

        @Override
        void check(List<ShortLongTuple> expected) {
            verifyPartition(context, rowSet, expected.size(), expected, valuesChunk, rowKeys, columnSource);
        }
    }

    public static class ShortMergeStuff extends MergeStuff<ShortLongTuple> {

        private final short arrayValues[];

        public ShortMergeStuff(List<ShortLongTuple> javaTuples) {
            super(javaTuples);
            arrayValues = new short[javaTuples.size()];
            for (int ii = 0; ii < javaTuples.size(); ++ii) {
                arrayValues[ii] = javaTuples.get(ii).getFirstElement();
            }
        }

        public void run() {
            // region mergesort
            MergeSort.mergeSort(posarray, posarray2, 0, arrayValues.length, 0, (pos1, pos2) -> Short.compare(arrayValues[(int)pos1], arrayValues[(int)pos2]));
            // endregion mergesort
        }
    }

    static class ShortMultiSortKernelStuff extends SortMultiKernelStuff<ShortLongLongTuple> {
        final WritableShortChunk<Any> primaryChunk;
        final WritableLongChunk<Any> secondaryChunk;
        final WritableLongChunk<Any> secondaryChunkPermuted;

        final LongIntTimsortKernel.LongIntSortKernelContext sortIndexContext;
        final WritableLongChunk<RowKeys> indicesToFetch;
        final WritableIntChunk<ChunkPositions> originalPositions;

        final ColumnSource<Long> secondaryColumnSource;

        private final ShortLongTimsortKernel.ShortLongSortKernelContext context;
        private final LongLongTimsortKernel.LongLongSortKernelContext secondarySortContext;
        private final ColumnSource.FillContext secondaryColumnSourceContext;

        ShortMultiSortKernelStuff(List<ShortLongLongTuple> javaTuples) {
            super(javaTuples.size());
            final int size = javaTuples.size();
            primaryChunk = WritableShortChunk.makeWritableChunk(size);
            secondaryChunk = WritableLongChunk.makeWritableChunk(size);
            secondaryChunkPermuted = WritableLongChunk.makeWritableChunk(size);
            context = ShortLongTimsortKernel.createContext(size);

            sortIndexContext = LongIntTimsortKernel.createContext(size);

            indicesToFetch = WritableLongChunk.makeWritableChunk(size);
            originalPositions = WritableIntChunk.makeWritableChunk(size);

            secondarySortContext = io.deephaven.engine.table.impl.sort.timsort.LongLongTimsortKernel.createContext(size);

            prepareMultiShortChunks(javaTuples, primaryChunk, secondaryChunk, rowKeys);

            secondaryColumnSource = new AbstractColumnSource.DefaultedImmutable<Long>(long.class) {
                @Override
                public Long get(long index) {
                    final long result = getLong(index);
                    return result == QueryConstants.NULL_LONG ? null : result;
                }

                @Override
                public long getLong(long index) {
                    final ShortLongLongTuple shortLongLongTuple = javaTuples.get((int) (index / 10));
                    return shortLongLongTuple.getSecondElement();
                }
            };

            secondaryColumnSourceContext = secondaryColumnSource.makeFillContext(size);
        }

        @Override
        public void run() {
            ShortLongTimsortKernel.sort(context, rowKeys, primaryChunk, offsets, lengths);
            ShortFindRunsKernel.findRuns(primaryChunk, offsets, lengths, offsetsOut, lengthsOut);
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
                secondaryColumnSource.fillChunk(secondaryColumnSourceContext, WritableLongChunk.downcast(secondaryChunk), RowSequenceFactory.wrapRowKeysChunkAsRowSequence(WritableLongChunk.downcast(indicesToFetch)));

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
        void check(List<ShortLongLongTuple> expected) {
            verify(expected.size(), expected, primaryChunk, secondaryChunk, rowKeys);
        }
    }

    static private void prepareShortChunks(List<ShortLongTuple> javaTuples, WritableShortChunk valueChunk, WritableLongChunk<RowKeys> rowKeys) {
        for (int ii = 0; ii < javaTuples.size(); ++ii) {
            valueChunk.set(ii, javaTuples.get(ii).getFirstElement());
            rowKeys.set(ii, javaTuples.get(ii).getSecondElement());
        }
    }

    static private void prepareMultiShortChunks(List<ShortLongLongTuple> javaTuples, WritableShortChunk valueChunk, WritableLongChunk secondaryChunk, WritableLongChunk<RowKeys> rowKeys) {
        for (int ii = 0; ii < javaTuples.size(); ++ii) {
            valueChunk.set(ii, javaTuples.get(ii).getFirstElement());
            secondaryChunk.set(ii, javaTuples.get(ii).getSecondElement());
            rowKeys.set(ii, javaTuples.get(ii).getThirdElement());
        }
    }

    @NotNull
    public static List<ShortLongTuple> generateShortRandom(Random random, int size) {
        final List<ShortLongTuple> javaTuples = new ArrayList<>();
        for (int ii = 0; ii < size; ++ii) {
            final short value = generateShortValue(random);
            final long longValue = ii * 10;

            final ShortLongTuple tuple = new ShortLongTuple(value, longValue);

            javaTuples.add(tuple);
        }
        return javaTuples;
    }

    @NotNull
    static List<ShortLongLongTuple> generateMultiShortRandom(Random random, int size) {
        final List<ShortLongTuple> primaryTuples = generateShortRandom(random, size);

        return primaryTuples.stream().map(clt -> new ShortLongLongTuple(clt.getFirstElement(), random.nextLong(), clt.getSecondElement())).collect(Collectors.toList());
    }

    @NotNull
    public static List<ShortLongTuple> generateShortRuns(Random random, int size) {
        return generateShortRuns(random, size, true, true);
    }

    @NotNull
    public static List<ShortLongTuple> generateAscendingShortRuns(Random random, int size) {
        return generateShortRuns(random, size, true, false);
    }

    @NotNull
    public static  List<ShortLongTuple> generateDescendingShortRuns(Random random, int size) {
        return generateShortRuns(random, size, false, true);
    }

    @NotNull
    static private List<ShortLongTuple> generateShortRuns(Random random, int size, boolean allowAscending, boolean allowDescending) {
        final List<ShortLongTuple> javaTuples = new ArrayList<>();
        int runStart = 0;
        while (runStart < size) {
            final int maxrun = size - runStart;
            final int runSize = Math.max(random.nextInt(200), maxrun);

            short value = generateShortValue(random);
            final boolean descending = !allowAscending || (allowDescending && random.nextBoolean());

            for (int ii = 0; ii < runSize; ++ii) {
                final long indexValue = (runStart + ii) * 10;
                final ShortLongTuple tuple = new ShortLongTuple(value, indexValue);

                javaTuples.add(tuple);

                if (descending) {
                    value = decrementShortValue(random, value);
                } else {
                    value = incrementShortValue(random, value);
                }
            }

            runStart += runSize;
        }
        return javaTuples;
    }

    static private void verify(int size, List<ShortLongTuple> javaTuples, ShortChunk shortChunk, LongChunk rowKeys) {
//        System.out.println("Verify: " + javaTuples);
//        dumpChunk(valuesChunk);

        for (int ii = 0; ii < size; ++ii) {
            final short timSorted = shortChunk.get(ii);
            final short javaSorted = javaTuples.get(ii).getFirstElement();

            final long timIndex = rowKeys.get(ii);
            final long javaIndex = javaTuples.get(ii).getSecondElement();

            TestCase.assertEquals("values[" + ii + "]", javaSorted, timSorted);
            TestCase.assertEquals("rowKeys[" + ii + "]", javaIndex, timIndex);
        }
    }

    static private void verifyPartition(ShortPartitionKernel.PartitionKernelContext context, RowSet source, int size, List<ShortLongTuple> javaTuples, ShortChunk shortChunk, LongChunk rowKeys, ColumnSource<Short> columnSource) {

        final ShortLongTuple [] pivots = context.getPivots();

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

            final short expectedPivotValue = pivots[ii].getFirstElement();
            final long expectedPivotKey = pivots[ii].getSecondElement();

            for (int jj = 0; jj < partition.size(); ++jj) {
                final long index = partition.get(jj);
                final short value = columnSource.get(index);
                if (gt(value, expectedPivotValue)) {
                    TestCase.fail("pivot[" + ii + "] = " + expectedPivotValue + ", " + expectedPivotKey + ": is exceeded by" + value);
                } else if (value == expectedPivotValue && index > expectedPivotKey) {
                    TestCase.fail("pivot[" + ii + "] = " + expectedPivotValue + ", " + expectedPivotKey + ": is exceeded by" + value + ", "  + index);
                }
            }
        }

        final List<ShortLongTuple> sortedTuples = new ArrayList<>(javaTuples);
        sortedTuples.sort(getJavaComparator());

        int lastSize = 0;

        for (int ii = 0; ii < results.length; ii++) {
            final RowSet partition = results[ii];

//            System.out.println("Partition[" + ii + "] " + partition.size());

//            System.out.println("(" + lastSize + ", " + (lastSize + partition.intSize()) + ")");
            final List<ShortLongTuple> expectedPartition = sortedTuples.subList(lastSize, lastSize + partition.intSize());
            lastSize += partition.intSize();
//            System.out.println("Expected Partition Max: " + expectedPartition.get(expectedPartition.size() - 1));

            final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
            expectedPartition.stream().mapToLong(ShortLongTuple::getSecondElement).forEach(builder::addKey);
            final RowSet expectedRowSet = builder.build();

            if (!expectedRowSet.equals(partition)) {
                System.out.println("partition.minus(expected): " + partition.minus(expectedRowSet));
                System.out.println("expectedRowSet.minus(partition): " + expectedRowSet.minus(partition));
            }

            TestCase.assertEquals(expectedRowSet, partition);
        }

//
//        for (int ii = 0; ii < size; ++ii) {
//            final short timSorted = valuesChunk.get(ii);
//            final short javaSorted = javaTuples.get(ii).getFirstElement();
//
//            final long timIndex = rowKeys.get(ii);
//            final long javaIndex = javaTuples.get(ii).getSecondElement();
//
//            TestCase.assertEquals("values[" + ii + "]", javaSorted, timSorted);
//            TestCase.assertEquals("rowKeys[" + ii + "]", javaIndex, timIndex);
//        }
    }

    static private void verify(int size, List<ShortLongLongTuple> javaTuples, ShortChunk primaryChunk, LongChunk secondaryChunk, LongChunk<RowKeys> rowKeys) {
//        System.out.println("Verify: " + javaTuples);
//        dumpChunks(primaryChunk, rowKeys);

        for (int ii = 0; ii < size; ++ii) {
            final short timSortedPrimary = primaryChunk.get(ii);
            final short javaSorted = javaTuples.get(ii).getFirstElement();

            final long timIndex = rowKeys.get(ii);
            final long javaIndex = javaTuples.get(ii).getThirdElement();

            TestCase.assertEquals("values[" + ii + "]", javaSorted, timSortedPrimary);
            TestCase.assertEquals("rowKeys[" + ii + "]", javaIndex, timIndex);
        }
    }

//    private static void dumpChunk(ShortChunk chunk) {
//        System.out.println("[" + IntStream.range(0, chunk.size()).mapToObj(chunk::get).map(c -> (short)(short)c).map(Object::toString).collect(Collectors.joining(",")) + "]");
//    }
//
//    private static void dumpOffsets(IntChunk starts, IntChunk lengths) {
//        System.out.println("[" + IntStream.range(0, starts.size()).mapToObj(idx -> "(" + starts.get(idx) + " -> " + lengths.get(idx) + ")").collect(Collectors.joining(",")) + "]");
//    }
//
//    private static void dumpChunks(ShortChunk primary, LongChunk secondary) {
//        System.out.println("[" + IntStream.range(0, primary.size()).mapToObj(ii -> new ShortLongTuple(primary.get(ii), secondary.get(ii)).toString()).collect(Collectors.joining(",")) + "]");
//    }

    // region comparison functions
    private static int doComparison(short lhs, short rhs) {
        return Short.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean gt(short lhs, short rhs) {
        return doComparison(lhs, rhs) > 0;
    }
}
