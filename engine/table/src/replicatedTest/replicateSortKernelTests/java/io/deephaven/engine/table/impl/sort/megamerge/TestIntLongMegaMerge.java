//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestCharLongMegaMerge and run "./gradlew replicateSortKernelTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sort.megamerge;

import io.deephaven.engine.table.ChunkSource;
import io.deephaven.chunk.util.hashing.IntChunkEquals;
import io.deephaven.chunk.util.hashing.LongChunkEquals;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.*;

public class TestIntLongMegaMerge {
    @Test
    public void testMergeAscending() {
        testMerge(true);
    }

    @Test
    public void testMergeDescending() {
        testMerge(false);
    }

    /**
     * A merge whose gallops cover most of the remaining run: ascending, the destination holds 10 and 30 to 39 and the
     * new chunk 0, 20 to 28 and 40 to 49, and the merged result holds all of them in order.
     */
    @Test
    public void testLongGallopsAscending() {
        testLongGallops(true);
    }

    /**
     * A descending merge whose gallops cover most of the remaining run: the destination holds 50 and 21 to 30 and the
     * new chunk 60, 32 to 40 and 11 to 20, and the merged result holds all of them in descending order.
     */
    @Test
    public void testLongGallopsDescending() {
        testLongGallops(false);
    }

    private void testLongGallops(boolean ascending) {
        final IntegerArraySource valuesSource = new IntegerArraySource();
        final LongArraySource keySource = new LongArraySource();

        final int[] destinationInts = {10, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39};
        final int[] chunkInts = {0, 20, 21, 22, 23, 24, 25, 26, 27, 28, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49};
        final int destinationSize = destinationInts.length;
        final int chunkSize = chunkInts.length;
        final int totalSize = destinationSize + chunkSize;

        try (final WritableIntChunk<Values> destinationValues = WritableIntChunk.makeWritableChunk(destinationSize);
                final WritableLongChunk<RowKeys> destinationKeys = WritableLongChunk.makeWritableChunk(destinationSize);
                final WritableIntChunk<Values> valuesChunk = WritableIntChunk.makeWritableChunk(chunkSize);
                final WritableLongChunk<RowKeys> keysChunk = WritableLongChunk.makeWritableChunk(chunkSize);
                final WritableIntChunk<Values> allValues = WritableIntChunk.makeWritableChunk(totalSize);
                final WritableLongChunk<RowKeys> allKeys = WritableLongChunk.makeWritableChunk(totalSize)) {
            // the descending values mirror the ascending ones, so the merge proceeds the same way
            for (int ii = 0; ii < destinationSize; ++ii) {
                destinationValues.set(ii, (int) (ascending ? destinationInts[ii] : 60 - destinationInts[ii]));
                destinationKeys.set(ii, ii);
            }
            for (int ii = 0; ii < chunkSize; ++ii) {
                valuesChunk.set(ii, (int) (ascending ? chunkInts[ii] : 60 - chunkInts[ii]));
                keysChunk.set(ii, destinationSize + ii);
            }
            allValues.copyFromChunk(destinationValues, 0, 0, destinationSize);
            allKeys.copyFromChunk(destinationKeys, 0, 0, destinationSize);
            allValues.copyFromChunk(valuesChunk, 0, destinationSize, chunkSize);
            allKeys.copyFromChunk(keysChunk, 0, destinationSize, chunkSize);

            MegaMergeTestUtils.doSort(ascending, destinationSize, destinationValues, destinationKeys);
            MegaMergeTestUtils.doSort(ascending, chunkSize, valuesChunk, keysChunk);
            MegaMergeTestUtils.doSort(ascending, totalSize, allValues, allKeys);

            if (ascending) {
                IntLongMegaMergeKernel.merge(keySource, valuesSource, 0, 0, destinationKeys, destinationValues);
                IntLongMegaMergeKernel.merge(keySource, valuesSource, 0, destinationSize, keysChunk, valuesChunk);
            } else {
                IntLongMegaMergeDescendingKernel.merge(keySource, valuesSource, 0, 0, destinationKeys,
                        destinationValues);
                IntLongMegaMergeDescendingKernel.merge(keySource, valuesSource, 0, destinationSize, keysChunk,
                        valuesChunk);
            }

            try (final ChunkSource.GetContext valueContext = valuesSource.makeGetContext(totalSize);
                    final ChunkSource.GetContext keyContext = keySource.makeGetContext(totalSize)) {
                final RowSequence rowSequence = RowSequenceFactory.forRange(0, totalSize - 1);
                final IntChunk<Values> checkValues = valuesSource.getChunk(valueContext, rowSequence).asIntChunk();
                final LongChunk<Values> checkKeys = keySource.getChunk(keyContext, rowSequence).asLongChunk();
                assertEquals(totalSize, IntChunkEquals.firstDifference(checkValues, allValues));
                assertEquals(totalSize, LongChunkEquals.firstDifference(checkKeys, allKeys));
            }
        }
    }

    private void testMerge(boolean ascending) {
        final IntegerArraySource valuesSource = new IntegerArraySource();
        final LongArraySource keySource = new LongArraySource();

        final int chunkSize = 1000;
        final int chunkCount = 100;
        int totalSize = chunkSize * chunkCount;

        try (final WritableIntChunk<Values> allValues = WritableIntChunk.makeWritableChunk(totalSize);
                final WritableLongChunk<RowKeys> allKeys = WritableLongChunk.makeWritableChunk(totalSize)) {

            for (int chunk = 0; chunk < chunkCount; ++chunk) {
                final int sizeAfterAddition = (chunk + 1) * chunkSize;

                try (final WritableIntChunk<Values> valuesChunk = WritableIntChunk.makeWritableChunk(chunkSize);
                        final WritableLongChunk<RowKeys> keysChunk = WritableLongChunk.makeWritableChunk(chunkSize)) {

                    final Random random = new Random(0);

                    for (int ii = 0; ii < chunkSize; ++ii) {
                        valuesChunk.set(ii, MegaMergeTestUtils.getRandomInt(random));
                        keysChunk.set(ii, chunk * chunkSize + ii);
                    }

                    MegaMergeTestUtils.doSort(ascending, chunkSize, valuesChunk, keysChunk);

                    if (ascending) {
                        IntLongMegaMergeKernel.merge(keySource, valuesSource, 0, sizeAfterAddition - chunkSize,
                                keysChunk, valuesChunk);
                    } else {
                        IntLongMegaMergeDescendingKernel.merge(keySource, valuesSource, 0,
                                sizeAfterAddition - chunkSize, keysChunk, valuesChunk);
                    }

                    allValues.setSize(sizeAfterAddition);
                    allKeys.setSize(sizeAfterAddition);
                    allValues.copyFromChunk(valuesChunk, 0, chunk * chunkSize, chunkSize);
                    allKeys.copyFromChunk(keysChunk, 0, chunk * chunkSize, chunkSize);
                }

                MegaMergeTestUtils.doSort(ascending, chunkSize * chunkCount, allValues, allKeys);

                try (final ChunkSource.GetContext valueContext = valuesSource.makeGetContext(sizeAfterAddition);
                        final ChunkSource.GetContext keyContext = keySource.makeGetContext(sizeAfterAddition)) {
                    final RowSequence rowSequence = RowSequenceFactory.forRange(0, sizeAfterAddition - 1);


                    final IntChunk<Values> checkValues =
                            valuesSource.getChunk(valueContext, rowSequence).asIntChunk();
                    final LongChunk<Values> checkKeys = keySource.getChunk(keyContext, rowSequence).asLongChunk();

                    assertEquals(checkValues.size(), allValues.size());
                    int firstDifferentValue = IntChunkEquals.firstDifference(checkValues, allValues);
                    if (firstDifferentValue < checkValues.size()) {
                        System.out.println("Expected Values:\n" + ChunkUtils.dumpChunk(allValues));
                        System.out.println("Actual Values:\n" + ChunkUtils.dumpChunk(checkValues));
                    }
                    assertEquals(allValues.size(), firstDifferentValue);

                    int firstDifferentKey = LongChunkEquals.firstDifference(checkKeys, allKeys);
                    if (firstDifferentKey < checkKeys.size()) {
                        System.out.println("Expected Indices:\n" + ChunkUtils.dumpChunk(allKeys));
                        System.out.println("Actual Indices:\n" + ChunkUtils.dumpChunk(checkKeys));
                    }
                    assertEquals(allKeys.size(), firstDifferentKey);
                }
            }

        }
    }
}
