//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestCharLongMegaMerge and run "./gradlew replicateSortKernelTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sort.megamerge;

import io.deephaven.engine.table.ChunkSource;
import io.deephaven.chunk.util.hashing.FloatChunkEquals;
import io.deephaven.chunk.util.hashing.LongChunkEquals;
import io.deephaven.engine.table.impl.sources.FloatArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Random;

public class TestFloatLongMegaMerge {
    @Test
    public void testMergeAscending() {
        testMerge(true);
    }

    @Test
    public void testMergeDescending() {
        testMerge(false);
    }

    private void testMerge(boolean ascending) {
        final FloatArraySource valuesSource = new FloatArraySource();
        final LongArraySource keySource = new LongArraySource();

        final int chunkSize = 1000;
        final int chunkCount = 100;
        int totalSize = chunkSize * chunkCount;

        try (final WritableFloatChunk<Values> allValues = WritableFloatChunk.makeWritableChunk(totalSize);
                final WritableLongChunk<RowKeys> allKeys = WritableLongChunk.makeWritableChunk(totalSize)) {

            for (int chunk = 0; chunk < chunkCount; ++chunk) {
                final int sizeAfterAddition = (chunk + 1) * chunkSize;

                try (final WritableFloatChunk<Values> valuesChunk = WritableFloatChunk.makeWritableChunk(chunkSize);
                        final WritableLongChunk<RowKeys> keysChunk = WritableLongChunk.makeWritableChunk(chunkSize)) {

                    final Random random = new Random(0);

                    for (int ii = 0; ii < chunkSize; ++ii) {
                        valuesChunk.set(ii, MegaMergeTestUtils.getRandomFloat(random));
                        keysChunk.set(ii, chunk * chunkSize + ii);
                    }

                    MegaMergeTestUtils.doSort(ascending, chunkSize, valuesChunk, keysChunk);

                    if (ascending) {
                        FloatLongMegaMergeKernel.merge(keySource, valuesSource, 0, sizeAfterAddition - chunkSize,
                                keysChunk, valuesChunk);
                    } else {
                        FloatLongMegaMergeDescendingKernel.merge(keySource, valuesSource, 0,
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


                    final FloatChunk<Values> checkValues =
                            valuesSource.getChunk(valueContext, rowSequence).asFloatChunk();
                    final LongChunk<Values> checkKeys = keySource.getChunk(keyContext, rowSequence).asLongChunk();

                    TestCase.assertEquals(checkValues.size(), allValues.size());
                    int firstDifferentValue = FloatChunkEquals.firstDifference(checkValues, allValues);
                    if (firstDifferentValue < checkValues.size()) {
                        System.out.println("Expected Values:\n" + ChunkUtils.dumpChunk(allValues));
                        System.out.println("Actual Values:\n" + ChunkUtils.dumpChunk(checkValues));
                    }
                    TestCase.assertEquals(allValues.size(), firstDifferentValue);

                    int firstDifferentKey = LongChunkEquals.firstDifference(checkKeys, allKeys);
                    if (firstDifferentKey < checkKeys.size()) {
                        System.out.println("Expected Indices:\n" + ChunkUtils.dumpChunk(allKeys));
                        System.out.println("Actual Indices:\n" + ChunkUtils.dumpChunk(checkKeys));
                    }
                    TestCase.assertEquals(allKeys.size(), firstDifferentKey);
                }
            }

        }
    }
}
