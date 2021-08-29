/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharLongMegaMerge and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sort.megamerge;

import io.deephaven.engine.v2.hashing.IntChunkEquals;
import io.deephaven.engine.v2.hashing.LongChunkEquals;
import io.deephaven.engine.v2.sources.IntegerArraySource;
import io.deephaven.engine.v2.sources.LongArraySource;
import io.deephaven.engine.structures.chunk.*;
import io.deephaven.engine.structures.chunk.Attributes.Values;
import io.deephaven.engine.v2.utils.ChunkUtils;
import io.deephaven.engine.v2.utils.OrderedKeys;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Random;

public class TestIntLongMegaMerge {
    @Test
    public void testMergeAscending() {
        testMerge(true);
    }

    @Test
    public void testMergeDescending() {
        testMerge(false);
    }

    private void testMerge(boolean ascending) {
        final IntegerArraySource valuesSource = new IntegerArraySource();
        final LongArraySource keySource = new LongArraySource();

        final int chunkSize = 1000;
        final int chunkCount = 100;
        int totalSize = chunkSize * chunkCount;

        try (final WritableIntChunk<Values> allValues = WritableIntChunk.makeWritableChunk(totalSize);
             final WritableLongChunk<Attributes.KeyIndices> allKeys = WritableLongChunk.makeWritableChunk(totalSize)) {

            for (int chunk = 0; chunk < chunkCount; ++chunk) {
                final int sizeAfterAddition = (chunk + 1) * chunkSize;

                try (final WritableIntChunk<Values> valuesChunk = WritableIntChunk.makeWritableChunk(chunkSize);
                     final WritableLongChunk<Attributes.KeyIndices> keysChunk = WritableLongChunk.makeWritableChunk(chunkSize)) {

                    final Random random = new Random(0);

                    for (int ii = 0; ii < chunkSize; ++ii) {
                        valuesChunk.set(ii, MegaMergeTestUtils.getRandomInt(random));
                        keysChunk.set(ii, chunk * chunkSize + ii);
                    }

                    MegaMergeTestUtils.doSort(ascending, chunkSize, valuesChunk, keysChunk);

                    if (ascending) {
                        IntLongMegaMergeKernel.merge(keySource, valuesSource, 0, sizeAfterAddition - chunkSize, keysChunk, valuesChunk);
                    } else {
                        IntLongMegaMergeDescendingKernel.merge(keySource, valuesSource, 0, sizeAfterAddition - chunkSize, keysChunk, valuesChunk);
                    }

                    allValues.setSize(sizeAfterAddition);
                    allKeys.setSize(sizeAfterAddition);
                    allValues.copyFromChunk(valuesChunk, 0, chunk * chunkSize, chunkSize);
                    allKeys.copyFromChunk(keysChunk, 0, chunk * chunkSize, chunkSize);
                }

                MegaMergeTestUtils.doSort(ascending, chunkSize * chunkCount, allValues, allKeys);

                try (final ChunkSource.GetContext valueContext = valuesSource.makeGetContext(sizeAfterAddition);
                     final ChunkSource.GetContext keyContext = keySource.makeGetContext(sizeAfterAddition)) {
                    final OrderedKeys orderedKeys = OrderedKeys.forRange(0, sizeAfterAddition - 1);


                    final IntChunk<Values> checkValues = valuesSource.getChunk(valueContext, orderedKeys).asIntChunk();
                    final LongChunk<Values> checkKeys = keySource.getChunk(keyContext, orderedKeys).asLongChunk();

                    TestCase.assertEquals(checkValues.size(), allValues.size());
                    int firstDifferentValue = IntChunkEquals.firstDifference(checkValues, allValues);
                    if (firstDifferentValue < checkValues.size()) {
                        System.out.println("Expected Values:\n" + ChunkUtils.dumpChunk(allValues));
                        System.out.println("Actual Values:\n" + ChunkUtils.dumpChunk(checkValues));
                    }
                    TestCase.assertEquals(allValues.size(), firstDifferentValue);

                    int firstDifferentKey = LongChunkEquals.firstDifference(checkKeys, allKeys);
                    if (firstDifferentKey < checkKeys.size()) {
                        System.out.println("Expected Keys:\n" + ChunkUtils.dumpChunk(allKeys));
                        System.out.println("Actual Keys:\n" + ChunkUtils.dumpChunk(checkKeys));
                    }
                    TestCase.assertEquals(allKeys.size(), firstDifferentKey);
                }
            }

        }
    }
}
