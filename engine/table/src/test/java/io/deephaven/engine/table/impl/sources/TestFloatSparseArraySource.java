//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestCharacterSparseArraySource and run "./gradlew replicateSourceAndChunkTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.ArrayGenerator;
import io.deephaven.chunk.Chunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.WritableColumnSource;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Random;

import static junit.framework.TestCase.assertFalse;

public class TestFloatSparseArraySource extends AbstractFloatColumnSourceTest {
    @NotNull
    @Override
    FloatSparseArraySource makeTestSource() {
        return new FloatSparseArraySource();
    }

    @Test
    public void confirmAliasingForbidden() {
        final Random rng = new Random(438269476);
        final int arraySize = 100;
        final int rangeStart = 20;
        final int rangeEnd = 80;
        final FloatSparseArraySource source = makeTestSource();
        source.ensureCapacity(arraySize);

        final float[] data = ArrayGenerator.randomFloats(rng, arraySize);
        for (int ii = 0; ii < data.length; ++ii) {
            source.set(ii, data[ii]);
        }
        // super hack
        final float[] peekedBlock = source.ensureBlock(0, 0, 0);

        try (RowSet srcKeys = RowSetFactory.fromRange(rangeStart, rangeEnd)) {
            try (RowSet destKeys = RowSetFactory.fromRange(rangeStart + 1, rangeEnd + 1)) {
                try (ChunkSource.GetContext srcContext = source.makeGetContext(arraySize)) {
                    try (ChunkSink.FillFromContext destContext = source.makeFillFromContext(arraySize)) {
                        Chunk chunk = source.getChunk(srcContext, srcKeys);
                        if (chunk.isAlias(peekedBlock)) {
                            // If the ArraySource gives out aliases of its blocks, then it should throw when we try to
                            // fill from that aliased chunk
                            boolean testFailed;
                            try {
                                source.fillFromChunk(destContext, chunk, destKeys);
                                testFailed = true;
                            } catch (UnsupportedOperationException uoe) {
                                testFailed = false;
                            }
                            assertFalse(testFailed);
                        }
                    }
                }
            }
        }
    }
}
