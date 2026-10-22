//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.fill;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FillFromElementKernelTest {
    /**
     * Fill positions 1 through 3 of a five element chunk with the second element of a source chunk, and check that
     * exactly those positions hold it.
     */
    private static void checkFill(final ChunkType chunkType, final Object fillValue, final Object other) {
        try (final WritableChunk<Values> src = chunkType.makeWritableChunk(3);
                final WritableChunk<Values> dest = chunkType.makeWritableChunk(5)) {
            src.fillWithBoxedValue(0, 3, other);
            src.fillWithBoxedValue(1, 1, fillValue);
            dest.fillWithBoxedValue(0, 5, other);

            FillFromElementKernel.makeFillFromElementKernel(chunkType).fillFromElement(dest, 1, 3, src, 1);

            final Object[] expected = {other, fillValue, fillValue, fillValue, other};
            for (int ii = 0; ii < expected.length; ++ii) {
                assertEquals(chunkType + " position " + ii, expected[ii], boxed(dest, ii));
            }
        }
    }

    private static Object boxed(final WritableChunk<Values> chunk, final int position) {
        switch (chunk.getChunkType()) {
            case Boolean:
                return chunk.asBooleanChunk().get(position);
            case Char:
                return chunk.asCharChunk().get(position);
            case Byte:
                return chunk.asByteChunk().get(position);
            case Short:
                return chunk.asShortChunk().get(position);
            case Int:
                return chunk.asIntChunk().get(position);
            case Long:
                return chunk.asLongChunk().get(position);
            case Float:
                return chunk.asFloatChunk().get(position);
            case Double:
                return chunk.asDoubleChunk().get(position);
            default:
                return chunk.asObjectChunk().get(position);
        }
    }

    @Test
    public void testFillFromElement() {
        checkFill(ChunkType.Boolean, true, false);
        checkFill(ChunkType.Char, 'x', 'a');
        checkFill(ChunkType.Byte, (byte) 7, (byte) 1);
        checkFill(ChunkType.Short, (short) 7, (short) 1);
        checkFill(ChunkType.Int, 7, 1);
        checkFill(ChunkType.Long, 7L, 1L);
        checkFill(ChunkType.Float, 7.0f, 1.0f);
        checkFill(ChunkType.Double, 7.0, 1.0);
        checkFill(ChunkType.Object, "seven", "one");
    }
}
