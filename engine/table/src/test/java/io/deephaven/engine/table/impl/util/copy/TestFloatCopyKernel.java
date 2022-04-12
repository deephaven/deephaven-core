/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharCopyKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.util.copy;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.WritableFloatChunk;
import org.junit.Test;

public class TestFloatCopyKernel {
    /**
     * There was an edge case (IDS-5504) leading to an array out of bounds exception caused by an inconsistency between
     * closed and half-open intervals.
     */
    @Test
    public void confirmEdgecaseFixed() {
        final WritableFloatChunk<Values> output = WritableFloatChunk.makeWritableChunk(64);
        final float[] baseInput = new float[64];
        final float[] overInput = new float[64];
        final long[] useOverInput = new long[1];
        useOverInput[0] = 1L << 63;  // This is the edge case
        FloatCopyKernel.conditionalCopy(output, baseInput, overInput, useOverInput, 0, 0, 64);
    }
}
