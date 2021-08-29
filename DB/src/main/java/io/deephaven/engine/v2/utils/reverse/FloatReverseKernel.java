/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharReverseKernel and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.utils.reverse;

import io.deephaven.engine.structures.chunk.*;

import static io.deephaven.engine.structures.chunk.Attributes.*;

public class FloatReverseKernel {
    public static <T extends Any> void reverse(WritableFloatChunk<T> chunkToReverse) {
        for (int ii = 0; ii < chunkToReverse.size() / 2; ++ii) {
            final int jj = chunkToReverse.size() - ii - 1;
            final float t = chunkToReverse.get(jj);
            chunkToReverse.set(jj, chunkToReverse.get(ii));
            chunkToReverse.set(ii, t);
        }
    }

    private static class FloatReverseKernelContext implements ReverseKernel {
        @Override
        public <T extends Any> void reverse(WritableChunk<T> chunkToReverse) {
            FloatReverseKernel.reverse(chunkToReverse.asWritableFloatChunk());
        }
    }

    public final static ReverseKernel INSTANCE = new FloatReverseKernelContext();
}
