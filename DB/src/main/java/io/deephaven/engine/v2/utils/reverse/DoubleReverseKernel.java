/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharReverseKernel and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.utils.reverse;

import io.deephaven.engine.structures.chunk.*;

import static io.deephaven.engine.structures.chunk.Attributes.*;

public class DoubleReverseKernel {
    public static <T extends Any> void reverse(WritableDoubleChunk<T> chunkToReverse) {
        for (int ii = 0; ii < chunkToReverse.size() / 2; ++ii) {
            final int jj = chunkToReverse.size() - ii - 1;
            final double t = chunkToReverse.get(jj);
            chunkToReverse.set(jj, chunkToReverse.get(ii));
            chunkToReverse.set(ii, t);
        }
    }

    private static class DoubleReverseKernelContext implements ReverseKernel {
        @Override
        public <T extends Any> void reverse(WritableChunk<T> chunkToReverse) {
            DoubleReverseKernel.reverse(chunkToReverse.asWritableDoubleChunk());
        }
    }

    public final static ReverseKernel INSTANCE = new DoubleReverseKernelContext();
}
