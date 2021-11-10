/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharReverseKernel and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.utils.reverse;

import io.deephaven.engine.chunk.*;

import static io.deephaven.engine.chunk.Attributes.*;

public class ShortReverseKernel {
    public static <T extends Any> void reverse(WritableShortChunk<T> chunkToReverse) {
        for (int ii = 0; ii < chunkToReverse.size() / 2; ++ii) {
            final int jj = chunkToReverse.size() - ii - 1;
            final short t = chunkToReverse.get(jj);
            chunkToReverse.set(jj, chunkToReverse.get(ii));
            chunkToReverse.set(ii, t);
        }
    }

    private static class ShortReverseKernelContext implements ReverseKernel {
        @Override
        public <T extends Any> void reverse(WritableChunk<T> chunkToReverse) {
            ShortReverseKernel.reverse(chunkToReverse.asWritableShortChunk());
        }
    }

    public final static ReverseKernel INSTANCE = new ShortReverseKernelContext();
}
