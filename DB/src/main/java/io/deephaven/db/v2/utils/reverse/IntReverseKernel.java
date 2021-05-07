/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharReverseKernel and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.utils.reverse;

import io.deephaven.db.v2.sources.chunk.*;

import static io.deephaven.db.v2.sources.chunk.Attributes.*;

public class IntReverseKernel {
    public static <T extends Any> void reverse(WritableIntChunk<T> chunkToReverse) {
        for (int ii = 0; ii < chunkToReverse.size() / 2; ++ii) {
            final int jj = chunkToReverse.size() - ii - 1;
            final int t = chunkToReverse.get(jj);
            chunkToReverse.set(jj, chunkToReverse.get(ii));
            chunkToReverse.set(ii, t);
        }
    }

    private static class IntReverseKernelContext implements ReverseKernel {
        @Override
        public <T extends Any> void reverse(WritableChunk<T> chunkToReverse) {
            IntReverseKernel.reverse(chunkToReverse.asWritableIntChunk());
        }
    }

    public final static ReverseKernel INSTANCE = new IntReverseKernelContext();
}
