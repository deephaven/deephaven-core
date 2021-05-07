/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharReverseKernel and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.utils.reverse;

import io.deephaven.db.v2.sources.chunk.*;

import static io.deephaven.db.v2.sources.chunk.Attributes.*;

public class ByteReverseKernel {
    public static <T extends Any> void reverse(WritableByteChunk<T> chunkToReverse) {
        for (int ii = 0; ii < chunkToReverse.size() / 2; ++ii) {
            final int jj = chunkToReverse.size() - ii - 1;
            final byte t = chunkToReverse.get(jj);
            chunkToReverse.set(jj, chunkToReverse.get(ii));
            chunkToReverse.set(ii, t);
        }
    }

    private static class ByteReverseKernelContext implements ReverseKernel {
        @Override
        public <T extends Any> void reverse(WritableChunk<T> chunkToReverse) {
            ByteReverseKernel.reverse(chunkToReverse.asWritableByteChunk());
        }
    }

    public final static ReverseKernel INSTANCE = new ByteReverseKernelContext();
}
