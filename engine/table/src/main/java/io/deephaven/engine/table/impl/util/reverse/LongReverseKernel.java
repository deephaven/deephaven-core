/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharReverseKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.util.reverse;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;

public class LongReverseKernel {
    public static <T extends Any> void reverse(WritableLongChunk<T> chunkToReverse) {
        for (int ii = 0; ii < chunkToReverse.size() / 2; ++ii) {
            final int jj = chunkToReverse.size() - ii - 1;
            final long t = chunkToReverse.get(jj);
            chunkToReverse.set(jj, chunkToReverse.get(ii));
            chunkToReverse.set(ii, t);
        }
    }

    private static class LongReverseKernelContext implements ReverseKernel {
        @Override
        public <T extends Any> void reverse(WritableChunk<T> chunkToReverse) {
            LongReverseKernel.reverse(chunkToReverse.asWritableLongChunk());
        }
    }

    public final static ReverseKernel INSTANCE = new LongReverseKernelContext();
}
