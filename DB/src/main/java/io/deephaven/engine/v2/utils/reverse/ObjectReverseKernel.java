package io.deephaven.engine.v2.utils.reverse;

import io.deephaven.engine.structures.chunk.*;

import static io.deephaven.engine.structures.chunk.Attributes.*;

public class ObjectReverseKernel {
    public static <T extends Any> void reverse(WritableObjectChunk<Object, T> chunkToReverse) {
        for (int ii = 0; ii < chunkToReverse.size() / 2; ++ii) {
            final int jj = chunkToReverse.size() - ii - 1;
            final Object t = chunkToReverse.get(jj);
            chunkToReverse.set(jj, chunkToReverse.get(ii));
            chunkToReverse.set(ii, t);
        }
    }

    private static class ObjectReverseKernelContext implements ReverseKernel {
        @Override
        public <T extends Any> void reverse(WritableChunk<T> chunkToReverse) {
            ObjectReverseKernel.reverse(chunkToReverse.asWritableObjectChunk());
        }
    }

    public final static ReverseKernel INSTANCE = new ObjectReverseKernelContext();
}
