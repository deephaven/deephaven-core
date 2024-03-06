//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharReverseKernel and run "./gradlew replicateReverseKernel" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.util.reverse;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;

public class BooleanReverseKernel {
    public static <T extends Any> void reverse(WritableBooleanChunk<T> chunkToReverse) {
        for (int ii = 0; ii < chunkToReverse.size() / 2; ++ii) {
            final int jj = chunkToReverse.size() - ii - 1;
            final boolean t = chunkToReverse.get(jj);
            chunkToReverse.set(jj, chunkToReverse.get(ii));
            chunkToReverse.set(ii, t);
        }
    }

    private static class BooleanReverseKernelContext implements ReverseKernel {
        @Override
        public <T extends Any> void reverse(WritableChunk<T> chunkToReverse) {
            BooleanReverseKernel.reverse(chunkToReverse.asWritableBooleanChunk());
        }
    }

    public final static ReverseKernel INSTANCE = new BooleanReverseKernelContext();
}
