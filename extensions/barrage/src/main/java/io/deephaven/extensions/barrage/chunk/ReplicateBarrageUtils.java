/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.extensions.barrage.chunk;

import io.deephaven.compilertools.ReplicatePrimitiveCode;
import io.deephaven.extensions.barrage.chunk.array.CharArrayExpansionKernel;

import java.io.IOException;

public class ReplicateBarrageUtils {
    public static void main(final String[] args) throws IOException {
        ReplicatePrimitiveCode.charToAllButBoolean(CharChunkInputStreamGenerator.class,
                ReplicatePrimitiveCode.MAIN_SRC);
        ReplicatePrimitiveCode.charToAll(CharArrayExpansionKernel.class, ReplicatePrimitiveCode.MAIN_SRC);
    }
}
