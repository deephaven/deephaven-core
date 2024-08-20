//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharDupExpandKernel and run "./gradlew replicateDupExpandKernel" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.join.dupexpand;

import java.util.Objects;
import io.deephaven.util.compare.ObjectComparisons;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;

public class ObjectDupExpandKernel implements DupExpandKernel {
    public static final ObjectDupExpandKernel INSTANCE = new ObjectDupExpandKernel();

    private ObjectDupExpandKernel() {} // use through the instance

    @Override
    public void expandDuplicates(int expandedSize, WritableChunk<? extends Any> chunkToExpand,
            IntChunk<ChunkLengths> keyRunLengths) {
        expandDuplicates(expandedSize, chunkToExpand.asWritableObjectChunk(), keyRunLengths);
    }

    public static void expandDuplicates(int expandedSize, WritableObjectChunk<Object, ? extends Any> chunkToExpand,
            IntChunk<ChunkLengths> keyRunLengths) {
        if (expandedSize == 0) {
            return;
        }

        int wpos = expandedSize;
        int rpos = chunkToExpand.size() - 1;
        chunkToExpand.setSize(expandedSize);

        for (; rpos >= 0; --rpos) {
            final int len = keyRunLengths.get(rpos);
            chunkToExpand.fillWithValue(wpos - len, len, chunkToExpand.get(rpos));
            wpos -= len;
        }
    }
}
