/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharDupExpandKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.join.dupexpand;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;

public class FloatDupExpandKernel implements DupExpandKernel {
    public static final FloatDupExpandKernel INSTANCE = new FloatDupExpandKernel();

    private FloatDupExpandKernel() {} // use through the instance

    @Override
    public void expandDuplicates(int expandedSize, WritableChunk<? extends Any> chunkToExpand, IntChunk<ChunkLengths> keyRunLengths) {
        expandDuplicates(expandedSize, chunkToExpand.asWritableFloatChunk(), keyRunLengths);
    }

    public static void expandDuplicates(int expandedSize, WritableFloatChunk<? extends Any> chunkToExpand, IntChunk<ChunkLengths> keyRunLengths) {
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
