package io.deephaven.engine.table.impl.join.dupexpand;

import io.deephaven.engine.chunk.*;

public class CharDupExpandKernel implements DupExpandKernel {
    public static final CharDupExpandKernel INSTANCE = new CharDupExpandKernel();

    private CharDupExpandKernel() {} // use through the instance

    @Override
    public void expandDuplicates(int expandedSize, WritableChunk<? extends Attributes.Any> chunkToExpand, IntChunk<Attributes.ChunkLengths> keyRunLengths) {
        expandDuplicates(expandedSize, chunkToExpand.asWritableCharChunk(), keyRunLengths);
    }

    public static void expandDuplicates(int expandedSize, WritableCharChunk<? extends Attributes.Any> chunkToExpand, IntChunk<Attributes.ChunkLengths> keyRunLengths) {
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
