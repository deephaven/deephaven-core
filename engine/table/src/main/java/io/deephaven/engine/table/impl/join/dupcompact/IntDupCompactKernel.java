/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharDupCompactKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.join.dupcompact;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.jetbrains.annotations.NotNull;

public class IntDupCompactKernel implements DupCompactKernel {
    static final IntDupCompactKernel INSTANCE = new IntDupCompactKernel();

    private IntDupCompactKernel() {} // use through the instance

    @Override
    public int compactDuplicates(
            @NotNull final WritableChunk<? extends Any> chunkToCompact,
            @NotNull final WritableLongChunk<RowKeys> keyIndices) {
        return compactDuplicates(chunkToCompact.asWritableIntChunk(), keyIndices);
    }

    private static int compactDuplicates(
            @NotNull final WritableIntChunk<? extends Any> chunkToCompact,
            @NotNull final WritableLongChunk<RowKeys> keyIndices) {
        final int inputSize = chunkToCompact.size();
        if (inputSize == 0) {
            return -1;
        }

        int wpos = 0;
        int rpos = 0;

        int last = chunkToCompact.get(0);

        while (rpos < inputSize) {
            final int current = chunkToCompact.get(rpos);
            if (!leq(last, current)) {
                return rpos;
            }
            last = current;

            while (rpos < inputSize - 1 && eq(current, chunkToCompact.get(rpos + 1))) {
                rpos++;
            }
            chunkToCompact.set(wpos, current);
            keyIndices.set(wpos, keyIndices.get(rpos));
            rpos++;
            wpos++;
        }
        chunkToCompact.setSize(wpos);
        keyIndices.setSize(wpos);

        return -1;
    }

    // region comparison functions
    private static int doComparison(int lhs, int rhs) {
        return Integer.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean leq(int lhs, int rhs) {
        return doComparison(lhs, rhs) <= 0;
    }

    private static boolean eq(int lhs, int rhs) {
        // region equality function
        return lhs == rhs;
        // endregion equality function
    }
}
