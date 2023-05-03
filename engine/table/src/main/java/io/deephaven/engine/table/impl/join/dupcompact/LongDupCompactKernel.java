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
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.jetbrains.annotations.NotNull;

public class LongDupCompactKernel implements DupCompactKernel {

    static final LongDupCompactKernel INSTANCE = new LongDupCompactKernel();

    private LongDupCompactKernel() {
        // Use the singleton INSTANCE
    }

    @Override
    public int compactDuplicates(
            @NotNull final WritableChunk<? extends Any> chunkToCompact,
            @NotNull final WritableLongChunk<RowKeys> rowKeys) {
        return compactDuplicates(chunkToCompact.asWritableLongChunk(), rowKeys);
    }

    @Override
    public int compactDuplicatesPreferFirst(
            @NotNull final WritableChunk<? extends Any> chunkToCompact,
            @NotNull final WritableIntChunk<ChunkPositions> chunkPositions) {
        return compactDuplicatesPreferFirst(chunkToCompact.asWritableLongChunk(), chunkPositions);
    }

    private static int compactDuplicates(
            @NotNull final WritableLongChunk<? extends Any> chunkToCompact,
            @NotNull final WritableLongChunk<RowKeys> rowKeys) {
        final int inputSize = chunkToCompact.size();
        if (inputSize == 0) {
            return -1;
        }

        int wpos = 0;
        int rpos = 0;

        long last = chunkToCompact.get(0);

        while (rpos < inputSize) {
            final long current = chunkToCompact.get(rpos);
            if (!leq(last, current)) {
                return rpos;
            }
            last = current;

            while (rpos < inputSize - 1 && eq(current, chunkToCompact.get(rpos + 1))) {
                rpos++;
            }
            chunkToCompact.set(wpos, current);
            rowKeys.set(wpos, rowKeys.get(rpos));
            rpos++;
            wpos++;
        }
        chunkToCompact.setSize(wpos);
        rowKeys.setSize(wpos);

        return -1;
    }

    private static int compactDuplicatesPreferFirst(
            @NotNull final WritableLongChunk<? extends Any> chunkToCompact,
            @NotNull final WritableIntChunk<ChunkPositions> chunkPositions) {
        final int inputSize = chunkToCompact.size();
        if (inputSize == 0) {
            return -1;
        }

        int wpos = 0;
        int rpos = 0;

        long last = chunkToCompact.get(0);

        while (rpos < inputSize) {
            final long current = chunkToCompact.get(rpos);
            if (!leq(last, current)) {
                return rpos;
            }
            last = current;

            chunkToCompact.set(wpos, current);
            chunkPositions.set(wpos, chunkPositions.get(rpos));
            rpos++;
            wpos++;

            while (rpos < inputSize && eq(current, chunkToCompact.get(rpos))) {
                rpos++;
            }
        }
        chunkToCompact.setSize(wpos);
        chunkPositions.setSize(wpos);

        return -1;
    }

    // region comparison functions
    private static int doComparison(long lhs, long rhs) {
        return Long.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean leq(long lhs, long rhs) {
        return doComparison(lhs, rhs) <= 0;
    }

    private static boolean eq(long lhs, long rhs) {
        // region equality function
        return lhs == rhs;
        // endregion equality function
    }
}
