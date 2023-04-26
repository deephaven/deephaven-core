/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharDupCompactKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.join.dupcompact;

import java.util.Objects;

import java.util.Objects;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.jetbrains.annotations.NotNull;

public class ObjectReverseDupCompactKernel implements DupCompactKernel {
    static final ObjectReverseDupCompactKernel INSTANCE = new ObjectReverseDupCompactKernel();

    private ObjectReverseDupCompactKernel() {} // use through the instance

    @Override
    public int compactDuplicates(
            @NotNull final WritableChunk<? extends Any> chunkToCompact,
            @NotNull final WritableLongChunk<RowKeys> keyIndices) {
        return compactDuplicates(chunkToCompact.asWritableObjectChunk(), keyIndices);
    }

    private static int compactDuplicates(
            @NotNull final WritableObjectChunk<Object, ? extends Any> chunkToCompact,
            @NotNull final WritableLongChunk<RowKeys> keyIndices) {
        final int inputSize = chunkToCompact.size();
        if (inputSize == 0) {
            return -1;
        }

        int wpos = 0;
        int rpos = 0;

        Object last = chunkToCompact.get(0);

        while (rpos < inputSize) {
            final Object current = chunkToCompact.get(rpos);
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
    // descending comparison
    private static int doComparison(Object lhs, Object rhs) {
        if (lhs == rhs) {
            return 0;
        }
        if (lhs == null) {
            return 1;
        }
        if (rhs == null) {
            return -1;
        }
        //noinspection unchecked
        return ((Comparable)rhs).compareTo(lhs);
    }
    // endregion comparison functions

    private static boolean leq(Object lhs, Object rhs) {
        return doComparison(lhs, rhs) <= 0;
    }

    private static boolean eq(Object lhs, Object rhs) {
        // region equality function
        return Objects.equals(lhs, rhs);
        // endregion equality function
    }
}
