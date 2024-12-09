//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.api.agg.util.AggCountType;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import org.jetbrains.annotations.NotNull;

class CharChunkedCountOperator extends BaseChunkedCountOperator {

    private final AggCountType.CharCountFunction countFunction;

    /**
     * Construct a count aggregation operator that tests individual data values.
     *
     * @param resultName The name of the result column
     */
    CharChunkedCountOperator(
            @NotNull final String resultName,
            @NotNull final AggCountType countType) {
        super(resultName);
        this.countFunction = AggCountType.getCharCountFunction(countType);
    }

    @Override
    protected int doCount(int chunkStart, int chunkSize, Chunk<? extends Values> values) {
        final CharChunk<? extends Values> asCharChunk = values.asCharChunk();
        int valueCount = 0;
        for (int ii = 0; ii < chunkSize; ++ii) {
            if (countFunction.count(asCharChunk.get(chunkStart + ii))) {
                valueCount++;
            }
        }
        return valueCount;
    }
}
