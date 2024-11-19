//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkedCountOperator and run "./gradlew replicateOperators" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by;

import io.deephaven.api.agg.util.AggCountType;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import org.jetbrains.annotations.NotNull;

class IntChunkedCountOperator extends BaseChunkedCountOperator {

    private final AggCountType.IntCountFunction countFunction;

    /**
     * Construct a count aggregation operator that tests individual data values.
     *
     * @param resultName The name of the result column
     */
    IntChunkedCountOperator(
            @NotNull final String resultName,
            @NotNull final AggCountType countType) {
        super(resultName);
        this.countFunction = AggCountType.getIntCountFunction(countType);
    }

    @Override
    protected int doCount(int chunkStart, int chunkSize, Chunk<? extends Values> values) {
        final IntChunk<? extends Values> asIntChunk = values.asIntChunk();
        int valueCount = 0;
        for (int ii = 0; ii < chunkSize; ++ii) {
            if (countFunction.count(asIntChunk.get(chunkStart + ii))) {
                valueCount++;
            }
        }
        return valueCount;
    }
}
