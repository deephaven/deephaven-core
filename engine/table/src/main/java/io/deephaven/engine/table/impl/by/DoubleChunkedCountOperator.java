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

class DoubleChunkedCountOperator extends BaseChunkedCountOperator {

    private final AggCountType.DoubleCountFunction countFunction;

    /**
     * Construct a count aggregation operator that tests individual data values.
     *
     * @param resultName The name of the result column
     */
    DoubleChunkedCountOperator(
            @NotNull final String resultName,
            @NotNull final AggCountType countType) {
        super(resultName);
        this.countFunction = AggCountType.getDoubleCountFunction(countType);
    }

    @Override
    protected int doCount(int chunkStart, int chunkSize, Chunk<? extends Values> values) {
        final DoubleChunk<? extends Values> asDoubleChunk = values.asDoubleChunk();
        int valueCount = 0;
        for (int ii = 0; ii < chunkSize; ++ii) {
            if (countFunction.count(asDoubleChunk.get(chunkStart + ii))) {
                valueCount++;
            }
        }
        return valueCount;
    }
}
