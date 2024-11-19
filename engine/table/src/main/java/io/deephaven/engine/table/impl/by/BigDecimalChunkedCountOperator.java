//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.api.agg.util.AggCountType;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;

class BigDecimalChunkedCountOperator extends BaseChunkedCountOperator {

    private final AggCountType.BigDecimalCountFunction countFunction;

    /**
     * Construct a count aggregation operator that tests individual data values.
     *
     * @param resultName The name of the result column
     */
    BigDecimalChunkedCountOperator(
            @NotNull final String resultName,
            @NotNull final AggCountType countType) {
        super(resultName);
        this.countFunction = AggCountType.getBigDecimalCountFunction(countType);
    }

    @Override
    protected int doCount(int chunkStart, int chunkSize, Chunk<? extends Values> values) {
        final ObjectChunk<BigDecimal, ? extends Values> valueChunk = values.asObjectChunk();
        int valueCount = 0;
        for (int ii = 0; ii < chunkSize; ++ii) {
            if (countFunction.count(valueChunk.get(chunkStart + ii))) {
                valueCount++;
            }
        }
        return valueCount;
    }
}
