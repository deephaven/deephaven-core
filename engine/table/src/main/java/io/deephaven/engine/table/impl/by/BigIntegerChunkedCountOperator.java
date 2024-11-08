//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.api.agg.spec.AggSpecCountValues;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import org.jetbrains.annotations.NotNull;

import java.math.BigInteger;

class BigIntegerChunkedCountOperator extends BaseChunkedCountOperator {

    private final BigIntegerCountFunction countFunction;

    /**
     * Construct a count aggregation operator that tests individual data values.
     *
     * @param resultName The name of the result column
     */
    BigIntegerChunkedCountOperator(
            @NotNull final String resultName,
            @NotNull final AggSpecCountValues.AggCountType countType) {
        super(resultName);
        this.countFunction = getBigIntegerCountFunction(countType);
    }

    @Override
    protected int doCount(int chunkStart, int chunkSize, Chunk<? extends Values> values) {
        final ObjectChunk<BigInteger, ? extends Values> valueChunk = values.asObjectChunk();
        int valueCount = 0;
        for (int ii = 0; ii < chunkSize; ++ii) {
            if (countFunction.count(valueChunk.get(chunkStart + ii))) {
                valueCount++;
            }
        }
        return valueCount;
    }
}
