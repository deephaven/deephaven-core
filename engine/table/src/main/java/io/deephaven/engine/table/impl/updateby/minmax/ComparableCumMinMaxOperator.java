//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.minmax;

import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectBinaryOperator;
import org.jetbrains.annotations.NotNull;

public final class ComparableCumMinMaxOperator<T extends Comparable<T>> extends BaseObjectBinaryOperator<T> {
    private final boolean isMax;

    public ComparableCumMinMaxOperator(@NotNull final MatchPair inputPair,
            final boolean isMax,
            final Class<T> colType) {
        super(inputPair, new String[] {inputPair.rightColumn}, colType);
        this.isMax = isMax;
    }

    @Override
    public UpdateByOperator copy() {
        return new ComparableCumMinMaxOperator<>(pair, isMax, colType);
    }

    @Override
    protected T doOperation(T bucketCurVal, T chunkCurVal) {
        if ((isMax && chunkCurVal.compareTo(bucketCurVal) > 0) ||
                (!isMax && chunkCurVal.compareTo(bucketCurVal) < 0)) {
            return chunkCurVal;
        }

        return bucketCurVal;
    }
}
