package io.deephaven.engine.table.impl.updateby.minmax;

import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectBinaryOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class ComparableCumMinMaxOperator<T extends Comparable<T>> extends BaseObjectBinaryOperator<T> {
    private final boolean isMax;

    public ComparableCumMinMaxOperator(@NotNull final MatchPair inputPair,
            final boolean isMax,
            @Nullable final RowRedirection rowRedirection,
            final Class<T> colType) {
        super(inputPair, new String[] {inputPair.rightColumn}, rowRedirection, colType);
        this.isMax = isMax;
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
