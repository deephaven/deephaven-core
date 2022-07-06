package io.deephaven.engine.table.impl.updateby.sum;

import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectBinaryOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.MathContext;

public final class BigDecimalCumSumOperator extends BaseObjectBinaryOperator<BigDecimal> {
    @NotNull
    private final MathContext mathContext;

    public BigDecimalCumSumOperator(@NotNull final MatchPair inputPair,
            @Nullable final RowRedirection rowRedirection,
            @NotNull final MathContext mathContext) {
        super(BigDecimal.class, inputPair, new String[] {inputPair.rightColumn}, rowRedirection);
        this.mathContext = mathContext;
    }

    @Override
    protected BigDecimal doOperation(BigDecimal bucketCurVal, BigDecimal chunkCurVal) {
        return bucketCurVal.add(chunkCurVal, mathContext);
    }
}
