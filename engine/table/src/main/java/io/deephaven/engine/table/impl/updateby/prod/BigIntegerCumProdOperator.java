package io.deephaven.engine.table.impl.updateby.prod;

import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectBinaryOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigInteger;

public final class BigIntegerCumProdOperator extends BaseObjectBinaryOperator<BigInteger> {
    public BigIntegerCumProdOperator(@NotNull final MatchPair inputPair,
                                     @NotNull final UpdateBy.UpdateByRedirectionContext redirContext) {
        super(BigInteger.class, inputPair, new String[] { inputPair.rightColumn }, redirContext);
    }

    @Override
    protected BigInteger doOperation(BigInteger bucketCurVal, BigInteger chunkCurVal) {
        return bucketCurVal.multiply(chunkCurVal);
    }
}
