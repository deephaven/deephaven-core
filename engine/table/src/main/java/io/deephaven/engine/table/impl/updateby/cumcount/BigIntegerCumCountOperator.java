//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.cumcount;

import io.deephaven.api.updateby.spec.CumCountSpec;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import org.jetbrains.annotations.NotNull;

import java.math.BigInteger;

public class BigIntegerCumCountOperator extends BaseCumCountOperator {

    @Override
    protected ValueCountFunction createValueCountFunction(
            final Chunk<? extends Values> chunk,
            final CumCountSpec.CumCountType countType) {

        final ObjectChunk<BigInteger, ? extends Values> valueChunk = chunk.asObjectChunk();
        switch (countType) {
            case NON_NULL:
            case FINITE:
                return index -> valueChunk.get(index) != null;
            case NULL:
                return index -> valueChunk.get(index) == null;
            case NEGATIVE:
                return index -> {
                    final BigInteger val = valueChunk.get(index);
                    return val != null && val.signum() < 0;
                };
            case POSITIVE:
                return index -> {
                    final BigInteger val = valueChunk.get(index);
                    return val != null && val.signum() > 0;
                };
            case ZERO:
                return index -> {
                    final BigInteger val = valueChunk.get(index);
                    return val != null && val.signum() == 0;
                };
            case NAN:
            case INFINITE:
                return index -> false;
        }
        throw new IllegalStateException("BigIntegerCumCountOperator - Unsupported CumCountType encountered: "
                + countType + " for type:" + columnType);

    }

    public BigIntegerCumCountOperator(
            @NotNull final MatchPair pair,
            @NotNull final CumCountSpec spec,
            @NotNull final Class<?> columnType) {
        super(pair, spec, columnType);
    }

    @Override
    public UpdateByOperator copy() {
        return new BigIntegerCumCountOperator(pair, spec, columnType);
    }
}
