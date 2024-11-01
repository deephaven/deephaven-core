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

import static io.deephaven.util.BooleanUtils.NULL_BOOLEAN_AS_BYTE;

public class BooleanCumCountOperator extends BaseCumCountOperator {
    @Override
    protected ValueCountFunction createValueCountFunction(
            final Chunk<? extends Values> chunk,
            final CumCountSpec.CumCountType countType) {

        final ByteChunk<?> valueChunk = chunk.asByteChunk();
        switch (countType) {
            case NON_NULL:
                return index -> valueChunk.get(index) != NULL_BOOLEAN_AS_BYTE;
            case NULL:
                return index -> valueChunk.get(index) == NULL_BOOLEAN_AS_BYTE;
        }
        throw new IllegalStateException("BooleanCumCountOperator - Unsupported CumCountType encountered: " + countType
                + " for type:" + columnType);
    }

    public BooleanCumCountOperator(
            @NotNull final MatchPair pair,
            @NotNull final CumCountSpec spec,
            @NotNull final Class<?> columnType) {
        super(pair, spec, columnType);
    }

    @Override
    public UpdateByOperator copy() {
        return new BooleanCumCountOperator(pair, spec, columnType);
    }
}
