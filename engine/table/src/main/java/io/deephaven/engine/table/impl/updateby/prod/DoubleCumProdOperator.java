//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatCumProdOperator and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.prod;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class DoubleCumProdOperator extends BaseDoubleUpdateByOperator {
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseDoubleUpdateByOperator.Context {
        public DoubleChunk<? extends Values> doubleValueChunk;

        protected Context(final int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            doubleValueChunk = valueChunks[0].asDoubleChunk();
        }

        @Override
        public void push(int pos, int count) {
            Assert.eq(count, "push count", 1);

            final double val = doubleValueChunk.get(pos);

            if (val != NULL_DOUBLE) {
                curVal = curVal == NULL_DOUBLE ? val : curVal * val;
            }
        }
    }

    public DoubleCumProdOperator(
            @NotNull final MatchPair pair
    // region extra-constructor-args
    // endregion extra-constructor-args
    ) {
        super(pair, new String[] {pair.rightColumn});
        // region constructor
        // endregion constructor
    }

    @Override
    public UpdateByOperator copy() {
        return new DoubleCumProdOperator(
                pair
        // region extra-copy-args
        // endregion extra-copy-args
        );
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize);
    }
}
