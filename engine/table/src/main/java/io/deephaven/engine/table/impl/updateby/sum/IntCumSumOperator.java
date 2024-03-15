//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharCumSumOperator and run "./gradlew replicateUpdateBy" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.updateby.sum;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public class IntCumSumOperator extends BaseLongUpdateByOperator {
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseLongUpdateByOperator.Context {
        public IntChunk<? extends Values> intValueChunk;

        protected Context(final int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            intValueChunk = valueChunks[0].asIntChunk();
        }

        @Override
        public void push(int pos, int count) {
            Assert.eq(count, "push count", 1);

            // read the value from the values chunk
            final int currentVal = intValueChunk.get(pos);

            if (curVal == NULL_LONG) {
                curVal = currentVal == NULL_INT ? NULL_LONG : currentVal;
            } else if (currentVal != NULL_INT) {
                curVal += currentVal;
            }
        }
    }

    public IntCumSumOperator(@NotNull final MatchPair pair
    // region extra-constructor-args
    // endregion extra-constructor-args
    ) {
        super(pair, new String[] {pair.rightColumn});
        // region constructor
        // endregion constructor
    }

    @Override
    public UpdateByOperator copy() {
        return new IntCumSumOperator(pair
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
