//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.sum;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseFloatUpdateByOperator;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class FloatCumSumOperator extends BaseFloatUpdateByOperator {

    protected class Context extends BaseFloatUpdateByOperator.Context {
        public FloatChunk<? extends Values> floatValueChunk;

        protected Context(final int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            floatValueChunk = valueChunks[0].asFloatChunk();
        }

        @Override
        public void push(int pos, int count) {
            Assert.eq(count, "push count", 1);

            // read the value from the values chunk
            final float currentVal = floatValueChunk.get(pos);

            if (curVal == NULL_FLOAT) {
                curVal = currentVal;
            } else if (currentVal != NULL_FLOAT) {
                curVal += currentVal;
            }
        }
    }

    public FloatCumSumOperator(@NotNull final MatchPair pair) {
        super(pair, new String[] {pair.rightColumn});
    }

    @Override
    public UpdateByOperator copy() {
        return new FloatCumSumOperator(pair);
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize);
    }
}
