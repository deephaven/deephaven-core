//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.sum;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import org.jetbrains.annotations.NotNull;

import java.math.BigInteger;

public final class BigIntegerCumSumOperator extends BaseObjectUpdateByOperator<BigInteger> {
    protected class Context extends BaseObjectUpdateByOperator<BigInteger>.Context {
        public ObjectChunk<BigInteger, ? extends Values> objectValueChunk;

        protected Context(final int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValueChunks(@NotNull final Chunk<? extends Values>[] valueChunks) {
            objectValueChunk = valueChunks[0].asObjectChunk();
        }

        @Override
        public void push(int pos, int count) {
            Assert.eq(count, "push count", 1);

            // read the value from the values chunk
            final BigInteger currentVal = objectValueChunk.get(pos);

            final boolean isCurrentNull = currentVal == null;
            if (curVal == null) {
                curVal = isCurrentNull ? null : currentVal;
            } else {
                if (!isCurrentNull) {
                    curVal = curVal.add(objectValueChunk.get(pos));
                }
            }
        }
    }

    public BigIntegerCumSumOperator(
            @NotNull final MatchPair inputPair) {
        super(inputPair, new String[] {inputPair.rightColumn}, BigInteger.class);
    }

    @Override
    public UpdateByOperator copy() {
        return new BigIntegerCumSumOperator(pair);
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize);
    }
}
