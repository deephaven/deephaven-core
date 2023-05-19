package io.deephaven.engine.table.impl.updateby.prod;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigInteger;

public final class BigIntegerCumProdOperator extends BaseObjectUpdateByOperator<BigInteger> {
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

            final BigInteger val = objectValueChunk.get(pos);

            if (val != null) {
                curVal = curVal == null ? val : curVal.multiply(val);
            }
        }

        @Override
        public void reset() {
            curVal = null;
        }
    }

    public BigIntegerCumProdOperator(@NotNull final MatchPair inputPair,
                                     @Nullable final RowRedirection rowRedirection) {
        super(inputPair, new String[] {inputPair.rightColumn}, rowRedirection, BigInteger.class);
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int affectedChunkSize, final int influencerChunkSize) {
        return new Context(affectedChunkSize);
    }

}
