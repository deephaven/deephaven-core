package io.deephaven.engine.table.impl.updateby.prod;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigInteger;

public final class BigIntegerCumProdOperator extends BaseObjectUpdateByOperator<BigInteger> {
    protected class Context extends BaseObjectUpdateByOperator<BigInteger>.Context {
        public ObjectChunk<BigInteger, ? extends Values> objectValueChunk;

        protected Context(final int chunkSize, final int chunkCount) {
            super(chunkSize, chunkCount);
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            objectValueChunk = valuesChunk.asObjectChunk();
        }

        @Override
        public void push(long key, int pos, int count) {
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
        super(inputPair, new String[] {inputPair.rightColumn}, rowRedirection, false, BigInteger.class);
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int chunkSize, final int chunkCount) {
        return new Context(chunkSize, chunkCount);
    }

}
