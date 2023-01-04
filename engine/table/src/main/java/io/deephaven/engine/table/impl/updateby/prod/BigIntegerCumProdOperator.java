package io.deephaven.engine.table.impl.updateby.prod;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigInteger;

public final class BigIntegerCumProdOperator extends BaseObjectUpdateByOperator<BigInteger> {
    protected class Context extends BaseObjectUpdateByOperator<BigInteger>.Context {
        public ObjectChunk<BigInteger, ? extends Values> objectValueChunk;

        protected Context(int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            objectValueChunk = valuesChunk.asObjectChunk();
        }

        @Override
        public void push(long key, int pos) {
            // read the value from the values chunk
            final BigInteger currentVal = objectValueChunk.get(pos);

            final boolean isCurrentNull = currentVal == null;
            if(curVal == null) {
                curVal = isCurrentNull ? null : currentVal;
            } else {
                if(!isCurrentNull) {
                    curVal = curVal.multiply(objectValueChunk.get(pos));
                }
            }
        }

        @Override
        public void reset() {
            curVal = null;
        }
    }

    public BigIntegerCumProdOperator(@NotNull final MatchPair inputPair,
                                     @Nullable final WritableRowRedirection rowRedirection) {
        super(inputPair, new String[] {inputPair.rightColumn}, rowRedirection, BigInteger.class);
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(int chunkSize) {
        return new Context(chunkSize);
    }
}
