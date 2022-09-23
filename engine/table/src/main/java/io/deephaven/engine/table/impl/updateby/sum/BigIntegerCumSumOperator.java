package io.deephaven.engine.table.impl.updateby.sum;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import org.jetbrains.annotations.NotNull;

import java.math.BigInteger;

public final class BigIntegerCumSumOperator extends BaseObjectUpdateByOperator<BigInteger> {
    protected class Context extends BaseObjectUpdateByOperator<BigInteger>.Context {
        public ObjectChunk<BigInteger, Values> objectValueChunk;

        protected Context(int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void storeValuesChunk(@NotNull final Chunk<Values> valuesChunk) {
            objectValueChunk = valuesChunk.asObjectChunk();
        }
    }

    public BigIntegerCumSumOperator(@NotNull final MatchPair inputPair,
                                    @NotNull final UpdateBy.UpdateByRedirectionContext redirContext) {
        super(inputPair, new String[] {inputPair.rightColumn}, redirContext, BigInteger.class);
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(int chunkSize) {
        return new Context(chunkSize);
    }

    @Override
    public void push(UpdateContext context, long key, int pos) {
        final Context ctx = (Context) context;

        // read the value from the values chunk
        final BigInteger currentVal = ctx.objectValueChunk.get(pos);

        final boolean isCurrentNull = currentVal == null;
        if(ctx.curVal == null) {
            ctx.curVal = isCurrentNull ? null : currentVal;
        } else {
            if(!isCurrentNull) {
                ctx.curVal = ctx.curVal.add(ctx.objectValueChunk.get(pos));
            }
        }
    }
}
