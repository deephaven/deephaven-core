package io.deephaven.engine.table.impl.updateby.prod;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.MathContext;

public final class BigDecimalCumProdOperator extends BaseObjectUpdateByOperator<BigDecimal> {
    @NotNull
    private final MathContext mathContext;

    protected class Context extends BaseObjectUpdateByOperator<BigDecimal>.Context {
        public ObjectChunk<BigDecimal, Values> objectValueChunk;

        protected Context(int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void storeValuesChunk(@NotNull final Chunk<Values> valuesChunk) {
            objectValueChunk = valuesChunk.asObjectChunk();
        }
    }

    public BigDecimalCumProdOperator(@NotNull final MatchPair inputPair,
            @NotNull final UpdateBy.UpdateByRedirectionContext redirContext,
            @NotNull final MathContext mathContext) {
        super(inputPair, new String[] {inputPair.rightColumn}, redirContext, BigDecimal.class);
        this.mathContext = mathContext;
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(int chunkSize, ColumnSource<?> inputSource) {
        return new Context(chunkSize);
    }

    @Override
    public void push(UpdateContext context, long key, int pos) {
        final Context ctx = (Context) context;

        // read the value from the values chunk
        final BigDecimal currentVal = ctx.objectValueChunk.get(pos);

        final boolean isCurrentNull = currentVal == null;
        if (ctx.curVal == null) {
            ctx.curVal = isCurrentNull ? null : currentVal;
        } else {
            if (!isCurrentNull) {
                ctx.curVal = ctx.curVal.multiply(ctx.objectValueChunk.get(pos), mathContext);
            }
        }
    }
}
