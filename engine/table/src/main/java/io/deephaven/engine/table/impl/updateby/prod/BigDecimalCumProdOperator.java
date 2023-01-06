package io.deephaven.engine.table.impl.updateby.prod;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.MathContext;

public final class BigDecimalCumProdOperator extends BaseObjectUpdateByOperator<BigDecimal> {
    @NotNull
    private final MathContext mathContext;

    protected class Context extends BaseObjectUpdateByOperator<BigDecimal>.Context {
        public ObjectChunk<BigDecimal, ? extends Values> objectValueChunk;

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
            final BigDecimal currentVal = objectValueChunk.get(pos);

            final boolean isCurrentNull = currentVal == null;
            if (curVal == null) {
                curVal = isCurrentNull ? null : currentVal;
            } else {
                if (!isCurrentNull) {
                    curVal = curVal.multiply(objectValueChunk.get(pos), mathContext);
                }
            }
        }

        @Override
        public void reset() {
            curVal = null;
        }
    }

    public BigDecimalCumProdOperator(@NotNull final MatchPair inputPair,
            @Nullable final RowRedirection rowRedirection,
            @NotNull final MathContext mathContext) {
        super(inputPair, new String[] {inputPair.rightColumn}, rowRedirection, BigDecimal.class);
        this.mathContext = mathContext;
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(int chunkSize) {
        return new Context(chunkSize);
    }
}
