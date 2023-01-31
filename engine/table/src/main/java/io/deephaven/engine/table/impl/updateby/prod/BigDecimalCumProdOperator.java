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

import java.math.BigDecimal;
import java.math.MathContext;

public final class BigDecimalCumProdOperator extends BaseObjectUpdateByOperator<BigDecimal> {
    @NotNull
    private final MathContext mathContext;

    protected class Context extends BaseObjectUpdateByOperator<BigDecimal>.Context {
        public ObjectChunk<BigDecimal, ? extends Values> objectValueChunk;

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

            final BigDecimal val = objectValueChunk.get(pos);

            if (val != null) {
                curVal = curVal == null ? val : curVal.multiply(val, mathContext);
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
        super(inputPair, new String[] {inputPair.rightColumn}, rowRedirection, false, BigDecimal.class);
        this.mathContext = mathContext;
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int chunkSize, final int chunkCount) {
        return new Context(chunkSize, chunkCount);
    }
}
