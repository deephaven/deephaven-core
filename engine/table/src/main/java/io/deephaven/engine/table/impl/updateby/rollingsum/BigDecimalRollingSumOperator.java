package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedObjectUpdateByOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.MathContext;

public final class BigDecimalRollingSumOperator extends BaseWindowedObjectUpdateByOperator<BigDecimal> {
    @NotNull
    private final MathContext mathContext;

    protected class Context extends BaseWindowedObjectUpdateByOperator<BigDecimal>.Context {
        public ObjectChunk<BigDecimal, Values> objectInfluencerValuesChunk;

        protected Context(final int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void storeInfluencerValuesChunk(@NotNull final Chunk<Values> influencerValuesChunk) {
            objectInfluencerValuesChunk = influencerValuesChunk.asObjectChunk();
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.UpdateContext makeUpdateContext(final int chunkSize, ColumnSource<?> inputSource) {
        return new Context(chunkSize);
    }

    public BigDecimalRollingSumOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @NotNull final OperationControl control,
            @Nullable final String timestampColumnName,
            final long reverseTimeScaleUnits,
            final long forwardTimeScaleUnits,
            @NotNull final UpdateBy.UpdateByRedirectionContext redirContext,
            @NotNull final MathContext mathContext) {
        super(pair, affectingColumns, control, timestampColumnName, reverseTimeScaleUnits, forwardTimeScaleUnits,
                redirContext, BigDecimal.class);
        this.mathContext = mathContext;
    }

    @Override
    public void push(UpdateContext context, long key, int pos) {
        final Context ctx = (Context) context;
        BigDecimal val = ctx.objectInfluencerValuesChunk.get(pos);

        // increase the running sum
        if (val != null) {
            if (ctx.curVal == null) {
                ctx.curVal = val;
            } else {
                ctx.curVal = ctx.curVal.add(val, mathContext);
            }
        } else {
            ctx.nullCount++;
        }
    }

    @Override
    public void pop(UpdateContext context) {
        final Context ctx = (Context) context;
        int pos = ctx.windowIndices.front();
        BigDecimal val = ctx.objectInfluencerValuesChunk.get(pos);

        // reduce the running sum
        if (val != null) {
            ctx.curVal = ctx.curVal.subtract(val, mathContext);
        } else {
            ctx.nullCount--;
        }
    }
}
