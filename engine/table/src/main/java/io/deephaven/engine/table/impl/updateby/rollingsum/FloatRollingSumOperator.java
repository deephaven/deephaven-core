package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedFloatUpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.PairwiseFloatRingBuffer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class FloatRollingSumOperator extends BaseWindowedFloatUpdateByOperator {
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseWindowedFloatUpdateByOperator.Context {
        public FloatChunk<Values> floatInfluencerValuesChunk;

        public PairwiseFloatRingBuffer pairwiseSum;

        protected Context(final int chunkSize) {
            super(chunkSize);
            this.pairwiseSum = new PairwiseFloatRingBuffer(64, 0.0f, Float::sum);
        }

        @Override
        public void close() {
            super.close();
            this.pairwiseSum.close();
        }
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(final int chunkSize, ColumnSource<?> inputSource) {
        return new Context(chunkSize);
    }

    public FloatRollingSumOperator(@NotNull final MatchPair pair,
                                   @NotNull final String[] affectingColumns,
                                   @NotNull final OperationControl control,
                                   @Nullable final String timestampColumnName,
                                   final long reverseTimeScaleUnits,
                                   final long forwardTimeScaleUnits,
                                   @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                                   // region extra-constructor-args
                                   // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, control, timestampColumnName, reverseTimeScaleUnits, forwardTimeScaleUnits, redirContext);
        // region constructor
        // endregion constructor
    }

    @Override
    public void push(UpdateContext context, long key, int pos) {
        final Context ctx = (Context) context;
        float val = ctx.floatInfluencerValuesChunk.get(pos);

        if (val != NULL_FLOAT) {
            ctx.pairwiseSum.push(val);
        } else {
            ctx.pairwiseSum.pushEmptyValue();
            ctx.nullCount++;
        }
    }

    @Override
    public void pop(UpdateContext context) {
        final Context ctx = (Context) context;
        int pos = ctx.windowIndices.front();
        float val = ctx.floatInfluencerValuesChunk.get(pos);

        if (val == NULL_FLOAT) {
            ctx.nullCount--;
        }
        ctx.pairwiseSum.pop();
    }

    @Override
    public void reset(UpdateContext context) {
        final Context ctx = (Context) context;
        // take this opportunity to clear the pairwise structure
        ctx.pairwiseSum.clear();
        ctx.nullCount = 0;
    }

    @Override
    public float result(UpdateContext context) {
        final Context ctx = (Context) context;
        if (ctx.pairwiseSum.size() == ctx.nullCount) {
            return NULL_FLOAT;
        }
        return ctx.pairwiseSum.evaluate();
    }

    @NotNull
    @Override
    public Map<String, ColumnSource<?>> getOutputColumns() {
        return Collections.singletonMap(pair.leftColumn, outputSource);
    }
}
