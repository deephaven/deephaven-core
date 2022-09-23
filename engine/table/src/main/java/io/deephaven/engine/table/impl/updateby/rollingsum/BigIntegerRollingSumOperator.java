package io.deephaven.engine.table.impl.updateby.rollingsum;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedObjectChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.ObjectSparseArraySource;
import io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedObjectUpdateByOperator;
import io.deephaven.engine.table.impl.util.SizedSafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Map;

public final class BigIntegerRollingSumOperator extends BaseWindowedObjectUpdateByOperator<BigInteger> {
    protected class Context extends BaseWindowedObjectUpdateByOperator<BigInteger>.Context {
        public ObjectChunk<BigInteger, Values> objectInfluencerValuesChunk;

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
    public UpdateByOperator.UpdateContext makeUpdateContext(final int chunkSize) {
        return new Context(chunkSize);
    }

    public BigIntegerRollingSumOperator(@NotNull final MatchPair pair,
                                        @NotNull final String[] affectingColumns,
                                        @NotNull final OperationControl control,
                                        @Nullable final String timestampColumnName,
                                        final long reverseTimeScaleUnits,
                                        final long forwardTimeScaleUnits,
                                        @NotNull final UpdateBy.UpdateByRedirectionContext redirContext
                                        // region extra-constructor-args
                                        // endregion extra-constructor-args
                                        ) {
        super(pair, affectingColumns, control, timestampColumnName, reverseTimeScaleUnits, forwardTimeScaleUnits, redirContext, BigInteger.class);
        // region constructor
        // endregion constructor        
    }

    @Override
    public void push(UpdateContext context, long key, int pos) {
        final Context ctx = (Context) context;
        BigInteger val = ctx.objectInfluencerValuesChunk.get(pos);

        // increase the running sum
        if (val != null) {
            if (ctx.curVal == null) {
                ctx.curVal = val;
            } else {
                ctx.curVal = ctx.curVal.add(val);
            }
        } else {
            ctx.nullCount++;
        }
    }

    @Override
    public void pop(UpdateContext context) {
        final Context ctx = (Context) context;
        int pos = ctx.windowIndices.front();
        BigInteger val = ctx.objectInfluencerValuesChunk.get(pos);

        // reduce the running sum
        if (val != null) {
            ctx.curVal = ctx.curVal.subtract(val);
        } else {
            ctx.nullCount--;
        }
    }

    @Override
    public void reset(UpdateContext context) {
        final Context ctx = (Context) context;
        ctx.curVal = null;
    }
}
