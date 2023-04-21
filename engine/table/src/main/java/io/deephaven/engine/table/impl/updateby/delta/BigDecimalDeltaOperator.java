package io.deephaven.engine.table.impl.updateby.delta;

import io.deephaven.api.updateby.DeltaControl;
import io.deephaven.api.updateby.NullBehavior;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

public class BigDecimalDeltaOperator extends BaseObjectUpdateByOperator<BigDecimal> {
    private final DeltaControl control;
    private final ColumnSource<?> inputSource;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseObjectUpdateByOperator<BigDecimal>.Context {
        public ObjectChunk<BigDecimal, ? extends Values> objectValueChunk;
        private BigDecimal lastVal = null;

        protected Context(final int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            objectValueChunk = valuesChunk.asObjectChunk();
        }

        @Override
        public void push(int pos, int count) {
            Assert.eq(count, "push count", 1);

            // read the value from the values chunk
            final BigDecimal currentVal = objectValueChunk.get(pos);

            // If the previous value is null, defer to the control object to decide what to do
            if (lastVal == null) {
                curVal = (control.nullBehavior() == NullBehavior.NullDominates)
                        ? null
                        : currentVal;
            } else if (currentVal != null) {
                curVal = currentVal.subtract(lastVal);
            } else {
                curVal = null;
            }

            lastVal = currentVal;
        }
    }

    public BigDecimalDeltaOperator(@NotNull final MatchPair pair,
            @Nullable final RowRedirection rowRedirection,
            @NotNull final DeltaControl control,
            @NotNull final ColumnSource<?> inputSource
    // region extra-constructor-args
    // endregion extra-constructor-args
    ) {
        super(pair, new String[] {pair.rightColumn}, rowRedirection, BigDecimal.class);
        this.control = control;
        this.inputSource = inputSource;
        // region constructor
        // endregion constructor
    }

    @Override
    public void initializeCumulative(@NotNull final UpdateByOperator.Context context,
            final long firstUnmodifiedKey,
            final long firstUnmodifiedTimestamp,
            @NotNull final RowSet bucketRowSet) {
        Context ctx = (Context) context;
        ctx.reset();
        if (firstUnmodifiedKey != NULL_ROW_KEY) {
            // Retrieve the value from the input column.
            ctx.lastVal = (BigDecimal) inputSource.get(firstUnmodifiedKey);
        } else {
            ctx.lastVal = null;
        }
    }

    // region extra-methods
    // endregion extra-methods

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int chunkSize) {
        return new Context(chunkSize);
    }
}
