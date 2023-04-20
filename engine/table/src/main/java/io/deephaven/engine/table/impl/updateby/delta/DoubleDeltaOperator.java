/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharDeltaOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.updateby.delta;

import io.deephaven.api.updateby.DeltaControl;
import io.deephaven.api.updateby.NullBehavior;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

public class DoubleDeltaOperator extends BaseDoubleUpdateByOperator {
    private final DeltaControl control;
    private final ColumnSource<?> inputSource;
    // region extra-fields
    // endregion extra-fields

    protected class Context extends BaseDoubleUpdateByOperator.Context {
        public DoubleChunk<? extends Values> doubleValueChunk;
        private double lastVal = NULL_DOUBLE;

        protected Context(final int chunkSize) {
            super(chunkSize);
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            doubleValueChunk = valuesChunk.asDoubleChunk();
        }

        @Override
        public void push(int pos, int count) {
            Assert.eq(count, "push count", 1);

            // read the value from the values chunk
            final double currentVal = doubleValueChunk.get(pos);

            // If the previous value is null, defer to the control object to decide what to do
            if (lastVal == NULL_DOUBLE) {
                curVal = (control.nullBehavior() == NullBehavior.NullDominates)
                        ? NULL_DOUBLE
                        : currentVal;
            } else if (currentVal != NULL_DOUBLE) {
                curVal = (double)(currentVal - lastVal);
            } else {
                curVal = NULL_DOUBLE;
            }

            lastVal = currentVal;
        }
    }

    public DoubleDeltaOperator(@NotNull final MatchPair pair,
                             @Nullable final RowRedirection rowRedirection,
                             @NotNull final DeltaControl control,
                             @NotNull final ColumnSource<?> inputSource
                             // region extra-constructor-args
                             // endregion extra-constructor-args
    ) {
        super(pair, new String[] { pair.rightColumn }, rowRedirection);
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
            ctx.lastVal = inputSource.getDouble(firstUnmodifiedKey);
        } else {
            ctx.lastVal = NULL_DOUBLE;
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
