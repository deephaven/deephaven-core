package io.deephaven.engine.table.impl;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.MatchPair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class UpdateByWindowedOperator extends UpdateByOperator {

    public abstract static class Context implements UpdateContext {
        public int nullCount = 0;

        @Override
        public void close() {}

        public abstract void accumulate(RowSequence inputKeys,
                Chunk<? extends Values> influencerValueChunkArr[],
                IntChunk<? extends Values> pushChunk,
                IntChunk<? extends Values> popChunk,
                int len);
    }

    /**
     * An operator that computes a windowed operation from a column
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this operation
     * @param control the control parameters for operation
     * @param timestampColumnName the optional time stamp column for windowing (uses ticks if not provided)
     * @param reverseTimeScaleUnits the time (us) or ticks to extend the window backwards
     * @param forwardTimeScaleUnits the time (us) or ticks to extend the window forwards
     * @param redirHelper the row redirection context to use for the operation
     */
    public UpdateByWindowedOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @NotNull final OperationControl control,
            @Nullable final String timestampColumnName,
            final long reverseTimeScaleUnits,
            final long forwardTimeScaleUnits,
            @NotNull final UpdateBy.UpdateByRedirectionHelper redirHelper) {
        super(pair, affectingColumns, control, timestampColumnName, reverseTimeScaleUnits, forwardTimeScaleUnits,
                redirHelper);
    }

    /**
     * Initialize the bucket context for this windowed operator
     */
    public abstract void initializeUpdate(@NotNull final UpdateContext context);

    @Override
    public void finishUpdate(@NotNull final UpdateContext context) {}
}
