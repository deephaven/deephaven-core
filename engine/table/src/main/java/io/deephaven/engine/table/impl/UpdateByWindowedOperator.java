package io.deephaven.engine.table.impl;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.MatchPair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class UpdateByWindowedOperator implements UpdateByOperator {
    protected final MatchPair pair;
    protected final String[] affectingColumns;

    protected UpdateBy.UpdateByRedirectionContext redirContext;

    protected final OperationControl control;
    protected final String timestampColumnName;

    protected final long reverseTimeScaleUnits;
    protected final long forwardTimeScaleUnits;

    public abstract class Context implements UpdateContext {
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
     * @param redirContext the row redirection context to use for the operation
     */
    public UpdateByWindowedOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @NotNull final OperationControl control,
            @Nullable final String timestampColumnName,
            final long reverseTimeScaleUnits,
            final long forwardTimeScaleUnits,
            @NotNull final UpdateBy.UpdateByRedirectionContext redirContext) {
        this.pair = pair;
        this.affectingColumns = affectingColumns;
        this.control = control;
        this.timestampColumnName = timestampColumnName;
        this.reverseTimeScaleUnits = reverseTimeScaleUnits;
        this.forwardTimeScaleUnits = forwardTimeScaleUnits;
        this.redirContext = redirContext;
    }

    public abstract void initializeUpdate(@NotNull final UpdateContext context);

    @Override
    public void finishUpdate(@NotNull final UpdateContext context) {}

    @Override
    public String getTimestampColumnName() {
        return this.timestampColumnName;
    }

    /*** Get the value of the backward-looking window (might be nanos or ticks) */
    @Override
    public long getPrevWindowUnits() {
        return reverseTimeScaleUnits;
    }

    /*** Get the value of the forward-looking window (might be nanos or ticks) */
    @Override
    public long getFwdWindowUnits() {
        return forwardTimeScaleUnits;
    }

    /*** Mostly will be a single input column, but some accept multiple input (WAvg for example) */
    @NotNull
    @Override
    public String[] getInputColumnNames() {
        return new String[] {pair.rightColumn};
    }

    @NotNull
    @Override
    public String[] getAffectingColumnNames() {
        return affectingColumns;
    }

    @NotNull
    @Override
    public String[] getOutputColumnNames() {
        return new String[] {pair.leftColumn};
    }
}
