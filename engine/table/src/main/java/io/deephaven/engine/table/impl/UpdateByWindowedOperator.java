package io.deephaven.engine.table.impl;

import io.deephaven.api.updateby.OperationControl;
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
        //
        // protected long currentInfluencerKey;
        //
        // candidate data for the window
        //
        // // allocate some chunks for holding the key, position and timestamp data
        // protected SizedLongChunk<RowKeys> influencerKeyChunk;
        // protected SizedLongChunk<RowKeys> influencerPosChunk;
        // protected SizedLongChunk<? extends Values> influencerTimestampChunk;
        //
        // // for use with a ticking window
        // protected RowSet affectedRowPositions;
        // protected RowSet influencerPositions;
        //
        // protected long currentInfluencerPosOrTimestamp;
        // protected int currentInfluencerIndex;

        // public abstract void loadInfluencerValueChunk();



        @Override
        public void close() {
            // try (final SizedLongChunk<RowKeys> ignoredChk1 = influencerKeyChunk;
            // final SizedLongChunk<RowKeys> ignoredChk2 = influencerPosChunk;
            // final SizedLongChunk<? extends Values> ignoredChk3 = influencerTimestampChunk;
            // final RowSet ignoredRs3 = affectedRowPositions;
            // final RowSet ignoredRs4 = influencerPositions;
            // ) {
            // }
        }
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

    @NotNull
    @Override
    public String getInputColumnName() {
        return pair.rightColumn;
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
