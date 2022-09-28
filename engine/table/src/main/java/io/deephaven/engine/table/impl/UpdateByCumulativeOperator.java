package io.deephaven.engine.table.impl;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public abstract class UpdateByCumulativeOperator implements UpdateByOperator {
    protected final MatchPair pair;
    protected final String[] affectingColumns;

    protected final UpdateBy.UpdateByRedirectionContext redirContext;

    // these will be used by the timestamp-aware operators (EMA for example)
    protected OperationControl control;
    protected long timeScaleUnits;
    protected String timestampColumnName;

    public abstract class Context implements UpdateContext {
        public long curTimestamp;

        protected Context(final int chunkSize) {
            curTimestamp = NULL_LONG;
        }

        public boolean isValueValid(long atKey) {
            throw new UnsupportedOperationException(
                    "isValueValid() must be overridden by time-aware cumulative operators");
        }

        @Override
        public void close() {}

        @FinalDefault
        public void pop() {
            throw new UnsupportedOperationException("Cumulative operators should never call pop()");
        }
    }

    public UpdateByCumulativeOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @NotNull final UpdateBy.UpdateByRedirectionContext redirContext) {
        this.pair = pair;
        this.affectingColumns = affectingColumns;
        this.redirContext = redirContext;

        this.timeScaleUnits = 0L;
        this.timestampColumnName = null;
    }

    abstract public void initializeUpdate(@NotNull final UpdateContext context, final long firstUnmodifiedKey,
            long firstUnmodifiedTimestamp);

    @Override
    public void finishUpdate(@NotNull final UpdateContext context) {}

    @Override
    public String getTimestampColumnName() {
        return timestampColumnName;
    }

    @Override
    public long getPrevWindowUnits() {
        return timeScaleUnits;
    }

    /** cumulative operators do not have a forward-looking window */
    @Override
    public long getFwdWindowUnits() {
        return 0L;
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
