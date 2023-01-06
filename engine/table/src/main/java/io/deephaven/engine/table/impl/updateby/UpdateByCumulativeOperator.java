package io.deephaven.engine.table.impl.updateby;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public abstract class UpdateByCumulativeOperator extends UpdateByOperator {
    public abstract static class Context implements UpdateContext {
        public long curTimestamp;

        protected Context() {
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

        public abstract void accumulate(RowSequence inputKeys,
                Chunk<? extends Values>[] valueChunkArr,
                LongChunk<? extends Values> tsChunk,
                int len);
    }

    /**
     * An operator that computes a cumulative operation from a column
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this operation
     * @param rowRedirection the row redirection context to use for the operation
     */
    public UpdateByCumulativeOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final RowRedirection rowRedirection) {
        super(pair, affectingColumns, null, null, 0L, 0L, rowRedirection);
    }

    /**
     * An operator that computes a cumulative operation from a column while providing an optional timestamp column name
     * and a
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this operation
     * @param rowRedirection the row redirection context to use for the operation
     */
    public UpdateByCumulativeOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final RowRedirection rowRedirection,
            @Nullable final String timestampColumnName,
            final long reverseTimeScaleUnits) {
        super(pair, affectingColumns, null, timestampColumnName, reverseTimeScaleUnits, 0L, rowRedirection);
    }


    /**
     * Initialize the bucket context for this cumulative operator
     */
    abstract public void initializeUpdate(@NotNull final UpdateContext context, final long firstUnmodifiedKey,
            long firstUnmodifiedTimestamp);

    @Override
    public void finishUpdate(@NotNull final UpdateContext context) {}
}
