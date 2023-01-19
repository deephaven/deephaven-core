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

public abstract class UpdateByCumulativeOperator extends UpdateByOperator {
    public abstract static class Context implements UpdateContext {
        /** Holds the chunks of input data for use by the accumulate call */
        public final Chunk<? extends Values>[] chunkArr;

        public Context(int chunkCount) {
            chunkArr = new Chunk[chunkCount];
        }

        public boolean isValueValid(long atKey) {
            throw new UnsupportedOperationException(
                    "isValueValid() must be overridden by time-aware cumulative operators");
        }

        @Override
        public void close() {}

        @FinalDefault
        public void pop(int count) {
            throw new UnsupportedOperationException("Cumulative operators should never call pop()");
        }

        public abstract void accumulate(RowSequence inputKeys,
                Chunk<? extends Values>[] valueChunkArr,
                LongChunk<? extends Values> tsChunk,
                int len);
    }

    /**
     * An operator that computes a cumulative operation from a column. The operation may be time or ticks aware (e.g.
     * EMA) and timestamp column name and time units (ticks or nanoseconds) may optionally be provided
     *
     * @param pair the {@link MatchPair} that defines the input/output for this operation
     * @param affectingColumns the names of the columns that affect this operation
     * @param rowRedirection the row redirection context to use for the operation
     */
    protected UpdateByCumulativeOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final RowRedirection rowRedirection,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits) {
        super(pair, affectingColumns, timestampColumnName, reverseWindowScaleUnits, 0L, rowRedirection);
    }


    /**
     * Initialize the bucket context for this cumulative operator
     */
    public void initializeUpdate(@NotNull final UpdateContext context, final long firstUnmodifiedKey,
            long firstUnmodifiedTimestamp) {}
}
