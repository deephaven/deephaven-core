package io.deephaven.engine.table.impl;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public abstract class UpdateByCumulativeOperator extends UpdateByOperator {
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

        public abstract void accumulate(RowSequence inputKeys,
                Chunk<? extends Values>[] valueChunkArr,
                LongChunk<? extends Values> tsChunk,
                int len);
    }

    public UpdateByCumulativeOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @NotNull final UpdateBy.UpdateByRedirectionContext redirContext) {
        super(pair, affectingColumns, null, null, 0L, 0L, redirContext);
    }

    abstract public void initializeUpdate(@NotNull final UpdateContext context, final long firstUnmodifiedKey,
            long firstUnmodifiedTimestamp);

    @Override
    public void finishUpdate(@NotNull final UpdateContext context) {}
}
