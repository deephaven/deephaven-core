package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.api.updateby.OperationControl;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.UpdateByWindowedOperator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class BaseWindowedCharUpdateByOperator extends UpdateByWindowedOperator {
    protected final ColumnSource<Character> valueSource;

    // region extra-fields
    // endregion extra-fields

    protected class Context extends UpdateWindowedContext {
        public WritableCharChunk<Values> candidateValuesChunk;

        @Override
        public void close() {
            super.close();
            if (candidateValuesChunk != null) {
                candidateValuesChunk.close();
                candidateValuesChunk = null;
            }
        }

        @Override
        public void loadCandidateValueChunk(RowSequence windowRowSequence) {
            // fill the window values chunk
            if (candidateValuesChunk == null) {
                candidateValuesChunk = WritableCharChunk.makeWritableChunk(WINDOW_CHUNK_SIZE);
            }
            try (ChunkSource.FillContext fc = valueSource.makeFillContext(WINDOW_CHUNK_SIZE)){
                valueSource.fillChunk(fc, candidateValuesChunk, windowRowSequence);
            }
        }
    }

    public BaseWindowedCharUpdateByOperator(@NotNull final MatchPair pair,
                                            @NotNull final String[] affectingColumns,
                                            @NotNull final OperationControl control,
                                            @Nullable final LongRecordingUpdateByOperator timeRecorder,
                                            @Nullable final String timestampColumnName,
                                            final long reverseTimeScaleUnits,
                                            final long forwardTimeScaleUnits,
                                            @NotNull final UpdateBy.UpdateByRedirectionContext redirContext,
                                            @NotNull final ColumnSource<Character> valueSource
                                            // region extra-constructor-args
                                            // endregion extra-constructor-args
                                    ) {
        super(pair, affectingColumns, control, timeRecorder, timestampColumnName, reverseTimeScaleUnits, forwardTimeScaleUnits, redirContext);
        this.valueSource = valueSource;
        // region constructor
        // endregion constructor
    }

    // region extra-methods
    // endregion extra-methods

    // region Addition
    /**
     * Add a chunk of values to the operator.
     *
     * @param ctx the context object
     * @param inputKeys the input keys for the chunk
     * @param workingChunk the chunk of values
     */
    protected abstract void doProcessChunk(@NotNull final Context ctx,
                                           @NotNull final RowSequence inputKeys,
                                           @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                                           @NotNull final Chunk<Values> workingChunk);

    // endregion

    // region Reprocessing

    public void resetForProcess(@NotNull final UpdateContext context,
                                @NotNull final RowSet sourceRowSet,
                                long firstUnmodifiedKey) {
        final Context ctx = (Context) context;
        ctx.workingRowSet = sourceRowSet;
    }

    @Override
    public void processChunk(@NotNull final UpdateContext updateContext,
                             @NotNull final RowSequence inputKeys,
                             @Nullable final LongChunk<OrderedRowKeys> keyChunk,
                             @NotNull final Chunk<Values> valuesChunk,
                             @NotNull final RowSet postUpdateSourceIndex) {
        final Context ctx = (Context) updateContext;
        ctx.loadDataChunks(inputKeys);
        doProcessChunk(ctx, inputKeys, keyChunk, valuesChunk);
        ctx.getModifiedBuilder().appendRowSequence(inputKeys);
    }

    // endregion
}
