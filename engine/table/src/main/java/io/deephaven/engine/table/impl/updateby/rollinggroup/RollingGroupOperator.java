package io.deephaven.engine.table.impl.updateby.rollinggroup;

import io.deephaven.base.ringbuffer.LongRingBuffer;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.WritableSourceWithPrepareForParallelPopulation;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.updateby.UpdateByOperator;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.engine.table.impl.util.RowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.NULL_INT;

public abstract class RollingGroupOperator extends UpdateByOperator {
    /** Store a mapping from row keys to bucket RowSets for rolling group operator */
    protected final WritableColumnSource<? extends RowSet> groupRowSetSource;
    protected final ObjectArraySource<? extends RowSet> innerGroupRowSetSource;
    /** these sources will retain the start and end keys (inclusive) for the window */
    protected final WritableColumnSource<Long> startSource;
    protected final LongArraySource innerStartSource;
    protected final WritableColumnSource<Long> endSource;
    protected final LongArraySource innerEndSource;

    public class Context extends UpdateByOperator.Context {
        private static final int RING_BUFFER_INITIAL_CAPACITY = 512;

        public final ChunkSink.FillFromContext groupRowSetSourceFillContext;
        public final WritableObjectChunk<RowSet, ? extends Values> groupRowSetSourceOutputValues;
        public final ChunkSink.FillFromContext startSourceFillContext;
        public final WritableLongChunk<Values> startSourceOutputValues;
        public final ChunkSink.FillFromContext endSourceFillContext;
        public final WritableLongChunk<Values> endSourceOutputValues;
        private final LongRingBuffer windowKeys;
        private long startKey = NULL_ROW_KEY;
        private long endKey = NULL_ROW_KEY;

        protected Context(final int chunkSize) {
            super(0);

            this.groupRowSetSourceFillContext = groupRowSetSource.makeFillFromContext(chunkSize);
            this.groupRowSetSourceOutputValues = WritableObjectChunk.makeWritableChunk(chunkSize);
            this.startSourceFillContext = groupRowSetSource.makeFillFromContext(chunkSize);
            this.startSourceOutputValues = WritableLongChunk.makeWritableChunk(chunkSize);
            this.endSourceFillContext = groupRowSetSource.makeFillFromContext(chunkSize);
            this.endSourceOutputValues = WritableLongChunk.makeWritableChunk(chunkSize);

            windowKeys = new LongRingBuffer(RING_BUFFER_INITIAL_CAPACITY, true);
        }

        @Override
        public void close() {}

        @Override
        public void accumulateCumulative(RowSequence inputKeys, Chunk<? extends Values>[] valueChunkArr,
                LongChunk<? extends Values> tsChunk, int len) {
            throw new IllegalStateException("accumulateCumulative() is invalid for RollingGroupOperator");
        }

        @Override
        public void accumulateRolling(RowSequence inputKeys,
                Chunk<? extends Values>[] influencerValueChunkArr,
                LongChunk<OrderedRowKeys> influencerKeyChunk,
                IntChunk<? extends Values> pushChunk,
                IntChunk<? extends Values> popChunk,
                int len) {

            setKeyChunk(influencerKeyChunk);
            int pushIndex = 0;

            // chunk processing
            for (int ii = 0; ii < len; ii++) {
                final int pushCount = pushChunk.get(ii);
                final int popCount = popChunk.get(ii);

                if (pushCount == NULL_INT) {
                    // writeNullToOutputChunk(ii);
                    continue;
                }

                // pop for this row
                if (popCount > 0) {
                    pop(popCount);
                }

                // push for this row
                if (pushCount > 0) {
                    push(pushIndex, pushCount);
                    pushIndex += pushCount;
                }

                // write the results to the output chunk
                writeToOutputChunk(ii);
            }

            // chunk output to column
            writeToOutputColumn(inputKeys);
        }

        @Override
        public void setValuesChunk(@NotNull final Chunk<? extends Values> valuesChunk) {
            // NOP
        }

        @Override
        public void push(int pos, int count) {
            windowKeys.ensureRemaining(count);

            for (int ii = 0; ii < count; ii++) {
                endKey = keyChunk.get(pos + ii);
                windowKeys.addUnsafe(endKey);
                if (startKey == NULL_ROW_KEY) {
                    startKey = endKey;
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(windowKeys.size(), "windowKeys.size()", count);

            for (int ii = 0; ii < count; ii++) {
                startKey = windowKeys.removeUnsafe();
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            startSourceOutputValues.set(outIdx, startKey);
            endSourceOutputValues.set(outIdx, endKey);
        }

        @Override
        public void reset() {
            // NOP
        }

        @Override
        public void writeToOutputColumn(@NotNull RowSequence inputKeys) {
            startSource.fillFromChunk(startSourceFillContext, startSourceOutputValues, inputKeys);
            endSource.fillFromChunk(endSourceFillContext, endSourceOutputValues, inputKeys);
            groupRowSetSource.fillFromChunk(groupRowSetSourceFillContext, groupRowSetSourceOutputValues, inputKeys);
        }

        public void assignBucketRowSource(final TrackingRowSet bucketRowSet, final RowSequence chunkRs) {
            groupRowSetSourceOutputValues.fillWithValue(0, chunkRs.intSize(), bucketRowSet);
        }
    }

    public RollingGroupOperator(@NotNull final MatchPair pair,
            @NotNull final String[] affectingColumns,
            @Nullable final RowRedirection rowRedirection,
            @Nullable final String timestampColumnName,
            final long reverseWindowScaleUnits,
            final long forwardWindowScaleUnits
    // region extra-constructor-args
    // endregion extra-constructor-args
    ) {
        super(pair, affectingColumns, rowRedirection, timestampColumnName, reverseWindowScaleUnits,
                forwardWindowScaleUnits, true);

        // for the sake of rolling group operators, we need to map from every row to its bucket row set (or the
        // non-null version) and to the start and end ranges of keys for the window slice. If we are using
        // redirection, use it for these structures as well.
        if (rowRedirection != null) {
            innerGroupRowSetSource = new ObjectArraySource<>(RowSet.class);
            innerStartSource = new LongArraySource();
            innerEndSource = new LongArraySource();

            groupRowSetSource = WritableRedirectedColumnSource.maybeRedirect(rowRedirection, innerGroupRowSetSource, 0);
            startSource = WritableRedirectedColumnSource.maybeRedirect(rowRedirection, innerStartSource, 0);
            endSource = WritableRedirectedColumnSource.maybeRedirect(rowRedirection, innerEndSource, 0);
        } else {
            groupRowSetSource = new ObjectSparseArraySource<>(RowSet.class);
            startSource = new LongSparseArraySource();
            endSource = new LongSparseArraySource();

            innerGroupRowSetSource = null;
            innerStartSource = null;
            innerEndSource = null;
        }
    }

    @NotNull
    @Override
    public UpdateByOperator.Context makeUpdateContext(final int chunkSize, final int chunkCount) {
        return new Context(chunkSize);
    }

    @Override
    public void prepareForParallelPopulation(final RowSet changedRows) {
        if (rowRedirection != null) {
            assert innerGroupRowSetSource != null;
            ((WritableSourceWithPrepareForParallelPopulation) innerGroupRowSetSource)
                    .prepareForParallelPopulation(changedRows);
            assert innerStartSource != null;
            ((WritableSourceWithPrepareForParallelPopulation) innerStartSource)
                    .prepareForParallelPopulation(changedRows);
            assert innerEndSource != null;
            ((WritableSourceWithPrepareForParallelPopulation) innerEndSource).prepareForParallelPopulation(changedRows);
        } else {
            ((WritableSourceWithPrepareForParallelPopulation) groupRowSetSource)
                    .prepareForParallelPopulation(changedRows);
            ((WritableSourceWithPrepareForParallelPopulation) startSource).prepareForParallelPopulation(changedRows);
            ((WritableSourceWithPrepareForParallelPopulation) endSource).prepareForParallelPopulation(changedRows);
        }
    }

    // region Shifts
    @Override
    public void applyOutputShift(@NotNull final RowSet subRowSetToShift, final long delta) {
        ((ObjectSparseArraySource<?>) groupRowSetSource).shift(subRowSetToShift, delta);
    }
    // endregion Shifts

    // region clear-output
    @Override
    public void clearOutputRows(final RowSet toClear) {
        // if we are redirected, clear the inner source
        if (rowRedirection != null) {
            ChunkUtils.fillWithNullValue(innerGroupRowSetSource, toClear);
        } else {
            ChunkUtils.fillWithNullValue(groupRowSetSource, toClear);
        }
    }
    // endregion clear-output


    @Override
    public void startTrackingPrev() {
        groupRowSetSource.startTrackingPrevValues();
        startSource.startTrackingPrevValues();
        endSource.startTrackingPrevValues();
    }

    @Override
    protected boolean requiresKeys() {
        return true;
    }
}
