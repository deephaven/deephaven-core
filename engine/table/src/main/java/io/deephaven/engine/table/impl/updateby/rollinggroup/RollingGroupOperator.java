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
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;

public abstract class RollingGroupOperator extends UpdateByOperator {
    /** Store a mapping from row keys to bucket RowSets */
    protected final WritableColumnSource<? extends RowSet> groupRowSetSource;
    protected final ObjectArraySource<? extends RowSet> innerGroupRowSetSource;

    /**
     * These sources will retain the position offsets from the current row position for the source and end of this
     * window. A primary benefit of storing offsets in position space vs. row keys is eliminating the need to shift keys
     * stored inside these column sources. Also, any insertion/modification/remove that occurs within this specified
     * window will trigger re-computation of these position offsets.
     *
     * NOTE: these offsets are inclusive and are stored as simple relative positions where start==0 or end==0 implies
     * the current row is included in the window. A start/end range of [-5,-3] defines a window including exactly 3 rows
     * beginning 4 rows earlier than this one and continuing through 2 rows earlier than this (inclusive).
     */
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
        private long startPos = NULL_LONG;
        private long endPos = NULL_LONG;

        protected Context(final int chunkSize) {
            super(0);

            groupRowSetSourceFillContext = groupRowSetSource.makeFillFromContext(chunkSize);
            groupRowSetSourceOutputValues = WritableObjectChunk.makeWritableChunk(chunkSize);
            if (timestampColumnName != null) {
                startSourceFillContext = startSource.makeFillFromContext(chunkSize);
                startSourceOutputValues = WritableLongChunk.makeWritableChunk(chunkSize);
                endSourceFillContext = endSource.makeFillFromContext(chunkSize);
                endSourceOutputValues = WritableLongChunk.makeWritableChunk(chunkSize);
                windowKeys = new LongRingBuffer(RING_BUFFER_INITIAL_CAPACITY, true);
            } else {
                startSourceFillContext = null;
                startSourceOutputValues = null;
                endSourceFillContext = null;
                endSourceOutputValues = null;
                windowKeys = null;
            }
        }

        @Override
        public void close() {
            SafeCloseable.closeArray(
                    groupRowSetSourceFillContext,
                    groupRowSetSourceOutputValues,
                    startSourceFillContext,
                    startSourceOutputValues,
                    endSourceFillContext,
                    endSourceOutputValues);
        }

        @Override
        public void accumulateCumulative(RowSequence inputKeys, Chunk<? extends Values>[] valueChunkArr,
                LongChunk<? extends Values> tsChunk, int len) {
            throw new IllegalStateException("accumulateCumulative() is invalid for RollingGroupOperator");
        }

        @Override
        public void accumulateRolling(RowSequence inputKeys,
                Chunk<? extends Values>[] influencerValueChunkArr,
                LongChunk<OrderedRowKeys> affectedPosChunk,
                LongChunk<OrderedRowKeys> influencerPosChunk,
                IntChunk<? extends Values> pushChunk,
                IntChunk<? extends Values> popChunk,
                int len) {

            if (timestampColumnName == null) {
                // The only work for ticks operators is to update the groupRowSetSource
                groupRowSetSource.fillFromChunk(groupRowSetSourceFillContext, groupRowSetSourceOutputValues, inputKeys);
                return;
            }

            setPosChunks(affectedPosChunk, influencerPosChunk);
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
                endPos = influencerPosChunk.get(pos + ii);
                windowKeys.addUnsafe(endPos);
                if (startPos == NULL_LONG) {
                    startPos = endPos;
                }
            }
        }

        @Override
        public void pop(int count) {
            Assert.geq(windowKeys.size(), "windowKeys.size()", count);

            for (int ii = 0; ii < count; ii++) {
                windowKeys.removeUnsafe();
                if (!windowKeys.isEmpty()) {
                    startPos = windowKeys.front();
                } else {
                    startPos = NULL_LONG;
                    endPos = NULL_LONG;
                }
            }
        }

        @Override
        public void writeToOutputChunk(int outIdx) {
            final long affectedPos = affectedPosChunk.get(outIdx);
            startSourceOutputValues.set(outIdx, startPos == NULL_LONG ? NULL_LONG : startPos - affectedPos);
            endSourceOutputValues.set(outIdx, endPos == NULL_LONG ? NULL_LONG : endPos - affectedPos);
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

        // For the sake of rolling group operators, we need to map from every row to its bucket row set (or the
        // non-null version) and to the start and end ranges of keys for the window slice. If we are using
        // redirection, use it for these structures as well.
        if (rowRedirection != null) {
            innerGroupRowSetSource = new ObjectArraySource<>(RowSet.class);
            groupRowSetSource = WritableRedirectedColumnSource.maybeRedirect(rowRedirection, innerGroupRowSetSource, 0);

        } else {
            groupRowSetSource = new ObjectSparseArraySource<>(RowSet.class);
            innerGroupRowSetSource = null;
        }

        // If we are computing through ticks, we won't need sources for start and end.
        if (timestampColumnName == null) {
            innerStartSource = null;
            innerEndSource = null;
            startSource = null;
            endSource = null;
            return;
        }

        // We are computing using the timestamp columns, create the appropriate sources (potentially redirected)
        if (rowRedirection == null) {
            startSource = new LongSparseArraySource();
            endSource = new LongSparseArraySource();
            innerStartSource = null;
            innerEndSource = null;
        } else {
            innerStartSource = new LongArraySource();
            innerEndSource = new LongArraySource();
            startSource = WritableRedirectedColumnSource.maybeRedirect(rowRedirection, innerStartSource, 0);
            endSource = WritableRedirectedColumnSource.maybeRedirect(rowRedirection, innerEndSource, 0);
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
            if (timestampColumnName != null) {
                assert innerStartSource != null;
                ((WritableSourceWithPrepareForParallelPopulation) innerStartSource)
                        .prepareForParallelPopulation(changedRows);
                assert innerEndSource != null;
                ((WritableSourceWithPrepareForParallelPopulation) innerEndSource)
                        .prepareForParallelPopulation(changedRows);
            }
        } else {
            ((WritableSourceWithPrepareForParallelPopulation) groupRowSetSource)
                    .prepareForParallelPopulation(changedRows);
            if (timestampColumnName != null) {
                ((WritableSourceWithPrepareForParallelPopulation) startSource)
                        .prepareForParallelPopulation(changedRows);
                ((WritableSourceWithPrepareForParallelPopulation) endSource).prepareForParallelPopulation(changedRows);
            }
        }
    }

    // region Shifts
    @Override
    public void applyOutputShift(@NotNull final RowSet subRowSetToShift, final long delta) {
        ((ObjectSparseArraySource<?>) groupRowSetSource).shift(subRowSetToShift, delta);
        if (timestampColumnName != null) {
            ((LongSparseArraySource) startSource).shift(subRowSetToShift, delta);
            ((LongSparseArraySource) endSource).shift(subRowSetToShift, delta);
        }
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
        if (rowRedirection != null) {
            assert innerGroupRowSetSource != null;
            innerGroupRowSetSource.startTrackingPrevValues();
        }

        // If we are time-based, track the start/end sources as well
        if (timestampColumnName != null) {
            startSource.startTrackingPrevValues();
            endSource.startTrackingPrevValues();
            if (rowRedirection != null) {
                assert innerStartSource != null;
                innerStartSource.startTrackingPrevValues();
                assert innerEndSource != null;
                innerEndSource.startTrackingPrevValues();
            }
        }
    }

    @Override
    protected boolean requiresKeys() {
        return true;
    }
}
