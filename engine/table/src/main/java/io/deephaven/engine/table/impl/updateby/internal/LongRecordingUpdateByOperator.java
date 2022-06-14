package io.deephaven.engine.table.impl.updateby.internal;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.UpdateBy;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

/**
 * An operator that simply remembers the current chunks during the add and reprocess phases.
 */
public class LongRecordingUpdateByOperator implements UpdateByOperator {
    private final String inputColumnName;
    private final String[] affectingColumns;
    private final ColumnSource<?> columnSource;
    private RecordingContext currentContext = null;

    public LongRecordingUpdateByOperator(@NotNull final String inputColumnName,
                                         @NotNull final String[] affectingColumns,
                                         @NotNull final ColumnSource<?> columnSource) {
        this.inputColumnName = inputColumnName;
        this.affectingColumns = affectingColumns;
        this.columnSource = ReinterpretUtils.maybeConvertToPrimitive(columnSource);
    }

    private class RecordingContext implements UpdateContext {
        private LongChunk<Values> addedChunk;

        @Override
        public void close() {
            addedChunk = null;
            currentContext = null;
        }
    }

    /**
     * Get the long value at the specified position in the currently retained chunk.
     *
     * @param chunkPosition the position
     * @return the value at the position
     */
    public long getLong(final int chunkPosition) {
        return Require.neqNull(currentContext, "context").addedChunk.get(chunkPosition);
    }

    /**
     * Get the current long value within the underlying {@link ColumnSource}.
     *
     * @param key the key
     * @return the current value at the key within the column source.
     */
    public long getCurrentLong(final long key) {
        return columnSource.getLong(key);
    }

    @Override
    public void reprocessChunk(@NotNull UpdateContext updateContext, @NotNull RowSequence inputKeys, @Nullable LongChunk<OrderedRowKeys> keyChunk, @NotNull Chunk<Values> valuesChunk, @NotNull RowSet postUpdateSourceIndex) {
        currentContext.addedChunk = valuesChunk.asLongChunk();
    }

    @Override
    public void reprocessChunk(@NotNull UpdateContext updateContext, @NotNull RowSequence chunkOk, @NotNull Chunk<Values> values, @NotNull LongChunk<? extends RowKeys> keyChunk, @NotNull IntChunk<RowKeys> bucketPositions, @NotNull IntChunk<ChunkPositions> runStartPositions, @NotNull IntChunk<ChunkLengths> runLengths) {
        currentContext.addedChunk = values.asLongChunk();
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(int chunkSize) {
        return this.currentContext = new RecordingContext();
    }

    @Override
    public void addChunk(@NotNull UpdateContext updateContext,
                         @NotNull RowSequence inputKeys,
                         @Nullable LongChunk<OrderedRowKeys> keyChunk,
                         @NotNull Chunk<Values> values,
                         long bucketPosition) {
        currentContext.addedChunk = values.asLongChunk();
    }

    @Override
    public void addChunk(@NotNull UpdateContext context,
                         @NotNull Chunk<Values> values,
                         @NotNull LongChunk<? extends RowKeys> keyChunk,
                         @NotNull IntChunk<RowKeys> bucketPositions,
                         @NotNull IntChunk<ChunkPositions> startPositions,
                         @NotNull IntChunk<ChunkLengths> runLengths) {
        currentContext.addedChunk = values.asLongChunk();
    }

    // region Unused methods
    @Override
    public void setBucketCapacity(int capacity) {

    }

    @NotNull
    @Override
    public String getInputColumnName() {
        return inputColumnName;
    }

    @NotNull
    @Override
    public String[] getAffectingColumnNames() {
        return affectingColumns;
    }

    @NotNull
    @Override
    public String[] getOutputColumnNames() {
        return CollectionUtil.ZERO_LENGTH_STRING_ARRAY;
    }

    @NotNull
    @Override
    public Map<String, ColumnSource<?>> getOutputColumns() {
        return Collections.emptyMap();
    }

    @Override
    public void initializeForUpdate(@NotNull UpdateContext ctx, @NotNull TableUpdate upstream, @NotNull RowSet resultSourceIndex, boolean usingBuckets, boolean isUpstreamAppendOnly) {

    }

    @Override
    public void initializeFor(@NotNull UpdateContext updateContext, @NotNull RowSet updateIndex, @NotNull UpdateBy.UpdateType type) {

    }

    @Override
    public void finishFor(@NotNull UpdateContext updateContext, @NotNull UpdateBy.UpdateType type) {

    }

    @NotNull
    @Override
    public RowSet getAdditionalModifications(@NotNull UpdateContext ctx) {
        return RowSetFactory.empty();
    }

    @Override
    public boolean anyModified(@NotNull UpdateContext ctx) {
        return false;
    }

    @Override
    public void startTrackingPrev() {

    }

    @Override
    public boolean requiresKeys() {
        return false;
    }

    @Override
    public boolean requiresValues(@NotNull UpdateContext ctx) {
        return true;
    }

    @Override
    public boolean canProcessNormalUpdate(@NotNull UpdateContext context) {
        return false;
    }

    @Override
    public void setChunkSize(@NotNull UpdateContext context, int chunkSize) {

    }

    @Override
    public void onBucketsRemoved(@NotNull RowSet removedBuckets) {

    }

    @Override
    public void modifyChunk(@NotNull UpdateContext updateContext, @Nullable LongChunk<OrderedRowKeys> prevKeyChunk, @Nullable LongChunk<OrderedRowKeys> keyChunk, @NotNull Chunk<Values> prevValuesChunk, @NotNull Chunk<Values> postValuesChunk, long bucketPosition) {

    }

    @Override
    public void removeChunk(@NotNull UpdateContext updateContext, @Nullable LongChunk<OrderedRowKeys> keyChunk, @NotNull Chunk<Values> prevValuesChunk, long bucketPosition) {

    }

    @Override
    public void applyShift(@NotNull UpdateContext updateContext, @NotNull RowSet prevIndex, @NotNull RowSetShiftData shifted) {

    }

    @Override
    public void applyOutputShift(@NotNull UpdateContext context, @NotNull RowSet subIndexToShift, long delta) {

    }

    @Override
    public void resetForReprocess(@NotNull UpdateContext context, @NotNull RowSet sourceIndex, long firstUnmodifiedKey) {

    }

    @Override
    public void resetForReprocess(@NotNull UpdateContext context, @NotNull RowSet bucketIndex, long bucketPosition, long firstUnmodifiedKey) {

    }

    // endregion
}
