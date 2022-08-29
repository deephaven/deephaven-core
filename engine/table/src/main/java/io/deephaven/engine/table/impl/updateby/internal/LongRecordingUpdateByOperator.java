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
import io.deephaven.engine.table.impl.UpdateByCumulativeOperator;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.UpdateByWindowedOperator;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

/**
 * An operator that simply remembers the current chunks during the add and reprocess phases.
 */
public class LongRecordingUpdateByOperator extends UpdateByCumulativeOperator {
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

    private class RecordingContext extends UpdateCumulativeContext {
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

    /**
     * Get the pervious long value within the underlying {@link ColumnSource}.
     *
     * @param key the key
     * @return the previous value at the key within the column source.
     */
    public long getPrevLong(final long key) {
        return columnSource.getPrevLong(key);
    }

    /**
     * Get the current underlying {@link ColumnSource}.
     *
     * @return the current value at the key within the column source.
     */
    public ColumnSource<?> getColumnSource() {
        return columnSource;
    }

    @Override
    public void processChunk(@NotNull UpdateContext updateContext, @NotNull RowSequence inputKeys, @Nullable LongChunk<OrderedRowKeys> keyChunk, @NotNull Chunk<Values> valuesChunk, @NotNull RowSet postUpdateSourceIndex) {
        currentContext.addedChunk = valuesChunk.asLongChunk();
    }

    @NotNull
    @Override
    public UpdateContext makeUpdateContext(int chunkSize) {
        return this.currentContext = new RecordingContext();
    }

    // region Unused methods

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
    public void setChunkSize(@NotNull UpdateContext context, int chunkSize) {
    }

    @Override
    public void applyOutputShift(@NotNull UpdateContext context, @NotNull RowSet subIndexToShift, long delta) {
    }

    @Override
    public void resetForProcess(@NotNull UpdateContext context, @NotNull RowSet sourceIndex, long firstUnmodifiedKey) {
    }

    // endregion
}
