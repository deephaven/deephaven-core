/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.aggregate;

import io.deephaven.base.ClampUtil;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.sources.UngroupedColumnSource;
import io.deephaven.util.SafeCloseable;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.util.QueryConstants.*;

/**
 * Base {@link ColumnSource} implementation for sliced rowset aggregation result columns.
 */
public abstract class BaseAggregateSlicedColumnSource<VECTOR_TYPE extends Vector, COMPONENT_TYPE>
        extends AbstractColumnSource<VECTOR_TYPE> implements AggregateColumnSource<VECTOR_TYPE, COMPONENT_TYPE> {

    protected final ColumnSource<COMPONENT_TYPE> aggregatedSource;
    protected final ColumnSource<COMPONENT_TYPE> aggregatedSourcePrev;
    protected final ColumnSource<? extends RowSet> groupRowSetSource;
    @Nullable
    protected final ColumnSource<Long> startSource;
    @Nullable
    protected final ColumnSource<Long> endSource;

    protected final long startOffset;
    protected final long endOffset;

    protected BaseAggregateSlicedColumnSource(
            @NotNull final Class<VECTOR_TYPE> vectorType,
            @NotNull final ColumnSource<COMPONENT_TYPE> aggregatedSource,
            @NotNull final ColumnSource<? extends RowSet> groupRowSetSource,
            @NotNull final ColumnSource<Long> startSource,
            @NotNull final ColumnSource<Long> endSource) {
        super(vectorType, aggregatedSource.getType());
        this.aggregatedSource = aggregatedSource;
        aggregatedSourcePrev = aggregatedSource.isImmutable() ? aggregatedSource : aggregatedSource.getPrevSource();
        this.groupRowSetSource = groupRowSetSource;
        this.startSource = startSource;
        this.endSource = endSource;

        startOffset = NULL_LONG;
        endOffset = NULL_LONG;
    }

    protected BaseAggregateSlicedColumnSource(
            @NotNull final Class<VECTOR_TYPE> vectorType,
            @NotNull final ColumnSource<COMPONENT_TYPE> aggregatedSource,
            @NotNull final ColumnSource<? extends RowSet> groupRowSetSource,
            final long startOffset,
            final long endOffset) {
        super(vectorType, aggregatedSource.getType());
        this.aggregatedSource = aggregatedSource;
        aggregatedSourcePrev = aggregatedSource.isImmutable() ? aggregatedSource : aggregatedSource.getPrevSource();
        this.groupRowSetSource = groupRowSetSource;
        this.startOffset = startOffset;
        this.endOffset = endOffset;

        startSource = null;
        endSource = null;
    }

    @Override
    public final UngroupedColumnSource<COMPONENT_TYPE> ungrouped() {
        return new UngroupedAggregateSlicedColumnSource<>(this);
    }

    @Override
    public final void startTrackingPrevValues() {}

    protected static final class AggregateSlicedFillContext implements FillContext {

        public final GetContext groupRowSetGetContext;
        public final GetContext startGetContext;
        public final GetContext endGetContext;

        private AggregateSlicedFillContext(@NotNull final ColumnSource<? extends RowSet> groupRowSetSource,
                @Nullable ColumnSource<Long> startSource, @Nullable ColumnSource<Long> endSource,
                final int chunkCapacity, final SharedContext sharedContext) {
            groupRowSetGetContext = groupRowSetSource.makeGetContext(chunkCapacity, sharedContext);
            startGetContext = startSource != null ? startSource.makeGetContext(chunkCapacity, sharedContext) : null;
            endGetContext = endSource != null ? endSource.makeGetContext(chunkCapacity, sharedContext) : null;
        }

        @Override
        public void close() {
            SafeCloseable.closeAll(groupRowSetGetContext, startGetContext, endGetContext);
        }
    }

    @Override
    public final FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new AggregateSlicedFillContext(groupRowSetSource, startSource, endSource, chunkCapacity, sharedContext);
    }

    @Override
    public final boolean isUngroupable() {
        return true;
    }

    private long getStartOffset(final long groupIndexKey) {
        return startSource != null ? startSource.getLong(groupIndexKey) : startOffset;
    }

    private long getEndOffset(final long groupIndexKey) {
        return endSource != null ? endSource.getLong(groupIndexKey) : endOffset;
    }

    private long getPrevStartOffset(final long groupIndexKey) {
        return startSource != null ? startSource.getPrevLong(groupIndexKey) : startOffset;
    }

    private long getPrevEndOffset(final long groupIndexKey) {
        return endSource != null ? endSource.getPrevLong(groupIndexKey) : endOffset;
    }

    @Override
    public long getUngroupedSize(final long groupIndexKey) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return 0;
        }

        final long startOffset = getStartOffset(groupIndexKey);
        final long endOffset = getEndOffset(groupIndexKey);

        if (startOffset == NULL_LONG || endOffset == NULL_LONG) {
            return 0;
        }

        final RowSet bucketRowSet = groupRowSetSource.get(groupIndexKey);
        final long rowPos = bucketRowSet.find(groupIndexKey);

        final long size = bucketRowSet.size();

        final long startPos = ClampUtil.clampLong(0, size, rowPos + startOffset);
        final long endPos = ClampUtil.clampLong(0, size, rowPos + endOffset);

        return endPos - startPos;
    }

    @Override
    public final long getUngroupedPrevSize(final long groupIndexKey) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return 0;
        }

        final long startOffset = getPrevStartOffset(groupIndexKey);
        final long endOffset = getPrevEndOffset(groupIndexKey);

        if (startOffset == NULL_LONG || endOffset == NULL_LONG) {
            return 0;
        }

        final RowSet bucketRowSet = getPrevGroupRowSet(groupIndexKey);
        final long rowPos = bucketRowSet.find(groupIndexKey);

        final long size = bucketRowSet.isTracking()
                ? bucketRowSet.trackingCast().sizePrev()
                : bucketRowSet.size();

        final long startPos = ClampUtil.clampLong(0, size, rowPos + startOffset);
        final long endPos = ClampUtil.clampLong(0, size, rowPos + endOffset);

        return endPos - startPos;
    }

    protected RowSet getPrevGroupRowSet(final long groupIndexKey) {
        final RowSet groupRowSetPrev = groupRowSetSource.getPrev(groupIndexKey);
        return groupRowSetPrev.isTracking()
                ? groupRowSetPrev.trackingCast().prev()
                : groupRowSetPrev;
    }

    private long getGroupOffsetKey(final long groupIndexKey, final int offsetInGroup) {
        final long startOffset = getStartOffset(groupIndexKey);
        final RowSet bucketRowSet = groupRowSetSource.get(groupIndexKey);

        final long rowPos = bucketRowSet.find(groupIndexKey);
        final long size = bucketRowSet.size();
        final long startPos = ClampUtil.clampLong(0, size, rowPos + startOffset);

        final long finalPos = startPos + offsetInGroup;
        return bucketRowSet.get(finalPos);
    }

    private long getPrevGroupOffsetKey(final long groupIndexKey, final int offsetInGroup) {
        final long startOffset = getPrevStartOffset(groupIndexKey);
        final RowSet bucketRowSet = getPrevGroupRowSet(groupIndexKey);

        final long rowPos = bucketRowSet.find(groupIndexKey);
        final long size = bucketRowSet.size();
        final long startPos = ClampUtil.clampLong(0, size, rowPos + startOffset);

        final long finalPos = startPos + offsetInGroup;
        return bucketRowSet.get(finalPos);
    }

    @Override
    public final Object getUngrouped(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        final long key = getGroupOffsetKey(groupIndexKey, offsetInGroup);
        return aggregatedSource.get(key);
    }

    @Override
    public final Object getUngroupedPrev(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        final long key = getPrevGroupOffsetKey(groupIndexKey, offsetInGroup);
        return aggregatedSource.getPrev(key);
    }

    @Override
    public final Boolean getUngroupedBoolean(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        final long key = getGroupOffsetKey(groupIndexKey, offsetInGroup);
        return aggregatedSource.getBoolean(key);
    }

    @Override
    public final Boolean getUngroupedPrevBoolean(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BOOLEAN;
        }
        final long key = getPrevGroupOffsetKey(groupIndexKey, offsetInGroup);
        return aggregatedSource.getPrevBoolean(key);
    }

    @Override
    public final double getUngroupedDouble(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_DOUBLE;
        }
        final long key = getGroupOffsetKey(groupIndexKey, offsetInGroup);
        return aggregatedSource.getDouble(key);
    }

    @Override
    public final double getUngroupedPrevDouble(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_DOUBLE;
        }
        final long key = getPrevGroupOffsetKey(groupIndexKey, offsetInGroup);
        return aggregatedSource.getPrevDouble(key);
    }

    @Override
    public final float getUngroupedFloat(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_FLOAT;
        }
        final long key = getGroupOffsetKey(groupIndexKey, offsetInGroup);
        return aggregatedSource.getFloat(key);
    }

    @Override
    public final float getUngroupedPrevFloat(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_FLOAT;
        }
        final long key = getPrevGroupOffsetKey(groupIndexKey, offsetInGroup);
        return aggregatedSource.getPrevFloat(key);
    }

    @Override
    public final byte getUngroupedByte(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BYTE;
        }
        final long key = getGroupOffsetKey(groupIndexKey, offsetInGroup);
        return aggregatedSource.getByte(key);
    }

    @Override
    public final byte getUngroupedPrevByte(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BYTE;
        }
        final long key = getPrevGroupOffsetKey(groupIndexKey, offsetInGroup);
        return aggregatedSource.getPrevByte(key);
    }

    @Override
    public final char getUngroupedChar(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_CHAR;
        }
        final long key = getGroupOffsetKey(groupIndexKey, offsetInGroup);
        return aggregatedSource.getChar(key);
    }

    @Override
    public final char getUngroupedPrevChar(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_CHAR;
        }
        final long key = getPrevGroupOffsetKey(groupIndexKey, offsetInGroup);
        return aggregatedSource.getPrevChar(key);
    }

    @Override
    public final short getUngroupedShort(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_SHORT;
        }
        final long key = getGroupOffsetKey(groupIndexKey, offsetInGroup);
        return aggregatedSource.getShort(key);
    }

    @Override
    public final short getUngroupedPrevShort(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_SHORT;
        }
        final long key = getPrevGroupOffsetKey(groupIndexKey, offsetInGroup);
        return aggregatedSource.getPrevShort(key);
    }

    @Override
    public final int getUngroupedInt(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_INT;
        }
        final long key = getGroupOffsetKey(groupIndexKey, offsetInGroup);
        return aggregatedSource.getInt(key);
    }

    @Override
    public final int getUngroupedPrevInt(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_INT;
        }
        final long key = getPrevGroupOffsetKey(groupIndexKey, offsetInGroup);
        return aggregatedSource.getPrevInt(key);
    }

    @Override
    public final long getUngroupedLong(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_LONG;
        }
        final long key = getGroupOffsetKey(groupIndexKey, offsetInGroup);
        return aggregatedSource.getLong(key);
    }

    @Override
    public final long getUngroupedPrevLong(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_LONG;
        }
        final long key = getPrevGroupOffsetKey(groupIndexKey, offsetInGroup);
        return aggregatedSource.getPrevLong(key);
    }

    @Override
    public boolean isStateless() {
        return aggregatedSource.isStateless()
                && groupRowSetSource.isStateless()
                && (startSource == null || startSource.isStateless())
                && (endSource == null || endSource.isStateless());
    }


    @Override
    public boolean isImmutable() {
        return aggregatedSource.isImmutable()
                && groupRowSetSource.isImmutable()
                && (startSource == null || startSource.isImmutable())
                && (endSource == null || endSource.isImmutable());
    }
}
