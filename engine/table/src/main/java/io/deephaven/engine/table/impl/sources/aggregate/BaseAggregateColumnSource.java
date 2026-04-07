//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.aggregate;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.vector.Vector;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.UngroupedColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.*;

/**
 * Base {@link ColumnSource} implementation for aggregation result columns.
 */
abstract class BaseAggregateColumnSource<VECTOR_TYPE extends Vector<VECTOR_TYPE>, COMPONENT_TYPE>
        extends AbstractColumnSource<VECTOR_TYPE>
        implements AggregateColumnSource<VECTOR_TYPE, COMPONENT_TYPE> {

    final ColumnSource<COMPONENT_TYPE> aggregatedSource;
    final ColumnSource<COMPONENT_TYPE> aggregatedSourcePrev;
    final ColumnSource<? extends RowSet> groupRowSetSource;

    BaseAggregateColumnSource(@NotNull final Class<VECTOR_TYPE> vectorType,
            @NotNull final ColumnSource<COMPONENT_TYPE> aggregatedSource,
            @NotNull final ColumnSource<? extends RowSet> groupRowSetSource) {
        super(vectorType, aggregatedSource.getType());
        this.aggregatedSource = aggregatedSource;
        aggregatedSourcePrev = aggregatedSource.isImmutable() ? aggregatedSource : aggregatedSource.getPrevSource();
        this.groupRowSetSource = groupRowSetSource;
    }

    @Override
    public final UngroupedColumnSource<COMPONENT_TYPE> ungrouped() {
        return new UngroupedAggregateColumnSource<>(this);
    }

    @Override
    public final void startTrackingPrevValues() {}

    static final class AggregateFillContext implements FillContext {

        final GetContext groupRowSetGetContext;

        private AggregateFillContext(@NotNull final ColumnSource<? extends RowSet> groupRowSetSource,
                final int chunkCapacity, final SharedContext sharedContext) {
            // TODO: Implement a proper shareable context to use with other instances that share a RowSet source.
            // Current usage is "safe" because RowSet sources are only exposed through this wrapper, and all
            // sources at a given level will pass through their ordered keys to the RowSet source unchanged.
            groupRowSetGetContext = groupRowSetSource.makeGetContext(chunkCapacity, sharedContext);
        }

        @Override
        public void close() {
            groupRowSetGetContext.close();
        }
    }

    @Override
    public final FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new AggregateFillContext(groupRowSetSource, chunkCapacity, sharedContext);
    }

    @Override
    public final boolean isUngroupable() {
        return true;
    }

    @Override
    public final long getUngroupedSize(final long groupRowKey) {
        if (groupRowKey == RowSequence.NULL_ROW_KEY) {
            return 0;
        }
        return groupRowSetSource.get(groupRowKey).size();
    }

    @Override
    public final long getUngroupedPrevSize(final long groupRowKey) {
        if (groupRowKey == RowSequence.NULL_ROW_KEY) {
            return 0;
        }
        final RowSet groupRowSetPrev = groupRowSetSource.getPrev(groupRowKey);
        return groupRowSetPrev.isTracking()
                ? groupRowSetPrev.trackingCast().sizePrev()
                : groupRowSetPrev.size();
    }

    @Override
    public void getUngroupedSize(FillContext fillContext, RowSequence rowSequence, WritableLongChunk<Values> sizes) {
        final AggregateFillContext afc = (AggregateFillContext) fillContext;
        final ObjectChunk<RowSet, ? extends Values> rowsetChunk =
                groupRowSetSource.getChunk(afc.groupRowSetGetContext, rowSequence).asObjectChunk();
        final int size = rowsetChunk.size();
        for (int ii = 0; ii < size; ++ii) {
            sizes.set(ii, rowsetChunk.get(ii).size());
        }
        sizes.setSize(size);
    }

    @Override
    public void getUngroupedPrevSize(FillContext fillContext, RowSequence rowSequence,
            WritableLongChunk<Values> sizes) {
        final AggregateFillContext afc = (AggregateFillContext) fillContext;
        final ObjectChunk<RowSet, ? extends Values> rowsetChunk =
                groupRowSetSource.getPrevChunk(afc.groupRowSetGetContext, rowSequence).asObjectChunk();
        final int size = rowsetChunk.size();
        for (int ii = 0; ii < size; ++ii) {
            final RowSet groupRowSetPrev = rowsetChunk.get(ii);
            if (groupRowSetPrev.isTracking()) {
                sizes.set(ii, groupRowSetPrev.trackingCast().sizePrev());
            } else {
                sizes.set(ii, groupRowSetPrev.size());
            }
        }
        sizes.setSize(size);
    }

    private long getPrevRowKey(final long groupIndexKey, final int offsetInGroup) {
        final RowSet groupRowSetPrev = groupRowSetSource.getPrev(groupIndexKey);
        return groupRowSetPrev.isTracking()
                ? groupRowSetPrev.trackingCast().getPrev(offsetInGroup)
                : groupRowSetPrev.get(offsetInGroup);
    }

    RowSet getPrevGroupRowSet(final long groupIndexKey) {
        final RowSet groupRowSetPrev = groupRowSetSource.getPrev(groupIndexKey);
        return groupRowSetPrev.isTracking()
                ? groupRowSetPrev.trackingCast().prev()
                : groupRowSetPrev;
    }

    @Override
    public final Object getUngrouped(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        return aggregatedSource.get(groupRowSetSource.get(groupRowKey).get(offsetInGroup));
    }

    @Override
    public final Object getUngroupedPrev(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        return aggregatedSource.getPrev(getPrevRowKey(groupRowKey, offsetInGroup));
    }

    @Override
    public final Boolean getUngroupedBoolean(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BOOLEAN;
        }
        return aggregatedSource.getBoolean(groupRowSetSource.get(groupRowKey).get(offsetInGroup));
    }

    @Override
    public final Boolean getUngroupedPrevBoolean(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BOOLEAN;
        }
        return aggregatedSource.getPrevBoolean(getPrevRowKey(groupRowKey, offsetInGroup));
    }

    @Override
    public final double getUngroupedDouble(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_DOUBLE;
        }
        return aggregatedSource.getDouble(groupRowSetSource.get(groupRowKey).get(offsetInGroup));
    }

    @Override
    public final double getUngroupedPrevDouble(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_DOUBLE;
        }
        return aggregatedSource.getPrevDouble(getPrevRowKey(groupRowKey, offsetInGroup));
    }

    @Override
    public final float getUngroupedFloat(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_FLOAT;
        }
        return aggregatedSource.getFloat(groupRowSetSource.get(groupRowKey).get(offsetInGroup));
    }

    @Override
    public final float getUngroupedPrevFloat(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_FLOAT;
        }
        return aggregatedSource.getPrevFloat(getPrevRowKey(groupRowKey, offsetInGroup));
    }

    @Override
    public final byte getUngroupedByte(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BYTE;
        }
        return aggregatedSource.getByte(groupRowSetSource.get(groupRowKey).get(offsetInGroup));
    }

    @Override
    public final byte getUngroupedPrevByte(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BYTE;
        }
        return aggregatedSource.getPrevByte(getPrevRowKey(groupRowKey, offsetInGroup));
    }

    @Override
    public final char getUngroupedChar(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_CHAR;
        }
        return aggregatedSource.getChar(groupRowSetSource.get(groupRowKey).get(offsetInGroup));
    }

    @Override
    public final char getUngroupedPrevChar(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_CHAR;
        }
        return aggregatedSource.getPrevChar(getPrevRowKey(groupRowKey, offsetInGroup));
    }

    @Override
    public final short getUngroupedShort(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_SHORT;
        }
        return aggregatedSource.getShort(groupRowSetSource.get(groupRowKey).get(offsetInGroup));
    }

    @Override
    public final short getUngroupedPrevShort(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_SHORT;
        }
        return aggregatedSource.getPrevShort(getPrevRowKey(groupRowKey, offsetInGroup));
    }

    @Override
    public final int getUngroupedInt(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_INT;
        }
        return aggregatedSource.getInt(groupRowSetSource.get(groupRowKey).get(offsetInGroup));
    }

    @Override
    public final int getUngroupedPrevInt(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_INT;
        }
        return aggregatedSource.getPrevInt(getPrevRowKey(groupRowKey, offsetInGroup));
    }

    @Override
    public final long getUngroupedLong(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_LONG;
        }
        return aggregatedSource.getLong(groupRowSetSource.get(groupRowKey).get(offsetInGroup));
    }

    @Override
    public final long getUngroupedPrevLong(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_LONG;
        }
        return aggregatedSource.getPrevLong(getPrevRowKey(groupRowKey, offsetInGroup));
    }

    @Override
    public boolean isStateless() {
        return aggregatedSource.isStateless() && groupRowSetSource.isStateless();
    }

    @Override
    public boolean isImmutable() {
        return aggregatedSource.isImmutable() && groupRowSetSource.isImmutable();
    }

    @Override
    public ColumnSource<COMPONENT_TYPE> getAggregatedSource() {
        return aggregatedSource;
    }
}
