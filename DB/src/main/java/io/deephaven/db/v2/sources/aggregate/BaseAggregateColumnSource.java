/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources.aggregate;

import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.v2.sources.AbstractColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.UngroupedColumnSource;
import io.deephaven.db.v2.sources.chunk.SharedContext;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.*;

/**
 * Base {@link ColumnSource} implementation for aggregation result columns.
 */
abstract class BaseAggregateColumnSource<DB_ARRAY_TYPE extends DbArrayBase, COMPONENT_TYPE>
    extends AbstractColumnSource<DB_ARRAY_TYPE>
    implements AggregateColumnSource<DB_ARRAY_TYPE, COMPONENT_TYPE> {

    final ColumnSource<COMPONENT_TYPE> aggregatedSource;
    final ColumnSource<Index> indexSource;

    BaseAggregateColumnSource(@NotNull final Class<DB_ARRAY_TYPE> dbArrayType,
        @NotNull final ColumnSource<COMPONENT_TYPE> aggregatedSource,
        @NotNull final ColumnSource<Index> indexSource) {
        super(dbArrayType, aggregatedSource.getType());
        this.aggregatedSource = aggregatedSource;
        this.indexSource = indexSource;
    }

    @Override
    public final UngroupedColumnSource<COMPONENT_TYPE> ungrouped() {
        return new UngroupedAggregateColumnSource<>(this);
    }

    @Override
    public final void startTrackingPrevValues() {}

    static final class AggregateFillContext implements FillContext {

        final GetContext indexGetContext;

        private AggregateFillContext(@NotNull final ColumnSource<Index> indexSource,
            final int chunkCapacity, final SharedContext sharedContext) {
            // TODO: Implement a proper shareable context to use with other instances that share an
            // index source.
            // Current usage is "safe" because index sources are only exposed through this wrapper,
            // and all
            // sources at a given level will pass through their ordered keys to the index source
            // unchanged.
            indexGetContext = indexSource.makeGetContext(chunkCapacity, sharedContext);
        }

        @Override
        public final void close() {
            indexGetContext.close();
        }
    }

    @Override
    public final FillContext makeFillContext(final int chunkCapacity,
        final SharedContext sharedContext) {
        return new AggregateFillContext(indexSource, chunkCapacity, sharedContext);
    }

    @Override
    public final boolean isUngroupable() {
        return true;
    }

    @Override
    public final long getUngroupedSize(final long groupIndexKey) {
        if (groupIndexKey == Index.NULL_KEY) {
            return 0;
        }
        return indexSource.get(groupIndexKey).size();
    }

    @Override
    public final long getUngroupedPrevSize(final long groupIndexKey) {
        if (groupIndexKey == Index.NULL_KEY) {
            return 0;
        }
        return indexSource.getPrev(groupIndexKey).getPrevIndex().size();
    }

    @Override
    public final Object getUngrouped(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == Index.NULL_KEY) {
            return null;
        }
        return aggregatedSource.get(indexSource.get(groupIndexKey).get(offsetInGroup));
    }

    @Override
    public final Object getUngroupedPrev(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == Index.NULL_KEY) {
            return null;
        }
        return aggregatedSource
            .getPrev(indexSource.getPrev(groupIndexKey).getPrevIndex().get(offsetInGroup));
    }

    @Override
    public final Boolean getUngroupedBoolean(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == Index.NULL_KEY) {
            return NULL_BOOLEAN;
        }
        return aggregatedSource.getBoolean(indexSource.get(groupIndexKey).get(offsetInGroup));
    }

    @Override
    public final Boolean getUngroupedPrevBoolean(final long groupIndexKey,
        final int offsetInGroup) {
        if (groupIndexKey == Index.NULL_KEY) {
            return NULL_BOOLEAN;
        }
        return aggregatedSource
            .getPrevBoolean(indexSource.getPrev(groupIndexKey).getPrevIndex().get(offsetInGroup));
    }

    @Override
    public final double getUngroupedDouble(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == Index.NULL_KEY) {
            return NULL_DOUBLE;
        }
        return aggregatedSource.getDouble(indexSource.get(groupIndexKey).get(offsetInGroup));
    }

    @Override
    public final double getUngroupedPrevDouble(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == Index.NULL_KEY) {
            return NULL_DOUBLE;
        }
        return aggregatedSource
            .getPrevDouble(indexSource.getPrev(groupIndexKey).getPrevIndex().get(offsetInGroup));
    }

    @Override
    public final float getUngroupedFloat(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == Index.NULL_KEY) {
            return NULL_FLOAT;
        }
        return aggregatedSource.getFloat(indexSource.get(groupIndexKey).get(offsetInGroup));
    }

    @Override
    public final float getUngroupedPrevFloat(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == Index.NULL_KEY) {
            return NULL_FLOAT;
        }
        return aggregatedSource
            .getPrevFloat(indexSource.getPrev(groupIndexKey).getPrevIndex().get(offsetInGroup));
    }

    @Override
    public final byte getUngroupedByte(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == Index.NULL_KEY) {
            return NULL_BYTE;
        }
        return aggregatedSource.getByte(indexSource.get(groupIndexKey).get(offsetInGroup));
    }

    @Override
    public final byte getUngroupedPrevByte(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == Index.NULL_KEY) {
            return NULL_BYTE;
        }
        return aggregatedSource
            .getPrevByte(indexSource.getPrev(groupIndexKey).getPrevIndex().get(offsetInGroup));
    }

    @Override
    public final char getUngroupedChar(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == Index.NULL_KEY) {
            return NULL_CHAR;
        }
        return aggregatedSource.getChar(indexSource.get(groupIndexKey).get(offsetInGroup));
    }

    @Override
    public final char getUngroupedPrevChar(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == Index.NULL_KEY) {
            return NULL_CHAR;
        }
        return aggregatedSource
            .getPrevChar(indexSource.getPrev(groupIndexKey).getPrevIndex().get(offsetInGroup));
    }

    @Override
    public final short getUngroupedShort(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == Index.NULL_KEY) {
            return NULL_SHORT;
        }
        return aggregatedSource.getShort(indexSource.get(groupIndexKey).get(offsetInGroup));
    }

    @Override
    public final short getUngroupedPrevShort(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == Index.NULL_KEY) {
            return NULL_SHORT;
        }
        return aggregatedSource
            .getPrevShort(indexSource.getPrev(groupIndexKey).getPrevIndex().get(offsetInGroup));
    }

    @Override
    public final int getUngroupedInt(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == Index.NULL_KEY) {
            return NULL_INT;
        }
        return aggregatedSource.getInt(indexSource.get(groupIndexKey).get(offsetInGroup));
    }

    @Override
    public final int getUngroupedPrevInt(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == Index.NULL_KEY) {
            return NULL_INT;
        }
        return aggregatedSource
            .getPrevInt(indexSource.getPrev(groupIndexKey).getPrevIndex().get(offsetInGroup));
    }

    @Override
    public final long getUngroupedLong(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == Index.NULL_KEY) {
            return NULL_LONG;
        }
        return aggregatedSource.getLong(indexSource.get(groupIndexKey).get(offsetInGroup));
    }

    @Override
    public final long getUngroupedPrevLong(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == Index.NULL_KEY) {
            return NULL_LONG;
        }
        return aggregatedSource
            .getPrevLong(indexSource.getPrev(groupIndexKey).getPrevIndex().get(offsetInGroup));
    }
}
