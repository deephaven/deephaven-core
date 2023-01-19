/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Single value source for Boolean.
 */
public class BooleanSingleValueSource extends SingleValueColumnSource<Boolean> implements MutableColumnSourceGetDefaults.ForBoolean {
    private Boolean current;
    private transient Boolean prev;

    BooleanSingleValueSource() {
        super(Boolean.class);
        current = QueryConstants.NULL_BOOLEAN;
        prev = QueryConstants.NULL_BOOLEAN;
    }

    @Nullable
    @Override
    public Boolean get(long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return QueryConstants.NULL_BOOLEAN;
        }
        return current;
    }

    @Nullable
    @Override
    public Boolean getPrev(long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return QueryConstants.NULL_BOOLEAN;
        }
        if (!isTrackingPrevValues || changeTime < LogicalClock.DEFAULT.currentStep()) {
            return current;
        }
        return prev;
    }

    @Override
    public final void set(Boolean value) {
        if (isTrackingPrevValues) {
            final long currentStep = LogicalClock.DEFAULT.currentStep();
            if (changeTime < currentStep) {
                prev = current;
                changeTime = currentStep;
            }
        }
        current = value;
    }

    @Override
    public final void set(long key, Boolean value) {
        set(value);
    }

    @Override
    public void setNull(long key) {
        // region null set
        set(QueryConstants.NULL_BOOLEAN);
        // endregion null set
    }

    @Override
    public final void fillFromChunk(
            @NotNull FillFromContext context,
            @NotNull Chunk<? extends Values> src,
            @NotNull RowSequence orderedKeys) {
        if (orderedKeys.isEmpty()) {
            return;
        }

        // We can only hold one value anyway, so arbitrarily take the first value in the chunk and ignore the rest.
        final ObjectChunk<Boolean, ? extends Values> chunk = src.asObjectChunk();
        set(chunk.get(0));
    }

    @Override
    public void fillFromChunkUnordered(
            @NotNull FillFromContext context,
            @NotNull Chunk<? extends Values> src,
            @NotNull LongChunk<RowKeys> keys) {
        if (keys.size() == 0) {
            return;
        }
        // We can only hold one value anyway, so arbitrarily take the first value in the chunk and ignore the rest.
        final ObjectChunk<Boolean, ? extends Values> chunk = src.asObjectChunk();
        set(chunk.get(0));
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        // We can only hold one value, fill the chunk with the value obtained from an arbitrarily valid rowKey
        destination.setSize(rowSequence.intSize());
        destination.asWritableObjectChunk().fillWithValue(0, rowSequence.intSize(), get(0));
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context,
            @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        // We can only hold one value, fill the chunk with the value obtained from an arbitrarily valid rowKey
        destination.setSize(rowSequence.intSize());
        destination.asWritableObjectChunk().fillWithValue(0, rowSequence.intSize(), get(0));
    }

    @Override
    public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        // We can only hold one value, fill the chunk with the value obtained from an arbitrarily valid rowKey
        Boolean value = get(0);
        final WritableObjectChunk<Boolean, ? super Values> destChunk = dest.asWritableObjectChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            destChunk.set(ii, keys.get(ii) == RowSequence.NULL_ROW_KEY ? null : value);
        }
        destChunk.setSize(keys.size());
    }

    @Override
    public void fillPrevChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        // We can only hold one value, fill the chunk with the value obtained from an arbitrarily valid rowKey
        Boolean value = getPrev(0);
        final WritableObjectChunk<Boolean, ? super Values> destChunk = dest.asWritableObjectChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            destChunk.set(ii, keys.get(ii) == RowSequence.NULL_ROW_KEY ? null : value);
        }
        destChunk.setSize(keys.size());
    }

    @Override
    public boolean providesFillUnordered() {
        return true;
    }
}