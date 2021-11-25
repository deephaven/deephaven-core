/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharacterSingleValueSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.util.QueryConstants;
import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.LongChunk;
import io.deephaven.engine.chunk.Chunk;
import io.deephaven.engine.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.type.TypeUtils.unbox;

/**
 * Single value source for Long.
 * <p>
 * The C-haracterSingleValueSource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-haracter is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class LongSingleValueSource extends SingleValueColumnSource<Long> implements MutableColumnSourceGetDefaults.ForLong {

    private long current;
    private transient long prev;

    // region Constructor
    public LongSingleValueSource() {
        super(long.class);
        current = QueryConstants.NULL_LONG;
        prev = QueryConstants.NULL_LONG;
    }
    // endregion Constructor

    @Override
    public final void set(Long value) {
        if (isTrackingPrevValues) {
            final long currentStep = LogicalClock.DEFAULT.currentStep();
            if (changeTime < currentStep) {
                prev = current;
                changeTime = currentStep;
            }
        }
        current = unbox(value);
    }

    // region UnboxedSetter
    @Override
    public final void set(long value) {
        if (isTrackingPrevValues) {
            final long currentStep = LogicalClock.DEFAULT.currentStep();
            if (changeTime < currentStep) {
                prev = current;
                changeTime = currentStep;
            }
        }
        current = value;
    }
    // endregion UnboxedSetter

    @Override
    public final void set(long key, long value) {
        set(value);
    }

    @Override
    public final long getLong(long index) {
        return current;
    }

    @Override
    public final long getPrevLong(long index) {
        if (!isTrackingPrevValues || changeTime < LogicalClock.DEFAULT.currentStep()) {
            return current;
        }
        return prev;
    }

    @Override
    public final void copy(ColumnSource<? extends Long> sourceColumn, long sourceKey, long destKey) {
        set(sourceColumn.get(sourceKey));
    }

    @Override
    public final void fillFromChunk(@NotNull FillFromContext context, @NotNull Chunk<? extends Attributes.Values> src, @NotNull RowSequence rowSequence) {
        if (rowSequence.size() == 0) {
            return;
        }
        // We can only hold one value anyway, so arbitrarily take the first value in the chunk and ignore the rest.
        final LongChunk<? extends Attributes.Values> chunk = src.asLongChunk();
        set(chunk.get(0));
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Attributes.Values> src, @NotNull LongChunk<Attributes.RowKeys> keys) {
        if (keys.size() == 0) {
            return;
        }
        // We can only hold one value anyway, so arbitrarily take the first value in the chunk and ignore the rest.
        final LongChunk<? extends Attributes.Values> chunk = src.asLongChunk();
        set(chunk.get(0));
    }
}