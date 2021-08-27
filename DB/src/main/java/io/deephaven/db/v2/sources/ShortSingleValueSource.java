/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharacterSingleValueSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.util.QueryConstants;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ShortChunk;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.type.TypeUtils.unbox;

/**
 * Single value source for Short.
 * <p>
 * The C-haracterSingleValueSource is replicated to all other types with
 * io.deephaven.db.v2.sources.Replicate.
 *
 * (C-haracter is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class ShortSingleValueSource extends SingleValueColumnSource<Short> implements MutableColumnSourceGetDefaults.ForShort {

    private short current;
    private transient short prev;

    // region Constructor
    public ShortSingleValueSource() {
        super(short.class);
        current = QueryConstants.NULL_SHORT;
        prev = QueryConstants.NULL_SHORT;
    }
    // endregion Constructor

    @Override
    public final void set(Short value) {
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
    public final void set(short value) {
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
    public final void set(long key, short value) {
        set(value);
    }

    @Override
    public final short getShort(long index) {
        return current;
    }

    @Override
    public final short getPrevShort(long index) {
        if (!isTrackingPrevValues || changeTime < LogicalClock.DEFAULT.currentStep()) {
            return current;
        }
        return prev;
    }

    @Override
    public final void copy(ColumnSource<? extends Short> sourceColumn, long sourceKey, long destKey) {
        set(sourceColumn.get(sourceKey));
    }

    @Override
    public final void fillFromChunk(@NotNull FillFromContext context, @NotNull Chunk<? extends Attributes.Values> src, @NotNull OrderedKeys orderedKeys) {
        if (orderedKeys.size() == 0) {
            return;
        }
        // We can only hold one value anyway, so arbitrarily take the first value in the chunk and ignore the rest.
        final ShortChunk<? extends Attributes.Values> chunk = src.asShortChunk();
        set(chunk.get(0));
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Attributes.Values> src, @NotNull LongChunk<Attributes.KeyIndices> keys) {
        if (keys.size() == 0) {
            return;
        }
        // We can only hold one value anyway, so arbitrarily take the first value in the chunk and ignore the rest.
        final ShortChunk<? extends Attributes.Values> chunk = src.asShortChunk();
        set(chunk.get(0));
    }
}