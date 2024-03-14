//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharacterSingleValueSource and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableObjectChunk;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_BOOLEAN;

/**
 * Single value source for Boolean.
 * <p>
 * The C-haracterSingleValueSource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-haracter is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class BooleanSingleValueSource extends SingleValueColumnSource<Boolean>
        implements MutableColumnSourceGetDefaults.ForBoolean {

    private Boolean current;
    private transient Boolean prev;

    // region Constructor
    public BooleanSingleValueSource() {
        super(Boolean.class);
        current = NULL_BOOLEAN;
        prev = NULL_BOOLEAN;
    }
    // endregion Constructor

    @Override
    public final void set(Boolean value) {
        if (isTrackingPrevValues) {
            final long currentStep = updateGraph.clock().currentStep();
            if (changeTime < currentStep) {
                prev = current;
                changeTime = currentStep;
            }
        }
        current = value;
    }

    // region UnboxedSetter
    // endregion UnboxedSetter

    @Override
    public final void setNull() {
        set(NULL_BOOLEAN);
    }

    @Override
    public final void set(long key, Boolean value) {
        set(value);
    }

    @Override
    public final Boolean get(long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BOOLEAN;
        }
        return current;
    }

    @Override
    public final Boolean getPrev(long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BOOLEAN;
        }
        if (!isTrackingPrevValues || changeTime < updateGraph.clock().currentStep()) {
            return current;
        }
        return prev;
    }

    @Override
    public final void fillFromChunk(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src,
            @NotNull RowSequence rowSequence) {
        if (rowSequence.size() == 0) {
            return;
        }
        // We can only hold one value anyway, so arbitrarily take the first value in the chunk and ignore the rest.
        final ObjectChunk<Boolean, ? extends Values> chunk = src.asObjectChunk();
        set(chunk.get(0));
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src,
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
        destination.setSize(rowSequence.intSize());
        destination.asWritableObjectChunk().fillWithValue(0, rowSequence.intSize(), current);
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context,
            @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        Boolean value = getPrev(0); // avoid duplicating the current vs prev logic in getPrev
        destination.setSize(rowSequence.intSize());
        destination.asWritableObjectChunk().fillWithValue(0, rowSequence.intSize(), value);
    }

    @Override
    public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        final WritableObjectChunk<Boolean, ? super Values> destChunk = dest.asWritableObjectChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            destChunk.set(ii, keys.get(ii) == RowSequence.NULL_ROW_KEY ? NULL_BOOLEAN : current);
        }
        destChunk.setSize(keys.size());
    }

    @Override
    public void fillPrevChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        Boolean value = getPrev(0); // avoid duplicating the current vs prev logic in getPrev
        final WritableObjectChunk<Boolean, ? super Values> destChunk = dest.asWritableObjectChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            destChunk.set(ii, keys.get(ii) == RowSequence.NULL_ROW_KEY ? NULL_BOOLEAN : value);
        }
        destChunk.setSize(keys.size());
    }

    @Override
    public boolean providesFillUnordered() {
        return true;
    }
}
