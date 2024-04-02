//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharacterSingleValueSource and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_SHORT;
import static io.deephaven.util.type.TypeUtils.unbox;

/**
 * Single value source for Short.
 * <p>
 * The C-haracterSingleValueSource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-haracter is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class ShortSingleValueSource extends SingleValueColumnSource<Short>
        implements MutableColumnSourceGetDefaults.ForShort {

    private short current;
    private transient short prev;

    // region Constructor
    public ShortSingleValueSource() {
        super(short.class);
        current = NULL_SHORT;
        prev = NULL_SHORT;
    }
    // endregion Constructor

    @Override
    public final void set(Short value) {
        if (isTrackingPrevValues) {
            final long currentStep = updateGraph.clock().currentStep();
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
            final long currentStep = updateGraph.clock().currentStep();
            if (changeTime < currentStep) {
                prev = current;
                changeTime = currentStep;
            }
        }
        current = value;
    }
    // endregion UnboxedSetter

    @Override
    public final void setNull() {
        set(NULL_SHORT);
    }

    @Override
    public final void set(long key, short value) {
        set(value);
    }

    @Override
    public final short getShort(long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_SHORT;
        }
        return current;
    }

    @Override
    public final short getPrevShort(long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_SHORT;
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
        final ShortChunk<? extends Values> chunk = src.asShortChunk();
        set(chunk.get(0));
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src,
            @NotNull LongChunk<RowKeys> keys) {
        if (keys.size() == 0) {
            return;
        }
        // We can only hold one value anyway, so arbitrarily take the first value in the chunk and ignore the rest.
        final ShortChunk<? extends Values> chunk = src.asShortChunk();
        set(chunk.get(0));
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        destination.setSize(rowSequence.intSize());
        destination.asWritableShortChunk().fillWithValue(0, rowSequence.intSize(), current);
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context,
            @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        short value = getPrevShort(0); // avoid duplicating the current vs prev logic in getPrevShort
        destination.setSize(rowSequence.intSize());
        destination.asWritableShortChunk().fillWithValue(0, rowSequence.intSize(), value);
    }

    @Override
    public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        final WritableShortChunk<? super Values> destChunk = dest.asWritableShortChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            destChunk.set(ii, keys.get(ii) == RowSequence.NULL_ROW_KEY ? NULL_SHORT : current);
        }
        destChunk.setSize(keys.size());
    }

    @Override
    public void fillPrevChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        short value = getPrevShort(0); // avoid duplicating the current vs prev logic in getPrevShort
        final WritableShortChunk<? super Values> destChunk = dest.asWritableShortChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            destChunk.set(ii, keys.get(ii) == RowSequence.NULL_ROW_KEY ? NULL_SHORT : value);
        }
        destChunk.setSize(keys.size());
    }

    @Override
    public boolean providesFillUnordered() {
        return true;
    }
}
