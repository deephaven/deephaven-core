//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharacterSingleValueSource and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.type.TypeUtils.unbox;

/**
 * Single value source for Object.
 * <p>
 * The C-haracterSingleValueSource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-haracter is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class ObjectSingleValueSource<T> extends SingleValueColumnSource<T>
        implements MutableColumnSourceGetDefaults.ForObject<T> {

    private T current;
    private transient T prev;

    // region Constructor
    public ObjectSingleValueSource(Class<T> type, Class<?> componentType) {
        super(type, componentType);
        current = null;
        prev = null;
    }
    // endregion Constructor

    @Override
    public final void set(T value) {
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
        set(null);
    }

    @Override
    public final void set(long key, T value) {
        set(value);
    }

    @Override
    public final T get(long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        return current;
    }

    @Override
    public final T getPrev(long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
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
        final ObjectChunk<T, ? extends Values> chunk = src.asObjectChunk();
        set(chunk.get(0));
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src,
            @NotNull LongChunk<RowKeys> keys) {
        if (keys.size() == 0) {
            return;
        }
        // We can only hold one value anyway, so arbitrarily take the first value in the chunk and ignore the rest.
        final ObjectChunk<T, ? extends Values> chunk = src.asObjectChunk();
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
        T value = getPrev(0); // avoid duplicating the current vs prev logic in getPrev
        destination.setSize(rowSequence.intSize());
        destination.asWritableObjectChunk().fillWithValue(0, rowSequence.intSize(), value);
    }

    @Override
    public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        final WritableObjectChunk<T, ? super Values> destChunk = dest.asWritableObjectChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            destChunk.set(ii, keys.get(ii) == RowSequence.NULL_ROW_KEY ? null : current);
        }
        destChunk.setSize(keys.size());
    }

    @Override
    public void fillPrevChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        T value = getPrev(0); // avoid duplicating the current vs prev logic in getPrev
        final WritableObjectChunk<T, ? super Values> destChunk = dest.asWritableObjectChunk();
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
