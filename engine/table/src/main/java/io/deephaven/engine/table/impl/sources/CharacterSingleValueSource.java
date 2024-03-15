//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_CHAR;
import static io.deephaven.util.type.TypeUtils.unbox;

/**
 * Single value source for Character.
 * <p>
 * The C-haracterSingleValueSource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-haracter is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class CharacterSingleValueSource extends SingleValueColumnSource<Character>
        implements MutableColumnSourceGetDefaults.ForChar {

    private char current;
    private transient char prev;

    // region Constructor
    public CharacterSingleValueSource() {
        super(char.class);
        current = NULL_CHAR;
        prev = NULL_CHAR;
    }
    // endregion Constructor

    @Override
    public final void set(Character value) {
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
    public final void set(char value) {
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
        set(NULL_CHAR);
    }

    @Override
    public final void set(long key, char value) {
        set(value);
    }

    @Override
    public final char getChar(long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_CHAR;
        }
        return current;
    }

    @Override
    public final char getPrevChar(long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_CHAR;
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
        final CharChunk<? extends Values> chunk = src.asCharChunk();
        set(chunk.get(0));
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src,
            @NotNull LongChunk<RowKeys> keys) {
        if (keys.size() == 0) {
            return;
        }
        // We can only hold one value anyway, so arbitrarily take the first value in the chunk and ignore the rest.
        final CharChunk<? extends Values> chunk = src.asCharChunk();
        set(chunk.get(0));
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        destination.setSize(rowSequence.intSize());
        destination.asWritableCharChunk().fillWithValue(0, rowSequence.intSize(), current);
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context,
            @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        char value = getPrevChar(0); // avoid duplicating the current vs prev logic in getPrevChar
        destination.setSize(rowSequence.intSize());
        destination.asWritableCharChunk().fillWithValue(0, rowSequence.intSize(), value);
    }

    @Override
    public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        final WritableCharChunk<? super Values> destChunk = dest.asWritableCharChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            destChunk.set(ii, keys.get(ii) == RowSequence.NULL_ROW_KEY ? NULL_CHAR : current);
        }
        destChunk.setSize(keys.size());
    }

    @Override
    public void fillPrevChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        char value = getPrevChar(0); // avoid duplicating the current vs prev logic in getPrevChar
        final WritableCharChunk<? super Values> destChunk = dest.asWritableCharChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            destChunk.set(ii, keys.get(ii) == RowSequence.NULL_ROW_KEY ? NULL_CHAR : value);
        }
        destChunk.setSize(keys.size());
    }

    @Override
    public boolean providesFillUnordered() {
        return true;
    }
}
