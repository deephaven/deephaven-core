//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharacterSingleValueSource and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;
import java.util.function.LongConsumer;

import static io.deephaven.util.QueryConstants.NULL_BYTE;
import static io.deephaven.util.type.TypeUtils.unbox;

/**
 * Single value source for Byte.
 * <p>
 * The C-haracterSingleValueSource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 * <p>
 * (C-haracter is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class ByteSingleValueSource extends SingleValueColumnSource<Byte>
        implements MutableColumnSourceGetDefaults.ForByte {

    private byte current;
    private transient byte prev;

    // region Constructor
    public ByteSingleValueSource() {
        super(byte.class);
        current = NULL_BYTE;
        prev = NULL_BYTE;
    }
    // endregion Constructor

    @Override
    public final void set(Byte value) {
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
    public final void set(byte value) {
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
        set(NULL_BYTE);
    }

    @Override
    public final void set(long key, byte value) {
        set(value);
    }

    @Override
    public final byte getByte(long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BYTE;
        }
        return current;
    }

    @Override
    public final byte getPrevByte(long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BYTE;
        }
        if (!isTrackingPrevValues || changeTime < updateGraph.clock().currentStep()) {
            return current;
        }
        return prev;
    }

    @Override
    public final void fillFromChunk(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src,
            @NotNull RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            return;
        }
        // We can only hold one value anyway, so arbitrarily take the first value in the chunk and ignore the rest.
        final ByteChunk<? extends Values> chunk = src.asByteChunk();
        set(chunk.get(0));
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src,
            @NotNull LongChunk<RowKeys> keys) {
        if (keys.size() == 0) {
            return;
        }
        // We can only hold one value anyway, so arbitrarily take the first value in the chunk and ignore the rest.
        final ByteChunk<? extends Values> chunk = src.asByteChunk();
        set(chunk.get(0));
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        destination.setSize(rowSequence.intSize());
        destination.asWritableByteChunk().fillWithValue(0, rowSequence.intSize(), current);
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context,
            @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        byte value = getPrevByte(0); // avoid duplicating the current vs prev logic in getPrevByte
        destination.setSize(rowSequence.intSize());
        destination.asWritableByteChunk().fillWithValue(0, rowSequence.intSize(), value);
    }

    @Override
    public void fillChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        final WritableByteChunk<? super Values> destChunk = dest.asWritableByteChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            destChunk.set(ii, keys.get(ii) == RowSequence.NULL_ROW_KEY ? NULL_BYTE : current);
        }
        destChunk.setSize(keys.size());
    }

    @Override
    public void fillPrevChunkUnordered(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        byte value = getPrevByte(0); // avoid duplicating the current vs prev logic in getPrevByte
        final WritableByteChunk<? super Values> destChunk = dest.asWritableByteChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            destChunk.set(ii, keys.get(ii) == RowSequence.NULL_ROW_KEY ? NULL_BYTE : value);
        }
        destChunk.setSize(keys.size());
    }

    @Override
    public boolean providesFillUnordered() {
        return true;
    }

    @Override
    public void estimatePushdownFilterCost(
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final PushdownFilterContext context,
            final JobScheduler jobScheduler,
            final LongConsumer onComplete,
            final Consumer<Exception> onError) {
        // Delegate to the shared code for RowKeyAgnosticChunkSource
        RowKeyAgnosticChunkSource.estimatePushdownFilterCostHelper(
                filter, selection, usePrev, context, jobScheduler, onComplete, onError);
    }

    @Override
    public void pushdownFilter(
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final PushdownFilterContext context,
            final long costCeiling,
            final JobScheduler jobScheduler,
            final Consumer<PushdownResult> onComplete,
            final Consumer<Exception> onError) {
        // Delegate to the shared code for RowKeyAgnosticChunkSource
        RowKeyAgnosticChunkSource.pushdownFilterHelper(this, filter, selection, usePrev, context, costCeiling,
                jobScheduler, onComplete, onError);
    }
}
