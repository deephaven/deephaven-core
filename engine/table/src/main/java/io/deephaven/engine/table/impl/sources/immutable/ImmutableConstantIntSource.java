/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ImmutableConstantCharSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.immutable;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.util.ShiftData;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

// region boxing imports
import static io.deephaven.util.QueryConstants.NULL_INT;
// endregion boxing imports

/**
 * Constant-value immutable {@link io.deephaven.engine.table.ColumnSource} of {@code int}.
 */
public class ImmutableConstantIntSource
        extends AbstractColumnSource<Integer>
        implements ImmutableColumnSourceGetDefaults.ForInt, ShiftData.ShiftCallback, InMemoryColumnSource,
        RowKeyAgnosticChunkSource<Values> {

    private final int value;

    // region constructor
    public ImmutableConstantIntSource(final int value) {
        super(Integer.class);
        this.value = value;
    }
    // endregion constructor

    @Override
    public final int getInt(final long rowKey) {
        if (rowKey == NULL_ROW_KEY) {
            return NULL_INT;
        }
        return value;
    }

    @Override
    public final void fillChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        final int size = rowSequence.intSize();
        destination.setSize(size);
        destination.asWritableIntChunk().fillWithValue(0, size, value);
    }

    @Override
    public final void fillPrevChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        fillChunk(context, destination, rowSequence);
    }

    @Override
    public final void shift(final long start, final long end, final long offset) {}

    // region reinterpret
    // endregion reinterpret

    @Override
    public void fillChunkUnordered(
            @NotNull FillContext context,
            @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        final WritableIntChunk<? super Values> destChunk = dest.asWritableIntChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            destChunk.set(ii, keys.get(ii) == RowSequence.NULL_ROW_KEY ? NULL_INT : value);
        }
        destChunk.setSize(keys.size());
    }

    @Override
    public void fillPrevChunkUnordered(
            @NotNull FillContext context,
            @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        fillChunkUnordered(context , dest, keys);
    }

    @Override
    public boolean providesFillUnordered() {
        return true;
    }
}
