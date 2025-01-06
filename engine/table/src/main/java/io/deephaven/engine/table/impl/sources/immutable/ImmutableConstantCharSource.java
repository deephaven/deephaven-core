//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.immutable;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.*;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

// region boxing imports
import static io.deephaven.util.QueryConstants.NULL_CHAR;
// endregion boxing imports

/**
 * Constant-value immutable {@link io.deephaven.engine.table.ColumnSource} of {@code char}.
 */
public class ImmutableConstantCharSource
        extends AbstractColumnSource<Character>
        implements ImmutableColumnSourceGetDefaults.ForChar, InMemoryColumnSource,
        RowKeyAgnosticChunkSource<Values> /* MIXIN_IMPLS */ {

    private final char value;

    // region constructor
    public ImmutableConstantCharSource(final char value) {
        super(Character.class);
        this.value = value;
    }
    // endregion constructor

    @Override
    public final char getChar(final long rowKey) {
        if (rowKey == NULL_ROW_KEY) {
            return NULL_CHAR;
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
        destination.asWritableCharChunk().fillWithValue(0, size, value);
    }

    @Override
    public final void fillPrevChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        fillChunk(context, destination, rowSequence);
    }

    @Override
    public void fillChunkUnordered(
            @NotNull FillContext context,
            @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        final WritableCharChunk<? super Values> destChunk = dest.asWritableCharChunk();
        for (int ii = 0; ii < keys.size(); ++ii) {
            destChunk.set(ii, keys.get(ii) == RowSequence.NULL_ROW_KEY ? NULL_CHAR : value);
        }
        destChunk.setSize(keys.size());
    }

    @Override
    public void fillPrevChunkUnordered(
            @NotNull FillContext context,
            @NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        fillChunkUnordered(context, dest, keys);
    }

    @Override
    public boolean providesFillUnordered() {
        return true;
    }

    // region reinterpretation
    // endregion reinterpretation
}
