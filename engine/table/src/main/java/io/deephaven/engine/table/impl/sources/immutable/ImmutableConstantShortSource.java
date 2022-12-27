/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ImmutableConstantCharSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.immutable;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.util.ShiftData;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

// region boxing imports
import static io.deephaven.util.QueryConstants.NULL_SHORT;
// endregion boxing imports

/**
 * Constant-value immutable {@link io.deephaven.engine.table.ColumnSource} of {@code short}.
 */
public class ImmutableConstantShortSource
        extends AbstractColumnSource<Short>
        implements ImmutableColumnSourceGetDefaults.ForShort, InMemoryColumnSource, ShiftData.ShiftCallback {

    private final short value;

    // region constructor
    public ImmutableConstantShortSource(final short value) {
        super(Short.class);
        this.value = value;
    }
    // endregion constructor

    @Override
    public final short getShort(final long rowKey) {
        if (rowKey == NULL_ROW_KEY) {
            return NULL_SHORT;
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
        destination.asWritableShortChunk().fillWithValue(0, size, value);
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
}
