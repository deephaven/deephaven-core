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
// endregion boxing imports

/**
 * Constant-value immutable {@link io.deephaven.engine.table.ColumnSource} of {@code Object}.
 */
public class ImmutableConstantObjectSource<T>
        extends AbstractColumnSource<T>
        implements ImmutableColumnSourceGetDefaults.ForObject<T>, InMemoryColumnSource, ShiftData.ShiftCallback {

    private final T value;

    // region constructor
    public ImmutableConstantObjectSource(@NotNull final Class<T> type, final Class<?> componentType, final T value) {
        super(type, componentType);
        this.value = value;
    }
    // endregion constructor

    @Override
    public final T get(final long rowKey) {
        if (rowKey == NULL_ROW_KEY) {
            return null;
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
        destination.asWritableObjectChunk().fillWithValue(0, size, value);
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
