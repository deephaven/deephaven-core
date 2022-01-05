/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ImmutableCharArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.immutable;

import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.util.type.TypeUtils;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * Simple array source for Immutable Long.
 * <p>
 * The ImmutableC-harArraySource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class ImmutableLongArraySource extends AbstractColumnSource<Long> implements ImmutableColumnSourceGetDefaults.ForLong {
    private final long[] data;

    public ImmutableLongArraySource(long[] source) {
        super(long.class);
        this.data = source;
    }

    @Override
    public long getLong(long index) {
        if (index < 0 || index >= data.length) {
            return NULL_LONG;
        }

        return data[(int)index];
    }

    @Override
    public Long get(long index) {
        if (index < 0 || index >= data.length) {
            return null;
        }

        return TypeUtils.box(data[(int)index]);
    }

    @Override
    public boolean isImmutable() {
        return true;
    }
}
