/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ImmutableCharArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.immutable;

import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.util.type.TypeUtils;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

/**
 * Simple array source for Immutable Short.
 * <p>
 * The ImmutableC-harArraySource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class ImmutableShortArraySource extends AbstractColumnSource<Short> implements ImmutableColumnSourceGetDefaults.ForShort {
    private final short[] data;

    public ImmutableShortArraySource(short[] source) {
        super(short.class);
        this.data = source;
    }

    @Override
    public short getShort(long index) {
        if (index < 0 || index >= data.length) {
            return NULL_SHORT;
        }

        return data[(int)index];
    }

    @Override
    public Short get(long index) {
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
