/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ImmutableCharArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.immutable;

import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.util.type.TypeUtils;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

/**
 * Simple array source for Immutable Byte.
 * <p>
 * The ImmutableC-harArraySource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class ImmutableByteArraySource extends AbstractColumnSource<Byte> implements ImmutableColumnSourceGetDefaults.ForByte {
    private final byte[] data;

    public ImmutableByteArraySource(byte[] source) {
        super(byte.class);
        this.data = source;
    }

    @Override
    public byte getByte(long index) {
        if (index < 0 || index >= data.length) {
            return NULL_BYTE;
        }

        return data[(int)index];
    }

    @Override
    public Byte get(long index) {
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
