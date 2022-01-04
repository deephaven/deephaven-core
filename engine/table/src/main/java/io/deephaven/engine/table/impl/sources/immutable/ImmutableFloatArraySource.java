/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ImmutableCharArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.immutable;

import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.util.type.TypeUtils;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

/**
 * Simple array source for Immutable Float.
 * <p>
 * The ImmutableC-harArraySource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class ImmutableFloatArraySource extends AbstractColumnSource<Float> implements ImmutableColumnSourceGetDefaults.ForFloat {
    private final float[] data;

    public ImmutableFloatArraySource(float[] source) {
        super(float.class);
        this.data = source;
    }

    @Override
    public float getFloat(long index) {
        if (index < 0 || index >= data.length) {
            return NULL_FLOAT;
        }

        return data[(int)index];
    }

    @Override
    public Float get(long index) {
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
