/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ImmutableCharArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.immutable;

import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.util.type.TypeUtils;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

/**
 * Simple array source for Immutable Double.
 * <p>
 * The ImmutableC-harArraySource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class ImmutableDoubleArraySource extends AbstractColumnSource<Double> implements ImmutableColumnSourceGetDefaults.ForDouble {
    private final double[] data;

    public ImmutableDoubleArraySource(double[] source) {
        super(double.class);
        this.data = source;
    }

    @Override
    public double getDouble(long index) {
        if (index < 0 || index >= data.length) {
            return NULL_DOUBLE;
        }

        return data[(int)index];
    }

    @Override
    public Double get(long index) {
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
