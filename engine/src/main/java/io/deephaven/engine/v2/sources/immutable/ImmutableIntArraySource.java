/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit ImmutableCharArraySource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sources.immutable;

import io.deephaven.engine.v2.sources.AbstractColumnSource;
import io.deephaven.engine.v2.sources.ImmutableColumnSourceGetDefaults;
import io.deephaven.util.type.TypeUtils;

import static io.deephaven.util.QueryConstants.NULL_INT;

/**
 * Simple array source for Immutable Int.
 * <p>
 * The ImmutableC-harArraySource is replicated to all other types with
 * io.deephaven.engine.v2.sources.Replicate.
 *
 * (C-har is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class ImmutableIntArraySource extends AbstractColumnSource<Integer> implements ImmutableColumnSourceGetDefaults.ForInt {
    private final int[] data;

    public ImmutableIntArraySource(int[] source) {
        super(int.class);
        this.data = source;
    }

    @Override
    public int getInt(long index) {
        if (index < 0 || index >= data.length) {
            return NULL_INT;
        }

        return data[(int)index];
    }

    @Override
    public Integer get(long index) {
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
