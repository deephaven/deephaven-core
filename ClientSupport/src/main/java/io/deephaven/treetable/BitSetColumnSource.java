package io.deephaven.treetable;

import io.deephaven.db.v2.sources.AbstractColumnSource;
import io.deephaven.db.v2.sources.ImmutableColumnSourceGetDefaults;

import java.util.BitSet;

/**
 * A {@link io.deephaven.db.v2.sources.ColumnSource} wrapping a bitset. It does not support previous values and is
 * immutable.
 *
 * @implNote This is intended only for use with {@link TreeSnapshotQuery}.
 */
public class BitSetColumnSource extends AbstractColumnSource<Boolean>
        implements ImmutableColumnSourceGetDefaults.ForBoolean {
    private final BitSet theSet;

    public BitSetColumnSource(BitSet theSet) {
        super(boolean.class);
        this.theSet = theSet;
    }

    @Override
    public Boolean get(long index) {
        return theSet.get((int) index);
    }

    @Override
    public boolean isImmutable() {
        return true;
    }
}
