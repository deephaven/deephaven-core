package io.deephaven.engine.table.impl.sources.immutable;

import io.deephaven.util.BooleanUtils;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;

public class ImmutableBooleanArraySource extends AbstractColumnSource<Boolean> implements ImmutableColumnSourceGetDefaults.ForBoolean {
    private final byte[] data;

    public ImmutableBooleanArraySource(boolean[] source) {
        super(boolean.class);
        this.data = new byte[source.length];
        for(int i = 0; i<source.length; i++) {
            this.data[i] = BooleanUtils.booleanAsByte(source[i]);
        }
    }

    public ImmutableBooleanArraySource(Boolean[] source) {
        super(boolean.class);
        this.data = new byte[source.length];
        for(int i = 0; i<source.length; i++) {
            this.data[i] = BooleanUtils.booleanAsByte(source[i]);
        }
    }

    public ImmutableBooleanArraySource(byte[] source) {
        super(boolean.class);
        this.data = source;
    }

    @Override
    public Boolean get(long index) {
        if (index < 0 || index >= data.length) {
            return null;
        }

        return BooleanUtils.byteAsBoolean(data[(int)index]);
    }

    @Override
    public boolean isImmutable() {
        return true;
    }
}
