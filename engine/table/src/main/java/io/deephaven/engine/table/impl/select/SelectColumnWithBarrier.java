package io.deephaven.engine.table.impl.select;


import java.util.Arrays;

public class SelectColumnWithBarrier extends WrappedSelectColumn {
    private final Object [] barriers;

    public SelectColumnWithBarrier(SelectColumn wrapped, Object ... barriers) {
        super(wrapped);
        if (wrapped.barriers() != null && wrapped.barriers().length > 0) {
            this.barriers = Arrays.copyOf(wrapped.barriers(), wrapped.barriers().length + barriers.length);
            System.arraycopy(barriers, 0, this.barriers, wrapped.barriers().length, barriers.length);
        } else {
            this.barriers = barriers;
        }
    }

    @Override
    public SelectColumn copy() {
        return new SelectColumnWithBarrier(inner.copy(), barriers);
    }

    @Override
    public Object[] barriers() {
        return barriers;
    }
}
