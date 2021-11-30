package io.deephaven.engine.rowset.impl.singlerange;

public final class SingleLongSingleRange extends SingleRange {
    private final long value;

    public SingleLongSingleRange(final long value) {
        this.value = value;
    }

    @Override
    public long rangeStart() {
        return value;
    }

    @Override
    public long rangeEnd() {
        return value;
    }

    @Override
    public long getCardinality() {
        return 1;
    }

    @Override
    public SingleRange copy() {
        return new SingleLongSingleRange(value);
    }
}
