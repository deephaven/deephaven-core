package io.deephaven.engine.rowset.impl.singlerange;

public final class SingleIntSingleRange extends SingleRange {
    private final int unsignedIntValue;

    public SingleIntSingleRange(final int unsignedIntValue) {
        this.unsignedIntValue = unsignedIntValue;
    }

    @Override
    public long rangeStart() {
        return unsignedIntToLong(unsignedIntValue);
    }

    @Override
    public long rangeEnd() {
        return unsignedIntToLong(unsignedIntValue);
    }

    @Override
    public long getCardinality() {
        return 1;
    }

    @Override
    public SingleRange copy() {
        return new SingleIntSingleRange(unsignedIntValue);
    }
}
