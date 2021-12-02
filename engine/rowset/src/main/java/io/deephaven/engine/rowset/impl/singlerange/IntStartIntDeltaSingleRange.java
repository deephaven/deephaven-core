package io.deephaven.engine.rowset.impl.singlerange;

public final class IntStartIntDeltaSingleRange extends SingleRange {
    private final int unsignedIntStart;
    private final int unsignedIntDelta;

    public IntStartIntDeltaSingleRange(final int unsignedIntStart, final int unsignedIntDelta) {
        this.unsignedIntStart = unsignedIntStart;
        this.unsignedIntDelta = unsignedIntDelta;
    }

    @Override
    public long rangeStart() {
        return unsignedIntToLong(unsignedIntStart);
    }

    @Override
    public long rangeEnd() {
        return rangeStart() + unsignedIntToLong(unsignedIntDelta);
    }

    @Override
    public long getCardinality() {
        return unsignedIntToLong(unsignedIntDelta) + 1;
    }

    @Override
    public SingleRange copy() {
        return new IntStartIntDeltaSingleRange(unsignedIntStart, unsignedIntDelta);
    }
}
