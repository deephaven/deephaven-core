package io.deephaven.engine.rowset.impl.singlerange;

// On 64 bit VMs object header overhead is 12 bytes, and objects sizes are rounded to 8 bytes.
// With an int (4 bytes) and a long (8 bytes), we get to 24 bytes, which does not waste memory per object.
public final class IntStartLongDeltaSingleRange extends SingleRange {
    private final int unsignedIntStart;
    private final long delta;

    public IntStartLongDeltaSingleRange(final int unsignedIntStart, final long delta) {
        this.unsignedIntStart = unsignedIntStart;
        this.delta = delta;
    }

    @Override
    public long rangeStart() {
        return unsignedIntToLong(unsignedIntStart);
    }

    @Override
    public long rangeEnd() {
        return rangeStart() + delta;
    }

    @Override
    public long getCardinality() {
        return delta + 1;
    }

    @Override
    public IntStartLongDeltaSingleRange copy() {
        return new IntStartLongDeltaSingleRange(unsignedIntStart, delta);
    }
}
