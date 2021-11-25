package io.deephaven.engine.rowset.impl.singlerange;

// On 64 bit VMs object header overhead is 12 bytes, and objects sizes are rounded to 8 bytes.
// With an int (4 bytes) and a long (8 bytes), we get to 24 bytes, which does not waste memory per object.
public final class LongStartIntDeltaSingleRange extends SingleRange {
    private final long rangeStart;
    private final int unsignedIntDelta;

    public LongStartIntDeltaSingleRange(final long rangeStart, final int unsignedIntDelta) {
        this.rangeStart = rangeStart;
        this.unsignedIntDelta = unsignedIntDelta;
    }

    @Override
    public long rangeStart() {
        return rangeStart;
    }

    @Override
    public long rangeEnd() {
        return rangeStart + delta();
    }

    @Override
    public long getCardinality() {
        return delta() + 1;
    }

    @Override
    public LongStartIntDeltaSingleRange copy() {
        return new LongStartIntDeltaSingleRange(rangeStart, unsignedIntDelta);
    }

    private long delta() {
        return unsignedIntToLong(unsignedIntDelta);
    }
}
