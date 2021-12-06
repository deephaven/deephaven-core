package io.deephaven.engine.rowset.impl.singlerange;

// On 64 bit VMs object header overhead is 12 bytes, and objects sizes are rounded to 8 bytes.
// With two shorts, we get to 16 bytes, which does not waste memory per object.
public final class ShortStartShortDeltaSingleRange extends SingleRange {
    private final short unsignedShortStart;
    private final short unsignedShortDelta;

    public ShortStartShortDeltaSingleRange(final short unsignedShortStart, final short unsignedShortDelta) {
        this.unsignedShortStart = unsignedShortStart;
        this.unsignedShortDelta = unsignedShortDelta;
    }

    @Override
    public long rangeStart() {
        return unsignedShortToLong(unsignedShortStart);
    }

    @Override
    public long rangeEnd() {
        return rangeStart() + delta();
    }

    @Override
    public long getCardinality() {
        return delta() + 1;
    }

    @Override
    public ShortStartShortDeltaSingleRange copy() {
        return new ShortStartShortDeltaSingleRange(unsignedShortStart, unsignedShortDelta);
    }

    private long delta() {
        return unsignedShortToLong(unsignedShortDelta);
    }
}
