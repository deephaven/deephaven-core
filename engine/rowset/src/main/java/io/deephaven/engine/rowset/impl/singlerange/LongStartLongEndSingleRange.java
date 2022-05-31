package io.deephaven.engine.rowset.impl.singlerange;

public final class LongStartLongEndSingleRange extends SingleRange {
    private final long start;
    private final long end;

    public LongStartLongEndSingleRange(final long start, final long end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public long rangeStart() {
        return start;
    }

    @Override
    public long rangeEnd() {
        return end;
    }

    @Override
    public long getCardinality() {
        return end - start + 1;
    }

    @Override
    public LongStartLongEndSingleRange copy() {
        return new LongStartLongEndSingleRange(start, end);
    }
}
