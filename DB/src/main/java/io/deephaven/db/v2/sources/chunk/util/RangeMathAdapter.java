package io.deephaven.db.v2.sources.chunk.util;

public class RangeMathAdapter {
    // has actual end field.
    // end field is already exclusive.
    // next range is 2 away.
    private static final RangeMathAdapter rangeVersion = new RangeMathAdapter(1, 0, 2);
    // end field overlaps with begin field
    // end field is inclusive (because it overlaps with begin field), so add 1 to make it exclusive
    // next range is 1 away
    private static final RangeMathAdapter valueVersion = new RangeMathAdapter(0, 1, 1);

    public static RangeMathAdapter get(boolean hasRanges) {
        return hasRanges ? rangeVersion : valueVersion;
    }

    public final int endOffset;
    public final int endAdjustment;
    public final int nextOffset;

    public RangeMathAdapter(int endOffset, int endAdjustment, int nextOffset) {
        this.endOffset = endOffset;
        this.endAdjustment = endAdjustment;
        this.nextOffset = nextOffset;
    }
}
