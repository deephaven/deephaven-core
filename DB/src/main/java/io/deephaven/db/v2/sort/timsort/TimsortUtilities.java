package io.deephaven.db.v2.sort.timsort;

public class TimsortUtilities {
    /**
     * The initial setting for the number of consecutive values taken from either run1 or run2 before we switch to
     * galloping mode.
     */
    public static final int INITIAL_GALLOP = 7;

    /**
     * Given a length of our input, we should pick a number that is between 32 and 64; ideally such that (length / run)
     * is just under 2^n < length, but only by a little bit.
     *
     * Take the six most significant bits of length; if the remaining bits have any ones, then add one.
     *
     * @param length the length of the values within a chunk to sort
     * @return the run length
     */
    public static int getRunLength(int length) {
        if (length <= 64) {
            return length;
        }

        final int clz = Integer.numberOfLeadingZeros(length);
        final int sixShift = (Integer.BYTES * 8) - (clz + 6);
        final int sixmsb = length >> sixShift;
        final int remainder = length & ~(sixmsb << sixShift);

        return sixmsb + (remainder == 0 ? 0 : 1);
    }
}
