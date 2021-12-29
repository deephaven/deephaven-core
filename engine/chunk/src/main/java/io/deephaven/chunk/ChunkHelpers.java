package io.deephaven.chunk;

/**
 * Implementation helpers for {@link Chunk chunks}.
 */
public class ChunkHelpers {

    static void checkSliceArgs(int size, int offset, int capacity) {
        if (offset < 0 || offset > size || capacity < 0 || capacity > size - offset) {
            throw new IllegalArgumentException(
                    String.format("New slice offset %d, capacity %d is incompatible with size %d",
                            offset, capacity, size));
        }
    }

    static void checkArrayArgs(int arrayLength, int offset, int capacity) {
        if (offset < 0 || capacity < 0 || offset + capacity > arrayLength) {
            throw new IllegalArgumentException(
                    String.format("offset %d, capacity %d is incompatible with array of length %d",
                            offset, capacity, arrayLength));
        }
    }

    /**
     * Determines, when copying data from the source array to the dest array, whether the copying should proceed in the
     * forward or reverse direction in order to be safe. The issue is that (like memmove), care needs to be taken when
     * the arrays are the same and the ranges overlap. In some cases (like when the arrays are different), either
     * direction will do; in those cases we will recommend copying in the forward direction for performance reasons.
     * When srcArray and destArray refer to the array, one of these five cases applies:
     *
     * <p>
     *
     * <pre>
     * Case 1: Source starts to the left of dest, and does not overlap. Recommend copying in forward direction.
     * SSSSS
     *      DDDDD
     *
     * Case 2: Source starts to the left of dest, and partially overlaps. MUST copy in the REVERSE direction.
     * SSSSS
     *    DDDDD
     *
     * Case 3: Source is on top of dest. Recommend copying in forward direction (really, you should do nothing).
     * SSSSS
     * DDDDD
     *
     * Case 4: Source starts to the right of dest, and partially overlaps. MUST copy in the FORWARD direction.
     *   SSSSS
     * DDDDD
     *
     * Case 5: Source starts to the right of dest, and does not overlap. Recommend copying in the forward direction.
     *      SSSSS
     * DDDDD
     * </pre>
     *
     * Note that case 2 is the only case where you need to copy backwards.
     *
     * @param srcArray The source array
     * @param srcOffset The starting offset in the srcArray
     * @param destArray The destination array
     * @param destOffset The starting offset in the destination array
     * @param length The number of elements that will be copied
     * @return true if the copy should proceed in the forward direction; false if it should proceed in the reverse
     *         direction
     */
    static <TARRAY> boolean canCopyForward(TARRAY srcArray, int srcOffset, TARRAY destArray,
            int destOffset, int length) {
        return srcArray != destArray || // arrays different
                srcOffset + length <= destOffset || // case 1
                srcOffset >= destOffset; // cases 3, 4, 5
    }
}
