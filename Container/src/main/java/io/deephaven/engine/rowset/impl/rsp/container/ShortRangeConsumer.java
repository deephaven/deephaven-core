package io.deephaven.engine.rowset.impl.rsp.container;

/**
 * A ShortRangeConsumer receives the ranges contained in a data structure. Each range is visited once, in increasing
 * unsigned order, with non-overlapped boundaries. In particular, the start position of a range needs to be strictly
 * greater than the end position of the previous range, both as unsigned values.
 * <p>
 * Usage:
 *
 * <pre>
 * {@code
 * bitmap.forEach(new ShortConsumer() {
 *   public boolean accept(short value) {
 *     // do something here
 *   }
 * });
 * }
 * </pre>
 */

public interface ShortRangeConsumer {
    /**
     * Provides a value to this consumer. A false return value indicates that the application providing values to this
     * consumer should not invoke it again.
     *
     * @param unsignedStart the unsigned short value for the start of this range.
     * @param unsignedEndInclusive the unsigned short value for the end of this range, inclusive.
     * @return false if don't want any more values after this one, true otherwise.
     */
    boolean accept(short unsignedStart, short unsignedEndInclusive);
}
