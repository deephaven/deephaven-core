package io.deephaven.csv.sinks;

/**
 * The system uses this interface to write to caller's column data structures. The reason this interface exists is so
 * that the caller can use whatever column data structure they want to for final storage of the data.
 * 
 * @param <TARRAY> The array data type (e.g. short[], double[], etc.) holding a chunk of data to be written to the
 *        target data structure. The caller specifies what Sinks to use via the {@link SinkFactory} class.
 */
public interface Sink<TARRAY> {
    /**
     * Write a chunk of data to the target data structure. Sample implementation: <code><pre>
     *     if (appending) {
     *         while (mycolumn.size() < destBegin) {
     *             myColumn.addNull();
     *         }
     *         int srcIndex = 0;
     *         for (long destIndex = destBegin; destIndex != destEnd; ++destIndex) {
     *             if (isNull[srcIndex]) {
     *                 myColumn.addNull();
     *             } else {
     *                 myColumn.add(src[srcIndex]);
     *             }
     *             ++srcIndex;
     *         }
     *     } else {
     *         // replacing
     *         int srcIndex = 0;
     *         for (long destIndex = destBegin; destIndex != destEnd; ++destIndex) {
     *             if (isNull[srcIndex]) {
     *                 myColumn[destIndex] = myNullRepresentation;
     *             } else {
     *                 myColumn[destIndex] = src[srcIndex];
     *             }
     *             ++srcIndex;
     *         }
     *
     *     }
     *     </pre></code>
     *
     * @param src The chunk of data, a typed array (short[], double[], etc) with valid elements in the half-open
     *        interval {@code [0..(destEnd - destBegin))}.
     * @param isNull A boolean array, with the same range of valid elements. A "true" value at position {@code i} means
     *        that {@code src[i]} should be ignored and the element should be considered as the "null value", whose
     *        representation depends on the target data structure. A "false" value means that {@code src[i]} should be
     *        interpreted normally.
     * @param destBegin The inclusive start index of the destination range.
     * @param destEnd The exclusive end index of the destination range.
     * @param appending A hint to the destination which indicates whether the system is appending to the data structure
     *        (if appending is true), or overwriting previously-written values (if appending is false). The caller
     *        promises to never span these two cases: i.e. it will never pass a chunk of data which partially overwrites
     *        values and then partially appends values. This flag is convenient but technically redundant because code
     *        can also determine what case it's in by comparing {@code destEnd} to the data structure's current size.
     */
    void write(final TARRAY src, final boolean[] isNull, final long destBegin, final long destEnd, boolean appending);
}
