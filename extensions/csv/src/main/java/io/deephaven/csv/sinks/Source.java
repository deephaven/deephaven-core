package io.deephaven.csv.sinks;

/**
 * The system uses this interface to read from the caller's column data structures. The system only needs to do so in a
 * limited number of cases, namely TARRAY = byte[], short[], int[], and long[]. This interface is used when the type
 * inference process guesses wrong and needs a fast path to read the data back from a narrower data structure and write
 * it to a wider one.
 * 
 * @param <TARRAY> The array data type (e.g. short[], int[], etc.) holding a chunk of data to be copied from the target
 *        data structure.
 */
public interface Source<TARRAY> {
    /**
     * Read a chunk of data from the src data structure. Sample implementation: <code><pre>
     *     int destIndex = 0;
     *     for (long srcIndex = srcBegin; srcIndex != srcEnd; ++srcIndex) {
     *         if (myColumn.hasNullAt(srcIndex)) {
     *             isNull[destIndex] = true;
     *         } else {
     *             dest[destIndex] = myColumn.getValueAt(srcIndex);
     *             isNull[destIndex] = false;
     *         }
     *         ++destIndex;
     *     }
     *     </pre></code>
     *
     * @param dest The chunk of data used to store values copied from the caller's column data structure.
     * @param isNull A boolean array, with the same range of valid elements. A "true" value at position {@code i} means
     *        the the corresponding element refers to the "null value" of the source data structure. A "false" value
     *        means that {@code dest[i]} should be interpreted normally.
     * @param srcBegin The inclusive start index of the source range.
     * @param srcEnd The exclusive end index of the source range.
     */
    void read(final TARRAY dest, final boolean[] isNull, final long srcBegin, final long srcEnd);
}
