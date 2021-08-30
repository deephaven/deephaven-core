package io.deephaven.parquet;


import org.apache.parquet.column.Dictionary;

import java.io.IOException;
import java.nio.IntBuffer;

/**
 * Provides the API for reading a single parquet page
 */
public interface ColumnPageReader extends AutoCloseable {

    /**
     * @return The number of rows in this ColumnChunk, or -1 if it's unknown.
     */
    default long numRows() throws IOException {
        return numValues();
    }

    /**
     * Triggers the value decompression and decoding
     * 
     * @param nullValue The value to be stored under the null entries
     * @return the data for that page in a format that makes sense for the given type - typically
     *         array of something that makes sense
     */
    Object materialize(Object nullValue) throws IOException;

    /**
     * Directly read the key integral values when there's a dictionary.
     *
     * @param keyDest A properly sized buffer (at least numValues()) to hold the keys
     * @param nullPlaceholder The value to use for nulls.
     *
     * @return A buffer holding the end of each repeated row. If the column is not repeating, null.
     */
    IntBuffer readKeyValues(IntBuffer keyDest, int nullPlaceholder) throws IOException;

    /**
     * @return The value stored under number DataPageHeader.num_values
     */
    int numValues() throws IOException;

    Dictionary getDictionary();

}
