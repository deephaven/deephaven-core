package io.deephaven.parquet;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.util.Iterator;

public interface ColumnChunkReader {
    /**
     * @return -1 if the current column doesn't guarantee fixed page size, otherwise the fixed page size
     */
    int getPageFixedSize();

    /**
     * @return The number of rows in this ColumnChunk, or -1 if it's unknown.
     */
    long numRows();

    /**
     * @return The value stored under the corresponding ColumnMetaData.num_values field
     */
    long numValues();

    /**
     * @return The depth of the number of nested repeated fields this column is a part of.
     * 0 means this is a simple (non-repeating) field,  1 means this is a flat array.
     */
    int getMaxRl();

    interface ColumnPageReaderIterator extends Iterator<ColumnPageReader>, AutoCloseable {
        @Override
        void close() throws IOException;
    }

    /**
     * @return An iterator over individual parquet pages
     */
    ColumnPageReaderIterator getPageIterator() throws IOException;

    /**
     * @return Whether this column chunk uses a dictionary-based encoding on every page
     */
    boolean usesDictionaryOnEveryPage();

    /**
     * @return Reference to the parquet dictionary for that page if available
     * @throws IOException if problem encountered with underlying storage
     */
    Dictionary getDictionary() throws IOException;

    PrimitiveType getType();
}
