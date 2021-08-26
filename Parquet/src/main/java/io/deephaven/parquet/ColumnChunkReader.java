package io.deephaven.parquet;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Supplier;

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
     * @return The depth of the number of nested repeated fields this column is a part of. 0 means this is a simple
     *         (non-repeating) field, 1 means this is a flat array.
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
     * @return Supplier for a Parquet dictionary for this column chunk
     * @apiNote The result will never return {@code null}. It will instead supply {@link #NULL_DICTIONARY}.
     */
    Supplier<Dictionary> getDictionarySupplier();

    Dictionary NULL_DICTIONARY = new NullDictionary();

    final class NullDictionary extends Dictionary {

        private NullDictionary() {
            super(null);
        }

        @Override
        public int getMaxId() {
            // Note that this will cause the "size" of the dictionary to be 0.
            return -1;
        }
    }

    PrimitiveType getType();
}
