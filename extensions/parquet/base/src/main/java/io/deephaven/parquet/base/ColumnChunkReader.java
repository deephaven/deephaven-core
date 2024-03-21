//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.schema.PrimitiveType;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.function.Function;

public interface ColumnChunkReader {
    /**
     * @return The number of rows in this ColumnChunk, or -1 if it's unknown.
     */
    long numRows();

    /**
     * @return The value stored under the corresponding ColumnMetaData.num_values field.
     */
    long numValues();

    /**
     * @return The depth of the number of nested repeated fields this column is a part of. 0 means this is a simple
     *         (non-repeating) field, 1 means this is a flat array.
     */
    int getMaxRl();

    /**
     * @return The offset index for this column chunk, or null if it not found in the metadata.
     */
    @Nullable
    OffsetIndex getOffsetIndex();

    /**
     * Used to iterate over column page readers for each page with the capability to set channel context to for reading
     * the pages.
     */
    interface ColumnPageReaderIterator {
        /**
         * @return Whether there are more pages to iterate over.
         */
        boolean hasNext();

        /**
         * @param channelContext The channel context to use for constructing the reader
         * @return The next page reader.
         */
        ColumnPageReader next(SeekableChannelContext channelContext);
    }

    /**
     * @return An iterator over individual parquet pages.
     */
    ColumnPageReaderIterator getPageIterator() throws IOException;

    interface ColumnPageDirectAccessor {
        /**
         * Directly access a page reader for a given page number.
         * 
         * @param pageNum The page number to access.
         * @param channelContext The channel context to use for constructing the reader
         */
        ColumnPageReader getPageReader(int pageNum, SeekableChannelContext channelContext);
    }

    /**
     * @return An accessor for individual parquet pages.
     */
    ColumnPageDirectAccessor getPageAccessor();

    /**
     * @return Whether this column chunk uses a dictionary-based encoding on every page.
     */
    boolean usesDictionaryOnEveryPage();

    /**
     * @return Supplier for a Parquet dictionary for this column chunk
     * @apiNote The result will never return {@code null}. It will instead supply {@link #NULL_DICTIONARY}.
     */
    Function<SeekableChannelContext, Dictionary> getDictionarySupplier();

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

    /**
     * @return The "version" string from deephaven specific parquet metadata, or null if it's not present.
     */
    @Nullable
    String getVersion();

    /**
     * @return The channel provider for this column chunk reader.
     */
    SeekableChannelsProvider getChannelsProvider();
}
