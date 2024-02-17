/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import io.deephaven.parquet.compress.CompressorAdapter;
import io.deephaven.parquet.compress.DeephavenCompressorAdapterFactory;
import io.deephaven.util.datastructures.LazyCachingSupplier;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.format.*;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import static org.apache.parquet.format.Encoding.PLAIN_DICTIONARY;
import static org.apache.parquet.format.Encoding.RLE_DICTIONARY;

public class ColumnChunkReaderImpl implements ColumnChunkReader {

    private final ColumnChunk columnChunk;
    private final SeekableChannelsProvider channelsProvider;
    private final Path rootPath;
    private final CompressorAdapter decompressor;
    private final ColumnDescriptor path;
    private final OffsetIndex offsetIndex;
    private final List<Type> fieldTypes;
    private final Supplier<Dictionary> dictionarySupplier;
    private final PageMaterializer.Factory nullMaterializerFactory;

    private Path filePath;
    /**
     * Number of rows in the row group of this column chunk.
     */
    private final long numRows;
    /**
     * Version string from deephaven specific parquet metadata, or null if it's not present.
     */
    private final String version;

    ColumnChunkReaderImpl(ColumnChunk columnChunk, SeekableChannelsProvider channelsProvider, Path rootPath,
            MessageType type, OffsetIndex offsetIndex, List<Type> fieldTypes, final long numRows,
            final String version) {
        this.channelsProvider = channelsProvider;
        this.columnChunk = columnChunk;
        this.rootPath = rootPath;
        this.path = type
                .getColumnDescription(columnChunk.meta_data.getPath_in_schema().toArray(new String[0]));
        if (columnChunk.getMeta_data().isSetCodec()) {
            decompressor = DeephavenCompressorAdapterFactory.getInstance()
                    .getByName(columnChunk.getMeta_data().getCodec().name());
        } else {
            decompressor = CompressorAdapter.PASSTHRU;
        }
        this.offsetIndex = offsetIndex;
        this.fieldTypes = fieldTypes;
        this.dictionarySupplier = new LazyCachingSupplier<>(this::getDictionary);
        this.nullMaterializerFactory = PageMaterializer.factoryForType(path.getPrimitiveType().getPrimitiveTypeName());
        this.numRows = numRows;
        this.version = version;
    }

    @Override
    public long numRows() {
        return numRows;
    }

    @Override
    public long numValues() {
        return columnChunk.getMeta_data().num_values;
    }

    @Override
    public int getMaxRl() {
        return path.getMaxRepetitionLevel();
    }

    public final OffsetIndex getOffsetIndex() {
        return offsetIndex;
    }

    @Override
    public Iterator<ColumnPageReader> getPageIterator() {
        final long dataPageOffset = columnChunk.meta_data.getData_page_offset();
        if (offsetIndex == null) {
            return new ColumnPageReaderIteratorImpl(dataPageOffset, columnChunk.getMeta_data().getNum_values());
        } else {
            return new ColumnPageReaderIteratorIndexImpl();
        }
    }

    @Override
    public final ColumnPageDirectAccessor getPageAccessor() {
        if (offsetIndex == null) {
            throw new UnsupportedOperationException("Cannot use direct accessor without offset index");
        }
        return new ColumnPageDirectAccessorImpl();
    }

    private Path getFilePath() {
        if (filePath != null) {
            return filePath;
        }
        if (columnChunk.isSetFile_path()) {
            return filePath = rootPath.resolve(columnChunk.getFile_path());
        } else {
            return filePath = rootPath;
        }
    }

    @Override
    public boolean usesDictionaryOnEveryPage() {
        final ColumnMetaData columnMeta = columnChunk.getMeta_data();
        if (columnMeta.encoding_stats == null) {
            // We don't know, so we bail out to "false"
            return false;
        }
        for (final PageEncodingStats encodingStat : columnMeta.encoding_stats) {
            if (encodingStat.page_type != PageType.DATA_PAGE
                    && encodingStat.page_type != PageType.DATA_PAGE_V2) {
                // Not a data page, skip
                continue;
            }
            // This is a data page
            if (encodingStat.encoding != PLAIN_DICTIONARY
                    && encodingStat.encoding != RLE_DICTIONARY) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Supplier<Dictionary> getDictionarySupplier() {
        return dictionarySupplier;
    }

    @NotNull
    private Dictionary getDictionary() {
        final long dictionaryPageOffset;
        final ColumnMetaData chunkMeta = columnChunk.getMeta_data();
        if (chunkMeta.isSetDictionary_page_offset()) {
            dictionaryPageOffset = chunkMeta.getDictionary_page_offset();
        } else if ((chunkMeta.isSetEncoding_stats() && (chunkMeta.getEncoding_stats().stream()
                .anyMatch(pes -> pes.getEncoding() == PLAIN_DICTIONARY
                        || pes.getEncoding() == RLE_DICTIONARY)))
                || (chunkMeta.isSetEncodings() && (chunkMeta.getEncodings().stream()
                        .anyMatch(en -> en == PLAIN_DICTIONARY || en == RLE_DICTIONARY)))) {
            // Fallback, inspired by
            // https://stackoverflow.com/questions/55225108/why-is-dictionary-page-offset-0-for-plain-dictionary-encoding
            dictionaryPageOffset = chunkMeta.getData_page_offset();
        } else {
            return NULL_DICTIONARY;
        }
        try (final SeekableByteChannel readChannel = channelsProvider.getReadChannel(getFilePath())) {
            readChannel.position(dictionaryPageOffset);
            return readDictionary(readChannel);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public PrimitiveType getType() {
        return path.getPrimitiveType();
    }

    @Override
    public String getVersion() {
        return version;
    }

    @NotNull
    private Dictionary readDictionary(ReadableByteChannel file) throws IOException {
        // explicitly not closing this, caller is responsible
        final InputStream inputStream = Channels.newInputStream(file);
        final PageHeader pageHeader = Util.readPageHeader(inputStream);
        if (pageHeader.getType() != PageType.DICTIONARY_PAGE) {
            // In case our fallback in getDictionary was too optimistic...
            return NULL_DICTIONARY;
        }
        final DictionaryPageHeader dictHeader = pageHeader.getDictionary_page_header();

        final BytesInput payload;
        int compressedPageSize = pageHeader.getCompressed_page_size();
        if (compressedPageSize == 0) {
            // Sometimes the size is explicitly empty, just use an empty payload
            payload = BytesInput.empty();
        } else {
            payload = decompressor.decompress(inputStream, compressedPageSize, pageHeader.getUncompressed_page_size());
        }

        final DictionaryPage dictionaryPage = new DictionaryPage(payload, dictHeader.getNum_values(),
                Encoding.valueOf(dictHeader.getEncoding().name()));

        return dictionaryPage.getEncoding().initDictionary(path, dictionaryPage);
    }

    private final class ColumnPageReaderIteratorImpl implements Iterator<ColumnPageReader> {
        private long currentOffset;
        private long remainingValues;

        ColumnPageReaderIteratorImpl(final long startOffset, final long numValues) {
            this.remainingValues = numValues;
            this.currentOffset = startOffset;
        }

        @Override
        public boolean hasNext() {
            return remainingValues > 0;
        }

        @Override
        public ColumnPageReader next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No next element");
            }
            // NB: The channels provider typically caches channels; this avoids maintaining a handle per column chunk
            try (final SeekableByteChannel readChannel = channelsProvider.getReadChannel(getFilePath())) {
                final long headerOffset = currentOffset;
                readChannel.position(currentOffset);
                // deliberately not closing this stream
                final PageHeader pageHeader = Util.readPageHeader(Channels.newInputStream(readChannel));
                currentOffset = readChannel.position() + pageHeader.getCompressed_page_size();
                if (pageHeader.isSetDictionary_page_header()) {
                    // Dictionary page; skip it
                    return next();
                }
                if (!pageHeader.isSetData_page_header() && !pageHeader.isSetData_page_header_v2()) {
                    throw new IllegalStateException(
                            "Expected data page, but neither v1 nor v2 data page header is set in file "
                                    + readChannel + " at offset " + headerOffset);
                }
                remainingValues -= pageHeader.isSetData_page_header()
                        ? pageHeader.getData_page_header().getNum_values()
                        : pageHeader.getData_page_header_v2().getNum_values();
                final org.apache.parquet.format.Encoding encoding;
                switch (pageHeader.type) {
                    case DATA_PAGE:
                        encoding = pageHeader.getData_page_header().getEncoding();
                        break;
                    case DATA_PAGE_V2:
                        encoding = pageHeader.getData_page_header_v2().getEncoding();
                        break;
                    default:
                        throw new UncheckedDeephavenException(
                                "Unknown parquet data page header type " + pageHeader.type);
                }
                final Supplier<Dictionary> pageDictionarySupplier =
                        (encoding == PLAIN_DICTIONARY || encoding == RLE_DICTIONARY)
                                ? dictionarySupplier
                                : () -> NULL_DICTIONARY;
                return new ColumnPageReaderImpl(channelsProvider, decompressor, pageDictionarySupplier,
                        nullMaterializerFactory, path, getFilePath(), fieldTypes, readChannel.position(), pageHeader,
                        ColumnPageReaderImpl.NULL_NUM_VALUES);
            } catch (IOException e) {
                throw new UncheckedDeephavenException("Error reading page header", e);
            }
        }
    }

    private final class ColumnPageReaderIteratorIndexImpl implements Iterator<ColumnPageReader> {
        private int pos;

        ColumnPageReaderIteratorIndexImpl() {
            pos = 0;
        }

        @Override
        public boolean hasNext() {
            return offsetIndex.getPageCount() > pos;
        }

        @Override
        public ColumnPageReader next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No next element");
            }
            // Following logic assumes that offsetIndex will store the number of values for a page instead of number
            // of rows (which can be different for array and vector columns). This behavior is because of a bug on
            // parquet writing side which got fixed in deephaven-core/pull/4844 and is only kept to support reading
            // parquet files written before deephaven-core/pull/4844.
            final int numValues = (int) (offsetIndex.getLastRowIndex(pos, columnChunk.getMeta_data().getNum_values())
                    - offsetIndex.getFirstRowIndex(pos) + 1);
            ColumnPageReaderImpl columnPageReader =
                    new ColumnPageReaderImpl(channelsProvider, decompressor, dictionarySupplier,
                            nullMaterializerFactory, path, getFilePath(), fieldTypes, offsetIndex.getOffset(pos), null,
                            numValues);
            pos++;
            return columnPageReader;
        }
    }

    private final class ColumnPageDirectAccessorImpl implements ColumnPageDirectAccessor {

        ColumnPageDirectAccessorImpl() {}

        @Override
        public ColumnPageReader getPageReader(final int pageNum) {
            if (pageNum < 0 || pageNum >= offsetIndex.getPageCount()) {
                throw new IndexOutOfBoundsException(
                        "pageNum=" + pageNum + ", offsetIndex.getPageCount()=" + offsetIndex.getPageCount());
            }
            // Page header and number of values will be populated later when we read the page header from the file
            return new ColumnPageReaderImpl(channelsProvider, decompressor, dictionarySupplier, nullMaterializerFactory,
                    path, getFilePath(), fieldTypes, offsetIndex.getOffset(pageNum), null,
                    ColumnPageReaderImpl.NULL_NUM_VALUES);
        }
    }
}
