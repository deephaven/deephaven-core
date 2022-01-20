package io.deephaven.parquet.base;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.parquet.base.util.Helpers;
import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import io.deephaven.util.datastructures.LazyCachingSupplier;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.format.*;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.parquet.format.Encoding.PLAIN_DICTIONARY;
import static org.apache.parquet.format.Encoding.RLE_DICTIONARY;

public class ColumnChunkReaderImpl implements ColumnChunkReader {

    private final ColumnChunk columnChunk;
    private final SeekableChannelsProvider channelsProvider;
    private final Path rootPath;
    private final ThreadLocal<CompressionCodecFactory.BytesInputDecompressor> decompressor;
    private final ColumnDescriptor path;
    private final OffsetIndex offsetIndex;
    private final List<Type> fieldTypes;
    private final Supplier<Dictionary> dictionarySupplier;
    private final PageMaterializer.Factory nullMaterializerFactory;

    private Path filePath;

    ColumnChunkReaderImpl(
            ColumnChunk columnChunk, SeekableChannelsProvider channelsProvider,
            Path rootPath, ThreadLocal<CodecFactory> codecFactory, MessageType type,
            OffsetIndex offsetIndex, List<Type> fieldTypes) {
        this.channelsProvider = channelsProvider;
        this.columnChunk = columnChunk;
        this.rootPath = rootPath;
        this.path = type
                .getColumnDescription(columnChunk.meta_data.getPath_in_schema().toArray(new String[0]));
        if (columnChunk.getMeta_data().isSetCodec()) {
            decompressor = ThreadLocal.withInitial(() -> codecFactory.get().getDecompressor(
                    CompressionCodecName.valueOf(columnChunk.getMeta_data().getCodec().name())));
        } else {
            decompressor = ThreadLocal.withInitial(
                    () -> codecFactory.get().getDecompressor(CompressionCodecName.UNCOMPRESSED));
        }
        this.offsetIndex = offsetIndex;
        this.fieldTypes = fieldTypes;
        this.dictionarySupplier = new LazyCachingSupplier<>(this::getDictionary);
        this.nullMaterializerFactory = PageMaterializer.factoryForType(path.getPrimitiveType().getPrimitiveTypeName());
    }

    @Override
    public int getPageFixedSize() {
        return -1;
    }

    @Override
    public long numRows() {
        return numValues();
    }

    @Override
    public long numValues() {
        return columnChunk.getMeta_data().num_values;
    }

    @Override
    public int getMaxRl() {
        return path.getMaxRepetitionLevel();
    }

    @Override
    public ColumnPageReaderIterator getPageIterator() {
        final long dataPageOffset = columnChunk.meta_data.getData_page_offset();
        if (offsetIndex == null) {
            return new ColumnPageReaderIteratorImpl(dataPageOffset, columnChunk.getMeta_data().getNum_values(),
                    path, channelsProvider);
        } else {
            return new ColumnPageReaderIteratorIndexImpl(path, channelsProvider);
        }
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
            return readDictionary(readChannel, dictionaryPageOffset);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public PrimitiveType getType() {
        return path.getPrimitiveType();
    }

    @NotNull
    private Dictionary readDictionary(SeekableByteChannel file, long dictionaryPageOffset)
            throws IOException {
        file.position(dictionaryPageOffset);
        final InputStream inputStream = Channels.newInputStream(file);
        final PageHeader pageHeader = Util.readPageHeader(inputStream);
        if (pageHeader.getType() != PageType.DICTIONARY_PAGE) {
            // In case our fallback in getDictionary was too optimistic...
            return NULL_DICTIONARY;
        }
        final DictionaryPageHeader dictHeader = pageHeader.getDictionary_page_header();

        BytesInput payload = BytesInput.from(Helpers.readFully(file, pageHeader.compressed_page_size));
        if (decompressor != null) {
            payload = decompressor.get().decompress(payload, pageHeader.uncompressed_page_size);
        }

        final DictionaryPage dictionaryPage =
                new DictionaryPage(payload, dictHeader.getNum_values(),
                        Encoding.valueOf(dictHeader.getEncoding().name()));

        return dictionaryPage.getEncoding().initDictionary(path, dictionaryPage);
    }

    class ColumnPageReaderIteratorImpl implements ColumnPageReaderIterator {
        private final SeekableChannelsProvider channelsProvider;
        private long currentOffset;
        private final ColumnDescriptor path;

        private long remainingValues;

        ColumnPageReaderIteratorImpl(final long startOffset,
                final long numValues,
                @NotNull final ColumnDescriptor path,
                @NotNull final SeekableChannelsProvider channelsProvider) {
            this.remainingValues = numValues;
            this.currentOffset = startOffset;
            this.path = path;
            this.channelsProvider = channelsProvider;
        }

        @Override
        public boolean hasNext() {
            return remainingValues > 0;
        }

        @Override
        public ColumnPageReader next() {
            if (!hasNext()) {
                throw new RuntimeException("No next element");
            }
            // NB: The channels provider typically caches channels; this avoids maintaining a handle per column chunk
            try (final SeekableByteChannel readChannel = channelsProvider.getReadChannel(getFilePath())) {
                final long headerOffset = currentOffset;
                readChannel.position(currentOffset);
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
                return new ColumnPageReaderImpl(
                        channelsProvider, decompressor::get, pageDictionarySupplier, nullMaterializerFactory,
                        path, getFilePath(), fieldTypes,
                        readChannel.position(), pageHeader, -1);
            } catch (IOException e) {
                throw new RuntimeException("Error reading page header", e);
            }
        }

        @Override
        public void close() {}
    }

    class ColumnPageReaderIteratorIndexImpl implements ColumnPageReaderIterator {
        private final SeekableChannelsProvider channelsProvider;
        private int pos;
        private final ColumnDescriptor path;

        ColumnPageReaderIteratorIndexImpl(ColumnDescriptor path,
                SeekableChannelsProvider channelsProvider) {
            this.path = path;
            this.channelsProvider = channelsProvider;
            pos = 0;
        }

        @Override
        public boolean hasNext() {
            return offsetIndex.getPageCount() > pos;
        }

        @Override
        public ColumnPageReader next() {
            if (!hasNext()) {
                throw new RuntimeException("No next element");
            }
            int rowCount =
                    (int) (offsetIndex.getLastRowIndex(pos, columnChunk.getMeta_data().getNum_values())
                            - offsetIndex.getFirstRowIndex(pos) + 1);
            ColumnPageReaderImpl columnPageReader =
                    new ColumnPageReaderImpl(channelsProvider, decompressor::get, dictionarySupplier,
                            nullMaterializerFactory,
                            path, getFilePath(), fieldTypes,
                            offsetIndex.getOffset(pos), null, rowCount);
            pos++;
            return columnPageReader;
        }

        @Override
        public void close() throws IOException {}
    }
}
