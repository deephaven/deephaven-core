package io.deephaven.parquet;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.parquet.utils.SeekableChannelsProvider;
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
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.List;

import static io.deephaven.parquet.utils.Helpers.readFully;

public class ColumnChunkReaderImpl implements ColumnChunkReader {

    private final ColumnChunk columnChunk;
    private final SeekableChannelsProvider channelsProvider;
    private final Path rootPath;
    private final ThreadLocal<CompressionCodecFactory.BytesInputDecompressor> decompressor;
    private final ColumnDescriptor path;
    private final OffsetIndex offsetIndex;
    private final List<Type> fieldTypes;
    private Dictionary dictionary;


    ColumnChunkReaderImpl(ColumnChunk columnChunk, SeekableChannelsProvider channelsProvider, Path rootPath, ThreadLocal<CodecFactory> codecFactory, MessageType type, OffsetIndex offsetIndex, List<Type> fieldTypes) {
        this.channelsProvider = channelsProvider;
        this.columnChunk = columnChunk;
        this.rootPath = rootPath;
        this.path = type.getColumnDescription(columnChunk.meta_data.getPath_in_schema().toArray(new String[0]));
        if (columnChunk.getMeta_data().isSetCodec()) {
            decompressor = ThreadLocal.withInitial(() -> codecFactory.get().getDecompressor(CompressionCodecName.valueOf(columnChunk.getMeta_data().getCodec().name())));
        } else {
            decompressor = ThreadLocal.withInitial(() -> codecFactory.get().getDecompressor(CompressionCodecName.UNCOMPRESSED));
        }
        this.offsetIndex = offsetIndex;
        this.fieldTypes = fieldTypes;
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
    public ColumnPageReaderIterator getPageIterator() throws IOException {

        SeekableByteChannel readChannel = channelsProvider.getReadChannel(getPath());
        long dataPageOffset = columnChunk.meta_data.getData_page_offset();
        Dictionary dictionary = getDictionary(readChannel);
        if (offsetIndex == null) {
            return new ColumnPageReaderIteratorImpl(readChannel,
                    dataPageOffset, columnChunk.getMeta_data().getNum_values(), path,
                    dictionary, channelsProvider);
        } else {
            readChannel.close();
            return new ColumnPageReaderIteratorIndexImpl(path, dictionary, channelsProvider);
        }
    }

    private Path getPath() {
        Path pagePath = rootPath;
        if (columnChunk.isSetFile_path()) {
            pagePath = pagePath.resolve(columnChunk.getFile_path());
        }
        return pagePath;
    }

    @Override
    public Dictionary getDictionary() throws IOException {
        if (dictionary != null) {
            return dictionary;
        }
        try (SeekableByteChannel channel = channelsProvider.getReadChannel(getPath())) {
            return getDictionary(channel);
        }
    }

    @Nullable
    private Dictionary getDictionary(SeekableByteChannel readChannel) throws IOException {
        if (dictionary != null) {
            return dictionary;
        }
        dictionary = columnChunk.getMeta_data().isSetDictionary_page_offset() ? readDictionary(readChannel, columnChunk.getMeta_data().getDictionary_page_offset()) : null;
        return dictionary;
    }


    @Override
    public PrimitiveType getType() {
        return path.getPrimitiveType();
    }

    private Dictionary readDictionary(SeekableByteChannel file, long dictionaryPageOffset) throws IOException {
        file.position(dictionaryPageOffset);
        InputStream inputStream = Channels.newInputStream(file);
        PageHeader pageHeader = Util.readPageHeader(inputStream);
        DictionaryPageHeader dicHeader = pageHeader.getDictionary_page_header();

        BytesInput payload = BytesInput.from(readFully(file, pageHeader.compressed_page_size));
        if (decompressor != null) {
            payload = decompressor.get().decompress(payload, pageHeader.uncompressed_page_size);
        }

        DictionaryPage dictionaryPage = new DictionaryPage(payload, dicHeader.getNum_values(),
                Encoding.valueOf(dicHeader.getEncoding().name()));

        return dictionaryPage.getEncoding().initDictionary(path, dictionaryPage);
    }

    class ColumnPageReaderIteratorImpl implements ColumnPageReaderIterator {
        private final SeekableChannelsProvider channelsProvider;
        private long currentOffset;
        private final SeekableByteChannel file;
        private final ColumnDescriptor path;

        long remainingValues;
        private final Dictionary dictionary;

        ColumnPageReaderIteratorImpl(SeekableByteChannel file, long startOffset, long numValues, ColumnDescriptor path, Dictionary dictionary, SeekableChannelsProvider channelsProvider) {
            this.remainingValues = numValues;
            this.currentOffset = startOffset;
            this.file = file;
            this.path = path;
            this.dictionary = dictionary;
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
            try {
                file.position(currentOffset);
                PageHeader pageHeader = Util.readPageHeader(Channels.newInputStream(file));
                currentOffset = file.position() + pageHeader.getCompressed_page_size();
                remainingValues -= pageHeader.isSetData_page_header() ? pageHeader.getData_page_header().num_values :
                        pageHeader.getData_page_header_v2().getNum_values();
                final org.apache.parquet.format.Encoding encoding;
                switch (pageHeader.type) {
                    case DATA_PAGE:
                        encoding = pageHeader.getData_page_header().getEncoding();
                        break;
                    case DATA_PAGE_V2:
                        encoding = pageHeader.getData_page_header_v2().getEncoding();
                        break;
                    default:
                        throw new UncheckedDeephavenException("Unknown parquet data page header type " + pageHeader.type);
                }
                final Dictionary pageDictionary =
                        (encoding == org.apache.parquet.format.Encoding.PLAIN_DICTIONARY
                                || encoding == org.apache.parquet.format.Encoding.RLE_DICTIONARY)
                        ? dictionary
                        : null
                        ;
                return new ColumnPageReaderImpl(
                        channelsProvider, file.position(), pageHeader,
                        decompressor, path, pageDictionary, getPath(),
                        -1, fieldTypes);
            } catch (IOException e) {
                throw new RuntimeException("Error reading page header", e);
            }
        }

        @Override
        public void close() throws IOException {
            file.close();
        }
    }

    class ColumnPageReaderIteratorIndexImpl implements ColumnPageReaderIterator {
        private final SeekableChannelsProvider channelsProvider;
        private int pos;
        private final ColumnDescriptor path;

        private final Dictionary dictionary;

        ColumnPageReaderIteratorIndexImpl(ColumnDescriptor path, Dictionary dictionary, SeekableChannelsProvider channelsProvider) {
            this.path = path;
            this.dictionary = dictionary;
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
            int rowCount = (int) (offsetIndex.getLastRowIndex(pos, columnChunk.getMeta_data().getNum_values()) - offsetIndex.getFirstRowIndex(pos) + 1);
            ColumnPageReaderImpl columnPageReader = new ColumnPageReaderImpl(channelsProvider, offsetIndex.getOffset(pos),
                    null, decompressor, path, dictionary, getPath(), rowCount,fieldTypes);
            pos++;
            return columnPageReader;
        }

        @Override
        public void close() throws IOException {
        }
    }
}
