//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.parquet.compress.CompressorAdapter;
import io.deephaven.parquet.compress.DeephavenCompressorAdapterFactory;
import io.deephaven.util.channel.SeekableChannelContext.ContextHolder;
import io.deephaven.util.datastructures.LazyCachingFunction;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.format.*;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

import static io.deephaven.base.FileUtils.convertToURI;
import static io.deephaven.parquet.base.ParquetFileReader.FILE_URI_SCHEME;
import static org.apache.parquet.format.Encoding.PLAIN_DICTIONARY;
import static org.apache.parquet.format.Encoding.RLE_DICTIONARY;

final class ColumnChunkReaderImpl implements ColumnChunkReader {

    private final ColumnChunk columnChunk;
    private final SeekableChannelsProvider channelsProvider;
    /**
     * If reading a single parquet file, root URI is the URI of the file, else the parent directory for a metadata file
     */
    private final URI rootURI;
    private final CompressorAdapter decompressor;
    private final ColumnDescriptor path;
    private final OffsetIndex offsetIndex;
    private final List<Type> fieldTypes;
    private final Function<SeekableChannelContext, Dictionary> dictionarySupplier;
    private final PageMaterializerFactory nullMaterializerFactory;

    private URI uri;
    /**
     * Number of rows in the row group of this column chunk.
     */
    private final long numRows;
    /**
     * Version string from deephaven specific parquet metadata, or null if it's not present.
     */
    private final String version;

    ColumnChunkReaderImpl(ColumnChunk columnChunk, SeekableChannelsProvider channelsProvider, URI rootURI,
            MessageType type, OffsetIndex offsetIndex, List<Type> fieldTypes, final long numRows,
            final String version) {
        this.channelsProvider = channelsProvider;
        this.columnChunk = columnChunk;
        this.rootURI = rootURI;
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
        this.dictionarySupplier = new LazyCachingFunction<>(this::getDictionary);
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

    public OffsetIndex getOffsetIndex() {
        return offsetIndex;
    }

    @Override
    public ColumnPageReaderIterator getPageIterator() {
        return new ColumnPageReaderIteratorImpl();
    }

    @Override
    public final ColumnPageDirectAccessor getPageAccessor() {
        if (offsetIndex == null) {
            throw new UnsupportedOperationException("Cannot use direct accessor without offset index");
        }
        return new ColumnPageDirectAccessorImpl();
    }

    private URI getURI() {
        if (uri != null) {
            return uri;
        }
        if (columnChunk.isSetFile_path() && FILE_URI_SCHEME.equals(rootURI.getScheme())) {
            return uri = convertToURI(Path.of(rootURI).resolve(columnChunk.getFile_path()), false);
        } else {
            // TODO(deephaven-core#5066): Add support for reading metadata files from non-file URIs
            return uri = rootURI;
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
    public Function<SeekableChannelContext, Dictionary> getDictionarySupplier() {
        return dictionarySupplier;
    }

    @NotNull
    private Dictionary getDictionary(final SeekableChannelContext channelContext) {
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
        // Use the context object provided by the caller, or create (and close) a new one
        try (
                final ContextHolder holder = SeekableChannelContext.ensureContext(channelsProvider, channelContext);
                final SeekableByteChannel ch = channelsProvider.getReadChannel(holder.get(), getURI());
                final InputStream in = channelsProvider.getInputStream(ch.position(dictionaryPageOffset))) {
            return readDictionary(in);
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

    @Override
    public SeekableChannelsProvider getChannelsProvider() {
        return channelsProvider;
    }

    @NotNull
    private Dictionary readDictionary(InputStream in) throws IOException {
        // explicitly not closing this, caller is responsible
        final PageHeader pageHeader = Util.readPageHeader(in);
        if (pageHeader.getType() != PageType.DICTIONARY_PAGE) {
            // In case our fallback in getDictionary was too optimistic...
            return NULL_DICTIONARY;
        }
        final DictionaryPageHeader dictHeader = pageHeader.getDictionary_page_header();
        final int compressedPageSize = pageHeader.getCompressed_page_size();
        final BytesInput payload;
        if (compressedPageSize == 0) {
            // Sometimes the size is explicitly empty, just use an empty payload
            payload = BytesInput.empty();
        } else {
            payload = decompressor.decompress(in, compressedPageSize, pageHeader.getUncompressed_page_size());
        }
        final Encoding encoding = Encoding.valueOf(dictHeader.getEncoding().name());
        final DictionaryPage dictionaryPage = new DictionaryPage(payload, dictHeader.getNum_values(), encoding);
        // We are safe to not copy the payload because the Dictionary doesn't hold a reference to dictionaryPage or
        // payload and thus doesn't hold a reference to the input stream.
        return encoding.initDictionary(path, dictionaryPage);
    }

    private final class ColumnPageReaderIteratorImpl implements ColumnPageReaderIterator {
        private long nextHeaderOffset;
        private long remainingValues;

        ColumnPageReaderIteratorImpl() {
            this.remainingValues = columnChunk.meta_data.getNum_values();
            this.nextHeaderOffset = columnChunk.meta_data.getData_page_offset();
        }

        @Override
        public boolean hasNext() {
            return remainingValues > 0;
        }

        @Override
        public ColumnPageReader next(@NotNull final SeekableChannelContext channelContext) {
            if (!hasNext()) {
                throw new NoSuchElementException("No next element");
            }
            // NB: The channels provider typically caches channels; this avoids maintaining a handle per column chunk
            final long headerOffset = nextHeaderOffset;
            try (
                    final ContextHolder holder = SeekableChannelContext.ensureContext(channelsProvider, channelContext);
                    final SeekableByteChannel ch = channelsProvider.getReadChannel(holder.get(), getURI())) {
                ch.position(headerOffset);
                final PageHeader pageHeader = readPageHeader(ch);
                // relying on exact position of ch
                final long dataOffset = ch.position();
                nextHeaderOffset = dataOffset + pageHeader.getCompressed_page_size();
                final PageType pageType = pageHeader.type;
                if (pageType == PageType.DICTIONARY_PAGE && headerOffset == columnChunk.meta_data.getData_page_offset()
                        && columnChunk.meta_data.getDictionary_page_offset() == 0) {
                    // https://stackoverflow.com/questions/55225108/why-is-dictionary-page-offset-0-for-plain-dictionary-encoding
                    // Skip the dictionary page and jump to the data page
                    return next(holder.get());
                }
                if (pageType != PageType.DATA_PAGE && pageType != PageType.DATA_PAGE_V2) {
                    throw new IllegalStateException("Expected data page, but got " + pageType + " at offset " +
                            headerOffset + " for file " + getURI());
                }
                final int numValuesInPage = getNumValues(pageHeader);
                remainingValues -= numValuesInPage;
                final Function<SeekableChannelContext, Dictionary> pageDictionarySupplier =
                        getPageDictionarySupplier(pageHeader);
                return new ColumnPageReaderImpl(channelsProvider, decompressor, pageDictionarySupplier,
                        nullMaterializerFactory, path, getURI(), fieldTypes, dataOffset, pageHeader, numValuesInPage);
            } catch (IOException e) {
                throw new UncheckedDeephavenException("Error reading page header at offset " + headerOffset + " for " +
                        "file " + getURI(), e);
            }
        }
    }

    private Function<SeekableChannelContext, Dictionary> getPageDictionarySupplier(final PageHeader pageHeader) {
        final org.apache.parquet.format.Encoding encoding = getEncoding(pageHeader);
        return (encoding == PLAIN_DICTIONARY || encoding == RLE_DICTIONARY)
                ? dictionarySupplier
                : (SeekableChannelContext context) -> NULL_DICTIONARY;
    }

    private static org.apache.parquet.format.Encoding getEncoding(PageHeader pageHeader) {
        switch (pageHeader.type) {
            case DATA_PAGE:
                return pageHeader.getData_page_header().getEncoding();
            case DATA_PAGE_V2:
                return pageHeader.getData_page_header_v2().getEncoding();
            default:
                throw new UncheckedDeephavenException(
                        "Unknown parquet data page header type " + pageHeader.type);
        }
    }

    private PageHeader readPageHeader(final SeekableByteChannel ch) throws IOException {
        try (final InputStream in = SeekableChannelsProvider.channelPositionInputStream(channelsProvider, ch)) {
            return Util.readPageHeader(in);
        }
    }

    private static int getNumValues(PageHeader pageHeader) {
        return pageHeader.isSetData_page_header()
                ? pageHeader.getData_page_header().getNum_values()
                : pageHeader.getData_page_header_v2().getNum_values();
    }

    private final class ColumnPageDirectAccessorImpl implements ColumnPageDirectAccessor {

        ColumnPageDirectAccessorImpl() {}

        @Override
        public ColumnPageReader getPageReader(final int pageNum, final SeekableChannelContext channelContext) {
            if (pageNum < 0 || pageNum >= offsetIndex.getPageCount()) {
                throw new IndexOutOfBoundsException(
                        "pageNum=" + pageNum + ", offsetIndex.getPageCount()=" + offsetIndex.getPageCount());
            }

            // Read the page header to determine whether we need to use dictionary for this page
            final long headerOffset = offsetIndex.getOffset(pageNum);
            try (
                    final ContextHolder holder = SeekableChannelContext.ensureContext(channelsProvider, channelContext);
                    final SeekableByteChannel ch = channelsProvider.getReadChannel(holder.get(), getURI())) {
                ch.position(headerOffset);
                final PageHeader pageHeader = readPageHeader(ch);
                final long dataOffset = ch.position();
                final PageType pageType = pageHeader.type;
                if (pageType != PageType.DATA_PAGE && pageType != PageType.DATA_PAGE_V2) {
                    throw new IllegalStateException("Expected data page, but got " + pageType + " for page number "
                            + pageNum + " at offset " + headerOffset + " for file " + getURI());
                }
                final Function<SeekableChannelContext, Dictionary> pageDictionarySupplier =
                        getPageDictionarySupplier(pageHeader);
                return new ColumnPageReaderImpl(channelsProvider, decompressor, pageDictionarySupplier,
                        nullMaterializerFactory, path, getURI(), fieldTypes, dataOffset, pageHeader,
                        getNumValues(pageHeader));
            } catch (final IOException e) {
                throw new UncheckedDeephavenException("Error reading page header for page number " + pageNum +
                        " at offset " + headerOffset + "  for file " + getURI(), e);
            }
        }
    }
}
