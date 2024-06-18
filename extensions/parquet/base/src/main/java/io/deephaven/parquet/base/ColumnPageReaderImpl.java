//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.Pair;
import io.deephaven.base.verify.Require;
import io.deephaven.parquet.base.materializers.IntMaterializer;
import io.deephaven.parquet.compress.CompressorAdapter;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelContext.ContextHolder;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.dictionary.DictionaryValuesReader;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.Type;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.apache.parquet.column.ValuesType.VALUES;

final class ColumnPageReaderImpl implements ColumnPageReader {
    private static final int NULL_OFFSET = -1;

    private final String columnName;
    private final SeekableChannelsProvider channelsProvider;
    private final CompressorAdapter compressorAdapter;
    private final Function<SeekableChannelContext, Dictionary> dictionarySupplier;
    private final PageMaterializerFactory pageMaterializerFactory;
    private final ColumnDescriptor path;
    private final URI uri;
    private final List<Type> fieldTypes;

    /**
     * The offset for data following the page header in the file.
     */
    private final long dataOffset;
    private final PageHeader pageHeader;
    private final int numValues;
    private int rowCount = -1;


    /**
     * Returns a {@link ColumnPageReader} object for reading the column page data from the file.
     *
     * @param columnName The name of the column.
     * @param channelsProvider The provider for {@link SeekableByteChannel} for reading the file.
     * @param compressorAdapter The adapter for decompressing the data.
     * @param dictionarySupplier The supplier for dictionary data, set as {@link ColumnChunkReader#NULL_DICTIONARY} if
     *        page isn't dictionary encoded
     * @param materializerFactory The factory for creating {@link PageMaterializer}.
     * @param path The path of the column.
     * @param uri The uri of the parquet file.
     * @param fieldTypes The types of the fields in the column.
     * @param dataOffset The offset for data following the page header in the file.
     * @param pageHeader The page header, should not be {@code null}.
     * @param numValues The number of values in the page.
     */
    ColumnPageReaderImpl(
            final String columnName,
            final SeekableChannelsProvider channelsProvider,
            final CompressorAdapter compressorAdapter,
            final Function<SeekableChannelContext, Dictionary> dictionarySupplier,
            final PageMaterializerFactory materializerFactory,
            final ColumnDescriptor path,
            final URI uri,
            final List<Type> fieldTypes,
            final long dataOffset,
            final PageHeader pageHeader,
            final int numValues) {
        this.columnName = columnName;
        this.channelsProvider = channelsProvider;
        this.compressorAdapter = compressorAdapter;
        this.dictionarySupplier = dictionarySupplier;
        this.pageMaterializerFactory = materializerFactory;
        this.path = path;
        this.uri = uri;
        this.fieldTypes = fieldTypes;
        this.dataOffset = dataOffset;
        this.pageHeader = Require.neqNull(pageHeader, "pageHeader");
        this.numValues = Require.geqZero(numValues, "numValues");
    }

    @Override
    public Object materialize(@NotNull final Object nullValue,
            @NotNull final SeekableChannelContext channelContext) throws IOException {
        try (
                final ContextHolder holder = SeekableChannelContext.ensureContext(channelsProvider, channelContext);
                final SeekableByteChannel ch = channelsProvider.getReadChannel(holder.get(), uri)) {
            ch.position(dataOffset);
            return readDataPage(nullValue, ch, holder.get());
        }
    }

    private int readRowCount(@NotNull final SeekableChannelContext channelContext) throws IOException {
        try (
                final ContextHolder holder = SeekableChannelContext.ensureContext(channelsProvider, channelContext);
                final SeekableByteChannel ch = channelsProvider.getReadChannel(holder.get(), uri)) {
            ch.position(dataOffset);
            return readRowCountFromDataPage(ch, holder.get());
        }
    }

    @Override
    public IntBuffer readKeyValues(
            final IntBuffer keyDest,
            final int nullPlaceholder,
            @NotNull final SeekableChannelContext channelContext) throws IOException {
        try (
                final ContextHolder holder = SeekableChannelContext.ensureContext(channelsProvider, channelContext);
                final SeekableByteChannel ch = channelsProvider.getReadChannel(holder.get(), uri)) {
            ch.position(dataOffset);
            return readKeysFromDataPage(keyDest, nullPlaceholder, ch, holder.get());
        }
    }

    /**
     * Callers must ensure resulting data page does not outlive the input stream.
     */
    private DataPageV1 readV1Unsafe(
            final InputStream in,
            @NotNull final SeekableChannelContext channelContext) throws IOException {
        if (pageHeader.type != PageType.DATA_PAGE) {
            throw new IllegalArgumentException("Expected parquet DATA_PAGE V1, found " + pageHeader.getType() + " for "
                    + "column: " + columnName + ", uri: " + uri);
        }
        final int uncompressedPageSize = pageHeader.getUncompressed_page_size();
        final int compressedPageSize = pageHeader.getCompressed_page_size();
        final BytesInput decompressedInput =
                compressorAdapter.decompress(in, compressedPageSize, uncompressedPageSize, channelContext);
        final DataPageHeader header = pageHeader.getData_page_header();
        return new DataPageV1(
                decompressedInput,
                header.getNum_values(),
                uncompressedPageSize,
                null, // TODO in the future might want to pull in statistics
                getEncoding(header.getRepetition_level_encoding()),
                getEncoding(header.getDefinition_level_encoding()),
                getEncoding(header.getEncoding()));
    }

    /**
     * Callers must ensure resulting data page does not outlive the input stream.
     */
    private DataPageV2 readV2Unsafe(
            final InputStream in,
            @NotNull final SeekableChannelContext channelContext) throws IOException {
        if (pageHeader.type != PageType.DATA_PAGE_V2) {
            throw new IllegalArgumentException("Expected parquet DATA_PAGE_V2, found " + pageHeader.getType() + " for "
                    + "column: " + columnName + ", uri: " + uri);
        }
        final int uncompressedPageSize = pageHeader.getUncompressed_page_size();
        final int compressedPageSize = pageHeader.getCompressed_page_size();
        final DataPageHeaderV2 header = pageHeader.getData_page_header_v2();
        final int compressedSize = compressedPageSize - header.getRepetition_levels_byte_length()
                - header.getDefinition_levels_byte_length();
        final int uncompressedSize = uncompressedPageSize - header.getRepetition_levels_byte_length()
                - header.getDefinition_levels_byte_length();
        // With our current single input stream `in` construction, we must copy out the bytes for repetition and
        // definition levels to proceed to data. We could theoretically restructure this to be fully lazy, either by
        // creating a BytesInput impl for SeekableByteChannel, or by migrating this logic one layer up and ensuring we
        // construct input streams separately for repetitionLevelsIn, definitionLevelsIn, and dataIn. Both of these
        // solutions would potentially suffer from a disconnect in cache-ability that a single input stream provides.
        final BytesInput repetitionLevels =
                BytesInput.copy(BytesInput.from(in, header.getRepetition_levels_byte_length()));
        final BytesInput definitionLevels =
                BytesInput.copy(BytesInput.from(in, header.getDefinition_levels_byte_length()));
        final BytesInput data = compressorAdapter.decompress(in, compressedSize, uncompressedSize, channelContext);
        return new DataPageV2(
                header.getNum_rows(),
                header.getNum_nulls(),
                header.getNum_values(),
                repetitionLevels,
                definitionLevels,
                getEncoding(header.getEncoding()),
                data,
                uncompressedPageSize,
                null, // TODO in the future might want to pull in statistics,
                false);
    }

    private int readRowCountFromDataPage(
            final SeekableByteChannel ch,
            @NotNull final SeekableChannelContext channelContext) throws IOException {
        switch (pageHeader.type) {
            case DATA_PAGE:
                try (final InputStream in = channelsProvider.getInputStream(ch, pageHeader.getCompressed_page_size())) {
                    return readRowCountFromPageV1(readV1Unsafe(in, channelContext));
                }
            case DATA_PAGE_V2:
                final DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
                return dataHeaderV2.getNum_rows();
            default:
                throw new UncheckedDeephavenException("Unsupported page type " + pageHeader.type + " for column: " +
                        columnName + ", uri: " + uri);
        }
    }

    private IntBuffer readKeysFromDataPage(
            final IntBuffer keyDest,
            final int nullPlaceholder,
            final SeekableByteChannel ch,
            @NotNull final SeekableChannelContext channelContext) throws IOException {
        switch (pageHeader.type) {
            case DATA_PAGE:
                try (final InputStream in = channelsProvider.getInputStream(ch, pageHeader.getCompressed_page_size())) {
                    return readKeysFromPageV1(readV1Unsafe(in, channelContext), keyDest, nullPlaceholder,
                            channelContext);
                }
            case DATA_PAGE_V2:
                try (final InputStream in = channelsProvider.getInputStream(ch, pageHeader.getCompressed_page_size())) {
                    return readKeysFromPageV2(readV2Unsafe(in, channelContext), keyDest, nullPlaceholder,
                            channelContext);
                }
            default:
                throw new IOException(String.format("Unexpected page of type %s of size %d", pageHeader.getType(),
                        pageHeader.getCompressed_page_size()) + " for column: " + columnName + ", uri: " + uri);
        }
    }

    private Object readDataPage(
            final Object nullValue,
            final SeekableByteChannel ch,
            @NotNull final SeekableChannelContext channelContext) throws IOException {
        switch (pageHeader.type) {
            case DATA_PAGE:
                try (final InputStream in = channelsProvider.getInputStream(ch, pageHeader.getCompressed_page_size())) {
                    return readPageV1(readV1Unsafe(in, channelContext), nullValue, channelContext);
                }
            case DATA_PAGE_V2:
                try (final InputStream in = channelsProvider.getInputStream(ch, pageHeader.getCompressed_page_size())) {
                    return readPageV2(readV2Unsafe(in, channelContext), nullValue, channelContext);
                }
            default:
                throw new IOException(String.format("Unexpected page of type %s of size %d", pageHeader.getType(),
                        pageHeader.getCompressed_page_size()) + " for column: " + columnName + ", uri: " + uri);
        }
    }

    private static Encoding getEncoding(final org.apache.parquet.format.Encoding encoding) {
        return org.apache.parquet.column.Encoding.valueOf(encoding.name());
    }

    private int readRowCountFromPageV1(final DataPageV1 page) {
        try {
            if (path.getMaxRepetitionLevel() != 0) {
                // TODO - move away from page and use ByteBuffers directly
                final ByteBuffer bytes = page.getBytes().toByteBuffer();
                bytes.order(ByteOrder.LITTLE_ENDIAN);
                final int length = bytes.getInt();
                return readRepetitionLevels(bytes.slice().limit(length));
            } else {
                return page.getValueCount();
            }
        } catch (final IOException e) {
            throw new ParquetDecodingException("Failed to read row count from Parquet V1 page for column: " +
                    columnName + ", uri: " + uri, e);
        }
    }

    /**
     * This method assumes that the input buffer is already positioned at the start of the repetition levels. It will
     * read the repetition levels and will increment the position of the input buffer to the end of the repetition
     * levels.
     */
    @Nullable
    private RunLengthBitPackingHybridBufferDecoder getRlDecoderPageV1(final ByteBuffer pageBytes) {
        if (path.getMaxRepetitionLevel() != 0) {
            final int length = pageBytes.getInt();
            final RunLengthBitPackingHybridBufferDecoder rlDecoder =
                    new RunLengthBitPackingHybridBufferDecoder(path.getMaxRepetitionLevel(),
                            pageBytes.slice().limit(length));
            pageBytes.position(pageBytes.position() + length);
            return rlDecoder;
        }
        return null;
    }

    /**
     * This method assumes that the input buffer is already positioned at the start of the definition levels. It will
     * read the definition levels and will increment the position of the input buffer to the end of the definition
     * levels.
     */
    @Nullable
    private RunLengthBitPackingHybridBufferDecoder getDlDecoderPageV1(final ByteBuffer pageBytes) {
        if (path.getMaxDefinitionLevel() > 0) {
            final int length = pageBytes.getInt();
            final RunLengthBitPackingHybridBufferDecoder dlDecoder =
                    new RunLengthBitPackingHybridBufferDecoder(path.getMaxDefinitionLevel(),
                            pageBytes.slice().limit(length));
            pageBytes.position(pageBytes.position() + length);
            return dlDecoder;
        }
        return null;
    }

    @Nullable
    private IntBuffer readKeysFromPageV1(
            final DataPageV1 page,
            final IntBuffer keyDest,
            final int nullPlaceholder,
            @NotNull final SeekableChannelContext channelContext) {
        try {
            // TODO - move away from page and use ByteBuffers directly
            final ByteBuffer bytes = page.getBytes().toByteBuffer();
            bytes.order(ByteOrder.LITTLE_ENDIAN);
            final RunLengthBitPackingHybridBufferDecoder rlDecoder = getRlDecoderPageV1(bytes);
            final RunLengthBitPackingHybridBufferDecoder dlDecoder = getDlDecoderPageV1(bytes);
            final ValuesReader dataReader =
                    new KeyIndexReader((DictionaryValuesReader) getDataReader(page.getValueEncoding(),
                            bytes, page.getValueCount(), channelContext));
            return readKeysFromPageCommon(keyDest, nullPlaceholder, rlDecoder, dlDecoder, dataReader);
        } catch (final IOException e) {
            throw new ParquetDecodingException("could not read page " + page + " in col " + path, e);
        }
    }

    @Nullable
    private IntBuffer readKeysFromPageCommon(
            final IntBuffer keyDest,
            final int nullPlaceholder,
            final RunLengthBitPackingHybridBufferDecoder rlDecoder,
            final RunLengthBitPackingHybridBufferDecoder dlDecoder,
            final ValuesReader dataReader) throws IOException {
        final Object result = materialize(IntMaterializer.Factory, dlDecoder, rlDecoder, dataReader, nullPlaceholder);
        if (result instanceof DataWithOffsets) {
            keyDest.put((int[]) ((DataWithOffsets) result).materializeResult);
            return ((DataWithOffsets) result).offsets;
        }
        keyDest.put((int[]) result);
        return null;
    }

    private int readRepetitionLevels(final ByteBuffer byteBuffer) throws IOException {
        final RunLengthBitPackingHybridBufferDecoder rlDecoder;
        rlDecoder = new RunLengthBitPackingHybridBufferDecoder(path.getMaxRepetitionLevel(), byteBuffer);
        int rowsRead = 0;
        int totalCount = 0;
        while (rlDecoder.hasNext() && totalCount < numValues) {
            rlDecoder.readNextRange();
            int count = rlDecoder.currentRangeCount();
            totalCount += count;
            if (totalCount > numValues) {
                count -= (totalCount - numValues);
                totalCount = numValues;
            }
            if (rlDecoder.currentValue() == 0) {
                rowsRead += count;
            }
        }
        return rowsRead;
    }

    private Object readPageV1(
            final DataPageV1 page,
            final Object nullValue,
            @NotNull final SeekableChannelContext channelContext) {
        try {
            // TODO - move away from page and use ByteBuffers directly
            final ByteBuffer bytes = page.getBytes().toByteBuffer();
            bytes.order(ByteOrder.LITTLE_ENDIAN);
            final RunLengthBitPackingHybridBufferDecoder rlDecoder = getRlDecoderPageV1(bytes);
            final RunLengthBitPackingHybridBufferDecoder dlDecoder = getDlDecoderPageV1(bytes);
            final ValuesReader dataReader =
                    getDataReader(page.getValueEncoding(), bytes, page.getValueCount(), channelContext);
            return materialize(pageMaterializerFactory, dlDecoder, rlDecoder, dataReader, nullValue);
        } catch (final IOException e) {
            throw new ParquetDecodingException("Failed to read parquet V1 page for column: " + columnName +
                    ", uri: " + uri, e);
        }
    }

    private Object materialize(
            final PageMaterializerFactory factory,
            final RunLengthBitPackingHybridBufferDecoder dlDecoder,
            final RunLengthBitPackingHybridBufferDecoder rlDecoder,
            final ValuesReader dataReader,
            final Object nullValue) throws IOException {
        if (dlDecoder == null) {
            return materializeNonNull(factory, numValues, dataReader);
        } else {
            return materializeWithNulls(factory, dlDecoder, rlDecoder, dataReader, nullValue);
        }
    }

    @Nullable
    private RunLengthBitPackingHybridBufferDecoder getRlDecoderPageV2(final DataPageV2 page) throws IOException {
        if (path.getMaxRepetitionLevel() != 0) {
            return new RunLengthBitPackingHybridBufferDecoder(path.getMaxRepetitionLevel(),
                    page.getRepetitionLevels().toByteBuffer());
        }
        return null;
    }

    @Nullable
    private RunLengthBitPackingHybridBufferDecoder getDlDecoderPageV2(final DataPageV2 page) throws IOException {
        if (path.getMaxDefinitionLevel() > 0) {
            return new RunLengthBitPackingHybridBufferDecoder(path.getMaxDefinitionLevel(),
                    page.getDefinitionLevels().toByteBuffer());
        }
        return null;
    }

    @Nullable
    private IntBuffer readKeysFromPageV2(
            final DataPageV2 page,
            final IntBuffer keyDest,
            final int nullPlaceholder,
            @NotNull final SeekableChannelContext channelContext) throws IOException {
        final RunLengthBitPackingHybridBufferDecoder rlDecoder = getRlDecoderPageV2(page);
        final RunLengthBitPackingHybridBufferDecoder dlDecoder = getDlDecoderPageV2(page);
        final ValuesReader dataReader =
                new KeyIndexReader((DictionaryValuesReader) getDataReader(page.getDataEncoding(),
                        page.getData().toByteBuffer(), page.getValueCount(), channelContext));
        return readKeysFromPageCommon(keyDest, nullPlaceholder, rlDecoder, dlDecoder, dataReader);
    }

    private Object readPageV2(
            final DataPageV2 page,
            final Object nullValue,
            @NotNull final SeekableChannelContext channelContext) {
        try {
            final RunLengthBitPackingHybridBufferDecoder rlDecoder = getRlDecoderPageV2(page);
            final RunLengthBitPackingHybridBufferDecoder dlDecoder = getDlDecoderPageV2(page);
            final ValuesReader dataReader = getDataReader(page.getDataEncoding(),
                    page.getData().toByteBuffer(), page.getValueCount(), channelContext);
            return materialize(pageMaterializerFactory, dlDecoder, rlDecoder, dataReader, nullValue);
        } catch (final IOException e) {
            throw new ParquetDecodingException("Failed to read parquet V2 page for column: " + columnName +
                    ", uri: " + uri, e);
        }
    }

    private static Object materializeWithNulls(
            final PageMaterializerFactory factory,
            final int numberOfValues,
            final IntBuffer nullOffsets,
            final ValuesReader dataReader,
            final Object nullValue) {
        final PageMaterializer materializer = factory.makeMaterializerWithNulls(dataReader, nullValue, numberOfValues);
        int startIndex = 0;
        int nextNullPos = nullOffsets.hasRemaining() ? nullOffsets.get() : numberOfValues;
        while (startIndex < numberOfValues) {
            int rangeEnd = startIndex;
            while (nextNullPos == rangeEnd && rangeEnd < numberOfValues) {
                rangeEnd++;
                nextNullPos = nullOffsets.hasRemaining() ? nullOffsets.get() : numberOfValues;
            }
            materializer.fillNulls(startIndex, rangeEnd);
            materializer.fillValues(rangeEnd, nextNullPos);
            startIndex = nextNullPos;
        }
        return materializer.data();
    }

    /**
     * Creates a list of offsets with null entries
     */
    private static IntBuffer combineOptionalAndRepeating(
            final IntBuffer nullOffsets,
            final IntBuffer repeatingRanges,
            final int nullValue) {
        final IntBuffer result = IntBuffer.allocate(nullOffsets.limit() + repeatingRanges.limit());
        int startIndex = 0;
        int nextNullPos = nullOffsets.hasRemaining() ? nullOffsets.get() : result.capacity();
        while (result.hasRemaining()) {
            int rangeEnd = startIndex;
            while (nextNullPos == rangeEnd) {
                rangeEnd++;
                nextNullPos = nullOffsets.hasRemaining() ? nullOffsets.get() : result.capacity();
            }
            for (int i = startIndex; i < rangeEnd && result.hasRemaining(); i++) {
                result.put(nullValue);
            }
            for (int i = rangeEnd; i < nextNullPos; i++) {
                result.put(repeatingRanges.get());
            }
            startIndex = nextNullPos;
        }
        result.flip();
        return result;
    }

    private Object materializeWithNulls(
            final PageMaterializerFactory factory,
            final RunLengthBitPackingHybridBufferDecoder dlDecoder,
            final RunLengthBitPackingHybridBufferDecoder rlDecoder,
            final ValuesReader dataReader,
            final Object nullValue) throws IOException {
        final Pair<Pair<Type.Repetition, IntBuffer>[], Integer> offsetsAndCount =
                getOffsetsAndNulls(dlDecoder, rlDecoder);
        final int updatedNumValues = offsetsAndCount.second;
        final Pair<Type.Repetition, IntBuffer>[] offsetAndNulls = offsetsAndCount.first;
        final List<IntBuffer> offsetsWithNull = new ArrayList<>();
        IntBuffer currentNullOffsets = null;
        for (final Pair<Type.Repetition, IntBuffer> offsetAndNull : offsetAndNulls) {
            if (offsetAndNull.first == Type.Repetition.OPTIONAL) {
                if (currentNullOffsets != null) {
                    throw new UnsupportedOperationException(
                            "Failed to read parquet page because nested optional levels are not supported, found for column: "
                                    + columnName + ", uri: " + uri);
                }
                currentNullOffsets = offsetAndNull.second;
            } else {
                if (currentNullOffsets != null) {
                    offsetsWithNull.add(combineOptionalAndRepeating(currentNullOffsets,
                            offsetAndNull.second, NULL_OFFSET));
                    currentNullOffsets = null;
                } else {
                    offsetsWithNull.add(offsetAndNull.second);
                }
            }
        }
        final Object values;
        if (currentNullOffsets != null && currentNullOffsets.hasRemaining()) {
            values = materializeWithNulls(factory, updatedNumValues, currentNullOffsets,
                    dataReader, nullValue);
        } else {
            values = materializeNonNull(factory, updatedNumValues, dataReader);
        }
        if (offsetsWithNull.isEmpty()) {
            return values;
        }
        if (offsetsWithNull.size() == 1) {
            return new DataWithOffsets(offsetsWithNull.get(0), values);
        }
        return new DataWithMultiLevelOffsets(offsetsWithNull.toArray(new IntBuffer[0]), values);
    }

    private static Object materializeNonNull(
            final PageMaterializerFactory factory,
            final int numberOfValues,
            final ValuesReader dataReader) {
        return factory.makeMaterializerNonNull(dataReader, numberOfValues).fillAll();
    }

    private ValuesReader getDataReader(
            final Encoding dataEncoding,
            final ByteBuffer in,
            final int valueCount,
            @NotNull final SeekableChannelContext channelContext) {
        if (dataEncoding == Encoding.DELTA_BYTE_ARRAY) {
            throw new RuntimeException("DELTA_BYTE_ARRAY encoding not supported");
        }
        final ValuesReader dataReader;
        if (dataEncoding.usesDictionary()) {
            final Dictionary dictionary = dictionarySupplier.apply(channelContext);
            if (dictionary == ColumnChunkReader.NULL_DICTIONARY) {
                throw new ParquetDecodingException(
                        "Failed to read parquet page for column: " + columnName + ", uri: " + uri +
                                " as the dictionary was missing for encoding: " + dataEncoding);
            }
            dataReader = new DictionaryValuesReader(dictionary);
        } else {
            dataReader = dataEncoding.getValuesReader(path, VALUES);
        }

        try {
            dataReader.initFromPage(valueCount, ByteBufferInputStream.wrap(in));
        } catch (final IOException e) {
            throw new ParquetDecodingException("Failed to read parquet page for column: " + columnName +
                    ", uri: " + uri, e);
        }
        return dataReader;
    }

    @Override
    public int numValues() {
        return numValues;
    }

    @NotNull
    @Override
    public Dictionary getDictionary(@NotNull final SeekableChannelContext channelContext) {
        return dictionarySupplier.apply(channelContext);
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public long numRows(@NotNull final SeekableChannelContext channelContext) throws IOException {
        if (rowCount == -1) {
            if (path.getMaxRepetitionLevel() == 0) {
                rowCount = numValues();
            } else {
                rowCount = readRowCount(channelContext);
            }
        }
        return rowCount;
    }

    private Pair<Pair<Type.Repetition, IntBuffer>[], Integer> getOffsetsAndNulls(
            final RunLengthBitPackingHybridBufferDecoder dlDecoder,
            final RunLengthBitPackingHybridBufferDecoder rlDecoder) throws IOException {
        dlDecoder.readNextRange();
        if (rlDecoder != null) {
            rlDecoder.readNextRange();
        }
        int dlRangeSize = dlDecoder.currentRangeCount();
        int currentDl = dlDecoder.currentValue();
        int rlRangeSize = rlDecoder == null ? numValues : rlDecoder.currentRangeCount();
        int currentRl = rlDecoder == null ? 0 : rlDecoder.currentValue();

        final LevelsController levelsController = new LevelsController(
                fieldTypes.stream().map(Type::getRepetition).toArray(Type.Repetition[]::new));
        for (int valuesProcessed = 0; valuesProcessed < numValues;) {
            if (dlRangeSize == 0) {
                dlDecoder.readNextRange();
                dlRangeSize = Math.min(numValues - valuesProcessed, dlDecoder.currentRangeCount());
                currentDl = dlDecoder.currentValue();
            }
            if (rlRangeSize == 0) {
                rlDecoder.readNextRange();
                rlRangeSize = Math.min(numValues - valuesProcessed, rlDecoder.currentRangeCount());
                currentRl = rlDecoder.currentValue();
            }
            final int currentRangeSize = Math.min(dlRangeSize, rlRangeSize);
            dlRangeSize -= currentRangeSize;
            rlRangeSize -= currentRangeSize;
            levelsController.addElements(currentRl, currentDl, currentRangeSize);
            valuesProcessed += currentRangeSize;
        }
        return levelsController.getFinalState();
    }
}
