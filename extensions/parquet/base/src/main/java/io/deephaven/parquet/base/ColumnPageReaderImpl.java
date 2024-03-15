//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Require;
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
    private PageHeader pageHeader;
    private int numValues;
    private int rowCount = -1;

    /**
     * Returns a {@link ColumnPageReader} object for reading the column page data from the file.
     *
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
    ColumnPageReaderImpl(SeekableChannelsProvider channelsProvider,
            CompressorAdapter compressorAdapter,
            Function<SeekableChannelContext, Dictionary> dictionarySupplier,
            PageMaterializerFactory materializerFactory,
            ColumnDescriptor path,
            URI uri,
            List<Type> fieldTypes,
            long dataOffset,
            PageHeader pageHeader,
            int numValues) {
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

    public int readRowCount(@NotNull final SeekableChannelContext channelContext) throws IOException {
        try (
                final ContextHolder holder = SeekableChannelContext.ensureContext(channelsProvider, channelContext);
                final SeekableByteChannel ch = channelsProvider.getReadChannel(holder.get(), uri)) {
            ch.position(dataOffset);
            return readRowCountFromDataPage(ch);
        }
    }

    @Override
    public IntBuffer readKeyValues(IntBuffer keyDest, int nullPlaceholder,
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
    private DataPageV1 readV1Unsafe(InputStream in) throws IOException {
        if (pageHeader.type != PageType.DATA_PAGE) {
            throw new IllegalArgumentException();
        }
        final int uncompressedPageSize = pageHeader.getUncompressed_page_size();
        final int compressedPageSize = pageHeader.getCompressed_page_size();
        final BytesInput decompressedInput = compressorAdapter.decompress(in, compressedPageSize, uncompressedPageSize);
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
    private DataPageV2 readV2Unsafe(InputStream in) throws IOException {
        if (pageHeader.type != PageType.DATA_PAGE_V2) {
            throw new IllegalArgumentException();
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
        final BytesInput data = compressorAdapter.decompress(in, compressedSize, uncompressedSize);
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

    private int readRowCountFromDataPage(SeekableByteChannel ch) throws IOException {
        switch (pageHeader.type) {
            case DATA_PAGE:
                try (final InputStream in = channelsProvider.getInputStream(ch)) {
                    return readRowCountFromPageV1(readV1Unsafe(in));
                }
            case DATA_PAGE_V2:
                DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
                return dataHeaderV2.getNum_rows();
            default:
                throw new RuntimeException("Unsupported type" + pageHeader.type);
        }
    }

    private IntBuffer readKeysFromDataPage(IntBuffer keyDest, int nullPlaceholder, SeekableByteChannel ch,
            @NotNull final SeekableChannelContext channelContext) throws IOException {
        switch (pageHeader.type) {
            case DATA_PAGE:
                try (final InputStream in = channelsProvider.getInputStream(ch)) {
                    return readKeysFromPageV1(readV1Unsafe(in), keyDest, nullPlaceholder, channelContext);
                }
            case DATA_PAGE_V2:
                try (final InputStream in = channelsProvider.getInputStream(ch)) {
                    readKeysFromPageV2(readV2Unsafe(in), keyDest, nullPlaceholder, channelContext);
                    return null;
                }
            default:
                throw new IOException(String.format("Unexpected page of type %s of size %d", pageHeader.getType(),
                        pageHeader.getCompressed_page_size()));
        }
    }

    private Object readDataPage(Object nullValue, SeekableByteChannel ch,
            @NotNull SeekableChannelContext channelContext) throws IOException {
        switch (pageHeader.type) {
            case DATA_PAGE:
                try (final InputStream in = channelsProvider.getInputStream(ch)) {
                    return readPageV1(readV1Unsafe(in), nullValue, channelContext);
                }
            case DATA_PAGE_V2:
                try (final InputStream in = channelsProvider.getInputStream(ch)) {
                    return readPageV2(readV2Unsafe(in), nullValue);
                }
            default:
                throw new IOException(String.format("Unexpected page of type %s of size %d", pageHeader.getType(),
                        pageHeader.getCompressed_page_size()));
        }
    }

    private Encoding getEncoding(org.apache.parquet.format.Encoding encoding) {
        return org.apache.parquet.column.Encoding.valueOf(encoding.name());
    }

    private int readRowCountFromPageV1(DataPageV1 page) {
        try {
            if (path.getMaxRepetitionLevel() != 0) {
                // TODO - move away from page and use ByteBuffers directly
                ByteBuffer bytes = page.getBytes().toByteBuffer();
                bytes.order(ByteOrder.LITTLE_ENDIAN);
                int length = bytes.getInt();
                return readRepetitionLevels(bytes.slice().limit(length));
            } else {
                return page.getValueCount();
            }
        } catch (IOException e) {
            throw new ParquetDecodingException("could not read page " + page + " in col " + path, e);
        }
    }

    private IntBuffer readKeysFromPageV1(DataPageV1 page, IntBuffer keyDest, int nullPlaceholder,
            @NotNull SeekableChannelContext channelContext) {
        RunLengthBitPackingHybridBufferDecoder rlDecoder = null;
        RunLengthBitPackingHybridBufferDecoder dlDecoder = null;
        try {
            ByteBuffer bytes = page.getBytes().toByteBuffer(); // TODO - move away from page and use
                                                               // ByteBuffers directly
            bytes.order(ByteOrder.LITTLE_ENDIAN);
            /*
             * IntBuffer offsets = null; if (path.getMaxRepetitionLevel() != 0) { int length = bytes.getInt(); offsets =
             * readRepetitionLevels((ByteBuffer) bytes.slice().limit(length), IntBuffer.allocate(INITIAL_BUFFER_SIZE));
             * bytes.position(bytes.position() + length); } if (path.getMaxDefinitionLevel() > 0) { int length =
             * bytes.getInt(); dlDecoder = new RunLenghBitPackingHybridBufferDecoder(path.getMaxDefinitionLevel(),
             * (ByteBuffer) bytes.slice().limit(length)); bytes.position(bytes.position() + length); } ValuesReader
             * dataReader = getDataReader(page.getValueEncoding(), bytes, page.getValueCount()); if (dlDecoder != null)
             * { readKeysWithNulls(keyDest, nullPlaceholder, numValues(), dlDecoder, dataReader); } else {
             * readKeysNonNulls(keyDest, numValues, dataReader); }
             */
            if (path.getMaxRepetitionLevel() != 0) {
                int length = bytes.getInt();
                rlDecoder = new RunLengthBitPackingHybridBufferDecoder(path.getMaxRepetitionLevel(),
                        bytes.slice().limit(length));
                bytes.position(bytes.position() + length);
            }
            if (path.getMaxDefinitionLevel() > 0) {
                int length = bytes.getInt();
                dlDecoder = new RunLengthBitPackingHybridBufferDecoder(path.getMaxDefinitionLevel(),
                        bytes.slice().limit(length));
                bytes.position(bytes.position() + length);
            }
            ValuesReader dataReader =
                    new KeyIndexReader((DictionaryValuesReader) getDataReader(page.getValueEncoding(),
                            bytes, page.getValueCount(), channelContext));
            Object result = materialize(PageMaterializer.IntFactory, dlDecoder, rlDecoder,
                    dataReader, nullPlaceholder);
            if (result instanceof DataWithOffsets) {
                keyDest.put((int[]) ((DataWithOffsets) result).materializeResult);
                return ((DataWithOffsets) result).offsets;
            }
            keyDest.put((int[]) result);
            return null;
        } catch (IOException e) {
            throw new ParquetDecodingException("could not read page " + page + " in col " + path,
                    e);
        }
    }

    private int readRepetitionLevels(ByteBuffer byteBuffer) throws IOException {
        RunLengthBitPackingHybridBufferDecoder rlDecoder;
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

    private Object readPageV1(DataPageV1 page, Object nullValue,
            @NotNull final SeekableChannelContext channelContext) {
        RunLengthBitPackingHybridBufferDecoder dlDecoder = null;
        try {
            // TODO - move away from page and use ByteBuffers directly
            ByteBuffer bytes = page.getBytes().toByteBuffer();
            bytes.order(ByteOrder.LITTLE_ENDIAN);
            RunLengthBitPackingHybridBufferDecoder rlDecoder = null;
            if (path.getMaxRepetitionLevel() != 0) {
                int length = bytes.getInt();
                rlDecoder = new RunLengthBitPackingHybridBufferDecoder(path.getMaxRepetitionLevel(),
                        bytes.slice().limit(length));
                bytes.position(bytes.position() + length);
            }
            if (path.getMaxDefinitionLevel() > 0) {
                int length = bytes.getInt();
                dlDecoder = new RunLengthBitPackingHybridBufferDecoder(path.getMaxDefinitionLevel(),
                        bytes.slice().limit(length));
                bytes.position(bytes.position() + length);
            }
            ValuesReader dataReader =
                    getDataReader(page.getValueEncoding(), bytes, page.getValueCount(), channelContext);
            return materialize(pageMaterializerFactory, dlDecoder, rlDecoder,
                    dataReader, nullValue);
        } catch (IOException e) {
            throw new ParquetDecodingException("could not read page " + page + " in col " + path,
                    e);
        }
    }

    private Object materialize(PageMaterializerFactory factory,
            RunLengthBitPackingHybridBufferDecoder dlDecoder,
            RunLengthBitPackingHybridBufferDecoder rlDecoder, ValuesReader dataReader, Object nullValue)
            throws IOException {
        if (dlDecoder == null) {
            return materializeNonNull(factory, numValues, dataReader);
        } else {
            return materializeWithNulls(factory, dlDecoder, rlDecoder, dataReader, nullValue);
        }
    }

    private void readKeysFromPageV2(DataPageV2 page, IntBuffer keyDest, int nullPlaceholder,
            @NotNull final SeekableChannelContext channelContext)
            throws IOException {
        if (path.getMaxRepetitionLevel() > 0) {
            throw new RuntimeException("Repeating levels not supported");
        }
        RunLengthBitPackingHybridBufferDecoder dlDecoder = null;

        if (path.getMaxDefinitionLevel() > 0) {
            dlDecoder = new RunLengthBitPackingHybridBufferDecoder(path.getMaxDefinitionLevel(),
                    page.getDefinitionLevels().toByteBuffer());
        }
        // LOG.debug("page data size {} bytes and {} records", page.getData().size(),
        // page.getValueCount());
        try {
            ValuesReader dataReader = getDataReader(page.getDataEncoding(),
                    page.getData().toByteBuffer(), page.getValueCount(), channelContext);
            if (dlDecoder != null) {
                readKeysWithNulls(keyDest, nullPlaceholder, dlDecoder, dataReader);
            } else {
                readKeysNonNulls(keyDest, dataReader);
            }
        } catch (IOException e) {
            throw new ParquetDecodingException("could not read page " + page + " in col " + path,
                    e);
        }
    }


    private Object readPageV2(DataPageV2 page, Object nullValue) throws IOException {
        throw new UnsupportedOperationException("Parquet V2 data pages are not supported");
    }

    private void readKeysWithNulls(IntBuffer keysBuffer, int nullPlaceholder,
            RunLengthBitPackingHybridBufferDecoder dlDecoder, ValuesReader dataReader)
            throws IOException {
        DictionaryValuesReader dictionaryValuesReader = (DictionaryValuesReader) dataReader;
        int startIndex = 0;
        while (dlDecoder.hasNext() && startIndex < numValues) {
            dlDecoder.readNextRange();
            int count = dlDecoder.currentRangeCount();
            int endIndex = Math.min(startIndex + count, numValues);
            if (dlDecoder.isNullRange()) {
                for (int i = startIndex; i < endIndex; i++) {
                    keysBuffer.put(nullPlaceholder);
                }
            } else {
                for (int i = startIndex; i < endIndex; i++) {
                    keysBuffer.put(dictionaryValuesReader.readValueDictionaryId());
                }
            }

            startIndex = endIndex;
        }
    }

    private void readKeysNonNulls(IntBuffer keysBuffer, ValuesReader dataReader)
            throws IOException {
        DictionaryValuesReader dictionaryValuesReader = (DictionaryValuesReader) dataReader;
        for (int i = 0; i < numValues; i++) {
            keysBuffer.put(dictionaryValuesReader.readValueDictionaryId());
        }
    }

    private static Object materializeWithNulls(PageMaterializerFactory factory,
            int numberOfValues,
            IntBuffer nullOffsets,
            ValuesReader dataReader, Object nullValue) {
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
    private static IntBuffer combineOptionalAndRepeating(IntBuffer nullOffsets, IntBuffer repeatingRanges,
            int nullValue) {
        IntBuffer result = IntBuffer.allocate(nullOffsets.limit() + repeatingRanges.limit());
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

    private Object materializeWithNulls(PageMaterializerFactory factory,
            RunLengthBitPackingHybridBufferDecoder dlDecoder,
            RunLengthBitPackingHybridBufferDecoder rlDecoder, ValuesReader dataReader, Object nullValue)
            throws IOException {
        Pair<Pair<Type.Repetition, IntBuffer>[], Integer> offsetsAndCount = getOffsetsAndNulls(dlDecoder, rlDecoder);
        int updatedNumValues = offsetsAndCount.second;
        Pair<Type.Repetition, IntBuffer>[] offsetAndNulls = offsetsAndCount.first;
        List<IntBuffer> offsetsWithNull = new ArrayList<>();
        IntBuffer currentNullOffsets = null;
        for (Pair<Type.Repetition, IntBuffer> offsetAndNull : offsetAndNulls) {
            if (offsetAndNull.first == Type.Repetition.OPTIONAL) {
                if (currentNullOffsets != null) {
                    throw new UnsupportedOperationException("Nested optional levels not supported");
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
        Object values;
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

    private static Object materializeNonNull(PageMaterializerFactory factory, int numberOfValues,
            ValuesReader dataReader) {
        return factory.makeMaterializerNonNull(dataReader, numberOfValues).fillAll();
    }

    private ValuesReader getDataReader(Encoding dataEncoding, ByteBuffer in, int valueCount,
            @NotNull final SeekableChannelContext channelContext) {
        if (dataEncoding == Encoding.DELTA_BYTE_ARRAY) {
            throw new RuntimeException("DELTA_BYTE_ARRAY encoding not supported");
        }
        ValuesReader dataReader;
        if (dataEncoding.usesDictionary()) {
            final Dictionary dictionary = dictionarySupplier.apply(channelContext);
            if (dictionary == ColumnChunkReader.NULL_DICTIONARY) {
                throw new ParquetDecodingException("Could not read page in col " + path + " as the dictionary was " +
                        "missing for encoding " + dataEncoding);
            }
            dataReader = new DictionaryValuesReader(dictionary);
        } else {
            dataReader = dataEncoding.getValuesReader(path, VALUES);
        }

        try {
            dataReader.initFromPage(valueCount, ByteBufferInputStream.wrap(in));
        } catch (IOException e) {
            throw new ParquetDecodingException("Could not read page in col " + path, e);
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
    public void close() throws Exception {

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
            RunLengthBitPackingHybridBufferDecoder dlDecoder,
            RunLengthBitPackingHybridBufferDecoder rlDecoder) throws IOException {
        dlDecoder.readNextRange();
        if (rlDecoder != null) {
            rlDecoder.readNextRange();
        }
        int dlRangeSize = dlDecoder.currentRangeCount();
        int currentDl = dlDecoder.currentValue();
        int rlRangeSize = rlDecoder == null ? numValues : rlDecoder.currentRangeCount();
        int currentRl = rlDecoder == null ? 0 : rlDecoder.currentValue();

        LevelsController levelsController = new LevelsController(
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
            int currentRangeSize = Math.min(dlRangeSize, rlRangeSize);
            dlRangeSize -= currentRangeSize;
            rlRangeSize -= currentRangeSize;
            levelsController.addElements(currentRl, currentDl, currentRangeSize);
            valuesProcessed += currentRangeSize;
        }
        return levelsController.getFinalState();
    }
}
