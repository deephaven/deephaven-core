/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.parquet.base.util.Helpers;
import io.deephaven.parquet.base.util.RunLengthBitPackingHybridBufferDecoder;
import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import io.deephaven.parquet.compress.CompressorAdapter;
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
import org.apache.parquet.format.Util;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.Type;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.parquet.column.ValuesType.VALUES;

public class ColumnPageReaderImpl implements ColumnPageReader {

    private static final int MAX_HEADER = 8192;
    private static final int START_HEADER = 128;
    public static final int NULL_OFFSET = -1;
    static final int NULL_NUM_VALUES = -1;

    private final SeekableChannelsProvider channelsProvider;
    private final CompressorAdapter compressorAdapter;
    private final Supplier<Dictionary> dictionarySupplier;
    private final PageMaterializer.Factory pageMaterializerFactory;
    private final ColumnDescriptor path;
    private final Path filePath;
    private final List<Type> fieldTypes;

    /**
     * Stores the offset from where the next byte should be read. Can be the offset of page header if
     * {@link #pageHeader} is {@code null}, else will be the offset of data.
     */
    private long offset;
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
     * @param filePath The path of the file.
     * @param fieldTypes The types of the fields in the column.
     * @param offset The offset for page header if supplied {@code pageHeader} is {@code null}. Else, the offset of data
     *        following the header in the page.
     * @param pageHeader The page header if it is already read from the file. Else, {@code null}.
     * @param numValues The number of values in the page if it is already read from the file. Else,
     *        {@value #NULL_NUM_VALUES}
     */
    ColumnPageReaderImpl(SeekableChannelsProvider channelsProvider,
            CompressorAdapter compressorAdapter,
            Supplier<Dictionary> dictionarySupplier,
            PageMaterializer.Factory materializerFactory,
            ColumnDescriptor path,
            Path filePath,
            List<Type> fieldTypes,
            long offset,
            PageHeader pageHeader,
            int numValues) {
        this.channelsProvider = channelsProvider;
        this.compressorAdapter = compressorAdapter;
        this.dictionarySupplier = dictionarySupplier;
        this.pageMaterializerFactory = materializerFactory;
        this.path = path;
        this.filePath = filePath;
        this.fieldTypes = fieldTypes;
        this.offset = offset;
        this.pageHeader = pageHeader;
        this.numValues = numValues;
    }

    @Override
    public Object materialize(Object nullValue) throws IOException {
        try (final SeekableByteChannel readChannel = channelsProvider.getReadChannel(filePath)) {
            ensurePageHeader(readChannel);
            return readDataPage(nullValue, readChannel);
        }
    }

    public int readRowCount() throws IOException {
        try (final SeekableByteChannel readChannel = channelsProvider.getReadChannel(filePath)) {
            ensurePageHeader(readChannel);
            return readRowCountFromDataPage(readChannel);
        }
    }


    @Override
    public IntBuffer readKeyValues(IntBuffer keyDest, int nullPlaceholder) throws IOException {
        try (final SeekableByteChannel readChannel = channelsProvider.getReadChannel(filePath)) {
            ensurePageHeader(readChannel);
            return readKeyFromDataPage(keyDest, nullPlaceholder, readChannel);
        }
    }

    /**
     * If {@link #pageHeader} is {@code null}, read it from the file, and increment the {@link #offset} by the length of
     * page header. Channel position would be set to the end of page header or beginning of data before returning.
     */
    private void ensurePageHeader(final SeekableByteChannel file) throws IOException {
        // Set this channel's position to appropriate offset for reading. If pageHeader is null, this offset would be
        // the offset of page header, else it would be the offset of data.
        file.position(offset);
        synchronized (this) {
            if (pageHeader == null) {
                int maxHeader = START_HEADER;
                boolean success;
                do {
                    final ByteBuffer headerBuffer = ByteBuffer.allocate(maxHeader);
                    file.read(headerBuffer);
                    headerBuffer.flip();
                    final ByteBufferInputStream bufferedIS = ByteBufferInputStream.wrap(headerBuffer);
                    try {
                        pageHeader = Util.readPageHeader(bufferedIS);
                        offset += bufferedIS.position();
                        success = true;
                    } catch (IOException e) {
                        success = false;
                        if (maxHeader > MAX_HEADER) {
                            throw e;
                        }
                        maxHeader <<= 1;
                        file.position(offset);
                    }
                } while (!success);
                file.position(offset);
                if (numValues >= 0) {
                    final int numValuesFromHeader = readNumValuesFromPageHeader(pageHeader);
                    if (numValues != numValuesFromHeader) {
                        throw new IllegalStateException(
                                "numValues = " + numValues + " different from number of values " +
                                        "read from the page header = " + numValuesFromHeader + " for column " + path);
                    }
                }
            }
            if (numValues == NULL_NUM_VALUES) {
                numValues = readNumValuesFromPageHeader(pageHeader);
            }
        }
    }

    private static int readNumValuesFromPageHeader(@NotNull final PageHeader header) throws IOException {
        switch (header.type) {
            case DATA_PAGE:
                return header.getData_page_header().getNum_values();
            case DATA_PAGE_V2:
                return header.getData_page_header_v2().getNum_values();
            default:
                throw new IOException(String.format("Unexpected page of type {%s}", header.getType()));
        }
    }

    private int readRowCountFromDataPage(ReadableByteChannel file) throws IOException {
        int uncompressedPageSize = pageHeader.getUncompressed_page_size();
        int compressedPageSize = pageHeader.getCompressed_page_size();

        switch (pageHeader.type) {
            case DATA_PAGE:
                final BytesInput decompressedInput =
                        compressorAdapter.decompress(Channels.newInputStream(file), compressedPageSize,
                                uncompressedPageSize);

                DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
                return readRowCountFromPageV1(new DataPageV1(
                        decompressedInput,
                        dataHeaderV1.getNum_values(),
                        uncompressedPageSize,
                        null, // TODO in the future might want to pull in statistics
                        getEncoding(dataHeaderV1.getRepetition_level_encoding()),
                        getEncoding(dataHeaderV1.getDefinition_level_encoding()),
                        getEncoding(dataHeaderV1.getEncoding())));
            case DATA_PAGE_V2:
                DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
                return dataHeaderV2.getNum_rows();
            default:
                throw new RuntimeException("Unsupported type" + pageHeader.type);
        }
    }

    private IntBuffer readKeyFromDataPage(IntBuffer keyDest, int nullPlaceholder,
            ReadableByteChannel file) throws IOException {
        int uncompressedPageSize = pageHeader.getUncompressed_page_size();
        int compressedPageSize = pageHeader.getCompressed_page_size();

        switch (pageHeader.type) {
            case DATA_PAGE:
                BytesInput decompressedInput =
                        compressorAdapter.decompress(Channels.newInputStream(file), compressedPageSize,
                                uncompressedPageSize);

                DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
                return readKeysFromPageV1(new DataPageV1(
                        decompressedInput,
                        dataHeaderV1.getNum_values(),
                        uncompressedPageSize,
                        null, // TODO in the future might want to pull in statistics
                        getEncoding(dataHeaderV1.getRepetition_level_encoding()),
                        getEncoding(dataHeaderV1.getDefinition_level_encoding()),
                        getEncoding(dataHeaderV1.getEncoding())), keyDest, nullPlaceholder);

            case DATA_PAGE_V2:
                DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
                int dataSize = compressedPageSize - dataHeaderV2.getRepetition_levels_byte_length()
                        - dataHeaderV2.getDefinition_levels_byte_length();
                BytesInput repetitionLevels =
                        Helpers.readBytes(file, dataHeaderV2.getRepetition_levels_byte_length());
                BytesInput definitionLevels =
                        Helpers.readBytes(file, dataHeaderV2.getDefinition_levels_byte_length());
                BytesInput data = compressorAdapter.decompress(Channels.newInputStream(file), dataSize,
                        uncompressedPageSize
                                - dataHeaderV2.getRepetition_levels_byte_length()
                                - dataHeaderV2.getDefinition_levels_byte_length());

                readKeysFromPageV2(new DataPageV2(
                        dataHeaderV2.getNum_rows(),
                        dataHeaderV2.getNum_nulls(),
                        dataHeaderV2.getNum_values(),
                        repetitionLevels,
                        definitionLevels,
                        getEncoding(dataHeaderV2.getEncoding()),
                        data,
                        uncompressedPageSize,
                        null, // TODO in the future might want to pull in statistics,
                        false), keyDest, nullPlaceholder);
                return null;
            default:
                throw new IOException(String.format("Unexpected page of type %s of size %d", pageHeader.getType(),
                        compressedPageSize));
        }
    }

    private Object readDataPage(Object nullValue, SeekableByteChannel file) throws IOException {
        final int uncompressedPageSize = pageHeader.getUncompressed_page_size();
        final int compressedPageSize = pageHeader.getCompressed_page_size();
        switch (pageHeader.type) {
            case DATA_PAGE:
                BytesInput decompressedInput =
                        compressorAdapter.decompress(Channels.newInputStream(file), compressedPageSize,
                                uncompressedPageSize);

                DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
                return readPageV1(new DataPageV1(
                        decompressedInput,
                        dataHeaderV1.getNum_values(),
                        uncompressedPageSize,
                        null, // TODO in the future might want to pull in statistics
                        getEncoding(dataHeaderV1.getRepetition_level_encoding()),
                        getEncoding(dataHeaderV1.getDefinition_level_encoding()),
                        getEncoding(dataHeaderV1.getEncoding())), nullValue);
            case DATA_PAGE_V2:
                DataPageHeaderV2 dataHeaderV2 = pageHeader.getData_page_header_v2();
                int dataSize = compressedPageSize - dataHeaderV2.getRepetition_levels_byte_length()
                        - dataHeaderV2.getDefinition_levels_byte_length();
                BytesInput repetitionLevels = Helpers.readBytes(file, dataHeaderV2.getRepetition_levels_byte_length());
                BytesInput definitionLevels = Helpers.readBytes(file, dataHeaderV2.getDefinition_levels_byte_length());
                BytesInput data = compressorAdapter.decompress(Channels.newInputStream(file), dataSize,
                        pageHeader.getUncompressed_page_size()
                                - dataHeaderV2.getRepetition_levels_byte_length()
                                - dataHeaderV2.getDefinition_levels_byte_length());

                return readPageV2(new DataPageV2(
                        dataHeaderV2.getNum_rows(),
                        dataHeaderV2.getNum_nulls(),
                        dataHeaderV2.getNum_values(),
                        repetitionLevels,
                        definitionLevels,
                        getEncoding(dataHeaderV2.getEncoding()),
                        data,
                        uncompressedPageSize,
                        null, // TODO in the future might want to pull in statistics,
                        false), nullValue);
            default:
                throw new IOException(String.format("Unexpected page of type %s of size %d", pageHeader.getType(),
                        compressedPageSize));
        }
    }

    private Encoding getEncoding(org.apache.parquet.format.Encoding encoding) {
        return org.apache.parquet.column.Encoding.valueOf(encoding.name());
    }

    private int readRowCountFromPageV1(DataPageV1 page) {
        try {
            ByteBuffer bytes = page.getBytes().toByteBuffer(); // TODO - move away from page and use
                                                               // ByteBuffers directly
            bytes.order(ByteOrder.LITTLE_ENDIAN);
            if (path.getMaxRepetitionLevel() != 0) {
                int length = bytes.getInt();
                return readRepetitionLevels(bytes.slice().limit(length));
            } else {
                return page.getValueCount();
            }
        } catch (IOException e) {
            throw new ParquetDecodingException("could not read page " + page + " in col " + path, e);
        }
    }

    private IntBuffer readKeysFromPageV1(DataPageV1 page, IntBuffer keyDest, int nullPlaceholder) {
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
                            bytes, page.getValueCount()));
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

    private Object readPageV1(DataPageV1 page, Object nullValue) {
        RunLengthBitPackingHybridBufferDecoder dlDecoder = null;
        try {
            ByteBuffer bytes = page.getBytes().toByteBuffer(); // TODO - move away from page and use
                                                               // ByteBuffers directly
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
                    getDataReader(page.getValueEncoding(), bytes, page.getValueCount());
            return materialize(pageMaterializerFactory, dlDecoder, rlDecoder,
                    dataReader, nullValue);
        } catch (IOException e) {
            throw new ParquetDecodingException("could not read page " + page + " in col " + path,
                    e);
        }
    }

    private Object materialize(PageMaterializer.Factory factory,
            RunLengthBitPackingHybridBufferDecoder dlDecoder,
            RunLengthBitPackingHybridBufferDecoder rlDecoder, ValuesReader dataReader, Object nullValue)
            throws IOException {
        if (dlDecoder == null) {
            return materializeNonNull(factory, numValues, dataReader);
        } else {
            return materializeWithNulls(factory, dlDecoder, rlDecoder, dataReader, nullValue);
        }
    }

    private void readKeysFromPageV2(DataPageV2 page, IntBuffer keyDest, int nullPlaceholder)
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
                    page.getData().toByteBuffer(), page.getValueCount());
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

    private static Object materializeWithNulls(PageMaterializer.Factory factory,
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

    private Object materializeWithNulls(PageMaterializer.Factory factory,
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

    private static Object materializeNonNull(PageMaterializer.Factory factory, int numberOfValues,
            ValuesReader dataReader) {
        return factory.makeMaterializerNonNull(dataReader, numberOfValues).fillAll();
    }

    private ValuesReader getDataReader(Encoding dataEncoding, ByteBuffer in, int valueCount) {
        if (dataEncoding == Encoding.DELTA_BYTE_ARRAY) {
            throw new RuntimeException("DELTA_BYTE_ARRAY encoding not supported");
        }
        ValuesReader dataReader;
        if (dataEncoding.usesDictionary()) {
            final Dictionary dictionary = dictionarySupplier.get();
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
    public int numValues() throws IOException {
        if (numValues >= 0) {
            return numValues;
        }
        try (final SeekableByteChannel readChannel = channelsProvider.getReadChannel(filePath)) {
            ensurePageHeader(readChannel);
            // Above will block till it populates numValues
            Assert.geqZero(numValues, "numValues");
            return numValues;
        }
    }

    @NotNull
    @Override
    public Dictionary getDictionary() {
        return dictionarySupplier.get();
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public long numRows() throws IOException {
        if (rowCount == -1) {
            if (path.getMaxRepetitionLevel() == 0) {
                rowCount = numValues();
            } else {
                rowCount = readRowCount();
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
