/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.base;

import io.deephaven.base.Pair;
import io.deephaven.parquet.base.util.Helpers;
import io.deephaven.parquet.base.util.RunLengthBitPackingHybridBufferDecoder;
import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import io.deephaven.parquet.compress.Compressor;
import org.apache.commons.io.IOUtils;
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

import java.io.BufferedInputStream;
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

    private final SeekableChannelsProvider channelsProvider;
    private final Compressor compressor;
    private final Supplier<Dictionary> dictionarySupplier;
    private final PageMaterializer.Factory pageMaterializerFactory;
    private final ColumnDescriptor path;
    private final Path filePath;
    private final List<Type> fieldTypes;

    private long offset;
    private PageHeader pageHeader;
    private int numValues;
    private int rowCount = -1;

    ColumnPageReaderImpl(SeekableChannelsProvider channelsProvider,
            Compressor compressor,
            Supplier<Dictionary> dictionarySupplier,
            PageMaterializer.Factory materializerFactory,
            ColumnDescriptor path,
            Path filePath,
            List<Type> fieldTypes,
            long offset,
            PageHeader pageHeader,
            int numValues) {
        this.channelsProvider = channelsProvider;
        this.compressor = compressor;
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
            readChannel.position(offset);
            ensurePageHeader(readChannel);
            return readDataPage(nullValue, readChannel);
        }
    }

    public int readRowCount() throws IOException {
        try (final SeekableByteChannel readChannel = channelsProvider.getReadChannel(filePath)) {
            readChannel.position(offset);
            ensurePageHeader(readChannel);
            return readRowCountFromDataPage(readChannel);
        }
    }


    @Override
    public IntBuffer readKeyValues(IntBuffer keyDest, int nullPlaceholder) throws IOException {
        try (final SeekableByteChannel readChannel = channelsProvider.getReadChannel(filePath)) {
            readChannel.position(offset);
            ensurePageHeader(readChannel);
            return readKeyFromDataPage(keyDest, nullPlaceholder, readChannel);
        }
    }

    private synchronized void ensurePageHeader(SeekableByteChannel file) throws IOException {
        if (pageHeader == null) {
            offset = file.position();
            int maxHeader = START_HEADER;
            boolean success;
            do {
                ByteBuffer headerBuffer = ByteBuffer.allocate(maxHeader);
                file.read(headerBuffer);
                headerBuffer.flip();
                ByteBufferInputStream bufferedIS = ByteBufferInputStream.wrap(headerBuffer);
                try {
                    pageHeader = Util.readPageHeader(bufferedIS);
                    offset += bufferedIS.position();
                    success = true;
                } catch (IOException e) {
                    success = false;
                    if (maxHeader > MAX_HEADER) {
                        throw e;
                    }
                    maxHeader *= 2;
                    file.position(offset);
                }
            } while (!success);
            file.position(offset);
        }
    }

    private int readRowCountFromDataPage(ReadableByteChannel file) throws IOException {
        int uncompressedPageSize = pageHeader.getUncompressed_page_size();
        int compressedPageSize = pageHeader.getCompressed_page_size();

        switch (pageHeader.type) {
            case DATA_PAGE:
                final BytesInput decompressedInput =
                        compressor.decompress(Channels.newInputStream(file), compressedPageSize, uncompressedPageSize);

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
                        compressor.decompress(Channels.newInputStream(file), compressedPageSize, uncompressedPageSize);

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
                BytesInput data = compressor.decompress(Channels.newInputStream(file), dataSize, uncompressedPageSize
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
                        compressor.decompress(Channels.newInputStream(file), compressedPageSize, uncompressedPageSize);

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
                BytesInput data = compressor.decompress(Channels.newInputStream(file), dataSize,
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
                    dataReader, nullPlaceholder, numValues);
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
                    dataReader, nullValue, numValues);
        } catch (IOException e) {
            throw new ParquetDecodingException("could not read page " + page + " in col " + path,
                    e);
        }
    }

    private Object materialize(PageMaterializer.Factory factory,
            RunLengthBitPackingHybridBufferDecoder dlDecoder,
            RunLengthBitPackingHybridBufferDecoder rlDecoder, ValuesReader dataReader, Object nullValue,
            int numValues) throws IOException {
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
                readKeysWithNulls(keyDest, nullPlaceholder, numValues(), dlDecoder, dataReader);
            } else {
                readKeysNonNulls(keyDest, numValues, dataReader);
            }
        } catch (IOException e) {
            throw new ParquetDecodingException("could not read page " + page + " in col " + path,
                    e);
        }
    }


    private Object readPageV2(DataPageV2 page, Object nullValue) throws IOException {
        throw new UnsupportedOperationException("Parquet V2 data pages are not supported");
    }

    private void readKeysWithNulls(IntBuffer keysBuffer, int nullPlaceholder, int numValues,
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

    private void readKeysNonNulls(IntBuffer keysBuffer, int numValues, ValuesReader dataReader)
            throws IOException {
        DictionaryValuesReader dictionaryValuesReader = (DictionaryValuesReader) dataReader;
        for (int i = 0; i < numValues; i++) {
            keysBuffer.put(dictionaryValuesReader.readValueDictionaryId());
        }
    }

    private Object materializeWithNulls(PageMaterializer.Factory factory,
            int numValues,
            IntBuffer nullOffsets,
            ValuesReader dataReader, Object nullValue) {
        final PageMaterializer materializer = factory.makeMaterializerWithNulls(dataReader, nullValue, numValues);
        int startIndex = 0;
        int nextNullPos = nullOffsets.hasRemaining() ? nullOffsets.get() : numValues;
        while (startIndex < numValues) {
            int rangeEnd = startIndex;
            while (nextNullPos == rangeEnd && rangeEnd < numValues) {
                rangeEnd++;
                nextNullPos = nullOffsets.hasRemaining() ? nullOffsets.get() : numValues;
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
    private IntBuffer combineOptionalAndRepeating(IntBuffer nullOffsets, IntBuffer repeatingRanges, int nullValue) {
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
        Pair<Pair<Type.Repetition, IntBuffer>[], Integer> offsetsAndCount =
                getOffsetsAndNulls(dlDecoder, rlDecoder, numValues);
        int numValues = offsetsAndCount.second;
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
            values = materializeWithNulls(factory, numValues, currentNullOffsets,
                    dataReader, nullValue);
        } else {
            values = materializeNonNull(factory, numValues, dataReader);
        }
        if (offsetsWithNull.isEmpty()) {
            return values;
        }
        if (offsetsWithNull.size() == 1) {
            return new DataWithOffsets(offsetsWithNull.get(0), values);
        }
        return new DataWithMultiLevelOffsets(offsetsWithNull.toArray(new IntBuffer[0]), values);
    }

    private Object materializeNonNull(PageMaterializer.Factory factory, int numValues, ValuesReader dataReader) {
        return factory.makeMaterializerNonNull(dataReader, numValues).fillAll();
    }

    private ValuesReader getDataReader(Encoding dataEncoding, ByteBuffer in, int valueCount) {
        if (dataEncoding == Encoding.DELTA_BYTE_ARRAY) {
            throw new RuntimeException("DELTA_BYTE_ARRAY encoding not supported");
        }
        ValuesReader dataReader;
        if (dataEncoding.usesDictionary()) {
            final Dictionary dictionary = dictionarySupplier.get();
            if (dictionary == ColumnChunkReader.NULL_DICTIONARY) {
                throw new ParquetDecodingException(
                        "Could not read page in col " + path
                                + " as the dictionary was missing for encoding " + dataEncoding);
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
        switch (pageHeader.type) {
            case DATA_PAGE:
                return numValues = pageHeader.getData_page_header().getNum_values();
            case DATA_PAGE_V2:
                return numValues = pageHeader.getData_page_header_v2().getNum_values();
            default:
                throw new IOException(
                        String.format("Unexpected page of type {%s}", pageHeader.getType()));
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
            RunLengthBitPackingHybridBufferDecoder rlDecoder, int numValues) throws IOException {
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
