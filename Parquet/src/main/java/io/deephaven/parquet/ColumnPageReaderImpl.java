package io.deephaven.parquet;

import io.deephaven.base.Pair;
import io.deephaven.parquet.utils.RunLenghBitPackingHybridBufferDecoder;
import io.deephaven.parquet.utils.SeekableChannelsProvider;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.dictionary.DictionaryValuesReader;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.deephaven.parquet.utils.Helpers.readFully;
import static org.apache.parquet.column.ValuesType.VALUES;

public class ColumnPageReaderImpl implements ColumnPageReader {

    private static final int MAX_HEADER = 8192;
    private static final int START_HEADER = 128;
    private static final int INITIAL_BUFFER_SIZE = 64;
    public static final int NULL_OFFSET = -1;
    private final SeekableChannelsProvider channelsProvider;
    private final List<Type> fieldTypes;
    private long offset;
    private PageHeader pageHeader;
    private final ThreadLocal<CompressionCodecFactory.BytesInputDecompressor> decompressor;

    private final ColumnDescriptor path;
    private final Dictionary dictionary;
    private final Path filePath;
    private int numValues;
    private int rowCount = -1;

    ColumnPageReaderImpl(SeekableChannelsProvider channelsProvider, long offset,
        PageHeader pageHeader,
        ThreadLocal<CompressionCodecFactory.BytesInputDecompressor> decompressor,
        ColumnDescriptor path,
        Dictionary dictionary, Path filePath, int numValues, List<Type> fieldTypes) {
        this.channelsProvider = channelsProvider;
        this.offset = offset;
        this.pageHeader = pageHeader;
        this.decompressor = decompressor;
        this.path = path;
        this.dictionary = dictionary;
        this.filePath = filePath;
        this.numValues = numValues;
        this.fieldTypes = fieldTypes;
    }

    @Override
    public Object materialize(Object nullValue) throws IOException {
        try (SeekableByteChannel file = channelsProvider.getReadChannel(filePath)) {
            file.position(offset);
            ensurePageHeader(file);
            return readDataPage(nullValue, file);
        }
    }

    public int readRowCount() throws IOException {
        try (SeekableByteChannel file = channelsProvider.getReadChannel(filePath)) {
            file.position(offset);
            ensurePageHeader(file);
            return readRowCountFromDataPage(file);
        }
    }


    @Override
    public IntBuffer readKeyValues(IntBuffer keyDest, int nullPlaceholder) throws IOException {
        try (SeekableByteChannel file = channelsProvider.getReadChannel(filePath)) {
            file.position(offset);
            ensurePageHeader(file);
            return readKeyFromDataPage(keyDest, nullPlaceholder, file);
        }
    }

    private synchronized void ensurePageHeader(SeekableByteChannel file) throws IOException {
        if (pageHeader == null) {
            offset = file.position();
            int maxHeader = START_HEADER;
            boolean success = true;
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

    private int readRowCountFromDataPage(SeekableByteChannel file) throws IOException {
        int uncompressedPageSize = pageHeader.getUncompressed_page_size();
        int compressedPageSize = pageHeader.getCompressed_page_size();

        switch (pageHeader.type) {
            case DATA_PAGE:
                ByteBuffer payload = readFully(file, compressedPageSize);
                DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
                BytesInput decompressedInput = decompressor.get()
                    .decompress(BytesInput.from(payload), pageHeader.getUncompressed_page_size());

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
        SeekableByteChannel file) throws IOException {
        int uncompressedPageSize = pageHeader.getUncompressed_page_size();
        int compressedPageSize = pageHeader.getCompressed_page_size();

        switch (pageHeader.type) {
            case DATA_PAGE:
                ByteBuffer payload = readFully(file, compressedPageSize);
                DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
                BytesInput decompressedInput = decompressor.get()
                    .decompress(BytesInput.from(payload), pageHeader.getUncompressed_page_size());

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
                ByteBuffer repetitionLevels =
                    readFully(file, dataHeaderV2.getRepetition_levels_byte_length());
                ByteBuffer definitionLevels =
                    readFully(file, dataHeaderV2.getDefinition_levels_byte_length());
                BytesInput data =
                    decompressor.get().decompress(BytesInput.from(readFully(file, dataSize)),
                        pageHeader.getUncompressed_page_size()
                            - dataHeaderV2.getRepetition_levels_byte_length()
                            - dataHeaderV2.getDefinition_levels_byte_length());
                readKeysFromPageV2(new DataPageV2(
                    dataHeaderV2.getNum_rows(),
                    dataHeaderV2.getNum_nulls(),
                    dataHeaderV2.getNum_values(),
                    BytesInput.from(repetitionLevels),
                    BytesInput.from(definitionLevels),
                    getEncoding(dataHeaderV2.getEncoding()),
                    data,
                    uncompressedPageSize,
                    null, // TODO in the future might want to pull in statistics,
                    false), keyDest, nullPlaceholder);
                return null;
            default:
                throw new IOException(String.format("Unexpecte page of type {} of size {}",
                    pageHeader.getType(), compressedPageSize));
        }
    }

    private Object readDataPage(Object nullValue, SeekableByteChannel file) throws IOException {
        int uncompressedPageSize = pageHeader.getUncompressed_page_size();
        int compressedPageSize = pageHeader.getCompressed_page_size();
        switch (pageHeader.type) {
            case DATA_PAGE:
                ByteBuffer payload = readFully(file, compressedPageSize);
                DataPageHeader dataHeaderV1 = pageHeader.getData_page_header();
                BytesInput decompressedInput = decompressor.get()
                    .decompress(BytesInput.from(payload), pageHeader.getUncompressed_page_size());

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
                ByteBuffer repetitionLevels =
                    readFully(file, dataHeaderV2.getRepetition_levels_byte_length());
                ByteBuffer definitionLevels =
                    readFully(file, dataHeaderV2.getDefinition_levels_byte_length());
                BytesInput data =
                    decompressor.get().decompress(BytesInput.from(readFully(file, dataSize)),
                        pageHeader.getUncompressed_page_size()
                            - dataHeaderV2.getRepetition_levels_byte_length()
                            - dataHeaderV2.getDefinition_levels_byte_length());
                return readPageV2(new DataPageV2(
                    dataHeaderV2.getNum_rows(),
                    dataHeaderV2.getNum_nulls(),
                    dataHeaderV2.getNum_values(),
                    BytesInput.from(repetitionLevels),
                    BytesInput.from(definitionLevels),
                    getEncoding(dataHeaderV2.getEncoding()),
                    data,
                    uncompressedPageSize,
                    null, // TODO in the future might want to pull in statistics,
                    false), nullValue);
            default:
                throw new IOException(String.format("Unexpecte page of type {} of size {}",
                    pageHeader.getType(), compressedPageSize));
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
                readRepetitionLevels((ByteBuffer) bytes.slice().limit(length));
                return rowCount;
            } else {
                return page.getValueCount();
            }
        } catch (IOException e) {
            throw new ParquetDecodingException("could not read page " + page + " in col " + path,
                e);
        }
    }

    private IntBuffer readKeysFromPageV1(DataPageV1 page, IntBuffer keyDest, int nullPlaceholder) {
        RunLenghBitPackingHybridBufferDecoder rlDecoder = null;
        RunLenghBitPackingHybridBufferDecoder dlDecoder = null;
        try {
            ByteBuffer bytes = page.getBytes().toByteBuffer(); // TODO - move away from page and use
                                                               // ByteBuffers directly
            bytes.order(ByteOrder.LITTLE_ENDIAN);
            /*
             * IntBuffer offsets = null; if (path.getMaxRepetitionLevel() != 0) { int length =
             * bytes.getInt(); offsets = readRepetitionLevels((ByteBuffer)
             * bytes.slice().limit(length), IntBuffer.allocate(INITIAL_BUFFER_SIZE));
             * bytes.position(bytes.position() + length); } if (path.getMaxDefinitionLevel() > 0) {
             * int length = bytes.getInt(); dlDecoder = new
             * RunLenghBitPackingHybridBufferDecoder(path.getMaxDefinitionLevel(), (ByteBuffer)
             * bytes.slice().limit(length)); bytes.position(bytes.position() + length); }
             * ValuesReader dataReader = getDataReader(page.getValueEncoding(), bytes,
             * page.getValueCount()); if (dlDecoder != null) { readKeysWithNulls(keyDest,
             * nullPlaceholder, numValues(), dlDecoder, dataReader); } else {
             * readKeysNonNulls(keyDest, numValues, dataReader); }
             */
            if (path.getMaxRepetitionLevel() != 0) {
                int length = bytes.getInt();
                rlDecoder = new RunLenghBitPackingHybridBufferDecoder(path.getMaxRepetitionLevel(),
                    (ByteBuffer) bytes.slice().limit(length));
                bytes.position(bytes.position() + length);
            }
            if (path.getMaxDefinitionLevel() > 0) {
                int length = bytes.getInt();
                dlDecoder = new RunLenghBitPackingHybridBufferDecoder(path.getMaxDefinitionLevel(),
                    (ByteBuffer) bytes.slice().limit(length));
                bytes.position(bytes.position() + length);
            }
            ValuesReader dataReader =
                new KeyIndexReader((DictionaryValuesReader) getDataReader(page.getValueEncoding(),
                    bytes, page.getValueCount()));
            Object result = materialize(PrimitiveType.PrimitiveTypeName.INT32, dlDecoder, rlDecoder,
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

    private void readRepetitionLevels(ByteBuffer byteBuffer) throws IOException {
        RunLenghBitPackingHybridBufferDecoder rlDecoder;
        rlDecoder =
            new RunLenghBitPackingHybridBufferDecoder(path.getMaxRepetitionLevel(), byteBuffer);
        rowCount = 0;
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
                rowCount += count;
            }
        }
    }

    private Object readPageV1(DataPageV1 page, Object nullValue) {
        RunLenghBitPackingHybridBufferDecoder dlDecoder = null;
        try {
            ByteBuffer bytes = page.getBytes().toByteBuffer(); // TODO - move away from page and use
                                                               // ByteBuffers directly
            bytes.order(ByteOrder.LITTLE_ENDIAN);
            RunLenghBitPackingHybridBufferDecoder rlDecoder = null;
            if (path.getMaxRepetitionLevel() != 0) {
                int length = bytes.getInt();
                rlDecoder = new RunLenghBitPackingHybridBufferDecoder(path.getMaxRepetitionLevel(),
                    (ByteBuffer) bytes.slice().limit(length));
                bytes.position(bytes.position() + length);
            }
            if (path.getMaxDefinitionLevel() > 0) {
                int length = bytes.getInt();
                dlDecoder = new RunLenghBitPackingHybridBufferDecoder(path.getMaxDefinitionLevel(),
                    (ByteBuffer) bytes.slice().limit(length));
                bytes.position(bytes.position() + length);
            }
            ValuesReader dataReader =
                getDataReader(page.getValueEncoding(), bytes, page.getValueCount());
            return materialize(path.getPrimitiveType().getPrimitiveTypeName(), dlDecoder, rlDecoder,
                dataReader, nullValue, numValues);
        } catch (IOException e) {
            throw new ParquetDecodingException("could not read page " + page + " in col " + path,
                e);
        }
    }

    private Object materialize(PrimitiveType.PrimitiveTypeName primitiveTypeName,
        RunLenghBitPackingHybridBufferDecoder dlDecoder,
        RunLenghBitPackingHybridBufferDecoder rlDecoder, ValuesReader dataReader, Object nullValue,
        int numValues) throws IOException {
        if (dlDecoder == null) {
            return materializeNonNull(numValues, primitiveTypeName, dataReader);
        } else {
            return materializeWithNulls(primitiveTypeName, dlDecoder, rlDecoder, dataReader,
                nullValue);
        }
    }

    private void readKeysFromPageV2(DataPageV2 page, IntBuffer keyDest, int nullPlaceholder)
        throws IOException {
        if (path.getMaxRepetitionLevel() > 0) {
            throw new RuntimeException("Repeating levels not supported");
        }
        RunLenghBitPackingHybridBufferDecoder dlDecoder = null;

        if (path.getMaxDefinitionLevel() > 0) {
            dlDecoder = new RunLenghBitPackingHybridBufferDecoder(path.getMaxDefinitionLevel(),
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
        RunLenghBitPackingHybridBufferDecoder dlDecoder, ValuesReader dataReader)
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

    interface MaterializerWithNulls {

        static MaterializerWithNulls forType(PrimitiveType.PrimitiveTypeName primitiveTypeName,
            ValuesReader dataReader, Object nullValue, int numValues) {
            switch (primitiveTypeName) {
                case INT32:
                    return new Int(dataReader, nullValue, numValues);
                case INT64:
                    return new Long(dataReader, nullValue, numValues);
                case FLOAT:
                    return new Float(dataReader, nullValue, numValues);
                case DOUBLE:
                    return new Double(dataReader, nullValue, numValues);
                case BOOLEAN:
                    return new Bool(dataReader, nullValue, numValues);
                case BINARY:
                case FIXED_LEN_BYTE_ARRAY:
                case INT96: {
                    return new Blob(dataReader, nullValue, numValues);
                }
                default:
                    throw new RuntimeException("Unexpected type name:" + primitiveTypeName);
            }
        }

        void fillNulls(int startIndex, int endIndex);

        void fillValues(int startIndex, int endIndex);

        Object data();

        class Int implements MaterializerWithNulls {

            final ValuesReader dataReader;

            final int nullValue;
            final int data[];

            Int(ValuesReader dataReader, Object nullValue, int numValues) {
                this.dataReader = dataReader;
                this.nullValue = (Integer) nullValue;
                this.data = new int[numValues];
            }

            @Override
            public void fillNulls(int startIndex, int endIndex) {

                Arrays.fill(data, startIndex, endIndex, nullValue);

            }

            @Override
            public void fillValues(int startIndex, int endIndex) {
                for (int i = startIndex; i < endIndex; i++) {
                    data[i] = dataReader.readInteger();
                }
            }

            @Override
            public Object data() {
                return data;
            }
        }

        class Long implements MaterializerWithNulls {

            final ValuesReader dataReader;

            final long nullValue;
            final long data[];

            Long(ValuesReader dataReader, Object nullValue, int numValues) {
                this.dataReader = dataReader;
                this.nullValue = (java.lang.Long) nullValue;
                this.data = new long[numValues];
            }

            @Override
            public void fillNulls(int startIndex, int endIndex) {
                Arrays.fill(data, startIndex, endIndex, nullValue);
            }

            @Override
            public void fillValues(int startIndex, int endIndex) {
                for (int i = startIndex; i < endIndex; i++) {
                    data[i] = dataReader.readLong();
                }
            }

            @Override
            public Object data() {
                return data;
            }
        }

        class Float implements MaterializerWithNulls {

            final ValuesReader dataReader;

            final float nullValue;
            final float data[];

            Float(ValuesReader dataReader, Object nullValue, int numValues) {
                this.dataReader = dataReader;
                this.nullValue = (java.lang.Float) nullValue;
                this.data = new float[numValues];
            }

            @Override
            public void fillNulls(int startIndex, int endIndex) {
                Arrays.fill(data, startIndex, endIndex, nullValue);
            }

            @Override
            public void fillValues(int startIndex, int endIndex) {
                for (int i = startIndex; i < endIndex; i++) {
                    data[i] = dataReader.readFloat();
                }
            }

            @Override
            public Object data() {
                return data;
            }
        }

        class Double implements MaterializerWithNulls {

            final ValuesReader dataReader;

            final double nullValue;
            final double data[];

            Double(ValuesReader dataReader, Object nullValue, int numValues) {
                this.dataReader = dataReader;
                this.nullValue = (java.lang.Double) nullValue;
                this.data = new double[numValues];
            }

            @Override
            public void fillNulls(int startIndex, int endIndex) {
                Arrays.fill(data, startIndex, endIndex, nullValue);
            }

            @Override
            public void fillValues(int startIndex, int endIndex) {
                for (int i = startIndex; i < endIndex; i++) {
                    data[i] = dataReader.readDouble();
                }
            }

            @Override
            public Object data() {
                return data;
            }
        }

        class Bool implements MaterializerWithNulls {

            final ValuesReader dataReader;

            final byte nullValue;
            final byte data[];

            Bool(ValuesReader dataReader, Object nullValue, int numValues) {
                this.dataReader = dataReader;
                this.nullValue = (Byte) nullValue;
                this.data = new byte[numValues];
            }

            @Override
            public void fillNulls(int startIndex, int endIndex) {
                Arrays.fill(data, startIndex, endIndex, nullValue);
            }

            @Override
            public void fillValues(int startIndex, int endIndex) {
                for (int i = startIndex; i < endIndex; i++) {
                    data[i] = (byte) (dataReader.readBoolean() ? 1 : 0);
                }
            }

            @Override
            public Object data() {
                return data;
            }
        }

        class Blob implements MaterializerWithNulls {

            final ValuesReader dataReader;

            final Binary nullValue;
            final Binary data[];

            Blob(ValuesReader dataReader, Object nullValue, int numValues) {
                this.dataReader = dataReader;
                this.nullValue = (Binary) nullValue;
                this.data = new Binary[numValues];
            }

            @Override
            public void fillNulls(int startIndex, int endIndex) {
                Arrays.fill(data, startIndex, endIndex, nullValue);
            }

            @Override
            public void fillValues(int startIndex, int endIndex) {
                for (int i = startIndex; i < endIndex; i++) {
                    data[i] = dataReader.readBytes();
                }
            }

            @Override
            public Object data() {
                return data;
            }
        }

    }

    private Object materializeWithNulls(int numValues,
        PrimitiveType.PrimitiveTypeName primitiveTypeName, IntBuffer nullOffsets,
        ValuesReader dataReader, Object nullValue) {
        MaterializerWithNulls materializer =
            MaterializerWithNulls.forType(primitiveTypeName, dataReader, nullValue, numValues);
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
     *
     * @param nullOffsets
     * @param repeatingRanges
     * @return
     */
    private IntBuffer combineOptionalAndRepeating(IntBuffer nullOffsets, IntBuffer repeatingRanges,
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

    private Object materializeWithNulls(PrimitiveType.PrimitiveTypeName primitiveTypeName,
        RunLenghBitPackingHybridBufferDecoder dlDecoder,
        RunLenghBitPackingHybridBufferDecoder rlDecoder, ValuesReader dataReader, Object nullValue)
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
        if (currentNullOffsets != null) {
            values = materializeWithNulls(numValues, primitiveTypeName, currentNullOffsets,
                dataReader, nullValue);
        } else {
            values = materializeNonNull(numValues, primitiveTypeName, dataReader);
        }
        if (offsetsWithNull.isEmpty()) {
            return values;
        }
        if (offsetsWithNull.size() == 1) {
            return new DataWithOffsets(offsetsWithNull.get(0), values);
        }
        return new DataWithMultiLevelOffsets(offsetsWithNull.toArray(new IntBuffer[0]), values);
    }


    private Object materializeNonNull(int numValues,
        PrimitiveType.PrimitiveTypeName primitiveTypeName, ValuesReader dataReader) {
        switch (primitiveTypeName) {
            case INT32:
                int[] intData = new int[numValues];
                for (int i = 0; i < intData.length; i++) {
                    intData[i] = dataReader.readInteger();
                }
                return intData;
            case INT64:
                long[] longData = new long[numValues];
                for (int i = 0; i < longData.length; i++) {
                    longData[i] = dataReader.readLong();
                }
                return longData;
            case FLOAT:
                float[] floatData = new float[numValues];
                for (int i = 0; i < floatData.length; i++) {
                    floatData[i] = dataReader.readFloat();
                }
                return floatData;
            case DOUBLE:
                double[] doubleData = new double[numValues];
                for (int i = 0; i < doubleData.length; i++) {
                    doubleData[i] = dataReader.readDouble();
                }
                return doubleData;
            case BOOLEAN:
                byte[] boolData = new byte[numValues];
                for (int i = 0; i < boolData.length; i++) {
                    boolData[i] = (byte) (dataReader.readBoolean() ? 1 : 0);
                }
                return boolData;
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
            case INT96:
                Binary[] binaryData = new Binary[numValues];
                for (int i = 0; i < binaryData.length; i++) {
                    binaryData[i] = dataReader.readBytes();
                }
                return binaryData;
            default:
                throw new RuntimeException("Unexpected type name:" + primitiveTypeName);
        }
    }


    private ValuesReader getDataReader(Encoding dataEncoding, ByteBuffer in, int valueCount) {
        if (dataEncoding == Encoding.DELTA_BYTE_ARRAY) {
            throw new RuntimeException("DELTA_BYTE_ARRAY encoding not supported");
        }
        ValuesReader dataReader;
        if (dataEncoding.usesDictionary()) {
            if (dictionary == null) {
                throw new ParquetDecodingException(
                    "could not read page in col " + path
                        + " as the dictionary was missing for encoding " + dataEncoding);
            }
            dataReader = new DictionaryValuesReader(dictionary);
        } else {
            dataReader = dataEncoding.getValuesReader(path, VALUES);
        }

        try {
            dataReader.initFromPage(valueCount, ByteBufferInputStream.wrap(in));
        } catch (IOException e) {
            throw new ParquetDecodingException("could not read page in col " + path, e);
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

    @Override
    public Dictionary getDictionary() {
        return dictionary;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public long numRows() throws IOException {
        if (path.getMaxRepetitionLevel() == 0) {
            return numValues();
        } else {
            if (rowCount == -1) {
                readRowCount();
            }
            return rowCount;
        }
    }


    private Pair<Pair<Type.Repetition, IntBuffer>[], Integer> getOffsetsAndNulls(
        RunLenghBitPackingHybridBufferDecoder dlDecoder,
        RunLenghBitPackingHybridBufferDecoder rlDecoder, int numValues) throws IOException {
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
/*
 * 0,3,1 1,3,2 1,2,1 1,3,2 0,1,1 0,0,1
 */
