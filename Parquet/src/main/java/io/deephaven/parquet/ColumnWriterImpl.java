package io.deephaven.parquet;

import io.deephaven.parquet.tempfix.ParquetMetadataConverter;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.format.*;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.util.HashSet;
import java.util.Set;

import static org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt;
import static org.apache.parquet.format.Util.writePageHeader;

public class ColumnWriterImpl implements ColumnWriter {

    private static final int MIN_SLAB_SIZE = 64;
    private final SeekableByteChannel writeChannel;
    private final ColumnDescriptor column;
    private final RowGroupWriterImpl owner;
    private final CompressionCodecFactory.BytesInputCompressor compressor;
    private boolean hasDictionary;
    private int pageCount = 0;
    private static final ParquetMetadataConverter metadataConverter =
        new ParquetMetadataConverter();


    private BulkWriter bulkWriter;
    private final int pageSize;
    private final ByteBufferAllocator allocator;
    private final RunLengthBitPackingHybridEncoder dlEncoder;
    private final RunLengthBitPackingHybridEncoder rlEncoder;
    private long dictionaryOffset = -1;
    private final Set<Encoding> encodings = new HashSet<>();
    private long firstDataPageOffset = -1;
    private long uncompressedLength;
    private long compressedLength;
    private long totalValueCount;
    private DictionaryPageHeader dictionaryPage;
    private final OffsetIndexBuilder offsetIndexBuilder;

    ColumnWriterImpl(
        final RowGroupWriterImpl owner,
        final SeekableByteChannel writeChannel,
        final ColumnDescriptor column,
        final CompressionCodecFactory.BytesInputCompressor compressor,
        final int pageSize,
        final ByteBufferAllocator allocator) {
        this.writeChannel = writeChannel;
        this.column = column;
        this.compressor = compressor;
        this.pageSize = pageSize;
        this.allocator = allocator;
        dlEncoder = column.getMaxDefinitionLevel() == 0 ? null
            : new RunLengthBitPackingHybridEncoder(
                getWidthFromMaxInt(column.getMaxDefinitionLevel()), MIN_SLAB_SIZE, pageSize,
                allocator);
        rlEncoder = column.getMaxRepetitionLevel() == 0 ? null
            : new RunLengthBitPackingHybridEncoder(
                getWidthFromMaxInt(column.getMaxRepetitionLevel()), MIN_SLAB_SIZE, pageSize,
                allocator);
        this.owner = owner;
        offsetIndexBuilder = OffsetIndexBuilder.getBuilder();
    }

    @Override
    public void addPageNoNulls(final Object pageData, final int valuesCount) throws IOException {
        initWriter();
        // noinspection unchecked
        bulkWriter.writeBulk(pageData, valuesCount);
        if (dlEncoder != null) {
            for (int i = 0; i < valuesCount; i++) {
                dlEncoder.writeInt(1); // TODO implement a bulk RLE writer
            }
        }
        writePage(bulkWriter.getByteBufferView(), valuesCount);
        bulkWriter.reset();
    }

    private void initWriter() {
        if (bulkWriter == null) {
            if (hasDictionary) {
                bulkWriter = new RleIntChunkedWriter(pageSize, allocator,
                    (byte) (32 - Integer.numberOfLeadingZeros(dictionaryPage.num_values)));
            } else {
                bulkWriter = getWriter(column.getPrimitiveType());
            }
        } else {
            bulkWriter.reset();
        }
    }

    @Override
    public void addDictionaryPage(final Object dictionaryValues, final int valuesCount)
        throws IOException {
        if (pageCount > 0) {
            throw new RuntimeException("Attempting to add dictionary past the first page");
        }
        BulkWriter dictionaryWriter = getWriter(column.getPrimitiveType());
        // noinspection unchecked
        dictionaryWriter.writeBulk(dictionaryValues, valuesCount);
        dictionaryOffset = writeChannel.position();
        writeDictionaryPage(dictionaryWriter.getByteBufferView(), valuesCount);
        pageCount++;
        hasDictionary = true;
        dictionaryPage =
            new DictionaryPageHeader(valuesCount, org.apache.parquet.format.Encoding.PLAIN);

    }

    public void writeDictionaryPage(final ByteBuffer dictionaryBuffer, final int valuesCount)
        throws IOException {
        long currentChunkDictionaryPageOffset = writeChannel.position();
        int uncompressedSize = dictionaryBuffer.remaining();
        BytesInput compressedBytes = compressor.compress(BytesInput.from(dictionaryBuffer));

        int compressedPageSize = (int) compressedBytes.size();

        metadataConverter.writeDictionaryPageHeader(
            uncompressedSize,
            compressedPageSize,
            valuesCount,
            Encoding.PLAIN,
            Channels.newOutputStream(writeChannel));
        long headerSize = writeChannel.position() - currentChunkDictionaryPageOffset;
        this.uncompressedLength += uncompressedSize + headerSize;
        this.compressedLength += compressedPageSize + headerSize;
        writeChannel.write(compressedBytes.toByteBuffer());
        encodings.add(Encoding.PLAIN);
    }

    private BulkWriter getWriter(final PrimitiveType primitiveType) {
        switch (primitiveType.getPrimitiveTypeName()) {
            case INT96:
                return new PlainFixedLenChunkedWriter(pageSize, 12, allocator);
            case FIXED_LEN_BYTE_ARRAY:
                return new PlainFixedLenChunkedWriter(pageSize,
                    column.getPrimitiveType().getTypeLength(), allocator);
            case INT32:
                return new PlainIntChunkedWriter(pageSize, allocator);
            case INT64:
                return new PlainLongChunkedWriter(pageSize, allocator);
            case FLOAT:
                return new PlainFloatChunkedWriter(pageSize, allocator);
            case DOUBLE:
                return new PlainDoubleChunkedWriter(pageSize, allocator);
            case BINARY:
                return new PlainBinaryChunkedWriter(pageSize, allocator);
            case BOOLEAN:
                return new PlainBooleanChunkedWriter(pageSize, allocator);
            default:
                throw new UnsupportedOperationException(
                    "Unknown type " + primitiveType.getPrimitiveTypeName());
        }

    }

    @Override
    public void addPage(final Object pageData, final Object nullValues, final int valuesCount)
        throws IOException {
        if (dlEncoder == null) {
            throw new RuntimeException("Null values not supported");
        }
        initWriter();
        // noinspection unchecked
        bulkWriter.writeBulkFilterNulls(pageData, nullValues, dlEncoder, valuesCount);
        writePage(bulkWriter.getByteBufferView(), valuesCount);
        bulkWriter.reset();
    }

    public void addVectorPage(
        final Object pageData,
        final IntBuffer repeatCount,
        final int nonNullValueCount,
        final Object nullValue) throws IOException {
        if (dlEncoder == null) {
            throw new RuntimeException("Null values not supported");
        }
        if (rlEncoder == null) {
            throw new RuntimeException("Repeating values not supported");
        }
        initWriter();
        // noinspection unchecked
        int valueCount = bulkWriter.writeBulkVector(pageData, repeatCount, rlEncoder, dlEncoder,
            nonNullValueCount, nullValue);
        writePage(bulkWriter.getByteBufferView(), valueCount);
        bulkWriter.reset();

    }

    private void writeDataPageV2Header(
        final int uncompressedSize,
        final int compressedSize,
        final int valueCount,
        final int nullCount,
        final int rowCount,
        final int rlByteLength,
        final int dlByteLength,
        final OutputStream to) throws IOException {
        writePageHeader(
            newDataPageV2Header(
                uncompressedSize, compressedSize,
                valueCount, nullCount, rowCount,
                rlByteLength, dlByteLength),
            to);
    }

    private PageHeader newDataPageV2Header(
        final int uncompressedSize,
        final int compressedSize,
        final int valueCount,
        final int nullCount,
        final int rowCount,
        final int rlByteLength,
        final int dlByteLength) {
        // TODO: pageHeader.crc = ...;
        DataPageHeaderV2 dataPageHeaderV2 = new DataPageHeaderV2(
            valueCount, nullCount, rowCount,
            hasDictionary ? org.apache.parquet.format.Encoding.PLAIN_DICTIONARY
                : org.apache.parquet.format.Encoding.PLAIN,
            dlByteLength, rlByteLength);
        PageHeader pageHeader =
            new PageHeader(PageType.DATA_PAGE_V2, uncompressedSize, compressedSize);
        pageHeader.setData_page_header_v2(dataPageHeaderV2);
        if (hasDictionary) {
            pageHeader.setDictionary_page_header(dictionaryPage);
        }
        encodings.add(Encoding.valueOf(dataPageHeaderV2.encoding.name()));
        return pageHeader;
    }


    public void writePageV2(
        final int rowCount,
        final int nullCount,
        final int valueCount,
        final BytesInput repetitionLevels,
        final BytesInput definitionLevels,
        final ByteBuffer data) throws IOException {
        int rlByteLength = (int) repetitionLevels.size();
        int dlByteLength = (int) definitionLevels.size();
        int uncompressedDataSize = data.remaining();
        int uncompressedSize =
            (int) (uncompressedDataSize + repetitionLevels.size() + definitionLevels.size());

        BytesInput compressedData = compressor.compress(BytesInput.from(data));
        int compressedSize =
            (int) (compressedData.size() + repetitionLevels.size() + definitionLevels.size());

        long initialOffset = writeChannel.position();
        if (firstDataPageOffset == -1) {
            firstDataPageOffset = initialOffset;
        }
        writeDataPageV2Header(
            uncompressedSize, compressedSize,
            valueCount, nullCount, rowCount,
            rlByteLength,
            dlByteLength,
            Channels.newOutputStream(writeChannel));
        long headerSize = writeChannel.position() - initialOffset;
        this.uncompressedLength += (uncompressedSize + headerSize);
        this.compressedLength += (compressedSize + headerSize);
        this.totalValueCount += valueCount;
        this.pageCount += 1;

        writeChannel.write(definitionLevels.toByteBuffer());
        writeChannel.write(compressedData.toByteBuffer());
    }

    private void writePage(final BytesInput bytes, final int valueCount,
        final Encoding valuesEncoding) throws IOException {
        long initialOffset = writeChannel.position();
        if (firstDataPageOffset == -1) {
            firstDataPageOffset = initialOffset;
        }

        long uncompressedSize = bytes.size();
        if (uncompressedSize > Integer.MAX_VALUE) {
            throw new ParquetEncodingException(
                "Cannot write page larger than Integer.MAX_VALUE bytes: " +
                    uncompressedSize);
        }
        BytesInput compressedBytes = compressor.compress(bytes);
        long compressedSize = compressedBytes.size();
        if (compressedSize > Integer.MAX_VALUE) {
            throw new ParquetEncodingException(
                "Cannot write compressed page larger than Integer.MAX_VALUE bytes: "
                    + compressedSize);
        }
        writeDataPageV1Header(
            (int) uncompressedSize,
            (int) compressedSize,
            valueCount,
            valuesEncoding,
            Channels.newOutputStream(writeChannel));
        long headerSize = writeChannel.position() - initialOffset;
        this.uncompressedLength += (uncompressedSize + headerSize);
        this.compressedLength += (compressedSize + headerSize);
        this.totalValueCount += valueCount;
        this.pageCount += 1;

        writeChannel.write(compressedBytes.toByteBuffer());
        offsetIndexBuilder.add((int) (writeChannel.position() - initialOffset), valueCount);

    }

    private void writeDataPageV1Header(
        final int uncompressedSize,
        final int compressedSize,
        final int valueCount,
        final Encoding valuesEncoding,
        final OutputStream to) throws IOException {
        writePageHeader(newDataPageHeader(uncompressedSize,
            compressedSize,
            valueCount,
            valuesEncoding), to);
    }

    private PageHeader newDataPageHeader(
        final int uncompressedSize,
        final int compressedSize,
        final int valueCount,
        final Encoding valuesEncoding) {
        PageHeader pageHeader =
            new PageHeader(PageType.DATA_PAGE, uncompressedSize, compressedSize);

        pageHeader.setData_page_header(new DataPageHeader(
            valueCount,
            org.apache.parquet.format.Encoding.valueOf(valuesEncoding.name()),
            org.apache.parquet.format.Encoding.valueOf(Encoding.RLE.name()),
            org.apache.parquet.format.Encoding.valueOf(Encoding.RLE.name())));
        return pageHeader;
    }

    /**
     * writes the current data to a new page in the page store
     *
     * @param valueCount how many rows have been written so far
     */
    private void writePage(final ByteBuffer encodedData, final long valueCount) {
        try {
            BytesInput bytes = BytesInput.from(encodedData);
            if (dlEncoder != null) {
                BytesInput dlBytesInput = dlEncoder.toBytes();
                bytes = BytesInput.concat(BytesInput.fromInt((int) dlBytesInput.size()),
                    dlBytesInput, bytes);
            }
            if (rlEncoder != null) {
                BytesInput rlBytesInput = rlEncoder.toBytes();
                bytes = BytesInput.concat(BytesInput.fromInt((int) rlBytesInput.size()),
                    rlBytesInput, bytes);
            }
            writePage(
                bytes,
                (int) valueCount, hasDictionary ? Encoding.RLE_DICTIONARY : Encoding.PLAIN);
        } catch (IOException e) {
            throw new ParquetEncodingException("could not write page for " + column.getPath()[0],
                e);
        }
        if (dlEncoder != null) {
            dlEncoder.reset();
        }
        if (rlEncoder != null) {
            rlEncoder.reset();
        }
    }

    @Override
    public void close() {
        owner.releaseWriter(this,
            ColumnChunkMetaData.get(ColumnPath.get(column.getPath()), column.getPrimitiveType(),
                compressor.getCodecName(),
                null, encodings, Statistics.createStats(column.getPrimitiveType()),
                firstDataPageOffset, dictionaryOffset, totalValueCount, compressedLength,
                uncompressedLength));
    }

    public ColumnDescriptor getColumn() {
        return column;
    }

    public OffsetIndex getOffsetIndex() {
        return offsetIndexBuilder.build(firstDataPageOffset);
    }
}
