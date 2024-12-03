//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import com.google.common.io.CountingOutputStream;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import io.deephaven.parquet.compress.CompressorAdapter;
import io.deephaven.util.QueryConstants;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.format.*;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.EnumSet;
import java.util.Set;

import static org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt;
import static org.apache.parquet.format.Util.writePageHeader;

final class ColumnWriterImpl implements ColumnWriter {

    private static final int MIN_SLAB_SIZE = 64;

    private final CountingOutputStream countingOutput;
    private final ColumnDescriptor column;
    private final RowGroupWriterImpl owner;
    private final CompressorAdapter compressorAdapter;
    private boolean hasDictionary;
    private int pageCount = 0;
    private Statistics<?> statistics;
    private static final ParquetMetadataConverter metadataConverter = new ParquetMetadataConverter();


    private BulkWriter bulkWriter;
    private final int targetPageSize;
    private final ByteBufferAllocator allocator;
    private final RunLengthBitPackingHybridEncoder dlEncoder;
    private final RunLengthBitPackingHybridEncoder rlEncoder;
    private long dictionaryOffset = -1;
    // The downstream writing code (ParquetFileWriter) seems to respect the traversal order of this set. As such, to
    // improve determinism, we are using an EnumSet.
    private final Set<Encoding> encodings = EnumSet.noneOf(Encoding.class);
    private long firstDataPageOffset = -1;
    private long uncompressedLength;
    private long compressedLength;
    private long totalValueCount;
    private DictionaryPageHeader dictionaryPage;
    private final OffsetIndexBuilder offsetIndexBuilder;

    private final EncodingStats.Builder encodingStatsBuilder = new EncodingStats.Builder();

    ColumnWriterImpl(
            final RowGroupWriterImpl owner,
            final CountingOutputStream countingOutput,
            final ColumnDescriptor column,
            final CompressorAdapter compressorAdapter,
            final int targetPageSize,
            final ByteBufferAllocator allocator) {
        this.countingOutput = countingOutput;
        this.column = column;
        this.compressorAdapter = compressorAdapter;
        this.targetPageSize = targetPageSize;
        this.allocator = allocator;
        dlEncoder = column.getMaxDefinitionLevel() == 0 ? null
                : new RunLengthBitPackingHybridEncoder(
                        getWidthFromMaxInt(column.getMaxDefinitionLevel()), MIN_SLAB_SIZE, targetPageSize, allocator);
        rlEncoder = column.getMaxRepetitionLevel() == 0 ? null
                : new RunLengthBitPackingHybridEncoder(
                        getWidthFromMaxInt(column.getMaxRepetitionLevel()), MIN_SLAB_SIZE, targetPageSize, allocator);
        this.owner = owner;
        offsetIndexBuilder = OffsetIndexBuilder.getBuilder();
        statistics = Statistics.createStats(column.getPrimitiveType());
    }

    @Override
    public void addPageNoNulls(@NotNull final Object pageData,
            final int valuesCount,
            @NotNull final Statistics<?> statistics)
            throws IOException {
        initWriter();
        // noinspection unchecked
        bulkWriter.writeBulk(pageData, valuesCount, statistics);
        if (dlEncoder != null) {
            for (int i = 0; i < valuesCount; i++) {
                dlEncoder.writeInt(1); // TODO implement a bulk RLE writer
            }
        }
        writePage(bulkWriter.getByteBufferView(), valuesCount, valuesCount);
        bulkWriter.reset();
    }

    private void initWriter() {
        if (bulkWriter == null) {
            if (hasDictionary) {
                bulkWriter = new RleIntChunkedWriter(targetPageSize, allocator,
                        (byte) (32 - Integer.numberOfLeadingZeros(dictionaryPage.num_values)));
            } else {
                bulkWriter = getWriter(column.getPrimitiveType());
            }
        } else {
            bulkWriter.reset();
        }
    }

    @Override
    public void addDictionaryPage(@NotNull final Object dictionaryValues, final int valuesCount) throws IOException {
        if (pageCount > 0) {
            throw new IllegalStateException("Attempting to add dictionary past the first page");
        }

        encodingStatsBuilder.addDictEncoding(org.apache.parquet.column.Encoding.PLAIN);

        // noinspection rawtypes
        final BulkWriter dictionaryWriter = getWriter(column.getPrimitiveType());

        // noinspection unchecked
        dictionaryWriter.writeBulk(dictionaryValues, valuesCount, NullStatistics.INSTANCE);
        dictionaryOffset = countingOutput.getCount();
        writeDictionaryPage(dictionaryWriter.getByteBufferView(), valuesCount);
        pageCount++;
        hasDictionary = true;
        dictionaryPage = new DictionaryPageHeader(valuesCount, org.apache.parquet.format.Encoding.PLAIN);
    }

    private void writeDictionaryPage(final ByteBuffer dictionaryBuffer, final int valuesCount) throws IOException {
        final long currentChunkDictionaryPageOffset = countingOutput.getCount();
        final int uncompressedSize = dictionaryBuffer.remaining();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final WritableByteChannel channel = Channels.newChannel(compressorAdapter.compress(baos))) {
            channel.write(dictionaryBuffer);
        }
        compressorAdapter.reset();
        final BytesInput compressedBytes = BytesInput.from(baos);

        final int compressedPageSize = (int) compressedBytes.size();

        metadataConverter.writeDictionaryPageHeader(
                uncompressedSize,
                compressedPageSize,
                valuesCount,
                Encoding.PLAIN,
                countingOutput);
        final long headerSize = countingOutput.getCount() - currentChunkDictionaryPageOffset;
        this.uncompressedLength += uncompressedSize + headerSize;
        this.compressedLength += compressedPageSize + headerSize;
        compressedBytes.writeAllTo(countingOutput);
        encodings.add(Encoding.PLAIN);
    }

    private BulkWriter getWriter(final PrimitiveType primitiveType) {
        switch (primitiveType.getPrimitiveTypeName()) {
            case INT96:
            case FIXED_LEN_BYTE_ARRAY:
                throw new UnsupportedOperationException("No support for writing FIXED_LENGTH or INT96 types");
            case INT32:
                final LogicalTypeAnnotation annotation = primitiveType.getLogicalTypeAnnotation();
                if (annotation != null) {
                    // Appropriately set the null value for different type of integers
                    if (LogicalTypeAnnotation.intType(8, true).equals(annotation)) {
                        return new PlainIntChunkedWriter(targetPageSize, allocator, QueryConstants.NULL_BYTE);
                    } else if (LogicalTypeAnnotation.intType(16, true).equals(annotation)) {
                        return new PlainIntChunkedWriter(targetPageSize, allocator, QueryConstants.NULL_SHORT);
                    } else if (LogicalTypeAnnotation.intType(16, false).equals(annotation)) {
                        return new PlainIntChunkedWriter(targetPageSize, allocator, QueryConstants.NULL_CHAR);
                    }
                }
                return new PlainIntChunkedWriter(targetPageSize, allocator);
            case INT64:
                return new PlainLongChunkedWriter(targetPageSize, allocator);
            case FLOAT:
                return new PlainFloatChunkedWriter(targetPageSize, allocator);
            case DOUBLE:
                return new PlainDoubleChunkedWriter(targetPageSize, allocator);
            case BINARY:
                return new PlainBinaryChunkedWriter(targetPageSize, allocator);
            case BOOLEAN:
                return new PlainBooleanChunkedWriter();
            default:
                throw new UnsupportedOperationException("Unknown type " + primitiveType.getPrimitiveTypeName());
        }

    }

    @Override
    public void addPage(@NotNull final Object pageData,
            final int valuesCount,
            @NotNull Statistics<?> statistics)
            throws IOException {
        if (dlEncoder == null) {
            throw new IllegalStateException("Null values not supported");
        }
        initWriter();
        // noinspection unchecked
        bulkWriter.writeBulkFilterNulls(pageData, dlEncoder, valuesCount, statistics);
        writePage(bulkWriter.getByteBufferView(), valuesCount, valuesCount);
        bulkWriter.reset();
    }

    public void addVectorPage(
            @NotNull final Object pageData,
            @NotNull final IntBuffer repeatCount,
            final int nonNullValueCount,
            @NotNull final Statistics<?> statistics) throws IOException {
        if (dlEncoder == null) {
            throw new IllegalStateException("Null values not supported");
        }
        if (rlEncoder == null) {
            throw new IllegalStateException("Repeating values not supported");
        }
        initWriter();
        // noinspection unchecked
        final int valueCount =
                bulkWriter.writeBulkVector(pageData, repeatCount, rlEncoder, dlEncoder, nonNullValueCount, statistics);
        writePage(bulkWriter.getByteBufferView(), valueCount, repeatCount.limit());
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
        final DataPageHeaderV2 dataPageHeaderV2 = new DataPageHeaderV2(
                valueCount, nullCount, rowCount,
                hasDictionary ? org.apache.parquet.format.Encoding.PLAIN_DICTIONARY
                        : org.apache.parquet.format.Encoding.PLAIN,
                dlByteLength, rlByteLength);
        final PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE_V2, uncompressedSize, compressedSize);
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
        final int rlByteLength = (int) repetitionLevels.size();
        final int dlByteLength = (int) definitionLevels.size();
        final int uncompressedDataSize = data.remaining();
        final int uncompressedSize = (int) (uncompressedDataSize + repetitionLevels.size() + definitionLevels.size());

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final WritableByteChannel channel = Channels.newChannel(compressorAdapter.compress(baos))) {
            channel.write(data);
        }
        compressorAdapter.reset();
        final BytesInput compressedData = BytesInput.from(baos);
        final int compressedSize = (int) (compressedData.size() + repetitionLevels.size() + definitionLevels.size());

        final long initialOffset = countingOutput.getCount();
        if (firstDataPageOffset == -1) {
            firstDataPageOffset = initialOffset;
        }
        writeDataPageV2Header(
                uncompressedSize, compressedSize,
                valueCount, nullCount, rowCount,
                rlByteLength,
                dlByteLength,
                countingOutput);
        final long headerSize = countingOutput.getCount() - initialOffset;
        this.uncompressedLength += (uncompressedSize + headerSize);
        this.compressedLength += (compressedSize + headerSize);
        this.totalValueCount += valueCount;
        this.pageCount += 1;

        definitionLevels.writeAllTo(countingOutput);
        compressedData.writeAllTo(countingOutput);
    }

    private void writePage(final BytesInput bytes, final int valueCount, final long rowCount,
            final Encoding valuesEncoding) throws IOException {
        final long initialOffset = countingOutput.getCount();
        if (firstDataPageOffset == -1) {
            firstDataPageOffset = initialOffset;
        }

        final long uncompressedSize = bytes.size();
        if (uncompressedSize > Integer.MAX_VALUE) {
            throw new ParquetEncodingException(
                    "Cannot write page larger than Integer.MAX_VALUE bytes: " +
                            uncompressedSize);
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final OutputStream cos = compressorAdapter.compress(baos)) {
            bytes.writeAllTo(cos);
        }
        compressorAdapter.reset();
        final BytesInput compressedBytes = BytesInput.from(baos);

        final long compressedSize = compressedBytes.size();
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
                countingOutput);
        final long headerSize = countingOutput.getCount() - initialOffset;
        this.uncompressedLength += (uncompressedSize + headerSize);
        this.compressedLength += (compressedSize + headerSize);
        this.totalValueCount += valueCount;
        this.pageCount += 1;

        compressedBytes.writeAllTo(countingOutput);
        offsetIndexBuilder.add((int) (countingOutput.getCount() - initialOffset), rowCount);
        encodings.add(valuesEncoding);
        encodingStatsBuilder.addDataEncoding(valuesEncoding);
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

    private static PageHeader newDataPageHeader(
            final int uncompressedSize,
            final int compressedSize,
            final int valueCount,
            final Encoding valuesEncoding) {
        final PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE, uncompressedSize, compressedSize);

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
     * @param valueCount how many values have been written so far
     * @param rowCount how many rows have been written so far, can be different from valueCount for vector/arrays
     */
    private void writePage(final ByteBuffer encodedData, final long valueCount, final long rowCount) {
        try {
            BytesInput bytes = BytesInput.from(encodedData);
            if (dlEncoder != null) {
                final BytesInput dlBytesInput = dlEncoder.toBytes();
                bytes = BytesInput.concat(BytesInput.fromInt((int) dlBytesInput.size()), dlBytesInput, bytes);
            }
            if (rlEncoder != null) {
                final BytesInput rlBytesInput = rlEncoder.toBytes();
                bytes = BytesInput.concat(BytesInput.fromInt((int) rlBytesInput.size()), rlBytesInput, bytes);
            }
            writePage(bytes, (int) valueCount, rowCount, hasDictionary ? Encoding.RLE_DICTIONARY : Encoding.PLAIN);
        } catch (IOException e) {
            throw new ParquetEncodingException("could not write page for " + column.getPath()[0], e);
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
                ColumnChunkMetaData.get(ColumnPath.get(column.getPath()),
                        column.getPrimitiveType(),
                        compressorAdapter.getCodecName(),
                        encodingStatsBuilder.build(),
                        encodings,
                        statistics,
                        firstDataPageOffset,
                        dictionaryOffset,
                        totalValueCount,
                        compressedLength,
                        uncompressedLength));

        // We do not call bout.close() because it closes the underlying writeChannel, and this class does not own the
        // writeChannel. Also, we are assuming that all the buffered data has already been flushed to the writeChannel.
    }

    public ColumnDescriptor getColumn() {
        return column;
    }

    OffsetIndex getOffsetIndex() {
        return offsetIndexBuilder.build(firstDataPageOffset);
    }

    @Override
    public void resetStats() {
        statistics = Statistics.createStats(column.getPrimitiveType());
    }

    @Override
    public Statistics<?> getStats() {
        return statistics;
    }
}
