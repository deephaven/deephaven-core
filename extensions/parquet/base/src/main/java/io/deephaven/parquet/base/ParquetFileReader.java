//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.parquet.format.*;
import org.apache.parquet.format.ColumnOrder;
import org.apache.parquet.format.Type;
import org.apache.parquet.schema.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.deephaven.base.FileUtils.convertToURI;

/**
 * Top level accessor for a parquet file which can read both from a file path string or a CLI style file URI,
 * ex."s3://bucket/key".
 */
public class ParquetFileReader {
    private static final int FOOTER_LENGTH_SIZE = 4;
    private static final String MAGIC_STR = "PAR1";
    static final byte[] MAGIC = MAGIC_STR.getBytes(StandardCharsets.US_ASCII);
    public static final String FILE_URI_SCHEME = "file";

    public final FileMetaData fileMetaData;
    private final SeekableChannelsProvider channelsProvider;

    /**
     * If reading a single parquet file, root URI is the URI of the file, else the parent directory for a metadata file
     */
    private final URI rootURI;
    private final MessageType type;

    /**
     * Create a new ParquetFileReader for the provided source.
     *
     * @param source The source path or URI for the parquet file or the parquet metadata file
     * @param channelsProvider The {@link SeekableChannelsProvider} to use for reading the file
     */
    public ParquetFileReader(final String source, final SeekableChannelsProvider channelsProvider)
            throws IOException {
        this(convertToURI(source, false), channelsProvider);
    }

    /**
     * Create a new ParquetFileReader for the provided source.
     *
     * @param parquetFileURI The URI for the parquet file or the parquet metadata file
     * @param channelsProvider The {@link SeekableChannelsProvider} to use for reading the file
     */
    public ParquetFileReader(final URI parquetFileURI, final SeekableChannelsProvider channelsProvider)
            throws IOException {
        this.channelsProvider = channelsProvider;
        if (!parquetFileURI.getRawPath().endsWith(".parquet") && FILE_URI_SCHEME.equals(parquetFileURI.getScheme())) {
            // Construct a new file URI for the parent directory
            rootURI = convertToURI(new File(parquetFileURI).getParentFile(), true);
        } else {
            // TODO(deephaven-core#5066): Add support for reading metadata files from non-file URIs
            rootURI = parquetFileURI;
        }
        try (
                final SeekableChannelContext context = channelsProvider.makeContext();
                final SeekableByteChannel ch = channelsProvider.getReadChannel(context, parquetFileURI)) {
            positionToFileMetadata(parquetFileURI, ch);
            try (final InputStream in = channelsProvider.getInputStream(ch)) {
                fileMetaData = Util.readFileMetaData(in);
            }
        }
        type = fromParquetSchema(fileMetaData.schema, fileMetaData.column_orders);
    }

    private static void positionToFileMetadata(URI parquetFileURI, SeekableByteChannel readChannel) throws IOException {
        final long fileLen = readChannel.size();
        if (fileLen < MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) { // MAGIC + data + footer +
            // footerIndex + MAGIC
            throw new InvalidParquetFileException(
                    parquetFileURI + " is not a Parquet file (too small length: " + fileLen + ")");
        }
        final byte[] trailer = new byte[Integer.BYTES + MAGIC.length];
        final long footerLengthIndex = fileLen - FOOTER_LENGTH_SIZE - MAGIC.length;
        readChannel.position(footerLengthIndex);
        Helpers.readBytes(readChannel, trailer);
        if (!Arrays.equals(MAGIC, 0, MAGIC.length, trailer, Integer.BYTES, trailer.length)) {
            throw new InvalidParquetFileException(
                    parquetFileURI + " is not a Parquet file. expected magic number at tail " + Arrays.toString(MAGIC)
                            + " but found "
                            + Arrays.toString(Arrays.copyOfRange(trailer, Integer.BYTES, trailer.length)));
        }
        final int footerLength = makeLittleEndianInt(trailer[0], trailer[1], trailer[2], trailer[3]);
        final long footerIndex = footerLengthIndex - footerLength;
        if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
            throw new InvalidParquetFileException(
                    "corrupted file: the footer index is not within the file: " + footerIndex);
        }
        readChannel.position(footerIndex);
    }

    private static int makeLittleEndianInt(byte b0, byte b1, byte b2, byte b3) {
        return (b0 & 0xff) | ((b1 & 0xff) << 8) | ((b2 & 0xff) << 16) | ((b3 & 0xff) << 24);
    }

    /**
     * @return The {@link SeekableChannelsProvider} used for this reader, appropriate to use for related file access
     */
    public SeekableChannelsProvider getChannelsProvider() {
        return channelsProvider;
    }

    private Set<String> columnsWithDictionaryUsedOnEveryDataPage = null;

    /**
     * Get the name of all columns that we can know for certain (a) have a dictionary, and (b) use the dictionary on all
     * data pages.
     *
     * @return A set of parquet column names that satisfies the required condition.
     */
    @SuppressWarnings("unused")
    public Set<String> getColumnsWithDictionaryUsedOnEveryDataPage() {
        if (columnsWithDictionaryUsedOnEveryDataPage == null) {
            columnsWithDictionaryUsedOnEveryDataPage =
                    calculateColumnsWithDictionaryUsedOnEveryDataPage();
        }
        return columnsWithDictionaryUsedOnEveryDataPage;
    }

    /**
     * True only if we are certain every data page in this column chunk uses dictionary encoding; note false also covers
     * the "we can't tell" case.
     */
    private static boolean columnChunkUsesDictionaryOnEveryPage(final ColumnChunk columnChunk) {
        final ColumnMetaData columnMeta = columnChunk.getMeta_data();
        if (columnMeta.encoding_stats == null) {
            return false; // this is false as "don't know".
        }
        for (PageEncodingStats encodingStat : columnMeta.encoding_stats) {
            if (encodingStat.page_type != PageType.DATA_PAGE
                    && encodingStat.page_type != PageType.DATA_PAGE_V2) {
                // skip non-data pages.
                continue;
            }
            // this is a data page.
            if (encodingStat.encoding != Encoding.PLAIN_DICTIONARY
                    && encodingStat.encoding != Encoding.RLE_DICTIONARY) {
                return false;
            }
        }
        return true;
    }

    private Set<String> calculateColumnsWithDictionaryUsedOnEveryDataPage() {
        final Set<String> result = new HashSet<>(fileMetaData.getSchemaSize());
        final List<RowGroup> rowGroups = fileMetaData.getRow_groups();
        final Iterator<RowGroup> riter = rowGroups.iterator();
        if (!riter.hasNext()) {
            // For an empty file we say all columns satisfy the property.
            for (SchemaElement se : fileMetaData.getSchema()) {
                if (!se.isSetNum_children()) { // We want only the leaves.
                    result.add(se.getName());
                }
            }
            return result;
        }
        // On the first pass, for row group zero, we are going to add all columns to the set
        // that satisfy the restriction.
        // On later passes after zero, we will remove any column that does not satisfy
        // the restriction.
        final RowGroup rg0 = riter.next();
        for (ColumnChunk columnChunk : rg0.columns) {
            if (columnChunkUsesDictionaryOnEveryPage(columnChunk)) {
                final String parquetColumnName = columnChunk.getMeta_data().path_in_schema.get(0);
                result.add(parquetColumnName);
            }
        }

        while (riter.hasNext()) {
            final RowGroup rowGroup = riter.next();
            for (ColumnChunk columnChunk : rowGroup.columns) {
                final String parquetColumnName = columnChunk.getMeta_data().path_in_schema.get(0);
                if (!result.contains(parquetColumnName)) {
                    continue;
                }
                if (!columnChunkUsesDictionaryOnEveryPage(columnChunk)) {
                    result.remove(parquetColumnName);
                }
            }
        }
        return result;
    }

    /**
     * Create a {@link RowGroupReader} object for provided row group number
     * 
     * @param version The "version" string from deephaven specific parquet metadata, or null if it's not present.
     */
    public RowGroupReader getRowGroup(final int groupNumber, final String version) {
        return new RowGroupReaderImpl(
                fileMetaData.getRow_groups().get(groupNumber),
                channelsProvider,
                rootURI,
                type,
                getSchema(),
                version);
    }

    private static MessageType fromParquetSchema(List<SchemaElement> schema, List<ColumnOrder> columnOrders)
            throws ParquetFileReaderException {
        final Iterator<SchemaElement> iterator = schema.iterator();
        final SchemaElement root = iterator.next();
        final Types.MessageTypeBuilder builder = Types.buildMessage();
        if (root.isSetField_id()) {
            builder.id(root.field_id);
        }
        buildChildren(builder, iterator, root.getNum_children(), columnOrders, 0);
        return builder.named(root.name);
    }

    private static void buildChildren(Types.GroupBuilder builder, Iterator<SchemaElement> schema,
            int childrenCount, List<ColumnOrder> columnOrders, int columnCount) throws ParquetFileReaderException {
        for (int i = 0; i < childrenCount; ++i) {
            SchemaElement schemaElement = schema.next();
            Object childBuilder;
            if (schemaElement.type != null) {
                Types.PrimitiveBuilder primitiveBuilder =
                        builder.primitive(getPrimitive(schemaElement.type),
                                fromParquetRepetition(schemaElement.repetition_type));
                if (schemaElement.isSetType_length()) {
                    primitiveBuilder.length(schemaElement.type_length);
                }

                if (schemaElement.isSetPrecision()) {
                    primitiveBuilder.precision(schemaElement.precision);
                }

                if (schemaElement.isSetScale()) {
                    primitiveBuilder.scale(schemaElement.scale);
                }

                if (columnOrders != null) {
                    org.apache.parquet.schema.ColumnOrder columnOrder =
                            fromParquetColumnOrder(columnOrders.get(columnCount));
                    if (columnOrder
                            .getColumnOrderName() == org.apache.parquet.schema.ColumnOrder.ColumnOrderName.TYPE_DEFINED_ORDER
                            && (schemaElement.type == Type.INT96
                                    || schemaElement.converted_type == ConvertedType.INTERVAL)) {
                        columnOrder = org.apache.parquet.schema.ColumnOrder.undefined();
                    }

                    primitiveBuilder.columnOrder(columnOrder);
                }

                childBuilder = primitiveBuilder;
            } else {
                childBuilder = builder.group(fromParquetRepetition(schemaElement.repetition_type));
                buildChildren((Types.GroupBuilder) childBuilder, schema, schemaElement.num_children,
                        columnOrders, columnCount);
            }

            if (schemaElement.isSetLogicalType()) {
                ((Types.Builder) childBuilder).as(getLogicalTypeAnnotation(schemaElement.logicalType));
            }

            if (schemaElement.isSetConverted_type()) {
                final LogicalTypeAnnotation originalType = getLogicalTypeAnnotation(
                        schemaElement.converted_type, schemaElement.logicalType, schemaElement);
                final LogicalTypeAnnotation newOriginalType = schemaElement.isSetLogicalType()
                        && getLogicalTypeAnnotation(schemaElement.logicalType) != null
                                ? getLogicalTypeAnnotation(schemaElement.logicalType)
                                : null;
                if (!originalType.equals(newOriginalType)) {
                    ((Types.Builder) childBuilder).as(originalType);
                }
            }

            if (schemaElement.isSetField_id()) {
                ((Types.Builder) childBuilder).id(schemaElement.field_id);
            }

            ((Types.Builder) childBuilder).named(schemaElement.name);
            ++columnCount;
        }

    }

    private static LogicalTypeAnnotation.TimeUnit convertTimeUnit(TimeUnit unit) throws ParquetFileReaderException {
        switch (unit.getSetField()) {
            case MICROS:
                return LogicalTypeAnnotation.TimeUnit.MICROS;
            case MILLIS:
                return LogicalTypeAnnotation.TimeUnit.MILLIS;
            case NANOS:
                return LogicalTypeAnnotation.TimeUnit.NANOS;
            default:
                throw new ParquetFileReaderException("Unknown time unit " + unit);
        }
    }

    static LogicalTypeAnnotation getLogicalTypeAnnotation(LogicalType type) throws ParquetFileReaderException {
        switch (type.getSetField()) {
            case MAP:
                return LogicalTypeAnnotation.mapType();
            case BSON:
                return LogicalTypeAnnotation.bsonType();
            case DATE:
                return LogicalTypeAnnotation.dateType();
            case ENUM:
                return LogicalTypeAnnotation.enumType();
            case JSON:
                return LogicalTypeAnnotation.jsonType();
            case LIST:
                return LogicalTypeAnnotation.listType();
            case TIME:
                final TimeType time = type.getTIME();
                return LogicalTypeAnnotation.timeType(time.isAdjustedToUTC, convertTimeUnit(time.unit));
            case STRING:
                return LogicalTypeAnnotation.stringType();
            case DECIMAL:
                final DecimalType decimal = type.getDECIMAL();
                return LogicalTypeAnnotation.decimalType(decimal.scale, decimal.precision);
            case INTEGER:
                final IntType integer = type.getINTEGER();
                return LogicalTypeAnnotation.intType(integer.bitWidth, integer.isSigned);
            case UNKNOWN:
                return null;
            case TIMESTAMP:
                final TimestampType timestamp = type.getTIMESTAMP();
                return LogicalTypeAnnotation.timestampType(timestamp.isAdjustedToUTC, convertTimeUnit(timestamp.unit));
            default:
                throw new ParquetFileReaderException("Unknown logical type " + type);
        }
    }

    private static org.apache.parquet.schema.Type.Repetition fromParquetRepetition(FieldRepetitionType repetition) {
        return org.apache.parquet.schema.Type.Repetition.valueOf(repetition.name());
    }

    private static PrimitiveType.PrimitiveTypeName getPrimitive(Type type) throws ParquetFileReaderException {
        switch (type) {
            case BYTE_ARRAY: // TODO: rename BINARY and remove this switch
                return PrimitiveType.PrimitiveTypeName.BINARY;
            case INT64:
                return PrimitiveType.PrimitiveTypeName.INT64;
            case INT32:
                return PrimitiveType.PrimitiveTypeName.INT32;
            case BOOLEAN:
                return PrimitiveType.PrimitiveTypeName.BOOLEAN;
            case FLOAT:
                return PrimitiveType.PrimitiveTypeName.FLOAT;
            case DOUBLE:
                return PrimitiveType.PrimitiveTypeName.DOUBLE;
            case INT96:
                return PrimitiveType.PrimitiveTypeName.INT96;
            case FIXED_LEN_BYTE_ARRAY:
                return PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
            default:
                throw new ParquetFileReaderException("Unknown type " + type);
        }
    }

    private static org.apache.parquet.schema.ColumnOrder fromParquetColumnOrder(ColumnOrder columnOrder) {
        if (columnOrder.isSetTYPE_ORDER()) {
            return org.apache.parquet.schema.ColumnOrder.typeDefined();
        }
        // The column order is not yet supported by this API
        return org.apache.parquet.schema.ColumnOrder.undefined();
    }

    private static LogicalTypeAnnotation getLogicalTypeAnnotation(final ConvertedType convertedType,
            final LogicalType logicalType, final SchemaElement schemaElement) throws ParquetFileReaderException {
        switch (convertedType) {
            case UTF8:
                return LogicalTypeAnnotation.stringType();
            case MAP:
                return LogicalTypeAnnotation.mapType();
            case MAP_KEY_VALUE:
                return LogicalTypeAnnotation.MapKeyValueTypeAnnotation.getInstance();
            case LIST:
                return LogicalTypeAnnotation.listType();
            case ENUM:
                return LogicalTypeAnnotation.enumType();
            case DECIMAL:
                final int scale = schemaElement == null ? 0 : schemaElement.scale;
                final int precision = schemaElement == null ? 0 : schemaElement.precision;
                return LogicalTypeAnnotation.decimalType(scale, precision);
            case DATE:
                return LogicalTypeAnnotation.dateType();
            case TIME_MILLIS:
                // isAdjustedToUTC parameter is ignored while reading Parquet TIME type, so disregard it here
                return LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS);
            case TIME_MICROS:
                return LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MICROS);
            case TIMESTAMP_MILLIS:
                // Converted type doesn't have isAdjustedToUTC parameter, so use the information from logical type
                return LogicalTypeAnnotation.timestampType(isAdjustedToUTC(logicalType),
                        LogicalTypeAnnotation.TimeUnit.MILLIS);
            case TIMESTAMP_MICROS:
                return LogicalTypeAnnotation.timestampType(isAdjustedToUTC(logicalType),
                        LogicalTypeAnnotation.TimeUnit.MICROS);
            case INTERVAL:
                return LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance();
            case INT_8:
                return LogicalTypeAnnotation.intType(8, true);
            case INT_16:
                return LogicalTypeAnnotation.intType(16, true);
            case INT_32:
                return LogicalTypeAnnotation.intType(32, true);
            case INT_64:
                return LogicalTypeAnnotation.intType(64, true);
            case UINT_8:
                return LogicalTypeAnnotation.intType(8, false);
            case UINT_16:
                return LogicalTypeAnnotation.intType(16, false);
            case UINT_32:
                return LogicalTypeAnnotation.intType(32, false);
            case UINT_64:
                return LogicalTypeAnnotation.intType(64, false);
            case JSON:
                return LogicalTypeAnnotation.jsonType();
            case BSON:
                return LogicalTypeAnnotation.bsonType();
            default:
                throw new ParquetFileReaderException(
                        "Can't convert converted type to logical type, unknown converted type " + convertedType);
        }
    }

    /**
     * Helper method to determine if a logical type is adjusted to UTC.
     * 
     * @param logicalType the logical type to check
     * @return true if the logical type is a timestamp adjusted to UTC, false otherwise
     */
    private static boolean isAdjustedToUTC(final LogicalType logicalType) {
        if (logicalType.getSetField() == LogicalType._Fields.TIMESTAMP) {
            return logicalType.getTIMESTAMP().isAdjustedToUTC;
        }
        return false;
    }

    public MessageType getSchema() {
        return type;
    }

    public int rowGroupCount() {
        return fileMetaData.getRow_groups().size();
    }
}
