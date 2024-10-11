//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.util.channel.CachedChannelProvider;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.parquet.format.*;
import org.apache.parquet.format.ColumnOrder;
import org.apache.parquet.format.Type;
import org.apache.parquet.schema.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.util.*;

import static io.deephaven.parquet.base.ParquetUtils.MAGIC;
import static io.deephaven.base.FileUtils.convertToURI;

/**
 * Top level accessor for a parquet file which can read from a CLI style file URI, ex."s3://bucket/key".
 */
public class ParquetFileReader {
    private static final int FOOTER_LENGTH_SIZE = 4;
    public static final String FILE_URI_SCHEME = "file";

    public final FileMetaData fileMetaData;
    private final SeekableChannelsProvider channelsProvider;

    /**
     * If reading a single parquet file, root URI is the URI of the file, else the parent directory for a metadata file
     */
    private final URI rootURI;
    private final MessageType schema;

    /**
     * Make a {@link ParquetFileReader} for the supplied {@link URI}. Wraps {@link IOException} as
     * {@link UncheckedIOException}.
     *
     * @param parquetFileURI The URI for the parquet file or the parquet metadata file
     * @param channelsProvider The {@link SeekableChannelsProvider} to use for reading the file
     * @return The new {@link ParquetFileReader}
     */
    public static ParquetFileReader create(
            @NotNull final URI parquetFileURI,
            @NotNull final SeekableChannelsProvider channelsProvider) {
        try {
            return new ParquetFileReader(parquetFileURI, channelsProvider);
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to create Parquet file reader: " + parquetFileURI, e);
        }
    }

    /**
     * Create a new ParquetFileReader for the provided source.
     *
     * @param parquetFileURI The URI for the parquet file or the parquet metadata file
     * @param provider The {@link SeekableChannelsProvider} to use for reading the file
     */
    private ParquetFileReader(
            @NotNull final URI parquetFileURI,
            @NotNull final SeekableChannelsProvider provider) throws IOException {
        this.channelsProvider = CachedChannelProvider.create(provider, 1 << 7);
        if (!parquetFileURI.getRawPath().endsWith(".parquet") && FILE_URI_SCHEME.equals(parquetFileURI.getScheme())) {
            // Construct a new file URI for the parent directory
            rootURI = convertToURI(new File(parquetFileURI).getParentFile(), true);
        } else {
            rootURI = parquetFileURI;
        }
        try (
                final SeekableChannelContext context = channelsProvider.makeSingleUseContext();
                final SeekableByteChannel ch = channelsProvider.getReadChannel(context, parquetFileURI)) {
            final int footerLength = positionToFileMetadata(parquetFileURI, ch);
            try (final InputStream in = channelsProvider.getInputStream(ch, footerLength)) {
                fileMetaData = Util.readFileMetaData(in);
            }
        }
        schema = fromParquetSchema(fileMetaData.schema, fileMetaData.column_orders);
    }

    /**
     * Read the footer length and position the channel to the start of the footer.
     *
     * @return The length of the footer
     */
    private static int positionToFileMetadata(URI parquetFileURI, SeekableByteChannel readChannel) throws IOException {
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
        return footerLength;
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
                schema,
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
                final LogicalTypeAnnotation logicalType = getLogicalTypeAnnotation(schemaElement.logicalType);
                ((Types.Builder) childBuilder).as(logicalType);
            } else if (schemaElement.isSetConverted_type()) {
                final LogicalTypeAnnotation logicalType = getLogicalTypeFromConvertedType(
                        schemaElement.converted_type, schemaElement);
                ((Types.Builder) childBuilder).as(logicalType);
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

    @Nullable
    private static LogicalTypeAnnotation getLogicalTypeAnnotation(@NotNull final LogicalType type)
            throws ParquetFileReaderException {
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

    /**
     * This method will convert the {@link ConvertedType} to a {@link LogicalTypeAnnotation} and should only be called
     * if the logical type is not set in the schema element.
     *
     * @see <a href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md">Reference for conversions</a>
     */
    private static LogicalTypeAnnotation getLogicalTypeFromConvertedType(
            final ConvertedType convertedType,
            final SchemaElement schemaElement) throws ParquetFileReaderException {
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
                return LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS);
            case TIME_MICROS:
                return LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MICROS);
            case TIMESTAMP_MILLIS:
                return LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS);
            case TIMESTAMP_MICROS:
                return LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS);
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

    public MessageType getSchema() {
        return schema;
    }
}
