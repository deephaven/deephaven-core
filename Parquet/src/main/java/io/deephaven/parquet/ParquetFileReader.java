package io.deephaven.parquet;


import io.deephaven.parquet.utils.SeekableChannelsProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.format.*;
import org.apache.parquet.format.ColumnOrder;
import org.apache.parquet.format.Type;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.schema.*;



import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static io.deephaven.parquet.utils.Helpers.readFully;


/**
 * Top level accessor for a parquet file
 */
public class ParquetFileReader {

    private static final String MAGIC_STR = "PAR1";
    static final byte[] MAGIC = MAGIC_STR.getBytes(Charset.forName("ASCII"));
    public final FileMetaData fileMetaData;
    private final SeekableChannelsProvider channelsProvider;
    private final ThreadLocal<CodecFactory> codecFactory;
    private final Path rootPath;
    private final MessageType type;

    public ParquetFileReader(String filePath, SeekableChannelsProvider channelsProvider,
        int pageSizeHint) throws IOException {
        this.channelsProvider = channelsProvider;
        this.codecFactory =
            ThreadLocal.withInitial(() -> new CodecFactory(new Configuration(), pageSizeHint));
        // Root path should be this file if a single file, else the parent directory for a metadata
        // file
        rootPath =
            filePath.endsWith(".parquet") ? Paths.get(filePath) : Paths.get(filePath).getParent();

        SeekableByteChannel f = channelsProvider.getReadChannel(filePath);
        long fileLen = f.size();

        int FOOTER_LENGTH_SIZE = 4;

        if (fileLen < MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) { // MAGIC + data + footer +
                                                                          // footerIndex + MAGIC
            throw new RuntimeException(
                filePath + " is not a Parquet file (too small length: " + fileLen + ")");
        }
        long footerLengthIndex = fileLen - FOOTER_LENGTH_SIZE - MAGIC.length;

        f.position(footerLengthIndex);

        int footerLength = readIntLittleEndian(f);
        byte[] magic = new byte[MAGIC.length];
        readFully(f, magic);
        if (!Arrays.equals(MAGIC, magic)) {
            throw new RuntimeException(
                filePath + " is not a Parquet file. expected magic number at tail "
                    + Arrays.toString(MAGIC) + " but found " + Arrays.toString(magic));
        }
        long footerIndex = footerLengthIndex - footerLength;
        if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
            throw new RuntimeException(
                "corrupted file: the footer index is not within the file: " + footerIndex);
        }
        f.position(footerIndex);
        byte[] footer = new byte[footerLength];
        readFully(f, footer);
        f.close();
        fileMetaData = Util.readFileMetaData(new ByteArrayInputStream(footer));
        type = fromParquetSchema(fileMetaData.schema, fileMetaData.column_orders);
    }

    /**
     * @return The {@link SeekableChannelsProvider} used for this reader, appropriate to use for
     *         related file acccess
     */
    public SeekableChannelsProvider getChannelsProvider() {
        return channelsProvider;
    }

    private Set<String> columnsWithDictionaryUsedOnEveryDataPage = null;

    /**
     * Get the name of all columns that we can know for certain (a) have a dictionary, and (b) use
     * the dictionary on all data pages.
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
     * True only if we are certain every data page in this column chunk uses dictionary encoding;
     * note false also covers the "we can't tell" case.
     */
    private boolean columnChunkUsesDictionaryOnEveryPage(final ColumnChunk columnChunk) {
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

    private int readIntLittleEndian(SeekableByteChannel f) throws IOException {
        ByteBuffer tempBuf = ByteBuffer.allocate(Integer.BYTES);
        tempBuf.order(ByteOrder.LITTLE_ENDIAN);
        int read = f.read(tempBuf);
        if (read != 4) {
            throw new IOException("Expected for bytes, only read " + read);
        }
        tempBuf.flip();
        return tempBuf.getInt();
    }

    public RowGroupReader getRowGroup(int groupNumber) {
        return new RowGroupReaderImpl(
            fileMetaData.getRow_groups().get(groupNumber),
            channelsProvider,
            rootPath,
            codecFactory,
            type,
            getSchema());
    }

    private static MessageType fromParquetSchema(List<SchemaElement> schema,
        List<ColumnOrder> columnOrders) {
        Iterator<SchemaElement> iterator = schema.iterator();
        SchemaElement root = iterator.next();
        Types.MessageTypeBuilder builder = Types.buildMessage();
        if (root.isSetField_id()) {
            builder.id(root.field_id);
        }
        buildChildren(builder, iterator, root.getNum_children(), columnOrders, 0);
        return builder.named(root.name);
    }

    private static void buildChildren(Types.GroupBuilder builder, Iterator<SchemaElement> schema,
        int childrenCount, List<ColumnOrder> columnOrders, int columnCount) {
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
                        fromParquetColumnOrder((ColumnOrder) columnOrders.get(columnCount));
                    if (columnOrder
                        .getColumnOrderName() == org.apache.parquet.schema.ColumnOrder.ColumnOrderName.TYPE_DEFINED_ORDER
                        && (schemaElement.type == org.apache.parquet.format.Type.INT96
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
                ((org.apache.parquet.schema.Types.Builder) childBuilder)
                    .as(getLogicalTypeAnnotation(schemaElement.logicalType));
            }

            if (schemaElement.isSetConverted_type()) {
                LogicalTypeAnnotation originalType =
                    getLogicalTypeAnnotation(schemaElement.converted_type, schemaElement);
                LogicalTypeAnnotation newOriginalType = schemaElement.isSetLogicalType()
                    && getLogicalTypeAnnotation(schemaElement.logicalType) != null
                        ? getLogicalTypeAnnotation(schemaElement.logicalType)
                        : null;
                if (!originalType.equals(newOriginalType)) {
                    ((org.apache.parquet.schema.Types.Builder) childBuilder).as(originalType);
                }
            }

            if (schemaElement.isSetField_id()) {
                ((org.apache.parquet.schema.Types.Builder) childBuilder).id(schemaElement.field_id);
            }

            ((org.apache.parquet.schema.Types.Builder) childBuilder).named(schemaElement.name);
            ++columnCount;
        }

    }

    private static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit convertTimeUnit(
        TimeUnit unit) {
        switch (unit.getSetField()) {
            case MICROS:
                return org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
            case MILLIS:
                return org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
            case NANOS:
                return org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.NANOS;
            default:
                throw new RuntimeException("Unknown time unit " + unit);
        }
    }



    static LogicalTypeAnnotation getLogicalTypeAnnotation(LogicalType type) {
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
                TimeType time = type.getTIME();
                return LogicalTypeAnnotation.timeType(time.isAdjustedToUTC,
                    convertTimeUnit(time.unit));
            case STRING:
                return LogicalTypeAnnotation.stringType();
            case DECIMAL:
                DecimalType decimal = type.getDECIMAL();
                return LogicalTypeAnnotation.decimalType(decimal.scale, decimal.precision);
            case INTEGER:
                IntType integer = type.getINTEGER();
                return LogicalTypeAnnotation.intType(integer.bitWidth, integer.isSigned);
            case UNKNOWN:
                return null;
            case TIMESTAMP:
                TimestampType timestamp = type.getTIMESTAMP();
                return LogicalTypeAnnotation.timestampType(timestamp.isAdjustedToUTC,
                    convertTimeUnit(timestamp.unit));
            default:
                throw new RuntimeException("Unknown logical type " + type);
        }
    }


    private static org.apache.parquet.schema.Type.Repetition fromParquetRepetition(
        FieldRepetitionType repetition) {
        return org.apache.parquet.schema.Type.Repetition.valueOf(repetition.name());
    }

    private static PrimitiveType.PrimitiveTypeName getPrimitive(Type type) {
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
                throw new RuntimeException("Unknown type " + type);
        }
    }

    private static org.apache.parquet.schema.ColumnOrder fromParquetColumnOrder(
        ColumnOrder columnOrder) {
        if (columnOrder.isSetTYPE_ORDER()) {
            return org.apache.parquet.schema.ColumnOrder.typeDefined();
        }
        // The column order is not yet supported by this API
        return org.apache.parquet.schema.ColumnOrder.undefined();
    }

    private static LogicalTypeAnnotation getLogicalTypeAnnotation(ConvertedType type,
        SchemaElement schemaElement) {
        switch (type) {
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
                int scale = schemaElement == null ? 0 : schemaElement.scale;
                int precision = schemaElement == null ? 0 : schemaElement.precision;
                return LogicalTypeAnnotation.decimalType(scale, precision);
            case DATE:
                return LogicalTypeAnnotation.dateType();
            case TIME_MILLIS:
                return LogicalTypeAnnotation.timeType(true,
                    org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS);
            case TIME_MICROS:
                return LogicalTypeAnnotation.timeType(true,
                    org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS);
            case TIMESTAMP_MILLIS:
                return LogicalTypeAnnotation.timestampType(true,
                    org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS);
            case TIMESTAMP_MICROS:
                return LogicalTypeAnnotation.timestampType(true,
                    org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS);
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
                throw new RuntimeException(
                    "Can't convert converted type to logical type, unknown converted type " + type);
        }
    }

    public MessageType getSchema() {
        return type;
    }

    public int rowGroupCount() {
        return fileMetaData.getRow_groups().size();
    }
}
