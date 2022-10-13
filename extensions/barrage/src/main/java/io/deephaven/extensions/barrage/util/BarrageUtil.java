/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.barrage.util;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.ClassUtil;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.extensions.barrage.chunk.vector.VectorExpansionKernel;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.flight.util.MessageHelper;
import io.deephaven.proto.flight.util.SchemaHelper;
import io.deephaven.time.DateTime;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.util.ColumnFormattingValues;
import io.deephaven.engine.util.config.MutableInputTable;
import io.deephaven.engine.table.impl.HierarchicalTableInfo;
import io.deephaven.engine.table.impl.RollupInfo;
import io.deephaven.chunk.ChunkType;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.Vector;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.util.Collections2;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.mutable.MutableObject;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;

public class BarrageUtil {
    public static final long FLATBUFFER_MAGIC = 0x6E687064;

    private static final Logger log = LoggerFactory.getLogger(BarrageUtil.class);

    // year is 4 bytes, month is 1 byte, day is 1 byte
    public static final ArrowType.FixedSizeBinary LOCAL_DATE_TYPE = new ArrowType.FixedSizeBinary(6);
    // hour, minute, second are each one byte, nano is 4 bytes
    public static final ArrowType.FixedSizeBinary LOCAL_TIME_TYPE = new ArrowType.FixedSizeBinary(7);

    /**
     * Note that arrow's wire format states that Timestamps without timezones are not UTC -- that they are no timezone
     * at all. It's very important that we mark these times as UTC.
     */
    public static final ArrowType.Timestamp NANO_SINCE_EPOCH_TYPE =
            new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC");

    private static final int ATTR_STRING_LEN_CUTOFF = 1024;

    private static final String ATTR_DH_PREFIX = "deephaven:";
    private static final String ATTR_ATTR_TAG = "attribute";
    private static final String ATTR_ATTR_TYPE_TAG = "attribute_type";
    private static final String ATTR_TYPE_TAG = "type";
    private static final String ATTR_COMPONENT_TYPE_TAG = "componentType";

    /**
     * These are the types that get special encoding but are otherwise not primitives. TODO (core#58): add custom
     * barrage serialization/deserialization support
     */
    private static final Set<Class<?>> supportedTypes = new HashSet<>(Collections2.<Class<?>>asImmutableList(
            BigDecimal.class,
            BigInteger.class,
            String.class,
            DateTime.class,
            Boolean.class));

    public static ByteString schemaBytesFromTable(final Table table) {
        return schemaBytesFromTable(table.getDefinition(), table.getAttributes());
    }

    public static ByteString schemaBytesFromTable(final TableDefinition table,
            final Map<String, Object> attributes) {
        // note that flight expects the Schema to be wrapped in a Message prefixed by a 4-byte identifier
        // (to detect end-of-stream in some cases) followed by the size of the flatbuffer message

        final FlatBufferBuilder builder = new FlatBufferBuilder();
        final int schemaOffset = BarrageUtil.makeSchemaPayload(builder, table, attributes);
        builder.finish(MessageHelper.wrapInMessage(builder, schemaOffset,
                org.apache.arrow.flatbuf.MessageHeader.Schema));

        return ByteStringAccess.wrap(MessageHelper.toIpcBytes(builder));
    }

    public static int makeSchemaPayload(final FlatBufferBuilder builder,
            final TableDefinition table,
            final Map<String, Object> attributes) {
        final Map<String, Map<String, String>> fieldExtraMetadata = new HashMap<>();
        final Function<String, Map<String, String>> getExtraMetadata =
                (colName) -> fieldExtraMetadata.computeIfAbsent(colName, k -> new HashMap<>());

        // noinspection unchecked
        final Map<String, String> descriptions =
                Optional.ofNullable((Map<String, String>) attributes.get(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE))
                        .orElse(Collections.emptyMap());
        final MutableInputTable inputTable = (MutableInputTable) attributes.get(Table.INPUT_TABLE_ATTRIBUTE);

        // find format columns
        final Set<String> formatColumns = new HashSet<>();
        table.getColumnNames().stream().filter(ColumnFormattingValues::isFormattingColumn).forEach(formatColumns::add);

        // create metadata on the schema for table attributes
        final Map<String, String> schemaMetadata = new HashMap<>();

        // copy primitives as strings
        Set<String> unsentAttributes = new HashSet<>();
        for (final Map.Entry<String, Object> entry : attributes.entrySet()) {
            final String key = entry.getKey();
            final Object val = entry.getValue();
            if (val instanceof Byte || val instanceof Short || val instanceof Integer ||
                    val instanceof Long || val instanceof Float || val instanceof Double ||
                    val instanceof Character || val instanceof Boolean ||
                    (val instanceof String && ((String) val).length() < ATTR_STRING_LEN_CUTOFF)) {
                putMetadata(schemaMetadata, ATTR_ATTR_TAG + "." + key, val.toString());
                putMetadata(schemaMetadata, ATTR_ATTR_TYPE_TAG + "." + key, val.getClass().getCanonicalName());
            } else {
                unsentAttributes.add(key);
            }
        }

        // copy rollup details
        // TODO-RWC: Delete me
        if (attributes.containsKey(Table.HIERARCHICAL_SOURCE_INFO_ATTRIBUTE)) {
            unsentAttributes.remove(Table.HIERARCHICAL_SOURCE_INFO_ATTRIBUTE);
            final HierarchicalTableInfo hierarchicalTableInfo =
                    (HierarchicalTableInfo) attributes.remove(Table.HIERARCHICAL_SOURCE_INFO_ATTRIBUTE);
            final String hierarchicalSourceKeyPrefix =
                    ATTR_ATTR_TAG + "." + Table.HIERARCHICAL_SOURCE_INFO_ATTRIBUTE + ".";
            putMetadata(schemaMetadata, hierarchicalSourceKeyPrefix + "hierarchicalColumnName",
                    hierarchicalTableInfo.getHierarchicalColumnName());
            if (hierarchicalTableInfo instanceof RollupInfo) {
                final RollupInfo rollupInfo = (RollupInfo) hierarchicalTableInfo;
                putMetadata(schemaMetadata, hierarchicalSourceKeyPrefix + "byColumns",
                        String.join(",", rollupInfo.byColumnNames));
                putMetadata(schemaMetadata, hierarchicalSourceKeyPrefix + "leafType", rollupInfo.getLeafType().name());

                // mark columns to indicate their sources
                for (final MatchPair matchPair : rollupInfo.getMatchPairs()) {
                    putMetadata(getExtraMetadata.apply(matchPair.leftColumn()), "rollup.sourceColumn",
                            matchPair.rightColumn());
                }
            }
        }

        // note which attributes have a value we couldn't send
        for (String unsentAttribute : unsentAttributes) {
            putMetadata(schemaMetadata, "unsent." + ATTR_ATTR_TAG + "." + unsentAttribute, "");
        }

        final List<Field> fields = new ArrayList<>();
        for (final ColumnDefinition<?> column : table.getColumns()) {
            final String colName = column.getName();
            final Map<String, String> extraMetadata = getExtraMetadata.apply(colName);

            // wire up style and format column references
            if (formatColumns.contains(colName + ColumnFormattingValues.TABLE_FORMAT_NAME)) {
                putMetadata(extraMetadata, "styleColumn", colName + ColumnFormattingValues.TABLE_FORMAT_NAME);
            }
            if (formatColumns.contains(colName + ColumnFormattingValues.TABLE_NUMERIC_FORMAT_NAME)) {
                putMetadata(extraMetadata, "numberFormatColumn",
                        colName + ColumnFormattingValues.TABLE_NUMERIC_FORMAT_NAME);
            }
            if (formatColumns.contains(colName + ColumnFormattingValues.TABLE_DATE_FORMAT_NAME)) {
                putMetadata(extraMetadata, "dateFormatColumn", colName + ColumnFormattingValues.TABLE_DATE_FORMAT_NAME);
            }

            // TODO-RWC: Add constituent=true metadata for secondary definition
            // TODO-RWC: Remember to reinterpret to primitive when chunk filling.
            fields.add(arrowFieldFor(colName, column, descriptions.get(colName), inputTable, extraMetadata));
        }

        return new Schema(fields, schemaMetadata).getSchema(builder);
    }

    private static void putMetadata(final Map<String, String> metadata, final String key, final String value) {
        metadata.put(ATTR_DH_PREFIX + key, value);
    }

    private static boolean maybeConvertForTimeUnit(final TimeUnit unit, final ConvertedArrowSchema result,
            final int i) {
        switch (unit) {
            case NANOSECOND:
                return true;
            case MICROSECOND:
                setConversionFactor(result, i, 1000);
                return true;
            case MILLISECOND:
                setConversionFactor(result, i, 1000 * 1000);
                return true;
            case SECOND:
                setConversionFactor(result, i, 1000 * 1000 * 1000);
                return true;
            default:
                return false;
        }
    }

    private static Class<?> getDefaultType(
            final String name, final ArrowType arrowType, final ConvertedArrowSchema result, final int i) {
        final String exMsg = "Schema did not include `" + ATTR_DH_PREFIX + ATTR_TYPE_TAG + "` metadata for field ";
        switch (arrowType.getTypeID()) {
            case Int:
                final ArrowType.Int intType = (ArrowType.Int) arrowType;
                if (intType.getIsSigned()) {
                    // SIGNED
                    switch (intType.getBitWidth()) {
                        case 8:
                            return byte.class;
                        case 16:
                            return short.class;
                        case 32:
                            return int.class;
                        case 64:
                            return long.class;
                    }
                } else {
                    // UNSIGNED
                    switch (intType.getBitWidth()) {
                        case 8:
                            return short.class;
                        case 16:
                            return int.class;
                        case 32:
                            return long.class;
                    }
                }
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, exMsg +
                        " of intType(signed=" + intType.getIsSigned() + ", bitWidth=" + intType.getBitWidth() + ")");
            case Bool:
                return java.lang.Boolean.class;
            case Duration:
                final ArrowType.Duration durationType = (ArrowType.Duration) arrowType;
                final TimeUnit durationUnit = durationType.getUnit();
                if (maybeConvertForTimeUnit(durationUnit, result, i)) {
                    return long.class;
                }
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, exMsg +
                        " of durationType(unit=" + durationUnit.toString() + ")");
            case Timestamp:
                final ArrowType.Timestamp timestampType = (ArrowType.Timestamp) arrowType;
                final String tz = timestampType.getTimezone();
                final TimeUnit timestampUnit = timestampType.getUnit();
                if (tz == null || "UTC".equals(tz)) {
                    if (maybeConvertForTimeUnit(timestampUnit, result, i)) {
                        return DateTime.class;
                    }
                }
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, exMsg +
                        " of timestampType(Timezone=" + tz +
                        ", Unit=" + timestampUnit.toString() + ")");
            case FloatingPoint:
                final ArrowType.FloatingPoint floatingPointType = (ArrowType.FloatingPoint) arrowType;
                switch (floatingPointType.getPrecision()) {
                    case SINGLE:
                        return float.class;
                    case DOUBLE:
                        return double.class;
                    case HALF:
                    default:
                        throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, exMsg +
                                " of floatingPointType(Precision=" + floatingPointType.getPrecision().toString() + ")");
                }
            case Utf8:
                return java.lang.String.class;
            default:
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, exMsg +
                        " of type " + arrowType.getTypeID().toString());
        }
    }

    public static class ConvertedArrowSchema {
        public final int nCols;
        public TableDefinition tableDef;
        // a multiplicative factor to apply when reading; useful for eg converting arrow timestamp time units
        // to the expected nanos value for DateTime.
        public int[] conversionFactors;
        public Map<String, Object> attributes;

        public ConvertedArrowSchema(final int nCols) {
            this.nCols = nCols;
        }
    }

    private static void setConversionFactor(final ConvertedArrowSchema result, final int i, final int factor) {
        if (result.conversionFactors == null) {
            result.conversionFactors = new int[result.nCols];
            Arrays.fill(result.conversionFactors, 1);
        }
        result.conversionFactors[i] = factor;
    }

    public static ConvertedArrowSchema convertArrowSchema(final ExportedTableCreationResponse response) {
        return convertArrowSchema(SchemaHelper.flatbufSchema(response));
    }

    public static ConvertedArrowSchema convertArrowSchema(
            final org.apache.arrow.flatbuf.Schema schema) {
        return convertArrowSchema(
                schema.fieldsLength(),
                i -> schema.fields(i).name(),
                i -> ArrowType.getTypeForField(schema.fields(i)),
                i -> visitor -> {
                    final org.apache.arrow.flatbuf.Field field = schema.fields(i);
                    for (int j = 0; j < field.customMetadataLength(); j++) {
                        final KeyValue keyValue = field.customMetadata(j);
                        visitor.accept(keyValue.key(), keyValue.value());
                    }
                },
                visitor -> {
                    for (int j = 0; j < schema.customMetadataLength(); j++) {
                        final KeyValue keyValue = schema.customMetadata(j);
                        visitor.accept(keyValue.key(), keyValue.value());
                    }
                });
    }

    public static ConvertedArrowSchema convertArrowSchema(final Schema schema) {
        return convertArrowSchema(
                schema.getFields().size(),
                i -> schema.getFields().get(i).getName(),
                i -> schema.getFields().get(i).getType(),
                i -> visitor -> {
                    schema.getFields().get(i).getMetadata().forEach(visitor);
                },
                visitor -> schema.getCustomMetadata().forEach(visitor));
    }

    private static ConvertedArrowSchema convertArrowSchema(
            final int numColumns,
            final IntFunction<String> getName,
            final IntFunction<ArrowType> getArrowType,
            final IntFunction<Consumer<BiConsumer<String, String>>> columnMetadataVisitor,
            final Consumer<BiConsumer<String, String>> tableMetadataVisitor) {
        final ConvertedArrowSchema result = new ConvertedArrowSchema(numColumns);
        final ColumnDefinition<?>[] columns = new ColumnDefinition[numColumns];

        for (int i = 0; i < numColumns; ++i) {
            final String origName = getName.apply(i);
            final String name = NameValidator.legalizeColumnName(origName);
            final MutableObject<Class<?>> type = new MutableObject<>();
            final MutableObject<Class<?>> componentType = new MutableObject<>();

            columnMetadataVisitor.apply(i).accept((key, value) -> {
                if (key.equals(ATTR_DH_PREFIX + ATTR_TYPE_TAG)) {
                    try {
                        type.setValue(ClassUtil.lookupClass(value));
                    } catch (final ClassNotFoundException e) {
                        throw new UncheckedDeephavenException("Could not load class from schema", e);
                    }
                } else if (key.equals(ATTR_DH_PREFIX + ATTR_COMPONENT_TYPE_TAG)) {
                    try {
                        componentType.setValue(ClassUtil.lookupClass(value));
                    } catch (final ClassNotFoundException e) {
                        throw new UncheckedDeephavenException("Could not load class from schema", e);
                    }
                }
            });

            if (type.getValue() == null) {
                Class<?> defaultType = getDefaultType(name, getArrowType.apply(i), result, i);
                type.setValue(defaultType);
            }
            columns[i] = ColumnDefinition.fromGenericType(name, type.getValue(), componentType.getValue());
        }

        result.tableDef = TableDefinition.of(columns);

        result.attributes = new HashMap<>();

        final HashMap<String, String> attributeTypeMap = new HashMap<>();
        tableMetadataVisitor.accept((key, value) -> {
            final String isAttributePrefix = ATTR_DH_PREFIX + ATTR_ATTR_TAG + ".";
            final String isAttributeTypePrefix = ATTR_DH_PREFIX + ATTR_ATTR_TYPE_TAG + ".";
            if (key.startsWith(isAttributePrefix)) {
                result.attributes.put(key.substring(isAttributePrefix.length()), value);
            } else if (key.startsWith(isAttributeTypePrefix)) {
                attributeTypeMap.put(key.substring(isAttributeTypePrefix.length()), value);
            }
        });

        attributeTypeMap.forEach((attrKey, attrType) -> {
            if (!result.attributes.containsKey(attrKey)) {
                // ignore if we receive a type for an unsent key (server code won't do this)
                log.warn().append("Schema included ").append(ATTR_ATTR_TYPE_TAG).append(" tag but not a corresponding ")
                        .append(ATTR_ATTR_TAG).append(" tag for key ").append(attrKey).append(".").endl();
                return;
            }

            Object currValue = result.attributes.get(attrKey);
            if (!(currValue instanceof String)) {
                // we just inserted this as a string in the block above
                throw new IllegalStateException();
            }

            final String stringValue = (String) currValue;
            switch (attrType) {
                case "java.lang.Byte":
                    result.attributes.put(attrKey, Byte.valueOf(stringValue));
                    break;
                case "java.lang.Short":
                    result.attributes.put(attrKey, Short.valueOf(stringValue));
                    break;
                case "java.lang.Integer":
                    result.attributes.put(attrKey, Integer.valueOf(stringValue));
                    break;
                case "java.lang.Long":
                    result.attributes.put(attrKey, Long.valueOf(stringValue));
                    break;
                case "java.lang.Float":
                    result.attributes.put(attrKey, Float.valueOf(stringValue));
                    break;
                case "java.lang.Double":
                    result.attributes.put(attrKey, Double.valueOf(stringValue));
                    break;
                case "java.lang.Character":
                    result.attributes.put(attrKey, stringValue.isEmpty() ? (char) 0 : stringValue.charAt(0));
                    break;
                case "java.lang.Boolean":
                    result.attributes.put(attrKey, Boolean.valueOf(stringValue));
                    break;
                case "java.lang.String":
                    // leave as is
                    break;
                default:
                    log.warn().append("Schema included unsupported ").append(ATTR_ATTR_TYPE_TAG).append(" tag of '")
                            .append(attrType).append("' for key ").append(attrKey).append(".").endl();
            }
        });

        return result;
    }

    private static boolean isTypeNativelySupported(final Class<?> typ) {
        if (typ.isPrimitive() || TypeUtils.isBoxedType(typ) || supportedTypes.contains(typ)
                || Vector.class.isAssignableFrom(typ)) {
            return true;
        }
        if (typ.isArray()) {
            return isTypeNativelySupported(typ.getComponentType());
        }
        return false;
    }

    private static Field arrowFieldFor(final String name, final ColumnDefinition<?> column, final String description,
            final MutableInputTable inputTable, final Map<String, String> extraMetadata) {
        // is hidden?
        final Class<?> type = column.getDataType();
        final Class<?> componentType = column.getComponentType();
        final Map<String, String> metadata = new HashMap<>(extraMetadata);

        if (isTypeNativelySupported(type)) {
            putMetadata(metadata, ATTR_TYPE_TAG, type.getCanonicalName());

            if (componentType != null) {
                if (isTypeNativelySupported(componentType)) {
                    putMetadata(metadata, ATTR_COMPONENT_TYPE_TAG, componentType.getCanonicalName());
                } else {
                    // otherwise, it will be converted to a string
                    putMetadata(metadata, ATTR_COMPONENT_TYPE_TAG, String.class.getCanonicalName());
                }
            }
        } else {
            // otherwise, it will be converted to a string
            putMetadata(metadata, ATTR_TYPE_TAG, String.class.getCanonicalName());
        }

        // only one of these will be true, if any are true the column will not be visible
        putMetadata(metadata, "isStyle", name.endsWith(ColumnFormattingValues.TABLE_FORMAT_NAME) + "");
        putMetadata(metadata, "isRowStyle",
                name.equals(ColumnFormattingValues.ROW_FORMAT_NAME + ColumnFormattingValues.TABLE_FORMAT_NAME) + "");
        putMetadata(metadata, "isDateFormat", name.endsWith(ColumnFormattingValues.TABLE_DATE_FORMAT_NAME) + "");
        putMetadata(metadata, "isNumberFormat", name.endsWith(ColumnFormattingValues.TABLE_NUMERIC_FORMAT_NAME) + "");
        putMetadata(metadata, "isRollupColumn", name.equals(RollupInfo.ROLLUP_COLUMN) + "");

        if (description != null) {
            putMetadata(metadata, "description", description);
        }
        if (inputTable != null) {
            putMetadata(metadata, "inputtable.isKey", inputTable.getKeyNames().contains(name) + "");
        }

        if (Vector.class.isAssignableFrom(type)) {
            return arrowFieldForVectorType(name, type, componentType, metadata);
        }

        return arrowFieldFor(name, type, componentType, metadata);
    }

    private static Field arrowFieldFor(
            final String name, final Class<?> type, final Class<?> componentType, final Map<String, String> metadata) {
        List<Field> children = Collections.emptyList();

        final FieldType fieldType = arrowFieldTypeFor(type, metadata);
        if (fieldType.getType().isComplex()) {
            if (type.isArray()) {
                children = Collections.singletonList(arrowFieldFor(
                        "", componentType, componentType.getComponentType(), Collections.emptyMap()));
            } else {
                throw new UnsupportedOperationException("Arrow Complex Type Not Supported: " + fieldType.getType());
            }
        }

        return new Field(name, fieldType, children);
    }

    private static FieldType arrowFieldTypeFor(final Class<?> type, final Map<String, String> metadata) {
        return new FieldType(true, arrowTypeFor(type), null, metadata);
    }

    private static ArrowType arrowTypeFor(Class<?> type) {
        if (TypeUtils.isBoxedType(type)) {
            type = TypeUtils.getUnboxedType(type);
        }
        final ChunkType chunkType = ChunkType.fromElementType(type);
        switch (chunkType) {
            case Boolean:
                return Types.MinorType.BIT.getType();
            case Char:
                return Types.MinorType.UINT2.getType();
            case Byte:
                return Types.MinorType.TINYINT.getType();
            case Short:
                return Types.MinorType.SMALLINT.getType();
            case Int:
                return Types.MinorType.INT.getType();
            case Long:
                return Types.MinorType.BIGINT.getType();
            case Float:
                return Types.MinorType.FLOAT4.getType();
            case Double:
                return Types.MinorType.FLOAT8.getType();
            case Object:
                if (type.isArray()) {
                    return Types.MinorType.LIST.getType();
                }
                if (type == LocalDate.class) {
                    return LOCAL_DATE_TYPE;
                }
                if (type == LocalTime.class) {
                    return LOCAL_TIME_TYPE;
                }
                if (type == BigDecimal.class
                        || type == BigInteger.class) {
                    return Types.MinorType.VARBINARY.getType();
                }
                if (type == DateTime.class) {
                    return NANO_SINCE_EPOCH_TYPE;
                }

                // everything gets converted to a string
                return Types.MinorType.VARCHAR.getType(); // aka Utf8
        }
        throw new IllegalStateException("No ArrowType for type: " + type + " w/chunkType: " + chunkType);
    }

    private static Field arrowFieldForVectorType(
            final String name, final Class<?> type, final Class<?> knownComponentType,
            final Map<String, String> metadata) {

        // Vectors are always lists.
        final FieldType fieldType = new FieldType(true, Types.MinorType.LIST.getType(), null, metadata);
        final Class<?> componentType = VectorExpansionKernel.getComponentType(type, knownComponentType);
        final List<Field> children = Collections.singletonList(arrowFieldFor(
                "", componentType, componentType.getComponentType(), Collections.emptyMap()));

        return new Field(name, fieldType, children);
    }
}
