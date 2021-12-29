/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
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
                putMetadata(schemaMetadata, "attribute." + key, val.toString());
            } else {
                unsentAttributes.add(key);
            }
        }

        // copy rollup details
        if (attributes.containsKey(Table.HIERARCHICAL_SOURCE_INFO_ATTRIBUTE)) {
            unsentAttributes.remove(Table.HIERARCHICAL_SOURCE_INFO_ATTRIBUTE);
            final HierarchicalTableInfo hierarchicalTableInfo =
                    (HierarchicalTableInfo) attributes.remove(Table.HIERARCHICAL_SOURCE_INFO_ATTRIBUTE);
            final String hierarchicalSourceKeyPrefix = "attribute." + Table.HIERARCHICAL_SOURCE_INFO_ATTRIBUTE + ".";
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
            putMetadata(schemaMetadata, "unsent.attribute." + unsentAttribute, "");
        }

        final Map<String, Field> fields = new LinkedHashMap<>();
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

            fields.put(colName, arrowFieldFor(colName, column, descriptions.get(colName), inputTable, extraMetadata));
        }

        return new Schema(new ArrayList<>(fields.values()), schemaMetadata).getSchema(builder);
    }

    private static void putMetadata(final Map<String, String> metadata, final String key, final String value) {
        metadata.put("deephaven:" + key, value);
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
        final String exMsg = "Schema did not include `deephaven:type` metadata for field ";
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
                });
    }

    public static ConvertedArrowSchema convertArrowSchema(final Schema schema) {
        return convertArrowSchema(
                schema.getFields().size(),
                i -> schema.getFields().get(i).getName(),
                i -> schema.getFields().get(i).getType(),
                i -> visitor -> {
                    schema.getFields().get(i).getMetadata().forEach(visitor);
                });
    }

    private static ConvertedArrowSchema convertArrowSchema(
            final int numColumns,
            final IntFunction<String> getName,
            final IntFunction<ArrowType> getArrowType,
            final IntFunction<Consumer<BiConsumer<String, String>>> visitMetadata) {
        final ConvertedArrowSchema result = new ConvertedArrowSchema(numColumns);
        final ColumnDefinition<?>[] columns = new ColumnDefinition[numColumns];

        for (int i = 0; i < numColumns; ++i) {
            final String origName = getName.apply(i);
            final String name = NameValidator.legalizeColumnName(origName);
            final MutableObject<Class<?>> type = new MutableObject<>();
            final MutableObject<Class<?>> componentType = new MutableObject<>();

            visitMetadata.apply(i).accept((key, value) -> {
                if (key.equals("deephaven:type")) {
                    try {
                        type.setValue(ClassUtil.lookupClass(value));
                    } catch (final ClassNotFoundException e) {
                        throw new UncheckedDeephavenException("Could not load class from schema", e);
                    }
                } else if (key.equals("deephaven:componentType")) {
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

        result.tableDef = new TableDefinition(columns);
        return result;
    }

    private static boolean isTypeNativelySupported(final Class<?> typ) {
        if (typ.isPrimitive() || TypeUtils.isBoxedType(typ) || supportedTypes.contains(typ)) {
            return true;
        }
        if (typ.isArray()) {
            return isTypeNativelySupported(typ.getComponentType());
        }
        return false;
    }

    private static Field arrowFieldFor(final String name, final ColumnDefinition<?> column, final String description,
            final MutableInputTable inputTable, final Map<String, String> extraMetadata) {
        List<Field> children = Collections.emptyList();

        // is hidden?
        final Class<?> type = column.getDataType();
        final Class<?> componentType = column.getComponentType();
        final Map<String, String> metadata = new HashMap<>(extraMetadata);

        if (isTypeNativelySupported(type)) {
            putMetadata(metadata, "type", type.getCanonicalName());
        } else {
            // otherwise will be converted to a string
            putMetadata(metadata, "type", String.class.getCanonicalName());
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

        return arrowFieldFor(name, type, componentType, metadata);
    }

    private static Field arrowFieldFor(
            final String name, final Class<?> type, final Class<?> componentType, final Map<String, String> metadata) {
        List<Field> children = Collections.emptyList();

        final FieldType fieldType = arrowFieldTypeFor(type, componentType, metadata);
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

    private static FieldType arrowFieldTypeFor(final Class<?> type, final Class<?> componentType,
            final Map<String, String> metadata) {
        return new FieldType(true, arrowTypeFor(type, componentType), null, metadata);
    }

    private static ArrowType arrowTypeFor(final Class<?> type, final Class<?> componentType) {
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
}
