/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.barrage.util;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.ClassUtil;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.NameValidator;
import io.deephaven.db.util.ColumnFormattingValues;
import io.deephaven.db.util.config.MutableInputTable;
import io.deephaven.db.v2.HierarchicalTableInfo;
import io.deephaven.db.v2.RollupInfo;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.grpc_api.barrage.BarrageStreamGenerator;
import io.deephaven.grpc_api.util.GrpcUtil;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.util.Collections2;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.mutable.MutableObject;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;

public class BarrageSchemaUtil {
    // per flight specification: 0xFFFFFFFF value is the first 4 bytes of a valid IPC message
    private static final int IPC_CONTINUATION_TOKEN = -1;

    public static final ArrowType.FixedSizeBinary LOCAL_DATE_TYPE = new ArrowType.FixedSizeBinary(6);// year is 4 bytes,
                                                                                                     // month is 1 byte,
                                                                                                     // day is 1 byte
    public static final ArrowType.FixedSizeBinary LOCAL_TIME_TYPE = new ArrowType.FixedSizeBinary(7);// hour, minute,
                                                                                                     // second are each
                                                                                                     // one byte, nano
                                                                                                     // is 4 bytes

    private static final int ATTR_STRING_LEN_CUTOFF = 1024;

    /**
     * These are the types that get special encoding but are otherwise not primitives. TODO (core#58): add custom
     * barrage serialization/deserialization support
     */
    private static final Set<Class<?>> supportedTypes = new HashSet<>(Collections2.<Class<?>>asImmutableList(
            BigDecimal.class,
            BigInteger.class,
            String.class,
            DBDateTime.class,
            Boolean.class));

    public static ByteString schemaBytesFromTable(final Table table) {
        return schemaBytesFromTable(table.getDefinition(), table.getAttributes());
    }

    public static ByteString schemaBytesFromTable(final TableDefinition table,
            final Map<String, Object> attributes) {
        // note that flight expects the Schema to be wrapped in a Message prefixed by a 4-byte identifier
        // (to detect end-of-stream in some cases) followed by the size of the flatbuffer message

        final FlatBufferBuilder builder = new FlatBufferBuilder();
        final int schemaOffset = BarrageSchemaUtil.makeSchemaPayload(builder, table, attributes);
        builder.finish(BarrageStreamGenerator.wrapInMessage(builder, schemaOffset,
                org.apache.arrow.flatbuf.MessageHeader.Schema));

        final ByteBuffer msg = builder.dataBuffer();

        int padding = msg.remaining() % 8;
        if (padding != 0) {
            padding = 8 - padding;
        }

        // 4 * 2 is for two ints; IPC_CONTINUATION_TOKEN followed by size of schema payload
        final byte[] byteMsg = new byte[msg.remaining() + 4 * 2 + padding];
        intToBytes(IPC_CONTINUATION_TOKEN, byteMsg, 0);
        intToBytes(msg.remaining(), byteMsg, 4);
        msg.get(byteMsg, 8, msg.remaining());

        return ByteStringAccess.wrap(byteMsg);
    }

    private static void intToBytes(int value, byte[] bytes, int offset) {
        bytes[offset + 3] = (byte) (value >>> 24);
        bytes[offset + 2] = (byte) (value >>> 16);
        bytes[offset + 1] = (byte) (value >>> 8);
        bytes[offset] = (byte) (value);
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
        for (final Map.Entry<String, Object> entry : attributes.entrySet()) {
            final String key = entry.getKey();
            final Object val = entry.getValue();
            if (val instanceof Byte || val instanceof Short || val instanceof Integer ||
                    val instanceof Long || val instanceof Float || val instanceof Double ||
                    val instanceof Character || val instanceof Boolean ||
                    (val instanceof String && ((String) val).length() < ATTR_STRING_LEN_CUTOFF)) {
                putMetadata(schemaMetadata, "attribute." + key, val.toString());
            }
        }

        // copy rollup details
        if (attributes.containsKey(Table.HIERARCHICAL_SOURCE_INFO_ATTRIBUTE)) {
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
                    putMetadata(getExtraMetadata.apply(matchPair.left()), "rollup.sourceColumn", matchPair.right());
                }
            }
        }

        final Map<String, Field> fields = new LinkedHashMap<>();
        for (final ColumnDefinition<?> column : table.getColumns()) {
            final String colName = column.getName();
            final Map<String, String> extraMetadata = getExtraMetadata.apply(colName);

            // wire up style and format column references
            if (formatColumns.contains(colName + ColumnFormattingValues.TABLE_FORMAT_NAME)) {
                putMetadata(extraMetadata, "styleColumn", colName + ColumnFormattingValues.TABLE_FORMAT_NAME);
            } else if (formatColumns.contains(colName + ColumnFormattingValues.TABLE_NUMERIC_FORMAT_NAME)) {
                putMetadata(extraMetadata, "numberFormatColumn",
                        colName + ColumnFormattingValues.TABLE_NUMERIC_FORMAT_NAME);
            } else if (formatColumns.contains(colName + ColumnFormattingValues.TABLE_DATE_FORMAT_NAME)) {
                putMetadata(extraMetadata, "dateFormatColumn", colName + ColumnFormattingValues.TABLE_DATE_FORMAT_NAME);
            }

            fields.put(colName, arrowFieldFor(colName, column, descriptions.get(colName), inputTable, extraMetadata));
        }

        return new Schema(new ArrayList<>(fields.values()), schemaMetadata).getSchema(builder);
    }

    private static void putMetadata(final Map<String, String> metadata, final String key, final String value) {
        metadata.put("deephaven:" + key, value);
    }

    public static TableDefinition schemaToTableDefinition(final org.apache.arrow.flatbuf.Schema schema) {
        return schemaToTableDefinition(schema.fieldsLength(), i -> schema.fields(i).name(), i -> visitor -> {
            final org.apache.arrow.flatbuf.Field field = schema.fields(i);
            for (int j = 0; j < field.customMetadataLength(); j++) {
                final KeyValue keyValue = field.customMetadata(j);
                visitor.accept(keyValue.key(), keyValue.value());
            }
        });
    }

    public static TableDefinition schemaToTableDefinition(final Schema schema) {
        return schemaToTableDefinition(schema.getFields().size(), i -> schema.getFields().get(i).getName(),
                i -> visitor -> {
                    schema.getFields().get(i).getMetadata().forEach(visitor);
                });
    }

    private static TableDefinition schemaToTableDefinition(final int numColumns, final IntFunction<String> getName,
            final IntFunction<Consumer<BiConsumer<String, String>>> visitMetadata) {
        final ColumnDefinition<?>[] columns = new ColumnDefinition[numColumns];

        for (int i = 0; i < numColumns; ++i) {
            final String name = NameValidator.legalizeColumnName(getName.apply(i));
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
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Schema did not include `deephaven:type` metadata");
            }
            columns[i] = ColumnDefinition.fromGenericType(name, type.getValue(), componentType.getValue());
        }

        return new TableDefinition(columns);
    }

    private static Field arrowFieldFor(final String name, final ColumnDefinition<?> column, final String description,
            final MutableInputTable inputTable, final Map<String, String> extraMetadata) {
        List<Field> children = Collections.emptyList();

        // is hidden?
        final Class<?> type = column.getDataType();
        final Class<?> componentType = column.getComponentType();
        final Map<String, String> metadata = new HashMap<>(extraMetadata);

        if (type.isPrimitive() || supportedTypes.contains(type)) {
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
            putMetadata(metadata, "inputtable.isKey", Arrays.asList(inputTable.getKeyNames()).contains(name) + "");
        }

        final FieldType fieldType = arrowFieldTypeFor(type, componentType, metadata);
        if (fieldType.getType().isComplex()) {
            if (type.isArray()) {
                children = Collections.singletonList(
                        new Field("", arrowFieldTypeFor(componentType, null, metadata), Collections.emptyList()));
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
                return Types.MinorType.SMALLINT.getType();
            case Byte:
                return Types.MinorType.UINT1.getType();
            case Short:
                return Types.MinorType.UINT2.getType();
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
                if (type == DBDateTime.class) {
                    return Types.MinorType.BIGINT.getType();
                }

                // everything gets converted to a string
                return Types.MinorType.VARCHAR.getType(); // aka Utf8
        }
        throw new IllegalStateException("No ArrowType for type: " + type + " w/chunkType: " + chunkType);
    }
}
