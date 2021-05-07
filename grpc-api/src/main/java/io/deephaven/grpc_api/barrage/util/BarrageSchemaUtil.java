package io.deephaven.grpc_api.barrage.util;

import com.google.flatbuffers.FlatBufferBuilder;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.util.ColumnFormattingValues;
import io.deephaven.db.util.config.MutableInputTable;
import io.deephaven.db.v2.HierarchicalTableInfo;
import io.deephaven.db.v2.RollupInfo;
import io.deephaven.db.v2.sources.ColumnSource;

import io.deephaven.db.v2.sources.chunk.ChunkType;
import org.apache.arrow.util.Collections2;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;
import java.util.function.Function;

public class BarrageSchemaUtil {
    public static final ArrowType.FixedSizeBinary LOCAL_DATE_TYPE = new ArrowType.FixedSizeBinary(6);// year is 4 bytes, month is 1 byte, day is 1 byte
    public static final ArrowType.FixedSizeBinary LOCAL_TIME_TYPE = new ArrowType.FixedSizeBinary(7);// hour, minute, second are each one byte, nano is 4 bytes

    private static final int ATTR_STRING_LEN_CUTOFF = 1024;

    private static final Map<String, Class<?>> primitiveTypeNameMap = new HashMap<>(16);
    private static void registerPrimitive(final Class<?> cls) {
        primitiveTypeNameMap.put(cls.getCanonicalName(), cls);
    }

    static {
        Arrays.asList(boolean.class, boolean[].class, byte.class, byte[].class, char.class, char[].class, double.class, double[].class,
                float.class, float[].class, int.class, int[].class, long.class, long[].class, short.class, short[].class)
                .forEach(BarrageSchemaUtil::registerPrimitive);
    }

    private static final Set<Class<?>> supportedTypes = new HashSet<>(Collections2.<Class<?>>asImmutableList(
            BigDecimal.class,
            BigInteger.class,
            String.class,
            DBDateTime.class,
            Boolean.class
    ));

    public static int makeSchemaPayload(final FlatBufferBuilder builder,
                                        final String[] columnNames,
                                        final ColumnSource<?>[] columnSources,
                                        final Map<String, Object> attributes) {
        final Map<String, Map<String, String>> fieldExtraMetadata = new HashMap<>();
        final Function<String, Map<String, String>> getExtraMetadata =
                (colName) -> fieldExtraMetadata.computeIfAbsent(colName, k -> new HashMap<>());

        //noinspection unchecked
        final Map<String, String> descriptions = Optional.ofNullable((Map<String, String>) attributes.get(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE)).orElse(Collections.emptyMap());
        final MutableInputTable inputTable = (MutableInputTable) attributes.get(Table.INPUT_TABLE_ATTRIBUTE);

        // find format columns
        final Set<String> formatColumns = new HashSet<>();
        for (final String colName : columnNames) {
            if (ColumnFormattingValues.isFormattingColumn(colName)) {
                formatColumns.add(colName);
            }
        }

        // create metadata on the schema for table attributes
        final Map<String, String> schemaMetadata = new HashMap<>();

        // copy primitives as strings
        for (final Map.Entry<String, Object> entry : attributes.entrySet()) {
            final String key = entry.getKey();
            final Object val = entry.getValue();
            if (val instanceof Byte || val instanceof Short || val instanceof Integer ||
                    val instanceof Long || val instanceof Float || val instanceof Double  ||
                    val instanceof Character || val instanceof Boolean ||
                    (val instanceof String && ((String)val).length() < ATTR_STRING_LEN_CUTOFF)) {
                putMetadata(schemaMetadata, "attribute." + key, val.toString());
            }
        }

        // copy rollup details
        if (attributes.containsKey(Table.HIERARCHICAL_SOURCE_INFO_ATTRIBUTE)) {
            final HierarchicalTableInfo hierarchicalTableInfo = (HierarchicalTableInfo) attributes.remove(Table.HIERARCHICAL_SOURCE_INFO_ATTRIBUTE);
            final String hierarchicalSourceKeyPrefix = "attribute." + Table.HIERARCHICAL_SOURCE_INFO_ATTRIBUTE + ".";
            putMetadata(schemaMetadata, hierarchicalSourceKeyPrefix + "hierarchicalColumnName", hierarchicalTableInfo.getHierarchicalColumnName());
            if (hierarchicalTableInfo instanceof RollupInfo) {
                final RollupInfo rollupInfo = (RollupInfo) hierarchicalTableInfo;
                putMetadata(schemaMetadata, hierarchicalSourceKeyPrefix + "byColumns", String.join(",", rollupInfo.byColumnNames));
                putMetadata(schemaMetadata, hierarchicalSourceKeyPrefix + "leafType", rollupInfo.getLeafType().name());

                // mark columns to indicate their sources
                for (final MatchPair matchPair : rollupInfo.getMatchPairs()) {
                    putMetadata(getExtraMetadata.apply(matchPair.left()), "rollup.sourceColumn", matchPair.right());
                }
            }
        }

        final Map<String, Field> fields = new LinkedHashMap<>();
        for (int i = 0; i < columnNames.length; ++i) {
            final String colName = columnNames[i];
            final Map<String, String> extraMetadata = getExtraMetadata.apply(colName);

            // wire up style and format column references
            if (formatColumns.contains(colName + ColumnFormattingValues.TABLE_FORMAT_NAME)) {
                putMetadata(extraMetadata, "styleColumn", colName + ColumnFormattingValues.TABLE_FORMAT_NAME);
            } else if (formatColumns.contains(colName + ColumnFormattingValues.TABLE_NUMERIC_FORMAT_NAME)) {
                putMetadata(extraMetadata, "numberFormatColumn", colName + ColumnFormattingValues.TABLE_NUMERIC_FORMAT_NAME);
            } else if (formatColumns.contains(colName + ColumnFormattingValues.TABLE_DATE_FORMAT_NAME)) {
                putMetadata(extraMetadata, "dateFormatColumn", colName + ColumnFormattingValues.TABLE_DATE_FORMAT_NAME);
            }

            fields.put(colName, arrowFieldFor(colName, columnSources[i], descriptions.get(colName), inputTable, extraMetadata));
        }

        return new Schema(new ArrayList<>(fields.values()), schemaMetadata).getSchema(builder);
    }

    private static void putMetadata(final Map<String, String> metadata, final String key, final String value) {
        metadata.put("deephaven:" + key, value);
    }

    private static Class<?> getClassFor(final String className) throws ClassNotFoundException {
        Class<?> cls =  primitiveTypeNameMap.get(className);
        if (cls == null) {
            cls = Class.forName(className);
        }
        return cls;
    }

    private static Field arrowFieldFor(final String name, final ColumnSource<?> source, final String description, final MutableInputTable inputTable, final Map<String, String> extraMetadata) {
        List<Field> children = Collections.emptyList();

        // is hidden?
        final Class<?> type = source.getType();
        final Map<String, String> metadata = new HashMap<>(extraMetadata);

        if (type.isPrimitive() || supportedTypes.contains(type)) {
            putMetadata(metadata, "type", type.getCanonicalName());
        } else {
            // otherwise will be converted to a string
            putMetadata(metadata, "type", String.class.getCanonicalName());
        }

        // only one of these will be true, if any are true the column will not be visible
        putMetadata(metadata, "isStyle", name.endsWith(ColumnFormattingValues.TABLE_FORMAT_NAME) + "");
        putMetadata(metadata, "isRowStyle", name.equals(ColumnFormattingValues.ROW_FORMAT_NAME + ColumnFormattingValues.TABLE_FORMAT_NAME) + "");
        putMetadata(metadata, "isDateFormat", name.endsWith(ColumnFormattingValues.TABLE_DATE_FORMAT_NAME) + "");
        putMetadata(metadata, "isNumberFormat", name.endsWith(ColumnFormattingValues.TABLE_NUMERIC_FORMAT_NAME) + "");
        putMetadata(metadata, "isRollupColumn", name.equals(RollupInfo.ROLLUP_COLUMN) + "");

        if (description != null) {
            putMetadata(metadata, "description", description);
        }
        if (inputTable != null) {
            putMetadata(metadata, "inputtable.isKey", Arrays.asList(inputTable.getKeyNames()).contains(name) + "");
        }

        final FieldType fieldType = arrowFieldTypeFor(type, source.getComponentType(), metadata);
        if (fieldType.getType().isComplex()) {
            if (type.isArray()) {
                final Class<?> componentType = type.getComponentType();
                children = Collections.singletonList(new Field("", arrowFieldTypeFor(componentType, null, metadata), Collections.emptyList()));
            } else {
                throw new UnsupportedOperationException("Arrow Complex Type Not Supported: " + fieldType.getType());
            }
        }

        return new Field(name, fieldType, children);
    }

    private static FieldType arrowFieldTypeFor(final Class<?> type, final Class<?> componentType, final Map<String, String> metadata) {
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
                return Types.MinorType.VARCHAR.getType(); //aka Utf8
        }
        throw new IllegalStateException("No ArrowType for type: " + type + " w/chunkType: " + chunkType);
    }
}
