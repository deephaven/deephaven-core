//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.data;

import com.google.flatbuffers.FlatBufferBuilder;
import io.deephaven.extensions.barrage.BarrageTypeInfo;
import io.deephaven.web.client.api.BigDecimalWrapper;
import io.deephaven.web.client.api.BigIntegerWrapper;
import io.deephaven.web.client.api.DateWrapper;
import io.deephaven.web.client.api.LocalDateWrapper;
import io.deephaven.web.client.api.LocalTimeWrapper;
import io.deephaven.web.client.api.LongWrapper;
import io.deephaven.web.client.api.barrage.WebBarrageUtils;
import org.apache.arrow.flatbuf.Field;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Type;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;

/**
 * Type model that represents all Arrow field types and supported deephaven type metadata.
 */
public abstract sealed class BarrageColumnType
        permits BarrageColumnType.Null, BarrageColumnType.IntType, BarrageColumnType.FloatingPoint,
        BarrageColumnType.Binary, BarrageColumnType.Utf8, BarrageColumnType.Bool,
        BarrageColumnType.Decimal, BarrageColumnType.Date, BarrageColumnType.Time,
        BarrageColumnType.Timestamp, BarrageColumnType.Interval, BarrageColumnType.List,
        BarrageColumnType.Struct, BarrageColumnType.Union, BarrageColumnType.FixedSizeBinary,
        BarrageColumnType.FixedSizeList, BarrageColumnType.Map, BarrageColumnType.Duration,
        BarrageColumnType.LargeBinary, BarrageColumnType.LargeUtf8, BarrageColumnType.LargeList,
        BarrageColumnType.RunEndEncoded, BarrageColumnType.BinaryView, BarrageColumnType.Utf8View,
        BarrageColumnType.ListView, BarrageColumnType.LargeListView {

    /**
     * Best effort mapping from string type info to supported Flight/Deephaven types.
     * 
     * @param deephavenType supports deephaven:type strings, as well as some specific JS shorthand
     * @return
     */
    public static BarrageColumnType fromString(String columnName, String deephavenType) {
        if (deephavenType.endsWith("[]")) {
            String componentType = deephavenType.substring(0, deephavenType.length() - 2);
            BarrageColumnType componentColumnType = fromString(null, componentType);
            return new List(columnName, componentColumnType);
        }

        switch (deephavenType) {
            case "string":
            case "java.lang.String":
                return new Utf8(columnName);
            case "java.time.Instant":
            case "datetime":
            case "java.time.ZonedDateTime":
                return new Timestamp(columnName, org.apache.arrow.flatbuf.TimeUnit.NANOSECOND, "UTC");
            case "int":
                return new IntType(columnName, 32, true);
            case "short":
                return new IntType(columnName, 16, true);
            case "long":
                return new IntType(columnName, 64, true);
            case "byte":
                return new IntType(columnName, 8, true);
            case "char":
                return new IntType(columnName, 16, false);
            case "float":
                return new FloatingPoint(columnName, org.apache.arrow.flatbuf.Precision.SINGLE);
            case "double":
                return new FloatingPoint(columnName, org.apache.arrow.flatbuf.Precision.DOUBLE);
            case "java.lang.Boolean":
            case "boolean":
            case "bool":
                return new Bool(columnName);
            case "java.math.BigDecimal":
                return new Binary(columnName, "java.math.BigDecimal");
            case "java.math.BigInteger":
                return new Binary(columnName, "java.math.BigInteger");
            case "java.time.LocalDate":
            case "localdate":
                return new Date(columnName, org.apache.arrow.flatbuf.DateUnit.DAY);
            case "java.time.LocalTime":
            case "localtime":
                return new Time(columnName, org.apache.arrow.flatbuf.TimeUnit.NANOSECOND, 64);
            default:
                throw new IllegalArgumentException("Unsupported deephaven type: " + deephavenType);
        }
    }

    public static BarrageColumnType fromArrowField(Field field) {
        java.util.Map<String, String> customMetadata =
                WebBarrageUtils.keyValuePairs("", field.customMetadataLength(), field::customMetadata);
        String columnName = field.name();
        switch (field.typeType()) {
            case Type.Null:
                return new Null(columnName, customMetadata);
            case Type.Int: {
                org.apache.arrow.flatbuf.Int intType = new org.apache.arrow.flatbuf.Int();
                field.type(intType);
                return new IntType(columnName, intType.bitWidth(), intType.isSigned(), customMetadata);
            }
            case Type.FloatingPoint: {
                org.apache.arrow.flatbuf.FloatingPoint fpType = new org.apache.arrow.flatbuf.FloatingPoint();
                field.type(fpType);
                return new FloatingPoint(columnName, fpType.precision(), customMetadata);
            }
            case Type.Binary:
                return new Binary(columnName, customMetadata.getOrDefault("deephaven:type", "java.lang.Object"),
                        customMetadata);
            case Type.Utf8:
                return new Utf8(columnName, customMetadata);
            case Type.Bool:
                return new Bool(columnName, customMetadata);
            case Type.Decimal: {
                org.apache.arrow.flatbuf.Decimal decType = new org.apache.arrow.flatbuf.Decimal();
                field.type(decType);
                return new Decimal(columnName, decType.precision(), decType.scale(), decType.bitWidth(),
                        customMetadata);
            }
            case Type.Date: {
                org.apache.arrow.flatbuf.Date dateType = new org.apache.arrow.flatbuf.Date();
                field.type(dateType);
                return new Date(columnName, dateType.unit(), customMetadata);
            }
            case Type.Time: {
                org.apache.arrow.flatbuf.Time timeType = new org.apache.arrow.flatbuf.Time();
                field.type(timeType);
                return new Time(columnName, timeType.unit(), timeType.bitWidth(), customMetadata);
            }
            case Type.Timestamp: {
                org.apache.arrow.flatbuf.Timestamp tsType = new org.apache.arrow.flatbuf.Timestamp();
                field.type(tsType);
                return new Timestamp(columnName, tsType.unit(), tsType.timezone(), customMetadata);
            }
            case Type.Interval: {
                org.apache.arrow.flatbuf.Interval intervalType = new org.apache.arrow.flatbuf.Interval();
                field.type(intervalType);
                return new Interval(columnName, intervalType.unit(), customMetadata);
            }
            case Type.List:
                return new List(columnName, fromArrowField(field.children(0)), customMetadata);
            case Type.Struct_: {
                java.util.List<BarrageColumnType> fields = new ArrayList<>(field.childrenLength());
                for (int i = 0; i < field.childrenLength(); i++) {
                    fields.add(fromArrowField(field.children(i)));
                }
                return new Struct(columnName, Collections.unmodifiableList(fields), customMetadata);
            }
            case Type.Union: {
                org.apache.arrow.flatbuf.Union unionType = new org.apache.arrow.flatbuf.Union();
                field.type(unionType);
                int[] typeIds = new int[unionType.typeIdsLength()];
                for (int i = 0; i < typeIds.length; i++) {
                    typeIds[i] = unionType.typeIds(i);
                }
                java.util.List<BarrageColumnType> fields = new ArrayList<>(field.childrenLength());
                for (int i = 0; i < field.childrenLength(); i++) {
                    fields.add(fromArrowField(field.children(i)));
                }
                return new Union(columnName, unionType.mode(), typeIds, Collections.unmodifiableList(fields),
                        customMetadata);
            }
            case Type.FixedSizeBinary: {
                org.apache.arrow.flatbuf.FixedSizeBinary fsbType = new org.apache.arrow.flatbuf.FixedSizeBinary();
                field.type(fsbType);
                return new FixedSizeBinary(columnName, fsbType.byteWidth(), customMetadata);
            }
            case Type.FixedSizeList: {
                org.apache.arrow.flatbuf.FixedSizeList fslType = new org.apache.arrow.flatbuf.FixedSizeList();
                field.type(fslType);
                return new FixedSizeList(columnName, fslType.listSize(), fromArrowField(field.children(0)),
                        customMetadata);
            }
            case Type.Map: {
                org.apache.arrow.flatbuf.Map mapType = new org.apache.arrow.flatbuf.Map();
                field.type(mapType);
                Field entriesField = field.children(0);
                return new Map(columnName, mapType.keysSorted(), fromArrowField(entriesField.children(0)),
                        fromArrowField(entriesField.children(1)), customMetadata);
            }
            case Type.Duration: {
                org.apache.arrow.flatbuf.Duration durType = new org.apache.arrow.flatbuf.Duration();
                field.type(durType);
                return new Duration(columnName, durType.unit(), customMetadata);
            }
            case Type.LargeBinary:
                return new LargeBinary(columnName, customMetadata);
            case Type.LargeUtf8:
                return new LargeUtf8(columnName, customMetadata);
            case Type.LargeList:
                return new LargeList(columnName, fromArrowField(field.children(0)), customMetadata);
            case Type.RunEndEncoded:
                return new RunEndEncoded(columnName, fromArrowField(field.children(0)),
                        fromArrowField(field.children(1)), customMetadata);
            case Type.BinaryView:
                return new BinaryView(columnName, customMetadata);
            case Type.Utf8View:
                return new Utf8View(columnName, customMetadata);
            case Type.ListView:
                return new ListView(columnName, fromArrowField(field.children(0)), customMetadata);
            case Type.LargeListView:
                return new LargeListView(columnName, fromArrowField(field.children(0)), customMetadata);
            default:
                throw new IllegalArgumentException("Unsupported Arrow type: " + Type.name(field.typeType()));
        }
    }

    @Nullable
    private final String columnName;
    private final java.util.Map<String, String> customMetadata;

    protected BarrageColumnType(@Nullable String columnName, java.util.Map<String, String> customMetadata) {
        this.columnName = columnName;
        this.customMetadata = Collections.unmodifiableMap(customMetadata);
    }

    protected BarrageColumnType(@Nullable String columnName) {
        this(columnName, java.util.Map.of());
    }

    public @Nullable String getColumnName() {
        return columnName;
    }

    public java.util.Map<String, String> customMetadata() {
        return customMetadata;
    }

    public abstract byte typeType();

    public abstract int writeType(FlatBufferBuilder builder);

    public int writeField(FlatBufferBuilder builder) {
        int typeOffset = writeType(builder);

        // Merge stored custom metadata with deephaven:type (stored metadata takes precedence)
        java.util.LinkedHashMap<String, String> mergedMetadata = new LinkedHashMap<>(customMetadata);
        if (!deephavenType().equals("java.lang.Object") && !mergedMetadata.containsKey("deephaven:type")) {
            mergedMetadata.put("deephaven:type", deephavenType());
        }

        int[] kvOffsets = new int[mergedMetadata.size()];
        int idx = 0;
        for (java.util.Map.Entry<String, String> entry : mergedMetadata.entrySet()) {
            kvOffsets[idx++] = KeyValue.createKeyValue(builder,
                    builder.createString(entry.getKey()),
                    builder.createString(entry.getValue()));
        }
        int metadataOffset = Field.createCustomMetadataVector(builder, kvOffsets);

        int[] childrenOffsets = writeChildren(builder);
        int childrenVector = Integer.MAX_VALUE;
        if (childrenOffsets != null) {
            childrenVector = Field.createChildrenVector(builder, childrenOffsets);
        }

        final int nameOffset;
        if (columnName != null) {
            nameOffset = builder.createString(columnName);
        } else {
            // A name is required for nested fields by the server implementation
            nameOffset = builder.createString("");
        }
        Field.startField(builder);
        Field.addNullable(builder, true);
        Field.addName(builder, nameOffset);
        Field.addTypeType(builder, typeType());
        Field.addType(builder, typeOffset);
        Field.addCustomMetadata(builder, metadataOffset);

        if (childrenOffsets != null) {
            Field.addChildren(builder, childrenVector);
        }

        return Field.endField(builder);
    }

    protected int[] writeChildren(FlatBufferBuilder builder) {
        return null;
    }

    public abstract Class<?> type();

    public @Nullable Class<?> componentType() {
        return null;
    }

    public abstract String deephavenType();

    public BarrageTypeInfo<Field> typeInfo() {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        builder.finish(writeField(builder));
        Field f = new Field();
        Field.getRootAsField(builder.dataBuffer(), f);
        return BarrageTypeInfo.make(type(), componentType(), f);
    }

    public String getDeephavenColumnAttr(String string) {
        return customMetadata.get("deephaven:" + string);
    }

    public static final class Null extends BarrageColumnType {
        public Null(@Nullable String columnName, java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            org.apache.arrow.flatbuf.Null.startNull(builder);
            return org.apache.arrow.flatbuf.Null.endNull(builder);
        }

        @Override
        public byte typeType() {
            return Type.Null;
        }

        @Override
        public String deephavenType() {
            return "java.lang.Object";
        }

        @Override
        public Class<?> type() {
            return Object.class;
        }
    }

    public static final class IntType extends BarrageColumnType {
        private final int bitWidth;
        private final boolean isSigned;

        public IntType(@Nullable String columnName, int bitWidth, boolean isSigned) {
            super(columnName);
            this.bitWidth = bitWidth;
            this.isSigned = isSigned;
        }

        public IntType(@Nullable String columnName, int bitWidth, boolean isSigned,
                java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
            this.bitWidth = bitWidth;
            this.isSigned = isSigned;
        }

        public int bitWidth() {
            return bitWidth;
        }

        public boolean isSigned() {
            return isSigned;
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            return org.apache.arrow.flatbuf.Int.createInt(builder, bitWidth, isSigned);
        }

        @Override
        public byte typeType() {
            return Type.Int;
        }

        @Override
        public String deephavenType() {
            if (isSigned) {
                switch (bitWidth) {
                    case 8:
                        return "byte";
                    case 16:
                        return "short";
                    case 32:
                        return "int";
                    case 64:
                        return "long";
                    default:
                        throw new IllegalStateException("Unsupported signed int bitWidth: " + bitWidth);
                }
            } else {
                switch (bitWidth) {
                    case 16:
                        return "char";
                    default:
                        throw new IllegalStateException("Unsupported unsigned int bitWidth: " + bitWidth);
                }
            }
        }

        @Override
        public Class<?> type() {
            if (isSigned) {
                switch (bitWidth) {
                    case 8:
                        return byte.class;
                    case 16:
                        return short.class;
                    case 32:
                        return int.class;
                    case 64:
                        return LongWrapper.class;
                    default:
                        throw new IllegalStateException("Unsupported signed int bitWidth: " + bitWidth);
                }
            } else {
                switch (bitWidth) {
                    case 16:
                        return char.class;
                    default:
                        throw new IllegalStateException("Unsupported unsigned int bitWidth: " + bitWidth);
                }
            }
        }
    }

    public static final class FloatingPoint extends BarrageColumnType {
        private final short precision;

        public FloatingPoint(@Nullable String columnName, short precision) {
            super(columnName);
            this.precision = precision;
        }

        public FloatingPoint(@Nullable String columnName, short precision,
                java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
            this.precision = precision;
        }

        /**
         * @return precision value from {@link org.apache.arrow.flatbuf.Precision}
         */
        public short precision() {
            return precision;
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            return org.apache.arrow.flatbuf.FloatingPoint.createFloatingPoint(builder, precision);
        }

        @Override
        public byte typeType() {
            return Type.FloatingPoint;
        }

        @Override
        public String deephavenType() {
            switch (precision) {
                case org.apache.arrow.flatbuf.Precision.SINGLE:
                    return "float";
                case org.apache.arrow.flatbuf.Precision.DOUBLE:
                    return "double";
                default:
                    throw new IllegalStateException("Unsupported floating point precision: " + precision);
            }
        }

        @Override
        public Class<?> type() {
            switch (precision) {
                case org.apache.arrow.flatbuf.Precision.SINGLE:
                    return float.class;
                case org.apache.arrow.flatbuf.Precision.DOUBLE:
                    return double.class;
                default:
                    throw new IllegalStateException("Unsupported floating point precision: " + precision);
            }
        }
    }

    public static final class Binary extends BarrageColumnType {
        private final String deephavenType;

        public Binary(@Nullable String columnName, String deephavenType) {
            super(columnName);
            this.deephavenType = deephavenType;
        }

        public Binary(@Nullable String columnName, String deephavenType, java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
            this.deephavenType = deephavenType;
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            org.apache.arrow.flatbuf.Binary.startBinary(builder);
            return org.apache.arrow.flatbuf.Binary.endBinary(builder);
        }

        @Override
        public byte typeType() {
            return Type.Binary;
        }

        @Override
        public String deephavenType() {
            return deephavenType;
        }

        @Override
        public Class<?> type() {
            // Only two types are explicitly supported as Binary by deephaven, interpret the rest as object
            if (deephavenType.equals("java.math.BigDecimal")) {
                return BigDecimalWrapper.class;
            }
            if (deephavenType.equals("java.math.BigInteger")) {
                return BigIntegerWrapper.class;
            }
            return Object.class;
        }
    }

    public static final class Utf8 extends BarrageColumnType {
        public Utf8(@Nullable String columnName) {
            super(columnName);
        }

        public Utf8(@Nullable String columnName, java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            org.apache.arrow.flatbuf.Utf8.startUtf8(builder);
            return org.apache.arrow.flatbuf.Utf8.endUtf8(builder);
        }

        @Override
        public byte typeType() {
            return Type.Utf8;
        }

        @Override
        public String deephavenType() {
            return "java.lang.String";
        }

        @Override
        public Class<?> type() {
            return String.class;
        }
    }

    public static final class Bool extends BarrageColumnType {
        public Bool(@Nullable String columnName) {
            super(columnName);
        }

        public Bool(@Nullable String columnName, java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            org.apache.arrow.flatbuf.Bool.startBool(builder);
            return org.apache.arrow.flatbuf.Bool.endBool(builder);
        }

        @Override
        public byte typeType() {
            return Type.Bool;
        }

        @Override
        public String deephavenType() {
            return "java.lang.Boolean";
        }

        @Override
        public Class<?> type() {
            return boolean.class;
        }
    }

    public static final class Decimal extends BarrageColumnType {
        private final int precision;
        private final int scale;
        private final int bitWidth;

        public Decimal(@Nullable String columnName, int precision, int scale, int bitWidth) {
            super(columnName);
            this.precision = precision;
            this.scale = scale;
            this.bitWidth = bitWidth;
        }

        public Decimal(@Nullable String columnName, int precision, int scale, int bitWidth,
                java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
            this.precision = precision;
            this.scale = scale;
            this.bitWidth = bitWidth;
        }

        public int precision() {
            return precision;
        }

        public int scale() {
            return scale;
        }

        public int bitWidth() {
            return bitWidth;
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            return org.apache.arrow.flatbuf.Decimal.createDecimal(builder, precision, scale, bitWidth);
        }

        @Override
        public byte typeType() {
            return Type.Decimal;
        }

        @Override
        public String deephavenType() {
            return "java.math.BigDecimal";
        }

        @Override
        public Class<?> type() {
            return BigDecimalWrapper.class;
        }
    }

    public static final class Date extends BarrageColumnType {
        private final short unit;

        public Date(@Nullable String columnName, short unit) {
            super(columnName);
            this.unit = unit;
        }

        public Date(@Nullable String columnName, short unit, java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
            this.unit = unit;
        }

        /**
         * @return unit value from {@link org.apache.arrow.flatbuf.DateUnit}
         */
        public short unit() {
            return unit;
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            return org.apache.arrow.flatbuf.Date.createDate(builder, unit);
        }

        @Override
        public byte typeType() {
            return Type.Date;
        }

        @Override
        public String deephavenType() {
            return "java.time.LocalDate";
        }

        @Override
        public Class<?> type() {
            return LocalDateWrapper.class;
        }
    }

    public static final class Time extends BarrageColumnType {
        private final short unit;
        private final int bitWidth;

        public Time(@Nullable String columnName, short unit, int bitWidth) {
            super(columnName);
            this.unit = unit;
            this.bitWidth = bitWidth;
        }

        public Time(@Nullable String columnName, short unit, int bitWidth,
                java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
            this.unit = unit;
            this.bitWidth = bitWidth;
        }

        /**
         * @return unit value from {@link org.apache.arrow.flatbuf.TimeUnit}
         */
        public short unit() {
            return unit;
        }

        public int bitWidth() {
            return bitWidth;
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            return org.apache.arrow.flatbuf.Time.createTime(builder, unit, bitWidth);
        }

        @Override
        public byte typeType() {
            return Type.Time;
        }

        @Override
        public String deephavenType() {
            return "java.time.LocalTime";
        }

        @Override
        public Class<?> type() {
            return LocalTimeWrapper.class;
        }
    }

    public static final class Timestamp extends BarrageColumnType {
        private final short unit;
        @Nullable
        private final String timezone;

        public Timestamp(@Nullable String columnName, short unit, @Nullable String timezone) {
            super(columnName);
            this.unit = unit;
            this.timezone = timezone;
        }

        public Timestamp(@Nullable String columnName, short unit, @Nullable String timezone,
                java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
            this.unit = unit;
            this.timezone = timezone;
        }

        /**
         * @return unit value from {@link org.apache.arrow.flatbuf.TimeUnit}
         */
        public short unit() {
            return unit;
        }

        @Nullable
        public String timezone() {
            return timezone;
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            int tzOffset = timezone != null ? builder.createString(timezone) : 0;
            return org.apache.arrow.flatbuf.Timestamp.createTimestamp(builder, unit, tzOffset);
        }

        @Override
        public byte typeType() {
            return Type.Timestamp;
        }

        @Override
        public String deephavenType() {
            return "java.time.Instant";
        }

        @Override
        public Class<?> type() {
            return DateWrapper.class;
        }
    }

    public static final class Interval extends BarrageColumnType {
        private final short unit;

        public Interval(@Nullable String columnName, short unit, java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
            this.unit = unit;
        }

        /**
         * @return unit value from {@link org.apache.arrow.flatbuf.IntervalUnit}
         */
        public short unit() {
            return unit;
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            return org.apache.arrow.flatbuf.Interval.createInterval(builder, unit);
        }

        @Override
        public byte typeType() {
            return Type.Interval;
        }

        @Override
        public String deephavenType() {
            return "java.time.Duration";
        }

        @Override
        public Class<?> type() {
            return Object.class;
        }
    }

    public static final class List extends BarrageColumnType {
        private final BarrageColumnType componentType;

        public List(@Nullable String columnName, BarrageColumnType componentType) {
            super(columnName);
            this.componentType = componentType;
        }

        public List(@Nullable String columnName, BarrageColumnType componentType,
                java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
            this.componentType = componentType;
        }

        public BarrageColumnType arrayComponentType() {
            return componentType;
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            org.apache.arrow.flatbuf.List.startList(builder);
            return org.apache.arrow.flatbuf.List.endList(builder);
        }

        @Override
        public byte typeType() {
            return Type.List;
        }

        @Override
        public String deephavenType() {
            return componentType.deephavenType() + "[]";
        }

        @Override
        protected int[] writeChildren(FlatBufferBuilder builder) {
            return new int[] {componentType.writeField(builder)};
        }

        @Override
        public Class<?> type() {
            return Object.class;
        }

        @Override
        public Class<?> componentType() {
            return componentType.type();
        }
    }

    public static final class Struct extends BarrageColumnType {
        private final java.util.List<BarrageColumnType> fields;

        public Struct(@Nullable String columnName, java.util.List<BarrageColumnType> fields,
                java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
            this.fields = fields;
        }

        public java.util.List<BarrageColumnType> fields() {
            return fields;
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            org.apache.arrow.flatbuf.Struct_.startStruct_(builder);
            return org.apache.arrow.flatbuf.Struct_.endStruct_(builder);
        }

        @Override
        public byte typeType() {
            return Type.Struct_;
        }

        @Override
        public String deephavenType() {
            return "java.lang.Object";
        }

        @Override
        protected int[] writeChildren(FlatBufferBuilder builder) {
            int[] offsets = new int[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                offsets[i] = fields.get(i).writeField(builder);
            }
            return offsets;
        }

        @Override
        public Class<?> type() {
            return Object.class;
        }
    }

    public static final class Union extends BarrageColumnType {
        private final short mode;
        private final int[] typeIds;
        private final java.util.List<BarrageColumnType> fields;

        public Union(@Nullable String columnName, short mode, int[] typeIds, java.util.List<BarrageColumnType> fields,
                java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
            this.mode = mode;
            this.typeIds = typeIds;
            this.fields = fields;
        }

        /**
         * @return mode value from {@link org.apache.arrow.flatbuf.UnionMode}
         */
        public short mode() {
            return mode;
        }

        public int[] typeIds() {
            return typeIds;
        }

        public java.util.List<BarrageColumnType> fields() {
            return fields;
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            int typeIdsOffset = org.apache.arrow.flatbuf.Union.createTypeIdsVector(builder, typeIds);
            return org.apache.arrow.flatbuf.Union.createUnion(builder, mode, typeIdsOffset);
        }

        @Override
        public byte typeType() {
            return Type.Union;
        }

        @Override
        public String deephavenType() {
            return "java.lang.Object";
        }

        @Override
        protected int[] writeChildren(FlatBufferBuilder builder) {
            int[] offsets = new int[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                offsets[i] = fields.get(i).writeField(builder);
            }
            return offsets;
        }

        @Override
        public Class<?> type() {
            return Object.class;
        }
    }

    public static final class FixedSizeBinary extends BarrageColumnType {
        private final int byteWidth;

        public FixedSizeBinary(@Nullable String columnName, int byteWidth,
                java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
            this.byteWidth = byteWidth;
        }

        public int byteWidth() {
            return byteWidth;
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            return org.apache.arrow.flatbuf.FixedSizeBinary.createFixedSizeBinary(builder, byteWidth);
        }

        @Override
        public byte typeType() {
            return Type.FixedSizeBinary;
        }

        @Override
        public String deephavenType() {
            return "byte[]";
        }

        @Override
        public Class<?> type() {
            return Object.class;
        }

        @Override
        public Class<?> componentType() {
            return byte.class;
        }
    }

    public static final class FixedSizeList extends BarrageColumnType {
        private final int listSize;
        private final BarrageColumnType componentType;

        public FixedSizeList(@Nullable String columnName, int listSize, BarrageColumnType componentType,
                java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
            this.listSize = listSize;
            this.componentType = componentType;
        }

        public int listSize() {
            return listSize;
        }

        public BarrageColumnType listComponentType() {
            return componentType;
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            return org.apache.arrow.flatbuf.FixedSizeList.createFixedSizeList(builder, listSize);
        }

        @Override
        public byte typeType() {
            return Type.FixedSizeList;
        }

        @Override
        public String deephavenType() {
            return componentType.deephavenType() + "[]";
        }

        @Override
        protected int[] writeChildren(FlatBufferBuilder builder) {
            return new int[] {componentType.writeField(builder)};
        }

        @Override
        public Class<?> type() {
            return Object.class;
        }

        @Override
        public Class<?> componentType() {
            return componentType.type();
        }
    }

    public static final class Map extends BarrageColumnType {
        private final boolean keysSorted;
        private final BarrageColumnType keyType;
        private final BarrageColumnType valueType;

        public Map(@Nullable String columnName, boolean keysSorted, BarrageColumnType keyType,
                BarrageColumnType valueType, java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
            this.keysSorted = keysSorted;
            this.keyType = keyType;
            this.valueType = valueType;
        }

        public boolean keysSorted() {
            return keysSorted;
        }

        public BarrageColumnType keyType() {
            return keyType;
        }

        public BarrageColumnType valueType() {
            return valueType;
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            return org.apache.arrow.flatbuf.Map.createMap(builder, keysSorted);
        }

        @Override
        public byte typeType() {
            return Type.Map;
        }

        @Override
        public String deephavenType() {
            return "java.lang.Object";
        }

        @Override
        protected int[] writeChildren(FlatBufferBuilder builder) {
            // Map has a single "entries" child which is a Struct with key and value children
            int keyFieldOffset = keyType.writeField(builder);
            int valueFieldOffset = valueType.writeField(builder);
            int entriesChildrenVector =
                    Field.createChildrenVector(builder, new int[] {keyFieldOffset, valueFieldOffset});

            // Write the Struct_ type for the entries field
            org.apache.arrow.flatbuf.Struct_.startStruct_(builder);
            int entriesTypeOffset = org.apache.arrow.flatbuf.Struct_.endStruct_(builder);

            int entriesNameOffset = builder.createString("entries");
            Field.startField(builder);
            Field.addName(builder, entriesNameOffset);
            Field.addNullable(builder, false);
            Field.addTypeType(builder, Type.Struct_);
            Field.addType(builder, entriesTypeOffset);
            Field.addChildren(builder, entriesChildrenVector);
            int entriesFieldOffset = Field.endField(builder);

            return new int[] {entriesFieldOffset};
        }

        @Override
        public Class<?> type() {
            return Object.class;
        }
    }

    public static final class Duration extends BarrageColumnType {
        private final short unit;

        public Duration(@Nullable String columnName, short unit, java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
            this.unit = unit;
        }

        /**
         * @return unit value from {@link org.apache.arrow.flatbuf.TimeUnit}
         */
        public short unit() {
            return unit;
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            return org.apache.arrow.flatbuf.Duration.createDuration(builder, unit);
        }

        @Override
        public byte typeType() {
            return Type.Duration;
        }

        @Override
        public String deephavenType() {
            return "java.time.Duration";
        }

        @Override
        public Class<?> type() {
            return Object.class;
        }
    }

    public static final class LargeBinary extends BarrageColumnType {
        public LargeBinary(@Nullable String columnName, java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            org.apache.arrow.flatbuf.LargeBinary.startLargeBinary(builder);
            return org.apache.arrow.flatbuf.LargeBinary.endLargeBinary(builder);
        }

        @Override
        public byte typeType() {
            return Type.LargeBinary;
        }

        @Override
        public String deephavenType() {
            return "java.math.BigInteger";
        }

        @Override
        public Class<?> type() {
            return BigIntegerWrapper.class;
        }
    }

    public static final class LargeUtf8 extends BarrageColumnType {
        public LargeUtf8(@Nullable String columnName, java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            org.apache.arrow.flatbuf.LargeUtf8.startLargeUtf8(builder);
            return org.apache.arrow.flatbuf.LargeUtf8.endLargeUtf8(builder);
        }

        @Override
        public byte typeType() {
            return Type.LargeUtf8;
        }

        @Override
        public String deephavenType() {
            return "java.lang.String";
        }

        @Override
        public Class<?> type() {
            return String.class;
        }
    }

    public static final class LargeList extends BarrageColumnType {
        private final BarrageColumnType componentType;

        public LargeList(@Nullable String columnName, BarrageColumnType componentType,
                java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
            this.componentType = componentType;
        }

        public BarrageColumnType listComponentType() {
            return componentType;
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            org.apache.arrow.flatbuf.LargeList.startLargeList(builder);
            return org.apache.arrow.flatbuf.LargeList.endLargeList(builder);
        }

        @Override
        public byte typeType() {
            return Type.LargeList;
        }

        @Override
        public String deephavenType() {
            return componentType.deephavenType() + "[]";
        }

        @Override
        protected int[] writeChildren(FlatBufferBuilder builder) {
            return new int[] {componentType.writeField(builder)};
        }

        @Override
        public Class<?> type() {
            return Object.class;
        }

        @Override
        public Class<?> componentType() {
            return componentType.type();
        }
    }

    public static final class RunEndEncoded extends BarrageColumnType {
        private final BarrageColumnType runEndsType;
        private final BarrageColumnType valuesType;

        public RunEndEncoded(@Nullable String columnName, BarrageColumnType runEndsType, BarrageColumnType valuesType,
                java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
            this.runEndsType = runEndsType;
            this.valuesType = valuesType;
        }

        public BarrageColumnType runEndsType() {
            return runEndsType;
        }

        public BarrageColumnType valuesType() {
            return valuesType;
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            org.apache.arrow.flatbuf.RunEndEncoded.startRunEndEncoded(builder);
            return org.apache.arrow.flatbuf.RunEndEncoded.endRunEndEncoded(builder);
        }

        @Override
        public byte typeType() {
            return Type.RunEndEncoded;
        }

        @Override
        public String deephavenType() {
            return valuesType.deephavenType();
        }

        @Override
        protected int[] writeChildren(FlatBufferBuilder builder) {
            return new int[] {runEndsType.writeField(builder), valuesType.writeField(builder)};
        }

        @Override
        public Class<?> type() {
            return valuesType.type();
        }
    }

    public static final class BinaryView extends BarrageColumnType {
        public BinaryView(@Nullable String columnName) {
            super(columnName);
        }

        public BinaryView(@Nullable String columnName, java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            org.apache.arrow.flatbuf.BinaryView.startBinaryView(builder);
            return org.apache.arrow.flatbuf.BinaryView.endBinaryView(builder);
        }

        @Override
        public byte typeType() {
            return Type.BinaryView;
        }

        @Override
        public String deephavenType() {
            return "byte[]";
        }

        @Override
        public Class<?> type() {
            return Object.class;
        }

        @Override
        public Class<?> componentType() {
            return byte.class;
        }
    }

    public static final class Utf8View extends BarrageColumnType {
        public Utf8View(@Nullable String columnName) {
            super(columnName);
        }

        public Utf8View(@Nullable String columnName, java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            org.apache.arrow.flatbuf.Utf8View.startUtf8View(builder);
            return org.apache.arrow.flatbuf.Utf8View.endUtf8View(builder);
        }

        @Override
        public byte typeType() {
            return Type.Utf8View;
        }

        @Override
        public String deephavenType() {
            return "java.lang.String";
        }

        @Override
        public Class<?> type() {
            return String.class;
        }
    }

    public static final class ListView extends BarrageColumnType {
        private final BarrageColumnType componentType;

        public ListView(@Nullable String columnName, BarrageColumnType componentType) {
            super(columnName);
            this.componentType = componentType;
        }

        public ListView(@Nullable String columnName, BarrageColumnType componentType,
                java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
            this.componentType = componentType;
        }

        public BarrageColumnType listComponentType() {
            return componentType;
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            org.apache.arrow.flatbuf.ListView.startListView(builder);
            return org.apache.arrow.flatbuf.ListView.endListView(builder);
        }

        @Override
        public byte typeType() {
            return Type.ListView;
        }

        @Override
        public String deephavenType() {
            return componentType.deephavenType() + "[]";
        }

        @Override
        protected int[] writeChildren(FlatBufferBuilder builder) {
            return new int[] {componentType.writeField(builder)};
        }

        @Override
        public Class<?> type() {
            return Object.class;
        }

        @Override
        public @Nullable Class<?> componentType() {
            return componentType.type();
        }
    }

    public static final class LargeListView extends BarrageColumnType {
        private final BarrageColumnType componentType;

        public LargeListView(@Nullable String columnName, BarrageColumnType componentType,
                java.util.Map<String, String> customMetadata) {
            super(columnName, customMetadata);
            this.componentType = componentType;
        }

        public BarrageColumnType listComponentType() {
            return componentType;
        }

        @Override
        public int writeType(FlatBufferBuilder builder) {
            org.apache.arrow.flatbuf.LargeListView.startLargeListView(builder);
            return org.apache.arrow.flatbuf.LargeListView.endLargeListView(builder);
        }

        @Override
        public byte typeType() {
            return Type.LargeListView;
        }

        @Override
        public String deephavenType() {
            return componentType.deephavenType() + "[]";
        }

        @Override
        protected int[] writeChildren(FlatBufferBuilder builder) {
            return new int[] {componentType.writeField(builder)};
        }

        @Override
        public Class<?> type() {
            return Object.class;
        }

        @Override
        public @Nullable Class<?> componentType() {
            return componentType.type();
        }
    }
}
