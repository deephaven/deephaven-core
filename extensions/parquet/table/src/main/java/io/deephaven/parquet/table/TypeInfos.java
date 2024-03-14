//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.CodecLookup;
import io.deephaven.stringset.StringSet;
import io.deephaven.util.codec.ExternalizableCodec;
import io.deephaven.util.codec.SerializableCodec;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.PrimitiveBuilder;
import org.jetbrains.annotations.NotNull;

import java.io.Externalizable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Supplier;

import static io.deephaven.engine.util.BigDecimalUtils.PrecisionAndScale;
import static io.deephaven.engine.util.BigDecimalUtils.computePrecisionAndScale;

/**
 * Contains the necessary information to convert a Deephaven table into a Parquet table. Both the schema translation,
 * and the data translation.
 */
public class TypeInfos {
    private static final TypeInfo[] TYPE_INFOS = new TypeInfo[] {
            IntType.INSTANCE,
            LongType.INSTANCE,
            ShortType.INSTANCE,
            BooleanType.INSTANCE,
            FloatType.INSTANCE,
            DoubleType.INSTANCE,
            CharType.INSTANCE,
            ByteType.INSTANCE,
            StringType.INSTANCE,
            InstantType.INSTANCE,
            BigIntegerType.INSTANCE,
            LocalDateType.INSTANCE,
            LocalTimeType.INSTANCE,
            LocalDateTimeType.INSTANCE,
    };

    private static final Map<Class<?>, TypeInfo> BY_CLASS;

    static {
        final Map<Class<?>, TypeInfo> fa = new HashMap<>();
        for (final TypeInfo typeInfo : TYPE_INFOS) {
            for (final Class<?> type : typeInfo.getTypes()) {
                fa.put(type, typeInfo);
            }
        }
        BY_CLASS = Collections.unmodifiableMap(fa);
    }

    private static Optional<TypeInfo> lookupTypeInfo(@NotNull final Class<?> clazz) {
        return Optional.ofNullable(BY_CLASS.get(clazz));
    }

    private static TypeInfo lookupTypeInfo(
            @NotNull final ColumnDefinition<?> column,
            @NotNull final ParquetInstructions instructions) {
        if (CodecLookup.codecRequired(column)
                || CodecLookup.explicitCodecPresent(instructions.getCodecName(column.getName()))) {
            return new CodecType<>();
        }
        final Class<?> componentType = column.getComponentType();
        if (componentType != null) {
            return lookupTypeInfo(componentType).orElseThrow(IllegalStateException::new);
        }
        final Class<?> dataType = column.getDataType();
        if (StringSet.class.isAssignableFrom(dataType)) {
            return lookupTypeInfo(String.class).orElseThrow(IllegalStateException::new);
        }
        return lookupTypeInfo(dataType).orElseThrow(IllegalStateException::new);
    }

    static Pair<String, String> getCodecAndArgs(
            @NotNull final ColumnDefinition<?> columnDefinition,
            @NotNull final ParquetInstructions instructions) {
        // Explicit codecs always take precedence
        final String colName = columnDefinition.getName();
        final String codecNameFromInstructions = instructions.getCodecName(colName);
        if (CodecLookup.explicitCodecPresent(codecNameFromInstructions)) {
            return new ImmutablePair<>(codecNameFromInstructions, instructions.getCodecArgs(colName));
        }
        // No need to impute a codec for any basic formats we already understand
        if (!CodecLookup.codecRequired(columnDefinition)) {
            return null;
        }

        // Impute an appropriate codec for the data type
        final Class<?> dataType = columnDefinition.getDataType();
        if (Externalizable.class.isAssignableFrom(dataType)) {
            return new ImmutablePair<>(ExternalizableCodec.class.getName(), dataType.getName());
        }
        return new ImmutablePair<>(SerializableCodec.class.getName(), null);
    }

    public static PrecisionAndScale getPrecisionAndScale(
            @NotNull final Map<String, Map<ParquetCacheTags, Object>> computedCache,
            @NotNull final String columnName,
            @NotNull final RowSet rowSet,
            @NotNull Supplier<ColumnSource<BigDecimal>> columnSourceSupplier) {
        return (PrecisionAndScale) computedCache
                .computeIfAbsent(columnName, unusedColumnName -> new HashMap<>())
                .computeIfAbsent(ParquetCacheTags.DECIMAL_ARGS,
                        uct -> parquetCompatible(computePrecisionAndScale(rowSet, columnSourceSupplier.get())));
    }

    private static PrecisionAndScale parquetCompatible(PrecisionAndScale pas) {
        // Parquet / SQL has a more limited range for DECIMAL(precision, scale).
        // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal
        // Scale must be zero or a positive integer less than the precision.
        // Precision is required and must be a non-zero positive integer.
        // Ultimately, this just means that the on-disk format is not as small and tight as it otherwise could be.
        // https://github.com/deephaven/deephaven-core/issues/3650
        if (pas.scale > pas.precision) {
            return new PrecisionAndScale(pas.scale, pas.scale);
        }
        return pas;
    }

    static TypeInfo bigDecimalTypeInfo(
            final Map<String, Map<ParquetCacheTags, Object>> computedCache,
            @NotNull final ColumnDefinition<?> column,
            final RowSet rowSet,
            final Map<String, ? extends ColumnSource<?>> columnSourceMap) {
        final String columnName = column.getName();
        // noinspection unchecked
        final PrecisionAndScale precisionAndScale = getPrecisionAndScale(
                computedCache, columnName, rowSet, () -> (ColumnSource<BigDecimal>) columnSourceMap.get(columnName));
        final Set<Class<?>> clazzes = Collections.singleton(BigDecimal.class);
        return new TypeInfo() {
            @Override
            public Set<Class<?>> getTypes() {
                return clazzes;
            }

            @Override
            public PrimitiveBuilder<PrimitiveType> getBuilderImpl(boolean required, boolean repeating, Class dataType) {
                return type(PrimitiveTypeName.BINARY, required, repeating)
                        .as(LogicalTypeAnnotation.decimalType(precisionAndScale.scale, precisionAndScale.precision));
            }
        };
    }

    static TypeInfo getTypeInfo(
            final Map<String, Map<ParquetCacheTags, Object>> computedCache,
            @NotNull final ColumnDefinition<?> column,
            final RowSet rowSet,
            final Map<String, ? extends ColumnSource<?>> columnSourceMap,
            @NotNull final ParquetInstructions instructions) {
        final Class<?> dataType = column.getDataType();
        if (BigDecimal.class.equals(dataType)) {
            return bigDecimalTypeInfo(computedCache, column, rowSet, columnSourceMap);
        }
        return lookupTypeInfo(column, instructions);
    }

    private static boolean isRequired(ColumnDefinition<?> columnDefinition) {
        return false;// TODO change this when adding optionals support
    }

    private static PrimitiveBuilder<PrimitiveType> type(PrimitiveTypeName type, boolean required, boolean repeating) {
        return repeating ? Types.repeated(type) : (required ? Types.required(type) : Types.optional(type));
    }

    private enum IntType implements TypeInfo {
        INSTANCE;

        private static final Set<Class<?>> clazzes = Collections
                .unmodifiableSet(new HashSet<>(Arrays.asList(int.class, Integer.class)));

        @Override
        public Set<Class<?>> getTypes() {
            return clazzes;
        }

        @Override
        public PrimitiveBuilder<PrimitiveType> getBuilderImpl(boolean required, boolean repeating, Class<?> dataType) {
            return type(PrimitiveTypeName.INT32, required, repeating).as(LogicalTypeAnnotation.intType(32, true));
        }
    }

    private enum LongType implements TypeInfo {
        INSTANCE;

        private static final Set<Class<?>> clazzes = Collections
                .unmodifiableSet(new HashSet<>(Arrays.asList(long.class, Long.class)));

        @Override
        public Set<Class<?>> getTypes() {
            return clazzes;
        }

        @Override
        public PrimitiveBuilder<PrimitiveType> getBuilderImpl(boolean required, boolean repeating, Class<?> dataType) {
            return type(PrimitiveTypeName.INT64, required, repeating);
        }
    }

    private enum ShortType implements TypeInfo {
        INSTANCE;

        private static final Set<Class<?>> clazzes = Collections
                .unmodifiableSet(new HashSet<>(Arrays.asList(short.class, Short.class)));

        @Override
        public Set<Class<?>> getTypes() {
            return clazzes;
        }

        @Override
        public PrimitiveBuilder<PrimitiveType> getBuilderImpl(boolean required, boolean repeating, Class<?> dataType) {
            return type(PrimitiveTypeName.INT32, required, repeating).as(LogicalTypeAnnotation.intType(16, true));
        }
    }

    private enum BooleanType implements TypeInfo {
        INSTANCE;

        private static final Set<Class<?>> clazzes = Collections
                .unmodifiableSet(new HashSet<>(Arrays.asList(boolean.class, Boolean.class)));

        @Override
        public Set<Class<?>> getTypes() {
            return clazzes;
        }

        @Override
        public PrimitiveBuilder<PrimitiveType> getBuilderImpl(boolean required, boolean repeating, Class<?> dataType) {
            return type(PrimitiveTypeName.BOOLEAN, required, repeating);
        }
    }

    private enum FloatType implements TypeInfo {
        INSTANCE;

        private static final Set<Class<?>> clazzes = Collections
                .unmodifiableSet(new HashSet<>(Arrays.asList(float.class, Float.class)));

        @Override
        public Set<Class<?>> getTypes() {
            return clazzes;
        }

        @Override
        public PrimitiveBuilder<PrimitiveType> getBuilderImpl(boolean required, boolean repeating, Class<?> dataType) {
            return type(PrimitiveTypeName.FLOAT, required, repeating);
        }
    }

    private enum DoubleType implements TypeInfo {
        INSTANCE;

        private static final Set<Class<?>> clazzes = Collections
                .unmodifiableSet(new HashSet<>(Arrays.asList(double.class, Double.class)));

        @Override
        public Set<Class<?>> getTypes() {
            return clazzes;
        }

        @Override
        public PrimitiveBuilder<PrimitiveType> getBuilderImpl(boolean required, boolean repeating, Class<?> dataType) {
            return type(PrimitiveTypeName.DOUBLE, required, repeating);
        }
    }

    private enum CharType implements TypeInfo {
        INSTANCE;

        private static final Set<Class<?>> clazzes = Collections
                .unmodifiableSet(new HashSet<>(Arrays.asList(char.class, Character.class)));

        @Override
        public Set<Class<?>> getTypes() {
            return clazzes;
        }

        @Override
        public PrimitiveBuilder<PrimitiveType> getBuilderImpl(boolean required, boolean repeating, Class<?> dataType) {
            return type(PrimitiveTypeName.INT32, required, repeating).as(LogicalTypeAnnotation.intType(16, false));
        }
    }

    private enum ByteType implements TypeInfo {
        INSTANCE;

        private static final Set<Class<?>> clazzes = Collections
                .unmodifiableSet(new HashSet<>(Arrays.asList(byte.class, Byte.class)));

        @Override
        public Set<Class<?>> getTypes() {
            return clazzes;
        }

        @Override
        public PrimitiveBuilder<PrimitiveType> getBuilderImpl(boolean required, boolean repeating, Class<?> dataType) {
            return type(PrimitiveTypeName.INT32, required, repeating).as(LogicalTypeAnnotation.intType(8, true));
        }
    }

    private enum StringType implements TypeInfo {
        INSTANCE;

        private static final Set<Class<?>> clazzes = Collections.singleton(String.class);

        @Override
        public Set<Class<?>> getTypes() {
            return clazzes;
        }

        @Override
        public PrimitiveBuilder<PrimitiveType> getBuilderImpl(boolean required, boolean repeating, Class<?> dataType) {
            return type(PrimitiveTypeName.BINARY, required, repeating)
                    .as(LogicalTypeAnnotation.stringType());
        }
    }

    private enum InstantType implements TypeInfo {
        INSTANCE;

        private static final Set<Class<?>> clazzes = Collections.singleton(Instant.class);

        @Override
        public Set<Class<?>> getTypes() {
            return clazzes;
        }

        @Override
        public PrimitiveBuilder<PrimitiveType> getBuilderImpl(boolean required, boolean repeating, Class<?> dataType) {
            // Write instants as Parquet TIMESTAMP(isAdjustedToUTC = true, unit = NANOS)
            return type(PrimitiveTypeName.INT64, required, repeating)
                    .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS));
        }
    }

    private enum LocalDateTimeType implements TypeInfo {
        INSTANCE;

        private static final Set<Class<?>> clazzes = Collections.singleton(LocalDateTime.class);

        @Override
        public Set<Class<?>> getTypes() {
            return clazzes;
        }

        @Override
        public PrimitiveBuilder<PrimitiveType> getBuilderImpl(boolean required, boolean repeating, Class<?> dataType) {
            // Write LocalDateTime as Parquet TIMESTAMP(isAdjustedToUTC = false, unit = NANOS)
            return type(PrimitiveTypeName.INT64, required, repeating)
                    .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS));
        }
    }

    private enum LocalDateType implements TypeInfo {
        INSTANCE;

        private static final Set<Class<?>> clazzes = Collections.singleton(LocalDate.class);

        @Override
        public Set<Class<?>> getTypes() {
            return clazzes;
        }

        @Override
        public PrimitiveBuilder<PrimitiveType> getBuilderImpl(boolean required, boolean repeating, Class<?> dataType) {
            return type(PrimitiveTypeName.INT32, required, repeating)
                    .as(LogicalTypeAnnotation.dateType());
        }
    }

    private enum LocalTimeType implements TypeInfo {
        INSTANCE;

        private static final Set<Class<?>> clazzes = Collections.singleton(LocalTime.class);

        @Override
        public Set<Class<?>> getTypes() {
            return clazzes;
        }

        @Override
        public PrimitiveBuilder<PrimitiveType> getBuilderImpl(boolean required, boolean repeating, Class<?> dataType) {
            // Always write in (isAdjustedToUTC = true, unit = NANOS) format
            return type(PrimitiveTypeName.INT64, required, repeating)
                    .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.NANOS));
        }
    }


    /**
     * We will encode BigIntegers as Decimal types. Parquet has no special type for BigIntegers, but we can maintain
     * external compatibility by encoding them as fixed length decimals of scale 1. Internally, we'll record that we
     * wrote this as a decimal, so we can properly decode it back to BigInteger.
     */
    private enum BigIntegerType implements TypeInfo {
        INSTANCE;

        private static final Set<Class<?>> clazzes = Collections.singleton(BigInteger.class);

        @Override
        public Set<Class<?>> getTypes() {
            return clazzes;
        }

        @Override
        public PrimitiveBuilder<PrimitiveType> getBuilderImpl(boolean required, boolean repeating, Class<?> dataType) {
            return type(PrimitiveTypeName.BINARY, required, repeating)
                    .as(LogicalTypeAnnotation.decimalType(0, 1));
        }
    }

    interface TypeInfo {

        Set<Class<?>> getTypes();

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        default boolean isValidFor(Class<?> clazz) {
            return getTypes().contains(clazz);
        }

        default PrimitiveBuilder<PrimitiveType> getBuilderImpl(boolean required, boolean repeating, Class<?> dataType) {
            throw new UnsupportedOperationException("Implement this method if using the default getBuilder()");
        }

        default PrimitiveBuilder<PrimitiveType> getBuilder(boolean required, boolean repeating, Class<?> dataType) {
            if (!isValidFor(dataType)) {
                throw new IllegalArgumentException("Invalid data type " + dataType);
            }
            return getBuilderImpl(required, repeating, dataType);
        }

        default Type createSchemaType(
                @NotNull final ColumnDefinition<?> columnDefinition,
                @NotNull final ParquetInstructions instructions) {
            final Class<?> dataType = columnDefinition.getDataType();
            final Class<?> componentType = columnDefinition.getComponentType();
            final String parquetColumnName =
                    instructions.getParquetColumnNameFromColumnNameOrDefault(columnDefinition.getName());

            final PrimitiveBuilder<PrimitiveType> builder;
            final boolean isRepeating;
            if (CodecLookup.explicitCodecPresent(instructions.getCodecName(columnDefinition.getName()))
                    || CodecLookup.codecRequired(columnDefinition)) {
                builder = getBuilder(isRequired(columnDefinition), false, dataType);
                isRepeating = false;
            } else if (componentType != null) {
                builder = getBuilder(isRequired(columnDefinition), false, componentType);
                isRepeating = true;
            } else if (StringSet.class.isAssignableFrom(dataType)) {
                builder = getBuilder(isRequired(columnDefinition), false, String.class);
                isRepeating = true;
            } else {
                builder = getBuilder(isRequired(columnDefinition), false, dataType);
                isRepeating = false;
            }
            if (!isRepeating) {
                return builder.named(parquetColumnName);
            }
            return Types.buildGroup(Type.Repetition.OPTIONAL).addField(
                    Types.buildGroup(Type.Repetition.REPEATED).addField(
                            builder.named("item")).named(parquetColumnName))
                    .as(LogicalTypeAnnotation.listType()).named(parquetColumnName);
        }
    }

    private static class CodecType<T> implements TypeInfo {

        CodecType() {}

        @Override
        public Set<Class<?>> getTypes() {
            throw new UnsupportedOperationException("Codec types are not being mapped");
        }

        @Override
        public PrimitiveBuilder<PrimitiveType> getBuilder(boolean required, boolean repeating, Class<?> dataType) {
            return type(PrimitiveTypeName.BINARY, required, repeating);
        }
    }
}
