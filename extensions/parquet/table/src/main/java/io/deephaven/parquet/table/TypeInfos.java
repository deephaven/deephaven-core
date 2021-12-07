package io.deephaven.parquet.table;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.CodecLookup;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.stringset.StringSet;
import io.deephaven.time.DateTime;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.RowSequence;
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
import java.util.*;
import java.util.function.Supplier;

/**
 * Contains the necessary information to convert a Deephaven table into a Parquet table. Both the schema translation,
 * and the data translation.
 */
class TypeInfos {

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
            DateTimeType.INSTANCE
    };

    private static final Map<Class<?>, TypeInfo> BY_CLASS;

    static {
        Map<Class<?>, TypeInfo> fa = new HashMap<>();
        for (TypeInfo typeInfo : TYPE_INFOS) {
            for (Class<?> type : typeInfo.getTypes()) {
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

    static class PrecisionAndScale {
        public final int precision;
        public final int scale;

        public PrecisionAndScale(final int precision, final int scale) {
            this.precision = precision;
            this.scale = scale;
        }
    }

    private static PrecisionAndScale computePrecisionAndScale(final TrackingRowSet rowSet,
                                                              final ColumnSource<BigDecimal> source) {
        final int sz = 4096;
        // we first compute max(precision - scale) and max(scale), which corresponds to
        // max(digits left of the decimal point), max(digits right of the decimal point).
        // Then we convert to (precision, scale) before returning.
        int maxPrecisionMinusScale = 0;
        int maxScale = 0;
        try (final ChunkSource.GetContext context = source.makeGetContext(sz);
                final RowSequence.Iterator it = rowSet.getRowSequenceIterator()) {
            final RowSequence rowSeq = it.getNextRowSequenceWithLength(sz);
            final ObjectChunk<BigDecimal, ? extends Values> chunk = source.getChunk(context, rowSeq).asObjectChunk();
            for (int i = 0; i < chunk.size(); ++i) {
                final BigDecimal x = chunk.get(i);
                final int precision = x.precision();
                final int scale = x.scale();
                final int precisionMinusScale = precision - scale;
                if (precisionMinusScale > maxPrecisionMinusScale) {
                    maxPrecisionMinusScale = precisionMinusScale;
                }
                if (scale > maxScale) {
                    maxScale = scale;
                }
            }
        }
        return new PrecisionAndScale(maxPrecisionMinusScale + maxScale, maxScale);
    }

    static PrecisionAndScale getPrecisionAndScale(
            final Map<String, Map<ParquetTableWriter.CacheTags, Object>> computedCache,
            final String columnName,
            final TrackingRowSet rowSet,
            Supplier<ColumnSource<BigDecimal>> columnSourceSupplier) {
        return (PrecisionAndScale) computedCache
                .computeIfAbsent(columnName, unusedColumnName -> new HashMap<>())
                .computeIfAbsent(ParquetTableWriter.CacheTags.DECIMAL_ARGS,
                        unusedCacheTag -> computePrecisionAndScale(rowSet, columnSourceSupplier.get()));
    }

    static TypeInfo bigDecimalTypeInfo(
            final Map<String, Map<ParquetTableWriter.CacheTags, Object>> computedCache,
            @NotNull final ColumnDefinition<?> column,
            final TrackingRowSet rowSet,
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
            public PrimitiveBuilder<PrimitiveType> getBuilder(boolean required, boolean repeating, Class dataType) {
                if (!isValidFor(dataType)) {
                    throw new IllegalArgumentException("Invalid data type " + dataType);
                }
                return type(PrimitiveTypeName.BINARY, required, repeating)
                        .as(LogicalTypeAnnotation.decimalType(precisionAndScale.scale, precisionAndScale.precision));
            }
        };
    }

    static TypeInfo getTypeInfo(
            final Map<String, Map<ParquetTableWriter.CacheTags, Object>> computedCache,
            @NotNull final ColumnDefinition<?> column,
            final TrackingRowSet rowSet,
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
        public PrimitiveBuilder<PrimitiveType> getBuilder(boolean required, boolean repeating, Class dataType) {
            if (!isValidFor(dataType)) {
                throw new IllegalArgumentException("Invalid data type " + dataType);
            }
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
        public PrimitiveBuilder<PrimitiveType> getBuilder(boolean required, boolean repeating, Class dataType) {
            if (!isValidFor(dataType)) {
                throw new IllegalArgumentException("Invalid data type " + dataType);
            }
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
        public PrimitiveBuilder<PrimitiveType> getBuilder(boolean required, boolean repeating, Class dataType) {
            if (!isValidFor(dataType)) {
                throw new IllegalArgumentException("Invalid data type " + dataType);
            }
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
        public PrimitiveBuilder<PrimitiveType> getBuilder(boolean required, boolean repeating, Class dataType) {
            if (!isValidFor(dataType)) {
                throw new IllegalArgumentException("Invalid data type " + dataType);
            }
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
        public PrimitiveBuilder<PrimitiveType> getBuilder(boolean required, boolean repeating, Class dataType) {
            if (!isValidFor(dataType)) {
                throw new IllegalArgumentException("Invalid data type " + dataType);
            }
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
        public PrimitiveBuilder<PrimitiveType> getBuilder(boolean required, boolean repeating, Class dataType) {
            if (!isValidFor(dataType)) {
                throw new IllegalArgumentException("Invalid data type " + dataType);
            }
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
        public PrimitiveBuilder<PrimitiveType> getBuilder(boolean required, boolean repeating, Class dataType) {
            if (!isValidFor(dataType)) {
                throw new IllegalArgumentException("Invalid data type " + dataType);
            }
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
        public PrimitiveBuilder<PrimitiveType> getBuilder(boolean required, boolean repeating, Class dataType) {
            if (!isValidFor(dataType)) {
                throw new IllegalArgumentException("Invalid data type " + dataType);
            }
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
        public PrimitiveBuilder<PrimitiveType> getBuilder(boolean required, boolean repeating, Class dataType) {
            if (!isValidFor(dataType)) {
                throw new IllegalArgumentException("Invalid data type " + dataType);
            }
            return type(PrimitiveTypeName.BINARY, required, repeating)
                    .as(LogicalTypeAnnotation.stringType());
        }
    }

    /**
     * TODO: newer versions of parquet seem to support NANOS, but this version seems to only support MICROS
     */
    private enum DateTimeType implements TypeInfo {
        INSTANCE;

        private static final Set<Class<?>> clazzes = Collections.singleton(DateTime.class);

        @Override
        public Set<Class<?>> getTypes() {
            return clazzes;
        }

        @Override
        public PrimitiveBuilder<PrimitiveType> getBuilder(boolean required, boolean repeating, Class dataType) {
            if (!isValidFor(dataType)) {
                throw new IllegalArgumentException("Invalid data type " + dataType);
            }
            return type(PrimitiveTypeName.INT64, required, repeating)
                    .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS));
        }
    }

    interface TypeInfo {

        Set<Class<?>> getTypes();

        default boolean isValidFor(Class<?> clazz) {
            return getTypes().contains(clazz);
        }

        default Type createSchemaType(
                @NotNull final ColumnDefinition<?> columnDefinition,
                @NotNull final ParquetInstructions instructions) {
            final Class<?> dataType = columnDefinition.getDataType();
            final Class<?> componentType = columnDefinition.getComponentType();

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
                return builder.named(columnDefinition.getName());
            }
            return Types.buildGroup(Type.Repetition.OPTIONAL).addField(
                    Types.buildGroup(Type.Repetition.REPEATED).addField(
                            builder.named("item")).named(columnDefinition.getName()))
                    .as(LogicalTypeAnnotation.listType()).named(columnDefinition.getName());
        }

        PrimitiveBuilder<PrimitiveType> getBuilder(boolean required, boolean repeating, Class dataType);
    }

    private static class CodecType<T> implements TypeInfo {

        CodecType() {}

        @Override
        public Set<Class<?>> getTypes() {
            throw new UnsupportedOperationException("Codec types are not being mapped");
        }

        @Override
        public PrimitiveBuilder<PrimitiveType> getBuilder(boolean required, boolean repeating, Class dataType) {

            return type(PrimitiveTypeName.BINARY, required, repeating);
        }
    }
}
