package io.deephaven.db.v2.parquet;

import io.deephaven.db.tables.CodecLookup;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.libs.StringSet;
import io.deephaven.db.tables.utils.DBDateTime;
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

import javax.validation.constraints.NotNull;
import java.io.Externalizable;
import java.util.*;

/**
 * Contains the necessary information to convert a Deephaven table into a Parquet table. Both the
 * schema translation, and the data translation.
 */
class TypeInfos {

    private static final TypeInfo[] TYPE_INFOS = new TypeInfo[]{
            IntType.INSTANCE,
            LongType.INSTANCE,
            ShortType.INSTANCE,
            BooleanType.INSTANCE,
            FloatType.INSTANCE,
            DoubleType.INSTANCE,
            CharType.INSTANCE,
            ByteType.INSTANCE,
            StringType.INSTANCE,
            DBDateTimeType.INSTANCE
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
            @NotNull final ParquetInstructions instructions
    ) {
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
            @NotNull final ParquetInstructions instructions
    ) {
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

    static TypeInfo getTypeInfo(
            @NotNull final ColumnDefinition<?> column,
            @NotNull final ParquetInstructions instructions
    ) {
        return lookupTypeInfo(column, instructions);
    }

    private static boolean isRequired(ColumnDefinition<?> columnDefinition) {
        return false;//TODO change this when adding optionals support
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
                    .as(LogicalTypeAnnotation.stringType())
                    ;
        }
    }

    /**
     * TODO: newer versions of parquet seem to support NANOS, but this version seems to only support
     * MICROS
     */
    private enum DBDateTimeType implements TypeInfo {
        INSTANCE;

        private static final Set<Class<?>> clazzes = Collections.singleton(DBDateTime.class);

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
                    .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
                    ;
        }
    }

    interface TypeInfo {

        Set<Class<?>> getTypes();

        default boolean isValidFor(Class<?> clazz) {
            return getTypes().contains(clazz);
        }

        default Type createSchemaType(
                @NotNull final ColumnDefinition<?> columnDefinition,
                @NotNull final ParquetInstructions instructions
        ) {
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
                            builder.named("item")).named(columnDefinition.getName())
            ).as(LogicalTypeAnnotation.listType()).named(columnDefinition.getName());
        }

        PrimitiveBuilder<PrimitiveType> getBuilder(boolean required, boolean repeating, Class dataType);
    }

    private static class CodecType<T> implements TypeInfo {

        CodecType() {
        }

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
