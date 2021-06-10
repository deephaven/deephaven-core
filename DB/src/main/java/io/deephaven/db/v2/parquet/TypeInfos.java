package io.deephaven.db.v2.parquet;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.tables.libs.StringSet;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.util.codec.ExternalizableCodec;
import io.deephaven.util.codec.SerializableCodec;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.PrimitiveBuilder;

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

    private static final Map<Class, TypeInfo> BY_CLASS;

    static {
        Map<Class, TypeInfo> fa = new HashMap<>();
        for (TypeInfo typeInfo : TYPE_INFOS) {
            for (Class<?> type : typeInfo.getTypes()) {
                fa.put(type, typeInfo);
            }
        }
        BY_CLASS = Collections.unmodifiableMap(fa);
    }

    private static Optional<TypeInfo> lookupTypeInfo(Class clazz) {
        return Optional.ofNullable(BY_CLASS.get(clazz));
    }

    private static Optional<TypeInfo> lookupTypeInfo(ColumnSource column) {
        return lookupTypeInfo(column.getType());
    }

    private static TypeInfo lookupTypeInfo(ColumnDefinition column) {
        if (column.getComponentType() != null && column.getObjectCodecType() == ColumnDefinition.ObjectCodecType.DEFAULT) {
            return lookupTypeInfo(column.getComponentType()).orElseGet(() -> new CodecType());
        }
        if (StringSet.class.isAssignableFrom(column.getDataType())) {
            return lookupTypeInfo(String.class).get();
        }
        return lookupTypeInfo(column.getDataType()).orElseGet(() -> new CodecType());
    }

    static Pair<String, String> getCodecAndArgs(ColumnDefinition columnDefinition) {
        final String objectCodecClass = columnDefinition.getObjectCodecClass();
        // Explicit codecs always take precedence
        if (objectCodecClass != null && !objectCodecClass.equals(ColumnDefinition.ObjectCodecType.DEFAULT.name())) {
            return new ImmutablePair<>(objectCodecClass, columnDefinition.getObjectCodecArguments());
        }
        // No need to impute a codec for any basic formats we already understand
        final Class<?> dataType = columnDefinition.getDataType();
        if (dataType.isPrimitive() || dataType == String.class || StringSet.class.isAssignableFrom(dataType) || dataType == Boolean.class || dataType == DBDateTime.class) {
            return null;
        }
        if (dataType.isArray() || DbArrayBase.class.isAssignableFrom(dataType)) {
            final Class<?> componentType = columnDefinition.getComponentType();
            if (componentType.isPrimitive() || componentType == Boolean.class) {
                return null;
            }
        }
        /* TODO (deephaven/deephaven-core/issues/622):
         *   1. We should maybe distinguish between "imputed codec" like this and "specified codec", although that may be
         *      less relevant if we remove codec information from the ColumnDefinition.
         *   2. We need to come up with a strategy to describe nested codecs (e.g. arrays of codec'd things). Right now
         *      we can either codec the whole thing, or codec the components, and we haven't defined this well. DHE
         *      would only apply it to the entire thing, but maybe an alternative option makes sense here.
         *   3. We need to write down data type and component type.
         */
        if (Externalizable.class.isAssignableFrom(dataType)) {
            return new ImmutablePair<>(ExternalizableCodec.class.getName(), columnDefinition.getDataType().getName());
        }
        return new ImmutablePair<>(SerializableCodec.class.getName(), "");

    }

    static TypeInfo getTypeInfo(ColumnSource column) throws SchemaMappingException {
        return lookupTypeInfo(column).orElseThrow(() -> new SchemaMappingException(
                "Unable to find TypeInfo for ColumnSource of type " + column
                        .getType()));
    }

    static TypeInfo getTypeInfo(ColumnDefinition column) throws SchemaMappingException {
        return lookupTypeInfo(column);
    }

    private static boolean isRequired(ColumnDefinition columnDefinition) {
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

        default Type createSchemaType(ColumnDefinition columnDefinition) {
            PrimitiveBuilder<PrimitiveType> builder;
            boolean isRepeating = true;
            if (columnDefinition.getComponentType() != null && columnDefinition.getObjectCodecType() == ColumnDefinition.ObjectCodecType.DEFAULT) {
                builder = getBuilder(isRequired(columnDefinition), false, columnDefinition.getComponentType());
            } else if (StringSet.class.isAssignableFrom(columnDefinition.getDataType())) {
                builder = getBuilder(isRequired(columnDefinition), false, String.class);
            } else {
                builder = getBuilder(isRequired(columnDefinition), false, columnDefinition.getDataType());
                isRepeating = false;
            }
            if (!isRepeating) {
                return builder.named(columnDefinition.getName());
            } else {
                return Types.buildGroup(Type.Repetition.OPTIONAL).addField(
                        Types.buildGroup(Type.Repetition.REPEATED).addField(
                                builder.named("item")).named(columnDefinition.getName())
                ).as(LogicalTypeAnnotation.listType()).named(columnDefinition.getName());
            }
        }

        PrimitiveBuilder<PrimitiveType> getBuilder(boolean required, boolean repeating, Class dataType);
    }

    /**
     * Maps data from {@link ColumnSource} to {@link org.apache.parquet.io.api.RecordConsumer}
     */
    static abstract class FieldAdapter {

        final ColumnSource column;
        final String fieldName;
        final int fieldIndex;

        FieldAdapter(ColumnSource column, String fieldName, int fieldIndex) {
            this.column = column;
            this.fieldName = fieldName;
            this.fieldIndex = fieldIndex;
        }

        abstract void write(RecordConsumer context, long tableIndex);
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